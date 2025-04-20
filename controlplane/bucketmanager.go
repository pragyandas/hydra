package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/common"
	"github.com/pragyandas/hydra/connection"
	"github.com/pragyandas/hydra/controlplane/actormonitor"
	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

// BucketManager handles the ownership and management of buckets across the cluster
type BucketManager struct {
	numBuckets            int
	membership            *Membership
	ownedBuckets          map[int]*BucketOwnership
	bucketMonitors        map[int]*actormonitor.ActorDeathMonitor
	kv                    jetstream.KeyValue
	connection            *connection.Connection
	mu                    sync.RWMutex
	actorResurrectionChan chan actor.ActorId
}

// NewBucketManager creates a new bucket manager
func NewBucketManager(connection *connection.Connection, membership *Membership, actorResurrectionChan chan actor.ActorId) *BucketManager {
	return &BucketManager{
		membership:            membership,
		ownedBuckets:          make(map[int]*BucketOwnership),
		bucketMonitors:        make(map[int]*actormonitor.ActorDeathMonitor),
		connection:            connection,
		actorResurrectionChan: actorResurrectionChan,
	}
}

// Start initializes the bucket manager
func (bm *BucketManager) Start(ctx context.Context, config BucketManagerConfig) error {
	logger := telemetry.GetLogger(ctx, "bucketmanager-start")

	bm.numBuckets = config.NumBuckets

	// Initialize KV store
	kv, err := bm.connection.JS.CreateKeyValue(ctx, config.KVConfig)
	if err != nil {
		if err == jetstream.ErrBucketExists {
			kv, err = bm.connection.JS.KeyValue(ctx, config.KVConfig.Bucket)
			if err != nil {
				return fmt.Errorf("failed to get existing KV store: %w", err)
			}
		} else {
			return fmt.Errorf("failed to create KV store: %w", err)
		}
	}
	bm.kv = kv

	logger.Debug("started bucket manager")
	return nil
}

// Stop gracefully shuts down the bucket manager
func (bm *BucketManager) Stop(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "bucketmanager-stop")

	// Stop all bucket monitors
	bm.mu.Lock()
	for bucket, monitor := range bm.bucketMonitors {
		monitor.Stop(ctx)
		delete(bm.bucketMonitors, bucket)
	}
	bm.mu.Unlock()

	logger.Debug("stopped control plane bucket manager")
}

// GetOwnedBuckets returns a list of buckets owned by this node
func (bm *BucketManager) GetOwnedBuckets() []int {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	ownedBuckets := make([]int, 0, len(bm.ownedBuckets))
	for bucket := range bm.ownedBuckets {
		ownedBuckets = append(ownedBuckets, bucket)
	}

	return ownedBuckets
}

// RecalculateBuckets recalculates which buckets this node should own based on the membership
func (bm *BucketManager) RecalculateBuckets(ctx context.Context, memberCount, selfIndex int) {
	logger := telemetry.GetLogger(ctx, "bucketmanager-recalculateBuckets")

	// Skip if the context is already cancelled
	if ctx.Err() != nil {
		logger.Debug("skipping bucket recalculation, context cancelled")
		return
	}

	// Calculate eligible buckets
	eligibleBuckets := bm.getEligibleBuckets(ctx, memberCount, selfIndex)

	// Create a map for easy lookups
	eligibleSet := make(map[int]struct{})
	for _, b := range eligibleBuckets {
		eligibleSet[b] = struct{}{}
	}

	// Release buckets we shouldn't own
	bm.mu.RLock()
	bucketsToRelease := make([]int, 0)
	for bucket := range bm.ownedBuckets {
		if _, isEligible := eligibleSet[bucket]; !isEligible {
			bucketsToRelease = append(bucketsToRelease, bucket)
		}
	}
	bm.mu.RUnlock()

	// Release all ineligible buckets
	for _, bucket := range bucketsToRelease {
		logger.Debug("releasing ineligible bucket", zap.Int("bucket", bucket))
		bm.releaseBucket(ctx, bucket)
	}

	// Try to claim eligible buckets - simplified synchronous version
	for _, bucket := range eligibleBuckets {
		// Skip if the context is cancelled
		if ctx.Err() != nil {
			logger.Debug("stopping bucket claim attempts, context cancelled")
			return
		}

		// If we already own this bucket, skip it
		bm.mu.RLock()
		alreadyOwned := false
		if _, exists := bm.ownedBuckets[bucket]; exists {
			alreadyOwned = true
		}
		bm.mu.RUnlock()

		if alreadyOwned {
			logger.Debug("bucket already owned, skipping", zap.Int("bucket", bucket))
			continue
		}

		if err := bm.claimBucket(ctx, bucket); err != nil {
			logger.Debug("failed to claim bucket",
				zap.Int("bucket", bucket),
				zap.Error(err))
		}
	}
}

// getEligibleBuckets calculates which buckets this node is eligible to own
func (bm *BucketManager) getEligibleBuckets(ctx context.Context, memberCount, selfIndex int) []int {
	logger := telemetry.GetLogger(ctx, "bucketmanager-getEligibleBuckets")

	if selfIndex == -1 {
		logger.Warn("couldn't find self in member list")
		return nil
	}

	buckets := make([]int, 0)
	// Calculate which buckets belong to us based on our index
	for bucket := 0; bucket < bm.numBuckets; bucket++ {
		if bucket%memberCount == selfIndex {
			buckets = append(buckets, bucket)
		}
	}

	logger.Info("eligible buckets", zap.Ints("buckets", buckets))
	return buckets
}

// claimBucket attempts to claim a single bucket
func (bm *BucketManager) claimBucket(ctx context.Context, bucket int) error {
	logger := telemetry.GetLogger(ctx, "bucketmanager-claimBucket")
	region, systemID := common.GetRegion(ctx), common.GetSystemID(ctx)

	// Skip if the context is already cancelled
	if ctx.Err() != nil {
		return ctx.Err()
	}

	key := fmt.Sprintf("%s.%d", region, bucket)
	entry, err := bm.kv.Get(ctx, key)

	// If the key doesn't exist, we can create it
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			logger.Debug("bucket not found, claiming it", zap.Int("bucket", bucket))

			ownership := &BucketOwnership{
				Owner:          systemID,
				LastUpdateTime: time.Now(),
			}
			data, err := json.Marshal(ownership)
			if err != nil {
				return err
			}
			_, err = bm.kv.Put(ctx, key, data)
			if err != nil {
				return err
			}

			bm.mu.Lock()
			bm.ownedBuckets[bucket] = ownership
			bm.mu.Unlock()

			if err := bm.startActorDeathMonitor(ctx, bucket); err != nil {
				return err
			}

			return nil
		}
		return err
	}

	// Key exists, check if we should take ownership
	var currentOwnership BucketOwnership
	if err := json.Unmarshal(entry.Value(), &currentOwnership); err != nil {
		return err
	}

	// If we already own it, just ensure our local state is consistent
	if currentOwnership.Owner == systemID {
		logger.Debug("bucket already owned by self, ensuring consistency", zap.Int("bucket", bucket))
		bm.mu.Lock()
		bm.ownedBuckets[bucket] = &currentOwnership

		// Ensure the monitor is running
		if _, exists := bm.bucketMonitors[bucket]; !exists {
			if err := bm.startActorDeathMonitor(ctx, bucket); err != nil {
				logger.Error("failed to start actor death monitor", zap.Error(err))
			}
		}

		bm.mu.Unlock()
		return nil
	}

	// If the current owner is active, we don't try to claim it
	if bm.membership.IsMemberActive(currentOwnership.Owner) {
		logger.Info("bucket already owned by active member, skipping",
			zap.Int("bucket", bucket),
			zap.String("owner", currentOwnership.Owner))
		return nil
	}

	// The owner is not active, try to claim it
	logger.Debug("bucket owned by inactive member, claiming",
		zap.Int("bucket", bucket),
		zap.String("previous_owner", currentOwnership.Owner))

	ownership := &BucketOwnership{
		Owner:          systemID,
		LastUpdateTime: time.Now(),
	}
	data, err := json.Marshal(ownership)
	if err != nil {
		return err
	}

	// Update with revision check to ensure atomicity
	_, err = bm.kv.Update(ctx, key, data, entry.Revision())
	if err != nil {
		if err == jetstream.ErrKeyExists {
			// Someone else updated it first, that's OK
			logger.Debug("bucket claimed by another node during our attempt",
				zap.Int("bucket", bucket))
			return nil
		}
		return err
	}

	// We successfully claimed the bucket
	bm.mu.Lock()
	bm.ownedBuckets[bucket] = ownership
	bm.mu.Unlock()

	if err := bm.startActorDeathMonitor(ctx, bucket); err != nil {
		logger.Error("failed to start actor death monitor", zap.Error(err))
	}

	logger.Info("successfully claimed bucket from inactive owner",
		zap.Int("bucket", bucket),
		zap.String("previous_owner", currentOwnership.Owner))

	return nil
}

// startActorDeathMonitor starts a monitor for actor deaths in a bucket
func (bm *BucketManager) startActorDeathMonitor(ctx context.Context, bucket int) error {
	logger := telemetry.GetLogger(ctx, "bucketmanager-startActorDeathMonitor")

	logger.Debug("starting death monitor for bucket", zap.Int("bucket", bucket))

	bm.mu.Lock()
	defer bm.mu.Unlock()

	// If a monitor already exists, do nothing
	if _, exists := bm.bucketMonitors[bucket]; exists {
		return nil
	}

	monitor := actormonitor.NewActorDeathMonitor(
		bm.connection,
		bucket,
		bm.actorResurrectionChan,
	)

	if err := monitor.Start(ctx); err != nil {
		return fmt.Errorf("failed to start death monitor for bucket %d: %w", bucket, err)
	}

	bm.bucketMonitors[bucket] = monitor
	logger.Debug("started death monitor for bucket", zap.Int("bucket", bucket))

	return nil
}

// releaseBucket releases ownership of a bucket
func (bm *BucketManager) releaseBucket(ctx context.Context, bucket int) {
	logger := telemetry.GetLogger(ctx, "bucketmanager-releaseBucket")
	region := common.GetRegion(ctx)

	// Delete the bucket from KV store
	key := fmt.Sprintf("%s.%d", region, bucket)
	err := bm.kv.Delete(ctx, key)
	if err != nil {
		logger.Error("failed to delete bucket", zap.Int("bucket", bucket), zap.Error(err))
		return
	}

	// Clean up local state
	bm.mu.Lock()
	delete(bm.ownedBuckets, bucket)

	// Stop and cleanup the bucket monitor
	if monitor, exists := bm.bucketMonitors[bucket]; exists {
		monitor.Stop(ctx)
		delete(bm.bucketMonitors, bucket)
	}
	bm.mu.Unlock()

	logger.Debug("released bucket", zap.Int("bucket", bucket))
}

// calculateBucket calculates which bucket an actor belongs to
func (bm *BucketManager) calculateBucket(actorType, actorID string) int {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%s.%s", actorType, actorID)))
	return int(h.Sum32()) % bm.numBuckets
}

// GetBucketKey returns the KV store key for an actor
func (bm *BucketManager) GetBucketKey(actorType, actorID, region string) string {
	bucket := bm.calculateBucket(actorType, actorID)
	return fmt.Sprintf("%s.%d.%s.%s", region, bucket, actorType, actorID)
}
