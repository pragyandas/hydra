package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/common"
	"github.com/pragyandas/hydra/connection"
	"github.com/pragyandas/hydra/controlplane/actormonitor"
	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

type BucketManager struct {
	numBuckets            int
	membership            *Membership
	ownedBuckets          map[int]*BucketOwnership
	bucketMonitors        map[int]*actormonitor.ActorDeathMonitor
	kv                    jetstream.KeyValue
	connection            *connection.Connection
	mu                    sync.RWMutex
	interestSub           *nats.Subscription
	claimMu               sync.Mutex
	actorResurrectionChan chan actor.ActorId
}

func NewBucketManager(connection *connection.Connection, membership *Membership, actorResurrectionChan chan actor.ActorId) *BucketManager {
	return &BucketManager{
		membership:            membership,
		ownedBuckets:          make(map[int]*BucketOwnership),
		bucketMonitors:        make(map[int]*actormonitor.ActorDeathMonitor),
		connection:            connection,
		actorResurrectionChan: actorResurrectionChan,
	}
}

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

	// Subscribe to bucket interest messages
	if err := bm.setupBucketInterestSubscription(ctx); err != nil {
		return fmt.Errorf("failed to setup bucket interest subscription: %w", err)
	}

	// Start periodic bucket safety check
	go bm.bucketDiscoveryLoop(ctx, config.SafetyCheckInterval)

	logger.Debug("started bucket manager")

	return nil
}

func (bm *BucketManager) Stop(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "bucketmanager-stop")

	if bm.interestSub != nil {
		bm.interestSub.Unsubscribe()
	}

	logger.Debug("stopped control plane bucket manager")
}

func (bm *BucketManager) setupBucketInterestSubscription(ctx context.Context) error {
	logger := telemetry.GetLogger(ctx, "bucketmanager-setupBucketInterestSubscription")
	region, systemID := common.GetRegion(), common.GetSystemID()

	sub, err := bm.connection.NC.Subscribe(fmt.Sprintf("bucketinterest.%s.%s", region, systemID), func(msg *nats.Msg) {
		var interest BucketInterest
		if err := json.Unmarshal(msg.Data, &interest); err != nil {
			logger.Error("failed to unmarshal bucket interest", zap.Error(err))
			return
		}
		bm.handleInterest(ctx, &interest)
	})

	if err != nil {
		return fmt.Errorf("failed to subscribe to bucket interest: %w", err)
	}
	bm.interestSub = sub
	return nil
}

func (bm *BucketManager) bucketDiscoveryLoop(ctx context.Context, interval time.Duration) {
	logger := telemetry.GetLogger(ctx, "bucketmanager-bucketDiscoveryLoop")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Debug("bucket discovery loop cancelled")
			return
		case <-ticker.C:
			bm.RecalculateBuckets(ctx)
		}
	}
}

func (bm *BucketManager) RecalculateBuckets(ctx context.Context) {
	eligibleBuckets := bm.getEligibleBuckets(ctx)

	if len(eligibleBuckets) > 0 {
		bm.tryClaimBuckets(ctx, eligibleBuckets)
	}
}

func (bm *BucketManager) getEligibleBuckets(ctx context.Context) []int {
	logger := telemetry.GetLogger(ctx, "bucketmanager-getEligibleBuckets")

	memberCount, selfIndex := bm.membership.GetMemberCountAndPosition()
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

	return buckets
}

func (bm *BucketManager) tryClaimBuckets(ctx context.Context, buckets []int) {
	logger := telemetry.GetLogger(ctx, "bucketmanager-tryClaimBuckets")

	// We should make sure only one instance of this is running
	if !bm.claimMu.TryLock() {
		logger.Debug("another instance of this is already running")
		return
	}

	defer bm.claimMu.Unlock()

	var wg sync.WaitGroup
	errChan := make(chan error, len(buckets))
	// TODO: Use task queue here
	// Tweak claim concurrency by changing the number of semaphore tokens
	semaphore := make(chan struct{}, 10)

	for _, bucket := range buckets {
		wg.Add(1)
		go func(b int) {
			defer wg.Done()

			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			}

			if err := bm.claimBucket(ctx, b); err != nil {
				errChan <- fmt.Errorf("failed to claim bucket %d: %w", b, err)
				return
			}
		}(bucket)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		logger.Debug("bucket claim operation cancelled", zap.Error(ctx.Err()))
	case <-done:
		close(errChan)
	}

	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		logger.Debug("some bucket claims failed", zap.Errors("errors", errors))
	}
}

func (bm *BucketManager) claimBucket(ctx context.Context, bucket int) error {
	logger := telemetry.GetLogger(ctx, "bucketmanager-claimBucket")

	region, systemID := common.GetRegion(), common.GetSystemID()

	// TODO: Make this . separated
	key := fmt.Sprintf("%s/%s/%d", bm.kv.Bucket(), region, bucket)
	ownership := &BucketOwnership{
		Owner:          systemID,
		LastUpdateTime: time.Now(),
	}

	data, err := json.Marshal(ownership)
	if err != nil {
		return err
	}

	// Try to create first
	_, err = bm.kv.Create(ctx, key, data)
	if err == nil {
		bm.mu.Lock()
		bm.ownedBuckets[bucket] = ownership
		bm.mu.Unlock()

		if err := bm.startActorDeathMonitor(ctx, bucket); err != nil {
			logger.Error("failed to start actor death monitor", zap.Error(err))
		}

		return nil
	}

	// If the bucket already exists, check if the owner is still active
	if err != jetstream.ErrKeyExists {
		return err
	}

	// Check current ownership
	entry, err := bm.kv.Get(ctx, key)
	if err != nil {
		return err
	}

	var currentOwnership BucketOwnership
	if err := json.Unmarshal(entry.Value(), &currentOwnership); err != nil {
		return err
	}

	// If the owner is still active, send claim interest message to the owner
	if !bm.isOwnerInactive(currentOwnership.Owner) {
		bm.publishInterest(bucket, currentOwnership.Owner)
		return nil
	}

	// Claim the bucket
	_, err = bm.kv.Update(ctx, key, data, entry.Revision())
	if err != nil {
		return err
	}

	bm.mu.Lock()
	bm.ownedBuckets[bucket] = ownership
	bm.mu.Unlock()

	if err := bm.startActorDeathMonitor(ctx, bucket); err != nil {
		logger.Error("failed to start actor death monitor", zap.Error(err))
	}

	return nil
}

func (bm *BucketManager) startActorDeathMonitor(ctx context.Context, bucket int) error {
	logger := telemetry.GetLogger(ctx, "bucketmanager-startBucketDeathMonitor")

	bm.mu.Lock()
	defer bm.mu.Unlock()

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

func (bm *BucketManager) isOwnerInactive(owner string) bool {
	return !bm.membership.IsMemberActive(owner)
}

func (bm *BucketManager) handleInterest(ctx context.Context, interest *BucketInterest) {
	bm.mu.RLock()
	owns := bm.ownedBuckets[interest.BucketID] != nil
	ownedCount := len(bm.ownedBuckets)
	bm.mu.RUnlock()

	// If we don't own the bucket, we don't need to do anything
	if !owns {
		return
	}

	memberCount, _ := bm.membership.GetMemberCountAndPosition()

	fairShare := float64(bm.numBuckets) / float64(memberCount)
	// TODO: Make this configurable
	maxBuckets := int(fairShare * 1.1) // 10% over fair shares

	if ownedCount > maxBuckets {
		bm.releaseBucket(ctx, interest.BucketID)
	}
}

func (bm *BucketManager) releaseBucket(ctx context.Context, bucket int) {
	region := common.GetRegion()
	key := fmt.Sprintf("%s/%s/%d", bm.kv.Bucket(), region, bucket)
	err := bm.kv.Delete(ctx, key)
	if err != nil {
		log.Printf("failed to delete bucket %d: %v", bucket, err)
		return
	}

	bm.mu.Lock()
	delete(bm.ownedBuckets, bucket)
	bm.mu.Unlock()
}

func (bm *BucketManager) publishInterest(bucketID int, owner string) error {
	region, systemID := common.GetRegion(), common.GetSystemID()

	msg := &BucketInterest{
		BucketID:  bucketID,
		FromNode:  systemID,
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return bm.connection.NC.Publish(fmt.Sprintf("bucketinterest.%s.%s", region, owner), data)
}

func (bm *BucketManager) calculateBucket(actorType, actorID string) int {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%s.%s", actorType, actorID)))
	return int(h.Sum32()) % bm.numBuckets
}

// Transport uses this to store actor metadata in the KV store
func (bm *BucketManager) GetBucketKey(actorType, actorID string) string {
	region := common.GetRegion()
	bucket := bm.calculateBucket(actorType, actorID)
	return fmt.Sprintf("%s.%d.%s.%s", region, bucket, actorType, actorID)
}
