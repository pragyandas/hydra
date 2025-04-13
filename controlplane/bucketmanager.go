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
	transferRequestSub    *nats.Subscription
	claimMu               sync.Mutex
	actorResurrectionChan chan actor.ActorId
	pendingTransfers      map[string]*BucketTransferRequest
	transferMu            sync.RWMutex
}

func NewBucketManager(connection *connection.Connection, membership *Membership, actorResurrectionChan chan actor.ActorId) *BucketManager {
	return &BucketManager{
		membership:            membership,
		ownedBuckets:          make(map[int]*BucketOwnership),
		bucketMonitors:        make(map[int]*actormonitor.ActorDeathMonitor),
		connection:            connection,
		actorResurrectionChan: actorResurrectionChan,
		pendingTransfers:      make(map[string]*BucketTransferRequest),
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

	// Subscribe to bucket transfer request messages
	if err := bm.setupBucketTransferSubscription(ctx); err != nil {
		return fmt.Errorf("failed to setup bucket transfer subscription: %w", err)
	}

	// Start periodic bucket safety check
	go bm.bucketDiscoveryLoop(ctx, config.SafetyCheckInterval)

	logger.Debug("started bucket manager")

	return nil
}

func (bm *BucketManager) Stop(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "bucketmanager-stop")

	if bm.transferRequestSub != nil {
		bm.transferRequestSub.Unsubscribe()
	}

	logger.Debug("stopped control plane bucket manager")
}

func (bm *BucketManager) setupBucketTransferSubscription(ctx context.Context) error {
	region, systemID := common.GetRegion(), common.GetSystemID()

	// Subscribe to transfer requests directed to this node
	sub, err := bm.connection.NC.Subscribe(fmt.Sprintf("buckettransfer.request.%s.%s", region, systemID), func(msg *nats.Msg) {
		bm.handleTransferRequest(ctx, msg)
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to bucket transfer requests: %w", err)
	}
	bm.transferRequestSub = sub
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

	logger.Info("eligible buckets", zap.Ints("buckets", buckets))

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
		// If we already own this bucket, skip it
		if _, exists := bm.ownedBuckets[bucket]; exists {
			continue
		}

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

	key := fmt.Sprintf("%s.%d", region, bucket)
	entry, err := bm.kv.Get(ctx, key)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			logger.Info("bucket not found, claiming it", zap.Int("bucket", bucket))

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
		}
		return err
	}

	var currentOwnership BucketOwnership
	if err := json.Unmarshal(entry.Value(), &currentOwnership); err != nil {
		return err
	}

	if bm.membership.IsMemberActive(currentOwnership.Owner) {
		bm.transferMu.Lock()
		if _, exists := bm.pendingTransfers[fmt.Sprintf("%d", bucket)]; exists {
			bm.transferMu.Unlock()
			return fmt.Errorf("transfer already pending for bucket %d", bucket)
		}

		bm.transferMu.Unlock()

		go bm.requestBucketTransferAsync(ctx, bucket, currentOwnership.Owner, entry.Revision())
		return nil
	} else {
		ownership := &BucketOwnership{
			Owner:          systemID,
			LastUpdateTime: time.Now(),
		}
		data, err := json.Marshal(ownership)
		if err != nil {
			return err
		}
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
}

// New async transfer method
func (bm *BucketManager) requestBucketTransferAsync(ctx context.Context, bucket int, currentOwner string, revision uint64) {
	logger := telemetry.GetLogger(ctx, "bucketmanager-requestBucketTransferAsync")
	region, systemID := common.GetRegion(), common.GetSystemID()

	// Create transfer request
	request := &BucketTransferRequest{
		BucketID: bucket,
		FromNode: systemID,
		ToNode:   systemID,
	}
	bm.transferMu.Lock()
	bm.pendingTransfers[fmt.Sprintf("%d", bucket)] = request
	bm.transferMu.Unlock()

	defer func() {
		bm.transferMu.Lock()
		delete(bm.pendingTransfers, fmt.Sprintf("%d", bucket))
		bm.transferMu.Unlock()
	}()

	requestData, err := json.Marshal(request)
	if err != nil {
		logger.Error("failed to marshal transfer request",
			zap.Error(err),
			zap.Int("bucket", bucket))
		return
	}

	maxRetries := 3
	baseDelay := 1 * time.Second
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			delay := time.Duration(baseDelay * time.Duration(1<<attempt))
			time.Sleep(delay)
		}

		reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		msg, err := bm.connection.NC.RequestWithContext(reqCtx,
			fmt.Sprintf("buckettransfer.request.%s.%s", region, currentOwner),
			requestData)
		cancel()

		if err != nil {
			logger.Error("failed to send transfer request",
				zap.Error(err),
				zap.Int("attempt", attempt+1),
				zap.Int("bucket", bucket))
			continue
		}

		var response BucketTransferResponse
		if err := json.Unmarshal(msg.Data, &response); err != nil {
			logger.Error("failed to parse transfer response",
				zap.Error(err),
				zap.Int("attempt", attempt+1),
				zap.Int("bucket", bucket))
			continue
		}

		if !response.Approved {
			logger.Info("transfer request was denied",
				zap.Int("bucket", bucket),
				zap.Int("attempt", attempt+1))
			return
		}

		// Try to claim the bucket
		ownership := &BucketOwnership{
			Owner:          systemID,
			LastUpdateTime: time.Now(),
		}
		data, err := json.Marshal(ownership)
		if err != nil {
			logger.Error("failed to marshal ownership",
				zap.Error(err),
				zap.Int("bucket", bucket))
			return
		}

		key := fmt.Sprintf("%s.%d", region, bucket)
		_, err = bm.kv.Update(ctx, key, data, revision)
		if err != nil {
			logger.Error("failed to claim bucket after transfer approval",
				zap.Error(err),
				zap.Int("bucket", bucket),
				zap.Int("attempt", attempt+1))
			continue
		}

		// Successfully claimed the bucket
		bm.mu.Lock()
		bm.ownedBuckets[bucket] = ownership
		bm.mu.Unlock()

		if err := bm.startActorDeathMonitor(ctx, bucket); err != nil {
			logger.Error("failed to start actor death monitor",
				zap.Error(err),
				zap.Int("bucket", bucket))
		}

		logger.Info("successfully claimed bucket after transfer",
			zap.Int("bucket", bucket),
			zap.Int("attempt", attempt+1))
		return
	}

	logger.Error("failed to transfer bucket after max retries",
		zap.Int("bucket", bucket),
		zap.Int("max_retries", maxRetries))
}

func (bm *BucketManager) handleTransferRequest(ctx context.Context, msg *nats.Msg) {
	logger := telemetry.GetLogger(ctx, "bucketmanager-handleTransferRequest")

	var request BucketTransferRequest
	if err := json.Unmarshal(msg.Data, &request); err != nil {
		logger.Error("failed to unmarshal transfer request", zap.Error(err))
		return
	}

	logger.Info("received transfer request",
		zap.Int("bucket", request.BucketID),
		zap.String("from_node", request.FromNode))

	response := &BucketTransferResponse{
		BucketID: request.BucketID,
		Approved: false, // Default to rejection
	}

	if _, exists := bm.ownedBuckets[request.BucketID]; !exists {
		data, _ := json.Marshal(response)
		msg.Respond(data)
		return
	}

	memberCount, _ := bm.membership.GetMemberCountAndPosition()
	fairShare := float64(bm.numBuckets) / float64(memberCount)
	maxBuckets := int(fairShare * 1.1)

	bm.mu.RLock()
	ownedCount := len(bm.ownedBuckets)
	bm.mu.RUnlock()

	if ownedCount > maxBuckets {
		bm.releaseBucket(ctx, request.BucketID)
		response.Approved = true
	} else {
		logger.Info("bucket transfer rejected as node is not over capacity", zap.Int("bucket", request.BucketID))
	}

	data, err := json.Marshal(response)
	if err != nil {
		logger.Error("failed to marshal transfer response", zap.Error(err))
		return
	}

	if err := msg.Respond(data); err != nil {
		logger.Error("failed to respond to transfer request", zap.Error(err))
		return
	}

	logger.Info("sent transfer response",
		zap.Int("bucket", request.BucketID),
		zap.Bool("approved", response.Approved))
}

func (bm *BucketManager) startActorDeathMonitor(ctx context.Context, bucket int) error {
	logger := telemetry.GetLogger(ctx, "bucketmanager-startBucketDeathMonitor")

	logger.Info("claimed bucket, now starting death monitor", zap.Int("bucket", bucket))

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
	logger.Info("started death monitor for bucket", zap.Int("bucket", bucket))

	return nil
}

func (bm *BucketManager) releaseBucket(ctx context.Context, bucket int) {
	region := common.GetRegion()
	key := fmt.Sprintf("%s.%d", region, bucket)
	err := bm.kv.Delete(ctx, key)
	if err != nil {
		log.Printf("failed to delete bucket %d: %v", bucket, err)
		return
	}

	bm.mu.Lock()
	delete(bm.ownedBuckets, bucket)
	bm.mu.Unlock()
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
