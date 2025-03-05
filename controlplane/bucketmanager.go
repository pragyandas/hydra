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
)

type BucketManager struct {
	systemID        string
	region          string
	numBuckets      int
	membership      *Membership
	ownedBuckets    map[int]*BucketOwnership
	nc              *nats.Conn
	js              jetstream.JetStream
	kv              jetstream.KeyValue
	mu              sync.RWMutex
	wg              sync.WaitGroup
	done            chan struct{}
	interestSub     *nats.Subscription
	discoveryTicker *time.Ticker
	claimMu         sync.Mutex
}

func NewBucketManager(systemID, region string, nc *nats.Conn, js jetstream.JetStream, membership *Membership) *BucketManager {
	return &BucketManager{
		systemID:     systemID,
		region:       region,
		membership:   membership,
		ownedBuckets: make(map[int]*BucketOwnership),
		nc:           nc,
		js:           js,
		done:         make(chan struct{}),
	}
}

func (bm *BucketManager) Start(ctx context.Context, config BucketManagerConfig) error {
	bm.numBuckets = config.NumBuckets

	// Initialize KV store
	kv, err := bm.js.CreateKeyValue(ctx, config.KVConfig)
	if err != nil {
		if err == jetstream.ErrBucketExists {
			kv, err = bm.js.KeyValue(ctx, config.KVConfig.Bucket)
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
	bm.wg.Add(1)
	go func() {
		defer bm.wg.Done()
		bm.bucketDiscoveryLoop(ctx, config.SafetyCheckInterval)
	}()

	return nil
}

func (bm *BucketManager) Stop() {
	if bm.discoveryTicker != nil {
		bm.discoveryTicker.Stop()
	}
	if bm.interestSub != nil {
		bm.interestSub.Unsubscribe()
	}
	close(bm.done)
	bm.wg.Wait()
}

func (bm *BucketManager) setupBucketInterestSubscription(ctx context.Context) error {
	// TODO: Make the nc subject configurable
	sub, err := bm.nc.Subscribe(fmt.Sprintf("bucketinterest.%s.%s", bm.region, bm.systemID), func(msg *nats.Msg) {
		var interest BucketInterest
		if err := json.Unmarshal(msg.Data, &interest); err != nil {
			log.Printf("failed to unmarshal bucket interest: %v", err)
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
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-bm.done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			bm.RecalculateBuckets(ctx)
		}
	}
}

func (bm *BucketManager) RecalculateBuckets(ctx context.Context) {
	eligibleBuckets := bm.getEligibleBuckets()

	if len(eligibleBuckets) > 0 {
		bm.tryClaimBuckets(ctx, eligibleBuckets)
	}
}

func (bm *BucketManager) getEligibleBuckets() []int {
	memberCount, selfIndex := bm.membership.GetMemberPosition()
	if selfIndex == -1 {
		log.Printf("warning: couldn't find self in member list")
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
	// We should make sure only one instance of this is running
	if !bm.claimMu.TryLock() {
		log.Printf("another instance of this is already running")
		return
	}

	defer bm.claimMu.Unlock()

	var wg sync.WaitGroup
	errChan := make(chan error, len(buckets))
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
		log.Printf("bucket claim operation cancelled: %v", ctx.Err())
	case <-done:
		close(errChan)
	}

	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		log.Printf("some bucket claims failed: %v", errors)
	}
}

func (bm *BucketManager) claimBucket(ctx context.Context, bucket int) error {
	key := fmt.Sprintf("%s/%s/%d", bm.kv.Bucket(), bm.region, bucket)
	ownership := &BucketOwnership{
		Owner:          bm.systemID,
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

	if !owns {
		return
	}

	memberCount, _ := bm.membership.GetMemberPosition()

	fairShare := float64(bm.numBuckets) / float64(memberCount)
	// TODO: Make this configurable
	maxBuckets := int(fairShare * 1.1) // 10% over fair shares

	if ownedCount > maxBuckets {
		bm.releaseBucket(ctx, interest.BucketID)
	}
}

func (bm *BucketManager) releaseBucket(ctx context.Context, bucket int) {
	key := fmt.Sprintf("%s/%s/%d", bm.kv.Bucket(), bm.region, bucket)
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
	msg := &BucketInterest{
		BucketID:  bucketID,
		FromNode:  bm.systemID,
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return bm.nc.Publish(fmt.Sprintf("bucketinterest.%s.%s", bm.region, owner), data)
}

func (bm *BucketManager) calculateBucket(actorType, actorID string) int {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%s/%s", actorType, actorID)))
	return int(h.Sum32()) % bm.numBuckets
}

// Transport uses this to store actor metadata in the KV store
func (bm *BucketManager) GetBucketKey(actorType, actorID string) string {
	bucket := bm.calculateBucket(actorType, actorID)
	return fmt.Sprintf("%s/%d/%s/%s", bm.region, bucket, actorType, actorID)
}
