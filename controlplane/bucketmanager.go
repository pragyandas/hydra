package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type BucketOwnership struct {
	Owner     string    `json:"owner"`
	Timestamp time.Time `json:"timestamp"`
}

type BucketManagerConfig struct {
	NumBuckets          int
	SafetyCheckInterval time.Duration
	KVConfig            jetstream.KeyValueConfig
}

type BucketManager struct {
	systemID     string
	region       string
	numBuckets   int
	membership   *Membership
	ownedBuckets map[int]struct{}
	js           jetstream.JetStream
	kv           jetstream.KeyValue
	mu           sync.RWMutex
	wg           sync.WaitGroup
	done         chan struct{}
}

func NewBucketManager(systemID, region string, js jetstream.JetStream, membership *Membership) *BucketManager {
	return &BucketManager{
		systemID:     systemID,
		region:       region,
		js:           js,
		membership:   membership,
		ownedBuckets: make(map[int]struct{}),
		done:         make(chan struct{}),
	}
}

func (bm *BucketManager) Start(ctx context.Context, config BucketManagerConfig) error {
	bm.numBuckets = config.NumBuckets

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

	// Start the membership listener
	bm.wg.Add(1)
	go func() {
		defer bm.wg.Done()
		bm.watchMembershipChanges(ctx)
	}()

	// Start the safety check loop
	bm.wg.Add(1)
	go func() {
		defer bm.wg.Done()
		bm.safetyCheckLoop(ctx, config.SafetyCheckInterval)
	}()

	// Perform initial bucket calculation
	return bm.RecalculateBuckets(ctx)
}

func (bm *BucketManager) Stop() {
	close(bm.done)
	bm.wg.Wait()
}

func (bm *BucketManager) safetyCheckLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-bm.done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := bm.performSafetyCheck(ctx); err != nil {
				log.Printf("safety check failed: %v", err)
			}
		}
	}
}

func (bm *BucketManager) performSafetyCheck(ctx context.Context) error {
	orphanBuckets := bm.findOrphanedBuckets(ctx)
	if len(orphanBuckets) > 0 {
		return bm.tryClaimBuckets(ctx, orphanBuckets)
	}
	return nil
}

func (bm *BucketManager) findOrphanedBuckets(ctx context.Context) []int {
	currentMembers := bm.membership.GetMembers()
	eligibleBuckets := bm.getEligibleBuckets(currentMembers)

	orphans := []int{}
	for _, bucket := range eligibleBuckets {
		ownership, err := bm.getBucketOwnership(ctx, bucket)
		if err != nil || ownership == nil || currentMembers[ownership.Owner] == nil {
			orphans = append(orphans, bucket)
		}
	}
	return orphans
}

func (bm *BucketManager) getEligibleBuckets(members map[string]*MemberInfo) []int {
	buckets := []int{}

	// Get sorted list of members for consistent assignment
	memberIDs := make([]string, 0, len(members))
	for id := range members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)

	for bucket := 0; bucket < bm.numBuckets; bucket++ {
		if len(memberIDs) == 0 || memberIDs[bucket%len(memberIDs)] == bm.systemID {
			buckets = append(buckets, bucket)
		}
	}

	return buckets
}

func (bm *BucketManager) getBucketOwnership(ctx context.Context, bucket int) (*BucketOwnership, error) {
	key := fmt.Sprintf("%s/%s/%d", bm.kv.Bucket(), bm.region, bucket)
	entry, err := bm.kv.Get(ctx, key)
	if err == jetstream.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var ownership BucketOwnership
	if err := json.Unmarshal(entry.Value(), &ownership); err != nil {
		return nil, err
	}
	return &ownership, nil
}

func (bm *BucketManager) tryClaimBuckets(ctx context.Context, buckets []int) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(buckets))
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

	// Wait for completion or context cancellation
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		close(errChan)
	}

	// Collect errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	// if len(errors) > 0 {
	// 	return fmt.Errorf("bucket claim errors: %v", errors)
	// }
	return nil
}

func (bm *BucketManager) claimBucket(ctx context.Context, bucket int) error {
	key := fmt.Sprintf("%s/%s/%d", bm.kv.Bucket(), bm.region, bucket)

	entry, existsErr := bm.kv.Get(ctx, key)
	if existsErr != nil && existsErr != jetstream.ErrKeyNotFound {
		return existsErr
	}

	if existsErr != jetstream.ErrKeyNotFound {
		var currentOwnership BucketOwnership
		if err := json.Unmarshal(entry.Value(), &currentOwnership); err != nil {
			return err
		}

		if member := bm.membership.GetMembers()[currentOwnership.Owner]; member != nil {
			return fmt.Errorf("bucket already owned by active member: %s", currentOwnership.Owner)
		}
	}

	ownership := BucketOwnership{
		Owner:     bm.systemID,
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(ownership)
	if err != nil {
		return err
	}

	if existsErr == jetstream.ErrKeyNotFound {
		if _, err := bm.kv.Create(ctx, key, data); err != nil {
			return err
		}
	} else {
		if _, err := bm.kv.Update(ctx, key, data, entry.Revision()); err != nil {
			return err
		}
	}

	bm.mu.Lock()
	bm.ownedBuckets[bucket] = struct{}{}
	bm.mu.Unlock()

	return nil
}

func (bm *BucketManager) RecalculateBuckets(ctx context.Context) error {
	orphans := bm.findOrphanedBuckets(ctx)

	bm.mu.Lock()
	bm.ownedBuckets = make(map[int]struct{})
	bm.mu.Unlock()

	if len(orphans) > 0 {
		if err := bm.tryClaimBuckets(ctx, orphans); err != nil {
			return err
		}
	}

	return nil
}

func (bm *BucketManager) OwnsBucket(bucket int) bool {
	_, owns := bm.ownedBuckets[bucket]
	return owns
}

func (bm *BucketManager) calculateBucket(actorType, actorID string) int {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%s/%s", actorType, actorID)))
	return int(h.Sum32()) % bm.numBuckets
}

// Transport uses this to store actor data in the KV store
func (bm *BucketManager) GetBucketKey(actorType, actorID string) string {
	bucket := bm.calculateBucket(actorType, actorID)
	return fmt.Sprintf("%s/%d/%s/%s", bm.region, bucket, actorType, actorID)
}

func (bm *BucketManager) watchMembershipChanges(ctx context.Context) {
	membersChan := bm.membership.GetMembersChannel()

	for {
		select {
		case <-ctx.Done():
			return
		case <-bm.done:
			return
		case <-membersChan:
			if err := bm.RecalculateBuckets(ctx); err != nil {
				log.Printf("failed to recalculate buckets: %v", err)
			}
		}
	}
}
