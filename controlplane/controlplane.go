package controlplane

import (
	"context"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

type ControlPlane struct {
	systemID          string
	region            string
	js                jetstream.JetStream
	membership        *Membership
	bucketManager     *BucketManager
	wg                sync.WaitGroup
	membershipChanged chan struct{}
	done              chan struct{}
}

type Config struct {
	MembershipConfig    MembershipConfig
	BucketManagerConfig BucketManagerConfig
}

func New(systemID, region string, js jetstream.JetStream) (*ControlPlane, error) {
	cp := &ControlPlane{
		systemID:          systemID,
		region:            region,
		js:                js,
		done:              make(chan struct{}),
		membershipChanged: make(chan struct{}, 1), // Buffered channel to avoid blocking
	}

	cp.membership = NewMembership(systemID, region, js, cp.membershipChanged)

	cp.bucketManager = NewBucketManager(systemID, region, js, cp.membership)

	return cp, nil
}

func (cp *ControlPlane) Start(ctx context.Context, config Config) error {
	if err := cp.membership.Start(ctx, config.MembershipConfig); err != nil {
		return err
	}

	if err := cp.bucketManager.Start(ctx, config.BucketManagerConfig); err != nil {
		return err
	}

	cp.wg.Add(1)
	go func() {
		defer cp.wg.Done()
		cp.handlemembershipUpdate(ctx)
	}()

	return nil
}

func (cp *ControlPlane) handlemembershipUpdate(ctx context.Context) {
	for {
		select {
		case <-cp.done:
			return
		case <-ctx.Done():
			return
		case <-cp.membershipChanged:
			cp.bucketManager.RecalculateBuckets(ctx)
		}
	}
}

func (cp *ControlPlane) Stop() error {
	close(cp.done)
	cp.wg.Wait()

	if cp.membership != nil {
		cp.membership.Stop()
	}

	if cp.bucketManager != nil {
		cp.bucketManager.Stop()
	}

	return nil
}

func (cp *ControlPlane) GetBucketKey(actorType, actorID string) string {
	return cp.bucketManager.GetBucketKey(actorType, actorID)
}

func (cp *ControlPlane) GetSystemID() string {
	return cp.systemID
}

func (cp *ControlPlane) GetRegion() string {
	return cp.region
}
