package controlplane

import (
	"context"
	"sync"

	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/connection"
	"github.com/pragyandas/hydra/telemetry"
)

type ControlPlane struct {
	connection        *connection.Connection
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

func New(connection *connection.Connection, actorResurrectionChan chan actor.ActorId) (*ControlPlane, error) {
	cp := &ControlPlane{
		connection:        connection,
		done:              make(chan struct{}),
		membershipChanged: make(chan struct{}, 1), // Buffered channel to avoid blocking
	}

	cp.membership = NewMembership(connection, cp.membershipChanged)

	cp.bucketManager = NewBucketManager(connection, cp.membership, actorResurrectionChan)

	return cp, nil
}

func (cp *ControlPlane) Start(ctx context.Context, config Config) error {
	logger := telemetry.GetLogger(ctx, "controlplane-start")

	tracer := telemetry.GetTracer()
	ctx, span := tracer.Start(ctx, "controlplane-start")
	defer span.End()

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

	logger.Info("started control plane")

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

func (cp *ControlPlane) Stop(ctx context.Context) error {
	logger := telemetry.GetLogger(ctx, "controlplane-stop")

	close(cp.done)
	cp.wg.Wait()

	if cp.membership != nil {
		cp.membership.Stop(ctx)
		logger.Info("gracefully closed control plane membership")
	}

	if cp.bucketManager != nil {
		cp.bucketManager.Stop(ctx)
		logger.Info("gracefully closed control plane bucket manager")
	}

	return nil
}

func (cp *ControlPlane) GetBucketKey(actorType, actorID string) string {
	return cp.bucketManager.GetBucketKey(actorType, actorID)
}
