package controlplane

import (
	"context"
	"sync"
	"time"

	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/connection"
	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

type ControlPlane struct {
	connection               *connection.Connection
	membership               *Membership
	bucketManager            *BucketManager
	bucketRecalculationTimer *time.Timer
	timerMu                  sync.Mutex
	wg                       sync.WaitGroup
	membershipChanged        chan struct{}
	done                     chan struct{}
}

type Config struct {
	MembershipConfig                         MembershipConfig
	BucketManagerConfig                      BucketManagerConfig
	BucketRecalculationStabilizationInterval time.Duration
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

	if err := cp.membership.Start(ctx, config.MembershipConfig); err != nil {
		return err
	}

	if err := cp.bucketManager.Start(ctx, config.BucketManagerConfig); err != nil {
		return err
	}

	cp.wg.Add(2)

	go func() {
		defer cp.wg.Done()
		cp.handlemembershipUpdate(ctx, config.BucketRecalculationStabilizationInterval)
	}()

	go func() {
		defer cp.wg.Done()
		cp.handleBucketRecalculationTimer(ctx)
	}()

	logger.Info("started control plane")
	return nil
}

func (cp *ControlPlane) handleBucketRecalculationTimer(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "controlplane-bucketRecalculation")

	for {
		cp.timerMu.Lock()
		var timerCh <-chan time.Time
		if cp.bucketRecalculationTimer != nil {
			timerCh = cp.bucketRecalculationTimer.C
		}
		cp.timerMu.Unlock()

		if timerCh == nil {
			select {
			case <-ctx.Done():
				return
			case <-cp.done:
				return
			default:
				continue
			}
		}

		select {
		case <-ctx.Done():
			cp.timerMu.Lock()
			if cp.bucketRecalculationTimer != nil {
				cp.bucketRecalculationTimer.Stop()
			}
			cp.timerMu.Unlock()
			return

		case <-cp.done:
			cp.timerMu.Lock()
			if cp.bucketRecalculationTimer != nil {
				cp.bucketRecalculationTimer.Stop()
			}
			cp.timerMu.Unlock()
			return

		case <-timerCh:
			logger.Info("membership stabilized, recalculating buckets")
			cp.bucketManager.RecalculateBuckets(ctx)

			cp.timerMu.Lock()
			cp.bucketRecalculationTimer = nil
			cp.timerMu.Unlock()
		}
	}
}

func (cp *ControlPlane) handlemembershipUpdate(ctx context.Context, stabilizationInterval time.Duration) {
	logger := telemetry.GetLogger(ctx, "controlplane-membershipUpdate")

	for {
		select {
		case <-cp.done:
			return
		case <-ctx.Done():
			return
		case <-cp.membershipChanged:
			logger.Info("membership changed, resetting stabilization timer",
				zap.Duration("interval", stabilizationInterval))

			cp.timerMu.Lock()
			if cp.bucketRecalculationTimer != nil {
				cp.bucketRecalculationTimer.Reset(stabilizationInterval)
			} else {
				cp.bucketRecalculationTimer = time.NewTimer(stabilizationInterval)
			}
			cp.timerMu.Unlock()
		}
	}
}

func (cp *ControlPlane) Stop(ctx context.Context) error {
	logger := telemetry.GetLogger(ctx, "controlplane-stop")

	close(cp.done)

	cp.timerMu.Lock()
	if cp.bucketRecalculationTimer != nil {
		cp.bucketRecalculationTimer.Stop()
	}
	cp.timerMu.Unlock()

	cp.wg.Wait()

	if cp.bucketManager != nil {
		cp.bucketManager.Stop(ctx)
	}

	logger.Debug("closed control plane")
	return nil
}

func (cp *ControlPlane) GetBucketKey(actorType, actorID string) string {
	return cp.bucketManager.GetBucketKey(actorType, actorID)
}
