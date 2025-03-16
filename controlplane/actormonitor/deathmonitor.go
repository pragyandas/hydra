package controlplane

import (
	"context"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

type DeathMonitor struct {
	systemID        string
	region          string
	bucketID        int
	actorKV         jetstream.KeyValue
	actorLivenessKV jetstream.KeyValue
	deadActors      map[string]struct{}
}

func NewDeathMonitor(systemID, region string, bucketID int, actorKV jetstream.KeyValue, actorLivenessKV jetstream.KeyValue) *DeathMonitor {
	return &DeathMonitor{
		systemID:        systemID,
		region:          region,
		bucketID:        bucketID,
		actorKV:         actorKV,
		actorLivenessKV: actorLivenessKV,
	}
}

func (m *DeathMonitor) Start(ctx context.Context) error {
	deadActors, err := m.findDeadActors(ctx)
	if err != nil {
		return err
	}
	m.deadActors = deadActors

	go m.monitorDeadActors(ctx)
	return nil
}

func (m *DeathMonitor) findDeadActors(ctx context.Context) (map[string]struct{}, error) {
	logger := telemetry.GetLogger(ctx, "death-monitor-find-dead-actors")

	// Get all registered actors
	registrationPrefix := fmt.Sprintf("%s/%s/%d/*/*",
		m.actorKV.Bucket(),
		m.region,
		m.bucketID)

	registrations, err := m.actorKV.ListKeys(ctx)
	if err != nil {
		logger.Error("failed to get actor registrations", zap.Error(err))
		return nil, fmt.Errorf("failed to get actor registrations: %w", err)
	}

	deadActors := make(map[string]struct{})
	for key := range registrations.Keys() {
		// It is unlikely that the actor registration key will not have the prefix
		// but we check to be safe
		if !strings.HasPrefix(key, registrationPrefix) {
			continue
		}

		_, err := m.actorLivenessKV.Get(ctx, key)
		// No liveness entry -> actor is dead
		if err != nil {
			if err == jetstream.ErrKeyNotFound {
				parts := strings.Split(key, "/")
				actorType := parts[3]
				actorId := parts[4]
				deadActors[fmt.Sprintf("%s/%s", actorType, actorId)] = struct{}{}
				continue
			}

			// We don't return this error as we want to continue monitoring for other dead actors
			logger.Error("failed to get actor liveness entry", zap.Error(err))
			continue
		}
	}

	return deadActors, nil
}

func (m *DeathMonitor) monitorDeadActors(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "death-monitor-monitor-dead-actors")

	prefix := fmt.Sprintf("%s/%s/%d/*/*", m.actorLivenessKV.Bucket(), m.region, m.bucketID)

	watcher, err := m.actorLivenessKV.Watch(ctx, prefix)
	if err != nil {
		logger.Error("failed to watch actor liveness", zap.Error(err))
		return
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case entry := <-watcher.Updates():
			if entry == nil {
				continue
			}

			parts := strings.Split(entry.Key(), "/")
			actorType := parts[3]
			actorId := parts[4]
			actorKey := fmt.Sprintf("%s/%s", actorType, actorId)

			switch entry.Operation() {
			case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
				// Actor is dead
				m.deadActors[actorKey] = struct{}{}
				logger.Debug("actor marked as dead", zap.String("actor", actorKey))
			case jetstream.KeyValuePut:
				// Actor is alive
				delete(m.deadActors, actorKey)
				logger.Debug("actor marked as alive", zap.String("actor", actorKey))
			}
		}
	}
}
