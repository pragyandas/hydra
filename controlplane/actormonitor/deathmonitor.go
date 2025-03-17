package actormonitor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/common"
	"github.com/pragyandas/hydra/connection"
	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

type ActorDeathMonitor struct {
	bucketID        int
	connection      *connection.Connection
	deadActors      map[string]struct{}
	mailboxMonitors map[string]*ActorMailboxMonitor
	mu              sync.RWMutex
}

func NewActorDeathMonitor(connection *connection.Connection, bucketID int) *ActorDeathMonitor {
	return &ActorDeathMonitor{
		bucketID:        bucketID,
		connection:      connection,
		deadActors:      make(map[string]struct{}),
		mailboxMonitors: make(map[string]*ActorMailboxMonitor),
	}
}

func (m *ActorDeathMonitor) Start(ctx context.Context) error {
	deadActors, err := m.findDeadActors(ctx)
	if err != nil {
		return err
	}
	m.deadActors = deadActors

	go m.monitorDeadActors(ctx)
	return nil
}

func (m *ActorDeathMonitor) findDeadActors(ctx context.Context) (map[string]struct{}, error) {
	logger := telemetry.GetLogger(ctx, "death-monitor-find-dead-actors")

	region := common.GetRegion()

	// Get all registered actors
	registrationPrefix := fmt.Sprintf("%s/%s/%d/*/*",
		m.connection.ActorKV.Bucket(),
		region,
		m.bucketID)

	registrations, err := m.connection.ActorKV.ListKeys(ctx)
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

		_, err := m.connection.ActorLivenessKV.Get(ctx, key)
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

func (m *ActorDeathMonitor) monitorDeadActors(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "death-monitor-monitor-dead-actors")

	region := common.GetRegion()
	prefix := fmt.Sprintf("%s/%s/%d/*/*", m.connection.ActorLivenessKV.Bucket(), region, m.bucketID)

	watcher, err := m.connection.ActorLivenessKV.Watch(ctx, prefix)
	if err != nil {
		logger.Error("failed to watch actor liveness", zap.Error(err))
		return
	}
	defer watcher.Stop()

	// May be a good idea to make it configurable
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Safety check to ensure we are resilient to missing KV updates
			if deadActors, err := m.findDeadActors(ctx); err == nil {
				m.mu.Lock()
				m.deadActors = deadActors
				m.mu.Unlock()
				logger.Debug("refreshed dead actors list", zap.Int("count", len(deadActors)))
			}
		case entry := <-watcher.Updates():
			if entry == nil {
				continue
			}

			parts := strings.Split(entry.Key(), "/")
			actorType := parts[3]
			actorId := parts[4]
			actorKey := fmt.Sprintf("%s/%s", actorType, actorId)

			m.mu.Lock()
			switch entry.Operation() {
			case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
				m.onActorDeath(ctx, actorType, actorId)
				logger.Debug("actor marked as dead", zap.String("actor", actorKey))
			case jetstream.KeyValuePut:
				m.onActorAlive(ctx, actorType, actorId)
				logger.Debug("actor marked as alive", zap.String("actor", actorKey))
			}
			m.mu.Unlock()
		}
	}
}

func (m *ActorDeathMonitor) onActorDeath(ctx context.Context, actorType, actorID string) {
	logger := telemetry.GetLogger(ctx, "death-monitor-on-actor-death")

	actorKey := fmt.Sprintf("%s/%s", actorType, actorID)

	m.deadActors[actorKey] = struct{}{}

	// Start monitoring mailbox if not already monitoring
	if _, exists := m.mailboxMonitors[actorKey]; !exists {
		monitor := NewActorMailboxMonitor(m.connection, actorType, actorID)
		m.mailboxMonitors[actorKey] = monitor

		go monitor.Start(ctx, func() {
			logger.Info("actor resurrection requested", zap.String("actor", actorKey))
			// TODO: Signal actor system to resurrect actor
		})
	}
}

func (m *ActorDeathMonitor) onActorAlive(ctx context.Context, actorType, actorID string) {
	logger := telemetry.GetLogger(ctx, "death-monitor-on-actor-alive")

	actorKey := fmt.Sprintf("%s/%s", actorType, actorID)
	delete(m.deadActors, actorKey)
	delete(m.mailboxMonitors, actorKey)

	logger.Info("actor resurrected", zap.String("actor", actorKey))
}
