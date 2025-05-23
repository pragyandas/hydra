package actormonitor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/common"
	"github.com/pragyandas/hydra/connection"
	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

type ActorDeathMonitor struct {
	bucketID              int
	connection            *connection.Connection
	deadActors            map[string]struct{}
	mailboxMonitors       map[string]*ActorMailboxMonitor
	actorResurrectionChan chan actor.ActorId
	mu                    sync.RWMutex
	wg                    sync.WaitGroup
	done                  chan struct{}
}

func NewActorDeathMonitor(connection *connection.Connection, bucketID int, actorResurrectionChan chan actor.ActorId) *ActorDeathMonitor {
	return &ActorDeathMonitor{
		bucketID:              bucketID,
		connection:            connection,
		deadActors:            make(map[string]struct{}),
		mailboxMonitors:       make(map[string]*ActorMailboxMonitor),
		actorResurrectionChan: actorResurrectionChan,
		done:                  make(chan struct{}),
	}
}

func (m *ActorDeathMonitor) Start(ctx context.Context) error {
	// Discover all dead actors for the bucket
	if err := m.findDeadActors(ctx); err != nil {
		return err
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.monitorDeadActors(ctx)
	}()
	return nil
}

func (m *ActorDeathMonitor) Stop(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "death-monitor-stop")

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, monitor := range m.mailboxMonitors {
		monitor.Stop()
	}
	close(m.done)
	m.wg.Wait()

	logger.Debug("stopped death monitor for bucket", zap.Int("bucket", m.bucketID))
}

func (m *ActorDeathMonitor) findDeadActors(ctx context.Context) error {
	logger := telemetry.GetLogger(ctx, "death-monitor-find-dead-actors")

	region := common.GetRegion(ctx)

	// Get all registered actors
	registrationPrefix := fmt.Sprintf("%s.%d", region, m.bucketID)

	registrations, err := m.connection.ActorKV.ListKeys(ctx)
	if err != nil {
		logger.Error("failed to get actor registrations", zap.Error(err))
		return fmt.Errorf("failed to get actor registrations: %w", err)
	}

	for key := range registrations.Keys() {
		if !strings.HasPrefix(key, registrationPrefix) {
			continue
		}

		entry, err := m.connection.ActorLivenessKV.Get(ctx, key)
		isActorDead := false
		// If actor is registered, but liveness entry is not found or nil, it is dead
		if err != nil {
			if err == jetstream.ErrKeyNotFound {
				logger.Debug("actor liveness entry not found", zap.String("key", key))
				isActorDead = true
			} else {
				logger.Error("failed to get actor liveness", zap.Error(err))
				continue
			}
		} else {
			if entry == nil {
				logger.Debug("actor liveness entry is nil", zap.String("key", key))
				isActorDead = true
			}
		}

		if isActorDead {
			parts := strings.Split(key, ".")
			if len(parts) < 4 {
				logger.Error("invalid actor key", zap.String("key", key))
				continue
			}
			actorType := parts[2]
			actorId := parts[3]
			actorKey := fmt.Sprintf("%s.%s", actorType, actorId)
			m.mu.Lock()
			m.onActorDeath(ctx, actorKey)
			m.mu.Unlock()
		}
	}

	return nil
}

func (m *ActorDeathMonitor) monitorDeadActors(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "death-monitor-monitor-dead-actors")

	region := common.GetRegion(ctx)
	prefix := fmt.Sprintf("%s.%d.*.*", region, m.bucketID)

	watcher, err := m.connection.ActorLivenessKV.Watch(ctx, prefix)
	if err != nil {
		logger.Error("failed to watch actor liveness", zap.Error(err))
		return
	}
	defer watcher.Stop()

	// May be a good idea to make this configurable
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.done:
			logger.Debug("death monitor stopped")
			return
		case <-ctx.Done():
			logger.Debug("context done, gracefully stopping death monitor")
			return
		case <-ticker.C:
			// Safety check to ensure we are resilient to missing KV updates
			m.mu.Lock()
			if err := m.findDeadActors(ctx); err != nil {
				logger.Error("failed to refresh dead actors", zap.Error(err))
			}
			m.mu.Unlock()
		case entry := <-watcher.Updates():
			if entry == nil {
				continue
			}

			parts := strings.Split(entry.Key(), ".")
			if len(parts) < 4 {
				logger.Error("invalid actor key", zap.String("key", entry.Key()))
				continue
			}
			actorType := parts[2]
			actorId := parts[3]
			actorKey := fmt.Sprintf("%s.%s", actorType, actorId)

			m.mu.Lock()
			switch entry.Operation() {
			case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
				m.onActorDeath(ctx, actorKey)
			case jetstream.KeyValuePut:
				if _, exists := m.deadActors[actorKey]; exists {
					m.onActorAlive(ctx, actorKey)
				}
			}
			m.mu.Unlock()
		}
	}
}

func (m *ActorDeathMonitor) onActorDeath(ctx context.Context, actorKey string) {
	logger := telemetry.GetLogger(ctx, "death-monitor-on-actor-death")

	parts := strings.Split(actorKey, ".")
	actorType := parts[0]
	actorID := parts[1]

	m.deadActors[actorKey] = struct{}{}

	// Start monitoring mailbox if not already monitoring
	if _, exists := m.mailboxMonitors[actorKey]; !exists {
		monitor := NewActorMailboxMonitor(m.connection, actorType, actorID)
		m.mailboxMonitors[actorKey] = monitor

		logger.Debug("starting mailbox monitor", zap.String("actor", actorKey))

		go monitor.Start(ctx, func() {
			m.actorResurrectionChan <- actor.ActorId{Type: actorType, ID: actorID}
			logger.Debug("actor resurrection requested", zap.String("actor", actorKey))
		})
	}
}

func (m *ActorDeathMonitor) onActorAlive(ctx context.Context, actorKey string) {
	logger := telemetry.GetLogger(ctx, "death-monitor-on-actor-alive")

	if mailboxMonitorForActor, exists := m.mailboxMonitors[actorKey]; exists {
		mailboxMonitorForActor.Stop()
		delete(m.mailboxMonitors, actorKey)
	}

	delete(m.deadActors, actorKey)
	logger.Debug("actor resurrected", zap.String("actor", actorKey))
}
