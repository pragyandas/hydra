package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/common"
	"github.com/pragyandas/hydra/connection"
	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

// Manages system membership and discovery
type Membership struct {
	connection        *connection.Connection
	kv                jetstream.KeyValue
	members           map[string]*MemberInfo
	membersChan       chan map[string]*MemberInfo
	mu                sync.RWMutex
	membershipChanged chan struct{}
	wg                sync.WaitGroup
	done              chan struct{}
	selfIndex         int
}

func NewMembership(connection *connection.Connection, membershipChanged chan struct{}) *Membership {
	return &Membership{
		connection:        connection,
		membershipChanged: membershipChanged,
		members:           make(map[string]*MemberInfo),
		membersChan:       make(chan map[string]*MemberInfo, 1),
		done:              make(chan struct{}),
	}
}

func (m *Membership) Start(ctx context.Context, config MembershipConfig) error {
	tracer := telemetry.GetTracer()
	ctx, span := tracer.Start(ctx, "membership-start")
	defer span.End()

	logger := telemetry.GetLogger(ctx, "membership")

	if err := m.initializeKV(ctx, config); err != nil {
		logger.Error("failed to initialize KV store", zap.Error(err))
		return err
	}

	if err := m.loadExistingState(ctx); err != nil {
		logger.Error("failed to load existing state", zap.Error(err))
		return err
	}

	logger.Info("loaded existing state")

	m.startBackgroundTasks(ctx, config.HeartbeatInterval)

	if err := m.register(ctx); err != nil {
		logger.Error("failed to register member", zap.Error(err))
		return err
	}

	logger.Info("registered member")

	m.updateMemberPosition()

	logger.Info("updated member position")

	return nil
}

func (m *Membership) Stop() {
	close(m.done)
	m.wg.Wait()
}

func (m *Membership) initializeKV(ctx context.Context, config MembershipConfig) error {
	kv, err := m.connection.JS.CreateKeyValue(ctx, config.KVConfig)
	if err != nil {
		if err == jetstream.ErrBucketExists {
			kv, err = m.connection.JS.KeyValue(ctx, config.KVConfig.Bucket)
			if err != nil {
				return fmt.Errorf("failed to get existing KV store: %w", err)
			}
		} else {
			return fmt.Errorf("failed to create KV store: %w", err)
		}
	}
	m.kv = kv
	return nil
}

func (m *Membership) loadExistingState(ctx context.Context) error {
	keys, err := m.kv.Keys(ctx)
	if err != nil && err != jetstream.ErrNoKeysFound {
		return fmt.Errorf("failed to list members: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.members = make(map[string]*MemberInfo)
	for _, key := range keys {
		entry, err := m.kv.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get member info: %w", err)
		}

		var info MemberInfo
		if err := json.Unmarshal(entry.Value(), &info); err != nil {
			return fmt.Errorf("failed to unmarshal member info: %w", err)
		}
		m.members[info.SystemID] = &info
	}
	return nil
}

func (m *Membership) register(ctx context.Context) error {
	systemID, region := common.GetSystemID(), common.GetRegion()

	info := &MemberInfo{
		SystemID:  systemID,
		Region:    region,
		Heartbeat: time.Now(),
		Metrics:   Metrics{},
	}

	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal member info: %w", err)
	}

	key := fmt.Sprintf("%s/%s/%s", m.kv.Bucket(), region, systemID)
	_, err = m.kv.Put(ctx, key, data)
	if err != nil {
		return fmt.Errorf("failed to register member: %w", err)
	}

	m.members[systemID] = info

	return nil
}

func (m *Membership) startBackgroundTasks(ctx context.Context, heartbeatInterval time.Duration) {
	// Start the membership watcher
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		if err := m.watchMembers(ctx); err != nil && err != context.Canceled {
			log.Printf("failed to watch members: %v", err)
		}
	}()

	// Start the heartbeat loop
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.heartbeatLoop(ctx, heartbeatInterval)
	}()
}

func (m *Membership) heartbeatLoop(ctx context.Context, heartbeatInterval time.Duration) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.heartbeat(ctx); err != nil {
				log.Printf("failed to heartbeat: %v", err)
			}
		}
	}
}

func (m *Membership) heartbeat(ctx context.Context) error {
	systemID, region := common.GetSystemID(), common.GetRegion()
	key := fmt.Sprintf("%s/%s/%s", m.kv.Bucket(), region, systemID)
	info := &MemberInfo{
		SystemID:  systemID,
		Region:    region,
		Heartbeat: time.Now(),
		Metrics: Metrics{
			ActorCount:  0,
			MemoryUsage: 0,
			CPUUsage:    0,
		},
	}

	data, _ := json.Marshal(info)
	_, err := m.kv.Put(ctx, key, data)
	if err != nil {
		return err
	}

	return nil
}

func (m *Membership) updateMemberPosition() {
	systemID := common.GetSystemID()

	m.mu.Lock()
	defer m.mu.Unlock()
	memberIDs := make([]string, 0, len(m.members))
	for id := range m.members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)

	selfIndex := -1
	for i, id := range memberIDs {
		if id == systemID {
			selfIndex = i
			break
		}
	}

	m.selfIndex = selfIndex
}

func (m *Membership) GetMemberCountAndPosition() (memberCount int, selfIndex int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.members), m.selfIndex
}

func (m *Membership) watchMembers(ctx context.Context) error {
	region := common.GetRegion()
	watcher, err := m.kv.Watch(ctx, fmt.Sprintf("%s/%s/", m.kv.Bucket(), region))
	if err != nil {
		return fmt.Errorf("failed to create watcher: %w", err)
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.done:
			return nil
		case entry := <-watcher.Updates():
			if entry == nil {
				continue
			}

			m.mu.Lock()
			var info MemberInfo
			if err := json.Unmarshal(entry.Value(), &info); err != nil {
				m.mu.Unlock()
				continue
			}

			membershipChanged := false
			if entry.Operation() == jetstream.KeyValuePut {
				m.members[info.SystemID] = &info
				membershipChanged = true
			} else if entry.Operation() == jetstream.KeyValueDelete {
				delete(m.members, info.SystemID)
				membershipChanged = true
			}

			if membershipChanged {
				m.updateMemberPosition()
				select {
				case m.membershipChanged <- struct{}{}:
				default:
					log.Printf("membership changed channel is full, dropping update")
				}
			}

		}
	}
}

func (m *Membership) IsMemberActive(memberID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.members[memberID]
	return exists
}
