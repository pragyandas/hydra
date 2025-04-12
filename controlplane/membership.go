package controlplane

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/common"
	"github.com/pragyandas/hydra/connection"
	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

// Manages system membership and discovery of other members
type Membership struct {
	connection        *connection.Connection
	kv                jetstream.KeyValue
	members           map[string]struct{}
	mu                sync.RWMutex
	membershipChanged chan struct{}
	selfIndex         int

	// TTL management for members
	memberTimers     map[string]*time.Timer
	memberTimerMu    sync.Mutex
	expirationWindow time.Duration
}

func NewMembership(connection *connection.Connection, membershipChanged chan struct{}) *Membership {
	return &Membership{
		connection:        connection,
		membershipChanged: membershipChanged,
		members:           make(map[string]struct{}),
		memberTimers:      make(map[string]*time.Timer),
	}
}

func (m *Membership) Start(ctx context.Context, config MembershipConfig) error {
	logger := telemetry.GetLogger(ctx, "membership-start")

	// Set expiration window to 5 missed heartbeats
	m.expirationWindow = config.HeartbeatInterval * 5

	if err := m.initializeKV(ctx, config); err != nil {
		logger.Error("failed to initialize KV store", zap.Error(err))
		return err
	}

	revision, err := m.register(ctx)
	if err != nil {
		logger.Error("failed to register member", zap.Error(err))
		return err
	}

	if err := m.loadExistingState(ctx); err != nil {
		logger.Error("failed to load existing state", zap.Error(err))
		return err
	}

	m.startBackgroundTasks(ctx, config.HeartbeatInterval, revision)

	m.updateMemberPosition()

	logger.Info("started control plane membership watcher")

	return nil
}

func (m *Membership) Stop(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "membership-stop")

	m.memberTimerMu.Lock()
	for _, timer := range m.memberTimers {
		timer.Stop()
	}
	m.memberTimers = make(map[string]*time.Timer)
	m.memberTimerMu.Unlock()

	logger.Debug("stopped membership service")
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

func (m *Membership) register(ctx context.Context) (uint64, error) {
	logger := telemetry.GetLogger(ctx, "membership-register")

	systemID := common.GetSystemID()
	region := common.GetRegion()

	key := fmt.Sprintf("%s.%s", region, systemID)

	revision, err := m.kv.Put(ctx, key, []byte(time.Now().Format(time.RFC3339)))
	if err != nil {
		return 0, fmt.Errorf("failed to register member: %w", err)
	}

	logger.Debug("registered member", zap.String("key", key), zap.Uint64("revision", revision))

	return revision, nil
}

func getMemberIDFromKey(key string) string {
	// key format: region.systemID
	parts := strings.Split(key, ".")
	if len(parts) < 2 {
		return ""
	}
	return parts[1]
}

func (m *Membership) loadExistingState(ctx context.Context) error {
	logger := telemetry.GetLogger(ctx, "membership-loadExistingState")
	keys, err := m.kv.Keys(ctx)
	if err != nil && err != jetstream.ErrNoKeysFound {
		return fmt.Errorf("failed to list members: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.members = make(map[string]struct{})
	for _, key := range keys {
		systemID := getMemberIDFromKey(key)
		if systemID == "" {
			logger.Warn("invalid member key format", zap.String("key", key))
			continue
		}

		// Any members in KV store are valid due to TTL
		m.members[systemID] = struct{}{}
		m.resetMemberTimer(ctx, systemID)
	}
	return nil
}

func (m *Membership) startBackgroundTasks(ctx context.Context, heartbeatInterval time.Duration, revision uint64) {
	logger := telemetry.GetLogger(ctx, "membership-startBackgroundTasks")

	// Start the membership watcher to watch health of other members
	go func() {
		if err := m.watchMembers(ctx); err != nil && err != context.Canceled {
			logger.Error("failed to watch members", zap.Error(err))
		}
	}()

	// Start the self heartbeat loop
	go m.heartbeatLoop(ctx, heartbeatInterval, revision)
}

func (m *Membership) heartbeatLoop(ctx context.Context, heartbeatInterval time.Duration, revision uint64) {
	logger := telemetry.GetLogger(ctx, "membership-heartbeatLoop")

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	systemID := common.GetSystemID()
	region := common.GetRegion()
	key := fmt.Sprintf("%s.%s", region, systemID)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			newRevision, err := m.kv.Update(ctx, key, []byte(time.Now().Format(time.RFC3339)), revision)
			if err != nil {
				logger.Error("failed to heartbeat", zap.Error(err))
				continue
			}
			revision = newRevision
			logger.Debug("membership heartbeat", zap.String("key", key), zap.Uint64("revision", newRevision))
		}
	}
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

func (m *Membership) checkExpiredMember(ctx context.Context, memberID string) {
	logger := telemetry.GetLogger(ctx, "membership-checkExpiredMember")

	m.memberTimerMu.Lock()
	delete(m.memberTimers, memberID)
	m.memberTimerMu.Unlock()

	region := common.GetRegion()
	key := fmt.Sprintf("%s.%s", region, memberID)

	_, err := m.kv.Get(ctx, key)

	if err != nil {
		// Member is dead
		logger.Info("member expired and not found in KV store", zap.String("memberID", memberID))

		m.mu.Lock()
		_, exists := m.members[memberID]
		delete(m.members, memberID)
		// safety check - only notify if we actually had this member
		membershipChanged := exists
		m.updateMemberPosition()
		m.mu.Unlock()

		if membershipChanged {
			logger.Info("membership changed", zap.String("memberID", memberID))
			select {
			case m.membershipChanged <- struct{}{}:
			default:
				logger.Info("membership changed channel is full, dropping update")
			}
		}
	} else {
		// Member still alive in KV, reset the timer
		m.resetMemberTimer(ctx, memberID)
	}
}

func (m *Membership) resetMemberTimer(ctx context.Context, memberID string) {
	if memberID == common.GetSystemID() {
		return
	}

	m.memberTimerMu.Lock()
	defer m.memberTimerMu.Unlock()

	if timer, exists := m.memberTimers[memberID]; exists {
		timer.Stop()
	}

	// Create new timer
	timer := time.AfterFunc(m.expirationWindow, func() {
		m.checkExpiredMember(ctx, memberID)
	})

	m.memberTimers[memberID] = timer
}

func (m *Membership) watchMembers(ctx context.Context) error {
	region := common.GetRegion()
	logger := telemetry.GetLogger(ctx, "membership-watchMembers")

	watcher, err := m.kv.Watch(ctx, region+".*")
	if err != nil {
		return fmt.Errorf("failed to create watcher: %w", err)
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			m.memberTimerMu.Lock()
			for _, timer := range m.memberTimers {
				timer.Stop()
			}
			m.memberTimers = make(map[string]*time.Timer)
			m.memberTimerMu.Unlock()

			return ctx.Err()

		case entry := <-watcher.Updates():
			if entry == nil {
				continue
			}

			systemID := getMemberIDFromKey(entry.Key())
			if systemID == "" {
				logger.Error("invalid member key format", zap.String("key", entry.Key()))
				continue
			}

			// Ignore own heartbeat
			if systemID == common.GetSystemID() {
				continue
			}

			if entry.Operation() == jetstream.KeyValuePut {
				membershipChanged := false

				m.mu.Lock()
				_, exists := m.members[systemID]
				if !exists {
					// New member
					m.members[systemID] = struct{}{}
					membershipChanged = true
				}
				m.mu.Unlock()

				m.resetMemberTimer(ctx, systemID)

				if membershipChanged {
					logger.Info("membership changed", zap.String("memberID", systemID))
					m.updateMemberPosition()

					select {
					case m.membershipChanged <- struct{}{}:
					default:
						logger.Debug("membership changed channel is full, dropping update")
					}
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
