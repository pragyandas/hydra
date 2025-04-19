package controlplane

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pragyandas/hydra/common"
	"github.com/pragyandas/hydra/connection"
	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

// Manages system membership and discovery of other members
type Membership struct {
	connection        *connection.Connection
	members           map[string]time.Time //Saves the last heartbeat time for each member
	mu                sync.RWMutex
	membershipChanged chan struct{}
	selfID            string
	region            string
	memberCount       int
	selfIndex         int
	done              chan struct{}
	wg                sync.WaitGroup
	heartbeatSub      *nats.Subscription
	config            MembershipConfig
}

func NewMembership(connection *connection.Connection, membershipChanged chan struct{}) *Membership {
	return &Membership{
		connection:        connection,
		members:           make(map[string]time.Time),
		membershipChanged: membershipChanged,
		done:              make(chan struct{}),
	}
}

func (m *Membership) Start(ctx context.Context, config MembershipConfig) error {
	logger := telemetry.GetLogger(ctx, "membership-start")

	m.region = common.GetRegion(ctx)
	m.selfID = common.GetSystemID(ctx)

	m.config = config

	sub, err := m.connection.NC.Subscribe(
		fmt.Sprintf("membership.%s", m.region),
		func(msg *nats.Msg) {
			m.handleHeartbeat(ctx, msg.Data)
		},
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to membership: %w", err)
	}
	m.heartbeatSub = sub

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.sendHeartbeats(ctx)
	}()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.checkHeartbeats(ctx)
	}()

	m.mu.Lock()
	m.members[m.selfID] = time.Now()
	m.updateMemberPosition()
	m.mu.Unlock()

	logger.Info("membership started",
		zap.String("selfID", m.selfID),
		zap.String("region", m.region))

	return nil
}

func (m *Membership) Stop(ctx context.Context) error {
	logger := telemetry.GetLogger(ctx, "membership-stop")

	if m.heartbeatSub != nil {
		if err := m.heartbeatSub.Unsubscribe(); err != nil {
			logger.Error("failed to unsubscribe from heartbeats", zap.Error(err))
		}
	}

	close(m.done)
	m.wg.Wait()

	logger.Info("membership stopped")
	return nil
}

func (m *Membership) sendHeartbeats(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "membership-heartbeat")
	ticker := time.NewTicker(m.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.done:
			return
		case <-ticker.C:
			err := m.connection.NC.Publish(
				fmt.Sprintf("membership.%s", m.region),
				[]byte(m.selfID),
			)
			if err != nil {
				logger.Error("failed to send heartbeat", zap.Error(err))
			}
		}
	}
}

func (m *Membership) handleHeartbeat(ctx context.Context, data []byte) {
	logger := telemetry.GetLogger(ctx, "membership-heartbeat")
	memberID := string(data)

	m.mu.Lock()
	defer m.mu.Unlock()

	isNewMember := false
	if _, exists := m.members[memberID]; !exists {
		isNewMember = true
	}

	// Update last seen time
	m.members[memberID] = time.Now()

	if isNewMember {
		m.updateMemberPosition()
		select {
		case m.membershipChanged <- struct{}{}:
		default:
			logger.Debug("channel full, skipping notification")
		}
	}
}

func (m *Membership) checkHeartbeats(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "membership-check")
	ticker := time.NewTicker(m.config.HeartbeatCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.done:
			return
		case <-ticker.C:
			m.mu.Lock()
			now := time.Now()
			membershipChanged := false

			for memberID, lastSeen := range m.members {
				if now.Sub(lastSeen) > m.config.HeartbeatMissedThreshold {
					delete(m.members, memberID)
					membershipChanged = true
					logger.Info("member expired", zap.String("memberID", memberID))
				}
			}

			if membershipChanged {
				m.updateMemberPosition()
				select {
				case m.membershipChanged <- struct{}{}:
				default:
					logger.Debug("channel full, skipping notification")
				}
			}
			m.mu.Unlock()
		}
	}
}

func (m *Membership) updateMemberPosition() {
	memberIDs := make([]string, 0, len(m.members))
	for id := range m.members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)

	m.selfIndex = -1
	for i, id := range memberIDs {
		if id == m.selfID {
			m.selfIndex = i
			break
		}
	}
	m.memberCount = len(memberIDs)
}

func (m *Membership) GetMemberCountAndPosition() (int, int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.memberCount, m.selfIndex
}

func (m *Membership) IsMemberActive(memberID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.members[memberID]
	return exists
}
