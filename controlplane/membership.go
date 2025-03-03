package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type MemberInfo struct {
	SystemID  string    `json:"system_id"`
	Region    string    `json:"region"`
	Status    string    `json:"status"`
	Heartbeat time.Time `json:"heartbeat"`
	Metrics   Metrics   `json:"metrics"`
}

// Currently a placeholder, but can be used in future for sophisticated bucket distribution
type Metrics struct {
	ActorCount  int     `json:"actor_count"`
	MemoryUsage float64 `json:"memory_usage"`
	CPUUsage    float64 `json:"cpu_usage"`
}

type MembershipConfig struct {
	KVConfig          jetstream.KeyValueConfig
	HeartbeatInterval time.Duration
}

// Manages system membership and discovery
type Membership struct {
	systemID          string
	region            string
	js                jetstream.JetStream
	kv                jetstream.KeyValue
	members           map[string]*MemberInfo
	membersChan       chan map[string]*MemberInfo
	mu                sync.RWMutex
	membershipChanged chan struct{}
	wg                sync.WaitGroup
	done              chan struct{}
}

func NewMembership(systemID, region string, js jetstream.JetStream, membershipChanged chan struct{}) *Membership {
	return &Membership{
		systemID:          systemID,
		region:            region,
		js:                js,
		membershipChanged: membershipChanged,
		members:           make(map[string]*MemberInfo),
		membersChan:       make(chan map[string]*MemberInfo, 1),
		done:              make(chan struct{}),
	}
}

func (m *Membership) initializeKV(ctx context.Context, config MembershipConfig) error {
	kv, err := m.js.CreateKeyValue(ctx, config.KVConfig)
	if err != nil {
		if err == jetstream.ErrBucketExists {
			kv, err = m.js.KeyValue(ctx, config.KVConfig.Bucket)
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
			continue // Skip entries we can't read
		}

		var info MemberInfo
		if err := json.Unmarshal(entry.Value(), &info); err != nil {
			continue // Skip entries we can't parse
		}
		m.members[info.SystemID] = &info
	}
	return nil
}

func (m *Membership) register(ctx context.Context) error {
	info := &MemberInfo{
		SystemID:  m.systemID,
		Region:    m.region,
		Status:    "active",
		Heartbeat: time.Now(),
		Metrics:   Metrics{},
	}

	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal member info: %w", err)
	}

	key := fmt.Sprintf("%s/%s/%s", m.kv.Bucket(), m.region, m.systemID)
	_, err = m.kv.Put(ctx, key, data)
	if err != nil {
		return fmt.Errorf("failed to register member: %w", err)
	}

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

func (m *Membership) Start(ctx context.Context, config MembershipConfig) error {
	// 1. Initialize KV store
	if err := m.initializeKV(ctx, config); err != nil {
		return err
	}

	// 2. Load existing state
	if err := m.loadExistingState(ctx); err != nil {
		return err
	}

	// 3. Register this member
	if err := m.register(ctx); err != nil {
		return err
	}

	// 4. Start background tasks
	m.startBackgroundTasks(ctx, config.HeartbeatInterval)

	return nil
}

func (m *Membership) Stop() {
	close(m.done)
	m.wg.Wait()
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
	key := fmt.Sprintf("%s/%s/%s", m.kv.Bucket(), m.region, m.systemID)
	info := &MemberInfo{
		SystemID:  m.systemID,
		Region:    m.region,
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

func (m *Membership) watchMembers(ctx context.Context) error {
	watcher, err := m.kv.Watch(ctx, ">")
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
				select {
				case m.membershipChanged <- struct{}{}:
				default:
					// Drop the update as a bucket recalculation is already in progress
					log.Printf("membership changed channel is full, dropping update")
				}
			}

			// Create a copy of the current state
			membersCopy := make(map[string]*MemberInfo, len(m.members))
			for k, v := range m.members {
				membersCopy[k] = v.Clone()
			}
			m.mu.Unlock()

			// Notify listeners of the update
			select {
			case m.membersChan <- membersCopy:
			default:
				// Channel is full, skip this update
			}
		}
	}
}

func (m *Membership) GetMembers() map[string]*MemberInfo {
	return m.members
}

func (m *Membership) GetMembersChannel() <-chan map[string]*MemberInfo {
	return m.membersChan
}

func (m *MemberInfo) Clone() *MemberInfo {
	return &MemberInfo{
		SystemID:  m.SystemID,
		Region:    m.Region,
		Status:    m.Status,
		Heartbeat: m.Heartbeat,
		Metrics:   m.Metrics,
	}
}
