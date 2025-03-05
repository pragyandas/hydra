package controlplane

import (
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

// BucketOwnership represents the current ownership status of a bucket
type BucketOwnership struct {
	Owner          string    `json:"owner"`
	LastUpdateTime time.Time `json:"last_update_time"`
}

// BucketInterest represents a node's interest in owning a bucket
type BucketInterest struct {
	BucketID  int       `json:"bucket_id"`
	FromNode  string    `json:"from_node"`
	Timestamp time.Time `json:"timestamp"`
}

type BucketManagerConfig struct {
	NumBuckets          int
	SafetyCheckInterval time.Duration
	KVConfig            jetstream.KeyValueConfig
	DiscoveryInterval   time.Duration
}
