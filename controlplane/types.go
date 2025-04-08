package controlplane

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

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
