package controlplane

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type MembershipConfig struct {
	HeartbeatInterval        time.Duration
	HeartbeatCheckInterval   time.Duration
	HeartbeatMissedThreshold time.Duration
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

// BucketTransferRequest represents a request to transfer a bucket to another node
type BucketTransferRequest struct {
	BucketID int    `json:"bucket_id"`
	FromNode string `json:"from_node"`
	ToNode   string `json:"to_node"`
}

// BucketTransferResponse represents a response to a bucket transfer request
type BucketTransferResponse struct {
	BucketID int  `json:"bucket_id"`
	Approved bool `json:"approved"`
}

type BucketManagerConfig struct {
	NumBuckets        int
	KVConfig          jetstream.KeyValueConfig
	DiscoveryInterval time.Duration
}
