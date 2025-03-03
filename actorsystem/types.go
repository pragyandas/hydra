package actorsystem

import (
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/controlplane"
)

type Config struct {
	ID                  string
	Region              string
	MessageStreamConfig jetstream.StreamConfig
	ActorKVConfig       jetstream.KeyValueConfig
	ControlPlaneConfig  controlplane.Config
	NatsURL             string
	ConnectOpts         []nats.Option
	RetryInterval       time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		ID:      GetSystemID(),
		Region:  GetRegion(),
		NatsURL: nats.DefaultURL,
		MessageStreamConfig: jetstream.StreamConfig{
			Name:     GetStreamName(),
			Subjects: []string{fmt.Sprintf("%s.>", GetStreamName())},
			Storage:  jetstream.MemoryStorage,
		},
		ActorKVConfig: jetstream.KeyValueConfig{
			Bucket:      GetKVBucket(),
			Description: "Actor data store",
			Storage:     jetstream.FileStorage,
		},
		ControlPlaneConfig: controlplane.Config{
			MembershipConfig: controlplane.MembershipConfig{
				KVConfig: jetstream.KeyValueConfig{
					Bucket:      GetMembershipKVBucket(),
					Description: "Membership data store",
					Storage:     jetstream.FileStorage,
					TTL:         3 * time.Second,
				},
				HeartbeatInterval: 1 * time.Second,
			},
			BucketManagerConfig: controlplane.BucketManagerConfig{
				NumBuckets:          16,
				SafetyCheckInterval: 5 * time.Second,
				KVConfig: jetstream.KeyValueConfig{
					Bucket:      GetBucketOwnershipKVBucket(),
					Description: "Bucket ownership data store",
					Storage:     jetstream.FileStorage,
				},
			},
		},
		RetryInterval: 5 * time.Second,
	}
}

type ActorSystemOption func(*ActorSystem)

type contextKey string

const (
	idKey = contextKey("id")
)

const (
	EnvSystemID                = "SYSTEM_ID"
	EnvRegion                  = "REGION"
	EnvStreamName              = "ACTORS_STREAM"
	EnvKVBucket                = "ACTORS_KV_BUCKET"
	EnvMembershipKVBucket      = "ACTORS_MEMBERSHIP_KV_BUCKET"
	EnvBucketOwnershipKVBucket = "ACTORS_BUCKET_OWNERSHIP_KV_BUCKET"
)

func GetSystemID() string {
	if envSystemID := os.Getenv(EnvSystemID); envSystemID != "" {
		return envSystemID
	}
	return "actorsystem"
}

func GetRegion() string {
	if envRegion := os.Getenv(EnvRegion); envRegion != "" {
		return envRegion
	}
	return "local"
}

func GetStreamName() string {
	if envStream := os.Getenv(EnvStreamName); envStream != "" {
		return envStream
	}
	return "actorstream"
}

func GetKVBucket() string {
	if envKVBucket := os.Getenv(EnvKVBucket); envKVBucket != "" {
		return envKVBucket
	}
	return "actorstore"
}

func GetMembershipKVBucket() string {
	if envMembershipKVBucket := os.Getenv(EnvMembershipKVBucket); envMembershipKVBucket != "" {
		return envMembershipKVBucket
	}
	return "members"
}

func GetBucketOwnershipKVBucket() string {
	if envBucketOwnershipKVBucket := os.Getenv(EnvBucketOwnershipKVBucket); envBucketOwnershipKVBucket != "" {
		return envBucketOwnershipKVBucket
	}
	return "bucketownership"
}
