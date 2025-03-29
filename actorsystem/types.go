package actorsystem

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/common"
	"github.com/pragyandas/hydra/controlplane"
)

type Config struct {
	ID                    string
	Region                string
	MessageStreamConfig   jetstream.StreamConfig
	KVConfig              jetstream.KeyValueConfig
	ActorLivenessKVConfig jetstream.KeyValueConfig
	ActorConfig           actor.Config
	ControlPlaneConfig    controlplane.Config
	NatsURL               string
	ConnectOpts           []nats.Option
	RetryInterval         time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		ID:      common.GetSystemID(),
		Region:  common.GetRegion(),
		NatsURL: nats.DefaultURL,
		ActorConfig: actor.Config{
			HeartbeatInterval:         GetActorLivenessHeartbeatInterval(),
			HeartbeatsMissedThreshold: GetHeartbeatsMissedThreshold(),
			ConsumerConfig: actor.ConsumerConfig{
				MaxDeliver: -1,
				AckWait:    100 * time.Millisecond,
			},
		},
		MessageStreamConfig: jetstream.StreamConfig{
			Name:     GetStreamName(),
			Subjects: []string{fmt.Sprintf("%s.>", GetStreamName())},
			Storage:  jetstream.FileStorage,
		},
		KVConfig: jetstream.KeyValueConfig{
			Bucket:      GetKVBucket(),
			Description: "Actor data store",
			Storage:     jetstream.FileStorage,
		},
		ActorLivenessKVConfig: jetstream.KeyValueConfig{
			Bucket:      GetActorLivenessKVBucket(),
			Description: "Actor liveness data store",
			Storage:     jetstream.FileStorage,
			History:     1,
			TTL:         GetActorLivenessHeartbeatInterval() * time.Duration(GetHeartbeatsMissedThreshold()),
		},
		ControlPlaneConfig: controlplane.Config{
			MembershipConfig: controlplane.MembershipConfig{
				KVConfig: jetstream.KeyValueConfig{
					Bucket:      GetMembershipKVBucket(),
					Description: "Membership data store",
					Storage:     jetstream.FileStorage,
					History:     1,
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
type TelemetryShutdown func(context.Context) error

type contextKey string

const (
	idKey = contextKey("id")
)

const (
	EnvStreamName                = "ACTORS_STREAM"
	EnvKVBucket                  = "ACTORS_KV_BUCKET"
	EnvActorLivenessKVBucket     = "ACTORS_ACTOR_LIVENESS_KV_BUCKET"
	EnvMembershipKVBucket        = "ACTORS_MEMBERSHIP_KV_BUCKET"
	EnvBucketOwnershipKVBucket   = "ACTORS_BUCKET_OWNERSHIP_KV_BUCKET"
	EnvHeartbeatInterval         = "ACTORS_HEARTBEAT_INTERVAL"
	EnvHeartbeatsMissedThreshold = "ACTORS_HEARTBEATS_MISSED_THRESHOLD"
)

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

func GetActorLivenessKVBucket() string {
	if envActorLivenessKVBucket := os.Getenv(EnvActorLivenessKVBucket); envActorLivenessKVBucket != "" {
		return envActorLivenessKVBucket
	}
	return "actorliveness"
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

func GetActorLivenessHeartbeatInterval() time.Duration {
	if envStr := os.Getenv(EnvHeartbeatInterval); envStr != "" {
		if val, err := time.ParseDuration(envStr); err == nil {
			return val
		}
	}
	return 1 * time.Second
}

func GetHeartbeatsMissedThreshold() int {
	if envStr := os.Getenv(EnvHeartbeatsMissedThreshold); envStr != "" {
		if val, err := strconv.Atoi(envStr); err == nil {
			return val
		}
	}
	return 3
}
