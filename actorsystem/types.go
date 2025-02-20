package actorsystem

import (
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Config struct {
	ID            string
	Hostname      string
	StreamConfig  jetstream.StreamConfig
	KVConfig      jetstream.KeyValueConfig
	NatsURL       string
	ConnectOpts   []nats.Option
	RetryInterval time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		ID:       "actorsystem",
		Hostname: "hostname",
		NatsURL:  nats.DefaultURL,
		StreamConfig: jetstream.StreamConfig{
			Name:     GetStreamName(),
			Subjects: []string{fmt.Sprintf("%s.>", GetStreamName())},
			Storage:  jetstream.MemoryStorage,
		},
		KVConfig: jetstream.KeyValueConfig{
			Bucket:      GetKVBucket(),
			Description: "Actor data store",
			Storage:     jetstream.FileStorage,
		},
		RetryInterval: 5 * time.Second,
	}
}

type ActorSystemOption func(*ActorSystem)

type contextKey string

const (
	idKey       = contextKey("id")
	hostnameKey = contextKey("hostname")
)

const (
	EnvStreamName = "ACTORS_STREAM"
	EnvKVBucket   = "ACTORS_KV_BUCKET"
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
