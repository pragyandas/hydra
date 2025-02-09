package actorsystem

import (
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
			Name:     "actorsystem-stream",
			Subjects: []string{"actors.>"},
			Storage:  jetstream.MemoryStorage,
		},
		KVConfig: jetstream.KeyValueConfig{
			Bucket:      "actors",
			Description: "Actor location store",
			TTL:         30 * time.Second,
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
