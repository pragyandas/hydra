package transport

import (
	"encoding/json"
	"os"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type Actor interface {
	ID() string
	Type() string
	MessageChannel() chan<- Message
}

// Add more methods from jetstream.Msg here if needed
type Message interface {
	Data() []byte
	Ack() error
	Nak() error
}

type Connection struct {
	JS         jetstream.JetStream
	KV         jetstream.KeyValue
	StreamName string
}

type ActorRegistration struct {
	Region      string    `json:"region"`
	NodeID      string    `json:"node_id"`
	FullSubject string    `json:"full_subject"`
	Active      bool      `json:"active"`
	LastActive  time.Time `json:"last_active"`
}

func (r ActorRegistration) ToJSON() []byte {
	data, _ := json.Marshal(r)
	return data
}

const (
	EnvRegion = "REGION"
	EnvNodeID = "NODE_ID"

	DefaultRegion = "local"
	DefaultNodeID = "local"
)

// GetRegion returns the region from env var or "local"
func GetRegion() string {
	if region := os.Getenv(EnvRegion); region != "" {
		return region
	}
	return DefaultRegion
}

// GetNodeID returns the node ID from env var or "local"
func GetNodeID() string {
	if nodeID := os.Getenv(EnvNodeID); nodeID != "" {
		return nodeID
	}
	return DefaultNodeID
}
