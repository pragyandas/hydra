package transport

import (
	"encoding/json"
	"time"
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

type ActorRegistration struct {
	CreatedAt time.Time `json:"created_at"`
}

func (r ActorRegistration) ToJSON() []byte {
	data, _ := json.Marshal(r)
	return data
}

type GetKVKey func(actorType, actorID string) string
