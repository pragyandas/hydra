package actor

import (
	"time"

	"github.com/pragyandas/hydra/transport"
)

type Config struct {
	HeartbeatInterval         time.Duration
	HeartbeatsMissedThreshold int
}

type ActorOption func(*Actor)

type MessageHandler func(msg []byte) error

type MessageHandlerFactory func(actor *Actor) MessageHandler

type ErrorHandler func(err error, msg Message)

type TransportFactory func(actor *Actor) (*transport.ActorTransport, error)

type Message = transport.Message

type ActorTransport = transport.ActorTransport
