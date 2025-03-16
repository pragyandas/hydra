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

type Handler func([]byte) error

type HandlerFactory func(actor *Actor) Handler

type TransportFactory func(actor *Actor) (*transport.ActorTransport, error)

type Message = transport.Message

type ActorTransport = transport.ActorTransport
