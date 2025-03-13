package actor

import (
	"github.com/pragyandas/hydra/transport"
)

type ActorOption func(*Actor)

type Handler func([]byte) error

type HandlerFactory func(actor *Actor) Handler

type TransportFactory func(actor *Actor) (*transport.ActorTransport, error)

type Message = transport.Message

type ActorTransport = transport.ActorTransport
