package actor

import (
	"context"

	"github.com/pragyandas/hydra/actorsystem/cache"
	"github.com/pragyandas/hydra/transport"
)

type ActorOption func(*Actor)

type Handler func([]byte) error

type HandlerFactory func(actor *Actor) Handler

type TransportFactory func(ctx context.Context, actor *Actor) (*transport.ActorTransport, error)

type Message = transport.Message

type ActorTransport = transport.ActorTransport

type Cache = cache.Cache
