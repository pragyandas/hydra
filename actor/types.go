package actor

import (
	"context"

	"github.com/pragyandas/hydra/actorsystem/cache"
	"github.com/pragyandas/hydra/transport"
)

type ActorOption func(*Actor)

// TransportFactory creates a transport for an actor
type TransportFactory func(ctx context.Context, actor *Actor) (*transport.ActorTransport, error)

type Message = transport.Message

type ActorTransport = transport.ActorTransport

type Cache = cache.Cache
