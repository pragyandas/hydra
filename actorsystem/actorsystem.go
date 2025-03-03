package actorsystem

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/actorsystem/cache"
	"github.com/pragyandas/hydra/controlplane"
	"github.com/pragyandas/hydra/transport"
)

type ActorSystem struct {
	id        string
	nc        *nats.Conn
	js        jetstream.JetStream
	stream    jetstream.Stream
	kv        jetstream.KeyValue
	config    *Config
	cp        *controlplane.ControlPlane
	ctx       context.Context
	ctxCancel context.CancelFunc
	cache     *cache.Cache
}

func NewActorSystem(parentCtx context.Context, config *Config) (*ActorSystem, error) {
	if config == nil {
		config = DefaultConfig()
	}

	ctx, cancel := context.WithCancel(parentCtx)
	ctx = context.WithValue(ctx, idKey, config.ID)

	nc, err := nats.Connect(config.NatsURL, config.ConnectOpts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		cancel()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	system := &ActorSystem{
		id:        config.ID,
		nc:        nc,
		js:        js,
		config:    config,
		ctx:       ctx,
		ctxCancel: cancel,
	}

	if err := system.initialize(ctx); err != nil {
		system.Close()
		return nil, err
	}

	return system, nil
}

func (as *ActorSystem) initialize(ctx context.Context) error {
	stream, err := as.js.CreateStream(ctx, as.config.MessageStreamConfig)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}
	as.stream = stream

	kv, err := as.js.CreateKeyValue(ctx, as.config.ActorKVConfig)
	if err != nil {
		if err == jetstream.ErrBucketExists {
			kv, err = as.js.KeyValue(ctx, as.config.ActorKVConfig.Bucket)
			if err != nil {
				return fmt.Errorf("failed to get existing KV store: %w", err)
			}
		} else {
			return fmt.Errorf("failed to create KV store: %w", err)
		}
	}
	as.kv = kv

	as.cp, err = controlplane.New(as.config.ID, as.config.Region, as.js)
	if err != nil {
		return fmt.Errorf("failed to create control plane: %w", err)
	}

	if err := as.cp.Start(ctx, as.config.ControlPlaneConfig); err != nil {
		return fmt.Errorf("failed to start control plane: %w", err)
	}

	cache, err := cache.NewCache(ctx, as.kv)
	if err != nil {
		return fmt.Errorf("failed to create cache: %w", err)
	}
	as.cache = cache

	return nil
}

func (as *ActorSystem) Close() {
	if as.ctxCancel != nil {
		as.ctxCancel()
	}

	if as.cache != nil {
		as.cache.Close()
	}

	if as.cp != nil {
		as.cp.Stop()
	}

	if as.nc != nil {
		as.nc.Close()
	}
}

func (as *ActorSystem) createTransport(ctx context.Context, a *actor.Actor) (*transport.ActorTransport, error) {
	return transport.NewActorTransport(ctx, &transport.Connection{
		JS:         as.js,
		KV:         as.kv,
		StreamName: as.config.MessageStreamConfig.Name,
	}, func(actorType, actorID string) string {
		actorBucket := as.cp.GetBucketKey(actorType, actorID)
		return fmt.Sprintf("%s/%s", as.config.ActorKVConfig.Bucket, actorBucket)
	}, a)
}

func (as *ActorSystem) NewActor(id string, actorType string, handlerFactory actor.HandlerFactory) (*actor.Actor, error) {
	actor, err := actor.NewActor(
		as.ctx,
		id,
		actorType,
		actor.WithHandlerFactory(handlerFactory),
		actor.WithTransport(as.createTransport),
		actor.WithCache(as.cache),
	)
	if err != nil {
		return nil, err
	}

	actor.Start()
	return actor, nil
}

func (as *ActorSystem) NewActorWithHandler(id string, actorType string, handler actor.Handler) (*actor.Actor, error) {
	actor, err := actor.NewActor(
		as.ctx,
		id,
		actorType,
		actor.WithHandler(handler),
		actor.WithTransport(as.createTransport),
		actor.WithCache(as.cache),
	)
	if err != nil {
		return nil, err
	}

	actor.Start()
	return actor, nil
}
