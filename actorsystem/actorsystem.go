package actorsystem

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/controlplane"
	"github.com/pragyandas/hydra/telemetry"
	"github.com/pragyandas/hydra/transport"
	"go.uber.org/zap"
)

type ActorSystem struct {
	id                string
	nc                *nats.Conn
	js                jetstream.JetStream
	stream            jetstream.Stream
	kv                jetstream.KeyValue
	config            *Config
	cp                *controlplane.ControlPlane
	ctx               context.Context
	ctxCancel         context.CancelFunc
	telemetryShutdown func(context.Context) error
}

func NewActorSystem(parentCtx context.Context, config *Config) (*ActorSystem, error) {
	// TODO: Merge partial config with default config
	if config == nil {
		config = DefaultConfig()
	}

	ctx, cancel := context.WithCancel(parentCtx)
	ctx = context.WithValue(ctx, idKey, config.ID)

	logger := telemetry.GetLogger(ctx, "actorsystem")
	logger.Info("starting actor system")

	shutdown, err := telemetry.SetupOTelSDK(ctx)
	if err != nil {
		logger.Error("failed to setup OTel SDK", zap.Error(err))
		cancel()
		return nil, fmt.Errorf("failed to setup OTel SDK: %w", err)
	}

	nc, err := nats.Connect(config.NatsURL, config.ConnectOpts...)
	if err != nil {
		logger.Error("failed to connect to NATS", zap.Error(err))
		cancel()
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		cancel()
		logger.Error("failed to create JetStream context", zap.Error(err))
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

	system.telemetryShutdown = shutdown

	if err := system.initialize(ctx); err != nil {
		logger.Error("failed to initialize actor system", zap.Error(err))
		system.Close(ctx)
		return nil, err
	}

	logger.Info("initialized actor system")

	return system, nil
}

func (as *ActorSystem) initialize(ctx context.Context) error {
	tracer := telemetry.GetTracer()
	ctx, span := tracer.Start(ctx, "actorsystem-initialize")
	defer span.End()

	logger := telemetry.GetLogger(ctx, "actorsystem-initialize")

	stream, err := as.js.CreateStream(ctx, as.config.MessageStreamConfig)
	if err != nil {
		logger.Error("failed to create stream", zap.Error(err))
		return fmt.Errorf("failed to create stream: %w", err)
	}
	as.stream = stream

	kv, err := as.js.CreateKeyValue(ctx, as.config.ActorKVConfig)
	if err != nil {
		if err == jetstream.ErrBucketExists {
			kv, err = as.js.KeyValue(ctx, as.config.ActorKVConfig.Bucket)
			if err != nil {
				logger.Error("failed to get existing KV store", zap.Error(err))
				return fmt.Errorf("failed to get existing KV store: %w", err)
			}
		} else {
			logger.Error("failed to create KV store", zap.Error(err))
			return fmt.Errorf("failed to create KV store: %w", err)
		}
	}
	as.kv = kv

	as.cp, err = controlplane.New(as.config.ID, as.config.Region, as.nc, as.js)
	if err != nil {
		logger.Error("failed to create control plane", zap.Error(err))
		return fmt.Errorf("failed to create control plane: %w", err)
	}

	if err := as.cp.Start(ctx, as.config.ControlPlaneConfig); err != nil {
		logger.Error("failed to start control plane", zap.Error(err))
		return fmt.Errorf("failed to start control plane: %w", err)
	}

	return nil
}

func (as *ActorSystem) Close(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "actorsystem-close")
	if as.ctxCancel != nil {
		as.ctxCancel()
	}

	if as.cp != nil {
		as.cp.Stop()
	}

	if as.nc != nil {
		as.nc.Close()
	}

	if as.telemetryShutdown != nil {
		err := as.telemetryShutdown(ctx)
		if err != nil {
			logger.Error("failed to shutdown OTel SDK", zap.Error(err))
		}
	}
}

func (as *ActorSystem) createTransport(ctx context.Context, a *actor.Actor) (*transport.ActorTransport, error) {

	getKVKey := func(actorType, actorID string) string {
		actorBucket := as.cp.GetBucketKey(actorType, actorID)
		return fmt.Sprintf("%s/%s", as.config.ActorKVConfig.Bucket, actorBucket)
	}

	return transport.NewActorTransport(ctx, &transport.Connection{
		JS:         as.js,
		KV:         as.kv,
		StreamName: as.config.MessageStreamConfig.Name,
	}, getKVKey, a)
}

func (as *ActorSystem) NewActor(id string, actorType string, handlerFactory actor.HandlerFactory) (*actor.Actor, error) {
	// TODO: Do not send context to the actor
	actor, err := actor.NewActor(
		as.ctx,
		id,
		actorType,
		actor.WithHandlerFactory(handlerFactory),
		actor.WithTransport(as.createTransport),
	)
	if err != nil {
		return nil, err
	}

	// TODO: Somehow send actor system context to the actor
	// If actor system context is cancelled, the actor should be cancelled
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
	)
	if err != nil {
		return nil, err
	}

	actor.Start()
	return actor, nil
}
