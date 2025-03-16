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
	actorLivenessKV   jetstream.KeyValue
	config            *Config
	cp                *controlplane.ControlPlane
	ctxCancel         context.CancelFunc
	actorFactory      ActorFactory
	telemetryShutdown TelemetryShutdown
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

	// TODO: Refactor: Move all NATS related code in all packages to connection package
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

func (system *ActorSystem) initialize(ctx context.Context) error {
	tracer := telemetry.GetTracer()
	ctx, span := tracer.Start(ctx, "actorsystem-initialize")
	defer span.End()

	logger := telemetry.GetLogger(ctx, "actorsystem-initialize")

	stream, err := system.js.CreateStream(ctx, system.config.MessageStreamConfig)
	if err != nil {
		logger.Error("failed to create stream", zap.Error(err))
		return fmt.Errorf("failed to create stream: %w", err)
	}
	system.stream = stream

	kv, err := system.js.CreateKeyValue(ctx, system.config.KVConfig)
	if err != nil {
		if err == jetstream.ErrBucketExists {
			kv, err = system.js.KeyValue(ctx, system.config.KVConfig.Bucket)
			if err != nil {
				logger.Error("failed to get existing KV store", zap.Error(err))
				return fmt.Errorf("failed to get existing KV store: %w", err)
			}
		} else {
			logger.Error("failed to create KV store", zap.Error(err))
			return fmt.Errorf("failed to create KV store: %w", err)
		}
	}
	system.kv = kv

	actorLivenessKV, err := system.js.CreateKeyValue(ctx, system.config.ActorLivenessKVConfig)
	if err != nil {
		if err == jetstream.ErrBucketExists {
			actorLivenessKV, err = system.js.KeyValue(ctx, system.config.ActorLivenessKVConfig.Bucket)
			if err != nil {
				logger.Error("failed to get existing actor liveness KV store", zap.Error(err))
				return fmt.Errorf("failed to get existing actor liveness KV store: %w", err)
			}
		} else {
			logger.Error("failed to create actor liveness KV store", zap.Error(err))
			return fmt.Errorf("failed to create actor liveness KV store: %w", err)
		}
	}
	system.actorLivenessKV = actorLivenessKV

	system.cp, err = controlplane.New(system.config.ID, system.config.Region, system.nc, system.js)
	if err != nil {
		logger.Error("failed to create control plane", zap.Error(err))
		return fmt.Errorf("failed to create control plane: %w", err)
	}

	if err := system.cp.Start(ctx, system.config.ControlPlaneConfig); err != nil {
		logger.Error("failed to start control plane", zap.Error(err))
		return fmt.Errorf("failed to start control plane: %w", err)
	}

	system.actorFactory = newActorFactory(ctx, system.config.ActorConfig)

	return nil
}

func (system *ActorSystem) Close(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "actorsystem-close")
	if system.ctxCancel != nil {
		system.ctxCancel()
	}

	if system.cp != nil {
		system.cp.Stop()
	}

	if system.nc != nil {
		system.nc.Close()
	}

	if system.telemetryShutdown != nil {
		err := system.telemetryShutdown(ctx)
		if err != nil {
			logger.Error("failed to shutdown OTel SDK", zap.Error(err))
		}
	}
}

func (system *ActorSystem) createTransport(a *actor.Actor) (*transport.ActorTransport, error) {

	getKVKey := func(actorType, actorID string) string {
		actorBucket := system.cp.GetBucketKey(actorType, actorID)
		return fmt.Sprintf("%s/%s", system.config.KVConfig.Bucket, actorBucket)
	}

	return transport.NewActorTransport(&transport.Connection{
		JS:              system.js,
		KV:              system.kv,
		ActorLivenessKV: system.actorLivenessKV,
		StreamName:      system.config.MessageStreamConfig.Name,
	}, getKVKey, a)
}

func (system *ActorSystem) NewActor(id string, actorType string, handlerFactory actor.HandlerFactory) (*actor.Actor, error) {
	actor, err := system.actorFactory(id, actorType, handlerFactory, system.createTransport)
	if err != nil {
		return nil, err
	}

	return actor, nil
}
