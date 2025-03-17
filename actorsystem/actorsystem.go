package actorsystem

import (
	"context"
	"fmt"

	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/connection"
	"github.com/pragyandas/hydra/controlplane"
	"github.com/pragyandas/hydra/telemetry"
	"github.com/pragyandas/hydra/transport"
	"go.uber.org/zap"
)

type ActorSystem struct {
	id                string
	connection        *connection.Connection
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

	connection, err := connection.New(ctx, config.NatsURL, config.ConnectOpts)
	if err != nil {
		logger.Error("failed to create connection", zap.Error(err))
		cancel()
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	system := &ActorSystem{
		id:         config.ID,
		connection: connection,
		config:     config,
		ctxCancel:  cancel,
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

	var err error
	if err = system.connection.Initialize(ctx, connection.Config{
		MessageStreamConfig:   system.config.MessageStreamConfig,
		KVConfig:              system.config.KVConfig,
		ActorLivenessKVConfig: system.config.ActorLivenessKVConfig,
	}); err != nil {
		logger.Error("failed to start connection", zap.Error(err))
		return fmt.Errorf("failed to start connection: %w", err)
	}

	system.cp, err = controlplane.New(system.connection)
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

	if system.connection != nil {
		system.connection.Close(ctx)
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

	return transport.NewActorTransport(system.connection, getKVKey, a)
}

func (system *ActorSystem) NewActor(id string, actorType string, handlerFactory actor.HandlerFactory) (*actor.Actor, error) {
	actor, err := system.actorFactory(id, actorType, handlerFactory, system.createTransport)
	if err != nil {
		return nil, err
	}

	return actor, nil
}
