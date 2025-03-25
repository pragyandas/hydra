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
	id                    string
	connection            *connection.Connection
	config                *Config
	cp                    *controlplane.ControlPlane
	ctxCancel             context.CancelFunc
	actorFactory          ActorFactory
	actorTypes            map[string]*actor.ActorType
	actorResurrectionChan chan actor.ActorId
	telemetryShutdown     TelemetryShutdown
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
		id:                    config.ID,
		connection:            connection,
		config:                config,
		ctxCancel:             cancel,
		actorTypes:            make(map[string]*actor.ActorType),
		actorResurrectionChan: make(chan actor.ActorId),
		telemetryShutdown:     shutdown,
	}

	system.cp, err = controlplane.New(system.connection, system.actorResurrectionChan)
	if err != nil {
		logger.Error("failed to create control plane", zap.Error(err))
		return nil, fmt.Errorf("failed to create control plane: %w", err)
	}

	if err := system.start(ctx); err != nil {
		logger.Error("failed to initialize actor system", zap.Error(err))
		system.Close(ctx)
		return nil, err
	}

	return system, nil
}

func (system *ActorSystem) start(ctx context.Context) error {
	tracer := telemetry.GetTracer()
	ctx, span := tracer.Start(ctx, "actorsystem-start")
	defer span.End()

	logger := telemetry.GetLogger(ctx, "actorsystem-start")

	var err error
	if err = system.connection.Initialize(ctx, connection.Config{
		MessageStreamConfig:   system.config.MessageStreamConfig,
		KVConfig:              system.config.KVConfig,
		ActorLivenessKVConfig: system.config.ActorLivenessKVConfig,
	}); err != nil {
		logger.Error("failed to start connection", zap.Error(err))
		return fmt.Errorf("failed to start connection: %w", err)
	}

	system.actorFactory = newActorFactory(ctx, system.config.ActorConfig)

	go system.handleActorResurrection(ctx)

	if err := system.cp.Start(ctx, system.config.ControlPlaneConfig); err != nil {
		logger.Error("failed to start control plane", zap.Error(err))
		return fmt.Errorf("failed to start control plane: %w", err)
	}

	logger.Info("started actor system")

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

func (system *ActorSystem) handleActorResurrection(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "actorsystem-handle-actor-resurrection")

	for {
		select {
		case <-ctx.Done():
			close(system.actorResurrectionChan)
			return
		case req, ok := <-system.actorResurrectionChan:
			if !ok {
				return
			}
			go func(req actor.ActorId) {
				if _, err := system.CreateActor(req.Type, req.ID); err != nil {
					logger.Error("failed to create actor", zap.Error(err))
				}
			}(req)
		}
	}

}

func (system *ActorSystem) createActorTransport(a *actor.Actor) (actor.ActorTransport, error) {

	getKVKey := func(actorType, actorID string) string {
		actorBucket := system.cp.GetBucketKey(actorType, actorID)
		return actorBucket
	}

	return transport.NewActorTransport(system.connection, getKVKey, a)
}

func (system *ActorSystem) createActorStateManager(a *actor.Actor, stateSerializer actor.StateSerializer) actor.ActorStateManager {
	return actor.NewStateManager(a, system.connection, stateSerializer)
}

func (system *ActorSystem) RegisterActorType(name string, config actor.ActorTypeConfig) error {
	aType, err := actor.NewActorType(name,
		actor.WithMessageHandlerFactory(config.MessageHandlerFactory),
		actor.WithMessageErrorHandler(config.MessageErrorHandler),
		actor.WithStateSerializer(config.StateSerializer),
	)
	if err != nil {
		return fmt.Errorf("failed to create actor type %s: %w", name, err)
	}

	system.actorTypes[name] = aType
	return nil
}

func (system *ActorSystem) CreateActor(actorType string, id string) (*actor.Actor, error) {
	aType, exists := system.actorTypes[actorType]
	if !exists {
		return nil, fmt.Errorf("actor type %s not registered", actorType)
	}

	actor, err := system.actorFactory(id,
		*aType,
		system.createActorTransport,
		system.createActorStateManager,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create actor: %w", err)
	}

	return actor, nil
}
