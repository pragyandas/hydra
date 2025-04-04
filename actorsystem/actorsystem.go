package actorsystem

import (
	"context"
	"fmt"
	"sync"

	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/common/utils"
	"github.com/pragyandas/hydra/connection"
	"github.com/pragyandas/hydra/controlplane"
	"github.com/pragyandas/hydra/telemetry"
	"github.com/pragyandas/hydra/transport"
	"go.uber.org/zap"
)

type ActorStatus int

const (
	ActorStatusStarting ActorStatus = iota
	ActorStatusStarted
)

type systemActor struct {
	actor  *actor.Actor
	status ActorStatus
}

type ActorSystem struct {
	config                *Config
	connection            *connection.Connection
	cp                    *controlplane.ControlPlane
	ctxCancel             context.CancelFunc
	actorFactory          ActorFactory
	actors                map[string]*systemActor
	actorTypes            map[string]*actor.ActorType
	actorResurrectionChan chan actor.ActorId
	telemetryShutdown     TelemetryShutdown
}

func NewActorSystem(config *Config) (*ActorSystem, error) {
	// TODO: Merge partial config with default config
	if config == nil {
		config = DefaultConfig()
	}

	system := &ActorSystem{
		config:     config,
		actors:     make(map[string]*systemActor),
		actorTypes: make(map[string]*actor.ActorType),
	}

	return system, nil
}

func (system *ActorSystem) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	ctx = context.WithValue(ctx, idKey, system.config.ID)
	system.ctxCancel = cancel

	logger := telemetry.GetLogger(ctx, "actorsystem-start")
	logger.Info("starting actor system")

	var err error
	system.telemetryShutdown, err = telemetry.SetupOTelSDK(ctx)
	if err != nil {
		logger.Error("failed to setup OTel SDK", zap.Error(err))
		cancel()
		return fmt.Errorf("failed to setup OTel SDK: %w", err)
	}
	tracer := telemetry.GetTracer()
	ctx, span := tracer.Start(ctx, "actorsystem-start")
	defer span.End()

	system.connection, err = connection.New(ctx, system.config.NatsURL, system.config.ConnectOpts)
	if err != nil {
		logger.Error("failed to create connection", zap.Error(err))
		cancel()
		return fmt.Errorf("failed to create connection: %w", err)
	}

	if err = system.connection.Initialize(ctx, connection.Config{
		MessageStreamConfig:   system.config.MessageStreamConfig,
		KVConfig:              system.config.KVConfig,
		ActorLivenessKVConfig: system.config.ActorLivenessKVConfig,
	}); err != nil {
		logger.Error("failed to start connection", zap.Error(err))
		return fmt.Errorf("failed to start connection: %w", err)
	}

	system.actorFactory = newActorFactory(ctx, system.config.ActorConfig)

	system.actorResurrectionChan = make(chan actor.ActorId)
	go system.handleActorResurrection(ctx, system.config.ActorResurrectionConcurrency)

	system.cp, err = controlplane.New(system.connection, system.actorResurrectionChan)
	if err != nil {
		logger.Error("failed to create control plane", zap.Error(err))
		return fmt.Errorf("failed to create control plane: %w", err)
	}

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
		logger.Info("actor system context cancelled")
	}

	if system.cp != nil {
		system.cp.Stop(ctx)
		logger.Info("gracefully closed control plane")
	}

	if system.actors != nil {
		for _, status := range system.actors {
			if status.actor != nil {
				status.actor.Close()
			}
		}
		// Reset the actors map
		system.actors = make(map[string]*systemActor)
		logger.Info("gracefully closed all actors")
	}

	if system.connection != nil {
		system.connection.Close(ctx)
		logger.Info("gracefully closed connection")
	}

	if system.telemetryShutdown != nil {
		err := system.telemetryShutdown(ctx)
		if err != nil {
			logger.Error("failed to shutdown OTel SDK", zap.Error(err))
		}
		logger.Info("gracefully closed OTel SDK")
	}
}

func (system *ActorSystem) handleActorResurrection(ctx context.Context, concurrency int) {
	logger := telemetry.GetLogger(ctx, "actorsystem-handle-actor-resurrection")

	resurrectionQueue := utils.NewTaskQueue(concurrency)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go resurrectionQueue.Run(ctx, &wg)

	for {
		select {
		case <-ctx.Done():
			close(system.actorResurrectionChan)
			wg.Wait()
			logger.Info("actor resurrection queue closed")
			return
		case req, ok := <-system.actorResurrectionChan:
			if !ok {
				return
			}

			newTask := utils.NewTask(fmt.Sprintf("%s.%s", req.Type, req.ID),
				func() {
					if _, err := system.CreateActor(req.Type, req.ID); err != nil {
						logger.Warn("failed to create actor",
							zap.String("type", req.Type),
							zap.String("id", req.ID),
							zap.Error(err))
					} else {
						logger.Info("actor resurrected successfully",
							zap.String("type", req.Type),
							zap.String("id", req.ID))
					}
				},
			)

			resurrectionQueue.Add(ctx, newTask)
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
		actor.WithActorConfig(config.ActorConfig),
	)
	if err != nil {
		return fmt.Errorf("failed to create actor type %s: %w", name, err)
	}

	system.actorTypes[name] = aType
	return nil
}

func (system *ActorSystem) CreateActor(actorType string, id string) (*actor.Actor, error) {
	key := fmt.Sprintf("%s.%s", actorType, id)
	if systemActor, exists := system.actors[key]; exists {
		if systemActor.status == ActorStatusStarting {
			return nil, fmt.Errorf("actor %s creation in progress", key)
		}
		if systemActor.actor != nil {
			return nil, fmt.Errorf("actor %s already exists", key)
		}
	}

	aType, exists := system.actorTypes[actorType]
	if !exists {
		return nil, fmt.Errorf("actor type %s not registered", actorType)
	}

	system.actors[key] = &systemActor{status: ActorStatusStarting}

	actor, err := system.actorFactory(id,
		*aType,
		system.createActorTransport,
		system.createActorStateManager,
	)

	if err != nil {
		delete(system.actors, key)
		return nil, fmt.Errorf("failed to create actor: %w", err)
	}

	actor = actor.WithCloseCallback(func() {
		delete(system.actors, key)
	})

	system.actors[key] = &systemActor{actor: actor, status: ActorStatusStarted}
	return actor, nil
}

func (system *ActorSystem) GetOrCreateActor(actorType string, id string) (*actor.Actor, error) {
	key := fmt.Sprintf("%s.%s", actorType, id)
	if systemActor, exists := system.actors[key]; exists {
		if systemActor.actor != nil {
			return systemActor.actor, nil
		}
		if systemActor.status == ActorStatusStarting {
			return nil, fmt.Errorf("actor %s creation is already in progress", key)
		}
	}
	return system.CreateActor(actorType, id)
}
