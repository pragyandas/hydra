package actorsystem

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/actor/transport"
	"github.com/pragyandas/hydra/common/utils"
	"github.com/pragyandas/hydra/connection"
	"github.com/pragyandas/hydra/controlplane"
	"github.com/pragyandas/hydra/telemetry"
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

func NewActorSystem(config *Config) *ActorSystem {
	// TODO: Merge partial config with default config
	if config == nil {
		config = DefaultConfig()
	}

	system := &ActorSystem{
		config:     config,
		actors:     make(map[string]*systemActor),
		actorTypes: make(map[string]*actor.ActorType),
	}

	return system
}

func (system *ActorSystem) WithNATSURL(url string) (*ActorSystem, error) {
	prefix := "nats://"
	if !strings.HasPrefix(url, prefix) {
		return nil, fmt.Errorf("url should have prefix %s", prefix)
	}
	system.config.NatsURL = url

	return system, nil
}

func (system *ActorSystem) Start(ctx context.Context) error {
	ctx = utils.EnrichContext(ctx, system.config.ID, system.config.Region)

	ctx, cancel := context.WithCancel(ctx)
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
	ctx = utils.EnrichContext(ctx, system.config.ID, system.config.Region)

	logger := telemetry.GetLogger(ctx, "actorsystem-close")

	if system.ctxCancel != nil {
		system.ctxCancel()
		logger.Debug("actor system context cancelled")
	}

	if system.cp != nil {
		system.cp.Stop(ctx)
		logger.Debug("gracefully closed control plane")
	}

	if system.actors != nil {
		for _, status := range system.actors {
			if status.actor != nil {
				status.actor.Close()
			}

		}
		// Reset the actors map
		system.actors = make(map[string]*systemActor)
		logger.Debug("gracefully closed all actors")
	}

	if system.connection != nil {
		system.connection.Close(ctx)
		logger.Debug("gracefully closed connection")
	}

	if system.telemetryShutdown != nil {
		err := system.telemetryShutdown(ctx)
		if err != nil {
			logger.Error("failed to shutdown OTel SDK", zap.Error(err))
		}
		logger.Debug("gracefully closed OTel SDK")
	}

	logger.Info("closed actor system",
		zap.String("region", system.config.Region),
		zap.String("id", system.config.ID))
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
			logger.Debug("actor resurrection queue closed")
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
						logger.Debug("actor resurrected successfully",
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
		actorBucket := system.cp.GetBucketKey(actorType, actorID, system.config.Region)
		return actorBucket
	}

	return transport.NewActorTransport(system.connection, getKVKey, a)
}

func (system *ActorSystem) createActorStateManager(a *actor.Actor, stateSerializer actor.StateSerializer) actor.ActorStateManager {
	return actor.NewStateManager(a, system.connection, stateSerializer)
}

func (system *ActorSystem) RegisterActorType(ctx context.Context, name string, config actor.ActorTypeConfig) error {
	ctx = utils.EnrichContext(ctx, system.config.ID, system.config.Region)

	aType, err := actor.NewActorType(
		ctx,
		name,
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

func (system *ActorSystem) GetOwnedBuckets() []int {
	return system.cp.GetOwnedBuckets()
}

func (system *ActorSystem) GetMemberCountAndPosition() (int, int) {
	return system.cp.GetMemberCountAndPosition()
}
