package actorsystem

import (
	"context"

	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

type ActorFactory func(id string,
	actorType actor.ActorType,
	transportFactory actor.TransportFactory,
	stateManagerFactory actor.StateManagerFactory,
) (*actor.Actor, error)

func newActorFactory(ctx context.Context, defaultConfig actor.Config) ActorFactory {
	return func(id string,
		actorType actor.ActorType,
		transportFactory actor.TransportFactory,
		stateManagerFactory actor.StateManagerFactory,
	) (*actor.Actor, error) {
		logger := telemetry.GetLogger(ctx, "actorsystem-new-actor")

		// Create base actor first
		actor, err := actor.NewActor(ctx, id, actorType.Name)
		if err != nil {
			logger.Error("failed to create actor", zap.Error(err))
			return nil, err
		}

		// Create components using actor instance
		messageHandler := actorType.MessageHandlerFactory(actor)
		transport, err := transportFactory(ctx, actor)
		if err != nil {
			logger.Error("failed to create transport", zap.Error(err))
			return nil, err
		}
		stateManager := stateManagerFactory(actor, actorType.StateSerializer)

		actorConfig := mergeActorConfig(actorType.ActorConfig, defaultConfig)

		// Build actor with components
		actor = actor.WithMessageHandler(messageHandler).
			WithTransport(transport).
			WithStateManager(stateManager).
			WithErrorHandler(actorType.MessageErrorHandler).
			WithConfig(actorConfig)

		err = actor.Start(ctx)
		if err != nil {
			logger.Error("failed to start actor", zap.Error(err))
			return nil, err
		}
		return actor, nil
	}
}

func mergeActorConfig(typeConfig, defaultConfig actor.Config) actor.Config {
	merged := defaultConfig
	if typeConfig.HeartbeatInterval != 0 {
		merged.HeartbeatInterval = typeConfig.HeartbeatInterval
	}
	if typeConfig.HeartbeatsMissedThreshold != 0 {
		merged.HeartbeatsMissedThreshold = typeConfig.HeartbeatsMissedThreshold
	}

	// Consumer config is not merged, it is set in entirety as
	if typeConfig.ConsumerConfig != (actor.ConsumerConfig{}) {
		merged.ConsumerConfig = typeConfig.ConsumerConfig
	}
	return merged
}
