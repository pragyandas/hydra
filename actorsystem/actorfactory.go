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

func newActorFactory(ctx context.Context, config actor.Config) ActorFactory {
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
		transport, err := transportFactory(actor)
		if err != nil {
			logger.Error("failed to create transport", zap.Error(err))
			return nil, err
		}
		stateManager := stateManagerFactory(actor, actorType.StateSerializer)

		// Apply options
		actor = actor.WithMessageHandler(messageHandler).
			WithTransport(transport).
			WithStateManager(stateManager).
			WithErrorHandler(actorType.MessageErrorHandler)

		err = actor.Start(ctx, config)
		if err != nil {
			logger.Error("failed to start actor", zap.Error(err))
			return nil, err
		}
		return actor, nil
	}
}
