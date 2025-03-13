package actorsystem

import (
	"context"

	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

type ActorFactory func(id string, actorType string, handlerFactory actor.HandlerFactory, transportFactory actor.TransportFactory) (*actor.Actor, error)

func newActorFactory(ctx context.Context) ActorFactory {
	return func(id string,
		actorType string,
		handlerFactory actor.HandlerFactory,
		transportFactory actor.TransportFactory,
	) (*actor.Actor, error) {
		logger := telemetry.GetLogger(ctx, "actorsystem-new-actor")
		actor, err := actor.NewActor(
			ctx,
			id,
			actorType,
			actor.WithHandlerFactory(handlerFactory),
			actor.WithTransport(transportFactory),
		)

		if err != nil {
			logger.Error("failed to create actor", zap.Error(err))
			return nil, err
		}

		err = actor.Start(ctx)
		if err != nil {
			logger.Error("failed to start actor", zap.Error(err))
			return nil, err
		}
		return actor, nil
	}
}
