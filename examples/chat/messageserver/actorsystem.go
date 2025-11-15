package messageserver

import (
	"context"

	as "github.com/pragyandas/hydra/actorsystem"
	"github.com/pragyandas/hydra/examples/chat/messageserver/actors"
)

func initializeActorSystem(ctx context.Context, natsURL string) (*as.ActorSystem, error) {
	actorSystem, err := as.NewActorSystem(nil).WithNATSURL(natsURL)
	if err != nil {
		return nil, err
	}
	if err := actorSystem.Start(ctx); err != nil {
		return nil, err
	}

	if err := actors.RegisterChatroomActor(actorSystem); err != nil {
		actorSystem.Close(ctx)
		return nil, err
	}
	if err := actors.RegisterUserActor(actorSystem); err != nil {
		actorSystem.Close(ctx)
		return nil, err
	}

	// Creating a single static chatroom for the example app
	if _, err := actorSystem.CreateActor("chatroom", "1"); err != nil {
		actorSystem.Close(ctx)
		return nil, err
	}

	return actorSystem, nil
}
