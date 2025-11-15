package actor

import (
	"context"
	"fmt"

	"github.com/pragyandas/hydra/telemetry"
)

// ActorType defines the behavior of an actor type
type ActorType struct {
	Name                  string
	MessageHandlerFactory MessageHandlerFactory
	MessageErrorHandler   ErrorHandler
	StateSerializer       StateSerializer
	ActorConfig           Config
}

func NewActorType(ctx context.Context, name string, opts ...ActorTypeOption) (*ActorType, error) {
	logger := telemetry.GetLogger(ctx, "actorsystem-new-actor-type")
	aType := &ActorType{
		Name: name,
	}

	for _, opt := range opts {
		opt(aType)
	}

	if aType.MessageHandlerFactory == nil {
		aType.MessageHandlerFactory = func(self *Actor) MessageHandler {
			return func(msg []byte) error {
				logger.Info(fmt.Sprintf("%s uses dummy handler, register a custom handler", name))
				return nil
			}
		}
	}

	return aType, nil
}

func WithMessageHandlerFactory(messageHandlerFactory MessageHandlerFactory) ActorTypeOption {
	return func(a *ActorType) {
		a.MessageHandlerFactory = messageHandlerFactory
	}
}

func WithMessageErrorHandler(messageErrorHandler ErrorHandler) ActorTypeOption {
	return func(a *ActorType) {
		a.MessageErrorHandler = messageErrorHandler
	}
}

func WithStateSerializer(stateSerializer StateSerializer) ActorTypeOption {
	return func(a *ActorType) {
		a.StateSerializer = stateSerializer
	}
}

func WithActorConfig(actorConfig Config) ActorTypeOption {
	return func(a *ActorType) {
		a.ActorConfig = actorConfig
	}
}
