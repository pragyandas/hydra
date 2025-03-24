package actor

import "fmt"

type ActorTypeOption func(*ActorType)

type ActorTypeConfig struct {
	MessageHandlerFactory MessageHandlerFactory
	MessageErrorHandler   ErrorHandler
	StateSerializer       StateSerializer
}

// ActorType defines the behavior of an actor type
type ActorType struct {
	Name                  string
	MessageHandlerFactory MessageHandlerFactory
	MessageErrorHandler   ErrorHandler
	StateSerializer       StateSerializer
}

func NewActorType(name string, opts ...ActorTypeOption) (*ActorType, error) {
	aType := &ActorType{
		Name: name,
	}

	for _, opt := range opts {
		opt(aType)
	}

	if aType.MessageHandlerFactory == nil {
		return nil, fmt.Errorf("message handler is required for actor type %s", name)
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
