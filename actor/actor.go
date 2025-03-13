package actor

import (
	"context"
	"fmt"
	"log"
)

type Actor struct {
	id        string
	actorType string
	handler   Handler
	msgCh     chan Message
	transport *ActorTransport
}

func WithHandlerFactory(factory HandlerFactory) ActorOption {
	return func(a *Actor) {
		a.handler = factory(a)
	}
}

func WithHandler(handler Handler) ActorOption {
	return func(a *Actor) {
		a.handler = handler
	}
}

func WithTransport(factory TransportFactory) ActorOption {
	return func(a *Actor) {
		transport, err := factory(a)
		if err != nil {
			log.Printf("Failed to create transport: %v", err)
			return
		}
		a.transport = transport
	}
}

func NewActor(
	ctx context.Context,
	id string,
	actorType string,
	opts ...ActorOption,
) (*Actor, error) {
	if id == "" {
		return nil, fmt.Errorf("id is required")
	}
	if actorType == "" {
		return nil, fmt.Errorf("actor type is required")
	}

	actor := &Actor{
		id:        id,
		actorType: actorType,
		msgCh:     make(chan Message),
	}

	// Apply options
	for _, opt := range opts {
		opt(actor)
	}

	if actor.transport == nil {
		return nil, fmt.Errorf("transport is required")
	}

	if actor.handler == nil {
		return nil, fmt.Errorf("handler is required")
	}

	return actor, nil
}

func (a *Actor) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)

	// Start message processing
	go a.processMessages(ctx)

	// Setup message transport
	if err := a.transport.Setup(ctx); err != nil {
		cancel()
		return fmt.Errorf("failed to start transport for actor %s: %w", a.id, err)
	}

	return nil
}

func (a *Actor) processMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-a.msgCh:
			if err := a.handler(msg.Data()); err != nil {
				log.Printf("Actor %s failed to process message: %v", a.id, err)
				msg.Nak()
				continue
			}
			msg.Ack()
		}
	}
}

func (a *Actor) MessageChannel() chan<- Message {
	return a.msgCh
}

func (a *Actor) ID() string {
	return a.id
}

func (a *Actor) Type() string {
	return a.actorType
}

func (a *Actor) SendMessage(actorType string, actorID string, message []byte) error {
	if err := a.transport.SendMessage(actorType, actorID, message); err != nil {
		return fmt.Errorf("failed to send message to actor %s: %w", actorID, err)
	}

	return nil
}
