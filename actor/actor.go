package actor

import (
	"context"
	"fmt"
	"log"

	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

type Actor struct {
	id           string
	actorType    string
	handler      MessageHandler
	msgCh        chan Message
	transport    *ActorTransport
	state        any
	errorHandler ErrorHandler
	cancel       context.CancelFunc
}

func WithMessageHandlerFactory(factory MessageHandlerFactory) ActorOption {
	return func(a *Actor) {
		a.handler = factory(a)
	}
}

func WithMessageHandler(handler MessageHandler) ActorOption {
	return func(a *Actor) {
		a.handler = handler
	}
}

func WithState(state any) ActorOption {
	return func(a *Actor) {
		a.state = state
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

func WithErrorHandler(handler ErrorHandler) ActorOption {
	return func(a *Actor) {
		a.errorHandler = handler
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

func (a *Actor) Start(ctx context.Context, config Config) error {
	ctx, cancel := context.WithCancel(ctx)
	a.cancel = cancel

	// Start message processing
	go a.processMessages(ctx)

	// Setup message transport
	if err := a.transport.Setup(ctx, config.HeartbeatInterval); err != nil {
		a.cancel()
		return fmt.Errorf("failed to start transport for actor %s: %w", a.id, err)
	}

	return nil
}

func (a *Actor) processMessages(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "actor-process-messages")

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-a.msgCh:
			if err := a.handler(msg.Data()); err != nil {
				if a.errorHandler != nil {
					// It's the responsibility of the error handler to ack or nak the message
					a.errorHandler(err, msg)
				} else {
					logger.Error("Actor failed to process message", zap.Error(err))
					msg.Nak()
				}
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

func (a *Actor) GetState() any {
	return a.state
}

func (a *Actor) SetState(state any) {
	a.state = state
}
