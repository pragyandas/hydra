package actor

import (
	"context"
	"fmt"
	"log"

	"github.com/pragyandas/hydra/actorsystem/cache"
)

type Actor struct {
	id                string
	actorType         string
	messageStreamName string
	handler           Handler
	ctx               context.Context
	ctxCancel         context.CancelFunc
	msgCh             chan Message
	transport         *ActorTransport
	cache             *cache.Cache
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
		transport, err := factory(a.ctx, a)
		if err != nil {
			log.Printf("Failed to create transport: %v", err)
			return
		}
		a.transport = transport
	}
}

func WithCache(cache *cache.Cache) ActorOption {
	return func(a *Actor) {
		a.cache = cache
	}
}

func NewActor(
	ctx context.Context,
	id string,
	actorType string,
	messageStreamName string,
	opts ...ActorOption,
) (*Actor, error) {
	if id == "" {
		return nil, fmt.Errorf("id is required")
	}
	if actorType == "" {
		return nil, fmt.Errorf("actor type is required")
	}

	ctx, cancel := context.WithCancel(ctx)

	actor := &Actor{
		id:                id,
		actorType:         actorType,
		messageStreamName: messageStreamName,
		ctx:               ctx,
		ctxCancel:         cancel,
		msgCh:             make(chan Message),
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

	if actor.cache == nil {
		return nil, fmt.Errorf("cache is required")
	}

	return actor, nil
}

func (a *Actor) Start() {
	go a.processMessages()
	if err := a.transport.Setup(); err != nil {
		log.Printf("Failed to start transport for actor %s: %v", a.id, err)
	}
}

func (a *Actor) processMessages() {
	for {
		select {
		case <-a.ctx.Done():
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

func (a *Actor) Stop() {
	a.ctxCancel()
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
	subject := fmt.Sprintf("%s.%s.%s", a.messageStreamName, actorType, actorID)

	if err := a.transport.SendMessage(a.ctx, subject, message); err != nil {
		return fmt.Errorf("failed to send message to actor %s: %w", subject, err)
	}

	return nil
}
