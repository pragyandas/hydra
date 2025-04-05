package actor

import (
	"context"
	"fmt"
	"sync"

	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

type Actor struct {
	id                  string
	actorType           string
	config              Config
	handler             MessageHandler
	msgCh               chan Message
	transport           ActorTransport
	closeCallback       func()
	stateManager        ActorStateManager
	state               any
	errorHandler        ErrorHandler
	cancel              context.CancelFunc
	messageProcessingWg sync.WaitGroup
}

func (a *Actor) WithMessageHandler(handler MessageHandler) *Actor {
	a.handler = handler
	return a
}

func (a *Actor) WithStateManager(stateManager ActorStateManager) *Actor {
	a.stateManager = stateManager
	return a
}

func (a *Actor) WithTransport(transport ActorTransport) *Actor {
	a.transport = transport
	return a
}

func (a *Actor) WithErrorHandler(handler ErrorHandler) *Actor {
	a.errorHandler = handler
	return a
}

func (a *Actor) WithCloseCallback(callback func()) *Actor {
	a.closeCallback = callback
	return a
}

func (a *Actor) WithConfig(config Config) *Actor {
	a.config = config
	return a
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
		id:                  id,
		actorType:           actorType,
		msgCh:               make(chan Message),
		messageProcessingWg: sync.WaitGroup{},
	}

	// Apply options
	for _, opt := range opts {
		opt(actor)
	}

	return actor, nil
}

func (a *Actor) Start(ctx context.Context) error {
	logger := telemetry.GetLogger(ctx, "actor-start")

	ctx, cancel := context.WithCancel(ctx)
	a.cancel = cancel

	// Load state from kv
	state, err := a.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to load state for actor %s: %w", a.id, err)
	}
	a.state = state

	// Start message processing
	go a.processMessages(ctx)

	// Setup message transport
	if err := a.transport.Setup(ctx, a.config.HeartbeatInterval, a.config.ConsumerConfig); err != nil {
		a.cancel()
		return fmt.Errorf("failed to start transport for actor %s: %w", a.id, err)
	}

	logger.Debug("started actor", zap.String("id", a.id), zap.String("actorType", a.actorType))

	return nil
}

func (a *Actor) Close() {
	if a.cancel != nil {
		a.cancel()
	}

	// wait for the message processing to finish
	a.messageProcessingWg.Wait()

	if a.closeCallback != nil {
		a.closeCallback()
	}
}

func (a *Actor) processMessages(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "actor-process-messages")
	for {
		select {
		case <-ctx.Done():
			logger.Debug("context done, stopping message processing", zap.String("actor", a.id))
			return
		case msg := <-a.msgCh:
			a.messageProcessingWg.Add(1)
			go func() {
				defer a.messageProcessingWg.Done()
				err := a.handler(msg.Data())
				if err != nil {
					if a.errorHandler != nil {
						// It's the responsibility of the error handler to ack or nak the message
						a.errorHandler(err, msg)
					} else {
						logger.Error("Actor failed to process message", zap.Error(err))
						msg.Nak()
					}
					return
				}

				msg.Ack()
			}()
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

func (a *Actor) GetState(ctx context.Context) (any, error) {
	return a.stateManager.Load(ctx)
}

func (a *Actor) SetState(ctx context.Context, state any) error {
	if err := a.stateManager.Save(ctx, state); err != nil {
		return fmt.Errorf("failed to save state for actor %s: %w", a.id, err)
	}
	a.state = state
	return nil
}
