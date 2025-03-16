package transport

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

type ActorTransport struct {
	conn          *Connection
	actor         Actor
	subject       string
	getKVKey      GetKVKey
	messageSender MessageSender
}

type GetKVKey func(actorType, actorID string) string

func NewActorTransport(conn *Connection, getKVKey GetKVKey, actor Actor) (*ActorTransport, error) {
	return &ActorTransport{
		conn:     conn,
		actor:    actor,
		getKVKey: getKVKey,
	}, nil
}

func (t *ActorTransport) Setup(ctx context.Context, heartbeatInterval time.Duration) error {
	logger := telemetry.GetLogger(ctx, "transport-setup")

	kvCtx, kvCtxCancel := context.WithTimeout(ctx, 5*time.Second)
	defer kvCtxCancel()

	// Create the actor registration in KV store
	key := t.getKVKey(t.actor.Type(), t.actor.ID())

	t.subject = fmt.Sprintf("%s.%s.%s",
		t.conn.StreamName,
		t.actor.Type(),
		t.actor.ID(),
	)

	actorRegistration := ActorRegistration{
		CreatedAt: time.Now(),
	}

	// Register the actor
	_, registerErr := t.conn.KV.Create(kvCtx, key, actorRegistration.ToJSON())
	if registerErr != nil {
		logger.Error("failed to register actor", zap.Error(registerErr))
		return fmt.Errorf("failed to register actor: %w", registerErr)
	}

	logger.Debug("registered actor", zap.String("key", key))

	// Create the liveness entry in KV store
	revision, err := t.conn.ActorLivenessKV.Put(kvCtx, key, []byte{})
	if err != nil {
		logger.Error("failed to create liveness entry", zap.Error(err))
		return fmt.Errorf("failed to register actor: %w", err)
	}

	logger.Debug("created liveness entry", zap.String("key", key))

	go t.maintainLiveness(ctx, key, revision, heartbeatInterval)

	if err := t.setupConsumer(ctx); err != nil {
		logger.Error("failed to setup consumer", zap.Error(err))
		return fmt.Errorf("failed to setup consumer: %w", err)
	}

	t.messageSender = newMessageSender(ctx, t.conn, t.actor)

	logger.Debug("setup consumer", zap.String("subject", t.subject))

	return nil
}

func (t *ActorTransport) setupConsumer(ctx context.Context) error {
	logger := telemetry.GetLogger(ctx, "transport-setup-consumer")

	consumer, err := t.conn.JS.CreateOrUpdateConsumer(ctx, t.conn.StreamName, jetstream.ConsumerConfig{
		Name:          fmt.Sprintf("%s-%s", t.actor.Type(), t.actor.ID()),
		FilterSubject: t.subject,
		MaxDeliver:    1,
		MaxAckPending: 1,
	})
	if err != nil {
		logger.Error("failed to create consumer", zap.Error(err))
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	// TODO: Pull message from consumer only if the actor message channel is not full
	_, err = consumer.Consume(func(msg jetstream.Msg) {
		t.actor.MessageChannel() <- msg
	})
	if err != nil {
		logger.Error("failed to consume messages", zap.Error(err))
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	return nil
}

func (t *ActorTransport) maintainLiveness(ctx context.Context, key string, revision uint64, heartbeatInterval time.Duration) {
	logger := telemetry.GetLogger(ctx, "transport-maintain-liveness")

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			newRevision, err := t.conn.KV.Update(ctx, key, []byte{}, revision)
			if err != nil {
				logger.Error("failed to update liveness entry", zap.Error(err))
				continue
			}

			logger.Debug("updated liveness entry", zap.String("key", key), zap.Uint64("revision", newRevision))
			revision = newRevision
		}
	}
}

func (t *ActorTransport) SendMessage(actorType string, actorID string, msg []byte) error {
	return t.messageSender(actorType, actorID, msg)
}
