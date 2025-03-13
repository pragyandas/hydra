package transport

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type ActorTransport struct {
	conn          *Connection
	actor         Actor
	subject       string
	revision      uint64
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

func (t *ActorTransport) Setup(ctx context.Context) error {
	kvCtx, kvCtxCancel := context.WithTimeout(ctx, 5*time.Second)
	defer kvCtxCancel()

	// Create the actor registration in KV store
	key := t.getKVKey(t.actor.Type(), t.actor.ID())

	t.subject = fmt.Sprintf("%s.%s.%s",
		t.conn.StreamName,
		t.actor.Type(),
		t.actor.ID(),
	)

	value := ActorRegistration{
		Region:     GetRegion(),
		Subject:    t.subject,
		LastActive: time.Now(),
	}

	revision, err := t.conn.KV.Create(kvCtx, key, value.ToJSON())
	if err != nil {
		return fmt.Errorf("failed to register actor: %w", err)
	}
	t.revision = revision

	if err := t.setupConsumer(ctx); err != nil {
		return fmt.Errorf("failed to setup consumer: %w", err)
	}

	t.messageSender = newMessageSender(ctx, t.conn, t.actor)

	go t.maintainLiveness(ctx, key, value)

	return nil
}

func (t *ActorTransport) setupConsumer(ctx context.Context) error {
	consumer, err := t.conn.JS.CreateOrUpdateConsumer(ctx, t.conn.StreamName, jetstream.ConsumerConfig{
		Name:          fmt.Sprintf("%s-%s", t.actor.Type(), t.actor.ID()),
		FilterSubject: t.subject,
		MaxDeliver:    1,
		MaxAckPending: 1,
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	// TODO: Pull message from consumer only if the actor message channel is not full
	_, err = consumer.Consume(func(msg jetstream.Msg) {
		t.actor.MessageChannel() <- msg
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	return nil
}

func (t *ActorTransport) maintainLiveness(ctx context.Context, key string, reg ActorRegistration) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			reg.LastActive = time.Now()
			revision, err := t.conn.KV.Update(ctx, key, reg.ToJSON(), t.revision)
			if err != nil {
				continue
			}
			t.revision = revision
		}
	}
}

func (t *ActorTransport) SendMessage(actorType string, actorID string, msg []byte) error {
	return t.messageSender(actorType, actorID, msg)
}
