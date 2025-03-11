package transport

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

type ActorTransport struct {
	conn     *Connection
	actor    Actor
	subject  string
	revision uint64
	getKVKey GetKVKey
	ctx      context.Context
	cancel   context.CancelFunc
}

type GetKVKey func(actorType, actorID string) string

func NewActorTransport(ctx context.Context, conn *Connection, getKVKey GetKVKey, actor Actor) (*ActorTransport, error) {
	transportCtx, cancel := context.WithCancel(ctx)
	return &ActorTransport{
		conn:     conn,
		actor:    actor,
		getKVKey: getKVKey,
		ctx:      transportCtx,
		cancel:   cancel,
	}, nil
}

func (t *ActorTransport) Setup() error {
	// Create a timeout context for the setup operation
	setupCtx, cancel := context.WithTimeout(t.ctx, 30*time.Second)
	defer cancel()

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

	revision, err := t.conn.KV.Create(setupCtx, key, value.ToJSON())
	if err != nil {
		return fmt.Errorf("failed to register actor: %w", err)
	}
	t.revision = revision

	if err := t.setupConsumer(); err != nil {
		return fmt.Errorf("failed to setup consumer: %w", err)
	}

	go t.maintainLiveness(key, value)

	return nil
}

func (t *ActorTransport) setupConsumer() error {
	consumer, err := t.conn.JS.CreateOrUpdateConsumer(t.ctx, t.conn.StreamName, jetstream.ConsumerConfig{
		Name:          fmt.Sprintf("%s-%s", t.actor.Type(), t.actor.ID()),
		FilterSubject: t.subject,
		MaxDeliver:    1,
		MaxAckPending: 1,
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	_, err = consumer.Consume(func(msg jetstream.Msg) {
		t.actor.MessageChannel() <- msg
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	return nil
}

func (t *ActorTransport) maintainLiveness(key string, reg ActorRegistration) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return

		case <-ticker.C:
			reg.LastActive = time.Now()
			revision, err := t.conn.KV.Update(t.ctx, key, reg.ToJSON(), t.revision)
			if err != nil {
				continue
			}
			t.revision = revision
		}
	}
}

func (t *ActorTransport) SendMessage(ctx context.Context, actorType string, actorID string, msg []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	headers := nats.Header{}
	headers.Set("message-id", nuid.Next())
	headers.Set("sender-id", fmt.Sprintf("%s.%s", t.actor.Type(), t.actor.ID()))
	headers.Set("msg-timestamp", time.Now().UTC().Format(time.RFC3339))

	subject := fmt.Sprintf("%s.%s.%s", t.conn.StreamName, actorType, actorID)

	natsMsg := nats.NewMsg(subject)
	natsMsg.Header = headers
	natsMsg.Data = msg

	_, err := t.conn.JS.PublishMsg(ctx, natsMsg)
	return err
}
