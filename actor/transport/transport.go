package transport

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/connection"
	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

type ActorTransport struct {
	connection    *connection.Connection
	actor         Actor
	subject       string
	getKVKey      GetKVKey
	messageSender MessageSender
}

type ConsumerConfig struct {
	MaxDeliver int
	AckWait    time.Duration
}

func NewActorTransport(connection *connection.Connection, getKVKey GetKVKey, actor Actor) (*ActorTransport, error) {
	return &ActorTransport{
		connection: connection,
		actor:      actor,
		getKVKey:   getKVKey,
	}, nil
}

func (t *ActorTransport) Setup(ctx context.Context, heartbeatInterval time.Duration, consumerConfig ConsumerConfig) error {
	logger := telemetry.GetLogger(ctx, "transport-setup")

	kvCtx, kvCtxCancel := context.WithTimeout(ctx, 5*time.Second)
	defer kvCtxCancel()

	// This creates actor key with region and bucket
	// Reference: controlplane/bucketmanager.go
	actorBucketKey := t.getKVKey(t.actor.Type(), t.actor.ID())

	// To avoid multiple actor instances due to split brain in membership change,
	// we check if the actor is already active elsewhere
	entry, err := t.connection.ActorLivenessKV.Get(kvCtx, actorBucketKey)
	if err == nil && entry != nil && entry.Value() != nil {
		logger.Error("actor already active elsewhere", zap.String("key", actorBucketKey))
		return fmt.Errorf("actor already active elsewhere")
	}

	t.subject = fmt.Sprintf("%s.%s.%s",
		t.connection.StreamName,
		t.actor.Type(),
		t.actor.ID(),
	)

	actorRegistration := ActorRegistration{
		CreatedAt: time.Now(),
	}

	// Register the actor in ActorKV
	_, registerErr := t.connection.ActorKV.Put(kvCtx, actorBucketKey, actorRegistration.ToJSON())
	if registerErr != nil {
		logger.Error("failed to register actor", zap.Error(registerErr))
		return fmt.Errorf("failed to register actor: %w", registerErr)
	}

	logger.Debug("registered actor", zap.String("key", actorBucketKey))

	// Create a liveness entry in ActorLivenessKV
	revision, err := t.connection.ActorLivenessKV.Put(kvCtx, actorBucketKey, []byte(time.Now().Format(time.RFC3339)))
	if err != nil {
		logger.Error("failed to create liveness entry", zap.Error(err))
		return fmt.Errorf("failed to register actor: %w", err)
	}

	logger.Debug("created liveness entry", zap.String("key", actorBucketKey))

	go t.maintainLiveness(ctx, actorBucketKey, revision, heartbeatInterval)

	if err := t.setupConsumer(ctx, consumerConfig); err != nil {
		logger.Error("failed to setup consumer", zap.Error(err))
		return fmt.Errorf("failed to setup consumer: %w", err)
	}

	t.messageSender = newMessageSender(ctx, t.connection, t.actor)

	logger.Debug("setup consumer", zap.String("subject", t.subject))

	return nil
}

func (t *ActorTransport) getConsumer(ctx context.Context, config ConsumerConfig) (jetstream.Consumer, error) {
	logger := telemetry.GetLogger(ctx, "transport-get-consumer")

	consumerName := fmt.Sprintf("%s-%s", t.actor.Type(), t.actor.ID())
	consumerConfig := jetstream.ConsumerConfig{
		Durable:       consumerName,
		FilterSubject: t.subject,
		AckWait:       config.AckWait,              // Redlivery will be triggered after this time
		MaxAckPending: 1,                           // Ensures strict ordering
		AckPolicy:     jetstream.AckExplicitPolicy, // Won't move to next message until ack is received
		MaxDeliver:    config.MaxDeliver,           // -1 means unlimited redelivery, depends on ack wait
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}

	existingConsumer, err := t.connection.JS.Consumer(ctx, t.connection.StreamName, consumerName)
	if err != nil {
		if err == jetstream.ErrConsumerNotFound {
			// create new consumer
			consumer, err := t.connection.JS.CreateOrUpdateConsumer(ctx, t.connection.StreamName, consumerConfig)
			if err != nil {
				logger.Error("failed to create consumer", zap.Error(err))
				return nil, fmt.Errorf("failed to create consumer: %w", err)
			}

			return consumer, nil
		}
		logger.Error("failed to get consumer", zap.Error(err))
		return nil, fmt.Errorf("failed to get consumer: %w", err)
	}

	info, err := existingConsumer.Info(ctx)

	if err != nil {
		logger.Error("failed to get consumer info", zap.Error(err))
		return nil, fmt.Errorf("failed to get consumer info: %w", err)
	}

	if info.AckFloor.Stream > 0 {
		// If consumer has pending messages, we need to start after the last delivered message
		consumerConfig.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		// Start after the last acknowledged message
		consumerConfig.OptStartSeq = info.AckFloor.Stream + 1

		err = t.connection.JS.DeleteConsumer(ctx, t.connection.StreamName, consumerName)
		if err != nil {
			logger.Error("failed to delete consumer", zap.Error(err))
			return nil, fmt.Errorf("failed to delete consumer: %w", err)
		}

		consumer, err := t.connection.JS.CreateOrUpdateConsumer(ctx, t.connection.StreamName, consumerConfig)
		if err != nil {
			logger.Error("failed to create consumer", zap.Error(err))
			return nil, fmt.Errorf("failed to create consumer: %w", err)
		}

		return consumer, nil
	}

	return existingConsumer, nil
}

func (t *ActorTransport) setupConsumer(ctx context.Context, config ConsumerConfig) error {
	logger := telemetry.GetLogger(ctx, "transport-setup-consumer")

	consumer, err := t.getConsumer(ctx, config)
	if err != nil {
		logger.Error("failed to get consumer", zap.Error(err))
		return fmt.Errorf("failed to get consumer: %w", err)
	}

	_, err = consumer.Consume(func(msg jetstream.Msg) {
		select {
		case <-ctx.Done():
			logger.Debug("context done, stopping consumer", zap.String("subject", t.subject))
		case t.actor.MessageChannel() <- msg:
			logger.Debug("message sent to actor", zap.String("subject", t.subject))
		}
	})
	if err != nil {
		logger.Error("failed to consume messages", zap.Error(err))
		return fmt.Errorf("failed to consume messages: %w", err)
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
			logger.Debug("context done, stopping liveness maintenance", zap.String("key", key))
			return
		case <-ticker.C:
			newRevision, err := t.connection.ActorLivenessKV.Update(ctx, key, []byte(time.Now().Format(time.RFC3339)), revision)
			if err != nil {
				logger.Error("failed to update liveness entry", zap.String("key", key), zap.Error(err))
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
