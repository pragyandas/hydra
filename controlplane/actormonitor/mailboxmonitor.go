package actormonitor

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/connection"
	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

type ActorMailboxMonitor struct {
	connection *connection.Connection
	actorType  string
	actorID    string
	done       chan struct{}
}

func NewActorMailboxMonitor(connection *connection.Connection, actorType, actorID string) *ActorMailboxMonitor {
	return &ActorMailboxMonitor{
		connection: connection,
		actorType:  actorType,
		actorID:    actorID,
		done:       make(chan struct{}),
	}
}

func (m *ActorMailboxMonitor) Start(ctx context.Context, resurrectionHandler func()) {
	logger := telemetry.GetLogger(ctx, "mailbox-monitor")

	consumerName := fmt.Sprintf("%s-%s", m.actorType, m.actorID)
	consumer, err := m.connection.JS.Consumer(ctx, m.connection.StreamName, consumerName)
	if err != nil {
		logger.Error("failed to get consumer", zap.Error(err))
		return
	}

	for {
		select {
		case <-m.done:
			return
		case <-ctx.Done():
			return
		default:
			// Use Next() to wait for a message. This is more efficient than polling.
			// It will block until a message is available or the timeout is reached.
			msg, err := consumer.Next(jetstream.FetchMaxWait(5*time.Second), jetstream.FetchHeartbeat(1*time.Second))
			if err != nil {
				logger.Debug("error waiting for next message", zap.Error(err))
				continue
			}

			// A message was received, which means the queue is not empty.
			// We immediately Nak() it to make it available again for the resurrected actor.
			// This is a safe operation that tells NATS to not consider this message delivered.
			msg.Nak()

			logger.Debug("dead actor has pending messages, requesting resurrection",
				zap.String("actor", fmt.Sprintf("%s/%s", m.actorType, m.actorID)))

			resurrectionHandler()
			return
		}
	}
}

func (m *ActorMailboxMonitor) Stop() {
	close(m.done)
}
