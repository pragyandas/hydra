package actormonitor

import (
	"context"
	"fmt"
	"time"

	"github.com/pragyandas/hydra/connection"
	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

type ActorMailboxMonitor struct {
	connection               *connection.Connection
	actorType                string
	actorID                  string
	actorResurrectionHandler func()
}

func NewActorMailboxMonitor(connection *connection.Connection, actorType, actorID string) *ActorMailboxMonitor {
	return &ActorMailboxMonitor{
		connection: connection,
		actorType:  actorType,
		actorID:    actorID,
	}
}

func (m *ActorMailboxMonitor) Start(ctx context.Context, resurrectionHandler func()) {
	logger := telemetry.GetLogger(ctx, "mailbox-monitor")

	consumerName := fmt.Sprintf("%s-%s", m.actorType, m.actorID)

	// JS does not support watching for pending messages, so we need to poll
	// TODO: Make this interval configurable, though 1 sec is good enough
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			consumer, err := m.connection.JS.Consumer(ctx, m.connection.StreamName, consumerName)
			if err != nil {
				continue
			}

			info, err := consumer.Info(ctx)
			if err != nil {
				continue
			}

			if info.NumPending > 0 {
				logger.Info("dead actor has pending messages",
					zap.String("actor", fmt.Sprintf("%s/%s", m.actorType, m.actorID)),
					zap.Uint64("pending", info.NumPending))

				m.actorResurrectionHandler()
				return
			}
		}
	}
}
