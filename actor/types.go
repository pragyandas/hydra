package actor

import (
	"context"
	"time"

	"github.com/pragyandas/hydra/transport"
)

type Config struct {
	HeartbeatInterval         time.Duration
	HeartbeatsMissedThreshold int
}

type ActorTransport interface {
	Setup(ctx context.Context, heartbeatInterval time.Duration) error
	SendMessage(actorType string, actorID string, msg []byte) error
}

type ActorOption func(*Actor)

type MessageHandler func(msg []byte) error

type MessageHandlerFactory func(actor *Actor) MessageHandler

type ErrorHandler func(err error, msg Message)

type TransportFactory func(actor *Actor) (ActorTransport, error)

type StateManagerFactory func(actor *Actor, stateSerializer StateSerializer) ActorStateManager

type Message = transport.Message
