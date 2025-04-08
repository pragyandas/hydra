package actor

import (
	"context"
	"fmt"
	"time"

	"github.com/pragyandas/hydra/transport"
)

type Config struct {
	HeartbeatInterval         time.Duration
	HeartbeatsMissedThreshold int
	ConsumerConfig            ConsumerConfig
}

type ConsumerConfig = transport.ConsumerConfig

type ActorOption func(*Actor)

type ActorTransport interface {
	Setup(ctx context.Context, heartbeatInterval time.Duration, consumerConfig ConsumerConfig) error
	SendMessage(actorType string, actorID string, msg []byte) error
}

type ActorStateManager interface {
	Save(ctx context.Context, state any) error
	Load(ctx context.Context) (any, error)
}

type StateSerializer interface {
	Serialize(state any) ([]byte, error)
	Deserialize(data []byte) (any, error)
}

type MessageHandler func(msg []byte) error

type MessageHandlerFactory func(actor *Actor) MessageHandler

type ErrorHandler func(err error, msg Message)

type TransportFactory func(actor *Actor) (ActorTransport, error)

type StateManagerFactory func(actor *Actor, stateSerializer StateSerializer) ActorStateManager

type Message = transport.Message

type ActorId struct {
	Type string
	ID   string
}

func (id ActorId) String() string {
	return fmt.Sprintf("%s.%s", id.Type, id.ID)
}

type ActorTypeOption func(*ActorType)

type ActorTypeConfig struct {
	MessageHandlerFactory MessageHandlerFactory
	MessageErrorHandler   ErrorHandler
	StateSerializer       StateSerializer
	ActorConfig           Config
}
