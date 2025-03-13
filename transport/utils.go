package transport

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

type MessageSender func(actorType string, actorID string, message []byte) error

func newMessageSender(ctx context.Context, conn *Connection, sender Actor) MessageSender {
	return func(actorType string, actorID string, message []byte) error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		headers := nats.Header{}
		headers.Set("message-id", nuid.Next())
		headers.Set("sender-id", fmt.Sprintf("%s.%s", sender.Type(), sender.ID()))
		headers.Set("msg-timestamp", time.Now().UTC().Format(time.RFC3339))

		subject := fmt.Sprintf("%s.%s.%s", conn.StreamName, actorType, actorID)

		natsMsg := nats.NewMsg(subject)
		natsMsg.Header = headers
		natsMsg.Data = message

		_, err := conn.JS.PublishMsg(ctx, natsMsg)
		return err
	}
}
