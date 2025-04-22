package actorsystemtest

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/test/e2e/utils"
)

func TestActorSystemRestartActorResurrection(t *testing.T) {
	testContext, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn := utils.SetupTestConnection(t)
	defer conn.Close()

	system := utils.SetupTestActorsystem(t, "test-system", conn, nil)
	if err := system.Start(testContext); err != nil {
		t.Fatalf("Failed to start actor system: %v", err)
	}

	const (
		numMessages            = 100
		processingTime         = 100 * time.Millisecond
		actorSystemRestartTime = 3 * time.Second
	)

	var processedCount atomic.Int32

	system.RegisterActorType("test", actor.ActorTypeConfig{
		MessageHandlerFactory: func(self *actor.Actor) actor.MessageHandler {
			return func(msg []byte) error {
				time.Sleep(processingTime)
				t.Logf("processed message %s", string(msg))
				processedCount.Add(1)
				return nil
			}
		},
		MessageErrorHandler: func(err error, msg actor.Message) {
			t.Errorf("failed to handle message: %v", err)
		},
	})

	actorA, err := system.CreateActor("test", "actor-a")
	if err != nil {
		t.Errorf("failed to create actor: %v", err)
	}

	actorB, err := system.CreateActor("test", "actor-b")
	if err != nil {
		t.Errorf("failed to create actor: %v", err)
	}

	for i := 0; i < numMessages; i++ {
		msg := []byte(fmt.Sprintf("message-%d", i))
		if err := actorA.SendMessage("test", actorB.ID(), msg); err != nil {
			t.Errorf("Failed to send message %d: %v", i, err)
		}
	}

	time.Sleep(actorSystemRestartTime)

	// Restart the actor system
	system.Close(testContext)
	t.Logf("actor system closed")

	// Sleep to ensure the actor is dead (liveness entry is TTL'ed in 3 seconds)
	time.Sleep(3 * time.Second)

	system.Start(testContext)
	t.Logf("actor system re-started")

	// Wait for the actor to be resurrected
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if processedCount.Load() == int32(numMessages) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if processedCount.Load() != int32(numMessages) {
		t.Errorf("expected %d messages to be processed, but got %d", numMessages, processedCount.Load())
		return
	}

	t.Logf("Successfully processed all %d messages with actor resurrection", numMessages)
}
