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

func TestActorResurrection(t *testing.T) {
	testContext, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	system, close := utils.SetupTestActorsystem(t)
	if err := system.Start(testContext); err != nil {
		t.Fatalf("Failed to start actor system: %v", err)
	}
	defer func() {
		system.Close(testContext)
		close()
		cancel()
	}()

	const (
		numMessages    = 100
		processingTime = 100 * time.Millisecond
		actorDeathTime = 3 * time.Second
	)

	var processedCount atomic.Int32

	slowHandler := func(self *actor.Actor) actor.MessageHandler {
		return func(msg []byte) error {
			time.Sleep(processingTime)
			processedCount.Add(1)
			t.Logf("processed message %d", processedCount.Load())
			return nil
		}
	}

	system.RegisterActorType("slow", actor.ActorTypeConfig{
		MessageHandlerFactory: slowHandler,
		MessageErrorHandler: func(err error, msg actor.Message) {
			t.Errorf("failed to handle message: %v", err)
		},
	})

	actorA, err := system.CreateActor("slow", "test-actor-a")
	if err != nil {
		t.Fatalf("failed to create actor: %v", err)
	}

	actorB, err := system.CreateActor("slow", "test-actor-b")
	if err != nil {
		t.Fatalf("failed to create actor: %v", err)
	}

	for i := 0; i < numMessages; i++ {
		msg := []byte(fmt.Sprintf("message-%d", i))
		if err := actorA.SendMessage("slow", actorB.ID(), msg); err != nil {
			t.Errorf("Failed to send message %d: %v", i, err)
		}
	}

	time.Sleep(actorDeathTime)

	actorB.Close()

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
