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

func TestActorSystemMembershipUpdate(t *testing.T) {
	testContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn := utils.SetupTestConnection(t)
	defer conn.Close()

	var processedMessagesCount atomic.Int32

	// Setup system1

	t.Logf("setting up system1")

	system1 := utils.SetupTestActorsystem(t, "system1", conn, nil)

	if err := system1.Start(testContext); err != nil {
		t.Fatalf("Failed to start actor system: %v", err)
	}
	defer system1.Close(testContext)

	system1.RegisterActorType("test", actor.ActorTypeConfig{
		MessageHandlerFactory: func(self *actor.Actor) actor.MessageHandler {
			return func(msg []byte) error {
				time.Sleep(10 * time.Millisecond)
				processedMessagesCount.Add(1)
				return nil
			}
		},
	})

	system1Actors := make([]*actor.Actor, 50)
	for i := 0; i < 50; i++ {
		actor, err := system1.CreateActor("test", fmt.Sprintf("actor-system1-%d", i))
		if err != nil {
			t.Fatalf("Failed to create actor: %v", err)
		}
		system1Actors[i] = actor
	}

	t.Logf("system1 started")
	time.Sleep(5 * time.Second)

	// Setup system2

	t.Logf("setting up system2")

	system2 := utils.SetupTestActorsystem(t, "system2", conn, nil)

	if err := system2.Start(testContext); err != nil {
		t.Fatalf("Failed to start actor system: %v", err)
	}

	system2.RegisterActorType("test", actor.ActorTypeConfig{
		MessageHandlerFactory: func(self *actor.Actor) actor.MessageHandler {
			return func(msg []byte) error {
				time.Sleep(10 * time.Millisecond)
				processedMessagesCount.Add(1)
				return nil
			}
		},
	})

	system2Actors := make([]*actor.Actor, 50)
	for i := 0; i < 50; i++ {
		actor, err := system2.CreateActor("test", fmt.Sprintf("actor-system2-%d", i))
		if err != nil {
			t.Fatalf("Failed to create actor: %v", err)
		}
		system2Actors[i] = actor
	}

	t.Logf("system2 started")

	// Send messages from actors in system1 to actors in system2
	var numMessages int32
	for _, actor1 := range system1Actors {
		for _, actor2 := range system2Actors {
			actor1.SendMessage(actor2.Type(), actor2.ID(), []byte(fmt.Sprintf("message from %s to %s", actor1.ID(), actor2.ID())))
			numMessages++
		}
	}
	t.Logf("sent messages")
	t.Logf("processed messages count: %d", processedMessagesCount.Load())

	//Stop one actor system
	system2.Close(testContext)

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if processedMessagesCount.Load() == int32(numMessages) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if processedMessagesCount.Load() != int32(numMessages) {
		t.Errorf("expected %d messages to be processed, but got %d", numMessages, processedMessagesCount.Load())
		return
	}

	time.Sleep(10 * time.Second)

	t.Logf("Successfully processed all %d messages with actor resurrection", numMessages)
}
