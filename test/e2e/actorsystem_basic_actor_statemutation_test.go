package actorsystemtest

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/actor/serializer"
	"github.com/pragyandas/hydra/test/e2e/utils"
)

type ActorTestState struct {
	Count int
}

func TestActorStateMutation(t *testing.T) {
	numActors := 10

	testContext, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn := utils.SetupTestConnection(t)
	defer conn.Close()

	system := utils.SetupTestActorsystem(t, "test-system", conn)
	if err := system.Start(testContext); err != nil {
		t.Fatalf("Failed to start actor system: %v", err)
	}
	defer system.Close(testContext)

	testDuration := *utils.TestDurationFlag
	var receivedCount atomic.Int32
	var cycleCount atomic.Int32

	startTime := time.Now()
	endTime := startTime.Add(testDuration)

	pingHandler := func(self *actor.Actor) actor.MessageHandler {
		return func(msg []byte) error {
			actorState, err := self.GetState(testContext)
			if err != nil {
				return fmt.Errorf("failed to get actor state: %w", err)
			}
			var state ActorTestState
			if actorState == nil {
				state = ActorTestState{Count: 0}
			} else {
				state = actorState.(ActorTestState)
				state.Count++
			}
			if err := self.SetState(testContext, state); err != nil {
				return fmt.Errorf("failed to set actor state: %w", err)
			}
			receivedCount.Add(1)
			if time.Now().Before(endTime) {
				return self.SendMessage("pong", self.ID(), msg)
			}
			return nil
		}
	}

	pongHandler := func(self *actor.Actor) actor.MessageHandler {
		return func(msg []byte) error {
			actorState, err := self.GetState(testContext)
			if err != nil {
				return fmt.Errorf("failed to get actor state: %w", err)
			}
			var state ActorTestState
			if actorState == nil {
				state = ActorTestState{Count: 0}
			} else {
				state = actorState.(ActorTestState)
				state.Count++
			}
			if err := self.SetState(testContext, state); err != nil {
				return fmt.Errorf("failed to set actor state: %w", err)
			}
			cycleCount.Add(1)
			receivedCount.Add(1)
			if time.Now().Before(endTime) {
				return self.SendMessage("ping", self.ID(), msg)
			}
			return nil
		}
	}

	system.RegisterActorType("ping", actor.ActorTypeConfig{
		MessageHandlerFactory: pingHandler,
		StateSerializer:       serializer.NewJSONSerializer(ActorTestState{}),
		MessageErrorHandler: func(err error, msg actor.Message) {
			t.Errorf("failed to handle message: %v", err)
		},
	})
	system.RegisterActorType("pong", actor.ActorTypeConfig{
		MessageHandlerFactory: pongHandler,
		StateSerializer:       serializer.NewJSONSerializer(ActorTestState{}),
		MessageErrorHandler: func(err error, msg actor.Message) {
			t.Errorf("failed to handle message: %v", err)
		},
	})

	var pingActors []*actor.Actor

	for i := 0; i < numActors; i++ {
		pingActor, err := system.CreateActor("ping", fmt.Sprintf("%d", i))
		if err != nil {
			t.Fatalf("Failed to create ping actor %d: %v", i, err)
		}
		pingActors = append(pingActors, pingActor)

		_, err = system.CreateActor("pong", fmt.Sprintf("%d", i))
		if err != nil {
			t.Fatalf("Failed to create pong actor %d: %v", i, err)
		}
	}

	// Send initial messages to start the ping-pong
	for i := 0; i < numActors; i++ {
		go func(i int) {
			pongActor := pingActors[i].ID()
			msg := []byte(fmt.Sprintf("ping-%d-%d", i, time.Now().UnixNano()))
			if err := pingActors[i].SendMessage("pong", pongActor, msg); err != nil {
				t.Errorf("Failed to send message from ping-%d to %s: %v", i, pongActor, err)
			}
		}(i)
	}

	time.Sleep(testDuration)

	duration := time.Since(startTime)
	count := receivedCount.Load()
	cycles := cycleCount.Load()

	messagesPerSecond := float64(count) / duration.Seconds()

	t.Logf("Test completed in %v", duration)
	t.Logf("Total messages: %d", count)
	t.Logf("Total cycles: %d", cycles)
	t.Logf("Throughput: %.2f messages/second", messagesPerSecond)
}
