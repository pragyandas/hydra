package actorsystemtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/actor/serializer"
	"github.com/pragyandas/hydra/test/e2e/utils"
)

type ActorState struct {
	Count int
}

func TestActorStateUpdate(t *testing.T) {
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

	testDuration := *utils.TestDurationFlag

	startTime := time.Now()
	endTime := startTime.Add(testDuration)

	pingHandler := func(self *actor.Actor) actor.MessageHandler {
		return func(msg []byte) error {
			actorState, err := self.GetState(testContext)
			if err != nil {
				t.Errorf("failed to get actor state: %v", err)
			}
			var state ActorState
			if actorState == nil {
				state = ActorState{Count: 0}
			} else {
				state = actorState.(ActorState)
				state.Count++
			}
			self.SetState(testContext, state)
			if time.Now().Before(endTime) {
				return self.SendMessage("pong", "1", msg)
			}
			return nil
		}
	}

	pongHandler := func(self *actor.Actor) actor.MessageHandler {
		return func(msg []byte) error {
			actorState, err := self.GetState(testContext)
			if err != nil {
				t.Errorf("failed to get actor state: %v", err)
			}
			var state ActorState
			if actorState == nil {
				state = ActorState{Count: 0}
			} else {
				state = actorState.(ActorState)
				state.Count++
			}
			self.SetState(testContext, state)
			if time.Now().Before(endTime) {
				return self.SendMessage("ping", "1", msg)
			}
			return nil
		}
	}

	system.RegisterActorType("ping", actor.ActorTypeConfig{
		MessageHandlerFactory: pingHandler,
		StateSerializer:       serializer.NewJSONSerializer(ActorState{}),
		MessageErrorHandler: func(err error, msg actor.Message) {
			t.Errorf("failed to handle message: %v", err)
		},
	})
	system.RegisterActorType("pong", actor.ActorTypeConfig{
		MessageHandlerFactory: pongHandler,
		StateSerializer:       serializer.NewJSONSerializer(ActorState{}),
		MessageErrorHandler: func(err error, msg actor.Message) {
			t.Errorf("failed to handle message: %v", err)
		},
	})

	pingActor, err := system.CreateActor("ping", "1")
	if err != nil {
		t.Fatalf("Failed to create ping actor: %v", err)
	}

	pongActor, err := system.CreateActor("pong", "1")
	if err != nil {
		t.Fatalf("Failed to create pong actor: %v", err)
	}

	// Send initial messages to start the ping-pong
	msg := []byte(fmt.Sprintf("ping-%d", time.Now().UnixNano()))
	if err := pingActor.SendMessage("pong", pongActor.ID(), msg); err != nil {
		t.Errorf("Failed to send message from ping, err: %v", err)
	}

	// Wait for test duration
	time.Sleep(testDuration)

	duration := time.Since(startTime)

	finalPingState, err := pingActor.GetState(testContext)
	if err != nil {
		t.Errorf("failed to get ping actor state: %v", err)
	}
	finalPongState, err := pongActor.GetState(testContext)
	if err != nil {
		t.Errorf("failed to get pong actor state: %v", err)
	}

	t.Logf("Test completed in %v", duration)
	t.Logf("Ping state: %+v", finalPingState)
	t.Logf("Pong state: %+v", finalPongState)
}
