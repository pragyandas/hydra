package actorsystemtest

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/actorsystem"
	"github.com/pragyandas/hydra/test/e2e/utils"
)

func TestActorClusterRedistribution(t *testing.T) {
	const (
		processingTime = 100 * time.Millisecond
	)

	defaultConfig := actorsystem.DefaultConfig()
	stabilizationInterval := defaultConfig.ControlPlaneConfig.BucketRecalculationStabilizationInterval

	testContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn := utils.SetupTestConnection(t)
	defer conn.Close()

	systemA := utils.SetupTestActorsystem(t, "system-a", conn, nil)
	if err := systemA.Start(testContext); err != nil {
		t.Fatalf("Failed to start actor system: %v", err)
	}

	var processedCount atomic.Int32

	slowHandler := func(self *actor.Actor) actor.MessageHandler {
		return func(msg []byte) error {
			time.Sleep(processingTime)
			processedCount.Add(1)
			t.Logf("processed message %s", string(msg))
			return nil
		}
	}

	systemA.RegisterActorType(testContext, "test-actor", actor.ActorTypeConfig{
		MessageHandlerFactory: slowHandler,
		MessageErrorHandler: func(err error, msg actor.Message) {
			t.Errorf("failed to handle message: %v", err)
		},
	})

	systemAActors := make([]*actor.Actor, 0)
	// Create 50 actors on systemA
	for i := 0; i < 50; i++ {
		actorA, err := systemA.CreateActor("test-actor", fmt.Sprintf("test-actor-A-%d", i))
		if err != nil {
			t.Fatalf("Failed to create actor: %v", err)
		}
		systemAActors = append(systemAActors, actorA)
	}

	systemB := utils.SetupTestActorsystem(t, "system-b", conn, nil)
	if err := systemB.Start(testContext); err != nil {
		t.Fatalf("Failed to start actor system: %v", err)
	}

	systemB.RegisterActorType(testContext, "test-actor", actor.ActorTypeConfig{
		MessageHandlerFactory: slowHandler,
		MessageErrorHandler: func(err error, msg actor.Message) {
			t.Errorf("failed to handle message: %v", err)
		},
	})

	systemBActors := make([]*actor.Actor, 0)
	// Create 50 actors on systemB
	for i := 0; i < 50; i++ {
		actorB, err := systemB.CreateActor("test-actor", fmt.Sprintf("test-actor-B-%d", i))
		if err != nil {
			t.Fatalf("Failed to create actor: %v", err)
		}
		systemBActors = append(systemBActors, actorB)
	}

	time.Sleep(stabilizationInterval + 2*time.Second) // Wait for stabilization

	bucketsA := systemA.GetOwnedBuckets()
	bucketsB := systemB.GetOwnedBuckets()

	t.Logf("systemA buckets: %v", bucketsA)
	t.Logf("systemB buckets: %v", bucketsB)

	numMessages := 0
	for _, actorB := range systemBActors {
		for _, actorA := range systemAActors {
			actorB.SendMessage("test-actor", actorA.ID(), []byte(fmt.Sprintf("message-%d", numMessages)))
			numMessages++
		}
	}

	// Kill systemA to redistribute actors
	systemA.Close(testContext)

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
