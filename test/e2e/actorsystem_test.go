package actorsystem

import (
	"context"
	"flag"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsd "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/actor/serializer"
	"github.com/pragyandas/hydra/actorsystem"
)

var testDurationFlag = flag.Duration("test.duration", 5*time.Second, "Duration for the actor communication test")

func TestActorCommunication(t *testing.T) {
	numActors := 1

	opts := &server.Options{
		Port:      -1,
		Host:      "127.0.0.1",
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	ns := natsd.RunServer(opts)
	if ns == nil {
		t.Fatal("Failed to create NATS test server")
	}
	defer ns.Shutdown()

	if !ns.ReadyForConnections(4 * time.Second) {
		t.Fatal("NATS server failed to start")
	}

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Failed to create JetStream context: %v", err)
	}

	defaultConfig := actorsystem.DefaultConfig()
	config := &actorsystem.Config{
		ID:                    "test-system",
		NatsURL:               ns.ClientURL(),
		MessageStreamConfig:   defaultConfig.MessageStreamConfig,
		KVConfig:              defaultConfig.KVConfig,
		ActorLivenessKVConfig: defaultConfig.ActorLivenessKVConfig,
		ActorConfig:           defaultConfig.ActorConfig,
		RetryInterval:         500 * time.Millisecond,
		Region:                "test-region",
		ControlPlaneConfig:    defaultConfig.ControlPlaneConfig,
	}

	kvs := []string{
		config.KVConfig.Bucket,
		config.ActorLivenessKVConfig.Bucket,
		config.ControlPlaneConfig.MembershipConfig.KVConfig.Bucket,
		config.ControlPlaneConfig.BucketManagerConfig.KVConfig.Bucket,
	}
	for _, kv := range kvs {
		js.DeleteKeyValue(kv)
	}

	for _, stream := range []string{config.MessageStreamConfig.Name} {
		js.DeleteStream(stream)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	system, err := actorsystem.NewActorSystem(ctx, config)
	if err != nil {
		t.Fatalf("Failed to create actor system: %v", err)
	}
	defer system.Close(ctx)

	testDuration := *testDurationFlag
	var receivedCount atomic.Int32
	var cycleCount atomic.Int32

	startTime := time.Now()
	endTime := startTime.Add(testDuration)

	pingHandler := func(self *actor.Actor) actor.MessageHandler {
		return func(msg []byte) error {
			receivedCount.Add(1)
			if time.Now().Before(endTime) {
				return self.SendMessage("pong", self.ID(), msg)
			}
			return nil
		}
	}

	pongHandler := func(self *actor.Actor) actor.MessageHandler {
		return func(msg []byte) error {
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
		StateSerializer:       serializer.NewJSONSerializer(),
		MessageErrorHandler: func(err error, msg actor.Message) {
			t.Errorf("failed to handle message: %v", err)
		},
	})
	system.RegisterActorType("pong", actor.ActorTypeConfig{
		MessageHandlerFactory: pongHandler,
		StateSerializer:       serializer.NewJSONSerializer(),
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
