package actorsystem

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsd "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/actorsystem"
)

var actorStateTestDurationFlag = flag.Duration("test.duration", 5*time.Second, "Duration for the actor state test")

type ActorState struct {
	Count int
}

type ActorStateSerializer struct{}

func (s *ActorStateSerializer) Serialize(state any) ([]byte, error) {
	return json.Marshal(state)
}

func (s *ActorStateSerializer) Deserialize(data []byte) (any, error) {
	var state ActorState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return state, nil
}

func TestActorStateUpdate(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	system, err := actorsystem.NewActorSystem(ctx, config)
	if err != nil {
		t.Fatalf("Failed to create actor system: %v", err)
	}
	defer system.Close(ctx)

	testDuration := *actorStateTestDurationFlag

	startTime := time.Now()
	endTime := startTime.Add(testDuration)

	pingHandler := func(self *actor.Actor) actor.MessageHandler {
		return func(msg []byte) error {
			actorState, err := self.GetState(ctx)
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
			self.SetState(ctx, state)
			if time.Now().Before(endTime) {
				return self.SendMessage("pong", "1", msg)
			}
			return nil
		}
	}

	pongHandler := func(self *actor.Actor) actor.MessageHandler {
		return func(msg []byte) error {
			actorState, err := self.GetState(ctx)
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
			self.SetState(ctx, state)
			if time.Now().Before(endTime) {
				return self.SendMessage("ping", "1", msg)
			}
			return nil
		}
	}

	system.RegisterActorType("ping", actor.ActorTypeConfig{
		MessageHandlerFactory: pingHandler,
		StateSerializer:       &ActorStateSerializer{},
		MessageErrorHandler: func(err error, msg actor.Message) {
			t.Errorf("failed to handle message: %v", err)
		},
	})
	system.RegisterActorType("pong", actor.ActorTypeConfig{
		MessageHandlerFactory: pongHandler,
		StateSerializer:       &ActorStateSerializer{},
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

	finalPingState, err := pingActor.GetState(ctx)
	if err != nil {
		t.Errorf("failed to get ping actor state: %v", err)
	}
	finalPongState, err := pongActor.GetState(ctx)
	if err != nil {
		t.Errorf("failed to get pong actor state: %v", err)
	}

	t.Logf("Test completed in %v", duration)
	t.Logf("Ping state: %+v", finalPingState)
	t.Logf("Pong state: %+v", finalPongState)
}
