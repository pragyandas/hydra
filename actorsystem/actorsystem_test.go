package actorsystem

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsd "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/actor"
)

func TestActorCommunication(t *testing.T) {
	// Update server creation with JetStream enabled
	opts := &server.Options{
		Port:      -1, // Use random port
		Host:      "127.0.0.1",
		JetStream: true,
		StoreDir:  t.TempDir(), // Use temp dir for storage
	}
	ns := natsd.RunServer(opts)
	if ns == nil {
		t.Fatal("Failed to create NATS test server")
	}
	defer ns.Shutdown()

	// Wait for server to be ready
	if !ns.ReadyForConnections(4 * time.Second) {
		t.Fatal("NATS server failed to start")
	}

	// Create config for test with local server URL
	config := &Config{
		ID:      "test-system",
		NatsURL: ns.ClientURL(), // Use embedded server URL
		StreamConfig: jetstream.StreamConfig{
			Name:     "test-stream",
			Subjects: []string{"actors.>"},
			Storage:  jetstream.MemoryStorage,
		},
		KVConfig: jetstream.KeyValueConfig{
			Bucket:      "test-coordination",
			Description: "Test coordination",
			TTL:         30 * time.Second,
			Storage:     jetstream.MemoryStorage,
		},
		Hostname: "test-node",
	}

	// Create actor system
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	system, err := NewActorSystem(ctx, config)
	if err != nil {
		t.Fatalf("Failed to create actor system: %v", err)
	}
	defer system.Close()
	var receivedCount atomic.Int32
	var pingActor *actor.Actor

	// Create ping actor
	pingHandler := func(msg []byte) error {
		fmt.Println("ping received message", string(msg))
		return pingActor.SendMessage("pong", "pong", msg)
	}

	// Create pong actor
	pongHandler := func(msg []byte) error {
		receivedCount.Add(1)
		pingActor.SendMessage("ping", "ping", msg)
		fmt.Println("pong received message", string(msg))
		return nil
	}

	// Create both actors
	pingActor, err = system.NewActor("ping", "ping", pingHandler)
	if err != nil {
		t.Fatalf("Failed to create ping actor: %v", err)
	}

	_, err = system.NewActor("pong", "pong", pongHandler)
	if err != nil {
		t.Fatalf("Failed to create pong actor: %v", err)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			count := receivedCount.Load()
			t.Logf("Test completed. Received %d messages", count)
			if count == 0 {
				t.Error("No messages were received")
			}
			return

		case <-ticker.C:
			if err := pingActor.SendMessage("pong", "pong", []byte(fmt.Sprintf("ping-%d", time.Now().Unix()))); err != nil {
				t.Errorf("Failed to send message: %v", err)
			}
		}
	}
}
