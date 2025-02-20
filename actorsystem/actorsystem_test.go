package actorsystem

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsd "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/actor"
)

func TestActorCommunication(t *testing.T) {
	numActors := 1

	opts := &server.Options{
		Port:       -1, // Use random port
		Host:       "127.0.0.1",
		JetStream:  true,
		MaxPayload: 8 * 1024 * 1024, // 8MB
		StoreDir:   t.TempDir(),
	}
	ns := natsd.RunServer(opts)
	if ns == nil {
		t.Fatal("Failed to create NATS test server")
	}
	defer ns.Shutdown()

	if !ns.ReadyForConnections(4 * time.Second) {
		t.Fatal("NATS server failed to start")
	}

	config := &Config{
		ID:      "test-system",
		NatsURL: ns.ClientURL(), // Use embedded server URL
		StreamConfig: jetstream.StreamConfig{
			Name:     GetStreamName(),
			Subjects: []string{fmt.Sprintf("%s.>", GetStreamName())},
			Storage:  jetstream.MemoryStorage,
		},
		KVConfig: jetstream.KeyValueConfig{
			Bucket:      GetKVBucket(),
			Description: "Actor data store",
			Storage:     jetstream.FileStorage,
		},
		RetryInterval: 100 * time.Millisecond,
		Hostname:      "test-node",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	system, err := NewActorSystem(ctx, config)
	if err != nil {
		t.Fatalf("Failed to create actor system: %v", err)
	}
	defer system.Close()
	var receivedCount atomic.Int32

	pingHandler := func(self *actor.Actor) actor.Handler {
		return func(msg []byte) error {
			receivedCount.Add(1)
			return self.SendMessage("pong", self.ID(), msg)
		}
	}

	pongHandler := func(self *actor.Actor) actor.Handler {
		return func(msg []byte) error {
			receivedCount.Add(1)
			return self.SendMessage("ping", self.ID(), msg)
		}
	}

	// Create array to store ping actors
	var pingActors []*actor.Actor

	// Create numActors ping actors and store them in array
	for i := 0; i < numActors; i++ {
		pingActor, err := system.NewActor(fmt.Sprintf("%d", i), "ping", pingHandler)
		if err != nil {
			t.Fatalf("Failed to create ping actor %d: %v", i, err)
		}
		pingActors = append(pingActors, pingActor)
	}

	// Create numActors pong actors
	for i := 0; i < numActors; i++ {
		_, err := system.NewActor(fmt.Sprintf("%d", i), "pong", pongHandler)
		if err != nil {
			t.Fatalf("Failed to create pong actor %d: %v", i, err)
		}
	}

	// Have each ping actor send a message to corresponding pong actor in parallel
	var wg sync.WaitGroup
	wg.Add(numActors)
	for i := 0; i < numActors; i++ {
		go func(i int) {
			defer wg.Done()
			pongActor := pingActors[i].ID()
			msg := []byte(fmt.Sprintf("ping-%d-%d", i, time.Now().UnixNano()))

			if err := pingActors[i].SendMessage("pong", pongActor, msg); err != nil {
				t.Errorf("Failed to send message from ping-%d to %s: %v", i, pongActor, err)
			}
		}(i)
	}
	wg.Wait()

	<-ctx.Done()
	count := receivedCount.Load()
	t.Logf("Test completed. Received %d messages", count)
	if count == 0 {
		t.Error("No messages were received")
	}
}
