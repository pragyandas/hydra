package utils

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsd "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/pragyandas/hydra/actorsystem"
)

func SetupTestActorsystem(t *testing.T) (context.Context, *actorsystem.ActorSystem, func()) {
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

	if !ns.ReadyForConnections(4 * time.Second) {
		t.Fatal("NATS server failed to start")
	}

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}

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

	testContext, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	system, err := actorsystem.NewActorSystem(testContext, config)
	if err != nil {
		t.Fatalf("Failed to create actor system: %v", err)
	}

	close := func() {
		system.Close(testContext)
		cancel()
		nc.Close()
		ns.Shutdown()
	}

	return testContext, system, close
}
