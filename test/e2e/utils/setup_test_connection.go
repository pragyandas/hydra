package utils

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsd "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/pragyandas/hydra/actorsystem"
)

type TestConnection struct {
	Server *server.Server
	Conn   *nats.Conn
	JS     nats.JetStreamContext
}

func (t *TestConnection) Close() {
	// Clean up the test environment before closing the connection
	config := actorsystem.DefaultConfig()
	kvs := []string{
		config.KVConfig.Bucket,
		config.ActorLivenessKVConfig.Bucket,
		config.ControlPlaneConfig.BucketManagerConfig.KVConfig.Bucket,
	}

	for _, kv := range kvs {
		t.JS.DeleteKeyValue(kv)
	}

	for _, stream := range []string{config.MessageStreamConfig.Name} {
		t.JS.DeleteStream(stream)
	}

	t.Conn.Close()
	t.Server.Shutdown()
}

func SetupTestConnection(t *testing.T) *TestConnection {
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

	return &TestConnection{
		Server: ns,
		Conn:   nc,
		JS:     js,
	}
}
