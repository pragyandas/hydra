package messageserver

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats-server/v2/server"
	natsd "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"

	"github.com/pragyandas/hydra/actorsystem"
	"github.com/pragyandas/hydra/examples/chat/messageserver/actors"
)

type NATSServer struct {
	Server *server.Server
	Conn   *nats.Conn
	JS     nats.JetStreamContext
}

type MessageServer struct {
	ns          *NATSServer
	actorSystem *actorsystem.ActorSystem
}

func Initialize(ctx context.Context) *MessageServer {
	natsServer := setupNATSServer()
	actorSystem, err := initializeActorSystem(ctx, natsServer.Server.ClientURL())
	if err != nil {
		log.Fatalf("Failed to create actor system: %v", err)
	}

	return &MessageServer{
		ns:          natsServer,
		actorSystem: actorSystem,
	}
}

func (s *MessageServer) CreateUser(id string, conn *websocket.Conn) (*actors.User, error) {
	user, err := actors.CreateUser(id, s.actorSystem, func(msg []byte) error {
		log.Println(string(msg))
		conn.WriteMessage(websocket.TextMessage, msg)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create user: %w", err)
	}

	return user, nil
}

func (s *MessageServer) Close() {
	s.actorSystem.Close(context.Background())
	config := actorsystem.DefaultConfig()
	kvs := []string{
		config.KVConfig.Bucket,
		config.ActorLivenessKVConfig.Bucket,
		config.ControlPlaneConfig.BucketManagerConfig.KVConfig.Bucket,
	}

	for _, kv := range kvs {
		s.ns.JS.DeleteKeyValue(kv)
	}

	for _, stream := range []string{config.MessageStreamConfig.Name} {
		s.ns.JS.DeleteStream(stream)
	}

	s.ns.Conn.Close()
	s.ns.Server.Shutdown()
}

// If you have an existing NATS server, no need to create this test server
func setupNATSServer() *NATSServer {
	opts := &server.Options{
		Port:      -1,
		Host:      "127.0.0.1",
		JetStream: true,
	}

	ns := natsd.RunServer(opts)
	if ns == nil {
		log.Fatal("Failed to create NATS test server")
	}

	if !ns.ReadyForConnections(4 * time.Second) {
		log.Fatal("NATS server failed to start")
	}

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Failed to create JetStream context: %v", err)
	}

	return &NATSServer{
		Server: ns,
		Conn:   nc,
		JS:     js,
	}
}
