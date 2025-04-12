package utils

import (
	"flag"
	"testing"
	"time"

	"github.com/pragyandas/hydra/actorsystem"
)

var TestDurationFlag = flag.Duration("test.duration", 5*time.Second, "Duration for the actor communication test")

func SetupTestActorsystem(t *testing.T, id string, conn *TestConnection) *actorsystem.ActorSystem {
	defaultConfig := actorsystem.DefaultConfig()
	config := &actorsystem.Config{
		ID:                    id,
		Region:                "test-region",
		ActorConfig:           defaultConfig.ActorConfig,
		MessageStreamConfig:   defaultConfig.MessageStreamConfig,
		KVConfig:              defaultConfig.KVConfig,
		ActorLivenessKVConfig: defaultConfig.ActorLivenessKVConfig,
		NatsURL:               conn.Server.ClientURL(),
		ControlPlaneConfig:    defaultConfig.ControlPlaneConfig,
		RetryInterval:         500 * time.Millisecond,
	}

	system, err := actorsystem.NewActorSystem(config)
	if err != nil {
		t.Fatalf("Failed to create actor system: %v", err)
	}

	return system
}
