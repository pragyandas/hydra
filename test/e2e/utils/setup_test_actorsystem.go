package utils

import (
	"flag"
	"testing"
	"time"

	"github.com/pragyandas/hydra/actorsystem"
)

var TestDurationFlag = flag.Duration("test.duration", 5*time.Second, "Duration for the actor communication test")

func SetupTestActorsystem(t *testing.T, id string, conn *TestConnection, config *actorsystem.Config) *actorsystem.ActorSystem {
	var defaultConfig *actorsystem.Config
	if config != nil {
		defaultConfig = config
	} else {
		defaultConfig = actorsystem.DefaultConfig()
	}

	defaultConfig.ControlPlaneConfig.BucketRecalculationStabilizationInterval = 5 * time.Second
	config = &actorsystem.Config{
		ID:                    id,
		Region:                "test-region",
		ActorConfig:           defaultConfig.ActorConfig,
		MessageStreamConfig:   defaultConfig.MessageStreamConfig,
		KVConfig:              defaultConfig.KVConfig,
		ActorLivenessKVConfig: defaultConfig.ActorLivenessKVConfig,
		ControlPlaneConfig:    defaultConfig.ControlPlaneConfig,
		RetryInterval:         500 * time.Millisecond,
	}

	system, err := actorsystem.NewActorSystem(config).WithNATSURL(conn.Server.ClientURL())
	if err != nil {
		t.Fatalf("Failed to create actor system: %v", err)
	}

	return system
}
