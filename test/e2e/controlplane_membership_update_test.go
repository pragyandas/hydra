package actorsystemtest

import (
	"context"
	"testing"
	"time"

	"github.com/pragyandas/hydra/actorsystem"
	"github.com/pragyandas/hydra/test/e2e/utils"
	"github.com/stretchr/testify/assert"
)

func TestControlPlaneMembershipBucketUpdate(t *testing.T) {
	config := actorsystem.DefaultConfig()
	stabilizationInterval := config.ControlPlaneConfig.BucketRecalculationStabilizationInterval

	config.ControlPlaneConfig.BucketManagerConfig.NumBuckets = 16

	testContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn := utils.SetupTestConnection(t)
	defer conn.Close()

	// Start system A
	systemA := utils.SetupTestActorsystem(t, "test-system-a", conn, config)
	systemA.Start(testContext)
	time.Sleep(stabilizationInterval)

	// Check system A is the only member
	memberCount, selfIndex := systemA.GetMemberCountAndPosition()
	assert.Equal(t, 1, memberCount)
	assert.Equal(t, 0, selfIndex)
	t.Logf("system A is the only member, member count: %d, self index: %d", memberCount, selfIndex)

	// Start system B
	systemB := utils.SetupTestActorsystem(t, "test-system-b", conn, config)
	systemB.Start(testContext)
	time.Sleep(stabilizationInterval)

	// Check system B has joined
	memberCount, selfIndex = systemB.GetMemberCountAndPosition()
	assert.Equal(t, 2, memberCount)
	assert.Equal(t, 1, selfIndex)
	t.Logf("system B has joined the cluster, member count: %d, self index: %d", memberCount, selfIndex)

	// Start system C
	systemC := utils.SetupTestActorsystem(t, "test-system-c", conn, config)
	systemC.Start(testContext)
	time.Sleep(stabilizationInterval)

	// Check system C has joined
	memberCount, selfIndex = systemC.GetMemberCountAndPosition()
	assert.Equal(t, 3, memberCount)
	assert.Equal(t, 2, selfIndex)
	t.Logf("system C has joined the cluster, member count: %d, self index: %d", memberCount, selfIndex)

	// Now all systems are running, let's check their positions
	memberCount, selfIndex = systemA.GetMemberCountAndPosition()
	assert.Equal(t, 3, memberCount)
	assert.Equal(t, 0, selfIndex)
	t.Logf("system A is at index 0, member count: %d, self index: %d", memberCount, selfIndex)

	memberCount, selfIndex = systemB.GetMemberCountAndPosition()
	assert.Equal(t, 3, memberCount)
	assert.Equal(t, 1, selfIndex)
	t.Logf("system B is at index 1, member count: %d, self index: %d", memberCount, selfIndex)

	memberCount, selfIndex = systemC.GetMemberCountAndPosition()
	assert.Equal(t, 3, memberCount)
	assert.Equal(t, 2, selfIndex)
	t.Logf("system C is at index 2, member count: %d, self index: %d", memberCount, selfIndex)

	time.Sleep(15 * time.Second) // Wait for buckets to be distributed

	bucketsOwned := systemA.GetOwnedBuckets()
	assert.Equal(t, 6, len(bucketsOwned))
	t.Logf("system A owns buckets: %v", bucketsOwned)

	bucketsOwned = systemB.GetOwnedBuckets()
	assert.Equal(t, 5, len(bucketsOwned))
	t.Logf("system B owns buckets: %v", bucketsOwned)

	bucketsOwned = systemC.GetOwnedBuckets()
	assert.Equal(t, 5, len(bucketsOwned))
	t.Logf("system C owns buckets: %v", bucketsOwned)

	// Systems leave the cluster in sequence: C, B, A

	systemC.Close(testContext)
	time.Sleep(stabilizationInterval + 10*time.Second) // Wait for buckets to be redistributed
	t.Logf("system C left the cluster")

	memberCount, selfIndex = systemA.GetMemberCountAndPosition()
	assert.Equal(t, 2, memberCount)
	assert.Equal(t, 0, selfIndex)
	t.Logf("system A is at index 0, member count: %d, self index: %d", memberCount, selfIndex)

	memberCount, selfIndex = systemB.GetMemberCountAndPosition()
	assert.Equal(t, 2, memberCount)
	assert.Equal(t, 1, selfIndex)
	t.Logf("system B is at index 1, member count: %d, self index: %d", memberCount, selfIndex)

	bucketsOwned = systemA.GetOwnedBuckets()
	assert.Equal(t, 8, len(bucketsOwned))
	t.Logf("system A owns buckets: %v", bucketsOwned)

	bucketsOwned = systemB.GetOwnedBuckets()
	assert.Equal(t, 8, len(bucketsOwned))
	t.Logf("system B owns buckets: %v", bucketsOwned)

	systemB.Close(testContext)
	time.Sleep(stabilizationInterval + 10*time.Second) // Wait for buckets to be redistributed
	t.Logf("system B left the cluster")

	memberCount, selfIndex = systemA.GetMemberCountAndPosition()
	assert.Equal(t, 1, memberCount)
	assert.Equal(t, 0, selfIndex)
	t.Logf("system A is at index 0, member count: %d, self index: %d", memberCount, selfIndex)

	bucketsOwned = systemA.GetOwnedBuckets()
	assert.Equal(t, 16, len(bucketsOwned))
	t.Logf("system A owns buckets: %v", bucketsOwned)

	systemA.Close(testContext)
	time.Sleep(10 * time.Second)
	t.Logf("system A left the cluster")
}
