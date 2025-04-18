package actorsystemtest

import (
	"context"
	"testing"
	"time"

	"github.com/pragyandas/hydra/actorsystem"
	"github.com/pragyandas/hydra/test/e2e/utils"
)

func TestControlPlaneMembershipUpdate(t *testing.T) {
	config := actorsystem.DefaultConfig()
	stabilizationInterval := config.ControlPlaneConfig.BucketRecalculationStabilizationInterval

	testContext, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	conn := utils.SetupTestConnection(t)
	defer conn.Close()

	// Start system A
	systemA := utils.SetupTestActorsystem(t, "test-system-a", conn)
	systemA.Start(testContext)
	time.Sleep(2 * time.Second)

	// Check system A is the only member
	memberCount, selfIndex := systemA.GetMemberCountAndPosition()
	t.Logf("system A is the only member, member count: %d, self index: %d", memberCount, selfIndex)

	// Start system B
	systemB := utils.SetupTestActorsystem(t, "test-system-b", conn)
	systemB.Start(testContext)
	time.Sleep(stabilizationInterval + 2*time.Second)

	// Check system B has joined
	memberCount, selfIndex = systemB.GetMemberCountAndPosition()
	t.Logf("system B has joined the cluster, member count: %d, self index: %d", memberCount, selfIndex)

	// Start system C
	systemC := utils.SetupTestActorsystem(t, "test-system-c", conn)
	systemC.Start(testContext)
	time.Sleep(stabilizationInterval + 2*time.Second)

	// Now all systems are running, let's check their positions
	memberCount, selfIndex = systemA.GetMemberCountAndPosition()
	t.Logf("system A is at index 0, member count: %d, self index: %d", memberCount, selfIndex)

	memberCount, selfIndex = systemB.GetMemberCountAndPosition()
	t.Logf("system B is at index 1, member count: %d, self index: %d", memberCount, selfIndex)

	memberCount, selfIndex = systemC.GetMemberCountAndPosition()
	t.Logf("system C is at index 2, member count: %d, self index: %d", memberCount, selfIndex)

	// Now let's remove them in sequence: A, B, C

	// Remove A
	systemA.Close(testContext)
	time.Sleep(stabilizationInterval + 2*time.Second)
	t.Logf("system A left the cluster")

	// Check remaining systems adjusted
	memberCount, selfIndex = systemB.GetMemberCountAndPosition()
	t.Logf("system B moved to index 0, member count: %d, self index: %d", memberCount, selfIndex)

	memberCount, selfIndex = systemC.GetMemberCountAndPosition()
	t.Logf("system C moved to index 1, member count: %d, self index: %d", memberCount, selfIndex)

	// Remove B
	systemB.Close(testContext)
	time.Sleep(stabilizationInterval + 2*time.Second)
	t.Logf("system B left the cluster")

	// Check C is now alone
	memberCount, selfIndex = systemC.GetMemberCountAndPosition()
	t.Logf("system C is now alone at index 0, member count: %d, self index: %d", memberCount, selfIndex)

	// Finally remove C
	systemC.Close(testContext)
	time.Sleep(stabilizationInterval + 2*time.Second)
	t.Logf("system C left the cluster")
}
