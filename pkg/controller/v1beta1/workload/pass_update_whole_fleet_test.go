package workload_test

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestRecreateWithTheWholeFleetUnavailable_ReplacesEveryInstanceAtOnce: six
// single-pod Instances serve version 1 under RecreatePod with every Instance
// allowed down at once, the strategy a disruptive canary force projects.
// The operator pushes version 2. Every Instance is replaced at the same time,
// so no pass leaves one waiting on another's budget slot, and the fleet then
// serves version 2 in full.
func TestRecreateWithTheWholeFleetUnavailable_ReplacesEveryInstanceAtOnce(t *testing.T) {
	const replicas = 6
	h := offTargetCrashHarness(t, false, workloadtypes.UpdateStrategyRecreatePod, 0, 1, workloadtypes.RestartPolicyNone, replicas)
	all := intstr.FromString("100%")
	h.lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable = &all

	replacing := func() int {
		n := 0
		for i := int32(0); i < replicas; i++ {
			row := h.instance(i)
			if row != nil && row.Operation != nil && row.Operation.Type == workloadtypes.InstanceOperationUpdate {
				n++
			}
		}
		return n
	}
	most := 0
	h.setTarget(h.revFixed, fixedImage)
	settled := h.run(60, func() bool {
		most = max(most, replacing())
		return h.settledOn(h.revFixed, replicas)
	})
	if most != replicas {
		h.dumpState("whole fleet")
		t.Fatalf("every Instance must be replaced at once under a 100%% budget, but at most %d of %d were at the same time; last hold %+v", most, replicas, h.lastHold())
	}
	if !settled {
		h.dumpState("whole fleet")
		t.Fatalf("the fleet never settled on version 2")
	}
}
