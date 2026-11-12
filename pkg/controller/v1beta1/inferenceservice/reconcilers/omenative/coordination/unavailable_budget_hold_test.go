package coordination

import (
	"strings"
	"testing"

	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The text of the group's unavailable budget hold. The budget counts an
// Instance as out of rotation whenever the replica's counters do not count
// it as serving, and a scale-up mid-roll makes every Instance it has not
// brought up yet such an Instance. The hold names them by ordinal and says
// why, so the operator reads which Instances spend the budget instead of a
// count that nothing visible accounts for.

// unavailableHold asks the group's unavailable budget gate alone, off the
// engine's replica as given, about one more drain-first start with inFlight
// Instances already taken this pass.
func unavailableHold(t *testing.T, isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, inFlight int32) (bool, string) {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(ir).Build()
	return ResolveGateContext(t.Context(), c, isvc, v1beta1.EngineComponent).CheckUnavailability(inFlight)
}

// scaledUpPrefill is the prefill side of the reference roll scaled from 12
// to 24 between two batches: 4 Instances on version 2, 8 still on version 1,
// and the 12 the scale-up adds published Creating when published is true,
// else not yet published at all, the replica still counting 12 rows.
func scaledUpPrefill(published bool) *v1beta1.InferenceReplica {
	m := batchMemberFixture{desired: 24, step: "10%", strategy: v1beta1.UpdateStrategyRecreatePod, promoted: 4, creating: 12}
	ir := batchReplica(v1beta1.EngineComponent, m)
	if !published {
		ir.Status.InstanceStatuses = ir.Status.InstanceStatuses[:12]
		ir.Status.Replicas = 12
	}
	return ir
}

// TestCheckUnavailability_HoldNamesTheInstancesAScaleUpIsStillCreating is
// the operator's story: 12 prefill at 10 percent beside 8 decode under the
// ratio rule, between two batches with 4 prefill on version 2, and the
// prefill side scaled to 24. The group names no budget, so its unavailable
// budget is a quarter of 24, 6, and the 12 Instances the scale-up has not
// brought up yet already count as out of rotation. The hold on the prefill
// side's next start says which ones: the Instances 12 to 23, still being
// created, whether the replica has published their rows or not yet. An
// Instance down beside them is named with its own reason, and the budget's
// numbers stay what they were.
func TestCheckUnavailability_HoldNamesTheInstancesAScaleUpIsStillCreating(t *testing.T) {
	isvc := batchService(batchGroupFixture{tolerance: ptr.To(int32(20))}, nil)
	const creating = "unavailable budget 6 exhausted (current 12, in-flight 0, would become 13); counted as unavailable: Instances 12 to 23 still being created"
	t.Run("the-new-rows-published-creating", func(t *testing.T) {
		if allowed, reason := unavailableHold(t, isvc, scaledUpPrefill(true), 0); allowed || reason != creating {
			t.Fatalf("allowed=%v reason=%q, want the hold %q", allowed, reason, creating)
		}
	})
	t.Run("the-new-rows-not-yet-published", func(t *testing.T) {
		if allowed, reason := unavailableHold(t, isvc, scaledUpPrefill(false), 0); allowed || reason != creating {
			t.Fatalf("allowed=%v reason=%q, want the hold %q", allowed, reason, creating)
		}
	})
	t.Run("an-instance-failed-beside-the-scale-up", func(t *testing.T) {
		ir := scaledUpPrefill(true)
		ir.Status.InstanceStatuses[7].Phase, ir.Status.InstanceStatuses[7].PodCount, ir.Status.InstanceStatuses[7].ServingPodCount = v1beta1.OMENativeInstanceFailed, 0, 0
		ir.Status.ServingReplicas--
		const want = "unavailable budget 6 exhausted (current 13, in-flight 0, would become 14); counted as unavailable: Instance 7 Failed, Instances 12 to 23 still being created"
		if allowed, reason := unavailableHold(t, isvc, ir, 0); allowed || reason != want {
			t.Fatalf("allowed=%v reason=%q, want the hold %q", allowed, reason, want)
		}
	})
	t.Run("the-gate-stack-holds-the-prefill-side-with-the-same-text", func(t *testing.T) {
		members := map[v1beta1.ComponentType]batchMemberFixture{
			v1beta1.EngineComponent:  {desired: 24, step: "10%", strategy: v1beta1.UpdateStrategyRecreatePod, promoted: 4, creating: 12},
			v1beta1.DecoderComponent: {desired: 8, step: "10%", strategy: v1beta1.UpdateStrategyRecreatePod, promoted: 1},
		}
		svc := batchService(batchGroupFixture{tolerance: ptr.To(int32(20))}, members)
		allowed, gate, reason := batchGate(t, svc, batchClient(t, members), v1beta1.EngineComponent, workloadtypes.UpdateStrategyRecreatePod, 0)
		if allowed || gate != v1beta1.RolloutHoldGateBudget || reason != creating {
			t.Fatalf("allowed=%v gate=%q reason=%q, want the Budget hold %q", allowed, gate, reason, creating)
		}
	})
}

// TestCheckUnavailability_HoldSpentByThisPassNamesNoInstance keeps the
// text a hold had when nothing is down: 4 engine gangs under a budget of
// one, the first drain of the pass taken and the second asked. Every
// Instance serves, so the hold carries the numbers alone, as it did.
func TestCheckUnavailability_HoldSpentByThisPassNamesNoInstance(t *testing.T) {
	const want = "unavailable budget 1 exhausted (current 0, in-flight 1, would become 2)"
	allowed, reason := unavailableHold(t, gangRollService(v1beta1.CoordinationPolicyBlueGreen), drainFirstGangRoll(gangRollFixture{desired: 4}, false), 1)
	if allowed || reason != want {
		t.Fatalf("allowed=%v reason=%q, want exactly %q", allowed, reason, want)
	}
}

// TestCheckUnavailability_HoldNamesEachInstanceDownWithItsReason reads the
// wording of every reason off one replica of 9 single-pod Instances: one
// whose drain is in flight, one restarting, one Failed, one Ready with its
// only pod gone, one whose lost pod is being built again (Pending on the
// revision it ran), and two the spec asks for that have no row yet; the two
// that serve are not named. A gang counts as serving only with its whole
// pod set in rotation, so a gang of two with one pod serving is named.
func TestCheckUnavailability_HoldNamesEachInstanceDownWithItsReason(t *testing.T) {
	isvc := batchService(batchGroupFixture{maxUnavailable: "1", components: []v1beta1.ComponentType{v1beta1.EngineComponent}}, nil)
	ir := batchReplica(v1beta1.EngineComponent, batchMemberFixture{desired: 9, strategy: v1beta1.UpdateStrategyRecreatePod, promoted: 2, inFlight: 1})
	rows := ir.Status.InstanceStatuses
	rows[3].Phase, rows[3].PodCount, rows[3].ServingPodCount = v1beta1.OMENativeInstanceRestarting, 1, 0
	rows[4].Phase, rows[4].PodCount, rows[4].ServingPodCount = v1beta1.OMENativeInstanceFailed, 0, 0
	rows[5].PodCount, rows[5].ServingPodCount = 0, 0
	rows[6].Phase, rows[6].PodCount, rows[6].ServingPodCount = v1beta1.OMENativeInstancePending, 0, 0
	ir.Status.InstanceStatuses = rows[:7]
	ir.Status.Replicas = 7
	ir.Status.ServingReplicas = 2
	const want = "unavailable budget 1 exhausted (current 7, in-flight 0, would become 8); counted as unavailable: Instance 2 being updated, Instance 3 restarting, Instance 4 Failed, Instance 5 not serving, Instance 6 being rebuilt, Instances 7, 8 still being created"
	if allowed, reason := unavailableHold(t, isvc, ir, 0); allowed || reason != want {
		t.Fatalf("allowed=%v reason=%q, want the hold %q", allowed, reason, want)
	}
	t.Run("a-gang-with-one-pod-serving-is-named", func(t *testing.T) {
		gang := batchReplica(v1beta1.EngineComponent, batchMemberFixture{desired: 2, strategy: v1beta1.UpdateStrategyRecreatePod, gang: true})
		gang.Status.InstanceStatuses[0].PodCount, gang.Status.InstanceStatuses[0].ServingPodCount = 3, 3
		gang.Status.InstanceStatuses[1].PodCount, gang.Status.InstanceStatuses[1].ServingPodCount = 3, 1
		gang.Status.ServingReplicas = 1
		allowed, reason := unavailableHold(t, isvc, gang, 0)
		if allowed || !strings.HasSuffix(reason, "; counted as unavailable: Instance 1 not serving") {
			t.Fatalf("allowed=%v reason=%q, want the gang with one of three pods serving named", allowed, reason)
		}
	})
}

// TestOrdinalSpans holds the list the clause writes: single ordinals, a
// pair, and three or more in a row as a span.
func TestOrdinalSpans(t *testing.T) {
	cases := []struct {
		ordinals []int32
		want     string
	}{
		{[]int32{3}, "3"},
		{[]int32{3, 7}, "3, 7"},
		{[]int32{12, 13}, "12, 13"},
		{[]int32{12, 13, 14}, "12 to 14"},
		{[]int32{3, 12, 13, 14, 15, 20}, "3, 12 to 15, 20"},
	}
	for _, tc := range cases {
		if got := ordinalSpans(tc.ordinals); got != tc.want {
			t.Errorf("ordinalSpans(%v) = %q, want %q", tc.ordinals, got, tc.want)
		}
	}
}
