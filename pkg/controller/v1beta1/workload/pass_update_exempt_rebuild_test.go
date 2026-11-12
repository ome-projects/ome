package workload_test

import (
	"slices"
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// batchStepHoldStandIn is what the batch step tells every start of a side
// whose peer's first batch has not served; here it stands in for the whole
// gate stack a rollout group puts in front of a fresh start.
const batchStepHoldStandIn = "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving)"

// heldGate is a coordination gate that admits the first admit starts and
// holds every later one under the Ratio gate with the batch step's words,
// until the test opens it.
type heldGate struct {
	admit int
	open  bool
}

func (g *heldGate) answer(string, workloadtypes.UpdateStrategyType, int32, int32) (bool, workloadtypes.RolloutHoldGate, string) {
	if g.open {
		return true, "", ""
	}
	if g.admit > 0 {
		g.admit--
		return true, "", ""
	}
	return false, workloadtypes.RolloutHoldGateRatio, batchStepHoldStandIn
}

// TestReconcile_GangShortOfAWorker_RollRebuildsItWhileTheGateHoldsEveryStart:
// three leader+worker gangs roll by recreate with room for two rebuilds at
// a time on the Component's own budget, behind a rollout group's gate that
// holds every fresh start the way the batch step holds a side whose
// peer's first batch has not served. A gang that loses its worker while
// the gate holds is already outside the group's serving count, so the roll
// rebuilds it onto the new revision without asking the gate: alone while
// nothing else moves, and beside the one recreate the gate admitted before
// it closed. The gangs the gate holds stay on version 1 and in rotation,
// no repair is recorded under either restart policy, and the roll finishes
// once the gate opens.
func TestReconcile_GangShortOfAWorker_RollRebuildsItWhileTheGateHoldsEveryStart(t *testing.T) {
	const replicas, short = 3, int32(2)
	for _, policy := range []workloadtypes.RestartPolicy{workloadtypes.RestartPolicyRecreateInstance, workloadtypes.RestartPolicyNone} {
		for _, tc := range []struct {
			name    string
			admit   int     // starts the gate admits before it holds every start
			rolling []int32 // the gangs whose recreate is open when the worker is lost
		}{
			{name: "every start held", admit: 0},
			{name: "beside an open recreate", admit: 1, rolling: []int32{0}},
		} {
			t.Run(string(policy)+"/"+tc.name, func(t *testing.T) {
				h := offTargetCrashHarness(t, true, workloadtypes.UpdateStrategyRecreatePod, 0, 2, policy, replicas)
				gate := &heldGate{admit: tc.admit}
				h.gate = gate.answer
				h.events = nil
				h.setTarget(h.revFixed, fixedImage)
				h.step()
				requireHeldBehindTheGate(t, h, tc.rolling)
				breakInstance(t, h, short, true, gangMemberLost)
				requireShortGangRebuiltUnasked(t, h, short, tc.rolling)
				gate.open = true
				if !h.run(80, func() bool { return h.settledOn(h.revFixed, replicas) }) {
					h.dumpState("after the gate opened")
					t.Fatalf("the roll never finished once the gate admitted the serving gangs")
				}
				if h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
					t.Fatalf("a restart was recorded for an Instance the roll owned: %v", h.events)
				}
			})
		}
	}
}

// requireHeldBehindTheGate reads the first pass of the push: the gangs in
// rolling have their recreate open and every other gang is untouched on
// version 1. With no start admitted the pass publishes the gate's words
// as its hold; with one admitted it publishes none, since an Instance
// made progress.
func requireHeldBehindTheGate(t *testing.T, h *recoveryHarness, rolling []int32) {
	t.Helper()
	for idx := int32(0); idx < h.replicas; idx++ {
		row := h.instance(idx)
		updating := row != nil && row.Phase == workloadtypes.InstancePhaseUpdating
		if want := slices.Contains(rolling, idx); updating != want {
			h.dumpState("after the push")
			t.Fatalf("instance %d updating = %v, want %v: the gate admits %d start(s) and holds the rest", idx, updating, want, len(rolling))
		}
	}
	hold := h.lastHold()
	if len(rolling) > 0 {
		if hold != nil {
			t.Fatalf("hold = %+v, want none: a start was admitted in the pass", hold)
		}
		return
	}
	if hold == nil || hold.Gate != workloadtypes.RolloutHoldGateRatio || hold.Reason != batchStepHoldStandIn {
		t.Fatalf("hold = %+v, want %q under the Ratio gate", hold, batchStepHoldStandIn)
	}
}

// requireShortGangRebuiltUnasked drives the passes after the worker's loss
// until the short gang serves the fixed revision, checking after every
// pass that no repair opened, that every gang the gate holds stays on
// version 1 and in rotation, and that every pod created after the loss
// runs the fix. With a recreate open beside it, the two are in flight at
// once.
func requireShortGangRebuiltUnasked(t *testing.T, h *recoveryHarness, short int32, rolling []int32) {
	t.Helper()
	lostAt := h.clk.Now()
	besideOpen := len(rolling) == 0
	invariant := func() {
		requireHeldGangsUntouched(t, h, short, rolling)
		requirePodsAfterTheLossRunTheFix(t, h, lostAt)
		besideOpen = besideOpen || rebuildOpenBesideAPeer(h, short, rolling)
	}
	done := func() bool { return slices.Contains(h.landedOn(h.revFixed), short) && h.instanceServes(short) }
	if !h.runWithInvariant(60, done, invariant) {
		h.dumpState("after the loss")
		t.Fatalf("the gang short of its worker was never rebuilt onto the fixed revision while the gate holds every start: last hold %+v", h.lastHold())
	}
	if !besideOpen {
		t.Fatalf("the short gang's rebuild never stood beside the open recreate the gate admitted before it closed")
	}
}

// requireHeldGangsUntouched fails when a repair opened, or when a gang
// other than the short one and the ones already rolling left version 1
// or rotation: the gate holds every start of those.
func requireHeldGangsUntouched(t *testing.T, h *recoveryHarness, short int32, rolling []int32) {
	t.Helper()
	if h.repairInFlight() {
		h.dumpState("repair opened")
		t.Fatalf("a repair opened on an Instance whose revision is not the roll target")
	}
	for idx := int32(0); idx < h.replicas; idx++ {
		if idx == short || slices.Contains(rolling, idx) {
			continue
		}
		if !h.untouchedOn(idx, h.revV1) || !h.instanceServes(idx) {
			h.dumpState("held gang moved")
			t.Fatalf("instance %d was taken by the roll while the gate holds every start", idx)
		}
	}
}

// requirePodsAfterTheLossRunTheFix fails when a pod created after lostAt
// runs anything but the fixed revision: the roll owns the rebuild.
func requirePodsAfterTheLossRunTheFix(t *testing.T, h *recoveryHarness, lostAt time.Time) {
	t.Helper()
	fixedHash := query.RevisionOf(h.revFixed).Hash()
	for _, pod := range h.livePods() {
		if pod.CreationTimestamp.Time.After(lostAt) && pod.Labels[query.LabelRevisionHash] != fixedHash {
			t.Fatalf("pod %s was created on %s after the loss; the roll owns the rebuild", pod.Name, pod.Spec.Containers[0].Image)
		}
	}
}

// rebuildOpenBesideAPeer reports the short gang's recreate open in the
// same pass as one of the rolling gangs' recreates.
func rebuildOpenBesideAPeer(h *recoveryHarness, short int32, rolling []int32) bool {
	if row := h.instance(short); row == nil || row.Phase != workloadtypes.InstancePhaseUpdating {
		return false
	}
	for _, idx := range rolling {
		if peer := h.instance(idx); peer != nil && peer.Phase == workloadtypes.InstancePhaseUpdating {
			return true
		}
	}
	return false
}
