package workload_test

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestReconcile_GangShortOfAWorker_RollSurgesItWhileTheGateHoldsEveryStart:
// three leader+worker gangs roll under SurgeThenDrain behind a rollout
// group's gate that holds every fresh start the way the batch step holds
// a side whose peer's first batch has not served. A gang that loses its
// worker while the gate holds serves nothing and is already outside the
// group's serving count, so the roll surges its replacement onto the new
// revision without asking the gate: alone while nothing else moves,
// beside the one surge the gate admitted before it closed, and, with the
// Component's own surge budget at one, only once that surge has handed
// over, since the budget still paces it. The gangs the gate holds stay on
// version 1 and in rotation, no repair is recorded under either restart
// policy, and the roll finishes once the gate opens.
func TestReconcile_GangShortOfAWorker_RollSurgesItWhileTheGateHoldsEveryStart(t *testing.T) {
	const replicas, short = 3, int32(2)
	for _, policy := range []workloadtypes.RestartPolicy{workloadtypes.RestartPolicyRecreateInstance, workloadtypes.RestartPolicyNone} {
		for _, tc := range []struct {
			name    string
			admit   int     // starts the gate admits before it holds every start
			surge   int32   // the Component's own surge budget
			rolling []int32 // the gangs whose surge is open when the worker is lost
		}{
			{name: "every start held", admit: 0, surge: 2},
			{name: "beside an open surge", admit: 1, surge: 2, rolling: []int32{0}},
			{name: "behind the Component's own surge budget", admit: 1, surge: 1, rolling: []int32{0}},
		} {
			t.Run(string(policy)+"/"+tc.name, func(t *testing.T) {
				h := offTargetCrashHarness(t, true, workloadtypes.UpdateStrategySurgeThenDrain, tc.surge, 0, policy, replicas)
				gate := &heldGate{admit: tc.admit}
				h.gate = gate.answer
				h.events = nil
				h.setTarget(h.revFixed, fixedImage)
				h.step()
				requireHeldBehindTheGate(t, h, tc.rolling)
				var release func()
				if tc.surge == 1 {
					release = keepTheOpenSurgeUnready(t, h, tc.rolling[0])
				}
				breakInstance(t, h, short, true, gangMemberLost)
				requireShortGangSurgedUnasked(t, h, short, tc.rolling, tc.surge, release)
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

// requireShortGangSurgedUnasked drives the passes after the worker's loss
// until the short gang's replacement serves the fixed revision under its
// own index and the short source is gone, checking after every pass that
// no repair opened, that every gang the gate holds stays on version 1 and
// in rotation, and that every pod created after the loss runs the fix.
// With room for two surges the short gang's surge stands beside the one
// the gate admitted before it closed; with room for one it waits for as
// long as that surge's replacement stays unready, the pass publishing no
// hold while an attempt is in flight, and opens once release lets the
// replacement go ready and the surge hands over. The Component's surge
// budget holds after every pass.
func requireShortGangSurgedUnasked(t *testing.T, h *recoveryHarness, short int32, rolling []int32, surgeBudget int32, release func()) {
	t.Helper()
	lostAt := h.clk.Now()
	replacement := int32(-1)
	budgetOfOne := surgeBudget == 1 && len(rolling) > 0
	besideOpen := budgetOfOne || len(rolling) == 0
	waits := 0
	invariant := func() {
		requireHeldGangsUntouched(t, h, short, rolling)
		requirePodsAfterTheLossRunTheFix(t, h, lostAt)
		rollStaysWithinBudget(t, h, workloadtypes.UpdateStrategySurgeThenDrain, surgeBudget, 0)
		if row := h.instance(short); row != nil && row.Operation != nil && row.Operation.SurgeIndex != nil {
			replacement = *row.Operation.SurgeIndex
		}
		besideOpen = besideOpen || rebuildOpenBesideAPeer(h, short, rolling)
		if budgetOfOne && shortGangWaitsOnTheSurgeBudget(t, h, short, rolling[0]) {
			waits++
			if waits == budgetWaitPasses && release != nil {
				release()
			}
		}
	}
	done := func() bool {
		return replacement >= 0 && h.instance(short) == nil && h.untouchedOn(replacement, h.revFixed) && h.instanceServes(replacement)
	}
	if !h.runWithInvariant(80, done, invariant) {
		h.dumpState("after the loss")
		t.Fatalf("the gang short of its worker was never replaced on the fixed revision while the gate holds every start: last hold %+v", h.lastHold())
	}
	if !besideOpen {
		t.Fatalf("the short gang's surge never stood beside the open surge the gate admitted before it closed")
	}
	if budgetOfOne && waits < budgetWaitPasses {
		t.Fatalf("the short gang waited %d pass(es) on the Component's own surge budget behind the open surge, want %d", waits, budgetWaitPasses)
	}
}

// budgetWaitPasses is how many passes the short gang stands behind the
// surge the gate admitted before it, that surge's replacement kept
// unready, before the replacement is let go ready.
const budgetWaitPasses = 3

// keepTheOpenSurgeUnready waits for the replacement gang of the surge open
// on idx to be created and fails readiness on its pods, so the surge stays
// in flight and keeps a Component surge budget of one spent. The returned
// func lets the replacement go ready.
func keepTheOpenSurgeUnready(t *testing.T, h *recoveryHarness, idx int32) func() {
	t.Helper()
	row := h.instance(idx)
	if row == nil || row.Operation == nil || row.Operation.SurgeIndex == nil {
		t.Fatalf("instance %d has no surge open to keep in flight", idx)
	}
	var pods []*corev1.Pod
	if !h.run(6, func() bool { pods = h.podsOf(*row.Operation.SurgeIndex); return len(pods) > 0 }) {
		h.dumpState("waiting for the replacement gang")
		t.Fatalf("the replacement gang of instance %d was never created", idx)
	}
	for _, pod := range pods {
		h.readinessFails[pod.UID] = true
	}
	return func() {
		for _, pod := range pods {
			delete(h.readinessFails, pod.UID)
		}
	}
}

// shortGangWaitsOnTheSurgeBudget reads one pass under a surge budget of
// one and reports whether the surge admitted before the loss is still
// open on its version 1 source while the short gang has no surge of its
// own. A surge of the short gang beside the open one fails the test. Once
// that source is gone its index is free for a later replacement, so the
// source is told apart by the revision it runs.
func shortGangWaitsOnTheSurgeBudget(t *testing.T, h *recoveryHarness, short, open int32) bool {
	t.Helper()
	peer := h.instance(open)
	if peer == nil || peer.Operation == nil || peer.RunningRevision != h.revV1.Name {
		return false
	}
	if row := h.instance(short); row != nil && row.Operation != nil {
		h.dumpState("surge beside the open one")
		t.Fatalf("the short gang's surge opened beside gang %d's while the Component's surge budget is one", open)
	}
	return true
}
