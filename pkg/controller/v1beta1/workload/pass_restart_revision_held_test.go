package workload_test

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Two Instances serve one revision under RecreateInstanceOnPodRestart with
// a retry record that allows one attempt. The first Instance's runner dies
// after Ready and the policy rebuilds it for free. The second Instance's
// runner then dies the same way, so its repair opens and its old set is
// deleted. While that set is still terminating, the first Instance's
// rebuilt runner dies: that set was the record's attempt, so the death
// spends the one attempt and the revision is held. The second Instance's
// old set is gone a few passes later, with the hold standing. The open
// repair reads the record again before its first create and builds no
// set: the row is written Failed with no operation, keeping the failure
// that opened the repair, one InstanceFailed warning names the held
// revision, and no pod of the new set is ever created. The row
// then waits on the record, and a corrected push rebuilds it first, as it
// takes every Instance that serves nothing.
func TestRepairOpenWhenItsRevisionTurnsHeld_BuildsNoSetAndTheRowSaysWhy(t *testing.T) {
	cases := []struct {
		name     string
		strategy workloadtypes.UpdateStrategyType
		multiPod bool
	}{
		{name: "two single-pod Instances under RecreatePod maxUnavailable 1", strategy: workloadtypes.UpdateStrategyRecreatePod},
		{name: "two single-pod Instances under SurgeThenDrain maxSurge 1", strategy: workloadtypes.UpdateStrategySurgeThenDrain},
		{name: "two leader+worker gangs under RecreatePod maxUnavailable 1", strategy: workloadtypes.UpdateStrategyRecreatePod, multiPod: true},
		{name: "two leader+worker gangs under SurgeThenDrain maxSurge 1", strategy: workloadtypes.UpdateStrategySurgeThenDrain, multiPod: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := heldMidRepairHarness(t, tc.multiPod, tc.strategy, 1)
			rebuiltSetNumber := openRepairThenHoldItsRevision(t, h)
			assertRepairStopsOnTheHold(t, h, rebuiltSetNumber)
			assertRowWaitsOnTheRecord(t, h, rebuiltSetNumber)
			assertCorrectedPushRebuildsTheRow(t, h, tc.strategy)
		})
	}
}

// noSetOnTheHeldRevision fails the test the moment a pod of Instance 1 with
// the open repair's set number exists.
func noSetOnTheHeldRevision(t *testing.T, h *recoveryHarness, rebuiltSetNumber int64) func() {
	t.Helper()
	return func() {
		for _, pod := range h.podsOf(1) {
			if inc, ok := query.InstanceIncarnationFromLabels(pod); ok && inc >= rebuiltSetNumber {
				h.dumpState("built on a held revision")
				t.Fatalf("pod %s of Instance 1 was created with set number %d after revision %s was held", pod.Name, inc, h.revV1.Name)
			}
		}
	}
}

// assertRepairStopsOnTheHold drives Instance 1's old set away and holds
// the repair to the stop: the row Failed with no operation and the
// opening failure kept, no pod, one InstanceFailed warning naming the
// held revision.
func assertRepairStopsOnTheHold(t *testing.T, h *recoveryHarness, rebuiltSetNumber int64) {
	t.Helper()
	stopped := func() bool {
		s := h.instance(1)
		return s != nil && s.Phase == workloadtypes.InstancePhaseFailed && s.Operation == nil
	}
	if !h.runWithInvariant(20, stopped, noSetOnTheHeldRevision(t, h, rebuiltSetNumber)) {
		h.dumpState("repair did not stop")
		t.Fatalf("Instance 1's repair did not stop on the held revision: %+v", *h.instance(1))
	}
	row := h.instance(1)
	if row.LastFailure == nil || row.LastFailure.PodName == "" {
		t.Errorf("the stop must keep the failure that opened the repair, got %+v", row.LastFailure)
	}
	if got := len(h.podsOf(1)); got != 0 {
		t.Errorf("Instance 1 has %d live pod(s) after the stop, want none", got)
	}
	var named []string
	for _, w := range h.failedWarnings {
		if strings.HasPrefix(w, "instance=1 ") {
			named = append(named, w)
		}
	}
	if len(named) != 1 || !strings.Contains(named[0], "held after 1 failed attempts") || !strings.Contains(named[0], "builds no set") {
		t.Errorf("InstanceFailed warnings for Instance 1 = %v, want exactly one naming the held revision", named)
	}
}

// assertRowWaitsOnTheRecord holds the stopped row across further passes:
// Failed with no operation, no pod, the record still held.
func assertRowWaitsOnTheRecord(t *testing.T, h *recoveryHarness, rebuiltSetNumber int64) {
	t.Helper()
	noSet := noSetOnTheHeldRevision(t, h, rebuiltSetNumber)
	for i := 0; i < 5; i++ {
		h.step()
		noSet()
		if s := h.instance(1); s.Phase != workloadtypes.InstancePhaseFailed || s.Operation != nil {
			t.Fatalf("pass %d after the stop: Instance 1 reads %+v, want Failed with no operation while the record holds", i+1, *s)
		}
	}
	if b := h.findBlock(h.revV1.Name); b == nil || b.State != workloadtypes.RetryBlockHeld {
		t.Fatalf("the record is %+v, want it still Held", b)
	}
}

// assertCorrectedPushRebuildsTheRow pushes the corrected revision and
// holds both Instances to settling on it; a recreate roll takes the
// podless Instance first, a surge roll orders its replacements by its own
// budget.
func assertCorrectedPushRebuildsTheRow(t *testing.T, h *recoveryHarness, strategy workloadtypes.UpdateStrategyType) {
	t.Helper()
	h.setTarget(h.revFixed, fixedImage)
	firstFixedPod := map[int32]int{}
	pass := 0
	if !h.runWithInvariant(120, func() bool { return h.settledOn(h.revFixed, 2) }, func() {
		pass++
		for _, pod := range h.livePods() {
			if pod.Spec.Containers[0].Image != fixedImage {
				continue
			}
			if idx, ok := query.InstanceIdxFromLabels(pod); ok {
				if _, seen := firstFixedPod[idx]; !seen {
					firstFixedPod[idx] = pass
				}
			}
		}
	}) {
		h.dumpState("corrected push")
		t.Fatalf("the corrected push did not settle both Instances on %s", h.revFixed.Name)
	}
	if strategy == workloadtypes.UpdateStrategyRecreatePod && firstFixedPod[1] > firstFixedPod[0] {
		t.Errorf("the corrected push built Instance 0 at pass %d before the podless Instance 1 at pass %d", firstFixedPod[0], firstFixedPod[1])
	}
	if !servingReady(h, 1, len(h.desired.Runners)) {
		t.Errorf("Instance 1 is not serving its full set on the corrected revision: %+v", *h.instance(1))
	}
}

// The same picture with a record that allows two attempts: the first
// Instance's rebuilt death spends one, so the record is in backoff, not
// Held, when the second Instance's repair reaches its create. A record in
// backoff paces the push's fresh starts; a rebuild of a revision the
// Instance already served is none, so the open repair builds its set and
// the Instance serves again with the attempt's set number.
func TestRepairOpenWhenItsRevisionCountsAnAttempt_StillBuildsItsSet(t *testing.T) {
	for _, multiPod := range []bool{false, true} {
		name := "two single-pod Instances"
		if multiPod {
			name = "two leader+worker gangs"
		}
		t.Run(name, func(t *testing.T) {
			h := heldMidRepairHarness(t, multiPod, workloadtypes.UpdateStrategyRecreatePod, 2)
			runners := len(h.desired.Runners)
			rebuiltSetNumber := openRepairThenCountOneAttempt(t, h)
			if !h.run(40, func() bool { return servingReady(h, 1, runners) }) {
				h.dumpState("rebuild under a record in backoff")
				t.Fatalf("Instance 1 did not serve again while the record was in backoff: %+v", *h.instance(1))
			}
			if got := h.instance(1).Incarnation; got != rebuiltSetNumber {
				t.Errorf("Instance 1 serves with set number %d, want the open repair's %d", got, rebuiltSetNumber)
			}
			for _, w := range h.failedWarnings {
				if strings.HasPrefix(w, "instance=1 ") {
					t.Errorf("Instance 1 was reported failed while its revision was only in backoff: %s", w)
				}
			}
		})
	}
}

// heldMidRepairHarness is two Instances serving v1 under
// RecreateInstanceOnPodRestart with a retry record of maxAttempts
// attempts. Passes are a fixed interval apart and a deleted pod lingers
// Terminating for three of them, so a sibling's crash can land while a
// repair still drains its old set.
func heldMidRepairHarness(t *testing.T, multiPod bool, strategy workloadtypes.UpdateStrategyType, maxAttempts int32) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarnessFor(t, multiPod, constants.MainContainerName)
	h.recorder = record.NewFakeRecorder(512)
	h.replicas = 2
	rebuild := workloadtypes.RestartPolicyRecreateInstance
	h.lifecycle.RestartPolicy = &rebuild
	h.retryPolicy = &workloadtypes.RetryPolicy{MaxAttempts: maxAttempts, InitialDelay: 20 * time.Second, MaxDelay: time.Minute, Multiplier: 2}
	h.stepCap = h.stepFloor
	h.podGrace = 3*h.stepFloor + h.stepFloor/3
	// A death inside the crash window after Ready is the policy's; the
	// window outlives the passes the picture takes between two deaths.
	h.stuckGrace = 10 * time.Minute
	h.useRollingBudget(strategy)
	if !h.run(60, func() bool { return h.settledOn(h.revV1, 2) }) {
		h.dumpState("initial create")
		t.Fatalf("the initial create never settled two Instances on v1")
	}
	return h
}

// openRepairThenHoldItsRevision drives the picture to the hold: Instance
// 0 is rebuilt once for free, Instance 1's repair opens and drains, and
// Instance 0's rebuilt runner dies while that drain is still under way,
// holding the revision. It returns the set number of Instance 1's open
// repair.
func openRepairThenHoldItsRevision(t *testing.T, h *recoveryHarness) int64 {
	t.Helper()
	rebuilt := openRepairThenCountOneAttempt(t, h)
	if b := h.findBlock(h.revV1.Name); b == nil || b.State != workloadtypes.RetryBlockHeld {
		h.dumpState("not held")
		t.Fatalf("the rebuilt set's death left the record %+v, want Held", b)
	}
	return rebuilt
}

// openRepairThenCountOneAttempt is openRepairThenHoldItsRevision up to the
// record counting Instance 0's rebuilt death; what the count makes of the
// record is the policy's.
func openRepairThenCountOneAttempt(t *testing.T, h *recoveryHarness) int64 {
	t.Helper()
	runners := len(h.desired.Runners)

	// Instance 0's runner dies once after Ready: the policy's free rebuild.
	inc0 := h.instance(0).Incarnation
	h.killRunnerOnce(runnerPodOf(t, h, 0))
	if !h.run(40, func() bool {
		s := h.instance(0)
		return s != nil && s.Incarnation > inc0 && servingReady(h, 0, runners)
	}) {
		h.dumpState("free rebuild")
		t.Fatalf("Instance 0 was not rebuilt and serving again after its first death: %+v", *h.instance(0))
	}
	if b := h.findBlock(h.revV1.Name); b != nil {
		t.Fatalf("the first death after Ready counted on the record: %+v", *b)
	}

	// Instance 1's runner dies once: its repair opens and its old set is
	// deleted, lingering Terminating.
	inc1 := h.instance(1).Incarnation
	h.killRunnerOnce(runnerPodOf(t, h, 1))
	if !h.run(20, func() bool {
		s := h.instance(1)
		return s != nil && s.Phase == workloadtypes.InstancePhaseRestarting && s.Incarnation > inc1 &&
			len(h.podsOf(1)) == 0 && terminatingPodsOf(t, h, 1) > 0
	}) {
		h.dumpState("repair open")
		t.Fatalf("Instance 1's repair did not open and delete its old set: %+v", *h.instance(1))
	}
	rebuilt := h.instance(1).Incarnation

	// While Instance 1's old set still terminates, Instance 0's rebuilt
	// runner dies: that set was the record's attempt, so the death counts.
	h.killRunnerOnce(runnerPodOf(t, h, 0))
	if !h.run(6, func() bool { return h.findBlock(h.revV1.Name) != nil }) {
		h.dumpState("count")
		t.Fatalf("the rebuilt set's death did not count on the record")
	}
	if terminatingPodsOf(t, h, 1) == 0 {
		h.dumpState("count after the drain")
		t.Fatalf("the record counted after Instance 1's old set was gone; the picture needs a longer pod grace")
	}
	return rebuilt
}

// runnerPodOf is the pod whose runner the picture stops: the leader of a
// gang, the only pod of a single-pod Instance.
func runnerPodOf(t *testing.T, h *recoveryHarness, idx int32) *corev1.Pod {
	t.Helper()
	for _, pod := range h.podsOf(idx) {
		if runner := pod.Labels[query.LabelRunner]; runner == "leader" || runner == "default" {
			return pod
		}
	}
	t.Fatalf("Instance %d has no runner pod to stop", idx)
	return nil
}

// terminatingPodsOf counts the pods of Instance idx that are on their way
// out: deleted, and still held by the kubelet model's grace.
func terminatingPodsOf(t *testing.T, h *recoveryHarness, idx int32) int {
	t.Helper()
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	n := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp == nil {
			continue
		}
		if i, ok := query.InstanceIdxFromLabels(pod); ok && i == idx {
			n++
		}
	}
	return n
}
