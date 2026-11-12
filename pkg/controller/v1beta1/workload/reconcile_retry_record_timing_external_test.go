package workload_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The retry record is one per revision for the whole Component, and it
// decides when the next attempt at that revision may start. The tests in
// this file lock two of its readings. Both are decided behaviour: a change
// to either needs an explicit decision first, not a quieter test.
//
//   - Another Instance promoting at the revision removes the record. An
//     attempt the escalation stopped behind that record starts again on
//     the next pass, not when the record's wait would have ended.
//   - A record that comes due admits one attempt per owner in the pass:
//     the roll's start on one Instance and the Create pass's first attempt
//     on a new Instance open together, because the Create pass reads the
//     record as it stood before the roll's start flipped it.

// retryRecordStory settles three single-pod Instances on version 1 under
// RecreatePod with unavail Instances out of rotation at a time, on a retry
// schedule whose first wait is one hour, and sets the fault: every pod
// created from now on waits in ImagePullBackOff until the fault is
// cleared, and a pod already waiting keeps waiting after that.
func retryRecordStory(t *testing.T, unavail int32) *recoveryHarness {
	t.Helper()
	h := offTargetCrashHarness(t, false, workloadtypes.UpdateStrategyRecreatePod, 0, unavail, workloadtypes.RestartPolicyNone, 3)
	h.retryPolicy = &workloadtypes.RetryPolicy{MaxAttempts: 3, InitialDelay: time.Hour, MaxDelay: 2 * time.Hour, Multiplier: 2}
	h.stuckGrace = 2 * time.Minute
	h.stepCap = 5 * time.Second
	h.armWedge("ImagePullBackOff", true)
	return h
}

// rollingInstance is the index of the one Instance whose row reads
// Updating, or -1 when none or more than one does.
func rollingInstance(h *recoveryHarness) int32 {
	found := int32(-1)
	for _, row := range h.irStatuses() {
		if row.Phase != workloadtypes.InstancePhaseUpdating {
			continue
		}
		if found >= 0 {
			return -1
		}
		found = row.Index
	}
	return found
}

// podOnImage is the live pod of idx that renders image, nil when none.
func podOnImage(h *recoveryHarness, idx int32, image string) *corev1.Pod {
	for _, pod := range h.podsOf(idx) {
		if pod.Spec.Containers[0].Image == image {
			return pod
		}
	}
	return nil
}

// waitingIn reports whether the pod's container waits in reason.
func waitingIn(pod *corev1.Pod, reason string) bool {
	if pod == nil || len(pod.Status.ContainerStatuses) == 0 {
		return false
	}
	waiting := pod.Status.ContainerStatuses[0].State.Waiting
	return waiting != nil && waiting.Reason == reason
}

// stoppedAttempt reports a Failed row keeping the attempt the escalation
// stopped with its pod: the operation stays on the row at the stopped step.
func stoppedAttempt(s *workloadtypes.InstanceStatus) bool {
	return s != nil && s.Phase == workloadtypes.InstancePhaseFailed && workloadtypes.OperationParked(s.Operation)
}

// firstStartCannotPull pushes version 2 and drives the passes until the
// one start the roll opened has a new pod waiting in ImagePullBackOff,
// then clears the fault so every pod created from now on starts, while
// that pod keeps waiting. It returns the Instance that started.
func firstStartCannotPull(t *testing.T, h *recoveryHarness) int32 {
	t.Helper()
	h.setTarget(h.revFixed, fixedImage)
	started := int32(-1)
	if !h.run(20, func() bool {
		started = rollingInstance(h)
		return started >= 0 && waitingIn(podOnImage(h, started, fixedImage), "ImagePullBackOff")
	}) {
		h.dumpState("after the push")
		t.Fatalf("no start of the roll has a version 2 pod waiting in ImagePullBackOff")
	}
	h.clearWedge()
	return started
}

// recordAfterTheStop drives the passes until the escalation stops the
// attempt on idx with its pod kept and counts the failure, and returns the
// record: Backoff, one attempt, the next due about an hour from now.
func recordAfterTheStop(t *testing.T, h *recoveryHarness, idx int32, rev string) workloadtypes.RetryBlock {
	t.Helper()
	if !h.run(80, func() bool {
		b := h.findBlock(rev)
		return stoppedAttempt(h.instance(idx)) && b != nil && b.State == workloadtypes.RetryBlockBackoff
	}) {
		h.dumpState("waiting for the stop")
		t.Fatalf("Instance %d's attempt was not stopped with its failure counted; row %+v record %+v", idx, h.instance(idx), h.findBlock(rev))
	}
	b := *h.findBlock(rev)
	if b.AttemptsStarted != 1 || b.NextRetryAt == nil || b.NextRetryAt.Time.Sub(h.clk.Now()) < 50*time.Minute {
		t.Fatalf("record after the stop = %+v at %s, want one attempt counted and the next due about an hour out", b, h.clk.Now())
	}
	return b
}

// siblingStarted drives the passes until an Instance other than stopped
// has an open attempt with a version 2 pod, and returns it.
func siblingStarted(t *testing.T, h *recoveryHarness, stopped int32) int32 {
	t.Helper()
	sibling := int32(-1)
	if !h.run(20, func() bool {
		for _, row := range h.irStatuses() {
			if row.Index != stopped && row.Phase == workloadtypes.InstancePhaseUpdating && podOnImage(h, row.Index, fixedImage) != nil {
				sibling = row.Index
				return true
			}
		}
		return false
	}) {
		h.dumpState("after the gate opened")
		t.Fatalf("no sibling of Instance %d opened an attempt with a version 2 pod", stopped)
	}
	return sibling
}

// TestReconcile_ASiblingsPromoteRemovesTheRecordAndTheStoppedAttemptStartsAgainAtOnce:
// three single-pod Instances roll to version 2 under RecreatePod with two
// Instances out of rotation at a time, behind a rollout group's gate that
// admits one start and holds the next until the test opens it. The first
// Instance's new pod cannot pull its image: once it has waited past the
// grace the escalation allows, the escalation stops the attempt, keeps
// the pod, and counts the failure, so
// the revision's record reads Backoff with the next attempt due in an
// hour. The sibling's attempt, opened once the gate let it through and
// before the count, runs with a pod that is not Ready yet. When that pod
// turns Ready the sibling promotes, and the promote removes the record: a
// revision one Instance now serves is not held back by another's failure.
// On the next pass the stopped attempt starts again, an hour before the
// record's time, and the roll finishes.
func TestReconcile_ASiblingsPromoteRemovesTheRecordAndTheStoppedAttemptStartsAgainAtOnce(t *testing.T) {
	h := retryRecordStory(t, 2)
	gate := &heldGate{admit: 1}
	h.gate = gate.answer
	rev := h.revFixed.Name
	stopped := firstStartCannotPull(t, h)

	// The gate opens: the sibling starts while the first attempt still runs
	// and no failure is counted yet. Its new pod runs but fails readiness.
	gate.open = true
	sibling := siblingStarted(t, h, stopped)
	h.failReadiness(sibling, "")

	record := recordAfterTheStop(t, h, stopped, rev)
	if row := h.instance(sibling); row == nil || row.Phase != workloadtypes.InstancePhaseUpdating {
		t.Fatalf("the sibling's attempt should still run at the count; row %+v", row)
	}

	// The sibling's pod turns Ready: the promote removes the record.
	h.restoreReadiness(sibling, "")
	if !h.run(20, func() bool { return h.instanceReadyOn(sibling, h.revFixed) }) {
		h.dumpState("waiting for the sibling's promote")
		t.Fatalf("Instance %d never promoted on version 2", sibling)
	}
	if b := h.findBlock(rev); b != nil {
		t.Fatalf("the sibling's promote should remove the record; record %+v", b)
	}

	// The stopped attempt starts again on the next pass, not at the record's time.
	restarted := func() bool {
		row := h.instance(stopped)
		return row != nil && !stoppedAttempt(row) && row.Phase != workloadtypes.InstancePhaseFailed
	}
	passes := 0
	for !restarted() && passes < 3 {
		h.step()
		passes++
	}
	if !restarted() || !h.clk.Now().Before(record.NextRetryAt.Time) {
		h.dumpState("after the promote")
		t.Fatalf("Instance %d should start again within a pass of the promote (decided behaviour: a sibling's promote removes the record a stopped attempt waits behind); after %d pass(es) at %s the row is %+v and the record was due at %s",
			stopped, passes, h.clk.Now(), h.instance(stopped), record.NextRetryAt.Time)
	}
	if !h.run(120, func() bool { return h.settledOn(h.revFixed, 3) }) {
		h.dumpState("finishing the roll")
		t.Fatalf("the roll never finished on version 2")
	}
}

// TestReconcile_TheDueRecordOpensTheRollsStartAndAScaleUpsCreateInOnePass:
// three single-pod Instances roll to version 2 under RecreatePod one
// Instance at a time. The first Instance's new pod cannot pull its image,
// so the escalation stops the attempt with its pod kept and the
// revision's record reads Backoff, due in an hour. During the wait the
// operator asks for a fourth Instance and lets two Instances be out of
// rotation at a time; the new Instance's first attempt waits on the same
// record. In the pass the record comes due, the roll starts the stopped
// attempt again, which flips the record to RetryInProgress, and the
// Create pass of the same pass, reading the record as it stood before
// that flip, opens Instance 3's first attempt too: two attempts at one
// revision in one pass, one per owner. (With the budget left at one, the
// Instance that has no pods yet fills it: the Create pass opens its
// attempt alone and the roll's start waits for it to serve.) Each attempt
// runs its own course; the first promote removes the record and the roll
// finishes with four Instances on version 2.
func TestReconcile_TheDueRecordOpensTheRollsStartAndAScaleUpsCreateInOnePass(t *testing.T) {
	h := retryRecordStory(t, 1)
	rev := h.revFixed.Name
	stopped := firstStartCannotPull(t, h)
	record := recordAfterTheStop(t, h, stopped, rev)
	attempt := h.instance(stopped).Incarnation

	// A fourth Instance is asked for during the wait, with room in the
	// budget for it beside one more start: its first attempt waits on the
	// record, and no row is written for it.
	two := intstr.FromInt32(2)
	h.lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable = &two
	h.replicas = 4
	h.setTarget(h.revFixed, fixedImage)
	h.step()
	if h.instance(3) != nil || len(h.podsOf(3)) != 0 {
		t.Fatalf("the new Instance should wait on the record; row %+v pods %d", h.instance(3), len(h.podsOf(3)))
	}
	if hold := h.lastHold(); hold == nil || hold.Gate != workloadtypes.RolloutHoldGateRetryBlock {
		t.Fatalf("hold = %+v, want the retry record named", hold)
	}

	// The record comes due: one pass opens both attempts.
	h.clk.SetTime(record.NextRetryAt.Time.Add(time.Second))
	h.step()
	row, created, b := h.instance(stopped), h.instance(3), h.findBlock(rev)
	switch {
	case row == nil || row.Phase != workloadtypes.InstancePhaseUpdating || workloadtypes.OperationParked(row.Operation) || row.Incarnation <= attempt:
		t.Fatalf("the roll should start Instance %d's attempt again in the due pass; row %+v", stopped, row)
	case created == nil || created.Phase != workloadtypes.InstancePhaseCreating || created.Operation == nil ||
		created.Operation.Type != workloadtypes.InstanceOperationCreate || created.Operation.TargetRevision != rev || len(h.podsOf(3)) != 1:
		t.Fatalf("the Create pass should open Instance 3's first attempt at version 2 in the same pass (decided behaviour: the due record admits one attempt per owner, the Create pass reading it as it stood before the roll's start flipped it); row %+v pods %d",
			created, len(h.podsOf(3)))
	case b == nil || b.State != workloadtypes.RetryBlockRetryInProgress || b.AttemptsStarted != 1:
		t.Fatalf("record after the due pass = %+v, want RetryInProgress with the one counted attempt", b)
	}
	if !h.run(160, func() bool { return h.settledOn(h.revFixed, 4) }) {
		h.dumpState("finishing the roll")
		t.Fatalf("the roll never finished with four Instances on version 2")
	}
	if b := h.findBlock(rev); b != nil {
		t.Fatalf("the record should be removed once an Instance promotes at the revision; record %+v", b)
	}
}
