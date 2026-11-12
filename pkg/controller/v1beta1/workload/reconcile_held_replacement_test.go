package workload_test

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const thirdImage = "registry.test/serving:v3"

// newHeldFailedReplacementHarness is nine single-pod Instances serving
// version 1 under SurgeThenDrain with one surge at a time. The operator
// holds Instances 0 to 4 with a partition of 5 and pushes version 2, whose
// image never pulls: Instance 5 surges, its replacement waits in
// ImagePullBackOff until the grace ends the attempt, and the row is Failed
// on version 1 with version 2 recorded as its target, the serving source
// standing beside the failed replacement and version 2's retry record in
// backoff. The operator then raises the partition to 9, which holds every
// Instance still off the target, so the roll may take none of them.
func newHeldFailedReplacementHarness(t *testing.T) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarness(t, false)
	h.recorder = record.NewFakeRecorder(1024)
	h.replicas = 9
	h.useRollingBudget(workloadtypes.UpdateStrategySurgeThenDrain)
	h.setTarget(h.revV1, goodImage)
	if !h.run(60, func() bool { return h.settledOn(h.revV1, 9) }) {
		h.dumpState("initial create")
		t.Fatalf("the initial create never converged on nine Instances")
	}

	five := int32(5)
	h.lifecycle.UpdateStrategy.RollingUpdate.Partition = &five
	h.setTarget(h.revBad, badImage)
	failed := func() bool {
		row := h.instance(5)
		return row != nil && row.Phase == workloadtypes.InstancePhaseFailed && row.Operation == nil &&
			row.RunningRevision == h.revV1.Name && row.TargetRevision == h.revBad.Name &&
			h.findBlock(h.revBad.Name) != nil
	}
	if !h.run(60, failed) {
		h.dumpState("replacement failed")
		t.Fatalf("Instance 5's replacement never failed beside its serving source")
	}
	if extra := h.extraPodsOf(5); len(extra) != 1 {
		h.dumpState("replacement failed")
		t.Fatalf("the failed replacement must stand beside the source, got %d", len(extra))
	}
	for idx := int32(0); idx < 9; idx++ {
		if idx != 5 && len(h.podsOf(idx)) != 1 {
			h.dumpState("replacement failed")
			t.Fatalf("only Instance 5 may have started, Instance %d has %d pods", idx, len(h.podsOf(idx)))
		}
	}

	nine := int32(9)
	h.lifecycle.UpdateStrategy.RollingUpdate.Partition = &nine
	h.setTarget(h.target, h.desired.PodSpec.Containers[0].Image)
	h.events = nil
	return h
}

// sourcePodOf is the one pod of Instance idx that serves its running
// revision.
func (h *recoveryHarness) sourcePodOf(idx int32) *corev1.Pod {
	h.t.Helper()
	for _, pod := range h.podsOf(idx) {
		if pod.Spec.Containers[0].Image == goodImage {
			return pod
		}
	}
	h.t.Fatalf("Instance %d has no source pod on version 1", idx)
	return nil
}

// extraPodsOf is every pod of Instance idx beside its source on version 1.
func (h *recoveryHarness) extraPodsOf(idx int32) []*corev1.Pod {
	var out []*corev1.Pod
	for _, pod := range h.podsOf(idx) {
		if pod.Spec.Containers[0].Image != goodImage {
			out = append(out, pod)
		}
	}
	return out
}

// rowsAsTheStatusWriterReadsThem is the published rows read back the way
// the InferenceReplica status writer reads them: a row published Ready
// over a failed replacement reads Failed again.
func rowsAsTheStatusWriterReadsThem(rows []workloadtypes.InstanceStatus) []workloadtypes.InstanceStatus {
	out := make([]workloadtypes.InstanceStatus, 0, len(rows))
	for _, row := range rows {
		out = append(out, v1beta1convert.InstanceStatusToWorkload(v1beta1convert.InstanceStatusFromWorkload(row)))
	}
	return out
}

// A failed replacement toward the revision the push still asks for stays
// under the held Instance: the retry is a start on that Instance, which the
// partition refuses. Once the backoff is due nothing starts, the extra pod
// stands, and the member's hold names Instance 5, its failed replacement
// and the exit.
func TestHeldInstance_FailedReplacementTowardTheTargetStaysUnderTheHoldThatNamesIt(t *testing.T) {
	h := newHeldFailedReplacementHarness(t)
	replacement := h.extraPodsOf(5)[0]
	source := h.sourcePodOf(5)

	due := func() bool {
		b := h.findBlock(h.revBad.Name)
		return b != nil && b.NextRetryAt != nil && !h.clk.Now().Before(b.NextRetryAt.Time)
	}
	if !h.run(20, due) {
		h.dumpState("backoff due")
		t.Fatalf("version 2's retry record never came due")
	}
	holdsBefore := len(h.holds)
	h.settle(4)

	row := h.instance(5)
	if row == nil || row.Phase != workloadtypes.InstancePhaseFailed || row.Operation != nil ||
		row.RunningRevision != h.revV1.Name || row.TargetRevision != h.revBad.Name {
		h.dumpState("still wanted")
		t.Fatalf("the failed attempt toward version 2 must stay on the held Instance while the push still asks for version 2, got %+v", row)
	}
	if extra := h.extraPodsOf(5); len(extra) != 1 || extra[0].UID != replacement.UID {
		h.dumpState("still wanted")
		t.Fatalf("the failed replacement toward a revision the push still asks for must stay under the hold, got %d extra pod(s)", len(extra))
	}
	if pods := h.podsOf(5); len(pods) != 2 || h.sourcePodOf(5).UID != source.UID {
		h.dumpState("still wanted")
		t.Fatalf("the serving source of Instance 5 must stand untouched beside the failed replacement, got %d pod(s)", len(pods))
	}
	for idx := int32(0); idx < 9; idx++ {
		if r := h.instance(idx); r != nil && r.Operation != nil {
			h.dumpState("still wanted")
			t.Fatalf("the partition holds every Instance, so nothing may start; Instance %d carries %+v", idx, r.Operation)
		}
	}
	hold := h.lastHold()
	if len(h.holds) == holdsBefore || hold.Gate != workloadtypes.RolloutHoldGateRetryBlock || hold.Target != h.revBad.Name ||
		!strings.Contains(hold.Reason, "Instance 5") || !strings.Contains(hold.Reason, "failed replacement") ||
		!strings.Contains(hold.Reason, "partition of 9") || !strings.Contains(hold.Reason, "lower the partition") {
		t.Fatalf("while the failed replacement stands under the partition every pass must report the hold naming Instance 5, the failed replacement and the exit, got %v", hold)
	}
}

// A push to version 3 removes version 2's retry record, and with it the
// only revision the failed replacement was for. The hold protects the
// serving source, not that extra: the failed replacement is removed under
// the held Instance, the row returns to Ready on version 1, and the member
// rests at its staged shape under the partition with nothing to hold on.
func TestHeldInstance_FailedReplacementOfALeftRevisionIsRemovedAndTheMemberRests(t *testing.T) {
	h := newHeldFailedReplacementHarness(t)
	replacement := h.extraPodsOf(5)[0]
	source := h.sourcePodOf(5)

	third := h.pushRevision(thirdImage)
	rested := func() bool {
		row := h.instance(5)
		return row != nil && row.Phase == workloadtypes.InstancePhaseReady && row.Operation == nil &&
			row.RunningRevision == h.revV1.Name && row.TargetRevision == "" && len(h.extraPodsOf(5)) == 0
	}
	if !h.run(20, rested) {
		h.dumpState("left revision")
		t.Fatalf("the failed replacement toward version 2, which no push asks for, must leave the held Instance and the row return to version 1; got row=%+v, extra pods=%d", h.instance(5), len(h.extraPodsOf(5)))
	}
	if h.findBlock(h.revBad.Name) != nil {
		t.Fatalf("the push to version 3 removes version 2's retry record, got %+v", h.findBlock(h.revBad.Name))
	}
	for _, pod := range h.podsOf(5) {
		if pod.UID == replacement.UID {
			t.Fatalf("the failed replacement %s must be gone", pod.Name)
		}
	}
	if pods := h.podsOf(5); len(pods) != 1 || pods[0].UID != source.UID {
		h.dumpState("left revision")
		t.Fatalf("the serving source of Instance 5 must be its only pod and untouched, got %d pod(s)", len(pods))
	}
	if !h.sawEvent(workloadtypes.EventReasonSupersededWreckageCleaned) || !h.sawEvent(workloadtypes.EventReasonSurgeAbandoned) {
		t.Fatalf("the removal and the return to version 1 are both recorded as events, got %v", h.events)
	}

	holdsBefore := len(h.holds)
	h.settle(3)
	rows, _ := h.publication()
	read := rowsAsTheStatusWriterReadsThem(rows)
	if !status.ReachedDesiredShape(read, third.Name, 9, 9) {
		t.Fatalf("the member must rest at its staged shape under partition 9 once the failed replacement is gone, got %+v", read)
	}
	for idx := int32(0); idx < 9; idx++ {
		if r := h.instance(idx); r == nil || r.Phase != workloadtypes.InstancePhaseReady || r.Operation != nil {
			t.Fatalf("every Instance rests Ready on the revision the partition holds it on, Instance %d reads %+v", idx, r)
		}
	}
	if len(h.holds) != holdsBefore {
		t.Fatalf("a member at rest holds on nothing, got %v", h.lastHold())
	}
}
