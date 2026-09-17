package workload_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// The restart pass gives every selected Instance its turn each pass. These
// tests drive two single-pod Instances under RecreateInstance through the
// closed-loop harness (corrective_recovery_test.go): both lose their pod so
// both are selected for restart in the same pass, and one Instance's status
// writes are rejected on every pass.

// newRestartHarness is the recovery harness with n single-pod Instances
// under RestartPolicy=RecreateInstance, converged Ready on v1.
func newRestartHarness(t *testing.T, replicas int32) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarness(t, false)
	policy := workload.RestartPolicyRecreateInstance
	h.lifecycle = workload.Lifecycle{RestartPolicy: &policy}
	h.replicas = replicas
	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.allReady(replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never converged on v1 for %d instances", replicas)
	}
	return h
}

// losePods deletes every live pod of the given Instances so the next pass
// selects each of them for restart (pod count below desired).
func (h *recoveryHarness) losePods(indices ...int32) {
	h.t.Helper()
	lose := map[int32]bool{}
	for _, idx := range indices {
		lose[idx] = true
	}
	for _, pod := range h.livePods() {
		if idx, ok := query.InstanceIdxFromLabels(pod); !ok || !lose[idx] {
			continue
		}
		if err := h.c.Delete(h.ctx, pod); err != nil {
			h.t.Fatalf("delete pod %s: %v", pod.Name, err)
		}
	}
}

func (h *recoveryHarness) instance(idx int32) *workload.InstanceStatus {
	sts := h.irStatuses()
	for i := range sts {
		if sts[i].Index == idx {
			return &sts[i]
		}
	}
	return nil
}

func (h *recoveryHarness) podsOf(idx int32) []*corev1.Pod {
	var out []*corev1.Pod
	for _, pod := range h.livePods() {
		if i, ok := query.InstanceIdxFromLabels(pod); ok && i == idx {
			out = append(out, pod)
		}
	}
	return out
}

// allReady reports whether exactly n Instances exist, each Ready with no
// Operation and exactly one live pod.
func (h *recoveryHarness) allReady(n int32) bool {
	sts := h.irStatuses()
	if len(sts) != int(n) {
		return false
	}
	for _, s := range sts {
		if s.Phase != workload.InstancePhaseReady || s.Operation != nil || len(h.podsOf(s.Index)) != 1 {
			return false
		}
	}
	return true
}

// atIncarnation reports whether the Instance's status and its live pods all
// carry the given incarnation.
func (h *recoveryHarness) atIncarnation(idx int32, incarnation int64) bool {
	s := h.instance(idx)
	if s == nil || s.Incarnation != incarnation {
		return false
	}
	pods := h.podsOf(idx)
	if len(pods) == 0 {
		return false
	}
	for _, pod := range pods {
		if pod.Labels[query.LabelInstanceIncarnation] != strconv.FormatInt(incarnation, 10) {
			return false
		}
	}
	return true
}

// TestRestartPass_IsolatesInstanceStatusWriteFailure: instance 0's status
// writes are rejected on every pass. Instance 1, selected in the same pass,
// must start and finish its restart regardless; each failing pass returns an
// error naming instance 0; once instance 0's writes succeed both converge.
func TestRestartPass_IsolatesInstanceStatusWriteFailure(t *testing.T) {
	h := newRestartHarness(t, 2)
	rejected := errors.New("status update rejected")
	h.mutateErr = map[int32]error{0: rejected}
	h.losePods(0, 1)

	_, err := h.stepResult()
	if err == nil {
		t.Fatal("the pass must fail while instance 0's status write is rejected")
	}
	if !errors.Is(err, rejected) {
		t.Fatalf("the returned error must wrap the rejected write; got %v", err)
	}
	if !strings.Contains(err.Error(), "restart instance 0") {
		t.Fatalf("the returned error must name instance 0; got %v", err)
	}
	if strings.Contains(err.Error(), "restart instance 1") {
		t.Fatalf("instance 1 did not fail and must not be reported; got %v", err)
	}
	if s := h.instance(0); s == nil || s.Phase != workload.InstancePhaseReady || s.Incarnation != 1 || s.Operation != nil {
		t.Fatalf("instance 0 must be untouched while its writes are rejected; got %+v", s)
	}
	if pods := h.podsOf(0); len(pods) != 0 {
		t.Fatalf("instance 0 must not be recreated before its Restarting write lands; got %d pods", len(pods))
	}
	if s := h.instance(1); s == nil || s.Phase != workload.InstancePhaseRestarting ||
		s.Operation == nil || s.Operation.Type != workload.InstanceOperationRestart {
		t.Fatalf("instance 1 must start its restart in the same pass; got %+v", s)
	}
	if !h.atIncarnation(1, 2) {
		h.dumpState("after first failing pass")
		t.Fatalf("instance 1 must have its replacement pod at incarnation 2")
	}

	_, err = h.stepResult()
	if err == nil || !strings.Contains(err.Error(), "restart instance 0") {
		t.Fatalf("the pass must keep failing on instance 0; got %v", err)
	}
	if s := h.instance(1); s == nil || s.Phase != workload.InstancePhaseReady || s.Operation != nil || !h.atIncarnation(1, 2) {
		h.dumpState("after second failing pass")
		t.Fatalf("instance 1 must complete its restart while instance 0 keeps failing; got %+v", s)
	}
	if pods := h.podsOf(0); len(pods) != 0 {
		t.Fatalf("instance 0 must still be untouched; got %d pods", len(pods))
	}

	h.mutateErr = nil
	converged := h.run(10, func() bool {
		return h.allReady(2) && h.atIncarnation(0, 2) && h.atIncarnation(1, 2)
	})
	if !converged {
		h.dumpState("after instance 0's writes were accepted")
		t.Fatalf("both instances must converge once instance 0's writes succeed")
	}
}

// TestRestartPass_NoErrors_RequeuesAndCreatesFreshIndices pins the
// error-free contract of a multi-Instance restart pass: every selected
// Instance advances, the pass requeues at the restart interval, and a
// surge-free index added by a concurrent scale-up is materialized in the
// same pass instead of waiting behind the in-flight restarts.
func TestRestartPass_NoErrors_RequeuesAndCreatesFreshIndices(t *testing.T) {
	h := newRestartHarness(t, 2)
	h.losePods(0, 1)
	h.replicas = 3
	h.setTarget(h.revV1, goodImage)

	res, err := h.stepResult()
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.Requeue || res.RequeueAfter != workloadops.RestartRequeueInterval {
		t.Fatalf("the restart pass must requeue at the restart interval; got %+v", res)
	}
	for _, idx := range []int32{0, 1} {
		if s := h.instance(idx); s == nil || s.Phase != workload.InstancePhaseRestarting ||
			s.Operation == nil || s.Operation.Type != workload.InstanceOperationRestart {
			t.Fatalf("instance %d must be restarting; got %+v", idx, s)
		}
		if !h.atIncarnation(idx, 2) {
			h.dumpState("restart pass")
			t.Fatalf("instance %d must have its replacement pod at incarnation 2", idx)
		}
	}
	if s := h.instance(2); s == nil || s.Phase != workload.InstancePhaseCreating ||
		s.Operation == nil || s.Operation.Type != workload.InstanceOperationCreate {
		t.Fatalf("fresh index 2 must begin its Create in the restart pass; got %+v", s)
	}
	if pods := h.podsOf(2); len(pods) != 1 {
		t.Fatalf("fresh index 2 must be materialized in the restart pass; got %d pods", len(pods))
	}

	if !h.run(10, func() bool { return h.allReady(3) }) {
		h.dumpState("after scale-up during restart")
		t.Fatalf("restarted and fresh instances must all converge")
	}
}
