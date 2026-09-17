package workload_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestPodAdmissionGated pins the gated-pod detector: a pod is "gated"
// (queued for admission, e.g. by Kueue) iff it still carries a scheduling
// gate. An un-gated or nil pod is not.
func TestPodAdmissionGated(t *testing.T) {
	gated := &corev1.Pod{Spec: corev1.PodSpec{
		SchedulingGates: []corev1.PodSchedulingGate{{Name: "kueue.x-k8s.io/admission"}},
	}}
	if !workload.PodAdmissionGated(gated) {
		t.Errorf("pod with a scheduling gate: got false want true")
	}
	if workload.PodAdmissionGated(&corev1.Pod{}) {
		t.Errorf("pod with no scheduling gate: got true want false")
	}
	if workload.PodAdmissionGated(nil) {
		t.Errorf("nil pod: got true want false")
	}
}

// TestReconcileGatedDeadlines_ParksGatedInstance: while an Instance's pods
// are admission-gated, its Operation.Deadline is parked (zeroed → "never
// expires") so the gated wait cannot count against InstanceReadyTimeout.
// The Instance is NOT failed — it is queued, not stuck.
func TestReconcileGatedDeadlines_ParksGatedInstance(t *testing.T) {
	now := time.Now()
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseCreating,
		Operation: &workload.InstanceOperation{
			Type:     workload.InstanceOperationCreate,
			Deadline: metav1.NewTime(now.Add(-1 * time.Minute)), // already past
		},
	}}
	input, store, _ := expireFixture(insts)

	if err := workload.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{0: true}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.IsZero() {
		t.Errorf("gated Deadline: got %v want zero (parked)", (*store)[0].Operation.Deadline)
	}
	if (*store)[0].Phase != workload.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (paused, not failed)", (*store)[0].Phase)
	}
}

// TestReconcileGatedDeadlines_RestartsOnUngate: once the pods clear the
// gate, a parked (zero) Deadline is (re)started as now+timeout — i.e. the
// timeout is measured from admission, not from operation start.
func TestReconcileGatedDeadlines_RestartsOnUngate(t *testing.T) {
	now := time.Now()
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseCreating,
		Operation: &workload.InstanceOperation{
			Type: workload.InstanceOperationCreate,
			// Deadline zero: parked while previously gated.
		},
	}}
	input, store, _ := expireFixture(insts)

	if err := workload.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	d := (*store)[0].Operation.Deadline
	if d.IsZero() {
		t.Fatalf("ungated Deadline: got zero want ~now+timeout")
	}
	if !d.Time.After(now.Add(25 * time.Minute)) {
		t.Errorf("restarted Deadline %v should be ~now+30m (measured from admission)", d.Time)
	}
}

// TestReconcileGatedDeadlines_UngatedNormalUntouched: the no-Kueue path is
// unchanged — an un-gated Instance with a normal (non-zero) Deadline is
// left exactly as the per-op writer stamped it.
func TestReconcileGatedDeadlines_UngatedNormalUntouched(t *testing.T) {
	now := time.Now()
	orig := metav1.NewTime(now.Add(20 * time.Minute))
	insts := []workload.InstanceStatus{{
		Index:     0,
		Phase:     workload.InstancePhaseCreating,
		Operation: &workload.InstanceOperation{Type: workload.InstanceOperationCreate, Deadline: orig},
	}}
	input, store, _ := expireFixture(insts)

	if err := workload.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.Equal(&orig) {
		t.Errorf("un-gated normal Deadline changed: got %v want %v (untouched)", (*store)[0].Operation.Deadline, orig)
	}
}

// TestReconcileGatedDeadlines_NonTransientUntouched: only transient ops
// (Creating/Updating/Restarting/Migrating) are subject to the clock; a
// Ready Instance is never touched even if its pods report gated.
func TestReconcileGatedDeadlines_NonTransientUntouched(t *testing.T) {
	now := time.Now()
	orig := metav1.NewTime(now.Add(5 * time.Minute))
	insts := []workload.InstanceStatus{{
		Index:     0,
		Phase:     workload.InstancePhaseReady,
		Operation: &workload.InstanceOperation{Type: workload.InstanceOperationCreate, Deadline: orig},
	}}
	input, store, _ := expireFixture(insts)

	if err := workload.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{0: true}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.Equal(&orig) {
		t.Errorf("non-transient Deadline changed: got %v want untouched", (*store)[0].Operation.Deadline)
	}
}

// TestGatedInstance_NotFailedByDeadlineBackstop is the regression guard for
// the fix: a gated Instance long past its ORIGINAL deadline must survive
// the deadline backstop — pause (parks the clock) then expire (sees the
// parked zero, skips). Without the pause step the backstop would fail it.
func TestGatedInstance_NotFailedByDeadlineBackstop(t *testing.T) {
	now := time.Now()
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseCreating,
		Operation: &workload.InstanceOperation{
			Type:     workload.InstanceOperationCreate,
			Deadline: metav1.NewTime(now.Add(-1 * time.Hour)), // long past
		},
	}}
	input, store, event := expireFixture(insts)

	if err := workload.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{0: true}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	// Rebuild the observation from the parked store, mirroring the fresh
	// status read the next reconcile's escalation pass consumes.
	input.ObservedState.InstanceStatuses = append([]workload.InstanceStatus(nil), (*store)...)
	if err := runEscalationPass(t, workload.Deps{}, input, workload.ComponentPlan{}, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if (*store)[0].Phase == workload.InstancePhaseFailed {
		t.Errorf("gated Instance was failed by the deadline backstop; want alive (clock paused while gated)")
	}
	if event.count != 0 {
		t.Errorf("event count: got %d want 0 (gated Instance must not be failed)", event.count)
	}
}

// TestReconcileGatedDeadlines_ParksQuotaBlockedInstance: a create the
// apiserver refused for lack of quota leaves no pod to carry a scheduling
// gate, so the wait is recorded on the Operation instead. The deadline
// parks on exactly the same terms — the instance is queued behind
// capacity, not stuck — and re-arms from the moment the block clears.
func TestReconcileGatedDeadlines_ParksQuotaBlockedInstance(t *testing.T) {
	now := time.Now()
	original := metav1.NewTime(now.Add(10 * time.Minute))
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseCreating,
		Operation: &workload.InstanceOperation{
			Type:     workload.InstanceOperationCreate,
			Deadline: original,
			Waiting:  workloadtypes.RejectionReasonQuotaExceeded,
		},
	}}
	input, store, _ := expireFixture(insts)

	// No pod is gated — the Operation's waiting reason is the only signal.
	if err := workload.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.IsZero() {
		t.Fatalf("quota-blocked Deadline: got %v want zero (parked)", (*store)[0].Operation.Deadline)
	}
	if (*store)[0].Phase != workload.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (waiting on capacity, not failed)", (*store)[0].Phase)
	}

	// The block clears (a create succeeded, so the create path cleared the
	// waiting reason): the clock restarts from that moment, not from the
	// operation's original start.
	(*store)[0].Operation.Waiting = ""
	cleared := append([]workload.InstanceStatus(nil), (*store)...)
	if err := workload.ReconcileGatedDeadlines(context.Background(), input, cleared, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines after unblock: %v", err)
	}
	d := (*store)[0].Operation.Deadline
	if d.IsZero() || !d.Time.After(original.Time) {
		t.Errorf("re-armed Deadline: got %v want a fresh now+timeout (later than the original %v)", d, original)
	}
}

// TestReconcileGatedDeadlines_QuotaBlockedStaysParkedWhileBlocked: parking
// is edge-triggered, so a still-blocked instance is not rewritten every
// pass — a workload queued behind quota for hours must not churn status.
func TestReconcileGatedDeadlines_QuotaBlockedStaysParkedWhileBlocked(t *testing.T) {
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseCreating,
		Operation: &workload.InstanceOperation{
			Type:    workload.InstanceOperationCreate,
			Waiting: workloadtypes.RejectionReasonQuotaExceeded,
		},
	}}
	input, store, _ := expireFixture(insts)

	if err := workload.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.IsZero() {
		t.Errorf("already-parked Deadline: got %v want still zero (no re-arm while blocked)", (*store)[0].Operation.Deadline)
	}
}

// TestQuotaBlockedInstance_NotFailedByDeadlineBackstop is the
// capacity-blocked twin of TestGatedInstance_NotFailedByDeadlineBackstop:
// an instance whose creates admission refused for lack of quota is queued
// behind capacity, not stuck. Its deadline predates the park, so the
// escalation pass must skip it on the same terms it skips an
// admission-gated one — otherwise the BROAD path fails it in the window
// before (or after) the park lands.
func TestQuotaBlockedInstance_NotFailedByDeadlineBackstop(t *testing.T) {
	now := time.Now()
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseCreating,
		Operation: &workload.InstanceOperation{
			Type:     workload.InstanceOperationCreate,
			Deadline: metav1.NewTime(now.Add(-1 * time.Hour)), // stamped before the block
			Waiting:  workloadtypes.RejectionReasonQuotaExceeded,
		},
	}}
	input, store, event := expireFixture(insts)

	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1), nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}
	if (*store)[0].Phase == workload.InstancePhaseFailed {
		t.Errorf("quota-blocked Instance was failed by the deadline backstop; want alive (waiting on capacity)")
	}
	if event.count != 0 {
		t.Errorf("event count: got %d want 0 (a capacity wait must not be reported as a failure)", event.count)
	}
}

// TestGangSurgeSource_HeldWhileItsSurgeWaitsOnQuota is the reader side of
// the gang-surge wait. A gang surge creates its replacement pods under
// the SURGE index, so that row is where the quota token lands — but the
// governing operation and the deadline it is judged by live on the
// SOURCE. Both readers must follow Operation.SurgeIndex, exactly as they
// already follow it for an admission-gated surge bucket: otherwise the
// source's clock keeps running through a wait its own replacement is
// stuck in, and the backstop fails a rollout that is merely queued.
func TestGangSurgeSource_HeldWhileItsSurgeWaitsOnQuota(t *testing.T) {
	now := time.Now()
	surgeIndex := int32(2)
	insts := []workload.InstanceStatus{
		{
			Index: 0,
			Phase: workload.InstancePhaseUpdating,
			Operation: &workload.InstanceOperation{
				Type:       workload.InstanceOperationUpdate,
				Deadline:   metav1.NewTime(now.Add(-1 * time.Hour)), // stamped before the block
				SurgeIndex: &surgeIndex,
			},
		},
		{
			Index: surgeIndex,
			Phase: workload.InstancePhaseCreating,
			Operation: &workload.InstanceOperation{
				Type:    workload.InstanceOperationUpdate,
				Waiting: workloadtypes.RejectionReasonQuotaExceeded,
			},
		},
	}
	input, store, event := expireFixture(insts)

	if err := workload.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.IsZero() {
		t.Errorf("source deadline: got %v want parked while its surge waits on quota", (*store)[0].Operation.Deadline)
	}

	// Escalation must skip the source too, covering the window before the
	// park lands (or after a restart re-reads a pre-park deadline).
	input.ObservedState.InstanceStatuses = append([]workload.InstanceStatus(nil), insts...)
	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 2), nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}
	if (*store)[0].Phase == workload.InstancePhaseFailed {
		t.Errorf("source was failed by the deadline backstop; want alive (its surge is queued behind capacity)")
	}
	if event.count != 0 {
		t.Errorf("event count: got %d want 0", event.count)
	}
}
