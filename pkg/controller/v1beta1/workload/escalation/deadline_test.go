package escalation_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// expireFixture wires a ReconcileInput whose MutateInstance applies the
// callback to the local insts slice (mirroring the per-Instance status
// the adapter would persist) and whose WarnInstanceFailed records the
// last event + a fire count. ObservedState carries its own copy of
// insts (the pre-write observation the escalation pass iterates).
// Returns the input and pointers the test asserts on.
func expireFixture(insts []workloadtypes.InstanceStatus) (workloadtypes.ReconcileInput, *[]workloadtypes.InstanceStatus, *struct {
	idx    int32
	reason string
	count  int
}) {
	store := append([]workloadtypes.InstanceStatus(nil), insts...)
	var event struct {
		idx    int32
		reason string
		count  int
	}
	input := workloadtypes.ReconcileInput{
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
			for i := range store {
				if store[i].Index == idx {
					mutate(&store[i])
					return nil
				}
			}
			return nil
		},
		WarnInstanceFailed: func(idx int32, _, reason string) {
			event.idx = idx
			event.reason = reason
			event.count++
		},
	}
	input.ObservedState.InstanceStatuses = append([]workloadtypes.InstanceStatus(nil), insts...)
	return input, &store, &event
}

// runEscalationPass drives the workload escalation pass with
// pre-bucketed pods, mirroring how Reconcile invokes it at the end of
// the pass pipeline. Instances come from input.ObservedState; the
// stuck-pod grace from input.StuckPodGrace; the disposition config from
// input.Disposition + plan.MigrationMode; desired pod counts from
// plan.Instances.
func runEscalationPass(t *testing.T, deps workloadtypes.Deps, input workloadtypes.ReconcileInput, plan workloadtypes.ComponentPlan, byIdx map[int32][]*corev1.Pod) error {
	t.Helper()
	return escalation.EscalateFromEvidenceForTest(context.Background(), deps, input, plan, nil, byIdx)
}

// singleInstancePlan returns a ComponentPlan whose desired pod count for
// instance idx is pods — the DesiredPodCountByInstance input the pass
// derives disposable-vs-gang routing from.
func singleInstancePlan(idx, pods int32) workloadtypes.ComponentPlan {
	return workloadtypes.ComponentPlan{
		Instances: []workloadtypes.InstancePlan{{Index: idx, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: pods}}}},
	}
}

// TestExpireOperations_ExpiredDeadlineFailsInstance pins the gang-path
// contract: a gang-surge Instance (Update Operation with a SurgeIndex)
// whose Operation.Deadline is in the past flips to Phase=Failed via
// MutateInstance with the Operation PRESERVED (the Failed-with-Operation
// continuation is what routes the dispatcher into the gang abandon
// path), fires WarnInstanceFailed once, and records a DeadlineExceeded
// LastFailure. Single-pod Update / Create expiries take the deadline
// disposition instead — see the disposition tests.
func TestExpireOperations_ExpiredDeadlineFailsInstance(t *testing.T) {
	now := time.Now()
	surgeIdx := int32(2)
	insts := []workloadtypes.InstanceStatus{{
		Index: 0,
		Phase: workloadtypes.InstancePhaseUpdating,
		Operation: &workloadtypes.InstanceOperation{
			Type:       workloadtypes.InstanceOperationUpdate,
			Step:       "Surge",
			SurgeIndex: &surgeIdx,
			Deadline:   metav1.NewTime(now.Add(-1 * time.Minute)),
		},
	}}
	input, store, event := expireFixture(insts)

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{}, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := (*store)[0]
	if got.Phase != workloadtypes.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if got.LastFailure == nil || got.LastFailure.Reason != escalation.DeadlineExceededReason {
		t.Errorf("LastFailure: got %+v want Reason=%s", got.LastFailure, escalation.DeadlineExceededReason)
	}
	// Operation preserved so operators see what was in flight AND the
	// gang abandon path can consume the continuation.
	if got.Operation == nil {
		t.Errorf("Operation should be preserved on the failed gang Instance")
	}
	if event.count != 1 {
		t.Errorf("event count: got %d want 1", event.count)
	}
}

// TestExpireOperations_DeadlineKeepsWorkloadCausedPodEvidence pins the
// evidence contract of the gang deadline stamp: when a blamed (surge
// bucket) pod shows a workload-caused waiting reason at the deadline,
// LastFailure records that pod's reason — the gang abandon charges the
// revision's retry ladder on it once the pods are gone — while pods
// without such a reason leave the bare DeadlineExceeded record.
func TestExpireOperations_DeadlineKeepsWorkloadCausedPodEvidence(t *testing.T) {
	now := time.Now()
	surgeIdx := int32(2)
	gangSurgeSource := func() []workloadtypes.InstanceStatus {
		return []workloadtypes.InstanceStatus{{
			Index: 0,
			Phase: workloadtypes.InstancePhaseUpdating,
			Operation: &workloadtypes.InstanceOperation{
				Type:       workloadtypes.InstanceOperationUpdate,
				Step:       "Surge",
				SurgeIndex: &surgeIdx,
				Deadline:   metav1.NewTime(now.Add(-1 * time.Minute)),
			},
		}}
	}
	waiting := func(name, reason string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
			Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}},
			}}},
		}
	}

	t.Run("workload-caused surge pod outranks the timeout", func(t *testing.T) {
		input, store, event := expireFixture(gangSurgeSource())
		byIdx := map[int32][]*corev1.Pod{surgeIdx: {
			waiting("engine-2-leader-0", "ContainerCreating"),
			waiting("engine-2-worker-0", "ImagePullBackOff"),
		}}
		if err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{}, byIdx); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := (*store)[0]
		if got.Phase != workloadtypes.InstancePhaseFailed || got.Operation == nil {
			t.Fatalf("got (phase=%q, op=%+v) want (Failed, Operation preserved)", got.Phase, got.Operation)
		}
		if got.LastFailure == nil || got.LastFailure.Reason != "ImagePullBackOff" || got.LastFailure.PodName != "engine-2-worker-0" {
			t.Errorf("LastFailure: got %+v want Reason=ImagePullBackOff PodName=engine-2-worker-0", got.LastFailure)
		}
		if event.count != 1 {
			t.Errorf("event count: got %d want 1", event.count)
		}
	})

	t.Run("pods without a workload-caused reason keep DeadlineExceeded", func(t *testing.T) {
		input, store, _ := expireFixture(gangSurgeSource())
		byIdx := map[int32][]*corev1.Pod{surgeIdx: {
			waiting("engine-2-leader-0", "ContainerCreating"),
			waiting("engine-2-worker-0", "CrashLoopBackOff"),
		}}
		if err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{}, byIdx); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := (*store)[0]
		if got.LastFailure == nil || got.LastFailure.Reason != escalation.DeadlineExceededReason {
			t.Errorf("LastFailure: got %+v want Reason=%s", got.LastFailure, escalation.DeadlineExceededReason)
		}
	})
}

// TestExpireOperations_NotYetExpiredUntouched pins the negative case: a
// transient-phase Instance whose Deadline is still in the future is left
// alone (no Phase change, no event).
func TestExpireOperations_NotYetExpiredUntouched(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index: 0,
		Phase: workloadtypes.InstancePhaseUpdating,
		Operation: &workloadtypes.InstanceOperation{
			Type:     workloadtypes.InstanceOperationUpdate,
			Step:     "Surge",
			Deadline: metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}}
	input, store, event := expireFixture(insts)

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{}, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if (*store)[0].Phase != workloadtypes.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating (untouched)", (*store)[0].Phase)
	}
	if event.count != 0 {
		t.Errorf("event count: got %d want 0 (not yet expired)", event.count)
	}
}

// TestExpireOperations_ZeroDeadlineNeverExpires pins that an unset
// (zero) Deadline is treated as "never expires" so an Instance whose
// per-op writer didn't stamp a deadline can't accidentally trip.
func TestExpireOperations_ZeroDeadlineNeverExpires(t *testing.T) {
	insts := []workloadtypes.InstanceStatus{{
		Index: 0,
		Phase: workloadtypes.InstancePhaseCreating,
		Operation: &workloadtypes.InstanceOperation{
			Type: workloadtypes.InstanceOperationCreate,
			Step: "Create",
			// Deadline left as the metav1.Time zero value.
		},
	}}
	input, store, event := expireFixture(insts)

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{}, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if (*store)[0].Phase != workloadtypes.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (zero deadline never expires)", (*store)[0].Phase)
	}
	if event.count != 0 {
		t.Errorf("event count: got %d want 0", event.count)
	}
}

// TestExpireOperations_AlreadyFailedNoOp pins idempotency: an already-
// Failed Instance must not re-fire the event even with an expired
// deadline still in front of it.
func TestExpireOperations_AlreadyFailedNoOp(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index: 0,
		Phase: workloadtypes.InstancePhaseFailed,
		Operation: &workloadtypes.InstanceOperation{
			Type:     workloadtypes.InstanceOperationUpdate,
			Deadline: metav1.NewTime(now.Add(-1 * time.Minute)),
		},
	}}
	mutated := false
	input := workloadtypes.ReconcileInput{
		MutateInstance: func(_ context.Context, _ int32, _ func(*workloadtypes.InstanceStatus) bool) error {
			mutated = true
			return nil
		},
		WarnInstanceFailed: func(_ int32, _, _ string) {
			t.Errorf("WarnInstanceFailed must not fire on an already-Failed Instance")
		},
	}
	input.ObservedState.InstanceStatuses = insts

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{}, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}
	if mutated {
		t.Errorf("MutateInstance must not be called for an already-Failed Instance")
	}
}

// TestExpireOperations_TerminalPhaseUntouched pins that Instances in
// terminal / non-recovery phases (Ready, Deleting) are skipped even with
// an expired deadline — the timeout only bounds in-flight transient ops.
func TestExpireOperations_TerminalPhaseUntouched(t *testing.T) {
	now := time.Now()
	expired := metav1.NewTime(now.Add(-1 * time.Minute))
	for _, phase := range []workloadtypes.InstancePhase{workloadtypes.InstancePhaseReady, workloadtypes.InstancePhaseDeleting} {
		insts := []workloadtypes.InstanceStatus{{
			Index:     0,
			Phase:     phase,
			Operation: &workloadtypes.InstanceOperation{Type: workloadtypes.InstanceOperationDelete, Deadline: expired},
		}}
		input, store, event := expireFixture(insts)
		if err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{}, nil); err != nil {
			t.Fatalf("escalation pass (%s): %v", phase, err)
		}
		if (*store)[0].Phase != phase {
			t.Errorf("phase %s: got %q want untouched", phase, (*store)[0].Phase)
		}
		if event.count != 0 {
			t.Errorf("phase %s: event count got %d want 0", phase, event.count)
		}
	}
}

// TestExpireOperations_MigratePairSkipped pins the migration-authority
// contract: instances carrying a Migrate Operation — the source
// (Phase=Migrating) AND the surge (Phase=Creating) — are skipped
// entirely even with an expired Operation.Deadline. Their fate belongs
// to the owner's status.migrations record, consumed by the dispatcher's
// migration-expiry pass; stamping the pair Failed here would mark a
// healthy serving source Failed while the record keeps driving the
// migration.
func TestExpireOperations_MigratePairSkipped(t *testing.T) {
	now := time.Now()
	expired := metav1.NewTime(now.Add(-1 * time.Minute))
	surgeIdx := int32(1)
	sourceIdx := int32(0)
	insts := []workloadtypes.InstanceStatus{
		{
			Index: 0,
			Phase: workloadtypes.InstancePhaseMigrating,
			Operation: &workloadtypes.InstanceOperation{
				Type:        workloadtypes.InstanceOperationMigrate,
				Step:        "CreateSurge",
				RequestUUID: "mig-1",
				SurgeIndex:  &surgeIdx,
				Deadline:    expired,
			},
		},
		{
			Index: 1,
			Phase: workloadtypes.InstancePhaseCreating,
			Operation: &workloadtypes.InstanceOperation{
				Type:        workloadtypes.InstanceOperationMigrate,
				Step:        "CreateSurge",
				RequestUUID: "mig-1",
				SurgeIndex:  &sourceIdx,
				Deadline:    expired,
			},
		},
	}
	input, store, event := expireFixture(insts)

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{}, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if got := (*store)[0]; got.Phase != workloadtypes.InstancePhaseMigrating || got.Operation == nil {
		t.Errorf("migration source must be untouched; got phase=%q op=%+v", got.Phase, got.Operation)
	}
	if got := (*store)[1]; got.Phase != workloadtypes.InstancePhaseCreating || got.Operation == nil {
		t.Errorf("migration surge must be untouched; got phase=%q op=%+v", got.Phase, got.Operation)
	}
	if event.count != 0 {
		t.Errorf("event count: got %d want 0 (Migrate pairs are entry-owned)", event.count)
	}
}

// TestExpireOperations_DeleteOperationSkipped pins Delete's ownership of
// terminal handling even when a stale observation carries a transient phase.
// DeleteBatch, rather than generic deadline escalation, decides when that
// durable operation is complete.
func TestExpireOperations_DeleteOperationSkipped(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index: 0,
		Phase: workloadtypes.InstancePhaseRestarting,
		Operation: &workloadtypes.InstanceOperation{
			Type:     workloadtypes.InstanceOperationDelete,
			Step:     "Drain",
			Deadline: metav1.NewTime(now.Add(-time.Minute)),
		},
	}}
	input, store, event := expireFixture(insts)

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{}, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := (*store)[0]
	if got.Phase != workloadtypes.InstancePhaseRestarting || got.Operation == nil || got.Operation.Type != workloadtypes.InstanceOperationDelete {
		t.Errorf("Delete operation must be untouched; got phase=%q op=%+v", got.Phase, got.Operation)
	}
	if event.count != 0 {
		t.Errorf("event count: got %d want 0", event.count)
	}
}

// TestExpireOperations_NoOperationUntouched pins that an Instance with no
// in-flight Operation is skipped (nothing to time out).
func TestExpireOperations_NoOperationUntouched(t *testing.T) {
	insts := []workloadtypes.InstanceStatus{{Index: 0, Phase: workloadtypes.InstancePhaseUpdating}}
	input, store, event := expireFixture(insts)
	if err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{}, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}
	if (*store)[0].Phase != workloadtypes.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating (no operation)", (*store)[0].Phase)
	}
	if event.count != 0 {
		t.Errorf("event count: got %d want 0", event.count)
	}
}

func TestReconcileGatedDeadlines_PausedHoldParksAndRearms(t *testing.T) {
	now := time.Now()
	held := []workloadtypes.InstanceStatus{{
		Index: 0,
		Phase: workloadtypes.InstancePhaseUpdating,
		Operation: &workloadtypes.InstanceOperation{
			Type:     workloadtypes.InstanceOperationUpdate,
			Step:     workloadtypes.UpdateStepSurge,
			Waiting:  workloadtypes.WaitingReasonPaused,
			Deadline: metav1.NewTime(now.Add(3 * time.Minute)),
		},
	}}
	input, store, _ := expireFixture(held)
	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, held, nil, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines (held): %v", err)
	}
	if !(*store)[0].Operation.Deadline.IsZero() {
		t.Errorf("held Deadline: got %v want zero (parked)", (*store)[0].Operation.Deadline)
	}

	released := []workloadtypes.InstanceStatus{{
		Index: 0,
		Phase: workloadtypes.InstancePhaseUpdating,
		Operation: &workloadtypes.InstanceOperation{
			Type: workloadtypes.InstanceOperationUpdate,
			Step: workloadtypes.UpdateStepSurge,
		},
	}}
	input, store, _ = expireFixture(released)
	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, released, nil, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines (released): %v", err)
	}
	d := (*store)[0].Operation.Deadline
	if d.IsZero() || !d.Time.After(now.Add(25*time.Minute)) {
		t.Errorf("released Deadline %v: want a full window from the unpause", d)
	}
}

// TestReconcileGatedDeadlines_ParksGatedInstance: while an Instance's pods
// are admission-gated, its Operation.Deadline is parked (zeroed → "never
// expires") so the gated wait cannot count against InstanceReadyTimeout.
// The Instance is NOT failed — it is queued, not stuck.
func TestReconcileGatedDeadlines_ParksGatedInstance(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index: 0,
		Phase: workloadtypes.InstancePhaseCreating,
		Operation: &workloadtypes.InstanceOperation{
			Type:     workloadtypes.InstanceOperationCreate,
			Deadline: metav1.NewTime(now.Add(-1 * time.Minute)), // already past
		},
	}}
	input, store, _ := expireFixture(insts)

	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{0: true}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.IsZero() {
		t.Errorf("gated Deadline: got %v want zero (parked)", (*store)[0].Operation.Deadline)
	}
	if (*store)[0].Phase != workloadtypes.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (paused, not failed)", (*store)[0].Phase)
	}
}

// TestReconcileGatedDeadlines_RestartsOnUngate: once the pods clear the
// gate, a parked (zero) Deadline is (re)started as now+timeout — i.e. the
// timeout is measured from admission, not from operation start.
func TestReconcileGatedDeadlines_RestartsOnUngate(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index: 0,
		Phase: workloadtypes.InstancePhaseCreating,
		Operation: &workloadtypes.InstanceOperation{
			Type: workloadtypes.InstanceOperationCreate,
			// Deadline zero: parked while previously gated.
		},
	}}
	input, store, _ := expireFixture(insts)

	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
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

// TestReconcileGatedDeadlines_UngatedNormalUntouched: an un-gated Instance
// with a live (non-zero) Deadline is left exactly as the per-op writer
// stamped it.
func TestReconcileGatedDeadlines_UngatedNormalUntouched(t *testing.T) {
	now := time.Now()
	orig := metav1.NewTime(now.Add(20 * time.Minute))
	insts := []workloadtypes.InstanceStatus{{
		Index:     0,
		Phase:     workloadtypes.InstancePhaseCreating,
		Operation: &workloadtypes.InstanceOperation{Type: workloadtypes.InstanceOperationCreate, Deadline: orig},
	}}
	input, store, _ := expireFixture(insts)

	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
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
	insts := []workloadtypes.InstanceStatus{{
		Index:     0,
		Phase:     workloadtypes.InstancePhaseReady,
		Operation: &workloadtypes.InstanceOperation{Type: workloadtypes.InstanceOperationCreate, Deadline: orig},
	}}
	input, store, _ := expireFixture(insts)

	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{0: true}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.Equal(&orig) {
		t.Errorf("non-transient Deadline changed: got %v want untouched", (*store)[0].Operation.Deadline)
	}
}

// TestGatedInstance_NotFailedByDeadlineBackstop: a gated Instance long past
// its original deadline must survive the deadline backstop — pause (parks
// the clock) then expire (sees the parked zero, skips). Without the pause
// step the backstop would fail it.
func TestGatedInstance_NotFailedByDeadlineBackstop(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index: 0,
		Phase: workloadtypes.InstancePhaseCreating,
		Operation: &workloadtypes.InstanceOperation{
			Type:     workloadtypes.InstanceOperationCreate,
			Deadline: metav1.NewTime(now.Add(-1 * time.Hour)), // long past
		},
	}}
	input, store, event := expireFixture(insts)

	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{0: true}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	// Rebuild the observation from the parked store, mirroring the fresh
	// status read the next reconcile's escalation pass consumes.
	input.ObservedState.InstanceStatuses = append([]workloadtypes.InstanceStatus(nil), (*store)...)
	if err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{}, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if (*store)[0].Phase == workloadtypes.InstancePhaseFailed {
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
	refusedAt := metav1.NewTime(now)
	insts := []workloadtypes.InstanceStatus{{
		Index: 0,
		Phase: workloadtypes.InstancePhaseCreating,
		Operation: &workloadtypes.InstanceOperation{
			Type:              workloadtypes.InstanceOperationCreate,
			Deadline:          original,
			Waiting:           workloadtypes.RejectionReasonQuotaExceeded,
			CapacityRefusedAt: &refusedAt,
		},
	}}
	input, store, _ := expireFixture(insts)

	// No pod is gated — the Operation's waiting reason is the only signal.
	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.IsZero() {
		t.Fatalf("quota-blocked Deadline: got %v want zero (parked)", (*store)[0].Operation.Deadline)
	}
	if (*store)[0].Phase != workloadtypes.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (waiting on capacity, not failed)", (*store)[0].Phase)
	}

	// The block clears (a create succeeded, so the create path retired
	// the recorded refusal): the clock restarts from that moment, not
	// from the operation's original start.
	(*store)[0].Operation.Waiting = ""
	(*store)[0].Operation.CapacityRefusedAt = nil
	cleared := append([]workloadtypes.InstanceStatus(nil), (*store)...)
	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, cleared, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines after unblock: %v", err)
	}
	d := (*store)[0].Operation.Deadline
	if d.IsZero() || !d.Time.After(original.Time) {
		t.Errorf("re-armed Deadline: got %v want a fresh now+timeout (later than the original %v)", d, original)
	}
}

// TestReconcileGatedDeadlines_QuotaTokenAloneParks: the token is written
// from the record one pass after the refusal and outlives it by one pass
// once a create lands. Either carrier parks, so a row still reporting
// QuotaExceeded is not re-armed on the pass the record was retired.
func TestReconcileGatedDeadlines_QuotaTokenAloneParks(t *testing.T) {
	insts := []workloadtypes.InstanceStatus{{
		Index: 0,
		Phase: workloadtypes.InstancePhaseCreating,
		Operation: &workloadtypes.InstanceOperation{
			Type:    workloadtypes.InstanceOperationCreate,
			Waiting: workloadtypes.RejectionReasonQuotaExceeded,
		},
	}}
	input, store, _ := expireFixture(insts)
	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.IsZero() {
		t.Errorf("Deadline: got %v want still parked while the row reports the wait", (*store)[0].Operation.Deadline)
	}
}

// TestReconcileGatedDeadlines_QuotaBlockedStaysParkedWhileBlocked: parking
// is edge-triggered, so a still-blocked instance is not rewritten every
// pass — a workload queued behind quota for hours must not churn status.
func TestReconcileGatedDeadlines_QuotaBlockedStaysParkedWhileBlocked(t *testing.T) {
	refusedAt := metav1.NewTime(time.Now())
	insts := []workloadtypes.InstanceStatus{{
		Index: 0,
		Phase: workloadtypes.InstancePhaseCreating,
		Operation: &workloadtypes.InstanceOperation{
			Type:              workloadtypes.InstanceOperationCreate,
			Waiting:           workloadtypes.RejectionReasonQuotaExceeded,
			CapacityRefusedAt: &refusedAt,
		},
	}}
	input, store, _ := expireFixture(insts)

	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
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
	refusedAt := metav1.NewTime(time.Now())
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index: 0,
		Phase: workloadtypes.InstancePhaseCreating,
		Operation: &workloadtypes.InstanceOperation{
			Type:              workloadtypes.InstanceOperationCreate,
			Deadline:          metav1.NewTime(now.Add(-1 * time.Hour)), // stamped before the block
			Waiting:           workloadtypes.RejectionReasonQuotaExceeded,
			CapacityRefusedAt: &refusedAt,
		},
	}}
	input, store, event := expireFixture(insts)

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1), nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}
	if (*store)[0].Phase == workloadtypes.InstancePhaseFailed {
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
	refusedAt := metav1.NewTime(time.Now())
	now := time.Now()
	surgeIndex := int32(2)
	insts := []workloadtypes.InstanceStatus{
		{
			Index: 0,
			Phase: workloadtypes.InstancePhaseUpdating,
			Operation: &workloadtypes.InstanceOperation{
				Type:       workloadtypes.InstanceOperationUpdate,
				Deadline:   metav1.NewTime(now.Add(-1 * time.Hour)), // stamped before the block
				SurgeIndex: &surgeIndex,
			},
		},
		{
			Index: surgeIndex,
			Phase: workloadtypes.InstancePhaseCreating,
			Operation: &workloadtypes.InstanceOperation{
				Type:              workloadtypes.InstanceOperationUpdate,
				Waiting:           workloadtypes.RejectionReasonQuotaExceeded,
				CapacityRefusedAt: &refusedAt,
			},
		},
	}
	input, store, event := expireFixture(insts)

	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.IsZero() {
		t.Errorf("source deadline: got %v want parked while its surge waits on quota", (*store)[0].Operation.Deadline)
	}

	// Escalation must skip the source too, covering the window before the
	// park lands (or after a restart re-reads a pre-park deadline).
	input.ObservedState.InstanceStatuses = append([]workloadtypes.InstanceStatus(nil), insts...)
	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 2), nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}
	if (*store)[0].Phase == workloadtypes.InstancePhaseFailed {
		t.Errorf("source was failed by the deadline backstop; want alive (its surge is queued behind capacity)")
	}
	if event.count != 0 {
		t.Errorf("event count: got %d want 0", event.count)
	}
}
