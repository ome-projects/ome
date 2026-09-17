package workload_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
)

// expireFixture wires a ReconcileInput whose MutateInstance applies the
// callback to the local insts slice (mirroring the per-Instance status
// the adapter would persist) and whose WarnInstanceFailed records the
// last event + a fire count. ObservedState carries its own copy of
// insts (the pre-write observation the escalation pass iterates).
// Returns the input and pointers the test asserts on.
func expireFixture(insts []workload.InstanceStatus) (workload.ReconcileInput, *[]workload.InstanceStatus, *struct {
	idx    int32
	reason string
	count  int
}) {
	store := append([]workload.InstanceStatus(nil), insts...)
	var event struct {
		idx    int32
		reason string
		count  int
	}
	input := workload.ReconcileInput{
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
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
	input.ObservedState.InstanceStatuses = append([]workload.InstanceStatus(nil), insts...)
	return input, &store, &event
}

// runEscalationPass drives the workload escalation pass with
// pre-bucketed pods, mirroring how Reconcile invokes it at the end of
// the pass pipeline. Instances come from input.ObservedState; the
// stuck-pod grace from input.StuckPodGrace; the disposition config from
// input.Disposition + plan.MigrationMode; desired pod counts from
// plan.Instances.
func runEscalationPass(t *testing.T, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, byIdx map[int32][]*corev1.Pod) error {
	t.Helper()
	return workload.EscalateFromEvidenceForTest(context.Background(), deps, input, plan, nil,
		workload.SnapshotWithPodsForTest(input, byIdx))
}

// singleInstancePlan returns a ComponentPlan whose desired pod count for
// instance idx is pods — the DesiredPodCountByInstance input the pass
// derives disposable-vs-gang routing from.
func singleInstancePlan(idx, pods int32) workload.ComponentPlan {
	return workload.ComponentPlan{
		Instances: []workload.InstancePlan{{Index: idx, Runners: []workload.RunnerPlan{{Name: "default", Size: pods}}}},
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
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseUpdating,
		Operation: &workload.InstanceOperation{
			Type:       workload.InstanceOperationUpdate,
			Step:       "Surge",
			SurgeIndex: &surgeIdx,
			Deadline:   metav1.NewTime(now.Add(-1 * time.Minute)),
		},
	}}
	input, store, event := expireFixture(insts)

	if err := runEscalationPass(t, workload.Deps{}, input, workload.ComponentPlan{}, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := (*store)[0]
	if got.Phase != workload.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if got.LastFailure == nil || got.LastFailure.Reason != workload.DeadlineExceededReason {
		t.Errorf("LastFailure: got %+v want Reason=%s", got.LastFailure, workload.DeadlineExceededReason)
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
	gangSurgeSource := func() []workload.InstanceStatus {
		return []workload.InstanceStatus{{
			Index: 0,
			Phase: workload.InstancePhaseUpdating,
			Operation: &workload.InstanceOperation{
				Type:       workload.InstanceOperationUpdate,
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
		if err := runEscalationPass(t, workload.Deps{}, input, workload.ComponentPlan{}, byIdx); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := (*store)[0]
		if got.Phase != workload.InstancePhaseFailed || got.Operation == nil {
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
		if err := runEscalationPass(t, workload.Deps{}, input, workload.ComponentPlan{}, byIdx); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := (*store)[0]
		if got.LastFailure == nil || got.LastFailure.Reason != workload.DeadlineExceededReason {
			t.Errorf("LastFailure: got %+v want Reason=%s", got.LastFailure, workload.DeadlineExceededReason)
		}
	})
}

// TestExpireOperations_NotYetExpiredUntouched pins the negative case: a
// transient-phase Instance whose Deadline is still in the future is left
// alone (no Phase change, no event).
func TestExpireOperations_NotYetExpiredUntouched(t *testing.T) {
	now := time.Now()
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseUpdating,
		Operation: &workload.InstanceOperation{
			Type:     workload.InstanceOperationUpdate,
			Step:     "Surge",
			Deadline: metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}}
	input, store, event := expireFixture(insts)

	if err := runEscalationPass(t, workload.Deps{}, input, workload.ComponentPlan{}, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if (*store)[0].Phase != workload.InstancePhaseUpdating {
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
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseCreating,
		Operation: &workload.InstanceOperation{
			Type: workload.InstanceOperationCreate,
			Step: "Create",
			// Deadline left as the metav1.Time zero value.
		},
	}}
	input, store, event := expireFixture(insts)

	if err := runEscalationPass(t, workload.Deps{}, input, workload.ComponentPlan{}, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if (*store)[0].Phase != workload.InstancePhaseCreating {
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
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseFailed,
		Operation: &workload.InstanceOperation{
			Type:     workload.InstanceOperationUpdate,
			Deadline: metav1.NewTime(now.Add(-1 * time.Minute)),
		},
	}}
	mutated := false
	input := workload.ReconcileInput{
		MutateInstance: func(_ context.Context, _ int32, _ func(*workload.InstanceStatus) bool) error {
			mutated = true
			return nil
		},
		WarnInstanceFailed: func(_ int32, _, _ string) {
			t.Errorf("WarnInstanceFailed must not fire on an already-Failed Instance")
		},
	}
	input.ObservedState.InstanceStatuses = insts

	if err := runEscalationPass(t, workload.Deps{}, input, workload.ComponentPlan{}, nil); err != nil {
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
	for _, phase := range []workload.InstancePhase{workload.InstancePhaseReady, workload.InstancePhaseDeleting} {
		insts := []workload.InstanceStatus{{
			Index:     0,
			Phase:     phase,
			Operation: &workload.InstanceOperation{Type: workload.InstanceOperationDelete, Deadline: expired},
		}}
		input, store, event := expireFixture(insts)
		if err := runEscalationPass(t, workload.Deps{}, input, workload.ComponentPlan{}, nil); err != nil {
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
	insts := []workload.InstanceStatus{
		{
			Index: 0,
			Phase: workload.InstancePhaseMigrating,
			Operation: &workload.InstanceOperation{
				Type:        workload.InstanceOperationMigrate,
				Step:        "CreateSurge",
				RequestUUID: "mig-1",
				SurgeIndex:  &surgeIdx,
				Deadline:    expired,
			},
		},
		{
			Index: 1,
			Phase: workload.InstancePhaseCreating,
			Operation: &workload.InstanceOperation{
				Type:        workload.InstanceOperationMigrate,
				Step:        "CreateSurge",
				RequestUUID: "mig-1",
				SurgeIndex:  &sourceIdx,
				Deadline:    expired,
			},
		},
	}
	input, store, event := expireFixture(insts)

	if err := runEscalationPass(t, workload.Deps{}, input, workload.ComponentPlan{}, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if got := (*store)[0]; got.Phase != workload.InstancePhaseMigrating || got.Operation == nil {
		t.Errorf("migration source must be untouched; got phase=%q op=%+v", got.Phase, got.Operation)
	}
	if got := (*store)[1]; got.Phase != workload.InstancePhaseCreating || got.Operation == nil {
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
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseRestarting,
		Operation: &workload.InstanceOperation{
			Type:     workload.InstanceOperationDelete,
			Step:     "Drain",
			Deadline: metav1.NewTime(now.Add(-time.Minute)),
		},
	}}
	input, store, event := expireFixture(insts)

	if err := runEscalationPass(t, workload.Deps{}, input, workload.ComponentPlan{}, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := (*store)[0]
	if got.Phase != workload.InstancePhaseRestarting || got.Operation == nil || got.Operation.Type != workload.InstanceOperationDelete {
		t.Errorf("Delete operation must be untouched; got phase=%q op=%+v", got.Phase, got.Operation)
	}
	if event.count != 0 {
		t.Errorf("event count: got %d want 0", event.count)
	}
}

// TestExpireOperations_NoOperationUntouched pins that an Instance with no
// in-flight Operation is skipped (nothing to time out).
func TestExpireOperations_NoOperationUntouched(t *testing.T) {
	insts := []workload.InstanceStatus{{Index: 0, Phase: workload.InstancePhaseUpdating}}
	input, store, event := expireFixture(insts)
	if err := runEscalationPass(t, workload.Deps{}, input, workload.ComponentPlan{}, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}
	if (*store)[0].Phase != workload.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating (no operation)", (*store)[0].Phase)
	}
	if event.count != 0 {
		t.Errorf("event count: got %d want 0", event.count)
	}
}
