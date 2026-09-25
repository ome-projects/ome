package ops

import (
	"context"
	"testing"
	"time"

	clocktesting "k8s.io/utils/clock/testing"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// rejectionDisposeRecorders collects every observable side effect of one
// DisposeAPIRejection call: the committed instance mutations and the
// workload.RetryBlock writes.
type rejectionDisposeRecorders struct {
	commits []workload.InstanceStatus
	blocks  []struct {
		rev   string
		block workload.RetryBlock
	}
}

// rejectionDisposeInput wires the workload.ReconcileInput closure recorders over
// insts. updateRevision seeds workload.ObservedState.UpdateRevision, the fallback
// target for an unpinned Create.
func rejectionDisposeInput(now time.Time, insts *[]workload.InstanceStatus, updateRevision string) (workload.ReconcileInput, *rejectionDisposeRecorders) {
	rec := &rejectionDisposeRecorders{}
	input := workload.ReconcileInput{
		Clock: clocktesting.NewFakeClock(now),
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
			for i := range *insts {
				if (*insts)[i].Index == idx {
					if mutate(&(*insts)[i]) {
						rec.commits = append(rec.commits, (*insts)[i])
					}
					return nil
				}
			}
			return nil
		},
		MutateRetryBlock: func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
			b := workload.RetryBlock{TargetRevision: rev}
			if d := mutate(&b); d != workload.RetryBlockUnchanged {
				rec.blocks = append(rec.blocks, struct {
					rev   string
					block workload.RetryBlock
				}{rev: rev, block: b})
			}
			return nil
		},
	}
	input.ObservedState.UpdateRevision = updateRevision
	return input, rec
}

// TestDisposeAPIRejection_PermanentWorkload: a 422 Invalid blames the pod
// template, so the attempt's target revision is held for retry AND the
// Operation is cleared + Phase=Failed in ONE mutation, with LastFailure
// carrying the apiserver's own message.
func TestDisposeAPIRejection_PermanentWorkload(t *testing.T) {
	t0 := time.Date(2026, 8, 3, 9, 0, 0, 0, time.UTC)
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseCreating,
		Operation: &workload.InstanceOperation{
			Type:           workload.InstanceOperationCreate,
			TargetRevision: "own-engine-badspec",
		},
	}}
	input, rec := rejectionDisposeInput(t0, &insts, "own-engine-badspec")
	rejection := workload.APIRejection{
		Class:   workload.APIRejectionPermanentWorkload,
		Reason:  workload.RejectionReasonInvalidPodSpec,
		Message: `Pod "engine-0-default-0" is invalid: spec.containers[0].resources.limits: Invalid value: "-1"`,
	}

	held, err := disposeAPIRejection(context.Background(), input, insts[0], rejection, true)
	if err != nil {
		t.Fatalf("DisposeAPIRejection: %v", err)
	}
	if held != "own-engine-badspec" {
		t.Fatalf("held revision: got %q want own-engine-badspec", held)
	}
	if len(rec.commits) != 1 {
		t.Fatalf("MutateInstance commits: got %d want 1", len(rec.commits))
	}
	after := rec.commits[0]
	if after.Phase != workload.InstancePhaseFailed || after.Operation != nil {
		t.Errorf("commit: got phase=%q op=%+v want Failed with the operation cleared", after.Phase, after.Operation)
	}
	if after.LastFailure == nil ||
		after.LastFailure.Reason != workload.RejectionReasonInvalidPodSpec ||
		after.LastFailure.Message != rejection.Message {
		t.Errorf("LastFailure: got %+v want reason=%s with the apiserver message", after.LastFailure, workload.RejectionReasonInvalidPodSpec)
	}
	if len(rec.blocks) != 1 || rec.blocks[0].rev != "own-engine-badspec" {
		t.Fatalf("workload.RetryBlock writes: got %+v want one for own-engine-badspec", rec.blocks)
	}
	if rec.blocks[0].block.State != workload.RetryBlockHeld {
		t.Errorf("block state: got %q want Held (nil policy exhausts immediately)", rec.blocks[0].block.State)
	}
}

// TestDisposeAPIRejection_FallsBackToUpdateRevision: an unpinned Create
// carries no TargetRevision, so the block lands on the owner's
// UpdateRevision — the revision the rejected pod was rendered from.
func TestDisposeAPIRejection_FallsBackToUpdateRevision(t *testing.T) {
	t0 := time.Date(2026, 8, 3, 9, 0, 0, 0, time.UTC)
	insts := []workload.InstanceStatus{{
		Index:     0,
		Phase:     workload.InstancePhaseCreating,
		Operation: &workload.InstanceOperation{Type: workload.InstanceOperationCreate},
	}}
	input, rec := rejectionDisposeInput(t0, &insts, "own-engine-update")

	held, err := disposeAPIRejection(context.Background(), input, insts[0], workload.APIRejection{
		Class:   workload.APIRejectionPermanentWorkload,
		Reason:  workload.RejectionReasonInvalidPodSpec,
		Message: "invalid pod spec",
	}, true)
	if err != nil {
		t.Fatalf("DisposeAPIRejection: %v", err)
	}
	if held != "own-engine-update" {
		t.Fatalf("held revision: got %q want own-engine-update", held)
	}
	if len(rec.blocks) != 1 || rec.blocks[0].rev != "own-engine-update" {
		t.Fatalf("workload.RetryBlock writes: got %+v want one for own-engine-update", rec.blocks)
	}
}

// TestDisposeAPIRejection_PermanentEnvironment: a namespace-terminating
// refusal is not the revision's fault — the instance is failed with the
// NamespaceTerminating reason and NO workload.RetryBlock, so a later namespace
// carries a clean retry ladder.
func TestDisposeAPIRejection_PermanentEnvironment(t *testing.T) {
	t0 := time.Date(2026, 8, 3, 9, 0, 0, 0, time.UTC)
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseCreating,
		Operation: &workload.InstanceOperation{
			Type:           workload.InstanceOperationCreate,
			TargetRevision: "own-engine-good",
		},
	}}
	input, rec := rejectionDisposeInput(t0, &insts, "own-engine-good")

	held, err := disposeAPIRejection(context.Background(), input, insts[0], workload.APIRejection{
		Class:   workload.APIRejectionPermanentEnvironment,
		Reason:  workload.RejectionReasonNamespaceTerminating,
		Message: "unable to create new content in namespace prod because it is being terminated",
	}, true)
	if err != nil {
		t.Fatalf("DisposeAPIRejection: %v", err)
	}
	if held != "" {
		t.Fatalf("held revision: got %q want empty (the revision is blameless)", held)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("workload.RetryBlock writes: got %+v want none", rec.blocks)
	}
	if len(rec.commits) != 1 {
		t.Fatalf("MutateInstance commits: got %d want 1", len(rec.commits))
	}
	after := rec.commits[0]
	if after.Phase != workload.InstancePhaseFailed || after.Operation != nil {
		t.Errorf("commit: got phase=%q op=%+v want Failed with the operation cleared", after.Phase, after.Operation)
	}
	if after.LastFailure == nil || after.LastFailure.Reason != workload.RejectionReasonNamespaceTerminating {
		t.Errorf("LastFailure: got %+v want reason=%s", after.LastFailure, workload.RejectionReasonNamespaceTerminating)
	}
}

// TestDisposeAPIRejection_NonPermanentIsNoOp: the transient classes carry
// their own pacing at the call site — routing one here must not fail an
// instance that is still legitimately in flight.
func TestDisposeAPIRejection_NonPermanentIsNoOp(t *testing.T) {
	t0 := time.Date(2026, 8, 3, 9, 0, 0, 0, time.UTC)
	for _, class := range []workload.APIRejectionClass{
		workload.APIRejectionTransient, workload.APIRejectionThrottled, workload.APIRejectionCapacityBlocked,
	} {
		insts := []workload.InstanceStatus{{
			Index:     0,
			Phase:     workload.InstancePhaseCreating,
			Operation: &workload.InstanceOperation{Type: workload.InstanceOperationCreate, TargetRevision: "rev-x"},
		}}
		input, rec := rejectionDisposeInput(t0, &insts, "rev-x")

		held, err := disposeAPIRejection(context.Background(), input, insts[0], workload.APIRejection{Class: class, Reason: "Whatever"}, true)
		if err != nil {
			t.Fatalf("class %v: DisposeAPIRejection: %v", class, err)
		}
		if held != "" || len(rec.commits) != 0 || len(rec.blocks) != 0 {
			t.Errorf("class %v: got held=%q commits=%+v blocks=%+v want nothing disposed", class, held, rec.commits, rec.blocks)
		}
	}
}

// TestDisposeAPIRejection_BlameWithheld: a caller whose rejected pod is
// not a faithful render of the target revision (a migration surge carries
// a placement overlay the revision never asked for) withholds the blame.
// The attempt still ends — the pod will never be admitted — but the
// revision keeps a clean retry ladder.
func TestDisposeAPIRejection_BlameWithheld(t *testing.T) {
	t0 := time.Date(2026, 8, 3, 9, 0, 0, 0, time.UTC)
	insts := []workload.InstanceStatus{{
		Index: 0,
		Phase: workload.InstancePhaseCreating,
		Operation: &workload.InstanceOperation{
			Type:           workload.InstanceOperationMigrate,
			TargetRevision: "own-engine-good",
		},
	}}
	input, rec := rejectionDisposeInput(t0, &insts, "own-engine-good")

	held, err := disposeAPIRejection(context.Background(), input, insts[0], workload.APIRejection{
		Class:   workload.APIRejectionPermanentWorkload,
		Reason:  workload.RejectionReasonInvalidPodSpec,
		Message: "invalid pod spec",
	}, false)
	if err != nil {
		t.Fatalf("DisposeAPIRejection: %v", err)
	}
	if held != "" {
		t.Errorf("held revision: got %q want empty (blame withheld)", held)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("workload.RetryBlock writes: got %+v want none", rec.blocks)
	}
	if len(rec.commits) != 1 || rec.commits[0].Phase != workload.InstancePhaseFailed {
		t.Errorf("commit: got %+v want the attempt still ended Failed", rec.commits)
	}
}
