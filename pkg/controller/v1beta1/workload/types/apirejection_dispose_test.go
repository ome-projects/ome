package types

import (
	"context"
	"testing"
	"time"

	clocktesting "k8s.io/utils/clock/testing"
)

// rejectionDisposeRecorders collects every observable side effect of one
// DisposeAPIRejection call: the committed instance mutations and the
// RetryBlock writes.
type rejectionDisposeRecorders struct {
	commits []InstanceStatus
	blocks  []struct {
		rev   string
		block RetryBlock
	}
}

// rejectionDisposeInput wires the ReconcileInput closure recorders over
// insts. updateRevision seeds ObservedState.UpdateRevision, the fallback
// target for an unpinned Create.
func rejectionDisposeInput(now time.Time, insts *[]InstanceStatus, updateRevision string) (ReconcileInput, *rejectionDisposeRecorders) {
	rec := &rejectionDisposeRecorders{}
	input := ReconcileInput{
		Clock: clocktesting.NewFakeClock(now),
		MutateInstance: func(_ context.Context, idx int32, mutate func(*InstanceStatus) bool) error {
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
		MutateRetryBlock: func(_ context.Context, rev string, mutate func(*RetryBlock) RetryBlockDisposition) error {
			b := RetryBlock{TargetRevision: rev}
			if d := mutate(&b); d != RetryBlockUnchanged {
				rec.blocks = append(rec.blocks, struct {
					rev   string
					block RetryBlock
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
	insts := []InstanceStatus{{
		Index: 0,
		Phase: InstancePhaseCreating,
		Operation: &InstanceOperation{
			Type:           InstanceOperationCreate,
			TargetRevision: "own-engine-badspec",
		},
	}}
	input, rec := rejectionDisposeInput(t0, &insts, "own-engine-badspec")
	rejection := APIRejection{
		Class:   APIRejectionPermanentWorkload,
		Reason:  RejectionReasonInvalidPodSpec,
		Message: `Pod "engine-0-default-0" is invalid: spec.containers[0].resources.limits: Invalid value: "-1"`,
	}

	held, err := DisposeAPIRejection(context.Background(), input, insts[0], rejection, true)
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
	if after.Phase != InstancePhaseFailed || after.Operation != nil {
		t.Errorf("commit: got phase=%q op=%+v want Failed with the operation cleared", after.Phase, after.Operation)
	}
	if after.LastFailure == nil ||
		after.LastFailure.Reason != RejectionReasonInvalidPodSpec ||
		after.LastFailure.Message != rejection.Message {
		t.Errorf("LastFailure: got %+v want reason=%s with the apiserver message", after.LastFailure, RejectionReasonInvalidPodSpec)
	}
	if len(rec.blocks) != 1 || rec.blocks[0].rev != "own-engine-badspec" {
		t.Fatalf("RetryBlock writes: got %+v want one for own-engine-badspec", rec.blocks)
	}
	if rec.blocks[0].block.State != RetryBlockHeld {
		t.Errorf("block state: got %q want Held (nil policy exhausts immediately)", rec.blocks[0].block.State)
	}
}

// TestDisposeAPIRejection_FallsBackToUpdateRevision: an unpinned Create
// carries no TargetRevision, so the block lands on the owner's
// UpdateRevision — the revision the rejected pod was rendered from.
func TestDisposeAPIRejection_FallsBackToUpdateRevision(t *testing.T) {
	t0 := time.Date(2026, 8, 3, 9, 0, 0, 0, time.UTC)
	insts := []InstanceStatus{{
		Index:     0,
		Phase:     InstancePhaseCreating,
		Operation: &InstanceOperation{Type: InstanceOperationCreate},
	}}
	input, rec := rejectionDisposeInput(t0, &insts, "own-engine-update")

	held, err := DisposeAPIRejection(context.Background(), input, insts[0], APIRejection{
		Class:   APIRejectionPermanentWorkload,
		Reason:  RejectionReasonInvalidPodSpec,
		Message: "invalid pod spec",
	}, true)
	if err != nil {
		t.Fatalf("DisposeAPIRejection: %v", err)
	}
	if held != "own-engine-update" {
		t.Fatalf("held revision: got %q want own-engine-update", held)
	}
	if len(rec.blocks) != 1 || rec.blocks[0].rev != "own-engine-update" {
		t.Fatalf("RetryBlock writes: got %+v want one for own-engine-update", rec.blocks)
	}
}

// TestDisposeAPIRejection_PermanentEnvironment: a namespace-terminating
// refusal is not the revision's fault — the instance is failed with the
// NamespaceTerminating reason and NO RetryBlock, so a later namespace
// carries a clean retry ladder.
func TestDisposeAPIRejection_PermanentEnvironment(t *testing.T) {
	t0 := time.Date(2026, 8, 3, 9, 0, 0, 0, time.UTC)
	insts := []InstanceStatus{{
		Index: 0,
		Phase: InstancePhaseCreating,
		Operation: &InstanceOperation{
			Type:           InstanceOperationCreate,
			TargetRevision: "own-engine-good",
		},
	}}
	input, rec := rejectionDisposeInput(t0, &insts, "own-engine-good")

	held, err := DisposeAPIRejection(context.Background(), input, insts[0], APIRejection{
		Class:   APIRejectionPermanentEnvironment,
		Reason:  RejectionReasonNamespaceTerminating,
		Message: "unable to create new content in namespace prod because it is being terminated",
	}, true)
	if err != nil {
		t.Fatalf("DisposeAPIRejection: %v", err)
	}
	if held != "" {
		t.Fatalf("held revision: got %q want empty (the revision is blameless)", held)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock writes: got %+v want none", rec.blocks)
	}
	if len(rec.commits) != 1 {
		t.Fatalf("MutateInstance commits: got %d want 1", len(rec.commits))
	}
	after := rec.commits[0]
	if after.Phase != InstancePhaseFailed || after.Operation != nil {
		t.Errorf("commit: got phase=%q op=%+v want Failed with the operation cleared", after.Phase, after.Operation)
	}
	if after.LastFailure == nil || after.LastFailure.Reason != RejectionReasonNamespaceTerminating {
		t.Errorf("LastFailure: got %+v want reason=%s", after.LastFailure, RejectionReasonNamespaceTerminating)
	}
}

// TestDisposeAPIRejection_NonPermanentIsNoOp: the transient classes carry
// their own pacing at the call site — routing one here must not fail an
// instance that is still legitimately in flight.
func TestDisposeAPIRejection_NonPermanentIsNoOp(t *testing.T) {
	t0 := time.Date(2026, 8, 3, 9, 0, 0, 0, time.UTC)
	for _, class := range []APIRejectionClass{
		APIRejectionTransient, APIRejectionThrottled, APIRejectionCapacityBlocked,
	} {
		insts := []InstanceStatus{{
			Index:     0,
			Phase:     InstancePhaseCreating,
			Operation: &InstanceOperation{Type: InstanceOperationCreate, TargetRevision: "rev-x"},
		}}
		input, rec := rejectionDisposeInput(t0, &insts, "rev-x")

		held, err := DisposeAPIRejection(context.Background(), input, insts[0], APIRejection{Class: class, Reason: "Whatever"}, true)
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
	insts := []InstanceStatus{{
		Index: 0,
		Phase: InstancePhaseCreating,
		Operation: &InstanceOperation{
			Type:           InstanceOperationMigrate,
			TargetRevision: "own-engine-good",
		},
	}}
	input, rec := rejectionDisposeInput(t0, &insts, "own-engine-good")

	held, err := DisposeAPIRejection(context.Background(), input, insts[0], APIRejection{
		Class:   APIRejectionPermanentWorkload,
		Reason:  RejectionReasonInvalidPodSpec,
		Message: "invalid pod spec",
	}, false)
	if err != nil {
		t.Fatalf("DisposeAPIRejection: %v", err)
	}
	if held != "" {
		t.Errorf("held revision: got %q want empty (blame withheld)", held)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock writes: got %+v want none", rec.blocks)
	}
	if len(rec.commits) != 1 || rec.commits[0].Phase != InstancePhaseFailed {
		t.Errorf("commit: got %+v want the attempt still ended Failed", rec.commits)
	}
}
