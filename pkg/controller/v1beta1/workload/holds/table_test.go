package holds

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestTable_OneOrderedPair: the precedence between authorities is
// first-writer-wins with exactly one exception — a token that reports a
// FACT about the Instance takes a row reporting the operator's decision
// to stop. Nothing else may take an incumbent's row, in either
// direction.
func TestTable_OneOrderedPair(t *testing.T) {
	facts := []string{
		types.RejectionReasonQuotaExceeded,
		types.WaitingReasonUnschedulable,
		types.WaitingReasonPodGroupTerminating,
		types.WaitingReasonNodeUnknown,
		types.WaitingReasonSourceUnrouted,
	}
	for _, token := range facts {
		if !takesRowFrom(token, types.WaitingReasonPaused) {
			t.Errorf("%s must take a paused row", token)
		}
		if !takesRowFrom(token, "") {
			t.Errorf("%s must take an unheld row", token)
		}
		if !takesRowFrom(token, token) {
			t.Errorf("%s must keep its own row", token)
		}
		if takesRowFrom(types.WaitingReasonPaused, token) {
			t.Errorf("the pause must not take a row reporting %s", token)
		}
		for _, other := range facts {
			if other == token {
				continue
			}
			if takesRowFrom(token, other) {
				t.Errorf("%s must not take a row reporting %s", token, other)
			}
		}
	}
}

// TestTable_EveryTokenIsWrittenByExactlyOneAuthority: the table is the
// one home for the six tokens, so a duplicate row — two authorities
// writing one token — is a precedence nobody decides.
func TestTable_EveryTokenIsWrittenByExactlyOneAuthority(t *testing.T) {
	seen := make(map[string]int, len(table))
	for _, a := range table {
		if a.token == "" {
			t.Error("an authority with no token cannot be read off a row")
		}
		if a.mayOwn == nil || a.report == nil {
			t.Errorf("%s: an authority needs both an owner rule and a reading", a.token)
		}
		seen[a.token]++
	}
	for token, count := range seen {
		if count != 1 {
			t.Errorf("%s is written by %d authorities, want 1", token, count)
		}
	}
	if last := table[len(table)-1].token; last != types.WaitingReasonPaused {
		t.Errorf("the weakest authority is consulted last; got %s", last)
	}
}

// TestTable_FactHoldsRefuseATeardown pins the writability column: which row owners
// each token may be recorded on. A teardown is the row every
// fact-bearing authority refuses — DeleteBatch owns a removal's pace and
// its own escalation — and each authority adds what its own wait cannot
// be true of.
func TestTable_FactHoldsRefuseATeardown(t *testing.T) {
	owners := []types.RowOwner{
		types.OwnerNone, types.OwnerCreate, types.OwnerUpdate,
		types.OwnerRestart, types.OwnerMigrate, types.OwnerDelete,
	}
	want := map[string]map[types.RowOwner]bool{
		// Admission can refuse a create on any row, so the refusal is
		// recorded wherever the create site left it.
		types.RejectionReasonQuotaExceeded: {
			types.OwnerNone: true, types.OwnerCreate: true, types.OwnerUpdate: true,
			types.OwnerRestart: true, types.OwnerMigrate: true, types.OwnerDelete: true,
		},
		types.WaitingReasonUnschedulable: {
			types.OwnerCreate: true, types.OwnerUpdate: true,
			types.OwnerRestart: true, types.OwnerMigrate: true,
		},
		types.WaitingReasonNodeUnknown: {
			types.OwnerCreate: true, types.OwnerUpdate: true,
			types.OwnerRestart: true, types.OwnerMigrate: true,
		},
		types.WaitingReasonSourceUnrouted: {
			types.OwnerUpdate: true,
		},
		types.WaitingReasonPodGroupTerminating: {
			types.OwnerNone: true, types.OwnerCreate: true,
			types.OwnerUpdate: true, types.OwnerRestart: true,
		},
		types.WaitingReasonPaused: {
			types.OwnerUpdate: true, types.OwnerMigrate: true,
		},
	}
	plan := types.ComponentPlan{Paused: true}
	for _, a := range table {
		expected, ok := want[a.token]
		if !ok {
			continue
		}
		for _, owner := range owners {
			if got := a.mayOwn(owner, plan); got != expected[owner] {
				t.Errorf("%s.mayOwn(%s) = %v, want %v", a.token, owner, got, expected[owner])
			}
		}
	}
}

// holdFixture wires a ReconcileInput whose MutateInstance applies the
// callback to a single in-memory row that the observation shares,
// mirroring how the adapters fold a committed mutation back into the
// state the rest of the pass reads.
func holdFixture(op *types.InstanceOperation) (types.ReconcileInput, *types.InstanceStatus) {
	statuses := []types.InstanceStatus{{Index: 0, Phase: types.InstancePhaseCreating, Operation: op}}
	row := &statuses[0]
	return types.ReconcileInput{
		ObservedState: types.WorkloadObservedState{InstanceStatuses: statuses},
		MutateInstance: func(_ context.Context, _ int32, mutate func(*types.InstanceStatus) bool) error {
			mutate(row)
			return nil
		},
	}, row
}

// TestExternalHold_OneOwnerPerToken pins the precedence every external
// hold shares: a token is never overwritten by a different authority,
// and it is only released by the authority that set it. Without that,
// the quota path and the scheduler hold silently steal and drop each
// other's token, and the row reports a wait nobody is actually in.
func TestExternalHold_OneOwnerPerToken(t *testing.T) {
	ctx, plan := context.Background(), types.ComponentPlan{}
	input, row := holdFixture(&types.InstanceOperation{Type: types.InstanceOperationCreate})

	entered, held, err := mark(ctx, input, plan, 0, schedulerHold, nil)
	if err != nil || !entered || !held {
		t.Fatalf("mark unschedulable: entered=%v held=%v err=%v want true, true, nil", entered, held, err)
	}
	if row.Operation.Waiting != types.WaitingReasonUnschedulable {
		t.Fatalf("Waiting: got %q want %q", row.Operation.Waiting, types.WaitingReasonUnschedulable)
	}

	// A quota refusal arrives while the scheduler hold stands.
	entered, held, err = mark(ctx, input, plan, 0, quotaHold, nil)
	if err != nil {
		t.Fatalf("mark quota: %v", err)
	}
	if entered || held {
		t.Errorf("mark quota: entered=%v held=%v want false, false (the token has an owner)", entered, held)
	}
	if row.Operation.Waiting != types.WaitingReasonUnschedulable {
		t.Errorf("Waiting: got %q want the scheduler hold untouched", row.Operation.Waiting)
	}

	// The quota path completing must not release someone else's hold.
	if err := release(ctx, input, 0, types.RejectionReasonQuotaExceeded); err != nil {
		t.Fatalf("clear quota: %v", err)
	}
	if row.Operation.Waiting != types.WaitingReasonUnschedulable {
		t.Errorf("Waiting: got %q want the scheduler hold still set", row.Operation.Waiting)
	}

	// Its own owner releases it.
	if err := release(ctx, input, 0, types.WaitingReasonUnschedulable); err != nil {
		t.Fatalf("clear unschedulable: %v", err)
	}
	if row.Operation.Waiting != "" {
		t.Errorf("Waiting: got %q want released", row.Operation.Waiting)
	}
}

// TestExternalHold_EdgeTriggered: re-marking a hold already in place
// reports no entry and writes nothing, so a wait that lasts for hours
// costs one status write. Evidence is compared by which pod is held, why
// and since when — never by the authority's message, which it
// recomputes continuously.
func TestExternalHold_EdgeTriggered(t *testing.T) {
	ctx, plan := context.Background(), types.ComponentPlan{}
	input, row := holdFixture(&types.InstanceOperation{Type: types.InstanceOperationCreate})
	at := metav1.Now()
	evidence := func(message string) *types.InstanceTermination {
		return &types.InstanceTermination{
			PodName: "engine-0-default-0",
			Reason:  types.WaitingReasonUnschedulable,
			Message: message,
			Time:    at,
		}
	}

	if entered, held, err := mark(ctx, input, plan, 0, schedulerHold, evidence("0/3 nodes")); err != nil || !entered || !held {
		t.Fatalf("first mark: entered=%v held=%v err=%v want true, true, nil", entered, held, err)
	}
	if entered, held, err := mark(ctx, input, plan, 0, schedulerHold, evidence("0/9 nodes")); err != nil || entered || !held {
		t.Fatalf("re-mark with a churned message: entered=%v held=%v err=%v want false, true, nil", entered, held, err)
	}
	if got := row.LastFailure; got == nil || got.Message != "0/3 nodes" {
		t.Errorf("LastFailure: got %+v want the message from hold start", got)
	}

	// A same-name pod recreated later starts a fresh hold: the condition
	// transition moved, so this is a new episode.
	fresh := evidence("0/3 nodes")
	fresh.Time = metav1.NewTime(at.Add(1))
	if entered, held, err := mark(ctx, input, plan, 0, schedulerHold, fresh); err != nil || entered || !held {
		t.Fatalf("re-mark at a new transition: entered=%v held=%v err=%v want false, true, nil", entered, held, err)
	}
	if got := row.LastFailure; got == nil || !got.Time.Equal(&fresh.Time) {
		t.Errorf("LastFailure.Time: got %+v want the new hold start", got)
	}
}

// TestExternalHold_OwnerRuleIsRecheckedOnTheFreshRow: the table's owner
// rule is decided from the pass-start observation, which can be stale
// by the time the mutation runs. A row that has since entered teardown
// must not have a hold — or an overwritten LastFailure — stamped onto
// it, so the rule is re-run inside the callback on the row the write
// actually sees.
func TestExternalHold_OwnerRuleIsRecheckedOnTheFreshRow(t *testing.T) {
	ctx, plan := context.Background(), types.ComponentPlan{}
	// Observed as an Update in flight; by the time the write lands the row
	// is a teardown.
	observed := types.InstanceStatus{
		Index: 0, Phase: types.InstancePhaseUpdating,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate},
	}
	fresh := &types.InstanceStatus{
		Index: 0, Phase: types.InstancePhaseDeleting,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationDelete},
		LastFailure: &types.InstanceTermination{
			PodName: "engine-0-default-0", Reason: "PodFailed",
		},
	}
	input := types.ReconcileInput{
		ObservedState: types.WorkloadObservedState{InstanceStatuses: []types.InstanceStatus{observed}},
		MutateInstance: func(_ context.Context, _ int32, mutate func(*types.InstanceStatus) bool) error {
			mutate(fresh)
			return nil
		},
	}
	entered, held, err := mark(ctx, input, plan, 0, schedulerHold,
		&types.InstanceTermination{PodName: "engine-0-default-0", Reason: types.WaitingReasonUnschedulable})
	if err != nil {
		t.Fatalf("mark: %v", err)
	}
	if entered || held {
		t.Errorf("entered=%v held=%v want false, false (the fresh row is a teardown)", entered, held)
	}
	if fresh.Operation.Waiting != "" {
		t.Errorf("Waiting: got %q want unset on a teardown", fresh.Operation.Waiting)
	}
	if fresh.LastFailure == nil || fresh.LastFailure.Reason != "PodFailed" {
		t.Errorf("LastFailure: got %+v want the teardown's own record untouched", fresh.LastFailure)
	}
}
