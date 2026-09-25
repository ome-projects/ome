package escalation_test

// Supersede-prune coverage: the end-of-pass GC must remove exactly the
// RetryBlocks whose revision nothing targets anymore — keeping every
// block still reachable through CurrentRevision, UpdateRevision, the
// roll target, or a live per-Instance Operation — and must not run at
// all under Paused or Teardown.

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// recordRetryBlockRemovals wires a recording MutateRetryBlock that
// captures the revision of every Remove disposition.
func recordRetryBlockRemovals(input *types.ReconcileInput) *[]string {
	removed := &[]string{}
	input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*types.RetryBlock) types.RetryBlockDisposition) error {
		b := types.RetryBlock{TargetRevision: rev}
		if mutate(&b) == types.RetryBlockRemove {
			*removed = append(*removed, rev)
		}
		return nil
	}
	return removed
}

// TestPruneSupersededRetryBlocks_KeepSetMatrix pins the keep set: one
// block per keep-set member (CurrentRevision, UpdateRevision, roll
// target, a live Operation.TargetRevision) is retained while a block
// matching none of them is removed.
func TestPruneSupersededRetryBlocks_KeepSetMatrix(t *testing.T) {
	input := minimalInput(t)
	removed := recordRetryBlockRemovals(&input)
	input.ObservedState.CurrentRevision = "own-engine-current1"
	input.ObservedState.UpdateRevision = "own-engine-update01"
	input.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseUpdating, Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationUpdate,
			TargetRevision: "own-engine-optarget",
		}},
	}
	input.ObservedState.RetryBlocks = []types.RetryBlock{
		{TargetRevision: "own-engine-current1", State: types.RetryBlockHeld},
		{TargetRevision: "own-engine-update01", State: types.RetryBlockHeld},
		{TargetRevision: "own-engine-rolltgt1", State: types.RetryBlockHeld},
		{TargetRevision: "own-engine-optarget", State: types.RetryBlockHeld},
		{TargetRevision: "own-engine-stale001", State: types.RetryBlockHeld},
	}
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "own-engine-rolltgt1"}}

	if err := escalation.PruneSupersededRetryBlocks(context.Background(), input, target); err != nil {
		t.Fatalf("prune: %v", err)
	}
	if len(*removed) != 1 || (*removed)[0] != "own-engine-stale001" {
		t.Fatalf("removed: got %v want [own-engine-stale001]", *removed)
	}
}

// TestPruneSupersededRetryBlocks_MultiStaleBatch removes every stale
// block in one pass, whatever its state — Backoff and Held orphans are
// the same class of dead entry once their revision is superseded.
func TestPruneSupersededRetryBlocks_MultiStaleBatch(t *testing.T) {
	input := minimalInput(t)
	removed := recordRetryBlockRemovals(&input)
	input.ObservedState.CurrentRevision = "own-engine-current1"
	input.ObservedState.RetryBlocks = []types.RetryBlock{
		{TargetRevision: "own-engine-stale001", State: types.RetryBlockHeld},
		{TargetRevision: "own-engine-current1", State: types.RetryBlockBackoff},
		{TargetRevision: "own-engine-stale002", State: types.RetryBlockBackoff},
		{TargetRevision: "own-engine-stale003", State: types.RetryBlockRetryInProgress},
	}

	if err := escalation.PruneSupersededRetryBlocks(context.Background(), input, nil); err != nil {
		t.Fatalf("prune: %v", err)
	}
	want := []string{"own-engine-stale001", "own-engine-stale002", "own-engine-stale003"}
	if len(*removed) != len(want) {
		t.Fatalf("removed: got %v want %v", *removed, want)
	}
	for i, rev := range want {
		if (*removed)[i] != rev {
			t.Fatalf("removed: got %v want %v", *removed, want)
		}
	}
}

// TestPruneSupersededRetryBlocks_NoSeamNoOp pins the unwired-adapter
// guard: a nil MutateRetryBlock is a clean no-op even with stale blocks
// present.
func TestPruneSupersededRetryBlocks_NoSeamNoOp(t *testing.T) {
	input := minimalInput(t)
	input.MutateRetryBlock = nil
	input.ObservedState.RetryBlocks = []types.RetryBlock{
		{TargetRevision: "own-engine-stale001", State: types.RetryBlockHeld},
	}
	if err := escalation.PruneSupersededRetryBlocks(context.Background(), input, nil); err != nil {
		t.Fatalf("prune with nil seam: %v", err)
	}
}

// TestReconcile_PrunesSupersededRetryBlock pins the dispatcher wiring:
// a normal (non-paused, non-teardown) reconcile removes a stale block
// and keeps the CurrentRevision one.
func TestReconcile_PrunesSupersededRetryBlock(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := types.Deps{Client: c}

	in := minimalInput(t)
	removed := recordRetryBlockRemovals(&in)
	in.ObservedState.CurrentRevision = "own-engine-current1"
	in.ObservedState.RetryBlocks = []types.RetryBlock{
		{TargetRevision: "own-engine-current1", State: types.RetryBlockHeld},
		{TargetRevision: "own-engine-stale001", State: types.RetryBlockHeld},
	}
	plan := minimalPlan()

	if _, err := workload.Reconcile(context.Background(), deps, in, plan, nil); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(*removed) != 1 || (*removed)[0] != "own-engine-stale001" {
		t.Fatalf("removed: got %v want [own-engine-stale001]", *removed)
	}
}

// TestReconcile_Paused_NoSupersededPrune: Paused suspends the lifecycle
// machinery, the prune included — a paused pass must leave every block
// untouched.
func TestReconcile_Paused_NoSupersededPrune(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := types.Deps{Client: c}

	in := minimalInput(t)
	removed := recordRetryBlockRemovals(&in)
	in.ObservedState.CurrentRevision = "own-engine-current1"
	in.ObservedState.RetryBlocks = []types.RetryBlock{
		{TargetRevision: "own-engine-stale001", State: types.RetryBlockHeld},
	}
	plan := minimalPlan()
	plan.Paused = true

	if _, err := workload.Reconcile(context.Background(), deps, in, plan, nil); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(*removed) != 0 {
		t.Fatalf("paused reconcile pruned %v, want none", *removed)
	}
}

// TestReconcile_Teardown_NoSupersededPrune: teardown runs only the
// Delete pipeline — no RetryBlock bookkeeping of any kind.
func TestReconcile_Teardown_NoSupersededPrune(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := types.Deps{Client: c}

	in := minimalInput(t)
	removed := recordRetryBlockRemovals(&in)
	in.Teardown = true
	in.ObservedState.RetryBlocks = []types.RetryBlock{
		{TargetRevision: "own-engine-stale001", State: types.RetryBlockHeld},
	}

	if _, err := workload.Reconcile(context.Background(), deps, in, minimalPlan(), nil); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(*removed) != 0 {
		t.Fatalf("teardown reconcile pruned %v, want none", *removed)
	}
}
