package replay_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/replay"
	types "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// recordingSink captures the resolved writes a store reports, so a test
// asserts on the same stream a trace renders.
type recordingSink struct {
	rows   []replay.RowCommit
	blocks []replay.RetryBlockCommit
}

func (s *recordingSink) RowCommitted(c replay.RowCommit) { s.rows = append(s.rows, c) }
func (s *recordingSink) RetryBlockCommitted(c replay.RetryBlockCommit) {
	s.blocks = append(s.blocks, c)
}

func setPhase(phase types.InstancePhase) func(*types.InstanceStatus) bool {
	return func(s *types.InstanceStatus) bool {
		if s.Phase == phase {
			return false
		}
		s.Phase = phase
		return true
	}
}

func TestRowStoreUpsertsAndReportsTheDiff(t *testing.T) {
	sink := &recordingSink{}
	store := replay.NewRowStore(nil, nil, sink)
	if err := store.MutateInstance(context.Background(), 0, setPhase(types.InstancePhaseCreating)); err != nil {
		t.Fatalf("mutate: %v", err)
	}
	rows := store.Rows()
	if len(rows) != 1 || rows[0].Phase != types.InstancePhaseCreating {
		t.Fatalf("rows = %+v", rows)
	}
	if len(sink.rows) != 1 || sink.rows[0].Outcome != replay.PreconditionOK || sink.rows[0].Before != nil {
		t.Fatalf("commit = %+v", sink.rows)
	}
}

// TestRowStoreKeepsInsertionOrder pins the adapter's list shape: an
// existing slot keeps its position and a new index lands at the end, which
// is what an order-sensitive read in the engine sees.
func TestRowStoreKeepsInsertionOrder(t *testing.T) {
	store := replay.NewRowStore([]types.InstanceStatus{{Index: 5}, {Index: 2}}, nil, nil)
	if err := store.MutateInstance(context.Background(), 1, setPhase(types.InstancePhaseReady)); err != nil {
		t.Fatalf("mutate: %v", err)
	}
	var got []int32
	for _, row := range store.Rows() {
		got = append(got, row.Index)
	}
	if len(got) != 3 || got[0] != 5 || got[1] != 2 || got[2] != 1 {
		t.Fatalf("order = %v, want [5 2 1]", got)
	}
}

func TestRowStoreReportsANoChangeMutation(t *testing.T) {
	sink := &recordingSink{}
	store := replay.NewRowStore([]types.InstanceStatus{{Index: 0, Phase: types.InstancePhaseReady}}, nil, sink)
	if err := store.MutateInstance(context.Background(), 0, setPhase(types.InstancePhaseReady)); err != nil {
		t.Fatalf("mutate: %v", err)
	}
	if len(sink.rows) != 1 || sink.rows[0].Outcome != replay.PreconditionNoChange {
		t.Fatalf("commit = %+v", sink.rows)
	}
}

// TestRowStorePreconditionDropsOnlyItsOwnMutation keeps the adapter's
// granularity: a per-mutation precondition is not a batch guard.
func TestRowStorePreconditionDropsOnlyItsOwnMutation(t *testing.T) {
	sink := &recordingSink{}
	store := replay.NewRowStore([]types.InstanceStatus{{Index: 0}, {Index: 1}}, nil, sink)
	err := store.ApplyInstanceMutations(context.Background(), []types.InstanceMutation{
		{Index: 0, Precondition: func(*types.InstanceStatus) bool { return false }, Mutate: setPhase(types.InstancePhaseReady)},
		{Index: 1, Mutate: setPhase(types.InstancePhaseReady)},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := store.Rows(); got[0].Phase != "" || got[1].Phase != types.InstancePhaseReady {
		t.Fatalf("rows = %+v", got)
	}
	if len(sink.rows) != 2 || sink.rows[0].Outcome != replay.PreconditionRejected || sink.rows[1].Outcome != replay.PreconditionOK {
		t.Fatalf("commits = %+v", sink.rows)
	}
}

// TestRowStoreBatchGuardRejectsEverything is the all-or-nothing contract a
// write-ahead mutation depends on.
func TestRowStoreBatchGuardRejectsEverything(t *testing.T) {
	sink := &recordingSink{}
	store := replay.NewRowStore([]types.InstanceStatus{{Index: 0}, {Index: 1}}, nil, sink)
	err := store.ApplyInstanceMutations(context.Background(), []types.InstanceMutation{
		{Index: 0, BatchPrecondition: func(types.InstanceMutationSnapshot) bool { return false }, Mutate: setPhase(types.InstancePhaseReady)},
		{Index: 1, Mutate: setPhase(types.InstancePhaseReady)},
	})
	if !errors.Is(err, types.ErrStatusMutationPrecondition) {
		t.Fatalf("err = %v", err)
	}
	for _, row := range store.Rows() {
		if row.Phase != "" {
			t.Fatalf("a rejected batch wrote %+v", row)
		}
	}
	if len(sink.rows) != 2 {
		t.Fatalf("every mutation of a rejected batch is reported; got %+v", sink.rows)
	}
	for _, commit := range sink.rows {
		if commit.Outcome != replay.PreconditionBatchRejected {
			t.Fatalf("outcome = %q", commit.Outcome)
		}
	}
}

// TestRowStoreBatchGuardSeesTheOwner proves the guard is handed the owner
// identity a generation-skew check reads.
func TestRowStoreBatchGuardSeesTheOwner(t *testing.T) {
	store := replay.NewRowStore([]types.InstanceStatus{{Index: 0}}, nil, nil)
	store.SetOwner("owner-uid", 7)
	var seen types.InstanceMutationSnapshot
	err := store.ApplyInstanceMutations(context.Background(), []types.InstanceMutation{{
		Index: 0,
		BatchPrecondition: func(s types.InstanceMutationSnapshot) bool {
			seen = s
			return true
		},
		Mutate: setPhase(types.InstancePhaseReady),
	}})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if seen.OwnerUID != "owner-uid" || seen.OwnerGeneration != 7 || len(seen.Instances) != 1 {
		t.Fatalf("snapshot = %+v", seen)
	}
}

func TestRowStoreRejectsAnIllFormedBatch(t *testing.T) {
	store := replay.NewRowStore([]types.InstanceStatus{{Index: 0}}, nil, nil)
	onCommit := func(*types.InstanceStatus, *types.InstanceStatus) {}
	for name, muts := range map[string][]types.InstanceMutation{
		"remove and mutate": {{Index: 0, Remove: true, Mutate: setPhase(types.InstancePhaseReady)}},
		"neither":           {{Index: 0}},
		"duplicate onCommit": {
			{Index: 0, Mutate: setPhase(types.InstancePhaseReady), OnCommit: onCommit},
			{Index: 0, Mutate: setPhase(types.InstancePhaseFailed)},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := store.ApplyInstanceMutations(context.Background(), muts); err == nil {
				t.Fatalf("an ill-formed batch must error rather than panic or silently drop")
			}
		})
	}
}

// TestRowStoreRemoveFiresOnCommitAfterTheWrite pins the ordering an
// external effect guarded by a removal depends on.
func TestRowStoreRemoveFiresOnCommitAfterTheWrite(t *testing.T) {
	store := replay.NewRowStore([]types.InstanceStatus{{Index: 0, Phase: types.InstancePhaseDeleting}}, nil, nil)
	var before, after *types.InstanceStatus
	var rowsAtCommit int
	err := store.ApplyInstanceMutations(context.Background(), []types.InstanceMutation{{
		Index: 0, Remove: true,
		OnCommit: func(p, c *types.InstanceStatus) {
			before, after, rowsAtCommit = p, c, len(store.Rows())
		},
	}})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if before == nil || before.Phase != types.InstancePhaseDeleting || after != nil {
		t.Fatalf("callback saw before=%+v after=%+v", before, after)
	}
	if rowsAtCommit != 0 {
		t.Fatalf("OnCommit ran before the removal landed")
	}
}

func TestRowStoreRemoveInstanceReportsWhetherItExisted(t *testing.T) {
	store := replay.NewRowStore([]types.InstanceStatus{{Index: 0}}, nil, nil)
	removed, err := store.RemoveInstance(context.Background(), 0)
	if err != nil || !removed {
		t.Fatalf("got %v, %v", removed, err)
	}
	removed, err = store.RemoveInstance(context.Background(), 0)
	if err != nil || removed {
		t.Fatalf("a second removal must report no change; got %v, %v", removed, err)
	}
}

func TestRowStoreRetryBlockWrites(t *testing.T) {
	sink := &recordingSink{}
	store := replay.NewRowStore(nil, nil, sink)
	ctx := context.Background()

	if err := store.MutateRetryBlock(ctx, "rev-a", func(b *types.RetryBlock) types.RetryBlockDisposition {
		b.State = types.RetryBlockBackoff
		b.AttemptsStarted = 1
		return types.RetryBlockPersist
	}); err != nil {
		t.Fatalf("persist: %v", err)
	}
	if blocks := store.RetryBlocks(); len(blocks) != 1 || blocks[0].TargetRevision != "rev-a" || blocks[0].AttemptsStarted != 1 {
		t.Fatalf("blocks = %+v", blocks)
	}
	if err := store.MutateRetryBlock(ctx, "rev-a", func(*types.RetryBlock) types.RetryBlockDisposition {
		return types.RetryBlockUnchanged
	}); err != nil {
		t.Fatalf("unchanged: %v", err)
	}
	if len(sink.blocks) != 1 {
		t.Fatalf("an unchanged disposition must write nothing; got %+v", sink.blocks)
	}
	if err := store.MutateRetryBlock(ctx, "rev-a", func(*types.RetryBlock) types.RetryBlockDisposition {
		return types.RetryBlockRemove
	}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if blocks := store.RetryBlocks(); len(blocks) != 0 {
		t.Fatalf("blocks = %+v", blocks)
	}
	if len(sink.blocks) != 2 || sink.blocks[1].Disposition != replay.RetryBlockRemoved {
		t.Fatalf("commits = %+v", sink.blocks)
	}
}

// TestRowStoreRetryBlockCommitsWithItsRows is the atomicity the seam
// promises: one write carries both sides.
func TestRowStoreRetryBlockCommitsWithItsRows(t *testing.T) {
	sink := &recordingSink{}
	store := replay.NewRowStore([]types.InstanceStatus{{Index: 0}}, nil, sink)
	err := store.ApplyInstanceMutationsWithRetryBlock(context.Background(),
		[]types.InstanceMutation{{Index: 0, Mutate: setPhase(types.InstancePhaseUpdating)}},
		"rev-a",
		func(b *types.RetryBlock) types.RetryBlockDisposition {
			b.State = types.RetryBlockRetryInProgress
			return types.RetryBlockPersist
		})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(sink.rows) != 1 || len(sink.blocks) != 1 {
		t.Fatalf("rows=%+v blocks=%+v", sink.rows, sink.blocks)
	}
}

func TestRowStoreRowsAreIsolatedCopies(t *testing.T) {
	store := replay.NewRowStore([]types.InstanceStatus{{Index: 0, Phase: types.InstancePhaseReady}}, nil, nil)
	rows := store.Rows()
	rows[0].Phase = types.InstancePhaseFailed
	if store.Rows()[0].Phase != types.InstancePhaseReady {
		t.Fatalf("a caller mutated the store through its own copy")
	}
}

// TestRowStoreInstallWiresEverySeam keeps a new status callback from being
// silently left unbacked.
func TestRowStoreInstallWiresEverySeam(t *testing.T) {
	store := replay.NewRowStore(nil, nil, nil)
	input := types.ReconcileInput{}
	store.Install(&input)
	if input.MutateInstance == nil || input.RemoveInstance == nil ||
		input.ApplyInstanceMutations == nil || input.ApplyInstanceMutationsWithRetryBlock == nil ||
		input.MutateRetryBlock == nil {
		t.Fatalf("Install left a status seam unwired: %+v", input)
	}
}

func TestRowStoreEmptyBatchWritesNothing(t *testing.T) {
	sink := &recordingSink{}
	store := replay.NewRowStore(nil, nil, sink)
	if err := store.ApplyInstanceMutationsWithRetryBlock(context.Background(), nil, "", nil); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(sink.rows) != 0 || len(sink.blocks) != 0 {
		t.Fatalf("an empty batch wrote %+v / %+v", sink.rows, sink.blocks)
	}
}

func TestRowStoreErrorNamesTheOffendingMutation(t *testing.T) {
	store := replay.NewRowStore(nil, nil, nil)
	err := store.ApplyInstanceMutations(context.Background(), []types.InstanceMutation{{Index: 3}})
	if err == nil || !strings.Contains(err.Error(), "index 3") {
		t.Fatalf("err = %v", err)
	}
}
