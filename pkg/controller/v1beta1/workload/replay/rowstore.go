package replay

import (
	"context"
	"fmt"

	k8stypes "k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	types "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Precondition outcomes recorded against one buffered mutation.
const (
	// PreconditionOK marks a mutation that committed.
	PreconditionOK = "ok"
	// PreconditionRejected marks a mutation whose own precondition
	// refused the fresh row, so the mutation was dropped and the rest of
	// the batch still committed.
	PreconditionRejected = "rejected"
	// PreconditionBatchRejected marks every mutation of a batch whose
	// shared guard refused the fresh owner snapshot: nothing commits.
	PreconditionBatchRejected = "batch-rejected"
	// PreconditionNoChange marks a mutation whose callback reported no
	// change, so there was nothing to write.
	PreconditionNoChange = "no-change"
)

// RetryBlock dispositions recorded against one retry-block write.
const (
	RetryBlockPersisted = "persist"
	RetryBlockRemoved   = "remove"
)

// RowCommit is one buffered InstanceStatus mutation as the store resolved
// it. Before is nil when the row did not exist, After is nil when the row
// was removed, and Outcome is the precondition verdict.
type RowCommit struct {
	Index   int32
	Remove  bool
	Outcome string
	Before  *types.InstanceStatus
	After   *types.InstanceStatus
}

// RetryBlockCommit is one resolved retry-block write.
type RetryBlockCommit struct {
	TargetRevision string
	Disposition    string
	Before         *types.RetryBlock
	After          *types.RetryBlock
}

// RowSink receives every resolved write in commit order. The driver wires
// the trace recorder; a caller with no interest in the detail may leave
// it nil.
type RowSink interface {
	RowCommitted(RowCommit)
	RetryBlockCommitted(RetryBlockCommit)
}

// RowStore is the in-memory owner status the engine writes rows through.
// It reproduces the adapter's contract: one fresh snapshot per attempt,
// batch guards evaluated before any mutation, per-mutation preconditions
// that drop only their own mutation, and OnCommit callbacks fired with
// isolated before/after copies only after the write commits.
//
// Not safe for concurrent use; one store belongs to one replay run.
type RowStore struct {
	ownerUID        k8stypes.UID
	ownerGeneration int64
	rows            []types.InstanceStatus
	retryBlocks     []types.RetryBlock
	sink            RowSink
}

// NewRowStore seeds a store from the rows an owner already persists.
func NewRowStore(rows []types.InstanceStatus, retryBlocks []types.RetryBlock, sink RowSink) *RowStore {
	return &RowStore{
		rows:        CloneRows(rows),
		retryBlocks: append([]types.RetryBlock(nil), retryBlocks...),
		sink:        sink,
	}
}

// SetOwner records the owner identity a batch guard reads.
func (s *RowStore) SetOwner(uid k8stypes.UID, generation int64) {
	s.ownerUID, s.ownerGeneration = uid, generation
}

// Rows returns an isolated copy of the persisted rows — the view the next
// reconcile observes. Order is the owner status list's own: existing slots
// keep their position and a new index lands at the end, which is what the
// adapter's status writer produces and what any order-sensitive read in
// the engine therefore sees.
func (s *RowStore) Rows() []types.InstanceStatus {
	return CloneRows(s.rows)
}

// RetryBlocks returns an isolated copy of the persisted retry blocks, in
// the same list order the owner status keeps them in.
func (s *RowStore) RetryBlocks() []types.RetryBlock {
	return append([]types.RetryBlock(nil), s.retryBlocks...)
}

// Install wires the store onto every status seam of a ReconcileInput.
func (s *RowStore) Install(input *types.ReconcileInput) {
	input.MutateInstance = s.MutateInstance
	input.RemoveInstance = s.RemoveInstance
	input.ApplyInstanceMutations = s.ApplyInstanceMutations
	input.ApplyInstanceMutationsWithRetryBlock = s.ApplyInstanceMutationsWithRetryBlock
	input.MutateRetryBlock = s.MutateRetryBlock
	if owner := input.OwnerObject; owner != nil {
		s.SetOwner(owner.GetUID(), owner.GetGeneration())
	}
}

// MutateInstance applies one mutation to one row, upserting the slot.
func (s *RowStore) MutateInstance(ctx context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
	return s.ApplyInstanceMutations(ctx, []types.InstanceMutation{{Index: idx, Mutate: mutate}})
}

// RemoveInstance drops one row, reporting whether it was there.
func (s *RowStore) RemoveInstance(_ context.Context, idx int32) (bool, error) {
	position := s.rowPosition(idx)
	if position < 0 {
		return false, nil
	}
	before := CloneRow(s.rows[position])
	s.rows = append(s.rows[:position], s.rows[position+1:]...)
	s.emitRow(RowCommit{Index: idx, Remove: true, Outcome: PreconditionOK, Before: &before})
	return true, nil
}

// ApplyInstanceMutations commits a batch with no retry-block write.
func (s *RowStore) ApplyInstanceMutations(ctx context.Context, muts []types.InstanceMutation) error {
	return s.ApplyInstanceMutationsWithRetryBlock(ctx, muts, "", nil)
}

// MutateRetryBlock commits a retry-block write with no row mutations.
func (s *RowStore) MutateRetryBlock(ctx context.Context, targetRevision string, mutate func(*types.RetryBlock) types.RetryBlockDisposition) error {
	return s.ApplyInstanceMutationsWithRetryBlock(ctx, nil, targetRevision, mutate)
}

// ApplyInstanceMutationsWithRetryBlock is the atomic seam: the complete
// batch is evaluated against one fresh snapshot and either every reported
// change lands in a single write or none does.
func (s *RowStore) ApplyInstanceMutationsWithRetryBlock(
	_ context.Context,
	muts []types.InstanceMutation,
	targetRevision string,
	mutateRetryBlock func(*types.RetryBlock) types.RetryBlockDisposition,
) error {
	if err := validateMutationBatch(muts); err != nil {
		return err
	}
	if len(muts) == 0 && mutateRetryBlock == nil {
		return nil
	}
	snapshot := types.InstanceMutationSnapshot{
		OwnerUID:        s.ownerUID,
		OwnerGeneration: s.ownerGeneration,
		Instances:       make(map[int32]types.InstanceStatus, len(s.rows)),
	}
	for _, row := range s.rows {
		snapshot.Instances[row.Index] = CloneRow(row)
	}
	for _, mutation := range muts {
		if mutation.BatchPrecondition != nil && !mutation.BatchPrecondition(snapshot) {
			for _, rejected := range muts {
				before := s.rowCopy(rejected.Index)
				s.emitRow(RowCommit{Index: rejected.Index, Remove: rejected.Remove, Outcome: PreconditionBatchRejected, Before: before})
			}
			return types.ErrStatusMutationPrecondition
		}
	}

	type committedMutation struct {
		callback func(*types.InstanceStatus, *types.InstanceStatus)
		record   RowCommit
	}
	next := CloneRows(s.rows)
	committed := make([]committedMutation, 0, len(muts))
	var pending []RowCommit
	for _, mutation := range muts {
		position := -1
		for i := range next {
			if next[i].Index == mutation.Index {
				position = i
				break
			}
		}
		if mutation.Remove {
			if position < 0 {
				pending = append(pending, RowCommit{Index: mutation.Index, Remove: true, Outcome: PreconditionNoChange})
				continue
			}
			before := CloneRow(next[position])
			if mutation.Precondition != nil && !mutation.Precondition(&before) {
				pending = append(pending, RowCommit{Index: mutation.Index, Remove: true, Outcome: PreconditionRejected, Before: &before})
				continue
			}
			next = append(next[:position], next[position+1:]...)
			record := RowCommit{Index: mutation.Index, Remove: true, Outcome: PreconditionOK, Before: &before}
			committed = append(committed, committedMutation{callback: mutation.OnCommit, record: record})
			continue
		}

		status := types.InstanceStatus{Index: mutation.Index}
		var before *types.InstanceStatus
		if position >= 0 {
			status = CloneRow(next[position])
			copied := CloneRow(status)
			before = &copied
		}
		if mutation.Precondition != nil && !mutation.Precondition(&status) {
			pending = append(pending, RowCommit{Index: mutation.Index, Outcome: PreconditionRejected, Before: before})
			continue
		}
		if !mutation.Mutate(&status) {
			pending = append(pending, RowCommit{Index: mutation.Index, Outcome: PreconditionNoChange, Before: before})
			continue
		}
		if position >= 0 {
			next[position] = CloneRow(status)
		} else {
			next = append(next, CloneRow(status))
		}
		after := CloneRow(status)
		record := RowCommit{Index: mutation.Index, Outcome: PreconditionOK, Before: before, After: &after}
		committed = append(committed, committedMutation{callback: mutation.OnCommit, record: record})
	}

	blockCommit, blockChanged := s.resolveRetryBlock(targetRevision, mutateRetryBlock)
	if len(committed) == 0 && !blockChanged {
		for _, record := range pending {
			s.emitRow(record)
		}
		return nil
	}
	s.rows = next
	for _, record := range pending {
		s.emitRow(record)
	}
	for _, mutation := range committed {
		s.emitRow(mutation.record)
	}
	if blockChanged {
		s.applyRetryBlock(blockCommit)
		s.emitRetryBlock(blockCommit)
	}
	for _, mutation := range committed {
		if mutation.callback != nil {
			mutation.callback(mutation.record.Before, mutation.record.After)
		}
	}
	return nil
}

// validateMutationBatch rejects a batch the adapter would reject. These
// are contract violations in the caller, not states the owner status can
// be in, so they surface as errors rather than as a dropped mutation: a
// mutation that sets neither Remove nor Mutate would otherwise dereference
// a nil callback, and an OnCommit on a duplicated index cannot name which
// of the two writes it reports.
func validateMutationBatch(muts []types.InstanceMutation) error {
	counts := make(map[int32]int, len(muts))
	for i := range muts {
		counts[muts[i].Index]++
	}
	for i := range muts {
		switch {
		case muts[i].Remove && muts[i].Mutate != nil:
			return fmt.Errorf("instance mutation %d for index %d sets both Remove and Mutate", i, muts[i].Index)
		case !muts[i].Remove && muts[i].Mutate == nil:
			return fmt.Errorf("instance mutation %d for index %d sets neither Remove nor Mutate", i, muts[i].Index)
		case muts[i].OnCommit != nil && counts[muts[i].Index] > 1:
			return fmt.Errorf("instance mutation %d for index %d uses OnCommit but the index appears %d times", i, muts[i].Index, counts[muts[i].Index])
		}
	}
	return nil
}

func (s *RowStore) resolveRetryBlock(targetRevision string, mutate func(*types.RetryBlock) types.RetryBlockDisposition) (RetryBlockCommit, bool) {
	if mutate == nil || targetRevision == "" {
		return RetryBlockCommit{}, false
	}
	block := types.RetryBlock{TargetRevision: targetRevision}
	var before *types.RetryBlock
	if existing := types.FindRetryBlock(s.retryBlocks, targetRevision); existing != nil {
		block = *existing
		copied := *existing
		before = &copied
	}
	switch mutate(&block) {
	case types.RetryBlockPersist:
		after := block
		return RetryBlockCommit{TargetRevision: targetRevision, Disposition: RetryBlockPersisted, Before: before, After: &after}, true
	case types.RetryBlockRemove:
		if before == nil {
			return RetryBlockCommit{}, false
		}
		return RetryBlockCommit{TargetRevision: targetRevision, Disposition: RetryBlockRemoved, Before: before}, true
	default:
		return RetryBlockCommit{}, false
	}
}

func (s *RowStore) applyRetryBlock(commit RetryBlockCommit) {
	position := -1
	for i := range s.retryBlocks {
		if s.retryBlocks[i].TargetRevision == commit.TargetRevision {
			position = i
			break
		}
	}
	switch commit.Disposition {
	case RetryBlockRemoved:
		if position >= 0 {
			s.retryBlocks = append(s.retryBlocks[:position], s.retryBlocks[position+1:]...)
		}
	case RetryBlockPersisted:
		if position >= 0 {
			s.retryBlocks[position] = *commit.After
			return
		}
		s.retryBlocks = append(s.retryBlocks, *commit.After)
	}
}

func (s *RowStore) rowPosition(idx int32) int {
	for i := range s.rows {
		if s.rows[i].Index == idx {
			return i
		}
	}
	return -1
}

func (s *RowStore) rowCopy(idx int32) *types.InstanceStatus {
	position := s.rowPosition(idx)
	if position < 0 {
		return nil
	}
	copied := CloneRow(s.rows[position])
	return &copied
}

func (s *RowStore) emitRow(record RowCommit) {
	if s.sink != nil {
		s.sink.RowCommitted(record)
	}
}

func (s *RowStore) emitRetryBlock(record RetryBlockCommit) {
	if s.sink != nil {
		s.sink.RetryBlockCommitted(record)
	}
}

// CloneRows deep-copies a row slice through the CRD round-trip the
// adapter performs, so a mutation cannot alias a caller's memory.
func CloneRows(rows []types.InstanceStatus) []types.InstanceStatus {
	cloned := make([]types.InstanceStatus, len(rows))
	for i := range rows {
		cloned[i] = CloneRow(rows[i])
	}
	return cloned
}

// CloneRow deep-copies one row.
func CloneRow(row types.InstanceStatus) types.InstanceStatus {
	converted := v1beta1convert.InstanceStatusFromWorkload(row)
	return v1beta1convert.InstanceStatusToWorkload(*converted.DeepCopy())
}
