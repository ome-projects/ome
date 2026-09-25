package status

import (
	"context"
	"fmt"
	"reflect"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The create-attempt stamps: the write-ahead claim a materialization
// opens with, the plain promote that ends it, the replacement that
// retargets an attempt without failing it, and the rollback that undoes
// a claim whose pod creates never happened. The Create pass batches
// these with its other rows, so each is a mutation the pass commits
// rather than a call that commits itself.

// CreateStepCreatePods is the write-ahead marker for a committed Instance
// materialization. The status mutation is persisted before any Pod create.
const CreateStepCreatePods = "CreatePods"

// CreatingMutation stamps Phase=Creating with a durable Create operation.
// Existing nonzero incarnations are preserved across retries. An attempt
// already committed is left as it is, except that an unpinned one takes
// the target revision the pass now knows.
func CreatingMutation(idx int32, incarnation int64, timeout time.Duration, targetRev string, now metav1.Time) types.InstanceMutation {
	return types.InstanceMutation{Index: idx, Mutate: func(s *types.InstanceStatus) bool {
		if s.Phase == types.InstancePhaseCreating &&
			s.Operation != nil && s.Operation.Type == types.InstanceOperationCreate &&
			s.Incarnation > 0 {
			if s.Operation.TargetRevision == "" && targetRev != "" {
				op := *s.Operation
				op.TargetRevision = targetRev
				s.Operation = &op
				return true
			}
			return false
		}
		if s.Incarnation == 0 {
			s.Incarnation = incarnation
		}
		s.Phase = types.InstancePhaseCreating
		s.Operation = &types.InstanceOperation{
			ID:             fmt.Sprintf("create-%d-%d", idx, now.Unix()),
			Type:           types.InstanceOperationCreate,
			Step:           CreateStepCreatePods,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       types.DeadlineAt(now, timeout),
			TargetRevision: targetRev,
		}
		return true
	}}
}

// StampReady idempotently moves the row to Phase=Ready and clears its
// Operation. RunningRevision is left as it is: this is the promote for a
// pod set whose revision the pass does not vouch for.
func StampReady(ctx context.Context, input types.ReconcileInput, idx int32) error {
	mutation := ReadyMutation(idx, input.Now())
	return input.MutateInstance(ctx, mutation.Index, mutation.Mutate)
}

// ReadyMutation is StampReady as a mutation, for the pass that commits it
// alongside other rows in one batch.
func ReadyMutation(idx int32, now time.Time) types.InstanceMutation {
	return types.InstanceMutation{Index: idx, Mutate: func(s *types.InstanceStatus) bool {
		if s.Phase == types.InstancePhaseReady && s.Operation == nil {
			return false
		}
		EnterReady(s, now)
		s.Operation = nil
		return true
	}}
}

// CreateReplacementMutation replaces a Create attempt with a fresh one:
// replacement is the new operation, pinned to the current target with
// its deadline re-armed, written in the attempt's place. The row never
// passes through Failed. Preconditioned on the retired attempt's exact
// identity — incarnation, phase, operation id and pinned revision — so
// it no-ops once the replacement has landed.
func CreateReplacementMutation(expected *types.InstanceStatus, replacement types.InstanceOperation) types.InstanceMutation {
	idx := expected.Index
	expectedIncarnation := expected.Incarnation
	expectedID := expected.Operation.ID
	expectedTarget := expected.Operation.TargetRevision
	return types.InstanceMutation{Index: idx, Mutate: func(current *types.InstanceStatus) bool {
		if current.Incarnation != expectedIncarnation || current.Phase != types.InstancePhaseCreating ||
			current.Operation == nil || current.Operation.Type != types.InstanceOperationCreate ||
			current.Operation.ID != expectedID || current.Operation.TargetRevision != expectedTarget {
			return false
		}
		op := replacement
		current.Operation = &op
		return true
	}}
}

// CreateRollbackMutation undoes a committed Create transition whose pod
// creates were never attempted: the row goes back to previous, or is
// removed when there was no row before the claim. Reports false when
// nothing was committed. The rollback owns only the lifecycle state it
// wrote — publication-only pod observations may have advanced since the
// commit and are carried over rather than reverted.
func CreateRollbackMutation(index int32, previous, current *types.InstanceStatus) (types.InstanceMutation, bool) {
	if current == nil {
		return types.InstanceMutation{}, false
	}
	committed := *current
	matchesCommitted := func(row *types.InstanceStatus) bool {
		return sameCreateTransitionOwnerState(*row, committed)
	}
	if previous == nil {
		return types.InstanceMutation{
			Index:        index,
			Remove:       true,
			Precondition: matchesCommitted,
		}, true
	}
	restored := *previous
	return types.InstanceMutation{
		Index: index,
		Mutate: func(row *types.InstanceStatus) bool {
			if !matchesCommitted(row) {
				return false
			}
			restoreCreateTransitionState(row, restored)
			return true
		},
	}, true
}

// sameCreateTransitionOwnerState compares the state that can authorize a
// Create rollback. Publication-only Pod observations do not own the lifecycle
// transition and may advance independently.
func sameCreateTransitionOwnerState(current, committed types.InstanceStatus) bool {
	current.ReadyPodCount = 0
	current.ScheduledPodCount = 0
	current.NodesOccupied = nil
	committed.ReadyPodCount = 0
	committed.ScheduledPodCount = 0
	committed.NodesOccupied = nil
	return reflect.DeepEqual(current, committed)
}

func restoreCreateTransitionState(row *types.InstanceStatus, previous types.InstanceStatus) {
	previous.ReadyPodCount = row.ReadyPodCount
	previous.ScheduledPodCount = row.ScheduledPodCount
	if row.NodesOccupied != nil {
		previous.NodesOccupied = append([]string(nil), row.NodesOccupied...)
	} else {
		previous.NodesOccupied = nil
	}
	*row = previous
}
