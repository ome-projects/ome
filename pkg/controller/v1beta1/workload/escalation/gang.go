// gang.go — the gang arm of the escalation pass. A gang is
// admitted as a unit, so its PodGroup carries facts no single member
// does, and one of them is terminal: a deterministic name held by
// ANOTHER controller ends the attempt, because nothing this controller
// does frees it.
//
// The two states that are not terminal never reach here. A name being
// collected is a wait the hold pass reports and the collection resolves
// (holds/gang.go); a group the gang scheduler has failed is recovered by
// the PodGroup pass's reset, so the row only withholds members while the
// replacement is built.
package escalation

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// gangTerminalReason is the LastFailure.Reason for a PodGroup state
// nothing this controller does can resolve, or "" otherwise. Only a
// foreign owner qualifies: the name belongs to another controller and
// will not be handed over. A failed group is recoverable — the PodGroup
// pass resets it — so it never ends an attempt, or the reset and the
// rebuild would chase each other through Failed forever.
func gangTerminalReason(state types.GangState) string {
	if state == types.GangStateOwnershipConflict {
		return types.PodGroupOwnershipConflictReason
	}
	return ""
}

// gangRowOwnedElsewhere reports whether the row belongs to an owner
// nothing read off a PodGroup may take it from. A teardown is
// DeleteBatch's, which paces its own removal past any wait and has
// nothing left to announce for a gang on its way out; a migration is its
// record's, the timeout authority for both pair rows. Both are durable
// claims, so an observation is never stale about one.
func gangRowOwnedElsewhere(op *types.InstanceOperation) bool {
	switch types.OwnerOfOperation(op) {
	case types.OwnerDelete, types.OwnerMigrate:
		return true
	}
	return false
}

// gangVerdictActionable reports whether a terminal PodGroup verdict may
// end this row. There must be an attempt in flight to end — the Failed
// stamp is a no-op without one, so announcing it would re-fire the same
// event every pass — and the row's owner must be one this arm may take
// it from.
func gangVerdictActionable(op *types.InstanceOperation) bool {
	return op != nil && !gangRowOwnedElsewhere(op)
}

// gangFailureSummary is the operator-facing reason for a terminal gang
// verdict. Leading with the grep-stable token keeps LastFailure.Reason
// stable through the disposition's short-reason split.
func gangFailureSummary(reason, message string) string {
	if message == "" {
		return reason
	}
	return reason + ": " + message
}

// escalateGangFailure ends an attempt whose PodGroup name belongs to
// another controller. ENVIRONMENT-CAUSED: no corrected pod template
// frees a name someone else owns, so the revision keeps a clean retry
// ladder, and the Operation is preserved for the gang abandon path.
//
// Idempotent announcement: the stamp is a no-op on a row already Failed
// for this reason, so the warning is withheld there too. Nothing clears
// the collision on its own, and a row an operator resets would otherwise
// re-announce the same conflict on the very next pass.
func escalateGangFailure(input types.ReconcileInput, stamps *failureStampBuffer, row types.InstanceStatus, gang types.GangObservation, reason string, now time.Time) {
	if row.Phase == types.InstancePhaseFailed && row.LastFailure != nil && row.LastFailure.Reason == reason {
		return
	}
	idx := row.Index
	summary := gangFailureSummary(reason, gang.Message)
	stamps.add(idx, status.StampFailedKeepingOperation(types.GangTermination(reason, gang.Message, metav1.NewTime(now))), func() {
		if input.WarnInstanceFailed != nil {
			input.WarnInstanceFailed(idx, "", summary)
		}
	})
}
