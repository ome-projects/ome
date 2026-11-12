package holds

import (
	"context"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// gangHold is what only an Instance's PodGroup can say, since a gang is
// admitted as a unit: the deterministic name is still occupied by an
// object being collected, so the group cannot be published yet. The wait
// resolves itself once that object is gone and never escalates; a name
// held by ANOTHER controller is a terminal answer instead, and belongs
// to the escalation pass (escalation/gang.go).
//
// Whether the gang can be PLACED is not read here. That reaches the
// Instance on its member pods and is the scheduler hold's.
var gangHold = authority{
	token:  types.WaitingReasonPodGroupTerminating,
	mayOwn: gangOwnerMayReport,
	report: reportGang,
}

// gangOwnerMayReport reports whether a row's owner is one a PodGroup
// reading may take. A teardown is DeleteBatch's, which paces its own
// removal past any wait and has nothing left to publish for a gang on
// its way out; a migration is its record's, the timeout authority for
// both pair rows. Both are durable claims, so an observation is never
// stale about one.
func gangOwnerMayReport(owner types.RowOwner, _ types.ComponentPlan) bool {
	switch owner {
	case types.OwnerDelete, types.OwnerMigrate:
		return false
	}
	return true
}

// reportGang reads the row's PodGroup state; a fully serving pod set
// retires the hold.
func reportGang(ctx context.Context, in PassInput, row Row) (reading, error) {
	gang := types.GangReadingFor(in.Input, *row.Status)
	token := gangHoldToken(gang.State)
	if token == "" {
		return reading{release: true}, nil
	}
	return unlessServing(ctx, in, row, func() reading {
		at := heldSince(*row.Status, token, in.Input.Now())
		return reading{waiting: true, evidence: types.GangTermination(token, gang.Message, at)}
	})
}

// gangHoldToken is the Waiting token a PodGroup state stops the clock
// on, or "" for a state that is not a wait: nothing to report, or a
// terminal answer the escalation ends outright.
func gangHoldToken(state types.GangState) string {
	if state == types.GangStateTerminating {
		return types.WaitingReasonPodGroupTerminating
	}
	return ""
}
