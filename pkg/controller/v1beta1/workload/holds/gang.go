package holds

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// gangHold is what only an Instance's PodGroup can say, since a gang is
// admitted as a unit: the deterministic name is still occupied by an
// object being collected, so the group cannot be announced yet. The wait
// resolves itself once that object is gone and never escalates — a name
// held by ANOTHER controller is a terminal verdict instead, and belongs
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
// removal past any wait and has nothing left to announce for a gang on
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

// reportGang reads the row's PodGroup state.
//
// A fully-serving pod set retires the hold: the PodGroup bookkeeping may
// be stale, but a group reading cannot stop a gang already in rotation.
func reportGang(ctx context.Context, in PassInput, row Row) (reading, error) {
	gang := types.GangReadingFor(in.Input, row.Status)
	token := gangHoldToken(gang.State)
	if token == "" {
		return reading{release: true}, nil
	}
	serving, err := in.serving(ctx, row)
	if err != nil {
		return reading{}, err
	}
	if serving {
		return reading{release: true}, nil
	}
	at := gangHoldObservedAt(row.Status, token, in.Input.Now())
	return reading{waiting: true, evidence: types.GangTermination(token, gang.Message, at)}, nil
}

// gangHoldToken is the Waiting token a PodGroup state parks the clock
// on, or "" for a state that is not a wait: nothing to report, or a
// terminal verdict the escalation ends outright.
func gangHoldToken(state types.GangState) string {
	if state == types.GangStateTerminating {
		return types.WaitingReasonPodGroupTerminating
	}
	return ""
}

// gangHoldObservedAt is the moment stamped on the hold's evidence: the
// one already recorded when this row holds the token, otherwise now.
//
// Nothing measures a window from it — the only gang wait is a name being
// collected, which resolves itself and never escalates. It is re-read so
// that re-observing an unchanged hold produces an identical record and
// the status no-op guard keeps the pass write-free.
func gangHoldObservedAt(s types.InstanceStatus, token string, now time.Time) metav1.Time {
	if s.Operation != nil && s.Operation.Waiting == token &&
		s.LastFailure != nil && s.LastFailure.Reason == token && !s.LastFailure.Time.IsZero() {
		return s.LastFailure.Time
	}
	return metav1.NewTime(now)
}
