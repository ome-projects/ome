package holds

import (
	"testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestQuota_ReportsTheRecordedRefusal: the create site records the
// refusal because no later pass can ask the apiserver for it again, and
// this authority is what turns that record into the wait an operator
// reads. A create that completes with no refusal retires the record, and
// that is the release.
func TestQuota_ReportsTheRecordedRefusal(t *testing.T) {
	t.Run("records the token from the refusal", func(t *testing.T) {
		store, in := newStore(refused(creatingRow("")))
		apply(t, in, types.ComponentPlan{}, nil)
		if got := store.waiting(0); got != types.RejectionReasonQuotaExceeded {
			t.Errorf("waiting = %q, want %q", got, types.RejectionReasonQuotaExceeded)
		}
		if lf := store.lastFailure(0); lf != nil {
			t.Errorf("lastFailure = %+v, want none: a quota message names current usage and moves every pass", lf)
		}
	})

	t.Run("released once the record is retired", func(t *testing.T) {
		store, in := newStore(creatingRow(types.RejectionReasonQuotaExceeded))
		apply(t, in, types.ComponentPlan{}, nil)
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want the token released", got)
		}
	})

	t.Run("edge-triggered while the refusal stands", func(t *testing.T) {
		store, in := newStore(refused(creatingRow("")))
		apply(t, in, types.ComponentPlan{}, nil)
		store.writes = 0
		apply(t, store.input(), types.ComponentPlan{}, nil)
		if store.writes != 0 {
			t.Errorf("writes on an unchanged refusal = %d, want 0", store.writes)
		}
	})

	t.Run("the deadline park reads the record and the token", func(t *testing.T) {
		recorded := refused(creatingRow(""))
		if !types.OperationCapacityRefused(recorded.Operation) || !types.OperationExternallyHeld(recorded.Operation) {
			t.Error("the park must see the refusal on the pass it was recorded, before any token is written")
		}
		reported := creatingRow(types.RejectionReasonQuotaExceeded)
		if types.OperationCapacityRefused(reported.Operation) || !types.OperationExternallyHeld(reported.Operation) {
			t.Error("the park must see the token on the pass it outlives the record, so a lingering report does not re-arm the clock")
		}
	})

	t.Run("announced only when the refusal becomes the report", func(t *testing.T) {
		token := types.RejectionReasonQuotaExceeded
		if !Enters(token, "") || !Enters(token, types.WaitingReasonPaused) {
			t.Error("an unheld or paused row enters the quota wait")
		}
		if Enters(token, token) {
			t.Error("a row already reporting the wait does not enter it again")
		}
		for _, fact := range []string{types.WaitingReasonUnschedulable, types.WaitingReasonNodeUnknown, types.WaitingReasonPodGroupTerminating, types.WaitingReasonSourceUnrouted} {
			if Enters(token, fact) {
				t.Errorf("a row reporting %s keeps that report; the refusal is recorded, not announced", fact)
			}
		}
	})
}
