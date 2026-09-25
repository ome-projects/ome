package holds

import (
	"context"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// quotaHold is admission refusing the operation's pod creates for lack
// of quota. The Instance is queued behind capacity an operator controls,
// not stuck, so the token parks the InstanceReadyTimeout clock.
//
// The one authority whose condition is not readable from the cluster: it
// is an apiserver answer to a call the create pass made, and no later
// pass can ask for it again. The create site records the refusal on the
// operation (status.RecordCapacityRefusal) and this reads the record, which
// is why the token lands on the pass after the refusal. The deadline
// does not wait for it — the park is anchored to the recorded refusal
// (types.OperationCapacityRefused), so the clock stops from the moment
// admission said no.
var quotaHold = authority{
	token:  types.RejectionReasonQuotaExceeded,
	mayOwn: anyOwner,
	report: reportQuota,
}

// anyOwner is the owner rule of a wait every attempt can be in: any
// operation that creates a pod can have that create refused.
func anyOwner(types.RowOwner, types.ComponentPlan) bool { return true }

// reportQuota reads the refusal the create site recorded. A create that
// completed with no refusal clears the record, and that is the release.
func reportQuota(_ context.Context, _ PassInput, row Row) (reading, error) {
	if !types.OperationCapacityRefused(row.Status.Operation) {
		return reading{release: true}, nil
	}
	return reading{waiting: true}, nil
}
