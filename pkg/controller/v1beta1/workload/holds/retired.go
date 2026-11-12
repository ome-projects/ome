package holds

import (
	"context"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// retiredReplacementHold is a gang surge's report on its own ending: the
// replacement gang was retired, its members are terminating, and the pair
// ends in one write once the last of them is gone. Until then the source
// holds its surge and the replacement its cleanup step, and this token is
// what names the wait on both rows.
//
// Like the admission wait it is recorded from the site that knows it, the
// retirement's delete site, and a Failed source is a row the pass never
// visits. The write that ends the pair removes the replacement's row and
// clears the source's operation, and the token goes with them. Under a
// pause that write does not run, so once no retired member stands the pass
// releases the token and the pause reports the rows. The deadline clock
// does not stop for it and nothing escalates while it stands: the deletion
// is the engine's own and the delete grace bounds it.
var retiredReplacementHold = authority{
	token:  types.WaitingReasonRetiredReplacementTerminating,
	mayOwn: updateOwnerOnly,
	report: reportRetiredReplacement,
}

// reportRetiredReplacement keeps the recorded wait while a retired member
// stands, and never records one: the delete site does. The pair's end
// takes the token away; under a pause the end does not run, so the wait
// is released here once no member stands, and the pause takes the row.
func reportRetiredReplacement(ctx context.Context, in PassInput, row Row) (reading, error) {
	if !types.OperationRetiredReplacementTerminating(row.Status.Operation) || !in.Plan.Paused {
		return reading{}, nil
	}
	_, members, err := in.rowPods(ctx, row)
	if err != nil {
		return reading{}, err
	}
	if len(members) == 0 {
		return reading{release: true}, nil
	}
	return reading{}, nil
}

// RecordRetiredReplacementTerminating records, from the delete site of a
// gang surge's retirement, that the retired replacement's members are
// terminating, on the row at idx: the source holding its surge or the
// replacement at its cleanup step. The token alone is written, so the
// failure record that retired the replacement stays on the row. Edge-
// triggered off the observation: a row already reporting the wait, or one
// another authority's fact already holds, is not written.
func RecordRetiredReplacementTerminating(ctx context.Context, input types.ReconcileInput, plan types.ComponentPlan, idx int32) error {
	row := input.ObservedState.Instance(idx)
	if row == nil || row.Operation == nil || !Enters(types.WaitingReasonRetiredReplacementTerminating, row.Operation.Waiting) {
		return nil
	}
	_, _, err := mark(ctx, input, plan, idx, retiredReplacementHold, nil, nil)
	return err
}
