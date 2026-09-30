package holds

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// provisioningHold is capacity a provisioner outside the workload has not
// readied for a pod the operation needs. The create site withholds such a
// pod until Deps.Provisioner places it, so neither admission nor the
// scheduler ever sees it and no other authority can report the wait; the
// row reports it here instead of spending its deadline in silence.
var provisioningHold = authority{
	token:  types.WaitingReasonCapacityProvisioning,
	mayOwn: anyAttemptOwner,
	report: reportProvisioning,
}

// reportProvisioning asks the provisioner about each target the row has
// no pod for. A target whose pod exists is not withheld: whatever holds
// that pod now is another authority's to report.
//
// A fully-serving pod set retires the hold, as it retires the
// scheduler's: a withheld replacement cannot stop a workload that is
// already in rotation.
//
// Unlike the scheduler's and quota's, the explanation is part of the
// evidence: it changes only when the capacity's state does, so a new one
// is stamped with the time it was first seen, and the row reports the
// state that withholds the pod now rather than the first one it saw.
func reportProvisioning(ctx context.Context, in PassInput, row Row) (reading, error) {
	provisioner := in.Deps.Provisioner
	if provisioner == nil || row.Instance == nil {
		return reading{release: true}, nil
	}
	own, _, err := in.rowPods(ctx, row)
	if err != nil {
		return reading{}, err
	}
	present := make(map[string]struct{}, len(own))
	for _, pod := range own {
		present[pod.Name] = struct{}{}
	}
	for _, t := range targets(in, row) {
		if _, ok := present[t.name]; ok {
			continue
		}
		message, pending, err := provisioner.Pending(ctx, in.Input, in.Plan, *row.Instance, t.runner, t.ordinal)
		if err != nil {
			return reading{}, err
		}
		if !pending {
			continue
		}
		serving, err := in.serving(ctx, row)
		if err != nil {
			return reading{}, err
		}
		if serving {
			return reading{release: true}, nil
		}
		at := heldSince(row.Status, types.WaitingReasonCapacityProvisioning, in.Input.Now())
		evidence := types.CapacityProvisioningTermination(t.name, message, at)
		if recorded := row.Status.LastFailure; recorded != nil && recorded.Message != evidence.Message {
			evidence.Time = metav1.NewTime(in.Input.Now())
		}
		return reading{waiting: true, evidence: evidence}, nil
	}
	return reading{release: true}, nil
}
