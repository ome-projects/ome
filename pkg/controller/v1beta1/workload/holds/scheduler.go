package holds

import (
	"context"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// schedulerHold is the scheduler's verdict on a pod it cannot place. The
// workload is waiting on cluster capacity, not failing: the token parks
// the InstanceReadyTimeout clock, and only an operator-configured grace
// turns the wait into a terminal failure (escalation/scheduler.go).
//
// Recorded for EVERY operation type, Migrate included. A migration
// stamps a deadline on both pair rows, and a replacement the scheduler
// cannot place must not burn it; the hold is scoped to the row whose OWN
// pod is unplaceable, so a serving source is never held for its surge's
// fate.
var schedulerHold = authority{
	token:  types.WaitingReasonUnschedulable,
	mayOwn: anyAttemptOwner,
	report: reportScheduler,
}

// reportScheduler reads the scheduler's verdict off the row's own pods.
//
// A fully-serving pod set retires the hold: the bookkeeping may be
// stale, but a stray unplaceable pod cannot stop a workload that is
// already in rotation.
func reportScheduler(ctx context.Context, in PassInput, row Row) (reading, error) {
	own, _, err := in.rowPods(ctx, row)
	if err != nil {
		return reading{}, err
	}
	pod, message, since := types.FirstUnschedulablePod(own)
	if pod == nil {
		return reading{release: true}, nil
	}
	serving, err := in.serving(ctx, row)
	if err != nil {
		return reading{}, err
	}
	if serving {
		return reading{release: true}, nil
	}
	return reading{waiting: true, evidence: types.UnschedulableTermination(pod, message, since)}, nil
}
