package holds

import (
	"context"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// schedulerHold is the scheduler's answer on a pod it cannot place. The
// workload is waiting on cluster capacity, not failing: the token stops
// the InstanceReadyTimeout clock, and only an operator-configured grace
// turns the wait into a terminal failure (escalation/scheduler.go).
//
// Recorded for EVERY operation type, Migrate included. A migration
// writes a deadline on both pair rows, and a replacement the scheduler
// cannot place must not spend it; the hold is scoped to the row whose OWN
// pod is unplaceable, so a serving source is never held for what becomes
// of its surge.
var schedulerHold = authority{
	token:  types.WaitingReasonUnschedulable,
	mayOwn: anyAttemptOwner,
	report: reportScheduler,
}

// reportScheduler reads the scheduler's answer off the row's own pods; a
// fully serving pod set retires the hold. A pod's PodScheduled time may be
// its creation behind a queue's gate, and then the wait carries no start of
// its own. heldSince dates the record, as for any hold with no time of its
// own, and decidePlacementWait starts the attempt's placement wait at the
// first pass that sees the refusal.
func reportScheduler(ctx context.Context, in PassInput, row Row) (reading, error) {
	own, _, err := in.rowPods(ctx, row)
	if err != nil {
		return reading{}, err
	}
	pod, message, since := types.FirstUnschedulablePod(own)
	if pod == nil {
		return reading{release: true}, nil
	}
	if evidence.UnschedulableSinceTheGate(pod, since) {
		stamped := heldSince(*row.Status, types.WaitingReasonUnschedulable, in.Input.Now())
		return unlessServing(ctx, in, row, func() reading {
			return reading{waiting: true, evidence: types.UnschedulableTermination(pod, message, stamped), startsWhenSeen: true}
		})
	}
	return unlessServing(ctx, in, row, func() reading {
		return reading{waiting: true, evidence: types.UnschedulableTermination(pod, message, since), waitSince: since}
	})
}
