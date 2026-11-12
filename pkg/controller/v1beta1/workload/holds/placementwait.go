package holds

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// placementWait is what one pass does with the attempt's placement wait
// (InstanceOperation.PlacementWaitSince): record its start, retire it, or
// leave it alone.
//
// The start is recorded when a pod name the attempt needs is held by a
// node that stopped reporting: the member on that node must be rebuilt,
// so the attempt cannot be fully placed until its replacement is. The
// start is the earlier of now and the scheduler's own wait, when another
// member is also unplaceable. It is kept while the attempt goes on
// waiting, even as the waiting pod changes: one member may be placed
// while another's name is still held, and the replacement member then
// waits on the scheduler in turn. A pass that sees neither wait but a pod
// still missing, unbound or held by a node that stopped reporting (the
// held name being freed by the force-delete sweep, its replacement not
// yet created or not yet refused by the scheduler) keeps it. It is
// retired once every pod the attempt asks for counts as placed
// (attemptPlaced), or once the attempt ends. A pass that sees neither wait
// but a pod of the attempt behind a scheduling gate retires it too. A queue
// may evict the attempt's pods, and OME then creates them behind the gate
// again. Time behind the gate is not a refusal, so the next refusal starts
// a fresh wait. A pod still refused beside a gated one keeps the start, so
// the grace still ends a gang that a queue admitted only in part.
//
// A scheduler wait alone records nothing: with no name held, the grace is
// measured from the unplaceable pod's own PodScheduled transition, as
// before. A member the grace ended and rebuilt then waits on its own
// clock. The exception is a refused pod whose PodScheduled time may be its
// creation behind a queue's gate. That time says nothing about the
// refusal, so the first pass that sees the refusal records the start. The
// start lives on the operation because the wait's record never replaces a
// container's exit record that the row keeps on LastFailure.
type placementWait struct {
	// since is the start to record; nil records nothing.
	since *metav1.Time
	// clear retires the recorded start.
	clear bool
}

// decidePlacementWait reads the pass's placement readings against the
// start the row recorded at the start of the pass. A Failed row has no
// attempt waiting, so none is recorded on it; the Failed write retired
// any start it held. Nor has the row of a gang surge replacement whose
// draining source reads Failed (drainSourceEnded): that ending is
// written on the source alone, so a start this row still carries is
// retired here, and none is recorded until the source opens its next
// attempt, which waits on its own clock.
//
// The scheduler's wait carries its own start, the PodScheduled
// transition, unless that time may be the pod's creation behind a queue's
// gate. A held name carries none: the held pod's creation can be days
// old. A wait with no start of its own starts now.
func decidePlacementWait(ctx context.Context, in PassInput, row Row, readings []reading) (placementWait, error) {
	if row.Excluded {
		return placementWait{}, nil
	}
	var recorded *metav1.Time
	if row.Status.Operation != nil {
		recorded = row.Status.Operation.PlacementWaitSince
	}
	if row.Status.Phase == types.InstancePhaseFailed {
		// The attempt ended; the write that ended it retired the start.
		return placementWait{}, nil
	}
	if drainSourceEnded(in.Rows, *row.Status) {
		return placementWait{clear: recorded != nil}, nil
	}
	scheduler, node, tracked := placementReadings(row, readings)
	if !tracked {
		return placementWait{}, nil
	}
	if scheduler.waiting || node.waiting {
		if recorded != nil || !(node.waiting || scheduler.startsWhenSeen) {
			return placementWait{}, nil
		}
		since := metav1.NewTime(in.Input.Now())
		if scheduler.waiting && !scheduler.waitSince.IsZero() && scheduler.waitSince.Before(&since) {
			since = scheduler.waitSince
		}
		return placementWait{since: &since}, nil
	}
	if recorded == nil {
		return placementWait{}, nil
	}
	gated, err := admissionGated(ctx, in, row)
	if err != nil || gated {
		return placementWait{clear: gated}, err
	}
	placed, err := attemptPlaced(ctx, in, row, node.nodeLost)
	if err != nil || !placed {
		return placementWait{}, err
	}
	return placementWait{clear: true}, nil
}

// sinceWith is the start a placement hold's record carries in its write.
func (w placementWait) sinceWith(token string) *metav1.Time {
	if placementToken(token) {
		return w.since
	}
	return nil
}

// clearsWith reports whether a placement hold's release retires the start
// in its write.
func (w placementWait) clearsWith(token string) bool {
	return w.clear && placementToken(token)
}

// apply retires the start when no placement hold's release carried it:
// the row reported another authority's wait, or none. A start to record
// always rides on the held name's own record, which every pass that
// decides one makes.
func (w placementWait) apply(ctx context.Context, in PassInput, row Row, readings []reading) error {
	if !w.clear {
		return nil
	}
	for i, a := range table {
		if readings[i].release && placementToken(a.token) && observedHoldingToken(in.Input, row.Status.Index, a.token) {
			return nil
		}
	}
	return status.ClearPlacementWait(ctx, in.Input, row.Status.Index)
}

// placementToken reports whether token is one of the two placement holds.
func placementToken(token string) bool {
	return token == types.WaitingReasonUnschedulable || token == types.WaitingReasonNodeUnknown
}

// placementReadings picks the scheduler's and the held name's readings
// out of the pass's readings. tracked is false for a row both skip: a
// leaving Instance the scale-down has not admitted, or a row with no
// attempt.
func placementReadings(row Row, readings []reading) (scheduler, node reading, tracked bool) {
	for i, a := range table {
		switch a.token {
		case types.WaitingReasonUnschedulable:
			scheduler = readings[i]
			tracked = tracked || !row.skips(a)
		case types.WaitingReasonNodeUnknown:
			node = readings[i]
			tracked = tracked || !row.skips(a)
		}
	}
	return scheduler, node, tracked
}

// drainSourceEnded reports whether replacement is the row of a gang surge
// replacement whose source reads Failed at the drain step. Once the
// replacement has taken over serving, an ending (the placement grace, a
// lost member) is written on the source's row alone, and the
// replacement's row stays live until the source opens its next attempt
// or ends the pair.
func drainSourceEnded(rows []Row, replacement types.InstanceStatus) bool {
	op := replacement.Operation
	if op == nil || op.Type != types.InstanceOperationUpdate || op.Step != types.UpdateStepGangSurgeTarget {
		return false
	}
	for _, r := range rows {
		src := r.Status.Operation
		if r.Status.Phase == types.InstancePhaseFailed && src != nil && src.Type == types.InstanceOperationUpdate &&
			src.Step == types.UpdateStepSurgeDrain && src.SurgeIndex != nil && *src.SurgeIndex == replacement.Index {
			return true
		}
	}
	return false
}

// admissionGated reports whether a pod of the row is held back by an
// admission scheduling gate. A pod being deleted is no longer part of the
// attempt and does not count.
func admissionGated(ctx context.Context, in PassInput, row Row) (bool, error) {
	own, _, err := in.rowPods(ctx, row)
	if err != nil {
		return false, err
	}
	for _, pod := range own {
		if types.PodAdmissionGated(pod) && pod.DeletionTimestamp == nil {
			return true, nil
		}
	}
	return false, nil
}

// attemptPlaced reports whether every pod the row's plan asks for exists,
// is not being deleted, is bound to a node, and is not named in nodeLost.
// A pod still held by a node that stopped reporting, or freed by the
// sweep in this pass, never counts as placed: the pass whose sweep frees
// a held name still lists that pod as bound, yet its member has to be
// rebuilt.
func attemptPlaced(ctx context.Context, in PassInput, row Row, nodeLost map[string]struct{}) (bool, error) {
	own, _, err := in.rowPods(ctx, row)
	if err != nil {
		return false, err
	}
	bound := make(map[string]bool, len(own))
	for _, pod := range own {
		if pod == nil || pod.DeletionTimestamp != nil || pod.Spec.NodeName == "" {
			continue
		}
		if _, lost := nodeLost[pod.Name]; !lost {
			bound[pod.Name] = true
		}
	}
	for name := range targetPodNames(in, row) {
		if !bound[name] {
			return false, nil
		}
	}
	return true, nil
}
