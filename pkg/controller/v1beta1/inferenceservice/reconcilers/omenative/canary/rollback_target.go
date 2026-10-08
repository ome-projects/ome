package canary

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// emptyTarget is one member whose stable revision, the target a rollback
// returns it to, runs no live Instance.
type emptyTarget struct {
	component v1beta1.ComponentType
	stable    string
}

// emptyRollbackTargets lists the members a rollback could not return to a
// revision they stand on. A member with more than one Instance keeps one on
// its stable revision for as long as its ladder is in flight (the held
// floor), so a stable revision with no pod there is not the one the ladder
// shifted traffic from: pointing the member's replica at it would rebuild
// every Instance onto a revision the fleet left. A single-Instance member
// stages in place and holds no floor, so an empty stable revision there is
// the ladder's own doing and is not listed. Pods are read per member from
// the unit-wide view, ready or not; the primary falls back to its ready
// pods when no unit-wide view was observed.
func emptyRollbackTargets(in ReconcileInputs, cs *v1beta1.CanaryStatus) []emptyTarget {
	var out []emptyTarget
	primaryPods, observed := in.GroupTotalPerRevisionPods[in.Component]
	if !observed {
		primaryPods = in.PerRevisionPods
	}
	stable := stableHashFor(cs, in.PerRevisionPods, cs.CanaryRevisionHash)
	if stable != "" && heldFloor(in.DesiredReplicas) > 0 && primaryPods[stable] == 0 {
		out = append(out, emptyTarget{component: in.Component, stable: stable})
	}
	for c, m := range in.Secondaries {
		pods, observed := in.GroupTotalPerRevisionPods[c]
		if !observed {
			pods = in.GroupReadyPerRevisionPods[c]
		}
		if m.StableRevisionHash != "" && heldFloor(m.DesiredReplicas) > 0 && pods[m.StableRevisionHash] == 0 {
			out = append(out, emptyTarget{component: c, stable: m.StableRevisionHash})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].component < out[j].component })
	return out
}

// describeEmptyTargets names each member with its empty stable revision.
func describeEmptyTargets(targets []emptyTarget) string {
	parts := make([]string, 0, len(targets))
	for _, t := range targets {
		parts = append(parts, fmt.Sprintf("%s (stable revision %s)", t.component, t.stable))
	}
	return strings.Join(parts, ", ")
}

// rollBackOnGateFailure starts the revert a failed gate decided, unless a
// member's stable revision runs no Instance: then the ladder parks Failed
// where it stands and signals no revert. The unit's split is written once
// more through the serving rule before the park, so a stable side with no
// serving pod carries no weight and the serving side carries the rest; a
// member whose stable serves keeps the step's split. The park reads as the
// stable revision being unavailable, the one marker the status carries for
// a rollback with nothing to return to; the event says which member and
// which revision. The operator's rollback request still proceeds from the
// park, and a new target re-arms the unit.
func rollBackOnGateFailure(in ReconcileInputs, cs *v1beta1.CanaryStatus, partition int32) *Result {
	if empty := emptyRollbackTargets(in, cs); len(empty) > 0 {
		holdGroupTraffic(in)
		parkFailed(in.ISVC, in.Component, cs, v1beta1.CanaryFailureStableRevisionMissing, in.Now)
		emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonCanaryStableRevisionEmpty,
			"%s is not rolled back on its own: %s runs no live Instance to return to; parked Failed at step %d until a rollback is requested (%s) or a new target appears",
			in.Component, describeEmptyTargets(empty), cs.CurrentStep, constants.RolloutRollbackAnnotation)
		return (&Result{Active: true, Partition: partition}).wake(in.ParkedRequeue)
	}
	cs.RolledBackRevisionHash = cs.CanaryRevisionHash
	return reconcileRollback(in, cs)
}

// warnEmptyRollbackTargets records, for a rollback the operator requested,
// that it returns a member to a stable revision running no Instance: the
// revert proceeds and rebuilds every Instance of that member onto it.
func warnEmptyRollbackTargets(in ReconcileInputs, cs *v1beta1.CanaryStatus) {
	if empty := emptyRollbackTargets(in, cs); len(empty) > 0 {
		emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonCanaryStableRevisionEmpty,
			"%s rolls back as requested although %s runs no live Instance; the revert rebuilds every Instance onto it",
			in.Component, describeEmptyTargets(empty))
	}
}
