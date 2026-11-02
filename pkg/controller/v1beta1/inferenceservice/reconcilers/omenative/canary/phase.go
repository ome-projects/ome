package canary

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// setPhase sets the RolloutPhase on a component's status, creating the status
// map and entry as needed. (Map values are structs, so it round-trips through a
// local copy.)
func setPhase(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, phase v1beta1.RolloutPhase) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[c]
	cs.RolloutPhase = phase
	isvc.Status.Components[c] = cs
}

// parkFailed records a stop in one place: the marker in status and the
// projected phase, so a stop can never exist in one and not the other. A
// stop already recorded keeps its reason and time. The first record is
// announced, after the event that names the reason's own detail where
// there is one: from this pass on the roll is held where it stands
// (EffectivePartition holds every Instance on the revision it runs) and the
// split stays as the stop left it.
func parkFailed(in ReconcileInputs, cs *v1beta1.CanaryStatus, reason v1beta1.CanaryFailureReason) {
	if cs.Failed == nil {
		cs.Failed = &v1beta1.CanaryFailure{Reason: reason, Time: &metav1.Time{Time: in.Now}}
		emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonCanaryRollHeld,
			"%s canary stopped at step %d (%s): the roll is held where it stands, %s, and the traffic split stays as written; a rollback (%s), a resume (%s) or a new revision ends the hold",
			in.Component, cs.CurrentStep, reason, heldMembers(in), constants.RolloutRollbackAnnotation, constants.RolloutResumeAnnotation)
	}
	setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseFailed)
}

// heldMembers describes how far each retargeted member's roll had come when
// the unit stopped: its serving Instances on its target revision against the
// count it is scaled to, the primary first and the secondaries by name.
func heldMembers(in ReconcileInputs) string {
	parts := []string{fmt.Sprintf("%s %d of %d Instances on revision %s", in.Component, readyCanaryCapacity(in), in.DesiredReplicas, in.CanaryRevisionHash)}
	names := make([]v1beta1.ComponentType, 0, len(in.Secondaries))
	for c, m := range in.Secondaries {
		if m.CanaryRevisionHash != "" && m.CanaryRevisionHash != m.StableRevisionHash {
			names = append(names, c)
		}
	}
	sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })
	for _, c := range names {
		m := in.Secondaries[c]
		parts = append(parts, fmt.Sprintf("%s %d of %d Instances on revision %s", c, m.ReadyCanaryCapacity, m.DesiredReplicas, m.CanaryRevisionHash))
	}
	return strings.Join(parts, ", ")
}
