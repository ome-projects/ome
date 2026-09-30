package canary

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
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

// parkFailed records a park in one place: the marker in status and the
// projected phase, so a park can never exist in one and not the other. A
// park already recorded keeps its reason and time.
func parkFailed(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, cs *v1beta1.CanaryStatus, reason v1beta1.CanaryFailureReason, now time.Time) {
	if cs.Failed == nil {
		cs.Failed = &v1beta1.CanaryFailure{Reason: reason, Time: &metav1.Time{Time: now}}
	}
	setPhase(isvc, c, v1beta1.RolloutPhaseFailed)
}
