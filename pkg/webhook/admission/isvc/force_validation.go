package isvc

import (
	"errors"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// validateForceRequest checks a canary force this request adds or changes.
// A request that leaves the force alone, or removes it, is admitted: removing
// a force stops it, and any responder must be able to stop one. The open run
// is read from the stored object, since a request cannot change status, so a
// force cannot arrive on create.
func (v *InferenceServiceValidator) validateForceRequest(oldIsvc, isvc *v1beta1.InferenceService) (admission.Warnings, error) {
	key := constants.RolloutForceModeAnnotation
	mode, present := isvc.Annotations[key]
	run, bound := isvc.Annotations[constants.RolloutForceRunAnnotation]
	if !present {
		return nil, runWithoutMode(oldIsvc, run, bound)
	}
	if oldIsvc != nil {
		if was, had := oldIsvc.Annotations[key]; had && was == mode && oldIsvc.Annotations[constants.RolloutForceRunAnnotation] == run {
			return nil, nil
		}
	}
	if !v.ForceCompleteConfigured {
		return nil, errors.New("the canary force is not configured on this cluster: the rollout.forceComplete block of the inferenceservice-config ConfigMap is absent (RolloutForceNotConfigured)")
	}
	if mode != constants.RolloutForceModeDisruptive {
		return nil, fmt.Errorf("%s=%s: this version of OME runs only the %s force mode (RolloutForceModeUnavailable)", key, mode, constants.RolloutForceModeDisruptive)
	}
	open := v1beta1.ActiveRolloutRunID(oldIsvc)
	if open == "" {
		return nil, fmt.Errorf("%s: this InferenceService has no open rollout run, so there is nothing to force (RolloutForceNoOpenRun)", key)
	}
	if run != open {
		return nil, fmt.Errorf("%s names run %q, but the open rollout run is %q, from status.rollout.activeRun.runID (RolloutForceRunMismatch)", constants.RolloutForceRunAnnotation, run, open)
	}
	if !runMovesACanaryGroup(oldIsvc) {
		return nil, fmt.Errorf("%s: the open rollout run %q starts no canary on this InferenceService (no canary group moves from an earlier version), so there is nothing to force (RolloutForceNoCanary)", key, open)
	}
	if paused, _ := constants.RolloutPauseState(isvc.Annotations); paused {
		return nil, fmt.Errorf("%s: the rollout is paused, and a force never overrides a pause; clear the pause first (RolloutForcePaused)", key)
	}
	return admission.Warnings{
		key + "=" + mode + " deletes every Instance of the canary group still on the old version now; the service serves nothing until the new pods are Ready",
	}, nil
}

// runWithoutMode refuses a run key the request adds or changes while no
// force mode is set: it forces nothing, and a later mode would start a force
// on it. A run key left behind when the mode key is removed is admitted, so
// removing the mode alone stops a force.
func runWithoutMode(oldIsvc *v1beta1.InferenceService, run string, bound bool) error {
	if !bound {
		return nil
	}
	if oldIsvc != nil {
		if was, had := oldIsvc.Annotations[constants.RolloutForceRunAnnotation]; had && was == run {
			return nil
		}
	}
	return fmt.Errorf("%s needs %s: a run ID alone forces nothing (RolloutForceRunWithoutMode)", constants.RolloutForceRunAnnotation, constants.RolloutForceModeAnnotation)
}

// runMovesACanaryGroup reports whether some canary group of the recorded plan
// has a canary the force can act on: one the open run starts, with an
// earlier revision recorded for its primary, or one already running on a
// group the run moves, whose stable revision may have been read from the
// replica rather than recorded in the run. A service whose rollout has no
// canary, a run that changes only other groups (whatever record an earlier
// run left on them), and a first rollout with no earlier revision have
// nothing to force.
func runMovesACanaryGroup(isvc *v1beta1.InferenceService) bool {
	for _, g := range rollout.CanaryGroups(isvc, rollout.Policies{}) {
		if rollout.RunStartsCanary(isvc, g) {
			return true
		}
		cs := rollout.CanaryStatusFor(&isvc.Status, rollout.PrimaryOf(g))
		if cs != nil && cs.RolledBackRevisionHash == "" && int(cs.CurrentStep) < len(g.Canary.Steps) &&
			v1beta1.RolloutRunRetargets(isvc, g.Components) {
			return true
		}
	}
	return false
}
