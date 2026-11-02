package placement

import (
	"context"
	"fmt"
	"maps"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// Event reasons for the cleanup release request on a source InferenceService.
const (
	// cleanupWaitReleasedReason: the request ended its plan revision's pause.
	cleanupWaitReleasedReason = "CleanupWaitReleased"
	// cleanupReleaseSkippedReason: the request was removed without effect.
	cleanupReleaseSkippedReason = "CleanupReleaseSkipped"
	// cleanupReleaseRejectedReason: the request was removed because its value
	// is not a plan revision.
	cleanupReleaseRejectedReason = "CleanupReleaseRejected"
	// cleanupReleaseNotRemovedReason: a request that cannot act on the stored
	// plan stays, because removing it failed.
	cleanupReleaseNotRemovedReason = "CleanupReleaseNotRemoved"
)

// cleanupReleaseRevision reads a cleanup release request: a plan revision
// written as a positive whole number, with no sign, spaces or leading zeros.
func cleanupReleaseRevision(value string) (int64, error) {
	revision, err := strconv.ParseInt(value, 10, 64)
	if err != nil || revision <= 0 || strconv.FormatInt(revision, 10) != value {
		return 0, fmt.Errorf("%q is not a placement plan revision", value)
	}
	return revision, nil
}

// releasesCleanupWait reports whether the source asks to end the cleanup wait
// of its current plan revision while that plan pauses member growth.
func releasesCleanupWait(source *v1beta1.InferenceService) bool {
	value, present := source.Annotations[constants.PlacementReleaseCleanupWaitAnnotation]
	if !present || source.Status.Placement == nil || source.Status.Placement.Plan == nil {
		return false
	}
	revision, err := cleanupReleaseRevision(value)
	accepted := source.Status.Placement.Plan
	return err == nil && accepted.PauseSurge && revision == accepted.Revision
}

// planIntentAnnotations returns the source annotations a plan's input digest
// covers. The cleanup release request names a plan revision, so setting or
// removing it must not make a new one. The API server stores no empty
// annotation map, so a map left empty counts as none.
func planIntentAnnotations(annotations map[string]string) map[string]string {
	if _, present := annotations[constants.PlacementReleaseCleanupWaitAnnotation]; !present {
		return annotations
	}
	out := maps.Clone(annotations)
	delete(out, constants.PlacementReleaseCleanupWaitAnnotation)
	if len(out) == 0 {
		return nil
	}
	return out
}

// answerOutdatedCleanupRelease removes a cleanup release request that cannot
// act on the stored plan: a value that is not a revision, a revision other than
// the current one, or a plan that does not pause. It reports whether it
// answered one. The pass then ends, and the next one starts from the updated
// source. Checking every pass before any plan write means a request for a later
// revision is gone before the plan can reach it.
//
// If the removal fails, only a request for a revision the plan has not reached
// returns the error and stops the pass: the plan must not reach that revision
// while the request stays. Any other request here can never act, because
// revisions only grow and a revision never changes whether it pauses, so the
// pass goes on. Both cases record a warning event.
func (r *Reconciler) answerOutdatedCleanupRelease(ctx context.Context, source *v1beta1.InferenceService) (bool, error) {
	key := constants.PlacementReleaseCleanupWaitAnnotation
	value, present := source.Annotations[key]
	if !present {
		return false, nil
	}
	var accepted *v1beta1.PlacementPlanStatus
	if source.Status.Placement != nil {
		accepted = source.Status.Placement.Plan
	}
	eventType, reason := corev1.EventTypeNormal, cleanupReleaseSkippedReason
	var message string
	revision, err := cleanupReleaseRevision(value)
	switch {
	case err != nil:
		eventType, reason = corev1.EventTypeWarning, cleanupReleaseRejectedReason
		message = fmt.Sprintf("Removed %s=%q: the value must be a placement plan revision, a positive whole number copied from status.placement.plan.revision", key, value)
	case accepted == nil:
		message = fmt.Sprintf("Removed %s=%s: the service has no placement plan yet, so there is nothing to release", key, value)
	case revision != accepted.Revision:
		message = fmt.Sprintf("Removed %s=%s: placement plan revision %d is current, and a request acts only on the revision it names", key, value, accepted.Revision)
	case !accepted.PauseSurge:
		message = fmt.Sprintf("Removed %s=%s: placement plan revision %d does not pause member growth, so there is nothing to release", key, value, accepted.Revision)
	default:
		return false, nil
	}
	removeErr := r.removeCleanupRelease(ctx, source, value, eventType, reason, message)
	switch {
	case removeErr == nil:
		return true, nil
	case err == nil && (accepted == nil || revision > accepted.Revision):
		r.recordEvent(source, corev1.EventTypeWarning, cleanupReleaseNotRemovedReason, fmt.Sprintf(
			"Could not remove %s=%s: %v. Placement plan revision %s does not exist yet. Placement waits until the annotation is gone, so the request cannot end that revision's pause",
			key, value, removeErr, value))
		return false, removeErr
	default:
		r.Log.Error(removeErr, "removing an outdated cleanup release request failed", "isvc", source.Namespace+"/"+source.Name, "value", value)
		r.recordEvent(source, corev1.EventTypeWarning, cleanupReleaseNotRemovedReason, cleanupReleaseStaysMessage(value, removeErr))
		return false, nil
	}
}

// answerEndedCleanupRelease removes the cleanup release request for a plan
// revision whose pause this pass ended. A request that released the wait was
// recorded when the pause ended, and one the pass did not need is recorded
// here. A failed removal records a warning instead.
func (r *Reconciler) answerEndedCleanupRelease(ctx context.Context, accepted *v1beta1.InferenceService, released bool) {
	key := constants.PlacementReleaseCleanupWaitAnnotation
	value := accepted.Annotations[key]
	var reason, message string
	if !released {
		reason = cleanupReleaseSkippedReason
		message = fmt.Sprintf("Removed %s=%s: the pause of placement plan revision %d ended without the request", key, value, accepted.Status.Placement.Plan.Revision)
	}
	if err := r.removeCleanupRelease(ctx, accepted, value, corev1.EventTypeNormal, reason, message); err != nil {
		// The request names a revision that is no longer current, so the next
		// pass removes it without effect.
		r.Log.Error(err, "removing an answered cleanup release request failed", "isvc", accepted.Namespace+"/"+accepted.Name)
		r.recordEvent(accepted, corev1.EventTypeWarning, cleanupReleaseNotRemovedReason, cleanupReleaseStaysMessage(value, err))
	}
}

// cleanupReleaseStaysMessage says that a request which can no longer act stays
// on the source, because removing it failed with err.
func cleanupReleaseStaysMessage(value string, err error) string {
	return fmt.Sprintf("Could not remove %s=%q: %v. The request cannot act on this or any later plan revision, so placement goes on. OME tries again on its next reconcile",
		constants.PlacementReleaseCleanupWaitAnnotation, value, err)
}

// removeCleanupRelease deletes an answered cleanup release request from the
// stored source, then records the event when reason is set. It reads the
// source again under conflict retry, so other changes survive. A value changed
// since the pass read it is a new request and stays for the next pass.
func (r *Reconciler) removeCleanupRelease(ctx context.Context, source *v1beta1.InferenceService, value, eventType, reason, message string) error {
	key := constants.PlacementReleaseCleanupWaitAnnotation
	removed := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceService{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(source), fresh); err != nil {
			return err
		}
		if current, present := fresh.Annotations[key]; fresh.UID != source.UID || !present || current != value {
			return nil
		}
		delete(fresh.Annotations, key)
		if err := r.Update(ctx, fresh); err != nil {
			return err
		}
		removed = true
		return nil
	})
	if err != nil {
		return client.IgnoreNotFound(err)
	}
	if !removed {
		return nil
	}
	r.Log.Info("removed an answered cleanup release request", "isvc", source.Namespace+"/"+source.Name, "value", value, "reason", reason)
	if reason != "" {
		r.recordEvent(source, eventType, reason, message)
	}
	return nil
}

// recordCleanupReleased records that the operator's request ended the pause of
// the accepted plan revision while members held more replicas than it assigns.
func (r *Reconciler) recordCleanupReleased(accepted *v1beta1.InferenceService, note string) {
	r.Log.Info("cleanup wait released at operator request", "isvc", accepted.Namespace+"/"+accepted.Name, "revision", accepted.Status.Placement.Plan.Revision)
	r.recordEvent(accepted, corev1.EventTypeNormal, cleanupWaitReleasedReason, note+". Member autoscalers and rollouts may add replicas again")
}

// cleanupReleasedNote says that the operator ended the cleanup wait of the
// accepted plan revision, with the replicas the members held against the plan.
func cleanupReleasedNote(accepted *v1beta1.InferenceService, observations map[string]plannedMemberObservation) string {
	var held int64
	for _, candidate := range accepted.Status.Placement.Candidates {
		observed := observations[candidate.Cluster]
		held += int64(observed.MemberState.Occupied) + int64(observed.RolloutReserved)
	}
	return fmt.Sprintf("The operator ended the AwaitingMemberCleanup wait of placement plan revision %d with %s. Members held %d replicas, counting replicas reserved for rolling updates, and the plan assigns %d",
		accepted.Status.Placement.Plan.Revision, constants.PlacementReleaseCleanupWaitAnnotation, held, accepted.Status.Placement.Plan.AssignedReplicas)
}

// recordEvent emits an event on the source; a Reconciler without a recorder
// emits none.
func (r *Reconciler) recordEvent(source *v1beta1.InferenceService, eventType, reason, message string) {
	if r.Recorder != nil {
		r.Recorder.Event(source, eventType, reason, message)
	}
}
