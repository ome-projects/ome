package inferenceservice

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// ttlExpiredReason is the event reason recorded when an InferenceService is
// deleted because spec.ttlSecondsAfterCreation has passed.
const ttlExpiredReason = "TTLExpired"

// ttlRemaining reports how long until the InferenceService's
// spec.ttlSecondsAfterCreation passes; a value <= 0 means it already has.
// ok is false when no TTL applies.
func ttlRemaining(isvc *v1beta1.InferenceService, now time.Time) (remaining time.Duration, ok bool) {
	ttl := isvc.Spec.TTLSecondsAfterCreation
	if ttl == nil || isvc.CreationTimestamp.IsZero() {
		return 0, false
	}
	expiry := isvc.CreationTimestamp.Add(time.Duration(*ttl) * time.Second)
	return expiry.Sub(now), true
}

// requeueBy returns result changed so the next reconcile runs no later than
// after from now. An error or an immediate requeue already runs sooner, so
// those results are kept as they are.
func requeueBy(result ctrl.Result, err error, after time.Duration) ctrl.Result {
	if err != nil || after <= 0 {
		return result
	}
	if result.Requeue && result.RequeueAfter == 0 {
		return result
	}
	if result.RequeueAfter == 0 || result.RequeueAfter > after {
		result.RequeueAfter = after
	}
	return result
}

// deleteExpired deletes an InferenceService whose
// spec.ttlSecondsAfterCreation has passed. Background propagation lets
// garbage collection remove what it owns, and the UID precondition keeps a
// recreated object with the same name from being deleted in its place.
func (r *InferenceServiceReconciler) deleteExpired(ctx context.Context, isvc *v1beta1.InferenceService) error {
	uid := isvc.UID
	err := r.Delete(ctx, isvc,
		client.PropagationPolicy(metav1.DeletePropagationBackground),
		client.Preconditions{UID: &uid},
	)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(isvc, corev1.EventTypeNormal, ttlExpiredReason,
			"Deleted because ttlSecondsAfterCreation=%d has passed", *isvc.Spec.TTLSecondsAfterCreation)
	}
	return nil
}

// now reads the reconciler's clock, or the real clock when none is set.
func (r *InferenceServiceReconciler) now() time.Time {
	if r.Clock != nil {
		return r.Clock.Now()
	}
	return time.Now()
}
