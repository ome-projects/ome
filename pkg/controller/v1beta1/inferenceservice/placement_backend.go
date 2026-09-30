package inferenceservice

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

var placementBackendConditions = apis.NewLivingConditionSet()

// holdPlacementBackend writes only the advisory condition. Serving state and
// child resources remain intact until every component resolves to OMENative.
func (r *InferenceServiceReconciler) holdPlacementBackend(ctx context.Context, service *v1beta1.InferenceService, cause error) (ctrl.Result, error) {
	return r.holdPlacementCondition(ctx, service, v1beta1.PlacementBackendReady, "UnsupportedPlacementBackend", cause)
}

func (r *InferenceServiceReconciler) holdPlacementCondition(ctx context.Context, service *v1beta1.InferenceService, condition apis.ConditionType, reason string, cause error) (ctrl.Result, error) {
	base := service.DeepCopy()
	placementBackendConditions.Manage(&service.Status).SetCondition(apis.Condition{
		Type: condition, Status: corev1.ConditionFalse, Reason: reason, Message: cause.Error(),
	})
	if equality.Semantic.DeepEqual(base.Status.Conditions, service.Status.Conditions) {
		return ctrl.Result{}, nil
	}
	err := r.Status().Patch(ctx, service, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	if apierrors.IsConflict(err) {
		return ctrl.Result{Requeue: true}, nil
	}
	return ctrl.Result{}, client.IgnoreNotFound(err)
}

func clearPlacementBackendHold(service *v1beta1.InferenceService) {
	if service.Status.GetCondition(v1beta1.PlacementBackendReady) != nil {
		placementBackendConditions.Manage(&service.Status).SetCondition(apis.Condition{
			Type: v1beta1.PlacementBackendReady, Status: corev1.ConditionTrue, Reason: "OMENativeResolved",
		})
	}
}
