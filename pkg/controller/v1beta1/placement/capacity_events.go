package placement

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func capacityFreshCondition(status corev1.ConditionStatus, reason, message string) policyCondition {
	return policyCondition{condType: apis.ConditionType(v1beta1.PlacementCapacityFresh), cond: apis.Condition{Status: status, Reason: reason, Message: message}}
}

func (r *Reconciler) isvcsForCapacityChange(ctx context.Context, obj client.Object) []ctrl.Request {
	root, ok := obj.(*v1beta1.AcceleratorQuota)
	if !ok || r.Capacity == nil || root.Name != r.Capacity.RootName {
		return nil
	}
	list := &v1beta1.InferenceServiceList{}
	if err := r.List(ctx, list, client.MatchingFields{placementEligibleIndexField: placementEligibleIndexValue}); err != nil {
		r.Log.Error(err, "list capacity-placement services")
		return nil
	}
	var requests []ctrl.Request
	for _, source := range list.Items {
		if IsPlacementEligible(&source) && placementMode(&source) == v1beta1.PlacementModeSplitByCapacity {
			requests = append(requests, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(&source)})
		}
	}
	return requests
}

var capacityRelevantChange = predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
	oldRoot, oldOK := e.ObjectOld.(*v1beta1.AcceleratorQuota)
	newRoot, newOK := e.ObjectNew.(*v1beta1.AcceleratorQuota)
	if !oldOK || !newOK {
		return true
	}
	return oldRoot.UID != newRoot.UID || !oldRoot.DeletionTimestamp.Equal(newRoot.DeletionTimestamp) || !equality.Semantic.DeepEqual(capacityEventRows(oldRoot), capacityEventRows(newRoot))
}}

// capacityEventRows excludes heartbeat versions, quota usage and high-water
// marks. Periodic refresh still checks report freshness and runtime changes.
func capacityEventRows(root *v1beta1.AcceleratorQuota) []v1beta1.AcceleratorCapacityStatus {
	rows := root.DeepCopy().Status.Capacity
	for i := range rows {
		rows[i].ObservedAt = nil
		rows[i].HighWaterMark = resource.Quantity{}
		for j := range rows[i].PerCluster {
			rows[i].PerCluster[j].ObservedAt = nil
			rows[i].PerCluster[j].ReportResourceVersion = ""
			rows[i].PerCluster[j].HighWaterMark = resource.Quantity{}
		}
	}
	return rows
}
