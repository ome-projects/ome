package inferenceservice

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// Metadata-only eviction requests wake consumers so gated services recheck
// admission. Provisioning clients, not this controller, own model intent.
func (r *InferenceServiceReconciler) isvcsReferencingEvictedModel(ctx context.Context, obj client.Object) []reconcile.Request {
	if obj.GetAnnotations()[constants.ModelArtifactResidencyAnnotation] != constants.ModelArtifactResidencyEvicted {
		return nil
	}
	var opts []client.ListOption
	if obj.GetNamespace() != "" {
		opts = append(opts, client.InNamespace(obj.GetNamespace()))
	}
	services := &v1beta1.InferenceServiceList{}
	if err := r.List(ctx, services, opts...); err != nil {
		r.Log.Error(err, "Failed to find consumers of evicted model", "model", obj.GetName())
		return nil
	}
	var requests []reconcile.Request
	for _, service := range services.Items {
		if !service.DeletionTimestamp.IsZero() {
			continue
		}
		uses := service.Annotations[constants.BaseModelName] == obj.GetName()
		if service.Spec.Model != nil {
			uses = uses || service.Spec.Model.Name == obj.GetName()
			for _, overlay := range service.Spec.Model.Overlays {
				uses = uses || overlay.Name == obj.GetName()
			}
		}
		if uses {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&service)})
		}
	}
	return requests
}
