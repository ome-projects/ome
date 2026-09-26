package inferenceservice

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// Only identity/request changes alter the mandatory artifact selectors. In
// particular, usage annotations and observed Model status must not feed back
// into the InferenceService reconciliation loop.
func modelArtifactChangePredicate() predicate.Predicate {
	return predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		if e.ObjectOld == nil || e.ObjectNew == nil {
			return true
		}
		return e.ObjectOld.GetUID() != e.ObjectNew.GetUID() ||
			e.ObjectOld.GetAnnotations()[constants.ModelArtifactRehydrationIDAnnotation] != e.ObjectNew.GetAnnotations()[constants.ModelArtifactRehydrationIDAnnotation]
	}}
}

// isvcsReferencingModel follows primary Model resolution, including a newly
// appearing namespaced Model taking over legacy cluster fallback. Overlays keep
// their existing readiness scope; they are not primary artifact requirements.
func (r *InferenceServiceReconciler) isvcsReferencingModel(ctx context.Context, model client.Object) []reconcile.Request {
	_, namespaced := model.(*v1beta1.BaseModel)
	if _, cluster := model.(*v1beta1.ClusterBaseModel); !namespaced && !cluster {
		return nil
	}
	services := &v1beta1.InferenceServiceList{}
	if err := r.List(ctx, services); err != nil {
		r.Log.Error(err, "List InferenceServices for Model change")
		return nil
	}
	var requests []reconcile.Request
	for _, service := range services.Items {
		ref := service.Spec.Model
		if ref == nil || ref.Name != model.GetName() {
			continue
		}
		kind, err := isvcutils.ModelReferenceKind(ref)
		if err != nil {
			continue
		}
		if namespaced {
			if kind == "ClusterBaseModel" || service.Namespace != model.GetNamespace() {
				continue
			}
		} else {
			if kind == "BaseModel" {
				continue
			}
			if kind == "" {
				local := &v1beta1.BaseModel{}
				err := r.Get(ctx, client.ObjectKey{Namespace: service.Namespace, Name: ref.Name}, local)
				if err == nil {
					continue
				}
				if !apierrors.IsNotFound(err) {
					// An uncertain cached lookup may enqueue extra work; resolution
					// during reconciliation still fails closed on lookup errors.
					r.Log.Error(err, "Check legacy Model scope", "service", client.ObjectKeyFromObject(&service))
				}
			}
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&service)})
	}
	return requests
}
