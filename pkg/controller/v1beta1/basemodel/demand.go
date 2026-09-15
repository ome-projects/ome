package basemodel

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

const modelDemandIndexField = "ome.io/model-demand-reference"

type modelDemandReference struct {
	kind string
	key  types.NamespacedName
}

// SetupModelDemandIndex registers the single manager-cache index used by both
// BaseModel reconcilers. Every model-agent intentionally remains unaware of
// InferenceServices.
func SetupModelDemandIndex(ctx context.Context, mgr ctrl.Manager) error {
	return mgr.GetFieldIndexer().IndexField(
		ctx,
		&v1beta1.InferenceService{},
		modelDemandIndexField,
		func(obj client.Object) []string {
			ref, ok := modelReferenceForInferenceService(obj)
			if !ok {
				return nil
			}
			return []string{modelDemandIndexValue(ref)}
		},
	)
}

func modelReferenceForInferenceService(obj client.Object) (modelDemandReference, bool) {
	isvc, ok := obj.(*v1beta1.InferenceService)
	if !ok || isvc.Spec.Model == nil || strings.TrimSpace(isvc.Spec.Model.Name) == "" {
		return modelDemandReference{}, false
	}
	if isvc.Spec.Model.APIGroup != nil {
		apiGroup := strings.TrimSpace(*isvc.Spec.Model.APIGroup)
		if apiGroup != "" && !strings.EqualFold(apiGroup, constants.OMEAPIGroupName) {
			return modelDemandReference{}, false
		}
	}

	kind := constants.ClusterBaseModel
	if isvc.Spec.Model.Kind != nil && strings.TrimSpace(*isvc.Spec.Model.Kind) != "" {
		kind = strings.TrimSpace(*isvc.Spec.Model.Kind)
	}
	name := strings.TrimSpace(isvc.Spec.Model.Name)
	switch {
	case strings.EqualFold(kind, constants.BaseModel):
		return modelDemandReference{
			kind: constants.BaseModel,
			key:  types.NamespacedName{Namespace: isvc.Namespace, Name: name},
		}, true
	case strings.EqualFold(kind, constants.ClusterBaseModel):
		return modelDemandReference{
			kind: constants.ClusterBaseModel,
			key:  types.NamespacedName{Name: name},
		}, true
	default:
		return modelDemandReference{}, false
	}
}

func modelDemandIndexValue(ref modelDemandReference) string {
	if ref.kind == constants.BaseModel {
		return "base:" + ref.key.String()
	}
	return "cluster:" + ref.key.Name
}

func modelDemandReferenceForModel(obj client.Object, isClusterScoped bool) modelDemandReference {
	if isClusterScoped {
		return modelDemandReference{
			kind: constants.ClusterBaseModel,
			key:  types.NamespacedName{Name: obj.GetName()},
		}
	}
	return modelDemandReference{
		kind: constants.BaseModel,
		key:  types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()},
	}
}

func modelDemandEventHandler(kind string) handler.EventHandler {
	enqueue := func(obj client.Object, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) {
		ref, ok := modelReferenceForInferenceService(obj)
		if !ok || ref.kind != kind {
			return
		}
		queue.Add(reconcile.Request{NamespacedName: ref.key})
	}

	return handler.Funcs{
		CreateFunc: func(_ context.Context, e event.CreateEvent, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			enqueue(e.Object, queue)
		},
		UpdateFunc: func(_ context.Context, e event.UpdateEvent, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			// Reconcile both sides so a model-reference replacement clears the old
			// model's demand as well as setting demand on the new model.
			enqueue(e.ObjectOld, queue)
			enqueue(e.ObjectNew, queue)
		},
		DeleteFunc: func(_ context.Context, e event.DeleteEvent, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			enqueue(e.Object, queue)
		},
	}
}

func reconcileModelDownloadDemand(
	ctx context.Context,
	kubeClient client.Client,
	obj client.Object,
	isClusterScoped bool,
) (bool, error) {
	ref := modelDemandReferenceForModel(obj, isClusterScoped)
	isvcs := &v1beta1.InferenceServiceList{}
	if err := kubeClient.List(
		ctx,
		isvcs,
		client.MatchingFields{modelDemandIndexField: modelDemandIndexValue(ref)},
	); err != nil {
		return false, fmt.Errorf("list serving demand for %s: %w", ref.key, err)
	}

	var count int32
	for i := range isvcs.Items {
		isvc := &isvcs.Items[i]
		if !isvc.DeletionTimestamp.IsZero() {
			continue
		}
		current, ok := modelReferenceForInferenceService(isvc)
		if ok && current == ref {
			count++
		}
	}

	_, status, err := shared.ModelSpecAndStatus(obj)
	if err != nil {
		return false, err
	}
	if modelDownloadDemandMatches(status.DownloadScheduling, count) {
		return false, nil
	}

	updateStatus := func(ctx context.Context, c client.Client, latest client.Object) error {
		_, latestStatus, err := shared.ModelSpecAndStatus(latest)
		if err != nil {
			return err
		}
		if count == 0 {
			latestStatus.DownloadScheduling = nil
		} else {
			latestStatus.DownloadScheduling = &v1beta1.ModelDownloadSchedulingStatus{
				ServingDemand:  true,
				ReferenceCount: count,
			}
		}
		return c.Status().Update(ctx, latest)
	}
	if err := shared.RetryUpdate(ctx, kubeClient, ctrl.Log.WithName("ModelDownloadDemand"), obj, "status", updateStatus); err != nil {
		return false, fmt.Errorf("project serving demand for %s: %w", ref.key, err)
	}
	return true, nil
}

func reconcileModelDownloadScheduling(
	ctx context.Context,
	kubeClient client.Client,
	obj client.Object,
	isClusterScoped bool,
	enabled bool,
) (bool, error) {
	if enabled {
		return reconcileModelDownloadDemand(ctx, kubeClient, obj, isClusterScoped)
	}
	return clearModelDownloadScheduling(ctx, kubeClient, obj)
}

func clearModelDownloadScheduling(
	ctx context.Context,
	kubeClient client.Client,
	obj client.Object,
) (bool, error) {
	_, status, err := shared.ModelSpecAndStatus(obj)
	if err != nil {
		return false, err
	}
	if status.DownloadScheduling == nil {
		return false, nil
	}

	updateStatus := func(ctx context.Context, c client.Client, latest client.Object) error {
		_, latestStatus, err := shared.ModelSpecAndStatus(latest)
		if err != nil {
			return err
		}
		latestStatus.DownloadScheduling = nil
		return c.Status().Update(ctx, latest)
	}
	if err := shared.RetryUpdate(ctx, kubeClient, ctrl.Log.WithName("ModelDownloadDemand"), obj, "status", updateStatus); err != nil {
		return false, fmt.Errorf("clear model download scheduling for %s: %w", client.ObjectKeyFromObject(obj), err)
	}
	return true, nil
}

func modelDownloadDemandMatches(status *v1beta1.ModelDownloadSchedulingStatus, count int32) bool {
	if count == 0 {
		return status == nil
	}
	return status != nil && status.ServingDemand && status.ReferenceCount == count
}
