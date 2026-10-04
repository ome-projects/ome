package inferenceservice

import (
	"context"
	"errors"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	knapis "knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/utils"
)

// deploymentModeChangedReason is the event reason, and the Component ready
// condition reason, recorded when a deployment mode change removes the
// objects of the backend a Component left.
const deploymentModeChangedReason = "DeploymentModeChanged"

// The workload object each deployment mode projects for a Component. A
// Component's other objects (Service, PodMonitor, PodDisruptionBudget,
// autoscaler) share their names across modes and are re-pointed in place by
// whichever backend the mode selects; the workload object is the one that
// differs, so it is what a mode change has to remove.
var (
	deploymentGVK       = appsv1.SchemeGroupVersion.WithKind("Deployment")
	serviceGVK          = v1.SchemeGroupVersion.WithKind("Service")
	leaderWorkerSetGVK  = lws.SchemeGroupVersion.WithKind(constants.LWSKind)
	inferenceReplicaGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceReplica")
)

// backendWorkloadKind is the workload object the deployment mode projects
// for a Component; false for a mode that projects none.
func backendWorkloadKind(mode constants.DeploymentModeType) (schema.GroupVersionKind, bool) {
	switch mode {
	case constants.RawDeployment:
		return deploymentGVK, true
	case constants.MultiNode:
		return leaderWorkerSetGVK, true
	case constants.OMENative:
		return inferenceReplicaGVK, true
	default:
		return schema.GroupVersionKind{}, false
	}
}

// componentReadyCondition is the top-level readiness condition of a Component.
func componentReadyCondition(component v1beta1.ComponentType) (knapis.ConditionType, bool) {
	switch component {
	case v1beta1.EngineComponent:
		return v1beta1.EngineReady, true
	case v1beta1.DecoderComponent:
		return v1beta1.DecoderReady, true
	case v1beta1.RouterComponent:
		return v1beta1.RouterReady, true
	default:
		return "", false
	}
}

// removedObject identifies an object a sweep deleted.
type removedObject struct {
	gvk       schema.GroupVersionKind
	name      string
	component v1beta1.ComponentType
}

// backendWorkloadList is an empty typed list for one workload kind. Typed
// lists read from the informer cache the owned-object watches already keep.
type backendWorkloadList struct {
	gvk  schema.GroupVersionKind
	list client.ObjectList
}

// cleanupRemovedComponents deletes resources for components no longer specified in the spec.
func (r *InferenceServiceReconciler) cleanupRemovedComponents(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	engine *v1beta1.EngineSpec,
	decoder *v1beta1.DecoderSpec,
	router *v1beta1.RouterSpec,
) error {
	active := map[v1beta1.ComponentType]bool{
		v1beta1.EngineComponent:  engine != nil,
		v1beta1.DecoderComponent: decoder != nil,
		v1beta1.RouterComponent:  router != nil,
	}
	_, err := r.deleteOrphanedResourcesByOwnerRef(ctx, isvc, active)
	return err
}

// cleanupReplacedBackends removes, for each Component in modes, the objects
// of every backend other than the one its mode selects: the workload object
// of another mode, and the per-revision Services only OMENative projects
// once the Component has left that mode. A Component runs under exactly one
// backend, because the backends share its Service, disruption budget,
// autoscaler and accelerator capacity, so the replaced backend is deleted in
// the pass that projects its successor rather than kept beside it. An
// InferenceReplica drains through its own finalizer; a Deployment or
// LeaderWorkerSet cascades through garbage collection. Each removal is
// recorded as an event, the Component's ready condition reads False until
// the selected backend reports readiness itself, and the OMENative status
// footprint of a Component that left the mode is cleared. Components absent
// from modes are removed from the spec and are swept by
// cleanupRemovedComponents.
func (r *InferenceServiceReconciler) cleanupReplacedBackends(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	modes map[v1beta1.ComponentType]constants.DeploymentModeType,
) error {
	if len(modes) == 0 {
		return nil
	}
	var errs []error
	selector := labels.Set{constants.InferenceServicePodLabelKey: isvc.Name}.AsSelector()
	for _, workload := range r.backendWorkloadLists() {
		owned, err := r.listOwnedObjects(ctx, workload.list, isvc, selector)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, obj := range owned {
			component := v1beta1.ComponentType(obj.GetLabels()[constants.OMEComponentLabel])
			mode, declared := modes[component]
			if !declared {
				continue
			}
			selected, known := backendWorkloadKind(mode)
			if !known || selected == workload.gvk {
				continue
			}
			deleted, err := r.deleteReplacedBackendObject(ctx, isvc, obj, workload.gvk, component, mode)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if !deleted {
				continue
			}
			if condType, ok := componentReadyCondition(component); ok {
				isvc.Status.SetCondition(condType, &knapis.Condition{
					Type:    condType,
					Status:  v1.ConditionFalse,
					Reason:  deploymentModeChangedReason,
					Message: fmt.Sprintf("%s now runs as %s; its %s %s is being removed", component, mode, workload.gvk.Kind, obj.GetName()),
				})
			}
		}
	}

	// Per-revision routing and headless Services exist only under OMENative.
	revisionSelector := labels.Set{
		constants.InferenceServicePodLabelKey: isvc.Name,
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}.AsSelector()
	services, err := r.listOwnedObjects(ctx, &v1.ServiceList{}, isvc, revisionSelector)
	if err != nil {
		errs = append(errs, err)
	}
	for _, svc := range services {
		component := v1beta1.ComponentType(svc.GetLabels()[constants.OMEComponentLabel])
		mode, declared := modes[component]
		if !declared || mode == constants.OMENative {
			continue
		}
		if _, err := r.deleteReplacedBackendObject(ctx, isvc, svc, serviceGVK, component, mode); err != nil {
			errs = append(errs, err)
		}
	}

	for component, mode := range modes {
		if mode == constants.OMENative {
			continue
		}
		if err := r.clearOMENativeStatus(ctx, isvc, component); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// cleanupVirtualDeployment removes every object a backend projected for an
// InferenceService that is a VirtualDeployment, which selects no backend and
// runs no workload, and clears the OMENative status footprint that the
// removed objects wrote.
func (r *InferenceServiceReconciler) cleanupVirtualDeployment(ctx context.Context, isvc *v1beta1.InferenceService) error {
	deleted, err := r.deleteOrphanedResourcesByOwnerRef(ctx, isvc, nil)
	if err != nil {
		return err
	}
	for _, obj := range deleted {
		r.recordDeploymentModeChange(isvc, fmt.Sprintf("the InferenceService is a VirtualDeployment and runs no workload; deleted %s %s of its %s",
			obj.gvk.Kind, obj.name, obj.component))
	}
	var errs []error
	for component := range isvc.Status.Components {
		if err := r.clearOMENativeStatus(ctx, isvc, component); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// backendWorkloadLists lists the workload kinds the cluster can hold. The
// LeaderWorkerSet scheme is registered only when its CRD is installed.
func (r *InferenceServiceReconciler) backendWorkloadLists() []backendWorkloadList {
	lists := []backendWorkloadList{
		{gvk: deploymentGVK, list: &appsv1.DeploymentList{}},
		{gvk: inferenceReplicaGVK, list: &v1beta1.InferenceReplicaList{}},
	}
	if r.Client.Scheme().Recognizes(leaderWorkerSetGVK) {
		lists = append(lists, backendWorkloadList{gvk: leaderWorkerSetGVK, list: &lws.LeaderWorkerSetList{}})
	}
	return lists
}

// deleteReplacedBackendObject requests deletion of one object of a backend
// the Component left and records it as an event. An object whose deletion
// is already requested is left to finish; it reports false.
func (r *InferenceServiceReconciler) deleteReplacedBackendObject(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	obj client.Object,
	gvk schema.GroupVersionKind,
	component v1beta1.ComponentType,
	mode constants.DeploymentModeType,
) (bool, error) {
	if obj.GetDeletionTimestamp() != nil {
		return false, nil
	}
	log.FromContext(ctx).Info("Deleting an object of a replaced backend",
		"gvk", gvk, "name", obj.GetName(), "component", component, "deploymentMode", mode)
	if err := r.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("delete %s %s/%s of the %s backend replaced by %s: %w",
			gvk.Kind, obj.GetNamespace(), obj.GetName(), component, mode, err)
	}
	r.recordDeploymentModeChange(isvc, fmt.Sprintf("%s now runs as %s; deleted %s %s of the backend it replaced",
		component, mode, gvk.Kind, obj.GetName()))
	return true, nil
}

// recordDeploymentModeChange emits the event that records one removal a
// deployment mode change caused. A reconciler without a recorder records
// nothing.
func (r *InferenceServiceReconciler) recordDeploymentModeChange(isvc *v1beta1.InferenceService, message string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Event(isvc, v1.EventTypeNormal, deploymentModeChangedReason, message)
}

// clearOMENativeStatus drops the OMENative projection from the status of a
// Component that runs under another backend. The status flush keeps the
// live lifecycle block over the pass's copy, so the live object is cleared
// first and the in-memory copy after it.
func (r *InferenceServiceReconciler) clearOMENativeStatus(ctx context.Context, isvc *v1beta1.InferenceService, component v1beta1.ComponentType) error {
	if !hasOMENativeStatus(isvc.Status.Components[component]) {
		return nil
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	key := client.ObjectKeyFromObject(isvc)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		live := &v1beta1.InferenceService{}
		if err := reader.Get(ctx, key, live); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		cs, ok := live.Status.Components[component]
		if !ok || !hasOMENativeStatus(cs) {
			return nil
		}
		live.Status.Components[component] = withoutOMENativeStatus(cs)
		return r.Status().Update(ctx, live)
	}); err != nil {
		return fmt.Errorf("clear the OMENative status of %s on %s: %w", component, key, err)
	}
	isvc.Status.Components[component] = withoutOMENativeStatus(isvc.Status.Components[component])
	return nil
}

// hasOMENativeStatus reports whether a Component status carries fields only
// the OMENative backend writes.
func hasOMENativeStatus(cs v1beta1.ComponentStatusSpec) bool {
	return cs.Lifecycle != nil ||
		cs.RolloutPhase != "" ||
		cs.LatestReadyRevision != "" ||
		cs.LatestRolledoutRevision != "" ||
		cs.PreviousRolledoutRevision != "" ||
		len(cs.Traffic) > 0
}

// withoutOMENativeStatus returns the Component status with the fields only
// the OMENative backend writes cleared.
func withoutOMENativeStatus(cs v1beta1.ComponentStatusSpec) v1beta1.ComponentStatusSpec {
	cs.Lifecycle = nil
	cs.RolloutPhase = ""
	cs.LatestReadyRevision = ""
	cs.LatestRolledoutRevision = ""
	cs.PreviousRolledoutRevision = ""
	cs.Traffic = nil
	return cs
}

// deleteOrphanedResourcesByOwnerRef deletes resources owned by isvc that are
// not in activeComponents and returns the objects it deleted.
func (r *InferenceServiceReconciler) deleteOrphanedResourcesByOwnerRef(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	activeComponents map[v1beta1.ComponentType]bool,
) ([]removedObject, error) {
	log := log.FromContext(ctx)

	selector := labels.Set{
		constants.InferenceServicePodLabelKey: isvc.Name,
	}.AsSelector()

	gvks, err := r.getAvailableResourceTypes()
	if err != nil {
		log.Error(err, "Failed to retrieve all available resource types, using core set")
		gvks = getCoreResourceTypes()
	}

	var deleted []removedObject
	for _, gvk := range gvks {
		objs, err := r.cleanupResourcesOfType(ctx, gvk, isvc, selector, activeComponents)
		if err != nil {
			log.Error(err, "Failed to cleanup resources of type", "gvk", gvk)
		}
		deleted = append(deleted, objs...)
	}
	return deleted, nil
}

// cleanupResourcesOfType deletes orphaned resources of a specific GVK and
// returns the ones it deleted.
func (r *InferenceServiceReconciler) cleanupResourcesOfType(
	ctx context.Context,
	gvk schema.GroupVersionKind,
	isvc *v1beta1.InferenceService,
	selector labels.Selector,
	activeComponents map[v1beta1.ComponentType]bool,
) ([]removedObject, error) {
	log := log.FromContext(ctx)

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(gvk)
	owned, err := r.listOwnedObjects(ctx, list, isvc, selector)
	if err != nil {
		return nil, err
	}

	var deleted []removedObject
	for _, obj := range owned {
		component := v1beta1.ComponentType(obj.GetLabels()[constants.OMEComponentLabel])
		if component == "" || activeComponents[component] {
			continue
		}
		// An object whose deletion is already requested is left to finish.
		if obj.GetDeletionTimestamp() != nil {
			continue
		}

		// Special handling for external service
		if component == "external-service" && gvk.Kind == "Service" {
			// External service should exist if ingress is disabled and there are active components
			if r.shouldKeepExternalService(isvc, activeComponents) {
				continue
			}
		}

		log.Info("Deleting orphaned resource", "gvk", gvk, "name", obj.GetName(), "component", component)
		if err := r.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
			return deleted, fmt.Errorf("delete %s/%s: %w", gvk.Kind, obj.GetName(), err)
		}
		deleted = append(deleted, removedObject{gvk: gvk, name: obj.GetName(), component: component})
	}
	return deleted, nil
}

// listOwnedObjects fills list with the objects of its kind in the namespace
// of isvc that match selector and returns the ones isvc owns. A kind the
// cluster or the scheme does not know yields nothing.
func (r *InferenceServiceReconciler) listOwnedObjects(
	ctx context.Context,
	list client.ObjectList,
	isvc *v1beta1.InferenceService,
	selector labels.Selector,
) ([]client.Object, error) {
	if err := r.List(ctx, list,
		client.InNamespace(isvc.Namespace),
		client.MatchingLabelsSelector{Selector: selector},
	); err != nil {
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list %T: %w", list, err)
	}
	items, err := meta.ExtractList(list)
	if err != nil {
		return nil, fmt.Errorf("extract %T: %w", list, err)
	}

	owned := make([]client.Object, 0, len(items))
	for _, item := range items {
		obj, ok := item.(client.Object)
		if !ok {
			return nil, fmt.Errorf("list item %T is not an object", item)
		}
		if r.isOwnedBy(obj, isvc) {
			owned = append(owned, obj)
		}
	}
	return owned, nil
}

// isOwnedBy returns true if obj is owned by isvc.
func (r *InferenceServiceReconciler) isOwnedBy(obj metav1.Object, isvc *v1beta1.InferenceService) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Kind == "InferenceService" &&
			ref.APIVersion == v1beta1.SchemeGroupVersion.String() &&
			ref.Name == isvc.Name &&
			ref.UID == isvc.UID {
			return true
		}
	}
	return false
}

// shouldKeepExternalService determines if the external service should be kept based on active components
func (r *InferenceServiceReconciler) shouldKeepExternalService(isvc *v1beta1.InferenceService, activeComponents map[v1beta1.ComponentType]bool) bool {
	// Check if ingress creation is disabled via annotation
	if val, ok := isvc.Annotations["ome.io/ingress-disable-creation"]; ok && val == "true" {
		// Keep external service if any component that can serve traffic is active
		return activeComponents[v1beta1.RouterComponent] ||
			activeComponents[v1beta1.EngineComponent]
	}
	return false
}

// getAvailableResourceTypes returns known and discovered GVKs.
func (r *InferenceServiceReconciler) getAvailableResourceTypes() ([]schema.GroupVersionKind, error) {
	core := getCoreResourceTypes()

	optionals := []struct {
		gvk schema.GroupVersionKind
	}{
		{gvk: leaderWorkerSetGVK},
		{gvk: schema.GroupVersionKind{Group: "keda.sh", Version: "v1alpha1", Kind: "ScaledObject"}},
		{gvk: schema.GroupVersionKind{Group: "monitoring.coreos.com", Version: "v1", Kind: "PodMonitor"}},
	}

	for _, res := range optionals {
		if r.ClientConfig == nil {
			continue
		}
		ok, err := utils.IsCrdAvailable(r.ClientConfig, res.gvk.GroupVersion().String(), res.gvk.Kind)
		if err != nil {
			log.Log.V(1).Info("Failed to check CRD", "gvk", res.gvk, "error", err)
			continue
		}
		if ok {
			core = append(core, res.gvk)
		}
	}

	return core, nil
}

// getCoreResourceTypes returns always-available Kubernetes resource types.
func getCoreResourceTypes() []schema.GroupVersionKind {
	return []schema.GroupVersionKind{
		deploymentGVK,
		serviceGVK,
		{Group: "autoscaling", Version: "v2", Kind: "HorizontalPodAutoscaler"},
		{Group: "policy", Version: "v1", Kind: "PodDisruptionBudget"},
		{Group: "networking.k8s.io", Version: "v1", Kind: "Ingress"},
		{Group: "", Version: "v1", Kind: "ConfigMap"},
		{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "Role"},
		{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "RoleBinding"},
		{Group: "", Version: "v1", Kind: "ServiceAccount"},
		{Group: "", Version: "v1", Kind: "PersistentVolumeClaim"},
		// OME's own per-Component projection: a removed component's IR must
		// get deletion requested; its finalizer drains pods before it goes.
		inferenceReplicaGVK,
	}
}
