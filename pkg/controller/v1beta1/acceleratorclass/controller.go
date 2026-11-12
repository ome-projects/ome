package acceleratorclass

import (
	"context"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// +kubebuilder:rbac:groups=ome.io,resources=acceleratorclasses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=acceleratorclasses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=acceleratorclasses/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

type AcceleratorClassReconciler struct {
	client.Client
	Log logr.Logger
}

func (r *AcceleratorClassReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("acceleratorclass", req.NamespacedName)

	ac := &v1beta1.AcceleratorClass{}
	if err := r.Get(ctx, req.NamespacedName, ac); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		log.Error(err, "failed to get AcceleratorClass")
		return ctrl.Result{}, err
	}

	if !ac.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(ac, constants.AcceleratorClassFinalizer) {
			controllerutil.RemoveFinalizer(ac, constants.AcceleratorClassFinalizer)
			if err := r.Update(ctx, ac); err != nil {
				return finalizerWriteResult(err)
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(ac, constants.AcceleratorClassFinalizer) {
		controllerutil.AddFinalizer(ac, constants.AcceleratorClassFinalizer)
		if err := r.Update(ctx, ac); err != nil {
			return finalizerWriteResult(err)
		}
	}

	// List nodes and apply filters
	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList); err != nil {
		log.Error(err, "failed to list nodes")
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}

	matchedNodes := make([]string, 0, len(nodeList.Items))
	for _, node := range nodeList.Items {
		if !nodePassesDiscovery(ac, &node) {
			continue
		}
		if !nodeMatchCapabilities(ac, &node) {
			continue
		}
		matchedNodes = append(matchedNodes, node.Name)
	}
	sort.Strings(matchedNodes)

	// Re-fetch the latest object and patch status against it so the patch
	// base carries a fresh resourceVersion; LastUpdated is stamped only when
	// the rest of the status actually changed.
	latest := &v1beta1.AcceleratorClass{}
	if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	desired := latest.DeepCopy()
	desired.Status.Nodes = matchedNodes
	desired.Status.AvailableNodes = int32(len(matchedNodes))

	// Only update status if something changed (except LastUpdated):
	if !acceleratorClassStatusEqualIgnoreTime(latest.Status, desired.Status) {
		desired.Status.LastUpdated = metav1.Now()
		if err := r.Status().Patch(ctx, desired, client.MergeFrom(latest)); err != nil {
			return requeueOnConflict(err)
		}
	}

	return ctrl.Result{}, nil
}

// requeueOnConflict turns a write error into the pass's result. Writes race
// with the Node-watch fan-out and this controller's own status patches, so a
// resourceVersion conflict requeues the pass. Any other error is returned.
func requeueOnConflict(err error) (ctrl.Result, error) {
	if errors.IsConflict(err) {
		return ctrl.Result{Requeue: true}, nil
	}
	return ctrl.Result{}, err
}

// finalizerWriteResult ends the pass when the class is already deleted and
// otherwise applies the conflict rule.
func finalizerWriteResult(err error) (ctrl.Result, error) {
	if errors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}
	return requeueOnConflict(err)
}

// SetupWithManager wires the controller and watches nodes to trigger reconciles
func (r *AcceleratorClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.AcceleratorClass{}).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				// Any node change could affect any AcceleratorClass; requeue all
				acList := &v1beta1.AcceleratorClassList{}
				if err := r.List(ctx, acList); err != nil {
					return nil
				}
				requests := make([]reconcile.Request, 0, len(acList.Items))
				for i := range acList.Items {
					requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&acList.Items[i])})
				}
				return requests
			}),
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc: func(e event.UpdateEvent) bool {
					oldNode, okOld := e.ObjectOld.(*corev1.Node)
					newNode, okNew := e.ObjectNew.(*corev1.Node)
					if !okOld || !okNew {
						return true
					}
					// Discovery matches on labels; capability matching on
					// capacity. Everything else (conditions, images,
					// heartbeats) cannot change class membership.
					return !equality.Semantic.DeepEqual(oldNode.Labels, newNode.Labels) ||
						!equality.Semantic.DeepEqual(oldNode.Status.Capacity, newNode.Status.Capacity)
				},
			}),
		).
		Complete(r)
}

func nodePassesDiscovery(ac *v1beta1.AcceleratorClass, node *corev1.Node) bool {
	// NodeSelector map: all key=value must match
	if len(ac.Spec.Discovery.NodeSelector) > 0 {
		for k, v := range ac.Spec.Discovery.NodeSelector {
			if node.Labels[k] != v {
				return false
			}
		}
	}

	return true
}

// nodeMatchCapabilities checks the node actually exposes the accelerator
// hardware this class declares. capabilities.memoryGB describes per-device
// GPU memory, which no standard node resource reports — the reliable signal
// is the class's declared extended resources (e.g. nvidia.com/gpu) being
// present in the node's capacity.
func nodeMatchCapabilities(ac *v1beta1.AcceleratorClass, node *corev1.Node) bool {
	for _, res := range ac.Spec.Resources {
		qty, ok := node.Status.Capacity[corev1.ResourceName(res.Name)]
		if !ok || qty.IsZero() {
			return false
		}
	}

	return true
}

// returns true if equal when ignoring LastUpdated
func acceleratorClassStatusEqualIgnoreTime(a, b v1beta1.AcceleratorClassStatus) bool {
	aCopy := a
	bCopy := b
	aCopy.LastUpdated = metav1.Time{}
	bCopy.LastUpdated = metav1.Time{}
	return equality.Semantic.DeepEqual(aCopy, bCopy)
}
