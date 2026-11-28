// Package kvcachepool manages node-local LMCache infrastructure for a pool.
package kvcachepool

import (
	"context"
	"fmt"
	"reflect"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/kvcache"
)

const (
	finalizer = "ome.io/kvcachepool-protection"
	poll      = 30 * time.Second
)

// +kubebuilder:rbac:groups=ome.io,resources=kvcachepools,verbs=get;list;watch;update
// +kubebuilder:rbac:groups=ome.io,resources=kvcachepools/status,verbs=get;update
// +kubebuilder:rbac:groups=ome.io,resources=kvcachepools/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=create;delete;get;list;patch;watch
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=list

type Reconciler struct {
	client.Client
	live client.Reader
}

func SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&ome.KVCachePool{}).
		Watches(&appsv1.DaemonSet{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, ds client.Object) []reconcile.Request {
			name := ds.GetAnnotations()[poolNameAnnotation]
			if name == "" || ds.GetLabels()[kvcache.ServerPoolUIDLabel] == "" {
				return nil
			}
			return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: ds.GetNamespace(), Name: name}}}
		})).Complete(&Reconciler{mgr.GetClient(), mgr.GetAPIReader()})
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var pool ome.KVCachePool
	if err := r.Get(ctx, req.NamespacedName, &pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !pool.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &pool)
	}
	if controllerutil.AddFinalizer(&pool, finalizer) {
		return ctrl.Result{}, r.Update(ctx, &pool)
	}
	old := pool.Status.DeepCopy()
	pool.Status.ObservedGeneration = pool.Generation
	pool.Status.Connection = nil
	pool.Status.Workloads = nil
	ready, reason, msg, err := r.reconcileServers(ctx, &pool)
	condition := metav1.ConditionFalse
	if ready {
		condition = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{Type: "Ready", Status: condition, Reason: reason, Message: msg, ObservedGeneration: pool.Generation})
	if !reflect.DeepEqual(old, &pool.Status) {
		if updateErr := r.Status().Update(ctx, &pool); updateErr != nil {
			return ctrl.Result{}, updateErr
		}
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	// Recheck managed children even if an event is missed.
	return ctrl.Result{RequeueAfter: poll}, nil
}

func belongsToPool(obj client.Object, pool *ome.KVCachePool) bool {
	return obj.GetLabels()[kvcache.ServerPoolUIDLabel] == string(pool.UID) &&
		obj.GetAnnotations()[poolNameAnnotation] == pool.Name && len(obj.GetOwnerReferences()) == 0
}

func (r *Reconciler) reconcileServers(ctx context.Context, pool *ome.KVCachePool) (bool, string, string, error) {
	ds, err := serverDaemonSet(pool)
	if err != nil {
		return false, "InvalidSpec", err.Error(), nil
	}
	cm := connectionMap(pool)
	for _, desired := range []client.Object{serverService(pool), cm, ds} {
		blank, err := r.Scheme().New(desired.GetObjectKind().GroupVersionKind())
		if err != nil {
			return false, "ResourceError", err.Error(), err
		}
		obj := blank.(client.Object)
		err = r.live.Get(ctx, client.ObjectKeyFromObject(desired), obj)
		switch {
		case apierrors.IsNotFound(err):
			// Create separately: apply-by-name could adopt a colliding object between GET and PATCH.
			err = r.Create(ctx, desired, client.FieldOwner("ome-kvcachepool"))
		case err != nil:
		case !belongsToPool(obj, pool):
			return false, "OwnershipConflict", fmt.Sprintf("%s %q is not managed by this pool", desired.GetObjectKind().GroupVersionKind().Kind, desired.GetName()), nil
		case obj.GetDeletionTimestamp() != nil:
			return false, "Provisioning", fmt.Sprintf("Waiting for %s to finish deleting", obj.GetName()), nil
		default:
			desired.SetResourceVersion(obj.GetResourceVersion())
			err = r.applyChild(ctx, desired)
		}
		if err != nil {
			return false, "ResourceError", err.Error(), err
		}
	}
	status := ds.Status
	count := status.DesiredNumberScheduled
	pool.Status.Workloads = []ome.KVCachePoolWorkloadStatus{{Name: "server", DesiredReplicas: count, ReadyReplicas: status.NumberReady}}
	if ds.Generation != status.ObservedGeneration || count == 0 || status.UpdatedNumberScheduled != count || status.NumberReady != count || status.NumberAvailable != count || status.NumberMisscheduled != 0 {
		return false, "ServersNotReady", fmt.Sprintf("%d of %d node-local servers ready, %d updated", status.NumberReady, count, status.UpdatedNumberScheduled), nil
	}
	pool.Status.Connection = &ome.KVCachePoolConnectionStatus{ConfigMapRef: corev1.LocalObjectReference{Name: cm.Name}}
	return true, "ServersReady", fmt.Sprintf("%d node-local servers ready", count), nil
}

// applyChild submits only the fields from the pool's desired manifest. Copy
// the server response back so readiness uses the applied DaemonSet generation
// and status, including on the reconcile that changes the Pod template.
func (r *Reconciler) applyChild(ctx context.Context, desired client.Object) error {
	intent := &unstructured.Unstructured{}
	if err := r.Scheme().Convert(desired, intent, nil); err != nil {
		return err
	}
	if err := r.Apply(ctx, client.ApplyConfigurationFromUnstructured(intent), client.FieldOwner("ome-kvcachepool"), client.ForceOwnership); err != nil {
		return err
	}
	return r.Scheme().Convert(intent, desired, nil)
}
