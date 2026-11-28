package kvcachepool

import (
	"context"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/kvcache"
)

func (r *Reconciler) finalize(ctx context.Context, pool *ome.KVCachePool) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(pool, finalizer) {
		return ctrl.Result{}, nil
	}
	// Live reads include services still terminating and pods left behind
	// during an unbind rollout. A foreground deletion cannot bypass this
	// check: server resources are explicitly managed, not garbage-collected.
	var services ome.InferenceServiceList
	if err := r.live.List(ctx, &services, client.InNamespace(pool.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	for _, svc := range services.Items {
		if ref := svc.Spec.KVCachePool; ref != nil && ref.Name == pool.Name {
			return r.waitForConsumer(ctx, pool, "InferenceService "+svc.Name)
		}
	}
	consumers := []client.ListOption{client.InNamespace(pool.Namespace), client.MatchingLabels{kvcache.PoolUIDLabel: string(pool.UID)}}
	// The service may already be unbound while its Deployment or an old
	// ReplicaSet can still create another pod from the bound template.
	var deploys appsv1.DeploymentList
	if err := r.live.List(ctx, &deploys, consumers...); err != nil {
		return ctrl.Result{}, err
	}
	for _, d := range deploys.Items {
		if d.Spec.Template.Labels[kvcache.PoolUIDLabel] == string(pool.UID) {
			return r.waitForConsumer(ctx, pool, "Deployment "+d.Name)
		}
	}
	var sets appsv1.ReplicaSetList
	if err := r.live.List(ctx, &sets, consumers...); err != nil {
		return ctrl.Result{}, err
	}
	for _, rs := range sets.Items {
		if rs.Spec.Replicas == nil || *rs.Spec.Replicas > 0 {
			return r.waitForConsumer(ctx, pool, "ReplicaSet "+rs.Name)
		}
	}
	var pods corev1.PodList
	if err := r.live.List(ctx, &pods, consumers...); err != nil {
		return ctrl.Result{}, err
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			return r.waitForConsumer(ctx, pool, "Pod "+pod.Name)
		}
	}
	// Remove the DaemonSet first and wait for its foreground deletion (including its Pods).
	name := kvcache.ServerName(pool.UID)
	for _, obj := range []client.Object{&appsv1.DaemonSet{}, &corev1.Service{}, &corev1.ConfigMap{}} {
		resourceName := name
		if _, ok := obj.(*corev1.ConfigMap); ok {
			resourceName += "-connection"
		}
		err := r.live.Get(ctx, client.ObjectKey{Namespace: pool.Namespace, Name: resourceName}, obj)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return ctrl.Result{}, err
		}
		if !belongsToPool(obj, pool) {
			continue
		}
		if obj.GetDeletionTimestamp() == nil {
			uid, rv := obj.GetUID(), obj.GetResourceVersion()
			opts := []client.DeleteOption{client.Preconditions{UID: &uid, ResourceVersion: &rv}}
			if _, ok := obj.(*appsv1.DaemonSet); ok {
				opts = append(opts, client.PropagationPolicy(metav1.DeletePropagationForeground))
			}
			if err := r.Delete(ctx, obj, opts...); client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	controllerutil.RemoveFinalizer(pool, finalizer)
	return ctrl.Result{}, r.Update(ctx, pool)
}

func (r *Reconciler) waitForConsumer(ctx context.Context, pool *ome.KVCachePool, consumer string) (ctrl.Result, error) {
	if meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: "InUse", Message: "Waiting for " + consumer + " to release the pool", ObservedGeneration: pool.Generation}) {
		if err := r.Status().Update(ctx, pool); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: poll}, nil
}
