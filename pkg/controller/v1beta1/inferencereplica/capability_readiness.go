package inferencereplica

import (
	"context"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// capabilityReadinessSentinelName is paired with an empty namespace, which
// cannot identify a real namespaced InferenceReplica. The wrapper below must
// match both fields before treating a request as the startup sentinel.
const capabilityReadinessSentinelName = "ome-inferencereplica-controller-ready"

func capabilityReadinessSource() source.Source {
	events := make(chan event.GenericEvent, 1)
	events <- event.GenericEvent{Object: &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: capabilityReadinessSentinelName},
	}}
	return source.Channel(events, &handler.EnqueueRequestForObject{})
}

func (r *Reconciler) reconcilerWithCapabilityReadiness() reconcile.Reconciler {
	if r.CapabilityReadiness == nil {
		return r
	}
	return newCapabilityReadinessReconciler(r, r.CapabilityReadiness)
}

type capabilityReadinessReconciler struct {
	delegate  reconcile.Reconciler
	readiness chan struct{}
	signal    sync.Once
}

func newCapabilityReadinessReconciler(delegate reconcile.Reconciler, readiness chan struct{}) reconcile.Reconciler {
	return &capabilityReadinessReconciler{delegate: delegate, readiness: readiness}
}

func (r *capabilityReadinessReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	if req.Namespace == "" && req.Name == capabilityReadinessSentinelName {
		// Controller workers start only after startEventSourcesAndQueueLocked
		// finishes every syncing source's WaitForSync. Reaching this reserved
		// queue item is therefore stronger than the shared cache's sync signal.
		r.signal.Do(func() { close(r.readiness) })
		return reconcile.Result{}, nil
	}
	return r.delegate.Reconcile(ctx, req)
}
