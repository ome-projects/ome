package inferencereplica

import (
	"context"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
)

// peerReplicasOf maps an InferenceReplica event to the serving peers of the
// same parent InferenceService. A Component's pass holds while a peer's
// projection or status lags its own (resolvePeerRevisions) and pairs the
// pods it renders on the peer's roll target, so the peer's catch-up and
// retargets must wake it: no other watch reaches it from the peer, and its
// own rate-limited requeue can be far off after a burst of spec churn. A
// replica with no parent, or whose parent declares no rollout groups, has
// no peers to wake.
func (r *Reconciler) peerReplicasOf(ctx context.Context, obj client.Object) []reconcile.Request {
	ir, ok := obj.(*v1beta1.InferenceReplica)
	if !ok {
		return nil
	}
	parent := r.resolveParentFrom(ctx, r.Client, ir)
	if !coordination.PeerEnvDeclared(parent) {
		return nil
	}
	peers := coordination.ServingPeers(parent, ir.Spec.Component)
	reqs := make([]reconcile.Request, 0, len(peers))
	for _, peer := range peers {
		reqs = append(reqs, reconcile.Request{NamespacedName: irprojector.RoleReplicaKey(parent, peer)})
	}
	return reqs
}

// peerRevisionPredicate admits the InferenceReplica transitions a sibling's
// peer-revision resolution reads; counter churn on a busy peer is left to
// peerCounterPredicate, which reaches only a sibling held on it. Creates
// and deletes pass: a peer appearing or vanishing changes what its
// siblings pair with.
func peerRevisionPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldIR, ok1 := e.ObjectOld.(*v1beta1.InferenceReplica)
			newIR, ok2 := e.ObjectNew.(*v1beta1.InferenceReplica)
			if !ok1 || !ok2 {
				return true
			}
			return peerRevisionInputsChanged(oldIR, newIR)
		},
	}
}

// peerRevisionInputsChanged reports whether an old→new InferenceReplica
// transition moved one of the fields a sibling's peer-revision resolution
// reads: the parent generation its projection reflects, its own generation
// (the spec target and the rollback pin travel with it), and the status
// that reports the roll it observed (ObservedGeneration, UpdateRevision,
// CurrentRevision).
func peerRevisionInputsChanged(oldIR, newIR *v1beta1.InferenceReplica) bool {
	if oldIR == nil || newIR == nil {
		return true
	}
	if oldIR.Generation != newIR.Generation {
		return true
	}
	key := constants.InferenceReplicaParentGenerationAnnotationKey
	if oldIR.Annotations[key] != newIR.Annotations[key] {
		return true
	}
	return oldIR.Status.ObservedGeneration != newIR.Status.ObservedGeneration ||
		oldIR.Status.UpdateRevision != newIR.Status.UpdateRevision ||
		oldIR.Status.CurrentRevision != newIR.Status.CurrentRevision
}

// heldPeerReplicasOf maps an InferenceReplica event to the serving peers of
// the same parent whose recorded RolloutHold waits on this replica's
// serving counters (coordination.GateWaitsOnPeerCounters). Nothing but
// this transition lifts such a denial before the denied replica's own
// requeue, which rides the rate-limited backoff unless a gate cadence is
// configured. Reading each peer's hold from the cache bounds the wake-ups:
// a peer's counter churn enqueues nothing while no sibling is held on it.
// A peer that cannot be read is enqueued rather than skipped; a spurious
// pass costs less than a missed release.
func (r *Reconciler) heldPeerReplicasOf(ctx context.Context, obj client.Object) []reconcile.Request {
	ir, ok := obj.(*v1beta1.InferenceReplica)
	if !ok {
		return nil
	}
	parent := r.resolveParentFrom(ctx, r.Client, ir)
	if !coordination.PeerEnvDeclared(parent) {
		return nil
	}
	var reqs []reconcile.Request
	for _, peer := range coordination.ServingPeers(parent, ir.Spec.Component) {
		key := irprojector.RoleReplicaKey(parent, peer)
		sibling := &v1beta1.InferenceReplica{}
		if err := r.Client.Get(ctx, key, sibling); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			reqs = append(reqs, reconcile.Request{NamespacedName: key})
			continue
		}
		if hold := sibling.Status.RolloutHold; hold != nil && coordination.GateWaitsOnPeerCounters(hold.Gate) {
			reqs = append(reqs, reconcile.Request{NamespacedName: key})
		}
	}
	return reqs
}

// peerCounterPredicate admits the InferenceReplica transitions a held
// sibling's pairing or ratio simulation reads: the serving counters and the
// per-Instance rows. Only an update carries such a transition; a peer
// appearing or vanishing is the peer-revision watch's event.
func peerCounterPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldIR, ok1 := e.ObjectOld.(*v1beta1.InferenceReplica)
			newIR, ok2 := e.ObjectNew.(*v1beta1.InferenceReplica)
			if !ok1 || !ok2 {
				return true
			}
			return peerCountersChanged(oldIR, newIR)
		},
	}
}

// peerCountersChanged reports whether an old→new InferenceReplica
// transition moved a value the pairing or ratio simulation reads from a
// peer: a Component-level replica counter, or a per-Instance row (serving
// pod counts, running revisions, operations in flight) in either stored
// encoding.
func peerCountersChanged(oldIR, newIR *v1beta1.InferenceReplica) bool {
	if oldIR == nil || newIR == nil {
		return true
	}
	o, n := &oldIR.Status, &newIR.Status
	return o.Replicas != n.Replicas ||
		o.ReadyReplicas != n.ReadyReplicas ||
		o.ServingReplicas != n.ServingReplicas ||
		o.AvailableReplicas != n.AvailableReplicas ||
		o.UpdatedReplicas != n.UpdatedReplicas ||
		o.UpdatedReadyReplicas != n.UpdatedReadyReplicas ||
		!equality.Semantic.DeepEqual(o.InstanceStatuses, n.InstanceStatuses) ||
		!equality.Semantic.DeepEqual(o.InstanceStatusColumns, n.InstanceStatusColumns)
}
