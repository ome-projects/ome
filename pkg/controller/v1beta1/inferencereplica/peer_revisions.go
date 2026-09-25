package inferencereplica

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// peerRevisions is one reconcile's resolution of which peer revision the
// pods rendered this pass pair with, per serving peer Component.
//
// Pairing rule (the invariant every rendered pod's
// OME_<PEER>_REVISION_ENDPOINT satisfies): a pod rendered for its
// Component's roll-target revision pairs with each peer's roll-target
// revision; a pod rendered for any other revision (a stable-partition or
// rolled-back Instance being re-created) pairs with the peer's last fully
// rolled revision. One ISVC update retargets every Component's IR in the
// same projector pass, so pods minted for the new revision pair with the
// new peer revision and pods minted before it keep pairing with the old
// one. Env vars are immutable, so the pairing is fixed for the pod's life.
type peerRevisions struct {
	// ownTarget is this Component's roll-target revision hash.
	ownTarget string
	// target is each peer's roll-target revision hash.
	target map[v1beta1.ComponentType]string
	// current is each peer's last fully rolled revision hash ("" until
	// the peer has completed a roll).
	current map[v1beta1.ComponentType]string
	// irs holds each resolved peer's IR, for Service-shape derivation.
	irs map[v1beta1.ComponentType]*v1beta1.InferenceReplica
}

// hashFor is the coordination.PeerRevisionFunc the render hook consumes.
func (p *peerRevisions) hashFor(peer v1beta1.ComponentType, podRevisionHash string) string {
	if p == nil {
		return ""
	}
	if podRevisionHash == "" || podRevisionHash == p.ownTarget {
		return p.target[peer]
	}
	if cur := p.current[peer]; cur != "" {
		return cur
	}
	return p.target[peer]
}

// resolvePeerRevisions reads every serving peer's IR and resolves the
// revisions pods rendered this pass pair with. ownTarget is this
// Component's roll-target ControllerRevision (nil when nothing renders).
//
// A peer's roll target is read from its authoritative status, so the
// resolution is only trustworthy once the peer reflects the same parent
// ISVC generation as ir (its projection has landed) and its status
// reflects its own current generation (its reconciler has stamped the
// target for that projection). Until both hold the returned hold reason
// is non-empty and the caller must not render: a pod paired against the
// peer's previous target would carry the stale pairing for its whole
// life. Every pass that holds still writes this Component's own status,
// so two peers waiting on each other converge instead of deadlocking.
//
// A peer with no IR (not IR-managed, or not projected yet) resolves to no
// revision: its pods get only the generic endpoint and nothing holds.
//
// Peer IRs are read through the cache: the generation stamp and the status
// travel on the same object, so a stale copy can only fail a freshness
// check (a conservative hold), never pass one against outdated status.
func (r *Reconciler) resolvePeerRevisions(ctx context.Context, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, ownTarget *appsv1.ControllerRevision) (*peerRevisions, string, error) {
	out := &peerRevisions{
		target:  map[v1beta1.ComponentType]string{},
		current: map[v1beta1.ComponentType]string{},
		irs:     map[v1beta1.ComponentType]*v1beta1.InferenceReplica{},
	}
	if ownTarget != nil {
		out.ownTarget = query.RevisionFromName(ownTarget.Name).Hash()
	}
	ownGeneration := ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]
	for _, peer := range coordination.ServingPeers(parent, ir.Spec.Component) {
		peerIR, err := irprojector.ComponentIR(ctx, r.Client, ir.Namespace, ir.Spec.ParentRef.Name, peer)
		if err != nil {
			return nil, "", err
		}
		if peerIR == nil {
			continue
		}
		if peerGeneration := peerIR.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]; peerGeneration != ownGeneration {
			return nil, fmt.Sprintf("%s IR projection reflects parent generation %q, this Component's reflects %q", peer, peerGeneration, ownGeneration), nil
		}
		if peerIR.Status.ObservedGeneration < peerIR.Generation || peerIR.Status.UpdateRevision == "" {
			return nil, fmt.Sprintf("%s IR status has not observed generation %d yet", peer, peerIR.Generation), nil
		}
		target, err := r.peerRollTargetHash(ctx, peerIR)
		if err != nil {
			return nil, "", err
		}
		out.target[peer] = target
		out.current[peer] = query.RevisionFromName(peerIR.Status.CurrentRevision).Hash()
		out.irs[peer] = peerIR
	}
	return out, "", nil
}

// peerRollTargetHash is the revision hash a peer's reconciler is rolling
// its Instances onto: the rollback revision while a rollback is pinned
// and its ControllerRevision still exists, else the spec target its
// status reports. Mirrors the peer's own roll-target selection.
func (r *Reconciler) peerRollTargetHash(ctx context.Context, peerIR *v1beta1.InferenceReplica) (string, error) {
	if peerIR.Spec.Pacing != nil && peerIR.Spec.Pacing.RollbackToRevision != nil && *peerIR.Spec.Pacing.RollbackToRevision != "" {
		name := *peerIR.Spec.Pacing.RollbackToRevision
		cr := &appsv1.ControllerRevision{}
		switch err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: peerIR.Namespace, Name: name}, cr); {
		case err == nil:
			return query.RevisionFromName(name).Hash(), nil
		case apierrors.IsNotFound(err):
			// Rollback target swept; the peer falls through to its spec target.
		default:
			return "", fmt.Errorf("get %s rollback revision %s: %w", peerIR.Spec.Component, name, err)
		}
	}
	return query.RevisionFromName(peerIR.Status.UpdateRevision).Hash(), nil
}

// ensurePeerRevisionServices creates, before any pod of this pass is
// rendered, the per-revision Services of every peer roll-target revision
// those pods will name. Creation is idempotent and never overwrites: the
// coordination layer owns drift correction once the peer revision has
// pods, and protects a target revision's Services from its orphan sweep
// until then.
func (r *Reconciler) ensurePeerRevisionServices(ctx context.Context, parent *v1beta1.InferenceService, peers *peerRevisions) error {
	if peers == nil {
		return nil
	}
	for peer, hash := range peers.target {
		if hash == "" {
			continue
		}
		peerIR := peers.irs[peer]
		if peerIR == nil {
			continue
		}
		if err := coordination.CreatePerRevisionServicesIfAbsent(ctx, r.Client, parent, peer, hash,
			coordination.RoutingSelectorForRunners(peerIR.Spec.Runners),
			coordination.ServingPortsFromRunners(peerIR.Spec.Runners)); err != nil {
			return fmt.Errorf("create %s per-revision services for revision %s: %w", peer, hash, err)
		}
	}
	return nil
}
