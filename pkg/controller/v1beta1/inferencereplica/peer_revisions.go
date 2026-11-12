package inferencereplica

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
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
	// peers holds every peer whose IR resolved this pass; a peer without an
	// entry resolves to no revision.
	peers map[v1beta1.ComponentType]peerRevision
}

// peerRevision is what one peer's IR reported: the revision hash its
// reconciler is rolling onto, the hash of its last fully rolled revision ("" until
// it has completed a roll) and the IR itself, whose Runners give the peer's
// per-revision Services their shape.
type peerRevision struct {
	target  string
	current string
	replica *v1beta1.InferenceReplica
}

// hashFor is the coordination.PeerRevisionFunc the render hook consumes.
func (p *peerRevisions) hashFor(peer v1beta1.ComponentType, podRevisionHash string) string {
	if p == nil {
		return ""
	}
	rev := p.peers[peer]
	if podRevisionHash == "" || podRevisionHash == p.ownTarget {
		return rev.target
	}
	if rev.current != "" {
		return rev.current
	}
	return rev.target
}

// resolvePeerRevisions reads every serving peer's IR and resolves the
// revisions pods rendered this pass pair with. ownTarget is this
// Component's roll-target ControllerRevision (nil when nothing renders).
//
// A peer's roll target is read from its authoritative status, so the
// resolution is only trustworthy once the peer reflects the same parent
// ISVC generation as ir (its projection has landed) and its status was
// written for the pod templates its spec holds (its reconciler has
// recorded the target of that projection, coordination.StatusObservedTemplates).
// Until both hold the returned hold reason is non-empty and the caller
// must not render: a pod paired against the peer's previous target would
// carry the stale pairing for its whole life. A spec write that changes no
// template (an annotation value the revision leaves out, a count, a pause)
// leaves the peer fresh. Every pass that holds still writes this
// Component's own status, so two peers waiting on each other converge
// instead of deadlocking.
//
// A peer with no IR (not IR-managed, or not projected yet) resolves to no
// revision: its pods get only the generic endpoint and nothing holds. The
// exception is a Component's first pods beside an encoder, which wait for
// every OMENative peer's IR (awaitsPeerProjection).
//
// Peer IRs are read through the cache: the generation stamp and the status
// travel on the same object, so a stale copy can only fail a freshness
// check (a conservative hold), never pass one against outdated status.
func (r *Reconciler) resolvePeerRevisions(ctx context.Context, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, ownTarget *appsv1.ControllerRevision) (*peerRevisions, string, error) {
	out := &peerRevisions{peers: map[v1beta1.ComponentType]peerRevision{}}
	if ownTarget != nil {
		out.ownTarget = query.RevisionFromName(ownTarget.Name).Hash()
	}
	ownGeneration := ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]
	for _, peer := range coordination.ServingPeers(parent, ir.Spec.Component) {
		peerIR, err := irprojector.ComponentIRFor(ctx, r.Client, parent, peer)
		if err != nil {
			return nil, "", err
		}
		if peerIR == nil {
			if awaitsPeerProjection(ir, parent, peer) {
				return nil, fmt.Sprintf("%s IR is not projected yet, and this Component has no pods to keep serving", peer), nil
			}
			continue
		}
		if peerGeneration := peerIR.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]; peerGeneration != ownGeneration {
			return nil, fmt.Sprintf("%s IR projection reflects parent generation %q, this Component's reflects %q", peer, peerGeneration, ownGeneration), nil
		}
		if !coordination.StatusObservedTemplates(peerIR) || peerIR.Status.UpdateRevision == "" {
			return nil, fmt.Sprintf("%s IR status has not recorded the roll target of its spec yet (generation %d)", peer, peerIR.Generation), nil
		}
		target, err := r.peerRollTargetHash(ctx, peerIR)
		if err != nil {
			return nil, "", err
		}
		out.peers[peer] = peerRevision{
			target:  target,
			current: query.RevisionFromName(peerIR.Status.CurrentRevision).Hash(),
			replica: peerIR,
		}
	}
	return out, "", nil
}

// awaitsPeerProjection reports whether a peer with no IR holds this
// Component's render. Beside an encoder every role is an OMENative IR that
// one service pass projects, one role after another, so a missing peer IR is
// on its way, and a first pod rendered before it lands would lack that peer's
// revision endpoint for its whole life. A Component that already has
// Instances keeps rendering, so a peer that cannot be projected never stops
// its repairs.
func awaitsPeerProjection(ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, peer v1beta1.ComponentType) bool {
	if parent.Spec.Encoder == nil || len(ir.Status.InstanceStatuses) > 0 {
		return false
	}
	view := v1beta1.SpecOf(&parent.Spec, peer)
	return view != nil && isvcutils.RoleDeploymentMode(view, parent.Spec.DeploymentMode) == constants.OMENative
}

// peerRollTargetHash is the revision hash a peer's reconciler is rolling
// its Instances onto, by the one rule the sweep of per-revision Services
// keeps a revision by (coordination.ReplicaRollTargetHash): the rollback
// revision while a rollback is pinned to a ControllerRevision that still
// exists and belongs to the peer's history, else the spec target its
// status reports.
func (r *Reconciler) peerRollTargetHash(ctx context.Context, peerIR *v1beta1.InferenceReplica) (string, error) {
	return coordination.ReplicaRollTargetHash(ctx, r.APIReader, peerIR)
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
	for peer, rev := range peers.peers {
		if rev.target == "" {
			continue
		}
		// The replica does not carry its Component's Service port declarations,
		// so this create passes none. The Component's own writer rewrites the
		// ports through EnsurePerRevisionServices before a route sends it traffic.
		if err := coordination.CreatePerRevisionServicesIfAbsent(ctx, r.Client, parent, peer, rev.target,
			coordination.RoutingSelectorForRunners(rev.replica.Spec.Runners),
			coordination.ServingPortsFromRunners(rev.replica.Spec.Runners), nil); err != nil {
			return fmt.Errorf("create %s per-revision services for revision %s: %w", peer, rev.target, err)
		}
	}
	return nil
}
