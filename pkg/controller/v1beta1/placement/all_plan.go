package placement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/plan"
	"sigs.k8s.io/ome/pkg/placement/protocol"
	"sigs.k8s.io/ome/pkg/placement/resolution"
)

// reconcileAllPlanned retains complete per-home policies during bounded moves.
// An incomplete initial inventory permits independent additive provisioning
// only while the original source intent and matched membership remain fixed.
func (r *Reconciler) reconcileAllPlanned(ctx context.Context, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, eligible []string, standing *placementObservations) (ctrl.Result, error) {
	proposal, err := r.allProposal(ctx, source, clusters, eligible)
	if err != nil {
		return r.writeSplitHold(ctx, source, standing, "HomePolicyUnresolved", err)
	}
	return r.executePlannedAllocation(ctx, source, eligible, standing, proposal)
}

func (r *Reconciler) allProposal(ctx context.Context, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, eligible []string) (plan.Proposal, error) {
	out := plan.Proposal{Mode: v1beta1.PlacementModeAll, Assignments: map[string]v1beta1.CandidateAllocationStatus{}}
	selector, err := placementSelector(source)
	if err != nil {
		return out, err
	}
	matched := map[string]types.UID{}
	terms := map[string][]int32{}
	registered := map[string]types.UID{}
	for i := range clusters {
		cluster := &clusters[i]
		if cluster.UID == "" || cluster.Name == "" || registered[cluster.Name] != "" {
			return out, fmt.Errorf("home inventory requires distinct identified registrations")
		}
		registered[cluster.Name] = cluster.UID
		if match, ok := selector.Match(cluster); ok && cluster.DeletionTimestamp.IsZero() {
			matched[cluster.Name], terms[cluster.Name] = cluster.UID, match.TermIndexes
		}
	}
	encoded, err := json.Marshal(struct {
		Spec                v1beta1.InferenceServiceSpec
		Labels, Annotations map[string]string
		Members             map[string]types.UID
	}{source.Spec, source.Labels, source.Annotations, matched})
	if err != nil {
		return out, err
	}
	digest := sha256.Sum256(encoded)
	out.InputDigest = hex.EncodeToString(digest[:])
	initial := source.Status.Placement == nil || source.Status.Placement.Plan == nil
	if !initial {
		previous := source.Status.Placement.Plan
		if previous.SourceUID != source.UID || previous.Mode != v1beta1.PlacementModeAll {
			return out, fmt.Errorf("standing allocation must be reconciled in its accepted placement mode")
		}
		out.AdoptionDigest, out.PauseSurge = previous.AdoptionDigest, previous.PauseSurge
		for _, candidate := range source.Status.Placement.Candidates {
			if candidate.Allocation == nil {
				return out, fmt.Errorf("standing home %q has no allocation authority", candidate.Cluster)
			}
			out.Assignments[candidate.Cluster] = *candidate.Allocation.DeepCopy()
		}
	} else {
		out.AdoptionDigest = out.InputDigest
		for _, candidate := range standingPlacementCandidates(source) {
			if registered[candidate.Cluster] == "" {
				return out, fmt.Errorf("standing home %q has no identified registration", candidate.Cluster)
			}
		}
		// A member write may precede its first successful source status write.
		for name, uid := range registered {
			out.Assignments[name] = v1beta1.CandidateAllocationStatus{ClusterUID: uid, InventoryPending: true}
		}
	}
	for name, uid := range matched {
		a, exists := out.Assignments[name]
		if exists && a.ClusterUID != uid {
			return out, fmt.Errorf("home %q registration identity changed", name)
		}
		if !exists {
			a = v1beta1.CandidateAllocationStatus{ClusterUID: uid, InventoryPending: true}
		}
		out.Assignments[name] = a
	}
	desired, err := r.derivedFor(source)
	if err != nil {
		return out, err
	}
	pending := false
	for _, name := range slices.Sorted(maps.Keys(out.Assignments)) {
		a := out.Assignments[name]
		a.MatchingTerms = terms[name]
		a.Matched = matched[name] != ""
		a.HomeInputsPending = a.Matched
		if matched[name] == "" {
			a.DesiredHome, a.DesiredReplicas = nil, 0
		}
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		if a.InventoryPending {
			err = r.adoptAllHome(cctx, source, name, &a, out.InputDigest, out.AdoptionDigest != "")
			if err != nil {
				r.Log.V(1).Info("home inventory is pending", "cluster", name, "error", err)
			}
		}
		if matched[name] != "" && slices.Contains(eligible, name) {
			home, resolveErr := r.resolveFullHome(cctx, source, desired, name, a.ClusterUID, out.InputDigest)
			if resolveErr == nil {
				a.HomeInputsPending = false
				a.DesiredHome = home
				a.DesiredReplicas, _ = protocol.ValidateReplicaFloors(home.ReplicaFloors)
			} else {
				r.Log.V(1).Info("home policy is unresolved", "cluster", name, "error", resolveErr)
			}
		}
		cancel()
		pending = pending || a.InventoryPending || a.HomeInputsPending
		out.Assignments[name] = a
	}
	if !pending {
		out.AdoptionDigest = ""
	}
	if pending && out.AdoptionDigest == out.InputDigest {
		// Unknown unmatched homes may still serve. They prevent interpreting new
		// homes as initial additive provisioning rather than a replacement move.
		additive := true
		for name, a := range out.Assignments {
			if matched[name] == "" && (a.InventoryPending || a.CurrentReplicas > 0) {
				additive = false
			}
			if a.CurrentReplicas > a.DesiredReplicas {
				additive = false
			}
		}
		if additive {
			for name, a := range out.Assignments {
				if !a.InventoryPending && !a.HomeInputsPending && a.CurrentHome == nil && a.DesiredHome != nil && a.DesiredHome.InputDigest == out.InputDigest {
					a.CurrentHome, a.CurrentReplicas = a.DesiredHome.DeepCopy(), a.DesiredReplicas
					out.Assignments[name] = a
				}
			}
		}
	}
	return out, nil
}

func (r *Reconciler) resolveFullHome(ctx context.Context, source, desired *v1beta1.InferenceService, name string, uid types.UID, digest string) (*v1beta1.PlacementHomePolicy, error) {
	cl, err := r.plannedClient(ctx, name, uid)
	if err != nil {
		return nil, err
	}
	member := &v1beta1.InferenceService{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(source), member); apierrors.IsNotFound(err) {
		member = nil
	} else if err != nil {
		return nil, err
	} else if !isOurDerived(member, source) {
		return nil, fmt.Errorf("home is not owned by this source")
	}
	_, floors, err := (resolution.Resolver{Client: cl, OperatorNamespace: r.MemberOperatorNamespace}).ResolveHome(ctx, desired, member)
	if err != nil {
		return nil, err
	}
	if placementMode(source) != v1beta1.PlacementModeSingle {
		if _, err := protocol.ValidatePositiveReplicaFloors(floors); err != nil {
			return nil, err
		}
	}
	return &v1beta1.PlacementHomePolicy{InputDigest: digest, ReplicaFloors: floors}, nil
}

func (r *Reconciler) adoptAllHome(ctx context.Context, source *v1beta1.InferenceService, name string, assignment *v1beta1.CandidateAllocationStatus, digest string, allowStanding bool) error {
	readSource := source.DeepCopy()
	readSource.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{SourceUID: source.UID}}
	observed, err := r.observePlannedHome(ctx, readSource, v1beta1.CandidatePlacement{Cluster: name, Allocation: assignment}, declaredComponents(source))
	if err != nil {
		return err
	}
	if !observed.Home.Absent {
		if !allowStanding {
			return fmt.Errorf("untracked standing resources cannot expand the original migration budget")
		}
		if observed.Member == nil {
			return fmt.Errorf("home has resources without a verifiable service policy")
		}
		home, err := r.resolveFullHome(ctx, source, observed.Member, name, assignment.ClusterUID, digest)
		if err != nil {
			return err
		}
		// An adopted policy describes the member spec, not unverified current
		// source intent. Only desired-policy resolution may authorize a refresh.
		home.InputDigest = string(observed.Member.UID) + "/" + observed.Member.ResourceVersion
		assignment.CurrentHome = home
		assignment.CurrentReplicas, _ = protocol.ValidateReplicaFloors(home.ReplicaFloors)
		assignment.OriginalReplicas = assignment.CurrentReplicas
	}
	assignment.InventoryPending = false
	return nil
}
