package placement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

// reconcileAllPlanned retains complete per-member policies during bounded moves.
// An incomplete initial inventory permits independent additive provisioning
// only while the original source intent and matched membership remain fixed.
func (r *Reconciler) reconcileAllPlanned(ctx context.Context, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, eligible []string, standing *placementObservations) (ctrl.Result, error) {
	proposal, holds, err := r.allProposal(ctx, source, clusters, eligible)
	if err != nil {
		return r.writeSplitHold(ctx, source, standing, "HomePolicyUnresolved", err)
	}
	refresh, err := r.prepareAllZeroFloors(ctx, source, &proposal)
	if err != nil {
		reason := "HomePolicyUnresolved"
		if errors.Is(err, errPositiveMovementFloor) {
			reason = "PositiveFloorRequired"
		}
		return r.writeSplitHold(ctx, source, standing, reason, err)
	}
	if refresh {
		return r.applyAllMovementFloors(ctx, source, eligible, standing, proposal)
	}
	acceptAllLocalMinimums(&proposal)
	return r.executePlannedAllocation(ctx, source, eligible, standing, proposal, holds)
}

// acceptAllLocalMinimums takes a lowered minimum as local scaling when every
// member stays: no cluster is added, released or drained, and no component's
// minimum grows on any member, so no capacity moves between clusters and no
// member needs more than it holds. Each member takes its new minimums at once
// and its autoscaler stays authoritative above them, the rule the
// zero-minimum path already applies. Any raised component minimum, an active
// pause, a pending inventory or an unresolved member leaves the proposal
// unchanged, so growth keeps the surge pause.
func acceptAllLocalMinimums(proposal *plan.Proposal) {
	if proposal.PauseSurge || proposal.AdoptionDigest != "" || proposal.SingleMove != nil {
		return
	}
	for _, a := range proposal.Assignments {
		if a.InventoryPending || a.HomeInputsPending || a.DrainRequested {
			return
		}
		if !a.Matched {
			if a.CurrentHome != nil || a.CurrentReplicas != 0 {
				return
			}
			continue
		}
		if a.CurrentHome == nil || a.DesiredHome == nil || a.DesiredHome.InputDigest != proposal.InputDigest {
			return
		}
		if a.DesiredReplicas > a.CurrentReplicas || componentMinimumRaised(a.CurrentHome, a.DesiredHome) {
			return
		}
	}
	for name, a := range proposal.Assignments {
		if !a.Matched || a.CurrentReplicas == a.DesiredReplicas {
			continue
		}
		a.CurrentHome = a.DesiredHome.DeepCopy()
		a.CurrentReplicas, a.OriginalReplicas = a.DesiredReplicas, a.DesiredReplicas
		proposal.Assignments[name] = a
	}
}

// componentMinimumRaised reports whether desired adds a component or raises
// any component's minimum over current.
func componentMinimumRaised(current, desired *v1beta1.PlacementHomePolicy) bool {
	held := make(map[v1beta1.ComponentType]int32, len(current.ReplicaFloors))
	for _, minimum := range current.ReplicaFloors {
		held[minimum.Component] = minimum.Replicas
	}
	for _, minimum := range desired.ReplicaFloors {
		if was, ok := held[minimum.Component]; !ok || minimum.Replicas > was {
			return true
		}
	}
	return false
}

// allProposal resolves every matched member's full policy. Members whose policy
// cannot be resolved stay pending; their causes are returned for the status.
func (r *Reconciler) allProposal(ctx context.Context, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, eligible []string) (plan.Proposal, []string, error) {
	out := plan.Proposal{Mode: v1beta1.PlacementModeAll, Assignments: map[string]v1beta1.CandidateAllocationStatus{}}
	var holds []string
	selector, err := placementSelector(source)
	if err != nil {
		return out, nil, err
	}
	matched := map[string]types.UID{}
	terms := map[string][]int32{}
	registered := map[string]types.UID{}
	for i := range clusters {
		cluster := &clusters[i]
		if cluster.UID == "" || cluster.Name == "" || registered[cluster.Name] != "" {
			return out, nil, fmt.Errorf("home inventory requires distinct identified registrations")
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
	}{source.Spec, source.Labels, planIntentAnnotations(source.Annotations), matched})
	if err != nil {
		return out, nil, err
	}
	digest := sha256.Sum256(encoded)
	out.InputDigest = hex.EncodeToString(digest[:])
	initial := source.Status.Placement == nil || source.Status.Placement.Plan == nil
	if !initial {
		previous := source.Status.Placement.Plan
		if previous.SourceUID != source.UID || previous.Mode != v1beta1.PlacementModeAll {
			return out, nil, fmt.Errorf("standing allocation must be reconciled in its accepted placement mode")
		}
		out.AdoptionDigest, out.PauseSurge = previous.AdoptionDigest, previous.PauseSurge
		for _, candidate := range source.Status.Placement.Candidates {
			if candidate.Allocation == nil {
				return out, nil, fmt.Errorf("standing home %q has no allocation authority", candidate.Cluster)
			}
			out.Assignments[candidate.Cluster] = *candidate.Allocation.DeepCopy()
		}
	} else {
		out.AdoptionDigest = out.InputDigest
		for _, candidate := range standingPlacementCandidates(source) {
			if registered[candidate.Cluster] == "" {
				return out, nil, fmt.Errorf("standing home %q has no identified registration", candidate.Cluster)
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
			return out, nil, fmt.Errorf("home %q registration identity changed", name)
		}
		if !exists {
			a = v1beta1.CandidateAllocationStatus{ClusterUID: uid, InventoryPending: true}
		}
		out.Assignments[name] = a
	}
	desired, err := r.derivedFor(source)
	if err != nil {
		return out, nil, err
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
		if a.InventoryPending {
			err = r.adoptAllModeMember(ctx, source, name, &a, out.InputDigest, out.AdoptionDigest != "")
			if err != nil {
				r.Log.V(1).Info("home inventory is pending", "cluster", name, "error", err)
			}
		}
		if matched[name] != "" && slices.Contains(eligible, name) {
			cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
			policy, resolveErr := r.resolveFullPolicy(cctx, source, desired, name, a.ClusterUID, out.InputDigest)
			cancel()
			if resolveErr == nil {
				a.HomeInputsPending = false
				a.DesiredHome = policy
				a.DesiredReplicas, _ = protocol.ValidateReplicaFloors(policy.ReplicaFloors)
			} else {
				r.Log.V(1).Info("home policy is unresolved", "cluster", name, "error", resolveErr)
				holds = append(holds, fmt.Sprintf("home policy is unresolved on %s: %v", name, resolveErr))
			}
		}
		pending = pending || a.InventoryPending || a.HomeInputsPending
		out.Assignments[name] = a
	}
	if !pending {
		out.AdoptionDigest = ""
	}
	if pending && out.AdoptionDigest == out.InputDigest {
		// Unknown unmatched members may still serve. They prevent interpreting new
		// members as initial additive provisioning rather than a replacement move.
		additive := true
		for name, a := range out.Assignments {
			if matched[name] == "" && (a.InventoryPending || a.CurrentHome != nil) {
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
	return out, holds, nil
}

func (r *Reconciler) resolveFullPolicy(ctx context.Context, source, desired *v1beta1.InferenceService, name string, uid types.UID, digest string) (*v1beta1.PlacementHomePolicy, error) {
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
	return &v1beta1.PlacementHomePolicy{InputDigest: digest, ReplicaFloors: floors}, nil
}

// adoptAllModeMember inventories one member before an allocation exists. ctx is the
// reconcile context: the member read and the policy resolution each bound
// their own calls by placeTimeout.
func (r *Reconciler) adoptAllModeMember(ctx context.Context, source *v1beta1.InferenceService, name string, assignment *v1beta1.CandidateAllocationStatus, digest string, allowStanding bool) error {
	readSource := source.DeepCopy()
	readSource.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{SourceUID: source.UID}}
	observed, err := r.observePlannedMember(ctx, readSource, v1beta1.CandidatePlacement{Cluster: name, Allocation: assignment}, declaredComponents(source))
	if err != nil {
		return err
	}
	if !observed.MemberState.Absent {
		if !allowStanding {
			return fmt.Errorf("untracked standing resources cannot expand the original migration budget")
		}
		if observed.Member == nil {
			return fmt.Errorf("home has resources without a verifiable service policy")
		}
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		policy, err := r.resolveFullPolicy(cctx, source, observed.Member, name, assignment.ClusterUID, digest)
		cancel()
		if err != nil {
			return err
		}
		// An adopted policy describes the member spec, not unverified current
		// source intent. Only desired-policy resolution may authorize a refresh.
		policy.InputDigest = string(observed.Member.UID) + "/" + observed.Member.ResourceVersion
		assignment.CurrentHome = policy
		assignment.CurrentReplicas, _ = protocol.ValidateReplicaFloors(policy.ReplicaFloors)
		assignment.OriginalReplicas = assignment.CurrentReplicas
	}
	assignment.InventoryPending = false
	return nil
}
