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
	"sigs.k8s.io/ome/pkg/placement/affinity"
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
		if !staysWithoutGrowing(a, proposal.InputDigest) {
			return
		}
	}
	for name, a := range proposal.Assignments {
		if a.Matched && a.CurrentReplicas != a.DesiredReplicas {
			proposal.Assignments[name] = withMinimumsTaken(a)
		}
	}
}

// staysWithoutGrowing reports whether a member keeps its place in the plan
// and asks for no more than it holds: it is not being inventoried, waiting
// for its inputs or draining; an unmatched member has nothing left; a matched
// member has a resolved policy at the plan's inputs that raises neither its
// count nor any component's minimum.
func staysWithoutGrowing(a v1beta1.CandidateAllocationStatus, inputDigest string) bool {
	if a.InventoryPending || a.HomeInputsPending || a.DrainRequested {
		return false
	}
	if !a.Matched {
		return a.CurrentHome == nil && a.CurrentReplicas == 0
	}
	if a.CurrentHome == nil || a.DesiredHome == nil || a.DesiredHome.InputDigest != inputDigest {
		return false
	}
	return a.DesiredReplicas <= a.CurrentReplicas && !componentMinimumRaised(a.CurrentHome, a.DesiredHome)
}

// withMinimumsTaken is the member with its new minimums as its current
// allocation: its own copy of the resolved policy, and the new count as what
// it runs and as where its change began.
func withMinimumsTaken(a v1beta1.CandidateAllocationStatus) v1beta1.CandidateAllocationStatus {
	a.CurrentHome = a.DesiredHome.DeepCopy()
	a.CurrentReplicas, a.OriginalReplicas = a.DesiredReplicas, a.DesiredReplicas
	return a
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
	selector, err := placementSelector(source)
	if err != nil {
		return out, nil, err
	}
	inventory, err := allRegistrations(clusters, selector)
	if err != nil {
		return out, nil, err
	}
	out.InputDigest, err = allInputDigest(source, inventory.matched)
	if err != nil {
		return out, nil, err
	}
	if err := carryStandingAllAssignments(source, inventory.registered, &out); err != nil {
		return out, nil, err
	}
	if err := assignMatchedMembers(inventory.matched, out.Assignments); err != nil {
		return out, nil, err
	}
	desired, err := r.derivedFor(source)
	if err != nil {
		return out, nil, err
	}
	var holds []string
	pending := false
	adopting := out.AdoptionDigest != ""
	for _, name := range slices.Sorted(maps.Keys(out.Assignments)) {
		a, hold := r.resolveAllAssignment(ctx, source, desired, name, out.Assignments[name], inventory, eligible, out.InputDigest, adopting)
		if hold != "" {
			holds = append(holds, hold)
		}
		pending = pending || a.InventoryPending || a.HomeInputsPending
		out.Assignments[name] = a
	}
	if !pending {
		out.AdoptionDigest = ""
	}
	if pending && out.AdoptionDigest == out.InputDigest {
		acceptAdditiveProvisioning(&out, inventory.matched)
	}
	return out, holds, nil
}

// allInventory is what the registrations say about the clusters: every
// identified registration, the matched members with their UIDs, and the
// affinity terms each matched member satisfied.
type allInventory struct {
	registered map[string]types.UID
	matched    map[string]types.UID
	terms      map[string][]int32
}

// allRegistrations reads the inventory; a nameless, unidentified or repeated
// registration refuses the plan. A registration being deleted matches nothing.
func allRegistrations(clusters []v1beta1.WorkloadCluster, selector *affinity.Selector) (allInventory, error) {
	inventory := allInventory{registered: map[string]types.UID{}, matched: map[string]types.UID{}, terms: map[string][]int32{}}
	for i := range clusters {
		cluster := &clusters[i]
		if cluster.UID == "" || cluster.Name == "" || inventory.registered[cluster.Name] != "" {
			return inventory, fmt.Errorf("home inventory requires distinct identified registrations")
		}
		inventory.registered[cluster.Name] = cluster.UID
		if match, ok := selector.Match(cluster); ok && cluster.DeletionTimestamp.IsZero() {
			inventory.matched[cluster.Name], inventory.terms[cluster.Name] = cluster.UID, match.TermIndexes
		}
	}
	return inventory, nil
}

// allInputDigest names the plan's inputs: the spec, the labels, the intent
// annotations and the matched members.
func allInputDigest(source *v1beta1.InferenceService, matched map[string]types.UID) (string, error) {
	encoded, err := json.Marshal(struct {
		Spec                v1beta1.InferenceServiceSpec
		Labels, Annotations map[string]string
		Members             map[string]types.UID
	}{source.Spec, source.Labels, planIntentAnnotations(source.Annotations), matched})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// carryStandingAllAssignments seeds the assignments. An accepted plan of this
// source in All mode carries its allocations, adoption digest and pause over.
// A first plan inventories every registration before it allocates, and
// refuses a standing member whose registration is unknown.
func carryStandingAllAssignments(source *v1beta1.InferenceService, registered map[string]types.UID, out *plan.Proposal) error {
	if source.Status.Placement != nil && source.Status.Placement.Plan != nil {
		previous := source.Status.Placement.Plan
		if previous.SourceUID != source.UID || previous.Mode != v1beta1.PlacementModeAll {
			return fmt.Errorf("standing allocation must be reconciled in its accepted placement mode")
		}
		out.AdoptionDigest, out.PauseSurge = previous.AdoptionDigest, previous.PauseSurge
		for _, candidate := range source.Status.Placement.Candidates {
			if candidate.Allocation == nil {
				return fmt.Errorf("standing home %q has no allocation authority", candidate.Cluster)
			}
			out.Assignments[candidate.Cluster] = *candidate.Allocation.DeepCopy()
		}
		return nil
	}
	out.AdoptionDigest = out.InputDigest
	for _, candidate := range standingPlacementCandidates(source) {
		if registered[candidate.Cluster] == "" {
			return fmt.Errorf("standing home %q has no identified registration", candidate.Cluster)
		}
	}
	// A member write may precede its first successful source status write.
	for name, uid := range registered {
		out.Assignments[name] = v1beta1.CandidateAllocationStatus{ClusterUID: uid, InventoryPending: true}
	}
	return nil
}

// assignMatchedMembers gives every matched member an assignment, keeping the
// one it has; a member whose registration changed identity refuses the plan.
func assignMatchedMembers(matched map[string]types.UID, assignments map[string]v1beta1.CandidateAllocationStatus) error {
	for name, uid := range matched {
		a, exists := assignments[name]
		if exists && a.ClusterUID != uid {
			return fmt.Errorf("home %q registration identity changed", name)
		}
		if !exists {
			a = v1beta1.CandidateAllocationStatus{ClusterUID: uid, InventoryPending: true}
		}
		assignments[name] = a
	}
	return nil
}

// resolveAllAssignment settles one member's assignment for the pass: it
// records the matched terms, inventories a member the plan has not adopted,
// and resolves a matched eligible member's policy. A resolution that fails
// leaves the member waiting for its inputs and names the cause.
func (r *Reconciler) resolveAllAssignment(ctx context.Context, source, desired *v1beta1.InferenceService, name string, a v1beta1.CandidateAllocationStatus, inventory allInventory, eligible []string, digest string, adopting bool) (v1beta1.CandidateAllocationStatus, string) {
	a.MatchingTerms = inventory.terms[name]
	a.Matched = inventory.matched[name] != ""
	a.HomeInputsPending = a.Matched
	if inventory.matched[name] == "" {
		a.DesiredHome, a.DesiredReplicas = nil, 0
	}
	if a.InventoryPending {
		if err := r.adoptAllModeMember(ctx, source, name, &a, digest, adopting); err != nil {
			r.Log.V(1).Info("home inventory is pending", "cluster", name, "error", err)
		}
	}
	if inventory.matched[name] == "" || !slices.Contains(eligible, name) {
		return a, ""
	}
	cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
	policy, resolveErr := r.resolveFullPolicy(cctx, source, desired, name, a.ClusterUID, digest)
	cancel()
	if resolveErr != nil {
		r.Log.V(1).Info("home policy is unresolved", "cluster", name, "error", resolveErr)
		return a, fmt.Sprintf("home policy is unresolved on %s: %v", name, resolveErr)
	}
	a.HomeInputsPending = false
	a.DesiredHome = policy
	a.DesiredReplicas, _ = protocol.ValidateReplicaFloors(policy.ReplicaFloors)
	return a, ""
}

// acceptAdditiveProvisioning takes a first plan's resolved members as their
// own current allocation when no unmatched member may still serve and no
// member shrinks: new members then read as an addition, not a replacement
// move. An unmatched member still being inventoried or holding a policy may
// serve, so it keeps the move reading.
func acceptAdditiveProvisioning(out *plan.Proposal, matched map[string]types.UID) {
	for name, a := range out.Assignments {
		if matched[name] == "" && (a.InventoryPending || a.CurrentHome != nil) {
			return
		}
		if a.CurrentReplicas > a.DesiredReplicas {
			return
		}
	}
	for name, a := range out.Assignments {
		if !a.InventoryPending && !a.HomeInputsPending && a.CurrentHome == nil && a.DesiredHome != nil && a.DesiredHome.InputDigest == out.InputDigest {
			a.CurrentHome, a.CurrentReplicas = a.DesiredHome.DeepCopy(), a.DesiredReplicas
			out.Assignments[name] = a
		}
	}
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
	_, floors, err := (resolution.Resolver{Client: cl, OperatorNamespace: r.MemberOperatorNamespace}).ResolveAssignedCluster(ctx, desired, member)
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
