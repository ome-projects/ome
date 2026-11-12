package placement

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
	"sigs.k8s.io/ome/pkg/placement/affinity"
)

// splitCeilingRaisedReason: a ratio-form member's maxReplicas was raised to its
// minReplicas, so the members' maxReplicas add up to more than the source's.
const splitCeilingRaisedReason = "SplitCeilingRaised"

// usesRatioForm reports whether the source's affinity terms set replica ratios.
func usesRatioForm(source *v1beta1.InferenceService) bool {
	return source.Spec.Placement != nil && affinity.UsesRatios(source.Spec.Placement.ClusterAffinity)
}

// ratioCeilings divides each component's maxReplicas over the clusters the
// accepted plan gives replicas, by their max ratios, and returns the named
// member's part. A secondary component without max ratios of its own uses the
// primary's. A member the plan gives no replicas or no max ratio, or a
// component without maxReplicas, gets its share. A part below the share is
// raised to the share, and raised reports by how much, so the members'
// maximums never cut into the fleet minimum. shares are the member's planned
// minimums, one per bounded component.
func ratioCeilings(source *v1beta1.InferenceService, candidates []v1beta1.CandidatePlacement, member v1beta1.CandidatePlacement, shares []v1beta1.PlacementComponentFloor) (ceilings map[v1beta1.ComponentType]int32, raised map[v1beta1.ComponentType]int32, err error) {
	if member.Cluster == "" || member.Allocation == nil {
		return nil, nil, fmt.Errorf("split member maximums require an identified allocation")
	}
	minimums, err := splitComponentMinimums(source)
	if err != nil {
		return nil, nil, err
	}
	allocations := allocationsByCluster(candidates, member)
	divider := ceilingDivider{source: source, member: member, primary: minimums.primary, allocations: allocations, weights: splitWeightsOf(source, allocations)}
	ceilings, raised = map[v1beta1.ComponentType]int32{}, map[v1beta1.ComponentType]int32{}
	for _, share := range shares {
		ceiling, raise, err := divider.ceiling(share)
		if err != nil {
			return nil, nil, err
		}
		ceilings[share.Component] = ceiling
		if raise > 0 {
			raised[share.Component] = raise
		}
	}
	return ceilings, raised, nil
}

// allocationsByCluster indexes the identified candidates' allocations by
// cluster; the member's own allocation is authoritative for its cluster.
func allocationsByCluster(candidates []v1beta1.CandidatePlacement, member v1beta1.CandidatePlacement) map[string]v1beta1.CandidateAllocationStatus {
	allocations := map[string]v1beta1.CandidateAllocationStatus{}
	for _, candidate := range candidates {
		if candidate.Allocation != nil && candidate.Cluster != "" {
			allocations[candidate.Cluster] = *candidate.Allocation
		}
	}
	allocations[member.Cluster] = *member.Allocation
	return allocations
}

// ceilingDivider divides a source's component maximums over a plan's
// allocations for one member, by the max ratios the allocations record.
type ceilingDivider struct {
	source      *v1beta1.InferenceService
	member      v1beta1.CandidatePlacement
	primary     v1beta1.ComponentType
	allocations map[string]v1beta1.CandidateAllocationStatus
	weights     splitWeights
}

// ceiling is the member's part of one component's maxReplicas, raised to the
// member's share of the minimum, and the amount it was raised by. The share
// alone is the ceiling when the source declares no maximum, the member or
// its cluster holds no replicas or no max ratio, or no cluster with replicas
// has a positive max ratio to divide by.
func (d ceilingDivider) ceiling(share v1beta1.PlacementComponentFloor) (ceiling, raised int32, err error) {
	total := componentMaxReplicas(d.source, share.Component)
	assigned := d.assignedMaxWeights(share.Component)
	if _, recorded := assigned[d.member.Cluster]; total <= 0 || d.member.Allocation.DesiredReplicas <= 0 || !recorded {
		return share.Replicas, 0, nil
	}
	plan, err := allocation.Apportion(total, assigned, 0)
	if err != nil {
		return 0, 0, err
	}
	if plan.Unassigned > 0 {
		return share.Replicas, 0, nil
	}
	part := plan.Targets[d.member.Cluster]
	if part < share.Replicas {
		return share.Replicas, share.Replicas - part, nil
	}
	return part, 0, nil
}

// assignedMaxWeights are the max ratios the component is divided by, kept
// for the clusters the plan gives replicas: the component's own max ratios,
// or the primary's for the primary component and for a component without
// any of its own.
func (d ceilingDivider) assignedMaxWeights(component v1beta1.ComponentType) map[string]int64 {
	maxWeights := d.weights.maxComponents[component]
	if component == d.primary || len(maxWeights) == 0 {
		maxWeights = d.weights.maxPrimary
	}
	assigned := map[string]int64{}
	for name, weight := range maxWeights {
		if d.allocations[name].DesiredReplicas > 0 {
			assigned[name] = weight
		}
	}
	return assigned
}

// componentMaxReplicas reads a component's maxReplicas from the source, or 0.
func componentMaxReplicas(source *v1beta1.InferenceService, component v1beta1.ComponentType) int32 {
	var declared int
	switch {
	case component == v1beta1.EngineComponent && source.Spec.Engine != nil:
		declared = source.Spec.Engine.MaxReplicas
	case component == v1beta1.DecoderComponent && source.Spec.Decoder != nil:
		declared = source.Spec.Decoder.MaxReplicas
	case component == v1beta1.RouterComponent && source.Spec.Router != nil:
		declared = source.Spec.Router.MaxReplicas
	}
	return int32(min(max(declared, 0), math.MaxInt32))
}

// declaredMinimum is the minReplicas the source declares for a component it
// declares, or nil.
func declaredMinimum(source *v1beta1.InferenceService, component v1beta1.ComponentType) *int {
	switch {
	case component == v1beta1.EngineComponent && source.Spec.Engine != nil:
		return source.Spec.Engine.MinReplicas
	case component == v1beta1.DecoderComponent && source.Spec.Decoder != nil:
		return source.Spec.Decoder.MinReplicas
	case component == v1beta1.RouterComponent && source.Spec.Router != nil:
		return source.Spec.Router.MinReplicas
	}
	return nil
}

// componentMinReplicas reads a component's minReplicas from the source, or 0.
func componentMinReplicas(source *v1beta1.InferenceService, component v1beta1.ComponentType) int32 {
	declared := declaredMinimum(source, component)
	if declared == nil {
		return 0
	}
	return int32(min(max(*declared, 0), math.MaxInt32))
}

// recordSplitCeilingRaised warns on the source once per accepted plan inputs
// when a member's maxReplicas is raised to its minReplicas. previous is the source
// as read before the plan was persisted; an unchanged input digest means the
// warning was already given for this plan. A previous read from a lagging cache
// can repeat the warning; it is advisory and changes nothing.
func (r *Reconciler) recordSplitCeilingRaised(previous, accepted *v1beta1.InferenceService) {
	if placementMode(accepted) != v1beta1.PlacementModeSplit || !usesRatioForm(accepted) || accepted.Status.Placement == nil || accepted.Status.Placement.Plan == nil {
		return
	}
	if previous.Status.Placement != nil && previous.Status.Placement.Plan != nil && previous.Status.Placement.Plan.InputDigest == accepted.Status.Placement.Plan.InputDigest {
		return
	}
	notes, err := splitCeilingRaisedNotes(accepted)
	if err != nil {
		r.Log.V(1).Info("raised split maximums unresolved", "error", err.Error())
		return
	}
	if len(notes) > 0 {
		r.recordEvent(accepted, corev1.EventTypeWarning, splitCeilingRaisedReason, strings.Join(notes, ". "))
	}
}

// splitCeilingRaisedNotes describes each member maxReplicas the accepted plan
// raises to its minReplicas, with the amount the members' maxReplicas exceed
// the source's. Members are measured at their desired shares, where the
// plan settles, with the shares and maximums splitPlannedBounds gives them.
func splitCeilingRaisedNotes(source *v1beta1.InferenceService) ([]string, error) {
	minimums, err := splitComponentMinimums(source)
	if err != nil {
		return nil, err
	}
	candidates := settledStatusCandidates(source.Status.Placement.Candidates)
	raises := ceilingRaises{totals: map[v1beta1.ComponentType]int64{}, byComponent: map[v1beta1.ComponentType][]ceilingRaise{}}
	for _, member := range candidates {
		if member.Allocation.DesiredReplicas <= 0 {
			continue
		}
		shares, _, err := minimums.memberShares(source, candidates, member, nil)
		if err != nil {
			return nil, err
		}
		ceilings, raised, err := ratioCeilings(source, candidates, member, shares)
		if err != nil {
			return nil, err
		}
		raises.add(member.Cluster, ceilings, raised)
	}
	return raises.notes(source), nil
}

// settledStatusCandidates copies the identified candidates with every member at its
// desired share, where the plan settles.
func settledStatusCandidates(candidates []v1beta1.CandidatePlacement) []v1beta1.CandidatePlacement {
	var settled []v1beta1.CandidatePlacement
	for _, candidate := range candidates {
		if candidate.Allocation == nil || candidate.Cluster == "" {
			continue
		}
		allocation := *candidate.Allocation.DeepCopy()
		allocation.CurrentReplicas = allocation.DesiredReplicas
		settled = append(settled, v1beta1.CandidatePlacement{Cluster: candidate.Cluster, Allocation: &allocation})
	}
	return settled
}

// ceilingRaise is one member's maxReplicas raised to its minReplicas.
type ceilingRaise struct {
	cluster  string
	from, to int32
}

// ceilingRaises collects, per component, what the members' maxReplicas add
// up to and which of them were raised.
type ceilingRaises struct {
	totals      map[v1beta1.ComponentType]int64
	byComponent map[v1beta1.ComponentType][]ceilingRaise
}

// add records one member's ceilings and the raises among them.
func (r ceilingRaises) add(cluster string, ceilings, raised map[v1beta1.ComponentType]int32) {
	for component, ceiling := range ceilings {
		r.totals[component] += int64(ceiling)
	}
	for component, amount := range raised {
		if amount > 0 {
			r.byComponent[component] = append(r.byComponent[component], ceilingRaise{cluster: cluster, from: ceilings[component] - amount, to: ceilings[component]})
		}
	}
}

// notes describes every raise, components in order and clusters by name,
// each with how far the members' maximums exceed the source's.
func (r ceilingRaises) notes(source *v1beta1.InferenceService) []string {
	var notes []string
	for _, component := range slices.Sorted(maps.Keys(r.byComponent)) {
		raises := r.byComponent[component]
		slices.SortFunc(raises, func(a, b ceilingRaise) int { return strings.Compare(a.cluster, b.cluster) })
		declared := componentMaxReplicas(source, component)
		for _, raise := range raises {
			notes = append(notes, fmt.Sprintf("%s maxReplicas on cluster %s raised from %d to its minReplicas %d; the members' maxReplicas add up to %d, %d more than the service's maxReplicas %d",
				component, raise.cluster, raise.from, raise.to, r.totals[component], r.totals[component]-int64(declared), declared))
		}
	}
	return notes
}
