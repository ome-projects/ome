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
// component without maxReplicas, gets its share. A part below the share is raised to the share,
// and raised reports by how much, so the members' maximums never cut into the
// fleet minimum. shares are the member's planned minimums, one per bounded
// component.
func ratioCeilings(source *v1beta1.InferenceService, candidates []v1beta1.CandidatePlacement, member v1beta1.CandidatePlacement, shares []v1beta1.PlacementComponentFloor) (ceilings map[v1beta1.ComponentType]int32, raised map[v1beta1.ComponentType]int32, err error) {
	if member.Cluster == "" || member.Allocation == nil {
		return nil, nil, fmt.Errorf("split member maximums require an identified allocation")
	}
	minimums, err := splitComponentMinimums(source)
	if err != nil {
		return nil, nil, err
	}
	allocations := map[string]v1beta1.CandidateAllocationStatus{}
	for _, candidate := range candidates {
		if candidate.Allocation != nil && candidate.Cluster != "" {
			allocations[candidate.Cluster] = *candidate.Allocation
		}
	}
	allocations[member.Cluster] = *member.Allocation
	weights := splitWeightsOf(source, allocations)
	ceilings, raised = map[v1beta1.ComponentType]int32{}, map[v1beta1.ComponentType]int32{}
	for _, share := range shares {
		component := share.Component
		total := componentMaxReplicas(source, component)
		maxWeights := weights.maxComponents[component]
		if component == minimums.primary || len(maxWeights) == 0 {
			maxWeights = weights.maxPrimary
		}
		assigned := map[string]int64{}
		for name, weight := range maxWeights {
			if allocations[name].DesiredReplicas > 0 {
				assigned[name] = weight
			}
		}
		if _, recorded := assigned[member.Cluster]; total <= 0 || member.Allocation.DesiredReplicas <= 0 || !recorded {
			ceilings[component] = share.Replicas
			continue
		}
		plan, err := allocation.Apportion(total, assigned, 0)
		if err != nil {
			return nil, nil, err
		}
		if plan.Unassigned > 0 {
			// No assigned cluster has a positive max ratio to divide by.
			ceilings[component] = share.Replicas
			continue
		}
		part := plan.Targets[member.Cluster]
		if part < share.Replicas {
			raised[component] = share.Replicas - part
		}
		ceilings[component] = max(part, share.Replicas)
	}
	return ceilings, raised, nil
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

// componentMinReplicas reads a component's minReplicas from the source, or 0.
func componentMinReplicas(source *v1beta1.InferenceService, component v1beta1.ComponentType) int32 {
	var declared *int
	switch {
	case component == v1beta1.EngineComponent && source.Spec.Engine != nil:
		declared = source.Spec.Engine.MinReplicas
	case component == v1beta1.DecoderComponent && source.Spec.Decoder != nil:
		declared = source.Spec.Decoder.MinReplicas
	case component == v1beta1.RouterComponent && source.Spec.Router != nil:
		declared = source.Spec.Router.MinReplicas
	}
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
	var candidates []v1beta1.CandidatePlacement
	for _, candidate := range source.Status.Placement.Candidates {
		if candidate.Allocation == nil || candidate.Cluster == "" {
			continue
		}
		settled := *candidate.Allocation.DeepCopy()
		settled.CurrentReplicas = settled.DesiredReplicas
		candidates = append(candidates, v1beta1.CandidatePlacement{Cluster: candidate.Cluster, Allocation: &settled})
	}
	type raise struct {
		cluster  string
		from, to int32
	}
	totals := map[v1beta1.ComponentType]int64{}
	raisedOn := map[v1beta1.ComponentType][]raise{}
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
		for component, ceiling := range ceilings {
			totals[component] += int64(ceiling)
		}
		for component, amount := range raised {
			if amount > 0 {
				raisedOn[component] = append(raisedOn[component], raise{cluster: member.Cluster, from: ceilings[component] - amount, to: ceilings[component]})
			}
		}
	}
	var notes []string
	for _, component := range slices.Sorted(maps.Keys(raisedOn)) {
		raises := raisedOn[component]
		slices.SortFunc(raises, func(a, b raise) int { return strings.Compare(a.cluster, b.cluster) })
		declared := componentMaxReplicas(source, component)
		for _, r := range raises {
			notes = append(notes, fmt.Sprintf("%s maxReplicas on cluster %s raised from %d to its minReplicas %d; the members' maxReplicas add up to %d, %d more than the service's maxReplicas %d",
				component, r.cluster, r.from, r.to, totals[component], totals[component]-int64(declared), declared))
		}
	}
	return notes, nil
}
