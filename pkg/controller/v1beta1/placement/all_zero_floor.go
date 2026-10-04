package placement

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
	"sigs.k8s.io/ome/pkg/placement/plan"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

func allHasZeroFloor(proposal plan.Proposal) bool {
	for _, a := range proposal.Assignments {
		if zeroHomeFloor(a.CurrentHome) || zeroHomeFloor(a.DesiredHome) {
			return true
		}
	}
	return false
}

// prepareAllZeroFloors preserves full homes independently of their replica
// count. Zero floors cannot reserve availability or join a shared surge pause.
func (r *Reconciler) prepareAllZeroFloors(ctx context.Context, source *v1beta1.InferenceService, proposal *plan.Proposal) (bool, error) {
	if !allHasZeroFloor(*proposal) {
		return false, nil
	}
	if proposal.PauseSurge {
		return false, errPositiveMovementFloor
	}
	initial := source.Status.Placement == nil || source.Status.Placement.Plan == nil
	adopting := initial || (proposal.AdoptionDigest != "" && proposal.AdoptionDigest == proposal.InputDigest)
	if proposal.AdoptionDigest != "" && !adopting {
		return false, fmt.Errorf("home inventory must complete before changing placement intent")
	}
	moving := false
	for _, a := range proposal.Assignments {
		if !a.Matched && a.InventoryPending {
			return false, fmt.Errorf("unmatched home inventory is unresolved")
		}
		moving = moving || a.DrainRequested || (a.CurrentHome != nil && !a.Matched)
		// Positive additions need shared accounting once initial fan-out ends.
		moving = moving || (!adopting && a.CurrentHome == nil && a.DesiredReplicas > 0)
	}
	if moving {
		return r.prepareAllMovementFloors(ctx, source, proposal)
	}
	for name, a := range proposal.Assignments {
		if a.InventoryPending || a.HomeInputsPending || !a.Matched || a.DesiredHome == nil || a.DesiredHome.InputDigest != proposal.InputDigest {
			continue
		}
		// An authored floor change on a retained home is local scaling. Its
		// autoscaler remains authoritative while no placement move is active.
		if a.CurrentHome != nil && a.CurrentReplicas != a.DesiredReplicas {
			a.OriginalReplicas = a.DesiredReplicas
		}
		a.CurrentHome = a.DesiredHome.DeepCopy()
		a.CurrentReplicas = a.DesiredReplicas
		proposal.Assignments[name] = a
	}
	return false, nil
}

func (r *Reconciler) prepareAllMovementFloors(ctx context.Context, source *v1beta1.InferenceService, proposal *plan.Proposal) (bool, error) {
	desired, err := r.derivedFor(source)
	if err != nil {
		return false, err
	}
	refreshed := false
	for name, a := range proposal.Assignments {
		if a.InventoryPending || a.HomeInputsPending {
			return false, fmt.Errorf("home %q inputs must be resolved before movement", name)
		}
		if err := positiveMovementFloor(a.DesiredHome); err != nil {
			return false, err
		}
		if !zeroHomeFloor(a.CurrentHome) {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		home, err := r.resolveFullHome(cctx, source, desired, name, a.ClusterUID, proposal.InputDigest)
		cancel()
		if err != nil {
			return false, err
		}
		if err := positiveMovementFloor(home); err != nil {
			return false, err
		}
		a.CurrentHome, a.DesiredHome = home.DeepCopy(), home.DeepCopy()
		a.CurrentReplicas, _ = protocol.ValidateReplicaFloors(home.ReplicaFloors)
		a.OriginalReplicas, a.DesiredReplicas = a.CurrentReplicas, a.CurrentReplicas
		proposal.Assignments[name] = a
		refreshed = true
	}
	return refreshed, nil
}

// applyAllMovementFloors publishes authored positive floors on retained homes
// before movement can pause growth and count those floors as availability.
func (r *Reconciler) applyAllMovementFloors(ctx context.Context, source *v1beta1.InferenceService, eligible []string, standing *placementObservations, proposal plan.Proposal) (ctrl.Result, error) {
	accepted, err := (plan.Store{Client: r.Client, Reader: r.APIReader}).Persist(ctx, source, proposal)
	if err != nil {
		return r.writeSplitHold(ctx, source, standing, "PlanNotPersisted", err)
	}
	for _, candidate := range accepted.Status.Placement.Candidates {
		if candidate.Allocation.CurrentHome == nil {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		err := r.placePlannedMember(cctx, accepted, candidate, true)
		cancel()
		if err != nil {
			return r.writeSplitHold(ctx, accepted, standing, "MemberApplicationFailed", err)
		}
	}
	return r.writeSplitObservations(ctx, accepted, r.observeSplitMembers(ctx, accepted, eligible), "AwaitingMemberConvergence", "")
}

// advanceAllZeroFloors acknowledges retained policy without treating autoscaled
// replicas as orphaned resources. Home removal uses positive-floor accounting.
func advanceAllZeroFloors(source *v1beta1.InferenceService, observations map[string]plannedHomeObservation) allocation.Step {
	out := allocation.Step{Targets: map[string]int32{}, Complete: true}
	for _, candidate := range source.Status.Placement.Candidates {
		a, observed := candidate.Allocation, observations[candidate.Cluster]
		out.Targets[candidate.Cluster] = a.CurrentReplicas
		if a.DrainRequested || a.CurrentReplicas != a.DesiredReplicas || (a.CurrentHome != nil) != (a.DesiredHome != nil) {
			out.Complete, out.Reason = false, "PositiveFloorRequired"
		} else if (a.CurrentHome != nil && (!observed.Home.Known || !observed.Home.Applied)) || (a.CurrentHome == nil && !observed.Home.Absent) {
			out.Complete = false
			if out.Reason == "" {
				out.Reason = "AwaitingMemberConvergence"
			}
		}
	}
	return out
}
