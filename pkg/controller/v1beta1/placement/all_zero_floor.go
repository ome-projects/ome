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
		if hasZeroReplicaFloor(a.CurrentHome) || hasZeroReplicaFloor(a.DesiredHome) {
			return true
		}
	}
	return false
}

// prepareAllZeroFloors preserves full-policy members independently of their replica
// count. Zero floors cannot reserve availability or join a shared surge pause.
func (r *Reconciler) prepareAllZeroFloors(ctx context.Context, source *v1beta1.InferenceService, proposal *plan.Proposal) (bool, error) {
	if !allHasZeroFloor(*proposal) {
		return false, nil
	}
	if proposal.PauseSurge {
		return false, errPositiveMovementFloor
	}
	adopting, err := allZeroAdoption(source, proposal)
	if err != nil {
		return false, err
	}
	moving, err := allZeroMovement(proposal, adopting)
	if err != nil {
		return false, err
	}
	if moving {
		return r.prepareAllMovementFloors(ctx, source, proposal)
	}
	acceptAllZeroPolicies(proposal)
	return false, nil
}

// allZeroAdoption reports whether members are still being adopted: before any
// plan exists, or while the inventory digest equals the current intent. An
// inventory taken under other intent must finish before the intent changes.
func allZeroAdoption(source *v1beta1.InferenceService, proposal *plan.Proposal) (bool, error) {
	initial := source.Status.Placement == nil || source.Status.Placement.Plan == nil
	adopting := initial || (proposal.AdoptionDigest != "" && proposal.AdoptionDigest == proposal.InputDigest)
	if proposal.AdoptionDigest != "" && !adopting {
		return false, fmt.Errorf("home inventory must complete before changing placement intent")
	}
	return adopting, nil
}

// allZeroMovement reports whether any member moves. An unmatched member whose
// inventory is still pending cannot be placed either way and is refused.
func allZeroMovement(proposal *plan.Proposal, adopting bool) (bool, error) {
	moving := false
	for _, a := range proposal.Assignments {
		if !a.Matched && a.InventoryPending {
			return false, fmt.Errorf("unmatched home inventory is unresolved")
		}
		moving = moving || assignmentMoves(a, adopting)
	}
	return moving, nil
}

// assignmentMoves reports a member that moves capacity between clusters: a
// requested drain, a retained member the current intent does not select, or,
// once initial fan-out ends, a new member with replicas.
func assignmentMoves(a v1beta1.CandidateAllocationStatus, adopting bool) bool {
	if a.DrainRequested || (a.CurrentHome != nil && !a.Matched) {
		return true
	}
	// Positive additions need shared accounting once initial fan-out ends.
	return !adopting && a.CurrentHome == nil && a.DesiredReplicas > 0
}

// acceptAllZeroPolicies takes each resolved desired policy of the current
// intent as its member's current policy. An authored floor change on a
// retained member is local scaling: its autoscaler remains authoritative
// while no placement move is active.
func acceptAllZeroPolicies(proposal *plan.Proposal) {
	for name, a := range proposal.Assignments {
		if !acceptsDesiredPolicy(a, proposal.InputDigest) {
			continue
		}
		if a.CurrentHome != nil && a.CurrentReplicas != a.DesiredReplicas {
			a.OriginalReplicas = a.DesiredReplicas
		}
		a.CurrentHome = a.DesiredHome.DeepCopy()
		a.CurrentReplicas = a.DesiredReplicas
		proposal.Assignments[name] = a
	}
}

// acceptsDesiredPolicy reports whether an assignment's desired policy can be
// taken as current: its inputs are resolved, it is matched, and the policy
// was resolved for the current intent.
func acceptsDesiredPolicy(a v1beta1.CandidateAllocationStatus, digest string) bool {
	return !a.InventoryPending && !a.HomeInputsPending && a.Matched && a.DesiredHome != nil && a.DesiredHome.InputDigest == digest
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
		if !hasZeroReplicaFloor(a.CurrentHome) {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		policy, err := r.resolveFullPolicy(cctx, source, desired, name, a.ClusterUID, proposal.InputDigest)
		cancel()
		if err != nil {
			return false, err
		}
		if err := positiveMovementFloor(policy); err != nil {
			return false, err
		}
		a.CurrentHome, a.DesiredHome = policy.DeepCopy(), policy.DeepCopy()
		a.CurrentReplicas, _ = protocol.ValidateReplicaFloors(policy.ReplicaFloors)
		a.OriginalReplicas, a.DesiredReplicas = a.CurrentReplicas, a.CurrentReplicas
		proposal.Assignments[name] = a
		refreshed = true
	}
	return refreshed, nil
}

// applyAllMovementFloors publishes authored positive floors on retained members
// before movement can pause growth and count those floors as availability.
func (r *Reconciler) applyAllMovementFloors(ctx context.Context, source *v1beta1.InferenceService, eligible []string, standing *placementObservations, proposal plan.Proposal) (ctrl.Result, error) {
	accepted, err := (plan.Store{Client: r.Client, Reader: r.APIReader}).Persist(ctx, source, proposal)
	if err != nil {
		return r.writeSplitHold(ctx, source, standing, reasonPlanNotPersisted, err)
	}
	for _, candidate := range accepted.Status.Placement.Candidates {
		if candidate.Allocation.CurrentHome == nil {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		err := r.placePlannedMember(cctx, accepted, candidate, true, nil)
		cancel()
		if err != nil {
			return r.writeSplitHold(ctx, accepted, standing, reasonMemberApplicationFailed, err)
		}
	}
	return r.writeSplitObservations(ctx, accepted, r.observeSplitMembers(ctx, accepted, eligible), reasonAwaitingMemberConvergence, "")
}

// advanceAllZeroFloors acknowledges retained policy without treating autoscaled
// replicas as orphaned resources. Member removal uses positive-floor accounting.
func advanceAllZeroFloors(source *v1beta1.InferenceService, observations map[string]plannedMemberObservation) allocation.Step {
	out := allocation.Step{Targets: map[string]int32{}, Complete: true}
	for _, candidate := range source.Status.Placement.Candidates {
		a, observed := candidate.Allocation, observations[candidate.Cluster]
		out.Targets[candidate.Cluster] = a.CurrentReplicas
		if a.DrainRequested || a.CurrentReplicas != a.DesiredReplicas || (a.CurrentHome != nil) != (a.DesiredHome != nil) {
			out.Complete, out.Reason = false, reasonPositiveMinimumRequired
		} else if (a.CurrentHome != nil && (!observed.MemberState.Known || !observed.MemberState.Applied)) || (a.CurrentHome == nil && !observed.MemberState.Absent) {
			out.Complete = false
			if out.Reason == "" {
				out.Reason = reasonAwaitingMemberConvergence
			}
		}
	}
	return out
}
