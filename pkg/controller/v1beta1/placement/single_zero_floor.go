package placement

import (
	"context"
	"errors"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/plan"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

var errPositiveMovementFloor = errors.New("placement movement requires positive component floors")

func zeroHomeFloor(home *v1beta1.PlacementHomePolicy) bool {
	if home == nil {
		return false
	}
	for _, floor := range home.ReplicaFloors {
		if floor.Replicas == 0 {
			return true
		}
	}
	return false
}

func positiveMovementFloor(home *v1beta1.PlacementHomePolicy) error {
	if home == nil {
		return nil
	}
	if _, err := protocol.ValidatePositiveReplicaFloors(home.ReplicaFloors); err != nil {
		return fmt.Errorf("%w: %v", errPositiveMovementFloor, err)
	}
	return nil
}

// retainZeroFloorWinner distinguishes idle demand and pending policy application
// from admission loss. Unverified inventory cannot authorize a new race.
func (r *Reconciler) retainZeroFloorWinner(ctx context.Context, source *v1beta1.InferenceService, winner string) (bool, error) {
	if source.Status.Placement == nil || source.Status.Placement.Plan == nil || source.Status.Placement.Plan.PauseSurge {
		return false, nil
	}
	for _, candidate := range source.Status.Placement.Candidates {
		if candidate.Cluster != winner || candidate.Allocation == nil || !zeroHomeFloor(candidate.Allocation.CurrentHome) {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		observed, err := r.observePlannedHome(cctx, source, candidate, declaredComponents(source))
		cancel()
		if err != nil {
			return false, err
		}
		return observed.IdleZeroFloor || observed.ScalingToZero || (observed.Member != nil && !observed.Home.Applied), nil
	}
	return false, nil
}

// refreshSingleMovementFloor applies an authored positive floor to the retained
// home before it can reserve availability for a placement move.
func (r *Reconciler) refreshSingleMovementFloor(ctx context.Context, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, standing *placementObservations) (bool, ctrl.Result, error) {
	winner := winnerCluster(source)
	if source.Status.Placement == nil || source.Status.Placement.Plan == nil || source.Status.Placement.Plan.SingleMove != nil {
		return false, ctrl.Result{}, nil
	}
	for _, candidate := range source.Status.Placement.Candidates {
		if candidate.Cluster != winner || candidate.Allocation == nil || !zeroHomeFloor(candidate.Allocation.CurrentHome) {
			continue
		}
		proposal, err := r.singleProposal(ctx, source, clusters, winner, []string{winner})
		if err == nil {
			err = positiveMovementFloor(proposal.Assignments[winner].CurrentHome)
		}
		if err != nil {
			reason := "HomePolicyUnresolved"
			if errors.Is(err, errPositiveMovementFloor) {
				reason = "PositiveFloorRequired"
			}
			result, writeErr := r.writeSplitHold(ctx, source, standing, reason, err)
			return true, result, writeErr
		}
		accepted, err := (plan.Store{Client: r.Client, Reader: r.APIReader}).Persist(ctx, source, proposal)
		if err != nil {
			result, writeErr := r.writeSplitHold(ctx, source, standing, "PlanNotPersisted", err)
			return true, result, writeErr
		}
		for _, target := range accepted.Status.Placement.Candidates {
			if target.Cluster == winner {
				cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
				err = r.placePlannedMember(cctx, accepted, target, true)
				cancel()
				if err != nil {
					result, writeErr := r.writeSplitHold(ctx, accepted, standing, "MemberApplicationFailed", err)
					return true, result, writeErr
				}
			}
		}
		r.sweepPlannedRace(ctx, accepted)
		standing.refresh(ctx, r, accepted, winner)
		result, err := r.writeSinglePlanStatus(ctx, accepted, standing, "AwaitingMemberConvergence", "")
		return true, result, err
	}
	return false, ctrl.Result{}, nil
}
