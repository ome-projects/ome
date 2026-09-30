package placement

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// retireCapacityFloor preserves an outgoing home's rendering while reducing
// its floor. Hardware weights authorize new allocation; they cannot prevent
// cleanup of an already accepted, observed reduction.
func (r *Reconciler) retireCapacityFloor(ctx context.Context, cl client.Client, source *v1beta1.InferenceService, assignment *v1beta1.CandidateAllocationStatus) error {
	member := &v1beta1.InferenceService{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(source), member); err != nil {
		return err
	}
	if !isOurDerived(member, source) || member.UID == "" || !member.DeletionTimestamp.IsZero() {
		return fmt.Errorf("retiring capacity floor requires an identified live derived service")
	}
	floor, err := adoptedMemberFloor(member)
	if err != nil {
		return err
	}
	if assignment.CurrentReplicas > floor {
		return fmt.Errorf("retiring capacity floor cannot grow member replicas")
	}
	policy := executionPolicy(source, assignment)
	if err := protocol.ValidateDemandComponents(member, policy.Demand); err != nil {
		return err
	}
	previous, err := protocol.FromDerived(member)
	if err != nil {
		return err
	}
	if err := protocol.Authorize(previous, policy); err != nil {
		return err
	}
	raw, err := protocol.Encode(policy)
	if err != nil {
		return err
	}
	if floor == assignment.CurrentReplicas && member.Annotations[constants.PlacementExecution] == raw {
		return nil
	}
	base := member.DeepCopy()
	setPlannedReplicas(member, assignment.CurrentReplicas, 0)
	if member.Annotations == nil {
		member.Annotations = map[string]string{}
	}
	member.Annotations[constants.PlacementExecution] = raw
	if err := r.checkPlanCurrent(ctx, source); err != nil {
		return err
	}
	return cl.Patch(ctx, member, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}
