package placement

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// retireCapacityFloor preserves an outgoing member's rendering while reducing
// its floor. Hardware weights authorize new allocation; they cannot prevent
// cleanup of an already accepted, observed reduction.
func (r *Reconciler) retireCapacityFloor(ctx context.Context, cl client.Client, source *v1beta1.InferenceService, candidate v1beta1.CandidatePlacement) error {
	assignment := candidate.Allocation
	member, err := retiringMember(ctx, cl, source, assignment)
	if err != nil {
		return err
	}
	raw, err := retirementAuthority(member, executionPolicy(source, assignment))
	if err != nil {
		return err
	}
	base := member.DeepCopy()
	bounds, err := splitPlannedBounds(source, source.Status.Placement.Candidates, candidate, memberMinimums(member))
	if err != nil {
		return err
	}
	setPlannedReplicas(member, bounds)
	if equality.Semantic.DeepEqual(base.Spec, member.Spec) && member.Annotations[constants.PlacementExecution] == raw {
		return nil
	}
	if member.Annotations == nil {
		member.Annotations = map[string]string{}
	}
	member.Annotations[constants.PlacementExecution] = raw
	if err := r.checkPlanCurrent(ctx, source); err != nil {
		return err
	}
	return cl.Patch(ctx, member, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}

// retiringMember reads the member whose capacity minimum retires. It must be
// this source's identified, live derived service, and the allocation may not
// ask for more replicas than the member's standing minimum.
func retiringMember(ctx context.Context, cl client.Client, source *v1beta1.InferenceService, assignment *v1beta1.CandidateAllocationStatus) (*v1beta1.InferenceService, error) {
	member := &v1beta1.InferenceService{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(source), member); err != nil {
		return nil, err
	}
	if !isOurDerived(member, source) || member.UID == "" || !member.DeletionTimestamp.IsZero() {
		return nil, fmt.Errorf("retiring capacity floor requires an identified live derived service")
	}
	floor, err := adoptedMemberFloor(member)
	if err != nil {
		return nil, err
	}
	if assignment.CurrentReplicas > floor {
		return nil, fmt.Errorf("retiring capacity floor cannot grow member replicas")
	}
	return member, nil
}

// retirementAuthority is the encoded execution policy the retirement writes
// on the member, once the member declares the components the policy's demand
// names and the member's current policy admits the policy's revision.
func retirementAuthority(member *v1beta1.InferenceService, policy *v1beta1.PlacementExecutionPolicy) (string, error) {
	if err := protocol.ValidateDemandComponents(member, policy.Demand); err != nil {
		return "", err
	}
	previous, err := protocol.FromDerived(member)
	if err != nil {
		return "", err
	}
	if err := protocol.Authorize(previous, policy); err != nil {
		return "", err
	}
	return protocol.Encode(policy)
}
