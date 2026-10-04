package placement

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/plan"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

type identifiedClusterClients interface {
	ClientForUID(string, types.UID) (workloadcluster.SelectivelyCachingClient, bool)
}

// plannedClient binds both the registration and transport to the accepted UID.
// A connection selected by name alone cannot authorize an allocation mutation.
func (r *Reconciler) plannedClient(ctx context.Context, name string, uid types.UID) (client.WithWatch, error) {
	direct, _, err := r.plannedTransport(ctx, name, uid)
	return direct, err
}

func (r *Reconciler) plannedTransport(ctx context.Context, name string, uid types.UID) (client.WithWatch, workloadcluster.SelectivelyCachingClient, error) {
	if r.APIReader == nil || uid == "" {
		return nil, nil, fmt.Errorf("planned placement requires a direct reader and cluster UID")
	}
	registration := &v1beta1.WorkloadCluster{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: name}, registration); err != nil {
		return nil, nil, err
	}
	if registration.UID != uid {
		return nil, nil, fmt.Errorf("cluster %q registration identity changed", name)
	}
	identified, ok := r.Clusters.(identifiedClusterClients)
	if !ok {
		return nil, nil, fmt.Errorf("placement transport cannot verify cluster identity")
	}
	remote, ok := identified.ClientForUID(name, uid)
	if !ok {
		return nil, nil, fmt.Errorf("cluster %q has no connection for its accepted UID", name)
	}
	direct, ok := workloadcluster.DirectClient(remote)
	if !ok {
		return nil, nil, fmt.Errorf("cluster %q transport cannot provide direct observations", name)
	}
	return direct, remote, nil
}

func (r *Reconciler) checkPlanCurrent(ctx context.Context, source *v1beta1.InferenceService) error {
	if r.APIReader == nil || source == nil || source.Spec.Placement == nil || source.Status.Placement == nil || source.Status.Placement.Plan == nil {
		return fmt.Errorf("member mutation requires persisted placement authority")
	}
	accepted := source.Status.Placement.Plan
	if source.UID == "" || accepted.SourceUID != source.UID || accepted.ObservedGeneration != source.Generation || accepted.ID == "" || accepted.Revision <= 0 {
		return fmt.Errorf("member mutation requires current source identity and plan generation")
	}
	live := &v1beta1.InferenceService{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(source), live); err != nil {
		return err
	}
	if !plan.SameSnapshot(source, live) {
		return plan.ErrStaleSnapshot
	}
	if !plan.SameAllocation(source, live) {
		return plan.ErrSchemaUnsupported
	}
	return nil
}

func executionPolicy(source *v1beta1.InferenceService, assignment *v1beta1.CandidateAllocationStatus) *v1beta1.PlacementExecutionPolicy {
	accepted := source.Status.Placement.Plan
	policy := &v1beta1.PlacementExecutionPolicy{
		PlanID: accepted.ID, Revision: accepted.Revision, SourceUID: source.UID,
		ClusterUID: assignment.ClusterUID, PauseSurge: accepted.PauseSurge,
	}
	if assignment.Capacity != nil {
		policy.Demand = assignment.Capacity.DemandContract.DeepCopy()
	}
	if assignment.CurrentHome != nil {
		policy.ReplicaFloors = assignment.CurrentHome.DeepCopy().ReplicaFloors
	}
	return policy
}

func checkPlannedAssignment(source *v1beta1.InferenceService, candidate v1beta1.CandidatePlacement) error {
	found := false
	for _, accepted := range source.Status.Placement.Candidates {
		if accepted.Cluster != candidate.Cluster {
			continue
		}
		if found || candidate.Allocation == nil || !equality.Semantic.DeepEqual(accepted.Allocation, candidate.Allocation) {
			return fmt.Errorf("member assignment does not match persisted allocation")
		}
		if accepted.Allocation.Capacity != nil {
			if _, err := plan.CanonicalCapacity(accepted.Allocation.Capacity); err != nil {
				return err
			}
		} else if placementMode(source) == v1beta1.PlacementModeSplitByCapacity && accepted.Allocation.DesiredReplicas > 0 {
			return fmt.Errorf("positive capacity allocation requires persisted demand authority")
		}
		found = true
	}
	if !found {
		return fmt.Errorf("member has no persisted allocation")
	}
	return nil
}

// placePlannedOn stamps member execution authority before any component can
// acknowledge it. The member's later IR observation is the application signal.
func (r *Reconciler) placePlannedOn(ctx context.Context, source *v1beta1.InferenceService, candidate v1beta1.CandidatePlacement) error {
	return r.placePlannedMember(ctx, source, candidate, false)
}

func (r *Reconciler) placePlannedMember(ctx context.Context, source *v1beta1.InferenceService, candidate v1beta1.CandidatePlacement, existingOnly bool) error {
	if err := r.checkPlanCurrent(ctx, source); err != nil {
		return err
	}
	if err := checkPlannedAssignment(source, candidate); err != nil {
		return err
	}
	assignment := candidate.Allocation
	if assignment == nil || assignment.CurrentReplicas < 0 || (assignment.CurrentReplicas == 0 && (assignment.CurrentHome == nil || assignment.DrainRequested)) {
		return fmt.Errorf("member creation requires a positive allocation or an active full home")
	}
	cl, transport, err := r.plannedTransport(ctx, candidate.Cluster, assignment.ClusterUID)
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, backendTargetKey{}, backendTarget{cluster: candidate.Cluster, uid: assignment.ClusterUID, transport: transport})
	if assignment.Capacity != nil && assignment.DesiredReplicas == 0 {
		return r.retireCapacityFloor(ctx, cl, source, assignment)
	}
	desired, err := r.derivedFor(source)
	if err != nil {
		return err
	}
	var ceiling int32
	if source.Spec.Placement.Split != nil {
		ceiling = source.Spec.Placement.Split.MaxReplicasPerCluster
	}
	if assignment.CurrentHome == nil {
		setPlannedReplicas(desired, assignment.CurrentReplicas, ceiling)
	} else if assignment.CurrentHome.InputDigest != source.Status.Placement.Plan.InputDigest || !equality.Semantic.DeepEqual(assignment.CurrentHome, assignment.DesiredHome) {
		return r.syncPlannedPolicy(ctx, source, candidate)
	}
	if err := r.checkCapacityApplication(ctx, cl, source, desired, assignment); err != nil {
		return err
	}
	policy := executionPolicy(source, assignment)
	raw, err := protocol.Encode(policy)
	if err != nil {
		return err
	}
	if desired.Annotations == nil {
		desired.Annotations = map[string]string{}
	}
	desired.Annotations[constants.PlacementExecution] = raw
	if err := r.checkPlanCurrent(ctx, source); err != nil {
		return err
	}
	return r.applyDerived(ctx, candidate.Cluster, cl, source, desired, existingOnly)
}

// setPlannedReplicas fixes member bounds at the allocation unless an explicit
// local ceiling permits autoscaling. A retained floor can exceed a reduced cap
// until its replacement serves.
func setPlannedReplicas(member *v1beta1.InferenceService, replicas, ceiling int32) {
	apply := func(component *v1beta1.ComponentExtensionSpec) {
		floor := int(replicas)
		component.MinReplicas = &floor
		if ceiling > 0 {
			component.MaxReplicas = int(max(ceiling, replicas))
		} else {
			component.MaxReplicas = floor
		}
	}
	if member.Spec.Engine != nil {
		apply(&member.Spec.Engine.ComponentExtensionSpec)
	}
	if member.Spec.Decoder != nil {
		apply(&member.Spec.Decoder.ComponentExtensionSpec)
	}
}

// syncPlannedPolicy updates coordination authority without a source refresh.
// Paused full-policy homes retain their accepted floors even when member
// runtime or operator defaults change during the transition.
func (r *Reconciler) syncPlannedPolicy(ctx context.Context, source *v1beta1.InferenceService, candidate v1beta1.CandidatePlacement) error {
	if err := r.checkPlanCurrent(ctx, source); err != nil {
		return err
	}
	if err := checkPlannedAssignment(source, candidate); err != nil {
		return err
	}
	policy := executionPolicy(source, candidate.Allocation)
	cl, err := r.plannedClient(ctx, candidate.Cluster, candidate.Allocation.ClusterUID)
	if err != nil {
		return err
	}
	member := &v1beta1.InferenceService{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(source), member); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !isOurDerived(member, source) || member.UID == "" || !member.DeletionTimestamp.IsZero() {
		return fmt.Errorf("member identity cannot authorize a surge pause")
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
	base := member.DeepCopy()
	if policy.PauseSurge && candidate.Allocation.CurrentHome != nil {
		for _, floor := range candidate.Allocation.CurrentHome.ReplicaFloors {
			var component *v1beta1.ComponentExtensionSpec
			switch floor.Component {
			case v1beta1.EngineComponent:
				if member.Spec.Engine != nil {
					component = &member.Spec.Engine.ComponentExtensionSpec
				}
			case v1beta1.DecoderComponent:
				if member.Spec.Decoder != nil {
					component = &member.Spec.Decoder.ComponentExtensionSpec
				}
			case v1beta1.RouterComponent:
				if member.Spec.Router != nil {
					component = &member.Spec.Router.ComponentExtensionSpec
				}
			}
			if component == nil {
				return fmt.Errorf("standing home is missing its accepted %s component", floor.Component)
			}
			value := int(floor.Replicas)
			component.MinReplicas = &value
		}
	}
	if member.Annotations == nil {
		member.Annotations = map[string]string{}
	}
	member.Annotations[constants.PlacementExecution] = raw
	if equality.Semantic.DeepEqual(base.Spec, member.Spec) && base.Annotations[constants.PlacementExecution] == raw {
		return nil
	}
	if err := r.checkPlanCurrent(ctx, source); err != nil {
		return err
	}
	return cl.Patch(ctx, member, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}

// deletePlannedOn requires independently verified drain and rechecks member
// authority before an identity-and-version conditional delete.
func (r *Reconciler) deletePlannedOn(ctx context.Context, source *v1beta1.InferenceService, candidate v1beta1.CandidatePlacement, drained bool) error {
	if err := r.checkPlanCurrent(ctx, source); err != nil {
		return err
	}
	if err := checkPlannedAssignment(source, candidate); err != nil {
		return err
	}
	assignment := candidate.Allocation
	if assignment == nil || assignment.CurrentReplicas != 0 || !assignment.DrainRequested || !drained {
		return fmt.Errorf("member removal requires a drained zero allocation")
	}
	cl, err := r.plannedClient(ctx, candidate.Cluster, assignment.ClusterUID)
	if err != nil {
		return err
	}
	member := &v1beta1.InferenceService{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(source), member); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !isOurDerived(member, source) || member.UID == "" || member.ResourceVersion == "" {
		return fmt.Errorf("member identity cannot authorize removal")
	}
	policy, err := protocol.FromDerived(member)
	if err != nil {
		return err
	}
	// The zero-floor plan can follow the member's last positive-floor policy.
	// A higher revision on the member means another plan already owns it.
	if err := protocol.Authorize(policy, executionPolicy(source, assignment)); err != nil {
		return err
	}
	if err := r.checkPlanCurrent(ctx, source); err != nil {
		return err
	}
	err = cl.Delete(ctx, member, client.Preconditions{UID: &member.UID, ResourceVersion: &member.ResourceVersion})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
