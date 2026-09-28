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
	if r.APIReader == nil || uid == "" {
		return nil, fmt.Errorf("planned placement requires a direct reader and cluster UID")
	}
	registration := &v1beta1.WorkloadCluster{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: name}, registration); err != nil {
		return nil, err
	}
	if registration.UID != uid {
		return nil, fmt.Errorf("cluster %q registration identity changed", name)
	}
	identified, ok := r.Clusters.(identifiedClusterClients)
	if !ok {
		return nil, fmt.Errorf("placement transport cannot verify cluster identity")
	}
	remote, ok := identified.ClientForUID(name, uid)
	if !ok {
		return nil, fmt.Errorf("cluster %q has no connection for its accepted UID", name)
	}
	direct, ok := workloadcluster.DirectClient(remote)
	if !ok {
		return nil, fmt.Errorf("cluster %q transport cannot provide direct observations", name)
	}
	return direct, nil
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
	return nil
}

func executionPolicy(source *v1beta1.InferenceService, assignment *v1beta1.CandidateAllocationStatus) *v1beta1.PlacementExecutionPolicy {
	accepted := source.Status.Placement.Plan
	return &v1beta1.PlacementExecutionPolicy{
		PlanID: accepted.ID, Revision: accepted.Revision, SourceUID: source.UID,
		ClusterUID: assignment.ClusterUID, PauseSurge: accepted.PauseSurge,
	}
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
	if err := r.checkPlanCurrent(ctx, source); err != nil {
		return err
	}
	if err := checkPlannedAssignment(source, candidate); err != nil {
		return err
	}
	assignment := candidate.Allocation
	if assignment == nil || assignment.CurrentReplicas <= 0 {
		return fmt.Errorf("positive planned assignment is required for member creation")
	}
	cl, err := r.plannedClient(ctx, candidate.Cluster, assignment.ClusterUID)
	if err != nil {
		return err
	}
	desired, err := r.derivedFor(source)
	if err != nil {
		return err
	}
	var ceiling int32
	if source.Spec.Placement.Split != nil {
		ceiling = source.Spec.Placement.Split.MaxReplicasPerCluster
	}
	setDerivedReplicas(desired, assignment.CurrentReplicas, ceiling)
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
	return r.applyDerived(ctx, candidate.Cluster, cl, source, desired)
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
