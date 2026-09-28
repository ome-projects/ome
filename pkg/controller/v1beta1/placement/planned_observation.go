package placement

import (
	"context"
	"fmt"
	"math"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

type plannedHomeObservation struct {
	Candidate         v1beta1.CandidatePlacement
	Home              allocation.Home
	RolloutReserved   int32
	PauseAcknowledged bool
}

// observePlannedHome reads a direct, identity-checked member inventory. Component
// requirements must include runtime inheritance, even before those IRs exist.
func (r *Reconciler) observePlannedHome(ctx context.Context, source *v1beta1.InferenceService, previous v1beta1.CandidatePlacement, components []v1beta1.ComponentType) (plannedHomeObservation, error) {
	out := plannedHomeObservation{Candidate: retainedUnknownCandidate(source, previous)}
	out.Candidate.ObservationKnown, out.Candidate.AppliedPlanID = false, ""
	if previous.Allocation == nil || source.Status.Placement == nil || source.Status.Placement.Plan == nil || len(components) == 0 {
		return out, fmt.Errorf("planned observation requires allocation authority and resolved components")
	}
	if source.UID == "" || source.Status.Placement.Plan.SourceUID != source.UID {
		return out, fmt.Errorf("planned observation requires verified source identity")
	}
	cl, err := r.plannedClient(ctx, previous.Cluster, previous.Allocation.ClusterUID)
	if err != nil {
		return out, err
	}
	member := &v1beta1.InferenceService{}
	memberExists := true
	if err := cl.Get(ctx, client.ObjectKeyFromObject(source), member); err != nil {
		if !apierrors.IsNotFound(err) {
			return out, err
		}
		memberExists = false
	} else if !isOurDerived(member, source) || member.UID == "" || member.ResourceVersion == "" {
		return out, fmt.Errorf("member service ownership is unverified")
	}
	irs := &v1beta1.InferenceReplicaList{}
	selector := client.MatchingLabels{constants.InferenceServicePodLabelKey: source.Name}
	if err := cl.List(ctx, irs, client.InNamespace(source.Namespace), selector); err != nil {
		return out, err
	}
	pods := &corev1.PodList{}
	if err := cl.List(ctx, pods, client.InNamespace(source.Namespace), selector); err != nil {
		return out, err
	}
	byOwner := map[types.UID][]corev1.Pod{}
	for _, pod := range pods.Items {
		owner := metav1.GetControllerOf(&pod)
		if owner == nil {
			return out, fmt.Errorf("member pod %q has no component owner", pod.Name)
		}
		byOwner[owner.UID] = append(byOwner[owner.UID], pod)
	}
	policy := executionPolicy(source, previous.Allocation)
	statuses := map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus{}
	counts := map[v1beta1.ComponentType]memberResourceCount{}
	applied := memberExists && member.DeletionTimestamp.IsZero()
	if memberExists {
		memberPolicy, err := protocol.FromDerived(member)
		if err != nil {
			return out, err
		}
		applied = applied && equality.Semantic.DeepEqual(memberPolicy, policy)
	}
	for i := range irs.Items {
		listed := &irs.Items[i]
		ir := &v1beta1.InferenceReplica{}
		if _, err := irstatus.GetDecoded(ctx, r.instanceStatusReader(cl), client.ObjectKeyFromObject(listed), ir); err != nil {
			return out, err
		}
		if ir.UID != listed.UID || ir.ResourceVersion != listed.ResourceVersion {
			return out, fmt.Errorf("component %q changed during inventory discovery", ir.Name)
		}
		owner := metav1.GetControllerOf(ir)
		if owner == nil || owner.APIVersion != v1beta1.SchemeGroupVersion.String() || owner.Kind != "InferenceService" || owner.Name != source.Name || owner.UID == "" || ir.Spec.ParentRef.Name != source.Name {
			return out, fmt.Errorf("component %q has unverified service ownership", ir.Name)
		}
		if memberExists && owner.UID != member.UID {
			return out, fmt.Errorf("component %q belongs to a different member service", ir.Name)
		}
		if !memberExists {
			p := ir.Spec.PlacementExecution
			if p == nil || p.SourceUID != source.UID || p.ClusterUID != previous.Allocation.ClusterUID {
				return out, fmt.Errorf("orphan component %q has unverified source identity", ir.Name)
			}
		}
		if _, duplicate := statuses[ir.Spec.Component]; duplicate {
			return out, fmt.Errorf("multiple live components claim %q", ir.Spec.Component)
		}
		if ir.Spec.Component != v1beta1.EngineComponent && ir.Spec.Component != v1beta1.DecoderComponent && ir.Spec.Component != v1beta1.RouterComponent {
			return out, fmt.Errorf("component %q has an unsupported role", ir.Name)
		}
		gangSizes, err := memberGangSizes(ctx, cl, ir, byOwner[ir.UID])
		if err != nil {
			return out, err
		}
		count, err := countMemberResources(ir, byOwner[ir.UID], gangSizes)
		if err != nil {
			return out, err
		}
		delete(byOwner, ir.UID)
		statuses[ir.Spec.Component], counts[ir.Spec.Component] = &ir.Status, count
		if ir.Spec.Replicas == nil || *ir.Spec.Replicas < 0 {
			return out, fmt.Errorf("component %q has unresolved desired replicas", ir.Name)
		}
		// Whole replica units contain Engine and Decoder. Router stays shared
		// within each home, with its own replica policy and readiness gate.
		if ir.Spec.Component != v1beta1.RouterComponent {
			// A paused limit is durable capacity authority, even if the current
			// autoscaler request is smaller. Requests above it remain deferred.
			committed := *ir.Spec.Replicas
			if ir.Spec.PlacementExecution != nil && ir.Spec.PlacementExecution.PauseSurge && ir.Spec.PlacementReplicaLimit != nil {
				committed = *ir.Spec.PlacementReplicaLimit
			}
			out.Home.Occupied = max(out.Home.Occupied, count.Occupied, committed)
			out.RolloutReserved = max(out.RolloutReserved, count.Reserved)
		}
		if policy.PauseSurge {
			applied = applied && ir.Spec.PlacementReplicaLimit != nil && *ir.Spec.PlacementReplicaLimit > 0
			if ir.Spec.Component != v1beta1.RouterComponent {
				applied = applied && ir.Spec.PlacementReplicaLimit != nil && *ir.Spec.PlacementReplicaLimit >= previous.Allocation.CurrentReplicas
			}
		}
		applied = applied && ir.Generation > 0 && ir.Status.ObservedGeneration == ir.Generation && ir.Status.PlacementObservedGeneration == ir.Generation && ir.DeletionTimestamp.IsZero() && equality.Semantic.DeepEqual(ir.Spec.PlacementExecution, policy)
		if memberExists {
			applied = applied && ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] == strconv.FormatInt(member.Generation, 10)
		}
		if ir.Spec.Component != v1beta1.RouterComponent {
			applied = applied && ir.Spec.Replicas != nil && *ir.Spec.Replicas >= previous.Allocation.CurrentReplicas
		}
		// A stable IR version fences both the decoded reservations and the Pod
		// cohort. Creates already authorized by that version remain charged.
		live := &v1beta1.InferenceReplica{}
		if err := cl.Get(ctx, client.ObjectKeyFromObject(ir), live); err != nil {
			return out, err
		}
		if ir.UID == "" || ir.ResourceVersion == "" || live.UID != ir.UID || live.ResourceVersion != ir.ResourceVersion {
			return out, fmt.Errorf("component %q changed during resource observation", ir.Name)
		}
	}
	if len(byOwner) > 0 {
		return out, fmt.Errorf("member pods remain without identified live components")
	}
	if memberExists {
		live := &v1beta1.InferenceService{}
		if err := cl.Get(ctx, client.ObjectKeyFromObject(member), live); err != nil {
			return out, err
		}
		if live.UID != member.UID || live.ResourceVersion != member.ResourceVersion {
			return out, fmt.Errorf("member changed during resource observation")
		}
	}
	scaled := make([]v1beta1.ComponentType, 0, len(components))
	ready := int32(math.MaxInt32)
	allAdmitted, allServing := true, true
	for _, component := range components {
		count, exists := counts[component]
		applied = applied && exists
		allAdmitted = allAdmitted && componentHasAdmittedInstance(statuses[component])
		if component == v1beta1.RouterComponent {
			allServing = allServing && statuses[component] != nil && statuses[component].ReadyReplicas > 0
			if count.Ready == 0 {
				ready = 0
			}
			continue
		}
		if component != v1beta1.EngineComponent && component != v1beta1.DecoderComponent {
			return out, fmt.Errorf("unresolved placement component %q", component)
		}
		scaled = append(scaled, component)
		ready = min(ready, count.Ready)
	}
	if len(scaled) == 0 {
		return out, fmt.Errorf("placement requires an engine or decoder component")
	}
	out.Home.Known = true
	out.Home.Absent = !memberExists && len(irs.Items) == 0 && len(pods.Items) == 0
	out.Home.Applied = applied
	out.Home.Ready = ready
	out.PauseAcknowledged = applied && policy.PauseSurge
	out.Candidate = v1beta1.CandidatePlacement{Cluster: previous.Cluster, Allocation: previous.Allocation.DeepCopy(), ObservationKnown: true, Phase: v1beta1.CandidatePhaseAdmitting}
	if applied {
		out.Candidate.AppliedPlanID = policy.PlanID
	}
	if memberExists && member.DeletionTimestamp.IsZero() {
		if allAdmitted {
			out.Candidate.AdmittedReplicas = placementAdmittedReplicas(scaled, statuses)
		}
		if out.Candidate.AdmittedReplicas > 0 {
			out.Candidate.Phase = v1beta1.CandidatePhaseAdmitted
			out.Candidate.Endpoint = endpointFor(member).DeepCopy()
		}
		if member.Status.IsConditionReady(v1beta1.IngressReady) && allAdmitted && allServing {
			out.Candidate.ReadyReplicas = placementReadyReplicas(scaled, statuses)
		} else {
			out.Home.Ready = 0
		}
	} else {
		out.Home.Ready = 0
	}
	return out, nil
}

// plannedTrafficEvidence accepts only an exact publication of this allocation.
// Missing publication is unknown; it cannot prove that old traffic has drained.
func plannedTrafficEvidence(source *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap, candidate v1beta1.CandidatePlacement) (known, routable, drained bool) {
	if source == nil || source.Status.Placement == nil || source.Status.Placement.Plan == nil || candidate.Allocation == nil || trafficMap == nil || !trafficMap.DeletionTimestamp.IsZero() || !TrafficMapHasExactController(trafficMap, source) {
		return false, false, false
	}
	accepted := source.Status.Placement.Plan
	if accepted.ID == "" || accepted.Revision <= 0 || accepted.SourceUID != source.UID || accepted.ObservedGeneration != source.Generation {
		return false, false, false
	}
	if trafficMap.Status.SourceUID != source.UID || trafficMap.Spec.PlacementPlanID != source.Status.Placement.Plan.ID || trafficMap.Spec.ObservedISVCGeneration != source.Generation || trafficMap.Generation <= 0 || !trafficMap.Status.Published || trafficMap.Status.ObservedTrafficMapGeneration != trafficMap.Generation {
		return false, false, false
	}
	var entry *v1beta1.TrafficMapEntry
	for i := range trafficMap.Spec.Entries {
		if trafficMap.Spec.Entries[i].Cluster == candidate.Cluster {
			if entry != nil {
				return false, false, false
			}
			entry = &trafficMap.Spec.Entries[i]
		}
	}
	if entry == nil || entry.Weight == 0 {
		return true, false, candidate.Allocation.DrainRequested
	}
	if entry.Weight < 0 || entry.Endpoint == nil || candidate.Endpoint == nil || entry.Endpoint.String() != candidate.Endpoint.String() {
		return false, false, false
	}
	return true, true, false
}
