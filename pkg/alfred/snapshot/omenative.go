package snapshot

import (
	"math"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/instancestatus"
)

const (
	observationReasonIRList       = "inference replica list unavailable"
	observationReasonIRMissing    = "inference replica is missing"
	observationReasonIRDuplicate  = "inference replica is duplicated"
	observationReasonIROwner      = "inference replica owner is invalid"
	observationReasonIRStale      = "inference replica status is stale"
	observationReasonIRStatus     = "inference replica status is invalid"
	observationReasonRunnerLayout = "inference replica runner layout is invalid"
	observationReasonPodIdentity  = "OMENative pod identity is invalid"
	observationReasonPodJoin      = "OMENative pod membership disagrees with status"
	observationReasonPodCounts    = "OMENative pod counts disagree with status"
)

type inferenceReplicaKey struct {
	namespace string
	parent    string
	component v1beta1.ComponentType
}

type inferenceReplicaOwnerKey struct {
	namespace string
	uid       types.UID
}

type inferenceReplicaIndex struct {
	byComponent map[inferenceReplicaKey][]*v1beta1.InferenceReplica
	byOwnerUID  map[inferenceReplicaOwnerKey][]*v1beta1.InferenceReplica
}

func indexInferenceReplicas(items []v1beta1.InferenceReplica) inferenceReplicaIndex {
	index := inferenceReplicaIndex{
		byComponent: make(map[inferenceReplicaKey][]*v1beta1.InferenceReplica),
		byOwnerUID:  make(map[inferenceReplicaOwnerKey][]*v1beta1.InferenceReplica),
	}
	for i := range items {
		ir := &items[i]
		key := inferenceReplicaKey{
			namespace: ir.Namespace,
			parent:    ir.Spec.ParentRef.Name,
			component: ir.Spec.Component,
		}
		index.byComponent[key] = append(index.byComponent[key], ir)
		if ir.UID != "" {
			ownerKey := inferenceReplicaOwnerKey{namespace: ir.Namespace, uid: ir.UID}
			index.byOwnerUID[ownerKey] = append(index.byOwnerUID[ownerKey], ir)
		}
	}
	return index
}

func routeWorkloadPods(
	isvc *v1beta1.InferenceService,
	pods map[v1beta1.ComponentType][]PodInfo,
	modes map[v1beta1.ComponentType]constants.DeploymentModeType,
	irIndex inferenceReplicaIndex,
) map[v1beta1.ComponentType][]PodInfo {
	routed := make(map[v1beta1.ComponentType][]PodInfo, len(pods))
	omeComponents := make([]v1beta1.ComponentType, 0, len(modes))
	for component, mode := range modes {
		if mode == constants.OMENative {
			omeComponents = append(omeComponents, component)
		}
	}
	sort.Slice(omeComponents, func(i, j int) bool { return omeComponents[i] < omeComponents[j] })

	declaredComponents := make([]v1beta1.ComponentType, 0, len(pods))
	for component := range pods {
		declaredComponents = append(declaredComponents, component)
	}
	sort.Slice(declaredComponents, func(i, j int) bool { return declaredComponents[i] < declaredComponents[j] })

	for _, declared := range declaredComponents {
		for _, pod := range pods[declared] {
			ownerMatches := irIndex.byOwnerUID[inferenceReplicaOwnerKey{
				namespace: pod.Namespace,
				uid:       pod.ControllerOwnerUID,
			}]
			if pod.ControllerOwnerPresent && pod.ControllerOwnerValid && len(ownerMatches) == 1 {
				owner := ownerMatches[0]
				expected := owner.Spec.Component
				if owner.Spec.ParentRef.Name == isvc.Name && modes[expected] == constants.OMENative {
					routed[expected] = append(routed[expected], pod)
					continue
				}
			}

			if len(ownerMatches) > 0 {
				for _, component := range omeComponents {
					routed[component] = append(routed[component], pod)
				}
				continue
			}

			if pod.ManagedBy == query.ManagedByOMENative {
				for _, component := range omeComponents {
					routed[component] = append(routed[component], pod)
				}
				continue
			}

			if declared != "" {
				routed[declared] = append(routed[declared], pod)
			}
		}
	}
	return routed
}

type runnerLayout struct {
	singlePod bool
	desired   int32
	workers   int32
}

func buildOMENativeComponent(
	isvc *v1beta1.InferenceService,
	componentType v1beta1.ComponentType,
	pods []PodInfo,
	irIndex inferenceReplicaIndex,
	irListErr error,
) *Component {
	component := &Component{Type: componentType, DeploymentMode: constants.OMENative}
	if irListErr != nil {
		invalidateComponent(component, observationReasonIRList)
		return component
	}

	key := inferenceReplicaKey{namespace: isvc.Namespace, parent: isvc.Name, component: componentType}
	matches := irIndex.byComponent[key]
	if len(matches) == 0 {
		invalidateComponent(component, observationReasonIRMissing)
		return component
	}
	if len(matches) != 1 {
		invalidateComponent(component, observationReasonIRDuplicate)
		return component
	}
	ir := matches[0]
	if !validInferenceReplicaOwner(ir.OwnerReferences, isvc) {
		invalidateComponent(component, observationReasonIROwner)
		return component
	}
	if ir.Status.ObservedGeneration != ir.Generation {
		invalidateComponent(component, observationReasonIRStale)
		return component
	}
	component.IR = ir
	component.StatusFresh = true

	layout, ok := validateRunnerLayout(ir.Spec.Runners)
	if !ok {
		invalidateComponent(component, observationReasonRunnerLayout)
		return component
	}
	normalized, err := instancestatus.Normalize(&ir.Status)
	if err != nil {
		invalidateComponent(component, observationReasonIRStatus)
		return component
	}

	rows := make(map[int32]*Instance, len(normalized.Rows))
	for i := range normalized.Rows {
		row := &normalized.Rows[i]
		instance := &Instance{
			Index:            row.Index,
			Incarnation:      row.Incarnation,
			Phase:            row.Phase,
			RunningRevision:  row.RunningRevision,
			TargetRevision:   row.TargetRevision,
			Admitted:         row.Admitted,
			ActiveOrdinal:    row.ActiveOrdinal,
			ServingPods:      row.ServingPodCount,
			AvailablePods:    row.AvailablePodCount,
			DesiredPods:      layout.desired,
			StatusPods:       row.PodCount,
			ObservationValid: true,
			NodesSet:         map[string]int{},
		}
		if row.Operation != nil {
			instance.Operation = row.Operation.DeepCopy()
		}
		component.Instances = append(component.Instances, instance)
		rows[row.Index] = instance
	}

	seen := make(map[int32]map[podMemberKey]struct{}, len(rows))
	for _, pod := range pods {
		instance, ok := rows[pod.InstanceIndex]
		if !validPodIdentity(pod, ir.UID, componentType) {
			if ok {
				invalidateInstance(component, instance, observationReasonPodIdentity)
			} else {
				invalidateComponent(component, observationReasonPodIdentity)
			}
			continue
		}
		if !ok {
			invalidateComponent(component, observationReasonPodIdentity)
			continue
		}
		steady := instance.Phase == v1beta1.OMENativeInstanceReady && instance.Operation == nil
		if steady && pod.Terminating {
			invalidateInstance(component, instance, observationReasonPodJoin)
		}
		if !podIdentityAllowedForLayout(pod, layout, instance.ActiveOrdinal, steady) {
			invalidateInstance(component, instance, observationReasonPodJoin)
			continue
		}
		if pod.Incarnation != instance.Incarnation {
			if steady || !pod.Terminating {
				invalidateInstance(component, instance, observationReasonPodJoin)
			}
		}
		member := podMemberKey{incarnation: pod.Incarnation, runner: pod.Runner, ordinal: pod.PodOrdinal}
		if seen[instance.Index] == nil {
			seen[instance.Index] = make(map[podMemberKey]struct{})
		}
		if _, duplicate := seen[instance.Index][member]; duplicate {
			invalidateInstance(component, instance, observationReasonPodJoin)
		} else {
			seen[instance.Index][member] = struct{}{}
		}
		addPodToInstance(instance, pod)
	}

	for i := range normalized.Rows {
		row := &normalized.Rows[i]
		instance := rows[row.Index]
		steady := instance.Phase == v1beta1.OMENativeInstanceReady && instance.Operation == nil
		if steady && !steadyMembershipMatches(instance, layout, seen[instance.Index]) {
			invalidateInstance(component, instance, observationReasonPodJoin)
		}
		if steady && (instance.StatusPods != layout.desired || instance.ObservedPods != layout.desired ||
			instance.ReadyPods != layout.desired || row.ServingPodCount != layout.desired ||
			row.AvailablePodCount != layout.desired) {
			invalidateInstance(component, instance, observationReasonPodCounts)
		}
		sort.Slice(instance.Pods, func(i, j int) bool {
			if instance.Pods[i].Runner != instance.Pods[j].Runner {
				return instance.Pods[i].Runner < instance.Pods[j].Runner
			}
			if instance.Pods[i].PodOrdinal != instance.Pods[j].PodOrdinal {
				return instance.Pods[i].PodOrdinal < instance.Pods[j].PodOrdinal
			}
			return instance.Pods[i].Name < instance.Pods[j].Name
		})
	}
	if component.ObservationReason == "" {
		component.ObservationValid = true
	}
	return component
}

func validInferenceReplicaOwner(owners []metav1.OwnerReference, isvc *v1beta1.InferenceService) bool {
	var controller *metav1.OwnerReference
	for i := range owners {
		owner := &owners[i]
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		if controller != nil {
			return false
		}
		controller = owner
	}
	if controller == nil || controller.Kind != "InferenceService" || controller.Name != isvc.Name || controller.UID != isvc.UID {
		return false
	}
	return controller.APIVersion == v1beta1.SchemeGroupVersion.String()
}

func validateRunnerLayout(runners []v1beta1.Runner) (runnerLayout, bool) {
	if len(runners) == 1 && runners[0].Name == v1beta1.RunnerNameDefault && runners[0].Size == 1 {
		return runnerLayout{singlePod: true, desired: 1}, true
	}
	if len(runners) != 2 {
		return runnerLayout{}, false
	}
	var leader, worker *v1beta1.Runner
	for i := range runners {
		runner := &runners[i]
		switch runner.Name {
		case v1beta1.RunnerNameLeader:
			if leader != nil {
				return runnerLayout{}, false
			}
			leader = runner
		case v1beta1.RunnerNameWorker:
			if worker != nil {
				return runnerLayout{}, false
			}
			worker = runner
		default:
			return runnerLayout{}, false
		}
	}
	if leader == nil || worker == nil || leader.Size != 1 || worker.Size < 1 || worker.Size == math.MaxInt32 {
		return runnerLayout{}, false
	}
	return runnerLayout{desired: worker.Size + 1, workers: worker.Size}, true
}

func validPodIdentity(pod PodInfo, ownerUID types.UID, component v1beta1.ComponentType) bool {
	return pod.ManagedBy == query.ManagedByOMENative &&
		pod.Component == component &&
		pod.InstanceIndexPresent && pod.InstanceIndexValid &&
		pod.IncarnationPresent && pod.IncarnationValid &&
		pod.RunnerPresent && pod.RunnerValid &&
		pod.PodOrdinalPresent && pod.PodOrdinalValid &&
		pod.ControllerOwnerPresent && pod.ControllerOwnerValid &&
		pod.ControllerOwnerUID == ownerUID
}

type podMemberKey struct {
	incarnation int64
	runner      v1beta1.RunnerName
	ordinal     int32
}

func podIdentityAllowedForLayout(pod PodInfo, layout runnerLayout, activeOrdinal int32, steady bool) bool {
	if layout.singlePod {
		if pod.Runner != v1beta1.RunnerNameDefault || pod.PodOrdinal > 1 {
			return false
		}
		return !steady || pod.PodOrdinal == activeOrdinal
	}
	if activeOrdinal != 0 {
		return false
	}
	switch pod.Runner {
	case v1beta1.RunnerNameLeader:
		return pod.PodOrdinal == 0
	case v1beta1.RunnerNameWorker:
		return pod.PodOrdinal >= 0 && pod.PodOrdinal < layout.workers
	default:
		return false
	}
}

func steadyMembershipMatches(instance *Instance, layout runnerLayout, seen map[podMemberKey]struct{}) bool {
	if instance.Incarnation <= 0 || instance.ActiveOrdinal < 0 || instance.ActiveOrdinal > 1 {
		return false
	}
	if layout.singlePod {
		if currentIncarnationMemberCount(seen, instance.Incarnation) != 1 {
			return false
		}
		_, ok := seen[podMemberKey{incarnation: instance.Incarnation, runner: v1beta1.RunnerNameDefault, ordinal: instance.ActiveOrdinal}]
		return ok
	}
	if instance.ActiveOrdinal != 0 || currentIncarnationMemberCount(seen, instance.Incarnation) != int(layout.desired) {
		return false
	}
	if _, ok := seen[podMemberKey{incarnation: instance.Incarnation, runner: v1beta1.RunnerNameLeader, ordinal: 0}]; !ok {
		return false
	}
	for ordinal := int32(0); ordinal < layout.workers; ordinal++ {
		if _, ok := seen[podMemberKey{incarnation: instance.Incarnation, runner: v1beta1.RunnerNameWorker, ordinal: ordinal}]; !ok {
			return false
		}
	}
	return true
}

func currentIncarnationMemberCount(seen map[podMemberKey]struct{}, incarnation int64) int {
	count := 0
	for member := range seen {
		if member.incarnation == incarnation {
			count++
		}
	}
	return count
}

func addPodToInstance(instance *Instance, pod PodInfo) {
	instance.Pods = append(instance.Pods, pod)
	instance.ObservedPods++
	instance.TotalGPUs += pod.GPUs
	if pod.Node != "" {
		instance.NodesSet[pod.Node]++
	}
	if pod.Ready {
		instance.ReadyPods++
	}
}

func invalidateComponent(component *Component, reason string) {
	component.ObservationValid = false
	if component.ObservationReason == "" {
		component.ObservationReason = reason
	}
}

func invalidateInstance(component *Component, instance *Instance, reason string) {
	instance.ObservationValid = false
	if instance.ObservationReason == "" {
		instance.ObservationReason = reason
	}
	invalidateComponent(component, reason)
}
