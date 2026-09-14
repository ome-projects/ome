package snapshot

import (
	"math"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

const (
	maxDenseInstanceStatuses      = 10_000
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
	pods []PodInfo,
	isvcs []v1beta1.InferenceService,
	irIndex inferenceReplicaIndex,
) (map[types.NamespacedName]map[v1beta1.ComponentType][]PodInfo,
	map[types.NamespacedName][]inferenceReplicaKey) {
	type workloadLayout struct {
		modes            map[v1beta1.ComponentType]constants.DeploymentModeType
		nativeComponents []v1beta1.ComponentType
	}

	layouts := make(map[types.NamespacedName]workloadLayout, len(isvcs))
	for i := range isvcs {
		isvc := &isvcs[i]
		key := types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}
		layout := workloadLayout{modes: make(map[v1beta1.ComponentType]constants.DeploymentModeType)}
		for _, spec := range workloadComponentSpecs(isvc) {
			layout.modes[spec.ctype] = spec.mode
			if spec.present && spec.mode == constants.OMENative {
				layout.nativeComponents = append(layout.nativeComponents, spec.ctype)
			}
		}
		sort.Slice(layout.nativeComponents, func(i, j int) bool {
			return layout.nativeComponents[i] < layout.nativeComponents[j]
		})
		layouts[key] = layout
	}

	routed := make(map[types.NamespacedName]map[v1beta1.ComponentType][]PodInfo)
	ownerResolvedTargets := make(map[types.NamespacedName][]inferenceReplicaKey)
	appendPod := func(key types.NamespacedName, component v1beta1.ComponentType, pod PodInfo) {
		if routed[key] == nil {
			routed[key] = make(map[v1beta1.ComponentType][]PodInfo)
		}
		routed[key][component] = append(routed[key][component], pod)
	}
	sortedTargets := func(targetSet map[inferenceReplicaKey]struct{}) []inferenceReplicaKey {
		targets := make([]inferenceReplicaKey, 0, len(targetSet))
		for target := range targetSet {
			targets = append(targets, target)
		}
		sort.Slice(targets, func(i, j int) bool {
			if targets[i].namespace != targets[j].namespace {
				return targets[i].namespace < targets[j].namespace
			}
			if targets[i].parent != targets[j].parent {
				return targets[i].parent < targets[j].parent
			}
			return targets[i].component < targets[j].component
		})
		return targets
	}

	sortedPods := append([]PodInfo(nil), pods...)
	sort.Slice(sortedPods, func(i, j int) bool {
		if sortedPods[i].Namespace != sortedPods[j].Namespace {
			return sortedPods[i].Namespace < sortedPods[j].Namespace
		}
		return sortedPods[i].Name < sortedPods[j].Name
	})
	for _, pod := range sortedPods {
		ownerRoutingTargets := make(map[inferenceReplicaKey]struct{})
		ownerOccupancyTargets := make(map[inferenceReplicaKey]struct{})
		ownerMatched := false
		if pod.ControllerOwnerPresent && pod.ControllerOwnerValid {
			ownerMatches := irIndex.byOwnerUID[inferenceReplicaOwnerKey{
				namespace: pod.Namespace,
				uid:       pod.ControllerOwnerUID,
			}]
			for _, owner := range ownerMatches {
				if !controllerOwnerMatchesInferenceReplica(pod, owner) {
					continue
				}
				ownerMatched = true
				target := inferenceReplicaKey{
					namespace: owner.Namespace,
					parent:    owner.Spec.ParentRef.Name,
					component: owner.Spec.Component,
				}
				// Even a stale or malformed IR remains positive OME ownership
				// evidence. Its nonempty parent is the narrowest truthful workload
				// identity available to keep node remediation fail closed.
				if target.parent != "" {
					ownerOccupancyTargets[target] = struct{}{}
				}
				layout, ok := layouts[types.NamespacedName{Namespace: target.namespace, Name: target.parent}]
				if ok && layout.modes[target.component] == constants.OMENative &&
					containsComponent(layout.nativeComponents, target.component) {
					ownerRoutingTargets[target] = struct{}{}
				}
			}
		}
		if len(ownerOccupancyTargets) > 0 {
			ownerResolvedTargets[types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}] =
				sortedTargets(ownerOccupancyTargets)
		}
		if len(ownerRoutingTargets) > 0 {
			for _, target := range sortedTargets(ownerRoutingTargets) {
				appendPod(types.NamespacedName{Namespace: target.namespace, Name: target.parent}, target.component, pod)
			}
			continue
		}

		layout, declaredWorkload := layouts[pod.ISVC]
		if !declaredWorkload {
			continue
		}
		if _, declaredComponent := layout.modes[pod.Component]; declaredComponent && pod.Component != "" {
			appendPod(pod.ISVC, pod.Component, pod)
			continue
		}
		if !ownerMatched && pod.ManagedBy == query.ManagedByOMENative {
			for _, component := range layout.nativeComponents {
				appendPod(pod.ISVC, component, pod)
			}
		}
	}
	return routed, ownerResolvedTargets
}

// projectOwnerResolvedNodeOccupancy replaces a physical GPU Pod's raw node
// classification with every OMENative workload/component target proven by
// its controller owner. Workload validation continues to consume the raw
// routed PodInfo above, so malformed label evidence is not repaired here.
//
// A Pod may project to multiple logical blockers when its owner UID is
// ambiguous. AllocatedGPUs is deliberately not recomputed from OMEPods: the
// physical Pod was accounted exactly once during ingestion.
func projectOwnerResolvedNodeOccupancy(
	nodes map[string]*Node,
	ownerResolvedTargets map[types.NamespacedName][]inferenceReplicaKey,
) {
	for _, node := range nodes {
		if node == nil {
			continue
		}
		omePods := make([]PodInfo, 0, len(node.OMEPods))
		otherOccupants := make([]PodInfo, 0, len(node.OtherOccupants))
		project := func(pod PodInfo, alreadyOME bool) {
			targets := ownerResolvedTargets[types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}]
			if len(targets) == 0 {
				if alreadyOME {
					omePods = append(omePods, pod)
				} else {
					otherOccupants = append(otherOccupants, pod)
				}
				return
			}
			for _, target := range targets {
				canonical := pod
				canonical.ISVC = types.NamespacedName{Namespace: target.namespace, Name: target.parent}
				canonical.Component = target.component
				omePods = append(omePods, canonical)
			}
		}
		for _, pod := range node.OMEPods {
			project(pod, true)
		}
		for _, pod := range node.OtherOccupants {
			project(pod, false)
		}
		node.OMEPods = omePods
		node.OtherOccupants = otherOccupants
	}
}

func containsComponent(components []v1beta1.ComponentType, target v1beta1.ComponentType) bool {
	for _, component := range components {
		if component == target {
			return true
		}
	}
	return false
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
	rows, ok := validatedDenseInstanceStatuses(ir.Status.InstanceStatuses)
	if !ok {
		invalidateComponent(component, observationReasonIRStatus)
		return component
	}

	instancesByIndex := make(map[int32]*Instance, len(rows))
	for i := range rows {
		row := &rows[i]
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
		instancesByIndex[row.Index] = instance
	}

	seen := make(map[int32]map[podMemberKey]struct{}, len(instancesByIndex))
	for _, pod := range pods {
		identityValid := validPodIdentity(pod, ir, isvc, componentType)
		if !identityValid && (!pod.InstanceIndexPresent || !pod.InstanceIndexValid) {
			invalidateComponent(component, observationReasonPodIdentity)
			continue
		}
		instance, ok := instancesByIndex[pod.InstanceIndex]
		if !identityValid {
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

	for i := range rows {
		row := &rows[i]
		instance := instancesByIndex[row.Index]
		steady := instance.Phase == v1beta1.OMENativeInstanceReady && instance.Operation == nil
		if steady && !steadyMembershipMatches(instance, layout, seen[instance.Index]) {
			invalidateInstance(component, instance, observationReasonPodJoin)
		}
		if steady && (instance.StatusPods != layout.desired || instance.ObservedPods != layout.desired ||
			instance.ReadyPods != layout.desired || row.ServingPodCount != layout.desired ||
			row.AvailablePodCount < 0 || row.AvailablePodCount > layout.desired) {
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

func validatedDenseInstanceStatuses(source []v1beta1.OMENativeInstanceStatus) ([]v1beta1.OMENativeInstanceStatus, bool) {
	if len(source) > maxDenseInstanceStatuses {
		return nil, false
	}
	seen := make(map[int32]struct{}, len(source))
	for i := range source {
		row := &source[i]
		if row.Index < 0 || !validOMENativeInstancePhase(row.Phase) || row.Incarnation < 0 ||
			row.PodCount < 0 || row.ServingPodCount < 0 || row.AvailablePodCount < 0 ||
			(row.ActiveOrdinal != 0 && row.ActiveOrdinal != 1) {
			return nil, false
		}
		if _, duplicate := seen[row.Index]; duplicate {
			return nil, false
		}
		seen[row.Index] = struct{}{}
	}
	return source, true
}

func validOMENativeInstancePhase(phase v1beta1.OMENativeInstancePhase) bool {
	switch phase {
	case v1beta1.OMENativeInstancePending,
		v1beta1.OMENativeInstanceCreating,
		v1beta1.OMENativeInstanceReady,
		v1beta1.OMENativeInstanceUpdating,
		v1beta1.OMENativeInstanceRestarting,
		v1beta1.OMENativeInstanceMigrating,
		v1beta1.OMENativeInstanceFailed,
		v1beta1.OMENativeInstanceDeleting:
		return true
	default:
		return false
	}
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

func validPodIdentity(
	pod PodInfo,
	ir *v1beta1.InferenceReplica,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
) bool {
	return pod.ManagedBy == query.ManagedByOMENative &&
		pod.ISVC == (types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}) &&
		pod.Component == component &&
		pod.InstanceIndexPresent && pod.InstanceIndexValid &&
		pod.IncarnationPresent && pod.IncarnationValid &&
		pod.RunnerPresent && pod.RunnerValid &&
		pod.PodOrdinalPresent && pod.PodOrdinalValid &&
		controllerOwnerMatchesInferenceReplica(pod, ir)
}

func controllerOwnerMatchesInferenceReplica(pod PodInfo, ir *v1beta1.InferenceReplica) bool {
	return ir != nil && pod.Namespace == ir.Namespace &&
		pod.ControllerOwnerPresent && pod.ControllerOwnerValid &&
		pod.ControllerOwnerAPIVersion == v1beta1.SchemeGroupVersion.String() &&
		pod.ControllerOwnerKind == "InferenceReplica" &&
		pod.ControllerOwnerName == ir.Name && pod.ControllerOwnerUID == ir.UID
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
