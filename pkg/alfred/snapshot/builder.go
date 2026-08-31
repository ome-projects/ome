package snapshot

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

const (
	// caScaleDownDisabledAnnotation is the operator-set cluster-autoscaler
	// annotation excluding a node from scale-down.
	caScaleDownDisabledAnnotation = "cluster-autoscaler.kubernetes.io/scale-down-disabled"
	// caToBeDeletedTaint is the taint the cluster-autoscaler places on a
	// node it is actively deleting.
	caToBeDeletedTaint = "ToBeDeletedByClusterAutoscaler"

	// defaultWorkloadPriority is the alfred.ome.io/priority value assumed
	// when the annotation is absent or unparsable.
	defaultWorkloadPriority = 0.5
)

// DefaultTriggerConditions is the node-condition set that marks a node
// Unhealthy when no configuration overrides it.
var DefaultTriggerConditions = []string{"GpuUnhealthy"}

// Options configures a snapshot build. The zero value is usable: default
// trigger conditions, no preemptible labels, workloads movable by default.
type Options struct {
	// TriggerConditions are the node condition types that mark a node
	// Unhealthy (policies.nodeHealth.triggerConditions).
	TriggerConditions []string
	// PreemptibleLabels are node-label keys whose presence (with any
	// value but "false") marks a node spot/preemptible.
	PreemptibleLabels []string
	// DefaultMovable is the cluster-wide movable default a workload
	// inherits when it carries no alfred.ome.io/movable annotation.
	DefaultMovable *bool
	// OMENativeExecutor is the structured executor capability observation.
	OMENativeExecutor OMENativeExecutorState
	// Now overrides the clock (tests); nil means time.Now.
	Now func() time.Time
}

func (o *Options) triggerConditions() []string {
	if len(o.TriggerConditions) == 0 {
		return DefaultTriggerConditions
	}
	return o.TriggerConditions
}

func (o *Options) defaultMovable() bool {
	if o.DefaultMovable == nil {
		return true
	}
	return *o.DefaultMovable
}

func (o *Options) now() time.Time {
	if o.Now == nil {
		return time.Now()
	}
	return o.Now()
}

// Build assembles a ClusterSnapshot from the cluster through a read-only
// client. It never writes, and it degrades per-object rather than failing
// wholesale wherever one broken object should not blind the caretaker (model
// resolution); only the core lists (nodes, pods, InferenceServices) are
// load-bearing enough to fail the build.
func Build(ctx context.Context, r client.Reader, opts Options) (*ClusterSnapshot, error) {
	s := &ClusterSnapshot{
		Timestamp:         opts.now(),
		Nodes:             map[string]*Node{},
		Workloads:         map[types.NamespacedName]*Workload{},
		Models:            map[ModelKey]*ModelAvailability{},
		OMENativeExecutor: opts.OMENativeExecutor,
	}

	var nodeList corev1.NodeList
	if err := r.List(ctx, &nodeList); err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		s.Nodes[node.Name] = buildNode(node, &opts)
	}

	var podList corev1.PodList
	if err := r.List(ctx, &podList); err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	// isvcPods groups OME pods by owning ISVC and component for workload
	// assembly below; node occupancy is filled in the same pass.
	isvcPods := map[types.NamespacedName]map[v1beta1.ComponentType][]PodInfo{}
	for i := range podList.Items {
		pod := &podList.Items[i]
		ingestPod(s, pod, isvcPods, &opts)
	}
	for _, n := range s.Nodes {
		n.FreeGPUs = n.TotalGPUs - n.AllocatedGPUs
		if n.FreeGPUs < 0 {
			// Overcommit cannot happen for extended resources; clamp
			// defensively so scoring never sees negative free capacity.
			n.FreeGPUs = 0
		}
	}
	sort.Slice(s.PendingPods, func(i, j int) bool {
		if s.PendingPods[i].Namespace != s.PendingPods[j].Namespace {
			return s.PendingPods[i].Namespace < s.PendingPods[j].Namespace
		}
		return s.PendingPods[i].Name < s.PendingPods[j].Name
	})

	var isvcList v1beta1.InferenceServiceList
	if err := r.List(ctx, &isvcList); err != nil {
		return nil, fmt.Errorf("list inferenceservices: %w", err)
	}
	var irList v1beta1.InferenceReplicaList
	irListErr := r.List(ctx, &irList)
	irIndex := indexInferenceReplicas(irList.Items)
	for i := range isvcList.Items {
		isvc := &isvcList.Items[i]
		key := types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}
		s.Workloads[key] = buildWorkload(isvc, isvcPods[key], irIndex, irListErr, &opts)
	}

	resolveModels(ctx, r, s)
	attributePendingPools(s)
	return s, nil
}

func buildNode(node *corev1.Node, opts *Options) *Node {
	resource, total := NodeGPUAllocatable(node)
	n := &Node{
		Name:              node.Name,
		Labels:            node.Labels,
		GPUPool:           GPUPoolForNode(node, resource),
		GPUResource:       resource,
		TotalGPUs:         total,
		Cordoned:          node.Spec.Unschedulable,
		ScaleDownDisabled: node.Annotations[caScaleDownDisabledAnnotation] == "true",
	}
	for _, taint := range node.Spec.Taints {
		if taint.Key == caToBeDeletedTaint {
			n.ScaleDownMarked = true
			break
		}
	}
	for _, key := range opts.PreemptibleLabels {
		if value, ok := node.Labels[key]; ok && value != "false" {
			n.Preemptible = true
			break
		}
	}
	triggers := opts.triggerConditions()
	for _, cond := range node.Status.Conditions {
		if cond.Status != corev1.ConditionTrue {
			continue
		}
		for _, trigger := range triggers {
			if string(cond.Type) == trigger {
				n.Unhealthy = true
				n.UnhealthyConditions = append(n.UnhealthyConditions, trigger)
				break
			}
		}
	}
	sort.Strings(n.UnhealthyConditions)
	return n
}

// ingestPod routes one pod into node occupancy, workload grouping, and
// pending pressure as applicable.
func ingestPod(s *ClusterSnapshot, pod *corev1.Pod, isvcPods map[types.NamespacedName]map[v1beta1.ComponentType][]PodInfo, opts *Options) {
	gpus := PodGPURequest(pod)
	isvcName := pod.Labels[constants.InferenceServicePodLabelKey]
	component := v1beta1.ComponentType(pod.Labels[constants.OMEComponentLabel])

	info := PodInfo{
		Namespace:   pod.Namespace,
		Name:        pod.Name,
		Node:        pod.Spec.NodeName,
		GPUs:        gpus,
		Ready:       podIsReady(pod),
		Terminating: pod.DeletionTimestamp != nil,
		ManagedBy:   pod.Labels[query.LabelManagedBy],
	}
	parseOMENativePodIdentity(&info, pod)
	if pod.Status.StartTime != nil {
		t := pod.Status.StartTime.Time
		info.StartTime = &t
	}
	if isvcName != "" {
		info.ISVC = types.NamespacedName{Namespace: pod.Namespace, Name: isvcName}
		info.Component = component
	}

	// Node occupancy: GPU-holding pods only.
	if gpus > 0 && podHoldsGPUs(pod) {
		if node, ok := s.Nodes[pod.Spec.NodeName]; ok {
			node.AllocatedGPUs += gpus
			if info.Terminating {
				node.TerminatingGPUs += gpus
			}
			if isvcName != "" {
				node.OMEPods = append(node.OMEPods, info)
			} else {
				node.OtherOccupants = append(node.OtherOccupants, info)
			}
		}
	}

	// Workload grouping retains every nonterminal OME pod, including
	// unscheduled pods. Checked OMENative joins must see malformed or pending
	// members rather than silently dropping them; Raw and LWS construction
	// filters back to scheduled pods below.
	if isvcName != "" && component != "" && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
		byComponent, ok := isvcPods[info.ISVC]
		if !ok {
			byComponent = map[v1beta1.ComponentType][]PodInfo{}
			isvcPods[info.ISVC] = byComponent
		}
		byComponent[component] = append(byComponent[component], info)
	}

	// Pending pressure: unscheduled GPU demand.
	if gpus > 0 && pod.Spec.NodeName == "" && pod.Status.Phase == corev1.PodPending && pod.DeletionTimestamp == nil {
		s.PendingPods = append(s.PendingPods, PendingPod{
			Namespace:    pod.Namespace,
			Name:         pod.Name,
			ISVC:         info.ISVC,
			GPUsNeeded:   gpus,
			NodeSelector: pod.Spec.NodeSelector,
			PendingSince: pendingSince(pod),
			Virtual:      false,
		})
	}
}

// pendingSince prefers the PodScheduled=False transition time (when the
// scheduler first failed to place the pod) and falls back to creation time.
func pendingSince(pod *corev1.Pod) time.Time {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse && !cond.LastTransitionTime.IsZero() {
			return cond.LastTransitionTime.Time
		}
	}
	return pod.CreationTimestamp.Time
}

func buildWorkload(
	isvc *v1beta1.InferenceService,
	pods map[v1beta1.ComponentType][]PodInfo,
	irIndex inferenceReplicaIndex,
	irListErr error,
	opts *Options,
) *Workload {
	w := &Workload{
		NamespacedName:      types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name},
		ISVC:                isvc,
		Components:          map[v1beta1.ComponentType]*Component{},
		Movable:             opts.defaultMovable(),
		Priority:            defaultWorkloadPriority,
		MigrationStateValid: true,
	}
	applyWorkloadOverrides(w, isvc.Annotations)

	if isvc.Spec.Model != nil {
		kind := ModelKindClusterBaseModel
		if isvc.Spec.Model.Kind != nil && *isvc.Spec.Model.Kind != "" {
			kind = *isvc.Spec.Model.Kind
		}
		w.ModelKey = ModelKey{Kind: kind, Name: isvc.Spec.Model.Name}
		if kind == ModelKindBaseModel {
			w.ModelKey.Namespace = isvc.Namespace
		}
	}

	engineMode, decoderMode, routerMode, err := isvcutils.DetermineDeploymentModes(
		isvc.Spec.Engine, isvc.Spec.Decoder, isvc.Spec.Router, nil, isvc.Spec.DeploymentMode)
	if err != nil {
		// An ISVC without an engine spec is invalid but may transiently
		// exist; fall back to the default mode so its pods still appear.
		engineMode, decoderMode, routerMode = constants.RawDeployment, constants.RawDeployment, constants.RawDeployment
	}
	specComponents := []struct {
		ctype   v1beta1.ComponentType
		present bool
		mode    constants.DeploymentModeType
	}{
		{v1beta1.EngineComponent, isvc.Spec.Engine != nil, engineMode},
		{v1beta1.DecoderComponent, isvc.Spec.Decoder != nil, decoderMode},
		{v1beta1.RouterComponent, isvc.Spec.Router != nil, routerMode},
	}
	for _, sc := range specComponents {
		if !sc.present && len(pods[sc.ctype]) == 0 {
			continue
		}
		component := &Component{
			Type:             sc.ctype,
			DeploymentMode:   sc.mode,
			ObservationValid: true,
		}
		if sc.mode == constants.OMENative {
			component = buildOMENativeComponent(isvc, sc.ctype, pods[sc.ctype], irIndex, irListErr)
		} else {
			component.Instances = buildInstances(sc.mode, scheduledPods(pods[sc.ctype]))
		}
		w.Components[sc.ctype] = component
	}

	applyMigrationState(w, isvc)
	return w
}

func scheduledPods(pods []PodInfo) []PodInfo {
	result := make([]PodInfo, 0, len(pods))
	for _, pod := range pods {
		if pod.Node != "" {
			result = append(result, pod)
		}
	}
	return result
}

func parseOMENativePodIdentity(info *PodInfo, pod *corev1.Pod) {
	info.InstanceIndex, info.InstanceIndexPresent, info.InstanceIndexValid = parseInt32Identity(pod.Labels, query.LabelInstanceIdx, false)
	info.Incarnation, info.IncarnationPresent, info.IncarnationValid = parseInt64Identity(pod.Labels, query.LabelInstanceIncarnation, true)
	rawRunner, runnerPresent := pod.Labels[query.LabelRunner]
	info.Runner = v1beta1.RunnerName(rawRunner)
	info.RunnerPresent = runnerPresent
	switch info.Runner {
	case v1beta1.RunnerNameDefault, v1beta1.RunnerNameLeader, v1beta1.RunnerNameWorker:
		info.RunnerValid = runnerPresent
	}
	info.PodOrdinal, info.PodOrdinalPresent, info.PodOrdinalValid = parseInt32Identity(pod.Labels, query.LabelPodOrdinal, false)

	for i := range pod.OwnerReferences {
		owner := &pod.OwnerReferences[i]
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		if info.ControllerOwnerPresent {
			info.ControllerOwnerValid = false
			return
		}
		info.ControllerOwnerPresent = true
		info.ControllerOwnerUID = owner.UID
		info.ControllerOwnerValid = owner.UID != ""
	}
}

func parseInt32Identity(labels map[string]string, key string, positive bool) (int32, bool, bool) {
	raw, present := labels[key]
	if !present {
		return 0, false, false
	}
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || value < 0 || (positive && value == 0) {
		return 0, true, false
	}
	return int32(value), true, true
}

func parseInt64Identity(labels map[string]string, key string, positive bool) (int64, bool, bool) {
	raw, present := labels[key]
	if !present {
		return 0, false, false
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 || (positive && value == 0) {
		return 0, true, false
	}
	return value, true, true
}

// buildInstances maps a component's pods onto Instances: one Instance per pod
// for RawDeployment (each replica moves alone), one atomic Instance holding
// every pod for multi-pod modes (the group must move together).
func buildInstances(mode constants.DeploymentModeType, pods []PodInfo) []*Instance {
	if len(pods) == 0 {
		return nil
	}
	sorted := make([]PodInfo, len(pods))
	copy(sorted, pods)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	if mode == constants.RawDeployment {
		instances := make([]*Instance, 0, len(sorted))
		for i, pod := range sorted {
			instances = append(instances, newInstance(int32(i), []PodInfo{pod}))
		}
		return instances
	}
	return []*Instance{newInstance(0, sorted)}
}

func newInstance(index int32, pods []PodInfo) *Instance {
	inst := &Instance{Index: index, Pods: pods, ObservationValid: true, NodesSet: map[string]int{}}
	for _, pod := range pods {
		inst.TotalGPUs += pod.GPUs
		if pod.Node != "" {
			inst.NodesSet[pod.Node]++
		}
		if pod.Ready {
			inst.ReadyPods++
		}
	}
	return inst
}

func applyWorkloadOverrides(w *Workload, annotations map[string]string) {
	if value, ok := annotations[constants.AlfredMovableAnnotationKey]; ok {
		if parsed, err := strconv.ParseBool(value); err == nil {
			w.Movable = parsed
		}
	}
	if value, ok := annotations[constants.AlfredPriorityAnnotationKey]; ok {
		if parsed, err := strconv.ParseFloat(value, 64); err == nil && parsed >= 0 && parsed <= 1 {
			w.Priority = parsed
		}
	}
	if value, ok := annotations[constants.AlfredCooldownMinutesAnnotationKey]; ok {
		if minutes, err := strconv.Atoi(value); err == nil && minutes >= 0 {
			d := time.Duration(minutes) * time.Minute
			w.CooldownOverride = &d
		}
	}
	w.TenantGroup = annotations[constants.AlfredTenantGroupAnnotationKey]
	w.SpotPolicy = annotations[constants.AlfredSpotPolicyAnnotationKey]
}

// applyMigrationState reconstructs cooldown and in-flight state from durable
// cluster state (request annotations + authoritative InferenceReplica status),
// so it survives Alfred leader failover by construction.
func applyMigrationState(w *Workload, isvc *v1beta1.InferenceService) {
	inFlight := map[string]InFlight{}
	pendingRequests := map[string]InFlight{}
	w.MigrationStateValid = true
	w.MigrationStateReason = ""

	for key, raw := range isvc.Annotations {
		if !strings.HasPrefix(key, audit.MigrationRequestAnnotationPrefix) {
			continue
		}
		uuid := strings.TrimPrefix(key, audit.MigrationRequestAnnotationPrefix)
		pending, err := parsePendingMigration(uuid, raw, w)
		if err != nil {
			// Malformed request annotations stay visible: any prefixed
			// write keeps the workload busy until the executor acknowledges
			// or rejects it.
			if w.MalformedRequests == nil {
				w.MalformedRequests = map[string]string{}
			}
			w.MalformedRequests[uuid] = err.Error()
			continue
		}
		inFlight[uuid] = pending
		pendingRequests[uuid] = pending
	}

	type migrationRow struct {
		component v1beta1.ComponentType
		status    *v1beta1.MigrationStatus
	}
	components := make([]v1beta1.ComponentType, 0, len(w.Components))
	for component := range w.Components {
		components = append(components, component)
	}
	sort.Slice(components, func(i, j int) bool { return components[i] < components[j] })
	var rows []migrationRow
	for _, component := range components {
		state := w.Components[component]
		if state == nil || state.IR == nil {
			continue
		}
		for i := range state.IR.Status.Migrations {
			rows = append(rows, migrationRow{component: component, status: &state.IR.Status.Migrations[i]})
		}
	}

	counts := map[string]int{}
	for _, row := range rows {
		if row.status.RequestUUID != "" {
			counts[row.status.RequestUUID]++
		}
	}
	for _, row := range rows {
		status := row.status
		delete(inFlight, status.RequestUUID)
		delete(w.MalformedRequests, status.RequestUUID)
		if status.RequestUUID == "" || counts[status.RequestUUID] != 1 || !validMigrationStatus(status) {
			invalidateMigrationState(w, migrationStateReasonStatusInvalid)
			continue
		}
		if status.Phase.Terminal() {
			at := status.StartedAt.Time
			if status.CompletedAt != nil {
				at = status.CompletedAt.Time
			}
			if w.LastMigration == nil || at.After(*w.LastMigration) {
				last := at
				w.LastMigration = &last
			}
			continue
		}
		source := currentMigrationInstance(w, row.component, status.SourceInstance)
		if source == nil {
			invalidateMigrationState(w, migrationStateReasonStatusInvalid)
			continue
		}
		if !source.ObservationValid {
			invalidateMigrationState(w, migrationStateReasonStatusInvalid)
		}
		requestedBy := ""
		if pending, ok := pendingRequests[status.RequestUUID]; ok &&
			pending.Component == row.component && pending.Instance == status.SourceInstance {
			requestedBy = pending.RequestedBy
		}
		inFlight[status.RequestUUID] = InFlight{
			UUID:        status.RequestUUID,
			Component:   row.component,
			Instance:    status.SourceInstance,
			FromNode:    status.FromNode,
			Phase:       status.Phase,
			RequestedAt: status.StartedAt.Time,
			RequestedBy: requestedBy,
		}
	}

	if len(w.MalformedRequests) != 0 {
		invalidateMigrationState(w, migrationStateReasonRequestInvalid)
	}

	if len(inFlight) == 0 {
		return
	}
	w.ActiveMigrations = make([]InFlight, 0, len(inFlight))
	for _, f := range inFlight {
		w.ActiveMigrations = append(w.ActiveMigrations, f)
	}
	sort.Slice(w.ActiveMigrations, func(i, j int) bool { return w.ActiveMigrations[i].UUID < w.ActiveMigrations[j].UUID })
}

// resolveModels resolves availability for every model referenced by at least
// one workload. Failures are recorded per model, never fatal.
func resolveModels(ctx context.Context, r client.Reader, s *ClusterSnapshot) {
	for _, w := range s.Workloads {
		key := w.ModelKey
		if key.Zero() {
			continue
		}
		if _, done := s.Models[key]; done {
			continue
		}
		avail := &ModelAvailability{Key: key}
		s.Models[key] = avail

		var spec *v1beta1.BaseModelSpec
		switch key.Kind {
		case ModelKindBaseModel:
			var bm v1beta1.BaseModel
			if err := r.Get(ctx, types.NamespacedName{Namespace: key.Namespace, Name: key.Name}, &bm); err != nil {
				avail.ResolveError = fmt.Sprintf("get basemodel: %v", err)
				continue
			}
			spec = &bm.Spec
		case ModelKindClusterBaseModel:
			var cbm v1beta1.ClusterBaseModel
			if err := r.Get(ctx, types.NamespacedName{Name: key.Name}, &cbm); err != nil {
				avail.ResolveError = fmt.Sprintf("get clusterbasemodel: %v", err)
				continue
			}
			spec = &cbm.Spec
		default:
			avail.ResolveError = fmt.Sprintf("unsupported model kind %q", key.Kind)
			continue
		}

		uri := ""
		if spec.Storage != nil && spec.Storage.StorageUri != nil {
			uri = *spec.Storage.StorageUri
		}
		if strings.HasPrefix(uri, storage.PVCStoragePrefix) {
			avail.Backend = BackendPVC
			resolvePVCAvailability(ctx, r, avail, key.Namespace, uri, s.Nodes)
			continue
		}
		avail.Backend = BackendPerNode
		avail.NodesReady = nodesReadyForModel(s.Nodes, key)
	}
}

// attributePendingPools attributes each pending pod's demand to a hardware
// pool: the pool is unambiguous when the pod's nodeSelector is satisfiable
// by exactly one pool's nodes. Ambiguous or unconstrained demand stays
// unattributed ("" — counted toward every pool by scoring). This is a
// documented heuristic; the candidate simulator, not the score, verifies
// real placements.
func attributePendingPools(s *ClusterSnapshot) {
	poolNodes := map[string][]*Node{}
	for _, n := range s.Nodes {
		if n.TotalGPUs > 0 {
			poolNodes[n.GPUPool] = append(poolNodes[n.GPUPool], n)
		}
	}
	if len(poolNodes) == 1 {
		for pool := range poolNodes {
			for i := range s.PendingPods {
				s.PendingPods[i].GPUPool = pool
			}
		}
		return
	}
	for i := range s.PendingPods {
		pending := &s.PendingPods[i]
		if len(pending.NodeSelector) == 0 {
			continue
		}
		var matches []string
		for pool, nodes := range poolNodes {
			for _, n := range nodes {
				if labelsSatisfy(n.Labels, pending.NodeSelector) {
					matches = append(matches, pool)
					break
				}
			}
		}
		if len(matches) == 1 {
			pending.GPUPool = matches[0]
		}
	}
}

func labelsSatisfy(labels, selector map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}
