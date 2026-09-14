package policy

import (
	"sort"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// MaxHintTargets bounds the scheduler-facing target suggestions retained in
// reports. PlacementTargetNodes retains the exhaustive feasibility set for
// arbitration replay.
const MaxHintTargets = 3

// SurgeMove is one member pod in a complete atomic place-then-free plan.
// Moves are ordered largest footprint first, then by pod identity.
type SurgeMove struct {
	Pod        string
	FromNode   string
	TargetNode string
	GPUs       int64
}

// SurgePlan accounts for every GPU member's headroom while source capacity
// remains occupied. The engine separately checks complete pod scheduling.
type SurgePlan struct {
	Moves                []SurgeMove
	HintTargetNodes      []string
	PlacementTargetNodes []string
}

// ModelAdvisoryReason returns the fail-closed model/storage classification
// shared by policies. An empty reason is the only model state that may move.
func ModelAdvisoryReason(snap *snapshot.ClusterSnapshot, w *snapshot.Workload) string {
	if snap == nil || w == nil || w.ModelKey.Zero() {
		return ""
	}
	avail, ok := snap.Models[w.ModelKey]
	if !ok || avail == nil || avail.ResolveError != "" {
		return AdvisoryModelUnresolved
	}
	if avail.VolumePinned {
		return AdvisoryVolumePinned
	}
	return ""
}

// OMENativeEligibility is the shared fail-closed execution baseline. It
// intentionally excludes policy-specific cooldowns: Defrag applies its
// ordinary workload cooldown, while Node Health leaves its five-minute floor
// to the Arbiter.
func OMENativeEligibility(snap *snapshot.ClusterSnapshot, w *snapshot.Workload,
	comp *snapshot.Component, inst *snapshot.Instance) string {

	if snap == nil || w == nil || comp == nil || inst == nil {
		return AdvisoryOMENativeObservationInvalid
	}
	if !w.Movable || !w.MigrationStateValid || len(w.MalformedRequests) > 0 ||
		len(w.ActiveMigrations) > 0 {
		return AdvisoryOMENativeStateIneligible
	}
	if comp.IR == nil || !comp.StatusFresh || !comp.ObservationValid {
		return AdvisoryOMENativeObservationInvalid
	}
	ir := comp.IR
	if ir.Spec.Paused {
		return AdvisoryOMENativeStateIneligible
	}
	if ir.Spec.Lifecycle != nil && ir.Spec.Lifecycle.MigrationPolicy != nil &&
		ir.Spec.Lifecycle.MigrationPolicy.Mode == v1beta1.MigrationPolicyModeNever {
		return AdvisoryOMENativeStateIneligible
	}
	desired := int32(1)
	if ir.Spec.Replicas != nil {
		desired = *ir.Spec.Replicas
	}
	status := &ir.Status
	if desired <= 0 || int32(len(comp.Instances)) != desired || status.Replicas != desired ||
		status.ReadyReplicas != desired || status.ServingReplicas != desired ||
		status.AvailableReplicas != desired || status.UpdatedReplicas != desired ||
		status.UpdatedReadyReplicas != desired {
		return AdvisoryOMENativeStateIneligible
	}
	if status.CurrentRevision == "" || status.UpdateRevision == "" ||
		status.CurrentRevision != status.UpdateRevision {
		return AdvisoryOMENativeStateIneligible
	}
	if !snap.OMENativeExecutor.Available {
		return AdvisoryOMENativeUnavailable
	}
	if !inst.ObservationValid {
		return AdvisoryOMENativeObservationInvalid
	}
	if !inst.Admitted || inst.Phase != v1beta1.OMENativeInstanceReady || inst.Operation != nil {
		return AdvisoryOMENativeStateIneligible
	}
	if inst.DesiredPods <= 0 || inst.StatusPods != inst.DesiredPods ||
		inst.ObservedPods != inst.DesiredPods || int32(len(inst.Pods)) != inst.DesiredPods ||
		inst.ReadyPods != inst.DesiredPods || inst.ServingPods != inst.DesiredPods ||
		inst.AvailablePods != inst.DesiredPods {
		return AdvisoryOMENativeStateIneligible
	}
	if inst.RunningRevision == "" || inst.RunningRevision != status.CurrentRevision ||
		inst.TargetRevision != "" {
		return AdvisoryOMENativeStateIneligible
	}
	for i := range inst.Pods {
		if !inst.Pods[i].Ready || inst.Pods[i].Terminating {
			return AdvisoryOMENativeStateIneligible
		}
	}
	return ""
}

// PlanAtomicSurge constructs a deterministic GPU headroom plan for an Instance.
// GPU replacements are accounted before releasing any source footprint, and
// every current Instance node is excluded as a target. CPU members participate
// in identity and source validation; the engine checks their full scheduling.
func PlanAtomicSurge(snap *snapshot.ClusterSnapshot, cfg *config.Config, w *snapshot.Workload,
	inst *snapshot.Instance) (SurgePlan, bool) {

	if snap == nil || cfg == nil || w == nil || inst == nil || ModelAdvisoryReason(snap, w) != "" {
		return SurgePlan{}, false
	}
	prints, sourceNodes, ok := validatedSurgeFootprints(w, inst)
	if !ok {
		return SurgePlan{}, false
	}

	pool := ""
	excluded := make(map[string]struct{}, len(sourceNodes))
	for nodeName := range sourceNodes {
		if snap.Nodes[nodeName] == nil {
			return SurgePlan{}, false
		}
		excluded[nodeName] = struct{}{}
	}
	// CPU companions may run on GPU-less nodes. Only positive GPU member
	// footprints determine the hardware pool used for this capacity plan.
	for _, print := range prints {
		node := snap.Nodes[print.node]
		if node.GPUPool == "" {
			return SurgePlan{}, false
		}
		if pool == "" {
			pool = node.GPUPool
		} else if node.GPUPool != pool {
			return SurgePlan{}, false
		}
	}
	if pool == "" {
		return SurgePlan{}, false
	}

	allowed, constrained := modelTargetNodes(snap, w)
	avoidSpot := workloadAvoidsSpotTarget(w, cfg)
	minimum := prints[len(prints)-1].gpus
	targets := make([]surgeTarget, 0)
	for _, node := range snap.PoolNodes(pool) {
		if node == nil {
			continue
		}
		if _, source := excluded[node.Name]; source {
			continue
		}
		if node.UnavailableAsTarget() ||
			(avoidSpot && node.Preemptible) || node.FreeGPUs < minimum {
			continue
		}
		if constrained {
			if _, ok := allowed[node.Name]; !ok {
				continue
			}
		}
		targets = append(targets, surgeTarget{name: node.Name, free: node.FreeGPUs})
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].free != targets[j].free {
			return targets[i].free < targets[j].free
		}
		return targets[i].name < targets[j].name
	})

	plan := SurgePlan{PlacementTargetNodes: make([]string, len(targets))}
	free := make(map[string]int64, len(targets))
	for i := range targets {
		plan.PlacementTargetNodes[i] = targets[i].name
		free[targets[i].name] = targets[i].free
	}
	for _, print := range prints {
		placed := false
		for _, target := range targets {
			if free[target.name] < print.gpus {
				continue
			}
			free[target.name] -= print.gpus
			plan.Moves = append(plan.Moves, SurgeMove{
				Pod:        print.pod,
				FromNode:   print.node,
				TargetNode: target.name,
				GPUs:       print.gpus,
			})
			placed = true
			break
		}
		if !placed {
			return SurgePlan{}, false
		}
	}
	hintCount := len(plan.PlacementTargetNodes)
	if hintCount > MaxHintTargets {
		hintCount = MaxHintTargets
	}
	plan.HintTargetNodes = append([]string(nil), plan.PlacementTargetNodes[:hintCount]...)
	return plan, true
}

func workloadAvoidsSpotTarget(w *snapshot.Workload, cfg *config.Config) bool {
	if cfg.SpotPolicy.AvoidAsTarget != nil && *cfg.SpotPolicy.AvoidAsTarget {
		return true
	}
	return w.SpotPolicy == "avoid"
}

type surgeFootprint struct {
	pod  string
	node string
	gpus int64
}

func validatedSurgeFootprints(w *snapshot.Workload, inst *snapshot.Instance) ([]surgeFootprint, map[string]int, bool) {
	prints := make([]surgeFootprint, 0, len(inst.Pods))
	sourceNodes := make(map[string]int, len(inst.NodesSet))
	var totalGPUs int64
	component := v1beta1.ComponentType("")
	for i := range inst.Pods {
		pod := &inst.Pods[i]
		if pod.Namespace != w.NamespacedName.Namespace || pod.Name == "" || pod.Node == "" || pod.GPUs < 0 ||
			pod.ISVC != w.NamespacedName || !pod.InstanceIndexPresent || !pod.InstanceIndexValid ||
			pod.InstanceIndex != inst.Index || !pod.IncarnationPresent || !pod.IncarnationValid ||
			pod.Incarnation != inst.Incarnation {
			return nil, nil, false
		}
		if component == "" {
			component = pod.Component
		} else if pod.Component != component {
			return nil, nil, false
		}
		sourceNodes[pod.Node]++
		totalGPUs += pod.GPUs
		if pod.GPUs == 0 {
			continue
		}
		prints = append(prints, surgeFootprint{
			pod:  pod.Namespace + "/" + pod.Name,
			node: pod.Node,
			gpus: pod.GPUs,
		})
	}
	if len(prints) == 0 || totalGPUs != inst.TotalGPUs || len(sourceNodes) != len(inst.NodesSet) {
		return nil, nil, false
	}
	for node, count := range sourceNodes {
		if inst.NodesSet[node] != count {
			return nil, nil, false
		}
	}
	sort.SliceStable(prints, func(i, j int) bool {
		if prints[i].gpus != prints[j].gpus {
			return prints[i].gpus > prints[j].gpus
		}
		return prints[i].pod < prints[j].pod
	})
	return prints, sourceNodes, true
}

type surgeTarget struct {
	name string
	free int64
}

func modelTargetNodes(snap *snapshot.ClusterSnapshot, w *snapshot.Workload) (map[string]struct{}, bool) {
	if w.ModelKey.Zero() {
		return nil, false
	}
	avail, ok := snap.Models[w.ModelKey]
	if !ok || avail == nil || avail.ResolveError != "" {
		return nil, true
	}
	var nodes []string
	switch avail.Backend {
	case snapshot.BackendPerNode:
		nodes = avail.NodesReady
	case snapshot.BackendPVC:
		if avail.PVCTopologyNodes == nil {
			return nil, false
		}
		nodes = avail.PVCTopologyNodes
	default:
		return nil, false
	}
	allowed := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		allowed[node] = struct{}{}
	}
	return allowed, true
}
