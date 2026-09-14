// Package nodehealth implements Alfred node evacuation: remediation markers
// and atomic migration findings for health failures and planned maintenance.
package nodehealth

import (
	"sort"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/constants"
)

// PolicyName is the stable policy label used by metrics and reports.
const PolicyName = "nodehealth"

// Policy is Policy #2. It is a pure classifier and planner; the engine owns
// reporting, simulation and guarded dispatch. The policy performs no writes.
type Policy struct{}

var _ policy.Policy = &Policy{}

// Name implements policy.Policy.
func (*Policy) Name() string { return PolicyName }

// Evaluate emits a marker for every non-clear health or requested maintenance
// observation and, unless signalOnly is set, one finding per affected atomic
// Instance. Unknown and suspect health do not independently request movement.
func (*Policy) Evaluate(snap *snapshot.ClusterSnapshot, cfg *config.Config) []policy.Candidate {
	if snap == nil || cfg == nil || cfg.Policies.NodeHealth.Enabled == nil ||
		!*cfg.Policies.NodeHealth.Enabled {
		return nil
	}

	markers := remediationMarkers(snap)
	if cfg.Policies.NodeHealth.SignalOnly {
		return markers
	}

	findings := evacuationFindings(snap, cfg)
	rankFindings(findings)
	return append(markers, findings...)
}

func remediationMarkers(snap *snapshot.ClusterSnapshot) []policy.Candidate {
	nodeNames := make([]string, 0, len(snap.Nodes))
	for name, node := range snap.Nodes {
		if node == nil || (!node.Health.Quarantined() && !node.Maintenance.Requested) {
			continue
		}
		nodeNames = append(nodeNames, name)
	}
	sort.Strings(nodeNames)

	markers := make([]policy.Candidate, 0, len(nodeNames))
	for _, name := range nodeNames {
		node := snap.Nodes[name]
		workloads, occupantsPresent := nodeOccupancy(node)
		markers = append(markers, policy.Candidate{
			Policy:     PolicyName,
			Reason:     policy.ReasonRemediationSignal,
			FromNode:   name,
			Executable: false,
			Remediation: &policy.NodeRemediation{
				Node:                   name,
				NodeUID:                node.UID,
				ObservedAt:             snap.Timestamp,
				Health:                 copyHealth(node.Health),
				Maintenance:            copyMaintenance(node.Maintenance),
				Workloads:              workloads,
				OMEGPUOccupantsPresent: occupantsPresent,
			},
		})
	}
	return markers
}

func copyMaintenance(in snapshot.NodeMaintenanceObservation) snapshot.NodeMaintenanceObservation {
	out := in
	out.Triggers = append([]string(nil), in.Triggers...)
	return out
}

func copyHealth(in snapshot.NodeHealthObservation) snapshot.NodeHealthObservation {
	out := in
	out.Conditions = append([]snapshot.NodeConditionObservation(nil), in.Conditions...)
	if in.SuspectUntil != nil {
		until := *in.SuspectUntil
		out.SuspectUntil = &until
	}
	return out
}

func nodeOccupancy(node *snapshot.Node) ([]string, bool) {
	seen := make(map[string]struct{})
	occupantsPresent := false
	for i := range node.OMEPods {
		pod := &node.OMEPods[i]
		if pod.GPUs <= 0 {
			continue
		}
		occupantsPresent = true
		if pod.ISVC.Name == "" {
			continue
		}
		seen[pod.ISVC.String()] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, occupantsPresent
}

type findingKey struct {
	workload  string
	component string
	instance  int32
}

func evacuationFindings(snap *snapshot.ClusterSnapshot, cfg *config.Config) []policy.Candidate {
	seen := make(map[findingKey]struct{})
	var findings []policy.Candidate
	for _, w := range sortedWorkloads(snap) {
		for _, comp := range sortedComponents(w) {
			physical := evacuationComponentPods(snap, w, comp)
			covered := make(map[componentPodKey]struct{}, len(physical))
			instances := append([]*snapshot.Instance(nil), comp.Instances...)
			sort.Slice(instances, func(i, j int) bool {
				if instances[i] == nil {
					return instances[j] != nil
				}
				if instances[j] == nil {
					return false
				}
				return instances[i].Index < instances[j].Index
			})
			for _, inst := range instances {
				if inst == nil || inst.TotalGPUs <= 0 {
					continue
				}
				from, reason := firstEvacuationMember(snap, inst)
				if from == "" {
					continue
				}
				key := findingKey{workload: w.NamespacedName.String(), component: string(comp.Type), instance: inst.Index}
				if _, duplicate := seen[key]; duplicate {
					continue
				}
				seen[key] = struct{}{}
				if candidate, ok := classify(snap, cfg, w, comp, inst, from, reason); ok {
					findings = append(findings, candidate)
					coverInstancePods(covered, physical, inst)
				}
			}
			if comp.DeploymentMode == constants.OMENative {
				if from, reason := firstUncoveredComponentNode(snap, physical, covered); from != "" {
					findings = append(findings, unresolvedComponentAdvisory(w, comp, from, reason))
				}
			}
		}
	}
	return findings
}

type componentPodKey struct {
	namespace string
	name      string
	node      string
	uid       types.UID
}

// evacuationComponentPods retains the physical Pod evidence for which this
// component owes either a resolvable Instance finding or a component-wide
// fallback. The enclosing Node is authoritative for physical placement.
func evacuationComponentPods(snap *snapshot.ClusterSnapshot, w *snapshot.Workload,
	comp *snapshot.Component) map[componentPodKey]struct{} {
	pods := make(map[componentPodKey]struct{})
	if comp.DeploymentMode != constants.OMENative {
		return pods
	}
	for nodeName, node := range snap.Nodes {
		if evacuationReason(node) == "" {
			continue
		}
		for i := range node.OMEPods {
			pod := &node.OMEPods[i]
			if pod.GPUs > 0 && pod.ISVC == w.NamespacedName && pod.Component == comp.Type {
				pods[componentPodKey{namespace: pod.Namespace, name: pod.Name, node: nodeName, uid: pod.UID}] = struct{}{}
			}
		}
	}
	return pods
}

func coverInstancePods(covered, physical map[componentPodKey]struct{}, inst *snapshot.Instance) {
	for i := range inst.Pods {
		pod := &inst.Pods[i]
		if pod.GPUs <= 0 || pod.Node == "" {
			continue
		}
		key := componentPodKey{namespace: pod.Namespace, name: pod.Name, node: pod.Node, uid: pod.UID}
		if _, ok := physical[key]; ok {
			covered[key] = struct{}{}
		}
	}
}

// firstUncoveredComponentNode chooses only a source node proven by physical
// Pod evidence that no emitted Instance finding covered. It deliberately does
// not infer a migration Instance index from rejected IR/Pod identity.
func firstUncoveredComponentNode(snap *snapshot.ClusterSnapshot, physical, covered map[componentPodKey]struct{}) (string, string) {
	from, reason := "", ""
	for pod := range physical {
		if _, ok := covered[pod]; ok {
			continue
		}
		candidateReason := evacuationReason(snap.Nodes[pod.node])
		if preferSource(pod.node, candidateReason, from, reason) {
			from, reason = pod.node, candidateReason
		}
	}
	return from, reason
}

// unresolvedComponentAdvisory preserves the workload/component identity
// proven by the ISVC and physical Pod evidence, but uses the component-wide
// sentinel and a zero footprint because no stable migration Instance exists.
func unresolvedComponentAdvisory(w *snapshot.Workload, comp *snapshot.Component, from, reason string) policy.Candidate {
	return policy.Candidate{
		Policy:         PolicyName,
		Workload:       w.NamespacedName,
		Component:      comp.Type,
		Instance:       policy.ComponentWideInstance,
		Mode:           comp.DeploymentMode,
		Reason:         reason,
		FromNode:       from,
		AdvisoryReason: policy.AdvisoryOMENativeObservationInvalid,
		Score:          w.Priority,
	}
}

func firstEvacuationMember(snap *snapshot.ClusterSnapshot, inst *snapshot.Instance) (string, string) {
	from, reason := "", ""
	for i := range inst.Pods {
		pod := &inst.Pods[i]
		if pod.Node == "" {
			continue
		}
		candidateReason := evacuationReason(snap.Nodes[pod.Node])
		if preferSource(pod.Node, candidateReason, from, reason) {
			from, reason = pod.Node, candidateReason
		}
	}
	return from, reason
}

func evacuationReason(node *snapshot.Node) string {
	if node == nil {
		return ""
	}
	if node.Health.State == snapshot.NodeHealthUnhealthy {
		return policy.ReasonNodeUnhealthy
	}
	if node.Maintenance.Requested {
		return policy.ReasonNodeMaintenance
	}
	return ""
}

// A real health failure wins over planned work, then node name makes source
// selection deterministic without inferring identity from aggregate counters.
func preferSource(node, reason, currentNode, currentReason string) bool {
	if reason == "" {
		return false
	}
	if currentNode == "" {
		return true
	}
	if reason != currentReason {
		return reason == policy.ReasonNodeUnhealthy
	}
	return node < currentNode
}

func classify(snap *snapshot.ClusterSnapshot, cfg *config.Config, w *snapshot.Workload,
	comp *snapshot.Component, inst *snapshot.Instance, from, reason string) (policy.Candidate, bool) {

	candidate := policy.Candidate{
		Policy:        PolicyName,
		Workload:      w.NamespacedName,
		Component:     comp.Type,
		Instance:      inst.Index,
		Mode:          comp.DeploymentMode,
		Reason:        reason,
		FromNode:      from,
		FootprintGPUs: inst.TotalGPUs,
		Score:         w.Priority,
	}

	switch comp.DeploymentMode {
	case constants.RawDeployment:
		candidate.AdvisoryReason = policy.AdvisoryRawDeploymentMigrationUnsupported
		return candidate, true
	case constants.MultiNode:
		if cfg.LWSRecommendationsEnabled == nil || !*cfg.LWSRecommendationsEnabled {
			return policy.Candidate{}, false
		}
		candidate.AdvisoryReason = policy.AdvisoryLWSMigrationUnsupported
		return candidate, true
	case constants.OMENative:
	default:
		return policy.Candidate{}, false
	}

	if reason := policy.ModelAdvisoryReason(snap, w); reason != "" {
		candidate.AdvisoryReason = reason
		return candidate, true
	}
	if cfg.OMENativeMigrationEnabled == nil || !*cfg.OMENativeMigrationEnabled {
		candidate.AdvisoryReason = policy.AdvisoryMigrationSurfaceDisabled
		return candidate, true
	}
	if reason := policy.OMENativeEligibility(snap, w, comp, inst); reason != "" {
		candidate.AdvisoryReason = reason
		return candidate, true
	}

	plan, ok := policy.PlanAtomicSurge(snap, cfg, w, inst)
	candidate.SurgeShaped = true
	if !ok {
		candidate.AdvisoryReason = policy.AdvisoryNoSurgeHeadroom
		return candidate, true
	}
	candidate.Executable = true
	candidate.HintTargetNodes = append([]string(nil), plan.HintTargetNodes...)
	candidate.PlacementTargetNodes = append([]string(nil), plan.PlacementTargetNodes...)
	return candidate, true
}

func rankFindings(findings []policy.Candidate) {
	sort.SliceStable(findings, func(i, j int) bool {
		a, b := &findings[i], &findings[j]
		if a.Reason != b.Reason {
			return a.Reason == policy.ReasonNodeUnhealthy
		}
		if a.Executable != b.Executable {
			return a.Executable
		}
		if a.Score != b.Score {
			return a.Score > b.Score
		}
		if a.FootprintGPUs != b.FootprintGPUs {
			return a.FootprintGPUs < b.FootprintGPUs
		}
		if a.Workload.String() != b.Workload.String() {
			return a.Workload.String() < b.Workload.String()
		}
		if a.Component != b.Component {
			return a.Component < b.Component
		}
		return a.Instance < b.Instance
	})
}

func sortedWorkloads(snap *snapshot.ClusterSnapshot) []*snapshot.Workload {
	out := make([]*snapshot.Workload, 0, len(snap.Workloads))
	for _, w := range snap.Workloads {
		if w != nil {
			out = append(out, w)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].NamespacedName.String() < out[j].NamespacedName.String()
	})
	return out
}

func sortedComponents(w *snapshot.Workload) []*snapshot.Component {
	out := make([]*snapshot.Component, 0, len(w.Components))
	for _, comp := range w.Components {
		if comp != nil {
			out = append(out, comp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Type < out[j].Type })
	return out
}
