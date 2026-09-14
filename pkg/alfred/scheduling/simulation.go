package scheduling

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// SimulationSchemaV1 is the only simulation contract understood here.
const SimulationSchemaV1 = "v1"

// ProfileIdentity fences a simulation to the exact scheduler profile used to
// produce it. SchedulerName is the actual effective Pod scheduler name, not a
// configurable override.
type ProfileIdentity struct {
	SchedulerName    string `json:"schedulerName"`
	Backend          string `json:"backend"`
	SchedulerVersion string `json:"schedulerVersion"`
	ConfigurationID  string `json:"configurationID"`
}

// PodIdentity uniquely identifies a replacement Pod within a request.
type PodIdentity struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	UID       types.UID `json:"uid,omitempty"`
}

// Request is a complete, immutable scheduling-simulation input. ReplacementPods
// are private predictive counterparts of observed Pods, not a promise of the
// exact objects a workload controller will create. They preserve scheduling
// constraints and consistently remap supported intra-gang identities. Bare
// templates are insufficient; unsupported or ambiguous models must be rejected.
// Alfred's scheduling/input package constructs these models from full public
// API observations without invoking a workload renderer or admission preview.
type Request struct {
	SchemaVersion   string                 `json:"schemaVersion"`
	RequestID       string                 `json:"requestID"`
	Profile         ProfileIdentity        `json:"profile"`
	ReplacementPods []corev1.Pod           `json:"replacementPods"`
	SourcePods      []corev1.Pod           `json:"sourcePods"`
	ClusterObjects  []runtime.RawExtension `json:"clusterObjects"`
	SnapshotID      string                 `json:"snapshotID"`
	SnapshotTime    metav1.Time            `json:"snapshotTime"`
	RequireGang     bool                   `json:"requireGang,omitempty"`
	ExcludedNodes   []string               `json:"excludedNodes"`
}

// Decision is a closed set of simulator outcomes.
type Decision string

const (
	DecisionFeasible    Decision = "Feasible"
	DecisionInfeasible  Decision = "Infeasible"
	DecisionUnsupported Decision = "Unsupported"
)

// SimulationReason is a closed set of simulator reasons.
type SimulationReason string

const (
	SimulationReasonPlacementFound      SimulationReason = "PlacementFound"
	SimulationReasonNoFeasiblePlacement SimulationReason = "NoFeasiblePlacement"
	SimulationReasonUnsupported         SimulationReason = "Unsupported"
)

// Placement assigns one requested replacement Pod to one snapshot node.
type Placement struct {
	Pod      PodIdentity `json:"pod"`
	NodeName string      `json:"nodeName"`
}

// Result is a simulator response. Even a Feasible decision is only a
// simulation claim: it is neither scheduler readiness nor a live reservation.
type Result struct {
	SchemaVersion string           `json:"schemaVersion"`
	RequestID     string           `json:"requestID"`
	SnapshotID    string           `json:"snapshotID"`
	SnapshotTime  metav1.Time      `json:"snapshotTime"`
	Profile       ProfileIdentity  `json:"profile"`
	Decision      Decision         `json:"decision"`
	Reason        SimulationReason `json:"reason"`
	Placements    []Placement      `json:"placements,omitempty"`
}

// Simulator evaluates a complete request without prescribing a transport.
type Simulator interface {
	Evaluate(context.Context, Request) (Result, error)
}

// ValidateResult returns nil only for a matching Feasible response with one
// valid placement for every replacement Pod. A nil error does not reserve
// nodes or assert that the live scheduler will make the same decision.
func ValidateResult(request Request, result Result) error {
	expected, excluded, err := validateRequest(request)
	if err != nil {
		return err
	}
	if result.SchemaVersion != request.SchemaVersion {
		return fmt.Errorf("simulation result schema version %q does not match request %q", result.SchemaVersion, request.SchemaVersion)
	}
	if result.RequestID != request.RequestID {
		return fmt.Errorf("simulation result request ID %q does not match request %q", result.RequestID, request.RequestID)
	}
	if result.SnapshotID != request.SnapshotID {
		return fmt.Errorf("simulation result snapshot ID %q does not match request %q", result.SnapshotID, request.SnapshotID)
	}
	if !result.SnapshotTime.Equal(&request.SnapshotTime) {
		return fmt.Errorf("simulation result snapshot time %s does not match request %s", result.SnapshotTime.Time, request.SnapshotTime.Time)
	}
	if result.Profile != request.Profile {
		return fmt.Errorf("simulation result profile identity does not match request")
	}
	if result.Decision != DecisionFeasible {
		if !knownDecision(result.Decision) {
			return fmt.Errorf("simulation result has unknown decision %q", result.Decision)
		}
		if !knownReason(result.Reason) {
			return fmt.Errorf("simulation result has unknown reason %q", result.Reason)
		}
		return fmt.Errorf("simulation result is not feasible: %s", result.Decision)
	}
	if result.Reason != SimulationReasonPlacementFound {
		return fmt.Errorf("feasible simulation result has invalid reason %q", result.Reason)
	}

	liveNodes, err := liveNodeNames(request.ClusterObjects)
	if err != nil {
		return err
	}
	if len(liveNodes) == 0 {
		return fmt.Errorf("simulation request contains no live nodes")
	}

	placed := make(map[PodIdentity]struct{}, len(result.Placements))
	for _, placement := range result.Placements {
		if _, ok := expected[placement.Pod]; !ok {
			return fmt.Errorf("simulation result contains unexpected pod %s/%s", placement.Pod.Namespace, placement.Pod.Name)
		}
		if _, duplicate := placed[placement.Pod]; duplicate {
			return fmt.Errorf("simulation result contains duplicate placement for pod %s/%s", placement.Pod.Namespace, placement.Pod.Name)
		}
		placed[placement.Pod] = struct{}{}
		if _, ok := liveNodes[placement.NodeName]; !ok {
			return fmt.Errorf("simulation result places pod %s/%s on unknown node %q", placement.Pod.Namespace, placement.Pod.Name, placement.NodeName)
		}
		if _, blocked := excluded[placement.NodeName]; blocked {
			return fmt.Errorf("simulation result places pod %s/%s on excluded node %q", placement.Pod.Namespace, placement.Pod.Name, placement.NodeName)
		}
	}
	for pod := range expected {
		if _, ok := placed[pod]; !ok {
			return fmt.Errorf("simulation result is missing placement for pod %s/%s", pod.Namespace, pod.Name)
		}
	}
	return nil
}

func validateRequest(request Request) (map[PodIdentity]struct{}, map[string]struct{}, error) {
	if request.SchemaVersion != SimulationSchemaV1 {
		return nil, nil, fmt.Errorf("simulation request schema version %q is unsupported", request.SchemaVersion)
	}
	if err := validateIdentifier("request ID", request.RequestID); err != nil {
		return nil, nil, err
	}
	if err := validateIdentifier("snapshot ID", request.SnapshotID); err != nil {
		return nil, nil, err
	}
	if request.SnapshotTime.IsZero() {
		return nil, nil, fmt.Errorf("snapshot time must not be zero")
	}
	if err := request.Profile.validate(); err != nil {
		return nil, nil, fmt.Errorf("profile identity: %w", err)
	}
	if len(request.ReplacementPods) == 0 {
		return nil, nil, fmt.Errorf("simulation request has no replacement pods")
	}

	excluded := make(map[string]struct{}, len(request.ExcludedNodes))
	for _, nodeName := range request.ExcludedNodes {
		if err := validateIdentifier("excluded node", nodeName); err != nil {
			return nil, nil, err
		}
		excluded[nodeName] = struct{}{}
	}

	expected := make(map[PodIdentity]struct{}, len(request.ReplacementPods))
	replacementNames := make(map[string]struct{}, len(request.ReplacementPods))
	for i := range request.ReplacementPods {
		pod := &request.ReplacementPods[i]
		identity, err := identityForReplacement(*pod)
		if err != nil {
			return nil, nil, fmt.Errorf("replacementPods[%d]: %w", i, err)
		}
		if pod.Spec.NodeName != "" {
			return nil, nil, fmt.Errorf("replacementPods[%d] is already bound to node %q", i, pod.Spec.NodeName)
		}
		if schedulerName := effectiveSchedulerName(pod.Spec.SchedulerName); schedulerName != request.Profile.SchedulerName {
			return nil, nil, fmt.Errorf("replacementPods[%d] scheduler name %q does not match profile %q", i, schedulerName, request.Profile.SchedulerName)
		}
		if _, duplicate := expected[identity]; duplicate {
			return nil, nil, fmt.Errorf("replacementPods[%d] has duplicate identity %s/%s", i, identity.Namespace, identity.Name)
		}
		nameKey := identity.Namespace + "/" + identity.Name
		if _, duplicate := replacementNames[nameKey]; duplicate {
			return nil, nil, fmt.Errorf("replacementPods[%d] has duplicate name %s", i, nameKey)
		}
		replacementNames[nameKey] = struct{}{}
		expected[identity] = struct{}{}
	}

	if len(request.SourcePods) == 0 {
		return nil, nil, fmt.Errorf("simulation request has no source pods")
	}
	sources := make(map[PodIdentity]struct{}, len(request.SourcePods))
	for i := range request.SourcePods {
		pod := &request.SourcePods[i]
		identity := podIdentity(*pod)
		if identity.Namespace == "" || identity.Name == "" || identity.UID == "" {
			return nil, nil, fmt.Errorf("source pod identity at index %d must include namespace, name, and UID", i)
		}
		if _, duplicate := sources[identity]; duplicate {
			return nil, nil, fmt.Errorf("source pod identity at index %d is duplicated", i)
		}
		sources[identity] = struct{}{}
		if pod.Spec.NodeName == "" {
			return nil, nil, fmt.Errorf("source pod %s/%s is not bound", pod.Namespace, pod.Name)
		}
		if _, explicit := excluded[pod.Spec.NodeName]; !explicit {
			return nil, nil, fmt.Errorf("source node %q for pod %s/%s is not explicitly excluded", pod.Spec.NodeName, pod.Namespace, pod.Name)
		}
	}
	return expected, excluded, nil
}

func (p ProfileIdentity) validate() error {
	if err := validateSchedulerName(p.SchedulerName); err != nil {
		return err
	}
	return validateProfile(Profile{
		Backend: p.Backend, SchedulerVersion: p.SchedulerVersion, ConfigurationID: p.ConfigurationID,
	})
}

func identityForReplacement(pod corev1.Pod) (PodIdentity, error) {
	identity := podIdentity(pod)
	if identity.Namespace == "" || identity.Name == "" {
		return PodIdentity{}, fmt.Errorf("pod identity must include namespace and name")
	}
	return identity, nil
}

func podIdentity(pod corev1.Pod) PodIdentity {
	return PodIdentity{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}
}

func knownDecision(decision Decision) bool {
	return decision == DecisionFeasible || decision == DecisionInfeasible || decision == DecisionUnsupported
}

func knownReason(reason SimulationReason) bool {
	return reason == SimulationReasonPlacementFound || reason == SimulationReasonNoFeasiblePlacement || reason == SimulationReasonUnsupported
}

func liveNodeNames(objects []runtime.RawExtension) (map[string]struct{}, error) {
	nodes := make(map[string]struct{})
	for i := range objects {
		object := objects[i]
		if object.Object != nil {
			gvk := object.Object.GetObjectKind().GroupVersionKind()
			switch node := object.Object.(type) {
			case *corev1.Node:
				if !gvk.Empty() && (gvk.Group != "" || gvk.Version != "v1" || gvk.Kind != "Node") {
					continue
				}
				if node.Name != "" {
					nodes[node.Name] = struct{}{}
				}
			default:
				if gvk.Group == "" && gvk.Version == "v1" && gvk.Kind == "Node" {
					accessor, err := apiMeta.Accessor(object.Object)
					if err != nil {
						return nil, fmt.Errorf("clusterObjects[%d] Node metadata: %w", i, err)
					}
					if accessor.GetName() != "" {
						nodes[accessor.GetName()] = struct{}{}
					}
				}
			}
			continue
		}
		if len(object.Raw) == 0 {
			continue
		}
		var metadata struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Metadata   struct {
				Name string `json:"name"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(object.Raw, &metadata); err != nil {
			return nil, fmt.Errorf("clusterObjects[%d] is malformed: %w", i, err)
		}
		if metadata.APIVersion == "v1" && metadata.Kind == "Node" && metadata.Metadata.Name != "" {
			nodes[metadata.Metadata.Name] = struct{}{}
		}
	}
	return nodes, nil
}
