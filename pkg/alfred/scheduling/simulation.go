package scheduling

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
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
	// MigrationFromNode selects the public migration API's single-node exclusion.
	// Empty retains the recommendation contract excluding every source node.
	MigrationFromNode string `json:"migrationFromNode,omitempty"`
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

// ValidateResponse validates the request and the complete response envelope for
// every decision. Feasible responses must contain one valid placement for every
// replacement Pod; negative responses must not contain placements.
func ValidateResponse(request Request, result Result) error {
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
	switch result.Decision {
	case DecisionFeasible:
		if result.Reason != SimulationReasonPlacementFound {
			return fmt.Errorf("feasible simulation result has invalid reason %q", result.Reason)
		}
	case DecisionInfeasible:
		if result.Reason != SimulationReasonNoFeasiblePlacement {
			return fmt.Errorf("infeasible simulation result has invalid reason %q", result.Reason)
		}
		if len(result.Placements) != 0 {
			return fmt.Errorf("infeasible simulation result must not contain placements")
		}
		return nil
	case DecisionUnsupported:
		if result.Reason != SimulationReasonUnsupported {
			return fmt.Errorf("unsupported simulation result has invalid reason %q", result.Reason)
		}
		if len(result.Placements) != 0 {
			return fmt.Errorf("unsupported simulation result must not contain placements")
		}
		return nil
	default:
		return fmt.Errorf("simulation result has unknown decision %q", result.Decision)
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

// ValidateResult returns nil only for a matching Feasible response with one
// valid placement for every replacement Pod. A nil error does not reserve
// nodes or assert that the live scheduler will make the same decision.
func ValidateResult(request Request, result Result) error {
	if err := ValidateResponse(request, result); err != nil {
		return err
	}
	if result.Decision != DecisionFeasible {
		return fmt.Errorf("simulation result is not feasible: %s", result.Decision)
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
	if request.MigrationFromNode != "" {
		if err := validateMigrationRequest(request); err != nil {
			return nil, nil, err
		}
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
		if _, explicit := excluded[pod.Spec.NodeName]; !explicit && request.MigrationFromNode == "" {
			return nil, nil, fmt.Errorf("source node %q for pod %s/%s is not explicitly excluded", pod.Spec.NodeName, pod.Namespace, pod.Name)
		}
	}
	return expected, excluded, nil
}

// validateMigrationRequest preserves occupancy while allowing the API's single
// from_node exclusion. It never grants capacity credit for an original Pod.
func validateMigrationRequest(request Request) error {
	from := request.MigrationFromNode
	if len(validation.IsDNS1123Subdomain(from)) != 0 {
		return fmt.Errorf("migrationFromNode must be a DNS1123 node name")
	}
	if len(request.ExcludedNodes) != 1 || request.ExcludedNodes[0] != from {
		return fmt.Errorf("migration request must exclude exactly migrationFromNode")
	}
	nodes := map[string]*corev1.Node{}
	pods := map[types.NamespacedName]*corev1.Pod{}
	for i, extension := range request.ClusterObjects {
		if extension.Object != nil && len(extension.Raw) != 0 {
			return fmt.Errorf("clusterObjects[%d] has ambiguous representations", i)
		}
		var node *corev1.Node
		var pod *corev1.Pod
		switch object := extension.Object.(type) {
		case *corev1.Node:
			node = object
		case *corev1.Pod:
			pod = object
		default:
			raw := extension.Raw
			if extension.Object != nil {
				var err error
				raw, err = json.Marshal(extension.Object)
				if err != nil {
					return fmt.Errorf("clusterObjects[%d]: %w", i, err)
				}
			}
			var header metav1.TypeMeta
			if err := json.Unmarshal(raw, &header); err != nil {
				return fmt.Errorf("clusterObjects[%d]: %w", i, err)
			}
			if header.APIVersion == "v1" {
				switch header.Kind {
				case "Node":
					node = &corev1.Node{}
					if err := json.Unmarshal(raw, node); err != nil {
						return err
					}
				case "Pod":
					pod = &corev1.Pod{}
					if err := json.Unmarshal(raw, pod); err != nil {
						return err
					}
				}
			}
		}
		if node != nil {
			if node.APIVersion != "" && node.APIVersion != "v1" || node.Kind != "" && node.Kind != "Node" {
				return fmt.Errorf("conflicting snapshot Node type")
			}
			if _, exists := nodes[node.Name]; exists {
				return fmt.Errorf("duplicate snapshot Node")
			}
			nodes[node.Name] = node
		}
		if pod != nil {
			if pod.APIVersion != "" && pod.APIVersion != "v1" || pod.Kind != "" && pod.Kind != "Pod" {
				return fmt.Errorf("conflicting snapshot Pod type")
			}
			key := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
			if _, exists := pods[key]; exists {
				return fmt.Errorf("duplicate snapshot Pod")
			}
			pods[key] = pod
		}
	}
	if node := nodes[from]; node == nil || node.DeletionTimestamp != nil {
		return fmt.Errorf("migrationFromNode must be a known live snapshot node")
	}
	hostsSource := false
	for i := range request.SourcePods {
		source := request.SourcePods[i].DeepCopy()
		if source.Spec.NodeName == from {
			hostsSource = true
		}
		occupied := pods[types.NamespacedName{Namespace: source.Namespace, Name: source.Name}]
		if occupied == nil {
			return fmt.Errorf("migration source Pod is missing from snapshot occupancy")
		}
		actual := occupied.DeepCopy()
		source.TypeMeta = metav1.TypeMeta{}
		actual.TypeMeta = metav1.TypeMeta{}
		if !apiequality.Semantic.DeepEqual(source, actual) {
			return fmt.Errorf("migration source Pod does not match snapshot occupancy")
		}
	}
	if !hostsSource {
		return fmt.Errorf("migrationFromNode does not host a source Pod")
	}
	for i := range request.ReplacementPods {
		if !hasMigrationExclusion(&request.ReplacementPods[i], from) {
			return fmt.Errorf("replacementPods[%d] lacks required migration hostname exclusion in every affinity term", i)
		}
	}
	return nil
}

func hasMigrationExclusion(pod *corev1.Pod, from string) bool {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil {
		return false
	}
	required := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if required == nil || len(required.NodeSelectorTerms) == 0 {
		return false
	}
	for _, term := range required.NodeSelectorTerms {
		found := false
		for _, requirement := range term.MatchExpressions {
			if requirement.Key == corev1.LabelHostname && requirement.Operator == corev1.NodeSelectorOpNotIn {
				for _, value := range requirement.Values {
					if value == from {
						found = true
					}
				}
			}
		}
		if !found {
			return false
		}
	}
	return true
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
