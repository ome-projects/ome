package input

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Source identifies one observed OMENative Instance to model as a surge
// relocation. It is only a lookup key; BuildRequest re-establishes every
// ownership, lifecycle, Pod and placement fact from the immutable Snapshot.
type Source struct {
	Namespace        string
	InferenceService string
	Component        v1beta1.ComponentType
	Instance         int32
	FromNode         string
}

type runnerLayout struct {
	single  bool
	workers int32
	total   int32
}

type sourceState struct {
	isvc    *v1beta1.InferenceService
	ir      *v1beta1.InferenceReplica
	row     *v1beta1.OMENativeInstanceStatus
	layout  runnerLayout
	members []podMember
	groups  []*unstructured.Unstructured
}

// BuildRequest constructs a closed scheduling prediction from one validated
// lossless snapshot. It performs no live reads or writes. The source Pods stay
// in ClusterObjects so surge capacity is evaluated while the original Instance
// remains occupied.
func BuildRequest(s *Snapshot, source Source, profiles scheduling.Config, requestID string, now time.Time, maxAge time.Duration) (scheduling.Request, error) {
	if err := s.Validate(now, maxAge); err != nil {
		return scheduling.Request{}, fmt.Errorf("source snapshot: %w", err)
	}
	if err := validateSourceKey(source); err != nil {
		return scheduling.Request{}, err
	}
	if requestID == "" || strings.TrimSpace(requestID) != requestID {
		return scheduling.Request{}, fmt.Errorf("request ID must not be blank or padded")
	}
	if err := profiles.Validate(); err != nil {
		return scheduling.Request{}, fmt.Errorf("scheduling profiles: %w", err)
	}

	state, err := resolveSource(s, source)
	if err != nil {
		return scheduling.Request{}, err
	}
	requireGang := !state.layout.single
	prospective := make([]corev1.PodTemplateSpec, 0, len(state.ir.Spec.Runners))
	for i := range state.ir.Spec.Runners {
		runner := state.ir.Spec.Runners[i]
		if runner.Template.Spec.NodeName != "" {
			return scheduling.Request{}, fmt.Errorf("runner %q template is explicitly pinned to node %q", runner.Name, runner.Template.Spec.NodeName)
		}
		prospective = append(prospective, *runner.Template.DeepCopy())
	}
	initial := scheduling.Select(profiles, prospective, requireGang)
	if initial.Status != scheduling.SelectionReady {
		return scheduling.Request{}, fmt.Errorf("scheduling profile unavailable: %s", initial.Reason)
	}

	replacements, identity, err := cloneMembers(s, source, state.members, requestID)
	if err != nil {
		return scheduling.Request{}, err
	}
	clusterObjects := copyRawExtensions(s.Objects)
	if requireGang {
		var group runtime.RawExtension
		group, err = clonePodGroup(state, identity)
		if err != nil {
			return scheduling.Request{}, err
		}
		clusterObjects = append(clusterObjects, group)
	}
	templates := make([]corev1.PodTemplateSpec, len(replacements))
	for i := range replacements {
		templates[i] = corev1.PodTemplateSpec{ObjectMeta: *replacements[i].ObjectMeta.DeepCopy(), Spec: *replacements[i].Spec.DeepCopy()}
	}
	selected := scheduling.Select(profiles, templates, requireGang)
	if selected.Status != scheduling.SelectionReady {
		return scheduling.Request{}, fmt.Errorf("replacement scheduling profile unavailable: %s", selected.Reason)
	}
	if selected.ProfileIdentity() != initial.ProfileIdentity() {
		return scheduling.Request{}, fmt.Errorf("replacement scheduling profile identity changed after normalization")
	}

	sourcePods := make([]corev1.Pod, len(state.members))
	nodes := make(map[string]struct{})
	for i := range state.members {
		sourcePods[i] = *state.members[i].pod.DeepCopy()
		nodes[state.members[i].pod.Spec.NodeName] = struct{}{}
	}
	excluded := make([]string, 0, len(nodes))
	for node := range nodes {
		excluded = append(excluded, node)
	}
	sort.Strings(excluded)

	// metav1.Time JSON has second precision; keep the original request
	// identical to the envelope returned through the simulator's wire.
	return scheduling.Request{
		SchemaVersion:   scheduling.SimulationSchemaV1,
		RequestID:       requestID,
		Profile:         selected.ProfileIdentity(),
		ReplacementPods: replacements,
		SourcePods:      sourcePods,
		ClusterObjects:  clusterObjects,
		SnapshotID:      s.ID,
		SnapshotTime:    metav1.NewTime(s.CompletedAt.UTC().Truncate(time.Second)),
		RequireGang:     requireGang,
		ExcludedNodes:   excluded,
	}, nil
}

func validateSourceKey(source Source) error {
	if problems := validation.IsDNS1123Label(source.Namespace); len(problems) != 0 {
		return fmt.Errorf("source namespace is invalid")
	}
	if problems := validation.IsDNS1123Subdomain(source.InferenceService); len(problems) != 0 {
		return fmt.Errorf("source InferenceService is invalid")
	}
	switch source.Component {
	case v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent:
	default:
		return fmt.Errorf("source component %q is unsupported", source.Component)
	}
	if source.Instance < 0 {
		return fmt.Errorf("source instance must be non-negative")
	}
	if problems := validation.IsDNS1123Subdomain(source.FromNode); len(problems) != 0 {
		return fmt.Errorf("source FromNode is invalid")
	}
	return nil
}

func resolveSource(s *Snapshot, source Source) (*sourceState, error) {
	state := &sourceState{}
	for i := range s.InferenceServices {
		candidate := &s.InferenceServices[i]
		if candidate.Namespace == source.Namespace && candidate.Name == source.InferenceService {
			if state.isvc != nil {
				return nil, fmt.Errorf("source InferenceService is ambiguous")
			}
			state.isvc = candidate
		}
	}
	if state.isvc == nil || state.isvc.UID == "" {
		return nil, fmt.Errorf("source InferenceService is missing or has no UID")
	}
	if state.isvc.DeletionTimestamp != nil {
		return nil, fmt.Errorf("source InferenceService is deleting")
	}
	if !componentDeclared(state.isvc, source.Component) {
		return nil, fmt.Errorf("source component is not declared")
	}
	component, ok := state.isvc.Status.Components[source.Component]
	if !ok || component.Lifecycle == nil || component.Lifecycle.ObservedGeneration != state.isvc.Generation {
		return nil, fmt.Errorf("InferenceService observation is missing or stale")
	}
	if component.Lifecycle.CurrentRevision == "" || component.Lifecycle.CurrentRevision != component.Lifecycle.UpdateRevision ||
		(component.RolloutPhase != "" && component.RolloutPhase != v1beta1.RolloutPhaseStable) {
		return nil, fmt.Errorf("InferenceService component is not at a stable current revision")
	}

	for i := range s.InferenceReplicas {
		candidate := &s.InferenceReplicas[i]
		if candidate.Namespace == source.Namespace && candidate.Spec.ParentRef.Name == source.InferenceService && candidate.Spec.Component == source.Component {
			if state.ir != nil {
				return nil, fmt.Errorf("source InferenceReplica is ambiguous")
			}
			state.ir = candidate
		}
	}
	if state.ir == nil || state.ir.UID == "" {
		return nil, fmt.Errorf("source InferenceReplica is missing or has no UID")
	}
	if state.ir.DeletionTimestamp != nil {
		return nil, fmt.Errorf("source InferenceReplica is deleting")
	}
	if !controllerOwnerMatches(state.ir.OwnerReferences, v1beta1.SchemeGroupVersion.String(), "InferenceService", state.isvc.Name, state.isvc.UID) {
		return nil, fmt.Errorf("source InferenceReplica owner does not match InferenceService UID")
	}
	if state.ir.Spec.Paused {
		return nil, fmt.Errorf("source InferenceReplica is paused")
	}
	if state.ir.Status.ObservedGeneration != state.ir.Generation {
		return nil, fmt.Errorf("InferenceReplica observation is stale")
	}
	if state.ir.Status.CurrentRevision == "" || state.ir.Status.CurrentRevision != state.ir.Status.UpdateRevision ||
		state.ir.Status.CurrentRevision != component.Lifecycle.CurrentRevision {
		return nil, fmt.Errorf("InferenceReplica revision is not current and stable")
	}
	for i := range state.ir.Status.Migrations {
		migration := &state.ir.Status.Migrations[i]
		if migration.Phase.Terminal() {
			continue
		}
		if migration.SourceInstance == source.Instance || (migration.SurgeInstance != nil && *migration.SurgeInstance == source.Instance) {
			return nil, fmt.Errorf("source instance has an active migration")
		}
	}

	layout, err := validateRunnerLayout(state.ir.Spec.Runners)
	if err != nil {
		return nil, err
	}
	state.layout = layout
	for i := range state.ir.Status.InstanceStatuses {
		row := &state.ir.Status.InstanceStatuses[i]
		if row.Index != source.Instance {
			continue
		}
		if state.row != nil {
			return nil, fmt.Errorf("source instance status is ambiguous")
		}
		state.row = row
	}
	if state.row == nil || state.row.Phase != v1beta1.OMENativeInstanceReady || state.row.Operation != nil {
		return nil, fmt.Errorf("source instance is not Ready and operation-free")
	}
	if state.row.Incarnation <= 0 || state.row.RunningRevision == "" || state.row.TargetRevision != "" ||
		state.row.RunningRevision != state.ir.Status.CurrentRevision {
		return nil, fmt.Errorf("source instance revision or incarnation is invalid")
	}
	if !state.row.Admitted || state.row.PodCount != layout.total || state.row.ServingPodCount != layout.total ||
		state.row.AvailablePodCount != layout.total {
		return nil, fmt.Errorf("source instance counts are not complete")
	}

	if err := state.readObservedObjects(s.Objects, source); err != nil {
		return nil, err
	}
	if err := state.validateMembers(source); err != nil {
		return nil, err
	}
	return state, nil
}

func componentDeclared(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) bool {
	switch component {
	case v1beta1.EngineComponent:
		return isvc.Spec.Engine != nil
	case v1beta1.DecoderComponent:
		return isvc.Spec.Decoder != nil
	case v1beta1.RouterComponent:
		return isvc.Spec.Router != nil
	default:
		return false
	}
}

func validateRunnerLayout(runners []v1beta1.Runner) (runnerLayout, error) {
	if len(runners) == 1 && runners[0].Name == v1beta1.RunnerNameDefault && runners[0].Size == 1 {
		return runnerLayout{single: true, total: 1}, nil
	}
	if len(runners) != 2 {
		return runnerLayout{}, fmt.Errorf("unknown gang runner profile")
	}
	var leaders, workers int32
	for i := range runners {
		switch runners[i].Name {
		case v1beta1.RunnerNameLeader:
			if leaders != 0 || runners[i].Size != 1 {
				return runnerLayout{}, fmt.Errorf("unknown gang leader profile")
			}
			leaders = 1
		case v1beta1.RunnerNameWorker:
			if workers != 0 || runners[i].Size < 1 {
				return runnerLayout{}, fmt.Errorf("unknown gang worker profile")
			}
			workers = runners[i].Size
		default:
			return runnerLayout{}, fmt.Errorf("unknown gang runner type %q", runners[i].Name)
		}
	}
	if leaders != 1 || workers < 1 {
		return runnerLayout{}, fmt.Errorf("unknown gang runner profile")
	}
	return runnerLayout{workers: workers, total: workers + 1}, nil
}

func (state *sourceState) readObservedObjects(objects []runtime.RawExtension, source Source) error {
	for i := range objects {
		raw := objects[i].Raw
		if len(raw) == 0 {
			return fmt.Errorf("snapshot object %d is empty", i)
		}
		var header struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
		}
		if err := json.Unmarshal(raw, &header); err != nil {
			return fmt.Errorf("snapshot object %d is malformed: %w", i, err)
		}
		switch {
		case header.APIVersion == "v1" && header.Kind == "Pod":
			var pod corev1.Pod
			if err := json.Unmarshal(raw, &pod); err != nil {
				return fmt.Errorf("snapshot Pod %d is malformed: %w", i, err)
			}
			claimsSource := pod.Namespace == source.Namespace && pod.Labels[labelInferenceService] == source.InferenceService &&
				pod.Labels[labelComponent] == string(source.Component) && pod.Labels[labelInstanceIndex] == fmt.Sprintf("%d", source.Instance)
			ownedByIR := controllerOwnerMatches(pod.OwnerReferences, v1beta1.SchemeGroupVersion.String(), "InferenceReplica", state.ir.Name, state.ir.UID)
			if claimsSource || ownedByIR {
				index, indexErr := parseNonNegativeInt32(pod.Labels, labelInstanceIndex)
				if claimsSource || (indexErr == nil && index == source.Instance) {
					if !claimsSource || !ownedByIR {
						return fmt.Errorf("source Pod owner or identity labels disagree")
					}
					member, err := state.memberFromPod(pod, source)
					if err != nil {
						return err
					}
					state.members = append(state.members, member)
					continue
				}
			}
			if pod.Spec.NodeName == "" {
				return fmt.Errorf("snapshot contains pending non-request Pod %s/%s", pod.Namespace, pod.Name)
			}
		case header.APIVersion == "scheduling.x-k8s.io/v1alpha1" && header.Kind == "PodGroup":
			var object map[string]any
			if err := json.Unmarshal(raw, &object); err != nil {
				return fmt.Errorf("snapshot PodGroup %d is malformed: %w", i, err)
			}
			state.groups = append(state.groups, &unstructured.Unstructured{Object: object})
		}
	}
	return nil
}

func (state *sourceState) memberFromPod(pod corev1.Pod, source Source) (podMember, error) {
	if pod.Labels[labelManagedBy] != managedByOMENative || !podReady(&pod) {
		return podMember{}, fmt.Errorf("source Pod %s/%s is not a bound Ready OMENative Pod", pod.Namespace, pod.Name)
	}
	incarnation, err := parsePositiveInt64(pod.Labels, labelInstanceIncarnation)
	if err != nil || incarnation != state.row.Incarnation {
		return podMember{}, fmt.Errorf("source Pod incarnation does not match status")
	}
	ordinal, err := parseNonNegativeInt32(pod.Labels, labelPodOrdinal)
	if err != nil {
		return podMember{}, err
	}
	runner := v1beta1.RunnerName(pod.Labels[labelRunner])
	if !revisionMatches(pod.Labels[labelRevisionHash], state.row.RunningRevision) {
		return podMember{}, fmt.Errorf("source Pod revision does not match running revision")
	}
	if err := rejectUnsupportedPodInputs(&pod); err != nil {
		return podMember{}, err
	}
	return podMember{pod: pod, runner: runner, ordinal: ordinal, incarnation: incarnation}, nil
}

func (state *sourceState) validateMembers(source Source) error {
	if len(state.members) != int(state.layout.total) {
		return fmt.Errorf("source does not contain the complete %d-member cohort", state.layout.total)
	}
	seen := make(map[string]bool, len(state.members))
	fromNode := false
	group := ""
	for i := range state.members {
		member := &state.members[i]
		key := fmt.Sprintf("%s/%d", member.runner, member.ordinal)
		if seen[key] {
			return fmt.Errorf("source member role/ordinal is reused")
		}
		seen[key] = true
		if member.pod.Spec.NodeName == source.FromNode {
			fromNode = true
		}
		if state.layout.single {
			if member.runner != v1beta1.RunnerNameDefault || member.ordinal != state.row.ActiveOrdinal || member.pod.Labels[labelPodGroup] != "" {
				return fmt.Errorf("source single-pod member identity is invalid")
			}
		} else {
			candidate := member.pod.Labels[labelPodGroup]
			if candidate == "" || (group != "" && group != candidate) {
				return fmt.Errorf("source gang PodGroup identity is missing or mixed")
			}
			group = candidate
		}
	}
	if !fromNode {
		return fmt.Errorf("source FromNode does not match actual placement")
	}
	if err := validateSourceIdentityConstraints(state.members); err != nil {
		return err
	}
	if state.layout.single {
		return validateObservedTopology(state)
	}
	if !seen[string(v1beta1.RunnerNameLeader)+"/0"] {
		return fmt.Errorf("source gang has no leader member")
	}
	for ordinal := int32(0); ordinal < state.layout.workers; ordinal++ {
		if !seen[fmt.Sprintf("%s/%d", v1beta1.RunnerNameWorker, ordinal)] {
			return fmt.Errorf("source gang is missing worker member %d", ordinal)
		}
	}
	if err := validateObservedPodGroup(state, group); err != nil {
		return err
	}
	if err := validateObservedTopology(state); err != nil {
		return err
	}
	sort.Slice(state.members, func(i, j int) bool {
		if state.members[i].runner != state.members[j].runner {
			return state.members[i].runner < state.members[j].runner
		}
		if state.members[i].ordinal != state.members[j].ordinal {
			return state.members[i].ordinal < state.members[j].ordinal
		}
		return state.members[i].pod.Name < state.members[j].pod.Name
	})
	return nil
}

func validateSourceIdentityConstraints(members []podMember) error {
	replacements := map[string]string{
		labelInstanceIndex:                   "2147483647",
		"source:" + labelInstanceIndex:       members[0].pod.Labels[labelInstanceIndex],
		labelInstanceIncarnation:             "9223372036854775807",
		"source:" + labelInstanceIncarnation: members[0].pod.Labels[labelInstanceIncarnation],
	}
	if group := members[0].pod.Labels[labelPodGroup]; group != "" {
		replacements[labelPodGroup] = "alfred-private-group"
		replacements["source:"+labelPodGroup] = group
	}
	for i := range members {
		copy := members[i].pod.DeepCopy()
		if err := normalizePrivateSelectors(copy, replacements); err != nil {
			return fmt.Errorf("source Pod %s has ambiguous identity constraint: %w", members[i].pod.Name, err)
		}
	}
	return nil
}

func copyRawExtensions(objects []runtime.RawExtension) []runtime.RawExtension {
	out := make([]runtime.RawExtension, len(objects))
	for i := range objects {
		out[i].Raw = append([]byte(nil), objects[i].Raw...)
	}
	return out
}
