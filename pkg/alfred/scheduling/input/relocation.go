package input

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

type syntheticIdentity struct {
	index       string
	incarnation string
	group       string
	groupUID    types.UID
	podPrefix   string
	podUID      string
	source      map[string]string
}

func cloneMembers(s *Snapshot, source Source, members []podMember, requestID string) ([]corev1.Pod, syntheticIdentity, error) {
	identity, err := newSyntheticIdentity(s, source, members, requestID)
	if err != nil {
		return nil, syntheticIdentity{}, err
	}
	occupiedNames, occupiedUIDs, err := snapshotIdentities(s.Objects)
	if err != nil {
		return nil, syntheticIdentity{}, err
	}
	if identity.group != "" {
		if occupiedNames[source.Namespace+"/PodGroup/"+identity.group] || occupiedUIDs[identity.groupUID] {
			return nil, syntheticIdentity{}, fmt.Errorf("synthetic PodGroup identity collides with snapshot")
		}
	}

	replacements := make([]corev1.Pod, len(members))
	rewrite := map[string]string{
		labelInstanceIndex:                   identity.index,
		"source:" + labelInstanceIndex:       identity.source[labelInstanceIndex],
		labelInstanceIncarnation:             identity.incarnation,
		"source:" + labelInstanceIncarnation: identity.source[labelInstanceIncarnation],
	}
	if identity.group != "" {
		rewrite[labelPodGroup] = identity.group
		rewrite["source:"+labelPodGroup] = identity.source[labelPodGroup]
	}
	for i := range members {
		pod := members[i].pod.DeepCopy()
		pod.Name = fmt.Sprintf("%s-%d", identity.podPrefix, i)
		pod.GenerateName = ""
		pod.UID = types.UID(fmt.Sprintf("%s-%d", identity.podUID, i))
		if occupiedNames[pod.Namespace+"/Pod/"+pod.Name] || occupiedUIDs[pod.UID] {
			return nil, syntheticIdentity{}, fmt.Errorf("synthetic Pod identity collides with snapshot")
		}
		pod.ResourceVersion = ""
		pod.Generation = 0
		pod.CreationTimestamp = metav1.Time{}
		pod.DeletionTimestamp = nil
		pod.DeletionGracePeriodSeconds = nil
		pod.ManagedFields = nil
		pod.SetSelfLink("")
		pod.Spec.NodeName = ""
		pod.Status = corev1.PodStatus{}
		pod.Labels[labelInstanceIndex] = identity.index
		pod.Labels[labelInstanceIncarnation] = identity.incarnation
		if identity.group != "" {
			pod.Labels[labelPodGroup] = identity.group
		}
		if err := normalizePrivateSelectors(pod, rewrite); err != nil {
			return nil, syntheticIdentity{}, fmt.Errorf("replacement Pod %s identity constraint: %w", members[i].pod.Name, err)
		}
		replacements[i] = *pod
		occupiedNames[pod.Namespace+"/Pod/"+pod.Name] = true
		occupiedUIDs[pod.UID] = true
	}
	return replacements, identity, nil
}

func newSyntheticIdentity(s *Snapshot, source Source, members []podMember, requestID string) (syntheticIdentity, error) {
	seed := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%d\x00%s", s.ID, requestID, source.Namespace, source.InferenceService, source.Instance, source.Component)
	digest := sha256.Sum256([]byte(seed))
	short := hex.EncodeToString(digest[:10])
	index := int64(binary.BigEndian.Uint32(digest[:4]) & math.MaxInt32)
	occupied, err := occupiedInstanceIndexes(s, source)
	if err != nil {
		return syntheticIdentity{}, err
	}
	for attempts := 0; attempts <= len(occupied); attempts++ {
		if !occupied[strconv.FormatInt(index, 10)] {
			break
		}
		index = (index + 1) & math.MaxInt32
		if attempts == len(occupied) {
			return syntheticIdentity{}, fmt.Errorf("no collision-free synthetic instance identity is available")
		}
	}
	incarnation := int64(binary.BigEndian.Uint32(digest[4:8]) & math.MaxInt32)
	if incarnation == members[0].incarnation || incarnation == 0 {
		incarnation++
	}
	identity := syntheticIdentity{
		index: strconv.FormatInt(index, 10), incarnation: strconv.FormatInt(incarnation, 10),
		podPrefix: "alfred-relocation-" + short, podUID: "alfred-relocation-" + hex.EncodeToString(digest[:]),
		source: map[string]string{
			labelInstanceIndex:       members[0].pod.Labels[labelInstanceIndex],
			labelInstanceIncarnation: members[0].pod.Labels[labelInstanceIncarnation],
		},
	}
	if group := members[0].pod.Labels[labelPodGroup]; group != "" {
		identity.group = "alfred-group-" + short
		identity.groupUID = types.UID("alfred-group-" + hex.EncodeToString(digest[:]))
		identity.source[labelPodGroup] = group
	}
	return identity, nil
}

func occupiedInstanceIndexes(s *Snapshot, source Source) (map[string]bool, error) {
	occupied := make(map[string]bool)
	for i := range s.InferenceReplicas {
		ir := &s.InferenceReplicas[i]
		if ir.Namespace != source.Namespace || ir.Spec.ParentRef.Name != source.InferenceService || ir.Spec.Component != source.Component {
			continue
		}
		for j := range ir.Status.InstanceStatuses {
			occupied[strconv.FormatInt(int64(ir.Status.InstanceStatuses[j].Index), 10)] = true
		}
	}
	for i := range s.Objects {
		var header struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
		}
		if err := json.Unmarshal(s.Objects[i].Raw, &header); err != nil {
			return nil, fmt.Errorf("snapshot object %d identity: %w", i, err)
		}
		if header.APIVersion != "v1" || header.Kind != "Pod" {
			continue
		}
		var pod corev1.Pod
		if err := json.Unmarshal(s.Objects[i].Raw, &pod); err != nil {
			return nil, fmt.Errorf("snapshot Pod %d identity: %w", i, err)
		}
		if pod.Namespace == source.Namespace && pod.Labels[labelInferenceService] == source.InferenceService && pod.Labels[labelComponent] == string(source.Component) {
			if value := pod.Labels[labelInstanceIndex]; value != "" {
				occupied[value] = true
			}
		}
	}
	return occupied, nil
}

func snapshotIdentities(objects []runtime.RawExtension) (map[string]bool, map[types.UID]bool, error) {
	names := make(map[string]bool)
	uids := make(map[types.UID]bool)
	for i := range objects {
		var object struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Namespace string    `json:"namespace"`
				Name      string    `json:"name"`
				UID       types.UID `json:"uid"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(objects[i].Raw, &object); err != nil {
			return nil, nil, fmt.Errorf("snapshot object %d identity: %w", i, err)
		}
		if object.Metadata.Name != "" {
			names[object.Metadata.Namespace+"/"+object.Kind+"/"+object.Metadata.Name] = true
		}
		if object.Metadata.UID != "" {
			uids[object.Metadata.UID] = true
		}
	}
	return names, uids, nil
}

func rejectUnsupportedPodInputs(pod *corev1.Pod) error {
	if len(pod.Spec.ResourceClaims) != 0 {
		return fmt.Errorf("source Pod uses unsupported dynamic resource claim")
	}
	for i := range pod.Spec.Containers {
		if len(pod.Spec.Containers[i].Resources.Claims) != 0 {
			return fmt.Errorf("source Pod container uses unsupported dynamic resource claim")
		}
	}
	for i := range pod.Spec.InitContainers {
		if len(pod.Spec.InitContainers[i].Resources.Claims) != 0 {
			return fmt.Errorf("source Pod init container uses unsupported dynamic resource claim")
		}
	}
	for i := range pod.Spec.Volumes {
		volume := &pod.Spec.Volumes[i]
		if volume.PersistentVolumeClaim != nil || volume.Ephemeral != nil || volume.CSI != nil {
			return fmt.Errorf("source Pod uses unsupported storage volume %q", volume.Name)
		}
	}
	return nil
}

func validateObservedPodGroup(state *sourceState, name string) error {
	var group *unstructured.Unstructured
	for _, candidate := range state.groups {
		if candidate.GetNamespace() == state.ir.Namespace && candidate.GetName() == name {
			if group != nil {
				return fmt.Errorf("source PodGroup is ambiguous")
			}
			group = candidate
		}
	}
	if group == nil || group.GetUID() == "" || group.GetDeletionTimestamp() != nil {
		return fmt.Errorf("source PodGroup is missing, deleting, or has no UID")
	}
	if !controllerOwnerMatches(group.GetOwnerReferences(), v1beta1.SchemeGroupVersion.String(), "InferenceReplica", state.ir.Name, state.ir.UID) {
		return fmt.Errorf("source PodGroup owner does not match InferenceReplica UID")
	}
	minMember, found, err := nestedInteger(group.Object, "spec", "minMember")
	if err != nil || !found || minMember != int64(state.layout.total) {
		return fmt.Errorf("source PodGroup does not describe the complete cohort")
	}
	if group.GetLabels()[labelInferenceService] != state.isvc.Name ||
		group.GetLabels()[labelComponent] != string(state.ir.Spec.Component) ||
		group.GetLabels()[labelManagedBy] != managedByOMENative ||
		group.GetLabels()[labelInstanceIndex] != fmt.Sprintf("%d", state.row.Index) {
		return fmt.Errorf("source PodGroup identity labels disagree")
	}
	return nil
}

func nestedInteger(object map[string]any, fields ...string) (int64, bool, error) {
	value, found, err := unstructured.NestedFieldNoCopy(object, fields...)
	if err != nil || !found {
		return 0, found, err
	}
	switch typed := value.(type) {
	case int64:
		return typed, true, nil
	case float64:
		if typed != float64(int64(typed)) {
			return 0, false, fmt.Errorf("field %s is not an integer", strings.Join(fields, "."))
		}
		return int64(typed), true, nil
	default:
		return 0, false, fmt.Errorf("field %s has unsupported number type %T", strings.Join(fields, "."), value)
	}
}

func clonePodGroup(state *sourceState, identity syntheticIdentity) (runtime.RawExtension, error) {
	sourceName := identity.source[labelPodGroup]
	var source *unstructured.Unstructured
	for _, candidate := range state.groups {
		if candidate.GetNamespace() == state.ir.Namespace && candidate.GetName() == sourceName {
			source = candidate
			break
		}
	}
	if source == nil {
		return runtime.RawExtension{}, fmt.Errorf("source PodGroup disappeared from validated state")
	}
	group := source.DeepCopy()
	group.SetName(identity.group)
	group.SetGenerateName("")
	group.SetUID(identity.groupUID)
	group.SetResourceVersion("")
	group.SetGeneration(0)
	group.SetCreationTimestamp(metav1.Time{})
	group.SetDeletionTimestamp(nil)
	group.SetDeletionGracePeriodSeconds(nil)
	group.SetManagedFields(nil)
	labels := group.GetLabels()
	labels[labelInstanceIndex] = identity.index
	group.SetLabels(labels)
	unstructured.RemoveNestedField(group.Object, "status")
	raw, err := json.Marshal(group)
	if err != nil {
		return runtime.RawExtension{}, fmt.Errorf("encode synthetic PodGroup: %w", err)
	}
	return runtime.RawExtension{Raw: raw}, nil
}

func validateObservedTopology(state *sourceState) error {
	configuredKey := ""
	if state.ir.Spec.TopologyKey != nil {
		configuredKey = *state.ir.Spec.TopologyKey
		if configuredKey == "" {
			return fmt.Errorf("configured topology key is empty")
		}
	}
	if state.ir.Spec.TopologySpread != nil && *state.ir.Spec.TopologySpread != v1beta1.TopologySpreadRequired &&
		*state.ir.Spec.TopologySpread != v1beta1.TopologySpreadPreferred {
		return fmt.Errorf("configured topology spread policy is unknown")
	}
	if !state.layout.single {
		groupName := state.members[0].pod.Labels[labelPodGroup]
		var group *unstructured.Unstructured
		for _, candidate := range state.groups {
			if candidate.GetNamespace() == state.ir.Namespace && candidate.GetName() == groupName {
				group = candidate
				break
			}
		}
		if group == nil {
			return fmt.Errorf("source topology PodGroup is missing")
		}
		observedGroupKey := group.GetAnnotations()[annotationTopologyKey]
		if observedGroupKey != configuredKey {
			return fmt.Errorf("configured topology key does not match observed PodGroup topology")
		}
		for i := range state.members {
			member := &state.members[i]
			if member.runner != v1beta1.RunnerNameWorker {
				continue
			}
			matched := 0
			if member.pod.Spec.Affinity != nil && member.pod.Spec.Affinity.PodAffinity != nil {
				for j := range member.pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
					term := &member.pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[j]
					if exactLeaderSelector(term.LabelSelector, state) && term.TopologyKey != configuredKey {
						return fmt.Errorf("configured topology key does not match observed worker topology")
					}
					if term.TopologyKey != configuredKey {
						continue
					}
					if exactLeaderSelector(term.LabelSelector, state) {
						matched++
					} else {
						return fmt.Errorf("observed worker topology affinity is uncertain")
					}
				}
			}
			if (configuredKey == "" && matched != 0) || (configuredKey != "" && matched != 1) {
				return fmt.Errorf("configured topology key does not match observed worker topology")
			}
		}
	}

	configuredSpread := state.ir.Spec.TopologySpread
	spreadKey := ""
	if state.ir.Spec.TopologySpreadKey != nil {
		spreadKey = *state.ir.Spec.TopologySpreadKey
	} else {
		spreadKey = configuredKey
	}
	for i := range state.members {
		member := &state.members[i]
		anchor := member.runner == v1beta1.RunnerNameDefault || member.runner == v1beta1.RunnerNameLeader
		matched := 0
		for j := range member.pod.Spec.TopologySpreadConstraints {
			constraint := &member.pod.Spec.TopologySpreadConstraints[j]
			if isOMEGeneratedSpread(constraint, state, member.runner) {
				if !anchor || configuredSpread == nil || spreadKey == "" || constraint.TopologyKey != spreadKey ||
					constraint.MaxSkew != 1 || constraint.WhenUnsatisfiable != spreadAction(*configuredSpread) {
					return fmt.Errorf("configured topology spread does not match observed Pod constraints")
				}
				matched++
			}
		}
		if anchor && configuredSpread != nil && matched != 1 {
			return fmt.Errorf("configured topology spread is not present on observed anchor Pod")
		}
	}
	if configuredSpread != nil && spreadKey == "" {
		return fmt.Errorf("configured topology spread has no topology key")
	}
	return nil
}

func exactLeaderSelector(selector *metav1.LabelSelector, state *sourceState) bool {
	if selector == nil || len(selector.MatchExpressions) != 0 || len(selector.MatchLabels) != 4 {
		return false
	}
	return selector.MatchLabels[labelInferenceService] == state.isvc.Name &&
		selector.MatchLabels[labelComponent] == string(state.ir.Spec.Component) &&
		selector.MatchLabels[labelInstanceIndex] == fmt.Sprintf("%d", state.row.Index) &&
		selector.MatchLabels[labelRunner] == string(v1beta1.RunnerNameLeader)
}

func isOMEGeneratedSpread(constraint *corev1.TopologySpreadConstraint, state *sourceState, runner v1beta1.RunnerName) bool {
	selector := constraint.LabelSelector
	if selector == nil || len(selector.MatchExpressions) != 0 || len(selector.MatchLabels) != 3 {
		return false
	}
	return selector.MatchLabels[labelInferenceService] == state.isvc.Name &&
		selector.MatchLabels[labelComponent] == string(state.ir.Spec.Component) &&
		selector.MatchLabels[labelRunner] == string(runner)
}

func spreadAction(policy v1beta1.TopologySpreadPolicy) corev1.UnsatisfiableConstraintAction {
	if policy == v1beta1.TopologySpreadRequired {
		return corev1.DoNotSchedule
	}
	return corev1.ScheduleAnyway
}
