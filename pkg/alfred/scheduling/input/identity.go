package input

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// These are public labels stamped on OMENative Pods. They intentionally live
// outside the controller tree: predictive input construction depends only on
// the public API and the observed Pod contract.
const (
	labelInferenceService    = "ome.io/inferenceservice"
	labelComponent           = "component"
	labelManagedBy           = "ome.io/managed-by"
	labelInstanceIndex       = "ome.io/instance-index"
	labelInstanceIncarnation = "ome.io/instance-incarnation"
	labelRunner              = "ome.io/runner"
	labelRevisionHash        = "ome.io/revision-hash"
	labelPodOrdinal          = "ome.io/pod-ordinal"
	labelPodGroup            = "scheduling.x-k8s.io/pod-group"
	annotationTopologyKey    = "ome.io/topology-key"
	managedByOMENative       = "OMENative"
)

var privateIdentityLabels = map[string]bool{
	labelInstanceIndex:       true,
	labelInstanceIncarnation: true,
	labelPodGroup:            true,
}

type podMember struct {
	pod         corev1.Pod
	runner      v1beta1.RunnerName
	ordinal     int32
	incarnation int64
}

func controllerOwnerMatches(owners []metav1.OwnerReference, apiVersion, kind, name string, uid types.UID) bool {
	var controller *metav1.OwnerReference
	for i := range owners {
		if owners[i].Controller == nil || !*owners[i].Controller {
			continue
		}
		if controller != nil {
			return false
		}
		controller = &owners[i]
	}
	return controller != nil && controller.APIVersion == apiVersion && controller.Kind == kind &&
		controller.Name == name && uid != "" && controller.UID == uid
}

func parseNonNegativeInt32(labels map[string]string, key string) (int32, error) {
	raw, ok := labels[key]
	if !ok || raw == "" {
		return 0, fmt.Errorf("missing %s identity", key)
	}
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("invalid %s identity", key)
	}
	return int32(value), nil
}

func parsePositiveInt64(labels map[string]string, key string) (int64, error) {
	raw, ok := labels[key]
	if !ok || raw == "" {
		return 0, fmt.Errorf("missing %s identity", key)
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("invalid %s identity", key)
	}
	return value, nil
}

func revisionMatches(labelValue, revision string) bool {
	if labelValue == "" || revision == "" {
		return false
	}
	return labelValue == revision || strings.HasSuffix(revision, "-"+labelValue)
}

func podReady(pod *corev1.Pod) bool {
	if pod == nil || pod.UID == "" || pod.Spec.NodeName == "" || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			return pod.Status.Conditions[i].Status == corev1.ConditionTrue
		}
	}
	return false
}

func normalizePrivateSelectors(pod *corev1.Pod, replacements map[string]string) error {
	if pod.Spec.Affinity != nil {
		if pod.Spec.Affinity.PodAffinity != nil {
			for i := range pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
				if err := normalizeAffinityTerm(&pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[i], pod, replacements); err != nil {
					return err
				}
			}
			for i := range pod.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
				if err := normalizeAffinityTerm(&pod.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution[i].PodAffinityTerm, pod, replacements); err != nil {
					return err
				}
			}
		}
		if pod.Spec.Affinity.PodAntiAffinity != nil {
			for i := range pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
				if err := normalizeAffinityTerm(&pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[i], pod, replacements); err != nil {
					return err
				}
			}
			for i := range pod.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
				if err := normalizeAffinityTerm(&pod.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[i].PodAffinityTerm, pod, replacements); err != nil {
					return err
				}
			}
		}
	}
	for i := range pod.Spec.TopologySpreadConstraints {
		constraint := &pod.Spec.TopologySpreadConstraints[i]
		if selectorUsesPrivateIdentity(constraint.LabelSelector, constraint.MatchLabelKeys) {
			if err := validatePrivateSelectorScope(constraint.LabelSelector, constraint.MatchLabelKeys, pod, replacements); err != nil {
				return fmt.Errorf("topology spread identity: %w", err)
			}
		}
		if err := normalizeLabelSelector(constraint.LabelSelector, replacements); err != nil {
			return fmt.Errorf("topology spread identity: %w", err)
		}
		if err := validateDynamicIdentityKeys(constraint.MatchLabelKeys, replacements); err != nil {
			return fmt.Errorf("topology spread matchLabelKeys identity: %w", err)
		}
	}
	return nil
}

func normalizeAffinityTerm(term *corev1.PodAffinityTerm, pod *corev1.Pod, replacements map[string]string) error {
	private := selectorUsesPrivateIdentity(term.LabelSelector, term.MatchLabelKeys, term.MismatchLabelKeys)
	if private && (term.NamespaceSelector != nil || hasForeignNamespace(term.Namespaces, pod.Namespace)) {
		return fmt.Errorf("cross-namespace identity selector is ambiguous")
	}
	if private {
		if err := validatePrivateSelectorScope(term.LabelSelector, term.MatchLabelKeys, pod, replacements); err != nil {
			return fmt.Errorf("affinity identity: %w", err)
		}
	}
	if err := normalizeLabelSelector(term.LabelSelector, replacements); err != nil {
		return fmt.Errorf("affinity identity: %w", err)
	}
	if err := validateDynamicIdentityKeys(term.MatchLabelKeys, replacements); err != nil {
		return fmt.Errorf("affinity matchLabelKeys identity: %w", err)
	}
	if err := validateDynamicIdentityKeys(term.MismatchLabelKeys, replacements); err != nil {
		return fmt.Errorf("affinity mismatchLabelKeys identity: %w", err)
	}
	return nil
}

// Private indices and incarnations are not namespace-wide identities. Only
// rewrite a selector when a positive constraint ties it to this PodGroup or
// this service/component instance. Negative and existence constraints alone
// cannot establish that scope, even if their values equal the source's labels.
func validatePrivateSelectorScope(selector *metav1.LabelSelector, matchKeys []string, pod *corev1.Pod, replacements map[string]string) error {
	selectsPrivate := func(key string) bool {
		value := sourceIdentityValue(replacements, key)
		return value != "" && (selectorSelectsValue(selector, key, value) || slices.Contains(matchKeys, key))
	}
	if selectsPrivate(labelPodGroup) {
		return nil
	}
	if selectorSelectsValue(selector, labelInferenceService, pod.Labels[labelInferenceService]) &&
		selectorSelectsValue(selector, labelComponent, pod.Labels[labelComponent]) && selectsPrivate(labelInstanceIndex) {
		return nil
	}
	return fmt.Errorf("private identity selector is not scoped to the source cohort")
}

func selectorSelectsValue(selector *metav1.LabelSelector, key, value string) bool {
	if selector == nil || value == "" {
		return false
	}
	if selector.MatchLabels[key] == value {
		return true
	}
	for _, requirement := range selector.MatchExpressions {
		if requirement.Key == key && requirement.Operator == metav1.LabelSelectorOpIn &&
			len(requirement.Values) == 1 && requirement.Values[0] == value {
			return true
		}
	}
	return false
}

func selectorUsesPrivateIdentity(selector *metav1.LabelSelector, keySets ...[]string) bool {
	if selector != nil {
		for key := range selector.MatchLabels {
			if privateIdentityLabels[key] {
				return true
			}
		}
		for i := range selector.MatchExpressions {
			if privateIdentityLabels[selector.MatchExpressions[i].Key] {
				return true
			}
		}
	}
	for _, keys := range keySets {
		for _, key := range keys {
			if privateIdentityLabels[key] {
				return true
			}
		}
	}
	return false
}

func hasForeignNamespace(namespaces []string, namespace string) bool {
	// An empty list means the incoming Pod's namespace. A non-empty list is
	// intentionally rejected for private identities: the source snapshot cannot
	// prove that a same-valued identity in another namespace is part of the gang.
	for _, candidate := range namespaces {
		if candidate != namespace {
			return true
		}
	}
	return false
}

func normalizeLabelSelector(selector *metav1.LabelSelector, replacements map[string]string) error {
	if selector == nil {
		return nil
	}
	for key, value := range selector.MatchLabels {
		if !privateIdentityLabels[key] {
			continue
		}
		want, known := replacements[key]
		if !known {
			return fmt.Errorf("identity key %s is not present on the source cohort", key)
		}
		old := sourceIdentityValue(replacements, key)
		if value != old {
			return fmt.Errorf("identity key %s references another cohort", key)
		}
		selector.MatchLabels[key] = want
	}
	for i := range selector.MatchExpressions {
		requirement := &selector.MatchExpressions[i]
		if !privateIdentityLabels[requirement.Key] {
			continue
		}
		if requirement.Operator == metav1.LabelSelectorOpExists || requirement.Operator == metav1.LabelSelectorOpDoesNotExist {
			continue
		}
		want, known := replacements[requirement.Key]
		if !known {
			return fmt.Errorf("identity key %s is not present on the source cohort", requirement.Key)
		}
		old := sourceIdentityValue(replacements, requirement.Key)
		if len(requirement.Values) != 1 || requirement.Values[0] != old {
			return fmt.Errorf("identity key %s has an ambiguous cohort expression", requirement.Key)
		}
		requirement.Values[0] = want
	}
	return nil
}

func validateDynamicIdentityKeys(keys []string, replacements map[string]string) error {
	for _, key := range keys {
		if privateIdentityLabels[key] {
			if _, ok := replacements[key]; !ok {
				return fmt.Errorf("identity key %s is not present on the source cohort", key)
			}
		}
	}
	return nil
}

// replacements holds both the destination value and a private companion entry
// containing the observed source value. Keeping this local prevents callers
// from accidentally applying the mapping to non-selector data.
func sourceIdentityValue(replacements map[string]string, key string) string {
	return replacements["source:"+key]
}
