package input

import (
	"fmt"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/scheduling"
)

// SourcePlacementTargets joins a complete result to original source Pods by
// runner/ordinal, never result order. Use the evaluated request: a later capture
// may produce different synthetic replacement identities despite equal state.
func SourcePlacementTargets(request scheduling.Request, result scheduling.Result) (map[types.NamespacedName]string, error) {
	if err := scheduling.ValidateResult(request, result); err != nil {
		return nil, err
	}
	if len(request.SourcePods) != len(request.ReplacementPods) {
		return nil, fmt.Errorf("source and replacement cohorts differ")
	}
	type memberKey struct {
		namespace, service, component, runner, ordinal string
	}
	keyFor := func(p *corev1.Pod) (memberKey, error) {
		if p.Labels[labelInferenceService] == "" || p.Labels[labelComponent] == "" || p.Labels[labelRunner] == "" {
			return memberKey{}, fmt.Errorf("pod member identity is incomplete")
		}
		if _, err := parseNonNegativeInt32(p.Labels, labelPodOrdinal); err != nil {
			return memberKey{}, err
		}
		return memberKey{p.Namespace, p.Labels[labelInferenceService], p.Labels[labelComponent], p.Labels[labelRunner], p.Labels[labelPodOrdinal]}, nil
	}
	sources := make(map[memberKey]*corev1.Pod, len(request.SourcePods))
	for i := range request.SourcePods {
		p := &request.SourcePods[i]
		key, err := keyFor(p)
		if err != nil || sources[key] != nil {
			return nil, fmt.Errorf("source cohort identity is ambiguous or incomplete")
		}
		sources[key] = p
	}
	placements := make(map[scheduling.PodIdentity]string, len(result.Placements))
	for _, placement := range result.Placements {
		placements[placement.Pod] = placement.NodeName
	}
	targets := make(map[types.NamespacedName]string, len(sources))
	for i := range request.ReplacementPods {
		p := &request.ReplacementPods[i]
		key, err := keyFor(p)
		source := sources[key]
		if err != nil || source == nil || !reflect.DeepEqual(source.Spec.Containers, p.Spec.Containers) ||
			!reflect.DeepEqual(source.Spec.InitContainers, p.Spec.InitContainers) || !reflect.DeepEqual(source.Spec.Overhead, p.Spec.Overhead) {
			return nil, fmt.Errorf("replacement cohort does not preserve source members")
		}
		delete(sources, key)
		targets[types.NamespacedName{Namespace: source.Namespace, Name: source.Name}] = placements[scheduling.PodIdentity{Namespace: p.Namespace, Name: p.Name, UID: p.UID}]
	}
	if len(sources) != 0 || len(targets) != len(request.SourcePods) {
		return nil, fmt.Errorf("placement does not cover the complete source cohort")
	}
	return targets, nil
}
