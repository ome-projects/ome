package scalermetadata

import (
	"maps"
	"slices"
	"strings"

	"sigs.k8s.io/ome/pkg/constants"
)

type propagatedKeys struct {
	labels      []string
	annotations []string
}

// Track clones rendered Component metadata and records the keys OME owns so
// removed user controls can converge without disturbing metadata injected by
// Kubernetes or another controller.
func Track(labels, annotations map[string]string) (map[string]string, map[string]string) {
	trackedLabels := maps.Clone(labels)
	trackedAnnotations := maps.Clone(annotations)
	if trackedAnnotations == nil {
		trackedAnnotations = map[string]string{}
	}
	keys := propagatedKeys{
		labels:      sortedKeys(trackedLabels, ""),
		annotations: sortedKeys(trackedAnnotations, constants.AutoscalerPropagatedMetadataKeys),
	}
	trackedAnnotations[constants.AutoscalerPropagatedMetadataKeys] = encode(keys)
	return trackedLabels, trackedAnnotations
}

// Contains reports whether every desired managed key has converged. Extra live
// keys are ignored because they may be server- or controller-owned.
func Contains(existingLabels, existingAnnotations, desiredLabels, desiredAnnotations map[string]string) bool {
	return mapContains(existingLabels, desiredLabels) && mapContains(existingAnnotations, desiredAnnotations)
}

// Merge overlays desired managed metadata onto the live maps, removes tracked
// keys omitted from the desired set, and preserves untracked live metadata.
func Merge(existingLabels, existingAnnotations, desiredLabels, desiredAnnotations map[string]string) (map[string]string, map[string]string) {
	mergedLabels := maps.Clone(existingLabels)
	mergedAnnotations := maps.Clone(existingAnnotations)
	previous := decode(existingAnnotations[constants.AutoscalerPropagatedMetadataKeys])
	desired := decode(desiredAnnotations[constants.AutoscalerPropagatedMetadataKeys])

	for _, key := range previous.labels {
		if !slices.Contains(desired.labels, key) {
			delete(mergedLabels, key)
		}
	}
	for _, key := range previous.annotations {
		if !slices.Contains(desired.annotations, key) {
			delete(mergedAnnotations, key)
		}
	}
	for key, value := range desiredLabels {
		if mergedLabels == nil {
			mergedLabels = map[string]string{}
		}
		mergedLabels[key] = value
	}
	for key, value := range desiredAnnotations {
		if mergedAnnotations == nil {
			mergedAnnotations = map[string]string{}
		}
		mergedAnnotations[key] = value
	}
	return mergedLabels, mergedAnnotations
}

func sortedKeys(values map[string]string, excluded string) []string {
	keys := slices.Sorted(maps.Keys(values))
	return slices.DeleteFunc(keys, func(key string) bool { return key == excluded })
}

func encode(keys propagatedKeys) string {
	return strings.Join(keys.labels, ",") + "|" + strings.Join(keys.annotations, ",")
}

func decode(value string) propagatedKeys {
	parts := strings.SplitN(value, "|", 2)
	if len(parts) != 2 {
		return propagatedKeys{}
	}
	return propagatedKeys{
		labels:      splitKeys(parts[0]),
		annotations: splitKeys(parts[1]),
	}
}

func splitKeys(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}

func mapContains(existing, desired map[string]string) bool {
	for key, value := range desired {
		if existing[key] != value {
			return false
		}
	}
	return true
}
