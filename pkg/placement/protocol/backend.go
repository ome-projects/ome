package protocol

import (
	"fmt"
	"slices"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// IsMember identifies a placement-owned service independently of its execution
// envelope, including an ownership handoff awaiting its first allocation.
func IsMember(service *v1beta1.InferenceService) bool {
	return service != nil && (service.Annotations[constants.PlacementOriginUID] != "" || service.Labels[constants.PlacementOrigin] != "")
}

// ValidateNativeModes requires the backend whose admission and surge are
// observable through InferenceReplicas for every declared component.
func ValidateNativeModes(modes map[v1beta1.ComponentType]constants.DeploymentModeType) error {
	if _, ok := modes[v1beta1.EngineComponent]; !ok {
		return fmt.Errorf("multicluster placement requires a resolved OMENative engine")
	}
	components := make([]v1beta1.ComponentType, 0, len(modes))
	for component := range modes {
		components = append(components, component)
	}
	slices.Sort(components)
	for _, component := range components {
		if modes[component] != constants.OMENative {
			return fmt.Errorf("multicluster placement requires OMENative %s; resolved backend is %q", component, modes[component])
		}
	}
	return nil
}
