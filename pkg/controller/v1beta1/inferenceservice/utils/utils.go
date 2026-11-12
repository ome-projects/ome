package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func LoadingMergedFineTunedWeight(fineTunedWeights []*v1beta1.FineTunedWeight) (bool, error) {
	mergedFineTunedWeights, err := IsMergedFineTunedWeight(fineTunedWeights[0])
	if err != nil {
		return false, err
	}
	return len(fineTunedWeights) == 1 && mergedFineTunedWeights, nil
}

func IsMergedFineTunedWeight(fineTunedWeight *v1beta1.FineTunedWeight) (bool, error) {
	if fineTunedWeight != nil {
		var configMap map[string]interface{}
		if err := json.Unmarshal(fineTunedWeight.Spec.Configuration.Raw, &configMap); err != nil {
			return false, err
		}
		if mergedWeights, exists := configMap[constants.FineTunedWeightMergedWeightsConfigKey]; exists && mergedWeights == true {
			return true, nil
		}
	}
	return false, nil
}

func GetScaledObjectName(isvcName string) string {
	const (
		prefix     = "scaledobject-"
		maxNameLen = 50
	)
	if len(isvcName) > maxNameLen {
		isvcName = isvcName[len(isvcName)-maxNameLen:]
	}
	return fmt.Sprintf("%s%s", prefix, isvcName)
}

// GetValueFromRawExtension extracts a value by key from a JSON-encoded runtime.RawExtension.
// It returns nil if the key does not exist or the data is not a map.
func GetValueFromRawExtension(raw runtime.RawExtension, key string) (interface{}, error) {
	if len(raw.Raw) == 0 {
		return nil, nil
	}

	var data map[string]interface{}
	if err := json.Unmarshal(raw.Raw, &data); err != nil {
		return nil, err
	}

	val, ok := data[key]
	if !ok {
		return nil, nil // or optionally return an error if key must exist
	}

	return val, nil
}

// TargetServicePort returns the port of the Service that fronts the
// InferenceService: the router's Service when hasRouter, else the engine's.
// The caller says which, since a role served through a referenced replica
// has no spec.router to test. Returns the Service's first port; a Service
// that cannot be read, or that declares no port, is an error, so a caller
// never names a port the Service does not declare.
func TargetServicePort(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, hasRouter bool) (int32, error) {
	serviceName := constants.EngineServiceName(isvc.Name)
	if hasRouter {
		serviceName = constants.RouterServiceName(isvc.Name)
	}
	ports, err := servicePorts(ctx, c, isvc.Namespace, serviceName)
	if err != nil {
		return 0, err
	}
	if len(ports) == 0 {
		return 0, fmt.Errorf("service %s/%s declares no port", isvc.Namespace, serviceName)
	}
	return ports[0].Port, nil
}

// servicePorts reads the ports of the named Service. Service names are
// truncated to 63 characters on creation, so the lookup truncates too.
func servicePorts(ctx context.Context, c client.Client, namespace, serviceName string) ([]corev1.ServicePort, error) {
	service := &corev1.Service{}
	key := types.NamespacedName{Name: constants.TruncateNameWithMaxLength(serviceName, 63), Namespace: namespace}
	if err := c.Get(ctx, key, service); err != nil {
		return nil, err
	}
	return service.Spec.Ports, nil
}

// firstPort returns the first port of a Service, or fallback when it has none.
func firstPort(ports []corev1.ServicePort, fallback int32) int32 {
	if len(ports) == 0 {
		return fallback
	}
	return ports[0].Port
}

// MergedRunnerPorts collects the container ports each merged Component's
// effective serving template declares, keyed by Component type. Per-revision
// routing uses the Leader template for a multi-pod shape and the top-level
// template for a single-pod one.
//
// A Component whose merged spec is absent, or whose runner container
// declares no port, is omitted: the caller decides how to degrade rather
// than receiving an invented port.
func MergedRunnerPorts(specs v1beta1.RoleSpecs) map[v1beta1.ComponentType][]corev1.ContainerPort {
	out := make(map[v1beta1.ComponentType][]corev1.ContainerPort, len(v1beta1.Roles))
	for _, view := range specs.Declared() {
		runner, containers := view.Runner, view.PodSpec.Containers
		if view.Leader != nil && workerSpawnsPods(view.Worker) {
			runner, containers = view.Leader.Runner, view.Leader.Containers
		}
		if ports := runnerContainerPorts(runner, containers); len(ports) > 0 {
			out[view.Type] = ports
		}
	}
	return out
}

// MergedServicePortAppProtocols returns each Component's ServicePortAppProtocols
// from the merged spec its stable Service is built from, so a runtime's
// declarations count. A Component that declares nothing is left out.
func MergedServicePortAppProtocols(specs v1beta1.RoleSpecs) map[v1beta1.ComponentType]map[string]string {
	out := make(map[v1beta1.ComponentType]map[string]string, len(v1beta1.Roles))
	for _, view := range specs.Declared() {
		if view.Extension == nil || len(view.Extension.ServicePortAppProtocols) == 0 {
			continue
		}
		out[view.Type] = view.Extension.ServicePortAppProtocols
	}
	return out
}

// runnerContainerPorts returns the ports of the container that serves a
// Component's traffic, resolved the way the pod renderer resolves it: the
// Runner's own ports win, otherwise the ports of the pod container the
// Runner merges into — the one sharing its name, or the first container
// when the Runner is absent or unnamed.
func runnerContainerPorts(runner *v1beta1.RunnerSpec, containers []corev1.Container) []corev1.ContainerPort {
	if runner != nil && len(runner.Container.Ports) > 0 {
		return runner.Container.Ports
	}
	if len(containers) == 0 {
		return nil
	}
	if runner != nil && runner.Container.Name != "" {
		for i := range containers {
			if containers[i].Name == runner.Container.Name {
				return containers[i].Ports
			}
		}
	}
	return containers[0].Ports
}

// ServiceHasPort reports whether the named Service exposes port. It reports
// false when the client is nil or the Service can't be read, so a caller
// never routes to a port it could not confirm.
func ServiceHasPort(ctx context.Context, c client.Client, namespace, serviceName string, port int32) bool {
	if c == nil {
		return false
	}
	ports, err := servicePorts(ctx, c, namespace, serviceName)
	if err != nil {
		return false
	}
	return slices.ContainsFunc(ports, func(p corev1.ServicePort) bool { return p.Port == port })
}

// ResolveServicePort returns the first port of the named Service, which is
// the authoritative serving port a component exposes (Services are built
// from the merged runner's containerPort before ingress runs). It falls
// back to defaultPort when the client is nil or the Service can't be read
// — e.g. during early reconciles before the Service exists.
func ResolveServicePort(ctx context.Context, c client.Client, namespace, serviceName string, defaultPort int32) int32 {
	if c == nil {
		return defaultPort
	}
	ports, err := servicePorts(ctx, c, namespace, serviceName)
	if err != nil {
		return defaultPort
	}
	return firstPort(ports, defaultPort)
}

// AddNodeSelectorForModelReadyNode adds a node selector to the pod spec
// for scheduling pods on nodes where the base model is ready.
//
// Parameters:
//   - podSpec: The pod spec to update (must not be nil)
//   - baseModelMeta: The metadata of the base model (ClusterBaseModel or BaseModel)
//
// The function:
//   - Determines the label key based on whether it's a ClusterBaseModel (empty namespace) or BaseModel
//   - Adds a node selector requiring nodes with "Ready" model status
//   - Skips if the label key already exists in the node selector
func AddNodeSelectorForModelReadyNode(podSpec *corev1.PodSpec, baseModelMeta *metav1.ObjectMeta) {
	if podSpec == nil || baseModelMeta == nil {
		return
	}

	// Determine if this is a ClusterBaseModel or BaseModel based on namespace
	var labelKey string
	isClusterScoped := baseModelMeta.Namespace == ""

	if isClusterScoped {
		// ClusterBaseModel
		labelKey = constants.GetClusterBaseModelLabel(baseModelMeta.Name)
	} else {
		// BaseModel (namespace-scoped)
		labelKey = constants.GetBaseModelLabel(baseModelMeta.Namespace, baseModelMeta.Name)
	}

	// Initialize node selector if nil
	if podSpec.NodeSelector == nil {
		podSpec.NodeSelector = make(map[string]string)
	}

	// Add the label if it doesn't already exist
	if _, exists := podSpec.NodeSelector[labelKey]; !exists {
		podSpec.NodeSelector[labelKey] = "Ready"
	}
}
