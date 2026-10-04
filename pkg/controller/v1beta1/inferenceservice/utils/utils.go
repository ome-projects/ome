package utils

import (
	"context"
	"encoding/json"
	"fmt"

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

// GetTargetServicePort returns the port of the target service (router or engine).
// For raw deployment mode, it uses RouterServiceName/EngineServiceName.
// Returns the port from the service, or constants.CommonISVCPort as default if service lookup fails.
func GetTargetServicePort(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService) (int32, error) {
	return TargetServicePort(ctx, c, isvc, isvc.Spec.Router != nil)
}

// TargetServicePort is GetTargetServicePort with the entrypoint spelled out:
// the router's Service when hasRouter, else the engine's. Callers that serve
// a role through a referenced replica have no spec.router to test.
func TargetServicePort(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, hasRouter bool) (int32, error) {
	var serviceName string
	if hasRouter {
		serviceName = constants.RouterServiceName(isvc.Name)
	} else {
		serviceName = constants.EngineServiceName(isvc.Name)
	}

	// if serviceName reached 63 character, the service name will be truncated during service creation. update name otherwise the service can't found
	serviceName = constants.TruncateNameWithMaxLength(serviceName, 63)

	service := &corev1.Service{}
	if err := c.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: isvc.Namespace}, service); err != nil {
		return 0, err
	}

	port := int32(constants.CommonISVCPort) // default port
	if len(service.Spec.Ports) > 0 {
		port = service.Spec.Ports[0].Port
	}

	return port, nil
}

// MergedRunnerPorts collects the container ports each merged Component's
// effective serving template declares, keyed by Component type. Per-revision
// routing uses the Leader template for multi-pod Engine and Decoder shapes and
// the top-level template for single-pod Components and Router.
//
// A Component whose merged spec is absent, or whose runner container
// declares no port, is omitted: the caller decides how to degrade rather
// than receiving an invented port.
func MergedRunnerPorts(engine *v1beta1.EngineSpec, decoder *v1beta1.DecoderSpec, router *v1beta1.RouterSpec) map[v1beta1.ComponentType][]corev1.ContainerPort {
	out := make(map[v1beta1.ComponentType][]corev1.ContainerPort, 3)
	if engine != nil {
		runner, containers := engine.Runner, engine.Containers
		if engine.Leader != nil && engineSpawnsMultiplePods(engine) {
			runner, containers = engine.Leader.Runner, engine.Leader.Containers
		}
		if ports := runnerContainerPorts(runner, containers); len(ports) > 0 {
			out[v1beta1.EngineComponent] = ports
		}
	}
	if decoder != nil {
		runner, containers := decoder.Runner, decoder.Containers
		if decoder.Leader != nil && decoderSpawnsMultiplePods(decoder) {
			runner, containers = decoder.Leader.Runner, decoder.Leader.Containers
		}
		if ports := runnerContainerPorts(runner, containers); len(ports) > 0 {
			out[v1beta1.DecoderComponent] = ports
		}
	}
	if router != nil {
		if ports := runnerContainerPorts(router.Runner, router.Containers); len(ports) > 0 {
			out[v1beta1.RouterComponent] = ports
		}
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

// ResolveServicePort returns the first port of the named Service, which is
// the authoritative serving port a component exposes (Services are built
// from the merged runner's containerPort before ingress runs). It falls
// back to defaultPort when the client is nil or the Service can't be read
// — e.g. during early reconciles before the Service exists.
func ResolveServicePort(ctx context.Context, c client.Client, namespace, serviceName string, defaultPort int32) int32 {
	if c == nil {
		return defaultPort
	}

	service := &corev1.Service{}
	// Service names are truncated to 63 chars on creation; match that here.
	if err := c.Get(ctx, types.NamespacedName{Name: constants.TruncateNameWithMaxLength(serviceName, 63), Namespace: namespace}, service); err != nil {
		return defaultPort
	}
	if len(service.Spec.Ports) > 0 {
		return service.Spec.Ports[0].Port
	}
	return defaultPort
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
