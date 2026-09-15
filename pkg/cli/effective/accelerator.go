package effective

import (
	"encoding/json"
	"errors"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

var ErrAcceleratorEvidenceInvalid = errors.New("accelerator evidence is invalid")

type AcceleratorActiveState string

const (
	AcceleratorActiveAvailable    AcceleratorActiveState = "Available"
	AcceleratorActiveUnavailable  AcceleratorActiveState = "Unavailable"
	AcceleratorActiveInconsistent AcceleratorActiveState = "Inconsistent"
)

type AcceleratorBaseState string

const (
	AcceleratorBaseAvailable   AcceleratorBaseState = "Available"
	AcceleratorBaseUnavailable AcceleratorBaseState = "Unavailable"
	AcceleratorBaseInvalid     AcceleratorBaseState = "Invalid"
)

type AcceleratorBaseRequest struct {
	Name     string
	Quantity string
}

type AcceleratorBaseComponent struct {
	Type     v1beta1.ComponentType
	State    AcceleratorBaseState
	Requests []AcceleratorBaseRequest
}

// AcceleratorBaseResolution is a payload-free projection of the pin-aware
// runtime/InferenceService merge. It intentionally excludes selectors,
// environment, arguments, pod metadata, and arbitrary status values.
type AcceleratorBaseResolution struct {
	StatusFreshness               StatusFreshness
	ActiveState                   AcceleratorActiveState
	ActiveOrigin                  ConfigurationOrigin
	ActiveConsistency             RevisionConsistencyState
	ActiveRevisionName            string
	RuntimeName                   string
	RuntimeKind                   string
	RuntimeNamespace              string
	ActiveSourceKind              string
	ActiveSourceName              string
	ActiveSourceNamespace         string
	ActiveSourceUID               string
	ActiveSourceResourceVersion   string
	ActiveSourceGeneration        int64
	RuntimeAcceleratorsConfigured bool
	Components                    []AcceleratorBaseComponent
}

func (AcceleratorBaseResolution) MarshalJSON() ([]byte, error) {
	return nil, ErrUnsafeRuntimeSerialization
}

func (AcceleratorBaseResolution) MarshalYAML() (any, error) {
	return nil, ErrUnsafeRuntimeSerialization
}

func (AcceleratorBaseResolution) String() string {
	return "<effective.AcceleratorBaseResolution redacted>"
}

func (AcceleratorBaseResolution) GoString() string {
	return "<effective.AcceleratorBaseResolution redacted>"
}

// ResolveAcceleratorBase exposes only resource requests from the same active,
// merged component templates used by the runtime diagnostics. It does not
// choose an AcceleratorClass; realized selection remains status authority.
func ResolveAcceleratorBase(
	isvc *v1beta1.InferenceService,
	state *RuntimeState,
) (AcceleratorBaseResolution, error) {
	if isvc == nil || state == nil || isvc.Name == "" || isvc.Namespace == "" ||
		isvc.UID == "" || isvc.ResourceVersion == "" || !state.MatchesInferenceService(isvc) ||
		!knownStatusFreshness(state.StatusFreshness) {
		return AcceleratorBaseResolution{}, ErrAcceleratorEvidenceInvalid
	}
	result := AcceleratorBaseResolution{
		StatusFreshness: state.StatusFreshness,
		ActiveState:     AcceleratorActiveUnavailable,
		Components:      unavailableAcceleratorComponents(isvc),
	}
	active, err := state.RequireActive()
	if err != nil {
		if errors.Is(err, ErrActiveRuntimeUnavailable) {
			return result, nil
		}
		return AcceleratorBaseResolution{}, ErrAcceleratorEvidenceInvalid
	}
	if active.spec == nil || active.RuntimeName == "" || active.RuntimeKind == "" ||
		!knownRevisionConsistency(active.Consistency) {
		return AcceleratorBaseResolution{}, ErrAcceleratorEvidenceInvalid
	}
	result.ActiveOrigin = active.Origin
	result.ActiveConsistency = active.Consistency
	result.ActiveRevisionName = active.RevisionName
	result.RuntimeName = active.RuntimeName
	result.RuntimeKind = active.RuntimeKind
	result.RuntimeNamespace = active.RuntimeNamespace
	if !bindAcceleratorActiveSource(&result, state, active) {
		return AcceleratorBaseResolution{}, ErrAcceleratorEvidenceInvalid
	}
	result.RuntimeAcceleratorsConfigured = active.spec.AcceleratorRequirements != nil &&
		len(active.spec.AcceleratorRequirements.AcceleratorClasses) > 0
	if active.Origin == ConfigurationOriginControllerRevision &&
		active.Consistency != RevisionConsistencyConsistent {
		result.ActiveState = AcceleratorActiveInconsistent
		result.Components = unavailableComponentsFromActive(active.components)
		return result, nil
	}
	if active.Origin != ConfigurationOriginLiveRuntime &&
		active.Origin != ConfigurationOriginControllerRevision {
		return AcceleratorBaseResolution{}, ErrAcceleratorEvidenceInvalid
	}
	result.ActiveState = AcceleratorActiveAvailable
	result.Components = make([]AcceleratorBaseComponent, 0, len(active.components))
	for _, component := range active.components {
		if component.Type == v1beta1.RouterComponent {
			continue
		}
		projected, err := acceleratorBaseComponent(isvc, active.spec, component)
		if err != nil {
			return AcceleratorBaseResolution{}, err
		}
		result.Components = append(result.Components, projected)
	}
	if len(result.Components) == 0 {
		return AcceleratorBaseResolution{}, ErrAcceleratorEvidenceInvalid
	}
	sortAcceleratorBaseComponents(result.Components)
	return result, nil
}

func bindAcceleratorActiveSource(
	result *AcceleratorBaseResolution,
	state *RuntimeState,
	active *ActiveConfiguration,
) bool {
	if result == nil || state == nil || active == nil {
		return false
	}
	switch active.Origin {
	case ConfigurationOriginLiveRuntime:
		live := state.live
		if live == nil || live.Runtime.Name != active.RuntimeName ||
			live.Runtime.Kind != active.RuntimeKind ||
			live.Runtime.Namespace != active.RuntimeNamespace ||
			!live.Runtime.IdentityObserved || live.Runtime.UID == "" ||
			live.Runtime.resourceVersion == "" || live.Runtime.Generation < 0 {
			return false
		}
		result.ActiveSourceKind = live.Runtime.Kind
		result.ActiveSourceName = live.Runtime.Name
		result.ActiveSourceNamespace = live.Runtime.Namespace
		result.ActiveSourceUID = live.Runtime.UID
		result.ActiveSourceResourceVersion = live.Runtime.resourceVersion
		result.ActiveSourceGeneration = live.Runtime.Generation
		return true
	case ConfigurationOriginControllerRevision:
		for i := range state.revisions {
			observation := &state.revisions[i]
			if observation.expectedName != active.RevisionName ||
				!hasAcceleratorRevisionRole(observation.roles, RuntimeRevisionRoleActive) {
				continue
			}
			if !observation.objectReturned || observation.Name != observation.expectedName ||
				observation.Namespace != observation.expectedNamespace || observation.UID == "" ||
				observation.ResourceVersion == "" {
				return false
			}
			result.ActiveSourceKind = "ControllerRevision"
			result.ActiveSourceName = observation.Name
			result.ActiveSourceNamespace = observation.Namespace
			result.ActiveSourceUID = observation.UID
			result.ActiveSourceResourceVersion = observation.ResourceVersion
			return true
		}
		return false
	default:
		return false
	}
}

func hasAcceleratorRevisionRole(roles []RuntimeRevisionRole, want RuntimeRevisionRole) bool {
	for _, role := range roles {
		if role == want {
			return true
		}
	}
	return false
}

// ResolveVirtualAcceleratorBase records that the controller's service-level
// VirtualDeployment early exit makes runtime-derived accelerator requests
// unavailable. It deliberately does not require or synthesize runtime state.
func ResolveVirtualAcceleratorBase(
	isvc *v1beta1.InferenceService,
) (AcceleratorBaseResolution, error) {
	if isvc == nil || isvc.Name == "" || isvc.Namespace == "" || isvc.UID == "" ||
		isvc.ResourceVersion == "" || isvc.Generation <= 0 || !IsServiceVirtualDeployment(isvc) {
		return AcceleratorBaseResolution{}, ErrAcceleratorEvidenceInvalid
	}
	freshness := deriveStatusFreshness(isvc.Generation, isvc.Status.ObservedGeneration)
	if !knownStatusFreshness(freshness) {
		return AcceleratorBaseResolution{}, ErrAcceleratorEvidenceInvalid
	}
	components := unavailableAcceleratorComponents(isvc)
	if len(components) == 0 {
		return AcceleratorBaseResolution{}, ErrAcceleratorEvidenceInvalid
	}
	return AcceleratorBaseResolution{
		StatusFreshness: freshness,
		ActiveState:     AcceleratorActiveUnavailable,
		Components:      components,
	}, nil
}

func acceleratorBaseComponent(
	isvc *v1beta1.InferenceService,
	runtimeSpec *v1beta1.ServingRuntimeSpec,
	component EffectiveComponent,
) (AcceleratorBaseComponent, error) {
	var podSpec *v1beta1.PodSpec
	var runner *v1beta1.RunnerSpec
	var mergeRuntimeResources bool
	switch component.Type {
	case v1beta1.EngineComponent:
		if component.engine == nil || isvc.Spec.Engine == nil {
			return AcceleratorBaseComponent{}, ErrAcceleratorEvidenceInvalid
		}
		podSpec, runner = engineServingTemplate(component.engine)
		mergeRuntimeResources = runnerResourcesUnspecified(isvc.Spec.Engine.Runner)
	case v1beta1.DecoderComponent:
		if component.decoder == nil || isvc.Spec.Decoder == nil {
			return AcceleratorBaseComponent{}, ErrAcceleratorEvidenceInvalid
		}
		podSpec, runner = decoderServingTemplate(component.decoder)
		mergeRuntimeResources = runnerResourcesUnspecified(isvc.Spec.Decoder.Runner)
	default:
		return AcceleratorBaseComponent{}, ErrAcceleratorEvidenceInvalid
	}
	container, err := mergedAcceleratorBaseContainer(
		podSpec, runner, runtimeSpec, mergeRuntimeResources,
	)
	if err != nil {
		return AcceleratorBaseComponent{
			Type: component.Type, State: AcceleratorBaseUnavailable,
			Requests: []AcceleratorBaseRequest{},
		}, nil
	}
	requests, valid := safeAcceleratorRequests(container.Resources.Requests)
	if !valid {
		return AcceleratorBaseComponent{
			Type: component.Type, State: AcceleratorBaseInvalid,
			Requests: []AcceleratorBaseRequest{},
		}, nil
	}
	return AcceleratorBaseComponent{
		Type: component.Type, State: AcceleratorBaseAvailable, Requests: requests,
	}, nil
}

func engineServingTemplate(spec *v1beta1.EngineSpec) (*v1beta1.PodSpec, *v1beta1.RunnerSpec) {
	if spec.Leader != nil {
		return &spec.Leader.PodSpec, spec.Leader.Runner
	}
	return &spec.PodSpec, spec.Runner
}

func decoderServingTemplate(spec *v1beta1.DecoderSpec) (*v1beta1.PodSpec, *v1beta1.RunnerSpec) {
	if spec.Leader != nil {
		return &spec.Leader.PodSpec, spec.Leader.Runner
	}
	return &spec.PodSpec, spec.Runner
}

func runnerResourcesUnspecified(runner *v1beta1.RunnerSpec) bool {
	return runner == nil || runner.Resources.Limits == nil &&
		runner.Resources.Requests == nil && len(runner.Resources.Claims) == 0
}

func safeAcceleratorRequests(resources corev1.ResourceList) ([]AcceleratorBaseRequest, bool) {
	if len(resources) > 64 {
		return []AcceleratorBaseRequest{}, false
	}
	result := make([]AcceleratorBaseRequest, 0, len(resources))
	for name, quantity := range resources {
		if len(validation.IsQualifiedName(string(name))) > 0 || quantity.Sign() < 0 {
			return []AcceleratorBaseRequest{}, false
		}
		canonical, err := resource.ParseQuantity(quantity.String())
		if err != nil || canonical.Sign() < 0 {
			return []AcceleratorBaseRequest{}, false
		}
		result = append(result, AcceleratorBaseRequest{Name: string(name), Quantity: canonical.String()})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, true
}

func unavailableAcceleratorComponents(isvc *v1beta1.InferenceService) []AcceleratorBaseComponent {
	result := []AcceleratorBaseComponent{}
	if isvc.Spec.Engine != nil {
		result = append(result, AcceleratorBaseComponent{
			Type: v1beta1.EngineComponent, State: AcceleratorBaseUnavailable,
			Requests: []AcceleratorBaseRequest{},
		})
	}
	if isvc.Spec.Decoder != nil {
		result = append(result, AcceleratorBaseComponent{
			Type: v1beta1.DecoderComponent, State: AcceleratorBaseUnavailable,
			Requests: []AcceleratorBaseRequest{},
		})
	}
	return result
}

func unavailableComponentsFromActive(components []EffectiveComponent) []AcceleratorBaseComponent {
	result := make([]AcceleratorBaseComponent, 0, len(components))
	for _, component := range components {
		if component.Type == v1beta1.EngineComponent || component.Type == v1beta1.DecoderComponent {
			result = append(result, AcceleratorBaseComponent{
				Type: component.Type, State: AcceleratorBaseUnavailable,
				Requests: []AcceleratorBaseRequest{},
			})
		}
	}
	sortAcceleratorBaseComponents(result)
	return result
}

func sortAcceleratorBaseComponents(components []AcceleratorBaseComponent) {
	sort.Slice(components, func(i, j int) bool {
		return componentOrder(components[i].Type) < componentOrder(components[j].Type)
	})
}

func componentOrder(component v1beta1.ComponentType) int {
	switch component {
	case v1beta1.EngineComponent:
		return 0
	case v1beta1.DecoderComponent:
		return 1
	default:
		return 2
	}
}

var _ json.Marshaler = AcceleratorBaseResolution{}
