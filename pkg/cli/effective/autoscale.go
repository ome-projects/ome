package effective

import (
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	pathvalidation "k8s.io/apimachinery/pkg/api/validation/path"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	controllerautoscaler "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/validation"
)

// ErrAutoscalingEvidenceInvalid indicates that autoscaling resolution was
// asked to combine objects that do not represent one bound API snapshot.
var ErrAutoscalingEvidenceInvalid = errors.New("autoscaling evidence is invalid")

// AutoscalingComponentState describes whether the controller's desired
// autoscaling configuration can be safely explained.
type AutoscalingComponentState string

const (
	AutoscalingComponentAvailable   AutoscalingComponentState = "Available"
	AutoscalingComponentUnavailable AutoscalingComponentState = "Unavailable"
	AutoscalingComponentUnsupported AutoscalingComponentState = "Unsupported"
	AutoscalingComponentInvalid     AutoscalingComponentState = "Invalid"
)

// AutoscalingSpecSource identifies the precedence rung that supplied a
// component autoscaler.
type AutoscalingSpecSource string

const (
	AutoscalingSpecSourceISVC    AutoscalingSpecSource = "isvc"
	AutoscalingSpecSourcePolicy  AutoscalingSpecSource = "policy"
	AutoscalingSpecSourceRuntime AutoscalingSpecSource = "runtime"
	AutoscalingSpecSourceLegacy  AutoscalingSpecSource = "legacy"
	AutoscalingSpecSourceDefault AutoscalingSpecSource = "default"
)

// AutoscalingManagedBy is the expected owner of scaling decisions.
type AutoscalingManagedBy string

const (
	AutoscalingManagedByOME      AutoscalingManagedBy = "ome"
	AutoscalingManagedByExternal AutoscalingManagedBy = "external"
	AutoscalingManagedByNone     AutoscalingManagedBy = "none"
)

// AutoscalingBoundsState records whether dispatch replica bounds are known.
type AutoscalingBoundsState string

const (
	AutoscalingBoundsAvailable   AutoscalingBoundsState = "Available"
	AutoscalingBoundsUnavailable AutoscalingBoundsState = "Unavailable"
	AutoscalingBoundsInvalid     AutoscalingBoundsState = "Invalid"
)

// ScaleToZeroState describes the effective ability to reach zero replicas.
type ScaleToZeroState string

const (
	ScaleToZeroNotRequested ScaleToZeroState = "NotRequested"
	ScaleToZeroEligible     ScaleToZeroState = "Eligible"
	ScaleToZeroUnsupported  ScaleToZeroState = "Unsupported"
	ScaleToZeroUnavailable  ScaleToZeroState = "Unavailable"
	ScaleToZeroInvalid      ScaleToZeroState = "Invalid"
)

// ScalingPolicySource identifies the whole-object scaling-policy precedence
// rung. The ISVC replaces the runtime policy wholesale when present.
type ScalingPolicySource string

const (
	ScalingPolicySourceISVC    ScalingPolicySource = "isvc"
	ScalingPolicySourceRuntime ScalingPolicySource = "runtime"
	ScalingPolicySourceDefault ScalingPolicySource = "default"
)

// ScalingPolicyState describes whether the stored coordination mode has an
// implementation in the current controllers.
type ScalingPolicyState string

const (
	ScalingPolicyAvailable   ScalingPolicyState = "Available"
	ScalingPolicyUnsupported ScalingPolicyState = "Unsupported"
	ScalingPolicyInvalid     ScalingPolicyState = "Invalid"
)

// AutoscalingIssueCode is a bounded, message-free desired-state diagnostic.
type AutoscalingIssueCode string

const (
	AutoscalingIssuePolicyResolutionUnavailable AutoscalingIssueCode = "PolicyResolutionUnavailable"
	AutoscalingIssuePolicyReferenceInvalid      AutoscalingIssueCode = "PolicyReferenceInvalid"
	AutoscalingIssueDeploymentModeUnsupported   AutoscalingIssueCode = "DeploymentModeUnsupported"
	AutoscalingIssueAutoscalerClassInvalid      AutoscalingIssueCode = "AutoscalerClassInvalid"
	AutoscalingIssueKEDATriggersRequired        AutoscalingIssueCode = "KEDATriggersRequired"
	AutoscalingIssueKEDAConfigurationInvalid    AutoscalingIssueCode = "KEDAConfigurationInvalid"
	AutoscalingIssueHPAMetricMalformed          AutoscalingIssueCode = "HPAMetricMalformed"
	AutoscalingIssueKEDAIdleNotBelowMinimum     AutoscalingIssueCode = "KEDAIdleNotBelowMinimum"
	AutoscalingIssueReservedHPANameCollision    AutoscalingIssueCode = "ReservedHPANameCollision"
	AutoscalingIssueLegacyAutoscalerInvalid     AutoscalingIssueCode = "LegacyAutoscalerInvalid"
	AutoscalingIssueReplicaBoundsInvalid        AutoscalingIssueCode = "ReplicaBoundsInvalid"
	AutoscalingIssueScaleToZeroInvalid          AutoscalingIssueCode = "ScaleToZeroInvalid"
	AutoscalingIssueScaleToZeroUnsupported      AutoscalingIssueCode = "ScaleToZeroUnsupported"
	AutoscalingIssueScalingPolicyUnsupported    AutoscalingIssueCode = "ScalingPolicyUnsupported"
	AutoscalingIssueScalingPolicyInvalid        AutoscalingIssueCode = "ScalingPolicyInvalid"
	AutoscalingIssueInheritanceUnavailable      AutoscalingIssueCode = "InheritanceUnavailable"
	AutoscalingIssueActiveRevisionInconsistent  AutoscalingIssueCode = "ActiveRevisionInconsistent"
)

// AutoscalingTargetReference is an allowlisted scale target identity.
type AutoscalingTargetReference struct {
	APIVersion string
	Kind       string
	Namespace  string
	Name       string
}

// AutoscalingBounds contains the exact bounds passed to the selected dispatch
// backend. Pointers preserve zero versus unavailable.
type AutoscalingBounds struct {
	State       AutoscalingBoundsState
	MinReplicas *int32
	MaxReplicas *int32
}

// EffectiveAutoscalingComponent is a payload-free explanation of one merged
// active component. It intentionally exposes only class, counts, bounds, and
// fixed diagnostic codes; HPA metrics and KEDA triggers remain private.
type EffectiveAutoscalingComponent struct {
	Type                 v1beta1.ComponentType
	DeploymentMode       constants.DeploymentModeType
	DeploymentModeSource ComponentDeploymentModeSource
	State                AutoscalingComponentState
	Class                v1beta1.AutoscalerClass
	ManagedBy            AutoscalingManagedBy
	SpecSource           AutoscalingSpecSource
	Target               *AutoscalingTargetReference
	Bounds               AutoscalingBounds
	ScaleToZero          ScaleToZeroState
	MetricCount          int
	TriggerCount         int
	Issues               []AutoscalingIssueCode
	autoscaler           *v1beta1.ComponentAutoscaler
}

// EffectiveScalingPolicy is the active whole-service policy and its support
// state. Empty policy modes normalize to Independent, matching admission.
type EffectiveScalingPolicy struct {
	Mode   v1beta1.ScalingMode
	Source ScalingPolicySource
	State  ScalingPolicyState
	Issues []AutoscalingIssueCode
}

// AutoscalingRuntimeReference is a safe runtime identity without metadata or
// resourceVersion.
type AutoscalingRuntimeReference struct {
	Kind       string
	Namespace  string
	Name       string
	UID        string
	Generation int64
	// IdentityObserved is true only when UID and resourceVersion were bound to
	// the exact live runtime snapshot used for desired-state resolution.
	IdentityObserved bool
}

// AutoscalingModelReference identifies the model snapshot that participated
// in automatic runtime selection. It is absent for explicit runtimes and for
// pinned active revisions, where the independently resolved live model did
// not select the desired configuration being explained.
type AutoscalingModelReference struct {
	Kind       string
	Namespace  string
	Name       string
	UID        string
	Generation int64
}

// AutoscalingRevisionReference identifies the optional active revision.
type AutoscalingRevisionReference struct {
	Namespace        string
	Name             string
	UID              string
	Role             RuntimeRevisionRole
	IdentityObserved bool
}

// AutoscalingActiveConfigurationState makes the absence of an active runtime
// explicit. VirtualDeployment is handled by the controller before runtime
// acquisition, so its desired autoscaling explanation has no active runtime
// configuration by design.
type AutoscalingActiveConfigurationState string

const (
	AutoscalingActiveConfigurationAvailable   AutoscalingActiveConfigurationState = "Available"
	AutoscalingActiveConfigurationUnavailable AutoscalingActiveConfigurationState = "Unavailable"
)

// AutoscalingInheritance carries independently observed source identities.
// It never claims those sources caused a reported status value.
type AutoscalingInheritance struct {
	State             InheritanceObservationState
	UnavailableReason InheritanceUnavailableReason
	Sources           []AutoscalingRuntimeReference
}

// AutoscalingActiveConfiguration identifies the pin-aware configuration used
// for desired-state resolution.
type AutoscalingActiveConfiguration struct {
	State       AutoscalingActiveConfigurationState
	Origin      ConfigurationOrigin
	Consistency RevisionConsistencyState
	Runtime     AutoscalingRuntimeReference
	Revision    *AutoscalingRevisionReference
	Inheritance AutoscalingInheritance
}

// AutoscalingResolution is internal evidence, not an output schema. Direct
// serialization is blocked so future additions cannot leak runtime payloads.
type AutoscalingResolution struct {
	StatusFreshness StatusFreshness
	Model           *AutoscalingModelReference
	Active          AutoscalingActiveConfiguration
	ScalingPolicy   EffectiveScalingPolicy
	Components      []EffectiveAutoscalingComponent
	Issues          []AutoscalingIssueCode
}

func (AutoscalingResolution) MarshalJSON() ([]byte, error) {
	return nil, ErrUnsafeRuntimeSerialization
}

func (AutoscalingResolution) MarshalYAML() (any, error) {
	return nil, ErrUnsafeRuntimeSerialization
}

func (AutoscalingResolution) String() string {
	return "<effective.AutoscalingResolution redacted>"
}

func (AutoscalingResolution) GoString() string {
	return "<effective.AutoscalingResolution redacted>"
}

// IsServiceVirtualDeployment reports the controller's service-level early
// exit. Only a valid top-level annotation triggers this path; component
// annotations and the typed field are resolved later by normal dispatch.
func IsServiceVirtualDeployment(isvc *v1beta1.InferenceService) bool {
	if isvc == nil {
		return false
	}
	mode, found := isvcutils.GetDeploymentModeFromAnnotations(isvc.Annotations)
	return found && mode == constants.VirtualDeployment
}

// ResolveVirtualAutoscaling explains the controller's VirtualDeployment
// early exit without acquiring model, runtime, revision, or child objects.
func ResolveVirtualAutoscaling(isvc *v1beta1.InferenceService) (AutoscalingResolution, error) {
	if isvc == nil || isvc.Name == "" || isvc.Namespace == "" || isvc.UID == "" ||
		isvc.ResourceVersion == "" || !IsServiceVirtualDeployment(isvc) {
		return AutoscalingResolution{}, ErrAutoscalingEvidenceInvalid
	}
	result := AutoscalingResolution{
		StatusFreshness: deriveStatusFreshness(isvc.Generation, isvc.Status.ObservedGeneration),
		Active:          AutoscalingActiveConfiguration{State: AutoscalingActiveConfigurationUnavailable},
		ScalingPolicy:   resolveScalingPolicy(isvc, nil),
		Components:      []EffectiveAutoscalingComponent{},
		Issues:          []AutoscalingIssueCode{},
	}
	legacyAdmissionInvalid := invalidLegacyAutoscalerAdmissionState(isvc)
	appendComponent := func(componentType v1beta1.ComponentType, ext *v1beta1.ComponentExtensionSpec) {
		scaleToZero := ScaleToZeroNotRequested
		if ext != nil && (ext.MinReplicas != nil && *ext.MinReplicas == 0 ||
			ext.Autoscaler != nil && ext.Autoscaler.Class == v1beta1.AutoscalerKEDA && ext.Autoscaler.Keda != nil &&
				ext.Autoscaler.Keda.IdleReplicaCount != nil && *ext.Autoscaler.Keda.IdleReplicaCount == 0) {
			scaleToZero = ScaleToZeroUnsupported
		}
		component := EffectiveAutoscalingComponent{
			Type: componentType, DeploymentMode: constants.VirtualDeployment,
			DeploymentModeSource: DeploymentModeServiceAnnotation,
			State:                AutoscalingComponentUnsupported, Bounds: AutoscalingBounds{State: AutoscalingBoundsUnavailable},
			ScaleToZero: scaleToZero, Issues: []AutoscalingIssueCode{AutoscalingIssueDeploymentModeUnsupported},
		}
		switch {
		case legacyAdmissionInvalid:
			component.Issues = nil
			invalidateAutoscalingComponent(&component, AutoscalingIssueLegacyAutoscalerInvalid)
		case isvcAutoscalerAdmissionIssue(ext) != "":
			component.SpecSource = AutoscalingSpecSourceISVC
			component.Issues = nil
			invalidateAutoscalingComponent(&component, isvcAutoscalerAdmissionIssue(ext))
		case ext != nil && invalidAutoscalerPolicyReference(ext.AutoscalerPolicyRef):
			component.SpecSource = AutoscalingSpecSourcePolicy
			component.Issues = nil
			invalidateAutoscalingComponent(&component, AutoscalingIssuePolicyReferenceInvalid)
		case invalidISVCReplicaBounds(ext):
			component.Issues = nil
			invalidateAutoscalingComponent(&component, AutoscalingIssueReplicaBoundsInvalid)
		case ext != nil && ext.MinReplicas != nil && *ext.MinReplicas == 0 &&
			!isvcScaleToZeroAdmissionGate(isvc, componentType):
			component.Issues = nil
			invalidateAutoscalingComponent(&component, AutoscalingIssueScaleToZeroInvalid)
		}
		result.Components = append(result.Components, component)
	}
	if isvc.Spec.Engine != nil {
		appendComponent(v1beta1.EngineComponent, &isvc.Spec.Engine.ComponentExtensionSpec)
	} else {
		appendComponent(v1beta1.EngineComponent, nil)
	}
	if isvc.Spec.Decoder != nil {
		appendComponent(v1beta1.DecoderComponent, &isvc.Spec.Decoder.ComponentExtensionSpec)
	}
	if isvc.Spec.Router != nil {
		appendComponent(v1beta1.RouterComponent, &isvc.Spec.Router.ComponentExtensionSpec)
	}
	result.Issues = canonicalAutoscalingIssues(append(result.Issues, result.ScalingPolicy.Issues...))
	return result, nil
}

// ResolveAutoscaling resolves desired autoscaling against the pin-aware active
// runtime configuration. It does not read child autoscalers or policy objects.
// Policy rendering depends on controller-local bindings and is therefore
// reported unavailable whenever a policy ref is the winning precedence rung.
func ResolveAutoscaling(isvc *v1beta1.InferenceService, state *RuntimeState) (AutoscalingResolution, error) {
	if isvc == nil || state == nil || isvc.Name == "" || isvc.Namespace == "" ||
		isvc.UID == "" || isvc.ResourceVersion == "" || !state.MatchesInferenceService(isvc) {
		return AutoscalingResolution{}, ErrAutoscalingEvidenceInvalid
	}
	active, err := state.RequireActive()
	if err != nil {
		return AutoscalingResolution{}, err
	}
	if active.spec == nil || active.RuntimeName == "" || active.RuntimeKind == "" ||
		!knownRevisionConsistency(active.Consistency) || !knownStatusFreshness(state.StatusFreshness) {
		return AutoscalingResolution{}, ErrAutoscalingEvidenceInvalid
	}

	result := AutoscalingResolution{
		StatusFreshness: state.StatusFreshness,
		Active: AutoscalingActiveConfiguration{
			State: AutoscalingActiveConfigurationAvailable, Origin: active.Origin, Consistency: active.Consistency,
			Runtime: AutoscalingRuntimeReference{
				Kind: active.RuntimeKind, Namespace: active.RuntimeNamespace, Name: active.RuntimeName,
			},
			Inheritance: autoscalingInheritance(state, active),
		},
		ScalingPolicy: resolveScalingPolicy(isvc, active.spec),
		Components:    make([]EffectiveAutoscalingComponent, 0, len(active.components)),
		Issues:        []AutoscalingIssueCode{},
	}
	if active.Origin == ConfigurationOriginLiveRuntime {
		live := state.LiveConfiguration()
		if live != nil && live.Runtime.Name == active.RuntimeName &&
			live.Runtime.Kind == active.RuntimeKind && live.Runtime.Namespace == active.RuntimeNamespace &&
			live.Runtime.IdentityObserved {
			result.Active.Runtime.UID = live.Runtime.UID
			result.Active.Runtime.Generation = live.Runtime.Generation
			result.Active.Runtime.IdentityObserved = true
		}
		if live != nil && live.Runtime.SelectionSource == RuntimeSelected && live.Model != nil {
			result.Model = &AutoscalingModelReference{
				Kind: live.Model.Kind, Namespace: live.Model.Namespace, Name: live.Model.Name,
				UID: string(live.Model.UID), Generation: live.Model.Generation,
			}
		}
	}
	if result.Active.Inheritance.State == InheritanceUnavailable {
		result.Issues = append(result.Issues, AutoscalingIssueInheritanceUnavailable)
	}
	if active.Origin == ConfigurationOriginControllerRevision && active.Consistency != RevisionConsistencyConsistent {
		result.Issues = append(result.Issues, AutoscalingIssueActiveRevisionInconsistent)
	}
	result.Active.Revision = activeRevisionReference(state, active)
	legacyAdmissionInvalid := invalidLegacyAutoscalerAdmissionState(isvc)

	runtimeMinimumSourceLocal := activeRuntimeHasOneDeclaredSource(state, active)
	for _, component := range active.components {
		resolved, err := resolveAutoscalingComponent(
			isvc, active.spec, component, legacyAdmissionInvalid, runtimeMinimumSourceLocal,
		)
		if err != nil {
			return AutoscalingResolution{}, err
		}
		result.Components = append(result.Components, resolved)
	}
	if len(result.Components) == 0 {
		return AutoscalingResolution{}, ErrAutoscalingEvidenceInvalid
	}
	result.Issues = append(result.Issues, result.ScalingPolicy.Issues...)
	result.Issues = canonicalAutoscalingIssues(result.Issues)
	return result, nil
}

func resolveAutoscalingComponent(
	isvc *v1beta1.InferenceService,
	runtimeSpec *v1beta1.ServingRuntimeSpec,
	component EffectiveComponent,
	legacyAdmissionInvalid bool,
	runtimeMinimumSourceLocal bool,
) (EffectiveAutoscalingComponent, error) {
	ext, annotations, err := effectiveComponentInputs(component)
	if err != nil {
		return EffectiveAutoscalingComponent{}, err
	}
	target := autoscalingTarget(isvc, component)
	result := EffectiveAutoscalingComponent{
		Type: component.Type, DeploymentMode: component.DeploymentMode,
		DeploymentModeSource: component.DeploymentModeSource,
		State:                AutoscalingComponentAvailable, Target: target,
		Bounds:      AutoscalingBounds{State: AutoscalingBoundsUnavailable},
		ScaleToZero: ScaleToZeroNotRequested, Issues: []AutoscalingIssueCode{},
	}
	if legacyAdmissionInvalid {
		invalidateAutoscalingComponent(&result, AutoscalingIssueLegacyAutoscalerInvalid)
		return result, nil
	}
	if issue := runtimeAutoscalerAdmissionIssue(runtimeSpec, component.Type, runtimeMinimumSourceLocal); issue != "" {
		result.SpecSource = AutoscalingSpecSourceRuntime
		invalidateAutoscalingComponent(&result, issue)
		return result, nil
	}

	inline := isvcComponentExtension(isvc, component.Type)
	if invalidISVCReplicaBounds(inline) {
		invalidateAutoscalingComponent(&result, AutoscalingIssueReplicaBoundsInvalid)
		return result, nil
	}
	policyRef := controllerautoscaler.ComponentPolicyRef(isvc, component.Type)
	// Admission validates the reference shape independently of precedence and
	// deployment-mode dispatch. A malformed stored ref therefore cannot be
	// hidden by an inline autoscaler or by a mode that has no autoscaler
	// backend. Do not fetch or expose the referenced policy payload here.
	if invalidAutoscalerPolicyReference(policyRef) {
		result.State = AutoscalingComponentInvalid
		result.Target = nil
		result.SpecSource = AutoscalingSpecSourcePolicy
		result.ScaleToZero = ScaleToZeroUnavailable
		result.Issues = []AutoscalingIssueCode{AutoscalingIssuePolicyReferenceInvalid}
		return result, nil
	}
	policyWins := inline != nil && inline.Autoscaler == nil && policyRef != nil &&
		autoscalerDispatchSupported(component.DeploymentMode)
	if policyWins {
		result.State = AutoscalingComponentUnavailable
		result.Target = nil
		result.SpecSource = AutoscalingSpecSourcePolicy
		result.Issues = append(result.Issues, AutoscalingIssuePolicyResolutionUnavailable)
		result.Bounds = resolveAutoscalingBounds(component.DeploymentMode, ext)
		if isvcExplicitlyRequestsZero(isvc, component.Type) && !isvcScaleToZeroAdmissionGate(isvc, component.Type) {
			invalidateAutoscalingComponent(&result, AutoscalingIssueScaleToZeroInvalid)
			return result, nil
		}
		if result.Bounds.State == AutoscalingBoundsInvalid {
			invalidateAutoscalingComponent(&result, AutoscalingIssueReplicaBoundsInvalid)
			return result, nil
		}
		// The rendered policy block is intentionally not acquired by this
		// read-only command. It may select KEDA idleReplicaCount=0 even when
		// the visible minimum is positive, so absence of a visible zero is not
		// evidence that scale-to-zero was not requested.
		result.ScaleToZero = ScaleToZeroUnavailable
		result.Issues = canonicalAutoscalingIssues(result.Issues)
		return result, nil
	}

	legacyAnnotations := effectiveLegacyAnnotations(isvc.Annotations, annotations)
	var resolved *v1beta1.ComponentAutoscaler
	var source controllerautoscaler.SpecSource
	if component.DeploymentMode == constants.RawDeployment {
		resolved, source, err = controllerautoscaler.ResolveRawComponentAutoscaler(
			runtimeSpec, isvc, component.Type, legacyAnnotations,
		)
		if err != nil {
			result.State = AutoscalingComponentInvalid
			result.Target = nil
			result.SpecSource = AutoscalingSpecSourceLegacy
			result.ScaleToZero = ScaleToZeroInvalid
			result.Bounds = AutoscalingBounds{State: AutoscalingBoundsInvalid}
			result.Issues = []AutoscalingIssueCode{AutoscalingIssueLegacyAutoscalerInvalid}
			return result, nil
		}
	} else {
		resolved, source = controllerautoscaler.ResolveComponentAutoscaler(runtimeSpec, isvc, component.Type)
	}
	result.SpecSource, err = mapAutoscalingSpecSource(source)
	if err != nil {
		return EffectiveAutoscalingComponent{}, err
	}
	if resolved == nil {
		return EffectiveAutoscalingComponent{}, ErrAutoscalingEvidenceInvalid
	}
	result.autoscaler = resolved.DeepCopy()
	result.Class = resolved.Class
	result.ManagedBy = managedByForClass(resolved.Class)
	result.MetricCount, result.TriggerCount = effectiveAutoscalerCounts(resolved)

	// Preserve admission's source-local validation before checking the merged
	// dispatch shape. An inline min=0/idle=0 pair is rejected at ISVC
	// admission, while a runtime autoscaler is validated against the runtime's
	// own declared minimum rather than an ISVC override layered above it.
	sourceMinimum, sourceValidated, err := autoscalerSourceMinimum(source, isvc, component.Type)
	if err != nil {
		return EffectiveAutoscalingComponent{}, err
	}
	if sourceValidated {
		if issue := validateResolvedAutoscaler(resolved, sourceMinimum); issue != "" {
			invalidateAutoscalingComponent(&result, issue)
			return result, nil
		}
	}

	result.Bounds = resolveAutoscalingBounds(component.DeploymentMode, ext)
	if result.Bounds.State == AutoscalingBoundsInvalid {
		invalidateAutoscalingComponent(&result, AutoscalingIssueReplicaBoundsInvalid)
		return result, nil
	}
	if result.Bounds.State == AutoscalingBoundsAvailable {
		minimum := int(*result.Bounds.MinReplicas)
		if issue := validateResolvedAutoscaler(resolved, &minimum); issue != "" {
			invalidateAutoscalingComponent(&result, issue)
			return result, nil
		}
	} else if !sourceValidated {
		// Unsupported dispatch modes do not have generated bounds, but the
		// resolved default/legacy block must still be structurally explainable.
		if issue := validateResolvedAutoscaler(resolved, ext.MinReplicas); issue != "" {
			invalidateAutoscalingComponent(&result, issue)
			return result, nil
		}
	}

	if target != nil && reservedHPANameCollision(resolved, target.Name) {
		invalidateAutoscalingComponent(&result, AutoscalingIssueReservedHPANameCollision)
		return result, nil
	}
	if result.Bounds.State == AutoscalingBoundsAvailable {
		if issue := validateDispatchedAutoscaler(resolved, result.Bounds, target); issue != "" {
			invalidateAutoscalingComponent(&result, issue)
			return result, nil
		}
	}

	result.ScaleToZero = resolveScaleToZero(isvc, component.Type, component.DeploymentMode, ext, resolved)
	if result.ScaleToZero == ScaleToZeroInvalid {
		invalidateAutoscalingComponent(&result, AutoscalingIssueScaleToZeroInvalid)
		return result, nil
	}
	if result.ScaleToZero == ScaleToZeroUnsupported {
		result.Issues = append(result.Issues, AutoscalingIssueScaleToZeroUnsupported)
	}

	if !autoscalerDispatchSupported(component.DeploymentMode) {
		result.State = AutoscalingComponentUnsupported
		result.Target = nil
		result.Bounds = AutoscalingBounds{State: AutoscalingBoundsUnavailable}
		result.Issues = append(result.Issues, AutoscalingIssueDeploymentModeUnsupported)
	}
	result.Issues = canonicalAutoscalingIssues(result.Issues)
	return result, nil
}

func autoscalerSourceMinimum(
	source controllerautoscaler.SpecSource,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
) (*int, bool, error) {
	switch source {
	case controllerautoscaler.SpecSourceISVC:
		ext := isvcComponentExtension(isvc, component)
		if ext == nil || ext.Autoscaler == nil {
			return nil, false, ErrAutoscalingEvidenceInvalid
		}
		return ext.MinReplicas, true, nil
	default:
		// The effective runtime spec is inheritance-flattened, so its minimum
		// may have been authored on a different runtime than its autoscaler.
		// The original source-local pairing is no longer available here. The
		// resolved block is validated below against the actual dispatch bound.
		return nil, false, nil
	}
}

func runtimeAutoscalerAdmissionIssue(
	runtimeSpec *v1beta1.ServingRuntimeSpec,
	component v1beta1.ComponentType,
	minimumSourceLocal bool,
) AutoscalingIssueCode {
	ext := runtimeComponentExtension(runtimeSpec, component)
	if ext == nil {
		return ""
	}
	if ext.AutoscalerPolicyRef != nil {
		return AutoscalingIssuePolicyReferenceInvalid
	}
	// An inheritance-flattened minimum may have been authored on a different
	// runtime than its autoscaler. Pair the values only when the independently
	// observed declaration chain proves the active runtime has one source.
	var minimum *int
	if minimumSourceLocal {
		minimum = ext.MinReplicas
	}
	return validateResolvedAutoscaler(ext.Autoscaler, minimum)
}

func activeRuntimeHasOneDeclaredSource(state *RuntimeState, active *ActiveConfiguration) bool {
	if state == nil || active == nil || active.Origin != ConfigurationOriginLiveRuntime {
		return false
	}
	live := state.LiveConfiguration()
	if live == nil || live.Runtime.Name != active.RuntimeName || live.Runtime.Kind != active.RuntimeKind ||
		live.Runtime.Namespace != active.RuntimeNamespace ||
		live.Runtime.DeclaredInheritance.State() != InheritanceObserved {
		return false
	}
	return len(live.Runtime.DeclaredInheritance.Chain()) == 1
}

func isvcAutoscalerAdmissionIssue(ext *v1beta1.ComponentExtensionSpec) AutoscalingIssueCode {
	if ext == nil {
		return ""
	}
	return validateResolvedAutoscaler(ext.Autoscaler, ext.MinReplicas)
}

func invalidAutoscalerPolicyReference(ref *v1beta1.AutoscalerPolicyRef) bool {
	return ref != nil && (ref.Name == "" ||
		(ref.Kind != "" && ref.Kind != constants.AutoscalerPolicyKind))
}

func runtimeComponentExtension(
	runtimeSpec *v1beta1.ServingRuntimeSpec,
	component v1beta1.ComponentType,
) *v1beta1.ComponentExtensionSpec {
	if runtimeSpec == nil {
		return nil
	}
	switch component {
	case v1beta1.EngineComponent:
		if runtimeSpec.EngineConfig != nil {
			return &runtimeSpec.EngineConfig.ComponentExtensionSpec
		}
	case v1beta1.DecoderComponent:
		if runtimeSpec.DecoderConfig != nil {
			return &runtimeSpec.DecoderConfig.ComponentExtensionSpec
		}
	case v1beta1.RouterComponent:
		if runtimeSpec.RouterConfig != nil {
			return &runtimeSpec.RouterConfig.ComponentExtensionSpec
		}
	}
	return nil
}

func invalidLegacyAutoscalerAdmissionState(isvc *v1beta1.InferenceService) bool {
	if _, err := validation.ValidateAutoscalerConfig(isvc); err != nil {
		return true
	}
	if err := validation.ValidateAutoscalerAnnotationConflict(isvc); err != nil {
		return true
	}
	return validation.ValidateAutoscalerTargetUtilizationPercentage(isvc) != nil
}

func invalidateAutoscalingComponent(component *EffectiveAutoscalingComponent, issue AutoscalingIssueCode) {
	component.State = AutoscalingComponentInvalid
	component.Target = nil
	component.Bounds = AutoscalingBounds{State: AutoscalingBoundsInvalid}
	component.ScaleToZero = ScaleToZeroInvalid
	component.Issues = canonicalAutoscalingIssues(append(component.Issues, issue))
}

func invalidISVCReplicaBounds(ext *v1beta1.ComponentExtensionSpec) bool {
	if ext == nil || !replicaBoundsFitInt32(ext) {
		return ext != nil
	}
	var max *int
	if ext.MaxReplicas != 0 {
		max = &ext.MaxReplicas
	}
	return validation.ValidateReplicaBounds(ext.MinReplicas, max) != nil
}

func effectiveComponentInputs(component EffectiveComponent) (*v1beta1.ComponentExtensionSpec, map[string]string, error) {
	switch component.Type {
	case v1beta1.EngineComponent:
		if component.engine == nil {
			return nil, nil, ErrAutoscalingEvidenceInvalid
		}
		return &component.engine.ComponentExtensionSpec, component.engine.Annotations, nil
	case v1beta1.DecoderComponent:
		if component.decoder == nil {
			return nil, nil, ErrAutoscalingEvidenceInvalid
		}
		return &component.decoder.ComponentExtensionSpec, component.decoder.Annotations, nil
	case v1beta1.RouterComponent:
		if component.router == nil {
			return nil, nil, ErrAutoscalingEvidenceInvalid
		}
		return &component.router.ComponentExtensionSpec, component.router.Annotations, nil
	default:
		return nil, nil, ErrAutoscalingEvidenceInvalid
	}
}

func isvcComponentExtension(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) *v1beta1.ComponentExtensionSpec {
	if isvc == nil {
		return nil
	}
	switch component {
	case v1beta1.EngineComponent:
		if isvc.Spec.Engine != nil {
			return &isvc.Spec.Engine.ComponentExtensionSpec
		}
	case v1beta1.DecoderComponent:
		if isvc.Spec.Decoder != nil {
			return &isvc.Spec.Decoder.ComponentExtensionSpec
		}
	case v1beta1.RouterComponent:
		if isvc.Spec.Router != nil {
			return &isvc.Spec.Router.ComponentExtensionSpec
		}
	}
	return nil
}

func effectiveLegacyAnnotations(service, component map[string]string) map[string]string {
	result := map[string]string{}
	for _, key := range []string{constants.AutoscalerClass, constants.TargetUtilizationPercentage} {
		if value, found := service[key]; found {
			result[key] = value
		}
		if value, found := component[key]; found {
			result[key] = value
		}
	}
	return result
}

func mapAutoscalingSpecSource(source controllerautoscaler.SpecSource) (AutoscalingSpecSource, error) {
	switch source {
	case controllerautoscaler.SpecSourceISVC:
		return AutoscalingSpecSourceISVC, nil
	case controllerautoscaler.SpecSourcePolicy:
		return AutoscalingSpecSourcePolicy, nil
	case controllerautoscaler.SpecSourceRuntime:
		return AutoscalingSpecSourceRuntime, nil
	case controllerautoscaler.SpecSourceLegacy:
		return AutoscalingSpecSourceLegacy, nil
	case controllerautoscaler.SpecSourceDefault:
		return AutoscalingSpecSourceDefault, nil
	default:
		return "", ErrAutoscalingEvidenceInvalid
	}
}

func managedByForClass(class v1beta1.AutoscalerClass) AutoscalingManagedBy {
	switch class {
	case v1beta1.AutoscalerHPA, v1beta1.AutoscalerKEDA:
		return AutoscalingManagedByOME
	case v1beta1.AutoscalerExternal:
		return AutoscalingManagedByExternal
	case v1beta1.AutoscalerNone:
		return AutoscalingManagedByNone
	default:
		return ""
	}
}

func effectiveAutoscalerCounts(autoscaler *v1beta1.ComponentAutoscaler) (int, int) {
	if autoscaler == nil {
		return 0, 0
	}
	switch autoscaler.Class {
	case v1beta1.AutoscalerHPA:
		if autoscaler.HPA == nil || len(autoscaler.HPA.Metrics) == 0 {
			return 1, 0
		}
		return len(autoscaler.HPA.Metrics), 0
	case v1beta1.AutoscalerKEDA:
		if autoscaler.Keda != nil {
			return 0, len(autoscaler.Keda.Triggers)
		}
	}
	return 0, 0
}

func validateResolvedAutoscaler(autoscaler *v1beta1.ComponentAutoscaler, minReplicas *int) AutoscalingIssueCode {
	if autoscaler == nil {
		return ""
	}
	if err := validation.ValidateAutoscaler(autoscaler, minReplicas); err == nil {
		return ""
	}
	switch autoscaler.Class {
	case v1beta1.AutoscalerKEDA:
		if autoscaler.Keda == nil || len(autoscaler.Keda.Triggers) == 0 {
			return AutoscalingIssueKEDATriggersRequired
		}
		return AutoscalingIssueKEDAIdleNotBelowMinimum
	case v1beta1.AutoscalerHPA:
		return AutoscalingIssueHPAMetricMalformed
	default:
		return AutoscalingIssueAutoscalerClassInvalid
	}
}

// validateDispatchedAutoscaler mirrors the deterministic validation performed
// by the child API that receives the selected configuration. It is called only
// for the winning, supported dispatch block: an unused opposite-class or
// shadowed runtime payload never affects controller dispatch.
func validateDispatchedAutoscaler(
	autoscaler *v1beta1.ComponentAutoscaler,
	bounds AutoscalingBounds,
	target *AutoscalingTargetReference,
) AutoscalingIssueCode {
	if autoscaler == nil {
		return AutoscalingIssueAutoscalerClassInvalid
	}
	switch autoscaler.Class {
	case v1beta1.AutoscalerHPA:
		if autoscaler.HPA != nil && !validHPAPayload(autoscaler.HPA) {
			return AutoscalingIssueHPAMetricMalformed
		}
	case v1beta1.AutoscalerKEDA:
		if !validKEDADispatch(autoscaler, bounds, target) {
			return AutoscalingIssueKEDAConfigurationInvalid
		}
	}
	return ""
}

func validKEDADispatch(
	autoscaler *v1beta1.ComponentAutoscaler,
	bounds AutoscalingBounds,
	target *AutoscalingTargetReference,
) bool {
	if autoscaler == nil || autoscaler.Keda == nil || target == nil ||
		bounds.State != AutoscalingBoundsAvailable || bounds.MinReplicas == nil || bounds.MaxReplicas == nil {
		return false
	}
	copy := autoscaler.DeepCopy()
	keda := copy.Keda
	if (keda.PollingInterval != nil && *keda.PollingInterval < 1) ||
		(keda.CooldownPeriod != nil && *keda.CooldownPeriod < 0) ||
		(keda.IdleReplicaCount != nil && *keda.IdleReplicaCount < 0) {
		return false
	}
	if err := kedav1.ValidateTriggers(keda.Triggers); err != nil {
		return false
	}

	scaledObjectName := isvcutils.GetScaledObjectName(target.Name)
	scaledObject := &kedav1.ScaledObject{
		ObjectMeta: metav1.ObjectMeta{Name: scaledObjectName, Namespace: target.Namespace},
		Spec: kedav1.ScaledObjectSpec{
			ScaleTargetRef: &kedav1.ScaleTarget{
				APIVersion: target.APIVersion, Kind: target.Kind, Name: target.Name,
			},
			PollingInterval:  keda.PollingInterval,
			CooldownPeriod:   keda.CooldownPeriod,
			IdleReplicaCount: keda.IdleReplicaCount,
			MinReplicaCount:  bounds.MinReplicas,
			MaxReplicaCount:  bounds.MaxReplicas,
			Advanced:         keda.Advanced,
			Triggers:         keda.Triggers,
			Fallback:         keda.Fallback,
		},
	}
	if err := kedav1.CheckReplicaCountBoundsAreValid(scaledObject); err != nil {
		return false
	}
	if err := kedav1.CheckFallbackValid(scaledObject); err != nil {
		return false
	}
	if scaledObject.IsUsingModifiers() {
		if _, err := kedav1.ValidateAndCompileScalingModifiers(scaledObject); err != nil {
			return false
		}
		activationTarget := scaledObject.Spec.Advanced.ScalingModifiers.ActivationTarget
		if activationTarget != "" {
			value, err := strconv.ParseFloat(activationTarget, 64)
			if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
				return false
			}
		}
	}
	if scaledObject.FallbackScalingModifiers() &&
		(!scaledObject.IsUsingModifiers() || scaledObject.Spec.Advanced.ScalingModifiers.Formula == "") {
		return false
	}
	if onlyCPUOrMemoryTriggers(keda.Triggers) && *bounds.MinReplicas == 0 {
		return false
	}
	if len(scaledObjectName) > 63 || len(k8svalidation.IsDNS1123Subdomain(scaledObjectName)) != 0 {
		return false
	}
	hpaName := "keda-hpa-" + scaledObjectName
	if keda.Advanced != nil && keda.Advanced.HorizontalPodAutoscalerConfig != nil {
		config := keda.Advanced.HorizontalPodAutoscalerConfig
		if config.Name != "" {
			hpaName = config.Name
		}
		if config.Behavior != nil && !validHPABehavior(config.Behavior) {
			return false
		}
	}
	return len(hpaName) <= 63 && len(k8svalidation.IsDNS1123Subdomain(hpaName)) == 0
}

func onlyCPUOrMemoryTriggers(triggers []kedav1.ScaleTriggers) bool {
	if len(triggers) == 0 {
		return false
	}
	for _, trigger := range triggers {
		if trigger.Type != "cpu" && trigger.Type != "memory" {
			return false
		}
	}
	return true
}

func validHPAPayload(hpa *v1beta1.HPAAutoscaler) bool {
	if hpa == nil {
		return true
	}
	for _, metric := range hpa.Metrics {
		if !validHPAMetric(metric) {
			return false
		}
	}
	return validHPABehavior(hpa.Behavior)
}

func validHPABehavior(behavior *autoscalingv2.HorizontalPodAutoscalerBehavior) bool {
	return behavior == nil ||
		(validHPAScalingRules(behavior.ScaleUp) && validHPAScalingRules(behavior.ScaleDown))
}

func validHPAMetric(metric autoscalingv2.MetricSpec) bool {
	sources := 0
	if metric.Object != nil {
		sources++
	}
	if metric.Pods != nil {
		sources++
	}
	if metric.Resource != nil {
		sources++
	}
	if metric.ContainerResource != nil {
		sources++
	}
	if metric.External != nil {
		sources++
	}
	if sources != 1 {
		return false
	}

	switch metric.Type {
	case autoscalingv2.ObjectMetricSourceType:
		return metric.Object != nil && validHPAObjectMetric(metric.Object)
	case autoscalingv2.PodsMetricSourceType:
		return metric.Pods != nil && validMetricIdentifier(metric.Pods.Metric) &&
			validMetricTarget(metric.Pods.Target) && metric.Pods.Target.AverageValue != nil
	case autoscalingv2.ResourceMetricSourceType:
		return metric.Resource != nil && metric.Resource.Name != "" &&
			validMetricTarget(metric.Resource.Target) && validResourceMetricTarget(metric.Resource.Target)
	case autoscalingv2.ContainerResourceMetricSourceType:
		return metric.ContainerResource != nil && validContainerResourceName(metric.ContainerResource.Name) &&
			len(k8svalidation.IsDNS1123Label(metric.ContainerResource.Container)) == 0 &&
			validMetricTarget(metric.ContainerResource.Target) &&
			validResourceMetricTarget(metric.ContainerResource.Target)
	case autoscalingv2.ExternalMetricSourceType:
		if metric.External == nil || !validMetricIdentifier(metric.External.Metric) ||
			!validMetricTarget(metric.External.Target) {
			return false
		}
		return (metric.External.Target.Value == nil) != (metric.External.Target.AverageValue == nil)
	default:
		return false
	}
}

func validContainerResourceName(name corev1.ResourceName) bool {
	value := string(name)
	if len(k8svalidation.IsQualifiedName(value)) != 0 {
		return false
	}
	if !strings.Contains(value, "/") {
		return name == corev1.ResourceCPU || name == corev1.ResourceMemory ||
			name == corev1.ResourceEphemeralStorage || strings.HasPrefix(value, corev1.ResourceHugePagesPrefix)
	}
	if strings.Contains(value, corev1.ResourceDefaultNamespacePrefix) {
		return true
	}
	if strings.HasPrefix(value, corev1.DefaultResourceRequestsPrefix) {
		return false
	}
	return len(k8svalidation.IsQualifiedName(corev1.DefaultResourceRequestsPrefix+value)) == 0
}

func validHPAObjectMetric(metric *autoscalingv2.ObjectMetricSource) bool {
	if metric == nil || metric.DescribedObject.Kind == "" || metric.DescribedObject.Name == "" ||
		len(pathvalidation.IsValidPathSegmentName(metric.DescribedObject.Kind)) != 0 ||
		len(pathvalidation.IsValidPathSegmentName(metric.DescribedObject.Name)) != 0 {
		return false
	}
	if _, err := schema.ParseGroupVersion(metric.DescribedObject.APIVersion); err != nil {
		return false
	}
	return validMetricIdentifier(metric.Metric) && validMetricTarget(metric.Target) &&
		(metric.Target.Value != nil || metric.Target.AverageValue != nil)
}

func validMetricIdentifier(metric autoscalingv2.MetricIdentifier) bool {
	return metric.Name != "" && len(pathvalidation.IsValidPathSegmentName(metric.Name)) == 0
}

func validMetricTarget(target autoscalingv2.MetricTarget) bool {
	switch target.Type {
	case autoscalingv2.UtilizationMetricType, autoscalingv2.ValueMetricType,
		autoscalingv2.AverageValueMetricType:
	default:
		return false
	}
	if target.Value != nil && target.Value.Sign() != 1 {
		return false
	}
	if target.AverageValue != nil && target.AverageValue.Sign() != 1 {
		return false
	}
	return target.AverageUtilization == nil || *target.AverageUtilization >= 1
}

func validResourceMetricTarget(target autoscalingv2.MetricTarget) bool {
	return (target.AverageUtilization == nil) != (target.AverageValue == nil)
}

func validHPAScalingRules(rules *autoscalingv2.HPAScalingRules) bool {
	if rules == nil {
		return true
	}
	if rules.StabilizationWindowSeconds != nil &&
		(*rules.StabilizationWindowSeconds < 0 || *rules.StabilizationWindowSeconds > 3600) {
		return false
	}
	if rules.SelectPolicy != nil {
		switch *rules.SelectPolicy {
		case autoscalingv2.MaxChangePolicySelect, autoscalingv2.MinChangePolicySelect,
			autoscalingv2.DisabledPolicySelect:
		default:
			return false
		}
	}
	if rules.Policies != nil && len(rules.Policies) == 0 {
		return false
	}
	for _, policy := range rules.Policies {
		if policy.Type != autoscalingv2.PodsScalingPolicy && policy.Type != autoscalingv2.PercentScalingPolicy {
			return false
		}
		if policy.Value <= 0 || policy.PeriodSeconds <= 0 || policy.PeriodSeconds > 1800 {
			return false
		}
	}
	return rules.Tolerance == nil || rules.Tolerance.Sign() >= 0
}

func reservedHPANameCollision(autoscaler *v1beta1.ComponentAutoscaler, name string) bool {
	if autoscaler == nil || autoscaler.Class != v1beta1.AutoscalerKEDA || autoscaler.Keda == nil ||
		autoscaler.Keda.Advanced == nil || autoscaler.Keda.Advanced.HorizontalPodAutoscalerConfig == nil {
		return false
	}
	return autoscaler.Keda.Advanced.HorizontalPodAutoscalerConfig.Name == name
}

func resolveAutoscalingBounds(mode constants.DeploymentModeType, ext *v1beta1.ComponentExtensionSpec) AutoscalingBounds {
	if ext == nil || !replicaBoundsFitInt32(ext) {
		return AutoscalingBounds{State: AutoscalingBoundsInvalid}
	}
	if !autoscalerDispatchSupported(mode) {
		return AutoscalingBounds{State: AutoscalingBoundsUnavailable}
	}
	var bounds controllerautoscaler.Bounds
	if mode == constants.RawDeployment {
		var max *int
		if ext.MaxReplicas != 0 {
			max = &ext.MaxReplicas
		}
		if err := validation.ValidateReplicaBounds(ext.MinReplicas, max); err != nil {
			return AutoscalingBounds{State: AutoscalingBoundsInvalid}
		}
		bounds = controllerautoscaler.RawEffectiveComponentBounds(ext)
	} else {
		bounds = controllerautoscaler.EffectiveComponentBounds(ext)
	}
	return AutoscalingBounds{
		State:       AutoscalingBoundsAvailable,
		MinReplicas: int32Pointer(bounds.Min), MaxReplicas: int32Pointer(bounds.Max),
	}
}

func replicaBoundsFitInt32(ext *v1beta1.ComponentExtensionSpec) bool {
	if ext.MinReplicas != nil && (int64(*ext.MinReplicas) < math.MinInt32 || int64(*ext.MinReplicas) > math.MaxInt32) {
		return false
	}
	return int64(ext.MaxReplicas) >= math.MinInt32 && int64(ext.MaxReplicas) <= math.MaxInt32
}

func int32Pointer(value int32) *int32 {
	copy := value
	return &copy
}

func resolveScaleToZero(
	isvc *v1beta1.InferenceService,
	componentType v1beta1.ComponentType,
	mode constants.DeploymentModeType,
	ext *v1beta1.ComponentExtensionSpec,
	autoscaler *v1beta1.ComponentAutoscaler,
) ScaleToZeroState {
	if ext == nil {
		return ScaleToZeroInvalid
	}
	requestedByMin := ext.MinReplicas != nil && *ext.MinReplicas == 0
	requestedByIdle := autoscaler != nil && autoscaler.Class == v1beta1.AutoscalerKEDA &&
		autoscaler.Keda != nil && autoscaler.Keda.IdleReplicaCount != nil &&
		*autoscaler.Keda.IdleReplicaCount == 0
	if !requestedByMin && !requestedByIdle {
		return ScaleToZeroNotRequested
	}
	explicitServiceZero := requestedByMin && isvcExplicitlyRequestsZero(isvc, componentType)
	if explicitServiceZero && !isvcScaleToZeroAdmissionGate(isvc, componentType) {
		return ScaleToZeroInvalid
	}
	if !autoscalerDispatchSupported(mode) {
		return ScaleToZeroUnsupported
	}
	if requestedByMin && mode == constants.OMENative {
		if explicitServiceZero && !requestedByIdle {
			return ScaleToZeroUnsupported
		}
		if !requestedByIdle {
			// OMENative floors inherited/runtime minimums at one; only an
			// independently declared KEDA idle count requests zero here.
			return ScaleToZeroNotRequested
		}
	}
	if autoscaler == nil || autoscaler.Class != v1beta1.AutoscalerKEDA || autoscaler.Keda == nil || len(autoscaler.Keda.Triggers) == 0 {
		return ScaleToZeroInvalid
	}
	return ScaleToZeroEligible
}

func isvcExplicitlyRequestsZero(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) bool {
	ext := isvcComponentExtension(isvc, component)
	return ext != nil && ext.MinReplicas != nil && *ext.MinReplicas == 0
}

func isvcScaleToZeroAdmissionGate(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) bool {
	if isvc == nil {
		return false
	}
	if constants.AutoscalerClassType(isvc.Annotations[constants.AutoscalerClass]) == constants.AutoscalerClassKEDA {
		return true
	}
	ext := isvcComponentExtension(isvc, component)
	return ext != nil && ext.Autoscaler != nil && ext.Autoscaler.Class == v1beta1.AutoscalerKEDA
}

func autoscalerDispatchSupported(mode constants.DeploymentModeType) bool {
	return mode == constants.RawDeployment || mode == constants.OMENative
}

func autoscalingTarget(isvc *v1beta1.InferenceService, component EffectiveComponent) *AutoscalingTargetReference {
	if isvc == nil || !autoscalerDispatchSupported(component.DeploymentMode) {
		return nil
	}
	name := isvc.Name + "-" + string(component.Type)
	if component.DeploymentMode == constants.RawDeployment {
		return &AutoscalingTargetReference{
			APIVersion: "apps/v1", Kind: "Deployment", Namespace: isvc.Namespace, Name: name,
		}
	}
	return &AutoscalingTargetReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceReplica",
		Namespace: isvc.Namespace, Name: name,
	}
}

func resolveScalingPolicy(isvc *v1beta1.InferenceService, runtimeSpec *v1beta1.ServingRuntimeSpec) EffectiveScalingPolicy {
	result := EffectiveScalingPolicy{Mode: v1beta1.ScalingIndependent, Source: ScalingPolicySourceDefault, State: ScalingPolicyAvailable, Issues: []AutoscalingIssueCode{}}
	var policy *v1beta1.ScalingPolicy
	if isvc != nil && isvc.Spec.ScalingPolicy != nil {
		policy = isvc.Spec.ScalingPolicy
		result.Source = ScalingPolicySourceISVC
	} else if runtimeSpec != nil && runtimeSpec.ScalingPolicy != nil {
		policy = runtimeSpec.ScalingPolicy
		result.Source = ScalingPolicySourceRuntime
	}
	if policy != nil && policy.Mode != "" {
		result.Mode = policy.Mode
	}
	switch result.Mode {
	case v1beta1.ScalingIndependent:
		return result
	case v1beta1.ScalingProportional, v1beta1.ScalingPinned:
		result.State = ScalingPolicyUnsupported
		result.Issues = []AutoscalingIssueCode{AutoscalingIssueScalingPolicyUnsupported}
	default:
		result.State = ScalingPolicyInvalid
		result.Issues = []AutoscalingIssueCode{AutoscalingIssueScalingPolicyInvalid}
	}
	return result
}

func autoscalingInheritance(state *RuntimeState, active *ActiveConfiguration) AutoscalingInheritance {
	result := AutoscalingInheritance{State: InheritanceUnavailable, UnavailableReason: InheritanceUnreadable, Sources: []AutoscalingRuntimeReference{}}
	if active.Origin == ConfigurationOriginControllerRevision {
		// A revision stores one flattened ServingRuntimeSpec, not the source
		// chain that produced it. Today's live inheritance chain is independent
		// evidence and must not be attributed to the pinned configuration.
		return AutoscalingInheritance{State: InheritanceNotRecorded, Sources: []AutoscalingRuntimeReference{}}
	}
	live := state.LiveConfiguration()
	if live == nil {
		return result
	}
	observation := live.Runtime.DeclaredInheritance
	result.State = observation.State()
	result.UnavailableReason = observation.UnavailableReason()
	if result.State != InheritanceObserved {
		return result
	}
	result.UnavailableReason = ""
	for _, source := range observation.Chain() {
		if source.UID == "" || source.ResourceVersion == "" {
			return AutoscalingInheritance{
				State: InheritanceUnavailable, UnavailableReason: InheritanceMalformed,
				Sources: []AutoscalingRuntimeReference{},
			}
		}
		result.Sources = append(result.Sources, AutoscalingRuntimeReference{
			Kind: source.Kind, Namespace: source.Namespace, Name: source.Name,
			UID: string(source.UID), Generation: source.Generation,
		})
	}
	for i := range result.Sources {
		if result.Sources[i].Kind == active.RuntimeKind && result.Sources[i].Namespace == active.RuntimeNamespace &&
			result.Sources[i].Name == active.RuntimeName {
			return result
		}
	}
	// A revision can remain active after its live source disappeared. The
	// independently observed chain must not be relabeled as that active source.
	if active.Origin == ConfigurationOriginLiveRuntime {
		return AutoscalingInheritance{State: InheritanceUnavailable, UnavailableReason: InheritanceMalformed, Sources: []AutoscalingRuntimeReference{}}
	}
	return result
}

func activeRevisionReference(state *RuntimeState, active *ActiveConfiguration) *AutoscalingRevisionReference {
	if active.Origin != ConfigurationOriginControllerRevision || active.RevisionName == "" {
		return nil
	}
	result := &AutoscalingRevisionReference{
		Name: active.RevisionName, Role: RuntimeRevisionRoleActive,
	}
	for _, observation := range state.RevisionObservations() {
		if observation.ExpectedName() != active.RevisionName ||
			!containsRevisionRole(observation.Roles(), RuntimeRevisionRoleActive) {
			continue
		}
		result.Namespace = observation.ExpectedNamespace()
		if observation.Name == observation.ExpectedName() &&
			observation.Namespace == observation.ExpectedNamespace() {
			result.UID = observation.UID
			result.IdentityObserved = true
		}
		return result
	}
	return result
}

func containsRevisionRole(roles []RuntimeRevisionRole, want RuntimeRevisionRole) bool {
	for _, role := range roles {
		if role == want {
			return true
		}
	}
	return false
}

func knownRevisionConsistency(value RevisionConsistencyState) bool {
	switch value {
	case RevisionConsistencyConsistent, RevisionConsistencyInconsistent, RevisionConsistencyUnknown:
		return true
	default:
		return false
	}
}

func knownStatusFreshness(value StatusFreshness) bool {
	switch value {
	case StatusFreshnessCurrent, StatusFreshnessStale, StatusFreshnessInconsistent, StatusFreshnessUnknown:
		return true
	default:
		return false
	}
}

func canonicalAutoscalingIssues(issues []AutoscalingIssueCode) []AutoscalingIssueCode {
	result := append([]AutoscalingIssueCode{}, issues...)
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	write := 0
	for _, issue := range result {
		if issue == "" || (write > 0 && result[write-1] == issue) {
			continue
		}
		result[write] = issue
		write++
	}
	return result[:write]
}
