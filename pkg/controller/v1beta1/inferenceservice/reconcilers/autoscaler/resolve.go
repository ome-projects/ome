package autoscaler

import (
	"fmt"
	"strconv"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// SpecSource identifies the layer that produced the resolved
// ComponentAutoscaler. Mirrored onto status.components.<comp>.autoscaler
// .specSource by the status writer so operators can see which layer of
// the inheritance chain won for a given Component.
//
// ISVC takes priority over the resolved ServingRuntime; if neither sets
// a block we fall back to the universal hpa-with-CPU=80% default (filled
// in by the HPA generator downstream when HPA is nil).
//
// Alpha API. The constants may change without notice.
type SpecSource string

const (
	// SpecSourceISVC indicates the resolved ComponentAutoscaler was
	// taken from isvc.Spec.<component>.ComponentExtensionSpec.Autoscaler.
	SpecSourceISVC SpecSource = "isvc"

	// SpecSourcePolicy indicates the resolved ComponentAutoscaler was
	// rendered from the AutoscalerPolicy referenced by
	// spec.<component>.autoscalerPolicyRef. Sits below the inline ISVC block
	// (inline wins — restoring an inline block is an atomic, policy-free
	// rollback) and above the runtime block (a runtime autoscaler is a
	// vendor-family default; the policy is a deliberate fleet decision).
	SpecSourcePolicy SpecSource = "policy"

	// SpecSourceRuntime indicates the resolved ComponentAutoscaler was
	// taken from runtime.Spec.<componentConfig>.ComponentExtensionSpec.Autoscaler.
	SpecSourceRuntime SpecSource = "runtime"

	// SpecSourceLegacy indicates the RawDeployment autoscaler was translated
	// from the effective legacy autoscaler annotations after both typed layers
	// were absent.
	SpecSourceLegacy SpecSource = "legacy"

	// SpecSourceDefault indicates neither the ISVC nor the resolved
	// ServingRuntime declared a Component-level Autoscaler block.
	// ResolveComponentAutoscaler returns the universal default —
	// {Class: hpa, HPA: nil} — which the HPA generator downstream
	// expands to a single CPU=80% Resource metric. The default ALWAYS
	// produces an HPA.
	SpecSourceDefault SpecSource = "default"
)

// ResolveComponentAutoscaler picks the authoritative ComponentAutoscaler
// for a single Component, considering the ISVC and the resolved
// ServingRuntime. The decision tree:
//
//  1. If isvc.Spec.<component>.Autoscaler != nil → return
//     (deep-copied isvc block, SpecSourceISVC).
//  2. Else if runtime.<componentConfig>.Autoscaler != nil → return
//     (deep-copied runtime block, SpecSourceRuntime).
//  3. Else → return ({Class: hpa, HPA: nil}, SpecSourceDefault).
//     The HPA generator materializes this into a single CPU=80% Resource
//     metric.
//
// NOTE: The default is ALWAYS Class=hpa with CPU=80%. There is no "only
// when Component declares a Resource metric" carve-out. The operator-
// visible default unconditionally produces an HPA. The Autoscaler block
// is fully authoritative. RawDeployment callers layer legacy annotation
// compatibility through ResolveRawComponentAutoscaler.
//
// Returned pointer is always non-nil and always a deep copy of the
// source (so callers can mutate freely without affecting the input
// objects). The component argument selects which Component-level
// Autoscaler field to consult on each spec.
//
// runtime may be nil — when it is, only the ISVC + default branches
// run. Unknown component values fall through to the default branch.
//
// Alpha API. The signature may change without notice.
func ResolveComponentAutoscaler(
	runtime *v1beta1.ServingRuntimeSpec,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
) (*v1beta1.ComponentAutoscaler, SpecSource) {
	resolved, source, _ := ResolveComponentAutoscalerWithPolicy(runtime, isvc, component, nil)
	return resolved, source
}

// ResolveComponentAutoscalerWithPolicy extends the chain with the policy
// layer: inline > policy > runtime > default. policy is the outcome of
// PolicyResolver.Resolve for this component; nil means no ref.
//
// The third return value is the fail-closed hold: the component's ref is set
// but the policy could not produce a block (missing, invalid, provider
// unbound). A hold means "keep the last-known-good scaler and raise a
// condition" — the caller must NOT dispatch the returned nil block, and must
// NOT let resolution fall through to runtime/default, which would silently
// swap the scaler class (for a GPU fleet pinned at max by a fail-to-max
// policy, a default CPU HPA is a scale-to-min during a policy outage).
// An inline block outranks the ref entirely, hold included: the escape hatch
// must work precisely when the policy machinery is broken.
func ResolveComponentAutoscalerWithPolicy(
	runtime *v1beta1.ServingRuntimeSpec,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
	policy *PolicyOutcome,
) (*v1beta1.ComponentAutoscaler, SpecSource, bool) {
	if a := isvcAutoscaler(isvc, component); a != nil {
		return a.DeepCopy(), SpecSourceISVC, false
	}
	if policy != nil {
		if policy.Hold {
			return nil, SpecSourcePolicy, true
		}
		if policy.Rendered != nil {
			return policy.Rendered.DeepCopy(), SpecSourcePolicy, false
		}
	}
	if a := runtimeAutoscaler(runtime, component); a != nil {
		return a.DeepCopy(), SpecSourceRuntime, false
	}
	return defaultAutoscaler(), SpecSourceDefault, false
}

// ResolveRawComponentAutoscaler applies RawDeployment's compatibility layer
// after the typed ISVC -> runtime resolution. A typed block always wins. Only
// the default branch may be replaced by the effective legacy annotation.
//
// The target-utilization annotation is translated into an explicit HPA metric
// so downstream shared dispatch does not need to inspect legacy metadata.
func ResolveRawComponentAutoscaler(
	runtime *v1beta1.ServingRuntimeSpec,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
	annotations map[string]string,
) (*v1beta1.ComponentAutoscaler, SpecSource, error) {
	resolved, source, _, err := ResolveRawComponentAutoscalerWithPolicy(runtime, isvc, component, annotations, nil)
	return resolved, source, err
}

// ResolveRawComponentAutoscalerWithPolicy is the RawDeployment counterpart of
// ResolveComponentAutoscalerWithPolicy: the shared typed chain (with the
// policy layer) runs first, and the legacy annotation still substitutes only
// the default branch — a policy ref outranks the deprecated whole-ISVC hint.
func ResolveRawComponentAutoscalerWithPolicy(
	runtime *v1beta1.ServingRuntimeSpec,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
	annotations map[string]string,
	policy *PolicyOutcome,
) (*v1beta1.ComponentAutoscaler, SpecSource, bool, error) {
	resolved, source, hold := ResolveComponentAutoscalerWithPolicy(runtime, isvc, component, policy)
	if hold {
		return nil, source, true, nil
	}
	if source != SpecSourceDefault {
		return resolved, source, false, nil
	}
	legacy, present, err := legacyRawAutoscaler(annotations)
	if err != nil {
		return nil, SpecSourceLegacy, false, err
	}
	if !present {
		return resolved, source, false, nil
	}
	return legacy, SpecSourceLegacy, false, nil
}

// legacyRawAutoscaler translates the supported legacy RawDeployment class
// values into the typed representation. The boolean distinguishes absent
// legacy input from an invalid value. Each call returns newly-owned data.
func legacyRawAutoscaler(annotations map[string]string) (*v1beta1.ComponentAutoscaler, bool, error) {
	rawClass, hasClass := annotations[constants.AutoscalerClass]
	_, hasTargetUtilization := annotations[constants.TargetUtilizationPercentage]
	if !hasClass && !hasTargetUtilization {
		return nil, false, nil
	}

	class := constants.AutoscalerClassType(rawClass)
	if !hasClass {
		class = constants.AutoscalerClassHPA
	}

	switch class {
	case constants.AutoscalerClassHPA:
		return legacyRawHPA(annotations), true, nil
	case constants.AutoscalerClassKEDA:
		return &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA}, true, nil
	case constants.AutoscalerClassExternal:
		return &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}, true, nil
	default:
		return nil, true, fmt.Errorf("unknown legacy autoscaler class %q in %s annotation", rawClass, constants.AutoscalerClass)
	}
}

// legacyRawHPA preserves the target-utilization annotation for RawDeployment.
// Invalid values are rejected by admission; defensively, an unparsable value
// leaves HPA nil so the existing HPA generator default remains in effect.
func legacyRawHPA(annotations map[string]string) *v1beta1.ComponentAutoscaler {
	resolved := &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}
	rawUtilization, ok := annotations[constants.TargetUtilizationPercentage]
	if !ok {
		return resolved
	}

	parsed, err := strconv.ParseInt(rawUtilization, 10, 32)
	if err != nil {
		return resolved
	}
	utilization := int32(parsed)
	resolved.HPA = &v1beta1.HPAAutoscaler{
		Metrics: []autoscalingv2.MetricSpec{
			{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name: corev1.ResourceCPU,
					Target: autoscalingv2.MetricTarget{
						Type:               autoscalingv2.UtilizationMetricType,
						AverageUtilization: &utilization,
					},
				},
			},
		},
	}
	return resolved
}

// isvcAutoscaler dereferences the Component-level Autoscaler block on
// the InferenceService. Returns nil when the ISVC, the Component, or
// the Autoscaler field is unset.
func isvcAutoscaler(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) *v1beta1.ComponentAutoscaler {
	if isvc == nil {
		return nil
	}
	if view := v1beta1.SpecOf(&isvc.Spec, component); view != nil {
		return view.Extension.Autoscaler
	}
	return nil
}

// runtimeAutoscaler dereferences the Component-level Autoscaler block
// on the resolved ServingRuntimeSpec. Returns nil when the runtime, the
// Component config, or the Autoscaler field is unset.
func runtimeAutoscaler(runtime *v1beta1.ServingRuntimeSpec, component v1beta1.ComponentType) *v1beta1.ComponentAutoscaler {
	if piece := v1beta1.PieceOf(runtime, component); piece != nil {
		return piece.Extension.Autoscaler
	}
	return nil
}

// defaultAutoscaler returns the universal default ComponentAutoscaler.
// Always a fresh allocation — never shares memory across calls — so a
// caller mutating the returned pointer cannot affect a subsequent
// resolution.
//
// The HPA generator treats HPA=nil as a sentinel and materializes a
// single CPU=80% Resource metric. Every Component without an explicit
// Autoscaler block gets the same baseline HPA.
func defaultAutoscaler() *v1beta1.ComponentAutoscaler {
	return &v1beta1.ComponentAutoscaler{
		Class: v1beta1.AutoscalerHPA,
	}
}
