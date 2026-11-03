package validation

import (
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const (
	priorityIsNotSameError = "different priorities assigned for the model format %s"
)

// ValidateRuntimeRouterCapacityEndpoint rejects a capacityEndpoint on a runtime
// router config. Ingress reads it from the InferenceService only, so a runtime
// value would never route anything.
func ValidateRuntimeRouterCapacityEndpoint(spec *v1beta1.ServingRuntimeSpec) error {
	if spec == nil || spec.RouterConfig == nil || spec.RouterConfig.CapacityEndpoint == nil {
		return nil
	}
	return fmt.Errorf("routerConfig: capacityEndpoint is not supported on serving runtimes; set spec.router.capacityEndpoint on the InferenceService")
}

// ValidateRuntimeAutoscalerPolicyRefs rejects an AutoscalerPolicyRef on any
// runtime component config. The embedded ComponentExtensionSpec admits the
// field structurally, but no controller reads it off a runtime — policy refs
// attach on the InferenceService only — so admitting one here would store
// dead configuration an operator reasonably expects to act.
func ValidateRuntimeAutoscalerPolicyRefs(spec *v1beta1.ServingRuntimeSpec) error {
	for _, r := range runtimeRoles(spec) {
		if r.extension.AutoscalerPolicyRef != nil {
			return fmt.Errorf("%s: autoscalerPolicyRef is not supported on serving runtimes; policy refs attach on the InferenceService only", r.field)
		}
	}
	return nil
}

func ValidateModelFormatPrioritySame(spec *v1beta1.ServingRuntimeSpec) error {
	nameToPriority := make(map[string]*int32)

	for _, f := range spec.SupportedModelFormats {
		if f.IsAutoSelectEnabled() {
			if existingPriority, ok := nameToPriority[f.Name]; ok {
				if existingPriority != nil && f.Priority != nil && (*existingPriority != *f.Priority) {
					return fmt.Errorf(priorityIsNotSameError, f.Name)
				}
			} else {
				nameToPriority[f.Name] = f.Priority
			}
		}
	}
	return nil
}
