package status

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils"
)

// TopLevelComponentReadyFromLifecycle derives Instance readiness using only lifecycle rollingUpdate.maxUnavailable; PDB fields are unrelated.
// An absent lifecycle budget is strict, and desiredReplicas keeps surge from inflating the floor.
// Unknown Component types return nil.
func TopLevelComponentReadyFromLifecycle(component v1beta1.ComponentType, cs *v1beta1.LifecycleStatus, lifecycle *v1beta1.LifecycleSpec, desiredReplicas *int32) *apis.Condition {
	readyType, ok := v1beta1.ReadyConditionOf(component)
	if !ok {
		return nil
	}
	cond := &apis.Condition{Type: readyType}
	if cs == nil || cs.Replicas == 0 {
		cond.Status = corev1.ConditionFalse
		cond.Reason = "NoReplicas"
		cond.Message = "Component has no desired Instances"
		return cond
	}
	// A budget that lets every Instance be unavailable still needs one
	// serving Instance for the component to read ready.
	floor := max(InstanceAvailabilityFloor(cs.Replicas, desiredReplicas, lifecycle), 1)
	switch {
	// Ready counts containers only; an Instance enters rotation through the
	// serving gate. A component with no serving Instance is not ready.
	case cs.ReadyReplicas >= cs.Replicas && cs.ServingReplicas > 0:
		cond.Status = corev1.ConditionTrue
		cond.Reason = "Ready"
		cond.Message = "All Instances are Ready"
	case cs.ServingReplicas >= floor:
		cond.Status = corev1.ConditionTrue
		cond.Reason = "MinimumAvailable"
		cond.Message = fmt.Sprintf("%d/%d Instances serving (min %d)", cs.ServingReplicas, cs.Replicas, floor)
	default:
		cond.Status = corev1.ConditionFalse
		cond.Reason = "InsufficientAvailable"
		cond.Message = fmt.Sprintf("%d/%d Instances serving, need %d", cs.ServingReplicas, cs.Replicas, floor)
	}
	return cond
}

// InstanceAvailabilityFloor returns the lifecycle availability floor for desired Instances.
// The desired count excludes rollout surge; nil or non-positive desired values use the live count.
func InstanceAvailabilityFloor(actual int32, desired *int32, lifecycle *v1beta1.LifecycleSpec) int32 {
	return utils.AvailabilityFloor(floorBasis(actual, desired), nil, nil, rolloutMaxUnavailable(lifecycle))
}

// floorBasis is the desired Instance count the availability floor scales against,
// nil/non-positive falls back to live count.
func floorBasis(actual int32, desired *int32) int32 {
	if desired == nil || *desired <= 0 || *desired >= actual {
		return actual
	}
	return *desired
}

// rolloutMaxUnavailable returns the Instance rollout availability budget.
func rolloutMaxUnavailable(lifecycle *v1beta1.LifecycleSpec) *intstr.IntOrString {
	if lifecycle == nil || lifecycle.UpdateStrategy == nil || lifecycle.UpdateStrategy.RollingUpdate == nil {
		return nil
	}
	return lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable
}
