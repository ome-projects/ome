package builders

import (
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
)

const inferenceReplicaScaleTargetKind = "InferenceReplica"

// componentCanBackRoute reports whether a component has current serving
// capacity. The Ready condition covers steady state. For an IR-managed
// component whose condition is False, positive ReadyReplicas and
// ServingReplicas keep its route present while it converges to a higher
// availability floor. GatewayAPIStrategy separately requires every route
// parent to have accepted the current route generation.
func componentCanBackRoute(
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
	readyCondition apis.ConditionType,
) bool {
	if isvc.Status.IsConditionReady(readyCondition) {
		return true
	}
	condition := isvc.Status.GetCondition(readyCondition)
	if condition == nil || condition.Status != corev1.ConditionFalse {
		return false
	}
	status, ok := isvc.Status.Components[component]
	return ok && status.ScaleTargetRef != nil &&
		status.ScaleTargetRef.APIVersion == v1beta1.SchemeGroupVersion.String() &&
		status.ScaleTargetRef.Kind == inferenceReplicaScaleTargetKind &&
		status.ScaleTargetRef.Name == irprojector.InferenceReplicaName(isvc.Name, component) &&
		status.Lifecycle != nil &&
		status.Lifecycle.ReadyReplicas > 0 && status.Lifecycle.ServingReplicas > 0
}
