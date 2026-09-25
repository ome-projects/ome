package v1beta1

import (
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// ComponentExtensionSpec defines the deployment configuration for a given InferenceService component
type ComponentExtensionSpec struct {
	// Minimum number of replicas, defaults to 1 but can be set to 0 to enable scale-to-zero.
	// +optional
	MinReplicas *int `json:"minReplicas,omitempty"`
	// Maximum number of replicas for autoscaling.
	// +optional
	MaxReplicas int `json:"maxReplicas,omitempty"`
	// TimeoutSeconds specifies the number of seconds to wait before timing out a request to the component.
	// +optional
	TimeoutSeconds *int64 `json:"timeoutSeconds,omitempty"`
	// Labels that will be added to the component pod.
	// More info: http://kubernetes.io/docs/user-guide/labels
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// Annotations that will be added to the component pod.
	// More info: http://kubernetes.io/docs/user-guide/annotations
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
	// ServicePortAppProtocols maps generated Service port names to their
	// Kubernetes appProtocol values. Declared ports use their container port name.
	// +optional
	ServicePortAppProtocols map[string]string `json:"servicePortAppProtocols,omitempty"`
	// MinAvailable specifies how many component pods must still be available after the eviction
	// +optional
	MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty"`
	// MaxUnavailable specifies how many component pods can be unavailable
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`

	// The deployment strategy to use to replace existing pods with new ones. Only applicable for raw deployment mode.
	// +optional
	DeploymentStrategy *appsv1.DeploymentStrategy `json:"deploymentStrategy,omitempty"`

	// Lifecycle groups OMENative-specific lifecycle policies for this
	// Component. Applies only when the Component resolves to deploymentMode
	// OMENative (spec.deploymentMode or the per-Component
	// ome.io/deploymentMode annotation); ignored otherwise. A field left
	// unset here inherits the ServingRuntime's value for the Component; the
	// controller fills what neither sets from cluster configuration and
	// fixed fallbacks and never writes them back. The status counterpart is
	// status.components.<component>.lifecycle.
	// +optional
	Lifecycle *LifecycleSpec `json:"lifecycle,omitempty"`

	// Autoscaler configures the per-Component autoscaler dispatch and the
	// underlying KEDA / HPA configuration. Takes priority over the legacy
	// ome.io/autoscalerClass annotation for this Component, and is the
	// only way to configure per-Component autoscaling: scale metrics live
	// here (Autoscaler.HPA.Metrics), alongside MinReplicas / MaxReplicas
	// above. Alpha. The API may change without notice.
	// +optional
	Autoscaler *ComponentAutoscaler `json:"autoscaler,omitempty"`

	// AutoscalerPolicyRef names a same-namespace AutoscalerPolicy whose
	// template renders this Component's autoscaler. An inline Autoscaler
	// block above always outranks the ref; the two may coexist, which is the
	// documented preview/rollback mechanism. Alpha. The API may change
	// without notice.
	// +optional
	AutoscalerPolicyRef *AutoscalerPolicyRef `json:"autoscalerPolicyRef,omitempty"`
}
