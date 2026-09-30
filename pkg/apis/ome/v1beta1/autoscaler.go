package v1beta1

import (
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/api/resource"
)

// AutoscalerClass identifies the autoscaler implementation for a Component.
// Alpha. The API may change without notice.
//
// +kubebuilder:validation:Enum=HPA;KEDA;External;None
type AutoscalerClass string

const (
	// AutoscalerHPA selects the HorizontalPodAutoscaler reconciler.
	AutoscalerHPA AutoscalerClass = "HPA"
	// AutoscalerKEDA selects the KEDA ScaledObject reconciler.
	AutoscalerKEDA AutoscalerClass = "KEDA"
	// AutoscalerExternal disables OME's autoscaler so an operator-owned scaler
	// can drive the Component. status.components.<comp>.scaleTargetRef
	// publishes the canonical target name for the operator to point at.
	AutoscalerExternal AutoscalerClass = "External"
	// AutoscalerNone means no autoscaler; the InferenceService controller owns
	// the replica count.
	AutoscalerNone AutoscalerClass = "None"
)

// ComponentAutoscaler configures the autoscaler for a single Component
// (engine / decoder / router). Takes priority over the legacy
// ome.io/autoscalerClass annotation for this Component, and is the only
// way to configure per-Component autoscaling on an InferenceService.
// Alpha. The API may change without notice.
type ComponentAutoscaler struct {
	// Class selects the autoscaler implementation.
	// +kubebuilder:validation:Enum=HPA;KEDA;External;None
	Class AutoscalerClass `json:"class"`

	// Keda holds KEDA-specific configuration. Required when Class == "KEDA".
	// +optional
	Keda *KedaAutoscaler `json:"keda,omitempty"`

	// HPA holds HorizontalPodAutoscaler-specific configuration. Optional when
	// Class == "HPA"; if absent the controller emits a default CPU=80% HPA.
	// +optional
	HPA *HPAAutoscaler `json:"hpa,omitempty"`
}

// KedaAutoscaler holds KEDA ScaledObject configuration passed through verbatim.
// Alpha. The API may change without notice.
type KedaAutoscaler struct {
	// Triggers are passed through verbatim to kedav1.ScaledObjectSpec.Triggers.
	// The full KEDA trigger surface (prometheus, cron, external, kafka, ...)
	// is supported.
	// +kubebuilder:validation:MinItems=1
	// +listType=atomic
	Triggers []kedav1.ScaleTriggers `json:"triggers"`

	// Advanced ScaledObject configuration —
	// horizontalPodAutoscalerConfig, restoreToOriginalReplicaCount.
	// +optional
	Advanced *kedav1.AdvancedConfig `json:"advanced,omitempty"`

	// PollingInterval is the interval, in seconds, at which KEDA checks each
	// trigger. Defaults to the KEDA-side default when unset.
	// +optional
	PollingInterval *int32 `json:"pollingInterval,omitempty"`

	// CooldownPeriod is the time, in seconds, KEDA waits before scaling down
	// after the last trigger fires. Defaults to the KEDA-side default when unset.
	// +optional
	CooldownPeriod *int32 `json:"cooldownPeriod,omitempty"`

	// IdleReplicaCount enables scaling to a fixed count when no triggers fire
	// (commonly 0 for scale-to-zero). Must be strictly less than the
	// effective MinReplicaCount.
	// +optional
	IdleReplicaCount *int32 `json:"idleReplicaCount,omitempty"`

	// Fallback policy applied when the metric source becomes unavailable.
	// +optional
	Fallback *kedav1.Fallback `json:"fallback,omitempty"`
}

// HPAAutoscaler holds HorizontalPodAutoscaler configuration passed through verbatim.
// Alpha. The API may change without notice.
type HPAAutoscaler struct {
	// Metrics are passed through verbatim to
	// autoscalingv2.HorizontalPodAutoscalerSpec.Metrics. Supports Resource,
	// ContainerResource, Pods, Object, and External metric sources. If empty,
	// the controller defaults to a single CPU=80% Resource metric.
	// +optional
	// +listType=atomic
	Metrics []autoscalingv2.MetricSpec `json:"metrics,omitempty"`

	// Behavior controls scaling stabilization windows and per-direction policies.
	// +optional
	Behavior *autoscalingv2.HorizontalPodAutoscalerBehavior `json:"behavior,omitempty"`
}

// ScalingMode declares how Components of an InferenceService scale relative
// to each other. Alpha. The API may change without notice.
//
// +kubebuilder:validation:Enum=Independent;Proportional;Pinned
type ScalingMode string

const (
	// ScalingIndependent — each Component autoscales on its own metrics. Default.
	ScalingIndependent ScalingMode = "Independent"
	// ScalingProportional is accepted by the schema, but admission rejects a
	// create or an update that sets or changes a policy with this mode: no
	// controller applies it.
	ScalingProportional ScalingMode = "Proportional"
	// ScalingPinned is accepted by the schema, but admission rejects a create
	// or an update that sets or changes a policy with this mode: no
	// controller applies it.
	ScalingPinned ScalingMode = "Pinned"
)

// ScalingPolicy defines coordination between the Components of an
// InferenceService. Alpha. The API may change without notice.
type ScalingPolicy struct {
	// Mode selects the coordination strategy.
	// +kubebuilder:validation:Enum=Independent;Proportional;Pinned
	// +kubebuilder:default=Independent
	Mode ScalingMode `json:"mode"`

	// Proportional configuration; required when Mode == "Proportional".
	// +optional
	Proportional *ProportionalPolicy `json:"proportional,omitempty"`
}

// ProportionalPolicy specifies the anchor Component and per-follower replica
// ratios for ScalingProportional. Alpha. The API may change without
// notice.
type ProportionalPolicy struct {
	// Anchor is the Component whose replica count drives followers.
	// +kubebuilder:validation:Enum=engine;decoder;router
	// +kubebuilder:default=engine
	Anchor ComponentType `json:"anchor"`

	// Ratios maps each follower Component to its replica ratio relative to the
	// anchor. A ratio of 1.0 means parity; 0.25 means one follower per four
	// anchor replicas (rounded up). Components not listed scale independently.
	// +optional
	Ratios map[ComponentType]resource.Quantity `json:"ratios,omitempty"`
}
