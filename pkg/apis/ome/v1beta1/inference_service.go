package v1beta1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/constants"
)

// InferenceServiceSpec is the top level type for this resource
type InferenceServiceSpec struct {
	// DeploymentMode selects the dispatch backend that drives every
	// Component on this InferenceService. When set, it propagates to
	// the Engine, Decoder, and Router resolver at read-time without
	// mutating per-Component annotations — `kubectl get -o yaml` stays
	// clean. A per-Component
	// `ome.io/deploymentMode` annotation still wins as an explicit
	// override (the escape hatch is preserved for backward compat and
	// mixed-mode experiments).
	//
	// Valid values:
	//   - "OMENative"     — Native lifecycle-managed pods (also
	//     handles multi-node serving when Leader/Worker are present)
	//   - "RawDeployment" — Kubernetes Deployment/HPA-backed dispatch
	//
	// MultiNode (LeaderWorkerSet-backed multi-node serving) is selected
	// via the ome.io/deploymentMode annotation or the operator-level
	// default, never via this field. PDDisaggregated is a shape
	// descriptor derived from engine+decoder pairing, and
	// VirtualDeployment is legacy; neither is accepted here.
	// +kubebuilder:validation:Enum=OMENative;RawDeployment
	// +optional
	DeploymentMode *constants.DeploymentModeType `json:"deploymentMode,omitempty"`

	// Engine defines the serving engine spec
	// This provides detailed container and pod specifications for model serving.
	// It allows defining the model runner (container spec), as well as complete pod specifications
	// including init containers, sidecar containers, and other pod-level configurations.
	// Engine can also be configured for multi-node deployments using leader and worker specifications.
	// +optional
	Engine *EngineSpec `json:"engine,omitempty"`

	// Decoder defines the decoder spec
	// This is specifically used for PD (Prefill-Decode) disaggregated serving deployments.
	// Similar to Engine in structure, it allows for container and pod specifications,
	// but is only utilized when implementing the disaggregated serving pattern
	// to separate the prefill and decode phases of inference.
	// +optional
	Decoder *DecoderSpec `json:"decoder,omitempty"`

	// Model defines the model to be used for inference, referencing either a BaseModel or a custom model.
	// This allows models to be managed independently of the serving configuration.
	// +optional
	Model *ModelRef `json:"model,omitempty"`

	// Runtime defines the serving runtime environment that will be used to execute the model.
	// It is an inference service spec template that determines how the service should be deployed.
	// Runtime is optional - if not defined, the operator will automatically select the best runtime
	// based on the model's size, architecture, format, quantization, and framework.
	// +optional
	Runtime *ServingRuntimeRef `json:"runtime,omitempty"`

	// Router defines the router spec
	// +optional
	Router *RouterSpec `json:"router,omitempty"`

	// AcceleratorSelector specifies accelerator selection preferences
	// +optional
	AcceleratorSelector *AcceleratorSelector `json:"acceleratorSelector,omitempty"`

	// Traffic configures the load-balancing policy applied to all
	// HTTPRoutes OME emits for this InferenceService. See the
	// TrafficSpec doc for details.
	// +optional
	Traffic *TrafficSpec `json:"traffic,omitempty"`

	// Rollout is the single home for rollout configuration on this
	// InferenceService: an ordered list of rollout groups, each polymorphic in
	// its progression (canary | blueGreen | rollingUpdate). Sequencing is the
	// group list order; a group's Components roll together.
	// +optional
	Rollout *RolloutSpec `json:"rollout,omitempty"`

	// ScalingPolicy coordinates replica counts across Components when more
	// than one autoscaler would otherwise drift (e.g., engine + decoder in
	// a PD-disaggregated service that requires 1:1 sharding). When nil,
	// every Component autoscales independently. Alpha. The API
	// may change without notice.
	// +optional
	ScalingPolicy *ScalingPolicy `json:"scalingPolicy,omitempty"`

	// Placement declares how the multi-cluster control plane selects and orders
	// the workload clusters this InferenceService is placed onto. When nil, the
	// control plane falls back to the ome.io/accelerator-requirements and
	// ome.io/cluster-selector annotations (unchanged legacy behavior). Only
	// consulted on the control-plane cluster; ignored in single-cluster
	// deployments. Alpha; the API may change without notice.
	// +optional
	Placement *PlacementSpec `json:"placement,omitempty"`
}

// GetRolloutGroups returns the ordered rollout groups (spec.rollout.groups), or
// nil if spec.rollout is unset. Nil-safe on a nil receiver so callers can drop
// their own guards.
func (s *InferenceServiceSpec) GetRolloutGroups() []RolloutGroup {
	if s == nil || s.Rollout == nil {
		return nil
	}
	return s.Rollout.Groups
}

// GetRolloutGroupOrdering returns spec.rollout.groupOrdering, defaulting to
// Sequential when the rollout block or the field is unset — an object written
// before the field existed keeps its original meaning. Nil-safe.
func (s *InferenceServiceSpec) GetRolloutGroupOrdering() RolloutGroupOrdering {
	if s == nil || s.Rollout == nil || s.Rollout.GroupOrdering == nil {
		return RolloutGroupOrderingSequential
	}
	return *s.Rollout.GroupOrdering
}

// RolloutPairingProtocol returns spec.rollout.pairingProtocol, or "" when the
// rollout block or the field is unset. Nil-safe on a nil receiver.
func (s *InferenceServiceSpec) RolloutPairingProtocol() string {
	if s == nil || s.Rollout == nil || s.Rollout.PairingProtocol == nil {
		return ""
	}
	return *s.Rollout.PairingProtocol
}

// GetCanaryGroup returns the first rollout group whose progression is canary, or
// nil if none. Admission rejects more than one canary group, so there is at most
// one; a canary group may span multiple Components (primary-driven). Nil-safe.
func (s *InferenceServiceSpec) GetCanaryGroup() *RolloutGroup {
	if s == nil || s.Rollout == nil {
		return nil
	}
	for i := range s.Rollout.Groups {
		if s.Rollout.Groups[i].Canary != nil {
			return &s.Rollout.Groups[i]
		}
	}
	return nil
}

// AcceleratorSelector defines how to select accelerators for the InferenceService
type AcceleratorSelector struct {
	// AcceleratorClass explicitly selects a specific AcceleratorClass
	// Takes precedence over other selectors
	// +optional
	AcceleratorClass *string `json:"acceleratorClass,omitempty"`

	// Constraints defines requirements that accelerators must meet
	// +optional
	Constraints *AcceleratorConstraints `json:"constraints,omitempty"`

	// Policy defines the selection policy when multiple accelerators match
	// +kubebuilder:validation:Enum=BestFit;Cheapest;MostCapable;FirstAvailable
	// +optional
	Policy AcceleratorSelectionPolicy `json:"policy,omitempty"`
}

// AcceleratorConstraints defines requirements for accelerator selection
type AcceleratorConstraints struct {
	// MinMemory in GB
	// +optional
	MinMemory *int64 `json:"minMemory,omitempty"`

	// MaxMemory in GB (useful for cost control)
	// +optional
	MaxMemory *int64 `json:"maxMemory,omitempty"`

	// MinComputePerformanceTFLOPS in TFLOPS
	// +optional
	MinComputePerformanceTFLOPS *int64 `json:"minComputePerformanceTFLOPS,omitempty"`

	//MinArchitectureVersion Compute capability (NVIDIA) or equivalent
	// +optional
	MinArchitectureVersion *string `json:"minArchitectureVersion,omitempty"`

	// RequiredFeatures that must be present
	// +optional
	// +listType=atomic
	RequiredFeatures []string `json:"requiredFeatures,omitempty"`

	// ExcludedClasses lists AcceleratorClasses to avoid
	// +optional
	// +listType=atomic
	ExcludedClasses []string `json:"excludedClasses,omitempty"`

	// ArchitectureFamilies limits selection to specific families
	// Examples: ["nvidia-hopper", "nvidia-ampere"]
	// +optional
	// +listType=atomic
	ArchitectureFamilies []string `json:"architectureFamilies,omitempty"`

	// PreferredPrecisions lists numeric precisions in order of preference
	// Examples: ["fp8", "fp16", "fp32"]
	// +optional
	// +listType=atomic
	PreferredPrecisions []string `json:"preferredPrecisions,omitempty"`
}

// AcceleratorSelectionPolicy defines how to select among matching accelerators
type AcceleratorSelectionPolicy string

const (
	// BestFit selects the accelerator that best matches model requirements
	BestFitPolicy AcceleratorSelectionPolicy = "BestFit"

	// Cheapest selects the lowest cost accelerator that meets requirements
	CheapestPolicy AcceleratorSelectionPolicy = "Cheapest"

	// MostCapable selects the most powerful accelerator available
	MostCapablePolicy AcceleratorSelectionPolicy = "MostCapable"

	// FirstAvailable selects the first matching accelerator (fastest scheduling)
	FirstAvailablePolicy AcceleratorSelectionPolicy = "FirstAvailable"
)

// EngineSpec defines the configuration for the Engine component (can be used for both single-node and multi-node deployments)
// Provides a comprehensive specification for deploying model serving containers and pods.
// It allows for complete Kubernetes pod configuration including main containers,
// init containers, sidecars, volumes, and other pod-level settings.
// For distributed deployments, it supports leader-worker architecture configuration.
type EngineSpec struct {
	// This spec provides a full PodSpec for the engine component
	// Allows complete customization of the Kubernetes Pod configuration including
	// containers, volumes, security contexts, affinity rules, and other pod settings.
	// +optional
	PodSpec `json:",inline"`

	// ComponentExtensionSpec defines deployment configuration like min/max replicas, scaling metrics, etc.
	// Controls scaling behavior and resource allocation for the engine component.
	ComponentExtensionSpec `json:",inline"`

	// Runner container override for customizing the engine container
	// This is essentially a container spec that can override the default container
	// Defines the main model runner container configuration, including image,
	// resource requests/limits, environment variables, and command.
	// +optional
	Runner *RunnerSpec `json:"runner,omitempty"`

	// Leader node configuration (only used for multi-node deployment)
	// Defines the pod and container spec for the leader node that coordinates
	// distributed inference in multi-node deployments.
	// +optional
	Leader *LeaderSpec `json:"leader,omitempty"`

	// Worker nodes configuration (only used for multi-node deployment)
	// Defines the pod and container spec for worker nodes that perform
	// distributed processing tasks as directed by the leader.
	// +optional
	Worker *WorkerSpec `json:"worker,omitempty"`

	// TopologyKey is the node-label key (e.g. an NVLink/RDMA fabric-domain label)
	// used to co-locate all pods of one multi-node (leader+worker) gang
	// into a single network / NVLink topology domain. When set on a
	// multi-node component, OME auto-generates the per-instance
	// worker→leader podAffinity that anchors every worker to its gang's
	// leader on a node sharing this label value — so users need not
	// hand-write that affinity. Only meaningful for multi-node components;
	// ignored for single-pod (no leader to anchor to). Unset means no
	// auto-generated gang affinity.
	// +optional
	TopologyKey *string `json:"topologyKey,omitempty"`

	// TopologySpread spreads this component's instances across fault
	// domains so losing one domain (rack / fabric slice / cube) cannot
	// take out every instance at once. Rendered as a standard
	// topologySpreadConstraint (maxSkew 1) on each instance's anchor pod
	// (the leader, or the sole pod of a single-pod instance); workers
	// follow their leader, so gangs stay whole. Required renders as
	// DoNotSchedule — balanced spreading enforced by any scheduler that
	// keeps PodTopologySpread's Filter, including the OME gang
	// scheduler's packing profile. Preferred renders as ScheduleAnyway —
	// advisory only, and currently without effect under the OME gang
	// scheduler (its profile disables the spreading Score and pre-narrows
	// gang candidates to one domain). Not part of the revision hash:
	// changing it shapes future placements without rolling a healthy
	// fleet. Unset keeps pure bin-packing.
	// +optional
	// +kubebuilder:validation:Enum=Preferred;Required
	TopologySpread *TopologySpreadPolicy `json:"topologySpread,omitempty"`

	// TopologySpreadKey is the node-label key naming the fault domains
	// TopologySpread spreads across. Defaults to TopologyKey — right for
	// fabrics where the co-location domain IS the failure zone (an NVLink
	// rack). Set it to a coarser label when the failure zone contains many
	// gang domains: e.g. co-locate by the TPU partition label while
	// spreading across cloud.google.com/gce-topology-subblock. Single-pod
	// components have no co-location key, so spreading them requires
	// setting this explicitly. Ignored unless TopologySpread is set.
	// +optional
	TopologySpreadKey *string `json:"topologySpreadKey,omitempty"`

	// AcceleratorOverride allows overriding the global accelerator selection for this component
	// +optional
	AcceleratorOverride *AcceleratorSelector `json:"acceleratorOverride,omitempty"`
}

// TopologySpreadPolicy selects how a component's instances spread across
// fault domains, expressed through a rendered topologySpreadConstraint on
// each instance's anchor pod.
type TopologySpreadPolicy string

const (
	// TopologySpreadPreferred renders whenUnsatisfiable: ScheduleAnyway —
	// best-effort spreading, honored by schedulers that score topology
	// spreading.
	TopologySpreadPreferred TopologySpreadPolicy = "Preferred"
	// TopologySpreadRequired renders whenUnsatisfiable: DoNotSchedule with
	// maxSkew 1 — balanced spreading (instance counts per fault domain may
	// differ by at most one); a placement that would over-skew waits.
	TopologySpreadRequired TopologySpreadPolicy = "Required"
)

// DecoderSpec defines the configuration for the Decoder component (token generation in PD-disaggregated deployment)
// Used specifically for prefill-decode disaggregated deployments to handle the token generation phase.
// Similar to EngineSpec in structure, it allows for detailed pod and container configuration,
// but is specifically used for the decode phase when separating prefill and decode processes.
type DecoderSpec struct {
	// This spec provides a full PodSpec for the decoder component
	// Allows complete customization of the Kubernetes Pod configuration including
	// containers, volumes, security contexts, affinity rules, and other pod settings.
	// +optional
	PodSpec `json:",inline"`

	// ComponentExtensionSpec defines deployment configuration like min/max replicas, scaling metrics, etc.
	// Controls scaling behavior and resource allocation for the decoder component.
	ComponentExtensionSpec `json:",inline"`

	// Runner container override for customizing the main container
	// This is essentially a container spec that can override the default container
	// Defines the main decoder container configuration, including image,
	// resource requests/limits, environment variables, and command.
	// +optional
	Runner *RunnerSpec `json:"runner,omitempty"`

	// Leader node configuration (only used for multi-node deployment)
	// Defines the pod and container spec for the leader node that coordinates
	// distributed token generation in multi-node deployments.
	// +optional
	Leader *LeaderSpec `json:"leader,omitempty"`

	// Worker nodes configuration (only used for multi-node deployment)
	// Defines the pod and container spec for worker nodes that perform
	// distributed token generation tasks as directed by the leader.
	// +optional
	Worker *WorkerSpec `json:"worker,omitempty"`

	// TopologyKey is the node-label key (e.g. an NVLink/RDMA fabric-domain label)
	// used to co-locate all pods of one multi-node (leader+worker) gang
	// into a single network / NVLink topology domain. When set on a
	// multi-node component, OME auto-generates the per-instance
	// worker→leader podAffinity that anchors every worker to its gang's
	// leader on a node sharing this label value — so users need not
	// hand-write that affinity. Only meaningful for multi-node components;
	// ignored for single-pod (no leader to anchor to). Unset means no
	// auto-generated gang affinity.
	// +optional
	TopologyKey *string `json:"topologyKey,omitempty"`

	// TopologySpread spreads this component's instances across fault
	// domains so losing one domain (rack / fabric slice / cube) cannot
	// take out every instance at once. Rendered as a standard
	// topologySpreadConstraint (maxSkew 1) on each instance's anchor pod
	// (the leader, or the sole pod of a single-pod instance); workers
	// follow their leader, so gangs stay whole. Required renders as
	// DoNotSchedule — balanced spreading enforced by any scheduler that
	// keeps PodTopologySpread's Filter, including the OME gang
	// scheduler's packing profile. Preferred renders as ScheduleAnyway —
	// advisory only, and currently without effect under the OME gang
	// scheduler (its profile disables the spreading Score and pre-narrows
	// gang candidates to one domain). Not part of the revision hash:
	// changing it shapes future placements without rolling a healthy
	// fleet. Unset keeps pure bin-packing.
	// +optional
	// +kubebuilder:validation:Enum=Preferred;Required
	TopologySpread *TopologySpreadPolicy `json:"topologySpread,omitempty"`

	// TopologySpreadKey is the node-label key naming the fault domains
	// TopologySpread spreads across. Defaults to TopologyKey — right for
	// fabrics where the co-location domain IS the failure zone (an NVLink
	// rack). Set it to a coarser label when the failure zone contains many
	// gang domains: e.g. co-locate by the TPU partition label while
	// spreading across cloud.google.com/gce-topology-subblock. Single-pod
	// components have no co-location key, so spreading them requires
	// setting this explicitly. Ignored unless TopologySpread is set.
	// +optional
	TopologySpreadKey *string `json:"topologySpreadKey,omitempty"`

	// AcceleratorOverride allows overriding the global accelerator selection for this component
	// +optional
	AcceleratorOverride *AcceleratorSelector `json:"acceleratorOverride,omitempty"`
}

// LeaderSpec defines the configuration for a leader node in a multi-node component
// The leader node coordinates the activities of worker nodes in distributed inference or
// token generation setups, handling task distribution and result aggregation.
type LeaderSpec struct {
	// Pod specification for the leader node
	// This overrides the main PodSpec when specified
	// Allows customization of the Kubernetes Pod configuration specifically for the leader node.
	// +optional
	PodSpec `json:",inline"`

	// Runner container override for customizing the main container
	// This is essentially a container spec that can override the default container
	// Provides fine-grained control over the container that executes the leader node's coordination logic.
	// +optional
	Runner *RunnerSpec `json:"runner,omitempty"`
}

// WorkerSpec defines the configuration for worker nodes in a multi-node component
// Worker nodes perform the distributed processing tasks assigned by the leader node,
// enabling horizontal scaling for compute-intensive workloads.
type WorkerSpec struct {
	// PodSpec for the worker
	// Allows customization of the Kubernetes Pod configuration specifically for worker nodes.
	// +optional
	PodSpec `json:",inline"`

	// Size of the worker, this is the number of pods in the worker.
	// Controls how many worker pod instances will be deployed for horizontal scaling.
	// +optional
	Size *int `json:"size,omitempty"`

	// Runner container override for customizing the main container
	// This is essentially a container spec that can override the default container
	// Provides fine-grained control over the container that executes the worker node's processing logic.
	// +optional
	Runner *RunnerSpec `json:"runner,omitempty"`
}

// RouterSpec defines the configuration for the Router component, which handles request routing
type RouterSpec struct {
	// PodSpec defines the container configuration for the router
	PodSpec `json:",inline"`

	// ComponentExtensionSpec defines deployment configuration like min/max replicas, scaling metrics, etc.
	ComponentExtensionSpec `json:",inline"`

	// This is essentially a container spec that can override the default container
	// +optional
	Runner *RunnerSpec `json:"runner,omitempty"`

	// Additional configuration parameters for the runner
	// This can include framework-specific settings
	// +optional
	Config map[string]string `json:"config,omitempty"`
}

// RunnerSpec defines container configuration plus additional config settings
// The Runner is the primary container that executes the model serving or token generation logic.
type RunnerSpec struct {
	// Container spec for the runner
	// Provides complete Kubernetes container configuration for the primary execution container.
	// +optional
	v1.Container `json:",inline"`
}

type ModelRef struct {
	// Name of the model being referenced
	// Identifies the specific model to be used for inference.
	Name string `json:"name"`

	// Kind of the model being referenced
	// Defaults to ClusterBaseModel
	// Specifies the Kubernetes resource kind of the referenced model.
	// +kubebuilder:default="ClusterBaseModel"
	Kind *string `json:"kind,omitempty"`

	// APIGroup of the resource being referenced
	// Defaults to `ome.io`
	// Specifies the Kubernetes API group of the referenced model.
	// +kubebuilder:default="ome.io"
	APIGroup *string `json:"apiGroup,omitempty"`

	// Optional FineTunedWeights references
	// References to fine-tuned weights that should be applied to the base model.
	// +optional
	// +listType=atomic
	FineTunedWeights []string `json:"fineTunedWeights,omitempty"`

	// Overlays declares additional BaseModels attached to every serving
	// pod alongside the primary. Each overlay is mounted (PVC/PerNode)
	// or env-addressable (Sharded) at /opt/ml/model-overlays/<name> and
	// exposed via OVERLAY_<UPPERCASED_NAME>_MODEL_PATH. Runner selects
	// at request time; OME does not detect failures or auto-swap.
	// Disabled or NotReady overlays are silently omitted.
	// +optional
	// +listType=map
	// +listMapKey=name
	Overlays []ModelOverlayRef `json:"overlays,omitempty"`
}

// ModelOverlayRef references a BaseModel attached as an overlay.
// Narrower than ModelRef on purpose: no nested Overlays (type-system
// forbids recursion) and no FineTunedWeights (weight composition lives
// on the primary).
type ModelOverlayRef struct {
	Name string `json:"name"`
	// +kubebuilder:default="ClusterBaseModel"
	Kind *string `json:"kind,omitempty"`
	// +kubebuilder:default="ome.io"
	APIGroup *string `json:"apiGroup,omitempty"`
}

type ServingRuntimeRef struct {
	// Name of the runtime being referenced
	// Identifies the specific runtime environment to be used for model execution.
	Name string `json:"name"`

	// Kind of the runtime being referenced
	// Defaults to ClusterServingRuntime
	// Specifies the Kubernetes resource kind of the referenced runtime.
	// ClusterServingRuntime is a cluster-wide runtime, while ServingRuntime is namespace-scoped.
	// +kubebuilder:default="ClusterServingRuntime"
	Kind *string `json:"kind,omitempty"`

	// APIGroup of the resource being referenced
	// Defaults to `ome.io`
	// Specifies the Kubernetes API group of the referenced runtime.
	// +kubebuilder:default="ome.io"
	APIGroup *string `json:"apiGroup,omitempty"`

	// AutoSync (default true) re-renders pod specs from the live
	// runtime every reconcile. When false, the ISVC pins to a
	// ControllerRevision snapshot; bump ome.io/runtime-sync or set
	// spec.runtime.revision to roll forward.
	// +optional
	// +kubebuilder:default=true
	AutoSync *bool `json:"autoSync,omitempty"`

	// Revision pins to a named ControllerRevision in the OME
	// namespace. Enables rollback; ignored when AutoSync is true.
	// +optional
	Revision *string `json:"revision,omitempty"`
}

// InferenceService is the Schema for the InferenceServices API
// +k8s:openapi-gen=true
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="URL",type="string",JSONPath=".status.url"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="BaseModel",type="string",JSONPath=".spec.model.name"
// +kubebuilder:printcolumn:name="Runtime",type="string",JSONPath=".spec.runtime.name"
// +kubebuilder:printcolumn:name="Plan-Source",type="string",JSONPath=".status.rollout.activeRun.plan.groups[0].source",priority=1
// +kubebuilder:printcolumn:name="Plan-Drift",type="string",JSONPath=".status.conditions[?(@.type=='RolloutPlanDrift')].status",priority=1
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:resource:path=inferenceservices,shortName=isvc
// +kubebuilder:storageversion
type InferenceService struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec InferenceServiceSpec `json:"spec,omitempty"`

	// +kubebuilder:pruning:PreserveUnknownFields
	Status InferenceServiceStatus `json:"status,omitempty"`
}

// InferenceServiceList contains a list of Service
// +k8s:openapi-gen=true
// +kubebuilder:object:root=true
type InferenceServiceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []InferenceService `json:"items"`
}

func init() {
	SchemeBuilder.Register(&InferenceService{}, &InferenceServiceList{})
	SchemeBuilder.Register(&ServingRuntime{}, &ServingRuntimeList{})
	SchemeBuilder.Register(&ClusterServingRuntime{}, &ClusterServingRuntimeList{})
}
