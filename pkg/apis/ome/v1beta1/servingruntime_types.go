package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/constants"
)

// +k8s:openapi-gen=true
type SupportedModelFormat struct {
	// Name of the model format this entry describes; matched against the
	// model's format name during runtime selection. ModelFormat.Name is
	// the preferred field for new runtimes.
	// +optional
	Name string `json:"name"`
	// ModelFormat of the model, e.g., "PyTorch", "TensorFlow", "ONNX", "SafeTensors"
	// +required
	ModelFormat *ModelFormat `json:"modelFormat"`
	// Deprecated: not consulted during runtime selection.
	// +optional
	ModelType *string `json:"modelType,omitempty"`
	// Version of the model format.
	// Runtime selection doesn't read it: to match on a version, set modelFormat.version.
	// It can be "major", "major.minor" or "major.minor.patch".
	// +optional
	Version *string `json:"version,omitempty"`
	// ModelFramework of the model, e.g., "PyTorch", "TensorFlow", "ONNX", "Transformers"
	// +required
	ModelFramework *ModelFrameworkSpec `json:"modelFramework,omitempty"`
	// ModelArchitecture of the model, e.g., "LlamaForCausalLM", "GemmaForCausalLM", "MixtralForCausalLM"
	// +optional
	ModelArchitecture *string `json:"modelArchitecture,omitempty"`

	// Quantization of the model, e.g., "fp8", "fbgemm_fp8", "int4"
	// +optional
	Quantization *ModelQuantization `json:"quantization,omitempty"`

	// DiffusionPipeline supported by this runtime (used for diffusion models).
	// +optional
	DiffusionPipeline *DiffusionPipelineSpec `json:"diffusionPipeline,omitempty"`

	// ModelCacheProviders lists the external model caches this runtime can
	// load a Sharded model from. Sharded distribution is in development: OME
	// can't serve a Sharded model yet, so this list has no effect.
	// +optional
	// +listType=atomic
	// +ome:since=v1.3
	ModelCacheProviders []ModelCacheProvider `json:"modelCacheProviders,omitempty"`

	// Set to true to allow the ServingRuntime to be used for automatic model placement if
	// this model format is specified with no explicit runtime.
	// +optional
	AutoSelect *bool `json:"autoSelect,omitempty"`

	// Priority of this serving runtime for auto selection.
	// This is used to select the serving runtime if more than one serving runtime supports the same model format.
	// The value should be greater than zero.  The higher the value, the higher the priority.
	// Priority is not considered if AutoSelect is either false or not specified.
	// Priority can be overridden by specifying the runtime in the InferenceService.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Priority *int32 `json:"priority,omitempty"`

	// AcceleratorConfig provides accelerator-specific overrides for this model format
	// +optional
	AcceleratorConfig map[string]*AcceleratorModelConfig `json:"acceleratorConfig,omitempty"`
}

// ModelCacheProvider names an external model cache that a runtime can load
// Sharded models from. Sharded distribution is in development.
type ModelCacheProvider string

const (
	// DragonFly is the provider name reserved for the sharded model cache,
	// which is in development.
	DragonFly ModelCacheProvider = "model_cache"
)

// AcceleratorModelConfig provides accelerator-specific overrides for this model format
// +k8s:openapi-gen=true
type AcceleratorModelConfig struct {
	// MinMemoryPerBillionParams specifies memory required per billion parameters.
	// OME doesn't read it.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinMemoryPerBillionParams *int64 `json:"minMemoryPerBillionParams,omitempty"`

	// TensorParallelismOverride overrides the default tensor parallelism settings
	// +optional
	TensorParallelismOverride *TensorParallelismConfig `json:"tensorParallelismOverride,omitempty"`

	// RuntimeArgsOverride provides accelerator-specific runtime arguments
	// +optional
	// +listType=atomic
	RuntimeArgsOverride []string `json:"runtimeArgsOverride,omitempty"`

	// EnvironmentOverride provides accelerator-specific environment variables
	// +optional
	EnvironmentOverride map[string]string `json:"environmentOverride,omitempty"`
}

// TensorParallelismConfig specifies tensor parallelism settings
// +k8s:openapi-gen=true
type TensorParallelismConfig struct {
	// tensorParallelSize specifies the size of the tensor parallelism
	// +optional
	// +kubebuilder:validation:Minimum=0
	TensorParallelSize *int64 `json:"tensorParallelSize,omitempty"`
	// pipelineParallelSize specifies the size of the pipeline parallelism
	// +optional
	// +kubebuilder:validation:Minimum=0
	PipelineParallelSize *int64 `json:"pipelineParallelSize,omitempty"`
	// dataParallelSize specifies the size of the data parallelism
	// +optional
	// +kubebuilder:validation:Minimum=0
	DataParallelSize *int64 `json:"dataParallelSize,omitempty"`
}

// +k8s:openapi-gen=true
type ServingRuntimePodSpec struct {
	// List of containers belonging to the pod.
	// Containers cannot currently be added or removed.
	// Cannot be updated.
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +optional
	Containers []corev1.Container `json:"containers" patchStrategy:"merge" patchMergeKey:"name"`

	// List of volumes that can be mounted by containers belonging to the pod.
	// More info: https://kubernetes.io/docs/concepts/storage/volumes
	// +optional
	// +patchMergeKey=name
	// +listType=map
	// +listMapKey=name
	// +patchStrategy=merge,retainKeys
	Volumes []corev1.Volume `json:"volumes,omitempty" patchStrategy:"merge,retainKeys" patchMergeKey:"name" protobuf:"bytes,1,rep,name=volumes"`

	// NodeSelector is a selector which must be true for the pod to fit on a node.
	// Selector which must match a node's labels for the pod to be scheduled on that node.
	// More info: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// If specified, the pod's scheduling constraints
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// If specified, the pod's tolerations.
	// +listType=atomic
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Labels that will be added to the pod.
	// More info: http://kubernetes.io/docs/user-guide/labels
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations that will be added to the pod.
	// More info: http://kubernetes.io/docs/user-guide/annotations
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
	// ImagePullSecrets is an optional list of references to secrets in the same namespace to use for pulling any of the images used by this PodSpec.
	// If specified, these secrets will be passed to individual puller implementations for them to use.
	// More info: https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,15,rep,name=imagePullSecrets"`

	// If specified, the pod will be dispatched by specified scheduler.
	// If not specified, the pod will be dispatched by default scheduler.
	// +optional
	SchedulerName string `json:"schedulerName,omitempty" protobuf:"bytes,19,opt,name=schedulerName"`

	// Use the host's ipc namespace.
	// Optional: Default to false.
	// +k8s:conversion-gen=false
	// +optional
	HostIPC bool `json:"hostIPC,omitempty" protobuf:"varint,13,opt,name=hostIPC"`

	// Set DNS policy for the pod.
	// Defaults to "ClusterFirst".
	// Valid values are 'ClusterFirstWithHostNet', 'ClusterFirst', 'Default' or 'None'.
	// DNS parameters given in DNSConfig will be merged with the policy selected with DNSPolicy.
	// To have DNS options set along with hostNetwork, you have to specify DNS policy
	// explicitly to 'ClusterFirstWithHostNet'.
	// +optional
	DNSPolicy corev1.DNSPolicy `json:"dnsPolicy,omitempty" protobuf:"bytes,6,opt,name=dnsPolicy,casttype=DNSPolicy"`

	// Host networking requested for this pod. Use the host's network namespace.
	// If this option is set, the ports that will be used must be specified.
	// Default to false.
	// +k8s:conversion-gen=false
	// +optional
	HostNetwork bool `json:"hostNetwork,omitempty" protobuf:"varint,11,opt,name=hostNetwork"`
}

// ServingRuntimeSpec defines the desired state of ServingRuntime.
// +k8s:openapi-gen=true
type ServingRuntimeSpec struct {
	// Model formats and version supported by this runtime
	// +listType=atomic
	SupportedModelFormats []SupportedModelFormat `json:"supportedModelFormats,omitempty"`

	// ModelSizeRange is the range of model sizes supported by this runtime
	// +optional
	ModelSizeRange *ModelSizeRangeSpec `json:"modelSizeRange,omitempty"`

	// Set to true to disable use of this runtime
	// +optional
	Disabled *bool `json:"disabled,omitempty"`

	// Router configuration for this runtime
	// +optional
	RouterConfig *RouterSpec `json:"routerConfig,omitempty"`

	// Engine configuration for this runtime
	// +optional
	EngineConfig *EngineSpec `json:"engineConfig,omitempty"`
	// Decoder configuration for this runtime
	// +optional
	DecoderConfig *DecoderSpec `json:"decoderConfig,omitempty"`

	// ProtocolVersions lists the inference protocols the runtime serves:
	// openAI, openInference-v1 or openInference-v2. Runtime selection
	// doesn't read it.
	// +optional
	// +listType=atomic
	ProtocolVersions []constants.InferenceServiceProtocol `json:"protocolVersions,omitempty"`

	// PodSpec for the serving runtime
	ServingRuntimePodSpec `json:",inline"`

	// ScalingPolicy default for InferenceServices binding this runtime.
	// Replaced wholesale by isvc.spec.scalingPolicy when set on the ISVC.
	// Alpha. The API may change without notice.
	// +optional
	// +ome:since=v1.3
	ScalingPolicy *ScalingPolicy `json:"scalingPolicy,omitempty"`

	// AcceleratorRequirements specifies the accelerator requirements for this runtime
	// +optional
	AcceleratorRequirements *AcceleratorRequirements `json:"acceleratorRequirements,omitempty"`
}

// AcceleratorRequirements specifies the accelerator requirements for this runtime
// +k8s:openapi-gen=true
type AcceleratorRequirements struct {
	// AcceleratorClasses lists the names of AcceleratorClasses this runtime supports
	// +optional
	// +listType=atomic
	AcceleratorClasses []string `json:"acceleratorClasses,omitempty"`

	// MinMemory specifies minimum GPU memory required in GB
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinMemory *int64 `json:"minMemory,omitempty"`

	// MinComputePerformanceTFLOPS specifies minimum compute capability in TFLOPS
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinComputePerformanceTFLOPS *int64 `json:"minComputePerformanceTFLOPS,omitempty"`

	//MinArchitectureVersion Compute capability (NVIDIA) or equivalent
	// +optional
	MinArchitectureVersion *string `json:"minArchitectureVersion,omitempty"`

	// RequiredFeatures lists hardware features that must be present.
	// Examples: ["tensor-cores", "fp8", "nvlink"].
	// +optional
	// +listType=atomic
	RequiredFeatures []string `json:"requiredFeatures,omitempty"`

	// PreferredPrecisions lists numeric precisions in order of preference.
	// Examples: ["fp8", "fp16", "fp32"].
	// +optional
	// +listType=atomic
	PreferredPrecisions []string `json:"preferredPrecisions,omitempty"`
}

// ModelSizeRangeSpec defines the range of model sizes supported by this runtime
// +k8s:openapi-gen=true
type ModelSizeRangeSpec struct {
	// Minimum model parameter count supported by this runtime, given as a
	// string with an optional suffix M (million), B (billion), or T (trillion),
	// e.g. "1B", "7B", "70B", "1T". It is compared against the model's parameter
	// size during runtime auto-selection. A value not in the <number>[M|B|T]
	// form is treated as 0.
	// +optional
	Min *string `json:"min,omitempty"`
	// Maximum model parameter count supported by this runtime, given as a
	// string with an optional suffix M (million), B (billion), or T (trillion),
	// e.g. "1B", "7B", "70B", "1T". It is compared against the model's parameter
	// size during runtime auto-selection. A value not in the <number>[M|B|T]
	// form is treated as 0.
	// +optional
	Max *string `json:"max,omitempty"`
}

// ServingRuntimeStatus defines the observed state of ServingRuntime
// +k8s:openapi-gen=true
type ServingRuntimeStatus struct {
	// InheritanceChain lists the runtime names walked when resolving
	// the ome.io/inherit-from chain, in root-first order. Single-entry
	// list ([self]) when no inheritance is declared. Operator-visible
	// only — consumers re-walk the chain in-memory at use time rather
	// than reading a pre-computed spec from status (avoids storing
	// the fully-merged spec, which is large).
	// +optional
	// +listType=atomic
	// +ome:since=v1.3
	InheritanceChain []string `json:"inheritanceChain,omitempty"`

	// Conditions reflect the runtime controller's view of inheritance
	// resolution. Currently only InheritanceReady (True/False) is
	// surfaced. Informational — consumers can block on
	// InheritanceReady=False if they want to gate on chain health.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +ome:since=v1.3
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ServingRuntime defines, within one namespace, the pods that serve a model
// and the model formats and sizes it supports, so OME can select it
// automatically.
// +k8s:openapi-gen=true
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Disabled",type="boolean",JSONPath=".spec.disabled"
// +kubebuilder:printcolumn:name="ModelFormat",type=string,JSONPath=".spec.supportedModelFormats[*].modelFormat.name"
// +kubebuilder:printcolumn:name="ModelFramework",type=string,JSONPath=".spec.supportedModelFormats[*].modelFramework.name"
// +kubebuilder:printcolumn:name="ModelFrameworkVersion",type=string,JSONPath=".spec.supportedModelFormats[*].modelFramework.version"
// +kubebuilder:printcolumn:name="ModelArchitecture",type="string",JSONPath=".spec.supportedModelFormats[*].modelArchitecture"
// +kubebuilder:printcolumn:name="ModelSizeMin",type="string",JSONPath=".spec.modelSizeRange.min"
// +kubebuilder:printcolumn:name="ModelSizeMax",type="string",JSONPath=".spec.modelSizeRange.max"
// +kubebuilder:printcolumn:name="Images",type="string",JSONPath=".spec.containers[*].image"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type ServingRuntime struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ServingRuntimeSpec   `json:"spec,omitempty"`
	Status ServingRuntimeStatus `json:"status,omitempty"`
}

// ServingRuntimeList contains a list of ServingRuntime
// +k8s:openapi-gen=true
// +kubebuilder:object:root=true
type ServingRuntimeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ServingRuntime `json:"items"`
}

// ClusterServingRuntime defines serving pods for the whole cluster: it has
// the same spec as a ServingRuntime, but it's cluster-scoped rather than
// namespaced.
// +k8s:openapi-gen=true
// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope="Cluster"
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Disabled",type="boolean",JSONPath=".spec.disabled"
// +kubebuilder:printcolumn:name="ModelFormat",type=string,JSONPath=".spec.supportedModelFormats[*].modelFormat.name"
// +kubebuilder:printcolumn:name="ModelFramework",type=string,JSONPath=".spec.supportedModelFormats[*].modelFramework.name"
// +kubebuilder:printcolumn:name="ModelFrameworkVersion",type=string,JSONPath=".spec.supportedModelFormats[*].modelFramework.version"
// +kubebuilder:printcolumn:name="ModelArchitecture",type="string",JSONPath=".spec.supportedModelFormats[*].modelArchitecture"
// +kubebuilder:printcolumn:name="ModelSizeMin",type="string",JSONPath=".spec.modelSizeRange.min"
// +kubebuilder:printcolumn:name="ModelSizeMax",type="string",JSONPath=".spec.modelSizeRange.max"
// +kubebuilder:printcolumn:name="Images",type="string",JSONPath=".spec.containers[*].image"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type ClusterServingRuntime struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ServingRuntimeSpec   `json:"spec,omitempty"`
	Status ServingRuntimeStatus `json:"status,omitempty"`
}

// ClusterServingRuntimeList contains a list of ServingRuntime
// +k8s:openapi-gen=true
// +kubebuilder:object:root=true
type ClusterServingRuntimeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterServingRuntime `json:"items"`
}

// SupportedRuntime is the schema for supported runtime result of automatic selection
type SupportedRuntime struct {
	Name string
	Spec ServingRuntimeSpec
}

func (srSpec *ServingRuntimeSpec) IsDisabled() bool {
	return srSpec.Disabled != nil && *srSpec.Disabled
}

func (srSpec *ServingRuntimeSpec) IsProtocolVersionSupported(modelProtocolVersion constants.InferenceServiceProtocol) bool {
	if len(modelProtocolVersion) == 0 || srSpec.ProtocolVersions == nil || len(srSpec.ProtocolVersions) == 0 {
		return true
	}
	for _, srProtocolVersion := range srSpec.ProtocolVersions {
		if srProtocolVersion == modelProtocolVersion {
			return true
		}
	}
	return false
}

// GetPriority returns the priority of the specified model. It returns nil if priority is not set or the model is not found.
func (srSpec *ServingRuntimeSpec) GetPriority(modelName string) *int32 {
	for _, model := range srSpec.SupportedModelFormats {
		if model.Name == modelName {
			return model.Priority
		}
	}
	return nil
}

func (f *SupportedModelFormat) IsAutoSelectEnabled() bool {
	return f.AutoSelect != nil && *f.AutoSelect
}

func (srSpec *ServingRuntimeSpec) SupportsAcceleratorClass(acceleratorClass string) bool {
	if srSpec.AcceleratorRequirements == nil {
		return false
	}
	for _, supported := range srSpec.AcceleratorRequirements.AcceleratorClasses {
		if supported == acceleratorClass {
			return true
		}
	}
	return false
}

// GetAcceleratorConfig returns the accelerator-specific config for a model format
func (f *SupportedModelFormat) GetAcceleratorConfig(acceleratorClass string) *AcceleratorModelConfig {
	if f.AcceleratorConfig == nil {
		return nil
	}
	config, ok := f.AcceleratorConfig[acceleratorClass]
	if !ok {
		return nil
	}
	return config
}
