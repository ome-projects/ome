package v1beta1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type ModelFormat struct {
	// Name of the format in which the model is stored, e.g., "ONNX", "TensorFlow SavedModel", "PyTorch", "SafeTensors"
	// +required
	Name string `json:"name"`
	// Version of the model format.
	// Used in validating that a runtime supports a predictor.
	// It Can be "major", "major.minor" or "major.minor.patch".
	// +optional
	Version *string `json:"version,omitempty"`
	// Operator for the selector with supported values: "Equal", "GreaterThan"
	// This is used to select the serving runtime based on the modelFormat version
	// +optional
	// +kubebuilder:default=Equal
	Operator *RuntimeSelectorOperator `json:"operator,omitempty"`
	// Weight of the model format in the runtime selector, used to prioritize modelFormat
	// +optional
	// +kubebuilder:default=1
	Weight int64 `json:"weight,omitempty"`
}

type ModelFrameworkSpec struct {
	// Name of the library in which the model is stored, e.g., "ONNXRuntime", "TensorFlow", "PyTorch", "Transformer", "TensorRTLLM"
	// +required
	Name string `json:"name"`
	// Version of the library.
	// Used in validating that a runtime supports a predictor.
	// It Can be "major", "major.minor" or "major.minor.patch".
	// +optional
	Version *string `json:"version,omitempty"`
	// Operator for the selector with supported values: "Equal", "GreaterThan"
	// This is used to select the serving runtime based on the modelFramework version
	// +optional
	// +kubebuilder:default=Equal
	Operator *RuntimeSelectorOperator `json:"operator,omitempty"`
	// Weight of the framework in the runtime selector, used to prioritize modelFramework
	// +optional
	// +kubebuilder:default=1
	Weight int64 `json:"weight,omitempty"`
}

// DiffusionComponentSpec captures an individual component used by a diffusion pipeline.
// The fields map directly to entries in a diffusers model_index.json file.
type DiffusionComponentSpec struct {
	// Library providing the component implementation, e.g., "diffusers" or "transformers".
	// +optional
	Library string `json:"library,omitempty"`

	// Type is the fully qualified class name for the component, e.g., "FlowMatchEulerDiscreteScheduler".
	// +optional
	Type string `json:"type,omitempty"`
}

// DiffusionPipelineSpec describes a diffusers pipeline so that runtimes can validate compatibility.
// When set, these fields should mirror the content of the model's model_index.json file.
type DiffusionPipelineSpec struct {
	// ClassName is the pipeline implementation, e.g., "StableDiffusionXLPipeline" or "QwenImagePipeline".
	// +optional
	ClassName *string `json:"className,omitempty"`

	// Scheduler component used by the pipeline.
	// +optional
	Scheduler *DiffusionComponentSpec `json:"scheduler,omitempty"`

	// TextEncoder component used by the pipeline.
	// +optional
	TextEncoder *DiffusionComponentSpec `json:"textEncoder,omitempty"`

	// Tokenizer component used by the pipeline.
	// +optional
	Tokenizer *DiffusionComponentSpec `json:"tokenizer,omitempty"`

	// Transformer (UNet/DiT) component used by the pipeline.
	// +optional
	Transformer *DiffusionComponentSpec `json:"transformer,omitempty"`

	// VAE component used by the pipeline.
	// +optional
	VAE *DiffusionComponentSpec `json:"vae,omitempty"`

	// AdditionalComponents captures any other pipeline parts keyed by their model_index.json entry.
	// +optional
	// +mapType=atomic
	AdditionalComponents map[string]DiffusionComponentSpec `json:"additionalComponents,omitempty" protobuf:"bytes,8,rep,name=additionalComponents"`
}

type RuntimeSelectorOperator string

const (
	RuntimeSelectorOpEqual              RuntimeSelectorOperator = "Equal"
	RuntimeSelectorOpGreaterThan        RuntimeSelectorOperator = "GreaterThan"
	RuntimeSelectorOpGreaterThanOrEqual RuntimeSelectorOperator = "GreaterThanOrEqual"
)

type StorageSpec struct {
	// Path is the absolute path where the model will be downloaded and stored on the node.
	// +optional
	Path *string `json:"path,omitempty"`

	// SchemaPath is the path to the model schema or configuration file within the storage system.
	// This can be used to validate the model or customize how it's loaded.
	// +optional
	SchemaPath *string `json:"schemaPath,omitempty"`

	// Parameters contain key-value pairs to override default storage credentials or configuration.
	// These values are typically used to configure access to object storage or mount options.
	// +optional
	Parameters *map[string]string `json:"parameters,omitempty"`

	// StorageKey is the name of the key in a Kubernetes Secret used to authenticate access to the model storage.
	// This key will be used to fetch credentials during model download or access.
	// +optional
	StorageKey *string `json:"key,omitempty"`

	// StorageUri specifies the source URI of the model in a supported storage backend.
	// Supported formats:
	// - OCI Object Storage:   oci://n/{namespace}/b/{bucket}/o/{object_path}
	// - Persistent Volume:    pvc://{pvc-name}/{sub-path}
	// - Vendor-specific:      vendor://{vendor-name}/{resource-type}/{resource-path}
	// - Hugging Face:         hf://{org}/{repo}[@{revision}]
	// - Amazon S3:            s3://{bucket}/{object_path}
	// - Azure Blob Storage:   az://{account}/{container}/{object_path}
	// - Google Cloud Storage: gs://{bucket}/{object_path}
	// - GitHub:               github://{org}/{repo}[@{tag}]
	// - Local filesystem:     local://{path}
	// This field is required.
	// +required
	StorageUri *string `json:"storageUri,omitempty"`

	// NodeSelector defines a set of key-value label pairs that must be present on a node
	// for the model to be scheduled and downloaded onto that node.
	// +optional
	// +mapType=atomic
	NodeSelector map[string]string `json:"nodeSelector,omitempty" protobuf:"bytes,7,rep,name=nodeSelector"`

	// NodeAffinity describes the node affinity rules that further constrain which nodes
	// are eligible to download and store this model, based on advanced scheduling policies.
	// +optional
	NodeAffinity *v1.NodeAffinity `json:"nodeAffinity,omitempty" protobuf:"bytes,1,opt,name=nodeAffinity"`

	// DownloadPolicy describes the policy of downloading model artifacts
	// Supported policies:
	// - AlwaysDownload: always download a copy of model artifact in destination path
	// - ReuseIfExists: if the identical model artifact has been downloaded in the node, such artifact will be reused
	// +optional
	DownloadPolicy *DownloadPolicy `json:"downloadPolicy,omitempty"`

	// DownloadPriority is the persistent user-selected priority for materializing
	// this model on eligible nodes. Serving demand observed by the model
	// controller can raise, but never lower, this priority.
	// +optional
	// +kubebuilder:default=Standard
	DownloadPriority *ModelDownloadPriority `json:"downloadPriority,omitempty"`
}

// +kubebuilder:validation:Enum=AlwaysDownload;ReuseIfExists
type DownloadPolicy string

const (
	AlwaysDownload DownloadPolicy = "AlwaysDownload"
	ReuseIfExists  DownloadPolicy = "ReuseIfExists"
)

// ModelDownloadPriority controls the relative order of queued node-local model
// downloads. It does not affect Kubernetes pod scheduling and does not preempt
// an active download.
// +kubebuilder:validation:Enum=Background;Standard;High
type ModelDownloadPriority string

const (
	ModelDownloadPriorityBackground ModelDownloadPriority = "Background"
	ModelDownloadPriorityStandard   ModelDownloadPriority = "Standard"
	ModelDownloadPriorityHigh       ModelDownloadPriority = "High"
)

// Distribution selects how a BaseModel's bytes are distributed across the cluster.
// +kubebuilder:validation:Enum=PerNode;Sharded
type Distribution string

const (
	// DistributionPerNode materializes a full copy of the model on each
	// selected node. The model agent downloads the model directly from its
	// source and places it on local storage. Default behavior; a nil
	// Distribution is treated as PerNode for backwards compatibility.
	DistributionPerNode Distribution = "PerNode"

	// DistributionSharded distributes the model as chunks across the cluster
	// via an external sharded cache. The BaseModel controller probes source
	// reachability and parses metadata through the cache; bytes are pulled
	// on demand at first inference (lazy fetch). The model agent skips this
	// BaseModel entirely. The specific sharded cache implementation is
	// configured at the cluster level.
	DistributionSharded Distribution = "Sharded"
)

// BaseModelSpec defines the desired state of BaseModel
type BaseModelSpec struct {
	// +optional
	ModelFormat ModelFormat `json:"modelFormat"`

	// ModelType defines the architecture family of the model (e.g., "bert", "gpt2", "llama").
	// This value typically corresponds to the "model_type" field in a Hugging Face model's config.json.
	// It is used to identify the transformer architecture and inform runtime selection and tokenizer behavior.
	// +optional
	ModelType *string `json:"modelType,omitempty"`

	// ModelFramework specifies the underlying framework used by the model,
	// such as "ONNX", "TensorFlow", "PyTorch", "Transformer", or "TensorRTLLM".
	// This value helps determine the appropriate runtime for model serving.
	// +optional
	ModelFramework *ModelFrameworkSpec `json:"modelFramework,omitempty"`

	// ModelArchitecture specifies the concrete model implementation or head,
	// such as "LlamaForCausalLM", "GemmaForCausalLM", or "MixtralForCausalLM".
	// This is often derived from the "architectures" field in Hugging Face config.json.
	// +optional
	ModelArchitecture *string `json:"modelArchitecture,omitempty"`

	// Quantization defines the quantization scheme applied to the model weights,
	// such as "fp8", "fbgemm_fp8", "int4", "nvfp4", or "mxfp4". This influences
	// runtime compatibility and performance.
	// +optional
	Quantization *ModelQuantization `json:"quantization,omitempty"`

	// ModelParameterSize indicates the total number of parameters in the model,
	// expressed in human-readable form such as "7B", "13B", or "175B".
	// This can be used for scheduling or runtime selection.
	// +optional
	ModelParameterSize *string `json:"modelParameterSize,omitempty"`

	// ModelCapabilities of the model, e.g., "TEXT_GENERATION", "TEXT_SUMMARIZATION", "TEXT_EMBEDDINGS"
	// +listType=atomic
	// +optional
	ModelCapabilities []string `json:"modelCapabilities,omitempty"`

	// API capabilities supported by the model, e.g., "OPENAI_V1_CHAT_COMPLETIONS"
	// +listType=atomic
	// +optional
	ApiCapabilities []string `json:"apiCapabilities,omitempty"`

	// Configuration of the model, stored as generic JSON for flexibility.
	// +optional
	ModelConfiguration runtime.RawExtension `json:"modelConfiguration,omitempty"`

	// Storage configuration for the model
	// +required
	Storage *StorageSpec `json:"storage,omitempty"`

	// Distribution selects how the model's bytes are made available across the
	// cluster. A nil value is treated as DistributionPerNode (the existing
	// per-node download behavior). Use DistributionSharded to opt the model
	// into a sharded distribution mode where chunks are spread across cluster
	// nodes by an external sharded cache.
	// +optional
	Distribution *Distribution `json:"distribution,omitempty"`

	// ModelExtension is the common extension of the model
	ModelExtensionSpec `json:",inline"`

	// ServingMode of the model, e.g., ["On-demand", "Dedicated"]
	// +optional
	// +listType=atomic
	ServingMode []string `json:"servingMode,omitempty"`

	// +optional
	// MaxTokens is the maximum number of tokens that can be processed by the model
	MaxTokens *int32 `json:"maxTokens,omitempty"`

	// DiffusionPipeline captures pipeline-specific metadata for diffusion models (from model_index.json).
	// +optional
	DiffusionPipeline *DiffusionPipelineSpec `json:"diffusionPipeline,omitempty"`

	// Additional metadata for the model
	// +optional
	AdditionalMetadata map[string]string `json:"additionalMetadata,omitempty"`
}

type ModelExtensionSpec struct {
	// DisplayName is the user-friendly name of the model
	// +optional
	DisplayName *string `json:"displayName,omitempty"`

	// +optional
	Version *string `json:"version,omitempty"`

	// Whether the model is enabled or not
	// +optional
	Disabled *bool `json:"disabled,omitempty"`

	// Vendor of the model, e.g., "NVIDIA", "Meta", "HuggingFace"
	// +optional
	Vendor *string `json:"vendor,omitempty"`

	// CompartmentID is the compartment ID of the model
	// +optional
	CompartmentID *string `json:"compartmentID,omitempty"`
}

// ServingMode enum
// +kubebuilder:validation:Enum=On-demand;Dedicated
type ServingMode string

const (
	// OnDemand Model Serving Mode
	OnDemand = "On-demand"
	// Dedicated Model Serving Mode
	Dedicated = "Dedicated"
)

type ModelQuantization string

const (
	ModelQuantizationFP8       ModelQuantization = "fp8"
	ModelQuantizationFbgemmFP8 ModelQuantization = "fbgemm_fp8"
	ModelQuantizationINT4      ModelQuantization = "int4"
	// ModelQuantizationNVFP4 is NVIDIA FP4. Used by ModelOpt's NVFP4 PTQ
	// pipeline; the on-disk convention packs two 4-bit values per byte
	// (uint8[out, in/2]) with FP8 per-group scales (group_size typically
	// 16) declared in hf_quant_config.json.
	ModelQuantizationNVFP4 ModelQuantization = "nvfp4"
	// ModelQuantizationMXFP4 is the OCP Microscaling FP4 format. Used by
	// the OpenAI gpt-oss family; declared via quant_method=mxfp4 in
	// quantization_config inside config.json.
	ModelQuantizationMXFP4 ModelQuantization = "mxfp4"
	// ModelQuantizationCompressedTensors is the compressed-tensors
	// container format (vLLM / llm-compressor). Unlike the values above it
	// names a FORMAT, not a precision: the actual bit-width (FP8 / INT8 /
	// INT4 / ...) is declared per-group in
	// quantization_config.config_groups. Declared via
	// quant_method="compressed-tensors" in config.json.
	ModelQuantizationCompressedTensors ModelQuantization = "compressed-tensors"
)

// ModelCapability enum
// +kubebuilder:validation:Enum=TEXT_GENERATION;TEXT_SUMMARIZATION;TEXT_EMBEDDINGS;TEXT_RERANK;CHAT;VISION;EMBEDDING;RERANK;TEXT_TO_TEXT;TEXT_TO_AUDIO;TEXT_TO_IMAGE;TEXT_TO_VIDEO;IMAGE_TEXT_TO_TEXT;IMAGE_TEXT_TO_AUDIO;IMAGE_TEXT_TO_IMAGE;IMAGE_TEXT_TO_VIDEO;VIDEO_TEXT_TO_TEXT;VIDEO_TEXT_TO_AUDIO;AUDIO_TO_TEXT;AUDIO_TEXT_TO_TEXT;AUDIO_TO_AUDIO;AUDIO_TRANSLATION
type ModelCapability string

const (
	// Deprecated: prefer the modality-pair values below.
	ModelCapabilityTextGeneration    ModelCapability = "TEXT_GENERATION"
	ModelCapabilityTextSummarization ModelCapability = "TEXT_SUMMARIZATION"
	ModelCapabilityTextEmbeddings    ModelCapability = "TEXT_EMBEDDINGS"
	ModelCapabilityTextRerank        ModelCapability = "TEXT_RERANK"
	ModelCapabilityChat              ModelCapability = "CHAT"
	ModelCapabilityVision            ModelCapability = "VISION"

	// Preferred capability names, expressed as modality pairs.
	ModelCapabilityEmbedding        ModelCapability = "EMBEDDING"
	ModelCapabilityRerank           ModelCapability = "RERANK"
	ModelCapabilityTextToText       ModelCapability = "TEXT_TO_TEXT"
	ModelCapabilityTextToAudio      ModelCapability = "TEXT_TO_AUDIO"
	ModelCapabilityTextToImage      ModelCapability = "TEXT_TO_IMAGE"
	ModelCapabilityTextToVideo      ModelCapability = "TEXT_TO_VIDEO"
	ModelCapabilityImageTextToText  ModelCapability = "IMAGE_TEXT_TO_TEXT"
	ModelCapabilityImageTextToAudio ModelCapability = "IMAGE_TEXT_TO_AUDIO"
	ModelCapabilityImageTextToImage ModelCapability = "IMAGE_TEXT_TO_IMAGE"
	ModelCapabilityImageTextToVideo ModelCapability = "IMAGE_TEXT_TO_VIDEO"
	ModelCapabilityVideoTextToText  ModelCapability = "VIDEO_TEXT_TO_TEXT"
	ModelCapabilityVideoTextToAudio ModelCapability = "VIDEO_TEXT_TO_AUDIO"
	ModelCapabilityAudioToText      ModelCapability = "AUDIO_TO_TEXT"
	ModelCapabilityAudioTextToText  ModelCapability = "AUDIO_TEXT_TO_TEXT"
	ModelCapabilityAudioToAudio     ModelCapability = "AUDIO_TO_AUDIO"
	ModelCapabilityAudioTranslation ModelCapability = "AUDIO_TRANSLATION"
	ModelCapabilityUnknown          ModelCapability = ""
)

// ModelAPICapability enum
// +kubebuilder:validation:Enum=OPENAI_V1_CHAT_COMPLETIONS;OPENAI_V1_RESPONSES;OPENAI_V1_EMBEDDINGS;OPENAI_V1_IMAGES_GENERATIONS;OPENAI_V1_IMAGES_EDITS;OPENAI_V1_AUDIO_SPEECH;OPENAI_V1_AUDIO_TRANSCRIPTIONS;OPENAI_V1_AUDIO_TRANSLATIONS;OPENAI_V1_REALTIME
type ModelAPICapability string

const (
	ModelAPICapabilityOpenAIv1ChatCompletions     ModelAPICapability = "OPENAI_V1_CHAT_COMPLETIONS"
	ModelAPICapabilityOpenAIv1Responses           ModelAPICapability = "OPENAI_V1_RESPONSES"
	ModelAPICapabilityOpenAIv1Embeddings          ModelAPICapability = "OPENAI_V1_EMBEDDINGS"
	ModelAPICapabilityOpenAIv1ImagesGenerations   ModelAPICapability = "OPENAI_V1_IMAGES_GENERATIONS"
	ModelAPICapabilityOpenAIv1ImagesEdits         ModelAPICapability = "OPENAI_V1_IMAGES_EDITS"
	ModelAPICapabilityOpenAIv1AudioSpeech         ModelAPICapability = "OPENAI_V1_AUDIO_SPEECH"
	ModelAPICapabilityOpenAIv1AudioTranscriptions ModelAPICapability = "OPENAI_V1_AUDIO_TRANSCRIPTIONS"
	ModelAPICapabilityOpenAIv1AudioTranslations   ModelAPICapability = "OPENAI_V1_AUDIO_TRANSLATIONS"
	ModelAPICapabilityOpenAIv1Realtime            ModelAPICapability = "OPENAI_V1_REALTIME"
)

// ModelWeightStatus enum
// +kubebuilder:validation:Enum=Deprecated;Experiment;Public;Internal
type ModelWeightStatus string

const (
	Deprecated = "Deprecated"
	Experiment = "Experiment"
	Public     = "Public"
	Internal   = "Internal"
)

// FineTunedWeightSpec defines the desired state of FineTunedWeight
type FineTunedWeightSpec struct {
	// Reference to the base model that this weight is fine-tuned from
	// +required
	BaseModelRef ObjectReference `json:"baseModelRef,omitempty"`

	// ModelType of the fine-tuned weight, e.g., "Distillation", "Adapter", "Tfew"
	// +required
	ModelType *string `json:"modelType,omitempty"` // e.g., "LoRA", "Adapter", "Distillation"

	// HyperParameters used for fine-tuning, stored as generic JSON for flexibility
	// +required
	HyperParameters runtime.RawExtension `json:"hyperParameters,omitempty"`

	// ModelExtension is the common extension of the model
	ModelExtensionSpec `json:",inline"`

	// Configuration of the fine-tuned weight, stored as generic JSON for flexibility
	// +optional
	Configuration runtime.RawExtension `json:"configuration,omitempty"`

	// Storage configuration for the fine-tuned weight
	// +required
	Storage *StorageSpec `json:"storage,omitempty"`

	// TrainingJobRef references the training job that produced this weight
	// +optional
	TrainingJobRef ObjectReference `json:"trainingJobRef,omitempty"`
}

// ObjectReference contains enough information to let you inspect or modify the referred object.
type ObjectReference struct {
	// Name of the referenced object
	// +required
	Name *string `json:"name,omitempty"`

	// Namespace of the referenced object
	Namespace *string `json:"namespace,omitempty"`
}

// LifeCycleState enum
// +kubebuilder:validation:Enum=Creating;Importing;In_Transit;In_Training;Ready;Failed
type LifeCycleState string

const (
	LifeCycleStateCreating   LifeCycleState = "Creating"
	LifeCycleStateImporting  LifeCycleState = "Importing"
	LifeCycleStateInTransit  LifeCycleState = "In_Transit"
	LifeCycleStateInTraining LifeCycleState = "In_Training"
	LifeCycleStateReady      LifeCycleState = "Ready"
	LifeCycleStateFailed     LifeCycleState = "Failed"
)

const (
	LifeCycleDetailImporting  string = "Creates Import Job"
	LifeCycleDetailInTransit  string = "Model is in transit"
	LifeCycleDetailInTraining string = "Model is in training"
	LifeCycleDetailReady      string = "Model is ready to use"
	LifeCycleDetailFailed     string = "Associated JobRun Failed"
)

const (
	// ModelConditionSourceReachable reports whether OME can access the model source.
	ModelConditionSourceReachable = "SourceReachable"
	// ModelConditionMetadataExtracted reports whether OME has extracted
	// enough metadata from the source (config.json / model_index.json /
	// safetensors headers) to populate the BaseModel spec.
	ModelConditionMetadataExtracted = "MetadataExtracted"
	// ModelConditionReady reports whether the model status is ready for serving consumers.
	ModelConditionReady = "Ready"

	// ModelConditionReasonNotChecked means the controller has not checked this condition yet.
	ModelConditionReasonNotChecked = "NotChecked"
	// ModelConditionReasonSourceReachable means the model source is reachable.
	ModelConditionReasonSourceReachable = "SourceReachable"
	// ModelConditionReasonSourceNotReachable means the model source is not reachable.
	ModelConditionReasonSourceNotReachable = "SourceNotReachable"
	// ModelConditionReasonMetadataPending means model metadata has not been parsed yet.
	ModelConditionReasonMetadataPending = "MetadataPending"
	// ModelConditionReasonMetadataParsed means model metadata has been parsed.
	ModelConditionReasonMetadataParsed = "MetadataParsed"
	// ModelConditionReasonMetadataUnparseable means the model source layout is
	// structurally incompatible with the cache provider's parsing requirements
	// (e.g. missing config.json/model_index.json or unresolvable safetensors
	// shards). The state is terminal until the spec or the source changes.
	ModelConditionReasonMetadataUnparseable = "MetadataUnparseable"
	// ModelConditionReasonProviderNotConfigured means no model cache provider is configured for the controller.
	ModelConditionReasonProviderNotConfigured = "ProviderNotConfigured"

	// ModelConditionReasonPVCInvalid means the PVC storage URI is malformed
	// or violates the BaseModel/ClusterBaseModel namespace rules.
	ModelConditionReasonPVCInvalid = "PVCInvalid"
	// ModelConditionReasonPVCNotFound means the referenced PVC does not exist.
	ModelConditionReasonPVCNotFound = "PVCNotFound"
	// ModelConditionReasonPVCNotBound means the referenced PVC exists but is not Bound.
	ModelConditionReasonPVCNotBound = "PVCNotBound"
	// ModelConditionReasonPVCValidated means the referenced PVC exists and is Bound.
	ModelConditionReasonPVCValidated = "PVCValidated"
	// ModelConditionReasonPVCMetadataPending means the PVC is valid but model
	// metadata has not yet been extracted.
	ModelConditionReasonPVCMetadataPending = "PVCMetadataPending"
	// ModelConditionReasonPVCMetadataExtracting means the metadata-extraction
	// Job is currently running against the PVC.
	ModelConditionReasonPVCMetadataExtracting = "PVCMetadataExtracting"
	// ModelConditionReasonPVCMetadataExtractionFailed means the metadata
	// extraction Job failed; reconcile will surface the Job's failure message.
	ModelConditionReasonPVCMetadataExtractionFailed = "PVCMetadataExtractionFailed"
	// ModelConditionReasonPVCMetadataReady means the metadata-extraction Job
	// completed successfully and the model is ready to serve.
	ModelConditionReasonPVCMetadataReady = "PVCMetadataReady"
	// ModelConditionReasonPVCConfigMissing means the controller has no
	// ome-agent image configured and therefore cannot spawn the metadata
	// extraction Job.
	ModelConditionReasonPVCConfigMissing = "PVCConfigMissing"
)

// ModelCacheStatus reports the cluster-wide cache provider identity
// for models that use a sharded distribution backend. In the lazy-
// fetch flow, the sharded-cache daemons pull bytes on demand at first
// inference, so there is no separate "placement progress" to report
// — Backend + SourceUri are enough to identify the cache mapping.
type ModelCacheStatus struct {
	// Backend identifies the configured cache provider.
	// +optional
	Backend string `json:"backend,omitempty"`

	// SourceUri is the model source URI being cached.
	// +optional
	SourceUri string `json:"sourceUri,omitempty"`
}

// ModelStatusSpec defines the observed state of Model weight
type ModelStatusSpec struct {
	// LifeCycle is an enum of Deprecated, Experiment, Public, Internal
	LifeCycle *string `json:"lifecycle,omitempty"`

	// Status of the model weight
	State LifeCycleState `json:"state"`

	// ObservedGeneration is the .metadata.generation the controller last
	// reconciled against. If this lags behind .metadata.generation, the
	// controller has not yet observed the latest spec edit.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastReconcileTime is the wall-clock timestamp of the controller's most
	// recent reconcile pass for this object. Liveness signal — if it falls
	// behind by more than the reconcile period, the controller is wedged or
	// not scheduled. Distinct from Condition.lastTransitionTime, which only
	// advances on actual status flips.
	// +optional
	LastReconcileTime *metav1.Time `json:"lastReconcileTime,omitempty"`

	// +listType=atomic
	NodesReady []string `json:"nodesReady,omitempty"`

	// +listType=atomic
	NodesFailed []string `json:"nodesFailed,omitempty"`

	// Conditions describe cluster-wide model readiness and cache state.
	// Sharded models report SourceReachable, MetadataExtracted, and Ready;
	// PerNode models continue to report node-level state through NodesReady
	// and NodesFailed.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Cache contains cluster-wide cache progress when the model uses a sharded
	// distribution backend.
	// +optional
	Cache *ModelCacheStatus `json:"cache,omitempty"`

	// DownloadScheduling is controller-derived scheduling state for node-local
	// model materialization. It is absent when no serving demand has been
	// observed.
	// +optional
	DownloadScheduling *ModelDownloadSchedulingStatus `json:"downloadScheduling,omitempty"`
}

// ModelDownloadSchedulingStatus contains bounded, controller-owned serving
// demand. Endpoint identities are intentionally omitted to keep model status
// low-cardinality and bounded.
type ModelDownloadSchedulingStatus struct {
	// ServingDemand is true while at least one live InferenceService references
	// this model.
	ServingDemand bool `json:"servingDemand,omitempty"`

	// ReferenceCount is the number of live InferenceServices referencing this
	// model.
	// +kubebuilder:validation:Minimum=0
	ReferenceCount int32 `json:"referenceCount,omitempty"`
}

// BaseModel is the Schema for the basemodels API
// +k8s:openapi-gen=true
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Disabled",type="boolean",JSONPath=".spec.disabled"
// +kubebuilder:printcolumn:name="Version",type="string",JSONPath=".spec.version"
// +kubebuilder:printcolumn:name="Vendor",type="string",JSONPath=".spec.vendor"
// +kubebuilder:printcolumn:name="Framework",type=string,JSONPath=".spec.modelFramework.name"
// +kubebuilder:printcolumn:name="FrameworkVersion",type=string,JSONPath=".spec.modelFramework.version"
// +kubebuilder:printcolumn:name="ModelFormat",type="string",JSONPath=".spec.modelFormat.name"
// +kubebuilder:printcolumn:name="Architecture",type="string",JSONPath=".spec.modelArchitecture"
// +kubebuilder:printcolumn:name="Capabilities",type="string",JSONPath=".spec.modelCapabilities[*]"
// +kubebuilder:printcolumn:name="Size",type="string",JSONPath=".spec.modelParameterSize"
// +kubebuilder:printcolumn:name="CompartmentID",type="string",JSONPath=".spec.compartmentID"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.state"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type BaseModel struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BaseModelSpec   `json:"spec,omitempty"`
	Status ModelStatusSpec `json:"status,omitempty"`
}

// ClusterBaseModel is the Schema for the basemodels API
// +k8s:openapi-gen=true
// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope="Cluster"
// +kubebuilder:printcolumn:name="Disabled",type="boolean",JSONPath=".spec.disabled"
// +kubebuilder:printcolumn:name="Version",type="string",JSONPath=".spec.version"
// +kubebuilder:printcolumn:name="Vendor",type="string",JSONPath=".spec.vendor"
// +kubebuilder:printcolumn:name="Framework",type=string,JSONPath=".spec.modelFramework.name"
// +kubebuilder:printcolumn:name="FrameworkVersion",type=string,JSONPath=".spec.modelFramework.version"
// +kubebuilder:printcolumn:name="ModelFormat",type="string",JSONPath=".spec.modelFormat.name"
// +kubebuilder:printcolumn:name="Architecture",type="string",JSONPath=".spec.modelArchitecture"
// +kubebuilder:printcolumn:name="Capabilities",type="string",JSONPath=".spec.modelCapabilities[*]"
// +kubebuilder:printcolumn:name="Size",type="string",JSONPath=".spec.modelParameterSize"
// +kubebuilder:printcolumn:name="CompartmentID",type="string",JSONPath=".spec.compartmentID"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.state"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type ClusterBaseModel struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BaseModelSpec   `json:"spec,omitempty"`
	Status ModelStatusSpec `json:"status,omitempty"`
}

// BaseModelList contains a list of BaseModel
// +k8s:openapi-gen=true
// +kubebuilder:object:root=true
type BaseModelList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BaseModel `json:"items"`
}

// ClusterBaseModelList contains a list of ClusterBaseModel
// +k8s:openapi-gen=true
// +kubebuilder:object:root=true
type ClusterBaseModelList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterBaseModel `json:"items"`
}

// FineTunedWeight is the Schema for the finetunedweights API
// +k8s:openapi-gen=true
// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope="Cluster"
// +kubebuilder:printcolumn:name="Disabled",type="boolean",JSONPath=".spec.disabled"
// +kubebuilder:printcolumn:name="Version",type="string",JSONPath=".spec.version"
// +kubebuilder:printcolumn:name="Vendor",type="string",JSONPath=".spec.vendor"
// +kubebuilder:printcolumn:name="CompartmentID",type="string",JSONPath=".spec.compartmentID"
// +kubebuilder:printcolumn:name="ModelType",type="string",JSONPath=".spec.modelType"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.state"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type FineTunedWeight struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FineTunedWeightSpec `json:"spec,omitempty"`
	Status ModelStatusSpec     `json:"status,omitempty"`
}

// FineTunedWeightList contains a list of FineTunedWeight
// +k8s:openapi-gen=true
// +kubebuilder:object:root=true
type FineTunedWeightList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FineTunedWeight `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BaseModel{}, &BaseModelList{})
	SchemeBuilder.Register(&FineTunedWeight{}, &FineTunedWeightList{})
	SchemeBuilder.Register(&ClusterBaseModel{}, &ClusterBaseModelList{})
}
