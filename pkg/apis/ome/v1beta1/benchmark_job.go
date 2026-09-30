package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BenchmarkJob runs genai-bench against an InferenceService or a URL and
// stores the results in the output location you choose.
// +k8s:openapi-gen=true
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.state"
// +kubebuilder:storageversion
type BenchmarkJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BenchmarkJobSpec   `json:"spec,omitempty"`
	Status BenchmarkJobStatus `json:"status,omitempty"`
}

// BenchmarkJobSpec defines the specification for a benchmark job.
// All fields within this specification collectively represent the desired
// state and configuration of a BenchmarkJob.
type BenchmarkJobSpec struct {
	// HuggingFaceSecretReference is a reference to a Kubernetes Secret containing the Hugging Face API key.
	// The referenced Secret must reside in the same namespace as the BenchmarkJob.
	// +optional
	HuggingFaceSecretReference *HuggingFaceSecretReference `json:"huggingFaceSecretReference,omitempty"`

	// Endpoint is the reference to the inference service to benchmark.
	// +required
	Endpoint EndpointSpec `json:"endpoint"`

	// ServiceMetadata records metadata about the backend model server or service being benchmarked.
	// This includes details such as server engine, version, and GPU configuration for filtering experiments.
	// +optional
	ServiceMetadata *ServiceMetadata `json:"serviceMetadata,omitempty"`

	// Task specifies the task to benchmark, pattern: <input-modality>-to-<output-modality> (e.g., "text-to-text", "image-to-text").
	// +kubebuilder:validation:Enum=text-to-text;image-to-text;text-to-embeddings;image-to-embeddings
	// +required
	Task string `json:"task"`

	// TrafficScenarios contains a list of traffic scenarios to simulate during the benchmark.
	// If not provided, defaults will be assigned via genai-bench.
	// +listType=set
	// +optional
	TrafficScenarios []string `json:"trafficScenarios,omitempty"`

	// NumConcurrency defines a list of concurrency levels to test during the benchmark.
	// If not provided, defaults will be assigned via genai-bench.
	// +listType=set
	// +optional
	NumConcurrency []int `json:"numConcurrency,omitempty"`

	// MaxTimePerIteration specifies the maximum time (in minutes) for a single iteration.
	// Each iteration runs for a specific combination of TrafficScenarios and NumConcurrency.
	// +required
	MaxTimePerIteration *int `json:"maxTimePerIteration"`

	// MaxRequestsPerIteration specifies the maximum number of requests for a single iteration.
	// Each iteration runs for a specific combination of TrafficScenarios and NumConcurrency.
	// +required
	MaxRequestsPerIteration *int `json:"maxRequestsPerIteration"`

	// AdditionalRequestParams contains additional request parameters as a map.
	// +optional
	AdditionalRequestParams map[string]string `json:"additionalRequestParams,omitempty"`

	// Dataset is the dataset used for benchmarking.
	// It is optional and only required for tasks other than "text-to-<output-modality>".
	// +optional
	Dataset *StorageSpec `json:"dataset,omitempty"`

	// OutputLocation specifies where the benchmark results will be stored (e.g., object storage).
	// +required
	OutputLocation *StorageSpec `json:"outputLocation"`

	// ResultFolderName specifies the name of the folder that stores the benchmark result. A default name will be assigned if not specified.
	// +optional
	ResultFolderName *string `json:"resultFolderName,omitempty"`

	// PodOverride customizes the benchmark pod. OME merges it into the pod
	// it builds by default, so fields you leave unset keep their defaults.
	// +optional
	PodOverride *PodOverride `json:"podOverride,omitempty"`
}

type PodOverride struct {
	// Image specifies the container image to use for the benchmark job.
	// +optional
	Image string `json:"image,omitempty"`

	// List of environment variables to set in the container.
	// +listType=map
	// +listMapKey=name
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`

	// List of sources to populate environment variables in the container.
	// +listType=atomic
	// +optional
	EnvFrom []corev1.EnvFromSource `json:"envFrom,omitempty"`

	// Pod volumes to mount into the container's filesystem.
	// +listType=map
	// +listMapKey=name
	// +optional
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty"`

	// Compute Resources required by this container.
	// Cannot be updated.
	// More info: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty" protobuf:"bytes,8,opt,name=resources"`

	// If specified, the pod's tolerations.
	// +optional
	// +listType=atomic
	Tolerations []corev1.Toleration `json:"tolerations,omitempty" protobuf:"bytes,22,opt,name=tolerations"`

	// NodeSelector is a selector which must be true for the pod to fit on a node.
	// Selector which must match a node's labels for the pod to be scheduled on that node.
	// More info: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/
	// +optional
	// +mapType=atomic
	NodeSelector map[string]string `json:"nodeSelector,omitempty" protobuf:"bytes,7,rep,name=nodeSelector"`

	// If specified, the pod's scheduling constraints
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty" protobuf:"bytes,18,opt,name=affinity"`

	// List of volumes that can be mounted by containers belonging to the pod.
	// More info: https://kubernetes.io/docs/concepts/storage/volumes
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge,retainKeys
	// +listType=atomic
	Volumes []corev1.Volume `json:"volumes,omitempty" patchStrategy:"merge,retainKeys" patchMergeKey:"name" protobuf:"bytes,1,rep,name=volumes"`
}

// HuggingFaceSecretReference defines a reference to a Kubernetes Secret containing the Hugging Face API key.
// This secret must reside in the same namespace as the BenchmarkJob.
// Cross-namespace references are not allowed for security and simplicity.
type HuggingFaceSecretReference struct {
	// Name of the secret containing the Hugging Face API key.
	// The secret must reside in the same namespace as the BenchmarkJob.
	// +required
	Name string `json:"name"`
}

// EndpointSpec defines a reference to an inference service.
// It supports either a Kubernetes-style reference (InferenceService) or an Endpoint struct for a direct URL.
// Cross-namespace references are supported for InferenceService but require appropriate RBAC permissions to access resources in the target namespace.
type EndpointSpec struct {
	// InferenceService holds a Kubernetes reference to an internal inference service.
	// +optional
	InferenceService *InferenceServiceReference `json:"inferenceService,omitempty"`

	// Endpoint holds the details of a direct endpoint for an external inference service, including URL and API details.
	// +optional
	Endpoint *Endpoint `json:"endpoint,omitempty"`
}

// InferenceServiceReference defines the reference to a Kubernetes inference service.
type InferenceServiceReference struct {
	// Name specifies the name of the inference service to benchmark.
	// +required
	Name string `json:"name"`

	// Namespace specifies the Kubernetes namespace where the inference service is deployed.
	// Cross-namespace references are allowed but require appropriate RBAC permissions.
	// +required
	Namespace string `json:"namespace"`
}

// Endpoint defines a direct URL-based inference service with additional API configuration.
type Endpoint struct {
	// URL represents the endpoint URL for the inference service.
	// +kubebuilder:validation:Pattern=`^(http|https)://`
	URL string `json:"url"`

	// APIFormat specifies the type of API, such as "openai" or "oci-cohere".
	// +kubebuilder:validation:Enum=openai;oci-cohere;cohere
	APIFormat string `json:"apiFormat"`

	// ModelName specifies the name of the model being served at the endpoint.
	// Useful for endpoints that require model-specific configuration. For instance,
	// for openai API, this is a required field in the payload
	ModelName string `json:"modelName,omitempty"`
}

// ServiceMetadata contains metadata fields for recording the backend model server's configuration and version details.
// This information helps track experiment context, enabling users to filter and query experiments based on server properties.
type ServiceMetadata struct {
	// Engine specifies the backend model server engine.
	// Supported values: "vLLM", "SGLang", "TGI".
	// +kubebuilder:validation:Enum=vLLM;SGLang;TGI
	Engine string `json:"engine"`

	// Version specifies the version of the model server (e.g., "0.5.3").
	Version string `json:"version"`

	// GpuType specifies the type of GPU used by the model server.
	// Supported values: "H100", "A100", "MI300", "A10".
	// +kubebuilder:validation:Enum=H100;A100;MI300;A10
	GpuType string `json:"gpuType"`

	// GpuCount indicates the number of GPU cards available on the model server.
	GpuCount int `json:"gpuCount"`
}

// BenchmarkJobStatus reflects the state and results of the benchmark job. It
// will be set and updated by the controller.
type BenchmarkJobStatus struct {
	// State represents the current state of the benchmark job: "Pending", "Running", "Completed", "Failed".
	// +kubebuilder:validation:Enum=Pending;Running;Completed;Failed
	// +required
	State string `json:"state"`

	// StartTime is the timestamp for when the benchmark job started.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is the timestamp for when the benchmark job completed, either successfully or unsuccessfully.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// LastReconcileTime is the timestamp for the last time the job was reconciled by the controller.
	// +optional
	LastReconcileTime *metav1.Time `json:"lastReconcileTime,omitempty"`

	// FailureMessage contains any error messages if the benchmark job failed.
	// +optional
	FailureMessage string `json:"failureMessage,omitempty"`

	// Details provide additional information or metadata about the benchmark job.
	// +optional
	Details string `json:"details,omitempty"`
}

// BenchmarkJobList contains a list of BenchmarkJob
// +k8s:openapi-gen=true
// +kubebuilder:object:root=true
type BenchmarkJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BenchmarkJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BenchmarkJob{}, &BenchmarkJobList{})
}
