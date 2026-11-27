package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// KVCachePool manages cache infrastructure shared by services in its namespace.
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,path=kvcachepools
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=".spec.provider.name"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
type KVCachePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              KVCachePoolSpec   `json:"spec"`
	Status            KVCachePoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type KVCachePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KVCachePool `json:"items"`
}

// +kubebuilder:validation:Enum=LMCache
type KVCacheProviderName string

const KVCacheProviderLMCache KVCacheProviderName = "LMCache"

// +kubebuilder:validation:Enum=NodeLocal
type KVCachePoolDeploymentMode string

const KVCachePoolNodeLocal KVCachePoolDeploymentMode = "NodeLocal"

type KVCachePoolSpec struct {
	Provider       KVCacheProviderSpec       `json:"provider"`
	DeploymentMode KVCachePoolDeploymentMode `json:"deploymentMode"`
	// Cache configures pool-wide policy.
	// +optional
	Cache *KVCachePoolPolicy `json:"cache,omitempty"`
	// Workloads selects the nodes on which OME runs its cache servers.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=1
	Workloads []KVCachePoolWorkload `json:"workloads"`
}

type KVCacheProviderSpec struct {
	Name   KVCacheProviderName   `json:"name"`
	Config KVCacheProviderConfig `json:"config"`
}

type KVCacheProviderConfig struct {
	// L1CapacityPerNode is the host-memory cache budget for each LMCache
	// server. Adding an eligible node adds one server with the same budget.
	// +kubebuilder:validation:XValidation:rule="quantity(string(self)).isGreaterThan(quantity('0'))",message="l1CapacityPerNode must be positive"
	L1CapacityPerNode resource.Quantity `json:"l1CapacityPerNode"`
}

// KVCachePoolPolicy describes pool-wide policy shared by providers.
// +kubebuilder:validation:XValidation:rule="!has(self.capacity)",message="LMCache NodeLocal uses spec.provider.config.l1CapacityPerNode; it cannot enforce a total pool capacity"
type KVCachePoolPolicy struct {
	// Capacity is the target capacity across the entire pool. LMCache
	// NodeLocal accepts a per-node budget only.
	// +optional
	Capacity *resource.Quantity `json:"capacity,omitempty"`
	// EvictionPolicy selects the policy for reclaiming cache entries.
	// +kubebuilder:validation:Enum=LRU
	// +optional
	EvictionPolicy string `json:"evictionPolicy,omitempty"`
}

type KVCachePoolWorkload struct {
	// Name identifies the provider workload role.
	// +kubebuilder:validation:Enum=server
	Name string `json:"name"`
	// NodeSelector explicitly selects the nodes eligible for one cache server each.
	// +kubebuilder:validation:MinProperties=1
	NodeSelector map[string]string `json:"nodeSelector"`
	// Tolerations apply to cache server Pods.
	// +listType=atomic
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// RuntimeClassName selects the NVIDIA runtime for the server; defaults to nvidia.
	// +optional
	RuntimeClassName string `json:"runtimeClassName,omitempty"`
	// Runner overrides the LMCache server container on each selected node.
	// +optional
	Runner *KVCacheServerRunner `json:"runner,omitempty"`
}

type KVCacheServerRunner struct {
	// Image is the server container image, including an optional tag or digest.
	// +optional
	Image string `json:"image,omitempty"`
	// Resources replaces the server's default container requests and limits.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// KVCachePoolRef identifies a KVCachePool in the service's namespace.
type KVCachePoolRef struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Name string `json:"name"`
	// +optional
	// +kubebuilder:validation:Enum=KVCachePool
	// +kubebuilder:default=KVCachePool
	Kind string `json:"kind,omitempty"`
	// +optional
	// +kubebuilder:validation:Enum=ome.io
	// +kubebuilder:default="ome.io"
	APIGroup string `json:"apiGroup,omitempty"`
}

// KVCacheConnectorSpec advertises a runtime's engine connector for a provider.
type KVCacheConnectorSpec struct {
	Provider KVCacheProviderName `json:"provider"`
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	DeploymentModes []KVCachePoolDeploymentMode `json:"deploymentModes"`
	Components      KVCacheConnectorComponents  `json:"components"`
}

type KVCacheConnectorComponents struct {
	Engine KVCacheEngineConnector `json:"engine"`
}

type KVCacheEngineConnector struct {
	ConnectorConfig KVCacheConnectorConfig `json:"connectorConfig"`
}

type KVCacheConnectorConfig struct {
	// ConnectorClass is the implementation loaded by the runtime. The LMCache
	// multiprocess connector uses the connection published by the pool.
	// +kubebuilder:validation:Enum=LMCacheMPConnector
	ConnectorClass string `json:"connectorClass"`
}

type KVCachePoolStatus struct {
	// ObservedGeneration identifies the pool spec reflected by this status.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	Connection *KVCachePoolConnectionStatus `json:"connection,omitempty"`
	// +listType=map
	// +listMapKey=name
	// +optional
	Workloads []KVCachePoolWorkloadStatus `json:"workloads,omitempty"`
}

type KVCachePoolConnectionStatus struct {
	// ConfigMapRef names the pool-managed connection ConfigMap in the pool's namespace.
	ConfigMapRef corev1.LocalObjectReference `json:"configMapRef"`
}

type KVCachePoolWorkloadStatus struct {
	Name            string `json:"name"`
	DesiredReplicas int32  `json:"desiredReplicas"`
	ReadyReplicas   int32  `json:"readyReplicas"`
}

func init() {
	SchemeBuilder.Register(&KVCachePool{}, &KVCachePoolList{})
}
