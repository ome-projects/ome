package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WorkloadClusterReady is the condition type reporting whether the control
// plane can currently reach the workload cluster. It is the ONLY status this
// registry carries — capacity is never mirrored here; a placer
// reads capacity live from the cluster's own Kueue.
const WorkloadClusterReady = "Ready"

// WorkloadCluster registers a workload cluster, typically a GPU cluster, that
// OME can place workloads onto. Cluster-scoped: it is fleet-level
// infrastructure, not a namespaced workload.
// Alpha. The API may change without notice.
// +genclient
// +genclient:nonNamespaced
// +k8s:openapi-gen=true
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=wlc
// +kubebuilder:printcolumn:name="Reachable",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +ome:since=v1.3
type WorkloadCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WorkloadClusterSpec   `json:"spec,omitempty"`
	Status WorkloadClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type WorkloadClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WorkloadCluster `json:"items"`
}

type WorkloadClusterSpec struct {
	// ClusterSource is how the control plane connects to this cluster.
	// +required
	ClusterSource ClusterConnectionSource `json:"clusterSource"`
}

// ClusterConnectionSource is a pluggable connection (modeled on Kueue's
// MultiKueueCluster.ClusterSource): exactly one of an in-cluster kubeconfig
// Secret or a SIG-Multicluster ClusterProfile reference.
// +kubebuilder:validation:XValidation:rule="has(self.kubeConfig) != has(self.clusterProfileRef)",message="exactly one of kubeConfig or clusterProfileRef must be set"
type ClusterConnectionSource struct {
	// +optional
	KubeConfig *KubeConfigSource `json:"kubeConfig,omitempty"`
	// +optional
	ClusterProfileRef *ClusterProfileRef `json:"clusterProfileRef,omitempty"`
}

// KubeConfigSource points at a Secret holding a kubeconfig for the cluster.
type KubeConfigSource struct {
	// SecretRef is the Secret holding the kubeconfig.
	// +required
	SecretRef corev1.SecretReference `json:"secretRef"`
	// Key is the Secret data key holding the kubeconfig. Defaults to "kubeconfig".
	// +optional
	// +kubebuilder:default="kubeconfig"
	Key string `json:"key,omitempty"`
}

// ClusterProfileRef references a SIG-Multicluster cluster-inventory-api
// ClusterProfile. The reference is recorded but not resolved.
type ClusterProfileRef struct {
	// +required
	Name string `json:"name"`
}

type WorkloadClusterStatus struct {
	// Conditions report connection health ONLY (the Ready condition). Never
	// capacity, never Kueue/quota state.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

func init() {
	SchemeBuilder.Register(&WorkloadCluster{}, &WorkloadClusterList{})
}
