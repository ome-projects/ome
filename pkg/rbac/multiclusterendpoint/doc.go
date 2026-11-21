// Package multiclusterendpoint carries the RBAC markers for the grants the
// multi-cluster placement endpoint publisher (pkg/controller/v1beta1/placement/
// endpoint) needs beyond the base manager role: writes on EndpointSlices and
// on gateway.networking.k8s.io BackendTLSPolicies, the resources that steer
// service routing and backend TLS for any workload on the cluster.
//
// The markers generate the ome-manager-multicluster-endpoint ClusterRole,
// which the ome-resources chart renders only when the multi-cluster control
// plane is enabled. They live outside pkg/controller because controller-gen
// merges every marker in its scan paths into one role: kept next to the
// endpoint controller they would silently leak into the base
// ome-manager-role, which a single-cluster deployment also holds.
package multiclusterendpoint

// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=backendtlspolicies,verbs=get;list;watch;create;update;patch;delete
