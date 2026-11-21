// Package tpuslice carries the RBAC markers for the grants TPU slice
// provisioning (pkg/tpuslice, driven by the InferenceReplica reconciler)
// needs beyond the base manager role: create and delete on
// accelerator.gke.io Slices, plus the reads the provisioned-slice watch and
// state reporting use.
//
// The markers generate the ome-manager-tpu-slice ClusterRole, which the
// ome-resources chart renders only when tpuSliceProvisioning is configured.
// They live outside pkg/controller because controller-gen merges every
// marker in its scan paths into one role: kept next to the reconciler they
// would silently leak into the base ome-manager-role, which a deployment
// that provisions no TPU slices also holds.
package tpuslice

// +kubebuilder:rbac:groups=accelerator.gke.io,resources=slices,verbs=get;list;watch;create;delete
