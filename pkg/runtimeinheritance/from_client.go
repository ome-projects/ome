package runtimeinheritance

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// ResolveClusterRuntime walks the inherit-from chain rooted at the
// named ClusterServingRuntime and returns the merged spec. A missing
// runtime reads as ErrParentNotFound; a missing parent further up the
// chain is a *ParentNotFoundError.
func ResolveClusterRuntime(ctx context.Context, c client.Reader, name string) (*v1beta1.ServingRuntimeSpec, []string, error) {
	start, err := fetchClusterRuntime(ctx, c, name)
	if err != nil {
		return nil, nil, err
	}
	return Resolve(ctx, start, ClusterRuntimeFetcher(c), constants.RuntimeInheritMaxDepth)
}

// ResolveNamespacedRuntime walks the inherit-from chain rooted at the
// named namespaced ServingRuntime. Parents are looked up in the same
// namespace first, falling back to cluster scope. The errors read as in
// ResolveClusterRuntime.
func ResolveNamespacedRuntime(ctx context.Context, c client.Reader, namespace, name string) (*v1beta1.ServingRuntimeSpec, []string, error) {
	start, err := fetchNamespacedRuntime(ctx, c, namespace, name)
	if err != nil {
		return nil, nil, err
	}
	return Resolve(ctx, start, NamespacedRuntimeFetcher(c, namespace), constants.RuntimeInheritMaxDepth)
}
