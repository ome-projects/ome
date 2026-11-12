package runtimeinheritance

import (
	"context"
	"errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// ClusterRuntimeRef is the resolver's view of a ClusterServingRuntime: its
// name, its spec and the parent its inherit-from annotation names.
func ClusterRuntimeRef(csr *v1beta1.ClusterServingRuntime) *RuntimeRef {
	return &RuntimeRef{
		Name:       csr.Name,
		Spec:       &csr.Spec,
		ParentName: csr.Annotations[constants.RuntimeInheritFromAnnotationKey],
	}
}

// NamespacedRuntimeRef is the resolver's view of a ServingRuntime.
func NamespacedRuntimeRef(sr *v1beta1.ServingRuntime) *RuntimeRef {
	return &RuntimeRef{
		Name:       sr.Name,
		Spec:       &sr.Spec,
		ParentName: sr.Annotations[constants.RuntimeInheritFromAnnotationKey],
	}
}

// ClusterRuntimeFetcher looks up the parents of a ClusterServingRuntime
// through c. A cluster-scoped runtime inherits from cluster scope only.
func ClusterRuntimeFetcher(c client.Reader) Fetcher {
	return func(ctx context.Context, name string) (*RuntimeRef, error) {
		return fetchClusterRuntime(ctx, c, name)
	}
}

// NamespacedRuntimeFetcher looks up the parents of a ServingRuntime in
// namespace through c. The ServingRuntime of the parent's name in that
// namespace is the parent when it exists; otherwise the
// ClusterServingRuntime of that name is. A runtime in another namespace is
// never a parent, so it reads as not found.
func NamespacedRuntimeFetcher(c client.Reader, namespace string) Fetcher {
	return func(ctx context.Context, name string) (*RuntimeRef, error) {
		ref, err := fetchNamespacedRuntime(ctx, c, namespace, name)
		if errors.Is(err, ErrParentNotFound) {
			return fetchClusterRuntime(ctx, c, name)
		}
		return ref, err
	}
}

// fetchNamespacedRuntime reads one ServingRuntime. A missing runtime reads
// as ErrParentNotFound; any other read error passes through as it is.
func fetchNamespacedRuntime(ctx context.Context, c client.Reader, namespace, name string) (*RuntimeRef, error) {
	sr := &v1beta1.ServingRuntime{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, sr); err != nil {
		return nil, missingAsParentNotFound(err)
	}
	return NamespacedRuntimeRef(sr), nil
}

// fetchClusterRuntime reads one ClusterServingRuntime with the same
// not-found rule.
func fetchClusterRuntime(ctx context.Context, c client.Reader, name string) (*RuntimeRef, error) {
	csr := &v1beta1.ClusterServingRuntime{}
	if err := c.Get(ctx, types.NamespacedName{Name: name}, csr); err != nil {
		return nil, missingAsParentNotFound(err)
	}
	return ClusterRuntimeRef(csr), nil
}

// missingAsParentNotFound turns a not-found read error into one that
// errors.Is matches as ErrParentNotFound; any other error passes through
// as it is.
func missingAsParentNotFound(err error) error {
	if apierrors.IsNotFound(err) {
		return errors.Join(ErrParentNotFound, err)
	}
	return err
}
