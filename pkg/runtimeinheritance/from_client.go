package runtimeinheritance

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// ResolveClusterRuntime walks the inherit-from chain rooted at the
// named ClusterServingRuntime and returns the merged spec.
func ResolveClusterRuntime(ctx context.Context, c client.Reader, name string) (*v1beta1.ServingRuntimeSpec, []string, error) {
	csr := &v1beta1.ClusterServingRuntime{}
	if err := c.Get(ctx, types.NamespacedName{Name: name}, csr); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, errors.Join(ErrParentNotFound, err)
		}
		return nil, nil, err
	}
	start := &RuntimeRef{
		Name:       csr.Name,
		Spec:       &csr.Spec,
		ParentName: csr.Annotations[constants.RuntimeInheritFromAnnotationKey],
	}
	return Resolve(ctx, start, clusterFetcher(c), constants.RuntimeInheritMaxDepth)
}

// ResolveNamespacedRuntime walks the inherit-from chain rooted at the
// named namespaced ServingRuntime. Parents are looked up in the same
// namespace first, falling back to cluster scope.
func ResolveNamespacedRuntime(ctx context.Context, c client.Reader, namespace, name string) (*v1beta1.ServingRuntimeSpec, []string, error) {
	sr := &v1beta1.ServingRuntime{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, sr); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, errors.Join(ErrParentNotFound, err)
		}
		return nil, nil, err
	}
	start := &RuntimeRef{
		Name:       sr.Name,
		Spec:       &sr.Spec,
		ParentName: sr.Annotations[constants.RuntimeInheritFromAnnotationKey],
	}
	return Resolve(ctx, start, namespacedFetcher(c, namespace), constants.RuntimeInheritMaxDepth)
}

func clusterFetcher(c client.Reader) Fetcher {
	return func(ctx context.Context, name string) (*RuntimeRef, error) {
		csr := &v1beta1.ClusterServingRuntime{}
		if err := c.Get(ctx, types.NamespacedName{Name: name}, csr); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, errors.Join(ErrParentNotFound, err)
			}
			return nil, err
		}
		return &RuntimeRef{
			Name:       csr.Name,
			Spec:       &csr.Spec,
			ParentName: csr.Annotations[constants.RuntimeInheritFromAnnotationKey],
		}, nil
	}
}

func namespacedFetcher(c client.Reader, namespace string) Fetcher {
	return func(ctx context.Context, name string) (*RuntimeRef, error) {
		sr := &v1beta1.ServingRuntime{}
		err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, sr)
		if err == nil {
			return &RuntimeRef{
				Name:       sr.Name,
				Spec:       &sr.Spec,
				ParentName: sr.Annotations[constants.RuntimeInheritFromAnnotationKey],
			}, nil
		}
		if !apierrors.IsNotFound(err) {
			return nil, err
		}
		csr := &v1beta1.ClusterServingRuntime{}
		if err := c.Get(ctx, types.NamespacedName{Name: name}, csr); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, errors.Join(ErrParentNotFound, err)
			}
			return nil, fmt.Errorf("fetch cluster parent %q: %w", name, err)
		}
		return &RuntimeRef{
			Name:       csr.Name,
			Spec:       &csr.Spec,
			ParentName: csr.Annotations[constants.RuntimeInheritFromAnnotationKey],
		}, nil
	}
}
