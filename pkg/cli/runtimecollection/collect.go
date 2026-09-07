// Package runtimecollection reads the bounded runtime snapshot used by the
// runtime inheritance graph.
package runtimecollection

import (
	"context"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/runtimegraph"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

// CollectionKind identifies the runtime list that failed during collection.
type CollectionKind string

const (
	CollectionClusterServingRuntime CollectionKind = "ClusterServingRuntime"
	CollectionServingRuntime        CollectionKind = "ServingRuntime"
)

// CollectionError preserves both the failed runtime kind and its original
// typed cause so callers can report partial collection evidence accurately.
type CollectionError struct {
	Kind  CollectionKind
	Cause error
}

func (e *CollectionError) Error() string {
	return fmt.Sprintf("list %s objects: %v", e.Kind, e.Cause)
}

func (e *CollectionError) Unwrap() error { return e.Cause }

// HasCollectionFailure reports whether err wraps a failure for kind.
func HasCollectionFailure(err error, kind CollectionKind) bool {
	var collectionErr *CollectionError
	return errors.As(err, &collectionErr) && collectionErr.Kind == kind
}

// KindCompleteness describes the bounded observations for one runtime kind.
type KindCompleteness struct {
	ObservedPages int
	ObservedItems int
	Truncated     bool
}

// Completeness keeps collection state separate for both required kinds.
type Completeness struct {
	ClusterServingRuntimes KindCompleteness
	ServingRuntimes        KindCompleteness
}

// Result is a runtime graph snapshot and its collection completeness.
type Result struct {
	Snapshot     runtimegraph.Snapshot
	Completeness Completeness
}

// ServingRuntimeResult contains one bounded namespaced-runtime collection.
// NamespaceAll may be used to collect cross-namespace descendants of a
// ClusterServingRuntime target.
type ServingRuntimeResult struct {
	ServingRuntimes []omev1beta1.ServingRuntime
	Completeness    KindCompleteness
}

// Collect reads all runtime namespaces through bounded Kubernetes pagination.
func Collect(ctx context.Context, client omeclient.OmeV1beta1Interface, limits paging.Limits) (Result, error) {
	return collect(ctx, client, metav1.NamespaceAll, limits)
}

// CollectInNamespace reads cluster runtimes and only the ServingRuntimes in
// namespace. It supports namespace-scoped callers without silently widening
// their RBAC requirements.
func CollectInNamespace(
	ctx context.Context,
	client omeclient.OmeV1beta1Interface,
	namespace string,
	limits paging.Limits,
) (Result, error) {
	return collect(ctx, client, namespace, limits)
}

func collect(
	ctx context.Context,
	client omeclient.OmeV1beta1Interface,
	servingRuntimeNamespace string,
	limits paging.Limits,
) (Result, error) {
	result := Result{Snapshot: runtimegraph.Snapshot{
		ClusterServingRuntimes: []omev1beta1.ClusterServingRuntime{},
		ServingRuntimes:        []omev1beta1.ServingRuntime{},
	}}
	clusterPages := 0
	clusters, err := paging.ListBounded(ctx, metav1.ListOptions{}, limits,
		func(requestCtx context.Context, options metav1.ListOptions) (paging.Page[omev1beta1.ClusterServingRuntime], error) {
			list, listErr := client.ClusterServingRuntimes().List(requestCtx, options)
			if requestErr := requestCtx.Err(); requestErr != nil {
				return paging.Page[omev1beta1.ClusterServingRuntime]{}, requestErr
			}
			if listErr != nil {
				return paging.Page[omev1beta1.ClusterServingRuntime]{}, listErr
			}
			if list == nil {
				return paging.Page[omev1beta1.ClusterServingRuntime]{}, errors.New("empty response")
			}
			clusterPages++
			return paging.Page[omev1beta1.ClusterServingRuntime]{Items: list.Items, Continue: list.Continue}, nil
		})
	result.Snapshot.ClusterServingRuntimes = copyClusterServingRuntimes(clusters.Items)
	result.Completeness.ClusterServingRuntimes = KindCompleteness{
		ObservedPages: clusterPages,
		ObservedItems: len(clusters.Items),
		Truncated:     clusters.Truncated,
	}
	if err != nil {
		return result, &CollectionError{
			Kind: CollectionClusterServingRuntime, Cause: err,
		}
	}

	servingRuntimes, err := CollectServingRuntimes(
		ctx, client, servingRuntimeNamespace, limits,
	)
	result.Snapshot.ServingRuntimes = servingRuntimes.ServingRuntimes
	result.Completeness.ServingRuntimes = servingRuntimes.Completeness
	if err != nil {
		return result, err
	}
	return result, nil
}

// CollectServingRuntimes reads ServingRuntimes in exactly namespace through
// bounded pagination. Use metav1.NamespaceAll only after a caller has decided
// it needs cross-namespace descendants.
func CollectServingRuntimes(
	ctx context.Context,
	client omeclient.OmeV1beta1Interface,
	namespace string,
	limits paging.Limits,
) (ServingRuntimeResult, error) {
	result := ServingRuntimeResult{ServingRuntimes: []omev1beta1.ServingRuntime{}}
	runtimePages := 0
	runtimes, err := paging.ListBounded(ctx, metav1.ListOptions{}, limits,
		func(requestCtx context.Context, options metav1.ListOptions) (paging.Page[omev1beta1.ServingRuntime], error) {
			list, listErr := client.ServingRuntimes(namespace).List(requestCtx, options)
			if requestErr := requestCtx.Err(); requestErr != nil {
				return paging.Page[omev1beta1.ServingRuntime]{}, requestErr
			}
			if listErr != nil {
				return paging.Page[omev1beta1.ServingRuntime]{}, listErr
			}
			if list == nil {
				return paging.Page[omev1beta1.ServingRuntime]{}, errors.New("empty response")
			}
			runtimePages++
			return paging.Page[omev1beta1.ServingRuntime]{Items: list.Items, Continue: list.Continue}, nil
		})
	result.ServingRuntimes = copyServingRuntimes(runtimes.Items)
	result.Completeness = KindCompleteness{
		ObservedPages: runtimePages,
		ObservedItems: len(runtimes.Items),
		Truncated:     runtimes.Truncated,
	}
	if err != nil {
		return result, &CollectionError{
			Kind: CollectionServingRuntime, Cause: err,
		}
	}
	return result, nil
}

func copyClusterServingRuntimes(items []omev1beta1.ClusterServingRuntime) []omev1beta1.ClusterServingRuntime {
	result := make([]omev1beta1.ClusterServingRuntime, len(items))
	for i := range items {
		result[i] = *items[i].DeepCopy()
	}
	return result
}

func copyServingRuntimes(items []omev1beta1.ServingRuntime) []omev1beta1.ServingRuntime {
	result := make([]omev1beta1.ServingRuntime, len(items))
	for i := range items {
		result[i] = *items[i].DeepCopy()
	}
	return result
}
