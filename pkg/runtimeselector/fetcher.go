package runtimeselector

import (
	"context"
	"errors"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/runtimeinheritance"
)

// DefaultRuntimeFetcher implements RuntimeFetcher over a controller-runtime
// reader: the manager's cached client in a controller, the uncached API
// reader in a webhook.
type DefaultRuntimeFetcher struct {
	client client.Reader
}

func NewDefaultRuntimeFetcher(client client.Reader) RuntimeFetcher {
	return &DefaultRuntimeFetcher{
		client: client,
	}
}

func (f *DefaultRuntimeFetcher) FetchRuntimes(ctx context.Context, namespace string) (*RuntimeCollection, error) {
	logger := log.FromContext(ctx)

	runtimes := &v1beta1.ServingRuntimeList{}
	if err := f.client.List(ctx, runtimes, client.InNamespace(namespace)); err != nil {
		return nil, err
	}

	clusterRuntimes := &v1beta1.ClusterServingRuntimeList{}
	if err := f.client.List(ctx, clusterRuntimes); err != nil {
		return nil, err
	}

	sortServingRuntimeList(runtimes)
	sortClusterServingRuntimeList(clusterRuntimes)

	logger.V(1).Info("Fetched runtimes",
		"namespace", namespace,
		"namespaceRuntimes", len(runtimes.Items),
		"clusterRuntimes", len(clusterRuntimes.Items))

	return &RuntimeCollection{
		NamespaceRuntimes: runtimes.Items,
		ClusterRuntimes:   clusterRuntimes.Items,
	}, nil
}

// GetRuntime resolves a runtime to its merged (inheritance-resolved) spec.
// kind scopes the lookup:
//
//   - KindServingRuntime: namespace-scoped only. The API server never
//     defaults the ref kind to this value, so it is always an explicit
//     declaration — a missing namespaced runtime is a RuntimeNotFoundError
//     rather than a silent resolve of a same-named cluster runtime.
//   - KindClusterServingRuntime: cluster-scoped takes precedence, with a
//     namespace-scoped fallback. The fallback exists because the API server
//     defaults the ref kind to ClusterServingRuntime, so a bare-name
//     reference to a namespace-scoped runtime must keep resolving.
//   - empty: namespace-scoped takes precedence over cluster-scoped on name
//     collision.
func (f *DefaultRuntimeFetcher) GetRuntime(ctx context.Context, name string, namespace string, kind string) (*v1beta1.ServingRuntimeSpec, bool, error) {
	scopes := []scope{namespaceScope, clusterScope}
	switch kind {
	case KindClusterServingRuntime:
		scopes = []scope{clusterScope, namespaceScope}
	case KindServingRuntime:
		scopes = []scope{namespaceScope}
	}

	for _, s := range scopes {
		spec, found, err := f.resolveScope(ctx, name, namespace, s)
		if err != nil {
			return nil, false, err
		}
		if found {
			return spec, s == clusterScope, nil
		}
	}
	return nil, false, &RuntimeNotFoundError{
		RuntimeName: name,
		Namespace:   namespace,
		Kind:        kind,
	}
}

// scope is one of the two places a runtime may live.
type scope int

const (
	namespaceScope scope = iota
	clusterScope
)

// resolveScope resolves name in a single scope through the inheritance
// resolver, which reads the runtime once and merges its inherit-from chain.
// A runtime missing from the scope reads as ErrParentNotFound: found=false,
// nil error. A missing parent further up the chain is a *ParentNotFoundError,
// which does not match ErrParentNotFound, so it returns as the error.
func (f *DefaultRuntimeFetcher) resolveScope(ctx context.Context, name string, namespace string, s scope) (*v1beta1.ServingRuntimeSpec, bool, error) {
	var spec *v1beta1.ServingRuntimeSpec
	var err error
	if s == clusterScope {
		spec, _, err = runtimeinheritance.ResolveClusterRuntime(ctx, f.client, name)
	} else {
		spec, _, err = runtimeinheritance.ResolveNamespacedRuntime(ctx, f.client, namespace, name)
	}
	if errors.Is(err, runtimeinheritance.ErrParentNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return spec, true, nil
}

func sortServingRuntimeList(runtimes *v1beta1.ServingRuntimeList) {
	sortByCreationTimestampThenName(runtimes.Items, func(r *v1beta1.ServingRuntime) (metav1.Time, string) {
		return r.CreationTimestamp, r.Name
	})
}

func sortClusterServingRuntimeList(runtimes *v1beta1.ClusterServingRuntimeList) {
	sortByCreationTimestampThenName(runtimes.Items, func(r *v1beta1.ClusterServingRuntime) (metav1.Time, string) {
		return r.CreationTimestamp, r.Name
	})
}

// sortByCreationTimestampThenName sorts items in place: newest first, then
// alphabetical by name as a tiebreaker. Newest-first gives operators a way
// to roll out a replacement runtime by creating it side-by-side with the old
// one and letting auto-selection naturally prefer the fresh definition.
func sortByCreationTimestampThenName[T any](items []T, key func(*T) (metav1.Time, string)) {
	sort.Slice(items, func(i, j int) bool {
		ti, ni := key(&items[i])
		tj, nj := key(&items[j])
		if !ti.Equal(&tj) {
			return ti.After(tj.Time)
		}
		return ni < nj
	})
}
