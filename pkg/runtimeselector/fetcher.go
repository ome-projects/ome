package runtimeselector

import (
	"context"
	"sort"

	"k8s.io/apimachinery/pkg/api/errors"
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
	clusterFirst := []bool{true, false}
	namespacedFirst := []bool{false, true}
	namespacedOnly := []bool{false}

	scopes := namespacedFirst
	switch kind {
	case KindClusterServingRuntime:
		scopes = clusterFirst
	case KindServingRuntime:
		scopes = namespacedOnly
	}

	for _, cluster := range scopes {
		spec, found, err := f.resolveScope(ctx, name, namespace, cluster)
		if err != nil {
			return nil, false, err
		}
		if found {
			return spec, cluster, nil
		}
	}
	return nil, false, &RuntimeNotFoundError{
		RuntimeName: name,
		Namespace:   namespace,
		Kind:        kind,
	}
}

// resolveScope resolves name in a single scope; found=false (with nil
// error) means the runtime does not exist in that scope.
func (f *DefaultRuntimeFetcher) resolveScope(ctx context.Context, name string, namespace string, cluster bool) (*v1beta1.ServingRuntimeSpec, bool, error) {
	if cluster {
		if _, err := f.getClusterHead(ctx, name); err != nil {
			if errors.IsNotFound(err) {
				return nil, false, nil
			}
			return nil, false, err
		}
		spec, _, err := runtimeinheritance.ResolveClusterRuntime(ctx, f.client, name)
		return spec, true, err
	}
	if _, err := f.getNamespacedHead(ctx, namespace, name); err != nil {
		if errors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	spec, _, err := runtimeinheritance.ResolveNamespacedRuntime(ctx, f.client, namespace, name)
	return spec, true, err
}

func (f *DefaultRuntimeFetcher) getNamespacedHead(ctx context.Context, namespace, name string) (*v1beta1.ServingRuntime, error) {
	sr := &v1beta1.ServingRuntime{}
	if err := f.client.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, sr); err != nil {
		return nil, err
	}
	return sr, nil
}

func (f *DefaultRuntimeFetcher) getClusterHead(ctx context.Context, name string) (*v1beta1.ClusterServingRuntime, error) {
	csr := &v1beta1.ClusterServingRuntime{}
	if err := f.client.Get(ctx, client.ObjectKey{Name: name}, csr); err != nil {
		return nil, err
	}
	return csr, nil
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
