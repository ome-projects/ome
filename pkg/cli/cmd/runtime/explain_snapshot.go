package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
)

var (
	errRuntimeSnapshotTruncated = errors.New(
		"runtime candidate snapshot exceeds the CLI collection limit",
	)
	errRuntimeSnapshotIdentity = errors.New(
		"runtime API returned a candidate with an unexpected identity",
	)
	errRuntimeSnapshotDuplicate = errors.New(
		"runtime API returned a duplicate candidate identity",
	)
	errRuntimeSnapshotInconsistent = errors.New(
		"runtime API returned an inconsistent candidate snapshot",
	)
)

type runtimeSnapshotRequestError struct{ cause error }

func (*runtimeSnapshotRequestError) Error() string {
	return "runtime candidate snapshot API request failed"
}

func (e *runtimeSnapshotRequestError) GoString() string { return e.Error() }

func (e *runtimeSnapshotRequestError) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte(e.Error()))
}

func (e *runtimeSnapshotRequestError) Unwrap() error { return e.cause }

func defaultRuntimeExplainLimits() paging.Limits {
	return paging.Limits{
		PageSize:       paging.ChunkSize,
		MaxItems:       1000,
		MaxPages:       4,
		RequestTimeout: 10 * time.Second,
	}
}

type runtimeCandidateSnapshot struct {
	client     *runtimeSnapshotClient
	candidates []runtimeCandidate
}

type runtimeSnapshotBudget struct {
	items int
	pages int
	base  paging.Limits
}

func newRuntimeSnapshotBudget(limits paging.Limits) (*runtimeSnapshotBudget, error) {
	switch {
	case limits.PageSize <= 0:
		return nil, errors.New("runtime candidate page size must be positive")
	case limits.MaxItems <= 0:
		return nil, errors.New("runtime candidate item limit must be positive")
	case limits.MaxPages <= 0:
		return nil, errors.New("runtime candidate page limit must be positive")
	case limits.RequestTimeout <= 0:
		return nil, errors.New("runtime candidate request timeout must be positive")
	default:
		return &runtimeSnapshotBudget{
			items: limits.MaxItems, pages: limits.MaxPages, base: limits,
		}, nil
	}
}

func (b *runtimeSnapshotBudget) limits() (paging.Limits, error) {
	if b.pages <= 0 {
		return paging.Limits{}, errRuntimeSnapshotTruncated
	}
	limits := b.base
	limits.MaxItems = b.items
	if limits.MaxItems == 0 {
		// One bounded probe is required to prove that the second runtime kind
		// is empty when the first kind consumed the exact shared item budget.
		limits.MaxItems = 1
	}
	limits.MaxPages = b.pages
	return limits, nil
}

func (b *runtimeSnapshotBudget) consume(pages, items int) {
	b.pages -= pages
	b.items -= items
}

// collectRuntimeCandidateSnapshot captures the complete namespace-scoped and
// cluster-scoped candidate set once. Every later selector LIST and inheritance
// GET is served by an immutable, deep-copying client backed by this capture.
func collectRuntimeCandidateSnapshot(
	ctx context.Context,
	live ctrlclient.Client,
	namespace string,
	limits paging.Limits,
) (*runtimeCandidateSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if live == nil {
		return nil, errors.New("runtime candidate client must not be nil")
	}
	if namespace == "" {
		return nil, errors.New("runtime candidate namespace must not be empty")
	}
	budget, err := newRuntimeSnapshotBudget(limits)
	if err != nil {
		return nil, err
	}

	namespaced, resourceVersion, err := collectNamespacedRuntimeCandidates(
		ctx, live, namespace, budget,
	)
	if err != nil {
		return nil, err
	}
	cluster, err := collectClusterRuntimeCandidates(
		ctx, live, resourceVersion, budget,
	)
	if err != nil {
		return nil, err
	}

	snapshotClient, candidates, err := newRuntimeSnapshotClient(
		live, namespace, namespaced, cluster,
	)
	if err != nil {
		return nil, err
	}
	return &runtimeCandidateSnapshot{
		client: snapshotClient, candidates: candidates,
	}, nil
}

func collectNamespacedRuntimeCandidates(
	ctx context.Context,
	live ctrlclient.Client,
	namespace string,
	budget *runtimeSnapshotBudget,
) ([]v1beta1.ServingRuntime, string, error) {
	limits, err := budget.limits()
	if err != nil {
		return nil, "", err
	}
	resourceVersion := ""
	metadataObserved := false
	result, err := paging.ListBounded(
		ctx, metav1.ListOptions{}, limits,
		func(requestCtx context.Context, options metav1.ListOptions) (
			paging.Page[v1beta1.ServingRuntime], error,
		) {
			response := &v1beta1.ServingRuntimeList{}
			request := &ctrlclient.ListOptions{
				Namespace: namespace, Limit: options.Limit, Continue: options.Continue,
			}
			listErr := live.List(requestCtx, response, request)
			if requestErr := requestCtx.Err(); requestErr != nil {
				return paging.Page[v1beta1.ServingRuntime]{}, requestErr
			}
			if listErr != nil {
				return paging.Page[v1beta1.ServingRuntime]{}, &runtimeSnapshotRequestError{cause: listErr}
			}
			if err := validateRuntimeListIdentity(
				response.TypeMeta,
				v1beta1.SchemeGroupVersion.WithKind("ServingRuntimeList"),
			); err != nil {
				return paging.Page[v1beta1.ServingRuntime]{}, err
			}
			if response.ResourceVersion == "" {
				return paging.Page[v1beta1.ServingRuntime]{}, errRuntimeSnapshotInconsistent
			}
			if int64(len(response.Items)) > options.Limit {
				return paging.Page[v1beta1.ServingRuntime]{}, errRuntimeSnapshotInconsistent
			}
			if metadataObserved && response.ResourceVersion != resourceVersion {
				return paging.Page[v1beta1.ServingRuntime]{}, errRuntimeSnapshotInconsistent
			}
			resourceVersion = response.ResourceVersion
			metadataObserved = true
			return paging.Page[v1beta1.ServingRuntime]{
				Items: response.Items, Continue: response.Continue,
			}, nil
		},
	)
	remainingItems := budget.items
	budget.consume(result.Pages, len(result.Items))
	if err != nil {
		return nil, "", preserveRuntimeSnapshotContext(ctx, err)
	}
	if result.Truncated || len(result.Items) > remainingItems {
		return nil, "", errRuntimeSnapshotTruncated
	}
	return copyServingRuntimeSnapshot(result.Items), resourceVersion, nil
}

func collectClusterRuntimeCandidates(
	ctx context.Context,
	live ctrlclient.Client,
	snapshotResourceVersion string,
	budget *runtimeSnapshotBudget,
) ([]v1beta1.ClusterServingRuntime, error) {
	if snapshotResourceVersion == "" {
		return nil, errRuntimeSnapshotInconsistent
	}
	limits, err := budget.limits()
	if err != nil {
		return nil, err
	}
	result, err := paging.ListBounded(
		ctx, metav1.ListOptions{}, limits,
		func(requestCtx context.Context, options metav1.ListOptions) (
			paging.Page[v1beta1.ClusterServingRuntime], error,
		) {
			response := &v1beta1.ClusterServingRuntimeList{}
			request := &ctrlclient.ListOptions{
				Limit: options.Limit, Continue: options.Continue,
			}
			if options.Continue == "" {
				// Both kinds are CRDs backed by kube-apiserver, so an Exact
				// first-page read at the ServingRuntime RV makes one coherent
				// cross-kind snapshot. Continuation requests must omit the RV.
				request.Raw = &metav1.ListOptions{
					ResourceVersion:      snapshotResourceVersion,
					ResourceVersionMatch: metav1.ResourceVersionMatchExact,
				}
			}
			listErr := live.List(requestCtx, response, request)
			if requestErr := requestCtx.Err(); requestErr != nil {
				return paging.Page[v1beta1.ClusterServingRuntime]{}, requestErr
			}
			if listErr != nil {
				return paging.Page[v1beta1.ClusterServingRuntime]{}, &runtimeSnapshotRequestError{cause: listErr}
			}
			if err := validateRuntimeListIdentity(
				response.TypeMeta,
				v1beta1.SchemeGroupVersion.WithKind("ClusterServingRuntimeList"),
			); err != nil {
				return paging.Page[v1beta1.ClusterServingRuntime]{}, err
			}
			if response.ResourceVersion == "" {
				return paging.Page[v1beta1.ClusterServingRuntime]{}, errRuntimeSnapshotInconsistent
			}
			if int64(len(response.Items)) > options.Limit {
				return paging.Page[v1beta1.ClusterServingRuntime]{}, errRuntimeSnapshotInconsistent
			}
			if response.ResourceVersion != snapshotResourceVersion {
				return paging.Page[v1beta1.ClusterServingRuntime]{}, errRuntimeSnapshotInconsistent
			}
			return paging.Page[v1beta1.ClusterServingRuntime]{
				Items: response.Items, Continue: response.Continue,
			}, nil
		},
	)
	remainingItems := budget.items
	budget.consume(result.Pages, len(result.Items))
	if err != nil {
		return nil, preserveRuntimeSnapshotContext(ctx, err)
	}
	if result.Truncated || len(result.Items) > remainingItems {
		return nil, errRuntimeSnapshotTruncated
	}
	return copyClusterServingRuntimeSnapshot(result.Items), nil
}

func validateRuntimeListIdentity(
	typeMeta metav1.TypeMeta,
	want schema.GroupVersionKind,
) error {
	got := schema.FromAPIVersionAndKind(typeMeta.APIVersion, typeMeta.Kind)
	if got.Empty() {
		return nil
	}
	if got != want {
		return errRuntimeSnapshotIdentity
	}
	return nil
}

func preserveRuntimeSnapshotContext(ctx context.Context, err error) error {
	if contextError := ctx.Err(); contextError != nil {
		return contextError
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return err
}

func copyServingRuntimeSnapshot(items []v1beta1.ServingRuntime) []v1beta1.ServingRuntime {
	result := make([]v1beta1.ServingRuntime, len(items))
	for i := range items {
		result[i] = *items[i].DeepCopy()
	}
	return result
}

func copyClusterServingRuntimeSnapshot(
	items []v1beta1.ClusterServingRuntime,
) []v1beta1.ClusterServingRuntime {
	result := make([]v1beta1.ClusterServingRuntime, len(items))
	for i := range items {
		result[i] = *items[i].DeepCopy()
	}
	return result
}

type runtimeSnapshotClient struct {
	ctrlclient.Client
	namespace  string
	namespaced map[string]*v1beta1.ServingRuntime
	cluster    map[string]*v1beta1.ClusterServingRuntime
	serving    []v1beta1.ServingRuntime
	clusters   []v1beta1.ClusterServingRuntime
}

func newRuntimeSnapshotClient(
	base ctrlclient.Client,
	namespace string,
	namespaced []v1beta1.ServingRuntime,
	cluster []v1beta1.ClusterServingRuntime,
) (*runtimeSnapshotClient, []runtimeCandidate, error) {
	client := &runtimeSnapshotClient{
		Client: base, namespace: namespace,
		namespaced: make(map[string]*v1beta1.ServingRuntime, len(namespaced)),
		cluster:    make(map[string]*v1beta1.ClusterServingRuntime, len(cluster)),
		// Collection already deep-copied these private slices. The snapshot
		// takes ownership and only exposes further deep copies.
		serving:  namespaced,
		clusters: cluster,
	}
	candidates := make([]runtimeCandidate, 0, len(namespaced)+len(cluster))
	for i := range client.serving {
		item := &client.serving[i]
		if len(validation.IsDNS1123Subdomain(item.Name)) > 0 || item.Namespace != namespace {
			return nil, nil, errRuntimeSnapshotIdentity
		}
		if _, duplicate := client.namespaced[item.Name]; duplicate {
			return nil, nil, errRuntimeSnapshotDuplicate
		}
		client.namespaced[item.Name] = item
		candidates = append(candidates, runtimeCandidate{name: item.Name})
	}
	for i := range client.clusters {
		item := &client.clusters[i]
		if len(validation.IsDNS1123Subdomain(item.Name)) > 0 || item.Namespace != "" {
			return nil, nil, errRuntimeSnapshotIdentity
		}
		if _, duplicate := client.cluster[item.Name]; duplicate {
			return nil, nil, errRuntimeSnapshotDuplicate
		}
		client.cluster[item.Name] = item
		candidates = append(candidates, runtimeCandidate{name: item.Name, isCluster: true})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].isCluster != candidates[j].isCluster {
			return !candidates[i].isCluster
		}
		return candidates[i].name < candidates[j].name
	})
	return client, candidates, nil
}

func (c *runtimeSnapshotClient) Get(
	ctx context.Context,
	key ctrlclient.ObjectKey,
	object ctrlclient.Object,
	_ ...ctrlclient.GetOption,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch target := object.(type) {
	case *v1beta1.ServingRuntime:
		if key.Namespace != c.namespace {
			return apierrors.NewNotFound(
				schema.GroupResource{Group: "ome.io", Resource: "servingruntimes"},
				key.Name,
			)
		}
		item, found := c.namespaced[key.Name]
		if !found {
			return apierrors.NewNotFound(
				schema.GroupResource{Group: "ome.io", Resource: "servingruntimes"},
				key.Name,
			)
		}
		item.DeepCopyInto(target)
		return nil
	case *v1beta1.ClusterServingRuntime:
		if key.Namespace != "" {
			return apierrors.NewNotFound(
				schema.GroupResource{Group: "ome.io", Resource: "clusterservingruntimes"},
				key.Name,
			)
		}
		item, found := c.cluster[key.Name]
		if !found {
			return apierrors.NewNotFound(
				schema.GroupResource{Group: "ome.io", Resource: "clusterservingruntimes"},
				key.Name,
			)
		}
		item.DeepCopyInto(target)
		return nil
	default:
		return errors.New("runtime snapshot does not support this GET")
	}
}

func (c *runtimeSnapshotClient) List(
	ctx context.Context,
	list ctrlclient.ObjectList,
	opts ...ctrlclient.ListOption,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	options := (&ctrlclient.ListOptions{}).ApplyOptions(opts)
	if !runtimeSnapshotListOptionsAllowed(options) {
		return errors.New("runtime snapshot received unexpected LIST options")
	}
	switch target := list.(type) {
	case *v1beta1.ServingRuntimeList:
		if options.Namespace != c.namespace {
			return errors.New("runtime snapshot received an unexpected namespace")
		}
		target.Items = copyServingRuntimeSnapshot(c.serving)
		target.ListMeta = metav1.ListMeta{}
		return nil
	case *v1beta1.ClusterServingRuntimeList:
		if options.Namespace != "" {
			return errors.New("runtime snapshot received an unexpected namespace")
		}
		target.Items = copyClusterServingRuntimeSnapshot(c.clusters)
		target.ListMeta = metav1.ListMeta{}
		return nil
	default:
		return errors.New("runtime snapshot does not support this LIST")
	}
}

func runtimeSnapshotListOptionsAllowed(options *ctrlclient.ListOptions) bool {
	if options == nil {
		return true
	}
	if options.Limit != 0 || options.Continue != "" ||
		options.LabelSelector != nil || options.FieldSelector != nil ||
		options.UnsafeDisableDeepCopy != nil || options.Raw != nil {
		return false
	}
	return true
}

func (s *runtimeCandidateSnapshot) rawSpec(
	candidate runtimeCandidate,
) *v1beta1.ServingRuntimeSpec {
	if s == nil || s.client == nil {
		return nil
	}
	if candidate.isCluster {
		item := s.client.cluster[candidate.name]
		if item == nil {
			return nil
		}
		return item.Spec.DeepCopy()
	}
	item := s.client.namespaced[candidate.name]
	if item == nil {
		return nil
	}
	return item.Spec.DeepCopy()
}

func (s *runtimeCandidateSnapshot) validate() error {
	if s == nil || s.client == nil {
		return errRuntimeSnapshotInconsistent
	}
	for _, candidate := range s.candidates {
		if s.rawSpec(candidate) == nil {
			return errRuntimeSnapshotInconsistent
		}
	}
	return nil
}
