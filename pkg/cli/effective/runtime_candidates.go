package effective

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

// RuntimeSelectionTruncated means automatic runtime selection could not
// observe every candidate within its safety limits. The error intentionally
// carries no object names, continuation tokens, or server response text.
type RuntimeSelectionTruncated struct{}

func (*RuntimeSelectionTruncated) Error() string {
	return "runtime selection candidates exceeded the CLI collection limit"
}

// ErrRuntimeObjectIdentityMismatch means a successful exact GET returned an
// object other than the requested key. Its text deliberately omits both
// identities so a hostile client cannot reflect object metadata to stderr.
var ErrRuntimeObjectIdentityMismatch = errors.New("runtime API returned an object with an unexpected identity")

// ErrRuntimeSnapshotChanged means two reads in one resolution returned
// different snapshots for the same runtime key. Its text deliberately omits
// resource metadata and runtime content.
var ErrRuntimeSnapshotChanged = errors.New("runtime API snapshot changed during collection")

// ErrRuntimeSnapshotUnbindable means a runtime snapshot could not be reduced
// to the private fingerprint used to bind repeated reads.
var ErrRuntimeSnapshotUnbindable = errors.New("runtime API snapshot could not be safely bound")

type runtimeSnapshotKey struct {
	kind      string
	namespace string
	name      string
}

type runtimeObjectSnapshot struct {
	key             runtimeSnapshotKey
	uid             string
	generation      int64
	resourceVersion string
	fingerprint     [sha256.Size]byte
}

func (s runtimeObjectSnapshot) identityObserved() bool {
	return s.uid != "" && s.resourceVersion != ""
}

type runtimeSnapshotProvider interface {
	runtimeSnapshot(kind, namespace, name string) (runtimeObjectSnapshot, bool)
}

type runtimeSnapshotResetter interface {
	resetRuntimeSnapshots()
}

// runtimeSnapshotClient binds every runtime object returned by LIST or GET to
// the first snapshot observed for its exact key. The fingerprint is private
// and includes only the runtime spec and declared-inheritance edge; arbitrary
// metadata never enters an error or report.
type runtimeSnapshotClient struct {
	ctrlclient.Client
	mu        sync.Mutex
	snapshots map[runtimeSnapshotKey]runtimeObjectSnapshot
}

func newRuntimeSnapshotClient(client ctrlclient.Client) *runtimeSnapshotClient {
	return &runtimeSnapshotClient{
		Client: client, snapshots: map[runtimeSnapshotKey]runtimeObjectSnapshot{},
	}
}

func (c *runtimeSnapshotClient) Get(
	ctx context.Context,
	key ctrlclient.ObjectKey,
	object ctrlclient.Object,
	opts ...ctrlclient.GetOption,
) error {
	if object == nil {
		return errors.New("runtime GET target must not be nil")
	}
	if err := c.Client.Get(ctx, key, object, opts...); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if object.GetName() != key.Name || object.GetNamespace() != key.Namespace {
		return ErrRuntimeObjectIdentityMismatch
	}
	return c.recordObject(object)
}

func (c *runtimeSnapshotClient) List(
	ctx context.Context,
	list ctrlclient.ObjectList,
	opts ...ctrlclient.ListOption,
) error {
	if err := c.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	options := (&ctrlclient.ListOptions{}).ApplyOptions(opts)
	namespace := options.Namespace
	switch typed := list.(type) {
	case *v1beta1.ServingRuntimeList:
		if typed == nil {
			return errors.New("ServingRuntime list must not be nil")
		}
		if options.Limit > 0 && int64(len(typed.Items)) > options.Limit {
			return &RuntimeSelectionTruncated{}
		}
		for i := range typed.Items {
			if typed.Items[i].Namespace != namespace {
				return ErrRuntimeObjectIdentityMismatch
			}
			if err := c.recordObject(&typed.Items[i]); err != nil {
				return err
			}
		}
	case *v1beta1.ClusterServingRuntimeList:
		if typed == nil {
			return errors.New("ClusterServingRuntime list must not be nil")
		}
		if options.Limit > 0 && int64(len(typed.Items)) > options.Limit {
			return &RuntimeSelectionTruncated{}
		}
		for i := range typed.Items {
			if typed.Items[i].Namespace != "" {
				return ErrRuntimeObjectIdentityMismatch
			}
			if err := c.recordObject(&typed.Items[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *runtimeSnapshotClient) recordObject(object ctrlclient.Object) error {
	snapshot, tracked, err := makeRuntimeObjectSnapshot(object)
	if err != nil || !tracked {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	previous, found := c.snapshots[snapshot.key]
	if found && previous != snapshot {
		return ErrRuntimeSnapshotChanged
	}
	if !found {
		c.snapshots[snapshot.key] = snapshot
	}
	return nil
}

func (c *runtimeSnapshotClient) runtimeSnapshot(kind, namespace, name string) (runtimeObjectSnapshot, bool) {
	if c == nil {
		return runtimeObjectSnapshot{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	snapshot, found := c.snapshots[runtimeSnapshotKey{kind: kind, namespace: namespace, name: name}]
	return snapshot, found
}

func (c *runtimeSnapshotClient) resetRuntimeSnapshots() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshots = map[runtimeSnapshotKey]runtimeObjectSnapshot{}
}

func makeRuntimeObjectSnapshot(object ctrlclient.Object) (runtimeObjectSnapshot, bool, error) {
	var kind string
	var spec *v1beta1.ServingRuntimeSpec
	switch typed := object.(type) {
	case *v1beta1.ServingRuntime:
		kind, spec = runtimeselector.KindServingRuntime, &typed.Spec
	case *v1beta1.ClusterServingRuntime:
		kind, spec = runtimeselector.KindClusterServingRuntime, &typed.Spec
	default:
		return runtimeObjectSnapshot{}, false, nil
	}
	payload, err := json.Marshal(struct {
		Spec        *v1beta1.ServingRuntimeSpec `json:"spec"`
		InheritFrom string                      `json:"inheritFrom,omitempty"`
	}{
		Spec: spec, InheritFrom: object.GetAnnotations()[constants.RuntimeInheritFromAnnotationKey],
	})
	if err != nil {
		return runtimeObjectSnapshot{}, true, ErrRuntimeSnapshotUnbindable
	}
	return runtimeObjectSnapshot{
		key: runtimeSnapshotKey{
			kind: kind, namespace: object.GetNamespace(), name: object.GetName(),
		},
		uid: string(object.GetUID()), generation: object.GetGeneration(),
		resourceVersion: object.GetResourceVersion(), fingerprint: sha256.Sum256(payload),
	}, true, nil
}

// NewBoundedRuntimeResolver creates a runtime resolver whose automatic
// selection candidate reads share one finite item and page budget. The
// resolver caches complete candidate lists because the operator selector can
// fetch the same collection a second time while constructing diagnostics.
func NewBoundedRuntimeResolver(client ctrlclient.Client, limits paging.Limits) (*RuntimeResolver, error) {
	if client == nil {
		return nil, errors.New("runtime client must not be nil")
	}
	if err := validateRuntimeCandidateLimits(limits); err != nil {
		return nil, err
	}

	tracked := newRuntimeSnapshotClient(client)
	bounded := &boundedRuntimeCandidateClient{
		Client:                     tracked,
		snapshots:                  tracked,
		limits:                     limits,
		remainingItems:             limits.MaxItems,
		remainingPages:             limits.MaxPages,
		servingRuntimeCache:        map[string]*v1beta1.ServingRuntimeList{},
		clusterServingRuntimeCache: map[string]*v1beta1.ClusterServingRuntimeList{},
	}
	return newRuntimeResolver(bounded, runtimeselector.New(bounded)), nil
}

func validateRuntimeCandidateLimits(limits paging.Limits) error {
	if limits.PageSize <= 0 {
		return errors.New("runtime selection page size must be positive")
	}
	if limits.MaxItems <= 0 {
		return errors.New("runtime selection item limit must be positive")
	}
	if limits.MaxPages <= 0 {
		return errors.New("runtime selection page limit must be positive")
	}
	if limits.RequestTimeout <= 0 {
		return errors.New("runtime selection request timeout must be positive")
	}
	return nil
}

type boundedRuntimeCandidateClient struct {
	ctrlclient.Client
	snapshots *runtimeSnapshotClient

	mu             sync.Mutex
	limits         paging.Limits
	remainingItems int
	remainingPages int

	servingRuntimeCache        map[string]*v1beta1.ServingRuntimeList
	clusterServingRuntimeCache map[string]*v1beta1.ClusterServingRuntimeList
}

func (c *boundedRuntimeCandidateClient) runtimeSnapshot(kind, namespace, name string) (runtimeObjectSnapshot, bool) {
	if c == nil || c.snapshots == nil {
		return runtimeObjectSnapshot{}, false
	}
	return c.snapshots.runtimeSnapshot(kind, namespace, name)
}

func (c *boundedRuntimeCandidateClient) resetRuntimeSnapshots() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.remainingItems = c.limits.MaxItems
	c.remainingPages = c.limits.MaxPages
	c.servingRuntimeCache = map[string]*v1beta1.ServingRuntimeList{}
	c.clusterServingRuntimeCache = map[string]*v1beta1.ClusterServingRuntimeList{}
	if c.snapshots != nil {
		c.snapshots.resetRuntimeSnapshots()
	}
	c.mu.Unlock()
}

func (c *boundedRuntimeCandidateClient) Get(
	ctx context.Context,
	key ctrlclient.ObjectKey,
	object ctrlclient.Object,
	opts ...ctrlclient.GetOption,
) error {
	if object == nil {
		return errors.New("runtime GET target must not be nil")
	}
	if err := c.Client.Get(ctx, key, object, opts...); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if object.GetName() != key.Name || object.GetNamespace() != key.Namespace {
		return ErrRuntimeObjectIdentityMismatch
	}
	return nil
}

func (c *boundedRuntimeCandidateClient) List(
	ctx context.Context,
	list ctrlclient.ObjectList,
	opts ...ctrlclient.ListOption,
) error {
	switch typed := list.(type) {
	case *v1beta1.ServingRuntimeList:
		if typed == nil {
			return errors.New("ServingRuntime list must not be nil")
		}
		return c.listServingRuntimes(ctx, typed, opts...)
	case *v1beta1.ClusterServingRuntimeList:
		if typed == nil {
			return errors.New("ClusterServingRuntime list must not be nil")
		}
		return c.listClusterServingRuntimes(ctx, typed, opts...)
	default:
		return c.Client.List(ctx, list, opts...)
	}
}

func (c *boundedRuntimeCandidateClient) listServingRuntimes(
	ctx context.Context,
	out *v1beta1.ServingRuntimeList,
	opts ...ctrlclient.ListOption,
) error {
	base, cacheKey, err := runtimeCandidateListOptions(opts)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if cached, found := c.servingRuntimeCache[cacheKey]; found {
		cached.DeepCopyInto(out)
		return nil
	}

	limits, err := c.remainingLimits()
	if err != nil {
		return err
	}
	var typeMeta metav1.TypeMeta
	var listMeta metav1.ListMeta
	metadataObserved := false
	result, err := paging.ListBounded(
		ctx,
		metav1.ListOptions{},
		limits,
		func(requestCtx context.Context, page metav1.ListOptions) (paging.Page[v1beta1.ServingRuntime], error) {
			response := &v1beta1.ServingRuntimeList{}
			request := runtimeCandidatePageOptions(base, page)
			listErr := c.Client.List(requestCtx, response, &request)
			if requestErr := requestCtx.Err(); requestErr != nil {
				return paging.Page[v1beta1.ServingRuntime]{}, requestErr
			}
			if listErr != nil {
				return paging.Page[v1beta1.ServingRuntime]{}, listErr
			}
			for i := range response.Items {
				if response.Items[i].Namespace != base.Namespace {
					return paging.Page[v1beta1.ServingRuntime]{}, ErrRuntimeObjectIdentityMismatch
				}
			}
			if !metadataObserved {
				typeMeta = response.TypeMeta
				listMeta = response.ListMeta
				metadataObserved = true
			}
			return paging.Page[v1beta1.ServingRuntime]{
				Items:    response.Items,
				Continue: response.Continue,
			}, nil
		},
	)
	c.consume(result.Pages, len(result.Items))
	if err != nil {
		return err
	}
	if result.Truncated {
		return &RuntimeSelectionTruncated{}
	}

	listMeta.Continue = ""
	listMeta.RemainingItemCount = nil
	complete := &v1beta1.ServingRuntimeList{
		TypeMeta: typeMeta,
		ListMeta: listMeta,
		Items:    result.Items,
	}
	complete.DeepCopyInto(out)
	c.servingRuntimeCache[cacheKey] = complete.DeepCopy()
	return nil
}

func (c *boundedRuntimeCandidateClient) listClusterServingRuntimes(
	ctx context.Context,
	out *v1beta1.ClusterServingRuntimeList,
	opts ...ctrlclient.ListOption,
) error {
	base, cacheKey, err := runtimeCandidateListOptions(opts)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if cached, found := c.clusterServingRuntimeCache[cacheKey]; found {
		cached.DeepCopyInto(out)
		return nil
	}

	limits, err := c.remainingLimits()
	if err != nil {
		return err
	}
	var typeMeta metav1.TypeMeta
	var listMeta metav1.ListMeta
	metadataObserved := false
	result, err := paging.ListBounded(
		ctx,
		metav1.ListOptions{},
		limits,
		func(requestCtx context.Context, page metav1.ListOptions) (paging.Page[v1beta1.ClusterServingRuntime], error) {
			response := &v1beta1.ClusterServingRuntimeList{}
			request := runtimeCandidatePageOptions(base, page)
			listErr := c.Client.List(requestCtx, response, &request)
			if requestErr := requestCtx.Err(); requestErr != nil {
				return paging.Page[v1beta1.ClusterServingRuntime]{}, requestErr
			}
			if listErr != nil {
				return paging.Page[v1beta1.ClusterServingRuntime]{}, listErr
			}
			for i := range response.Items {
				if response.Items[i].Namespace != "" {
					return paging.Page[v1beta1.ClusterServingRuntime]{}, ErrRuntimeObjectIdentityMismatch
				}
			}
			if !metadataObserved {
				typeMeta = response.TypeMeta
				listMeta = response.ListMeta
				metadataObserved = true
			}
			return paging.Page[v1beta1.ClusterServingRuntime]{
				Items:    response.Items,
				Continue: response.Continue,
			}, nil
		},
	)
	c.consume(result.Pages, len(result.Items))
	if err != nil {
		return err
	}
	if result.Truncated {
		return &RuntimeSelectionTruncated{}
	}

	listMeta.Continue = ""
	listMeta.RemainingItemCount = nil
	complete := &v1beta1.ClusterServingRuntimeList{
		TypeMeta: typeMeta,
		ListMeta: listMeta,
		Items:    result.Items,
	}
	complete.DeepCopyInto(out)
	c.clusterServingRuntimeCache[cacheKey] = complete.DeepCopy()
	return nil
}

func (c *boundedRuntimeCandidateClient) remainingLimits() (paging.Limits, error) {
	if c.remainingItems <= 0 || c.remainingPages <= 0 {
		return paging.Limits{}, &RuntimeSelectionTruncated{}
	}
	limits := c.limits
	limits.MaxItems = c.remainingItems
	limits.MaxPages = c.remainingPages
	return limits, nil
}

func (c *boundedRuntimeCandidateClient) consume(pages, items int) {
	c.remainingPages -= pages
	c.remainingItems -= items
	if c.remainingPages < 0 {
		c.remainingPages = 0
	}
	if c.remainingItems < 0 {
		c.remainingItems = 0
	}
}

type runtimeCandidateQuery struct {
	Namespace             string             `json:"namespace,omitempty"`
	Options               metav1.ListOptions `json:"options"`
	UnsafeDisableDeepCopy *bool              `json:"unsafeDisableDeepCopy,omitempty"`
}

func runtimeCandidateListOptions(opts []ctrlclient.ListOption) (ctrlclient.ListOptions, string, error) {
	applied := (&ctrlclient.ListOptions{}).ApplyOptions(opts)
	base := copyRuntimeCandidateListOptions(*applied)

	normalized := copyRuntimeCandidateListOptions(base)
	raw := normalized.AsListOptions()
	if raw.Watch {
		return ctrlclient.ListOptions{}, "", errors.New("runtime selection candidate LIST must not use watch semantics")
	}
	raw.Limit = 0
	raw.Continue = ""
	normalized.Limit = 0
	normalized.Continue = ""
	encoded, err := json.Marshal(runtimeCandidateQuery{
		Namespace:             normalized.Namespace,
		Options:               *raw,
		UnsafeDisableDeepCopy: normalized.UnsafeDisableDeepCopy,
	})
	if err != nil {
		return ctrlclient.ListOptions{}, "", fmt.Errorf("normalize runtime selection candidate query: %w", err)
	}
	return base, string(encoded), nil
}

func runtimeCandidatePageOptions(base ctrlclient.ListOptions, page metav1.ListOptions) ctrlclient.ListOptions {
	request := copyRuntimeCandidateListOptions(base)
	request.Limit = page.Limit
	request.Continue = page.Continue
	if request.Raw != nil {
		request.Raw.Limit = page.Limit
		request.Raw.Continue = page.Continue
	}
	return request
}

func copyRuntimeCandidateListOptions(options ctrlclient.ListOptions) ctrlclient.ListOptions {
	copied := options
	if options.Raw != nil {
		raw := *options.Raw
		copied.Raw = &raw
	}
	return copied
}
