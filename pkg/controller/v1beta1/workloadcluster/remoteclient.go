// This file is copied and adapted from kubernetes-sigs/kueue
// (pkg/controller/admissionchecks/multikueue/remote_client.go), Apache-2.0.
// Synced from kueue commit ab2dff32b (2026-06-20); re-diff against upstream
// before editing or extending.
// It provides a controller-runtime client that selectively caches chosen
// kinds via a background informer cache, used by the WorkloadCluster
// connection Manager.

/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package workloadcluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// cacheIndexOption defines a field to index on the cache before starting
type CacheIndexOption struct {
	Object       client.Object
	Field        string
	ExtractValue client.IndexerFunc
}

// SelectivelyCachingClient embeds standard WithWatch and adds direct event handler
// registration capabilities for cached informers.
type SelectivelyCachingClient interface {
	client.WithWatch
	// AddCacheEventHandler registers resource event handlers on the cache's underlying informer.
	// Returns an error if the object kind is not cached.
	AddCacheEventHandler(ctx context.Context, obj client.Object, handler toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error)
}

type neverCachingClient struct {
	client.WithWatch
}

func (c *neverCachingClient) AddCacheEventHandler(ctx context.Context, obj client.Object, handler toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	return nil, errors.New("NeverCachingClient does not support watch event handlers")
}

// NewNeverCachingClient just wraps around a client. Useful for unit tests.
func NewNeverCachingClient(fakeClient client.WithWatch) SelectivelyCachingClient {
	return &neverCachingClient{
		WithWatch: fakeClient,
	}
}

// DirectClientProvider explicitly exposes an uncached transport through a wrapper.
// Implementations must preserve live reads and the underlying connection identity.
type DirectClientProvider interface {
	DirectClient() client.WithWatch
}

// DirectClient exposes the transport without informer reads. Callers that use
// observations to release shared capacity must verify physical absence live.
func DirectClient(c SelectivelyCachingClient) (client.WithWatch, bool) {
	switch c := c.(type) {
	case DirectClientProvider:
		direct := c.DirectClient()
		return direct, direct != nil
	case *neverCachingClient:
		if c != nil {
			return c.WithWatch, c.WithWatch != nil
		}
	case *selectivelyCachingClient:
		if c != nil {
			return c.WithWatch, c.WithWatch != nil
		}
	}
	return nil, false
}

type selectivelyCachingClient struct {
	client.WithWatch
	cachedReader client.Reader
	cache        cache.Cache
	// cacheCtx bounds every read by the lifetime of its member connection.
	cacheCtx    context.Context
	scheme      *runtime.Scheme
	cachedKinds sets.Set[schema.GroupKind]
	synced      *atomic.Bool
	streams     *sync.Map // GroupKind -> *cacheStream
}

// cacheStopped reports whether the background cache has been torn down.
func (c *selectivelyCachingClient) cacheStopped() bool {
	return c.cacheCtx != nil && c.cacheCtx.Err() != nil
}

// cacheAvailable rejects observations from an unsynchronized or disconnected
// informer. It never converts an unavailable cache into a direct API read.
func (c *selectivelyCachingClient) cacheAvailable(gk schema.GroupKind) error {
	if c.cacheStopped() || c.synced == nil || !c.synced.Load() {
		return fmt.Errorf("%w: %s has not synchronized or is stopped", ErrMemberCacheUnavailable, gk)
	}
	if c.streams != nil {
		stream, exists := c.streams.Load(gk)
		if !exists || !stream.(*cacheStream).ready() {
			return fmt.Errorf("%w: %s watch is reconnecting", ErrMemberCacheUnavailable, gk)
		}
	}
	return nil
}

// cacheReadContext ends a read when its caller or member connection stops.
// The release function removes the cancellation callback after the read.
func (c *selectivelyCachingClient) cacheReadContext(ctx context.Context) (context.Context, func()) {
	if c.cacheCtx == nil {
		return ctx, func() {}
	}
	rctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.cacheCtx, cancel)
	return rctx, func() {
		stop()
		cancel()
	}
}

func (c *selectivelyCachingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	// Metadata-only reads use a distinct informer type from the structured
	// objects configured in cachedKinds, so they must remain direct reads.
	if _, metadataOnly := obj.(*metav1.PartialObjectMetadata); metadataOnly {
		return c.WithWatch.Get(ctx, key, obj, opts...)
	}

	gvk, err := apiutil.GVKForObject(obj, c.scheme)
	if err != nil {
		return err
	}
	if !c.cachedKinds.Has(gvk.GroupKind()) {
		return c.WithWatch.Get(ctx, key, obj, opts...)
	}

	if err := c.cacheAvailable(gvk.GroupKind()); err != nil {
		return err
	}
	rctx, release := c.cacheReadContext(ctx)
	defer release()
	err = c.cachedReader.Get(rctx, key, obj, opts...)
	if availabilityErr := c.cacheAvailable(gvk.GroupKind()); availabilityErr != nil {
		return availabilityErr
	}
	return err
}

func (c *selectivelyCachingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, metadataOnly := list.(*metav1.PartialObjectMetadataList); metadataOnly {
		return c.WithWatch.List(ctx, list, opts...)
	}

	gvk, err := apiutil.GVKForObject(list, c.scheme)
	if err != nil {
		return err
	}
	gk := gvk.GroupKind()
	gk.Kind = strings.TrimSuffix(gk.Kind, "List")
	if !c.cachedKinds.Has(gk) {
		return c.WithWatch.List(ctx, list, opts...)
	}

	if err := c.cacheAvailable(gk); err != nil {
		return err
	}
	rctx, release := c.cacheReadContext(ctx)
	defer release()
	err = c.cachedReader.List(rctx, list, opts...)
	if availabilityErr := c.cacheAvailable(gk); availabilityErr != nil {
		return availabilityErr
	}
	return err
}

func (c *selectivelyCachingClient) AddCacheEventHandler(ctx context.Context, obj client.Object, handler toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	gvk, err := apiutil.GVKForObject(obj, c.scheme)
	if err != nil {
		return nil, err
	}
	gk := gvk.GroupKind()
	if !c.cachedKinds.Has(gk) {
		return nil, fmt.Errorf("kind %s is not cached", gvk.String())
	}
	// GetInformer reaches the same lazily-created-informer path as a cached
	// read, so registering a handler on a torn-down cache would block instead
	// of failing. Report it: the caller's reconnect path can act on an error,
	// never on a hang.
	if c.cacheStopped() {
		return nil, fmt.Errorf("cannot add event handler for kind %s: remote cache is stopped", gvk.String())
	}
	rctx, release := c.cacheReadContext(ctx)
	defer release()
	inf, err := c.cache.GetInformer(rctx, obj)
	if err != nil {
		return nil, fmt.Errorf("getting informer for caching: %w", err)
	}
	return inf.AddEventHandler(handler)
}

// podGroupKind is the GroupKind the remote cache must never hold: across many
// large clusters the Pod informer alone would dwarf everything the placer reads
// (aggregated derived-ISVC status, not remote Pods).
var podGroupKind = corev1.SchemeGroupVersion.WithKind("Pod").GroupKind()

// NewSelectivelyCachingClient constructs the background cache, registers indexes,
// starts it in the background, and returns the client.
//
// defaultSelector scopes every cached kind to the caller-supplied label selector
// for callers that require filtered observations; pass nil to cache
// unfiltered. There is intentionally no in-code default selector — the value is
// the caller's to supply, per the repo's no-magic-values rule. Caching Pods is
// rejected outright: the remote cache must never LIST+WATCH cluster-wide Pods.
//
// syncTimeout bounds the initial informer LIST/sync: a cache that cannot start
// or sync fails the construction rather than leaving later reads and handler
// registration blocked on a cache that never becomes usable. It is the caller's
// to supply; non-positive bounds the wait by ctx alone.
func NewSelectivelyCachingClient(
	ctx context.Context,
	restConfig *rest.Config,
	directClient client.WithWatch,
	scheme *runtime.Scheme,
	cachedKinds sets.Set[schema.GroupKind],
	indexes []CacheIndexOption,
	defaultSelector labels.Selector,
	syncTimeout time.Duration,
) (SelectivelyCachingClient, error) {
	if cachedKinds.Has(podGroupKind) {
		return nil, fmt.Errorf("refusing to cache kind %s: the remote cache must never hold cluster-wide Pods", podGroupKind.String())
	}
	streams := &sync.Map{}
	remoteCache, err := newRemoteCache(restConfig, directClient, scheme, defaultSelector, syncTimeout, streams)
	if err != nil {
		return nil, err
	}
	if err := registerCacheIndexes(ctx, remoteCache, indexes); err != nil {
		return nil, err
	}
	if err := registerCachedKinds(ctx, remoteCache, scheme, cachedKinds); err != nil {
		return nil, err
	}
	runCtx, err := startRemoteCache(ctx, remoteCache, syncTimeout)
	if err != nil {
		return nil, err
	}
	var synced atomic.Bool
	synced.Store(true)
	return &selectivelyCachingClient{
		WithWatch:    directClient,
		cachedReader: remoteCache,
		cache:        remoteCache,
		// Cache exit and connection teardown both invalidate this reader.
		cacheCtx:    runCtx,
		scheme:      scheme,
		cachedKinds: cachedKinds,
		synced:      &synced,
		streams:     streams,
	}, nil
}

// newRemoteCache builds the informer cache behind the client. Every cached
// object loses its managedFields, as in the manager's own cache. The cache's
// own requests carry no deadline, since a short request deadline would
// expire a healthy watch over and over: LIST and watch establishment keep
// their own bounds, and discovery keeps the direct client's deadline through
// its shared REST mapper.
func newRemoteCache(
	restConfig *rest.Config,
	directClient client.WithWatch,
	scheme *runtime.Scheme,
	defaultSelector labels.Selector,
	syncTimeout time.Duration,
	streams *sync.Map,
) (cache.Cache, error) {
	cacheOpts := cache.Options{
		Scheme:                      scheme,
		ReaderFailOnMissingInformer: true,
		NewInformer:                 trackedInformerFactory(restConfig, scheme, syncTimeout, streams),
		DefaultTransform:            cache.TransformStripManagedFields(),
		Mapper:                      directClient.RESTMapper(),
	}
	if defaultSelector != nil {
		cacheOpts.DefaultLabelSelector = defaultSelector
	}
	cacheConfig := rest.CopyConfig(restConfig)
	cacheConfig.Timeout = 0
	remoteCache, err := cache.New(cacheConfig, cacheOpts)
	if err != nil {
		return nil, fmt.Errorf("creating remote cache: %w", err)
	}
	return remoteCache, nil
}

// trackedInformerFactory is the cache's informer constructor: each informer
// lists and watches through a trackedListWatch whose stream, kept by kind,
// tells the client whether that kind's watch is established.
func trackedInformerFactory(restConfig *rest.Config, scheme *runtime.Scheme, syncTimeout time.Duration, streams *sync.Map) func(toolscache.ListerWatcher, runtime.Object, time.Duration, toolscache.Indexers) toolscache.SharedIndexInformer {
	return func(lw toolscache.ListerWatcher, object runtime.Object, resync time.Duration, indexes toolscache.Indexers) toolscache.SharedIndexInformer {
		stream := &cacheStream{}
		if obj, ok := object.(client.Object); ok {
			if gvk, err := apiutil.GVKForObject(obj, scheme); err == nil {
				streams.Store(gvk.GroupKind(), stream)
			}
		}
		return toolscache.NewSharedIndexInformer(&trackedListWatch{
			ListerWatcher: lw, stream: stream, listTimeout: restConfig.Timeout, watchEstablishTimeout: syncTimeout,
		}, object, resync, indexes)
	}
}

// registerCacheIndexes adds the caller's field indexes before the cache starts.
func registerCacheIndexes(ctx context.Context, remoteCache cache.Cache, indexes []CacheIndexOption) error {
	for _, opt := range indexes {
		if err := remoteCache.IndexField(ctx, opt.Object, opt.Field, opt.ExtractValue); err != nil {
			return fmt.Errorf("registering index for %s on %s: %w", opt.Field, opt.Object.GetObjectKind().GroupVersionKind().Kind, err)
		}
	}
	return nil
}

// registerCachedKinds opens an informer for every cached kind up front, so
// the initial synchronization covers kinds with no index or event handler yet.
func registerCachedKinds(ctx context.Context, remoteCache cache.Cache, scheme *runtime.Scheme, cachedKinds sets.Set[schema.GroupKind]) error {
	for kind := range cachedKinds {
		object := objectOfKind(scheme, kind)
		if object == nil {
			return fmt.Errorf("cached kind %s is not registered in the scheme", kind)
		}
		if _, err := remoteCache.GetInformer(ctx, object, cache.BlockUntilSynced(false)); err != nil {
			return fmt.Errorf("registering informer for %s: %w", kind, err)
		}
	}
	return nil
}

// objectOfKind is an empty object of the kind in its preferred registered
// version; nil when the scheme knows no version of it.
func objectOfKind(scheme *runtime.Scheme, kind schema.GroupKind) client.Object {
	for _, version := range scheme.PrioritizedVersionsForGroup(kind.Group) {
		if candidate, err := scheme.New(kind.WithVersion(version.Version)); err == nil {
			object, _ := candidate.(client.Object)
			return object
		}
	}
	return nil
}

// startRemoteCache starts the cache and waits for its first synchronization,
// bounded by syncTimeout when positive and by ctx otherwise. It returns the
// context that ends when the cache stops; a cache that stops before it
// synchronizes, or fails to synchronize, is stopped and reported.
func startRemoteCache(ctx context.Context, remoteCache cache.Cache, syncTimeout time.Duration) (context.Context, error) {
	runCtx, cancelRun := context.WithCancel(ctx)
	startErr := make(chan error, 1)
	go func() {
		defer cancelRun()
		startErr <- remoteCache.Start(runCtx)
	}()
	syncCtx := runCtx
	if syncTimeout > 0 {
		var cancelSync context.CancelFunc
		syncCtx, cancelSync = context.WithTimeout(runCtx, syncTimeout)
		defer cancelSync()
	}
	syncDone := make(chan bool, 1)
	go func() { syncDone <- remoteCache.WaitForCacheSync(syncCtx) }()
	select {
	case err := <-startErr:
		if err == nil {
			err = errors.New("remote cache stopped before initial synchronization")
		}
		cancelRun()
		return nil, fmt.Errorf("starting remote cache: %w", err)
	case ok := <-syncDone:
		if !ok {
			cancelRun()
			if err := syncCtx.Err(); err != nil {
				return nil, fmt.Errorf("synchronizing remote cache: %w", err)
			}
			return nil, errors.New("synchronizing remote cache failed")
		}
	}
	ctrl.LoggerFrom(ctx).V(2).Info("Remote cache successfully synchronized")
	return runCtx, nil
}
