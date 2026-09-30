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
	// cacheCtx is the context the background cache runs under. Disconnect
	// cancels it to tear the cache down, and once that has happened the cache
	// can never serve a read again — but controller-runtime does not surface
	// that as an error. Informers.Start leaves `started` true when it stops and
	// only sets `stopped`, so a kind whose informer does not exist yet is
	// created, silently NOT started (startInformerLocked returns early on
	// `stopped`), and still reported as started; Informers.Get then parks in
	// WaitForCacheSync on an informer that can never sync, bounded only by the
	// CALLER's context. For a reconcile that context is the manager's, i.e.
	// effectively forever, and under the single-worker default one such read
	// wedges the controller for the lifetime of the process. Every cached-path
	// read therefore consults this and falls back to the direct client rather
	// than handing the caller a hang.
	//
	// Nil means "always live", so hand-built clients in tests keep the
	// pre-existing behavior.
	cacheCtx    context.Context
	scheme      *runtime.Scheme
	cachedKinds sets.Set[schema.GroupKind]
	synced      *atomic.Bool
}

// cacheStopped reports whether the background cache has been torn down.
func (c *selectivelyCachingClient) cacheStopped() bool {
	return c.cacheCtx != nil && c.cacheCtx.Err() != nil
}

// servesFromCache reports whether a read of gk should route to the cached
// reader: the kind must be configured for caching AND the cache must still be
// running. A stopped cache is skipped rather than consulted, because a read
// that reaches it does not fail — it blocks (see cacheCtx).
func (c *selectivelyCachingClient) servesFromCache(gk schema.GroupKind) bool {
	return c.cachedKinds.Has(gk) && !c.cacheStopped()
}

// cacheReadContext derives the context for a cached-path read: it ends when
// EITHER the caller's context ends or the cache is torn down. This closes the
// window where the cache is live at the servesFromCache check and cancelled a
// moment later, with the read already parked inside WaitForCacheSync. The
// returned release func must be called to drop the AfterFunc registration.
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

// cacheDiedDuringRead reports whether a failed cached read should be retried
// against the direct client: the cache went away mid-read while the caller's
// own context is still live, so the failure is a teardown artifact rather than
// a real read error the caller should have to interpret.
func (c *selectivelyCachingClient) cacheDiedDuringRead(ctx context.Context) bool {
	return c.cacheStopped() && ctx.Err() == nil
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
	if !c.servesFromCache(gvk.GroupKind()) {
		return c.WithWatch.Get(ctx, key, obj, opts...)
	}

	rctx, release := c.cacheReadContext(ctx)
	defer release()
	if !c.synced.Load() && !c.cache.WaitForCacheSync(rctx) {
		return c.WithWatch.Get(ctx, key, obj, opts...)
	}
	if err := c.cachedReader.Get(rctx, key, obj, opts...); err != nil {
		if c.cacheDiedDuringRead(ctx) {
			return c.WithWatch.Get(ctx, key, obj, opts...)
		}
		return err
	}
	return nil
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
	if !c.servesFromCache(gk) {
		return c.WithWatch.List(ctx, list, opts...)
	}

	rctx, release := c.cacheReadContext(ctx)
	defer release()
	if !c.synced.Load() && !c.cache.WaitForCacheSync(rctx) {
		return c.WithWatch.List(ctx, list, opts...)
	}
	if err := c.cachedReader.List(rctx, list, opts...); err != nil {
		if c.cacheDiedDuringRead(ctx) {
			return c.WithWatch.List(ctx, list, opts...)
		}
		return err
	}
	return nil
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
// (mirrors the manager cache's placement-origin scoping); pass nil to cache
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

	cacheOpts := cache.Options{
		Scheme: scheme,
		// Strip managedFields from every cached object (typically a large
		// fraction of each object's serialized size and never read here),
		// mirroring the manager cache in cmd/manager/main.go.
		DefaultTransform: cache.TransformStripManagedFields(),
	}
	if defaultSelector != nil {
		cacheOpts.DefaultLabelSelector = defaultSelector
	}

	remoteCache, err := cache.New(restConfig, cacheOpts)
	if err != nil {
		return nil, fmt.Errorf("creating remote cache: %w", err)
	}

	for _, opt := range indexes {
		err = remoteCache.IndexField(ctx, opt.Object, opt.Field, opt.ExtractValue)
		if err != nil {
			return nil, fmt.Errorf("registering index for %s on %s: %w", opt.Field, opt.Object.GetObjectKind().GroupVersionKind().Kind, err)
		}
	}

	var synced atomic.Bool

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
		synced.Store(true)
		ctrl.LoggerFrom(ctx).V(2).Info("Remote cache successfully synchronized")
	}

	cachedClient := &selectivelyCachingClient{
		WithWatch:    directClient,
		cachedReader: remoteCache,
		cache:        remoteCache,
		// ctx is the caller's cache context (Manager.buildRemoteClient derives a
		// cancellable one per connection and cancels it on Disconnect), which is
		// exactly the lifetime the cached read paths must respect.
		cacheCtx:    ctx,
		scheme:      scheme,
		cachedKinds: cachedKinds,
		synced:      &synced,
	}

	return cachedClient, nil
}
