// The cross-cluster status watch-funnel below is adapted from
// kubernetes-sigs/kueue (pkg/controller/admissionchecks/multikueue/
// multikueuecluster.go startWatcher/queueWorkloadEvent and workload.go
// source.Channel), Apache-2.0: a per-remote-cluster watch whose events are
// resolved to a LOCAL object key and pushed onto a buffered channel the
// local controller consumes via source.Channel, so cross-cluster status
// converges on events rather than a fixed poll.

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
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// DefaultFunnelResyncInterval is the fallback cadence at which the StatusFunnel
// reconciles its per-cluster watch set against the Manager's connected clusters
// (establishing a watch for a newly-connected cluster, dropping one for a
// departed cluster). It is NOT the status-convergence latency — events flow as
// they happen — only how often newly-connected clusters are noticed. The
// operative value is config-driven (functional option); this is the
// graceful-degradation default.
const DefaultFunnelResyncInterval = 10 * time.Second

// DefaultFunnelBufferSize is the fallback depth of the buffered event channel
// the funnel pushes resolved local keys onto. Small and bounded on purpose: a
// burst of remote status churn must not let the funnel accumulate unbounded
// memory, and a dropped event is harmless because the long safety-requeue on
// the consumer re-reconciles regardless. The operative value is config-driven;
// this is the graceful-degradation default.
const DefaultFunnelBufferSize = 10

// LocalKeyResolver maps a remote object delivered by a workload-cluster informer
// to the LOCAL (control-plane) object key the funnel should re-reconcile, and
// reports whether the object is one this control plane owns. It is injected by
// the caller (the placement package, which owns the origin/control-plane label
// semantics) so this package does not import placement — mirroring how
// CacheOptions injects the cached kinds and origin selector. Returning ok=false
// drops the event (an object we did not derive, or one lacking the origin
// marker).
type LocalKeyResolver func(remote client.Object) (local types.NamespacedName, ok bool)

// FunnelConfig is the caller-supplied configuration for a StatusFunnel. The
// watched kind, the empty list/object constructors, the origin-scoped watch
// selector, and the remote-to-local key resolver are all placement concerns
// supplied here rather than hardcoded, so this package stays free of placement
// imports and of magic values.
type FunnelConfig struct {
	// MetadataOnly emits events from a direct metadata LIST/WATCH without caching
	// resource specs or status. NewList must return PartialObjectMetadataList.
	MetadataOnly bool

	// NewList returns an empty list of the watched kind, typed or metadata-only.
	NewList func() client.ObjectList
	// NewObject creates the local event stub. In cached mode it also identifies
	// the watched kind when registering the informer handler.
	NewObject func() client.Object
	// Resolve maps a remote object to a local key unless ResolveCluster is set.
	Resolve LocalKeyResolver
	// ResolveCluster emits a cluster-scoped event for shared dependencies whose
	// consumers must be resolved by the receiving controller, outside the informer.
	ResolveCluster func(cluster string, remote client.Object) (client.Object, bool)
	// WatchSelector scopes metadata LIST/WATCH requests. Cached informer
	// events are filtered by the resolver. Nil watches all objects.
	WatchSelector labels.Selector
	// ResyncInterval overrides how often the watch set is reconciled against the
	// connected clusters. Zero falls back to DefaultFunnelResyncInterval.
	ResyncInterval time.Duration
	// BufferSize overrides the event channel depth. Zero (or negative) falls back
	// to DefaultFunnelBufferSize.
	BufferSize int
}

// StatusFunnel turns scoped member events into local reconcile triggers.
// Cached kinds share informer watches; metadata-only kinds use a direct
// LIST/WATCH and retain no resource payloads. Both send bounded, nonblocking
// wakeups and follow member connection replacement.
type StatusFunnel struct {
	mgr *Manager
	cfg FunnelConfig

	events chan event.GenericEvent

	mu      sync.Mutex
	watched map[string]context.CancelFunc // cluster -> cancel for its watch goroutine
}

// NewStatusFunnel constructs a funnel over the given Manager. The buffered event
// channel is created up front so Events() is safe to wire into source.Channel
// before Start runs.
func NewStatusFunnel(mgr *Manager, cfg FunnelConfig) *StatusFunnel {
	size := cfg.BufferSize
	if size <= 0 {
		size = DefaultFunnelBufferSize
	}
	return &StatusFunnel{
		mgr:     mgr,
		cfg:     cfg,
		events:  make(chan event.GenericEvent, size),
		watched: map[string]context.CancelFunc{},
	}
}

// Events returns the receive end of the funnel's buffered channel. Wire it into
// the consuming controller with source.Channel; the funnel pushes a
// GenericEvent carrying a stub local object (name/namespace set) for each remote
// derived change it resolves to one of this control plane's sources.
func (f *StatusFunnel) Events() <-chan event.GenericEvent {
	return f.events
}

// resyncInterval returns the configured watch-set reconcile cadence, falling
// back to the package default.
func (f *StatusFunnel) resyncInterval() time.Duration {
	if f.cfg.ResyncInterval > 0 {
		return f.cfg.ResyncInterval
	}
	return DefaultFunnelResyncInterval
}

// Start runs the funnel until ctx is cancelled. It periodically reconciles the
// set of watched clusters against the Manager's connected set, then tears every
// watch down on shutdown. Implements manager.Runnable.
func (f *StatusFunnel) Start(ctx context.Context) error {
	// The funnel cannot operate without a resolver or kind constructors; rather
	// than push a nil-laden event, no-op gracefully (degrade to the consumer's
	// safety requeue) so a partially-wired manager still starts.
	if (f.cfg.Resolve == nil && f.cfg.ResolveCluster == nil) || f.cfg.NewList == nil || f.cfg.NewObject == nil {
		ctrl.LoggerFrom(ctx).Info("status funnel not fully configured; cross-cluster status will rely on the safety requeue")
		<-ctx.Done()
		return nil
	}

	ticker := time.NewTicker(f.resyncInterval())
	defer ticker.Stop()

	f.reconcileWatches(ctx)
	for {
		select {
		case <-ctx.Done():
			f.stopAll()
			return nil
		case <-ticker.C:
			f.reconcileWatches(ctx)
		}
	}
}

// reconcileWatches starts a watch goroutine for every newly-connected cluster
// and cancels the goroutine for every cluster that has since disconnected.
func (f *StatusFunnel) reconcileWatches(ctx context.Context) {
	connected := make(map[string]struct{})
	for _, name := range f.mgr.Connected() {
		connected[name] = struct{}{}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	// Drop watches for clusters no longer connected.
	for name, cancel := range f.watched {
		if _, ok := connected[name]; !ok {
			cancel()
			delete(f.watched, name)
		}
	}
	// Start watches for clusters not yet watched.
	for name := range connected {
		if _, ok := f.watched[name]; ok {
			continue
		}
		wctx, cancel := context.WithCancel(ctx)
		f.watched[name] = cancel
		go f.watchCluster(wctx, name)
	}
}

// stopAll cancels every per-cluster watch goroutine. Called on Start shutdown.
func (f *StatusFunnel) stopAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for name, cancel := range f.watched {
		cancel()
		delete(f.watched, name)
	}
}

// watchCluster registers one handler for each member connection. The shared
// informer owns its network watch; connection polling only detects replacement.
func (f *StatusFunnel) watchCluster(ctx context.Context, cluster string) {
	if f.cfg.MetadataOnly {
		f.watchMetadataCluster(ctx, cluster)
		return
	}
	ticker := time.NewTicker(f.resyncInterval())
	defer ticker.Stop()
	var handlerGeneration uint64
	for ctx.Err() == nil {
		_, generation, connected := f.mgr.clientForGeneration(cluster)
		if connected && handlerGeneration != generation {
			if err := f.registerCacheHandler(ctx, cluster, generation); err != nil {
				ctrl.LoggerFrom(ctx).V(2).Info("member cache handler registration failed; retrying", "cluster", cluster, "error", err.Error())
			} else {
				handlerGeneration = generation
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// watchListOpts builds the list options for establishWatch: scope the stream to
// this control plane's derived objects via the origin selector when supplied.
func (f *StatusFunnel) watchListOpts() []client.ListOption {
	if f.cfg.WatchSelector == nil {
		return nil
	}
	return []client.ListOption{client.MatchingLabelsSelector{Selector: f.cfg.WatchSelector}}
}

// registerCacheHandler installs the resolve-and-push event handler on the
// cluster's derived-object cache informer. Add/Update/Delete all re-trigger the
// local source: a derived appearing, changing status, or vanishing on a workload
// cluster are each events the placer must observe.
func (f *StatusFunnel) registerCacheHandler(ctx context.Context, cluster string, generation uint64) error {
	cl, currentGeneration, ok := f.mgr.clientForGeneration(cluster)
	if !ok {
		return fmt.Errorf("cluster %q is not connected", cluster)
	}
	if currentGeneration != generation {
		return fmt.Errorf("cluster %q client changed from generation %d to %d", cluster, generation, currentGeneration)
	}
	h := toolscache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { f.pushFromCluster(ctx, cluster, obj) },
		UpdateFunc: func(_, newObj any) { f.pushFromCluster(ctx, cluster, newObj) },
		DeleteFunc: func(obj any) { f.pushFromCluster(ctx, cluster, deletedObject(obj)) },
	}
	_, err := cl.AddCacheEventHandler(ctx, f.cfg.NewObject(), h)
	return err
}

func (f *StatusFunnel) pushFromCluster(ctx context.Context, cluster string, obj any) {
	if f.cfg.ResolveCluster == nil {
		f.push(ctx, obj)
		return
	}
	remote, ok := obj.(client.Object)
	if !ok || remote == nil {
		return
	}
	object, ok := f.cfg.ResolveCluster(cluster, remote)
	if !ok || object == nil {
		return
	}
	select {
	case f.events <- event.GenericEvent{Object: object}:
	default:
		ctrl.LoggerFrom(ctx).V(4).Info("member dependency event channel full; safety requeue will recover", "cluster", cluster)
	}
}

// push resolves a remote object to its local key and, if it is one of this
// control plane's deriveds, enqueues a GenericEvent carrying a stub local object
// (name/namespace only — the consumer re-reads the live object). The send is
// non-blocking: when the buffered channel is full the event is DROPPED rather
// than blocking the informer's handler goroutine. A dropped event only delays
// convergence to the consumer's long safety requeue; blocking the shared
// informer callback would stall every other handler on that cache.
func (f *StatusFunnel) push(ctx context.Context, obj any) {
	co, ok := obj.(client.Object)
	if !ok || co == nil {
		return
	}
	key, ok := f.cfg.Resolve(co)
	if !ok {
		return
	}
	stub := f.cfg.NewObject()
	stub.SetNamespace(key.Namespace)
	stub.SetName(key.Name)
	select {
	case f.events <- event.GenericEvent{Object: stub}:
	default:
		ctrl.LoggerFrom(ctx).V(4).Info("status funnel: event channel full, dropping (safety requeue will recover)",
			"namespace", key.Namespace, "name", key.Name)
	}
}

// deletedObject unwraps a cache tombstone (DeletedFinalStateUnknown) to the
// underlying object so a delete event resolves the same way an add/update does.
func deletedObject(obj any) any {
	if tombstone, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
		return tombstone.Obj
	}
	return obj
}
