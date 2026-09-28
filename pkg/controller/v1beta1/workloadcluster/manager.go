package workloadcluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Manager holds one live remote client per registered WorkloadCluster, keyed by
// cluster name. It is the cross-cluster transport: callers create
// and watch remote objects via ClientFor(name).
//
// Safe for concurrent use across distinct cluster names; Connect/Disconnect for
// the SAME name must be serialized by the caller (the WorkloadCluster reconciler
// is single-worker per key, so this holds in practice).
type Manager struct {
	scheme     *runtime.Scheme
	execPolicy ExecCredentialPolicy
	// tuning sets QPS/Burst/per-call-timeout on every remote rest.Config. The
	// zero value leaves client-go's own defaults in place (graceful degradation,
	// no magic literal baked in).
	tuning ClientTuning
	// cacheOpts selects which kinds the remote client caches and how the cache is
	// scoped. The zero value (empty CachedKinds) keeps the never-caching client.
	cacheOpts CacheOptions
	// reconnect is the establish/retry backoff schedule the transport uses when
	// (re)establishing a remote connection. The zero value falls back to
	// defaultReconnectBackoff via the ReconnectBackoff accessor.
	reconnect reconnectBackoff

	// baseCtx is the long-lived context remote clients derive from, so their
	// cache informers are scoped to the controller-manager lifetime rather than a
	// request-scoped reconcile ctx. Set by Start (the Runnable entrypoint);
	// nil-safe — Connect falls back to its caller's ctx.
	baseCtx context.Context

	mu             sync.RWMutex
	conns          map[string]*remoteConn
	nextGeneration uint64

	// newClient builds a caching-capable client (+ a cancel for its background
	// watch context) from raw kubeconfig bytes. Injected for tests; defaults to
	// buildRemoteClient.
	newClient func(ctx context.Context, raw []byte, scheme *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error)
}

// Start records the controller-manager's long-lived context as the base for all
// remote-client and cache-informer contexts, then blocks until that context is
// cancelled, at which point it disconnects every cluster so no background watch
// leaks past manager shutdown. It implements
// sigs.k8s.io/controller-runtime/pkg/manager.Runnable so the cmd wires it with
// mgr.Add(clusterManager).
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	m.baseCtx = ctx
	m.mu.Unlock()
	<-ctx.Done()
	m.DisconnectAll()
	return nil
}

// DisconnectAll disposes every connected client. Used on manager shutdown.
func (m *Manager) DisconnectAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, c := range m.conns {
		if c.cancel != nil {
			c.cancel()
		}
		delete(m.conns, name)
	}
}

type remoteConn struct {
	client     SelectivelyCachingClient
	cancel     context.CancelFunc
	configHash string
	generation uint64
	clusterUID types.UID
}

// NewManager returns an empty Manager whose default builder constructs a
// controller-runtime client from the validated kubeconfig. With no CacheOptions
// configured (the default) the client is never-caching; SetCacheOptions with a
// non-empty CachedKinds set switches it to a selectively-caching client.
func NewManager(scheme *runtime.Scheme) *Manager {
	m := &Manager{
		scheme: scheme,
		conns:  map[string]*remoteConn{},
	}
	m.newClient = m.buildRemoteClient
	return m
}

// SetExecCredentialPolicy configures the exec credential policy for this manager.
func (m *Manager) SetExecCredentialPolicy(p ExecCredentialPolicy) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.execPolicy = p
}

// SetClientTuning configures the QPS/Burst/per-call-timeout applied to every
// remote rest.Config built after this call. Call before the WorkloadCluster
// reconciler connects clusters (i.e. during wiring); already-connected clients
// keep their tuning until their kubeconfig changes and they are rebuilt.
func (m *Manager) SetClientTuning(t ClientTuning) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tuning = t
}

// SetCacheOptions configures the remote informer cache. A non-empty
// CachedKinds switches buildRemoteClient to a selectively-caching client whose
// cache is scoped by DefaultSelector and indexed by Indexes; an empty set keeps
// the never-caching client. As with tuning, this applies to clients built after
// the call, so set it during wiring.
func (m *Manager) SetCacheOptions(o CacheOptions) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cacheOpts = o
}

// SetReconnectBackoff records the establish/retry backoff schedule the transport
// uses when (re)establishing a remote connection. Wired from the reconciler's
// resolved config at Setup so the schedule is config-driven.
func (m *Manager) SetReconnectBackoff(b reconnectBackoff) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconnect = b
}

// ReconnectBackoff returns the configured establish/retry backoff, falling back
// to the package default when none was set. The watch-funnel reconnect path
// consumes this via EstablishWatch / retryAfter.
func (m *Manager) ReconnectBackoff() reconnectBackoff {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.reconnect.establishInitial <= 0 {
		return defaultReconnectBackoff()
	}
	return m.reconnect
}

// EstablishWatch opens a remote watch on the named cluster's client, bounded by
// the configured establish timeout for the given (1-based) attempt. On timeout
// the in-flight Watch is abandoned and errWatchEstablishTimeout is returned so
// the caller falls back to the retry backoff (retryAfter). Returns (nil, false,
// nil) when the cluster is not connected. This is the establish half of the
// two-backoff reconnect scheme; the status-mirroring watch funnel consumes it.
func (m *Manager) EstablishWatch(ctx context.Context, cluster string, list client.ObjectList, attempt int, opts ...client.ListOption) (watch.Interface, uint64, bool, error) {
	cl, generation, ok := m.clientForGeneration(cluster)
	if !ok {
		return nil, 0, false, nil
	}
	timeout := m.ReconnectBackoff().establishWaitTime(attempt)
	w, err := establishWatch(ctx, cl, list, timeout, opts...)
	if err != nil {
		return nil, generation, true, err
	}
	return w, generation, true, nil
}

// ClientFor returns the live client for a cluster, or (nil, false) if none is
// connected.
func (m *Manager) ClientFor(name string) (SelectivelyCachingClient, bool) {
	cl, _, ok := m.clientForGeneration(name)
	return cl, ok
}

// ClientForUID returns a client only when it belongs to the requested registry
// object. An unknown UID cannot establish cluster identity.
func (m *Manager) ClientForUID(name string, uid types.UID) (SelectivelyCachingClient, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.conns[name]
	if !ok || uid == "" || c.clusterUID != uid {
		return nil, false
	}
	return c.client, true
}

// clientForGeneration returns the current client together with an identity
// that changes whenever Connect replaces it. The funnel uses the generation to
// attach one informer handler to each remote cache instance.
func (m *Manager) clientForGeneration(name string) (SelectivelyCachingClient, uint64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.conns[name]
	if !ok {
		return nil, 0, false
	}
	return c.client, c.generation, true
}

// Connect ensures a live client for the named cluster built from raw. It is a
// no-op when the kubeconfig is unchanged; on change it builds a fresh client
// and disposes the old one (cancels its watch context).
func (m *Manager) Connect(ctx context.Context, name string, raw []byte) error {
	return m.connect(ctx, name, "", raw)
}

// ConnectFor binds a remote client to a WorkloadCluster UID. A recreated
// registration gets a fresh client and cache even when its credential is equal.
func (m *Manager) ConnectFor(ctx context.Context, name string, uid types.UID, raw []byte) error {
	if uid == "" {
		return fmt.Errorf("workloadcluster %q: connection requires a registry UID", name)
	}
	return m.connect(ctx, name, uid, raw)
}

func (m *Manager) connect(ctx context.Context, name string, uid types.UID, raw []byte) error {
	hash := hashBytes(raw)

	m.mu.RLock()
	if cur, ok := m.conns[name]; ok && cur.configHash == hash && cur.clusterUID == uid {
		m.mu.RUnlock()
		return nil
	}
	m.mu.RUnlock()

	cl, cancel, err := m.newClient(ctx, raw, m.scheme)
	if err != nil {
		// A failed rebuild cannot retain a client for a different credential or
		// registry identity. Its requests and cached objects belong elsewhere.
		m.Disconnect(name)
		return fmt.Errorf("workloadcluster %q: build remote client: %w", name, err)
	}

	m.mu.Lock()
	if old, ok := m.conns[name]; ok && old.cancel != nil {
		old.cancel()
	}
	m.nextGeneration++
	m.conns[name] = &remoteConn{client: cl, cancel: cancel, configHash: hash, generation: m.nextGeneration, clusterUID: uid}
	m.mu.Unlock()
	return nil
}

// BaseContext returns the long-lived context wired by Start, or nil if Start
// has not run yet (e.g. in unit tests that drive Connect directly).
func (m *Manager) BaseContext() context.Context {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.baseCtx
}

// Disconnect disposes the client for a cluster (cancels its watch context) and
// drops it from the registry. No-op if absent.
func (m *Manager) Disconnect(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.conns[name]; ok {
		if c.cancel != nil {
			c.cancel()
		}
		delete(m.conns, name)
	}
}

// Connected returns the names of all currently-connected clusters.
func (m *Manager) Connected() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.conns))
	for name := range m.conns {
		names = append(names, name)
	}
	return names
}

func hashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// buildRemoteClient is the default Manager.newClient: validate+parse the
// kubeconfig (RESTConfigFromKubeConfig), apply the configured client tuning,
// build a watch-capable controller-runtime client, and — when CacheOptions
// names cached kinds — wrap it in a selectively-caching client backed by a
// background informer cache. The returned cancel tears that cache down on
// Disconnect; the never-caching path returns a no-op cancel.
func (m *Manager) buildRemoteClient(ctx context.Context, raw []byte, scheme *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
	// Snapshot config under the lock: distinct cluster names build concurrently
	// and a setter may run during wiring.
	m.mu.RLock()
	tuning := m.tuning
	cacheOpts := m.cacheOpts
	execPolicy := m.execPolicy
	m.mu.RUnlock()

	restConfig, err := RESTConfigFromKubeConfig(raw, execPolicy)
	if err != nil {
		return nil, nil, err
	}
	// Raise the per-cluster rate limits and per-call timeout from the client-go
	// defaults (5 QPS / 10 burst) so fan-out across many remote apiservers is not
	// throttled. Zero leaves client-go's default untouched (no magic literal).
	tuning.apply(restConfig)

	wc, err := client.NewWithWatch(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return nil, nil, fmt.Errorf("new watch client: %w", err)
	}

	// No cached kinds configured: never-caching client has no background
	// goroutines, so there is nothing to tear down — return a no-op cancel.
	if !cacheOpts.enabled() {
		return NewNeverCachingClient(wc), func() {}, nil
	}

	// Cached kinds configured: derive a cancellable context from the long-lived
	// base ctx so the cache's informers are scoped to the manager lifetime, and
	// return its cancel so Disconnect tears the cache down (no leaked watches).
	cacheCtx, cancel := context.WithCancel(ctx)
	// The initial cache sync is a watch establishment, so it takes the configured
	// establish budget: a cache that cannot sync must fail the connection rather
	// than count as connected.
	cl, err := NewSelectivelyCachingClient(
		cacheCtx, restConfig, wc, scheme,
		cacheOpts.CachedKinds, cacheOpts.Indexes, cacheOpts.DefaultSelector,
		m.ReconnectBackoff().establishInitial,
	)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("new selectively-caching client: %w", err)
	}
	return cl, cancel, nil
}
