package workloadcluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestDirectClientBypassesInformerReads(t *testing.T) {
	for _, tt := range []struct {
		name string
		wrap func(client.WithWatch, client.Reader) SelectivelyCachingClient
		want bool
	}{
		{name: "uncached transport", wrap: func(c client.WithWatch, _ client.Reader) SelectivelyCachingClient { return NewNeverCachingClient(c) }, want: true},
		{name: "selectively cached transport", wrap: func(c client.WithWatch, cached client.Reader) SelectivelyCachingClient {
			return &selectivelyCachingClient{WithWatch: c, cachedReader: cached}
		}, want: true},
		{name: "nil transport", wrap: func(client.WithWatch, client.Reader) SelectivelyCachingClient { return NewNeverCachingClient(nil) }},
		{name: "unsupported wrapper", wrap: func(c client.WithWatch, _ client.Reader) SelectivelyCachingClient {
			return struct{ SelectivelyCachingClient }{NewNeverCachingClient(c)}
		}},
		{name: "nil client", wrap: func(client.WithWatch, client.Reader) SelectivelyCachingClient { return nil }},
		{name: "nil direct wrapper", wrap: func(client.WithWatch, client.Reader) SelectivelyCachingClient { return (*neverCachingClient)(nil) }},
		{name: "nil cached wrapper", wrap: func(client.WithWatch, client.Reader) SelectivelyCachingClient {
			return (*selectivelyCachingClient)(nil)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			direct := &recordingClient{}
			cached := &recordingReader{}
			got, ok := DirectClient(tt.wrap(direct, cached))
			if diff := cmp.Diff(tt.want, ok); diff != "" {
				t.Fatalf("available (-want +got):\n%s", diff)
			}
			if !ok {
				if got != nil {
					t.Fatal("unverified transport returned a client")
				}
				return
			}
			if err := got.Get(t.Context(), client.ObjectKey{Name: "service-a"}, &v1beta1.InferenceService{}); err != nil {
				t.Fatal(err)
			}
			if err := got.List(t.Context(), &corev1.PodList{}); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff([4]int{1, 1, 0, 0}, [4]int{direct.getCalls, direct.listCalls, cached.getCalls, cached.listCalls}); diff != "" {
				t.Errorf("direct and cached read counts (-want +got):\n%s", diff)
			}
		})
	}
}

type recordingClient struct {
	client.WithWatch
	getCalls  int
	listCalls int
}

func (c *recordingClient) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	c.getCalls++
	return nil
}

func (c *recordingClient) List(context.Context, client.ObjectList, ...client.ListOption) error {
	c.listCalls++
	return nil
}

type recordingReader struct {
	getCalls  int
	listCalls int
}

func (r *recordingReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	r.getCalls++
	return nil
}

func (r *recordingReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	r.listCalls++
	return nil
}

// blockingReader parks inside the cached read until its context ends, standing
// in for controller-runtime's WaitForCacheSync on an informer that can never
// sync. It signals entry so a test can tear the cache down at exactly the
// moment a read is in flight.
type blockingReader struct {
	entered chan struct{}
	once    sync.Once
}

func (r *blockingReader) enter() {
	r.once.Do(func() { close(r.entered) })
}

func (r *blockingReader) Get(ctx context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	r.enter()
	<-ctx.Done()
	return ctx.Err()
}

func (r *blockingReader) List(ctx context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	r.enter()
	<-ctx.Done()
	return ctx.Err()
}

func TestNeverCachingClient_NoCacheHandler(t *testing.T) {
	fc := fake.NewClientBuilder().Build()
	c := NewNeverCachingClient(fc)
	if _, err := c.AddCacheEventHandler(context.Background(), &corev1.Pod{}, nil); err == nil {
		t.Errorf("AddCacheEventHandler on NeverCachingClient: got nil error, want error")
	}
}

// NewSelectivelyCachingClient must refuse to cache Pods before any cluster
// contact: a cluster-wide Pod informer is exactly the OOM the remote cache
// guards against. The guard returns before cache.New, so a nil restConfig
// proves it short-circuits.
func TestNewSelectivelyCachingClient_RejectsPodKind(t *testing.T) {
	podGK := schema.GroupKind{Group: "", Kind: "Pod"}
	_, err := NewSelectivelyCachingClient(
		context.Background(),
		(*rest.Config)(nil),
		fake.NewClientBuilder().Build(),
		nil,
		sets.New(podGK),
		nil,
		nil,
		time.Second,
	)
	if err == nil {
		t.Fatalf("NewSelectivelyCachingClient with Pod in cachedKinds: got nil error, want rejection")
	}
}

func TestSelectivelyCachingClient_MetadataOnlyReadsBypassCache(t *testing.T) {
	direct := &recordingClient{WithWatch: fake.NewClientBuilder().Build()}
	cached := &recordingReader{}
	metadataGK := schema.GroupKind{Group: "ome.io", Kind: "InferenceService"}
	var synced atomic.Bool
	synced.Store(true)
	c := &selectivelyCachingClient{
		WithWatch:    direct,
		cachedReader: cached,
		cachedKinds:  sets.New(metadataGK),
		synced:       &synced,
	}

	metadata := &metav1.PartialObjectMetadata{}
	metadata.SetGroupVersionKind(metadataGK.WithVersion("v1beta1"))
	if err := c.Get(context.Background(), client.ObjectKey{Name: "example"}, metadata); err != nil {
		t.Fatalf("Get PartialObjectMetadata: %v", err)
	}

	metadataList := &metav1.PartialObjectMetadataList{}
	metadataList.SetGroupVersionKind(metadataGK.WithVersion("v1beta1").GroupVersion().WithKind("InferenceServiceList"))
	if err := c.List(context.Background(), metadataList); err != nil {
		t.Fatalf("List PartialObjectMetadataList: %v", err)
	}

	if direct.getCalls != 1 || direct.listCalls != 1 {
		t.Fatalf("direct metadata reads: got Get=%d List=%d, want Get=1 List=1", direct.getCalls, direct.listCalls)
	}
	if cached.getCalls != 0 || cached.listCalls != 0 {
		t.Fatalf("cached metadata reads: got Get=%d List=%d, want Get=0 List=0", cached.getCalls, cached.listCalls)
	}
}

func omeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatalf("register ome scheme: %v", err)
	}
	return s
}

// newCachedTestClient builds a selectivelyCachingClient over recording halves,
// caching isvcGK() — the kind the remote cache holds in production
// (cmd/manager) and in the integration suites alike — and reporting the cache
// as already synced.
func newCachedTestClient(t *testing.T, cacheCtx context.Context) (*selectivelyCachingClient, *recordingClient, *recordingReader) {
	t.Helper()
	direct := &recordingClient{WithWatch: fake.NewClientBuilder().Build()}
	cached := &recordingReader{}
	var synced atomic.Bool
	synced.Store(true)
	return &selectivelyCachingClient{
		WithWatch:    direct,
		cachedReader: cached,
		cacheCtx:     cacheCtx,
		scheme:       omeScheme(t),
		cachedKinds:  sets.New(isvcGK()),
		synced:       &synced,
	}, direct, cached
}

// A cached read against a torn-down cache must go direct. Routing it to the
// cached reader is what wedges a reconcile: controller-runtime lazily creates
// the informer, declines to start it because the cache is stopped, still
// reports it as started, and then blocks in WaitForCacheSync on the CALLER's
// context — which for a reconcile is the manager's, i.e. forever.
func TestSelectivelyCachingClient_StoppedCacheReadsGoDirect(t *testing.T) {
	cacheCtx, stopCache := context.WithCancel(context.Background())
	c, direct, cached := newCachedTestClient(t, cacheCtx)
	stopCache()

	if err := c.Get(context.Background(), client.ObjectKey{Name: "example"}, &v1beta1.InferenceService{}); err != nil {
		t.Fatalf("Get on stopped cache: %v", err)
	}
	if err := c.List(context.Background(), &v1beta1.InferenceServiceList{}); err != nil {
		t.Fatalf("List on stopped cache: %v", err)
	}

	if direct.getCalls != 1 || direct.listCalls != 1 {
		t.Fatalf("direct reads after cache stop: got Get=%d List=%d, want Get=1 List=1", direct.getCalls, direct.listCalls)
	}
	if cached.getCalls != 0 || cached.listCalls != 0 {
		t.Fatalf("cached reads after cache stop: got Get=%d List=%d, want Get=0 List=0", cached.getCalls, cached.listCalls)
	}
}

// While the cache is live the cached reader still serves the read — the
// teardown guard must not quietly turn every cached read into a direct one.
func TestSelectivelyCachingClient_LiveCacheReadsUseCache(t *testing.T) {
	c, direct, cached := newCachedTestClient(t, context.Background())

	if err := c.Get(context.Background(), client.ObjectKey{Name: "example"}, &v1beta1.InferenceService{}); err != nil {
		t.Fatalf("Get on live cache: %v", err)
	}
	if err := c.List(context.Background(), &v1beta1.InferenceServiceList{}); err != nil {
		t.Fatalf("List on live cache: %v", err)
	}

	if cached.getCalls != 1 || cached.listCalls != 1 {
		t.Fatalf("cached reads on live cache: got Get=%d List=%d, want Get=1 List=1", cached.getCalls, cached.listCalls)
	}
	if direct.getCalls != 0 || direct.listCalls != 0 {
		t.Fatalf("direct reads on live cache: got Get=%d List=%d, want Get=0 List=0", direct.getCalls, direct.listCalls)
	}
}

// A cached read already in flight when the cache is torn down must be released
// and served directly, not left parked. This is the narrow window that the
// servesFromCache pre-check alone cannot cover.
func TestSelectivelyCachingClient_CacheTeardownReleasesInFlightRead(t *testing.T) {
	cacheCtx, stopCache := context.WithCancel(context.Background())
	direct := &recordingClient{WithWatch: fake.NewClientBuilder().Build()}
	blocking := &blockingReader{entered: make(chan struct{})}
	var synced atomic.Bool
	synced.Store(true)
	c := &selectivelyCachingClient{
		WithWatch:    direct,
		cachedReader: blocking,
		cacheCtx:     cacheCtx,
		scheme:       omeScheme(t),
		cachedKinds:  sets.New(isvcGK()),
		synced:       &synced,
	}

	done := make(chan error, 1)
	go func() {
		done <- c.Get(context.Background(), client.ObjectKey{Name: "example"}, &v1beta1.InferenceService{})
	}()

	<-blocking.entered
	stopCache() // Disconnect lands while the read is parked inside the cache.

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("in-flight read after cache teardown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight cached read did not return after the cache was torn down")
	}
	if direct.getCalls != 1 {
		t.Fatalf("direct Get after teardown: got %d, want 1", direct.getCalls)
	}
}

// Registering a watch handler on a torn-down cache must report an error rather
// than block in GetInformer, which reaches the same lazily-created-informer
// path as a cached read.
func TestSelectivelyCachingClient_AddCacheEventHandlerOnStoppedCache(t *testing.T) {
	cacheCtx, stopCache := context.WithCancel(context.Background())
	c, _, _ := newCachedTestClient(t, cacheCtx)
	stopCache()

	if _, err := c.AddCacheEventHandler(context.Background(), &v1beta1.InferenceService{}, nil); err == nil {
		t.Fatal("AddCacheEventHandler on a stopped cache: got nil error, want error")
	}
}
