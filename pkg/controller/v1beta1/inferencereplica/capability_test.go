package inferencereplica

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

var capabilityNow = time.Date(2026, 8, 31, 17, 0, 0, 0, time.UTC)

func TestCapabilityPublisherCreatesTrustPayload(t *testing.T) {
	c := capabilityClient(t)
	publisher := NewCapabilityPublisher(c, c, cacheSyncerFunc(func(context.Context) bool { return true }), nil, "ome", "manager-7")
	publisher.clock = clocktesting.NewFakeClock(capabilityNow)

	if err := publisher.PublishOnce(context.Background()); err != nil {
		t.Fatalf("PublishOnce() error = %v", err)
	}

	lease := getCapabilityLease(t, c)
	if got := lease.Annotations["ome.io/migration-request-schema"]; got != "v1" {
		t.Fatalf("schema annotation = %q, want v1", got)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "manager-7" {
		t.Fatalf("holderIdentity = %v, want manager-7", lease.Spec.HolderIdentity)
	}
	if lease.Spec.RenewTime == nil || !lease.Spec.RenewTime.Time.Equal(capabilityNow) {
		t.Fatalf("renewTime = %v, want %v", lease.Spec.RenewTime, capabilityNow)
	}
	if lease.Spec.AcquireTime != nil || lease.Spec.LeaseDurationSeconds != nil || lease.Spec.LeaseTransitions != nil {
		t.Fatalf("capability Lease contains election fields: %+v", lease.Spec)
	}
}

func TestCapabilityPublisherRejectsInvalidIdentityBeforeAPI(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		holder    string
	}{
		{name: "empty namespace", holder: "manager-7"},
		{name: "whitespace namespace", namespace: " \t\n", holder: "manager-7"},
		{name: "empty holder", namespace: "ome"},
		{name: "whitespace holder", namespace: "ome", holder: " \t\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := capabilityClient(t)
			var apiCalls atomic.Int32
			tracked := interceptor.NewClient(base, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					apiCalls.Add(1)
					return c.Get(ctx, key, obj, opts...)
				},
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					apiCalls.Add(1)
					return c.Create(ctx, obj, opts...)
				},
				Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					apiCalls.Add(1)
					return c.Update(ctx, obj, opts...)
				},
			})
			publisher := NewCapabilityPublisher(tracked, tracked, cacheSyncerFunc(func(context.Context) bool { return true }), nil, tt.namespace, tt.holder)

			if err := publisher.PublishOnce(context.Background()); err == nil {
				t.Fatal("PublishOnce() error = nil, want invalid identity error")
			}
			if got := apiCalls.Load(); got != 0 {
				t.Fatalf("API calls = %d, want 0", got)
			}
		})
	}
}

func TestCapabilityPublisherStartRejectsInvalidIdentity(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		holder    string
	}{
		{name: "empty namespace", holder: "manager-7"},
		{name: "whitespace namespace", namespace: " \t\n", holder: "manager-7"},
		{name: "empty holder", namespace: "ome"},
		{name: "whitespace holder", namespace: "ome", holder: " \t\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := capabilityClient(t)
			var apiCalls, cacheSyncs atomic.Int32
			tracked := interceptor.NewClient(base, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					apiCalls.Add(1)
					return c.Get(ctx, key, obj, opts...)
				},
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					apiCalls.Add(1)
					return c.Create(ctx, obj, opts...)
				},
				Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					apiCalls.Add(1)
					return c.Update(ctx, obj, opts...)
				},
			})
			publisher := NewCapabilityPublisher(tracked, tracked, cacheSyncerFunc(func(context.Context) bool {
				cacheSyncs.Add(1)
				return true
			}), closedCapabilityReadiness(), tt.namespace, tt.holder)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			if err := publisher.Start(ctx); err == nil {
				t.Fatal("Start() error = nil, want invalid identity error")
			}
			if got := cacheSyncs.Load(); got != 0 {
				t.Fatalf("cache syncs = %d, want 0", got)
			}
			if got := apiCalls.Load(); got != 0 {
				t.Fatalf("API calls = %d, want 0", got)
			}
		})
	}
}

func TestCapabilityPublisherStartWaitsForControllerReadiness(t *testing.T) {
	base := capabilityClient(t)
	readiness := make(chan struct{})
	cacheEntered := make(chan struct{}, 1)
	created := make(chan struct{}, 1)
	var cacheCalls, apiCalls atomic.Int32
	syncer := cacheSyncerFunc(func(context.Context) bool {
		cacheCalls.Add(1)
		cacheEntered <- struct{}{}
		return true
	})
	tracked := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			apiCalls.Add(1)
			return c.Get(ctx, key, obj, opts...)
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			apiCalls.Add(1)
			if err := c.Create(ctx, obj, opts...); err != nil {
				return err
			}
			created <- struct{}{}
			return nil
		},
	})
	publisher := NewCapabilityPublisher(tracked, tracked, syncer, readiness, "ome", "manager-7")
	publisher.clock = clocktesting.NewFakeClock(capabilityNow)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- publisher.Start(ctx) }()

	select {
	case <-cacheEntered:
		t.Fatal("cache sync began before controller readiness")
	case <-time.After(50 * time.Millisecond):
	}
	assert.Equal(t, int32(0), cacheCalls.Load())
	assert.Equal(t, int32(0), apiCalls.Load())

	close(readiness)
	waitForCapabilitySignal(t, cacheEntered, "cache sync after controller readiness")
	waitForCapabilitySignal(t, created, "immediate publish after controller readiness")
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Start() did not stop after cancellation")
	}
}

func TestCapabilityPublisherStartCancelsBeforeControllerReadiness(t *testing.T) {
	base := capabilityClient(t)
	var cacheCalls, apiCalls atomic.Int32
	tracked := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			apiCalls.Add(1)
			return c.Get(ctx, key, obj, opts...)
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			apiCalls.Add(1)
			return c.Create(ctx, obj, opts...)
		},
	})
	publisher := NewCapabilityPublisher(tracked, tracked, cacheSyncerFunc(func(context.Context) bool {
		cacheCalls.Add(1)
		return true
	}), make(chan struct{}), "ome", "manager-7")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.NoError(t, publisher.Start(ctx))
	assert.Equal(t, int32(0), cacheCalls.Load())
	assert.Equal(t, int32(0), apiCalls.Load())
}

func TestCapabilityPublisherStartRejectsMissingControllerReadiness(t *testing.T) {
	base := capabilityClient(t)
	var cacheCalls atomic.Int32
	publisher := NewCapabilityPublisher(base, base, cacheSyncerFunc(func(context.Context) bool {
		cacheCalls.Add(1)
		return true
	}), nil, "ome", "manager-7")

	if err := publisher.Start(context.Background()); err == nil {
		t.Fatal("Start() error = nil, want missing controller readiness error")
	}
	assert.Equal(t, int32(0), cacheCalls.Load())
}

func TestCapabilityPublisherTakesOverAndPreservesMetadata(t *testing.T) {
	oldHolder := "manager-old"
	duration := int32(15)
	transitions := int32(4)
	acquired := metav1.NewMicroTime(capabilityNow.Add(-time.Hour))
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "ome",
			Name:        "ome-inferencereplica-executor",
			Labels:      map[string]string{"keep-label": "yes"},
			Annotations: map[string]string{"keep.example/key": "keep", "ome.io/migration-request-schema": "old"},
			Finalizers:  []string{"keep.example/finalizer"},
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &oldHolder,
			AcquireTime:          &acquired,
			LeaseDurationSeconds: &duration,
			LeaseTransitions:     &transitions,
		},
	}
	c := capabilityClient(t, existing)
	publisher := NewCapabilityPublisher(c, c, cacheSyncerFunc(func(context.Context) bool { return true }), nil, "ome", "manager-new")
	publisher.clock = clocktesting.NewFakeClock(capabilityNow)

	if err := publisher.PublishOnce(context.Background()); err != nil {
		t.Fatalf("PublishOnce() error = %v", err)
	}

	lease := getCapabilityLease(t, c)
	if lease.Labels["keep-label"] != "yes" || lease.Annotations["keep.example/key"] != "keep" {
		t.Fatalf("unrelated metadata was not preserved: labels=%v annotations=%v", lease.Labels, lease.Annotations)
	}
	if len(lease.Finalizers) != 1 || lease.Finalizers[0] != "keep.example/finalizer" {
		t.Fatalf("finalizers = %v, want preserved", lease.Finalizers)
	}
	if got := lease.Annotations["ome.io/migration-request-schema"]; got != "v1" {
		t.Fatalf("schema annotation = %q, want v1", got)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "manager-new" {
		t.Fatalf("holderIdentity = %v, want manager-new", lease.Spec.HolderIdentity)
	}
	if lease.Spec.AcquireTime != nil || lease.Spec.LeaseDurationSeconds != nil || lease.Spec.LeaseTransitions != nil {
		t.Fatalf("stale election fields survived takeover: %+v", lease.Spec)
	}
}

func TestCapabilityPublisherRetriesConflictWithFreshRead(t *testing.T) {
	holder := "manager-old"
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ome", Name: "ome-inferencereplica-executor"},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder},
	}
	base := capabilityClient(t, existing)
	var reads, updates atomic.Int32
	reader := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			reads.Add(1)
			return c.Get(ctx, key, obj, opts...)
		},
	})
	writer := interceptor.NewClient(base, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if updates.Add(1) == 1 {
				return apierrors.NewConflict(capabilityLeaseResource(), obj.GetName(), errors.New("concurrent update"))
			}
			return c.Update(ctx, obj, opts...)
		},
	})
	publisher := NewCapabilityPublisher(reader, writer, cacheSyncerFunc(func(context.Context) bool { return true }), nil, "ome", "manager-new")
	publisher.clock = clocktesting.NewFakeClock(capabilityNow)
	publisher.retryBackoff = wait.Backoff{Steps: 3}

	if err := publisher.PublishOnce(context.Background()); err != nil {
		t.Fatalf("PublishOnce() error = %v", err)
	}
	if got := reads.Load(); got != 2 {
		t.Fatalf("fresh API reads = %d, want 2", got)
	}
	if got := updates.Load(); got != 2 {
		t.Fatalf("updates = %d, want 2", got)
	}
	if lease := getCapabilityLease(t, base); lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "manager-new" {
		t.Fatalf("conflict retry did not publish new holder: %+v", lease.Spec)
	}
}

func TestCapabilityPublisherRetriesCreateRaceWithFreshRead(t *testing.T) {
	holder := "racing-manager"
	racing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ome",
			Name:      "ome-inferencereplica-executor",
			Labels:    map[string]string{"racing-writer": "preserve"},
		},
		Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder},
	}
	base := capabilityClient(t, racing)
	var reads atomic.Int32
	reader := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if reads.Add(1) == 1 {
				return apierrors.NewNotFound(capabilityLeaseResource(), key.Name)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	publisher := NewCapabilityPublisher(reader, base, cacheSyncerFunc(func(context.Context) bool { return true }), nil, "ome", "manager-new")
	publisher.clock = clocktesting.NewFakeClock(capabilityNow)
	publisher.retryBackoff = wait.Backoff{Steps: 3}

	if err := publisher.PublishOnce(context.Background()); err != nil {
		t.Fatalf("PublishOnce() error = %v", err)
	}
	if got := reads.Load(); got != 2 {
		t.Fatalf("fresh API reads = %d, want 2", got)
	}
	lease := getCapabilityLease(t, base)
	if lease.Labels["racing-writer"] != "preserve" {
		t.Fatalf("racing writer metadata was lost: %v", lease.Labels)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "manager-new" {
		t.Fatalf("create race did not converge on new holder: %+v", lease.Spec)
	}
}

func TestCapabilityPublisherRetriesAreBounded(t *testing.T) {
	holder := "manager-old"
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ome", Name: "ome-inferencereplica-executor"},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder},
	}
	base := capabilityClient(t, existing)
	var updates atomic.Int32
	writer := interceptor.NewClient(base, interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.UpdateOption) error {
			updates.Add(1)
			return apierrors.NewConflict(capabilityLeaseResource(), obj.GetName(), errors.New("persistent conflict"))
		},
	})
	publisher := NewCapabilityPublisher(base, writer, cacheSyncerFunc(func(context.Context) bool { return true }), nil, "ome", "manager-new")
	publisher.clock = clocktesting.NewFakeClock(capabilityNow)
	publisher.retryBackoff = wait.Backoff{Steps: 3}

	err := publisher.PublishOnce(context.Background())
	if !apierrors.IsConflict(err) {
		t.Fatalf("PublishOnce() error = %v, want conflict", err)
	}
	if got := updates.Load(); got != 3 {
		t.Fatalf("updates = %d, want bounded 3 attempts", got)
	}
}

func TestCapabilityPublisherWaitsForCacheSyncRenewsAndDoesNotDelete(t *testing.T) {
	base := capabilityClient(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	syncer := cacheSyncerFunc(func(ctx context.Context) bool {
		close(entered)
		select {
		case <-release:
			return true
		case <-ctx.Done():
			return false
		}
	})
	created := make(chan struct{}, 1)
	updated := make(chan struct{}, 1)
	var writes, deletes atomic.Int32
	writer := interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if err := c.Create(ctx, obj, opts...); err != nil {
				return err
			}
			writes.Add(1)
			created <- struct{}{}
			return nil
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if err := c.Update(ctx, obj, opts...); err != nil {
				return err
			}
			writes.Add(1)
			updated <- struct{}{}
			return nil
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			deletes.Add(1)
			return c.Delete(ctx, obj, opts...)
		},
	})
	fakeClock := clocktesting.NewFakeClock(capabilityNow)
	publisher := NewCapabilityPublisher(base, writer, syncer, closedCapabilityReadiness(), "ome", "manager-7")
	publisher.clock = fakeClock

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- publisher.Start(ctx) }()
	<-entered
	if got := writes.Load(); got != 0 {
		t.Fatalf("writes before cache sync = %d, want 0", got)
	}
	close(release)
	waitForCapabilitySignal(t, created, "immediate capability create")
	waitForCapabilityTicker(t, fakeClock)
	fakeClock.Step(10 * time.Second)
	waitForCapabilitySignal(t, updated, "periodic capability renewal")

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start() did not stop after cancellation")
	}
	if got := deletes.Load(); got != 0 {
		t.Fatalf("deletes on cancellation = %d, want 0", got)
	}
	lease := getCapabilityLease(t, base)
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "manager-7" {
		t.Fatalf("capability was cleared on cancellation: %+v", lease.Spec)
	}
	if lease.Spec.RenewTime == nil || !lease.Spec.RenewTime.Time.Equal(capabilityNow.Add(10*time.Second)) {
		t.Fatalf("periodic renewTime = %v, want %v", lease.Spec.RenewTime, capabilityNow.Add(10*time.Second))
	}
}

func TestCapabilityPublisherStartContinuesAfterPublishError(t *testing.T) {
	base := capabilityClient(t)
	firstFailed := make(chan struct{}, 1)
	created := make(chan struct{}, 1)
	var creates atomic.Int32
	writer := interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if creates.Add(1) == 1 {
				firstFailed <- struct{}{}
				return errors.New("temporary apiserver failure")
			}
			if err := c.Create(ctx, obj, opts...); err != nil {
				return err
			}
			created <- struct{}{}
			return nil
		},
	})
	fakeClock := clocktesting.NewFakeClock(capabilityNow)
	publisher := NewCapabilityPublisher(base, writer, cacheSyncerFunc(func(context.Context) bool { return true }), closedCapabilityReadiness(), "ome", "manager-7")
	publisher.clock = fakeClock

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- publisher.Start(ctx) }()
	waitForCapabilitySignal(t, firstFailed, "initial publish failure")
	select {
	case err := <-done:
		t.Fatalf("Start() stopped after publish error: %v", err)
	default:
	}
	waitForCapabilityTicker(t, fakeClock)
	fakeClock.Step(10 * time.Second)
	waitForCapabilitySignal(t, created, "publish retry on next heartbeat")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start() error = %v", err)
	}
}

func TestCapabilityPublisherNeedsLeaderElection(t *testing.T) {
	c := capabilityClient(t)
	publisher := NewCapabilityPublisher(c, c, cacheSyncerFunc(func(context.Context) bool { return true }), nil, "ome", "manager-7")
	if !publisher.NeedLeaderElection() {
		t.Fatal("capability publisher must run only after leader election")
	}
}

type cacheSyncerFunc func(context.Context) bool

func (f cacheSyncerFunc) WaitForCacheSync(ctx context.Context) bool { return f(ctx) }

func capabilityClient(t *testing.T, objects ...client.Object) client.WithWatch {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add coordination scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func getCapabilityLease(t *testing.T, c client.Reader) *coordinationv1.Lease {
	t.Helper()
	lease := &coordinationv1.Lease{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "ome", Name: "ome-inferencereplica-executor"}, lease); err != nil {
		t.Fatalf("get capability Lease: %v", err)
	}
	return lease
}

func capabilityLeaseResource() schema.GroupResource {
	return schema.GroupResource{Group: coordinationv1.GroupName, Resource: "leases"}
}

func waitForCapabilitySignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitForCapabilityTicker(t *testing.T, fakeClock *clocktesting.FakeClock) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !fakeClock.HasWaiters() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for capability ticker")
		}
		time.Sleep(time.Millisecond)
	}
}

func closedCapabilityReadiness() <-chan struct{} {
	ready := make(chan struct{})
	close(ready)
	return ready
}
