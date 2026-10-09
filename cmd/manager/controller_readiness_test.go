package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type capturingManager struct {
	manager.Manager
	added []manager.Runnable
	err   error
}

func (m *capturingManager) Add(r manager.Runnable) error {
	m.added = append(m.added, r)
	return m.err
}

func (m *capturingManager) GetControllerOptions() config.Controller {
	return config.Controller{EnableWarmup: ptr.To(true)}
}

type testSource struct {
	warmupErr error
	started   bool
	leader    bool
}

func (s *testSource) NeedLeaderElection() bool     { return s.leader }
func (s *testSource) Warmup(context.Context) error { return s.warmupErr }
func (s *testSource) Start(context.Context) error {
	s.started = true
	return errors.New("controller stopped")
}

func TestControllerReadinessRequiresEverySource(t *testing.T) {
	base := &capturingManager{}
	m := &sourceReadyManager{Manager: base}
	require.Error(t, m.sourcesReady(nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for range 2 {
		require.NoError(t, m.Add(&testSource{leader: true}))
	}
	require.Error(t, m.sourcesReady(nil))
	require.NoError(t, base.added[0].(warmingRunnable).Warmup(ctx))
	require.Error(t, m.sourcesReady(nil))
	require.NoError(t, base.added[1].(warmingRunnable).Warmup(ctx))
	require.NoError(t, m.sourcesReady(nil))
}

func TestControllerReadinessKeepsStandbysReadyWithoutReconciling(t *testing.T) {
	for _, leader := range []bool{false, true} {
		base := &capturingManager{}
		m := &sourceReadyManager{Manager: base}
		source := &testSource{leader: leader}
		require.NoError(t, m.Add(source))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tracked := base.added[0].(warmingRunnable)
		require.Equal(t, leader, tracked.NeedLeaderElection())
		require.NoError(t, tracked.Warmup(ctx))
		require.NoError(t, m.sourcesReady(nil))
		require.False(t, source.started, "warmup must not start reconciliation")
		require.Error(t, tracked.Start(ctx))
		require.Error(t, m.sourcesReady(nil), "exited controllers must not remain ready")
	}
}

func TestControllerReadinessRejectsMissingCRDOrRBACAndTimeout(t *testing.T) {
	for _, err := range []error{errors.New("missing InferenceReplica CRD"), errors.New("forbidden: list endpointslices"), context.DeadlineExceeded} {
		base := &capturingManager{}
		m := &sourceReadyManager{Manager: base}
		require.NoError(t, m.Add(&testSource{warmupErr: err, leader: true}))
		require.ErrorIs(t, base.added[0].(warmingRunnable).Warmup(context.Background()), err)
		require.Error(t, m.sourcesReady(nil))
	}
}

func TestControllerReadinessCancelledWarmup(t *testing.T) {
	base := &capturingManager{}
	m := &sourceReadyManager{Manager: base}
	require.NoError(t, m.Add(&testSource{}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, base.added[0].(warmingRunnable).Warmup(ctx), context.Canceled)
	require.Error(t, m.sourcesReady(nil))
}

func TestControllerReadinessPreservesUnrelatedRunnableAndAddError(t *testing.T) {
	base := &capturingManager{}
	m := &sourceReadyManager{Manager: base}
	r := manager.RunnableFunc(func(context.Context) error { return nil })
	require.NoError(t, m.Add(r))
	require.IsType(t, r, base.added[0])
	require.Empty(t, m.sources)
	base.err = errors.New("registration failed")
	require.ErrorIs(t, m.Add(&testSource{}), base.err)
	require.Empty(t, m.sources)
}

type gatedSource struct {
	started chan struct{}
	synced  chan struct{}
}

func (s *gatedSource) Start(context.Context, workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	close(s.started)
	return nil
}

func (s *gatedSource) WaitForSync(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.synced:
		return nil
	}
}

func TestControllerReadinessUsesRealControllerRuntimeWarmup(t *testing.T) {
	base := &capturingManager{}
	m := &sourceReadyManager{Manager: base}
	c, err := controller.New("readiness-regression", m, controller.Options{
		Logger: logr.Discard(), CacheSyncTimeout: time.Second,
		Reconciler: reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
			t.Error("standby must not reconcile")
			return reconcile.Result{}, nil
		}),
	})
	require.NoError(t, err)
	s := &gatedSource{started: make(chan struct{}), synced: make(chan struct{})}
	require.NoError(t, c.Watch(s))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- base.added[0].(warmingRunnable).Warmup(ctx) }()
	select {
	case <-s.started:
	case <-time.After(5 * time.Second):
		t.Fatal("controller did not start its watch sources")
	}
	require.Error(t, m.sourcesReady(nil), "webhook readiness cannot hide an unsynced watch")
	close(s.synced)
	require.NoError(t, <-result)
	require.NoError(t, m.sourcesReady(nil))
	cancel()
	require.Eventually(t, func() bool { return m.sourcesReady(nil) != nil }, time.Second, time.Millisecond)
}
