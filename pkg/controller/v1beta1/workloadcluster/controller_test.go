package workloadcluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func wcWithSecret(name string) *v1beta1.WorkloadCluster {
	return &v1beta1.WorkloadCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid")},
		Spec: v1beta1.WorkloadClusterSpec{ClusterSource: v1beta1.ClusterConnectionSource{
			KubeConfig: &v1beta1.KubeConfigSource{SecretRef: corev1.SecretReference{Name: "kc", Namespace: "ome-system"}, Key: "kubeconfig"},
		}},
	}
}

// validKubeconfig is a minimal, parseable kubeconfig that also passes
// validateRESTConfig (no exec/token-file/insecure) so a reconcile gets past the
// BadKubeConfig gate and reaches the injected Probe.
const validKubeconfig = `apiVersion: v1
kind: Config
clusters:
- name: c
  cluster:
    server: https://example.com
    certificate-authority-data: ZHVtbXk=
contexts:
- name: ctx
  context: {cluster: c, user: u}
current-context: ctx
users:
- name: u
  user:
    token: abc123
`

func kcSecret(data string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "kc", Namespace: "ome-system"},
		Data:       map[string][]byte{"kubeconfig": []byte(data)},
	}
}

func newReconciler(t *testing.T, s *runtime.Scheme, probe func(context.Context, []byte) error, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	c := fakeclient.New(t, fakeclient.WithScheme(s), fakeclient.WithObjects(objs...), fakeclient.WithStatusSubresource(&v1beta1.WorkloadCluster{}))
	return &Reconciler{Client: c, Scheme: s, Log: log.Log, Probe: probe}, c
}

func readyCond(c client.Client, name string) *metav1.Condition {
	wc := &v1beta1.WorkloadCluster{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: name}, wc)
	return apimeta.FindStatusCondition(wc.Status.Conditions, v1beta1.WorkloadClusterReady)
}

func TestReconcile_Reachable(t *testing.T) {
	s := fakeclient.Scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret(validKubeconfig)
	probe := func(context.Context, []byte) error { return nil }
	r, c := newReconciler(t, s, probe, wc, secret)

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
	require.NoError(t, err)

	cond := readyCond(c, "c1")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "Reachable", cond.Reason)
}

func TestReconcile_SecretNotFound(t *testing.T) {
	s := fakeclient.Scheme(t)
	r, c := newReconciler(t, s, func(context.Context, []byte) error { return nil }, wcWithSecret("c1"))
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
	require.NoError(t, err)
	cond := readyCond(c, "c1")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "SecretNotFound", cond.Reason)
}

func TestWorkloadClustersForSecretUsesReferenceIndex(t *testing.T) {
	s := fakeclient.Scheme(t)
	match := wcWithSecret("match")
	other := wcWithSecret("other")
	other.Spec.ClusterSource.KubeConfig.SecretRef.Name = "different"
	c := fakeclient.New(t,
		fakeclient.WithScheme(s),
		fakeclient.WithObjects(match, other),
		fakeclient.WithIndex(&v1beta1.WorkloadCluster{}, kubeConfigSecretRefIndex, kubeConfigSecretRefValues))
	r := &Reconciler{Client: c, Log: log.Log}
	reqs := r.workloadClustersForSecret(context.Background(), kcSecret(validKubeconfig))
	require.Len(t, reqs, 1)
	assert.Equal(t, "match", reqs[0].Name)
}

func TestReconcile_ConnectionFailed(t *testing.T) {
	s := fakeclient.Scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret(validKubeconfig) // valid kubeconfig -> reaches the probe
	probe := func(context.Context, []byte) error { return errors.New("dial tcp: timeout") }
	r, c := newReconciler(t, s, probe, wc, secret)
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
	require.NoError(t, err)
	cond := readyCond(c, "c1")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "ConnectionFailed", cond.Reason)
}

// TestReconcile_BadKubeConfig: a malformed kubeconfig is rejected BEFORE the
// probe runs (the probe here would say "Reachable" but must never be reached),
// keeping BadKubeConfig distinct from ConnectionFailed.
func TestReconcile_BadKubeConfig(t *testing.T) {
	s := fakeclient.Scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret("this is not a kubeconfig")
	probeReached := false
	probe := func(context.Context, []byte) error { probeReached = true; return nil }
	r, c := newReconciler(t, s, probe, wc, secret)
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
	require.NoError(t, err)
	cond := readyCond(c, "c1")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "BadKubeConfig", cond.Reason)
	assert.False(t, probeReached, "probe must not run when the kubeconfig is malformed")
}

func TestReconcile_ClusterProfileUnsupported(t *testing.T) {
	s := fakeclient.Scheme(t)
	wc := &v1beta1.WorkloadCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c1"},
		Spec: v1beta1.WorkloadClusterSpec{ClusterSource: v1beta1.ClusterConnectionSource{
			ClusterProfileRef: &v1beta1.ClusterProfileRef{Name: "cp1"}}},
	}
	r, c := newReconciler(t, s, func(context.Context, []byte) error { return nil }, wc)
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
	require.NoError(t, err)
	cond := readyCond(c, "c1")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "ClusterProfileUnsupported", cond.Reason)
}

func TestReconcile_ConnectsManagerWhenReachable(t *testing.T) {
	s := fakeclient.Scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret(validKubeconfig)
	mgr := NewManager(s)
	mgr.newClient = func(_ context.Context, raw []byte, _ *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
		return &stubClient{id: "c1"}, func() {}, nil
	}
	r, _ := newReconciler(t, s, func(context.Context, []byte) error { return nil }, wc, secret)
	r.Manager = mgr

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
	require.NoError(t, err)

	_, ok := mgr.ClientFor("c1")
	assert.True(t, ok, "a Reachable cluster must be connected in the Manager")
}

// TestReconcile_BackoffHoldsConnectionOnTransientProbeFailure: a previously
// reachable+connected cluster whose /version probe fails once must NOT be torn
// down — within the grace window the live client stays connected and Ready is
// held True (reason ProbeFailedRetrying).
func TestReconcile_BackoffHoldsConnectionOnTransientProbeFailure(t *testing.T) {
	s := fakeclient.Scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret(validKubeconfig)
	mgr := NewManager(s)
	mgr.newClient = func(_ context.Context, raw []byte, _ *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
		return &stubClient{id: "c1"}, func() {}, nil
	}
	fail := false
	r, c := newReconciler(t, s, func(context.Context, []byte) error {
		if fail {
			return errors.New("dial tcp: i/o timeout")
		}
		return nil
	}, wc, secret)
	r.Manager = mgr
	r.ConnectionGracePeriod = time.Hour // generous grace so the blip is held

	// Pass 1: reachable -> connected.
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
	require.NoError(t, err)
	_, ok := mgr.ClientFor("c1")
	require.True(t, ok, "cluster must be connected after a reachable pass")

	// Pass 2: transient probe failure within grace -> held.
	fail = true
	_, err = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
	require.NoError(t, err)
	_, ok = mgr.ClientFor("c1")
	assert.True(t, ok, "a transient probe failure within grace must NOT disconnect the live client")
	cond := readyCond(c, "c1")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status, "Ready must be held True within grace")
	assert.Equal(t, "ProbeFailedRetrying", cond.Reason)
}

func TestReconcile_BackoffHoldsConnectionOnTransientSecretReadFailure(t *testing.T) {
	s := fakeclient.Scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret(validKubeconfig)
	base := fakeclient.New(t, fakeclient.WithScheme(s), fakeclient.WithObjects(wc, secret), fakeclient.WithStatusSubresource(&v1beta1.WorkloadCluster{}))
	// The reconciler's view refuses Secret reads once the test says so; the
	// status it writes is still read through base.
	secretReadFails := false
	c := fakeclient.Wrap(t, base, fakeclient.WithInterceptors(interceptor.Funcs{
		Get: func(ctx context.Context, next client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Secret); ok && secretReadFails {
				return errors.New("local apiserver timeout")
			}
			return next.Get(ctx, key, obj, opts...)
		},
	}))
	mgr := NewManager(s)
	mgr.newClient = func(_ context.Context, _ []byte, _ *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
		return &stubClient{id: "c1"}, func() {}, nil
	}
	r := &Reconciler{
		Client: c, Scheme: s, Log: log.Log,
		Probe:   func(context.Context, []byte) error { return nil },
		Manager: mgr, ConnectionGracePeriod: time.Hour,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
	require.NoError(t, err)
	_, ok := mgr.ClientFor("c1")
	require.True(t, ok)

	secretReadFails = true
	_, err = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
	require.NoError(t, err)
	_, ok = mgr.ClientFor("c1")
	assert.True(t, ok, "a transient Secret read failure within grace must not disconnect the live client")
	cond := readyCond(base, "c1")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "ProbeFailedRetrying", cond.Reason)
}

// TestReconcile_BackoffTearsDownAfterGrace: once the grace window elapses, a
// persistent probe failure flips Ready=False and disconnects the client.
func TestReconcile_BackoffTearsDownAfterGrace(t *testing.T) {
	s := fakeclient.Scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret(validKubeconfig)
	mgr := NewManager(s)
	mgr.newClient = func(_ context.Context, raw []byte, _ *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
		return &stubClient{id: "c1"}, func() {}, nil
	}
	fail := false
	r, c := newReconciler(t, s, func(context.Context, []byte) error {
		if fail {
			return errors.New("dial tcp: i/o timeout")
		}
		return nil
	}, wc, secret)
	r.Manager = mgr
	r.ConnectionGracePeriod = time.Hour

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
	require.NoError(t, err)

	// Simulate that the failure run started well outside the grace window.
	fail = true
	r.healthOf("c1").firstFailure = time.Now().Add(-2 * time.Hour)

	_, err = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
	require.NoError(t, err)
	_, ok := mgr.ClientFor("c1")
	assert.False(t, ok, "after grace elapses a persistent failure must disconnect the client")
	cond := readyCond(c, "c1")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "ConnectionFailed", cond.Reason)
}

// TestReconcile_NoGraceForNeverConnected: a cluster that was never connected
// (probe fails on the very first pass) must flip to Ready=False immediately —
// there is no live connection to protect.
func TestReconcile_NoGraceForNeverConnected(t *testing.T) {
	s := fakeclient.Scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret(validKubeconfig)
	mgr := NewManager(s)
	mgr.newClient = func(_ context.Context, raw []byte, _ *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
		return &stubClient{id: "c1"}, func() {}, nil
	}
	r, c := newReconciler(t, s, func(context.Context, []byte) error { return errors.New("dial tcp: timeout") }, wc, secret)
	r.Manager = mgr
	r.ConnectionGracePeriod = time.Hour

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
	require.NoError(t, err)
	_, ok := mgr.ClientFor("c1")
	assert.False(t, ok, "a never-connected cluster has nothing to hold")
	cond := readyCond(c, "c1")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "ConnectionFailed", cond.Reason)
}

// TestReconcile_RecoversAfterTransientFailure: a blip held within grace, then a
// recovered probe, clears the failure clock and reports Reachable again.
func TestReconcile_RecoversAfterTransientFailure(t *testing.T) {
	s := fakeclient.Scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret(validKubeconfig)
	mgr := NewManager(s)
	mgr.newClient = func(_ context.Context, raw []byte, _ *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
		return &stubClient{id: "c1"}, func() {}, nil
	}
	fail := false
	r, c := newReconciler(t, s, func(context.Context, []byte) error {
		if fail {
			return errors.New("blip")
		}
		return nil
	}, wc, secret)
	r.Manager = mgr
	r.ConnectionGracePeriod = time.Hour

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}}
	_, err := r.Reconcile(context.Background(), req)
	require.NoError(t, err)
	fail = true
	_, err = r.Reconcile(context.Background(), req)
	require.NoError(t, err)
	fail = false
	_, err = r.Reconcile(context.Background(), req)
	require.NoError(t, err)

	assert.True(t, r.healthOf("c1").firstFailure.IsZero(), "a recovered probe must clear the failure clock")
	cond := readyCond(c, "c1")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "Reachable", cond.Reason)
}

func TestReconcile_DisconnectsManagerWhenDeleted(t *testing.T) {
	s := fakeclient.Scheme(t)
	mgr := NewManager(s)
	mgr.newClient = func(_ context.Context, raw []byte, _ *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
		return &stubClient{}, func() {}, nil
	}
	require.NoError(t, mgr.Connect(context.Background(), "gone", []byte("x")))
	r, _ := newReconciler(t, s, func(context.Context, []byte) error { return nil }) // no objects -> NotFound
	r.Manager = mgr

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "gone"}})
	require.NoError(t, err)

	_, ok := mgr.ClientFor("gone")
	assert.False(t, ok, "a deleted WorkloadCluster must be disconnected")
}

// TestReconcile_DecidesConnectionWhetherOrNotItConnects: once the controller
// has reconciled a registration, a missing connection is its decision, whether
// the cluster connected, failed to connect, or was removed.
func TestReconcile_DecidesConnectionWhetherOrNotItConnects(t *testing.T) {
	for _, tt := range []struct {
		name      string
		probe     func(context.Context, []byte) error
		objects   bool
		connected bool
	}{
		{name: "reachable", probe: func(context.Context, []byte) error { return nil }, objects: true, connected: true},
		{name: "connection failed", probe: func(context.Context, []byte) error { return errors.New("dial tcp: timeout") }, objects: true},
		{name: "registration deleted", probe: func(context.Context, []byte) error { return nil }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := fakeclient.Scheme(t)
			mgr := NewManager(s)
			mgr.newClient = func(_ context.Context, raw []byte, _ *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
				return &stubClient{id: "c1"}, func() {}, nil
			}
			var objects []client.Object
			if tt.objects {
				objects = []client.Object{wcWithSecret("c1"), kcSecret(validKubeconfig)}
			}
			r, _ := newReconciler(t, s, tt.probe, objects...)
			r.Manager = mgr
			require.True(t, mgr.ConnectionPending("c1", ""))

			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
			require.NoError(t, err)
			_, ok := mgr.ClientFor("c1")
			assert.Equal(t, tt.connected, ok)
			assert.False(t, mgr.ConnectionPending("c1", ""))
			if tt.objects {
				assert.False(t, mgr.ConnectionPending("c1", "c1-uid"))
			}
		})
	}
}
