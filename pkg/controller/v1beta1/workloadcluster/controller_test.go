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
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(s))
	require.NoError(t, clientgoscheme.AddToScheme(s))
	return s
}

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

func newReconciler(s *runtime.Scheme, probe func(context.Context, []byte) error, objs ...client.Object) (*Reconciler, client.Client) {
	c := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(objs...).WithStatusSubresource(&v1beta1.WorkloadCluster{}).Build()
	return &Reconciler{Client: c, Scheme: s, Log: log.Log, Probe: probe}, c
}

type secretReadErrorClient struct {
	client.Client
	fail bool
}

func (c *secretReadErrorClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if c.fail {
		if _, ok := obj.(*corev1.Secret); ok {
			return errors.New("local apiserver timeout")
		}
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func readyCond(c client.Client, name string) *metav1.Condition {
	wc := &v1beta1.WorkloadCluster{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: name}, wc)
	return apimeta.FindStatusCondition(wc.Status.Conditions, v1beta1.WorkloadClusterReady)
}

func TestReconcile_Reachable(t *testing.T) {
	s := scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret(validKubeconfig)
	probe := func(context.Context, []byte) error { return nil }
	r, c := newReconciler(s, probe, wc, secret)

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
	require.NoError(t, err)

	cond := readyCond(c, "c1")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "Reachable", cond.Reason)
}

func TestReconcile_SecretNotFound(t *testing.T) {
	s := scheme(t)
	r, c := newReconciler(s, func(context.Context, []byte) error { return nil }, wcWithSecret("c1"))
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
	require.NoError(t, err)
	cond := readyCond(c, "c1")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "SecretNotFound", cond.Reason)
}

func TestWorkloadClustersForSecretUsesReferenceIndex(t *testing.T) {
	s := scheme(t)
	match := wcWithSecret("match")
	other := wcWithSecret("other")
	other.Spec.ClusterSource.KubeConfig.SecretRef.Name = "different"
	c := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(match, other).
		WithIndex(&v1beta1.WorkloadCluster{}, kubeConfigSecretRefIndex, kubeConfigSecretRefValues).Build()
	r := &Reconciler{Client: c, Log: log.Log}
	reqs := r.workloadClustersForSecret(context.Background(), kcSecret(validKubeconfig))
	require.Len(t, reqs, 1)
	assert.Equal(t, "match", reqs[0].Name)
}

func TestReconcile_ConnectionFailed(t *testing.T) {
	s := scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret(validKubeconfig) // valid kubeconfig -> reaches the probe
	probe := func(context.Context, []byte) error { return errors.New("dial tcp: timeout") }
	r, c := newReconciler(s, probe, wc, secret)
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
	s := scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret("this is not a kubeconfig")
	probeReached := false
	probe := func(context.Context, []byte) error { probeReached = true; return nil }
	r, c := newReconciler(s, probe, wc, secret)
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
	require.NoError(t, err)
	cond := readyCond(c, "c1")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "BadKubeConfig", cond.Reason)
	assert.False(t, probeReached, "probe must not run when the kubeconfig is malformed")
}

func TestReconcile_ClusterProfileUnsupported(t *testing.T) {
	s := scheme(t)
	wc := &v1beta1.WorkloadCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c1"},
		Spec: v1beta1.WorkloadClusterSpec{ClusterSource: v1beta1.ClusterConnectionSource{
			ClusterProfileRef: &v1beta1.ClusterProfileRef{Name: "cp1"}}},
	}
	r, c := newReconciler(s, func(context.Context, []byte) error { return nil }, wc)
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "c1"}})
	require.NoError(t, err)
	cond := readyCond(c, "c1")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "ClusterProfileUnsupported", cond.Reason)
}

func TestReconcile_ConnectsManagerWhenReachable(t *testing.T) {
	s := scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret(validKubeconfig)
	mgr := NewManager(s)
	mgr.newClient = func(_ context.Context, raw []byte, _ *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
		return &stubClient{id: "c1"}, func() {}, nil
	}
	r, _ := newReconciler(s, func(context.Context, []byte) error { return nil }, wc, secret)
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
	s := scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret(validKubeconfig)
	mgr := NewManager(s)
	mgr.newClient = func(_ context.Context, raw []byte, _ *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
		return &stubClient{id: "c1"}, func() {}, nil
	}
	fail := false
	r, c := newReconciler(s, func(context.Context, []byte) error {
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
	s := scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret(validKubeconfig)
	base := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(wc, secret).WithStatusSubresource(&v1beta1.WorkloadCluster{}).Build()
	c := &secretReadErrorClient{Client: base}
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

	c.fail = true
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
	s := scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret(validKubeconfig)
	mgr := NewManager(s)
	mgr.newClient = func(_ context.Context, raw []byte, _ *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
		return &stubClient{id: "c1"}, func() {}, nil
	}
	fail := false
	r, c := newReconciler(s, func(context.Context, []byte) error {
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
	r.mu.Lock()
	if r.firstFailure == nil {
		r.firstFailure = map[string]time.Time{}
	}
	r.firstFailure["c1"] = time.Now().Add(-2 * time.Hour)
	r.mu.Unlock()

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
	s := scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret(validKubeconfig)
	mgr := NewManager(s)
	mgr.newClient = func(_ context.Context, raw []byte, _ *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
		return &stubClient{id: "c1"}, func() {}, nil
	}
	r, c := newReconciler(s, func(context.Context, []byte) error { return errors.New("dial tcp: timeout") }, wc, secret)
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
	s := scheme(t)
	wc := wcWithSecret("c1")
	secret := kcSecret(validKubeconfig)
	mgr := NewManager(s)
	mgr.newClient = func(_ context.Context, raw []byte, _ *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
		return &stubClient{id: "c1"}, func() {}, nil
	}
	fail := false
	r, c := newReconciler(s, func(context.Context, []byte) error {
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

	r.mu.Lock()
	_, stillFailing := r.firstFailure["c1"]
	r.mu.Unlock()
	assert.False(t, stillFailing, "a recovered probe must clear the failure clock")
	cond := readyCond(c, "c1")
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "Reachable", cond.Reason)
}

func TestReconcile_DisconnectsManagerWhenDeleted(t *testing.T) {
	s := scheme(t)
	mgr := NewManager(s)
	mgr.newClient = func(_ context.Context, raw []byte, _ *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
		return &stubClient{}, func() {}, nil
	}
	require.NoError(t, mgr.Connect(context.Background(), "gone", []byte("x")))
	r, _ := newReconciler(s, func(context.Context, []byte) error { return nil }) // no objects -> NotFound
	r.Manager = mgr

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "gone"}})
	require.NoError(t, err)

	_, ok := mgr.ClientFor("gone")
	assert.False(t, ok, "a deleted WorkloadCluster must be disconnected")
}
