package acceleratorquota

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// fakeRegistry records what the connector asked the transport to do.
type fakeRegistry struct {
	connected  map[string]string
	disconnect []string
	connectErr error
	identities map[string]types.UID
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{connected: map[string]string{}, identities: map[string]types.UID{}}
}

func (f *fakeRegistry) ConnectFor(_ context.Context, name string, uid types.UID, kubeconfig []byte) error {
	if f.connectErr != nil {
		return f.connectErr
	}
	f.connected[name] = string(kubeconfig)
	f.identities[name] = uid
	return nil
}

func (f *fakeRegistry) Disconnect(name string) {
	f.disconnect = append(f.disconnect, name)
	delete(f.connected, name)
	delete(f.identities, name)
}

// Connected is what the Connector compares across a pass to decide whether the
// reachable set moved, so it must reflect Connect and Disconnect immediately.
func (f *fakeRegistry) Connected() []string {
	out := make([]string, 0, len(f.connected))
	for name := range f.connected {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func connectScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatalf("add ome scheme: %v", err)
	}
	return s
}

func workloadCluster(name string, ready bool, secretName, key string) *v1beta1.WorkloadCluster {
	wc := &v1beta1.WorkloadCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid")},
		Spec: v1beta1.WorkloadClusterSpec{
			ClusterSource: v1beta1.ClusterConnectionSource{
				KubeConfig: &v1beta1.KubeConfigSource{
					SecretRef: corev1.SecretReference{Name: secretName, Namespace: "ome"},
					Key:       key,
				},
			},
		},
	}
	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}
	wc.Status.Conditions = []metav1.Condition{{
		Type: v1beta1.WorkloadClusterReady, Status: status,
		Reason: "Probed", LastTransitionTime: metav1.Now(),
	}}
	return wc
}

func kubeconfigSecret(name, key, body string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ome"},
		Data:       map[string][]byte{key: []byte(body)},
	}
}

// The registry decides what is reachable and the transport follows it. Getting
// this backwards in either direction is expensive: connecting to a cluster the
// registry has given up on wastes informers on an unreachable apiserver, and
// dropping one it still calls Ready silently stops projection to a live member.
func TestConnectorMirrorsTheRegistry(t *testing.T) {
	tests := []struct {
		name string
		// objects seeds the control plane. Absent WorkloadCluster => deregistered.
		objects        []client.Object
		wantConnected  map[string]string
		wantDisconnect []string
	}{
		{
			name: "a Ready cluster is connected with its kubeconfig",
			objects: []client.Object{
				workloadCluster("member-a", true, "member-a-kubeconfig", "kubeconfig"),
				kubeconfigSecret("member-a-kubeconfig", "kubeconfig", "KUBECONFIG-A"),
			},
			wantConnected: map[string]string{"member-a": "KUBECONFIG-A"},
		},
		{
			// The registry applies its own grace period before flipping Ready
			// off, so by the time this is False the blip window has passed and
			// holding the client would only keep a dead one warm.
			name: "a cluster the registry calls unready is dropped",
			objects: []client.Object{
				workloadCluster("member-a", false, "member-a-kubeconfig", "kubeconfig"),
				kubeconfigSecret("member-a-kubeconfig", "kubeconfig", "KUBECONFIG-A"),
			},
			wantDisconnect: []string{"member-a"},
		},
		{
			name:           "a deregistered cluster is dropped",
			objects:        nil,
			wantDisconnect: []string{"member-a"},
		},
		{
			// The credential lives at whichever key the source names, and a
			// connector that assumed the default would connect nothing.
			name: "a non-default Secret key is honoured",
			objects: []client.Object{
				workloadCluster("member-a", true, "creds", "admin.conf"),
				kubeconfigSecret("creds", "admin.conf", "KUBECONFIG-CUSTOM"),
			},
			wantConnected: map[string]string{"member-a": "KUBECONFIG-CUSTOM"},
		},
		{
			// An unset key is the API's documented default. Objects the
			// apiserver defaulted carry it explicitly, but one built in memory
			// does not, and both reach this code.
			name: "an unset Secret key falls back to kubeconfig",
			objects: []client.Object{
				workloadCluster("member-a", true, "creds", ""),
				kubeconfigSecret("creds", "kubeconfig", "KUBECONFIG-DEFAULTED"),
			},
			wantConnected: map[string]string{"member-a": "KUBECONFIG-DEFAULTED"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := connectScheme(t)
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(tc.objects...).Build()
			reg := newFakeRegistry()
			conn := &Connector{Client: c, Log: logf.Log.WithName("test"), Clusters: reg}

			_, err := conn.Reconcile(context.Background(),
				ctrl.Request{NamespacedName: types.NamespacedName{Name: "member-a"}})
			if err != nil {
				t.Fatalf("Reconcile() = %v", err)
			}

			want := tc.wantConnected
			if want == nil {
				want = map[string]string{}
			}
			if diff := cmp.Diff(want, reg.connected); diff != "" {
				t.Errorf("connections mismatch (-want +got):\n%s", diff)
			}
			for name := range want {
				var wc v1beta1.WorkloadCluster
				if err := c.Get(context.Background(), types.NamespacedName{Name: name}, &wc); err != nil {
					t.Fatal(err)
				}
				if wc.UID == "" {
					t.Fatal("registry must have a UID")
				}
				if diff := cmp.Diff(wc.UID, reg.identities[name]); diff != "" {
					t.Errorf("connection UID (-want +got):\n%s", diff)
				}
			}
			if diff := cmp.Diff(tc.wantDisconnect, reg.disconnect); diff != "" {
				t.Errorf("disconnections mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// The restraint that makes a second process safe beside the registry's owner.
// WorkloadCluster status has one writer whose grace and backoff state is in
// memory and not behind a lease; a second writer would flap the condition and
// lose the argument silently, since conflicts on that path requeue unreported.
func TestConnectorNeverWritesTheRegistry(t *testing.T) {
	s := connectScheme(t)
	var writes []string
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(
			workloadCluster("member-a", true, "creds", "kubeconfig"),
			kubeconfigSecret("creds", "kubeconfig", "KUBECONFIG-A"),
		).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, obj client.Object,
				_ ...client.CreateOption) error {
				writes = append(writes, "create "+obj.GetName())
				return nil
			},
			Update: func(_ context.Context, _ client.WithWatch, obj client.Object,
				_ ...client.UpdateOption) error {
				writes = append(writes, "update "+obj.GetName())
				return nil
			},
			Patch: func(_ context.Context, _ client.WithWatch, obj client.Object,
				_ client.Patch, _ ...client.PatchOption) error {
				writes = append(writes, "patch "+obj.GetName())
				return nil
			},
			Delete: func(_ context.Context, _ client.WithWatch, obj client.Object,
				_ ...client.DeleteOption) error {
				writes = append(writes, "delete "+obj.GetName())
				return nil
			},
			SubResourceUpdate: func(_ context.Context, _ client.Client, _ string, obj client.Object,
				_ ...client.SubResourceUpdateOption) error {
				writes = append(writes, "status-update "+obj.GetName())
				return nil
			},
			SubResourcePatch: func(_ context.Context, _ client.Client, _ string, obj client.Object,
				_ client.Patch, _ ...client.SubResourcePatchOption) error {
				writes = append(writes, "status-patch "+obj.GetName())
				return nil
			},
		}).Build()

	conn := &Connector{Client: c, Log: logf.Log.WithName("test"), Clusters: newFakeRegistry()}
	if _, err := conn.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: "member-a"}}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	if len(writes) != 0 {
		t.Errorf("the connector wrote to the control plane: %v", writes)
	}

	// And it claims nothing: a finalizer here would hold a registry entry open
	// against a deletion this plane has no say in.
	var wc v1beta1.WorkloadCluster
	if err := c.Get(context.Background(), types.NamespacedName{Name: "member-a"}, &wc); err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(wc.Finalizers) != 0 {
		t.Errorf("the connector added finalizers: %v", wc.Finalizers)
	}
}

// What must be retried and what must not. A credential this plane cannot read
// is its own misconfiguration and clears when someone fixes it, so the pass
// fails and backs off. A source it structurally cannot use will never resolve,
// so retrying it forever would be noise.
func TestConnectorRetryBehaviour(t *testing.T) {
	profileOnly := &v1beta1.WorkloadCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "member-a"},
		Spec: v1beta1.WorkloadClusterSpec{
			ClusterSource: v1beta1.ClusterConnectionSource{
				ClusterProfileRef: &v1beta1.ClusterProfileRef{Name: "profile-a"},
			},
		},
	}
	profileOnly.Status.Conditions = []metav1.Condition{{
		Type: v1beta1.WorkloadClusterReady, Status: metav1.ConditionTrue,
		Reason: "Probed", LastTransitionTime: metav1.Now(),
	}}

	tests := []struct {
		name       string
		objects    []client.Object
		connectErr error
		wantErr    bool
		// wantDropped is set when the pass should also release any client held.
		wantDropped bool
	}{
		{
			name:    "a missing Secret is retried",
			objects: []client.Object{workloadCluster("member-a", true, "gone", "kubeconfig")},
			wantErr: true,
		},
		{
			name: "a Secret missing the key is retried",
			objects: []client.Object{
				workloadCluster("member-a", true, "creds", "kubeconfig"),
				kubeconfigSecret("creds", "wrong-key", "KUBECONFIG-A"),
			},
			wantErr: true,
		},
		{
			name: "a transport that refuses the credential is retried",
			objects: []client.Object{
				workloadCluster("member-a", true, "creds", "kubeconfig"),
				kubeconfigSecret("creds", "kubeconfig", "KUBECONFIG-A"),
			},
			connectErr: errors.New("invalid kubeconfig"),
			wantErr:    true,
		},
		{
			// Nothing about a clusterProfileRef will change on the next pass, so
			// failing would be an endless backoff over a supported-but-unbuilt
			// feature rather than a fault anyone can act on.
			name:        "a source this plane cannot resolve is not retried",
			objects:     []client.Object{profileOnly},
			wantDropped: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := connectScheme(t)
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(tc.objects...).Build()
			reg := newFakeRegistry()
			reg.connectErr = tc.connectErr
			conn := &Connector{Client: c, Log: logf.Log.WithName("test"), Clusters: reg}

			_, err := conn.Reconcile(context.Background(),
				ctrl.Request{NamespacedName: types.NamespacedName{Name: "member-a"}})
			if (err != nil) != tc.wantErr {
				t.Fatalf("Reconcile() error = %v, want error: %v", err, tc.wantErr)
			}
			if tc.wantDropped && len(reg.disconnect) == 0 {
				t.Error("an unusable source left a client connected")
			}
			if len(reg.connected) != 0 {
				t.Errorf("a failed pass reported a connection: %v", reg.connected)
			}
		})
	}
}

// A read failure that is not NotFound must not be mistaken for deregistration:
// dropping every client whenever the apiserver hiccups would take the whole
// fleet offline for the duration of a blip.
func TestConnectorDoesNotDropOnAReadFailure(t *testing.T) {
	s := connectScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
				return apierrors.NewServiceUnavailable("apiserver down")
			},
		}).Build()
	reg := newFakeRegistry()
	conn := &Connector{Client: c, Log: logf.Log.WithName("test"), Clusters: reg}

	if _, err := conn.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: "member-a"}}); err == nil {
		t.Fatal("Reconcile() = nil, want the read failure surfaced")
	}
	if len(reg.disconnect) != 0 {
		t.Errorf("an unreadable registry dropped clients: %v", reg.disconnect)
	}
}

// The projector holds every proportional split while a registered member is
// missing from the basis, and nothing else tells it the fleet is whole again:
// its own watches see AcceleratorQuota edits and a resync tick, neither of
// which fires when a member reconnects. So the transport has to say so, or a
// recovered fleet waits out the resync interval before a correct split appears.
//
// Only a MOVE in the reachable set counts. Connect is content-keyed and called
// on every pass, so signalling whenever it runs would wake the whole fleet on
// every member's health probe.
func TestConnectorWakesTheProjectorOnlyWhenReachabilityMoves(t *testing.T) {
	tests := []struct {
		name     string
		existing []client.Object
		already  map[string]string
		req      string
		wantWake bool
	}{
		{
			name:     "a member becoming reachable wakes it",
			existing: []client.Object{workloadCluster("member-a", true, "kc-a", "kubeconfig"), kubeconfigSecret("kc-a", "kubeconfig", "body")},
			req:      "member-a",
			wantWake: true,
		},
		{
			// The steady state: the registry re-reconciles on every health
			// probe and Connect is idempotent. Waking here would re-run the
			// fleet once a minute per member for no news.
			name:     "an already-connected member does not",
			existing: []client.Object{workloadCluster("member-a", true, "kc-a", "kubeconfig"), kubeconfigSecret("kc-a", "kubeconfig", "body")},
			already:  map[string]string{"member-a": "kubeconfig"},
			req:      "member-a",
			wantWake: false,
		},
		{
			name:     "a member going unready wakes it",
			existing: []client.Object{workloadCluster("member-a", false, "kc-a", "kubeconfig")},
			already:  map[string]string{"member-a": "kubeconfig"},
			req:      "member-a",
			wantWake: true,
		},
		{
			name:     "a deregistered member wakes it",
			already:  map[string]string{"member-a": "kubeconfig"},
			req:      "member-a",
			wantWake: true,
		},
		{
			// Already gone, so nothing moved.
			name:     "a member that was never connected going away does not",
			req:      "member-a",
			wantWake: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := newFakeRegistry()
			for name, kc := range tc.already {
				reg.connected[name] = kc
			}
			// Buffered past the one signal a pass can send, so a spurious second
			// one shows up as a count rather than a block.
			ch := make(chan event.GenericEvent, 4)

			c := &Connector{
				Client: fake.NewClientBuilder().
					WithScheme(connectScheme(t)).
					WithObjects(tc.existing...).Build(),
				Log:      logf.Log.WithName("test"),
				Clusters: reg,
				Changed:  ch,
				Root:     "root",
			}
			if _, err := c.Reconcile(context.Background(),
				ctrl.Request{NamespacedName: types.NamespacedName{Name: tc.req}}); err != nil {
				t.Fatalf("Reconcile() = %v", err)
			}

			switch got := len(ch); {
			case tc.wantWake && got != 1:
				t.Errorf("reachability moved but the projector was signalled %d times, want 1", got)
			case !tc.wantWake && got != 0:
				t.Errorf("nothing moved but the projector was signalled %d times, want 0", got)
			}
			if tc.wantWake {
				evt := <-ch
				if evt.Object.GetName() != "root" {
					t.Errorf("signal named %q, want the root %q -- the projector keys its pass on it",
						evt.Object.GetName(), "root")
				}
			}
		})
	}
}

// A Connector with nowhere to signal must still work: workload mode wires no
// channel, and neither do most tests.
func TestConnectorWithoutAChannel(t *testing.T) {
	reg := newFakeRegistry()
	c := &Connector{
		Client: fake.NewClientBuilder().
			WithScheme(connectScheme(t)).
			WithObjects(workloadCluster("member-a", true, "kc-a", "kubeconfig"), kubeconfigSecret("kc-a", "kubeconfig", "body")).Build(),
		Log:      logf.Log.WithName("test"),
		Clusters: reg,
	}
	if _, err := c.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: "member-a"}}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}
	if _, ok := reg.connected["member-a"]; !ok {
		t.Error("the member was not connected")
	}
}
