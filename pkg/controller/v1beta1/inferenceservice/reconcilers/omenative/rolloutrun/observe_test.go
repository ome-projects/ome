package rolloutrun

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

// policyReadCounter counts the RolloutPolicy reads that reach a reader.
type policyReadCounter struct {
	client.Reader
	policyGets int
}

func (r *policyReadCounter) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1beta1.RolloutPolicy); ok {
		r.policyGets++
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func refCanaryISVC() *v1beta1.InferenceService {
	return isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		PolicyRef:  &v1beta1.RolloutPolicyRef{Name: "canary-std-v1", Progression: v1beta1.RolloutProgressionCanary},
	})
}

func fakeClient(t *testing.T, objects ...runtime.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()
}

func policyDigest(t *testing.T, policy *v1beta1.RolloutPolicy) string {
	t.Helper()
	d, err := rolloutpolicy.PortableDigest(&policy.Spec)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// laggingCache is a cache still serving the stale body of a policy whose
// live object already carries the new one.
func laggingCache(t *testing.T, isvc *v1beta1.InferenceService) (Inputs, *v1beta1.RolloutPolicy, *v1beta1.RolloutPolicy) {
	t.Helper()
	stale := &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "canary-std-v1", Namespace: "ns", Generation: 1},
		Spec:       v1beta1.RolloutPolicySpec{Canary: canaryBody(50, 100)},
	}
	live := stale.DeepCopy()
	live.Generation = 2
	live.Spec.Canary = canaryBody(25, 50, 100)
	in := Inputs{
		Client:         fakeClient(t, isvc, stale),
		Reader:         fakeClient(t, irFixture(oldRev, newRev), live),
		ISVC:           isvc,
		Now:            time.Unix(1000, 0),
		FeatureEnabled: true,
	}
	return in, stale, live
}

func TestOpenPinsTheLivePolicyBodyWhenTheCacheLags(t *testing.T) {
	isvc := refCanaryISVC()
	in, stale, live := laggingCache(t, isvc)

	out, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Opened || out.Parked {
		t.Fatalf("the run must open, got %+v", out)
	}
	pinned := isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
	if pinned.PortableDigest != policyDigest(t, live) || pinned.PortableDigest == policyDigest(t, stale) {
		t.Fatalf("pinned digest %s, want the live body's %s (stale %s)", pinned.PortableDigest, policyDigest(t, live), policyDigest(t, stale))
	}
	if pinned.PolicyGeneration != live.Generation || len(pinned.Group.Canary.Steps) != 3 {
		t.Fatalf("pinned provenance and body must be the live policy's, got %+v", pinned)
	}
	// The set handed back is the cached reading, and the executors still see
	// the live body on this pass because the view reads the pin.
	if out.Policies.ByName[stale.Name].Generation != stale.Generation {
		t.Fatalf("the per-pass set is the cached reading, got %+v", out.Policies.ByName[stale.Name])
	}
	if g := rollout.CanaryGroupFor(isvc, out.Policies, v1beta1.EngineComponent); g == nil || len(g.Canary.Steps) != 3 {
		t.Fatalf("the view must read the pin while the run is open, got %+v", g)
	}
}

func TestRepinPinsTheLivePolicyBodyWhenTheCacheLags(t *testing.T) {
	isvc := refCanaryISVC()
	in, stale, live := laggingCache(t, isvc)
	// Open from the stale body: the live reader serves it too at first.
	liveReader := in.Reader
	in.Reader = fakeClient(t, irFixture(oldRev, newRev), stale)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if got := isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest; got != policyDigest(t, stale) {
		t.Fatalf("precondition: the run pins the stale body, got %s", got)
	}

	in.Reader = liveReader
	isvc.Annotations = map[string]string{constants.RolloutRepinAnnotation: "now"}
	out, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !out.StateChanged {
		t.Fatalf("the repin must replace the plan, got %+v", out)
	}
	if got := isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest; got != policyDigest(t, live) {
		t.Fatalf("repinned digest %s, want the live body's %s", got, policyDigest(t, live))
	}
}

func TestPassesThatPinNothingReadNoLivePolicy(t *testing.T) {
	policy := &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "canary-std-v1", Namespace: "ns", Generation: 1},
		Spec:       v1beta1.RolloutPolicySpec{Canary: canaryBody(50, 100)},
	}
	t.Run("the open reads the policy live once", func(t *testing.T) {
		isvc := refCanaryISVC()
		in := testInputs(t, isvc, irFixture(oldRev, newRev), policy.DeepCopy())
		counter := &policyReadCounter{Reader: in.Reader}
		in.Reader = counter
		if out, err := Reconcile(context.Background(), in); err != nil || !out.Opened {
			t.Fatalf("open: %v %+v", err, out)
		}
		if counter.policyGets != 1 {
			t.Fatalf("the open reads the pinned policy live once, got %d reads", counter.policyGets)
		}
	})
	t.Run("an open run that moved nothing", func(t *testing.T) {
		isvc := refCanaryISVC()
		in := testInputs(t, isvc, irFixture(oldRev, newRev), policy.DeepCopy())
		if _, err := Reconcile(context.Background(), in); err != nil {
			t.Fatal(err)
		}
		counter := &policyReadCounter{Reader: in.Reader}
		in.Reader = counter
		if _, err := Reconcile(context.Background(), in); err != nil {
			t.Fatal(err)
		}
		if counter.policyGets != 0 {
			t.Fatalf("a pass that pins nothing must not read policies live, got %d reads", counter.policyGets)
		}
		if got := isvc.Status.Rollout.Groups[0].ObservedDigest; got != policyDigest(t, policy) {
			t.Fatalf("the resolution view is served from the cache, got %q", got)
		}
	})
	t.Run("no run and nothing diverged", func(t *testing.T) {
		isvc := refCanaryISVC()
		in := testInputs(t, isvc, irFixture(oldRev, oldRev), policy.DeepCopy())
		counter := &policyReadCounter{Reader: in.Reader}
		in.Reader = counter
		out, err := Reconcile(context.Background(), in)
		if err != nil || out.Opened || v1beta1.RolloutRunActive(isvc) {
			t.Fatalf("settled: %v %+v", err, out)
		}
		if counter.policyGets != 0 {
			t.Fatalf("a settled pass must not read policies live, got %d reads", counter.policyGets)
		}
		// The executors' view still resolves the reference, from the cache.
		if g := rollout.CanaryGroupFor(isvc, out.Policies, v1beta1.EngineComponent); g == nil || len(g.Canary.Steps) != 2 {
			t.Fatalf("the view must resolve the reference through the cached set, got %+v", g)
		}
	})
}
