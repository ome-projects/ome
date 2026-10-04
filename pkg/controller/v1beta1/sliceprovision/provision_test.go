package sliceprovision

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/tpuslice"
	"sigs.k8s.io/ome/pkg/tpuslice/gke"
)

var testOwner = Owner{Kind: "Replica", Namespace: "ns", Name: "svc-engine", UID: "uid-a"}

// newFakeClient assigns UIDs on create, as the API server does, and selects
// pods by node, as the API server's field selector does.
func newFakeClient(objs ...client.Object) client.WithWatch {
	return fake.NewClientBuilder().WithObjects(objs...).WithIndex(&corev1.Pod{}, podNodeNameField, func(o client.Object) []string {
		return []string{o.(*corev1.Pod).Spec.NodeName}
	}).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if obj.GetUID() == "" {
				obj.SetUID(uuid.NewUUID())
			}
			return c.Create(ctx, obj, opts...)
		},
	}).Build()
}

func newProvisioner(t *testing.T, c client.Client, owner Owner) *Provisioner {
	t.Helper()
	p, err := New(testConfig(), c, c, c, owner)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

func demand(t *testing.T, topology string) Demand {
	t.Helper()
	return Demand{Shape: tpuslice.Shape{Accelerator: "tpu-a", Topology: mustTopology(t, topology)}, SliceType: "type-a"}
}

// stored returns the slot's slice as the API server would hold it after the
// owner created it for d.
func stored(t *testing.T, p *Provisioner, d Demand, slot Slot, mutate ...func(*unstructured.Unstructured)) *unstructured.Unstructured {
	t.Helper()
	u, err := gke.Build(p.spec(d, slot))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	u.SetUID(uuid.NewUUID())
	for _, m := range mutate {
		m(u)
	}
	return u
}

func withState(reason, message string) func(*unstructured.Unstructured) {
	return func(u *unstructured.Unstructured) {
		u.Object["status"] = map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "True", "reason": reason, "message": message},
			},
		}
	}
}

func withPartitions(ids ...interface{}) func(*unstructured.Unstructured) {
	return func(u *unstructured.Unstructured) {
		u.Object["spec"].(map[string]interface{})["partitionIds"] = ids
	}
}

func withName(name string) func(*unstructured.Unstructured) {
	return func(u *unstructured.Unstructured) { u.SetName(name) }
}

func withLabel(key, value string) func(*unstructured.Unstructured) {
	return func(u *unstructured.Unstructured) {
		l := u.GetLabels()
		if value == "" {
			delete(l, key)
		} else {
			l[key] = value
		}
		u.SetLabels(l)
	}
}

func withFinalizer(u *unstructured.Unstructured) { u.SetFinalizers([]string{"example.com/hold"}) }

// deleting marks u as being deleted. A zero time would read back as not
// deleted.
func deleting(u *unstructured.Unstructured) {
	now := metav1.Now()
	u.SetDeletionTimestamp(&now)
}

func getSlice(t *testing.T, c client.Client, name string) (*unstructured.Unstructured, bool) {
	t.Helper()
	u := gke.NewObject()
	if err := c.Get(context.Background(), client.ObjectKey{Name: name}, u); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false
		}
		t.Fatalf("get slice %s: %v", name, err)
	}
	return u, true
}

func TestSlotFor(t *testing.T) {
	single := workload.InstancePlan{Index: 3, Runners: []workload.RunnerPlan{{Name: workload.RunnerDefault, Size: 1}}}
	gang := workload.InstancePlan{Index: 4, Runners: []workload.RunnerPlan{
		{Name: workload.RunnerLeader, Size: 1}, {Name: workload.RunnerWorker, Size: 1},
	}}
	if got := SlotFor(single, 1); got != (Slot{Instance: 3, Ordinal: 1}) {
		t.Fatalf("SlotFor(single, 1) = %+v, want its own slot per ordinal", got)
	}
	for _, ordinal := range []int32{0, 1} {
		if got := SlotFor(gang, ordinal); got != (Slot{Instance: 4}) {
			t.Fatalf("SlotFor(gang, %d) = %+v, want one slot for the whole gang", ordinal, got)
		}
	}
}

func TestOwnerOf(t *testing.T) {
	tests := []struct {
		annotation string
		want       types.NamespacedName
		ok         bool
	}{
		{annotation: "ns/svc-engine", want: types.NamespacedName{Namespace: "ns", Name: "svc-engine"}, ok: true},
		{annotation: "", ok: false},
		{annotation: "svc-engine", ok: false},
		{annotation: "/svc-engine", ok: false},
		{annotation: "ns/", ok: false},
	}
	for _, tt := range tests {
		u := gke.NewObject()
		if tt.annotation != "" {
			u.SetAnnotations(map[string]string{AnnotationOwner: tt.annotation})
		}
		got, ok := OwnerOf(u)
		if ok != tt.ok || got != tt.want {
			t.Fatalf("OwnerOf(%q) = %v, %v; want %v, %v", tt.annotation, got, ok, tt.want, tt.ok)
		}
	}
}

func TestCheckConfigRejectsReservedKeys(t *testing.T) {
	if err := CheckConfig(nil); err == nil {
		t.Fatal("CheckConfig(nil) = nil, want an error")
	}
	tests := []struct {
		name   string
		mutate func(*controllerconfig.TPUSliceProvisioningConfig)
		want   string
	}{
		{name: "owner kind label", mutate: func(c *controllerconfig.TPUSliceProvisioningConfig) { c.Slice.OwnerKindLabel = LabelOwnerUID }, want: LabelOwnerUID},
		{name: "owner name label", mutate: func(c *controllerconfig.TPUSliceProvisioningConfig) { c.Slice.OwnerNameLabel = query.LabelInstanceIdx }, want: query.LabelInstanceIdx},
		{name: "annotation", mutate: func(c *controllerconfig.TPUSliceProvisioningConfig) { c.Slice.Annotations[AnnotationOwner] = "x" }, want: AnnotationOwner},
		{name: "pod annotation", mutate: func(c *controllerconfig.TPUSliceProvisioningConfig) {
			c.Slice.PodAnnotations = []string{AnnotationOwner}
		}, want: AnnotationOwner},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			tt.mutate(cfg)
			err := CheckConfig(cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("CheckConfig error = %v, want it to name %s", err, tt.want)
			}
			if _, err := New(cfg, newFakeClient(), newFakeClient(), newFakeClient(), testOwner); err == nil {
				t.Fatal("New accepted a config that overwrites controller keys")
			}
		})
	}
}

func TestNewRejectsOwner(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Owner)
	}{
		{name: "empty name", mutate: func(o *Owner) { o.Name = "" }},
		{name: "invalid name", mutate: func(o *Owner) { o.Name = "Svc_Engine" }},
		{name: "empty namespace", mutate: func(o *Owner) { o.Namespace = "" }},
		{name: "empty kind", mutate: func(o *Owner) { o.Kind = "" }},
		{name: "empty UID", mutate: func(o *Owner) { o.UID = "" }},
		{name: "UID not a label value", mutate: func(o *Owner) { o.UID = "a/b" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner := testOwner
			tt.mutate(&owner)
			if _, err := New(testConfig(), newFakeClient(), newFakeClient(), newFakeClient(), owner); err == nil {
				t.Fatalf("New accepted owner %+v", owner)
			}
		})
	}
}

func TestName(t *testing.T) {
	c := newFakeClient()
	p := newProvisioner(t, c, testOwner)
	name := p.Name(Slot{Instance: 3, Ordinal: 1})
	if !strings.HasPrefix(name, "svc-engine-3-1-") || len(name) != len("svc-engine-3-1-")+hashLength {
		t.Fatalf("Name = %q, want svc-engine-3-1-<hash>", name)
	}
	if again := newProvisioner(t, c, testOwner).Name(Slot{Instance: 3, Ordinal: 1}); again != name {
		t.Fatalf("Name is not stable: %q then %q", name, again)
	}
	if other := p.Name(Slot{Instance: 3}); other == name {
		t.Fatalf("slots (3,1) and (3,0) share name %q", name)
	}
	for _, owner := range []Owner{
		{Kind: testOwner.Kind, Namespace: "other", Name: testOwner.Name, UID: testOwner.UID},
		{Kind: testOwner.Kind, Namespace: testOwner.Namespace, Name: testOwner.Name, UID: "uid-b"},
	} {
		if other := newProvisioner(t, c, owner).Name(Slot{Instance: 3, Ordinal: 1}); other == name {
			t.Fatalf("owner %+v shares slice name %q with %+v", owner, name, testOwner)
		}
	}
}

func TestNameFitsASliceName(t *testing.T) {
	long := strings.Repeat("a.b-", 62) + "c"
	// Each is cut just after its hyphen for one of the slots below.
	cutAtHyphen := []string{strings.Repeat("a", 35) + "-b", strings.Repeat("a", 26) + "-b"}
	for _, ownerName := range append(cutAtHyphen, long, "a") {
		owner := testOwner
		owner.Name = ownerName
		p := newProvisioner(t, newFakeClient(), owner)
		for _, slot := range []Slot{{}, {Instance: 2147483647, Ordinal: 1}} {
			name := p.Name(slot)
			if errs := validation.IsDNS1123Label(name); len(errs) > 0 {
				t.Fatalf("Name(%+v) for owner %q = %q: %v", slot, ownerName, name, errs)
			}
			if len(name) > gke.MaxNameLength {
				t.Fatalf("Name(%+v) for owner %q = %q is %d characters, over %d", slot, ownerName, name, len(name), gke.MaxNameLength)
			}
			if strings.Contains(name, "--") {
				t.Fatalf("Name(%+v) for owner %q = %q keeps a trailing hyphen of the cut owner name", slot, ownerName, name)
			}
		}
	}
}

func TestNameKeepsAnOwnerNameThatFits(t *testing.T) {
	// 36 characters leave room for exactly the suffix of a single-digit slot.
	for _, ownerName := range []string{testOwner.Name, strings.Repeat("a", 36)} {
		owner := testOwner
		owner.Name = ownerName
		p := newProvisioner(t, newFakeClient(), owner)
		if got, want := p.Name(Slot{Instance: 4}), ownerName+"-4-0-"+p.hash; got != want {
			t.Fatalf("Name for owner %q = %q, want %q", ownerName, got, want)
		}
	}
}

func TestEnsureCreatesSlice(t *testing.T) {
	c := newFakeClient()
	cfg := testConfig()
	p, err := New(cfg, c, c, c, testOwner)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	slot := Slot{Instance: 2, Ordinal: 1}
	pl, err := p.Ensure(context.Background(), demand(t, "2x2x1"), slot)
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if pl.Ready || pl.NodeSelector != nil || !strings.Contains(pl.Reason, "waiting for partition assignment") {
		t.Fatalf("Ensure placement = %+v, want a new slice to hold its pods", pl)
	}
	u, found := getSlice(t, c, p.Name(slot))
	if !found {
		t.Fatalf("Ensure did not create slice %s", p.Name(slot))
	}
	wantLabels := map[string]string{
		LabelOwnerUID:            "uid-a",
		query.LabelManagedBy:     query.ManagedByOMENative,
		query.LabelInstanceIdx:   "2",
		query.LabelPodOrdinal:    "1",
		"example.com/owner-kind": "Replica",
		"example.com/owner-name": "svc-engine",
	}
	if diff := cmp.Diff(wantLabels, u.GetLabels()); diff != "" {
		t.Fatalf("slice labels mismatch (-want +got):\n%s", diff)
	}
	wantAnnotations := map[string]string{"example.com/managed-by": "scheduler", AnnotationOwner: "ns/svc-engine"}
	if diff := cmp.Diff(wantAnnotations, u.GetAnnotations()); diff != "" {
		t.Fatalf("slice annotations mismatch (-want +got):\n%s", diff)
	}
	s, err := gke.Parse(u)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if s.Type != "type-a" || s.Topology != "2x2x1" {
		t.Fatalf("slice is %s %s, want type-a 2x2x1", s.Type, s.Topology)
	}
	if owner, ok := OwnerOf(u); !ok || owner != (types.NamespacedName{Namespace: "ns", Name: "svc-engine"}) {
		t.Fatalf("OwnerOf(created slice) = %v, %v", owner, ok)
	}
	if diff := cmp.Diff(testConfig().Slice.Annotations, cfg.Slice.Annotations); diff != "" {
		t.Fatalf("Ensure mutated the configured annotations (-want +got):\n%s", diff)
	}
}

// TestEnsureCopiesPodAnnotations pins that a created slice carries the pod
// template annotations whose keys the configuration lists, and only those.
func TestEnsureCopiesPodAnnotations(t *testing.T) {
	c := newFakeClient()
	cfg := testConfig()
	cfg.Slice.PodAnnotations = []string{"example.com/priority", "example.com/tolerance"}
	p, err := New(cfg, c, c, c, testOwner)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	pod := map[string]string{"example.com/priority": "7", "example.com/unlisted": "x", AnnotationOwner: "other/owner"}
	p.SetPodAnnotations(pod)
	if _, err := p.Ensure(context.Background(), demand(t, "2x2x1"), Slot{}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	u, found := getSlice(t, c, p.Name(Slot{}))
	if !found {
		t.Fatalf("Ensure did not create slice %s", p.Name(Slot{}))
	}
	want := map[string]string{"example.com/managed-by": "scheduler", "example.com/priority": "7", AnnotationOwner: "ns/svc-engine"}
	if diff := cmp.Diff(want, u.GetAnnotations()); diff != "" {
		t.Fatalf("slice annotations mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(testConfig().Slice.Annotations, cfg.Slice.Annotations); diff != "" {
		t.Fatalf("Ensure mutated the configured annotations (-want +got):\n%s", diff)
	}
	if pod["example.com/priority"] != "7" || len(pod) != 3 {
		t.Fatalf("SetPodAnnotations mutated the pod annotations: %v", pod)
	}
}

func TestEnsureTruncatesOwnerNameLabel(t *testing.T) {
	owner := testOwner
	owner.Name = strings.Repeat("a", 62) + ".b"
	c := newFakeClient()
	p := newProvisioner(t, c, owner)
	if _, err := p.Ensure(context.Background(), demand(t, "2x2x1"), Slot{}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	u, _ := getSlice(t, c, p.Name(Slot{}))
	got := u.GetLabels()["example.com/owner-name"]
	if got != strings.Repeat("a", 62) {
		t.Fatalf("owner name label = %q, want the name cut to a label value", got)
	}
}

func TestEnsureObservesExistingSlice(t *testing.T) {
	d := demand(t, "2x2x1")
	slot := Slot{Instance: 1}
	tests := []struct {
		name       string
		mutate     []func(*unstructured.Unstructured)
		demand     Demand
		wantReady  bool
		wantReason string
	}{
		{name: "active", mutate: []func(*unstructured.Unstructured){withState("ACTIVE", "")}, demand: d, wantReady: true},
		{name: "degraded but bindable", mutate: []func(*unstructured.Unstructured){withState("ACTIVE_DEGRADED", "one link down")}, demand: d, wantReady: true},
		{name: "not yet active", mutate: []func(*unstructured.Unstructured){withState("PROVISIONING", "waiting for capacity")}, demand: d, wantReason: "is PROVISIONING: waiting for capacity"},
		{name: "state without message", mutate: []func(*unstructured.Unstructured){withState("FAILED", "")}, demand: d, wantReason: "is FAILED"},
		{name: "not yet placed", demand: d, wantReason: "is waiting for partition assignment"},
		{name: "placed without state", mutate: []func(*unstructured.Unstructured){withPartitions("p-1")}, demand: d, wantReason: "has partitions assigned and no state yet"},
		{name: "state after partitions are cleared", mutate: []func(*unstructured.Unstructured){withState("UNHEALTHY", "evicted")}, demand: d, wantReason: "is UNHEALTHY: evicted"},
		{name: "other shape", mutate: []func(*unstructured.Unstructured){withState("ACTIVE", "")}, demand: demand(t, "2x2x2"), wantReason: "is type-a 2x2x1, want type-a 2x2x2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newFakeClient()
			p := newProvisioner(t, c, testOwner)
			existing := stored(t, p, d, slot, tt.mutate...)
			if err := c.Create(context.Background(), existing); err != nil {
				t.Fatalf("seed: %v", err)
			}
			before, _ := getSlice(t, c, existing.GetName())
			pl, err := p.Ensure(context.Background(), tt.demand, slot)
			if err != nil {
				t.Fatalf("Ensure: %v", err)
			}
			if pl.Ready != tt.wantReady || pl.Slice.UID != existing.GetUID() {
				t.Fatalf("Ensure placement = %+v, want ready %v on the existing slice", pl, tt.wantReady)
			}
			if tt.wantReady {
				if diff := cmp.Diff(map[string]string{keySlice: existing.GetName()}, pl.NodeSelector); diff != "" || pl.Reason != "" {
					t.Fatalf("ready placement node selector mismatch (-want +got):\n%s reason %q", diff, pl.Reason)
				}
			} else if pl.NodeSelector != nil || !strings.Contains(pl.Reason, tt.wantReason) {
				t.Fatalf("Ensure placement = %+v, want reason containing %q and no node selector", pl, tt.wantReason)
			}
			after, _ := getSlice(t, c, existing.GetName())
			if diff := cmp.Diff(before, after); diff != "" {
				t.Fatalf("Ensure modified an existing slice (-before +after):\n%s", diff)
			}
		})
	}
}

func TestEnsureWaitsForTerminatingSlice(t *testing.T) {
	c := newFakeClient()
	p := newProvisioner(t, c, testOwner)
	d := demand(t, "2x2x1")
	existing := stored(t, p, d, Slot{}, withState("ACTIVE", ""), withFinalizer)
	if err := c.Create(context.Background(), existing); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := c.Delete(context.Background(), existing); err != nil {
		t.Fatalf("delete: %v", err)
	}
	pl, err := p.Ensure(context.Background(), d, Slot{})
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if pl.Ready || !strings.Contains(pl.Reason, "being released") {
		t.Fatalf("Ensure placement = %+v, want pods held until the old slice is gone", pl)
	}
}

func TestEnsureRefusesForeignSlice(t *testing.T) {
	d := demand(t, "2x2x1")
	for name, mutate := range map[string]func(*unstructured.Unstructured){
		"another owner": withLabel(LabelOwnerUID, "uid-b"),
		"no owner":      withLabel(LabelOwnerUID, ""),
	} {
		t.Run(name, func(t *testing.T) {
			c := newFakeClient()
			p := newProvisioner(t, c, testOwner)
			foreign := stored(t, p, d, Slot{}, withState("ACTIVE", ""), mutate)
			if err := c.Create(context.Background(), foreign); err != nil {
				t.Fatalf("seed: %v", err)
			}
			pl, err := p.Ensure(context.Background(), d, Slot{})
			if !errors.Is(err, gke.ErrOwnershipConflict) || pl.Ready {
				t.Fatalf("Ensure = %+v, %v; want ErrOwnershipConflict", pl, err)
			}
		})
	}
}

func TestEnsureReadError(t *testing.T) {
	boom := errors.New("boom")
	c := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return boom
		},
	}).Build()
	p := newProvisioner(t, c, testOwner)
	if _, err := p.Ensure(context.Background(), demand(t, "2x2x1"), Slot{}); !errors.Is(err, boom) {
		t.Fatalf("Ensure error = %v, want the read error", err)
	}
}

func TestEnsureReadsLive(t *testing.T) {
	c := newFakeClient()
	live := newFakeClient()
	p, err := New(testConfig(), c, live, c, testOwner)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	d := demand(t, "2x2x1")
	if err := live.Create(context.Background(), stored(t, p, d, Slot{}, withState("ACTIVE", ""))); err != nil {
		t.Fatalf("seed: %v", err)
	}
	pl, err := p.Ensure(context.Background(), d, Slot{})
	if err != nil || !pl.Ready {
		t.Fatalf("Ensure = %+v, %v; want the live read's ready slice", pl, err)
	}
}

func TestEnsureCountsOnlySlicesItCreates(t *testing.T) {
	d := demand(t, "2x2x1")
	existing := stored(t, newProvisioner(t, newFakeClient(), testOwner), d, Slot{})
	// createdAfterRead reports the slice missing to Ensure's read, as when it
	// is created between that read and Ensure's create.
	createdAfterRead := func(t *testing.T) client.Client {
		gets := 0
		t.Cleanup(func() {
			if gets < 2 {
				t.Errorf("Ensure read the slice %d times, want the read before and after its create", gets)
			}
		})
		return fake.NewClientBuilder().WithObjects(existing.DeepCopy()).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if gets++; gets == 1 {
					return apierrors.NewNotFound(gke.GroupVersion.WithResource("slices").GroupResource(), key.Name)
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).Build()
	}
	for _, tc := range []struct {
		name   string
		client func(*testing.T) client.Client
		want   float64
	}{
		{name: "no slice", client: func(*testing.T) client.Client { return newFakeClient() }, want: 1},
		{name: "slice exists", client: func(*testing.T) client.Client { return newFakeClient(existing.DeepCopy()) }},
		{name: "slice created after the read", client: createdAfterRead},
	} {
		t.Run(tc.name, func(t *testing.T) {
			created := slicesCreated.WithLabelValues("type-a", "2x2x1")
			before := testutil.ToFloat64(created)
			if _, err := newProvisioner(t, tc.client(t), testOwner).Ensure(context.Background(), d, Slot{}); err != nil {
				t.Fatalf("Ensure: %v", err)
			}
			if got := testutil.ToFloat64(created) - before; got != tc.want {
				t.Fatalf("created slices counted = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEnsureCountsCreateFailures(t *testing.T) {
	d := demand(t, "2x2x1")
	gr := gke.GroupVersion.WithResource("slices").GroupResource()
	p := newProvisioner(t, newFakeClient(), testOwner)
	failingCreate := func(err error) func(*testing.T) client.Client {
		return func(*testing.T) client.Client {
			return fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
				Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
					return err
				},
			}).Build()
		}
	}
	// appearsAfterRead reports existing missing to Ensure's read, as when it
	// is created between that read and Ensure's create.
	appearsAfterRead := func(existing *unstructured.Unstructured) func(*testing.T) client.Client {
		return func(*testing.T) client.Client {
			gets := 0
			return fake.NewClientBuilder().WithObjects(existing.DeepCopy()).WithInterceptorFuncs(interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if gets++; gets == 1 {
						return apierrors.NewNotFound(gr, key.Name)
					}
					return c.Get(ctx, key, obj, opts...)
				},
			}).Build()
		}
	}
	for _, tc := range []struct {
		name   string
		client func(*testing.T) client.Client
		want   string // the reason counted, empty when Ensure succeeds
	}{
		{name: "created", client: func(*testing.T) client.Client { return newFakeClient() }},
		{name: "own slice created after the read", client: appearsAfterRead(stored(t, p, d, Slot{}))},
		{name: "denied", client: failingCreate(apierrors.NewForbidden(gr, "s", errors.New("denied by webhook"))), want: "Forbidden"},
		{name: "invalid", client: failingCreate(apierrors.NewInvalid(gke.GroupVersion.WithKind(gke.Kind).GroupKind(), "s", nil)), want: "Invalid"},
		{name: "webhook unreachable", client: failingCreate(apierrors.NewInternalError(errors.New("connection refused"))), want: "InternalError"},
		{name: "network error", client: failingCreate(errors.New("connection reset by peer")), want: "Unknown"},
		{name: "another owner's slice created after the read", client: appearsAfterRead(stored(t, p, d, Slot{}, withLabel(LabelOwnerUID, "uid-b"))), want: "OwnershipConflict"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sliceCreateFailures.Reset()
			_, err := newProvisioner(t, tc.client(t), testOwner).Ensure(context.Background(), d, Slot{})
			if gotErr, wantErr := err != nil, tc.want != ""; gotErr != wantErr {
				t.Fatalf("Ensure error = %v, want an error: %t", err, wantErr)
			}
			wantSeries := 0
			if tc.want != "" {
				wantSeries = 1
				if got := testutil.ToFloat64(sliceCreateFailures.WithLabelValues("type-a", "2x2x1", tc.want)); got != 1 {
					t.Fatalf("create failures{type-a,2x2x1,%s} = %v, want 1", tc.want, got)
				}
			}
			if got := testutil.CollectAndCount(sliceCreateFailures); got != wantSeries {
				t.Fatalf("create failure series = %d, want %d", got, wantSeries)
			}
		})
	}
}

func TestObserve(t *testing.T) {
	d := demand(t, "2x2x1")
	slot := Slot{Instance: 1}
	tests := []struct {
		name       string
		seed       []func(*unstructured.Unstructured)
		absent     bool
		demand     Demand
		wantReady  bool
		wantReason string
		wantErr    error
	}{
		{name: "not created", absent: true, demand: d, wantReason: "not created yet"},
		{name: "active", seed: []func(*unstructured.Unstructured){withState("ACTIVE", "")}, demand: d, wantReady: true},
		{name: "not yet active", seed: []func(*unstructured.Unstructured){withState("PROVISIONING", "")}, demand: d, wantReason: "is PROVISIONING"},
		{name: "other shape", seed: []func(*unstructured.Unstructured){withState("ACTIVE", "")}, demand: demand(t, "2x2x2"), wantReason: "want type-a 2x2x2"},
		{name: "foreign", seed: []func(*unstructured.Unstructured){withLabel(LabelOwnerUID, "uid-b")}, demand: d, wantErr: gke.ErrOwnershipConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newFakeClient()
			p := newProvisioner(t, c, testOwner)
			if !tt.absent {
				if err := c.Create(context.Background(), stored(t, p, d, slot, tt.seed...)); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}
			pl, err := p.Observe(context.Background(), tt.demand, slot)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) || pl.Ready {
					t.Fatalf("Observe = %+v, %v; want %v", pl, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Observe: %v", err)
			}
			if pl.Ready != tt.wantReady || !strings.Contains(pl.Reason, tt.wantReason) {
				t.Fatalf("Observe placement = %+v, want ready %v with reason containing %q", pl, tt.wantReady, tt.wantReason)
			}
			if tt.wantReady {
				if diff := cmp.Diff(map[string]string{keySlice: p.Name(slot)}, pl.NodeSelector); diff != "" {
					t.Fatalf("ready placement node selector mismatch (-want +got):\n%s", diff)
				}
			}
			if _, found := getSlice(t, c, p.Name(slot)); found == tt.absent {
				t.Fatalf("slice present = %v after Observe, want %v: Observe never creates", found, !tt.absent)
			}
		})
	}
}

func TestObserveReadsThroughCache(t *testing.T) {
	cached := newFakeClient()
	live := newFakeClient()
	p, err := New(testConfig(), cached, live, live, testOwner)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	d := demand(t, "2x2x1")
	if err := live.Create(context.Background(), stored(t, p, d, Slot{}, withState("ACTIVE", ""))); err != nil {
		t.Fatalf("seed: %v", err)
	}
	pl, err := p.Observe(context.Background(), d, Slot{})
	if err != nil || pl.Ready {
		t.Fatalf("Observe = %+v, %v; want the cached read, which has no slice", pl, err)
	}
	boom := errors.New("boom")
	failing := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return boom
		},
	}).Build()
	if p, err = New(testConfig(), failing, live, live, testOwner); err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := p.Observe(context.Background(), d, Slot{}); !errors.Is(err, boom) {
		t.Fatalf("Observe error = %v, want the read error", err)
	}
}

func pod(name string, nodeSelector map[string]string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec:       corev1.PodSpec{NodeSelector: nodeSelector},
		Status:     corev1.PodStatus{Phase: phase},
	}
}

func TestPinned(t *testing.T) {
	p := newProvisioner(t, newFakeClient(), testOwner)
	terminating := pod("terminating", map[string]string{keySlice: "s-terminating"}, corev1.PodRunning)
	terminating.DeletionTimestamp = &metav1.Time{}
	got := p.Pinned([]*corev1.Pod{
		pod("running", map[string]string{keySlice: "s-running"}, corev1.PodRunning),
		pod("pending", map[string]string{keySlice: "s-pending"}, corev1.PodPending),
		terminating,
		pod("failed", map[string]string{keySlice: "s-failed"}, corev1.PodFailed),
		pod("succeeded", map[string]string{keySlice: "s-succeeded"}, corev1.PodSucceeded),
		pod("unpinned", map[string]string{keyTopology: "2x2x1"}, corev1.PodRunning),
		nil,
	})
	want := map[string]struct{}{"s-running": {}, "s-pending": {}, "s-terminating": {}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Pinned mismatch (-want +got):\n%s", diff)
	}
}

func TestHeld(t *testing.T) {
	p := newProvisioner(t, newFakeClient(), testOwner)
	terminating := pod("terminating", map[string]string{keySlice: "s-terminating"}, corev1.PodRunning)
	terminating.DeletionTimestamp = &metav1.Time{}
	got := p.Held([]*corev1.Pod{
		pod("running", map[string]string{keySlice: "s-running"}, corev1.PodRunning),
		pod("pending", map[string]string{keySlice: "s-pending"}, corev1.PodPending),
		terminating,
		pod("failed", map[string]string{keySlice: "s-failed"}, corev1.PodFailed),
		pod("succeeded", map[string]string{keySlice: "s-succeeded"}, corev1.PodSucceeded),
		pod("unpinned", map[string]string{keyTopology: "2x2x1"}, corev1.PodRunning),
		nil,
	})
	want := map[string]struct{}{"s-running": {}, "s-pending": {}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Held mismatch (-want +got):\n%s", diff)
	}
}

// confined is a running pod, created at created, of a 2x2x1 slice it is
// confined to.
func confined(name, slice string, created time.Time) *corev1.Pod {
	p := pod(name, map[string]string{keyAccelerator: "tpu-a", keyTopology: "2x2x1", keySlice: slice}, corev1.PodRunning)
	p.UID = types.UID("uid-" + name)
	p.CreationTimestamp = metav1.NewTime(created)
	return p
}

func withCreated(at time.Time) func(*unstructured.Unstructured) {
	return func(u *unstructured.Unstructured) { u.SetCreationTimestamp(metav1.NewTime(at)) }
}

// boundTo binds pod to node, as the scheduler does.
func boundTo(pod *corev1.Pod, node string) *corev1.Pod {
	pod.Spec.NodeName = node
	return pod
}

// lostPods lists the lost slices, in order, each with its pods' names.
func lostPods(lost []LostSlice) [][]string {
	got := [][]string{}
	for _, l := range lost {
		names := []string{l.Name}
		for _, p := range l.Pods {
			names = append(names, p.Name)
		}
		got = append(got, names)
	}
	return got
}

func TestLost(t *testing.T) {
	d := demand(t, "2x2x1")
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	earlier, later := created.Add(-time.Minute), created.Add(time.Minute)
	build := newProvisioner(t, newFakeClient(), testOwner)
	other := newProvisioner(t, newFakeClient(), Owner{Kind: "Replica", Namespace: "ns", Name: "svc-engine", UID: "uid-b"})
	a, b := build.Name(Slot{Instance: 0}), build.Name(Slot{Instance: 1})
	deletingPod := confined("deleting", a, created)
	deletingPod.DeletionTimestamp = &metav1.Time{Time: later}
	failedPod := confined("failed", a, created)
	failedPod.Status.Phase = corev1.PodFailed

	for _, tc := range []struct {
		name   string
		slices []*unstructured.Unstructured
		nodes  []client.Object
		pods   []*corev1.Pod
		want   [][]string
		// wantWhy is how each lost slice was lost, checked when set.
		wantWhy []string
	}{
		{
			name:   "slice intact",
			slices: []*unstructured.Unstructured{stored(t, build, d, Slot{}, withCreated(earlier))},
			pods:   []*corev1.Pod{confined("p", a, created)},
			want:   [][]string{},
		},
		{
			name:    "slice gone",
			pods:    []*corev1.Pod{confined("leader", a, created), confined("worker", a, created)},
			want:    [][]string{{a, "leader", "worker"}},
			wantWhy: []string{"was deleted"},
		},
		{
			name:   "slice being deleted",
			slices: []*unstructured.Unstructured{stored(t, build, d, Slot{}, withCreated(earlier), withFinalizer, deleting)},
			pods:   []*corev1.Pod{confined("p", a, created)},
			want:   [][]string{{a, "p"}},
		},
		{
			name:    "slice newer than a pod",
			slices:  []*unstructured.Unstructured{stored(t, build, d, Slot{}, withCreated(created))},
			pods:    []*corev1.Pod{confined("old", a, earlier), confined("same-second", a, created), confined("new", a, later)},
			want:    [][]string{{a, "old"}},
			wantWhy: []string{"was deleted"},
		},
		{
			name: "pods being deleted or done",
			pods: []*corev1.Pod{deletingPod, failedPod, nil},
			want: [][]string{},
		},
		{
			name: "slice the owner does not name",
			pods: []*corev1.Pod{confined("static", "static-slice", created), confined("foreign", other.Name(Slot{}), created)},
			want: [][]string{},
		},
		{
			name:   "another owner's slice at the owner's name",
			slices: []*unstructured.Unstructured{stored(t, other, d, Slot{}, withName(a), withCreated(later))},
			pods:   []*corev1.Pod{boundTo(confined("p", a, created), "host-a")},
			want:   [][]string{},
		},
		{
			name:   "each slice by name",
			slices: []*unstructured.Unstructured{stored(t, build, d, Slot{Instance: 2}, withCreated(earlier))},
			pods: []*corev1.Pod{
				confined("on-b", b, created), confined("on-a", a, created), confined("intact", build.Name(Slot{Instance: 2}), created),
			},
			want: [][]string{{a, "on-a"}, {b, "on-b"}},
		},
		{
			name:   "slice holds the nodes of its pods",
			slices: []*unstructured.Unstructured{stored(t, build, d, Slot{}, withCreated(earlier), withPartitions("p-1"))},
			nodes:  []client.Object{node("host-a", map[string]string{keySlice: a}), node("host-b", map[string]string{keySlice: a})},
			pods:   []*corev1.Pod{boundTo(confined("leader", a, created), "host-a"), boundTo(confined("worker", a, created), "host-b")},
			want:   [][]string{},
		},
		{
			name:   "pods not yet bound follow the slice",
			slices: []*unstructured.Unstructured{stored(t, build, d, Slot{}, withCreated(earlier))},
			pods:   []*corev1.Pod{confined("pending", a, created)},
			want:   [][]string{},
		},
		{
			name:    "slice without partitions",
			slices:  []*unstructured.Unstructured{stored(t, build, d, Slot{}, withCreated(earlier))},
			pods:    []*corev1.Pod{boundTo(confined("leader", a, created), "host-a"), confined("pending", a, created)},
			want:    [][]string{{a, "leader"}},
			wantWhy: []string{"lost its partitions"},
		},
		{
			name:   "slice moved off one of its pods' nodes",
			slices: []*unstructured.Unstructured{stored(t, build, d, Slot{}, withCreated(earlier), withPartitions("p-2"))},
			nodes:  []client.Object{node("host-a", map[string]string{keySlice: a}), node("host-b", map[string]string{keySlice: b})},
			pods: []*corev1.Pod{
				boundTo(confined("leader", a, created), "host-a"), boundTo(confined("worker", a, created), "host-b"), confined("pending", a, created),
			},
			want:    [][]string{{a, "leader", "worker"}},
			wantWhy: []string{"moved off node host-b"},
		},
		{
			name:    "slice moved off an unlabeled node and a node that is gone",
			slices:  []*unstructured.Unstructured{stored(t, build, d, Slot{}, withCreated(earlier), withPartitions("p-2"))},
			nodes:   []client.Object{node("host-a", nil)},
			pods:    []*corev1.Pod{boundTo(confined("worker", a, created), "host-b"), boundTo(confined("leader", a, created), "host-a")},
			want:    [][]string{{a, "worker", "leader"}},
			wantWhy: []string{"moved off nodes host-a, host-b"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objs := append([]client.Object(nil), tc.nodes...)
			for _, s := range tc.slices {
				objs = append(objs, s)
			}
			p := newProvisioner(t, newFakeClient(objs...), testOwner)
			lost, err := p.Lost(context.Background(), tc.pods)
			if err != nil {
				t.Fatalf("Lost: %v", err)
			}
			if diff := cmp.Diff(tc.want, lostPods(lost)); diff != "" {
				t.Fatalf("lost slices (-want +got):\n%s", diff)
			}
			if tc.wantWhy == nil {
				return
			}
			whys := make([]string, 0, len(lost))
			for _, l := range lost {
				whys = append(whys, l.Why)
			}
			if diff := cmp.Diff(tc.wantWhy, whys); diff != "" {
				t.Fatalf("how the slices were lost (-want +got):\n%s", diff)
			}
		})
	}
}

// TestLostConfirmsLive pins the reads: a cached read that finds the slice
// intact is trusted, and any other is confirmed live.
func TestLostConfirmsLive(t *testing.T) {
	d := demand(t, "2x2x1")
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	build := newProvisioner(t, newFakeClient(), testOwner)
	pods := []*corev1.Pod{confined("p", build.Name(Slot{}), created)}
	intact := func() client.Object { return stored(t, build, d, Slot{}, withCreated(created.Add(-time.Minute))) }
	boom := errors.New("boom")
	failing := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return boom
		},
	}).Build()

	for _, tc := range []struct {
		name         string
		cached, live client.Client
		wantLost     bool
		wantErr      error
	}{
		{name: "cache lags the live slice", cached: newFakeClient(), live: newFakeClient(intact())},
		{name: "gone from both", cached: newFakeClient(), live: newFakeClient(), wantLost: true},
		{name: "intact in the cache", cached: newFakeClient(intact()), live: failing},
		{name: "cached read fails", cached: failing, live: newFakeClient(), wantErr: boom},
		{name: "live read fails", cached: newFakeClient(), live: failing, wantErr: boom},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(testConfig(), tc.cached, tc.live, tc.live, testOwner)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			lost, err := p.Lost(context.Background(), pods)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Lost error = %v, want %v", err, tc.wantErr)
			}
			if got := len(lost) > 0; got != tc.wantLost {
				t.Fatalf("Lost = %v, want lost: %v", lostPods(lost), tc.wantLost)
			}
		})
	}
}

// TestLostConfirmsAMoveLive pins the reads behind a slice moved off its pods'
// nodes: a cached read that finds the slice holding them is trusted, and any
// other is confirmed live.
func TestLostConfirmsAMoveLive(t *testing.T) {
	d := demand(t, "2x2x1")
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	build := newProvisioner(t, newFakeClient(), testOwner)
	name := build.Name(Slot{})
	pods := []*corev1.Pod{boundTo(confined("p", name, created), "host-a")}
	slice := func(partitions ...interface{}) client.Object {
		mutate := []func(*unstructured.Unstructured){withCreated(created.Add(-time.Minute))}
		if len(partitions) > 0 {
			mutate = append(mutate, withPartitions(partitions...))
		}
		return stored(t, build, d, Slot{}, mutate...)
	}
	held := func() client.Object { return node("host-a", map[string]string{keySlice: name}) }
	moved := func() client.Object { return node("host-a", map[string]string{keySlice: "elsewhere"}) }
	boom := errors.New("boom")
	failingNodes := func(objs ...client.Object) client.WithWatch {
		return interceptor.NewClient(newFakeClient(objs...), interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Node); ok {
					return boom
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})
	}

	for _, tc := range []struct {
		name         string
		cached, live client.WithWatch
		wantLost     bool
		wantErr      error
	}{
		{name: "cache lags the live partitions", cached: newFakeClient(slice(), held()), live: newFakeClient(slice("p-1"), held())},
		{name: "no partitions in both", cached: newFakeClient(slice(), held()), live: newFakeClient(slice(), held()), wantLost: true},
		{name: "cache lags the live node", cached: newFakeClient(slice("p-1"), moved()), live: newFakeClient(slice("p-1"), held())},
		{name: "moved in both", cached: newFakeClient(slice("p-1"), moved()), live: newFakeClient(slice("p-1"), moved()), wantLost: true},
		{name: "node held in the cache", cached: newFakeClient(slice("p-1"), held()), live: failingNodes(slice("p-1"))},
		{name: "cached node read fails", cached: failingNodes(slice("p-1")), live: newFakeClient(slice("p-1"), held()), wantErr: boom},
		{name: "live node read fails", cached: newFakeClient(slice("p-1"), moved()), live: failingNodes(slice("p-1")), wantErr: boom},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(testConfig(), tc.cached, tc.live, tc.live, testOwner)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			lost, err := p.Lost(context.Background(), pods)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Lost error = %v, want %v", err, tc.wantErr)
			}
			if got := len(lost) > 0; got != tc.wantLost {
				t.Fatalf("Lost = %v, want lost: %v", lostPods(lost), tc.wantLost)
			}
		})
	}
}

func TestNames(t *testing.T) {
	for _, ownerName := range []string{testOwner.Name, "svc.engine", strings.Repeat("a.b-", 62) + "c", strings.Repeat("a", 35) + "-b", strings.Repeat("a", 26) + "-b"} {
		owner := testOwner
		owner.Name = ownerName
		p := newProvisioner(t, newFakeClient(), owner)
		for _, slot := range []Slot{{}, {Instance: 3, Ordinal: 1}, {Instance: 2147483647, Ordinal: 1}} {
			if name := p.Name(slot); !p.names(name) {
				t.Errorf("owner %q: names(%q) = false, want the name of slot %+v", ownerName, name, slot)
			}
		}
	}
	p := newProvisioner(t, newFakeClient(), testOwner)
	other := newProvisioner(t, newFakeClient(), Owner{Kind: "Replica", Namespace: "ns", Name: "svc-engine", UID: "uid-b"})
	for _, name := range []string{
		"",
		"static-slice",
		other.Name(Slot{}),
		"svc-decode-0-0-" + p.hash,
		"svc-engine-00-0-" + p.hash,
		"svc-engine-0-" + p.hash,
		"svc-engine-0--1-" + p.hash,
		"svc-engine-x-0-" + p.hash,
		"svc-engine-2147483648-0-" + p.hash,
		"0-0-" + p.hash,
		p.Name(Slot{}) + "x",
	} {
		if p.names(name) {
			t.Errorf("names(%q) = true, want it not the owner's", name)
		}
	}
}

// pinnedSet is a pinned-slice source that counts its calls.
func pinnedSet(calls *int, names ...string) func(context.Context) (map[string]struct{}, error) {
	return func(context.Context) (map[string]struct{}, error) {
		*calls++
		set := map[string]struct{}{}
		for _, name := range names {
			set[name] = struct{}{}
		}
		return set, nil
	}
}

func parse(t *testing.T, u *unstructured.Unstructured) gke.Slice {
	t.Helper()
	s, err := gke.Parse(u)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return s
}

func TestFits(t *testing.T) {
	p := newProvisioner(t, newFakeClient(), testOwner)
	d := demand(t, "2x2x1")
	slot := Slot{Instance: 1}
	if !p.Fits(parse(t, stored(t, p, d, slot)), d, slot) {
		t.Fatal("Fits rejected the slice Ensure creates for the slot")
	}
	if p.Fits(parse(t, stored(t, p, d, slot)), demand(t, "2x2x2"), slot) {
		t.Fatal("Fits accepted a slice of another shape")
	}
	if p.Fits(parse(t, stored(t, p, d, slot)), d, Slot{Instance: 1, Ordinal: 1}) {
		t.Fatal("Fits accepted another slot's slice")
	}
	if p.Fits(parse(t, stored(t, p, d, slot, withName("stray"))), d, slot) {
		t.Fatal("Fits accepted a slice not at the slot's name")
	}
}

func TestHolds(t *testing.T) {
	c := newFakeClient()
	p := newProvisioner(t, c, testOwner)
	other := newProvisioner(t, c, Owner{Kind: "Replica", Namespace: "ns", Name: "svc-engine", UID: "uid-b"})
	d := demand(t, "2x2x1")
	if held, err := p.Holds(context.Background()); err != nil || held {
		t.Fatalf("Holds = %v, %v; want none held", held, err)
	}
	if err := c.Create(context.Background(), stored(t, other, d, Slot{})); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if held, err := p.Holds(context.Background()); err != nil || held {
		t.Fatalf("Holds = %v, %v; want another owner's slice not counted", held, err)
	}
	if err := c.Create(context.Background(), stored(t, p, d, Slot{Instance: 2})); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if held, err := p.Holds(context.Background()); err != nil || !held {
		t.Fatalf("Holds = %v, %v; want the owner's slice counted", held, err)
	}
}

func TestSweep(t *testing.T) {
	c := newFakeClient()
	p := newProvisioner(t, c, testOwner)
	other := newProvisioner(t, c, Owner{Kind: "Replica", Namespace: "ns", Name: "svc-engine", UID: "uid-b"})
	d := demand(t, "2x2x1")
	wide := demand(t, "2x2x2")
	wanted := map[Slot]bool{{Instance: 0}: true, {Instance: 2}: true, {Instance: 3}: true, {Instance: 5}: true}
	keep := func(slot Slot, s gke.Slice) bool { return wanted[slot] && p.Fits(s, d, slot) }

	seed := map[string]*unstructured.Unstructured{
		"wanted":               stored(t, p, d, Slot{Instance: 0}),
		"unwanted but pinned":  stored(t, p, d, Slot{Instance: 0, Ordinal: 1}),
		"unwanted":             stored(t, p, d, Slot{Instance: 1}),
		"wanted, other shape":  stored(t, p, wide, Slot{Instance: 2}),
		"other shape, pinned":  stored(t, p, wide, Slot{Instance: 3}),
		"no slot labels":       stored(t, p, d, Slot{Instance: 4}, withLabel(query.LabelPodOrdinal, "")),
		"not at the slot name": stored(t, p, d, Slot{Instance: 5}, withName("stray")),
		"terminating":          stored(t, p, d, Slot{Instance: 6}, withFinalizer),
		"another owner":        stored(t, other, d, Slot{Instance: 7}),
	}
	for _, u := range seed {
		if err := c.Create(context.Background(), u); err != nil {
			t.Fatalf("seed %s: %v", u.GetName(), err)
		}
	}
	if err := c.Delete(context.Background(), seed["terminating"]); err != nil {
		t.Fatalf("delete: %v", err)
	}

	var calls int
	pinned := pinnedSet(&calls, seed["unwanted but pinned"].GetName(), seed["other shape, pinned"].GetName())
	if err := p.Sweep(context.Background(), keep, pinned); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if calls != 1 {
		t.Fatalf("Sweep read pins %d times, want once", calls)
	}

	for key, want := range map[string]bool{
		"wanted":               true,
		"unwanted but pinned":  true,
		"unwanted":             false,
		"wanted, other shape":  false,
		"other shape, pinned":  true,
		"no slot labels":       false,
		"not at the slot name": false,
		"terminating":          true,
		"another owner":        true,
	} {
		if _, found := getSlice(t, c, seed[key].GetName()); found != want {
			t.Errorf("slice %q (%s) present = %v, want %v", key, seed[key].GetName(), found, want)
		}
	}
}

func TestSweepWithoutKeepReleasesAllUnpinned(t *testing.T) {
	c := newFakeClient()
	p := newProvisioner(t, c, testOwner)
	d := demand(t, "2x2x1")
	held := stored(t, p, d, Slot{Instance: 0})
	drop := stored(t, p, d, Slot{Instance: 1})
	for _, u := range []*unstructured.Unstructured{held, drop} {
		if err := c.Create(context.Background(), u); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	var calls int
	if err := p.Sweep(context.Background(), nil, pinnedSet(&calls, held.GetName())); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if _, found := getSlice(t, c, held.GetName()); !found {
		t.Fatal("Sweep released a pinned slice")
	}
	if _, found := getSlice(t, c, drop.GetName()); found {
		t.Fatal("Sweep kept an unpinned slice nothing claims")
	}
}

func TestSweepReadsPinsOnlyToRelease(t *testing.T) {
	c := newFakeClient()
	p := newProvisioner(t, c, testOwner)
	d := demand(t, "2x2x1")
	terminating := stored(t, p, d, Slot{Instance: 1}, withFinalizer)
	for _, u := range []*unstructured.Unstructured{stored(t, p, d, Slot{Instance: 0}), terminating} {
		if err := c.Create(context.Background(), u); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if err := c.Delete(context.Background(), terminating); err != nil {
		t.Fatalf("delete: %v", err)
	}
	all := func(Slot, gke.Slice) bool { return true }
	var calls int
	if err := p.Sweep(context.Background(), all, pinnedSet(&calls)); err != nil || calls != 0 {
		t.Fatalf("Sweep = %v after %d pin reads; want no read with nothing to release", err, calls)
	}
	if err := p.Sweep(context.Background(), all, nil); err != nil {
		t.Fatalf("Sweep with nothing to release and no pin source: %v", err)
	}
}

func TestSweepReleasesNothingWithoutPins(t *testing.T) {
	c := newFakeClient()
	p := newProvisioner(t, c, testOwner)
	u := stored(t, p, demand(t, "2x2x1"), Slot{Instance: 1})
	if err := c.Create(context.Background(), u); err != nil {
		t.Fatalf("seed: %v", err)
	}
	boom := errors.New("boom")
	for name, pinned := range map[string]func(context.Context) (map[string]struct{}, error){
		"no source":  nil,
		"read error": func(context.Context) (map[string]struct{}, error) { return nil, boom },
	} {
		err := p.Sweep(context.Background(), nil, pinned)
		if err == nil || (pinned != nil && !errors.Is(err, boom)) {
			t.Fatalf("Sweep (%s) error = %v, want it reported", name, err)
		}
		if _, found := getSlice(t, c, u.GetName()); !found {
			t.Fatalf("Sweep (%s) released a slice without knowing its pins", name)
		}
	}
}

func TestSweepReadsThroughCache(t *testing.T) {
	cached := newFakeClient()
	w := newFakeClient()
	p, err := New(testConfig(), cached, w, w, testOwner)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	d := demand(t, "2x2x1")
	seen := stored(t, p, d, Slot{Instance: 1})
	unseen := stored(t, p, d, Slot{Instance: 2})
	for _, u := range []*unstructured.Unstructured{seen, unseen} {
		if err := w.Create(context.Background(), u.DeepCopy()); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if err := cached.Create(context.Background(), seen.DeepCopy()); err != nil {
		t.Fatalf("seed cache: %v", err)
	}
	var calls int
	if err := p.Sweep(context.Background(), nil, pinnedSet(&calls)); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if _, found := getSlice(t, w, seen.GetName()); found {
		t.Fatal("Sweep kept an unpinned slice the cache shows")
	}
	if _, found := getSlice(t, w, unseen.GetName()); !found {
		t.Fatal("Sweep released a slice the cache does not show")
	}
}

func TestSweepReportsReleaseErrors(t *testing.T) {
	boom := errors.New("boom")
	c := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error { return boom },
	}).Build()
	p := newProvisioner(t, c, testOwner)
	if err := c.Create(context.Background(), stored(t, p, demand(t, "2x2x1"), Slot{Instance: 1})); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var calls int
	if err := p.Sweep(context.Background(), nil, pinnedSet(&calls)); !errors.Is(err, boom) {
		t.Fatalf("Sweep error = %v, want the delete error", err)
	}
}

func TestReleaseInstance(t *testing.T) {
	c := newFakeClient()
	p := newProvisioner(t, c, testOwner)
	other := newProvisioner(t, c, Owner{Kind: "Replica", Namespace: "ns", Name: "svc-engine", UID: "uid-b"})
	d := demand(t, "2x2x1")
	current := stored(t, p, d, Slot{Instance: 1})
	surge := stored(t, p, d, Slot{Instance: 1, Ordinal: 1}, withFinalizer)
	sibling := stored(t, p, d, Slot{Instance: 2})
	foreign := stored(t, other, d, Slot{Instance: 1})
	for _, u := range []*unstructured.Unstructured{current, surge, sibling, foreign} {
		if err := c.Create(context.Background(), u); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	complete, err := p.ReleaseInstance(context.Background(), 1)
	if err != nil || complete {
		t.Fatalf("ReleaseInstance = %v, %v; want an accepted release not yet complete", complete, err)
	}
	if _, found := getSlice(t, c, current.GetName()); found {
		t.Fatal("ReleaseInstance kept the instance's slice")
	}
	held, found := getSlice(t, c, surge.GetName())
	if !found || held.GetDeletionTimestamp() == nil {
		t.Fatal("ReleaseInstance did not release the instance's other slot")
	}
	for _, u := range []*unstructured.Unstructured{sibling, foreign} {
		if _, found := getSlice(t, c, u.GetName()); !found {
			t.Fatalf("ReleaseInstance released %s, which is not the instance's", u.GetName())
		}
	}

	if complete, err := p.ReleaseInstance(context.Background(), 1); err != nil || complete {
		t.Fatalf("ReleaseInstance = %v, %v; want incomplete while a slice is held", complete, err)
	}
	held.SetFinalizers(nil)
	if err := c.Update(context.Background(), held); err != nil {
		t.Fatalf("drop finalizer: %v", err)
	}
	if complete, err := p.ReleaseInstance(context.Background(), 1); err != nil || !complete {
		t.Fatalf("ReleaseInstance = %v, %v; want complete once none is left", complete, err)
	}
}

func TestReleaseAll(t *testing.T) {
	c := newFakeClient()
	p := newProvisioner(t, c, testOwner)
	other := newProvisioner(t, c, Owner{Kind: "Replica", Namespace: "ns", Name: "svc-engine", UID: "uid-b"})
	d := demand(t, "2x2x1")
	mine := []*unstructured.Unstructured{stored(t, p, d, Slot{Instance: 0}), stored(t, p, d, Slot{Instance: 3, Ordinal: 1})}
	foreign := stored(t, other, d, Slot{Instance: 0})
	for _, u := range append(mine, foreign) {
		if err := c.Create(context.Background(), u); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if complete, err := p.ReleaseAll(context.Background()); err != nil || complete {
		t.Fatalf("ReleaseAll = %v, %v; want accepted releases not yet complete", complete, err)
	}
	for _, u := range mine {
		if _, found := getSlice(t, c, u.GetName()); found {
			t.Fatalf("ReleaseAll kept %s", u.GetName())
		}
	}
	if _, found := getSlice(t, c, foreign.GetName()); !found {
		t.Fatal("ReleaseAll released another owner's slice")
	}
	if complete, err := p.ReleaseAll(context.Background()); err != nil || !complete {
		t.Fatalf("ReleaseAll = %v, %v; want complete once none is left", complete, err)
	}
}

func TestReleaseReadsLiveAndReportsErrors(t *testing.T) {
	boom := errors.New("boom")
	c := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error { return boom },
	}).Build()
	live := newFakeClient()
	p, err := New(testConfig(), c, live, c, testOwner)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := live.Create(context.Background(), stored(t, p, demand(t, "2x2x1"), Slot{Instance: 1})); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if complete, err := p.ReleaseInstance(context.Background(), 1); !errors.Is(err, boom) || complete {
		t.Fatalf("ReleaseInstance = %v, %v; want the delete error for the slice the live read found", complete, err)
	}
	if complete, err := p.ReleaseAll(context.Background()); !errors.Is(err, boom) || complete {
		t.Fatalf("ReleaseAll = %v, %v; want the delete error", complete, err)
	}

	listErr := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error { return boom },
	}).Build()
	p, err = New(testConfig(), listErr, listErr, listErr, testOwner)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if complete, err := p.ReleaseAll(context.Background()); !errors.Is(err, boom) || complete {
		t.Fatalf("ReleaseAll = %v, %v; want the list error", complete, err)
	}
	if err := p.Sweep(context.Background(), nil, nil); !errors.Is(err, boom) {
		t.Fatalf("Sweep error = %v, want the list error", err)
	}
	if _, err := p.Holds(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("Holds error = %v, want the list error", err)
	}
}

// hostPod is a pod bound to node that requests chips, selecting the slices
// in selector.
func hostPod(name, node string, chips int64, phase corev1.PodPhase, selector map[string]string) *corev1.Pod {
	p := pod(name, selector, phase)
	p.Namespace = "other"
	p.Spec.NodeName = node
	p.Spec.Containers = podSpec("tpu-a", "2x2x1", chips).Containers
	return p
}

func sliceHost(name, slice string) *corev1.Node {
	return node(name, map[string]string{keySlice: slice})
}

// Releasing a slice deactivates its partition, so a slice is kept while a pod
// of another workload holds chips on its hosts, on every release path.
func TestReleaseKeepsSlicesOtherWorkloadsHold(t *testing.T) {
	d := demand(t, "2x2x1")
	for _, tt := range []struct {
		name string
		pods func(slice string) []client.Object
		kept bool
	}{
		{name: "another workload's pod holds chips", kept: true, pods: func(slice string) []client.Object {
			return []client.Object{hostPod("holder", "host-a", 4, corev1.PodRunning, nil)}
		}},
		{name: "a pending pod already bound to the host", kept: true, pods: func(slice string) []client.Object {
			return []client.Object{hostPod("holder", "host-a", 4, corev1.PodPending, nil)}
		}},
		{name: "a pod with no chips", pods: func(slice string) []client.Object {
			return []client.Object{hostPod("cpu-only", "host-a", 0, corev1.PodRunning, nil)}
		}},
		{name: "a finished pod", pods: func(slice string) []client.Object {
			return []client.Object{hostPod("done", "host-a", 4, corev1.PodSucceeded, nil)}
		}},
		{name: "the slice's own pod", pods: func(slice string) []client.Object {
			return []client.Object{hostPod("own", "host-a", 4, corev1.PodRunning, map[string]string{keySlice: slice})}
		}},
		{name: "a pod on another node", pods: func(slice string) []client.Object {
			return []client.Object{hostPod("elsewhere", "host-b", 4, corev1.PodRunning, nil)}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, path := range []string{"ReleaseInstance", "ReleaseAll", "Sweep"} {
				t.Run(path, func(t *testing.T) {
					p := newProvisioner(t, newFakeClient(), testOwner)
					u := stored(t, p, d, Slot{})
					objs := append([]client.Object{u, sliceHost("host-a", u.GetName()), sliceHost("host-b", "other-slice")}, tt.pods(u.GetName())...)
					c := newFakeClient(objs...)
					p = newProvisioner(t, c, testOwner)
					var told []string
					p.OnReleaseDeferred(func(s gke.Slice, holders []string) { told = append(told, holders...) })
					before := testutil.ToFloat64(sliceReleasesDeferred.WithLabelValues("type-a", "2x2x1"))

					ctx := context.Background()
					switch path {
					case "ReleaseInstance":
						complete, err := p.ReleaseInstance(ctx, 0)
						if err != nil || (tt.kept && complete) {
							t.Fatalf("ReleaseInstance = %v, %v; want an incomplete release while the slice is kept", complete, err)
						}
					case "ReleaseAll":
						complete, err := p.ReleaseAll(ctx)
						if err != nil || (tt.kept && complete) {
							t.Fatalf("ReleaseAll = %v, %v; want an incomplete release while the slice is kept", complete, err)
						}
					case "Sweep":
						var calls int
						if err := p.Sweep(ctx, nil, pinnedSet(&calls)); err != nil {
							t.Fatalf("Sweep: %v", err)
						}
					}
					if _, found := getSlice(t, c, u.GetName()); found != tt.kept {
						t.Fatalf("slice present = %v, want %v", found, tt.kept)
					}
					deferred := testutil.ToFloat64(sliceReleasesDeferred.WithLabelValues("type-a", "2x2x1")) - before
					if tt.kept && (deferred != 1 || len(told) != 1 || told[0] != "other/holder") {
						t.Fatalf("deferred %v times, told %v; want once, naming other/holder", deferred, told)
					}
					if !tt.kept && (deferred != 0 || len(told) != 0) {
						t.Fatalf("deferred %v times, told %v; want no deferral", deferred, told)
					}
				})
			}
		})
	}
}

// A slice is released once the other workload's pods are gone.
func TestReleaseCompletesOnceOtherWorkloadsLeave(t *testing.T) {
	d := demand(t, "2x2x1")
	p := newProvisioner(t, newFakeClient(), testOwner)
	u := stored(t, p, d, Slot{})
	holder := hostPod("holder", "host-a", 4, corev1.PodRunning, nil)
	c := newFakeClient(u, sliceHost("host-a", u.GetName()), holder)
	p = newProvisioner(t, c, testOwner)
	ctx := context.Background()
	if complete, err := p.ReleaseInstance(ctx, 0); err != nil || complete {
		t.Fatalf("ReleaseInstance = %v, %v; want the slice kept", complete, err)
	}
	if err := c.Delete(ctx, holder); err != nil {
		t.Fatalf("delete holder: %v", err)
	}
	if _, err := p.ReleaseInstance(ctx, 0); err != nil {
		t.Fatalf("ReleaseInstance: %v", err)
	}
	if complete, err := p.ReleaseInstance(ctx, 0); err != nil || !complete {
		t.Fatalf("ReleaseInstance = %v, %v; want the release complete once the holder left", complete, err)
	}
}

// A failed read of the hosts keeps the slice: releasing it blind could cut a
// workload off from its chips.
func TestReleaseKeepsSlicesWhenHostsCannotBeRead(t *testing.T) {
	boom := errors.New("boom")
	d := demand(t, "2x2x1")
	p := newProvisioner(t, newFakeClient(), testOwner)
	u := stored(t, p, d, Slot{})
	base := newFakeClient(u)
	c := interceptor.NewClient(base, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.NodeList); ok {
				return boom
			}
			return c.List(ctx, list, opts...)
		},
	})
	p = newProvisioner(t, c, testOwner)
	if _, err := p.ReleaseInstance(context.Background(), 0); !errors.Is(err, boom) {
		t.Fatalf("ReleaseInstance error = %v, want the node read's", err)
	}
	if _, found := getSlice(t, base, u.GetName()); !found {
		t.Fatal("slice released although its hosts could not be read")
	}
}
