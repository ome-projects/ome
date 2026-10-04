package sliceprovision

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// TestMetricNames pins the metrics' names, which dashboards and alerts key on.
func TestMetricNames(t *testing.T) {
	slicesCreated.WithLabelValues("type-a", "2x2x1")
	slicesReleased.WithLabelValues("type-a", "2x2x1")
	sliceCreateFailures.WithLabelValues("type-a", "2x2x1", "Forbidden")
	sliceProvisionSeconds.WithLabelValues("type-a", "2x2x1")
	slicesLost.WithLabelValues("type-a", "2x2x1")
	for _, name := range []string{
		"ome_tpu_slice_created_total",
		"ome_tpu_slice_released_total",
		"ome_tpu_slice_create_failures_total",
		"ome_tpu_slice_provision_duration_seconds",
		"ome_tpu_slice_lost_total",
	} {
		if got, err := testutil.GatherAndCount(ctrlmetrics.Registry, name); err != nil || got == 0 {
			t.Errorf("%s: %d series (err %v), want it registered", name, got, err)
		}
	}
}

func TestObserveDeleted(t *testing.T) {
	p := newProvisioner(t, newFakeClient(), testOwner)
	d := demand(t, "2x2x1")
	notSlice := &unstructured.Unstructured{}
	notSlice.SetGroupVersionKind(schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"})
	notSlice.SetLabels(map[string]string{LabelOwnerUID: "uid-a"})
	unparsable := stored(t, p, d, Slot{}, func(u *unstructured.Unstructured) {
		u.Object["spec"].(map[string]interface{})["topology"] = int64(2)
	})
	for _, tc := range []struct {
		name string
		obj  client.Object
		want bool
	}{
		{name: "provisioned slice", obj: stored(t, p, d, Slot{}), want: true},
		{name: "provisioned slice being released", obj: stored(t, p, d, Slot{}, withFinalizer, deleting), want: true},
		{name: "slice without the owner label", obj: stored(t, p, d, Slot{}, withLabel(LabelOwnerUID, ""))},
		{name: "unparsable slice", obj: unparsable},
		{name: "another kind", obj: notSlice},
		{name: "typed object", obj: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{LabelOwnerUID: "uid-a"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			slicesReleased.Reset()
			ObserveDeleted(tc.obj)
			wantSeries := 0
			if tc.want {
				wantSeries = 1
				if got := testutil.ToFloat64(slicesReleased.WithLabelValues("type-a", "2x2x1")); got != 1 {
					t.Fatalf("released{type-a,2x2x1} = %v, want 1", got)
				}
			}
			if got := testutil.CollectAndCount(slicesReleased); got != wantSeries {
				t.Fatalf("released series = %d, want %d", got, wantSeries)
			}
		})
	}
}

// fakeProvisionClock times provisioning on a fake clock until the test ends.
func fakeProvisionClock(t *testing.T) *clocktesting.FakeClock {
	t.Helper()
	c := clocktesting.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	provisionClock = c
	t.Cleanup(func() { provisionClock = clock.RealClock{} })
	return c
}

// provisionTimes is a series of the provisioning histogram.
type provisionTimes struct {
	Count uint64
	Sum   float64
}

// collectProvisionTimes gathers the provisioning histogram, keyed
// type/topology.
func collectProvisionTimes(t *testing.T) map[string]provisionTimes {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(sliceProvisionSeconds)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	got := map[string]provisionTimes{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			l := map[string]string{}
			for _, p := range m.GetLabel() {
				l[p.GetName()] = p.GetValue()
			}
			got[l["slice_type"]+"/"+l["topology"]] = provisionTimes{Count: m.GetHistogram().GetSampleCount(), Sum: m.GetHistogram().GetSampleSum()}
		}
	}
	return got
}

// createSlice has a provisioner on c create the slot's slice for d, and
// returns the slice as the API server stored it.
func createSlice(t *testing.T, c client.Client, d Demand, slot Slot) *unstructured.Unstructured {
	t.Helper()
	p := newProvisioner(t, c, testOwner)
	if _, err := p.Ensure(context.Background(), d, slot); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	u, found := getSlice(t, c, p.Name(slot))
	if !found {
		t.Fatalf("slice %s not created", p.Name(slot))
	}
	return u
}

func TestObserveReady(t *testing.T) {
	d := demand(t, "2x2x1")
	// timedOnce is the histogram after one slice took secs to be ready.
	timedOnce := func(secs float64) map[string]provisionTimes {
		return map[string]provisionTimes{"type-a/2x2x1": {Count: 1, Sum: secs}}
	}
	for _, tc := range []struct {
		name string
		// seen are the states the watch sees the created slice in, one
		// every 30s. Empty is no state yet.
		seen     []string
		deleting bool // the slice is being deleted when seen
		want     map[string]provisionTimes
	}{
		{name: "ready", seen: []string{"ACTIVE"}, want: timedOnce(30)},
		{name: "ready in another ready state", seen: []string{"ACTIVE_DEGRADED"}, want: timedOnce(30)},
		{name: "pending, then ready", seen: []string{"", "PROVISIONING", "ACTIVE"}, want: timedOnce(90)},
		{name: "ready again after a failed health probe", seen: []string{"ACTIVE", "SliceHealthProbeFailed", "ACTIVE"}, want: timedOnce(30)},
		{name: "never ready", seen: []string{"", "FAILED"}},
		{name: "ready while being deleted", seen: []string{"ACTIVE"}, deleting: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clk := fakeProvisionClock(t)
			sliceProvisionSeconds.Reset()
			created := createSlice(t, newFakeClient(), d, Slot{})
			for _, state := range tc.seen {
				clk.Step(30 * time.Second)
				u := created.DeepCopy()
				if state != "" {
					withState(state, "")(u)
				}
				if tc.deleting {
					withFinalizer(u)
					deleting(u)
				}
				ObserveReady(u, testConfig().Slice.ReadyStates)
			}
			want := tc.want
			if want == nil {
				want = map[string]provisionTimes{}
			}
			if diff := cmp.Diff(want, collectProvisionTimes(t)); diff != "" {
				t.Fatalf("provisioning histogram (-want +got):\n%s", diff)
			}
		})
	}
}

// TestObserveReadyTimesOnlySlicesItCreated pins that only the slices this
// process created are timed, and only until they are released.
func TestObserveReadyTimesOnlySlicesItCreated(t *testing.T) {
	d := demand(t, "2x2x1")
	// seenReady is u as the watch sees it once the provider reports it ready.
	seenReady := func(u *unstructured.Unstructured) *unstructured.Unstructured {
		u = u.DeepCopy()
		withState("ACTIVE", "")(u)
		return u
	}
	for _, tc := range []struct {
		name string
		seen func(t *testing.T) client.Object // what the watch sees
	}{
		{name: "slice that already existed", seen: func(t *testing.T) client.Object {
			existing := stored(t, newProvisioner(t, newFakeClient(), testOwner), d, Slot{})
			return seenReady(createSlice(t, newFakeClient(existing), d, Slot{}))
		}},
		{name: "slice released before it was ready", seen: func(t *testing.T) client.Object {
			created := createSlice(t, newFakeClient(), d, Slot{})
			ObserveDeleted(created)
			return seenReady(created)
		}},
		{name: "typed object", seen: func(t *testing.T) client.Object {
			created := createSlice(t, newFakeClient(), d, Slot{})
			return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: created.GetUID()}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sliceProvisionSeconds.Reset()
			ObserveReady(tc.seen(t), testConfig().Slice.ReadyStates)
			if got := collectProvisionTimes(t); len(got) != 0 {
				t.Fatalf("provisioning histogram = %v, want nothing timed", got)
			}
		})
	}
}

// collectCounts gathers counter c, keyed type/topology.
func collectCounts(t *testing.T, c *prometheus.CounterVec) map[string]float64 {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	got := map[string]float64{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			l := map[string]string{}
			for _, p := range m.GetLabel() {
				l[p.GetName()] = p.GetValue()
			}
			got[l["slice_type"]+"/"+l["topology"]] = m.GetCounter().GetValue()
		}
	}
	return got
}

// TestInitSeries pins that each configured slice type and topology has its
// created, released and provisioning series at zero from startup, and that a
// slice counts under them.
func TestInitSeries(t *testing.T) {
	clk := fakeProvisionClock(t)
	slicesCreated.Reset()
	slicesReleased.Reset()
	sliceCreateFailures.Reset()
	sliceProvisionSeconds.Reset()
	type series struct {
		Created, Released map[string]float64
		Provisioning      map[string]provisionTimes
	}
	collect := func() series {
		return series{collectCounts(t, slicesCreated), collectCounts(t, slicesReleased), collectProvisionTimes(t)}
	}

	InitSeries(testConfig())
	want := series{
		Created:      map[string]float64{"type-a/2x2x1": 0, "type-a/2x2x2": 0},
		Released:     map[string]float64{"type-a/2x2x1": 0, "type-a/2x2x2": 0},
		Provisioning: map[string]provisionTimes{"type-a/2x2x1": {}, "type-a/2x2x2": {}},
	}
	if diff := cmp.Diff(want, collect()); diff != "" {
		t.Fatalf("series at startup (-want +got):\n%s", diff)
	}
	if got := testutil.CollectAndCount(sliceCreateFailures); got != 0 {
		t.Fatalf("create failure series at startup = %d, want none", got)
	}

	// A 2x2x1 slice, ready after 30s and then released, counts under its
	// series, and adds none.
	created := createSlice(t, newFakeClient(), demand(t, "2x2x1"), Slot{})
	clk.Step(30 * time.Second)
	ready := created.DeepCopy()
	withState("ACTIVE", "")(ready)
	ObserveReady(ready, testConfig().Slice.ReadyStates)
	ObserveDeleted(ready)
	want.Created["type-a/2x2x1"] = 1
	want.Released["type-a/2x2x1"] = 1
	want.Provisioning["type-a/2x2x1"] = provisionTimes{Count: 1, Sum: 30}
	if diff := cmp.Diff(want, collect()); diff != "" {
		t.Fatalf("series after a 2x2x1 slice (-want +got):\n%s", diff)
	}
}

// TestRecovered pins that a lost slice counts once, under the slice type and
// topology its pods select, on a series that exists from startup.
func TestRecovered(t *testing.T) {
	slicesLost.Reset()
	InitSeries(testConfig())
	want := map[string]float64{"type-a/2x2x1": 0, "type-a/2x2x2": 0}
	if diff := cmp.Diff(want, collectCounts(t, slicesLost)); diff != "" {
		t.Fatalf("series at startup (-want +got):\n%s", diff)
	}

	p := newProvisioner(t, newFakeClient(), testOwner)
	name := p.Name(Slot{})
	p.Recovered(LostSlice{Name: name, Pods: []*corev1.Pod{confined("leader", name, time.Time{}), confined("worker", name, time.Time{})}})
	unselected := confined("unselected", name, time.Time{})
	delete(unselected.Spec.NodeSelector, keyTopology)
	unconfigured := confined("unconfigured", name, time.Time{})
	unconfigured.Spec.NodeSelector[keyAccelerator] = "tpu-z"
	for _, l := range []LostSlice{{Name: name}, {Name: name, Pods: []*corev1.Pod{unselected}}, {Name: name, Pods: []*corev1.Pod{unconfigured}}} {
		p.Recovered(l)
	}
	want["type-a/2x2x1"] = 1
	if diff := cmp.Diff(want, collectCounts(t, slicesLost)); diff != "" {
		t.Fatalf("series after a lost 2x2x1 slice (-want +got):\n%s", diff)
	}
}

// collectStates gathers the slice gauge, keyed type/topology/state.
func collectStates(t *testing.T) map[string]float64 {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(sliceStates)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	got := map[string]float64{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			l := map[string]string{}
			for _, p := range m.GetLabel() {
				l[p.GetName()] = p.GetValue()
			}
			got[l["slice_type"]+"/"+l["topology"]+"/"+l["state"]] = m.GetGauge().GetValue()
		}
	}
	return got
}

// stateSource reads the slices c holds for testOwner's kind. Only the owners
// in live exist.
func stateSource(c client.Reader, live ...types.UID) StateSource {
	return StateSource{
		Slices:    c,
		Config:    testConfig(),
		OwnerKind: testOwner.Kind,
		OwnerExists: func(_ context.Context, uid types.UID) (bool, error) {
			return slices.Contains(live, uid), nil
		},
	}
}

// report reports src on the slice gauge until the test ends.
func report(t *testing.T, src StateSource) {
	t.Cleanup(ReportStates(src))
}

func TestSliceStates(t *testing.T) {
	p := newProvisioner(t, newFakeClient(), testOwner)
	other := newProvisioner(t, newFakeClient(), Owner{Kind: "Other", Namespace: "ns", Name: "svc-engine", UID: "uid-b"})
	d := demand(t, "2x2x1")
	for _, tc := range []struct {
		name      string
		slice     *unstructured.Unstructured
		ownerGone bool
		want      string
	}{
		{name: "ready", slice: stored(t, p, d, Slot{}, withState("ACTIVE", "")), want: stateReady},
		{name: "ready in another ready state", slice: stored(t, p, d, Slot{}, withState("ACTIVE_DEGRADED", "")), want: stateReady},
		{name: "pending without a state", slice: stored(t, p, d, Slot{}), want: statePending},
		{name: "pending in a state that is not ready", slice: stored(t, p, d, Slot{}, withState("FAILED", "")), want: statePending},
		{name: "terminating", slice: stored(t, p, d, Slot{}, withState("ACTIVE", ""), withFinalizer, deleting), want: stateTerminating},
		{name: "terminating after its owner is gone", slice: stored(t, p, d, Slot{}, withState("ACTIVE", ""), withFinalizer, deleting), ownerGone: true, want: stateTerminating},
		{name: "orphaned", slice: stored(t, p, d, Slot{}, withState("ACTIVE", "")), ownerGone: true, want: stateOrphaned},
		{name: "slice without the owner label", slice: stored(t, p, d, Slot{}, withLabel(LabelOwnerUID, ""))},
		{name: "slice of another owner kind", slice: stored(t, other, d, Slot{})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var live []types.UID
			if !tc.ownerGone {
				live = append(live, testOwner.UID)
			}
			report(t, stateSource(newFakeClient(tc.slice), live...))
			want := map[string]float64{}
			if tc.want != "" {
				want["type-a/2x2x1/"+tc.want] = 1
			}
			if diff := cmp.Diff(want, collectStates(t)); diff != "" {
				t.Fatalf("slice gauge (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSliceStatesCountsEachSeriesAndLooksUpEachOwnerOnce(t *testing.T) {
	a := newProvisioner(t, newFakeClient(), testOwner)
	b := newProvisioner(t, newFakeClient(), Owner{Kind: testOwner.Kind, Namespace: "ns", Name: "svc-other", UID: "uid-b"})
	small, large := demand(t, "2x2x1"), demand(t, "2x2x2")
	src := stateSource(newFakeClient(
		stored(t, a, small, Slot{Instance: 0}, withState("ACTIVE", "")),
		stored(t, a, small, Slot{Instance: 1}, withState("ACTIVE", "")),
		stored(t, a, large, Slot{Instance: 2}),
		stored(t, a, small, Slot{Instance: 3}, withFinalizer, deleting),
		stored(t, b, small, Slot{Instance: 0}, withState("ACTIVE", "")),
		stored(t, b, large, Slot{Instance: 1}),
	), testOwner.UID)
	exists, lookups := src.OwnerExists, 0
	src.OwnerExists = func(ctx context.Context, uid types.UID) (bool, error) {
		lookups++
		return exists(ctx, uid)
	}
	report(t, src)
	want := map[string]float64{
		"type-a/2x2x1/ready":       2,
		"type-a/2x2x2/pending":     1,
		"type-a/2x2x1/terminating": 1,
		"type-a/2x2x1/orphaned":    1,
		"type-a/2x2x2/orphaned":    1,
	}
	if diff := cmp.Diff(want, collectStates(t)); diff != "" {
		t.Fatalf("slice gauge (-want +got):\n%s", diff)
	}
	if lookups != 2 {
		t.Fatalf("owner lookups = %d, want one for each owner", lookups)
	}
}

func TestReportStates(t *testing.T) {
	p := newProvisioner(t, newFakeClient(), testOwner)
	c := newFakeClient(stored(t, p, demand(t, "2x2x1"), Slot{}, withState("ACTIVE", "")))
	if got := collectStates(t); len(got) != 0 {
		t.Fatalf("without a source the gauge reports %v, want nothing", got)
	}
	stopEarlier := ReportStates(stateSource(c))
	stopLater := ReportStates(stateSource(c, testOwner.UID))
	t.Cleanup(stopLater)
	stopEarlier()
	if diff := cmp.Diff(map[string]float64{"type-a/2x2x1/ready": 1}, collectStates(t)); diff != "" {
		t.Fatalf("after the replaced source stopped (-want +got):\n%s", diff)
	}
	stopLater()
	if got := collectStates(t); len(got) != 0 {
		t.Fatalf("after its source stopped the gauge reports %v, want nothing", got)
	}
}

func TestSliceStatesReportNothingWhenAReadFails(t *testing.T) {
	a := newProvisioner(t, newFakeClient(), testOwner)
	b := newProvisioner(t, newFakeClient(), Owner{Kind: testOwner.Kind, Namespace: "ns", Name: "svc-other", UID: "uid-b"})
	d := demand(t, "2x2x1")
	objs := []client.Object{stored(t, a, d, Slot{}, withState("ACTIVE", "")), stored(t, b, d, Slot{}, withState("ACTIVE", ""))}
	listFails := stateSource(fake.NewClientBuilder().WithObjects(objs...).WithInterceptorFuncs(interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return errors.New("injected list failure")
		},
	}).Build(), testOwner.UID, "uid-b")
	lookupFails := stateSource(newFakeClient(objs...), testOwner.UID)
	exists := lookupFails.OwnerExists
	lookupFails.OwnerExists = func(ctx context.Context, uid types.UID) (bool, error) {
		if uid == "uid-b" {
			return false, errors.New("injected owner lookup failure")
		}
		return exists(ctx, uid)
	}
	for _, tc := range []struct {
		name string
		src  StateSource
	}{
		{name: "listing the slices fails", src: listFails},
		{name: "one owner lookup fails", src: lookupFails},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report(t, tc.src)
			if got := collectStates(t); len(got) != 0 {
				t.Fatalf("the gauge reports %v, want nothing rather than a partial count", got)
			}
		})
	}
}
