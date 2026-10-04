package inferencereplica

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/sliceprovision"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func TestSlicesOptedIn(t *testing.T) {
	optedIn := map[string]string{constants.TPUSliceProvisioningAnnotationKey: "true"}
	runner := func(name v1beta1.RunnerName, annotations map[string]string) v1beta1.Runner {
		return v1beta1.Runner{Name: name, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: annotations}}}
	}
	for _, tc := range []struct {
		name    string
		runners []v1beta1.Runner
		want    bool
	}{
		{"default runner opts in", []v1beta1.Runner{runner(v1beta1.RunnerNameDefault, optedIn)}, true},
		{"leader runner opts in", []v1beta1.Runner{runner(v1beta1.RunnerNameWorker, nil), runner(v1beta1.RunnerNameLeader, optedIn)}, true},
		{"worker runner alone does not", []v1beta1.Runner{runner(v1beta1.RunnerNameLeader, nil), runner(v1beta1.RunnerNameWorker, optedIn)}, false},
		{"another value does not", []v1beta1.Runner{runner(v1beta1.RunnerNameDefault, map[string]string{constants.TPUSliceProvisioningAnnotationKey: "yes"})}, false},
		{"no annotation", []v1beta1.Runner{runner(v1beta1.RunnerNameDefault, nil)}, false},
		{"no runners", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ir := baselineIR("llama-engine", "prod", 1)
			ir.Spec.Runners = tc.runners
			if got := slicesOptedIn(ir); got != tc.want {
				t.Fatalf("slicesOptedIn = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSliceProvisioner_NilWithoutConfigOrCRD pins that no slice is touched
// unless provisioning is configured and the Slice CRD was found at setup.
func TestSliceProvisioner_NilWithoutConfigOrCRD(t *testing.T) {
	ir := optedInIR("llama-engine", "prod", 1)
	r, c := newSliceReconciler(t, ir)
	r.sliceReader = nil
	if p, err := r.sliceProvisioner(ir); p != nil || err != nil {
		t.Fatalf("without the Slice CRD: sliceProvisioner = %v, %v; want nil, nil", p, err)
	}
	r.sliceReader, r.TPUSliceProvisioning = c, nil
	if p, err := r.sliceProvisioner(ir); p != nil || err != nil {
		t.Fatalf("without configuration: sliceProvisioner = %v, %v; want nil, nil", p, err)
	}
}

func TestReleasingSlices(t *testing.T) {
	ctx := context.Background()
	errFinalize := errors.New("injected finalize failure")
	for _, tc := range []struct {
		name         string
		finalize     func(context.Context, int32) (bool, error)
		wantErr      bool
		wantReleased bool
		// wantComplete is the result once the slices are gone.
		wantComplete bool
	}{
		{name: "no finalize releases the slices", wantReleased: true, wantComplete: true},
		{
			name:         "a complete finalize releases the slices",
			finalize:     func(context.Context, int32) (bool, error) { return true, nil },
			wantReleased: true,
			wantComplete: true,
		},
		{
			name:         "an incomplete finalize still releases the slices",
			finalize:     func(context.Context, int32) (bool, error) { return false, nil },
			wantReleased: true,
		},
		{
			name:     "a failed finalize releases nothing",
			finalize: func(context.Context, int32) (bool, error) { return false, errFinalize },
			wantErr:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ir := optedInIR("llama-engine", "prod", 2)
			r, c := newSliceReconciler(t, ir)
			own := seedSlice(t, r, c, ir, sliceprovision.Slot{Instance: 0}, sliceStateReady)
			other := seedSlice(t, r, c, ir, sliceprovision.Slot{Instance: 1}, sliceStateReady)
			p, err := r.sliceProvisioner(ir)
			if err != nil {
				t.Fatalf("sliceProvisioner: %v", err)
			}
			finalize := releasingSlices(tc.finalize, p)

			complete, err := finalize(ctx, 0)
			if tc.wantErr {
				if !errors.Is(err, errFinalize) {
					t.Fatalf("finalize error = %v, want %v", err, errFinalize)
				}
			} else if err != nil {
				t.Fatalf("finalize: %v", err)
			}
			if complete {
				t.Fatal("finalize must not complete on the pass that releases the slices")
			}
			if released := getSlice(t, c, own) == nil; released != tc.wantReleased {
				t.Fatalf("Instance 0's slice released = %v, want %v", released, tc.wantReleased)
			}
			if getSlice(t, c, other) == nil {
				t.Fatal("finalizing Instance 0 released Instance 1's slice")
			}
			if !tc.wantReleased {
				return
			}
			complete, err = finalize(ctx, 0)
			if err != nil {
				t.Fatalf("second finalize: %v", err)
			}
			if complete != tc.wantComplete {
				t.Fatalf("finalize once the slices are gone = %v, want %v", complete, tc.wantComplete)
			}
		})
	}
}

func TestProvisionSlices(t *testing.T) {
	ctx := context.Background()
	optedOut := func() *v1beta1.InferenceReplica {
		ir := optedInIR("llama-engine", "prod", 1)
		ir.Spec.Runners[0].Template.Annotations = nil
		return ir
	}
	for _, tc := range []struct {
		name string
		ir   *v1beta1.InferenceReplica
		// held seeds a slice for Instance 0.
		held       bool
		wantPass   bool
		wantPlacer bool
	}{
		{name: "an opted-out owner holding no slice is untouched", ir: optedOut()},
		{name: "an opted-out owner holding a slice only releases", ir: optedOut(), held: true, wantPass: true},
		{name: "an opted-in owner places its pods", ir: optedInIR("llama-engine", "prod", 1), wantPass: true, wantPlacer: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, c := newSliceReconciler(t, tc.ir)
			if tc.held {
				seedSlice(t, r, c, tc.ir, sliceprovision.Slot{Instance: 0}, sliceStateReady)
			}
			var deps workloadtypes.Deps
			var input workloadtypes.ReconcileInput
			pass, err := r.provisionSlices(ctx, tc.ir, &deps, &input)
			if err != nil {
				t.Fatalf("provisionSlices: %v", err)
			}
			if (pass != nil) != tc.wantPass {
				t.Fatalf("pass = %+v, want a pass: %v", pass, tc.wantPass)
			}
			if (input.FinalizeInstanceResources != nil) != tc.wantPass {
				t.Fatalf("Instance finalization releases slices = %v, want %v", input.FinalizeInstanceResources != nil, tc.wantPass)
			}
			if (deps.Provisioner != nil) != tc.wantPlacer {
				t.Fatalf("Provisioner set = %v, want %v", deps.Provisioner != nil, tc.wantPlacer)
			}
			if tc.wantPlacer && deps.Provisioner != pass.placer {
				t.Fatal("the engine must place pods through the pass's placer, which Sweep keeps the slices of")
			}
		})
	}
}

// TestProvisionSlices_UnconfiguredLeavesThePassAlone pins that a
// controller without slice provisioning changes nothing the engine sees.
func TestProvisionSlices_UnconfiguredLeavesThePassAlone(t *testing.T) {
	ir := optedInIR("llama-engine", "prod", 1)
	r, _ := newReconciler(t, ir)
	var deps workloadtypes.Deps
	var input workloadtypes.ReconcileInput
	pass, err := r.provisionSlices(context.Background(), ir, &deps, &input)
	if err != nil || pass != nil {
		t.Fatalf("provisionSlices = %+v, %v; want nil, nil", pass, err)
	}
	if deps.Provisioner != nil || input.FinalizeInstanceResources != nil {
		t.Fatal("an unconfigured controller must leave the engine's provisioner and finalization unset")
	}
}

func TestProvisionSlices_CachedReadFails(t *testing.T) {
	ir := optedInIR("llama-engine", "prod", 1)
	ir.Spec.Runners[0].Template.Annotations = nil
	r, c := newSliceReconciler(t, ir)
	r.sliceReader = interceptor.NewClient(c, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*unstructured.UnstructuredList); ok {
				return errors.New("injected slice list failure")
			}
			return c.List(ctx, list, opts...)
		},
	})
	var deps workloadtypes.Deps
	var input workloadtypes.ReconcileInput
	if _, err := r.provisionSlices(context.Background(), ir, &deps, &input); err == nil || !strings.Contains(err.Error(), "injected slice list failure") {
		t.Fatalf("provisionSlices error = %v, want the slice list failure", err)
	}
}

func TestSlicePassSweep(t *testing.T) {
	ctx := context.Background()
	var none *slicePass
	if err := none.sweep(ctx, workloadtypes.ReconcileInput{}, workloadtypes.ComponentPlan{}); err != nil {
		t.Fatalf("a nil pass must release nothing: %v", err)
	}

	ir := optedInIR("llama-engine", "prod", 1)
	ir.Spec.Runners[0].Template.Annotations = nil
	r, c := newSliceReconciler(t, ir)
	unpinned := seedSlice(t, r, c, ir, sliceprovision.Slot{Instance: 0}, sliceStateReady)
	pinned := seedSlice(t, r, c, ir, sliceprovision.Slot{Instance: 1}, sliceStateReady)
	pod := podForIR(ir, 1, "default", 0, true, true)
	pod.Spec.NodeSelector = map[string]string{sliceKeySlice: pinned}
	if err := c.Create(ctx, pod); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	var deps workloadtypes.Deps
	var input workloadtypes.ReconcileInput
	pass, err := r.provisionSlices(ctx, ir, &deps, &input)
	if err != nil || pass == nil {
		t.Fatalf("provisionSlices = %+v, %v; want a releasing pass", pass, err)
	}
	if err := pass.sweep(ctx, input, workloadtypes.ComponentPlan{}); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if getSlice(t, c, unpinned) != nil {
		t.Error("a slice no pod is confined to must be released once the owner opts out")
	}
	if getSlice(t, c, pinned) == nil {
		t.Error("a slice a pod is confined to must be kept")
	}
}

// TestSlicePassSweep_OptedIn pins that an opted-in pass's placer reads the
// component's pods live to decide which slices are pinned.
func TestSlicePassSweep_OptedIn(t *testing.T) {
	ctx := context.Background()
	ir := optedInIR("llama-engine", "prod", 1)
	r, c := newSliceReconciler(t, ir)
	unpinned := seedSlice(t, r, c, ir, sliceprovision.Slot{Instance: 5}, sliceStateReady)
	pinned := seedSlice(t, r, c, ir, sliceprovision.Slot{Instance: 6}, sliceStateReady)
	pod := podForIR(ir, 6, "default", 0, true, true)
	pod.Spec.NodeSelector = map[string]string{sliceKeySlice: pinned}
	if err := c.Create(ctx, pod); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	var deps workloadtypes.Deps
	var input workloadtypes.ReconcileInput
	pass, err := r.provisionSlices(ctx, ir, &deps, &input)
	if err != nil || pass == nil || pass.placer == nil {
		t.Fatalf("provisionSlices = %+v, %v; want a placing pass", pass, err)
	}
	if err := pass.sweep(ctx, input, workloadtypes.ComponentPlan{}); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if getSlice(t, c, unpinned) != nil {
		t.Error("a slice no pod is confined to and no Instance wants must be released")
	}
	if getSlice(t, c, pinned) == nil {
		t.Error("a slice a pod is confined to must be kept")
	}

	seedSlice(t, r, c, ir, sliceprovision.Slot{Instance: 7}, sliceStateReady)
	r.APIReader = podListFailingReader{Reader: c}
	pass, err = r.provisionSlices(ctx, ir, &deps, &input)
	if err != nil || pass == nil {
		t.Fatalf("provisionSlices = %+v, %v; want a placing pass", pass, err)
	}
	if err := pass.sweep(ctx, input, workloadtypes.ComponentPlan{}); err == nil {
		t.Fatal("the placer must read pods through the live reader")
	}
}

// TestPinnedSlices pins the live read Sweep relies on: every pod of the
// component counts, whatever controls it, and a terminal pod pins nothing.
func TestPinnedSlices(t *testing.T) {
	ir := optedInIR("llama-engine", "prod", 1)
	owned := podForIR(ir, 0, "default", 0, true, true)
	owned.Spec.NodeSelector = map[string]string{sliceKeySlice: "owned"}
	foreign := podForIR(ir, 1, "default", 0, true, true)
	foreign.OwnerReferences = nil
	foreign.Spec.NodeSelector = map[string]string{sliceKeySlice: "foreign"}
	terminal := podForIR(ir, 2, "default", 0, false, false)
	terminal.Status.Phase = corev1.PodSucceeded
	terminal.Spec.NodeSelector = map[string]string{sliceKeySlice: "terminal"}
	other := podForIR(ir, 3, "default", 0, true, true)
	other.Labels[constants.OMEComponentLabel] = string(v1beta1.DecoderComponent)
	other.Spec.NodeSelector = map[string]string{sliceKeySlice: "other-component"}
	r, _ := newSliceReconciler(t, ir, owned, foreign, terminal, other)
	p, err := r.sliceProvisioner(ir)
	if err != nil {
		t.Fatalf("sliceProvisioner: %v", err)
	}

	got, err := r.pinnedSlices(ir, p)(context.Background())
	if err != nil {
		t.Fatalf("pinnedSlices: %v", err)
	}
	want := map[string]struct{}{"owned": {}, "foreign": {}}
	if len(got) != len(want) {
		t.Fatalf("pinnedSlices = %v, want %v", got, want)
	}
	for name := range want {
		if _, ok := got[name]; !ok {
			t.Fatalf("pinnedSlices = %v, want %v", got, want)
		}
	}
}

func TestPinnedSlices_ReadsLive(t *testing.T) {
	ir := optedInIR("llama-engine", "prod", 1)
	r, c := newSliceReconciler(t, ir)
	p, err := r.sliceProvisioner(ir)
	if err != nil {
		t.Fatalf("sliceProvisioner: %v", err)
	}
	r.APIReader = podListFailingReader{Reader: c}
	if _, err := r.pinnedSlices(ir, p)(context.Background()); err == nil {
		t.Fatal("pinnedSlices must read pods through the live reader")
	}
}

func TestReleaseSlicesPastDeadline(t *testing.T) {
	ir := terminatingIR("llama-engine", "prod", 1, time.Now())
	r, c := newSliceReconciler(t, ir)
	rec := record.NewFakeRecorder(8)
	r.Recorder = rec
	free := seedSlice(t, r, c, ir, sliceprovision.Slot{Instance: 0}, sliceStateReady)
	pinned := seedSlice(t, r, c, ir, sliceprovision.Slot{Instance: 1}, sliceStateReady)
	pod := podForIR(ir, 1, "default", 0, true, true)
	pod.Spec.NodeSelector = map[string]string{sliceKeySlice: pinned}
	if err := c.Create(context.Background(), pod); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	r.releaseSlicesPastDeadline(context.Background(), ir)
	for _, name := range []string{free, pinned} {
		if getSlice(t, c, name) != nil {
			t.Errorf("past the deadline every held slice is released, pods or not; %s is left", name)
		}
	}
	if events := drainEvents(rec); len(events) != 0 {
		t.Fatalf("a successful release must not warn, got %v", events)
	}
}

func TestReleaseSlicesPastDeadline_WarnsOnFailure(t *testing.T) {
	ir := terminatingIR("llama-engine", "prod", 1, time.Now())
	r, c := newSliceReconciler(t, ir)
	rec := record.NewFakeRecorder(8)
	r.Recorder = rec
	seedSlice(t, r, c, ir, sliceprovision.Slot{Instance: 0}, sliceStateReady)
	r.Client = interceptor.NewClient(c, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*unstructured.Unstructured); ok {
				return errors.New("injected slice delete failure")
			}
			return c.Delete(ctx, obj, opts...)
		},
	})

	r.releaseSlicesPastDeadline(context.Background(), ir)
	warnings := eventsContaining(drainEvents(rec), ReasonTeardownDeadlineExceeded)
	if len(warnings) != 1 {
		t.Fatalf("expected one TeardownDeadlineExceeded warning, got %v", warnings)
	}
	for _, want := range []string{"injected slice delete failure", sliceprovision.LabelOwnerUID + "=" + string(ir.UID)} {
		if !strings.Contains(warnings[0], want) {
			t.Errorf("warning must contain %q; got %q", want, warnings[0])
		}
	}
}

func TestReleaseSlicesPastDeadline_UnconfiguredDoesNothing(t *testing.T) {
	ir := terminatingIR("llama-engine", "prod", 1, time.Now())
	r, _ := newReconciler(t, ir)
	rec := record.NewFakeRecorder(8)
	r.Recorder = rec
	r.releaseSlicesPastDeadline(context.Background(), ir)
	if events := drainEvents(rec); len(events) != 0 {
		t.Fatalf("an unconfigured controller must not warn about slices, got %v", events)
	}
}

func TestIRUIDIndexExtractor(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	if got := irUIDIndexExtractor(ir); len(got) != 1 || got[0] != string(ir.UID) {
		t.Fatalf("irUIDIndexExtractor = %v, want [%s]", got, ir.UID)
	}
	ir.UID = ""
	if got := irUIDIndexExtractor(ir); got != nil {
		t.Fatalf("an InferenceReplica without a UID must not be indexed, got %v", got)
	}
}

func TestIRExists(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	_, c := newReconciler(t, ir)
	for _, tc := range []struct {
		name string
		uid  types.UID
		want bool
	}{
		{name: "the InferenceReplica", uid: ir.UID, want: true},
		{name: "an earlier InferenceReplica of the same name", uid: "llama-engine-earlier-uid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := irExists(c)(context.Background(), tc.uid); err != nil || got != tc.want {
				t.Fatalf("irExists(%s) = %v, %v; want %v", tc.uid, got, err, tc.want)
			}
		})
	}
}

func TestIRExists_ListFails(t *testing.T) {
	_, base := newReconciler(t)
	c := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return errors.New("injected list failure")
		},
	})
	if _, err := irExists(c)(context.Background(), "llama-engine-uid"); err == nil || !strings.Contains(err.Error(), "injected list failure") {
		t.Fatalf("irExists must return the list error, got %v", err)
	}
}

// reportedSliceStates reads the TPU slice gauge from the controller-runtime
// registry, summed by state.
func reportedSliceStates(t *testing.T) map[string]float64 {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	got := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "ome_tpu_slices" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "state" {
					got[label.GetValue()] += metric.GetGauge().GetValue()
				}
			}
		}
	}
	return got
}

// createdSliceSeries reads the topologies that have a series of sliceType on
// the TPU slice created counter in the controller-runtime registry.
func createdSliceSeries(t *testing.T, sliceType string) []string {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	var got []string
	for _, family := range families {
		if family.GetName() != "ome_tpu_slice_created_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["slice_type"] == sliceType {
				got = append(got, labels["topology"])
			}
		}
	}
	return got
}

// blockingInformers holds each GetInformer call until the test receives it
// on entered and then sends on release, as a starting cache holds the call
// until the informer syncs.
type blockingInformers struct {
	cache.Informers
	entered chan struct{}
	release chan struct{}
}

func (b *blockingInformers) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	select {
	case b.entered <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return b.Informers.GetInformer(ctx, obj, opts...)
}

// startReportSliceStates runs r.reportSliceStates until ctx is done. Its
// result arrives on the returned channel.
func startReportSliceStates(ctx context.Context, r *Reconciler, informers cache.Informers, reader client.Reader) <-chan error {
	done := make(chan error, 1)
	go func() { done <- r.reportSliceStates(informers, reader)(ctx) }()
	return done
}

// awaitReturn fails the test unless done delivers nil in time.
func awaitReturn(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("reportSliceStates returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("reportSliceStates did not return")
	}
}

func TestReportSliceStates(t *testing.T) {
	ir := optedInIR("llama-engine", "prod", 1)
	earlier := ir.DeepCopy()
	earlier.UID = "llama-engine-earlier-uid"
	gone := optedInIR("mistral-engine", "prod", 1)
	r, c := newSliceReconciler(t, ir)
	seedSlice(t, r, c, ir, sliceprovision.Slot{}, sliceStateReady)
	seedSlice(t, r, c, earlier, sliceprovision.Slot{Instance: 1}, sliceStateReady)
	seedSlice(t, r, c, gone, sliceprovision.Slot{}, sliceStateReady)
	informers := &blockingInformers{
		Informers: &informertest.FakeInformers{Scheme: testScheme(t)},
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startReportSliceStates(ctx, r, informers, c)

	// Reporting waits for the slice and the InferenceReplica informers.
	for synced := 0; synced < 2; synced++ {
		select {
		case <-informers.entered:
		case <-time.After(10 * time.Second):
			t.Fatalf("reportSliceStates waited for %d informers, want 2", synced)
		}
		if got := reportedSliceStates(t); len(got) != 0 {
			t.Fatalf("reported %v with %d of 2 informers synced, want nothing", got, synced)
		}
		informers.release <- struct{}{}
	}
	want := map[string]float64{"ready": 1, "orphaned": 2}
	for deadline := time.Now().Add(10 * time.Second); !maps.Equal(reportedSliceStates(t), want); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("slice gauge = %v, want %v", reportedSliceStates(t), want)
		}
	}
	cancel()
	awaitReturn(t, done)
	if got := reportedSliceStates(t); len(got) != 0 {
		t.Fatalf("reported %v after stopping, want nothing", got)
	}
}

func TestReportSliceStates_InformerFails(t *testing.T) {
	ir := optedInIR("llama-engine", "prod", 1)
	r, c := newSliceReconciler(t, ir)
	seedSlice(t, r, c, ir, sliceprovision.Slot{}, sliceStateReady)
	informers := &informertest.FakeInformers{Scheme: testScheme(t), Error: errors.New("injected informer failure")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	awaitReturn(t, startReportSliceStates(ctx, r, informers, c))
	if got := reportedSliceStates(t); len(got) != 0 {
		t.Fatalf("reported %v without synced informers, want nothing", got)
	}
}

// TestSlicesCarryPodAnnotations pins that the slices provisioned for an
// InferenceReplica carry the annotations of its own pod template whose keys
// the configuration lists, and no others.
func TestSlicesCarryPodAnnotations(t *testing.T) {
	ctx := context.Background()
	ir := optedInIR("llama-engine", "prod", 1)
	ir.Spec.Runners[0].Template.Annotations["example.com/priority"] = "7"
	ir.Spec.Runners[0].Template.Annotations["example.com/unlisted"] = "x"
	r, c := newSliceReconciler(t, ir)
	r.TPUSliceProvisioning.Slice.PodAnnotations = []string{"example.com/priority", "example.com/absent"}
	for pass := 0; pass < 2; pass++ {
		r.Expectations = workloadtypes.NewExpectations()
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
	}
	p, err := r.sliceProvisioner(ir)
	if err != nil {
		t.Fatalf("sliceProvisioner: %v", err)
	}
	slice := getSlice(t, c, p.Name(sliceprovision.Slot{Instance: 0}))
	if slice == nil {
		t.Fatalf("slices = %v, want the Instance's slice provisioned", sliceNames(t, c))
	}
	got := slice.GetAnnotations()
	if got["example.com/priority"] != "7" {
		t.Fatalf("slice annotations = %v, want example.com/priority=7 from the pod template", got)
	}
	for _, key := range []string{"example.com/unlisted", "example.com/absent"} {
		if _, ok := got[key]; ok {
			t.Fatalf("slice annotations = %v, want no %s", got, key)
		}
	}
}

// TestSliceReleaseDeferredIsReported pins that a release kept back by another
// workload's pods on the slice's hosts is reported on the InferenceReplica,
// and that the slice survives it.
func TestSliceReleaseDeferredIsReported(t *testing.T) {
	ctx := context.Background()
	ir := optedInIR("llama-engine", "prod", 1)
	r, c := newSliceReconciler(t, ir)
	rec := &capturingRecorder{}
	r.Recorder = rec
	name := seedSlice(t, r, c, ir, sliceprovision.Slot{Instance: 0}, sliceStateReady)
	host := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host-a", Labels: map[string]string{sliceKeySlice: name}}}
	holder := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "holder", Namespace: "other"},
		Spec: corev1.PodSpec{NodeName: "host-a", Containers: []corev1.Container{{
			Name:      "main",
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{sliceChipResource: resource.MustParse("4")}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for _, o := range []client.Object{host, holder} {
		if err := c.Create(ctx, o); err != nil {
			t.Fatalf("create %s: %v", o.GetName(), err)
		}
	}
	p, err := r.sliceProvisioner(ir)
	if err != nil {
		t.Fatalf("sliceProvisioner: %v", err)
	}
	complete, err := p.ReleaseAll(ctx)
	if err != nil || complete {
		t.Fatalf("ReleaseAll = %v, %v; want the slice kept", complete, err)
	}
	if getSlice(t, c, name) == nil {
		t.Fatal("a slice another workload holds chips on must be kept")
	}
	if len(rec.events) != 1 || rec.events[0].reason != string(sliceprovision.EventReasonSliceReleaseDeferred) ||
		rec.events[0].kind != corev1.EventTypeWarning || !strings.Contains(rec.events[0].message, "other/holder") || rec.events[0].object != ir {
		t.Fatalf("events = %+v, want one %s warning on the InferenceReplica naming other/holder", rec.events, sliceprovision.EventReasonSliceReleaseDeferred)
	}
}

// TestRecoverLostSlices pins the recovery of pods on a slice deleted, or
// moved off their node, from outside: they are deleted and reported once a
// live read confirms the cached one, a pod already gone is not an error, and
// a failed delete is.
func TestRecoverLostSlices(t *testing.T) {
	ctx := context.Background()
	if err := (&Reconciler{}).recoverLostSlices(ctx, nil, workloadtypes.ReconcileInput{}, nil); err != nil {
		t.Fatalf("a nil pass must recover nothing: %v", err)
	}
	failPodDeletes := func(err error) func(*Reconciler, client.WithWatch) {
		return func(r *Reconciler, c client.WithWatch) {
			r.Client = interceptor.NewClient(c, interceptor.Funcs{
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if _, ok := obj.(*corev1.Pod); ok {
						return err
					}
					return c.Delete(ctx, obj, opts...)
				},
			})
		}
	}
	lostSeries := map[string]string{"slice_type": "type-a", "topology": "2x2x1"}
	for _, tc := range []struct {
		name string
		// lose deletes the slice from outside.
		lose bool
		// bound binds the pod to node host-a, which carries the slice's
		// label when holds is set; partitioned gives the slice partitions.
		bound, holds, partitioned bool
		// wire adapts the reconciler, whose reads and writes all serve c.
		wire      func(*Reconciler, client.WithWatch)
		wantErr   string
		wantGone  bool
		wantEvent bool
		// wantWhy is how the event says the slice was lost.
		wantWhy string
	}{
		{name: "slice intact"},
		{name: "slice lost", lose: true, wantGone: true, wantEvent: true, wantWhy: "was deleted"},
		{name: "slice holds the bound pod's node", bound: true, holds: true, partitioned: true},
		{
			name: "slice without partitions under a bound pod", bound: true, holds: true,
			wantGone: true, wantEvent: true, wantWhy: "lost its partitions",
		},
		{
			name: "slice moved off the bound pod's node", bound: true, partitioned: true,
			wantGone: true, wantEvent: true, wantWhy: "moved off node host-a",
		},
		{
			name: "cache lags the live slice",
			wire: func(r *Reconciler, _ client.WithWatch) { r.sliceReader = fake.NewClientBuilder().Build() },
		},
		{
			name: "pod being deleted per the live read",
			lose: true,
			wire: func(r *Reconciler, c client.WithWatch) {
				r.APIReader = interceptor.NewClient(c, interceptor.Funcs{
					List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						if err := c.List(ctx, list, opts...); err != nil {
							return err
						}
						if pods, ok := list.(*corev1.PodList); ok {
							for i := range pods.Items {
								pods.Items[i].DeletionTimestamp = &metav1.Time{Time: time.Now()}
							}
						}
						return nil
					},
				})
			},
		},
		{
			name:      "pod already gone",
			lose:      true,
			wire:      failPodDeletes(apierrors.NewNotFound(corev1.Resource("pods"), "gone")),
			wantEvent: true,
		},
		{
			name:    "pod delete fails",
			lose:    true,
			wire:    failPodDeletes(errors.New("injected pod delete failure")),
			wantErr: "injected pod delete failure",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ir := optedInIR("llama-engine", "prod", 1)
			r, c := newSliceReconciler(t, ir)
			rec := &capturingRecorder{}
			r.Recorder = rec
			name := seedSlice(t, r, c, ir, sliceprovision.Slot{Instance: 0}, sliceStateReady)
			pod := podForIR(ir, 0, "default", 0, true, true)
			pod.Spec.NodeSelector = map[string]string{sliceKeyAccelerator: "tpu-a", sliceKeyTopology: "2x2x1", sliceKeySlice: name}
			if tc.bound {
				pod.Spec.NodeName = "host-a"
				host := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host-a", Labels: map[string]string{}}}
				if tc.holds {
					host.Labels[sliceKeySlice] = name
				}
				if err := c.Create(ctx, host); err != nil {
					t.Fatalf("create host: %v", err)
				}
			}
			if tc.partitioned {
				setSlicePartitions(t, c, name, "partition-a")
			}
			if err := c.Create(ctx, pod); err != nil {
				t.Fatalf("create pod: %v", err)
			}
			if tc.lose {
				if err := c.Delete(ctx, getSlice(t, c, name)); err != nil {
					t.Fatalf("delete slice: %v", err)
				}
			}
			if tc.wire != nil {
				tc.wire(r, c)
			}
			p, err := r.sliceProvisioner(ir)
			if err != nil {
				t.Fatalf("sliceProvisioner: %v", err)
			}
			before := irStatusMetric(t, "ome_tpu_slice_lost_total", lostSeries)

			err = r.recoverLostSlices(ctx, ir, workloadtypes.ReconcileInput{OwnerObject: ir}, &slicePass{p: p})
			if tc.wantErr == "" && err != nil || tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("recoverLostSlices error = %v, want %q", err, tc.wantErr)
			}
			getErr := c.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
			if gone := apierrors.IsNotFound(getErr); gone != tc.wantGone {
				t.Fatalf("pod gone = %v (get error %v), want %v", gone, getErr, tc.wantGone)
			}
			wantCount := 0.0
			if tc.wantEvent {
				wantCount = 1
				if len(rec.events) != 1 || rec.events[0].reason != string(sliceprovision.EventReasonSliceLost) ||
					rec.events[0].kind != corev1.EventTypeWarning || rec.events[0].object != ir ||
					!strings.Contains(rec.events[0].message, name) || !strings.Contains(rec.events[0].message, pod.Name) ||
					!strings.Contains(rec.events[0].message, tc.wantWhy) {
					t.Fatalf("events = %+v, want one %s warning on the InferenceReplica naming %s and %s, saying it %s",
						rec.events, sliceprovision.EventReasonSliceLost, name, pod.Name, tc.wantWhy)
				}
			} else if len(rec.events) != 0 {
				t.Fatalf("events = %+v, want none", rec.events)
			}
			if got := irStatusMetric(t, "ome_tpu_slice_lost_total", lostSeries) - before; got != wantCount {
				t.Fatalf("lost slices counted = %v, want %v", got, wantCount)
			}
		})
	}
}
