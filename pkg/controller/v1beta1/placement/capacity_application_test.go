package placement

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	quotacapacity "sigs.k8s.io/ome/pkg/quota/capacity"
)

func TestCapacityApplicationRechecksAcceptedInputs(t *testing.T) {
	for _, tt := range []struct {
		name    string
		setup   func(*testing.T, *capacityProviderFixture)
		edit    func(*testing.T, *capacityProviderFixture)
		wantErr bool
	}{
		{name: "matching inputs"},
		{name: "catchall flavor has empty labels", setup: func(t *testing.T, f *capacityProviderFixture) {
			cl := f.workers["member-a"]
			flavor := &kueuev1beta2.ResourceFlavor{}
			if err := cl.Get(t.Context(), client.ObjectKey{Name: "gpu-a"}, flavor); err != nil {
				t.Fatal(err)
			}
			flavor.Spec.NodeLabels = map[string]string{}
			if err := cl.Update(t.Context(), flavor); err != nil {
				t.Fatal(err)
			}
			root := &v1beta1.AcceleratorQuota{}
			if err := cl.Get(t.Context(), client.ObjectKey{Name: providerConfig().RootName}, root); err != nil {
				t.Fatal(err)
			}
			root.Status.Capacity[0].Attribution.NodeLabels = map[string]string{}
			root.Status.Capacity[0].Attribution.FlavorSetHash = quotacapacity.MappingFingerprint([]string{"example.com/gpu"}, []quotacapacity.Flavor{{Name: flavor.Name, UID: flavor.UID, NodeLabels: flavor.Spec.NodeLabels}})
			if err := cl.Update(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			f.syncFleet(t)
		}},
		{name: "heartbeat version can advance", edit: func(t *testing.T, f *capacityProviderFixture) {
			f.clock.Step(time.Second)
			root := &v1beta1.AcceleratorQuota{}
			cl := f.workers["member-a"]
			if err := cl.Get(t.Context(), client.ObjectKey{Name: providerConfig().RootName}, root); err != nil {
				t.Fatal(err)
			}
			root.Status.Capacity[0].ObservedAt = &metav1.Time{Time: f.clock.Now()}
			if err := cl.Update(t.Context(), root); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "hardware changed", edit: func(t *testing.T, f *capacityProviderFixture) {
			root := &v1beta1.AcceleratorQuota{}
			cl := f.workers["member-a"]
			if err := cl.Get(t.Context(), client.ObjectKey{Name: providerConfig().RootName}, root); err != nil {
				t.Fatal(err)
			}
			root.Status.Capacity[0].Allocatable = resource.MustParse("20")
			if err := cl.Update(t.Context(), root); err != nil {
				t.Fatal(err)
			}
		}, wantErr: true},
		{name: "root replaced", edit: func(t *testing.T, f *capacityProviderFixture) {
			root := &v1beta1.AcceleratorQuota{}
			cl := f.workers["member-a"]
			if err := cl.Get(t.Context(), client.ObjectKey{Name: providerConfig().RootName}, root); err != nil {
				t.Fatal(err)
			}
			root.UID = "replacement"
			if err := cl.Update(t.Context(), root); err != nil {
				t.Fatal(err)
			}
		}, wantErr: true},
		{name: "runtime image changed", edit: func(t *testing.T, f *capacityProviderFixture) {
			runtime := &v1beta1.ClusterServingRuntime{}
			cl := f.workers["member-a"]
			if err := cl.Get(t.Context(), client.ObjectKey{Name: "runtime-a"}, runtime); err != nil {
				t.Fatal(err)
			}
			runtime.Spec.EngineConfig.Runner.Image = "example.com/serving:v2"
			if err := cl.Update(t.Context(), runtime); err != nil {
				t.Fatal(err)
			}
		}, wantErr: true},
		{name: "configuration missing", edit: func(_ *testing.T, f *capacityProviderFixture) { f.r.Capacity = nil }, wantErr: true},
		{name: "report expired", edit: func(_ *testing.T, f *capacityProviderFixture) { f.clock.Step(providerConfig().MaxAge) }, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newCapacityProviderFixture(t)
			if tt.setup != nil {
				tt.setup(t, f)
			}
			samples, err := f.r.readSplitCapacity(t.Context(), f.source, f.clusters, f.clock.Now())
			if err != nil {
				t.Fatal(err)
			}
			evidence, err := recordedCapacitySample(samples["member-a"])
			if err != nil {
				t.Fatal(err)
			}
			assignment := &v1beta1.CandidateAllocationStatus{Capacity: evidence, ClusterUID: f.clusters[0].UID}
			desired, err := f.r.derivedFor(f.source)
			if err != nil {
				t.Fatal(err)
			}
			if tt.edit != nil {
				tt.edit(t, f)
			}
			err = f.r.checkCapacityApplication(t.Context(), f.workers["member-a"], f.source, desired, assignment)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("application: %s: %v", diff, err)
			}
		})
	}
}

// capacitySample is the accepted hardware record a capacity assignment carries.
type capacitySample = v1beta1.PlacementCapacitySample

// capacityRefusalCase is one check of the accepted sample against the
// member: an edit of the sample, the fixture or the clock, and the refusal
// expected, empty when the check passes.
type capacityRefusalCase struct {
	name string
	edit func(*testing.T, *capacityProviderFixture, *capacitySample) *capacitySample
	want string
}

// sampleEdit turns an edit of the accepted sample into a case edit.
func sampleEdit(edit func(*capacitySample)) func(*testing.T, *capacityProviderFixture, *capacitySample) *capacitySample {
	return func(_ *testing.T, _ *capacityProviderFixture, s *capacitySample) *capacitySample {
		edit(s)
		return s
	}
}

// memberReportEdit turns an edit of the member's capacity report into a case edit.
func memberReportEdit(edit func(*capacityProviderFixture, *v1beta1.AcceleratorQuota)) func(*testing.T, *capacityProviderFixture, *capacitySample) *capacitySample {
	return func(t *testing.T, f *capacityProviderFixture, s *capacitySample) *capacitySample {
		t.Helper()
		root := &v1beta1.AcceleratorQuota{}
		if err := f.workers["member-a"].Get(t.Context(), client.ObjectKey{Name: providerConfig().RootName}, root); err != nil {
			t.Fatal(err)
		}
		edit(f, root)
		if err := f.workers["member-a"].Update(t.Context(), root); err != nil {
			t.Fatal(err)
		}
		return s
	}
}

func capacityRefusalCases() []capacityRefusalCase {
	const (
		rendering = "member rendering differs from accepted capacity demand"
		report    = "accepted member capacity report changed before application"
		hardware  = "accepted member hardware changed before application"
		stale     = "member hardware report is stale before application"
		missing   = "accepted member hardware pool is missing before application"
	)
	return []capacityRefusalCase{
		{name: "no sample passes", edit: func(_ *testing.T, _ *capacityProviderFixture, _ *capacitySample) *capacitySample { return nil }},
		{name: "a changed demand fingerprint", want: rendering, edit: sampleEdit(func(s *capacitySample) { s.DemandContract.Fingerprint = "other" })},
		{name: "a replaced report", want: report, edit: sampleEdit(func(s *capacitySample) { s.Pools[0].ReportUID = "other-report" })},
		{name: "a pool of another flavor", want: missing, edit: sampleEdit(func(s *capacitySample) { s.Pools[0].ResourceFlavor = "gpu-z" })},
		{name: "a pool of another resource", want: missing, edit: sampleEdit(func(s *capacitySample) { s.Pools[0].ResourceName = "example.com/other" })},
		{name: "more hardware than accepted", want: hardware, edit: sampleEdit(func(s *capacitySample) { s.Pools[0].Allocatable++ })},
		{name: "another flavor mapping", want: hardware, edit: sampleEdit(func(s *capacitySample) { s.Pools[0].Attribution.FlavorSetHash = "other-mapping" })},
		{name: "a second pool that drifted", want: missing, edit: sampleEdit(func(s *capacitySample) {
			drifted := *s.Pools[0].DeepCopy()
			drifted.ResourceFlavor = "gpu-z"
			s.Pools = append(s.Pools, drifted)
		})},
		{name: "a report as old as the age bound", want: stale, edit: memberReportEdit(func(f *capacityProviderFixture, _ *v1beta1.AcceleratorQuota) { f.clock.Step(providerConfig().MaxAge) })},
		{name: "a report one second younger than the age bound passes", edit: memberReportEdit(func(f *capacityProviderFixture, _ *v1beta1.AcceleratorQuota) {
			f.clock.Step(providerConfig().MaxAge - time.Second)
		})},
		{name: "a report without an observation time", want: stale, edit: memberReportEdit(func(_ *capacityProviderFixture, root *v1beta1.AcceleratorQuota) {
			root.Status.Capacity[0].ObservedAt = nil
		})},
		{name: "a report dated in the future", want: stale, edit: memberReportEdit(func(f *capacityProviderFixture, root *v1beta1.AcceleratorQuota) {
			root.Status.Capacity[0].ObservedAt = &metav1.Time{Time: f.clock.Now().Add(time.Minute)}
		})},
	}
}

// acceptedCapacitySample reads the member's capacity as the plan records it
// and resolves the derived service the write would carry.
func acceptedCapacitySample(t *testing.T, f *capacityProviderFixture) (*capacitySample, *v1beta1.InferenceService) {
	t.Helper()
	samples, err := f.r.readSplitCapacity(t.Context(), f.source, f.clusters, f.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := recordedCapacitySample(samples["member-a"])
	if err != nil {
		t.Fatal(err)
	}
	desired, err := f.r.derivedFor(f.source)
	if err != nil {
		t.Fatal(err)
	}
	return accepted, desired
}

// checkCapacityRefusals runs each case on a fresh fixture: the accepted
// sample is read from the member, edited by the case (a case without an edit
// checks the sample as recorded), and the refusal is compared with the one
// expected.
func checkCapacityRefusals(t *testing.T, fixture func(*testing.T) *capacityProviderFixture, cases []capacityRefusalCase) {
	t.Helper()
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			f := fixture(t)
			accepted, desired := acceptedCapacitySample(t, f)
			if tt.edit != nil {
				accepted = tt.edit(t, f, accepted)
			}
			assignment := &v1beta1.CandidateAllocationStatus{Capacity: accepted, ClusterUID: f.clusters[0].UID}
			got := ""
			if err := f.r.checkCapacityApplication(t.Context(), f.workers["member-a"], f.source, desired, assignment); err != nil {
				got = err.Error()
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("refusal (-want +got):\n%s", diff)
			}
		})
	}
}

// TestCapacityApplicationRefusalTexts holds the text of each refusal the
// application check gives when the accepted sample and the member's current
// report drift apart, and that an assignment without a sample passes. Every
// pool of the sample is checked, and a report is fresh only while it is
// younger than the age bound and not dated in the future.
func TestCapacityApplicationRefusalTexts(t *testing.T) {
	checkCapacityRefusals(t, newCapacityProviderFixture, capacityRefusalCases())
}

// prefillDecodeCapacityFixture is the capacity fixture with a decoder beside
// the engine: the source asks for 8 decoders beside its 12 engines, so one
// measured unit holds 3 engines and 2 decoders, and every member's runtime
// renders a decoder that asks for the same hardware as the engine.
func prefillDecodeCapacityFixture(t *testing.T) *capacityProviderFixture {
	t.Helper()
	f := newCapacityProviderFixture(t)
	f.source.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(8)}}
	resources := corev1.ResourceList{"example.com/gpu": *resource.NewQuantity(2, resource.DecimalSI)}
	for name, worker := range f.workers {
		runtime := &v1beta1.ClusterServingRuntime{}
		if err := worker.Get(t.Context(), client.ObjectKey{Name: "runtime-a"}, runtime); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		runtime.Spec.DecoderConfig = &v1beta1.DecoderSpec{PodSpec: v1beta1.PodSpec{NodeSelector: map[string]string{"hardware": "a"}}, Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "runner", Image: "example.com/serving:v1", Resources: corev1.ResourceRequirements{Requests: resources.DeepCopy(), Limits: resources.DeepCopy()}}}}
		if err := worker.Update(t.Context(), runtime); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	return f
}

func prefillDecodeCapacityCases() []capacityRefusalCase {
	const (
		rendering = "member rendering differs from accepted capacity demand"
		hardware  = "accepted member hardware changed before application"
	)
	return []capacityRefusalCase{
		{name: "the sample as recorded passes"},
		{name: "the engine listed before the decoder", want: rendering, edit: sampleEdit(func(s *capacitySample) {
			s.DemandContract.Components[0], s.DemandContract.Components[1] = s.DemandContract.Components[1], s.DemandContract.Components[0]
		})},
		{name: "a changed decoder rendering", want: rendering, edit: sampleEdit(func(s *capacitySample) { s.DemandContract.Components[0].RenderingHash = "other" })},
		{name: "accelerators counted once instead of per unit", want: hardware, edit: sampleEdit(func(s *capacitySample) { s.Pools[0].Allocatable = 16 })},
	}
}

// TestCapacityApplicationPrefillDecodeSample holds the check on a
// prefill/decode service. The accepted sample lists the components in name
// order (decoder, engine) while the member resolves them engine first, and
// counts the hardware in units of 3 engines: the sample as recorded passes,
// one in the member's order is refused as a different rendering, and the raw
// accelerator count is refused as changed hardware.
func TestCapacityApplicationPrefillDecodeSample(t *testing.T) {
	accepted, _ := acceptedCapacitySample(t, prefillDecodeCapacityFixture(t))
	if components := accepted.DemandContract.Components; len(components) != 2 || components[0].Component != v1beta1.DecoderComponent || accepted.Pools[0].Allocatable != 48 {
		t.Fatalf("accepted sample: components %v, allocatable %d; want the decoder before the engine and 16 accelerators times 3 units", components, accepted.Pools[0].Allocatable)
	}
	checkCapacityRefusals(t, prefillDecodeCapacityFixture, prefillDecodeCapacityCases())
}
