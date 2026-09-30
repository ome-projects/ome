package placement

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
			evidence, err := capacityEvidence(samples["member-a"])
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
