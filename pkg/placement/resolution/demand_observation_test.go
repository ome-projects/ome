package resolution

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func updateDemandInput[T client.Object](t *testing.T, cl client.Client, object T, change func(T)) {
	t.Helper()
	if err := cl.Get(t.Context(), client.ObjectKeyFromObject(object), object); err != nil {
		t.Fatal(err)
	}
	change(object)
	if err := cl.Update(t.Context(), object); err != nil {
		t.Fatal(err)
	}
}

func refreshDemandReport(t *testing.T, f *demandFixture, cl client.Client) {
	t.Helper()
	var catalog kueuev1beta2.ResourceFlavorList
	if err := cl.List(t.Context(), &catalog); err != nil {
		t.Fatal(err)
	}
	f.flavors = nil
	for i := range catalog.Items {
		f.flavors = append(f.flavors, &catalog.Items[i])
	}
	updateDemandInput(t, cl, f.root, func(*v1beta1.AcceleratorQuota) { f.report() })
}

func TestResolvedDemandDependencyFences(t *testing.T) {
	for _, tt := range []struct {
		name            string
		mutate          func(*testing.T, *demandFixture, client.Client)
		wantInvalid     bool
		wantFingerprint bool
		wantFreshError  bool
	}{
		{name: "unchanged"},
		{name: "replica floor is not unit demand", mutate: func(_ *testing.T, f *demandFixture, _ client.Client) { f.service.Spec.Engine.MinReplicas = ptr.To(9) }},
		{name: "hardware observation changes", wantInvalid: true, mutate: func(t *testing.T, f *demandFixture, cl client.Client) {
			updateDemandInput(t, cl, f.root, func(root *v1beta1.AcceleratorQuota) { root.Status.Capacity[0].Allocatable = resource.MustParse("8") })
		}},
		{name: "quota changes", wantInvalid: true, mutate: func(t *testing.T, f *demandFixture, cl client.Client) {
			updateDemandInput(t, cl, f.root, func(root *v1beta1.AcceleratorQuota) {
				root.Spec.Budgets = []v1beta1.AcceleratorBudget{{ResourceName: unitGPU, ResourceFlavor: "gpu-a", Nominal: resource.MustParse("100")}}
			})
		}},
		{name: "report heartbeat changes", wantInvalid: true, mutate: func(t *testing.T, f *demandFixture, cl client.Client) {
			updateDemandInput(t, cl, f.root, func(root *v1beta1.AcceleratorQuota) {
				root.Status.Capacity[0].ObservedAt = ptr.To(metav1.NewTime(time.Unix(100, 0)))
			})
		}},
		{name: "root incarnation changes", wantInvalid: true, mutate: func(t *testing.T, f *demandFixture, cl client.Client) {
			if err := cl.Delete(t.Context(), f.root); err != nil {
				t.Fatal(err)
			}
			f.root.UID, f.root.ResourceVersion = "replacement-root", ""
			if err := cl.Create(t.Context(), f.root); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "flavor metadata changes", wantInvalid: true, mutate: func(t *testing.T, f *demandFixture, cl client.Client) {
			updateDemandInput(t, cl, f.flavors[0], func(flavor *kueuev1beta2.ResourceFlavor) { flavor.Annotations = map[string]string{"note": "changed"} })
		}},
		{name: "flavor labels change without report refresh", wantInvalid: true, wantFreshError: true, mutate: func(t *testing.T, f *demandFixture, cl client.Client) {
			updateDemandInput(t, cl, f.flavors[0], func(flavor *kueuev1beta2.ResourceFlavor) { flavor.Spec.NodeLabels["zone"] = "east" })
		}},
		{name: "flavor labels and report change", wantInvalid: true, wantFingerprint: true, mutate: func(t *testing.T, f *demandFixture, cl client.Client) {
			updateDemandInput(t, cl, f.flavors[0], func(flavor *kueuev1beta2.ResourceFlavor) { flavor.Spec.NodeLabels["zone"] = "east" })
			refreshDemandReport(t, f, cl)
		}},
		{name: "unused flavor is replaced", wantInvalid: true, wantFingerprint: true, mutate: func(t *testing.T, f *demandFixture, cl client.Client) {
			flavor := f.flavors[1]
			if err := cl.Delete(t.Context(), flavor); err != nil {
				t.Fatal(err)
			}
			flavor.UID, flavor.ResourceVersion = "replacement-flavor", ""
			if err := cl.Create(t.Context(), flavor); err != nil {
				t.Fatal(err)
			}
			refreshDemandReport(t, f, cl)
		}},
		{name: "unused catalog member added", wantInvalid: true, wantFingerprint: true, mutate: func(t *testing.T, f *demandFixture, cl client.Client) {
			flavor := &kueuev1beta2.ResourceFlavor{ObjectMeta: metav1.ObjectMeta{Name: "gpu-c", UID: "flavor-c"}, Spec: kueuev1beta2.ResourceFlavorSpec{NodeLabels: map[string]string{"hardware": "c"}}}
			if err := cl.Create(t.Context(), flavor); err != nil {
				t.Fatal(err)
			}
			refreshDemandReport(t, f, cl)
		}},
		{name: "root deleted", wantInvalid: true, wantFreshError: true, mutate: func(t *testing.T, f *demandFixture, cl client.Client) {
			if err := cl.Delete(t.Context(), f.root); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "flavor deleted", wantInvalid: true, wantFreshError: true, mutate: func(t *testing.T, f *demandFixture, cl client.Client) {
			if err := cl.Delete(t.Context(), f.flavors[0]); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newDemandFixture()
			cl := unitClient(t, f.runtimeFixture, interceptor.Funcs{})
			resolver := Resolver{Client: cl, OperatorNamespace: f.namespace}
			original, err := resolver.ResolveDemand(t.Context(), f.service, nil, f.root.Name)
			if err != nil {
				t.Fatal(err)
			}
			if tt.mutate != nil {
				tt.mutate(t, &f, cl)
			}
			if diff := cmp.Diff(tt.wantInvalid, original.Check(t.Context()) != nil); diff != "" {
				t.Fatalf("invalidated observation (-want +got):\n%s", diff)
			}
			fresh, err := resolver.ResolveDemand(t.Context(), f.service, nil, f.root.Name)
			if diff := cmp.Diff(tt.wantFreshError, err != nil); diff != "" {
				t.Fatalf("fresh resolution error (-want +got):\n%s\nerror: %v", diff, err)
			}
			if tt.wantFreshError {
				if diff := cmp.Diff(true, fresh == nil); diff != "" {
					t.Fatalf("partial demand (-want +got):\n%s", diff)
				}
				return
			}
			if diff := cmp.Diff(tt.wantFingerprint, original.Demand.Fingerprint != fresh.Demand.Fingerprint); diff != "" {
				t.Fatalf("changed demand fingerprint (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolveDemandRejectsConcurrentChanges(t *testing.T) {
	for _, target := range []string{"root", "flavor"} {
		t.Run(target, func(t *testing.T) {
			f := newDemandFixture()
			changed := false
			cl := unitClient(t, f.runtimeFixture, interceptor.Funcs{List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if err := cl.List(ctx, list, opts...); err != nil {
					return err
				}
				if _, ok := list.(*kueuev1beta2.ResourceFlavorList); ok && !changed {
					changed = true
					if target == "root" {
						updateDemandInput(t, cl, f.root, func(root *v1beta1.AcceleratorQuota) { root.Status.Capacity[0].Allocatable = resource.MustParse("8") })
					} else {
						updateDemandInput(t, cl, f.flavors[0], func(flavor *kueuev1beta2.ResourceFlavor) { flavor.Spec.NodeLabels["zone"] = "east" })
					}
				}
				return nil
			}})
			got, err := (Resolver{Client: cl, OperatorNamespace: f.namespace}).ResolveDemand(t.Context(), f.service, nil, f.root.Name)
			if err == nil {
				t.Fatal("changed observation was accepted")
			}
			if diff := cmp.Diff(true, got == nil); diff != "" {
				t.Fatalf("partial demand (-want +got):\n%s", diff)
			}
		})
	}
}
