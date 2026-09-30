package resolution

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/capacity"
	quotacapacity "sigs.k8s.io/ome/pkg/quota/capacity"
)

type demandFixture struct {
	runtimeFixture
	root    *v1beta1.AcceleratorQuota
	flavors []*kueuev1beta2.ResourceFlavor
}

func newDemandFixture() demandFixture {
	f, _ := newUnitFixture()
	f.service.Spec.Engine.NodeSelector = map[string]string{"hardware": "a"}
	d := demandFixture{runtimeFixture: f, root: &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "capacity-root", UID: "root-uid", ResourceVersion: "1"},
		Spec:       v1beta1.AcceleratorQuotaSpec{Role: v1beta1.AcceleratorQuotaRoleCohort},
	}, flavors: []*kueuev1beta2.ResourceFlavor{
		{ObjectMeta: metav1.ObjectMeta{Name: "gpu-a", UID: "flavor-a", ResourceVersion: "1"}, Spec: kueuev1beta2.ResourceFlavorSpec{NodeLabels: map[string]string{"hardware": "a"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "gpu-b", UID: "flavor-b", ResourceVersion: "1"}, Spec: kueuev1beta2.ResourceFlavorSpec{NodeLabels: map[string]string{"hardware": "b"}}},
	}}
	d.report()
	d.extra = append(d.extra, d.root)
	for _, flavor := range d.flavors {
		d.extra = append(d.extra, flavor)
	}
	return d
}

func (f *demandFixture) report() {
	var flavors []quotacapacity.Flavor
	for _, flavor := range f.flavors {
		flavors = append(flavors, quotacapacity.Flavor{Name: flavor.Name, UID: flavor.UID, NodeLabels: flavor.Spec.NodeLabels})
	}
	hash := quotacapacity.MappingFingerprint([]string{unitGPU}, flavors)
	f.root.Status.Capacity = nil
	for _, flavor := range flavors {
		f.root.Status.Capacity = append(f.root.Status.Capacity, v1beta1.AcceleratorCapacityStatus{
			ResourceName: unitGPU, ResourceFlavor: flavor.Name, Allocatable: resource.MustParse("16"),
			Attribution: &v1beta1.AcceleratorCapacityAttribution{FlavorUID: flavor.UID, NodeLabels: maps.Clone(flavor.NodeLabels), FlavorSetHash: hash, Complete: true},
		})
	}
}

func TestResolveDemand(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*demandFixture)
		want   string
	}{
		{name: "identified root and catalog", want: "2"},
		{name: "combined engine and decoder", want: "6", mutate: func(f *demandFixture) {
			f.service.Spec.Decoder = &v1beta1.DecoderSpec{}
			f.service.Spec.Decoder.NodeSelector = map[string]string{"hardware": "a"}
			f.runtime.Spec.DecoderConfig.Runner = unitRunner("4")
		}},
		{name: "leader and workers", want: "14", mutate: func(f *demandFixture) { unitGang(&f.runtimeFixture) }},
		{name: "model readiness stays in the pod", want: "2", mutate: func(f *demandFixture) { f.model() }},
		{name: "verified zero hardware still maps demand", want: "2", mutate: func(f *demandFixture) {
			f.root.Status.Capacity[0].Allocatable = resource.MustParse("0")
		}},
		{name: "quota and high water do not size demand", want: "2", mutate: func(f *demandFixture) {
			f.root.Spec.Budgets = []v1beta1.AcceleratorBudget{{ResourceName: unitGPU, ResourceFlavor: "gpu-b", Nominal: resource.MustParse("999")}}
			f.root.Status.Capacity[0].HighWaterMark = resource.MustParse("999")
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newDemandFixture()
			if tt.mutate != nil {
				tt.mutate(&f)
			}
			before := unitJSON(t, []any{f.service, f.runtime, f.extra})
			writes := 0
			reject := func() error { writes++; return errors.New("unexpected object write") }
			cl := unitClient(t, f.runtimeFixture, interceptor.Funcs{
				Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error { return reject() },
				Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error { return reject() },
				Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
					return reject()
				},
				Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error { return reject() },
			})
			got, err := (Resolver{Client: cl, OperatorNamespace: f.namespace}).ResolveDemand(t.Context(), f.service, nil, f.root.Name)
			if err != nil {
				t.Fatal(err)
			}
			want := []capacity.Pool{{ResourceName: unitGPU, ResourceFlavor: "gpu-a", Quantity: resource.MustParse(tt.want), FlavorUID: "flavor-a",
				NodeLabels: map[string]string{"hardware": "a"}, FlavorSetHash: f.root.Status.Capacity[0].Attribution.FlavorSetHash}}
			if diff := cmp.Diff(unitJSON(t, want), unitJSON(t, got.Demand.Pools)); diff != "" {
				t.Fatalf("resolved pools (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(f.root.ObjectMeta, got.Report); diff != "" {
				t.Fatalf("report provenance (-want +got):\n%s", diff)
			}
			contract := &v1beta1.PlacementDemandContract{Fingerprint: got.Demand.Fingerprint}
			for _, component := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
				if hash, exists := got.Unit.Rendering[component]; exists {
					contract.Components = append(contract.Components, v1beta1.PlacementComponentDemand{Component: component, RenderingHash: hash})
				}
			}
			if diff := cmp.Diff(contract, got.Contract); diff != "" {
				t.Fatalf("execution contract (-want +got):\n%s", diff)
			}
			if err := got.Check(t.Context()); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(0, writes); diff != "" {
				t.Fatalf("object writes (-want +got):\n%s", diff)
			}
			got.Demand.Pools[0].NodeLabels["hardware"] = "mutated"
			got.Report.Labels = map[string]string{"changed": "true"}
			got.Hardware[0].Attribution.NodeLabels["hardware"] = "mutated"
			got.Contract.Components[0].RenderingHash = "mutated"
			if diff := cmp.Diff(contract.Components[0].RenderingHash, got.Unit.Rendering[v1beta1.EngineComponent]); diff != "" {
				t.Fatalf("contract aliases unit rendering:\n%s", diff)
			}
			if diff := cmp.Diff(before, unitJSON(t, []any{f.service, f.runtime, f.extra})); diff != "" {
				t.Fatalf("mutated inputs (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolveDemandRejectsUnknownInputs(t *testing.T) {
	for _, tt := range []struct {
		name    string
		mutate  func(*demandFixture)
		root    string
		wantErr string
	}{
		{name: "root name required", wantErr: "root name"},
		{name: "missing root", root: "missing", wantErr: "not found"},
		{name: "missing rendering", root: "capacity-root", wantErr: "not found", mutate: func(f *demandFixture) { f.service.Spec.Runtime.Name = "missing" }},
		{name: "child cohort is not a root", root: "capacity-root", wantErr: "local quota root", mutate: func(f *demandFixture) { f.root.Spec.ParentRef = &v1beta1.AcceleratorQuotaParentRef{Name: "parent"} }},
		{name: "leaf is not a root", root: "capacity-root", wantErr: "local quota root", mutate: func(f *demandFixture) { f.root.Spec.Role = v1beta1.AcceleratorQuotaRoleClusterQueue }},
		{name: "projected cohort is not a local root", root: "capacity-root", wantErr: "local quota root", mutate: func(f *demandFixture) { f.root.Labels = map[string]string{v1beta1.AcceleratorQuotaOriginLabel: ""} }},
		{name: "root identity missing", root: "capacity-root", wantErr: "live identity", mutate: func(f *demandFixture) { f.root.UID = "" }},
		{name: "flavor identity missing", root: "capacity-root", wantErr: "live identity", mutate: func(f *demandFixture) { f.flavors[0].UID = "" }},
		{name: "ambiguous hardware", root: "capacity-root", wantErr: "multiple", mutate: func(f *demandFixture) { f.service.Spec.Engine.NodeSelector = nil }},
		{name: "missing attribution", root: "capacity-root", wantErr: "incomplete or changed", mutate: func(f *demandFixture) { f.root.Status.Capacity[0].Attribution = nil }},
		{name: "fleet report cannot size member", root: "capacity-root", wantErr: "unverified local", mutate: func(f *demandFixture) {
			f.root.Status.Capacity[0].PerCluster = []v1beta1.AcceleratorClusterCapacityStatus{{Cluster: "member-a"}}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newDemandFixture()
			if tt.mutate != nil {
				tt.mutate(&f)
			}
			got, err := (Resolver{Client: unitClient(t, f.runtimeFixture, interceptor.Funcs{}), OperatorNamespace: f.namespace}).ResolveDemand(t.Context(), f.service, nil, tt.root)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want %q", err, tt.wantErr)
			}
			if diff := cmp.Diff(true, got == nil); diff != "" {
				t.Fatalf("no partial demand (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolveDemandReadFailures(t *testing.T) {
	for _, tt := range []struct {
		name    string
		root    bool
		read    int
		partial bool
	}{
		{name: "root read", root: true, read: 1},
		{name: "root recheck", root: true, read: 2},
		{name: "catalog read", read: 1},
		{name: "catalog recheck", read: 2},
		{name: "partial catalog", read: 1, partial: true},
		{name: "partial catalog recheck", read: 2, partial: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newDemandFixture()
			reads := 0
			readErr := errors.New("capacity read failed")
			cl := unitClient(t, f.runtimeFixture, interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
					if _, ok := object.(*v1beta1.AcceleratorQuota); ok && tt.root {
						reads++
						if reads == tt.read {
							return readErr
						}
					}
					return cl.Get(ctx, key, object, opts...)
				},
				List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if err := cl.List(ctx, list, opts...); err != nil {
						return err
					}
					if _, ok := list.(*kueuev1beta2.ResourceFlavorList); ok && !tt.root {
						reads++
						if reads == tt.read {
							if tt.partial {
								list.SetContinue("next-page")
							} else {
								return readErr
							}
						}
					}
					return nil
				},
			})
			got, err := (Resolver{Client: cl, OperatorNamespace: f.namespace}).ResolveDemand(t.Context(), f.service, nil, f.root.Name)
			if tt.partial {
				if err == nil || !strings.Contains(err.Error(), "incomplete") {
					t.Fatalf("error = %v, want incomplete catalog", err)
				}
			} else if !errors.Is(err, readErr) {
				t.Fatalf("error = %v, want %v", err, readErr)
			}
			if diff := cmp.Diff(true, got == nil); diff != "" {
				t.Fatalf("no partial demand (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolvedDemandRequiresObservation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		demand *ResolvedDemand
	}{
		{name: "nil"},
		{name: "empty", demand: &ResolvedDemand{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.demand.Check(t.Context()); err == nil {
				t.Fatal("unobserved demand was accepted")
			}
		})
	}
}
