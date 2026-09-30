package capacity

import (
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	quotacapacity "sigs.k8s.io/ome/pkg/quota/capacity"
)

func flavorCatalog() []quotacapacity.Flavor {
	return []quotacapacity.Flavor{
		{Name: "gpu-a", UID: "a-uid", NodeLabels: map[string]string{"hardware": "a"}},
		{Name: "gpu-b", UID: "b-uid", NodeLabels: map[string]string{"hardware": "b"}},
	}
}

func flavorReports(flavors []quotacapacity.Flavor) []v1beta1.AcceleratorCapacityStatus {
	resources := []string{demandGPU, demandTPU}
	hash := quotacapacity.MappingFingerprint(resources, flavors)
	var reports []v1beta1.AcceleratorCapacityStatus
	for _, name := range resources {
		for _, flavor := range flavors {
			reports = append(reports, v1beta1.AcceleratorCapacityStatus{ResourceName: name, ResourceFlavor: flavor.Name,
				Attribution: &v1beta1.AcceleratorCapacityAttribution{FlavorUID: flavor.UID, NodeLabels: flavor.NodeLabels, FlavorSetHash: hash, Complete: true}})
		}
	}
	return reports
}

func flavorUnit(t *testing.T, spec ReplicaUnit) UnitDemand {
	t.Helper()
	unit, err := MeasureUnit(spec, []string{demandGPU, demandTPU})
	if err != nil {
		t.Fatal(err)
	}
	return unit
}

func requiredHardware(values ...string) *corev1.Affinity {
	return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "hardware", Operator: corev1.NodeSelectorOpIn, Values: values}}}},
	}}}
}

func TestAttributeUnit(t *testing.T) {
	type pool struct{ Resource, Flavor, Quantity string }
	for _, tt := range []struct {
		name    string
		mutate  func(*ReplicaUnit, *[]quotacapacity.Flavor)
		want    []pool
		wantErr string
	}{
		{name: "explicit hardware", want: []pool{{demandGPU, "gpu-a", "2"}}},
		{name: "model readiness is a scheduling constraint", want: []pool{{demandGPU, "gpu-a", "2"}}, mutate: func(u *ReplicaUnit, _ *[]quotacapacity.Flavor) {
			u.Engine[0].Spec.NodeSelector["models.example.com/ready"] = "true"
		}},
		{name: "shared engine and decoder pool", want: []pool{{demandGPU, "gpu-a", "6"}}, mutate: func(u *ReplicaUnit, _ *[]quotacapacity.Flavor) {
			u.Decoder = []PodSet{demandSet("primary", 1, gpuContainer("runner", "4"))}
			u.Decoder[0].Spec.NodeSelector = map[string]string{"hardware": "a"}
		}},
		{name: "workers contribute once", want: []pool{{demandGPU, "gpu-a", "14"}}, mutate: func(u *ReplicaUnit, _ *[]quotacapacity.Flavor) {
			u.Engine = append(u.Engine, demandSet("workers", 4, gpuContainer("runner", "3")))
			u.Engine[1].Spec.NodeSelector = map[string]string{"hardware": "a"}
		}},
		{name: "distinct component pools", want: []pool{{demandGPU, "gpu-a", "2"}, {demandGPU, "gpu-b", "4"}}, mutate: func(u *ReplicaUnit, _ *[]quotacapacity.Flavor) {
			u.Decoder = []PodSet{demandSet("primary", 1, gpuContainer("runner", "4"))}
			u.Decoder[0].Spec.NodeSelector = map[string]string{"hardware": "b"}
		}},
		{name: "resource units remain separate", want: []pool{{demandGPU, "gpu-a", "2"}, {demandTPU, "gpu-a", "4"}}, mutate: func(u *ReplicaUnit, _ *[]quotacapacity.Flavor) {
			u.Engine[0].Spec.Containers = []corev1.Container{demandContainer("runner", map[string]string{demandGPU: "2", demandTPU: "4"})}
		}},
		{name: "cpu-only component needs no pool", want: []pool{{demandGPU, "gpu-a", "2"}}, mutate: func(u *ReplicaUnit, _ *[]quotacapacity.Flavor) {
			u.Decoder = []PodSet{demandSet("primary", 1, demandContainer("runner", nil))}
		}},
		{name: "required affinity selects flavor", want: []pool{{demandGPU, "gpu-a", "2"}}, mutate: func(u *ReplicaUnit, _ *[]quotacapacity.Flavor) {
			u.Engine[0].Spec.NodeSelector = nil
			u.Engine[0].Spec.Affinity = requiredHardware("a")
		}},
		{name: "catch-all remains ambiguous", wantErr: "multiple", mutate: func(_ *ReplicaUnit, f *[]quotacapacity.Flavor) {
			*f = append(*f, quotacapacity.Flavor{Name: "specific", UID: "specific-uid", NodeLabels: map[string]string{"hardware": "a", "zone": "one"}})
		}},
		{name: "specific label resolves nested pool", want: []pool{{demandGPU, "specific", "2"}}, mutate: func(u *ReplicaUnit, f *[]quotacapacity.Flavor) {
			*f = append(*f, quotacapacity.Flavor{Name: "specific", UID: "specific-uid", NodeLabels: map[string]string{"hardware": "a", "zone": "one"}})
			u.Engine[0].Spec.NodeSelector["zone"] = "one"
		}},
		{name: "unselected flavors", wantErr: "multiple", mutate: func(u *ReplicaUnit, _ *[]quotacapacity.Flavor) {
			u.Engine[0].Spec.NodeSelector = nil
		}},
		{name: "affinity alternatives", wantErr: "multiple", mutate: func(u *ReplicaUnit, _ *[]quotacapacity.Flavor) {
			u.Engine[0].Spec.NodeSelector = nil
			u.Engine[0].Spec.Affinity = requiredHardware("a", "b")
		}},
		{name: "no compatible flavor", wantErr: "no compatible", mutate: func(u *ReplicaUnit, _ *[]quotacapacity.Flavor) {
			u.Engine[0].Spec.NodeSelector["hardware"] = "missing"
		}},
		{name: "preferred affinity does not select hardware", wantErr: "multiple", mutate: func(u *ReplicaUnit, _ *[]quotacapacity.Flavor) {
			u.Engine[0].Spec.NodeSelector = nil
			u.Engine[0].Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{Weight: 1, Preference: requiredHardware("a").NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0]}}}}
		}},
		{name: "sole catch-all flavor", want: []pool{{demandGPU, "all", "2"}}, mutate: func(_ *ReplicaUnit, f *[]quotacapacity.Flavor) {
			*f = []quotacapacity.Flavor{{Name: "all", UID: "all-uid"}}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			spec, flavors := demandUnit(), flavorCatalog()
			spec.Engine[0].Spec.NodeSelector = map[string]string{"hardware": "a"}
			if tt.mutate != nil {
				tt.mutate(&spec, &flavors)
			}
			unit, reports := flavorUnit(t, spec), flavorReports(flavors)
			before := demandJSON(t, []any{unit, flavors, reports})
			got, err := AttributeUnit(unit, flavors, reports)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want %q", err, tt.wantErr)
				}
				if diff := cmp.Diff(Demand{}, got); diff != "" {
					t.Fatalf("partial demand (-want +got):\n%s", diff)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var actual []pool
			for _, p := range got.Pools {
				actual = append(actual, pool{p.ResourceName, p.ResourceFlavor, p.Quantity.String()})
			}
			if diff := cmp.Diff(tt.want, actual); diff != "" {
				t.Fatalf("pools (-want +got):\n%s", diff)
			}
			if got.Fingerprint == "" {
				t.Fatal("missing fingerprint")
			}
			for i := range got.Pools {
				if got.Pools[i].NodeLabels == nil {
					got.Pools[i].NodeLabels = map[string]string{}
				}
				got.Pools[i].NodeLabels["unrelated"] = "changed"
				got.Pools[i].Quantity.Set(0)
			}
			if diff := cmp.Diff(before, demandJSON(t, []any{unit, flavors, reports})); diff != "" {
				t.Fatalf("input mutation (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAttributeUnitRejectsUnverifiedInputs(t *testing.T) {
	for _, tt := range []struct {
		name    string
		mutate  func(*UnitDemand, *[]quotacapacity.Flavor, *[]v1beta1.AcceleratorCapacityStatus)
		wantErr string
	}{
		{name: "missing unit", wantErr: "measured", mutate: func(u *UnitDemand, _ *[]quotacapacity.Flavor, _ *[]v1beta1.AcceleratorCapacityStatus) {
			*u = UnitDemand{}
		}},
		{name: "missing catalog", wantErr: "catalog is empty", mutate: func(_ *UnitDemand, f *[]quotacapacity.Flavor, _ *[]v1beta1.AcceleratorCapacityStatus) {
			*f = nil
		}},
		{name: "unidentified flavor", wantErr: "identity", mutate: func(_ *UnitDemand, f *[]quotacapacity.Flavor, _ *[]v1beta1.AcceleratorCapacityStatus) {
			(*f)[0].UID = ""
		}},
		{name: "duplicate flavor", wantErr: "duplicate hardware", mutate: func(_ *UnitDemand, f *[]quotacapacity.Flavor, _ *[]v1beta1.AcceleratorCapacityStatus) {
			*f = append(*f, (*f)[0])
		}},
		{name: "empty label value", wantErr: "label presence", mutate: func(_ *UnitDemand, f *[]quotacapacity.Flavor, _ *[]v1beta1.AcceleratorCapacityStatus) {
			(*f)[0].NodeLabels["hardware"] = ""
		}},
		{name: "missing reports", wantErr: "no identified", mutate: func(_ *UnitDemand, _ *[]quotacapacity.Flavor, r *[]v1beta1.AcceleratorCapacityStatus) {
			*r = nil
		}},
		{name: "duplicate report", wantErr: "duplicate capacity", mutate: func(_ *UnitDemand, _ *[]quotacapacity.Flavor, r *[]v1beta1.AcceleratorCapacityStatus) {
			*r = append(*r, (*r)[0])
		}},
		{name: "unattributed row", wantErr: "incomplete or changed", mutate: func(_ *UnitDemand, _ *[]quotacapacity.Flavor, r *[]v1beta1.AcceleratorCapacityStatus) {
			(*r)[0].Attribution = nil
		}},
		{name: "changed catalog", wantErr: "unverified local", mutate: func(_ *UnitDemand, f *[]quotacapacity.Flavor, _ *[]v1beta1.AcceleratorCapacityStatus) {
			(*f)[0].UID = "replacement"
		}},
		{name: "unknown report flavor", wantErr: "unverified local", mutate: func(_ *UnitDemand, _ *[]quotacapacity.Flavor, r *[]v1beta1.AcceleratorCapacityStatus) {
			(*r)[0].ResourceFlavor = "unknown"
		}},
		{name: "fleet row", wantErr: "unverified local", mutate: func(_ *UnitDemand, _ *[]quotacapacity.Flavor, r *[]v1beta1.AcceleratorCapacityStatus) {
			(*r)[0].PerCluster = []v1beta1.AcceleratorClusterCapacityStatus{{Cluster: "member-a"}}
		}},
		{name: "missing matrix row", wantErr: "incomplete or changed", mutate: func(_ *UnitDemand, _ *[]quotacapacity.Flavor, r *[]v1beta1.AcceleratorCapacityStatus) {
			*r = (*r)[1:]
		}},
		{name: "stale mapping hash", wantErr: "incomplete or changed", mutate: func(_ *UnitDemand, _ *[]quotacapacity.Flavor, r *[]v1beta1.AcceleratorCapacityStatus) {
			(*r)[0].Attribution.FlavorSetHash = "old"
		}},
		{name: "incomplete resource attribution", wantErr: "complete hardware", mutate: func(_ *UnitDemand, _ *[]quotacapacity.Flavor, r *[]v1beta1.AcceleratorCapacityStatus) {
			(*r)[0].Attribution.Complete = false
		}},
		{name: "nil template", wantErr: "invalid measured", mutate: func(u *UnitDemand, _ *[]quotacapacity.Flavor, _ *[]v1beta1.AcceleratorCapacityStatus) {
			u.Pods[0].Spec = nil
		}},
		{name: "duplicate pod set", wantErr: "invalid measured", mutate: func(u *UnitDemand, _ *[]quotacapacity.Flavor, _ *[]v1beta1.AcceleratorCapacityStatus) {
			u.Pods = append(u.Pods, u.Pods[0])
		}},
		{name: "invalid component", wantErr: "invalid measured", mutate: func(u *UnitDemand, _ *[]quotacapacity.Flavor, _ *[]v1beta1.AcceleratorCapacityStatus) {
			u.Pods[0].Component = v1beta1.RouterComponent
		}},
		{name: "no accelerator demand", wantErr: "no accelerator pools", mutate: func(u *UnitDemand, _ *[]quotacapacity.Flavor, _ *[]v1beta1.AcceleratorCapacityStatus) {
			u.Pods[0].Requests = nil
		}},
		{name: "fractional demand", wantErr: "invalid accelerator", mutate: func(u *UnitDemand, _ *[]quotacapacity.Flavor, _ *[]v1beta1.AcceleratorCapacityStatus) {
			u.Pods[0].Requests[demandGPU] = resource.MustParse("1.5")
		}},
		{name: "missing resource in report", wantErr: "complete hardware", mutate: func(u *UnitDemand, _ *[]quotacapacity.Flavor, _ *[]v1beta1.AcceleratorCapacityStatus) {
			u.Pods[0].Requests = corev1.ResourceList{"example.com/other": resource.MustParse("2")}
		}},
		{name: "combined overflow", wantErr: "combined demand", mutate: func(u *UnitDemand, _ *[]quotacapacity.Flavor, _ *[]v1beta1.AcceleratorCapacityStatus) {
			p := u.Pods[0]
			p.Name = "workers"
			p.Requests = corev1.ResourceList{demandGPU: *resource.NewQuantity(math.MaxInt64, resource.DecimalSI)}
			u.Pods = append(u.Pods, p)
		}},
		{name: "invalid affinity", wantErr: "Unsupported value", mutate: func(u *UnitDemand, _ *[]quotacapacity.Flavor, _ *[]v1beta1.AcceleratorCapacityStatus) {
			u.Pods[0].Spec.Affinity = requiredHardware("a")
			u.Pods[0].Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Operator = "Unknown"
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			spec := demandUnit()
			spec.Engine[0].Spec.NodeSelector = map[string]string{"hardware": "a"}
			unit, flavors := flavorUnit(t, spec), flavorCatalog()
			reports := flavorReports(flavors)
			tt.mutate(&unit, &flavors, &reports)
			got, err := AttributeUnit(unit, flavors, reports)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want %q", err, tt.wantErr)
			}
			if diff := cmp.Diff(Demand{}, got); diff != "" {
				t.Fatalf("partial result (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAttributedDemandFingerprint(t *testing.T) {
	for _, tt := range []struct {
		name    string
		mutate  func(*UnitDemand, *[]quotacapacity.Flavor, *[]v1beta1.AcceleratorCapacityStatus)
		changed bool
	}{
		{name: "ordering", mutate: func(u *UnitDemand, f *[]quotacapacity.Flavor, r *[]v1beta1.AcceleratorCapacityStatus) {
			slices.Reverse(u.Pods)
			slices.Reverse(*f)
			slices.Reverse(*r)
		}},
		{name: "hardware amount", mutate: func(_ *UnitDemand, _ *[]quotacapacity.Flavor, r *[]v1beta1.AcceleratorCapacityStatus) {
			(*r)[0].Allocatable = resource.MustParse("100")
		}},
		{name: "historical unmapped pool", mutate: func(_ *UnitDemand, _ *[]quotacapacity.Flavor, r *[]v1beta1.AcceleratorCapacityStatus) {
			*r = append(*r, v1beta1.AcceleratorCapacityStatus{ResourceName: demandGPU, ResourceFlavor: "retired"})
		}},
		{name: "rendered inputs", changed: true, mutate: func(u *UnitDemand, _ *[]quotacapacity.Flavor, _ *[]v1beta1.AcceleratorCapacityStatus) {
			u.Fingerprint = "changed"
		}},
		{name: "other flavor identity", changed: true, mutate: func(_ *UnitDemand, f *[]quotacapacity.Flavor, r *[]v1beta1.AcceleratorCapacityStatus) {
			(*f)[1].UID = "new"
			*r = flavorReports(*f)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			spec := demandUnit()
			spec.Engine[0].Spec.NodeSelector = map[string]string{"hardware": "a"}
			unit, flavors := flavorUnit(t, spec), flavorCatalog()
			reports := flavorReports(flavors)
			before, err := AttributeUnit(unit, flavors, reports)
			if err != nil {
				t.Fatal(err)
			}
			tt.mutate(&unit, &flavors, &reports)
			after, err := AttributeUnit(unit, flavors, reports)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.changed, before.Fingerprint != after.Fingerprint); diff != "" {
				t.Fatalf("fingerprint changed (-want +got):\n%s", diff)
			}
		})
	}
}
