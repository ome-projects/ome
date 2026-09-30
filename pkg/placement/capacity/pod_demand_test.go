package capacity

import (
	"encoding/json"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const (
	demandGPU = "example.com/gpu"
	demandTPU = "example.com/tpu"
)

func demandContainer(name string, requests map[string]string) corev1.Container {
	values := corev1.ResourceList{}
	for name, quantity := range requests {
		values[corev1.ResourceName(name)] = resource.MustParse(quantity)
	}
	return corev1.Container{Name: name, Image: "example.com/serving:v1", Resources: corev1.ResourceRequirements{Requests: values.DeepCopy(), Limits: values}}
}

func gpuContainer(name, quantity string) corev1.Container {
	return demandContainer(name, map[string]string{demandGPU: quantity})
}

func demandSet(name string, count int64, containers ...corev1.Container) PodSet {
	return PodSet{Name: name, Count: count, Spec: &corev1.PodSpec{Containers: containers}}
}

func demandUnit() ReplicaUnit {
	return ReplicaUnit{InputFingerprint: "resolved-inputs", Engine: []PodSet{demandSet("primary", 1, gpuContainer("runner", "2"))}}
}

func demandJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func TestMeasureUnitAccounting(t *testing.T) {
	type measured struct {
		Component v1beta1.ComponentType
		Name      string
		Count     int64
		Requests  map[string]string
	}
	primary := func(gpu string) []measured {
		return []measured{{Component: v1beta1.EngineComponent, Name: "primary", Count: 1, Requests: map[string]string{demandGPU: gpu}}}
	}
	for _, tt := range []struct {
		name   string
		mutate func(*ReplicaUnit)
		want   []measured
	}{
		{name: "single container", want: primary("2")},
		{name: "regular containers run concurrently", want: primary("5"), mutate: func(unit *ReplicaUnit) {
			unit.Engine[0].Spec.Containers = append(unit.Engine[0].Spec.Containers, gpuContainer("sidecar", "3"))
		}},
		{name: "missing request defaults to limit", want: primary("2"), mutate: func(unit *ReplicaUnit) {
			unit.Engine[0].Spec.Containers[0].Resources.Requests = nil
		}},
		{name: "sequential init peak exceeds app", want: primary("8"), mutate: func(unit *ReplicaUnit) {
			unit.Engine[0].Spec.InitContainers = []corev1.Container{gpuContainer("init-a", "8"), gpuContainer("init-b", "3")}
		}},
		{name: "app exceeds sequential init peak", want: primary("2"), mutate: func(unit *ReplicaUnit) {
			unit.Engine[0].Spec.InitContainers = []corev1.Container{gpuContainer("init-a", "1"), gpuContainer("init-b", "1")}
		}},
		{name: "running sidecar overlaps later init", want: primary("8"), mutate: func(unit *ReplicaUnit) {
			a, b := gpuContainer("sidecar-a", "2"), gpuContainer("sidecar-b", "3")
			a.RestartPolicy, b.RestartPolicy = ptr.To(corev1.ContainerRestartPolicyAlways), ptr.To(corev1.ContainerRestartPolicyAlways)
			unit.Engine[0].Spec.InitContainers = []corev1.Container{a, gpuContainer("init", "6"), b}
		}},
		{name: "later sidecars do not overlap earlier init", want: primary("7"), mutate: func(unit *ReplicaUnit) {
			a, b := gpuContainer("sidecar-a", "2"), gpuContainer("sidecar-b", "3")
			a.RestartPolicy, b.RestartPolicy = ptr.To(corev1.ContainerRestartPolicyAlways), ptr.To(corev1.ContainerRestartPolicyAlways)
			unit.Engine[0].Spec.InitContainers = []corev1.Container{gpuContainer("init", "6"), a, b}
		}},
		{name: "init limits receive request defaulting", want: primary("8"), mutate: func(unit *ReplicaUnit) {
			init := gpuContainer("init", "8")
			init.Resources.Requests = nil
			unit.Engine[0].Spec.InitContainers = []corev1.Container{init}
		}},
		{name: "overhead applies after initialization peak", want: primary("9"), mutate: func(unit *ReplicaUnit) {
			unit.Engine[0].Spec.InitContainers = []corev1.Container{gpuContainer("init", "8")}
			unit.Engine[0].Spec.Overhead = corev1.ResourceList{demandGPU: resource.MustParse("1")}
		}},
		{name: "separate accelerator resources retain separate peaks", want: []measured{{Component: v1beta1.EngineComponent, Name: "primary", Count: 1, Requests: map[string]string{demandGPU: "8", demandTPU: "4"}}}, mutate: func(unit *ReplicaUnit) {
			unit.Engine[0].Spec.Containers = []corev1.Container{demandContainer("runner", map[string]string{demandGPU: "2", demandTPU: "4"})}
			unit.Engine[0].Spec.InitContainers = []corev1.Container{demandContainer("init", map[string]string{demandGPU: "8", demandTPU: "1"})}
		}},
		{name: "engine decoder and workers remain attributable", want: []measured{
			{Component: v1beta1.EngineComponent, Name: "primary", Count: 1, Requests: map[string]string{demandGPU: "2"}},
			{Component: v1beta1.EngineComponent, Name: "workers", Count: 3, Requests: map[string]string{demandGPU: "6"}},
			{Component: v1beta1.DecoderComponent, Name: "primary", Count: 1, Requests: map[string]string{demandGPU: "4"}},
		}, mutate: func(unit *ReplicaUnit) {
			unit.Engine = append(unit.Engine, demandSet("workers", 3, gpuContainer("runner", "2")))
			unit.Decoder = []PodSet{demandSet("primary", 1, gpuContainer("runner", "4"))}
		}},
		{name: "decoder alone", want: []measured{{Component: v1beta1.DecoderComponent, Name: "primary", Count: 1, Requests: map[string]string{demandGPU: "2"}}}, mutate: func(unit *ReplicaUnit) {
			unit.Decoder, unit.Engine = unit.Engine, nil
		}},
		{name: "cpu-only component contributes no accelerators", want: append(primary("2"), measured{Component: v1beta1.DecoderComponent, Name: "primary", Count: 1}), mutate: func(unit *ReplicaUnit) {
			unit.Decoder = []PodSet{demandSet("primary", 1, demandContainer("runner", map[string]string{"cpu": "500m", "memory": "2Gi"}))}
		}},
		{name: "zero accelerators do not form a pool", want: primary("2"), mutate: func(unit *ReplicaUnit) {
			unit.Engine[0].Spec.Containers = append(unit.Engine[0].Spec.Containers, gpuContainer("sidecar", "0"))
		}},
		{name: "decimal quantities are exact", want: primary("2"), mutate: func(unit *ReplicaUnit) {
			for _, values := range []corev1.ResourceList{unit.Engine[0].Spec.Containers[0].Resources.Requests, unit.Engine[0].Spec.Containers[0].Resources.Limits} {
				q := values[demandGPU]
				q.ToDec()
				values[demandGPU] = q
			}
		}},
		{name: "maximum whole quantity", want: primary("9223372036854775807"), mutate: func(unit *ReplicaUnit) {
			unit.Engine[0].Spec.Containers = []corev1.Container{gpuContainer("runner", "9223372036854775807")}
		}},
		{name: "maximum exact multiplication", want: []measured{{Component: v1beta1.EngineComponent, Name: "primary", Count: math.MaxInt64, Requests: map[string]string{demandGPU: "9223372036854775807"}}}, mutate: func(unit *ReplicaUnit) {
			unit.Engine[0].Count = math.MaxInt64
			unit.Engine[0].Spec.Containers[0] = gpuContainer("runner", "1")
		}},
		{name: "native pod resource budget does not replace accelerators", want: primary("2"), mutate: func(unit *ReplicaUnit) {
			unit.Engine[0].Spec.Resources = &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}
		}},
		{name: "unconfigured zero resource needs no pool", want: primary("2"), mutate: func(unit *ReplicaUnit) {
			unit.Engine[0].Spec.Containers[0].Resources.Limits["example.com/unused"] = resource.MustParse("0")
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			unit := demandUnit()
			if tt.mutate != nil {
				tt.mutate(&unit)
			}
			before := demandJSON(t, unit)
			got, err := MeasureUnit(unit, []string{demandGPU, demandTPU})
			if err != nil {
				t.Fatal(err)
			}
			var measuredPods []measured
			for _, pod := range got.Pods {
				m := measured{Component: pod.Component, Name: pod.Name, Count: pod.Count}
				for name, quantity := range pod.Requests {
					if m.Requests == nil {
						m.Requests = map[string]string{}
					}
					m.Requests[string(name)] = quantity.String()
				}
				measuredPods = append(measuredPods, m)
			}
			if diff := cmp.Diff(tt.want, measuredPods); diff != "" {
				t.Fatalf("pod demand (-want +got):\n%s", diff)
			}
			if got.Fingerprint == "" {
				t.Fatal("missing demand fingerprint")
			}
			got.Pods[0].Spec.Containers[0].Image = "changed"
			for name := range got.Pods[0].Requests {
				got.Pods[0].Requests[name] = resource.MustParse("1")
			}
			if diff := cmp.Diff(before, demandJSON(t, unit)); diff != "" {
				t.Fatalf("input mutation (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMeasureUnitRejectsUnresolvedDemand(t *testing.T) {
	for _, tt := range []struct {
		name      string
		mutate    func(*ReplicaUnit, *[]string)
		wantError string
	}{
		{name: "rendering identity missing", wantError: "rendering inputs", mutate: func(u *ReplicaUnit, _ *[]string) { u.InputFingerprint = "" }},
		{name: "components absent", wantError: "component pod sets", mutate: func(u *ReplicaUnit, _ *[]string) { u.Engine = nil }},
		{name: "unconfigured accelerators", wantError: "must be configured", mutate: func(_ *ReplicaUnit, r *[]string) { *r = nil }},
		{name: "native resource configuration", wantError: "not an extended resource", mutate: func(_ *ReplicaUnit, r *[]string) { *r = []string{"cpu"} }},
		{name: "invalid resource configuration", wantError: "not an extended resource", mutate: func(_ *ReplicaUnit, r *[]string) { *r = []string{"bad name/gpu"} }},
		{name: "empty resource name", wantError: "not an extended resource", mutate: func(_ *ReplicaUnit, r *[]string) { *r = []string{""} }},
		{name: "reserved resource namespace", wantError: "not an extended resource", mutate: func(_ *ReplicaUnit, r *[]string) { *r = []string{"kubernetes.io/device"} }},
		{name: "reserved resource subdomain", wantError: "not an extended resource", mutate: func(_ *ReplicaUnit, r *[]string) { *r = []string{"devices.kubernetes.io/device"} }},
		{name: "quota request prefix", wantError: "not an extended resource", mutate: func(_ *ReplicaUnit, r *[]string) { *r = []string{"requests.example.com/gpu"} }},
		{name: "unnamed pod set", wantError: "requires a name", mutate: func(u *ReplicaUnit, _ *[]string) { u.Engine[0].Name = "" }},
		{name: "duplicate pod set", wantError: "duplicate engine", mutate: func(u *ReplicaUnit, _ *[]string) { u.Engine = append(u.Engine, u.Engine[0]) }},
		{name: "missing pod template", wantError: "rendered spec", mutate: func(u *ReplicaUnit, _ *[]string) { u.Engine[0].Spec = nil }},
		{name: "zero pod count", wantError: "positive count", mutate: func(u *ReplicaUnit, _ *[]string) { u.Engine[0].Count = 0 }},
		{name: "negative pod count", wantError: "positive count", mutate: func(u *ReplicaUnit, _ *[]string) { u.Engine[0].Count = -1 }},
		{name: "pod count multiplication overflow", wantError: "exceeds whole int64", mutate: func(u *ReplicaUnit, _ *[]string) { u.Engine[0].Count = math.MaxInt64 }},
		{name: "no regular containers", wantError: "requires regular containers", mutate: func(u *ReplicaUnit, _ *[]string) { u.Engine[0].Spec.Containers = nil }},
		{name: "ephemeral container in desired shape", wantError: "no ephemeral containers", mutate: func(u *ReplicaUnit, _ *[]string) {
			u.Engine[0].Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug"}}}
		}},
		{name: "pod dynamic claims", wantError: "dynamic resource claims", mutate: func(u *ReplicaUnit, _ *[]string) {
			u.Engine[0].Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: "device", ResourceClaimName: ptr.To("claim")}}
		}},
		{name: "container dynamic claims", wantError: "dynamic resource claims", mutate: func(u *ReplicaUnit, _ *[]string) {
			u.Engine[0].Spec.Containers[0].Resources.Claims = []corev1.ResourceClaim{{Name: "device"}}
		}},
		{name: "init dynamic claims", wantError: "dynamic resource claims", mutate: func(u *ReplicaUnit, _ *[]string) {
			init := gpuContainer("init", "1")
			init.Resources.Claims = []corev1.ResourceClaim{{Name: "device"}}
			u.Engine[0].Spec.InitContainers = []corev1.Container{init}
		}},
		{name: "pod-level dynamic claims", wantError: "dynamic resource claims", mutate: func(u *ReplicaUnit, _ *[]string) {
			u.Engine[0].Spec.Resources = &corev1.ResourceRequirements{Claims: []corev1.ResourceClaim{{Name: "device"}}}
		}},
		{name: "unsupported pod-level accelerator", wantError: "unsupported pod-level resource", mutate: func(u *ReplicaUnit, _ *[]string) {
			u.Engine[0].Spec.Resources = &corev1.ResourceRequirements{Limits: corev1.ResourceList{demandGPU: resource.MustParse("8")}}
		}},
		{name: "unconfigured container accelerator", wantError: "absent from accelerator configuration", mutate: func(u *ReplicaUnit, _ *[]string) {
			u.Engine[0].Spec.Containers = append(u.Engine[0].Spec.Containers, demandContainer("sidecar", map[string]string{"example.com/other": "2"}))
		}},
		{name: "unconfigured overhead", wantError: "pod overhead", mutate: func(u *ReplicaUnit, _ *[]string) {
			u.Engine[0].Spec.Overhead = corev1.ResourceList{"example.com/other": resource.MustParse("2")}
		}},
		{name: "negative request", wantError: "nonnegative whole units", mutate: func(u *ReplicaUnit, _ *[]string) {
			u.Engine[0].Spec.Containers[0] = gpuContainer("runner", "-1")
		}},
		{name: "negative init limit", wantError: "nonnegative whole units", mutate: func(u *ReplicaUnit, _ *[]string) {
			init := gpuContainer("init", "-1")
			init.Resources.Requests = nil
			u.Engine[0].Spec.InitContainers = []corev1.Container{init}
		}},
		{name: "fractional demand", wantError: "nonnegative whole units", mutate: func(u *ReplicaUnit, _ *[]string) {
			u.Engine[0].Spec.Containers[0] = gpuContainer("runner", "500m")
		}},
		{name: "quantity overflow", wantError: "nonnegative whole units", mutate: func(u *ReplicaUnit, _ *[]string) {
			u.Engine[0].Spec.Containers[0] = gpuContainer("runner", "9223372036854775808")
		}},
		{name: "sum overflow", wantError: "pod demand", mutate: func(u *ReplicaUnit, _ *[]string) {
			u.Engine[0].Spec.Containers = append(u.Engine[0].Spec.Containers, gpuContainer("sidecar", "9223372036854775807"))
		}},
		{name: "initialization peak overflow", wantError: "pod demand", mutate: func(u *ReplicaUnit, _ *[]string) {
			sidecar := gpuContainer("sidecar", "9223372036854775807")
			sidecar.RestartPolicy = ptr.To(corev1.ContainerRestartPolicyAlways)
			u.Engine[0].Spec.InitContainers = []corev1.Container{sidecar, gpuContainer("init", "1")}
		}},
		{name: "fractional overhead", wantError: "pod overhead", mutate: func(u *ReplicaUnit, _ *[]string) {
			u.Engine[0].Spec.Overhead = corev1.ResourceList{demandGPU: resource.MustParse("500m")}
		}},
		{name: "request without limit", wantError: "equal request and limit", mutate: func(u *ReplicaUnit, _ *[]string) {
			u.Engine[0].Spec.Containers[0].Resources.Limits = nil
		}},
		{name: "request below limit", wantError: "equal request and limit", mutate: func(u *ReplicaUnit, _ *[]string) {
			u.Engine[0].Spec.Containers[0].Resources.Limits[demandGPU] = resource.MustParse("3")
		}},
		{name: "explicit zero request with positive limit", wantError: "equal request and limit", mutate: func(u *ReplicaUnit, _ *[]string) {
			u.Engine[0].Spec.Containers[0].Resources.Requests[demandGPU] = resource.MustParse("0")
		}},
		{name: "no positive demand", wantError: "no positive accelerator demand", mutate: func(u *ReplicaUnit, _ *[]string) {
			u.Engine[0].Spec.Containers[0] = gpuContainer("runner", "0")
		}},
		{name: "incomplete decoder rejects whole unit", wantError: "decoder pod set", mutate: func(u *ReplicaUnit, _ *[]string) {
			u.Decoder = []PodSet{{Name: "primary", Count: 1}}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			unit, resources := demandUnit(), []string{demandGPU, demandTPU}
			tt.mutate(&unit, &resources)
			before := demandJSON(t, unit)
			got, err := MeasureUnit(unit, resources)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("error = %v, want containing %q", err, tt.wantError)
			}
			if diff := cmp.Diff(UnitDemand{}, got); diff != "" {
				t.Fatalf("partial result (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(before, demandJSON(t, unit)); diff != "" {
				t.Fatalf("input mutation (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMeasureUnitRetainsPodTemplate(t *testing.T) {
	for _, tt := range []struct {
		name         string
		omitRequests bool
	}{
		{name: "explicit requests"},
		{name: "defaulted requests", omitRequests: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			unit := demandUnit()
			pod := unit.Engine[0].Spec
			pod.NodeSelector = map[string]string{"accelerator": "type-a"}
			pod.RuntimeClassName = ptr.To("sandbox")
			pod.Overhead = corev1.ResourceList{demandGPU: resource.MustParse("1")}
			pod.SchedulerName = "configured-scheduler"
			pod.Tolerations = []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}}
			pod.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "accelerator", Operator: corev1.NodeSelectorOpIn, Values: []string{"type-a"}}}}}}}}
			want := pod.DeepCopy()
			if tt.omitRequests {
				pod.Containers[0].Resources.Requests = nil
			}
			got, err := MeasureUnit(unit, []string{demandGPU})
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(want, got.Pods[0].Spec, cmp.Comparer(func(a, b resource.Quantity) bool { return a.Cmp(b) == 0 })); diff != "" {
				t.Fatalf("rendered template (-want +got):\n%s", diff)
			}
			before := demandJSON(t, unit)
			got.Pods[0].Spec.NodeSelector["accelerator"] = "changed"
			got.Pods[0].Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values[0] = "changed"
			got.Pods[0].Spec.Overhead[demandGPU] = resource.MustParse("4")
			if diff := cmp.Diff(before, demandJSON(t, unit)); diff != "" {
				t.Fatalf("aliased input (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMeasureUnitFingerprint(t *testing.T) {
	for _, tt := range []struct {
		name     string
		mutate   func(*ReplicaUnit, *[]string)
		wantSame bool
	}{
		{name: "unchanged", wantSame: true},
		{name: "configured resources reordered and repeated", wantSame: true, mutate: func(_ *ReplicaUnit, resources *[]string) { *resources = []string{demandTPU, demandGPU, demandGPU} }},
		{name: "pod sets reordered", wantSame: true, mutate: func(unit *ReplicaUnit, _ *[]string) { slices.Reverse(unit.Engine) }},
		{name: "request defaults normalize", wantSame: true, mutate: func(unit *ReplicaUnit, _ *[]string) { unit.Engine[0].Spec.Containers[0].Resources.Requests = nil }},
		{name: "rendering dependencies", mutate: func(unit *ReplicaUnit, _ *[]string) { unit.InputFingerprint = "other-runtime-model-or-config" }},
		{name: "resource configuration", mutate: func(_ *ReplicaUnit, resources *[]string) { *resources = append(*resources, "example.com/other") }},
		{name: "pod image", mutate: func(unit *ReplicaUnit, _ *[]string) {
			unit.Engine[0].Spec.Containers[0].Image = "example.com/serving:v2"
		}},
		{name: "pod count", mutate: func(unit *ReplicaUnit, _ *[]string) { unit.Engine[1].Count++ }},
		{name: "accelerator requests", mutate: func(unit *ReplicaUnit, _ *[]string) { unit.Engine[0].Spec.Containers[0] = gpuContainer("runner", "4") }},
		{name: "node selector", mutate: func(unit *ReplicaUnit, _ *[]string) {
			unit.Engine[0].Spec.NodeSelector = map[string]string{"accelerator": "type-a"}
		}},
		{name: "required affinity", mutate: func(unit *ReplicaUnit, _ *[]string) {
			unit.Engine[0].Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "accelerator", Operator: corev1.NodeSelectorOpIn, Values: []string{"type-a"}}}}}}}}
		}},
		{name: "toleration", mutate: func(unit *ReplicaUnit, _ *[]string) {
			unit.Engine[0].Spec.Tolerations = []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}}
		}},
		{name: "runtime class", mutate: func(unit *ReplicaUnit, _ *[]string) { unit.Engine[0].Spec.RuntimeClassName = ptr.To("sandbox") }},
		{name: "overhead", mutate: func(unit *ReplicaUnit, _ *[]string) {
			unit.Engine[0].Spec.Overhead = corev1.ResourceList{demandGPU: resource.MustParse("1")}
		}},
		{name: "decoder placement shape", mutate: func(unit *ReplicaUnit, _ *[]string) {
			unit.Decoder = []PodSet{demandSet("primary", 1, gpuContainer("runner", "2"))}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			unit, resources := demandUnit(), []string{demandGPU, demandTPU}
			unit.Engine = append(unit.Engine, demandSet("workers", 2, gpuContainer("runner", "2")))
			original, err := MeasureUnit(unit, resources)
			if err != nil {
				t.Fatal(err)
			}
			if tt.mutate != nil {
				tt.mutate(&unit, &resources)
			}
			got, err := MeasureUnit(unit, resources)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.wantSame, original.Fingerprint == got.Fingerprint); diff != "" {
				t.Fatalf("same fingerprint (-want +got):\n%s", diff)
			}
		})
	}
}
