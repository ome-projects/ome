package resolution

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

const unitGPU = "example.com/gpu"

func unitResources(quantity string) corev1.ResourceRequirements {
	values := corev1.ResourceList{unitGPU: resource.MustParse(quantity)}
	return corev1.ResourceRequirements{Requests: values.DeepCopy(), Limits: values}
}

func unitRunner(quantity string) *v1beta1.RunnerSpec {
	return &v1beta1.RunnerSpec{Container: corev1.Container{Name: "runner", Image: "example.com/serving:v1", Resources: unitResources(quantity)}}
}

func newUnitFixture() (runtimeFixture, *corev1.ConfigMap) {
	f := newRuntimeFixture()
	f.service.Spec.Decoder, f.service.Spec.Router = nil, nil
	f.runtime.Spec.EngineConfig.Runner = unitRunner("2")
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: f.namespace, UID: "config-uid"}, Data: map[string]string{
		controllerconfig.AcceleratorResourcesConfigName: `["example.com/gpu"]`,
	}}
	f.extra = append(f.extra, cm)
	return f, cm
}

func unitGang(f *runtimeFixture) {
	f.runtime.Spec.EngineConfig.Leader = &v1beta1.LeaderSpec{Runner: unitRunner("2")}
	f.runtime.Spec.EngineConfig.Worker = &v1beta1.WorkerSpec{Runner: unitRunner("3"), Size: ptr.To(4)}
}

func unitAccelerator(f *runtimeFixture) *v1beta1.AcceleratorClass {
	ac := &v1beta1.AcceleratorClass{ObjectMeta: metav1.ObjectMeta{Name: "accelerator-a", UID: "accelerator-uid"}, Spec: v1beta1.AcceleratorClassSpec{
		Resources: []v1beta1.AcceleratorResource{{Name: unitGPU, Quantity: resource.MustParse("8")}},
		Discovery: v1beta1.AcceleratorDiscovery{NodeSelector: map[string]string{"accelerator": "type-a"}},
	}}
	f.runtime.Spec.AcceleratorRequirements = &v1beta1.AcceleratorRequirements{AcceleratorClasses: []string{ac.Name}}
	f.service.Spec.AcceleratorSelector = &v1beta1.AcceleratorSelector{AcceleratorClass: ptr.To(ac.Name)}
	f.extra = append(f.extra, ac)
	return ac
}

func unitFineTunedWeight(f *runtimeFixture) *v1beta1.FineTunedWeight {
	f.model()
	f.service.Spec.Model.FineTunedWeights = []string{"adapter"}
	weight := &v1beta1.FineTunedWeight{ObjectMeta: metav1.ObjectMeta{Name: "adapter", UID: "adapter-uid"}, Spec: v1beta1.FineTunedWeightSpec{
		HyperParameters: runtime.RawExtension{Raw: []byte(`{"strategy":"lora"}`)},
		Configuration:   runtime.RawExtension{Raw: []byte(`{}`)},
	}}
	f.extra = append(f.extra, weight)
	return weight
}

func unitClient(t *testing.T, f runtimeFixture, intercept interceptor.Funcs) client.WithWatch {
	t.Helper()
	scheme := runtimeScheme(t)
	if err := nodev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kueuev1beta2.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	objects := append([]client.Object{f.runtime}, f.extra...)
	if f.standing != nil {
		objects = append(objects, f.standing)
	}
	for i := range objects {
		objects[i] = objects[i].DeepCopyObject().(client.Object)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithInterceptorFuncs(intercept).Build()
}

func unitJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestResolveRenderedUnit(t *testing.T) {
	type pod struct {
		Component v1beta1.ComponentType
		Name      string
		Count     int64
		GPU       string
		Selector  map[string]string
	}
	primary := func(gpu string) []pod {
		return []pod{{Component: v1beta1.EngineComponent, Name: "primary", Count: 1, GPU: gpu}}
	}
	accelerated := func(gpu string) []pod {
		out := primary(gpu)
		out[0].Selector = map[string]string{"accelerator": "type-a"}
		return out
	}
	gang := []pod{{Component: v1beta1.EngineComponent, Name: "primary", Count: 1, GPU: "2"}, {Component: v1beta1.EngineComponent, Name: "workers", Count: 4, GPU: "12"}}
	for _, tt := range []struct {
		name   string
		mutate func(*runtimeFixture, *corev1.ConfigMap)
		want   []pod
	}{
		{name: "runtime runner", want: primary("2")},
		{name: "native single pod", want: primary("2"), mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.service.Spec.DeploymentMode = ptr.To(constants.OMENative)
		}},
		{name: "model readiness selector retained", want: []pod{{Component: v1beta1.EngineComponent, Name: "primary", Count: 1, GPU: "2", Selector: map[string]string{constants.GetClusterBaseModelLabel("model-a"): "Ready"}}}, mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.model()
		}},
		{name: "runtime container fills runner resources", want: primary("6"), mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.runtime.Spec.EngineConfig.Runner.Resources = corev1.ResourceRequirements{}
			f.runtime.Spec.ServingRuntimePodSpec.Containers = []corev1.Container{{Name: "runner", Resources: unitResources("6")}}
		}},
		{name: "accelerator overrides runtime resources", want: accelerated("8"), mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) { unitAccelerator(f) }},
		{name: "source runner resources override accelerator", want: accelerated("4"), mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			unitAccelerator(f)
			f.service.Spec.Engine.Runner = unitRunner("4")
		}},
		{name: "pod container overrides merged accelerator runner", want: accelerated("6"), mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			unitAccelerator(f)
			f.service.Spec.Engine.PodSpec.Containers = []corev1.Container{{Name: "runner", Resources: unitResources("6")}}
		}},
		{name: "additional container contributes resources", want: primary("3"), mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.service.Spec.Engine.PodSpec.Containers = []corev1.Container{{Name: "sidecar", Image: "example.com/sidecar:v1", Resources: unitResources("1")}}
		}},
		{name: "initialization peak contributes resources", want: primary("6"), mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.service.Spec.Engine.PodSpec.InitContainers = []corev1.Container{{Name: "init", Image: "example.com/init:v1", Resources: unitResources("6")}}
		}},
		{name: "leader and workers use their own runners", want: gang, mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) { unitGang(f) }},
		{name: "explicit multinode mode counts workers", want: gang, mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			unitGang(f)
			f.service.Spec.DeploymentMode = ptr.To(constants.MultiNode)
		}},
		{name: "raw deployment uses only primary template", want: primary("2"), mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			unitGang(f)
			f.service.Spec.DeploymentMode = ptr.To(constants.RawDeployment)
		}},
		{name: "member worker default is applied", want: []pod{{Component: v1beta1.EngineComponent, Name: "primary", Count: 1, GPU: "2"}, {Component: v1beta1.EngineComponent, Name: "workers", Count: 1, GPU: "3"}}, mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			unitGang(f)
			f.runtime.Spec.EngineConfig.Worker.Size = nil
		}},
		{name: "engine and decoder are one unit", want: append(primary("2"), pod{Component: v1beta1.DecoderComponent, Name: "primary", Count: 1, GPU: "4"}), mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.service.Spec.Decoder = &v1beta1.DecoderSpec{}
			f.runtime.Spec.DecoderConfig.Runner = unitRunner("4")
		}},
		{name: "decoder leader and workers contribute to the unit", want: append(primary("2"),
			pod{Component: v1beta1.DecoderComponent, Name: "primary", Count: 1, GPU: "4"},
			pod{Component: v1beta1.DecoderComponent, Name: "workers", Count: 3, GPU: "6"}), mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.service.Spec.Decoder = &v1beta1.DecoderSpec{}
			f.runtime.Spec.DecoderConfig.Leader = &v1beta1.LeaderSpec{Runner: unitRunner("4")}
			f.runtime.Spec.DecoderConfig.Worker = &v1beta1.WorkerSpec{Runner: unitRunner("2"), Size: ptr.To(3)}
		}},
		{name: "fine-tuned weights render both components", want: []pod{
			{Component: v1beta1.EngineComponent, Name: "primary", Count: 1, GPU: "2", Selector: map[string]string{constants.GetClusterBaseModelLabel("model-a"): "Ready"}},
			{Component: v1beta1.DecoderComponent, Name: "primary", Count: 1, GPU: "4", Selector: map[string]string{constants.GetClusterBaseModelLabel("model-a"): "Ready"}},
		}, mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			unitFineTunedWeight(f)
			f.service.Spec.Decoder = &v1beta1.DecoderSpec{}
			f.runtime.Spec.DecoderConfig.Runner = unitRunner("4")
		}},
		{name: "router resources excluded", want: primary("2"), mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.service.Spec.Router = &v1beta1.RouterSpec{}
			f.runtime.Spec.RouterConfig.Runner = unitRunner("8")
		}},
		{name: "worker resources follow component override rules", want: []pod{{Component: v1beta1.EngineComponent, Name: "primary", Count: 1, GPU: "8", Selector: map[string]string{"accelerator": "type-a"}}, {Component: v1beta1.EngineComponent, Name: "workers", Count: 4, GPU: "32", Selector: map[string]string{"accelerator": "type-a"}}}, mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			unitGang(f)
			unitAccelerator(f)
			f.service.Spec.Engine.Worker = &v1beta1.WorkerSpec{Runner: unitRunner("6")}
		}},
		{name: "source node selector overrides class and runtime", want: []pod{{Component: v1beta1.EngineComponent, Name: "primary", Count: 1, GPU: "8", Selector: map[string]string{"accelerator": "type-b", "pool": "shared"}}}, mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			unitAccelerator(f)
			f.runtime.Spec.ServingRuntimePodSpec.NodeSelector = map[string]string{"accelerator": "runtime", "pool": "shared"}
			f.service.Spec.Engine.NodeSelector = map[string]string{"accelerator": "type-b"}
		}},
		{name: "runtime class adds overhead and selector", want: []pod{{Component: v1beta1.EngineComponent, Name: "primary", Count: 1, GPU: "3", Selector: map[string]string{"sandbox": "enabled"}}}, mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.service.Spec.Engine.RuntimeClassName = ptr.To("sandbox")
			f.extra = append(f.extra, &nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "sandbox", UID: "class-uid"}, Handler: "sandbox-handler", Overhead: &nodev1.Overhead{PodFixed: corev1.ResourceList{unitGPU: resource.MustParse("1")}}, Scheduling: &nodev1.Scheduling{NodeSelector: map[string]string{"sandbox": "enabled"}}})
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, cm := newUnitFixture()
			if tt.mutate != nil {
				tt.mutate(&f, cm)
			}
			before := unitJSON(t, []any{f.service, f.runtime, f.extra})
			writes := 0
			cl := unitClient(t, f, interceptor.Funcs{
				Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
					writes++
					return fmt.Errorf("unexpected create")
				},
				Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
					writes++
					return fmt.Errorf("unexpected update")
				},
				Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
					writes++
					return fmt.Errorf("unexpected patch")
				},
				Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
					writes++
					return fmt.Errorf("unexpected delete")
				},
			})
			got, err := (Resolver{Client: cl, OperatorNamespace: f.namespace}).ResolveUnit(t.Context(), f.service, f.standing)
			if err != nil {
				t.Fatal(err)
			}
			var pods []pod
			for _, demand := range got.Demand.Pods {
				quantity := demand.Requests[unitGPU]
				pods = append(pods, pod{Component: demand.Component, Name: demand.Name, Count: demand.Count, GPU: quantity.String(), Selector: demand.Spec.NodeSelector})
			}
			if diff := cmp.Diff(tt.want, pods); diff != "" {
				t.Fatalf("rendered demand (-want +got):\n%s", diff)
			}
			if err := got.Check(t.Context()); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(0, writes); diff != "" {
				t.Fatalf("writes (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(before, unitJSON(t, []any{f.service, f.runtime, f.extra})); diff != "" {
				t.Fatalf("mutated inputs (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolveUnitRejectsUnknownRendering(t *testing.T) {
	for _, tt := range []struct {
		name      string
		mutate    func(*runtimeFixture, *corev1.ConfigMap)
		wantError string
	}{
		{name: "namespace missing", wantError: "operator namespace", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) { f.namespace = "" }},
		{name: "runtime missing", wantError: "not found", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) { f.service.Spec.Runtime.Name = "missing" }},
		{name: "engine missing", wantError: "engine component is required", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) { f.service.Spec.Engine = nil }},
		{name: "configuration absent", wantError: "not found", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) { f.extra = nil }},
		{name: "configuration unverified", wantError: "live identity", mutate: func(_ *runtimeFixture, cm *corev1.ConfigMap) { cm.UID = "" }},
		{name: "accelerator resources absent", wantError: "configured member accelerator", mutate: func(_ *runtimeFixture, cm *corev1.ConfigMap) { cm.Data = nil }},
		{name: "malformed accelerator resources", wantError: "config json", mutate: func(_ *runtimeFixture, cm *corev1.ConfigMap) {
			cm.Data[controllerconfig.AcceleratorResourcesConfigName] = "{"
		}},
		{name: "invalid deploy configuration", wantError: "must be an object", mutate: func(_ *runtimeFixture, cm *corev1.ConfigMap) { cm.Data[controllerconfig.DeployConfigName] = "null" }},
		{name: "accelerator class missing", wantError: "not found", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			unitAccelerator(f)
			f.extra = f.extra[:1]
		}},
		{name: "accelerator class unverified", wantError: "live identity", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) { unitAccelerator(f).UID = "" }},
		{name: "virtual deployment has no hardware shape", wantError: "no supported replica shape", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.service.Spec.DeploymentMode = ptr.To(constants.VirtualDeployment)
		}},
		{name: "orphan worker", wantError: "requires a leader", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			unitGang(f)
			f.runtime.Spec.EngineConfig.Leader = nil
		}},
		{name: "zero worker count", wantError: "positive resolved worker", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			unitGang(f)
			f.runtime.Spec.EngineConfig.Worker.Size = ptr.To(0)
		}},
		{name: "unrenderable primary", wantError: "no containers", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) { f.runtime.Spec.EngineConfig.Runner = nil }},
		{name: "unrenderable worker", wantError: "no containers", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			unitGang(f)
			f.runtime.Spec.EngineConfig.Worker.Runner = nil
		}},
		{name: "unrenderable decoder", wantError: "no containers", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) { f.service.Spec.Decoder = &v1beta1.DecoderSpec{} }},
		{name: "decoder accelerator missing", wantError: "not found", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			unitAccelerator(f)
			f.service.Spec.Decoder = &v1beta1.DecoderSpec{AcceleratorOverride: &v1beta1.AcceleratorSelector{AcceleratorClass: ptr.To("missing")}}
		}},
		{name: "decoder gang incomplete", wantError: "decoder demand", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.service.Spec.Decoder = &v1beta1.DecoderSpec{}
			f.runtime.Spec.DecoderConfig.Runner = unitRunner("2")
			f.runtime.Spec.DecoderConfig.Worker = &v1beta1.WorkerSpec{Runner: unitRunner("3"), Size: ptr.To(1)}
		}},
		{name: "overlay identity missing", wantError: "live identity", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.model()
			f.service.Spec.Model.Overlays = []v1beta1.ModelOverlayRef{{Name: "overlay"}}
			f.extra = append(f.extra, &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "overlay"}})
		}},
		{name: "fine-tuned weight malformed", wantError: "cannot unmarshal", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			unitFineTunedWeight(f).Spec.Configuration = runtime.RawExtension{Raw: []byte(`[]`)}
		}},
		{name: "fine-tuned strategy absent", wantError: "hyper-parameter", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			unitFineTunedWeight(f).Spec.HyperParameters = runtime.RawExtension{Raw: []byte(`{}`)}
		}},
		{name: "unknown accelerator resource", wantError: "absent from accelerator configuration", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.runtime.Spec.EngineConfig.Runner.Resources.Limits["example.com/other"] = resource.MustParse("1")
		}},
		{name: "runtime class missing", wantError: "not found", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.service.Spec.Engine.RuntimeClassName = ptr.To("missing")
		}},
		{name: "runtime class name empty", wantError: "name is empty", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) { f.service.Spec.Engine.RuntimeClassName = ptr.To("") }},
		{name: "overhead without class", wantError: "no identified RuntimeClass", mutate: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.service.Spec.Engine.Overhead = corev1.ResourceList{unitGPU: resource.MustParse("1")}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, cm := newUnitFixture()
			tt.mutate(&f, cm)
			got, err := (Resolver{Client: unitClient(t, f, interceptor.Funcs{}), OperatorNamespace: f.namespace}).ResolveUnit(t.Context(), f.service, f.standing)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("error = %v, want containing %q", err, tt.wantError)
			}
			if diff := cmp.Diff(true, got == nil); diff != "" {
				t.Fatalf("no partial result (-want +got):\n%s", diff)
			}
		})
	}
}
