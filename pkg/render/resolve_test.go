package render

import (
	"errors"
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/acceleratorclassselector"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

// resolveFixture is a service naming a cluster runtime whose engine config
// carries the runner; helpers add a model, a sharded model or an accelerator
// class the way a cluster would carry them.
type resolveFixture struct {
	service *v1beta1.InferenceService
	runtime *v1beta1.ClusterServingRuntime
	extra   []client.Object
}

func newResolveFixture() *resolveFixture {
	rt := &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime-a"}, Spec: v1beta1.ServingRuntimeSpec{
		EngineConfig: &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(2), MaxReplicas: 4},
			Runner:                 &v1beta1.RunnerSpec{Container: corev1.Container{Name: "runner", Image: "example.com/serving:v1"}},
		},
		RouterConfig: &v1beta1.RouterSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "runner", Image: "example.com/router:v1"}}},
	}}
	service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "team-a"}, Spec: v1beta1.InferenceServiceSpec{
		Runtime: &v1beta1.ServingRuntimeRef{Name: rt.Name, Kind: ptr.To(runtimeselector.KindClusterServingRuntime)},
		Engine:  &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1)}},
	}}
	return &resolveFixture{service: service, runtime: rt}
}

func (f *resolveFixture) model() *v1beta1.ClusterBaseModel {
	f.service.Spec.Model = &v1beta1.ModelRef{Name: "model-a"}
	model := &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model-a", Generation: 1}, Spec: v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "pytorch"}}}
	f.extra = append(f.extra, model)
	f.runtime.Spec.SupportedModelFormats = []v1beta1.SupportedModelFormat{{ModelFormat: &v1beta1.ModelFormat{Name: "pytorch"}, AutoSelect: ptr.To(true)}}
	return model
}

func (f *resolveFixture) sharded() *v1beta1.ClusterBaseModel {
	model := f.model()
	model.Spec.Distribution = ptr.To(v1beta1.DistributionSharded)
	return model
}

func (f *resolveFixture) accelerator() *v1beta1.AcceleratorClass {
	ac := &v1beta1.AcceleratorClass{ObjectMeta: metav1.ObjectMeta{Name: "accelerator-a"}, Spec: v1beta1.AcceleratorClassSpec{
		Discovery: v1beta1.AcceleratorDiscovery{NodeSelector: map[string]string{"accelerator": "type-a"}},
	}}
	f.runtime.Spec.AcceleratorRequirements = &v1beta1.AcceleratorRequirements{AcceleratorClasses: []string{ac.Name}}
	f.service.Spec.AcceleratorSelector = &v1beta1.AcceleratorSelector{AcceleratorClass: ptr.To(ac.Name)}
	f.extra = append(f.extra, ac)
	return ac
}

func (f *resolveFixture) inputs(t *testing.T) Inputs {
	t.Helper()
	objects := append([]client.Object{}, f.extra...)
	if f.runtime != nil {
		objects = append(objects, f.runtime)
	}
	c := newRenderClient(t, objects...)
	return Inputs{Service: f.service, Client: c, Runtimes: runtimeselector.New(c), Accelerators: acceleratorclassselector.New(c), Config: &controllerconfig.InferenceServicesConfig{}, Log: logr.Discard()}
}

func TestResolveNamedRuntime(t *testing.T) {
	f := newResolveFixture()
	f.model()
	res, err := Resolve(t.Context(), f.inputs(t))
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff([]any{"runtime-a", true, true, true, false}, []any{res.RuntimeName, res.RuntimeIsCluster, res.UserSpecifiedRuntime, res.Model != nil, res.CompatibilityAdvisory != nil}); diff != "" {
		t.Errorf("resolution (-want +got):\n%s", diff)
	}
	if res.Specs.Engine == nil || res.Specs.Decoder != nil || res.Specs.Router != nil {
		t.Fatalf("merged specs = %+v, want the declared engine only", res.Specs)
	}
	// The service's replica floor wins over the runtime's; the runner comes
	// from the runtime.
	if diff := cmp.Diff([]any{1, 4, "example.com/serving:v1"}, []any{*res.Specs.Engine.MinReplicas, res.Specs.Engine.MaxReplicas, res.Specs.Engine.Runner.Image}); diff != "" {
		t.Errorf("merged engine (-want +got):\n%s", diff)
	}
}

func TestResolveAutoSelectsRuntimeForModel(t *testing.T) {
	f := newResolveFixture()
	f.model()
	f.service.Spec.Runtime = nil
	res, err := Resolve(t.Context(), f.inputs(t))
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff([]any{"runtime-a", true, false, true}, []any{res.RuntimeName, res.RuntimeIsCluster, res.UserSpecifiedRuntime, res.Specs.Engine != nil}); diff != "" {
		t.Errorf("auto-selection (-want +got):\n%s", diff)
	}
}

func TestResolveNamedRuntimeDeclaredFormatMismatch(t *testing.T) {
	f := newResolveFixture()
	f.model().Spec.ModelFormat.Name = "safetensors"
	res, err := Resolve(t.Context(), f.inputs(t))
	if err != nil {
		t.Fatal(err)
	}
	if !runtimeselector.IsRuntimeCompatibilityError(res.CompatibilityAdvisory) {
		t.Fatalf("advisory = %v, want the declared-format mismatch", res.CompatibilityAdvisory)
	}
	if res.Specs.Engine == nil || res.Specs.Engine.Runner == nil {
		t.Fatal("named runtime with a declared-format mismatch was not merged")
	}
}

func TestResolveShardedModelKeepsCompatibilityCheck(t *testing.T) {
	f := newResolveFixture()
	model := f.sharded()
	model.Status.Conditions = []metav1.Condition{{Type: v1beta1.ModelConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: model.Generation}}
	model.Spec.ModelFormat.Name = "safetensors"
	res, err := Resolve(t.Context(), f.inputs(t))
	if !runtimeselector.IsRuntimeCompatibilityError(err) {
		t.Fatalf("resolution = %v, %v; want the compatibility error for a sharded model", res, err)
	}
}

func TestResolveDisabledRuntime(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*resolveFixture, *Inputs)
	}{
		{name: "named runtime validated against the model", edit: func(f *resolveFixture, _ *Inputs) { f.model() }},
		{name: "named runtime without a model"},
		{name: "supplied runtime", edit: func(f *resolveFixture, in *Inputs) {
			in.Runtime, in.RuntimeName, in.RuntimeIsCluster = f.runtime.Spec.DeepCopy(), "runtime-a", true
			f.runtime = nil
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newResolveFixture()
			f.runtime.Spec.Disabled = ptr.To(true)
			var supplied Inputs
			if tt.edit != nil {
				tt.edit(f, &supplied)
			}
			in := f.inputs(t)
			in.Runtime, in.RuntimeName, in.RuntimeIsCluster = supplied.Runtime, supplied.RuntimeName, supplied.RuntimeIsCluster
			res, err := Resolve(t.Context(), in)
			if res != nil || !runtimeselector.IsRuntimeDisabledError(err) {
				t.Fatalf("resolution = %v, %v; want a RuntimeDisabledError", res, err)
			}
			disabled := err.(*runtimeselector.RuntimeDisabledError)
			if diff := cmp.Diff([]any{"runtime-a", true}, []any{disabled.RuntimeName, disabled.IsCluster}); diff != "" {
				t.Errorf("disabled runtime identity (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolveWithoutTemplateSource(t *testing.T) {
	f := newResolveFixture()
	f.service.Spec.Runtime = nil
	res, err := Resolve(t.Context(), f.inputs(t))
	if res != nil || !errors.Is(err, ErrNoTemplateSource) {
		t.Fatalf("resolution = %v, %v; want ErrNoTemplateSource", res, err)
	}
}

func TestResolveShardedModelNotReady(t *testing.T) {
	f := newResolveFixture()
	f.sharded()
	for name, resolve := range map[string]func(Inputs) error{
		"ResolveModel": func(in Inputs) error { _, _, _, err := ResolveModel(t.Context(), in); return err },
		"Resolve":      func(in Inputs) error { _, err := Resolve(t.Context(), in); return err },
	} {
		t.Run(name, func(t *testing.T) {
			err := resolve(f.inputs(t))
			var notReady *ModelNotReadyError
			if !errors.As(err, &notReady) {
				t.Fatalf("error = %v, want *ModelNotReadyError", err)
			}
			if notReady.Model != "model-a" || notReady.Message == "" {
				t.Errorf("not-ready report = %+v, want the model name and its readiness message", notReady)
			}
		})
	}
}

func TestModelNotReadyErrorMessage(t *testing.T) {
	if diff := cmp.Diff("model model-a is not ready", (&ModelNotReadyError{Model: "model-a"}).Error()); diff != "" {
		t.Error(diff)
	}
	if diff := cmp.Diff("model model-a is not ready: loading", (&ModelNotReadyError{Model: "model-a", Message: "loading"}).Error()); diff != "" {
		t.Error(diff)
	}
}

func TestResolveSuppliedRuntimeSkipsLookup(t *testing.T) {
	f := newResolveFixture()
	spec := f.runtime.Spec.DeepCopy()
	f.runtime = nil
	in := f.inputs(t)
	in.Runtime, in.RuntimeName, in.RuntimeIsCluster = spec, "pinned-a", false
	res, err := Resolve(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff([]any{"pinned-a", false, true, "example.com/serving:v1"}, []any{res.RuntimeName, res.RuntimeIsCluster, res.UserSpecifiedRuntime, res.Specs.Engine.Runner.Image}); diff != "" {
		t.Errorf("supplied runtime (-want +got):\n%s", diff)
	}
}

func TestResolveSuppliedModelAndOverlaysSkipReads(t *testing.T) {
	f := newResolveFixture()
	f.service.Spec.Model = &v1beta1.ModelRef{Name: "model-a", Overlays: []v1beta1.ModelOverlayRef{{Name: "overlay-a"}}}
	f.runtime.Spec.SupportedModelFormats = []v1beta1.SupportedModelFormat{{ModelFormat: &v1beta1.ModelFormat{Name: "pytorch"}}}
	overlays := []isvcutils.ResolvedOverlay{{Ref: v1beta1.ModelOverlayRef{Name: "overlay-a"}, SkipReason: "overlay \"overlay-a\" not found"}}
	in := f.inputs(t)
	in.Model, in.ModelMeta, in.ModelRead = &v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "pytorch"}}, &metav1.ObjectMeta{Name: "model-a"}, true
	in.Overlays, in.OverlaysRead = overlays, true
	res, err := Resolve(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(overlays, res.Overlays); diff != "" {
		t.Errorf("overlays (-want +got):\n%s", diff)
	}
	if res.Model != in.Model || res.ModelMeta != in.ModelMeta {
		t.Error("supplied model was not carried as authoritative")
	}
}

func TestResolveRequiresServiceAndSelector(t *testing.T) {
	f := newResolveFixture()
	for _, tt := range []struct {
		name string
		edit func(*Inputs)
	}{
		{name: "service", edit: func(in *Inputs) { in.Service = nil }},
		{name: "runtime selector", edit: func(in *Inputs) { in.Runtimes = nil }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			in := f.inputs(t)
			tt.edit(&in)
			if res, err := Resolve(t.Context(), in); err == nil || res != nil {
				t.Fatalf("resolution = %v, %v; want an error", res, err)
			}
		})
	}
}

func TestPrepare(t *testing.T) {
	res := &Resolved{Runtime: &v1beta1.ServingRuntimeSpec{}, Specs: Specs{
		Engine: &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}, Worker: &v1beta1.WorkerSpec{}},
		Router: &v1beta1.RouterSpec{},
	}}
	if err := Prepare(res, nil, &controllerconfig.DeployConfig{}); err != nil {
		t.Fatal(err)
	}
	want := map[v1beta1.ComponentType]constants.DeploymentModeType{v1beta1.EngineComponent: constants.OMENative, v1beta1.RouterComponent: constants.RawDeployment}
	if diff := cmp.Diff(want, res.Modes); diff != "" {
		t.Errorf("modes (-want +got):\n%s", diff)
	}
	if res.Specs.Engine.Worker.Size == nil || *res.Specs.Engine.Worker.Size != 1 {
		t.Errorf("worker size = %v, want the one-worker default", res.Specs.Engine.Worker.Size)
	}
	if err := Prepare(&Resolved{Specs: Specs{Router: &v1beta1.RouterSpec{}}}, nil, nil); err == nil {
		t.Fatal("a service without an engine was prepared")
	}
}

// PrepareRole prepares one role without requiring an engine: the mode of that
// role alone is recorded and only its spec receives the defaults.
func TestPrepareRole(t *testing.T) {
	res := &Resolved{Runtime: &v1beta1.ServingRuntimeSpec{}, Specs: Specs{
		Decoder: &v1beta1.DecoderSpec{Leader: &v1beta1.LeaderSpec{}, Worker: &v1beta1.WorkerSpec{}},
	}}
	if err := PrepareRole(res, v1beta1.DecoderComponent, nil, &controllerconfig.DeployConfig{}); err != nil {
		t.Fatal(err)
	}
	want := map[v1beta1.ComponentType]constants.DeploymentModeType{v1beta1.DecoderComponent: constants.OMENative}
	if diff := cmp.Diff(want, res.Modes); diff != "" {
		t.Errorf("modes (-want +got):\n%s", diff)
	}
	if res.Specs.Decoder.Worker.Size == nil || *res.Specs.Decoder.Worker.Size != 1 {
		t.Errorf("worker size = %v, want the one-worker default", res.Specs.Decoder.Worker.Size)
	}
	mode := constants.OMENative
	single := &Resolved{Specs: Specs{Engine: &v1beta1.EngineSpec{}}}
	if err := PrepareRole(single, v1beta1.EngineComponent, &mode, nil); err != nil {
		t.Fatal(err)
	}
	if single.Modes[v1beta1.EngineComponent] != constants.OMENative {
		t.Errorf("engine mode = %q, want the spec mode", single.Modes[v1beta1.EngineComponent])
	}
	if err := PrepareRole(single, v1beta1.RouterComponent, nil, nil); err == nil {
		t.Fatal("a role the merged specs do not declare was prepared")
	}
}

func TestPieceAcceleratorClass(t *testing.T) {
	for _, tt := range []struct {
		name      string
		component v1beta1.ComponentType
		class     bool
		wantClass bool
	}{
		{name: "engine with the runtime's class", component: v1beta1.EngineComponent, class: true, wantClass: true},
		{name: "decoder with the runtime's class", component: v1beta1.DecoderComponent, class: true, wantClass: true},
		{name: "engine without a class", component: v1beta1.EngineComponent},
		{name: "router takes no class", component: v1beta1.RouterComponent, class: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newResolveFixture()
			f.model()
			f.service.Spec.Decoder, f.service.Spec.Router = &v1beta1.DecoderSpec{}, &v1beta1.RouterSpec{}
			var ac *v1beta1.AcceleratorClass
			if tt.class {
				ac = f.accelerator()
			}
			in := f.inputs(t)
			res, err := Resolve(t.Context(), in)
			if err != nil {
				t.Fatal(err)
			}
			if err := Prepare(res, ptr.To(constants.OMENative), nil); err != nil {
				t.Fatal(err)
			}
			piece, picked, err := res.Piece(t.Context(), in, tt.component)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff([]any{constants.OMENative, "runtime-a", true, tt.component != v1beta1.RouterComponent}, []any{piece.DeploymentMode, piece.RuntimeName, piece.BaseModel != nil, piece.SupportedModelFormat != nil}); diff != "" {
				t.Errorf("piece (-want +got):\n%s", diff)
			}
			if !tt.wantClass {
				if piece.AcceleratorClass != nil || piece.AcceleratorClassName != "" || picked != nil {
					t.Fatalf("piece carries class %q (%v); want none", piece.AcceleratorClassName, picked)
				}
				return
			}
			if picked == nil || picked.Name != ac.Name || piece.AcceleratorClassName != ac.Name {
				t.Fatalf("picked class = %v, name %q; want %s", picked, piece.AcceleratorClassName, ac.Name)
			}
			if diff := cmp.Diff(ac.Spec, *piece.AcceleratorClass); diff != "" {
				t.Errorf("class spec (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMergeErrorReportsCause(t *testing.T) {
	cause := errors.New("failed to merge engine specs")
	err := &MergeError{Err: cause}
	if diff := cmp.Diff(cause.Error(), err.Error()); diff != "" {
		t.Error(diff)
	}
	if !errors.Is(err, cause) {
		t.Error("merge error hides its cause")
	}
}

func TestValidateResolvedRuntimeEnabled(t *testing.T) {
	disabled := true
	tests := []struct {
		name        string
		runtimeName string
		runtimeSpec *v1beta1.ServingRuntimeSpec
		isCluster   bool
		wantErr     bool
	}{
		{
			name:        "nil runtime spec",
			runtimeName: "nil-runtime",
		},
		{
			name:        "enabled runtime",
			runtimeName: "enabled-runtime",
			runtimeSpec: &v1beta1.ServingRuntimeSpec{},
		},
		{
			name:        "disabled namespaced runtime",
			runtimeName: "namespaced-runtime",
			runtimeSpec: &v1beta1.ServingRuntimeSpec{Disabled: &disabled},
			wantErr:     true,
		},
		{
			name:        "disabled cluster runtime",
			runtimeName: "cluster-runtime",
			runtimeSpec: &v1beta1.ServingRuntimeSpec{Disabled: &disabled},
			isCluster:   true,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateResolvedRuntimeEnabled(tt.runtimeSpec, tt.runtimeName, tt.isCluster)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("validateResolvedRuntimeEnabled() error = %v", err)
				}
				return
			}

			var disabledErr *runtimeselector.RuntimeDisabledError
			if !errors.As(err, &disabledErr) {
				t.Fatalf("validateResolvedRuntimeEnabled() error = %T, want *runtimeselector.RuntimeDisabledError", err)
			}
			if disabledErr.RuntimeName != tt.runtimeName {
				t.Errorf("RuntimeName = %q, want %q", disabledErr.RuntimeName, tt.runtimeName)
			}
			if disabledErr.IsCluster != tt.isCluster {
				t.Errorf("IsCluster = %t, want %t", disabledErr.IsCluster, tt.isCluster)
			}
		})
	}
}
