package render

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

func newRenderClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, v1beta1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func TestRenderRequiresInputs(t *testing.T) {
	for _, component := range []struct {
		name   string
		render func(context.Context, *Piece, *v1beta1.InferenceService, bool) (Rendered, error)
	}{
		{name: "engine", render: func(ctx context.Context, p *Piece, service *v1beta1.InferenceService, missing bool) (Rendered, error) {
			var spec *v1beta1.EngineSpec
			if !missing {
				spec = &v1beta1.EngineSpec{}
			}
			return RenderEngine(ctx, p, service, spec)
		}},
		{name: "decoder", render: func(ctx context.Context, p *Piece, service *v1beta1.InferenceService, missing bool) (Rendered, error) {
			var spec *v1beta1.DecoderSpec
			if !missing {
				spec = &v1beta1.DecoderSpec{}
			}
			return RenderDecoder(ctx, p, service, spec)
		}},
		{name: "router", render: func(ctx context.Context, p *Piece, service *v1beta1.InferenceService, missing bool) (Rendered, error) {
			var spec *v1beta1.RouterSpec
			if !missing {
				spec = &v1beta1.RouterSpec{}
			}
			return RenderRouter(ctx, p, service, spec)
		}},
	} {
		t.Run(component.name, func(t *testing.T) {
			for _, name := range []string{"inputs", "client", "service", "spec"} {
				t.Run(name, func(t *testing.T) {
					piece := &Piece{Client: newRenderClient(t)}
					service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "team-a"}}
					switch name {
					case "inputs":
						piece = nil
					case "client":
						piece.Client = nil
					case "service":
						service = nil
					}
					got, err := component.render(t.Context(), piece, service, name == "spec")
					if err == nil {
						t.Fatal("incomplete rendering input was accepted")
					}
					if diff := cmp.Diff(Rendered{}, got); diff != "" {
						t.Fatalf("partial rendering (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}

func TestRenderDecoderRejectsInvalidInputs(t *testing.T) {
	for _, tt := range []struct {
		name   string
		weight bool
		object bool
		worker bool
	}{
		{name: "fine-tuned weight unavailable", weight: true},
		{name: "fine-tuned strategy unavailable", weight: true, object: true},
		{name: "worker template has no container", worker: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "team-a"}, Spec: v1beta1.InferenceServiceSpec{Decoder: &v1beta1.DecoderSpec{}}}
			spec := &v1beta1.DecoderSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "runner", Image: "example.com/serving:v1"}}}
			if tt.worker {
				spec.Worker = &v1beta1.WorkerSpec{}
			}
			if tt.weight {
				service.Spec.Model = &v1beta1.ModelRef{FineTunedWeights: []string{"adapter"}}
			}
			var objects []client.Object
			if tt.object {
				objects = append(objects, &v1beta1.FineTunedWeight{ObjectMeta: metav1.ObjectMeta{Name: "adapter"}, Spec: v1beta1.FineTunedWeightSpec{
					Configuration:   runtime.RawExtension{Raw: []byte(`{}`)},
					HyperParameters: runtime.RawExtension{Raw: []byte(`{}`)},
				}})
			}
			got, err := RenderDecoder(t.Context(), &Piece{Client: newRenderClient(t, objects...)}, service, spec)
			if err == nil {
				t.Fatal("incomplete decoder rendering was accepted")
			}
			if diff := cmp.Diff(Rendered{}, got); diff != "" {
				t.Fatalf("partial rendering (-want +got):\n%s", diff)
			}
		})
	}
}

// TestRenderComponentDispatches renders every role through RenderComponent
// and checks the caller's specs come back untouched: the router's config env
// and the completed runner containers exist on the copies the library
// rendered from, not on the inputs.
func TestRenderComponentDispatches(t *testing.T) {
	ctx := t.Context()
	piece := &Piece{Client: newRenderClient(t), InferenceServiceConfig: &controllerconfig.InferenceServicesConfig{}}
	service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "team-a"}}
	runner := func() *v1beta1.RunnerSpec {
		return &v1beta1.RunnerSpec{Container: corev1.Container{Name: "runner", Image: "example.com/serving:v1"}}
	}
	specs := Specs{
		Engine:  &v1beta1.EngineSpec{Runner: runner()},
		Decoder: &v1beta1.DecoderSpec{Runner: runner()},
		Router:  &v1beta1.RouterSpec{Runner: runner(), Config: map[string]string{"ROUTER_BACKEND": "engine"}},
	}
	before := Specs{Engine: specs.Engine.DeepCopy(), Decoder: specs.Decoder.DeepCopy(), Router: specs.Router.DeepCopy()}
	for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent} {
		got, err := RenderComponent(ctx, piece, service, specs, c)
		if err != nil {
			t.Fatalf("render %s: %v", c, err)
		}
		if want := service.Name + "-" + string(c); got.ObjectMeta.Name != want {
			t.Fatalf("%s object name = %q, want %q", c, got.ObjectMeta.Name, want)
		}
		runners := got.Runners()
		if len(runners) != 1 || runners[0].Name != v1beta1.RunnerNameDefault || got.ComponentExt == nil {
			t.Fatalf("%s rendered %d runners (component extension %v), want one default runner with its component extension", c, len(runners), got.ComponentExt)
		}
		if c == v1beta1.RouterComponent {
			env := runners[0].Template.Spec.Containers[0].Env
			if len(env) != 1 || env[0].Name != "ROUTER_BACKEND" || env[0].Value != "engine" {
				t.Fatalf("router config env = %v, want the config as env", env)
			}
			// The runners follow a template edit made after rendering.
			got.Primary.ServiceAccountName = "router-account"
			if sa := got.Runners()[0].Template.Spec.ServiceAccountName; sa != "router-account" {
				t.Fatalf("runners do not follow the template edit: service account %q", sa)
			}
		}
	}
	if diff := cmp.Diff(before, specs); diff != "" {
		t.Fatalf("rendering mutated the caller's specs (-before +after):\n%s", diff)
	}
	if _, err := RenderComponent(ctx, piece, service, specs, v1beta1.ComponentType("sidecar")); err == nil {
		t.Fatal("an unknown component was rendered")
	}
}

func TestRuntimeDeclaresPiece(t *testing.T) {
	rt := &v1beta1.ServingRuntimeSpec{
		EngineConfig: &v1beta1.EngineSpec{},
		RouterConfig: &v1beta1.RouterSpec{},
	}
	for _, tc := range []struct {
		name      string
		rt        *v1beta1.ServingRuntimeSpec
		component v1beta1.ComponentType
		want      bool
	}{
		{"engine declared", rt, v1beta1.EngineComponent, true},
		{"router declared", rt, v1beta1.RouterComponent, true},
		{"decoder not declared", rt, v1beta1.DecoderComponent, false},
		{"decoder declared", &v1beta1.ServingRuntimeSpec{DecoderConfig: &v1beta1.DecoderSpec{}}, v1beta1.DecoderComponent, true},
		{"unknown component", rt, v1beta1.ComponentType("sidecar"), false},
		{"no runtime", nil, v1beta1.EngineComponent, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := RuntimeDeclaresPiece(tc.rt, tc.component); got != tc.want {
				t.Fatalf("RuntimeDeclaresPiece = %v, want %v", got, tc.want)
			}
		})
	}
}
