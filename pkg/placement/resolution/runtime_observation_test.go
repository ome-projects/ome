package resolution

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func fixtureClient(t *testing.T, f runtimeFixture) client.WithWatch {
	t.Helper()
	objects := append([]client.Object{}, f.extra...)
	if f.runtime != nil {
		objects = append(objects, f.runtime)
	}
	if f.standing != nil {
		objects = append(objects, f.standing)
	}
	return fake.NewClientBuilder().WithScheme(runtimeScheme(t)).WithObjects(objects...).Build()
}

func TestResolveStandingIdentity(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(*testing.T, *runtimeFixture, client.Client)
		wantErr bool
	}{
		{name: "identified standing member"},
		{name: "read live pin rather than caller status", edit: func(_ *testing.T, f *runtimeFixture, _ client.Client) {
			f.standing.Status.PinnedRevisionName = "caller-only-pin"
		}},
		{name: "unexpected existing member", edit: func(_ *testing.T, f *runtimeFixture, _ client.Client) { f.standing = nil }, wantErr: true},
		{name: "different member name", edit: func(_ *testing.T, f *runtimeFixture, _ client.Client) { f.standing.Name = "another" }, wantErr: true},
		{name: "different member namespace", edit: func(_ *testing.T, f *runtimeFixture, _ client.Client) { f.standing.Namespace = "team-b" }, wantErr: true},
		{name: "missing member UID", edit: func(_ *testing.T, f *runtimeFixture, _ client.Client) { f.standing.UID = "" }, wantErr: true},
		{name: "missing member version", edit: func(_ *testing.T, f *runtimeFixture, _ client.Client) { f.standing.ResourceVersion = "" }, wantErr: true},
		{name: "different version with unchanged inputs", edit: func(_ *testing.T, f *runtimeFixture, _ client.Client) { f.standing.ResourceVersion = "earlier" }},
		{name: "stale member spec", edit: func(_ *testing.T, f *runtimeFixture, _ client.Client) { f.standing.Spec.Engine.MinReplicas = ptr.To(7) }, wantErr: true},
		{name: "stale member labels", edit: func(_ *testing.T, f *runtimeFixture, _ client.Client) {
			f.standing.Labels = map[string]string{"selected": "earlier"}
		}, wantErr: true},
		{name: "stale member annotation", edit: func(_ *testing.T, f *runtimeFixture, _ client.Client) {
			f.standing.Annotations = map[string]string{constants.RuntimeSyncAnnotationKey: "earlier"}
		}, wantErr: true},
		{name: "member disappeared", edit: func(t *testing.T, f *runtimeFixture, cl client.Client) {
			if err := cl.Delete(t.Context(), f.standing); err != nil {
				t.Fatal(err)
			}
		}, wantErr: true},
		{name: "member recreated", edit: func(t *testing.T, f *runtimeFixture, cl client.Client) {
			if err := cl.Delete(t.Context(), f.standing); err != nil {
				t.Fatal(err)
			}
			other := f.standing.DeepCopy()
			other.UID, other.ResourceVersion = "replacement", ""
			if err := cl.Create(t.Context(), other); err != nil {
				t.Fatal(err)
			}
		}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newRuntimeFixture()
			f.pin(t, false)
			cl := fixtureClient(t, f)
			if tt.edit != nil {
				tt.edit(t, &f, cl)
			}
			got, err := (Resolver{Client: cl, OperatorNamespace: f.namespace}).Resolve(t.Context(), f.service, f.standing)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("standing identity (-want +got):\n%s\n%v", diff, err)
			}
			if tt.wantErr {
				if got != nil {
					t.Fatal("unverified standing member returned a resolution")
				}
			} else if diff := cmp.Diff("example:v1", got.Engine.Runner.Image); diff != "" {
				t.Error(diff)
			}
		})
	}
}

func TestResolveDeclaredComponents(t *testing.T) {
	for _, tt := range []struct {
		name string
		want []v1beta1.ComponentType
	}{
		{name: "none"},
		{name: "engine", want: []v1beta1.ComponentType{v1beta1.EngineComponent}},
		{name: "decoder", want: []v1beta1.ComponentType{v1beta1.DecoderComponent}},
		{name: "router", want: []v1beta1.ComponentType{v1beta1.RouterComponent}},
		{name: "engine and decoder", want: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}},
		{name: "all", want: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newRuntimeFixture()
			f.service.Spec.Engine, f.service.Spec.Decoder, f.service.Spec.Router = nil, nil, nil
			for _, component := range tt.want {
				switch component {
				case v1beta1.EngineComponent:
					f.service.Spec.Engine = &v1beta1.EngineSpec{}
				case v1beta1.DecoderComponent:
					f.service.Spec.Decoder = &v1beta1.DecoderSpec{}
				case v1beta1.RouterComponent:
					f.service.Spec.Router = &v1beta1.RouterSpec{}
				}
			}
			got, err := (Resolver{Client: fixtureClient(t, f)}).Resolve(t.Context(), f.service, nil)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got.Components()); diff != "" {
				t.Errorf("runtime created an undeclared component:\n%s", diff)
			}
		})
	}
}

func TestRuntimeResolutionDependencyCheck(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(*testing.T, *runtimeFixture) client.Object
	}{
		{name: "runtime", setup: func(_ *testing.T, f *runtimeFixture) client.Object { return f.runtime }},
		{name: "runtime parent", setup: func(_ *testing.T, f *runtimeFixture) client.Object {
			parent := f.runtime.DeepCopy()
			parent.Name, parent.UID = "parent", "parent-uid"
			f.runtime.Annotations = map[string]string{constants.RuntimeInheritFromAnnotationKey: parent.Name}
			f.extra = append(f.extra, parent)
			return parent
		}},
		{name: "model", setup: func(_ *testing.T, f *runtimeFixture) client.Object { return f.model() }},
		{name: "revision", setup: func(t *testing.T, f *runtimeFixture) client.Object { return f.pin(t, true) }},
		{name: "standing member", setup: func(t *testing.T, f *runtimeFixture) client.Object { f.pin(t, false); return f.standing }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newRuntimeFixture()
			dependency := tt.setup(t, &f)
			cl := fixtureClient(t, f)
			got, err := (Resolver{Client: cl, OperatorNamespace: f.namespace}).Resolve(t.Context(), f.service, f.standing)
			if err != nil {
				t.Fatal(err)
			}
			if err := cl.Get(t.Context(), client.ObjectKeyFromObject(dependency), dependency); err != nil {
				t.Fatal(err)
			}
			dependency.SetAnnotations(map[string]string{"changed": "true"})
			if err := cl.Update(t.Context(), dependency); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(true, got.Check(t.Context()) != nil); diff != "" {
				t.Errorf("changed dependency accepted:\n%s", diff)
			}
		})
	}
}

func TestResolveDependencyAdditions(t *testing.T) {
	for _, tt := range []struct {
		name    string
		prepare func(*runtimeFixture) client.Object
		wantErr bool
	}{
		{name: "namespaced model shadows cluster model", prepare: func(f *runtimeFixture) client.Object {
			model := f.model()
			return &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: model.Name, Namespace: f.service.Namespace, UID: "namespaced-model"}, Spec: *model.Spec.DeepCopy()}
		}, wantErr: true},
		{name: "namespaced runtime shadows cluster runtime", prepare: func(f *runtimeFixture) client.Object {
			f.service.Spec.Runtime.Kind = nil
			local := f.namespaced()
			f.extra = nil
			return local
		}, wantErr: true},
		{name: "automatic catalog gains namespaced candidate", prepare: func(f *runtimeFixture) client.Object {
			f.model()
			f.service.Spec.Runtime = nil
			local := f.namespaced()
			f.extra = f.extra[:1]
			return local
		}, wantErr: true},
		{name: "other namespace does not change automatic catalog", prepare: func(f *runtimeFixture) client.Object {
			f.model()
			f.service.Spec.Runtime = nil
			local := f.namespaced()
			local.Namespace = "team-b"
			f.extra = f.extra[:1]
			return local
		}},
		{name: "member service appears", prepare: func(f *runtimeFixture) client.Object { return f.service.DeepCopy() }, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newRuntimeFixture()
			addition := tt.prepare(&f)
			cl := fixtureClient(t, f)
			got, err := (Resolver{Client: cl}).Resolve(t.Context(), f.service, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := cl.Create(t.Context(), addition); err != nil {
				t.Fatal(err)
			}
			err = got.Check(t.Context())
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Errorf("dependency addition:\n%s\n%v", diff, err)
			}
		})
	}
}

func TestResolveReadErrors(t *testing.T) {
	for _, target := range []string{"member", "model", "runtime", "catalog", "revision"} {
		t.Run(target, func(t *testing.T) {
			f := newRuntimeFixture()
			switch target {
			case "model":
				f.model()
			case "runtime":
				f.service.Spec.Runtime.AutoSync = ptr.To(false)
			case "catalog":
				f.model()
				f.service.Spec.Runtime = nil
			case "revision":
				f.pin(t, true)
			}
			want := errors.New("member read denied")
			cl := interceptor.NewClient(fixtureClient(t, f), interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
					match := false
					switch object.(type) {
					case *v1beta1.InferenceService:
						match = target == "member"
					case *v1beta1.BaseModel, *v1beta1.ClusterBaseModel:
						match = target == "model"
					case *v1beta1.ServingRuntime, *v1beta1.ClusterServingRuntime:
						match = target == "runtime"
					case *appsv1.ControllerRevision:
						match = target == "revision"
					}
					if match {
						return want
					}
					return cl.Get(ctx, key, object, opts...)
				},
				List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if target == "catalog" {
						return want
					}
					return cl.List(ctx, list, opts...)
				},
			})
			got, err := (Resolver{Client: cl, OperatorNamespace: f.namespace}).Resolve(t.Context(), f.service, nil)
			if diff := cmp.Diff(true, errors.Is(err, want)); diff != "" {
				t.Errorf("API error lost:\n%s\n%v", diff, err)
			}
			if got != nil {
				t.Fatal("failed read returned a resolution")
			}
		})
	}
}

func TestResolveInputChurn(t *testing.T) {
	for _, tt := range []struct {
		name       string
		changeRead int
	}{
		{name: "between runtime reads", changeRead: 2},
		{name: "during final dependency check", changeRead: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newRuntimeFixture()
			calls := 0
			cl := interceptor.NewClient(fixtureClient(t, f), interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
				if err := cl.Get(ctx, key, object, opts...); err != nil {
					return err
				}
				if _, ok := object.(*v1beta1.ClusterServingRuntime); ok {
					calls++
					if calls >= tt.changeRead {
						object.SetResourceVersion("changed")
					}
				}
				return nil
			}})
			got, err := (Resolver{Client: cl}).Resolve(t.Context(), f.service, nil)
			if diff := cmp.Diff(true, err != nil); diff != "" {
				t.Error(diff)
			}
			if got != nil {
				t.Fatal("changing runtime returned a resolution")
			}
		})
	}
}

func TestResolveMissingInputs(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*Resolver, **v1beta1.InferenceService)
	}{
		{name: "client", edit: func(r *Resolver, _ **v1beta1.InferenceService) { r.Client = nil }},
		{name: "service", edit: func(_ *Resolver, s **v1beta1.InferenceService) { *s = nil }},
		{name: "name", edit: func(_ *Resolver, s **v1beta1.InferenceService) { (*s).Name = "" }},
		{name: "namespace", edit: func(_ *Resolver, s **v1beta1.InferenceService) { (*s).Namespace = "" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newRuntimeFixture()
			r := Resolver{Client: fixtureClient(t, f)}
			tt.edit(&r, &f.service)
			_, err := r.Resolve(t.Context(), f.service, nil)
			if diff := cmp.Diff(true, err != nil); diff != "" {
				t.Error(diff)
			}
		})
	}
}

func TestRuntimeCheckRequiresObservation(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value *Runtime
	}{
		{name: "nil runtime"},
		{name: "unobserved runtime", value: &Runtime{}},
		{name: "empty observations", value: &Runtime{reads: &snapshotClient{}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(true, tt.value.Check(t.Context()) != nil); diff != "" {
				t.Error(diff)
			}
		})
	}
}
