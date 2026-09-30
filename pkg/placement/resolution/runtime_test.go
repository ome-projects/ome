package resolution

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimerevision"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

type runtimeFixture struct {
	service, standing *v1beta1.InferenceService
	runtime           *v1beta1.ClusterServingRuntime
	extra             []client.Object
	namespace         string
}

func newRuntimeFixture() runtimeFixture {
	rt := &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime-a", UID: "runtime-uid"}, Spec: v1beta1.ServingRuntimeSpec{
		EngineConfig:  &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(4), MaxReplicas: 8}, Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "runner", Image: "example:v1"}}},
		DecoderConfig: &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(4), MaxReplicas: 8}},
		RouterConfig:  &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1)}},
	}}
	service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "team-a", UID: "service-uid"}, Spec: v1beta1.InferenceServiceSpec{
		Runtime: &v1beta1.ServingRuntimeRef{Name: rt.Name, Kind: ptr.To(runtimeselector.KindClusterServingRuntime)},
		Engine:  &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(2)}},
		Decoder: &v1beta1.DecoderSpec{}, Router: &v1beta1.RouterSpec{},
	}}
	return runtimeFixture{service: service, runtime: rt, namespace: "operator-system"}
}

func (f *runtimeFixture) pin(t *testing.T, explicit bool) *appsv1.ControllerRevision {
	t.Helper()
	data, err := json.Marshal(f.runtime.Spec)
	if err != nil {
		t.Fatal(err)
	}
	rev := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "runtime-pin", Namespace: f.namespace, UID: "revision-uid", Labels: map[string]string{
		constants.RuntimeRevisionOfLabelKey:     f.runtime.Name,
		constants.RuntimeRevisionOfKindLabelKey: runtimeselector.KindClusterServingRuntime,
	}}, Data: runtime.RawExtension{Raw: data}}
	f.extra = append(f.extra, rev)
	f.service.Spec.Runtime.AutoSync = ptr.To(false)
	if explicit {
		f.service.Spec.Runtime.Revision = ptr.To(rev.Name)
	} else {
		f.standing = f.service.DeepCopy()
		f.standing.ResourceVersion = "1"
		f.standing.Status.PinnedRevisionName = rev.Name
	}
	return rev
}

func (f *runtimeFixture) model() *v1beta1.ClusterBaseModel {
	f.service.Spec.Model = &v1beta1.ModelRef{Name: "model-a"}
	model := &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model-a", UID: "model-uid", Generation: 1}, Spec: v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "pytorch"}}}
	f.extra = append(f.extra, model)
	f.runtime.Spec.SupportedModelFormats = []v1beta1.SupportedModelFormat{{ModelFormat: &v1beta1.ModelFormat{Name: "pytorch"}, AutoSelect: ptr.To(true)}}
	return model
}

func (f *runtimeFixture) sharded() *v1beta1.ClusterBaseModel {
	model := f.model()
	model.Spec.Distribution = ptr.To(v1beta1.DistributionSharded)
	model.Status.Conditions = []metav1.Condition{{Type: v1beta1.ModelConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: model.Generation}}
	return model
}

func (f *runtimeFixture) namespaced() *v1beta1.ServingRuntime {
	local := &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: f.runtime.Name, Namespace: f.service.Namespace, UID: "local-runtime"}, Spec: *f.runtime.Spec.DeepCopy()}
	local.Spec.EngineConfig.Runner.Image = "example:local"
	f.extra = append(f.extra, local)
	return local
}

func runtimeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{v1beta1.AddToScheme, appsv1.AddToScheme, corev1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func TestResolveRuntime(t *testing.T) {
	for _, tt := range []struct {
		name          string
		edit          func(*testing.T, *runtimeFixture)
		wantImage     string
		wantNamespace bool
		wantErr       bool
	}{
		{name: "live runtime fills declared components", wantImage: "example:v1"},
		{name: "runtime parent supplies components", edit: func(_ *testing.T, f *runtimeFixture) {
			parent := f.runtime.DeepCopy()
			parent.Name, parent.UID = "parent", "parent-uid"
			f.extra = append(f.extra, parent)
			f.runtime.Annotations = map[string]string{constants.RuntimeInheritFromAnnotationKey: parent.Name}
			f.runtime.Spec.DecoderConfig, f.runtime.Spec.RouterConfig = nil, nil
		}, wantImage: "example:v1"},
		{name: "explicit namespaced runtime wins scope", edit: func(_ *testing.T, f *runtimeFixture) {
			f.namespaced()
			f.service.Spec.Runtime.Kind = ptr.To(runtimeselector.KindServingRuntime)
		}, wantImage: "example:local", wantNamespace: true},
		{name: "cluster scope takes precedence", edit: func(_ *testing.T, f *runtimeFixture) { f.namespaced() }, wantImage: "example:v1"},
		{name: "unspecified scope prefers namespaced runtime", edit: func(_ *testing.T, f *runtimeFixture) {
			f.namespaced()
			f.service.Spec.Runtime.Kind = nil
		}, wantImage: "example:local", wantNamespace: true},
		{name: "live cluster scope permits namespaced fallback", edit: func(_ *testing.T, f *runtimeFixture) {
			f.namespaced()
			f.runtime = nil
		}, wantImage: "example:local", wantNamespace: true},
		{name: "explicit namespaced scope cannot fall back", edit: func(_ *testing.T, f *runtimeFixture) {
			f.service.Spec.Runtime.Kind = ptr.To(runtimeselector.KindServingRuntime)
		}, wantErr: true},
		{name: "automatic selection uses member model and catalog", edit: func(_ *testing.T, f *runtimeFixture) {
			f.model()
			f.service.Spec.Runtime = nil
		}, wantImage: "example:v1"},
		{name: "automatic selection prefers namespaced candidate", edit: func(_ *testing.T, f *runtimeFixture) {
			f.model()
			f.namespaced()
			f.service.Spec.Runtime = nil
		}, wantImage: "example:local", wantNamespace: true},
		{name: "automatic selection without matches holds", edit: func(_ *testing.T, f *runtimeFixture) {
			f.model()
			f.service.Spec.Runtime = nil
			f.runtime.Spec.SupportedModelFormats = nil
		}, wantErr: true},
		{name: "explicit runtime accepts declared format mismatch", edit: func(_ *testing.T, f *runtimeFixture) {
			f.model()
			f.runtime.Spec.SupportedModelFormats = nil
		}, wantImage: "example:v1"},
		{name: "disabled model holds", edit: func(_ *testing.T, f *runtimeFixture) { f.model().Spec.Disabled = ptr.To(true) }, wantErr: true},
		{name: "malformed model holds", edit: func(_ *testing.T, f *runtimeFixture) { f.model().Spec.ModelFormat.Name = "" }, wantErr: true},
		{name: "missing model holds", edit: func(_ *testing.T, f *runtimeFixture) { f.service.Spec.Model = &v1beta1.ModelRef{Name: "missing"} }, wantErr: true},
		{name: "unready sharded model holds", edit: func(_ *testing.T, f *runtimeFixture) { model := f.sharded(); model.Status.Conditions = nil }, wantErr: true},
		{name: "stale sharded readiness holds", edit: func(_ *testing.T, f *runtimeFixture) { model := f.sharded(); model.Generation++ }, wantErr: true},
		{name: "initial pin predicts live without writes", edit: func(_ *testing.T, f *runtimeFixture) { f.service.Spec.Runtime.AutoSync = ptr.To(false) }, wantImage: "example:v1"},
		{name: "initial pin without live source holds", edit: func(_ *testing.T, f *runtimeFixture) {
			f.service.Spec.Runtime.AutoSync = ptr.To(false)
			f.runtime = nil
		}, wantErr: true},
		{name: "source pin status does not seed member pin", edit: func(_ *testing.T, f *runtimeFixture) {
			f.service.Spec.Runtime.AutoSync = ptr.To(false)
			f.service.Status.PinnedRevisionName = "source-only-pin"
		}, wantImage: "example:v1"},
		{name: "initial pin without declared scope uses cluster runtime", edit: func(_ *testing.T, f *runtimeFixture) {
			f.service.Spec.Runtime.AutoSync = ptr.To(false)
			f.service.Spec.Runtime.Kind = nil
		}, wantImage: "example:v1"},
		{name: "initial pin with ambiguous fallback scope holds", edit: func(_ *testing.T, f *runtimeFixture) {
			f.service.Spec.Runtime.AutoSync = ptr.To(false)
			f.namespaced()
			f.runtime = nil
		}, wantErr: true},
		{name: "initial pin with unknown kind holds", edit: func(_ *testing.T, f *runtimeFixture) {
			f.service.Spec.Runtime.AutoSync = ptr.To(false)
			f.service.Spec.Runtime.Kind = ptr.To("UnknownRuntime")
		}, wantErr: true},
		{name: "explicit pin overrides changed live runtime", edit: func(t *testing.T, f *runtimeFixture) {
			f.pin(t, true)
			f.runtime.Spec.EngineConfig.Runner.Image = "example:v2"
		}, wantImage: "example:v1"},
		{name: "enabled pin survives disabled live runtime", edit: func(t *testing.T, f *runtimeFixture) {
			f.pin(t, true)
			f.runtime.Spec.Disabled = ptr.To(true)
		}, wantImage: "example:v1"},
		{name: "disabled pin holds", edit: func(t *testing.T, f *runtimeFixture) {
			f.runtime.Spec.Disabled = ptr.To(true)
			f.pin(t, true)
			f.runtime.Spec.Disabled = nil
		}, wantErr: true},
		{name: "standing pin agrees with live runtime", edit: func(t *testing.T, f *runtimeFixture) { f.pin(t, false) }, wantImage: "example:v1"},
		{name: "standing pin survives source removal", edit: func(t *testing.T, f *runtimeFixture) { f.pin(t, false); f.runtime = nil }, wantImage: "example:v1"},
		{name: "unacknowledged pin drift holds", edit: func(t *testing.T, f *runtimeFixture) {
			f.pin(t, false)
			f.runtime.Spec.EngineConfig.Runner.Image = "example:v2"
		}, wantErr: true},
		{name: "consumed sync token cannot advance pin", edit: func(t *testing.T, f *runtimeFixture) {
			f.pin(t, false)
			f.runtime.Spec.EngineConfig.Runner.Image = "example:v2"
			f.service.Annotations = map[string]string{constants.RuntimeSyncAnnotationKey: "sync-a"}
			f.standing.Status.LastRuntimeSyncToken = "sync-a"
		}, wantErr: true},
		{name: "sync acknowledgement advances predicted runtime", edit: func(t *testing.T, f *runtimeFixture) {
			f.pin(t, false)
			f.runtime.Spec.EngineConfig.Runner.Image = "example:v2"
			f.service.Annotations = map[string]string{constants.RuntimeSyncAnnotationKey: "sync-a"}
		}, wantImage: "example:v2"},
		{name: "unconfigured revision namespace holds", edit: func(t *testing.T, f *runtimeFixture) { f.pin(t, true); f.namespace = "" }, wantErr: true},
		{name: "foreign revision holds", edit: func(t *testing.T, f *runtimeFixture) {
			f.pin(t, true).Labels[constants.RuntimeRevisionOfLabelKey] = "another-runtime"
		}, wantErr: true},
		{name: "unidentified revision scope holds", edit: func(t *testing.T, f *runtimeFixture) {
			delete(f.pin(t, true).Labels, constants.RuntimeRevisionOfKindLabelKey)
		}, wantErr: true},
		{name: "missing revision holds", edit: func(t *testing.T, f *runtimeFixture) { f.pin(t, true); f.extra = nil }, wantErr: true},
		{name: "malformed revision holds", edit: func(t *testing.T, f *runtimeFixture) { f.pin(t, true).Data.Raw = []byte("{") }, wantErr: true},
		{name: "cluster revision carrying namespace holds", edit: func(t *testing.T, f *runtimeFixture) {
			f.pin(t, true).Labels[constants.RuntimeRevisionOfNamespaceLabelKey] = f.service.Namespace
		}, wantErr: true},
		{name: "deleted source does not permit revision scope mismatch", edit: func(t *testing.T, f *runtimeFixture) {
			f.pin(t, true)
			f.service.Spec.Runtime.Kind = ptr.To(runtimeselector.KindServingRuntime)
			f.runtime = nil
		}, wantErr: true},
		{name: "namespaced pin preserves its scope", edit: func(t *testing.T, f *runtimeFixture) {
			f.namespaced().Spec = *f.runtime.Spec.DeepCopy()
			rev := f.pin(t, true)
			rev.Labels[constants.RuntimeRevisionOfKindLabelKey] = runtimeselector.KindServingRuntime
			rev.Labels[constants.RuntimeRevisionOfNamespaceLabelKey] = f.service.Namespace
			f.service.Spec.Runtime.Kind = ptr.To(runtimeselector.KindServingRuntime)
		}, wantImage: "example:v1", wantNamespace: true},
		{name: "foreign namespaced pin holds", edit: func(t *testing.T, f *runtimeFixture) {
			rev := f.pin(t, true)
			rev.Labels[constants.RuntimeRevisionOfKindLabelKey] = runtimeselector.KindServingRuntime
			rev.Labels[constants.RuntimeRevisionOfNamespaceLabelKey] = "another-team"
			f.service.Spec.Runtime.Kind = ptr.To(runtimeselector.KindServingRuntime)
			f.runtime = nil
		}, wantErr: true},
		{name: "missing parent holds", edit: func(_ *testing.T, f *runtimeFixture) {
			f.runtime.Annotations = map[string]string{constants.RuntimeInheritFromAnnotationKey: "missing"}
		}, wantErr: true},
		{name: "inheritance cycle holds", edit: func(_ *testing.T, f *runtimeFixture) {
			f.runtime.Annotations = map[string]string{constants.RuntimeInheritFromAnnotationKey: f.runtime.Name}
		}, wantErr: true},
		{name: "disabled runtime holds", edit: func(_ *testing.T, f *runtimeFixture) { f.runtime.Spec.Disabled = ptr.To(true) }, wantErr: true},
		{name: "missing runtime holds", edit: func(_ *testing.T, f *runtimeFixture) { f.runtime = nil }, wantErr: true},
		{name: "unidentified runtime holds", edit: func(_ *testing.T, f *runtimeFixture) { f.runtime.UID = "" }, wantErr: true},
		{name: "missing runtime and model holds", edit: func(_ *testing.T, f *runtimeFixture) { f.service.Spec.Runtime = nil }, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newRuntimeFixture()
			if tt.edit != nil {
				tt.edit(t, &f)
			}
			objects := f.extra
			if f.runtime != nil {
				objects = append(objects, f.runtime)
			}
			if f.standing != nil {
				objects = append(objects, f.standing)
			}
			base := fake.NewClientBuilder().WithScheme(runtimeScheme(t)).WithObjects(objects...).Build()
			writes := 0
			c := interceptor.NewClient(base, interceptor.Funcs{
				Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
					writes++
					return nil
				},
				Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
					writes++
					return nil
				},
				Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
					writes++
					return nil
				},
			})
			before, previous := f.service.DeepCopy(), f.standing.DeepCopy()
			got, err := (Resolver{Client: c, OperatorNamespace: f.namespace}).Resolve(t.Context(), f.service, f.standing)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("%s: %v", diff, err)
			}
			if diff := cmp.Diff(before, f.service); diff != "" {
				t.Errorf("desired service changed:\n%s", diff)
			}
			if diff := cmp.Diff(previous, f.standing); diff != "" {
				t.Errorf("standing service changed:\n%s", diff)
			}
			if diff := cmp.Diff(0, writes); diff != "" {
				t.Errorf("read-only resolver wrote:\n%s", diff)
			}
			if tt.wantErr {
				if got != nil {
					t.Fatal("unresolved runtime returned authority")
				}
				return
			}
			if diff := cmp.Diff(tt.wantImage, got.Engine.Runner.Image); diff != "" {
				t.Error(diff)
			}
			if diff := cmp.Diff(!tt.wantNamespace, got.IsCluster); diff != "" {
				t.Error(diff)
			}
			if diff := cmp.Diff([]v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent}, got.Components()); diff != "" {
				t.Error(diff)
			}
			if diff := cmp.Diff([]int{2, 8, 4, 8, 1}, []int{*got.Engine.MinReplicas, got.Engine.MaxReplicas, *got.Decoder.MinReplicas, got.Decoder.MaxReplicas, *got.Router.MinReplicas}); diff != "" {
				t.Errorf("runtime inheritance or source override lost:\n%s", diff)
			}
			wantHash, _, err := runtimerevision.Hash(got.Spec)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(wantHash, got.Hash); diff != "" {
				t.Error(diff)
			}
			if err := got.Check(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
