package resolution

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestResolveHome(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(*runtimeFixture, *corev1.ConfigMap)
		want    []v1beta1.PlacementComponentFloor
		wantErr bool
	}{
		{name: "source before runtime", want: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 2}, {Component: v1beta1.DecoderComponent, Replicas: 2}, {Component: v1beta1.RouterComponent, Replicas: 1}}},
		{name: "runtime before configuration", edit: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.service.Spec.Engine.MinReplicas = nil
			f.service.Spec.Decoder.MinReplicas = nil
		}, want: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 4}, {Component: v1beta1.DecoderComponent, Replicas: 4}, {Component: v1beta1.RouterComponent, Replicas: 1}}},
		{name: "configured default", edit: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.service.Spec.Engine.MinReplicas = nil
			f.service.Spec.Decoder = nil
			f.service.Spec.Router = nil
			f.runtime.Spec.EngineConfig.MinReplicas = nil
		}, want: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 3}}},
		{name: "unresolved default", edit: func(f *runtimeFixture, cm *corev1.ConfigMap) {
			f.service.Spec.Engine.MinReplicas = nil
			f.runtime.Spec.EngineConfig.MinReplicas = nil
			cm.Data["deploy"] = `{"defaultDeploymentMode":"RawDeployment"}`
		}, wantErr: true},
		{name: "zero engine with a positive decoder", edit: func(f *runtimeFixture, _ *corev1.ConfigMap) { f.service.Spec.Engine.MinReplicas = ptr.To(0) }, want: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent}, {Component: v1beta1.DecoderComponent, Replicas: 2}, {Component: v1beta1.RouterComponent, Replicas: 1}}},
		{name: "explicit zero whole home", edit: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.service.Spec.Engine.MinReplicas, f.service.Spec.Decoder.MinReplicas = ptr.To(0), ptr.To(0)
		}, want: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent}, {Component: v1beta1.DecoderComponent}, {Component: v1beta1.RouterComponent, Replicas: 1}}},
		{name: "explicit zero router", edit: func(f *runtimeFixture, _ *corev1.ConfigMap) { f.service.Spec.Router.MinReplicas = ptr.To(0) }, want: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 2}, {Component: v1beta1.DecoderComponent, Replicas: 2}, {Component: v1beta1.RouterComponent}}},
		{name: "decoder floor above the engine floor", edit: func(f *runtimeFixture, _ *corev1.ConfigMap) { f.service.Spec.Decoder.MinReplicas = ptr.To(5) }, want: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 2}, {Component: v1beta1.DecoderComponent, Replicas: 5}, {Component: v1beta1.RouterComponent, Replicas: 1}}},
		{name: "ratio floors with an independent router", edit: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.service.Spec.Engine.MinReplicas, f.service.Spec.Decoder.MinReplicas, f.service.Spec.Router.MinReplicas = ptr.To(60), ptr.To(40), ptr.To(8)
		}, want: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 60}, {Component: v1beta1.DecoderComponent, Replicas: 40}, {Component: v1beta1.RouterComponent, Replicas: 8}}},
		{name: "ratio floors inherited from the runtime", edit: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.service.Spec.Engine.MinReplicas, f.service.Spec.Decoder.MinReplicas = nil, nil
			f.runtime.Spec.EngineConfig.MinReplicas, f.runtime.Spec.DecoderConfig.MinReplicas = ptr.To(6), ptr.To(4)
		}, want: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 6}, {Component: v1beta1.DecoderComponent, Replicas: 4}, {Component: v1beta1.RouterComponent, Replicas: 1}}},
		{name: "router backend is required", edit: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.service.Spec.Router.Annotations = map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}
		}, wantErr: true},
		{name: "operator namespace required for inherited floor", edit: func(f *runtimeFixture, _ *corev1.ConfigMap) {
			f.namespace = ""
			f.service.Spec.Engine.MinReplicas = nil
			f.runtime.Spec.EngineConfig.MinReplicas = nil
		}, wantErr: true},
		{name: "unrelated malformed config unused", edit: func(_ *runtimeFixture, cm *corev1.ConfigMap) { cm.Data["deploy"] = "invalid" }, want: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 2}, {Component: v1beta1.DecoderComponent, Replicas: 2}, {Component: v1beta1.RouterComponent, Replicas: 1}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newRuntimeFixture()
			f.service.Spec.DeploymentMode = ptr.To(constants.OMENative)
			f.service.Spec.Decoder.MinReplicas = ptr.To(2)
			cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: f.namespace, UID: "config-uid"}, Data: map[string]string{"deploy": `{"defaultDeploymentMode":"RawDeployment","replicas":{"defaultMinReplicas":3}}`}}
			f.extra = append(f.extra, cm)
			if tt.edit != nil {
				tt.edit(&f, cm)
			}
			before := f.service.DeepCopy()
			_, got, err := (Resolver{Client: fixtureClient(t, f), OperatorNamespace: f.namespace}).ResolveHome(t.Context(), f.service, nil)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("error (-want +got): %s: %v", diff, err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(before, f.service); diff != "" {
				t.Fatalf("source mutated: %s", diff)
			}
		})
	}
}

func TestHomeConfigurationSnapshot(t *testing.T) {
	f := newRuntimeFixture()
	f.service.Spec.DeploymentMode = ptr.To(constants.OMENative)
	f.service.Spec.Engine.MinReplicas = nil
	f.runtime.Spec.EngineConfig.MinReplicas = nil
	f.service.Spec.Decoder = nil
	f.service.Spec.Router = nil
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: f.namespace, UID: "config-uid"}, Data: map[string]string{"deploy": `{"defaultDeploymentMode":"RawDeployment","replicas":{"defaultMinReplicas":3}}`}}
	f.extra = append(f.extra, cm)
	cl := fixtureClient(t, f)
	resolved, _, err := (Resolver{Client: cl, OperatorNamespace: f.namespace}).ResolveHome(t.Context(), f.service, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.Get(t.Context(), client.ObjectKeyFromObject(cm), cm); err != nil {
		t.Fatal(err)
	}
	cm.Data["deploy"] = `{"defaultDeploymentMode":"RawDeployment","replicas":{"defaultMinReplicas":9}}`
	if err := cl.Update(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	if err := resolved.Check(t.Context()); err == nil {
		t.Fatal("changed member floor remained authorized")
	}
}
