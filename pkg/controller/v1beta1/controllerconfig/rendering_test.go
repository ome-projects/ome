package controllerconfig

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestRenderingConfigMatchesMemberLoaders(t *testing.T) {
	for _, tt := range []struct {
		name string
		data map[string]string
	}{
		{name: "unconfigured fields stay absent"},
		{name: "configured member rendering", data: map[string]string{
			AcceleratorResourcesConfigName: `["example.com/gpu"]`,
			DeployConfigName:               `{"defaultDeploymentMode":"RawDeployment","terminationGracePeriodSeconds":45,"minReadySeconds":2}`,
			PodMonitorConfigName:           `{"labels":{"monitor":"enabled"}}`,
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace}, Data: tt.data}
			before := cm.DeepCopy()
			got, gotDeploy, err := RenderingConfig(cm)
			if err != nil {
				t.Fatal(err)
			}
			cl := fake.NewSimpleClientset(cm.DeepCopy())
			want, err := NewInferenceServicesConfig(cl)
			if err != nil {
				t.Fatal(err)
			}
			wantDeploy, err := NewDeployConfig(cl)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("component configuration (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(wantDeploy, gotDeploy); diff != "" {
				t.Fatalf("deployment configuration (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(before, cm); diff != "" {
				t.Fatalf("input mutation (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRenderingConfigRejectsInvalidInputs(t *testing.T) {
	for _, tt := range []struct {
		name string
		cm   *corev1.ConfigMap
	}{
		{name: "no configuration"},
		{name: "malformed accelerator names", cm: &corev1.ConfigMap{Data: map[string]string{AcceleratorResourcesConfigName: "{"}}},
		{name: "malformed deploy", cm: &corev1.ConfigMap{Data: map[string]string{DeployConfigName: "{"}}},
		{name: "null deploy", cm: &corev1.ConfigMap{Data: map[string]string{DeployConfigName: " null "}}},
		{name: "incomplete deploy", cm: &corev1.ConfigMap{Data: map[string]string{DeployConfigName: "{}"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, deploy, err := RenderingConfig(tt.cm)
			if err == nil {
				t.Fatal("invalid configuration was accepted")
			}
			if diff := cmp.Diff([]bool{true, true}, []bool{cfg == nil, deploy == nil}); diff != "" {
				t.Fatalf("no partial configuration (-want +got):\n%s", diff)
			}
		})
	}
}
