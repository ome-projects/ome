package protocol

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestIsMember(t *testing.T) {
	for _, tt := range []struct {
		name    string
		service *v1beta1.InferenceService
		want    bool
	}{
		{name: "nil"},
		{name: "local", service: &v1beta1.InferenceService{}},
		{name: "execution annotation alone", service: &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.PlacementExecution: "malformed"}}}},
		{name: "source affinity alone", service: &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{Placement: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle}}}},
		{name: "origin annotation", service: &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.PlacementOriginUID: "source-a"}}}, want: true},
		{name: "origin label", service: &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{constants.PlacementOrigin: "source-a"}}}, want: true},
		{name: "empty markers", service: &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{constants.PlacementOrigin: ""}, Annotations: map[string]string{constants.PlacementOriginUID: ""}}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, IsMember(tt.service)); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestValidateNativeModes(t *testing.T) {
	for _, tt := range []struct {
		name  string
		modes map[v1beta1.ComponentType]constants.DeploymentModeType
		want  string
	}{
		{name: "unresolved inventory", want: "multicluster placement requires a resolved OMENative engine"},
		{name: "engine", modes: map[v1beta1.ComponentType]constants.DeploymentModeType{v1beta1.EngineComponent: constants.OMENative}},
		{name: "all components", modes: map[v1beta1.ComponentType]constants.DeploymentModeType{v1beta1.EngineComponent: constants.OMENative, v1beta1.DecoderComponent: constants.OMENative, v1beta1.RouterComponent: constants.OMENative}},
		{name: "raw engine", modes: map[v1beta1.ComponentType]constants.DeploymentModeType{v1beta1.EngineComponent: constants.RawDeployment}, want: `multicluster placement requires OMENative engine; resolved backend is "RawDeployment"`},
		{name: "multinode decoder", modes: map[v1beta1.ComponentType]constants.DeploymentModeType{v1beta1.EngineComponent: constants.OMENative, v1beta1.DecoderComponent: constants.MultiNode}, want: `multicluster placement requires OMENative decoder; resolved backend is "MultiNode"`},
		{name: "unresolved router", modes: map[v1beta1.ComponentType]constants.DeploymentModeType{v1beta1.EngineComponent: constants.OMENative, v1beta1.RouterComponent: ""}, want: `multicluster placement requires OMENative router; resolved backend is ""`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := ""
			if err := ValidateNativeModes(tt.modes); err != nil {
				got = err.Error()
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
