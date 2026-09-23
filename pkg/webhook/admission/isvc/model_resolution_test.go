package isvc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

func TestPrimaryModelAdmissionResolution(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	uri := "pvc://other-team:model-pvc/model"
	local := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "team", UID: "local-uid"}}
	cluster := &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", UID: "cluster-uid"}, Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: &uri}}}
	for _, tc := range []struct {
		name, gate, kind, wantUID string
		wantErr                   bool
	}{
		{name: "legacy defaulted kind", kind: "ClusterBaseModel", wantUID: "local-uid"},
		{name: "pending cluster", gate: "pending", kind: "ClusterBaseModel", wantUID: "cluster-uid", wantErr: true},
		{name: "admitted cluster", gate: "admitted", kind: "ClusterBaseModel", wantUID: "cluster-uid", wantErr: true},
		{name: "gated namespaced", gate: "pending", kind: "BaseModel", wantUID: "local-uid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(local, cluster).Build()
			isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "team", Annotations: map[string]string{}}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "model", Kind: &tc.kind}}}
			if tc.gate != "" {
				isvc.Annotations[constants.ArtifactStartupGateAnnotation] = tc.gate
			}
			validator := &InferenceServiceValidator{Client: c}
			err := validator.validateModelExists(context.Background(), isvc)
			if tc.wantErr {
				require.ErrorContains(t, err, "another namespace")
			} else {
				require.NoError(t, err)
			}
			_, metadata, _, err := isvcutils.ReconcileBaseModelWithStatus(c, isvc)
			require.NoError(t, err)
			require.EqualValues(t, tc.wantUID, metadata.UID)
		})
	}
}

func TestPrimaryModelRuntimeResolution(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	local := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "team"}, Spec: v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "local-format"}}}
	cluster := &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model"}, Spec: v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "cluster-format"}}}
	runtimeFor := func(name, format string) *v1beta1.ClusterServingRuntime {
		return &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1beta1.ServingRuntimeSpec{SupportedModelFormats: []v1beta1.SupportedModelFormat{{ModelFormat: &v1beta1.ModelFormat{Name: format}, AutoSelect: boolPtr(true)}}}}
	}
	for _, gate := range []string{"", "pending", "admitted"} {
		for _, explicit := range []bool{false, true} {
			name := gate + "/auto"
			if explicit {
				name = gate + "/explicit"
			}
			t.Run(name, func(t *testing.T) {
				c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(local, cluster, runtimeFor("local-runtime", "local-format"), runtimeFor("cluster-runtime", "cluster-format")).Build()
				isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "team", Annotations: map[string]string{}}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "model", Kind: stringPtr("ClusterBaseModel")}, Engine: &v1beta1.EngineSpec{}}}
				wantRuntime := "local-runtime"
				if gate != "" {
					isvc.Annotations[constants.ArtifactStartupGateAnnotation] = gate
					wantRuntime = "cluster-runtime"
				}
				if explicit {
					isvc.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: wantRuntime}
				}
				validator := &InferenceServiceValidator{Client: c, RuntimeSelector: runtimeselector.New(c)}
				warnings, err := validator.validateRuntimeAndModelResolution(context.Background(), isvc)
				require.NoError(t, err)
				if explicit {
					require.Empty(t, warnings, "validate compatibility against the selected model, not the same-named peer")
				} else {
					require.EqualValues(t, []string{"Runtime " + wantRuntime + " will be auto-selected for model model"}, warnings)
				}
				model, _, _, err := isvcutils.ReconcileBaseModelWithStatus(c, isvc)
				require.NoError(t, err)
				selected, err := validator.RuntimeSelector.SelectRuntime(context.Background(), model, isvc)
				require.NoError(t, err)
				require.Equal(t, wantRuntime, selected.Name)
			})
		}
	}
}

func TestPrimaryModelRuntimeResolutionEmptyReference(t *testing.T) {
	validator := &InferenceServiceValidator{}
	isvc := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{}, Engine: &v1beta1.EngineSpec{}}}
	require.NotPanics(t, func() {
		_, err := validator.validateRuntimeAndModelResolution(context.Background(), isvc)
		require.Error(t, err)
	})
}
