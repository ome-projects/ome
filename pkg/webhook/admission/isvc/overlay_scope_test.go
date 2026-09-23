package isvc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

func TestOverlayAdmissionValidatesRenderedModelScope(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	primary := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: "team"}}
	local := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "overlay", Namespace: "team", UID: "local"}}
	cluster := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "overlay", UID: "cluster"},
		Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: ptr.To("pvc://other-team:weights/overlay")}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(primary, local, cluster).Build()
	for _, kind := range []string{"BaseModel", "ClusterBaseModel"} {
		t.Run(kind, func(t *testing.T) {
			service := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "team"},
				Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{
					Name: "primary", Overlays: []v1beta1.ModelOverlayRef{{Name: "overlay", Kind: ptr.To(kind)}},
				}},
			}
			resolved, err := isvcutils.ResolveOverlays(c, service)
			require.NoError(t, err)
			require.Len(t, resolved, 1)
			require.False(t, resolved[0].Skipped())
			validator := &InferenceServiceValidator{Client: c}
			err = validator.validateModelExists(context.Background(), service)
			if kind == "ClusterBaseModel" {
				require.ErrorContains(t, err, "other-team")
				require.Equal(t, cluster.UID, resolved[0].Meta.UID)
			} else {
				require.NoError(t, err)
				require.Equal(t, local.UID, resolved[0].Meta.UID)
			}
		})
	}
}
