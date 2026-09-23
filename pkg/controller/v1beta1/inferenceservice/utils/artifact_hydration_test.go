package utils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestArtifactStartupGate(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "ns", UID: "model-uid", Annotations: map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "r2"}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(model).Build()
	for _, tc := range []struct {
		name, gate, uid, request string
		wantErr                  bool
	}{
		{"legacy", "", "", "", false},
		{"pending", "pending", "model-uid", "r2", true},
		{"current", "admitted", "model-uid", "r2", false},
		{"old UID", "admitted", "old", "r2", true},
		{"old request", "admitted", "model-uid", "r1", true},
		{"missing request", "admitted", "model-uid", "", true},
		{"unknown gate", "unknown", "model-uid", "r2", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "ns", Annotations: map[string]string{}}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "model"}}}
			if tc.gate != "" {
				isvc.Annotations[constants.ArtifactStartupGateAnnotation] = tc.gate
			}
			isvc.Annotations[constants.ArtifactModelUIDAnnotation] = tc.uid
			isvc.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = tc.request
			err := ValidateArtifactStartupGate(context.Background(), c, isvc)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestModelReconciliationNeverChangesArtifactIntent(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "ns", Annotations: map[string]string{constants.ModelArtifactResidencyAnnotation: constants.ModelArtifactResidencyEvicted, constants.ModelArtifactRehydrationIDAnnotation: "r1"}}}
	service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "ns"}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "model"}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(model, service).Build()
	_, _, _, err := ReconcileBaseModelWithStatus(c, service)
	require.NoError(t, err)
	latest := &v1beta1.BaseModel{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(model), latest))
	require.Equal(t, model.Annotations, latest.Annotations)
	require.Empty(t, service.Annotations)
}

func TestArtifactStartupGateHealthyWithoutRequest(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "ns", UID: "model-uid"}}
	service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Annotations: map[string]string{constants.ArtifactStartupGateAnnotation: "admitted", constants.ArtifactModelUIDAnnotation: "model-uid"}}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "model"}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(model).Build()
	require.NoError(t, ValidateArtifactStartupGate(context.Background(), c, service))
	service.Annotations[constants.ArtifactRehydrationGenerationAnnotation] = "1"
	require.Error(t, ValidateArtifactStartupGate(context.Background(), c, service), "marked restoration must bind a request")
}
