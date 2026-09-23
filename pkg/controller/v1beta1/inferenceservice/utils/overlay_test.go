package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestResolveOverlaysSkipsMissingModelKinds(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	baseKind, clusterKind, group := "BaseModel", "ClusterBaseModel", "ome.io"
	for _, tc := range []struct {
		name        string
		kind, group *string
	}{
		{name: "legacy unqualified"},
		{name: "explicit BaseModel", kind: &baseKind},
		{name: "explicit ClusterBaseModel", kind: &clusterKind},
		// CRD defaulting supplies both fields before the controller reads the ISVC.
		{name: "defaulted ClusterBaseModel", kind: &clusterKind, group: &group},
	} {
		t.Run(tc.name, func(t *testing.T) {
			available := &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "available"}}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(available).Build()
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns"},
				Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Overlays: []v1beta1.ModelOverlayRef{
					{Name: "missing", Kind: tc.kind, APIGroup: tc.group},
					{Name: "available", Kind: &clusterKind},
				}}},
			}
			overlays, err := ResolveOverlays(c, isvc)
			require.NoError(t, err)
			require.Len(t, overlays, 2)
			require.True(t, overlays[0].Skipped())
			require.Equal(t, `overlay "missing" not found`, overlays[0].SkipReason)
			require.False(t, overlays[1].Skipped())
			require.Equal(t, "available", overlays[1].Meta.Name)
		})
	}
}

func TestOverlayEnvVarName(t *testing.T) {
	tests := []struct {
		modelName string
		want      string
	}{
		{"foo", "OVERLAY_FOO_MODEL_PATH"},
		{"foo-pvc", "OVERLAY_FOO_PVC_MODEL_PATH"},
		{"llama-70b-pd-test", "OVERLAY_LLAMA_70B_PD_TEST_MODEL_PATH"},
		{"already_underscored", "OVERLAY_ALREADY_UNDERSCORED_MODEL_PATH"},
		{"Mixed-Case-Name", "OVERLAY_MIXED_CASE_NAME_MODEL_PATH"},
	}
	for _, tc := range tests {
		t.Run(tc.modelName, func(t *testing.T) {
			assert.Equal(t, tc.want, OverlayEnvVarName(tc.modelName))
		})
	}
}

// Two overlays whose names sanitize to the same env var must be
// rejected by webhook. The sanitization function itself doesn't
// enforce uniqueness; this test pins the collision behaviour so the
// webhook check has a stable specification to validate against.
func TestSanitizeOverlayName_HyphenUnderscoreCollision(t *testing.T) {
	assert.Equal(t, sanitizeOverlayName("foo-bar"), sanitizeOverlayName("foo_bar"),
		"hyphens and underscores collapse to the same sanitized form — webhook must reject this combination")
}

func TestOverlayMountPath(t *testing.T) {
	assert.Equal(t, "/opt/ml/model-overlays/foo-pvc", OverlayMountPath("foo-pvc"))
	assert.Equal(t, "/opt/ml/model-overlays/llama-70b-pd-test", OverlayMountPath("llama-70b-pd-test"))
}
