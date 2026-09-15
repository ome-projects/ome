package mutate

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func safeTarget() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"}, ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"), ResourceVersion: "42", Generation: 7,
	}}
}

func TestSafeScalarRefusesUnmistakableCredentialUserinfo(t *testing.T) {
	for _, value := range []string{"https://user:SECRET_PASSWORD@example.invalid", "user:password@host", "ssh://git:password@github.com", "user:@host"} {
		require.False(t, SafeScalar(value), value)
	}
	for _, value := range []string{"user@host", "system:admin", "-prod", "ct1:12345678", "example/path"} {
		require.True(t, SafeScalar(value), value)
	}
}

// A missing identity, ownership or bound check must never admit a mutation.
func TestValidateTargetRejectsUnsafeOwnershipAndIdentity(t *testing.T) {
	cases := []struct {
		name   string
		change func(*v1beta1.InferenceService)
	}{
		{"empty name", func(v *v1beta1.InferenceService) { v.Name = "" }},
		{"invalid name", func(v *v1beta1.InferenceService) { v.Name = "bad/name" }},
		{"wrong kind", func(v *v1beta1.InferenceService) { v.Kind = "ServingRuntime" }},
		{"wrong version type", func(v *v1beta1.InferenceService) { v.APIVersion = "other.io/v1" }},
		{"empty namespace", func(v *v1beta1.InferenceService) { v.Namespace = "" }},
		{"missing uid", func(v *v1beta1.InferenceService) { v.UID = "" }},
		{"missing version", func(v *v1beta1.InferenceService) { v.ResourceVersion = "" }},
		{"invalid generation", func(v *v1beta1.InferenceService) { v.Generation = 0 }},
		{"control uid", func(v *v1beta1.InferenceService) { v.UID = "uid\x1b-secret" }},
		{"hostile version", func(v *v1beta1.InferenceService) { v.ResourceVersion = "Bearer SECRET_TOKEN" }},
		{"deleting", func(v *v1beta1.InferenceService) { now := metav1.Now(); v.DeletionTimestamp = &now }},
		{"placement spec", func(v *v1beta1.InferenceService) { v.Spec.Placement = &v1beta1.PlacementSpec{} }},
		{"placement status", func(v *v1beta1.InferenceService) { v.Status.Placement = &v1beta1.PlacementStatus{} }},
		{"placement finalizer", func(v *v1beta1.InferenceService) { v.Finalizers = []string{"ome.io/placement"} }},
		{"legacy requirements", func(v *v1beta1.InferenceService) {
			v.Annotations = map[string]string{"ome.io/accelerator-requirements": ""}
		}},
		{"legacy selector", func(v *v1beta1.InferenceService) { v.Annotations = map[string]string{"ome.io/cluster-selector": ""} }},
		{"origin label", func(v *v1beta1.InferenceService) { v.Labels = map[string]string{constants.PlacementOrigin: ""} }},
		{"origin annotation", func(v *v1beta1.InferenceService) { v.Annotations = map[string]string{constants.PlacementOriginUID: ""} }},
		{"control plane label", func(v *v1beta1.InferenceService) { v.Labels = map[string]string{constants.PlacementControlPlane: ""} }},
		{"too many annotations", func(v *v1beta1.InferenceService) {
			v.Annotations = map[string]string{}
			for i := 0; i < 257; i++ {
				v.Annotations[strings.Repeat("a", i+1)] = ""
			}
		}},
		{"oversized private annotation", func(v *v1beta1.InferenceService) {
			v.Annotations = map[string]string{"private": strings.Repeat("s", 65537)}
		}},
		{"too many finalizers", func(v *v1beta1.InferenceService) { v.Finalizers = make([]string, 65) }},
		{"oversized label metadata", func(v *v1beta1.InferenceService) { v.Labels = map[string]string{"private": strings.Repeat("s", 65537)} }},
		{"oversized finalizer metadata", func(v *v1beta1.InferenceService) { v.Finalizers = []string{strings.Repeat("s", 65537)} }},
		{"oversized pinned groups", func(v *v1beta1.InferenceService) {
			v.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{Plan: v1beta1.RolloutRunPlan{Groups: make([]v1beta1.RolloutRunGroup, 4)}}}
		}},
		{"oversized pinned components", func(v *v1beta1.InferenceService) {
			v.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{Group: v1beta1.RolloutGroup{Components: make([]v1beta1.ComponentType, 4)}}}}}}
		}},
		{"oversized pinned steps", func(v *v1beta1.InferenceService) {
			v.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{Group: v1beta1.RolloutGroup{Canary: &v1beta1.GroupCanary{Steps: make([]v1beta1.RolloutGroupStep, 21)}}}}}}}
		}},
		{"oversized pinned metrics", func(v *v1beta1.InferenceService) {
			v.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{Group: v1beta1.RolloutGroup{Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{{Analysis: &v1beta1.RolloutAnalysis{Metrics: make([]v1beta1.AnalysisMetric, 11)}}}}}}}}}}
		}},
		{"oversized status metrics", func(v *v1beta1.InferenceService) {
			v.Status.Canary = &v1beta1.CanaryStatus{MetricResults: make([]v1beta1.AnalysisMetricResult, 11)}
		}},
		{"oversized coordination", func(v *v1beta1.InferenceService) {
			v.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{Groups: make([]v1beta1.RolloutCoordinationGroupStatus, 4)}
		}},
		{"oversized coordination members", func(v *v1beta1.InferenceService) {
			v.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{Groups: []v1beta1.RolloutCoordinationGroupStatus{{Components: make([]v1beta1.ComponentType, 4)}}}
		}},
		{"oversized component traffic", func(v *v1beta1.InferenceService) {
			v.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{v1beta1.EngineComponent: {Traffic: make([]v1beta1.ComponentTrafficTarget, 9)}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := safeTarget()
			tc.change(v)
			err := ValidateTarget(v)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "SECRET")
			require.NotContains(t, err.Error(), "\x1b")
		})
	}
	require.Error(t, ValidateTarget(nil))
	require.NoError(t, ValidateTarget(safeTarget()))
}
