package modelagent

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestLegacyArtifactReplacementCanPublishAfterRestart(t *testing.T) {
	for _, stage := range []string{"download", "status"} {
		t.Run(stage, func(t *testing.T) {
			g, task, _ := newDirectArtifactTestModel(t)
			task.BaseModel.UID = "replacement"
			task.BaseModel.Spec.Storage.Path = stringPtr(t.TempDir())
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			ctx := context.Background()
			if stage == "download" {
				release, acquired, err := g.acquireDirectArtifactDownload(ctx, task)
				require.NoError(t, err)
				require.True(t, acquired)
				defer release()
				require.NoError(t, g.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
					BaseModel: task.BaseModel, ModelStatus: ModelStatusReady,
				}))
			} else {
				require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
			}
			require.Equal(t, task.BaseModel.UID, directArtifactEntry(t, g, task).ModelUID)
			require.Equal(t, ModelStatusReady, directArtifactEntry(t, g, task).Status)
		})
	}
}

func TestLegacyArtifactOwnerHandoffRequiresLiveIdentity(t *testing.T) {
	g, task, _ := newDirectArtifactTestModel(t)
	task.BaseModel.Spec.Storage.Path = stringPtr(t.TempDir())
	task.BaseModel.UID = "stale-replacement"
	release, acquired, err := g.acquireDirectArtifactDownload(context.Background(), task)
	if release != nil {
		defer release()
	}
	require.Error(t, err)
	require.False(t, acquired)
	require.EqualValues(t, "uid", directArtifactEntry(t, g, task).ModelUID)
}

func TestArtifactPlacementChangeKeepsEligibleDownload(t *testing.T) {
	for _, placement := range []string{"selector", "affinity"} {
		for _, eligible := range []bool{true, false} {
			name := placement + "/eligible"
			if !eligible {
				name = placement + "/ineligible"
			}
			t.Run(name, func(t *testing.T) {
				g, task, _ := newDirectArtifactTestModel(t)
				node, err := g.kubeClient.CoreV1().Nodes().Get(context.Background(), g.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				node.Labels["pool"] = "gpu"
				_, err = g.kubeClient.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
				require.NoError(t, err)
				live := task.BaseModel.DeepCopy()
				value := "gpu"
				if !eligible {
					value = "other"
				}
				if placement == "selector" {
					live.Spec.Storage.NodeSelector = map[string]string{"pool": value}
				} else {
					live.Spec.Storage.NodeAffinity = &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key: "pool", Operator: corev1.NodeSelectorOpIn, Values: []string{value},
						}}}},
					}}
				}
				g.modelClient = omefake.NewSimpleClientset(live)
				skip, err := g.shouldSkipArtifactTask(context.Background(), task)
				require.NoError(t, err)
				require.Equal(t, !eligible, skip)
				if eligible {
					require.NoError(t, g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{
						BaseModel: task.BaseModel, ModelStateOnNode: Ready,
					}))
					require.Equal(t, ModelStatusReady, directArtifactEntry(t, g, task).Status)
				}
			})
		}
	}
}

// Keep the content comparison distinct from placement compatibility.
func TestArtifactContentChangeStillRejectsDownload(t *testing.T) {
	for _, change := range []func(*v1beta1.BaseModel){
		func(m *v1beta1.BaseModel) { m.Spec.Storage.StorageUri = stringPtr("hf://other/model") },
		func(m *v1beta1.BaseModel) { m.Spec.Storage.Path = stringPtr("/different/path") },
		func(m *v1beta1.BaseModel) { m.Spec.Storage.Parameters = &map[string]string{"revision": "other"} },
	} {
		g, task, _ := newDirectArtifactTestModel(t)
		live := task.BaseModel.DeepCopy()
		change(live)
		g.modelClient = omefake.NewSimpleClientset(live)
		skip, err := g.shouldSkipArtifactTask(context.Background(), task)
		require.NoError(t, err)
		require.True(t, skip)
	}
}

func TestArtifactPolicyComparisonMatchesScout(t *testing.T) {
	for _, tc := range []struct {
		name   string
		uri    string
		policy v1beta1.DownloadPolicy
		stale  bool
	}{
		{name: "explicit default", uri: "hf://org/model", policy: v1beta1.AlwaysDownload},
		{name: "HF policy change", uri: "hf://org/model", policy: v1beta1.ReuseIfExists, stale: true},
		{name: "OCI policy change", uri: "oci://n/ns/b/models/o/model", policy: v1beta1.ReuseIfExists},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, task, _ := newDirectArtifactTestModel(t)
			task.BaseModel.Spec.Storage.StorageUri = &tc.uri
			live := task.BaseModel.DeepCopy()
			live.Spec.Storage.DownloadPolicy = &tc.policy
			g.modelClient = omefake.NewSimpleClientset(live)
			skip, err := g.shouldSkipArtifactTask(context.Background(), task)
			require.NoError(t, err)
			require.Equal(t, tc.stale, skip)
		})
	}
}
