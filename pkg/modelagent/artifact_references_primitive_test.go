package modelagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestPersistedArtifactReferencesRetainPendingClaims(t *testing.T) {
	entry := ModelEntry{ModelUID: "uid", Status: ModelStatusUpdating, DirectArtifactPath: "direct",
		DirectArtifactPendingEviction: &DirectArtifactPendingEviction{Path: "direct-receipt"},
		HfArtifactPendingDeletion:     &HfArtifactPendingDeletion{ChildPath: "child", ParentPath: "parent"},
		Config:                        &ModelConfig{Artifact: Artifact{ParentPath: map[string]string{"legacy": "legacy-path"}}}}
	require.ElementsMatch(t, []string{"direct", "direct-receipt", "child", "parent", "legacy-path"}, persistedModelArtifactReferences(entry, false))
	require.ElementsMatch(t, []string{"direct", "direct-receipt", "child", "legacy-path"}, persistedModelArtifactReferences(entry, true))
	entry.Status = ModelStatusEvicted
	require.False(t, completedArtifactEviction(entry), "a status alone cannot release outstanding claims")
	entry.DirectArtifactPendingEviction, entry.HfArtifactPendingDeletion, entry.Config = nil, nil, nil
	require.True(t, completedArtifactEviction(entry))
	require.Empty(t, persistedModelArtifactReferences(entry, false), "completed history must not retain an obsolete path")
}

func TestLiveArtifactReferencesFilterOnlyUnpersistedPlacement(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, persisted := range []bool{false, true} {
			for _, eligible := range []bool{false, true} {
				t.Run(fmt.Sprintf("cluster=%v/persisted=%v/eligible=%v", cluster, persisted, eligible), func(t *testing.T) {
					ctx := context.Background()
					g, owner, path := newDirectArtifactTestModel(t)
					other := owner.BaseModel.DeepCopy()
					other.Name, other.UID = "reader", "reader-uid"
					other.Spec.Storage.Path = nil
					other.Spec.Storage.StorageUri = stringPtr("local://" + path)
					if !eligible {
						other.Spec.Storage.NodeSelector = map[string]string{"pool": "another-node"}
					}
					key := getModelID(other, nil)
					if cluster {
						cbm := &v1beta1.ClusterBaseModel{ObjectMeta: other.ObjectMeta, Spec: other.Spec}
						cbm.Namespace = ""
						key = getModelID(nil, cbm)
						_, err := g.modelClient.OmeV1beta1().ClusterBaseModels().Create(ctx, cbm, metav1.CreateOptions{})
						require.NoError(t, err)
					} else {
						_, err := g.modelClient.OmeV1beta1().BaseModels(other.Namespace).Create(ctx, other, metav1.CreateOptions{})
						require.NoError(t, err)
					}
					if persisted {
						require.NoError(t, g.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
							return writeModelEntry(cm.Data, key, ModelEntry{ModelUID: other.UID, Status: ModelStatusReady})
						}))
					}
					cm, err := g.configMapReconciler.getConfigMap(ctx)
					require.NoError(t, err)
					used, err := g.sharedEvictionPathReferenced(ctx, owner, cm.Data, path, false)
					require.NoError(t, err)
					require.Equal(t, persisted || eligible, used)
				})
			}
		}
	}
}

func TestPersistedSharedReceiptProtectsAliasedDirectPath(t *testing.T) {
	ctx := context.Background()
	g, owner, path := newDirectArtifactTestModel(t)
	alias := filepath.Join(t.TempDir(), "borrowed-parent")
	require.NoError(t, os.Symlink(path, alias))
	require.NoError(t, g.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
		return writeModelEntry(cm.Data, "default.basemodel.old-reader", ModelEntry{ModelUID: "old-reader", Status: ModelStatusUpdating,
			HfArtifactPendingDeletion: &HfArtifactPendingDeletion{ParentPath: alias, ChildPath: filepath.Join(g.modelRootDir, "child")}})
	}))
	cm, err := g.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, err)
	used, err := g.sharedEvictionPathReferenced(ctx, owner, cm.Data, path, false)
	require.NoError(t, err)
	require.True(t, used, "pending parent ownership survives absent CR and alias spelling")
	used, err = g.sharedEvictionPathReferenced(ctx, owner, cm.Data, path, true)
	require.NoError(t, err)
	require.False(t, used, "a receipt parent is not a reference to every child beneath it")
}
