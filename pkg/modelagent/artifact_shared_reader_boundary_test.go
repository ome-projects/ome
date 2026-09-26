package modelagent

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestSharedLocalReaderExternalStoreFailsClosedOnConfigMapLoss(t *testing.T) {
	g, target, input := newSharedEvictionTestModel(t)
	g.modelRootDir = t.TempDir()
	peer := sharedReaderPeer(t, g)
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "late-reader", Namespace: "default", UID: "late-reader"},
		Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + input.Parent.LocalPath)}}}
	task := &GopherTask{TaskType: Download, BaseModel: model}
	injected := false
	var published ModelStatus
	g.modelClient.(*omefake.Clientset).PrependReactor("list", "clusterbasemodels", func(k8stesting.Action) (bool, runtime.Object, error) {
		_, err := os.Lstat(input.ChildModelPath)
		if injected || !os.IsNotExist(err) {
			return false, nil, nil
		}
		// The final reference list has completed; eviction still owns the
		// external parent lock when a fresh peer loses all persisted authority.
		injected = true
		require.NoError(t, g.kubeClient.(*k8sfake.Clientset).Tracker().Delete(corev1.SchemeGroupVersion.WithResource("configmaps"), g.configMapReconciler.namespace, g.configMapReconciler.nodeName))
		require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(model))
		require.NoError(t, peer.processTask(task))
		published = directArtifactEntry(t, peer, task).Status
		return true, &v1beta1.ClusterBaseModelList{}, nil
	})
	require.NoError(t, g.processTask(target))
	require.True(t, injected)
	require.NoDirExists(t, input.Parent.LocalPath)
	require.NotEqual(t, ModelStatusReady, published)
	require.Empty(t, directArtifactEntry(t, peer, task).DirectArtifactPath)
}

func TestSharedLocalReaderExternalParentChecksBothAliasRoots(t *testing.T) {
	for _, location := range []string{"configured store", "shared store", "transitive configured alias", "third shared layout", "managed parent traversal", "same-parent traversal", "protected family traversal", "unmanaged external"} {
		t.Run(location, func(t *testing.T) {
			child := "model-1"
			if location == "protected family traversal" {
				child = "family/model-1"
			}
			g, _, input := newSharedEvictionTestModelAt(t, child)
			if location != "protected family traversal" {
				g.modelRootDir = t.TempDir()
			}
			root := t.TempDir()
			if location == "configured store" || location == "transitive configured alias" {
				root = g.modelRootDir
			}
			if location == "shared store" {
				root = input.ModelStoreRoot
			}
			if location == "third shared layout" {
				root = filepath.Join(root, "_artifacts", "other", "parent")
			}
			alias := filepath.Join(root, "direct-owner", "alias")
			require.NoError(t, os.MkdirAll(filepath.Dir(alias), 0700))
			require.NoError(t, os.Symlink(input.Parent.LocalPath, alias))
			if location == "transitive configured alias" {
				outer := filepath.Join(t.TempDir(), "external-alias")
				require.NoError(t, os.Symlink(alias, outer))
				alias = outer
			}
			if location == "managed parent traversal" || location == "same-parent traversal" || location == "protected family traversal" {
				owner := filepath.Join(g.modelRootDir, "direct-owner")
				if location == "same-parent traversal" {
					owner = filepath.Join(input.Parent.LocalPath, "reader-directory")
				} else if location == "protected family traversal" {
					owner = filepath.Join(filepath.Dir(input.ChildModelPath), "reader-directory")
				}
				require.NoError(t, os.MkdirAll(owner, 0700))
				relative, err := filepath.Rel(owner, input.Parent.LocalPath)
				require.NoError(t, err)
				alias = filepath.Join(t.TempDir(), "outside-alias")
				require.NoError(t, os.Symlink(owner+"/"+relative, alias))
			}
			peer := sharedReaderPeer(t, g)
			model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "alias-reader", Namespace: "default", UID: "alias-reader"},
				Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + alias)}}}
			require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(model))
			task := &GopherTask{TaskType: Download, BaseModel: model}
			require.NoError(t, peer.processTask(task))
			entry := directArtifactEntry(t, peer, task)
			if location == "unmanaged external" || location == "same-parent traversal" || location == "protected family traversal" {
				require.Equal(t, ModelStatusReady, entry.Status)
			} else {
				require.NotEqual(t, ModelStatusReady, entry.Status)
			}
			require.Empty(t, entry.DirectArtifactPath)
			require.Empty(t, entry.HfArtifactKey)
		})
	}
}

func TestSharedLocalReaderOutwardAliasCannotBecomeLegacy(t *testing.T) {
	for _, spelling := range []string{"parent alias", "transitive alias", "unmanaged external"} {
		for _, missingAuthority := range []bool{false, true} {
			t.Run(spelling+map[bool]string{false: "/persisted", true: "/missing authority"}[missingAuthority], func(t *testing.T) {
				g, target, input := newSharedEvictionTestModel(t)
				g.modelRootDir = t.TempDir()
				external := t.TempDir()
				alias := filepath.Join(input.Parent.LocalPath, "external-alias")
				if spelling == "unmanaged external" {
					alias = filepath.Join(t.TempDir(), "external-alias")
				}
				require.NoError(t, os.Symlink(external, alias))
				if spelling == "transitive alias" {
					outer := filepath.Join(t.TempDir(), "outside-alias")
					require.NoError(t, os.Symlink(alias, outer))
					alias = outer
				}
				peer := sharedReaderPeer(t, g)
				model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "outward-reader", Namespace: "default", UID: "outward-reader"},
					Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + alias)}}}
				task := &GopherTask{TaskType: Download, BaseModel: model}
				injected := false
				var published ModelStatus
				g.modelClient.(*omefake.Clientset).PrependReactor("list", "clusterbasemodels", func(k8stesting.Action) (bool, runtime.Object, error) {
					_, err := os.Lstat(input.ChildModelPath)
					if injected || !os.IsNotExist(err) {
						return false, nil, nil
					}
					// A fresh process arrives after the final reference list, while
					// eviction still owns the parent containing the outward alias.
					injected = true
					if missingAuthority {
						require.NoError(t, g.kubeClient.(*k8sfake.Clientset).Tracker().Delete(corev1.SchemeGroupVersion.WithResource("configmaps"), g.configMapReconciler.namespace, g.configMapReconciler.nodeName))
					}
					require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(model))
					require.NoError(t, peer.processTask(task))
					published = directArtifactEntry(t, peer, task).Status
					return true, &v1beta1.ClusterBaseModelList{}, nil
				})
				require.NoError(t, g.processTask(target))
				require.True(t, injected)
				require.NoDirExists(t, input.Parent.LocalPath)
				require.DirExists(t, external)
				if spelling == "unmanaged external" {
					require.Equal(t, ModelStatusReady, published)
				} else {
					require.NotEqual(t, ModelStatusReady, published, "the actual walk traverses a removable shared parent")
				}
				entry := directArtifactEntry(t, peer, task)
				require.Empty(t, entry.DirectArtifactPath)
				require.Empty(t, entry.HfArtifactKey)
			})
		}
	}
}
