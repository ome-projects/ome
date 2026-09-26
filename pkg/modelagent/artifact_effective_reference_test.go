package modelagent

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestDirectEvictionPreservesEffectiveLiveReference(t *testing.T) {
	for _, source := range []string{"local", "hf", "oci"} {
		for _, kind := range []string{"BaseModel", "ClusterBaseModel"} {
			for _, claim := range []string{"unprocessed", "ready-without-config"} {
				for _, spelling := range []string{"nil", "empty"} {
					t.Run(source+"/"+kind+"/"+claim+"/"+spelling, func(t *testing.T) {
						ctx := context.Background()
						g, target, path := newEvictionTestModel(t)
						uri := "local://" + path
						if source != "local" {
							newPath := filepath.Join(g.modelRootDir, source+":")
							require.NoError(t, os.Rename(path, newPath))
							path = newPath
							target.BaseModel.Spec.Storage.Path = &path
							_, err := g.modelClient.OmeV1beta1().BaseModels(target.BaseModel.Namespace).Update(ctx, target.BaseModel, metav1.UpdateOptions{})
							require.NoError(t, err)
							cm, err := g.configMapReconciler.getConfigMap(ctx)
							require.NoError(t, err)
							entry := directArtifactEntry(t, g, target)
							entry.Config.Artifact.ParentPath[getModelID(target.BaseModel, nil)] = path
							_, err = writeModelEntry(cm.Data, getModelID(target.BaseModel, nil), entry)
							require.NoError(t, err)
							_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
							require.NoError(t, err)
							if source == "hf" {
								uri = "hf://org/model"
							} else {
								uri = "oci://n/ns/b/models/o/model"
							}
						}
						other := target.BaseModel.DeepCopy()
						other.Name, other.UID, other.Annotations = "consumer", "consumer-uid", nil
						other.Spec.Storage.StorageUri = &uri
						other.Spec.Storage.Path = nil
						if spelling == "empty" {
							other.Spec.Storage.Path = stringPtr("")
						}
						node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
						require.NoError(t, err)
						node.Labels["kubernetes.io/hostname"] = node.Name
						_, err = g.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
						require.NoError(t, err)
						other.Spec.Storage.NodeSelector = map[string]string{"kubernetes.io/hostname": node.Labels["kubernetes.io/hostname"]}
						scout := Scout{nodeInfo: node, logger: g.logger}
						require.True(t, scout.shouldDownloadModel(other.Spec.Storage))
						key := getModelID(other, nil)
						if kind == "BaseModel" {
							_, err = g.modelClient.OmeV1beta1().BaseModels(other.Namespace).Create(ctx, other, metav1.CreateOptions{})
						} else {
							cluster := &v1beta1.ClusterBaseModel{ObjectMeta: other.ObjectMeta, Spec: other.Spec}
							cluster.Namespace = ""
							key = getModelID(nil, cluster)
							_, err = g.modelClient.OmeV1beta1().ClusterBaseModels().Create(ctx, cluster, metav1.CreateOptions{})
						}
						require.NoError(t, err)
						if claim == "ready-without-config" {
							cm, err := g.configMapReconciler.getConfigMap(ctx)
							require.NoError(t, err)
							_, err = writeModelEntry(cm.Data, key, ModelEntry{Name: other.Name, ModelUID: other.UID, Status: ModelStatusReady})
							require.NoError(t, err)
							_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
							require.NoError(t, err)
						}
						err = g.evictDirectArtifact(ctx, target)
						assert.Error(t, err, "eligible live reference must protect the owned artifact")
						assert.FileExists(t, filepath.Join(path, "weights"), "eviction deleted another Model's effective reference")
					})
				}
			}
		}
	}
}

func TestDirectEvictionPreservesTraversedReferenceEntries(t *testing.T) {
	for _, claim := range []string{"live", "live off-node", "persisted", "persisted off-node", "unrelated live", "unrelated persisted"} {
		t.Run(claim, func(t *testing.T) {
			ctx := context.Background()
			g, target, path := newEvictionTestModel(t)
			node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			node.Labels["kubernetes.io/hostname"] = node.Name
			_, err = g.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			require.NoError(t, err)
			destination := filepath.Join(g.modelRootDir, "other-family", "data")
			require.NoError(t, os.MkdirAll(destination, 0700))
			intermediate := filepath.Join(path, "alias")
			unrelated := claim == "unrelated live" || claim == "unrelated persisted"
			if unrelated {
				intermediate = filepath.Join(filepath.Dir(destination), "alias")
			}
			require.NoError(t, os.Symlink(destination, intermediate))
			outside := filepath.Join(t.TempDir(), "outside-alias")
			require.NoError(t, os.Symlink(intermediate, outside))
			other := target.BaseModel.DeepCopy()
			other.Name, other.UID, other.Annotations = "consumer", "consumer-uid", nil
			other.Spec.Storage.StorageUri = stringPtr("local://" + outside)
			other.Spec.Storage.Path = nil
			if claim == "live off-node" || claim == "persisted off-node" {
				other.Spec.Storage.NodeSelector = map[string]string{"kubernetes.io/hostname": "another-node"}
			}
			persisted := claim == "persisted" || claim == "persisted off-node" || claim == "unrelated persisted"
			if claim != "persisted" && claim != "unrelated persisted" {
				_, err = g.modelClient.OmeV1beta1().BaseModels(other.Namespace).Create(ctx, other, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			if persisted {
				cm, err := g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				entry := directArtifactEntry(t, g, target)
				entry.Name, entry.ModelUID = other.Name, other.UID
				entry.Config.Artifact.ParentPath = map[string]string{getModelID(other, nil): outside}
				_, err = writeModelEntry(cm.Data, getModelID(other, nil), entry)
				require.NoError(t, err)
				_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			err = g.evictDirectArtifact(ctx, target)
			if unrelated || claim == "live off-node" {
				require.NoError(t, err, "common filesystem ancestors are not references to every artifact")
				require.NoDirExists(t, path)
			} else {
				require.Error(t, err, "the reference must preserve its traversed alias, not only its final target")
				info, err := os.Stat(outside)
				require.NoError(t, err)
				require.True(t, info.IsDir())
				require.FileExists(t, filepath.Join(path, "weights"))
			}
		})
	}
}
