package modelagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	modelslister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestDeleteCapturedEvictionCannotRemoveRestoration(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, stage := range []string{"pending snapshot", "pending new request", "completed snapshot", "completed new request", "completed CAS conflict"} {
			t.Run(fmt.Sprintf("%s/queued=%t", stage, queued), func(t *testing.T) {
				ctx := context.Background()
				g, task, path := newEvictionTestModel(t)
				if queued {
					g.baseModelLister = modelslister.NewBaseModelLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{}))
					g.clusterBaseModelLister = modelslister.NewClusterBaseModelLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{}))
					g.gopherChan = make(chan *GopherTask, 1)
					g.samePathWaitDelay = time.Millisecond
				}
				key := getModelID(task.BaseModel, nil)
				restored := directArtifactEntry(t, g, task)
				restored.DirectArtifactPath = path
				if stage == "pending snapshot" || stage == "pending new request" {
					require.NoError(t, g.persistDirectEviction(ctx, task, path, false))
				} else {
					require.NoError(t, g.processTask(task))
				}
				captured, err := g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				deleting := task.BaseModel.DeepCopy()
				deleting.Annotations = nil
				deleting.Spec.Storage.NodeSelector = map[string]string{"kubernetes.io/hostname": "another-node"}
				_, err = g.modelClient.OmeV1beta1().BaseModels(deleting.Namespace).Update(ctx, deleting, metav1.UpdateOptions{})
				require.NoError(t, err)
				task.BaseModel, task.TaskType = deleting, Delete
				client := g.kubeClient.(*k8sfake.Clientset)
				g.configMapReconciler = NewConfigMapReconciler(g.configMapReconciler.nodeName, g.configMapReconciler.namespace, client, g.logger)
				injected := false
				restore := func() {
					injected = true
					latest := deleting.DeepCopy()
					latest.Spec.Storage.NodeSelector = nil
					latest.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "new-request"}
					require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Update(v1beta1.SchemeGroupVersion.WithResource("basemodels"), latest, latest.Namespace))
					require.NoError(t, os.MkdirAll(path, 0700))
					require.NoError(t, os.WriteFile(filepath.Join(path, "weights"), []byte("newly restored"), 0600))
					readyCM := captured.DeepCopy()
					if stage == "pending new request" || stage == "completed new request" {
						restored, err = existingModelEntry(captured.Data, key)
						require.NoError(t, err)
						restored.ArtifactRehydrationID = "new-request"
					}
					_, err := writeModelEntry(readyCM.Data, key, restored)
					require.NoError(t, err)
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), readyCM, readyCM.Namespace))
					obj, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", g.configMapReconciler.nodeName)
					require.NoError(t, err)
					node := obj.(*corev1.Node).DeepCopy()
					node.Labels[constants.GetBaseModelLabel(latest.Namespace, latest.Name)] = "Ready"
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
				}
				if stage == "completed CAS conflict" {
					client.PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
						cm := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
						if injected || cm.Data[key] != "" {
							return false, nil, nil
						}
						restore()
						return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, fmt.Errorf("restoration won the CAS"))
					})
				} else {
					client.PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
						if injected {
							return false, nil, nil
						}
						// Another process restores the same UID after this snapshot,
						// before Delete acquires the path lock or removes the entry.
						restore()
						return true, captured.DeepCopy(), nil
					})
				}
				if queued {
					require.NoError(t, g.processTask(task))
					select {
					case retry := <-g.gopherChan:
						assert.NoError(t, g.processTask(retry))
					case <-time.After(time.Second):
						t.Fatal("guarded cleanup failure was not queued")
					}
				} else {
					handled, err := g.resumeDirectEvictionOnDelete(ctx, task)
					require.True(t, handled)
					assert.Error(t, err, "captured cleanup must reject restored ownership")
				}
				require.True(t, injected)
				contents, err := os.ReadFile(filepath.Join(path, "weights"))
				assert.NoError(t, err, "captured cleanup must preserve restored bytes")
				assert.Equal(t, "newly restored", string(contents))
				cm, err := g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				actual, err := existingModelEntry(cm.Data, key)
				assert.NoError(t, err, "captured cleanup must preserve restored Ready ownership")
				assert.Equal(t, restored, actual)
				node, err := client.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				assert.Equal(t, "Ready", node.Labels[constants.GetBaseModelLabel(deleting.Namespace, deleting.Name)])
			})
		}
	}
}

func TestDeleteEvictionRetryRetainsCleanupScope(t *testing.T) {
	for _, complete := range []bool{false, true} {
		for _, next := range []string{"unchanged", "restored", "changed receipt", "new request", "replacement UID", "missing entry"} {
			t.Run(fmt.Sprintf("complete=%t/%s", complete, next), func(t *testing.T) {
				ctx := context.Background()
				g, task, path := newEvictionTestModel(t)
				g.baseModelLister = modelslister.NewBaseModelLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{}))
				g.clusterBaseModelLister = modelslister.NewClusterBaseModelLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{}))
				g.gopherChan = make(chan *GopherTask, 1)
				g.samePathWaitDelay = time.Millisecond
				key := getModelID(task.BaseModel, nil)
				restored := directArtifactEntry(t, g, task)
				restored.DirectArtifactPath = path
				if complete {
					require.NoError(t, g.processTask(task))
				} else {
					require.NoError(t, g.persistDirectEviction(ctx, task, path, false))
				}
				task.BaseModel.Spec.Storage.NodeSelector = map[string]string{"kubernetes.io/hostname": "another-node"}
				_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
				require.NoError(t, err)
				task.TaskType = Delete
				client := g.kubeClient.(*k8sfake.Clientset)
				g.configMapReconciler = NewConfigMapReconciler(g.configMapReconciler.nodeName, g.configMapReconciler.namespace, client, g.logger)
				reads := 0
				client.PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
					reads++
					if reads == 2 {
						return true, nil, fmt.Errorf("temporary ConfigMap failure after capturing cleanup")
					}
					return false, nil, nil
				})
				require.NoError(t, g.processTask(task))
				require.Equal(t, 2, reads)
				select {
				case retry := <-g.gopherChan:
					cm, err := g.configMapReconciler.getConfigMap(ctx)
					require.NoError(t, err)
					if next != "unchanged" {
						require.NoError(t, os.MkdirAll(path, 0700))
						require.NoError(t, os.WriteFile(filepath.Join(path, "weights"), []byte("newly restored"), 0600))
						if next == "missing entry" {
							delete(cm.Data, key)
						} else {
							if next == "changed receipt" || next == "new request" {
								restored.Status = ModelStatusEvicting
								restored.Config = nil
								if next == "changed receipt" {
									restored.DirectArtifactPath = filepath.Join(g.modelRootDir, "different-receipt")
									require.NoError(t, os.Mkdir(restored.DirectArtifactPath, 0700))
									require.NoError(t, os.WriteFile(filepath.Join(restored.DirectArtifactPath, "weights"), []byte("different receipt"), 0600))
								} else {
									restored.ArtifactRehydrationID = "new-request"
								}
								restored.DirectArtifactPendingEviction = &DirectArtifactPendingEviction{ModelUID: task.BaseModel.UID, Path: restored.DirectArtifactPath}
							}
							if next == "replacement UID" {
								restored.ModelUID = "replacement"
								latest := task.BaseModel.DeepCopy()
								latest.UID = restored.ModelUID
								require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Update(v1beta1.SchemeGroupVersion.WithResource("basemodels"), latest, latest.Namespace))
							}
							_, err = writeModelEntry(cm.Data, key, restored)
							require.NoError(t, err)
						}
						_, err = client.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
						require.NoError(t, err)
					}
					assert.NoError(t, g.processTask(retry))
					actual, err := g.configMapReconciler.getConfigMap(ctx)
					require.NoError(t, err)
					if next == "unchanged" {
						require.NoDirExists(t, path, "transient errors must retry the original cleanup")
						require.NotContains(t, actual.Data, key)
					} else {
						assert.Equal(t, cm.Data, actual.Data, "superseded cleanup must preserve current ownership")
						contents, err := os.ReadFile(filepath.Join(path, "weights"))
						assert.NoError(t, err)
						assert.Equal(t, "newly restored", string(contents))
						if next == "changed receipt" {
							assert.FileExists(t, filepath.Join(restored.DirectArtifactPath, "weights"))
						}
					}
				case <-time.After(time.Second):
					t.Fatal("transient cleanup failure was not queued")
				}
			})
		}
	}
}
