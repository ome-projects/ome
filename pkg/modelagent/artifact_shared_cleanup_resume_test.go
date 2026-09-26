package modelagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	modelslister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/xet"
)

func TestSharedReceiptResumersProtectLocalReferences(t *testing.T) {
	for _, resume := range []string{"download", "override", "source and policy change", "child reference", "persisted child", "persisted parent", "lookup failure", "delete", "unreferenced"} {
		t.Run(resume, func(t *testing.T) {
			ctx := context.Background()
			g, evict, input := newSharedEvictionTestModel(t)
			peer := sharedReaderPeer(t, g)
			borrower := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "reader", Namespace: "default", UID: "reader"},
				Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + input.Parent.LocalPath)}}}
			read := &GopherTask{TaskType: Download, BaseModel: borrower}
			api := g.modelClient.(*omefake.Clientset)
			if resume != "child reference" && resume != "persisted child" && resume != "unreferenced" {
				require.NoError(t, api.Tracker().Add(borrower))
				require.NoError(t, peer.processTask(read))
				require.Equal(t, ModelStatusReady, directArtifactEntry(t, peer, read).Status)
			}
			lost, cleanupFinished := false, false
			g.kubeClient.(*k8sfake.Clientset).PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
				cm := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
				if err != nil {
					return false, nil, nil
				}
				if lost {
					cleanupFinished = entry.HfArtifactPendingDeletion == nil
					return false, nil, nil
				}
				if entry.HfArtifactPendingDeletion == nil {
					return false, nil, nil
				}
				lost = true
				require.NoError(t, g.kubeClient.(*k8sfake.Clientset).Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
				return true, nil, errors.New("lost committed receipt response")
			})
			require.NoError(t, g.processTask(evict))
			require.True(t, lost)
			require.NotNil(t, directArtifactEntry(t, g, evict).HfArtifactPendingDeletion)
			latest := evict.BaseModel.DeepCopy()
			delete(latest.Annotations, ArtifactResidencyAnnotation)
			if resume == "source and policy change" {
				latest.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/models/o/replacement")
				latest.Spec.Storage.Path = stringPtr(filepath.Join(g.modelRootDir, "replacement"))
				latest.Spec.Storage.DownloadPolicy = nil
				delete(latest.Annotations, hfModelIDAnnotationKey)
				delete(latest.Annotations, hfSHAAnnotationKey)
			}
			_, err := g.modelClient.OmeV1beta1().BaseModels(latest.Namespace).Update(ctx, latest, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.NoError(t, g.processTask(evict), "withdrawn eviction must release its barrier")
			if resume == "child reference" {
				borrower.Spec.Storage.StorageUri = stringPtr("local://" + input.ChildModelPath)
				require.NoError(t, api.Tracker().Add(borrower))
			}
			if resume == "persisted child" || resume == "persisted parent" {
				path := input.ChildModelPath
				if resume == "persisted parent" {
					path = input.Parent.LocalPath
					require.NoError(t, g.modelClient.OmeV1beta1().BaseModels(borrower.Namespace).Delete(ctx, borrower.Name, metav1.DeleteOptions{}))
				}
				require.NoError(t, g.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
					return writeModelEntry(cm.Data, getModelID(borrower, nil), ModelEntry{Name: borrower.Name, ModelUID: borrower.UID, Status: ModelStatusReady,
						Config: &ModelConfig{Artifact: Artifact{ParentPath: map[string]string{"reader": path}}}})
				}))
			}
			task := &GopherTask{TaskType: Download, BaseModel: latest}
			if resume == "override" {
				task.TaskType = DownloadOverride
			} else if resume == "delete" {
				task.TaskType = Delete
				require.NoError(t, g.modelClient.OmeV1beta1().BaseModels(latest.Namespace).Delete(ctx, latest.Name, metav1.DeleteOptions{}))
			}
			index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			require.NoError(t, index.Add(latest))
			g.baseModelLister = modelslister.NewBaseModelLister(index)
			g.metrics, g.modelConfigParser = peer.metrics, peer.modelConfigParser
			// Stop a replacement download only after cleanup, so network work
			// cannot mask removed bytes. Cleanup itself still runs in processTask.
			api.PrependReactor("get", "basemodels", func(k8stesting.Action) (bool, runtime.Object, error) {
				if cleanupFinished {
					return true, nil, errors.New("stop before replacement download")
				}
				return false, nil, nil
			})
			lookupFailed := false
			if resume == "lookup failure" {
				api.PrependReactor("list", "basemodels", func(k8stesting.Action) (bool, runtime.Object, error) {
					lookupFailed = true
					return true, nil, errors.New("reference lookup unavailable")
				})
			}
			require.NoError(t, g.processTask(task))
			if resume == "unreferenced" {
				require.True(t, cleanupFinished)
				require.NoDirExists(t, input.Parent.LocalPath)
				return
			}
			require.DirExists(t, input.Parent.LocalPath)
			if resume == "child reference" || resume == "persisted child" || resume == "lookup failure" {
				require.True(t, g.sharedHfArtifactHandler().files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
				if resume == "lookup failure" {
					require.True(t, lookupFailed)
					require.NotNil(t, directArtifactEntry(t, g, task).HfArtifactPendingDeletion)
				} else {
					require.True(t, cleanupFinished, "ordinary cleanup may release its receipt while preserving borrowed files")
					require.NotEqual(t, ModelStatusEvicted, directArtifactEntry(t, g, task).Status)
				}
			} else {
				require.Equal(t, ModelStatusReady, directArtifactEntry(t, peer, read).Status)
				_, err := os.Lstat(input.ChildModelPath)
				require.True(t, os.IsNotExist(err))
			}
		})
	}
}

func TestSharedReceiptAppearingAfterDownloadPreflightIsGuarded(t *testing.T) {
	ctx := context.Background()
	g, task, input := newSharedEvictionTestModel(t)
	task.TaskType = Download
	delete(task.BaseModel.Annotations, ArtifactResidencyAnnotation)
	_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	created := false
	g.modelClient.(*omefake.Clientset).PrependReactor("get", "basemodels", func(k8stesting.Action) (bool, runtime.Object, error) {
		if created {
			return false, nil, nil
		}
		created = true
		_, err := g.sharedHfArtifactHandler().repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
		require.NoError(t, err)
		borrower := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "late-reader", Namespace: "default", UID: "late-reader"},
			Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + input.ChildModelPath)}}}
		require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(borrower))
		return false, nil, nil
	})
	result, err := g.runHfArtifactDownload(ctx, task, input, true, nil, func(string) error {
		t.Fatal("pending cleanup cannot reach a downloader")
		return nil
	})
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, hfArtifactTaskRetry, result.Outcome)
	require.True(t, g.sharedHfArtifactHandler().files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
	require.Nil(t, directArtifactEntry(t, g, task).HfArtifactPendingDeletion)
}

func TestSharedReceiptPreservedChildRejectsReplacementSource(t *testing.T) {
	ctx := context.Background()
	g, task, input := newSharedEvictionTestModel(t)
	require.NoError(t, os.WriteFile(filepath.Join(input.Parent.LocalPath, "borrowed-weights"), []byte("keep"), 0o600))
	_, err := g.sharedHfArtifactHandler().repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
	require.NoError(t, err)
	task.TaskType = Download
	delete(task.BaseModel.Annotations, ArtifactResidencyAnnotation)
	task.BaseModel.Spec.Storage.StorageUri = stringPtr("hf://replacement/model")
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	task.HfResolvedRevision = input.Parent.Identity.CommitSHA
	_, err = g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	borrower := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "child-reader", Namespace: "default", UID: "child-reader"},
		Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + input.ChildModelPath)}}}
	require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(borrower))
	downloads := 0
	source := directHfSource{download: func(context.Context, *GopherTask, *xet.DownloadConfig) error {
		downloads++
		return nil
	}}
	waiting, err := source.process(ctx, g, task, task.BaseModel.Spec, true)
	require.Error(t, err, "a preserved child link cannot become a replacement download destination")
	require.False(t, waiting)
	require.Zero(t, downloads)
	require.Nil(t, directArtifactEntry(t, g, task).HfArtifactPendingDeletion)
	contents, err := os.ReadFile(filepath.Join(input.ChildModelPath, "borrowed-weights"))
	require.NoError(t, err, "the retained child must not become a dangling link")
	require.Equal(t, "keep", string(contents))
}

func TestSharedReceiptRevalidatesCurrentTaskUnderLock(t *testing.T) {
	for _, changed := range []string{"UID", "source", "path", "intent"} {
		t.Run(changed, func(t *testing.T) {
			ctx := context.Background()
			g, task, input := newSharedEvictionTestModel(t)
			task.TaskType = Download
			delete(task.BaseModel.Annotations, ArtifactResidencyAnnotation)
			_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			_, err = g.sharedHfArtifactHandler().repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
			require.NoError(t, err)
			reads := 0
			g.modelClient.(*omefake.Clientset).PrependReactor("get", "basemodels", func(k8stesting.Action) (bool, runtime.Object, error) {
				reads++
				if reads == 2 {
					latest := task.BaseModel.DeepCopy()
					switch changed {
					case "UID":
						latest.UID = "replacement"
					case "source":
						latest.Spec.Storage.StorageUri = stringPtr("hf://different/model")
					case "path":
						latest.Spec.Storage.Path = stringPtr(filepath.Join(g.modelRootDir, "new-path"))
					case "intent":
						latest.Annotations[ArtifactResidencyAnnotation] = string(ModelStatusEvicted)
					}
					require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Update(v1beta1.SchemeGroupVersion.WithResource("basemodels"), latest, latest.Namespace))
				}
				return false, nil, nil
			})
			handled, waiting, err := g.resumeHfArtifactChildDeletion(ctx, task, true)
			require.NoError(t, err)
			require.True(t, handled)
			require.True(t, waiting)
			require.GreaterOrEqual(t, reads, 2)
			require.True(t, g.sharedHfArtifactHandler().files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
			require.NotNil(t, directArtifactEntry(t, g, task).HfArtifactPendingDeletion)
		})
	}
}
