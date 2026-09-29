package modelagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	modelslister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/xet"
)

func TestSharedOrdinaryReadyLabelsRejectReplacedChild(t *testing.T) {
	s, task, input := newSharedEvictionFixture(t)
	ctx := context.Background()
	replacement := task.BaseModel.DeepCopy()
	replacement.UID = "replacement-uid"
	delete(replacement.Annotations, artifactResidencyAnnotation)
	_, err := s.omeClient.OmeV1beta1().BaseModels(replacement.Namespace).Update(ctx, replacement, metav1.UpdateOptions{})
	require.NoError(t, err)
	label, err := getModelLabelKey(&NodeLabelOp{BaseModel: task.BaseModel})
	require.NoError(t, err)
	node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	node.Labels[label] = string(Updating)
	_, err = s.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	_ = s.updateHfArtifactChildLabels(ctx, map[string]ModelStatus{input.ChildModelKey: ModelStatusReady})
	node, err = s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, string(Updating), node.Labels[label])
}

func TestSharedRestoreReplacementClaimsInterruptedCleanup(t *testing.T) {
	for _, stage := range []string{"pending", "files removed", "restart"} {
		t.Run(stage, func(t *testing.T) {
			s, task, input := newSharedRestoreFixture(t)
			ctx := context.Background()
			h := s.sharedHfArtifactHandler()
			_, err := h.repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
			require.NoError(t, err)
			if stage == "files removed" {
				input.RetainDeletionReceipt = true
				result, err := h.handleDelete(ctx, input)
				require.NoError(t, err)
				require.Equal(t, hfArtifactTaskDone, result.Outcome)
			}
			if stage == "restart" {
				s.configMapReconciler = NewConfigMapReconciler(s.configMapReconciler.nodeName, s.configMapReconciler.namespace, s.kubeClient, s.logger)
				WithArtifactEviction(s.omeClient)(s)
				s.hfArtifactHandlerOnce = sync.Once{}
				h = s.sharedHfArtifactHandler()
			}
			replacement := task.BaseModel.DeepCopy()
			replacement.UID = "replacement-uid"
			task.BaseModel = replacement
			_, err = s.omeClient.OmeV1beta1().BaseModels(replacement.Namespace).Update(ctx, replacement, metav1.UpdateOptions{})
			require.NoError(t, err)
			indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
			require.NoError(t, indexer.Add(replacement))
			s.baseModelLister = modelslister.NewBaseModelLister(indexer)
			handled, waiting, err := s.resumeHfArtifactChildDeletion(ctx, task, true)
			require.NoError(t, err)
			require.True(t, handled)
			require.False(t, waiting)
			pending, err := h.repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			require.Nil(t, pending)
			assertChildPathMissing(t, input.ChildModelPath)
			require.NoDirExists(t, input.Parent.LocalPath)
			require.Equal(t, replacement.UID, s.configMapReconciler.modelCache[input.ChildModelKey].ModelUID)
			next, eligible, err := newHfArtifactTaskInputForOCI(task, task.BaseModel.Spec.Storage, s.modelRootDir)
			require.NoError(t, err)
			require.True(t, eligible)
			result, err := s.runHfArtifactDownload(ctx, task, next, true, func(string) (bool, error) { return false, nil }, writeTestHfArtifactFiles)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome, "%+v", result)
			cm, err := s.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			child, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			require.Equal(t, replacement.UID, child.ModelUID)
			require.Equal(t, artifactRehydrationID(task), child.ArtifactRehydrationID)
		})
	}
}

func TestSharedRestoreReplacementCleanupRejectsChangedAuthority(t *testing.T) {
	for _, stage := range []string{"stale lister", "before receipt CAS", "after receipt CAS", "changed request", "changed receipt"} {
		t.Run(stage, func(t *testing.T) {
			s, task, input := newSharedRestoreFixture(t)
			ctx := context.Background()
			h := s.sharedHfArtifactHandler()
			_, err := h.repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
			require.NoError(t, err)
			replacement := task.BaseModel.DeepCopy()
			replacement.UID = "replacement-uid"
			task.BaseModel = replacement
			_, err = s.omeClient.OmeV1beta1().BaseModels(replacement.Namespace).Update(ctx, replacement, metav1.UpdateOptions{})
			require.NoError(t, err)
			indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
			listed := replacement.DeepCopy()
			if stage == "stale lister" {
				listed.UID = input.ChildModelUID
			}
			require.NoError(t, indexer.Add(listed))
			s.baseModelLister = modelslister.NewBaseModelLister(indexer)
			injected := false
			client := s.kubeClient.(*fake.Clientset)
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				child, err := existingModelEntry(cm.Data, input.ChildModelKey)
				require.NoError(t, err)
				if injected || child.HfArtifactPendingDeletion == nil || child.HfArtifactPendingDeletion.ModelUID != replacement.UID {
					return false, nil, nil
				}
				injected = true
				if stage == "changed receipt" {
					object, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), cm.Namespace, cm.Name)
					require.NoError(t, err)
					concurrent := object.(*corev1.ConfigMap).DeepCopy()
					child, err := existingModelEntry(concurrent.Data, input.ChildModelKey)
					require.NoError(t, err)
					child.HfArtifactPendingDeletion.ModelUID = "concurrent-owner"
					_, err = writeModelEntry(concurrent.Data, input.ChildModelKey, child)
					require.NoError(t, err)
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), concurrent, cm.Namespace))
				} else {
					current := replacement.DeepCopy()
					current.UID = "newer-replacement"
					if stage == "changed request" {
						current.UID = replacement.UID
						current.Annotations[testRestoreAnnotation] = "request-2"
					}
					_, err = s.omeClient.OmeV1beta1().BaseModels(current.Namespace).Update(ctx, current, metav1.UpdateOptions{})
					require.NoError(t, err)
				}
				if stage == "after receipt CAS" {
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
					return true, cm, nil
				}
				return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, cm.Name, errors.New("concurrent replacement"))
			})
			handled, _, err := s.resumeHfArtifactChildDeletion(ctx, task, true)
			require.True(t, handled)
			require.Error(t, err)
			require.Equal(t, stage != "stale lister", injected)
			assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
			require.DirExists(t, input.Parent.LocalPath)
			require.Equal(t, input.ChildModelUID, s.configMapReconciler.modelCache[input.ChildModelKey].ModelUID, "failed handoff must not adopt the replacement locally")
			pending, err := h.repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			require.NotNil(t, pending)
			expected := input.ChildModelUID
			if stage == "after receipt CAS" {
				expected = replacement.UID
			}
			if stage == "changed receipt" {
				expected = "concurrent-owner"
			}
			require.Equal(t, expected, pending.ModelUID)
		})
	}
}

func TestSharedRestoreReplacementThroughNormalHFEntry(t *testing.T) {
	for _, receipt := range []bool{false, true} {
		t.Run(map[bool]string{false: "no receipt", true: "exact receipt"}[receipt], func(t *testing.T) {
			s, task, input := newSharedRestoreFixture(t)
			ctx := context.Background()
			if receipt {
				_, err := s.sharedHfArtifactHandler().repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
				require.NoError(t, err)
			}
			task.BaseModel = task.BaseModel.DeepCopy()
			task.BaseModel.UID = "replacement-uid"
			task.BaseModel.Spec.Storage.StorageUri = ptr("hf://" + input.Parent.Identity.ModelID)
			_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel}}
			s.metrics = NewMetrics(prometheus.NewRegistry())
			downloads := 0
			contents := map[string]string{"config.json": "{}", "model.safetensors": "weights"}
			manifest := testHfSnapshotManifest(contents)
			manifest.SHA = input.Parent.Identity.CommitSHA
			source := directHfSource{
				resolve: func(context.Context, string, string, string, string) (string, error) {
					return input.Parent.Identity.CommitSHA, nil
				},
				manifest: func(context.Context, string, string, string, string) (hfSnapshotManifest, error) {
					return manifest, nil
				},
				download: func(_ context.Context, _ *GopherTask, config *xet.DownloadConfig) error {
					downloads++
					require.NoError(t, os.MkdirAll(config.LocalDir, 0755))
					for name, content := range contents {
						require.NoError(t, os.WriteFile(filepath.Join(config.LocalDir, name), []byte(content), 0600))
					}
					return nil
				},
			}
			err = s.processTaskWithSourceAdapters(task, true, func(ctx context.Context, current *GopherTask, spec v1beta1.BaseModelSpec, allow bool) (bool, error) {
				return source.process(ctx, s, current, spec, allow)
			}, nil)
			if !receipt {
				require.ErrorContains(t, err, "superseded")
				require.Zero(t, downloads)
				assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, downloads)
			cm, err := s.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			child, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			require.Nil(t, child.HfArtifactPendingDeletion)
			require.Equal(t, task.BaseModel.UID, child.ModelUID)
			require.Equal(t, artifactRehydrationID(task), child.ArtifactRehydrationID)
			require.Equal(t, ModelStatusReady, child.Status)
		})
	}
}
