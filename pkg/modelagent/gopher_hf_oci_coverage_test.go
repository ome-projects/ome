package modelagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	modelslister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

func TestSharedOCIChildLabelsFollowLiveModels(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, state := range []string{"live", "deleted", "missing", "no lister", "node error"} {
			name := "BaseModel/" + state
			if cluster {
				name = "ClusterBaseModel/" + state
			}
			t.Run(name, func(t *testing.T) {
				s, task, _ := newTestHfArtifactGopher(t)
				client := s.configMapReconciler.kubeClient.(*k8sfake.Clientset)
				nodeName := s.configMapReconciler.nodeName
				_, err := client.CoreV1().Nodes().Create(context.Background(), &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: nodeName, Labels: map[string]string{"unrelated": "retained"}},
				}, metav1.CreateOptions{})
				require.NoError(t, err)
				s.nodeLabelReconciler = NewNodeLabelReconciler(nodeName, client, 1, s.logger)
				indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
				metadata := task.BaseModel.ObjectMeta
				if state == "deleted" {
					now := metav1.Now()
					metadata.DeletionTimestamp = &now
				}
				var key, label string
				if cluster {
					metadata.Namespace = ""
					model := &v1beta1.ClusterBaseModel{ObjectMeta: metadata}
					key = getModelID(nil, model)
					label = constants.GetClusterBaseModelLabel(model.Name)
					s.clusterBaseModelLister = modelslister.NewClusterBaseModelLister(indexer)
					if state != "missing" {
						require.NoError(t, indexer.Add(model))
					}
					if state == "no lister" {
						s.clusterBaseModelLister = nil
					}
				} else {
					model := &v1beta1.BaseModel{ObjectMeta: metadata}
					key = getModelID(model, nil)
					label = constants.GetBaseModelLabel(model.Namespace, model.Name)
					s.baseModelLister = modelslister.NewBaseModelLister(indexer)
					if state != "missing" {
						require.NoError(t, indexer.Add(model))
					}
					if state == "no lister" {
						s.baseModelLister = nil
					}
				}
				nodeErr := errors.New("node unavailable")
				if state == "node error" {
					client.PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
						return true, nil, nodeErr
					})
				}
				before, err := s.configMapReconciler.getConfigMap(context.Background())
				require.NoError(t, err)
				err = s.updateHfArtifactChildLabels(context.Background(), map[string]ModelStatus{key: ModelStatusFailed})
				switch state {
				case "no lister":
					require.ErrorContains(t, err, "lister is unavailable")
				case "node error":
					require.ErrorContains(t, err, nodeErr.Error())
				default:
					require.NoError(t, err)
				}
				node, err := client.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				if state == "live" {
					require.Equal(t, string(ModelStatusFailed), node.Labels[label])
				} else {
					require.NotContains(t, node.Labels, label)
				}
				require.Equal(t, "retained", node.Labels["unrelated"])
				after, err := s.configMapReconciler.getConfigMap(context.Background())
				require.NoError(t, err)
				require.Equal(t, before.Data, after.Data, "label synchronization must not overwrite persisted repair statuses")
			})
		}
	}
}

func TestSharedOCIChildLabelsRejectInvalidAndCanceledUpdates(t *testing.T) {
	s, _, _ := newTestHfArtifactGopher(t)
	s.nodeLabelReconciler = NewNodeLabelReconciler("node-1", s.configMapReconciler.kubeClient, 1, s.logger)
	require.ErrorContains(t, s.updateHfArtifactChildLabels(context.Background(), map[string]ModelStatus{"invalid": ModelStatusReady}), "invalid shared artifact child key")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.updateHfArtifactChildLabels(ctx, map[string]ModelStatus{"default.basemodel.model": ModelStatusReady}), context.Canceled)
}

func TestSharedOCIStatusRechecksReferenceAndReleasesLock(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	handler := s.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	client := s.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	gets := 0
	client.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		gets++
		if gets != 2 {
			return false, nil, nil
		}
		object, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), action.GetNamespace(), action.(ktesting.GetAction).GetName())
		require.NoError(t, err)
		cm := object.(*corev1.ConfigMap)
		child, err := existingModelEntry(cm.Data, input.ChildModelKey)
		require.NoError(t, err)
		child.HfArtifactKey = ""
		_, err = writeModelEntry(cm.Data, input.ChildModelKey, child)
		require.NoError(t, err)
		return true, cm, nil
	})
	_, err := s.lockHfChildStatus(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready})
	require.ErrorContains(t, err, "reference changed")
	unlock, acquired := handler.tryParentOperation(input.Parent.Key)
	require.True(t, acquired, "a rejected status update must release its parent operation lock")
	unlock()
}

func TestSharedOCIPreserveHandlesMissingAndBusyReferences(t *testing.T) {
	for _, scenario := range []string{"missing", "busy", "lookup error", "changed parent", "missing marker"} {
		t.Run(scenario, func(t *testing.T) {
			s, _, input := newTestHfArtifactGopher(t)
			handler := s.sharedHfArtifactHandler()
			if scenario != "missing" {
				require.NoError(t, runTestHfArtifactDownload(handler, input))
			}
			switch scenario {
			case "busy":
				unlock, acquired := handler.tryParentOperation(input.Parent.Key)
				require.True(t, acquired)
				defer unlock()
			case "lookup error":
				client := s.configMapReconciler.kubeClient.(*k8sfake.Clientset)
				client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("cannot read ownership")
				})
			case "changed parent":
				input.Parent.Key += ".old"
			case "missing marker":
				require.NoError(t, os.Remove(filepath.Join(input.Parent.LocalPath, constants.HfArtifactReadyMarkerFileName)))
			}
			result, err := s.releaseHfArtifactChild(context.Background(), input, true)
			require.NoError(t, err)
			if scenario == "missing" || scenario == "missing marker" {
				require.Equal(t, hfArtifactTaskDone, result.Outcome)
			} else {
				require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			}
			if scenario != "missing" {
				require.DirExists(t, input.Parent.LocalPath)
				require.True(t, handler.files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
			}
			if scenario == "missing marker" {
				parent, found, err := handler.repository.Get(context.Background(), input.Parent.Identity)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, HfArtifactStatusFailed, parent.Status)
				require.Empty(t, parent.Children)
				require.Empty(t, parent.LockID)
			}
		})
	}
}

func TestSharedOCIUnavailableOwnershipNeverFallsBackToDownload(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	task.SharedArtifact = true
	s.samePathWaitDelay = time.Millisecond
	client := s.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	lookupErr := errors.New("ownership unavailable")
	client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, lookupErr
	})
	result, err := s.runHfArtifactDownload(context.Background(), task, input, true, nil, func(string) error {
		t.Fatal("must not download without ownership")
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskRetry, result.Outcome)
	require.ErrorContains(t, result.RetryReason, lookupErr.Error())
	_, finish, proceed, err := s.beginTask(task)
	defer finish(false)
	require.NoError(t, err)
	require.False(t, proceed)
	select {
	case retried := <-s.gopherChan:
		require.Same(t, task, retried)
	case <-time.After(time.Second):
		t.Fatal("ownership lookup failure must queue a retry")
	}
	require.NoDirExists(t, input.Parent.LocalPath)
}

func TestSharedOCIValidationReportsCredentialFailure(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	parameters := map[string]string{"auth": "unsupported-test-auth"}
	task.BaseModel.Spec.Storage.Parameters = &parameters
	valid, err := s.validateHfOCIArtifact(context.Background(), task, &ociobjectstore.ObjectURI{}, input.Parent.LocalPath)
	require.False(t, valid)
	require.ErrorContains(t, err, "failed to create ociobjectstore data store")
	require.NoDirExists(t, input.Parent.LocalPath)
}

func TestSharedOCIWorkerPublishesReuseAndDeletion(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	s.logger = zaptest.NewLogger(t).Sugar()
	handler := s.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	client := s.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	_, err := client.CoreV1().Nodes().Create(context.Background(), &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: s.configMapReconciler.nodeName, Labels: map[string]string{"kubernetes.io/hostname": s.configMapReconciler.nodeName}},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	s.nodeLabelReconciler = NewNodeLabelReconciler(s.configMapReconciler.nodeName, client, 1, s.logger)
	s.metrics = NewMetrics(prometheus.NewRegistry())
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, indexer.Add(task.BaseModel))
	s.baseModelLister = modelslister.NewBaseModelLister(indexer)
	require.NoError(t, s.processTask(task))
	cm, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
	require.NoError(t, err)
	require.Equal(t, ModelStatusReady, entry.Status)
	require.Equal(t, input.Parent.Key, entry.HfArtifactKey)
	node, err := client.CoreV1().Nodes().Get(context.Background(), s.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, string(Ready), node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
	require.NoError(t, s.processTask(&GopherTask{TaskType: Delete, BaseModel: task.BaseModel}))
	cm, err = s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	require.NotContains(t, cm.Data, input.ChildModelKey)
	require.NotContains(t, cm.Data, input.Parent.Key)
	require.NoDirExists(t, input.Parent.LocalPath)
	assertChildPathMissing(t, input.ChildModelPath)
}

func TestSharedOCIDetachDefersWhileParentIsUpdating(t *testing.T) {
	for _, source := range []string{"oci", "hf"} {
		t.Run(source, func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			handler := s.sharedHfArtifactHandler()
			require.NoError(t, runTestHfArtifactDownload(handler, input))
			_, acquired, err := handler.repository.TryAcquireLockForRepair(context.Background(), input.Parent)
			require.NoError(t, err)
			require.True(t, acquired)
			task.BaseModel.Spec.Storage.DownloadPolicy = nil
			s.samePathWaitDelay = time.Millisecond
			var waiting bool
			if source == "oci" {
				var handled bool
				handled, waiting, err = s.processHfOCIArtifact(context.Background(), task, task.BaseModel.Spec, true)
				require.True(t, handled)
			} else {
				waiting, err = s.detachHfArtifactForDefaultDownload(context.Background(), task, task.BaseModel.Spec, true)
			}
			require.NoError(t, err)
			require.True(t, waiting)
			select {
			case retry := <-s.gopherChan:
				require.Same(t, task, retry)
			case <-time.After(time.Second):
				t.Fatal("source change must retry until shared files are no longer updating")
			}
			require.True(t, handler.files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
			parent, found, err := handler.repository.GetParentForChild(context.Background(), input.ChildModelKey)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, HfArtifactStatusUpdating, parent.Status)
		})
	}
}

func TestSharedOCIRoutingPreservesOrdinaryDeletionSemantics(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, source := range []string{"oci://n/ns/b/bucket/o/model", "hf://Org/Model", "local:///models/local", "vendor://vendor/model", "pvc://volume/model"} {
			name := "BaseModel/" + source
			if cluster {
				name = "ClusterBaseModel/" + source
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				s, task, input := newTestHfArtifactGopher(t)
				task.TaskType = Delete
				task.BaseModel.Annotations = nil
				task.BaseModel.Spec.Storage.DownloadPolicy = nil
				task.BaseModel.Spec.Storage.StorageUri = &source
				if cluster {
					task.ClusterBaseModel = &v1beta1.ClusterBaseModel{ObjectMeta: task.BaseModel.ObjectMeta, Spec: task.BaseModel.Spec}
					task.ClusterBaseModel.Namespace = ""
					task.BaseModel = nil
				}
				client := s.configMapReconciler.kubeClient.(*k8sfake.Clientset)
				_, err := client.CoreV1().Nodes().Create(context.Background(), &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: s.configMapReconciler.nodeName, Labels: map[string]string{"kubernetes.io/hostname": s.configMapReconciler.nodeName}},
				}, metav1.CreateOptions{})
				require.NoError(t, err)
				s.nodeLabelReconciler = NewNodeLabelReconciler(s.configMapReconciler.nodeName, client, 1, s.logger)
				require.NoError(t, s.configMapReconciler.ReconcileModelStatus(context.Background(), &ConfigMapStatusOp{
					BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel, ModelStatus: ModelStatusReady,
				}))
				if source == "hf://Org/Model" {
					require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
						key := getModelID(task.BaseModel, task.ClusterBaseModel)
						entry, err := existingModelEntry(cm.Data, key)
						if err != nil {
							return false, err
						}
						entry.Config = &ModelConfig{Artifact: Artifact{ParentPath: map[string]string{key: input.ChildModelPath}}}
						return writeModelEntry(cm.Data, key, entry)
					}))
				}
				require.NoError(t, s.nodeLabelReconciler.ReconcileNodeLabels(&NodeLabelOp{
					BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel, ModelStateOnNode: Ready,
				}))
				require.NoError(t, os.MkdirAll(input.ChildModelPath, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(input.ChildModelPath, "weights"), []byte("retained when externally managed"), 0o600))
				require.NoError(t, s.processTask(task))
				cm, err := s.configMapReconciler.getConfigMap(context.Background())
				require.NoError(t, err)
				require.NotContains(t, cm.Data, getModelID(task.BaseModel, task.ClusterBaseModel))
				if source == "oci://n/ns/b/bucket/o/model" || source == "hf://Org/Model" {
					require.NoDirExists(t, input.ChildModelPath)
				} else {
					require.FileExists(t, filepath.Join(input.ChildModelPath, "weights"))
				}
			})
		}
	}
}
