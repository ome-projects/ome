package modelagent

import (
	"context"
	"errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"os"
	"path/filepath"
	"sigs.k8s.io/ome/pkg/modelparser"
	"sigs.k8s.io/ome/pkg/xet"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func newInterruptedSharedEvictionFixture(t *testing.T) (*Gopher, *GopherTask, hfArtifactTaskInput) {
	t.Helper()
	s, task := newDirectEvictionFixture(t, "oci://n/ns/b/models/o/artifacts/org/model/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	policy := v1beta1.ReuseIfExists
	task.BaseModel.Spec.Storage.DownloadPolicy = &policy
	task.BaseModel.Annotations[hfModelIDAnnotationKey] = "org/model"
	task.BaseModel.Annotations[hfSHAAnnotationKey] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	ctx := context.Background()
	_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, s.processTask(task))
	require.NoDirExists(t, *task.BaseModel.Spec.Storage.Path)
	task.TaskType, task.Sequence = Download, 0
	delete(task.BaseModel.Annotations, artifactResidencyAnnotation)
	task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "request-1"
	_, err = s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	input, eligible, err := newHfArtifactTaskInputForOCI(task, task.BaseModel.Spec.Storage, s.modelRootDir)
	require.NoError(t, err)
	require.True(t, eligible)
	writes := 0
	_, err = s.runHfArtifactDownload(ctx, task, input, true, nil, func(path string) error {
		writes++
		require.NoError(t, os.WriteFile(filepath.Join(path, "partial"), []byte("partial bytes"), 0600))
		return errors.New("download interrupted")
	})
	require.ErrorContains(t, err, "download interrupted")
	require.Equal(t, 1, writes)
	s.markModelOnNodeFailed(s.artifactRestoreContext(ctx, task), task)
	task.TaskType, task.Sequence = Evict, 0
	delete(task.BaseModel.Annotations, constants.ModelArtifactRehydrationIDAnnotation)
	task.BaseModel.Annotations[artifactResidencyAnnotation] = "Evicted"
	_, err = s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	return s, task, input
}

func TestEvictionCleansInterruptedSharedPreparation(t *testing.T) {
	for _, scenario := range []string{"unreferenced", "other consumer", "restart"} {
		t.Run(scenario, func(t *testing.T) {
			s, task, input := newInterruptedSharedEvictionFixture(t)
			ctx := context.Background()
			var other hfArtifactTaskInput
			if scenario == "other consumer" {
				other = input
				other.ChildModelKey, other.ChildModelUID = "default.basemodel.other", "other-uid"
				other.ChildModelPath = filepath.Join(input.ModelStoreRoot, "other")
				model := task.BaseModel.DeepCopy()
				model.Name, model.UID = "other", other.ChildModelUID
				model.Spec.Storage.Path = &other.ChildModelPath
				delete(model.Annotations, artifactResidencyAnnotation)
				_, err := s.omeClient.OmeV1beta1().BaseModels(model.Namespace).Create(ctx, model, metav1.CreateOptions{})
				require.NoError(t, err)
				seedTestChildModelEntry(t, s.sharedHfArtifactHandler().repository, other)
				require.NoError(t, runTestHfArtifactDownload(s.sharedHfArtifactHandler(), other))
			}
			if scenario == "restart" {
				client := s.kubeClient.(*fake.Clientset)
				interrupted := false
				client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
					require.NoError(t, err)
					if interrupted || entry.HfArtifactPendingDeletion == nil {
						return false, nil, nil
					}
					interrupted = true
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
					return true, nil, errors.New("lost pending cleanup response")
				})
				require.ErrorContains(t, s.processTask(task), "lost pending cleanup response")
				require.True(t, interrupted)
				require.FileExists(t, filepath.Join(input.Parent.LocalPath, "partial"))
				reconciler := NewConfigMapReconciler(s.configMapReconciler.nodeName, s.configMapReconciler.namespace, client, s.logger)
				agent, err := NewGopher(modelparser.NewModelConfigParser(s.logger), reconciler, &xet.Config{}, client,
					1, 1, 1, s.modelRootDir, make(chan *GopherTask, 1), time.Minute,
					NewNodeLabelReconciler(reconciler.nodeName, client, 1, s.logger), s.metrics, s.logger,
					&mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel.DeepCopy()}}, &mockClusterBaseModelLister{},
					WithArtifactEviction(s.omeClient))
				require.NoError(t, err)
				s = agent
			}
			task.Sequence = 0
			require.NoError(t, s.processTask(task))
			cm, err := s.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			require.Equal(t, ModelStatusEvicted, entry.Status)
			require.Nil(t, entry.HfArtifactPendingDownload)
			require.Nil(t, entry.HfArtifactPendingDeletion)
			require.Nil(t, entry.DirectArtifactPendingDeletion)
			_, err = os.Lstat(input.ChildModelPath)
			require.ErrorIs(t, err, os.ErrNotExist)
			if scenario == "other consumer" {
				assertChildSymlinkTarget(t, other.ChildModelPath, input.Parent.LocalPath)
				require.DirExists(t, input.Parent.LocalPath)
			} else {
				require.NoDirExists(t, input.Parent.LocalPath)
			}
			_, err = s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Get(ctx, task.BaseModel.Name, metav1.GetOptions{})
			require.NoError(t, err, "eviction retains the Model CR")
		})
	}
}

func TestDirectEvictionRejectsSharedIntentAtCommit(t *testing.T) {
	s, task := newDirectEvictionFixture(t, "hf://org/model")
	client := s.kubeClient.(*fake.Clientset)
	key, path := getModelID(task.BaseModel, nil), *task.BaseModel.Spec.Storage.Path
	injected := false
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		entry, err := existingModelEntry(cm.Data, key)
		require.NoError(t, err)
		if injected || entry.DirectArtifactPendingDeletion == nil {
			return false, nil, nil
		}
		injected = true
		obj, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), cm.Namespace, cm.Name)
		require.NoError(t, err)
		current := obj.(*corev1.ConfigMap)
		entry, err = existingModelEntry(current.Data, key)
		require.NoError(t, err)
		entry.HfArtifactPendingDownload = &HfArtifactPendingDownload{ModelUID: task.BaseModel.UID, ChildPath: path}
		_, err = writeModelEntry(current.Data, key, entry)
		require.NoError(t, err)
		require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), current, cm.Namespace))
		return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("concurrent Shared intent"))
	})
	require.ErrorContains(t, s.processTask(task), "Direct artifact ownership changed")
	require.True(t, injected)
	require.FileExists(t, filepath.Join(path, "weights"))
	cm, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, key)
	require.NoError(t, err)
	require.Nil(t, entry.DirectArtifactPendingDeletion)
	require.NotNil(t, entry.HfArtifactPendingDownload)
}

func TestSharedIntentRejectsDirectCleanupReceipt(t *testing.T) {
	s, task := newDirectEvictionFixture(t, "hf://org/model")
	seedDirectCleanup(t, s, task)
	input := hfArtifactTaskInput{ChildModelKey: getModelID(task.BaseModel, nil), ChildModelUID: task.BaseModel.UID, ChildModelPath: *task.BaseModel.Spec.Storage.Path}
	require.ErrorContains(t, s.sharedHfArtifactHandler().repository.rememberDownload(context.Background(), input), "unfinished Direct artifact cleanup")
	cm, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
	require.NoError(t, err)
	require.NotNil(t, entry.DirectArtifactPendingDeletion)
	require.Nil(t, entry.HfArtifactPendingDownload)
}
