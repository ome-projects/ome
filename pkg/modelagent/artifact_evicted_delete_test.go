package modelagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func newForeignEvictedDeleteFixture(t *testing.T) (*Gopher, *GopherTask) {
	t.Helper()
	s, task := newDirectEvictionFixture(t, "oci://n/ns/b/bucket/o/model")
	ctx := context.Background()
	require.NoError(t, s.processTask(task))
	replacement := task.BaseModel.DeepCopy()
	replacement.UID, replacement.Annotations = "replacement-uid", nil
	require.NoError(t, s.omeClient.OmeV1beta1().BaseModels(replacement.Namespace).Delete(ctx, replacement.Name, metav1.DeleteOptions{}))
	_, err := s.omeClient.OmeV1beta1().BaseModels(replacement.Namespace).Create(ctx, replacement, metav1.CreateOptions{})
	require.NoError(t, err)
	s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{replacement}}
	return s, &GopherTask{TaskType: Delete, BaseModel: replacement, NodeIneligible: true}
}

func TestDeleteRemovesForeignTerminalEviction(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		t.Run(map[bool]string{false: "ineligible", true: "deleting"}[deleting], func(t *testing.T) {
			s, task := newForeignEvictedDeleteFixture(t)
			if deleting {
				now := metav1.Now()
				task.BaseModel.DeletionTimestamp = &now
				_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			// The old UID's completion must not let the replacement skip ordinary deletion.
			path := *task.BaseModel.Spec.Storage.Path
			require.NoError(t, os.MkdirAll(path, 0755))
			require.NoError(t, os.WriteFile(filepath.Join(path, "replacement"), []byte("new bytes"), 0600))
			require.NoError(t, s.processTask(task))
			require.NoDirExists(t, path)
			cm, err := s.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			require.NotContains(t, cm.Data, getModelID(task.BaseModel, nil))
		})
	}
}

func TestForeignTerminalEvictionDeleteRetriesFinalRemoval(t *testing.T) {
	s, task := newForeignEvictedDeleteFixture(t)
	now := metav1.Now()
	task.BaseModel.DeletionTimestamp = &now
	_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	key := getModelID(task.BaseModel, nil)
	client := s.kubeClient.(*fake.Clientset)
	fail := true
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		if _, exists := cm.Data[key]; exists || !fail {
			return false, nil, nil
		}
		return true, nil, errors.New("final removal unavailable")
	})
	require.ErrorContains(t, s.processTask(task), "final removal unavailable")
	fail = false
	task.Sequence = 0
	require.NoError(t, s.processTask(task))
	cm, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	require.NotContains(t, cm.Data, key)
}

func TestForeignTerminalEvictionDeletePreservesNewerCompletion(t *testing.T) {
	s, task := newForeignEvictedDeleteFixture(t)
	client := s.kubeClient.(*fake.Clientset)
	key := getModelID(task.BaseModel, nil)
	injected := false
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		if _, exists := cm.Data[key]; exists || injected {
			return false, nil, nil
		}
		injected = true
		obj, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), cm.Namespace, cm.Name)
		require.NoError(t, err)
		latest := obj.(*corev1.ConfigMap)
		_, err = writeModelEntry(latest.Data, key, ModelEntry{Name: task.BaseModel.Name, ModelUID: "newer-uid", Status: ModelStatusEvicted})
		require.NoError(t, err)
		require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), latest, latest.Namespace))
		return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("newer completion"))
	})
	require.ErrorContains(t, s.processTask(task), "ownership changed")
	require.True(t, injected)
	cm, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, key)
	require.NoError(t, err)
	require.EqualValues(t, "newer-uid", entry.ModelUID)
}
