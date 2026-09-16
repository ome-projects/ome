package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestHfArtifactRepositoryMarkReadyRestoresPersistedChildStatuses(t *testing.T) {
	parent := testHfArtifactWithStatus(testHfArtifactEntry(t), HfArtifactStatusUpdating)
	parent.LockID = "repair-lock"
	statuses := map[string]ModelStatus{
		"default.basemodel.first":    ModelStatusReady,
		"default.basemodel.second":   ModelStatusReady,
		"default.basemodel.updating": ModelStatusUpdating,
		"default.basemodel.failed":   ModelStatusFailed,
	}
	data := make(map[string]string)
	for key := range statuses {
		parent.Children[key] = "/models/" + key
		_, err := writeModelEntry(data, key, ModelEntry{Name: key, Status: ModelStatusFailed, HfArtifactKey: parent.Key})
		require.NoError(t, err)
	}
	// Seed the persisted representation to also cover a retry after process restart.
	encoded, err := json.Marshal(struct {
		HfArtifactEntry
		ChildStatusesBeforeRepair map[string]ModelStatus `json:"childStatusesBeforeRepair,omitempty"`
	}{parent, statuses})
	require.NoError(t, err)
	data[parent.Key] = string(encoded)
	repository, client := newTestHfArtifactRepository(t, data)
	writes := 0
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		writes++
		updated := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		stored, decodeErr := decodeHfArtifactEntry(parent.Key, updated.Data[parent.Key])
		require.NoError(t, decodeErr)
		assert.Equal(t, HfArtifactStatusReady, stored.Status)
		for key, want := range statuses {
			child, childErr := existingModelEntry(updated.Data, key)
			require.NoError(t, childErr)
			assert.Equal(t, want, child.Status, "child %s must be restored with parent Ready", key)
		}
		assert.NotContains(t, updated.Data[parent.Key], "childStatusesBeforeRepair")
		return false, nil, nil
	})

	require.NoError(t, repository.MarkReady(context.Background(), parent))
	assert.Equal(t, 1, writes)
	require.NoError(t, repository.MarkReady(context.Background(), parent))
	assert.Equal(t, 1, writes, "completion retry must be a no-op")
}

func TestHfArtifactRepositoryFailsChildrenAtomicallyAndPreservesSnapshot(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	repository := h.repository
	prior := map[string]ModelStatus{first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusUpdating}
	setTestRepairChildStatus(t, repository, second.ChildModelKey, ModelStatusUpdating)
	parent, acquired, err := repository.TryAcquireLockForRepair(context.Background(), first.Parent)
	require.NoError(t, err)
	require.True(t, acquired)
	client := repository.configMaps.kubeClient.(*fake.Clientset)
	updates := 0
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		updates++
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		stored, err := decodeHfArtifactEntry(parent.Key, cm.Data[parent.Key])
		require.NoError(t, err)
		assert.Equal(t, prior, stored.ChildStatusesBeforeRepair)
		for key := range prior {
			child, err := existingModelEntry(cm.Data, key)
			require.NoError(t, err)
			assert.Equal(t, ModelStatusFailed, child.Status)
		}
		if updates == 1 {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, cm.Name, errors.New("concurrent update"))
		}
		return false, nil, nil
	})

	failed, err := repository.MarkChildrenFailedForRepair(context.Background(), parent)
	require.NoError(t, err)
	assert.Equal(t, 2, updates, "conflict retries must preserve atomicity")
	assert.Equal(t, prior, failed.ChildStatusesBeforeRepair)
	_, err = repository.MarkChildrenFailedForRepair(context.Background(), parent)
	require.NoError(t, err)
	assert.Equal(t, 2, updates, "repeated failure is a no-op")
	require.NoError(t, repository.MarkFailed(context.Background(), parent))
	parent, acquired, err = repository.TryAcquireLockForRepair(context.Background(), first.Parent)
	require.NoError(t, err)
	require.True(t, acquired)
	failed, err = repository.MarkChildrenFailedForRepair(context.Background(), parent)
	require.NoError(t, err)
	assert.Equal(t, prior, failed.ChildStatusesBeforeRepair)
}

func TestHfArtifactRepositoryRepairSkipsReplacedRelationships(t *testing.T) {
	for _, scenario := range []string{"changed parent", "changed child path", "missing reference", "missing child", "independent status change", "no saved status"} {
		t.Run(scenario, func(t *testing.T) {
			h, first, second := newTestHfArtifactRepair(t)
			repository := h.repository
			parent, acquired, err := repository.TryAcquireLockForRepair(context.Background(), first.Parent)
			require.NoError(t, err)
			require.True(t, acquired)
			parent, err = repository.MarkChildrenFailedForRepair(context.Background(), parent)
			require.NoError(t, err)
			require.NoError(t, repository.configMaps.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
				stored, err := decodeHfArtifactEntry(parent.Key, cm.Data[parent.Key])
				if err != nil {
					return false, err
				}
				child, err := existingModelEntry(cm.Data, second.ChildModelKey)
				if err != nil {
					return false, err
				}
				switch scenario {
				case "changed parent":
					child.HfArtifactKey = "artifact.huggingface.other"
				case "changed child path":
					stored.Children[second.ChildModelKey] += "-replacement"
				case "missing reference":
					delete(stored.Children, second.ChildModelKey)
				case "independent status change":
					child.Status = ModelStatusUpdating
				case "no saved status":
					delete(stored.ChildStatusesBeforeRepair, second.ChildModelKey)
				}
				if _, err := writeModelEntry(cm.Data, second.ChildModelKey, child); err != nil {
					return false, err
				}
				if scenario == "missing child" {
					delete(cm.Data, second.ChildModelKey)
				}
				_, err = writeHfArtifactEntry(cm.Data, stored)
				return true, err
			}))
			before, err := repository.configMaps.getConfigMap(context.Background())
			require.NoError(t, err)
			statuses, err := repository.ChildStatusesToRestore(context.Background(), parent)
			require.NoError(t, err)
			assert.Equal(t, map[string]ModelStatus{first.ChildModelKey: ModelStatusReady}, statuses)

			require.NoError(t, repository.MarkReady(context.Background(), parent))

			after, err := repository.configMaps.getConfigMap(context.Background())
			require.NoError(t, err)
			assert.Equal(t, before.Data[second.ChildModelKey], after.Data[second.ChildModelKey])
			assertTestRepairChildStatuses(t, h, map[string]ModelStatus{first.ChildModelKey: ModelStatusReady})
			assert.Empty(t, testRepairParent(t, h, first).ChildStatusesBeforeRepair)
		})
	}
}

func TestHfArtifactRepositoryRemoveReferenceDropsSavedRepairStatus(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	parent, acquired, err := h.repository.TryAcquireLockForRepair(context.Background(), first.Parent)
	require.NoError(t, err)
	require.True(t, acquired)
	parent, err = h.repository.MarkChildrenFailedForRepair(context.Background(), parent)
	require.NoError(t, err)
	require.NoError(t, h.repository.MarkFailed(context.Background(), parent))

	result, err := h.repository.RemoveModelReference(context.Background(), parent, first.ChildModelKey, first.ChildModelUID, first.ChildModelPath)

	require.NoError(t, err)
	assert.True(t, result.ReferenceRemoved)
	assert.Equal(t, map[string]ModelStatus{second.ChildModelKey: ModelStatusReady}, result.Artifact.ChildStatusesBeforeRepair)
	assert.Equal(t, result.Artifact.ChildStatusesBeforeRepair, testRepairParent(t, h, first).ChildStatusesBeforeRepair)
}

func TestHfArtifactRepositoryRepairRejectsStaleLockAndParentPath(t *testing.T) {
	for _, scenario := range []string{"stale lock", "stale parent path", "corrupt child"} {
		t.Run(scenario, func(t *testing.T) {
			h, first, _ := newTestHfArtifactRepair(t)
			parent, acquired, err := h.repository.TryAcquireLockForRepair(context.Background(), first.Parent)
			require.NoError(t, err)
			require.True(t, acquired)
			switch scenario {
			case "stale lock":
				parent.LockID = "stale-lock"
			case "stale parent path":
				parent.LocalPath = "/replacement-root" + parent.LocalPath
			case "corrupt child":
				cm, err := h.repository.configMaps.getConfigMap(context.Background())
				require.NoError(t, err)
				cm.Data[first.ChildModelKey] = "{"
				_, err = h.repository.configMaps.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			before, err := h.repository.configMaps.getConfigMap(context.Background())
			require.NoError(t, err)
			_, err = h.repository.MarkChildrenFailedForRepair(context.Background(), parent)
			require.Error(t, err)
			after, err := h.repository.configMaps.getConfigMap(context.Background())
			require.NoError(t, err)
			assert.Equal(t, before.Data, after.Data)
		})
	}
}
