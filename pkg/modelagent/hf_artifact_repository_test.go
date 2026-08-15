package modelagent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func newTestHfArtifactRepository(t *testing.T, data map[string]string) (*HfArtifactRepository, *fake.Clientset) {
	t.Helper()
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1", Namespace: "ome"},
		Data:       data,
	}
	client := fake.NewSimpleClientset(configMap)
	reconciler := NewConfigMapReconciler(configMap.Name, configMap.Namespace, client, zaptest.NewLogger(t).Sugar())
	return newHfArtifactRepository(reconciler), client
}

func testHfArtifactEntry(t *testing.T) HfArtifactEntry {
	t.Helper()
	identity, err := newHfArtifactIdentity("Qwen/Qwen3-8B", testHFCommitSHA)
	require.NoError(t, err)
	return HfArtifactEntry{
		Key:       hfArtifactConfigMapKey(identity),
		Identity:  identity,
		LocalPath: "/models/store/_artifacts/Qwen/Qwen3-8B/" + testHFCommitSHA,
	}
}

func testHfArtifactWithStatus(artifact HfArtifactEntry, status HfArtifactStatus) HfArtifactEntry {
	artifact.Status = status
	if artifact.Children == nil {
		artifact.Children = make(map[string]string)
	}
	return artifact
}

func testModelEntryJSON(t *testing.T, name string) string {
	t.Helper()
	encoded, err := json.Marshal(ModelEntry{Name: name, Status: ModelStatusUpdating})
	require.NoError(t, err)
	return string(encoded)
}

func TestHfArtifactRepositoryLockLifecycle(t *testing.T) {
	modelKey := "default.basemodel.model-1"
	repository, _ := newTestHfArtifactRepository(t, map[string]string{modelKey: testModelEntryJSON(t, "model-1")})
	artifact := testHfArtifactEntry(t)

	first, acquired, err := repository.TryAcquireLock(context.Background(), artifact)
	require.NoError(t, err)
	assert.True(t, acquired)
	assert.Equal(t, HfArtifactStatusUpdating, first.Status)
	assert.NotEmpty(t, first.LockID)

	second, acquired, err := repository.TryAcquireLock(context.Background(), artifact)
	require.NoError(t, err)
	assert.False(t, acquired)
	assert.Equal(t, HfArtifactStatusUpdating, second.Status)

	require.NoError(t, repository.MarkReady(context.Background(), first))
	require.NoError(t, repository.AddModelReference(context.Background(), artifact, modelKey, types.UID("model-1-uid"), "/models/model-1"))
	require.NoError(t, repository.AddModelReference(context.Background(), artifact, modelKey, types.UID("model-1-uid"), "/models/model-1"))

	ready, acquired, err := repository.TryAcquireLock(context.Background(), artifact)
	require.NoError(t, err)
	assert.False(t, acquired)
	assert.Equal(t, HfArtifactStatusReady, ready.Status)
	assert.Equal(t, map[string]string{modelKey: "/models/model-1"}, ready.Children)
}

func TestHfArtifactRepositoryGetsParentForChild(t *testing.T) {
	childModelKey := "default.basemodel.model-1"
	repository, client := newTestHfArtifactRepository(t, map[string]string{
		childModelKey: testModelEntryJSON(t, "model-1"),
	})
	parent := testHfArtifactEntry(t)
	lockedParent, acquired, err := repository.TryAcquireLock(context.Background(), parent)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, repository.MarkReady(context.Background(), lockedParent))
	require.NoError(t, repository.AddModelReference(
		context.Background(),
		parent,
		childModelKey,
		types.UID("model-1-uid"),
		"/models/model-1",
	))
	reads := 0
	client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		reads++
		if reads > 1 {
			return true, &corev1.ConfigMap{}, nil
		}
		return false, nil, nil
	})

	foundParent, found, err := repository.GetParentForChild(context.Background(), childModelKey)

	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, parent.Key, foundParent.Key)
	assert.Equal(t, "/models/model-1", foundParent.Children[childModelKey])
	assert.Equal(t, 1, reads, "both reference directions must come from one snapshot")
}

func TestHfArtifactRepositoryGetParentForChildRejectsInvalidRecords(t *testing.T) {
	for _, name := range []string{
		"missing parent", "corrupt parent", "corrupt child", "invalid parent key",
		"missing reverse reference", "empty child path", "invalid parent status",
	} {
		t.Run(name, func(t *testing.T) {
			childKey := "default.basemodel.model-1"
			parent := testHfArtifactWithStatus(testHfArtifactEntry(t), HfArtifactStatusReady)
			parent.Children[childKey] = "/models/model-1"
			child := ModelEntry{Name: "model-1", Status: ModelStatusReady, HfArtifactKey: parent.Key}
			switch name {
			case "invalid parent key":
				child.HfArtifactKey = "default.basemodel.other"
			case "missing reverse reference":
				delete(parent.Children, childKey)
			case "empty child path":
				parent.Children[childKey] = " "
			case "invalid parent status":
				parent.Status = "Unknown"
			}
			data := make(map[string]string)
			_, err := writeHfArtifactEntry(data, parent)
			require.NoError(t, err)
			_, err = writeModelEntry(data, childKey, child)
			require.NoError(t, err)
			switch name {
			case "missing parent":
				delete(data, parent.Key)
			case "corrupt parent":
				data[parent.Key] = "{"
			case "corrupt child":
				data[childKey] = "{"
			}
			repository, _ := newTestHfArtifactRepository(t, data)

			_, found, err := repository.GetParentForChild(context.Background(), childKey)

			require.Error(t, err)
			assert.False(t, found)
		})
	}
}

func TestHfArtifactRepositoryAcquiresFailedArtifactForRepair(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	artifact := testHfArtifactEntry(t)

	locked, acquired, err := repository.TryAcquireLock(context.Background(), artifact)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, repository.MarkFailed(context.Background(), locked))

	locked, acquired, err = repository.TryAcquireLockForRepair(context.Background(), artifact)
	require.NoError(t, err)
	assert.True(t, acquired)
	assert.Equal(t, HfArtifactStatusUpdating, locked.Status)
}

func TestHfArtifactRepositoryFencesStaleLockOwner(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	artifact := testHfArtifactEntry(t)

	first, acquired, err := repository.TryAcquireLock(context.Background(), artifact)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, repository.MarkFailed(context.Background(), first))
	second, acquired, err := repository.TryAcquireLockForRepair(context.Background(), artifact)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NotEqual(t, first.LockID, second.LockID)

	err = repository.MarkReady(context.Background(), first)
	assert.ErrorContains(t, err, "lock ID")
	require.NoError(t, repository.MarkReady(context.Background(), second))
}

func TestHfArtifactRepositoryRejectsStaleOwnerAfterTerminalStatusMatches(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	artifact := testHfArtifactEntry(t)

	first, acquired, err := repository.TryAcquireLock(context.Background(), artifact)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, repository.MarkFailed(context.Background(), first))

	second, acquired, err := repository.TryAcquireLockForRepair(context.Background(), artifact)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, repository.MarkReady(context.Background(), second))

	err = repository.MarkReady(context.Background(), first)
	assert.ErrorContains(t, err, "lock ID")
}

func TestHfArtifactRepositoryTerminalTransitionsAreIdempotent(t *testing.T) {
	tests := []struct {
		name       string
		wantStatus HfArtifactStatus
		transition func(context.Context, *HfArtifactRepository, HfArtifactEntry) error
	}{
		{
			name:       "Ready",
			wantStatus: HfArtifactStatusReady,
			transition: func(ctx context.Context, repository *HfArtifactRepository, artifact HfArtifactEntry) error {
				return repository.MarkReady(ctx, artifact)
			},
		},
		{
			name:       "Failed",
			wantStatus: HfArtifactStatusFailed,
			transition: func(ctx context.Context, repository *HfArtifactRepository, artifact HfArtifactEntry) error {
				return repository.MarkFailed(ctx, artifact)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repository, _ := newTestHfArtifactRepository(t, map[string]string{})
			artifact := testHfArtifactEntry(t)
			locked, acquired, err := repository.TryAcquireLock(context.Background(), artifact)
			require.NoError(t, err)
			require.True(t, acquired)

			require.NoError(t, tt.transition(context.Background(), repository, locked))
			require.NoError(t, tt.transition(context.Background(), repository, locked))

			stored, found, err := repository.Get(context.Background(), artifact.Identity)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, tt.wantStatus, stored.Status)
			assert.Empty(t, stored.LockID)
		})
	}
}

func TestHfArtifactRepositoryPreservesUpdatingOwnershipWhenRepairingMetadata(t *testing.T) {
	artifact := testHfArtifactEntry(t)
	expectedStored := HfArtifactEntry{
		Key:       artifact.Key,
		Status:    HfArtifactStatusUpdating,
		LocalPath: artifact.LocalPath,
		Children:  map[string]string{"default.basemodel.existing": "/models/existing-child"},
		LockID:    "existing-owner",
	}
	encoded, err := json.Marshal(expectedStored)
	require.NoError(t, err)
	repository, _ := newTestHfArtifactRepository(t, map[string]string{artifact.Key: string(encoded)})

	stored, acquired, err := repository.TryAcquireLock(context.Background(), artifact)
	require.NoError(t, err)
	assert.False(t, acquired)
	assert.Equal(t, expectedStored.Children, stored.Children)
	assert.Equal(t, artifact.Identity, stored.Identity)
	assert.Equal(t, expectedStored.LockID, stored.LockID)
}

func TestHfArtifactRepositoryRejectsInvalidOrConflictingIdentity(t *testing.T) {
	expected := testHfArtifactEntry(t)
	tests := []struct {
		name     string
		identity HfArtifactIdentity
	}{
		{
			name: "malformed identity",
			identity: HfArtifactIdentity{
				ModelID:   "../invalid",
				CommitSHA: expected.Identity.CommitSHA,
			},
		},
		{
			name: "different valid identity",
			identity: HfArtifactIdentity{
				ModelID:   "Qwen/Other",
				CommitSHA: expected.Identity.CommitSHA,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stored := testHfArtifactWithStatus(expected, HfArtifactStatusReady)
			stored.Identity = tt.identity
			encoded, err := json.Marshal(stored)
			require.NoError(t, err)
			repository, _ := newTestHfArtifactRepository(t, map[string]string{expected.Key: string(encoded)})

			_, _, err = repository.TryAcquireLock(context.Background(), expected)
			assert.ErrorContains(t, err, "identity")
		})
	}
}

func TestHfArtifactRepositoryAcquiresReadyArtifactAtWrongPathForRebuild(t *testing.T) {
	expected := testHfArtifactEntry(t)
	stored := testHfArtifactWithStatus(expected, HfArtifactStatusReady)
	stored.LocalPath = "/models/old-model-owned-parent"
	encoded, err := json.Marshal(stored)
	require.NoError(t, err)
	repository, _ := newTestHfArtifactRepository(t, map[string]string{expected.Key: string(encoded)})

	stored, acquired, err := repository.TryAcquireLock(context.Background(), expected)
	require.NoError(t, err)
	assert.True(t, acquired)
	assert.Equal(t, expected.LocalPath, stored.LocalPath)
	assert.Equal(t, HfArtifactStatusUpdating, stored.Status)
}

func TestHfArtifactRepositoryDoesNotMoveUpdatingArtifact(t *testing.T) {
	expected := testHfArtifactEntry(t)
	stored := testHfArtifactWithStatus(expected, HfArtifactStatusUpdating)
	stored.LocalPath = "/models/active-download"
	stored.LockID = "active-owner"
	encoded, err := json.Marshal(stored)
	require.NoError(t, err)
	repository, client := newTestHfArtifactRepository(t, map[string]string{expected.Key: string(encoded)})

	_, _, err = repository.TryAcquireLock(context.Background(), expected)
	assert.ErrorContains(t, err, "Updating and uses noncanonical path")

	configMap, err := client.CoreV1().ConfigMaps("ome").Get(context.Background(), "node-1", metav1.GetOptions{})
	require.NoError(t, err)
	storedAfter, err := decodeHfArtifactEntry(expected.Key, configMap.Data[expected.Key])
	require.NoError(t, err)
	assert.Equal(t, stored.LocalPath, storedAfter.LocalPath)
}

func TestHfArtifactRepositoryRejectsInvalidExpectedArtifact(t *testing.T) {
	repository, client := newTestHfArtifactRepository(t, map[string]string{})
	expected := testHfArtifactEntry(t)
	expected.Key = "artifact.huggingface.wrong"

	_, _, err := repository.TryAcquireLock(context.Background(), expected)
	assert.ErrorContains(t, err, "does not match identity")

	expected = testHfArtifactEntry(t)
	expected.LocalPath = ""
	_, _, err = repository.TryAcquireLock(context.Background(), expected)
	assert.ErrorContains(t, err, "path is empty")

	expected = testHfArtifactEntry(t)
	expected.LocalPath = "/models/not_artifacts/Qwen/Qwen3-8B/" + testHFCommitSHA
	_, _, err = repository.TryAcquireLock(context.Background(), expected)
	assert.ErrorContains(t, err, "is not canonical")

	configMap, err := client.CoreV1().ConfigMaps("ome").Get(context.Background(), "node-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Empty(t, configMap.Data)
}

func TestValidateHfArtifactIdentityAndPathAcceptsUppercaseCommitSHA(t *testing.T) {
	identity := HfArtifactIdentity{
		ModelID:   "Qwen/Qwen3-8B",
		CommitSHA: strings.ToUpper(testHFCommitSHA),
	}
	artifact := HfArtifactEntry{
		Key:       hfArtifactConfigMapKey(identity),
		Identity:  identity,
		LocalPath: canonicalHfArtifactPath("/models/store/model-1", identity),
	}

	assert.NoError(t, validateHfArtifactIdentityAndPath(artifact))
}

func TestHfArtifactRepositoryRemovesModelReferenceAndDeletesUnreferencedArtifactEntry(t *testing.T) {
	modelKey := "default.basemodel.model-1"
	repository, client := newTestHfArtifactRepository(t, map[string]string{modelKey: testModelEntryJSON(t, "model-1")})
	artifact := testHfArtifactEntry(t)
	locked, acquired, err := repository.TryAcquireLock(context.Background(), artifact)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, repository.MarkReady(context.Background(), locked))
	require.NoError(t, repository.AddModelReference(context.Background(), artifact, modelKey, types.UID("model-1-uid"), "/models/model-1"))

	removal, err := repository.RemoveModelReference(context.Background(), artifact, modelKey, types.UID("model-1-uid"), "/models/model-1")
	require.NoError(t, err)
	assert.True(t, removal.ReferenceRemoved)
	assert.True(t, removal.LastReferenceRemoved)
	assert.Empty(t, removal.Artifact.Children)
	assert.Equal(t, HfArtifactStatusUpdating, removal.Artifact.Status)

	err = repository.AddModelReference(context.Background(), artifact, modelKey, types.UID("model-1-uid"), "/models/model-1")
	assert.ErrorContains(t, err, "Updating")

	deleted, err := repository.DeleteIfUnreferenced(context.Background(), artifact)
	assert.ErrorContains(t, err, "lock ID")
	assert.False(t, deleted)

	deleted, err = repository.DeleteIfUnreferenced(context.Background(), removal.Artifact)
	require.NoError(t, err)
	assert.True(t, deleted)

	configMap, err := client.CoreV1().ConfigMaps("ome").Get(context.Background(), "node-1", metav1.GetOptions{})
	require.NoError(t, err)
	_, exists := configMap.Data[artifact.Key]
	assert.False(t, exists)
	var model ModelEntry
	require.NoError(t, json.Unmarshal([]byte(configMap.Data[modelKey]), &model))
	assert.Empty(t, model.HfArtifactKey)
}

func TestHfArtifactRepositoryKeepsArtifactWithRemainingReference(t *testing.T) {
	firstKey := "default.basemodel.model-1"
	secondKey := "default.basemodel.model-2"
	repository, _ := newTestHfArtifactRepository(t, map[string]string{
		firstKey:  testModelEntryJSON(t, "model-1"),
		secondKey: testModelEntryJSON(t, "model-2"),
	})
	artifact := testHfArtifactEntry(t)
	locked, acquired, err := repository.TryAcquireLock(context.Background(), artifact)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, repository.MarkReady(context.Background(), locked))
	require.NoError(t, repository.AddModelReference(context.Background(), artifact, firstKey, types.UID("model-1-uid"), "/models/model-1"))
	require.NoError(t, repository.AddModelReference(context.Background(), artifact, secondKey, types.UID("model-2-uid"), "/models/model-2"))

	removal, err := repository.RemoveModelReference(context.Background(), artifact, firstKey, types.UID("model-1-uid"), "/models/model-1")
	require.NoError(t, err)
	assert.False(t, removal.LastReferenceRemoved)
	assert.Equal(t, HfArtifactStatusReady, removal.Artifact.Status)
	assert.Equal(t, map[string]string{secondKey: "/models/model-2"}, removal.Artifact.Children)

	deleted, err := repository.DeleteIfUnreferenced(context.Background(), artifact)
	require.NoError(t, err)
	assert.False(t, deleted)
}

func TestHfArtifactRepositoryMissingArtifactMutationsDoNotCreateEntry(t *testing.T) {
	modelKey := "default.basemodel.model-1"
	repository, client := newTestHfArtifactRepository(t, map[string]string{modelKey: testModelEntryJSON(t, "model-1")})
	artifact := testHfArtifactEntry(t)

	assert.Error(t, repository.MarkReady(context.Background(), artifact))
	assert.Error(t, repository.MarkFailed(context.Background(), artifact))
	assert.Error(t, repository.AddModelReference(context.Background(), artifact, modelKey, types.UID("model-1-uid"), "/models/model-1"))
	removal, err := repository.RemoveModelReference(context.Background(), artifact, modelKey, types.UID("model-1-uid"), "/models/model-1")
	require.NoError(t, err)
	assert.False(t, removal.ReferenceRemoved)

	configMap, err := client.CoreV1().ConfigMaps("ome").Get(context.Background(), "node-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Len(t, configMap.Data, 1)
}

func TestHfArtifactRepositoryRejectsMissingArtifactReferencedByModel(t *testing.T) {
	modelKey := "default.basemodel.model-1"
	artifact := testHfArtifactEntry(t)
	model := ModelEntry{
		Name:          "model-1",
		Status:        ModelStatusReady,
		HfArtifactKey: artifact.Key,
	}
	modelJSON, err := json.Marshal(model)
	require.NoError(t, err)
	repository, _ := newTestHfArtifactRepository(t, map[string]string{modelKey: string(modelJSON)})

	_, err = repository.RemoveModelReference(context.Background(), artifact, modelKey, types.UID("model-1-uid"), "/models/model-1")
	assert.ErrorContains(t, err, "references missing Hugging Face artifact")
}

func TestHfArtifactRepositoryDoesNotStealUpdatingArtifactForDeletion(t *testing.T) {
	modelKey := "default.basemodel.model-1"
	repository, _ := newTestHfArtifactRepository(t, map[string]string{modelKey: testModelEntryJSON(t, "model-1")})
	artifact := testHfArtifactEntry(t)
	locked, acquired, err := repository.TryAcquireLock(context.Background(), artifact)
	require.NoError(t, err)
	require.True(t, acquired)

	_, err = repository.RemoveModelReference(context.Background(), artifact, modelKey, types.UID("model-1-uid"), "/models/model-1")
	assert.ErrorContains(t, err, "is Updating")

	stored, found, err := repository.Get(context.Background(), artifact.Identity)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, locked.LockID, stored.LockID)
}

func TestHfArtifactEntryJSONSchema(t *testing.T) {
	identity := HfArtifactIdentity{
		ModelID:   "Qwen/Qwen3-8B",
		CommitSHA: testHFCommitSHA,
	}
	entry := HfArtifactEntry{
		Key:       hfArtifactConfigMapKey(identity),
		Status:    HfArtifactStatusReady,
		Identity:  identity,
		LocalPath: "/models/store/_artifacts/Qwen/Qwen3-8B/" + testHFCommitSHA,
		Children: map[string]string{
			"default.basemodel.model-1": "/models/store/model-1",
		},
	}

	encoded, err := json.Marshal(entry)
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"key": "artifact.huggingface.Qwen.Qwen3-8B.a82fd547628e.`+testHFCommitSHA+`",
		"status": "Ready",
		"identity": {
			"modelId": "Qwen/Qwen3-8B",
			"commitSha": "`+testHFCommitSHA+`"
		},
		"localPath": "/models/store/_artifacts/Qwen/Qwen3-8B/`+testHFCommitSHA+`",
		"children": {
			"default.basemodel.model-1": "/models/store/model-1"
		}
	}`, string(encoded))
	assert.NotContains(t, string(encoded), `"config"`)
	assert.NotContains(t, string(encoded), `"artifact"`)
}

func TestHfArtifactRepositoryAddModelReferenceWritesBothDirections(t *testing.T) {
	modelKey := "default.basemodel.model-1"
	modelPath := "/models/store/model-1"
	repository, client := newTestHfArtifactRepository(t, map[string]string{modelKey: testModelEntryJSON(t, "model-1")})
	artifact := testHfArtifactEntry(t)

	locked, acquired, err := repository.TryAcquireLock(context.Background(), artifact)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, repository.MarkReady(context.Background(), locked))
	require.NoError(t, repository.AddModelReference(context.Background(), artifact, modelKey, types.UID("model-1-uid"), modelPath))

	configMap, err := client.CoreV1().ConfigMaps("ome").Get(context.Background(), "node-1", metav1.GetOptions{})
	require.NoError(t, err)

	var storedArtifact HfArtifactEntry
	require.NoError(t, json.Unmarshal([]byte(configMap.Data[artifact.Key]), &storedArtifact))
	assert.Equal(t, modelPath, storedArtifact.Children[modelKey])

	var storedModel ModelEntry
	require.NoError(t, json.Unmarshal([]byte(configMap.Data[modelKey]), &storedModel))
	assert.Equal(t, artifact.Key, storedModel.HfArtifactKey)
}

func TestHfArtifactRepositoryRejectsReferencePathReplacement(t *testing.T) {
	modelKey := "default.basemodel.model-1"
	repository, _ := newTestHfArtifactRepository(t, map[string]string{
		modelKey: testModelEntryJSON(t, "model-1"),
	})
	artifact := testHfArtifactEntry(t)
	locked, acquired, err := repository.TryAcquireLock(context.Background(), artifact)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, repository.MarkReady(context.Background(), locked))
	require.NoError(t, repository.AddModelReference(
		context.Background(), artifact, modelKey, types.UID("model-1-uid"), "/models/model-1",
	))

	err = repository.AddModelReference(
		context.Background(), artifact, modelKey, types.UID("model-1-uid"), "/models/model-1-new-path",
	)
	assert.ErrorContains(t, err, "does not match recorded")
	_, err = repository.RemoveModelReference(
		context.Background(), artifact, modelKey, types.UID("model-1-uid"), "/models/model-1-new-path",
	)
	assert.ErrorContains(t, err, "does not match recorded")

	parent, found, getErr := repository.Get(context.Background(), artifact.Identity)
	require.NoError(t, getErr)
	require.True(t, found)
	assert.Equal(t, map[string]string{modelKey: "/models/model-1"}, parent.Children)
}

func TestHfArtifactRepositoryRejectsOneSidedRelationshipOnReferenceRemoval(t *testing.T) {
	modelKey := "default.basemodel.model-1"
	artifact := testHfArtifactWithStatus(testHfArtifactEntry(t), HfArtifactStatusReady)
	artifact.Children = map[string]string{modelKey: "/models/model-1"}
	artifactJSON, err := json.Marshal(artifact)
	require.NoError(t, err)
	repository, _ := newTestHfArtifactRepository(t, map[string]string{
		artifact.Key: string(artifactJSON),
		modelKey:     testModelEntryJSON(t, "model-1"),
	})

	_, err = repository.RemoveModelReference(context.Background(), artifact, modelKey, types.UID("model-1-uid"), "/models/model-1")
	assert.ErrorContains(t, err, "do not contain matching references")
}

func TestHfArtifactRepositoryRejectsStaleModelReferenceMutation(t *testing.T) {
	modelKey := "default.basemodel.model-1"
	currentUID := types.UID("current-model-uid")
	staleUID := types.UID("stale-model-uid")
	repository, client := newTestHfArtifactRepository(t, map[string]string{
		modelKey: testModelEntryJSON(t, "model-1"),
	})
	repository.configMaps.cacheMutex.Lock()
	repository.configMaps.modelCache[modelKey] = &CacheEntry{ModelUID: currentUID}
	repository.configMaps.cacheMutex.Unlock()

	artifact := testHfArtifactEntry(t)
	locked, acquired, err := repository.TryAcquireLock(context.Background(), artifact)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, repository.MarkReady(context.Background(), locked))

	require.NoError(t, repository.AddModelReference(context.Background(), artifact, modelKey, staleUID, "/models/model-1"))
	stored, found, err := repository.Get(context.Background(), artifact.Identity)
	require.NoError(t, err)
	require.True(t, found)
	assert.Empty(t, stored.Children)

	require.NoError(t, repository.AddModelReference(context.Background(), artifact, modelKey, currentUID, "/models/model-1"))
	removal, err := repository.RemoveModelReference(context.Background(), artifact, modelKey, staleUID, "/models/model-1")
	require.NoError(t, err)
	assert.False(t, removal.ReferenceRemoved)

	configMap, err := client.CoreV1().ConfigMaps("ome").Get(context.Background(), "node-1", metav1.GetOptions{})
	require.NoError(t, err)
	stored, err = decodeHfArtifactEntry(artifact.Key, configMap.Data[artifact.Key])
	require.NoError(t, err)
	assert.Equal(t, map[string]string{modelKey: "/models/model-1"}, stored.Children)
	var model ModelEntry
	require.NoError(t, json.Unmarshal([]byte(configMap.Data[modelKey]), &model))
	assert.Equal(t, artifact.Key, model.HfArtifactKey)
}
