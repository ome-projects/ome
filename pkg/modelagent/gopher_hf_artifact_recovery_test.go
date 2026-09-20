package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func newArtifactRecoveryTestParent(t *testing.T, status ModelStatus) (*Gopher, ArtifactIdentity, string, string) {
	t.Helper()
	identity := ArtifactIdentity{
		OriginType: ArtifactOriginTypeHuggingFace, HFModelID: "test/model",
		HFCommitSHA: "cdbee75f17c01a7cc42f958dc650907174af0554",
	}
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	parentPath := filepath.Join(t.TempDir(), "parent")
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(status, identity.HFModelID, identity.HFCommitSHA, parentKey, parentPath, []string{"existing-child"}),
	}))
	return g, identity, parentKey, parentPath
}

func artifactRecoveryTestEntry(t *testing.T, g *Gopher, parentKey string) ModelEntry {
	t.Helper()
	exists, data, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), parentKey)
	require.NoError(t, err)
	require.True(t, exists)
	var entry ModelEntry
	require.NoError(t, json.Unmarshal([]byte(data), &entry))
	return entry
}

func TestArtifactStartupRecoveryRetriesFailedWrites(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ready      bool
		invalid    bool
		wantStatus ModelStatus
	}{
		{name: "completed download", ready: true, wantStatus: ModelStatusReady},
		{name: "incomplete download", wantStatus: ModelStatusFailed},
		{name: "invalid reservation", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, _, parentKey, parentPath := newArtifactRecoveryTestParent(t, ModelStatusUpdating)
			if tc.ready {
				require.NoError(t, writeHuggingFaceArtifactReadyMarker(parentPath))
			}
			client := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
			if tc.invalid {
				cm, err := g.configMapReconciler.getConfigMap(context.Background())
				require.NoError(t, err)
				cm.Data[parentKey] = modelEntryJSON(ModelStatusUpdating)
				_, err = client.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			original := artifactRecoveryTestEntry(t, g, parentKey)
			failUpdate := true
			client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
				if failUpdate {
					return true, nil, errors.New("API temporarily unavailable")
				}
				return false, nil, nil
			})

			require.False(t, g.recoverStartupHuggingFaceArtifactParents(context.Background()))
			assert.True(t, g.isStartupHuggingFaceArtifactParentRecoveryPending())
			assert.Equal(t, original, artifactRecoveryTestEntry(t, g, parentKey))

			failUpdate = false
			stop, err := g.ensureStartupHuggingFaceArtifactParentsRecovered(context.Background(), nil, parentKey)
			require.NoError(t, err)
			assert.False(t, stop)
			assert.False(t, g.isStartupHuggingFaceArtifactParentRecoveryPending())
			if tc.invalid {
				exists, _, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), parentKey)
				require.NoError(t, err)
				assert.False(t, exists)
			} else {
				entry := artifactRecoveryTestEntry(t, g, parentKey)
				assert.Equal(t, tc.wantStatus, entry.Status)
				assert.Equal(t, original.Config.Artifact, entry.Config.Artifact)
			}
		})
	}
}

func TestArtifactStartupRecoveryWaitsForAPIAndTimesOut(t *testing.T) {
	g, _, parentKey, _ := newArtifactRecoveryTestParent(t, ModelStatusUpdating)
	g.gopherChan = make(chan *GopherTask, 1)
	g.samePathWaitDelay = time.Millisecond
	g.samePathWaitTimeout = time.Second
	client := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("API temporarily unavailable")
	})
	require.False(t, g.recoverStartupHuggingFaceArtifactParents(context.Background()))
	task := &GopherTask{TaskType: Download, BaseModel: &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "child"}}}
	stop, err := g.ensureStartupHuggingFaceArtifactParentsRecovered(context.Background(), task, parentKey)
	require.NoError(t, err)
	require.True(t, stop)
	require.False(t, task.SamePathWaitStartedAt.IsZero())
	select {
	case queued := <-g.gopherChan:
		assert.Same(t, task, queued)
	case <-time.After(time.Second):
		t.Fatal("startup recovery did not requeue the waiting model")
	}

	task.SamePathWaitStartedAt = time.Now().Add(-2 * time.Second)
	stop, err = g.ensureStartupHuggingFaceArtifactParentsRecovered(context.Background(), task, parentKey)
	require.ErrorContains(t, err, "timed out waiting for startup Hugging Face artifact parent recovery")
	assert.True(t, stop)
	assert.True(t, g.isStartupHuggingFaceArtifactParentRecoveryPending())
	assert.Empty(t, g.gopherChan)
	assert.False(t, g.requeueStartupHuggingFaceArtifactParentRecoveryWait(nil, parentKey))
	g.gopherChan = nil
	assert.False(t, g.requeueStartupHuggingFaceArtifactParentRecoveryWait(task, parentKey))
}

func TestArtifactStartupRecoveryLeavesUnrelatedAndMalformedEntriesAlone(t *testing.T) {
	g, _, parentKey, _ := newArtifactRecoveryTestParent(t, ModelStatusReady)
	cm, err := g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	cm.Data[parentKey+".malformed"] = "{invalid-json"
	cm.Data["basemodel.customer"] = modelEntryJSON(ModelStatusUpdating)
	g = newGopherWithConfigMap(cm)
	require.True(t, g.recoverStartupHuggingFaceArtifactParents(context.Background()))
	got, err := g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	assert.Equal(t, cm.Data, got.Data)
	assert.True(t, (&Gopher{logger: zap.NewNop().Sugar()}).recoverStartupHuggingFaceArtifactParents(context.Background()))
}

func TestArtifactStartupDefersLocalValidationOnlyOnce(t *testing.T) {
	modelPath := t.TempDir()
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "ready", Namespace: "customer"},
		Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
			StorageUri: stringPtr("oci://n/ns/b/bucket/o/model"), Path: &modelPath,
		}},
	}
	g := &Gopher{
		logger: zap.NewNop().Sugar(), taskQueue: newGopherTaskQueue(),
		startupReadyModelKeys: map[string]struct{}{getModelID(model, nil): {}},
	}
	task := &GopherTask{TaskType: Download, BaseModel: model}
	require.True(t, g.deferStartupReadyRevalidationIfLocalPathExists(task))
	queued, ok := g.taskQueue.popNormal()
	require.True(t, ok)
	assert.Same(t, task, queued)
	assert.True(t, queued.RevalidationReplay)
	assert.True(t, queued.NormalPriorityOnly)
	assert.False(t, g.deferStartupReadyRevalidationIfLocalPathExists(queued))
	assert.Zero(t, g.taskQueue.len())
}

func TestArtifactRepairStopsWhenReadyMarkerCannotBeRemoved(t *testing.T) {
	for _, operation := range []string{"repair", "rebuild"} {
		for _, failStatusUpdate := range []bool{false, true} {
			t.Run(operation+"/failed-status-update="+map[bool]string{false: "false", true: "true"}[failStatusUpdate], func(t *testing.T) {
				g, identity, parentKey, parentPath := newArtifactRecoveryTestParent(t, ModelStatusReady)
				marker := huggingFaceArtifactReadyMarkerPath(parentPath)
				require.NoError(t, os.MkdirAll(marker, 0755))
				require.NoError(t, os.WriteFile(filepath.Join(marker, "unexpected-child"), []byte("keep"), 0644))
				if failStatusUpdate {
					g.configMapReconciler.kubeClient.(*k8sfake.Clientset).PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
						cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
						var entry ModelEntry
						require.NoError(t, json.Unmarshal([]byte(cm.Data[parentKey]), &entry))
						if entry.Status == ModelStatusFailed {
							return true, nil, errors.New("cannot persist Failed state")
						}
						return false, nil, nil
					})
				}
				if operation == "repair" {
					artifact, repaired, err := g.repairHuggingFaceOriginArtifactParent(context.Background(), nil, "child", filepath.Join(filepath.Dir(parentPath), "child"), parentKey, parentPath, identity, func(string) error {
						t.Fatal("must not download while the previous ready marker remains")
						return nil
					})
					require.ErrorContains(t, err, "before repair")
					assert.Nil(t, artifact)
					assert.False(t, repaired)
				} else {
					path, acquired, err := g.tryAcquireHuggingFaceArtifactParentRebuild(context.Background(), parentKey, parentPath, identity)
					require.ErrorContains(t, err, "before rebuild")
					assert.Empty(t, path)
					assert.False(t, acquired)
				}
				entry := artifactRecoveryTestEntry(t, g, parentKey)
				if failStatusUpdate {
					assert.Equal(t, ModelStatusUpdating, entry.Status)
				} else {
					assert.Equal(t, ModelStatusFailed, entry.Status)
				}
				assert.Equal(t, []string{"existing-child"}, entry.Config.Artifact.ChildrenPaths)
				assert.FileExists(t, filepath.Join(marker, "unexpected-child"))
			})
		}
	}
}

func TestArtifactRepairRecoversAfterReadyUpdateFails(t *testing.T) {
	g, identity, parentKey, parentPath := newArtifactRecoveryTestParent(t, ModelStatusReady)
	childPath := filepath.Join(filepath.Dir(parentPath), "child")
	updateErr := errors.New("cannot persist Ready state")
	failReady := true
	g.configMapReconciler.kubeClient.(*k8sfake.Clientset).PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		var entry ModelEntry
		require.NoError(t, json.Unmarshal([]byte(cm.Data[parentKey]), &entry))
		if failReady && entry.Status == ModelStatusReady {
			return true, nil, updateErr
		}
		return false, nil, nil
	})
	artifact, repaired, err := g.repairHuggingFaceOriginArtifactParent(context.Background(), nil, "child", childPath, parentKey, parentPath, identity, func(path string) error {
		require.Equal(t, parentPath, path)
		writeMinimalModelConfig(t, path)
		return nil
	})
	require.ErrorIs(t, err, updateErr)
	assert.Nil(t, artifact)
	assert.False(t, repaired)
	assert.True(t, hasHuggingFaceArtifactReadyMarker(parentPath))
	assert.Equal(t, ModelStatusUpdating, artifactRecoveryTestEntry(t, g, parentKey).Status)
	_, err = os.Lstat(childPath)
	require.True(t, os.IsNotExist(err))

	// A marker proves the download completed, but an unavailable API still
	// prevents the waiting task from treating this parent as Ready.
	wait, err := g.requeueIfHuggingFaceArtifactParentUpdating(context.Background(), nil, identity)
	require.ErrorContains(t, err, "timed out waiting")
	assert.True(t, wait)
	failReady = false
	wait, err = g.requeueIfHuggingFaceArtifactParentUpdating(context.Background(), nil, identity)
	require.NoError(t, err)
	assert.False(t, wait)
	assert.Equal(t, ModelStatusReady, artifactRecoveryTestEntry(t, g, parentKey).Status)
	artifact, err = g.linkHuggingFaceOriginArtifact(context.Background(), nil, "child", childPath, parentKey, parentPath, identity)
	require.NoError(t, err)
	assert.Equal(t, parentPath, artifact.ParentPath[parentKey])
	assert.ElementsMatch(t, []string{"existing-child", childPath}, artifactRecoveryTestEntry(t, g, parentKey).Config.Artifact.ChildrenPaths)
}

func TestArtifactStartupValidationPreservesDownloadFailure(t *testing.T) {
	for _, failStatusUpdate := range []bool{false, true} {
		t.Run(map[bool]string{false: "marks Failed", true: "Failed update unavailable"}[failStatusUpdate], func(t *testing.T) {
			g, identity, parentKey, parentPath := newArtifactRecoveryTestParent(t, ModelStatusReady)
			require.NoError(t, writeHuggingFaceArtifactReadyMarker(parentPath))
			g.startupHuggingFaceParentValidationKeys = map[string]struct{}{parentKey: {}}
			downloadErr := errors.New("checksum mismatch")
			stop, err := g.validateStartupHuggingFaceArtifactParentIfNeeded(context.Background(), nil, parentKey, identity, func(path string) error {
				assert.Equal(t, parentPath, path)
				assert.False(t, hasHuggingFaceArtifactReadyMarker(path))
				if failStatusUpdate {
					g.configMapReconciler.kubeClient.(*k8sfake.Clientset).PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
						return true, nil, errors.New("API temporarily unavailable")
					})
				}
				return downloadErr
			})
			require.ErrorIs(t, err, downloadErr)
			assert.False(t, stop)
			assert.False(t, hasHuggingFaceArtifactReadyMarker(parentPath))
			entry := artifactRecoveryTestEntry(t, g, parentKey)
			if failStatusUpdate {
				assert.Equal(t, ModelStatusUpdating, entry.Status)
			} else {
				assert.Equal(t, ModelStatusFailed, entry.Status)
			}
			assert.Equal(t, []string{"existing-child"}, entry.Config.Artifact.ChildrenPaths)
		})
	}
}

func TestArtifactLinkFailureDoesNotRecordChild(t *testing.T) {
	for _, operation := range []string{"reuse", "repair"} {
		t.Run(operation, func(t *testing.T) {
			g, identity, parentKey, parentPath := newArtifactRecoveryTestParent(t, ModelStatusReady)
			require.NoError(t, writeHuggingFaceArtifactReadyMarker(parentPath))
			blockedDir := filepath.Join(filepath.Dir(parentPath), "blocked")
			require.NoError(t, os.WriteFile(blockedDir, []byte("existing file"), 0644))
			childPath := filepath.Join(blockedDir, "child")
			task := &GopherTask{TaskType: Download, BaseModel: &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "child"}}}
			var artifact *Artifact
			var linked bool
			var err error
			if operation == "reuse" {
				artifact, linked, err = g.reuseHuggingFaceOriginArtifactIfPossible(context.Background(), task, v1beta1.BaseModelSpec{
					Storage: &v1beta1.StorageSpec{DownloadPolicy: dp(v1beta1.ReuseIfExists)},
				}, "BaseModel", "default", "child", childPath, identity)
			} else {
				artifact, linked, err = g.repairHuggingFaceOriginArtifactParent(context.Background(), task, "child", childPath, parentKey, parentPath, identity, func(string) error { return nil })
			}
			require.ErrorContains(t, err, "failed to create child directory")
			assert.Nil(t, artifact)
			assert.False(t, linked)
			entry := artifactRecoveryTestEntry(t, g, parentKey)
			assert.Equal(t, ModelStatusReady, entry.Status)
			assert.Equal(t, []string{"existing-child"}, entry.Config.Artifact.ChildrenPaths)
			assert.True(t, hasHuggingFaceArtifactReadyMarker(parentPath))
		})
	}
}
