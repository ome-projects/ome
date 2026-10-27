package modelagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/ome/pkg/constants"
)

// Preserve disk state but lose both the ConfigMap and the previous process's
// caches, as happens when a patched host rejoins with a new agent.
func newTestColdHfArtifactGopher(t *testing.T) (*Gopher, *GopherTask, hfArtifactTaskInput, hfArtifactTaskInput) {
	t.Helper()
	old, task, first := newTestHfArtifactGopher(t)
	handler := old.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, first))
	second := testHfArtifactTaskInput(t, first.ModelStoreRoot, "model-2")
	seedTestChildModelEntry(t, handler.repository, second)
	require.NoError(t, runTestHfArtifactDownload(handler, second))
	client := old.configMapReconciler.kubeClient.(*fake.Clientset)
	require.NoError(t, client.CoreV1().ConfigMaps("ome").Delete(context.Background(), "node-1", metav1.DeleteOptions{}))
	fresh := &Gopher{
		configMapReconciler: NewConfigMapReconciler("node-1", "ome", client, old.logger),
		modelRootDir:        first.ModelStoreRoot,
		logger:              old.logger,
	}
	handler = fresh.sharedHfArtifactHandler()
	require.NoError(t, fresh.hfArtifactStartup.recover(context.Background()))
	require.Empty(t, fresh.hfArtifactStartup.pending)
	seedTestChildModelEntry(t, handler.repository, first)
	seedTestChildModelEntry(t, handler.repository, second)
	return fresh, task, first, second
}

func TestGopherHfArtifactColdRecoveryRestoresReferences(t *testing.T) {
	for _, marker := range []bool{false, true} {
		t.Run(map[bool]string{false: "without marker", true: "with marker"}[marker], func(t *testing.T) {
			s, task, first, second := newTestColdHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			if !marker {
				require.NoError(t, os.Remove(filepath.Join(first.Parent.LocalPath, constants.HfArtifactReadyMarkerFileName)))
			}
			file := filepath.Join(first.Parent.LocalPath, "config.json")
			before, err := os.Stat(file)
			require.NoError(t, err)
			validations := 0
			validate := func(path string) (bool, error) {
				validations++
				require.Equal(t, first.Parent.LocalPath, path)
				data, err := os.ReadFile(filepath.Join(path, "config.json"))
				return string(data) == "{}", err
			}
			download := func(string) error { t.Fatal("retained valid files must not be downloaded"); return nil }
			result, err := s.runHfArtifactDownload(context.Background(), task, first, true, validate, download)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome)
			require.Equal(t, 1, validations)
			parent, found, err := h.repository.GetParentForChild(context.Background(), first.ChildModelKey)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, HfArtifactStatusReady, parent.Status)
			// Other children recover from their own current CR tasks, not by
			// guessing CR identities from retained symlink names.
			siblingTask := *task
			siblingTask.BaseModel = task.BaseModel.DeepCopy()
			siblingTask.BaseModel.Name = "model-2"
			siblingTask.BaseModel.UID = second.ChildModelUID
			siblingTask.BaseModel.Spec.Storage.Path = &second.ChildModelPath
			result, err = s.runHfArtifactDownload(context.Background(), &siblingTask, second, true, validate, download)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome)
			assertParentEntryReady(t, h.repository, first.Parent, map[string]string{
				first.ChildModelKey: first.ChildModelPath, second.ChildModelKey: second.ChildModelPath,
			})
			after, err := os.Stat(file)
			require.NoError(t, err)
			require.True(t, os.SameFile(before, after))
			require.Equal(t, before.ModTime(), after.ModTime())
			assertChildSymlinkTarget(t, first.ChildModelPath, first.Parent.LocalPath)
			assertChildSymlinkTarget(t, second.ChildModelPath, first.Parent.LocalPath)
		})
	}
}

func TestGopherHfArtifactColdRecoveryResetsOnlyUnreferencedInvalidFiles(t *testing.T) {
	for _, tc := range []struct {
		name  string
		valid bool
		err   error
	}{
		{name: "invalid files"},
		{name: "valid files", valid: true},
		{name: "inspection error", err: errors.New("manifest unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, marker := range []bool{false, true} {
				t.Run(map[bool]string{false: "without marker", true: "with marker"}[marker], func(t *testing.T) {
					s, task, input := newTestHfArtifactGopher(t)
					require.NoError(t, writeTestHfArtifactFiles(input.Parent.LocalPath))
					stale := filepath.Join(input.Parent.LocalPath, "old-shard.safetensors")
					require.NoError(t, os.WriteFile(stale, []byte("partial download"), 0o644))
					before, err := os.Stat(stale)
					require.NoError(t, err)
					markerPath := filepath.Join(input.Parent.LocalPath, constants.HfArtifactReadyMarkerFileName)
					if marker {
						require.NoError(t, os.WriteFile(markerPath, []byte("previous-owner"), 0o644))
					}
					downloads := 0
					result, err := s.runHfArtifactDownload(context.Background(), task, input, true,
						func(path string) (bool, error) {
							require.FileExists(t, stale, "validation must precede cleanup")
							return tc.valid, tc.err
						}, func(path string) error {
							downloads++
							entries, err := os.ReadDir(path)
							require.NoError(t, err)
							require.Empty(t, entries, "unreferenced invalid files must be cleared before downloading")
							return writeTestHfArtifactFiles(path)
						})
					if tc.err != nil {
						require.ErrorIs(t, err, tc.err)
					} else {
						require.NoError(t, err)
						require.Equal(t, hfArtifactTaskDone, result.Outcome)
						assertParentEntryReady(t, s.sharedHfArtifactHandler().repository, input.Parent,
							map[string]string{input.ChildModelKey: input.ChildModelPath})
					}
					if !tc.valid && tc.err == nil {
						require.Equal(t, 1, downloads)
						require.NoFileExists(t, stale)
						return
					}
					// Valid files and inspection failures must both retain the original bytes.
					require.Zero(t, downloads)
					after, err := os.Stat(stale)
					require.NoError(t, err)
					require.True(t, os.SameFile(before, after))
					require.Equal(t, before.ModTime(), after.ModTime())
					if tc.err != nil && marker {
						bytes, err := os.ReadFile(markerPath)
						require.NoError(t, err)
						require.Equal(t, "previous-owner", string(bytes))
					}
				})
			}
		})
	}
}

func TestGopherHfArtifactColdRecoveryPreservesUnverifiedFiles(t *testing.T) {
	for _, validationErr := range []error{nil, errors.New("manifest unavailable")} {
		t.Run(map[bool]string{false: "invalid files", true: "inspection error"}[validationErr != nil], func(t *testing.T) {
			s, task, first, second := newTestColdHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			marker, err := os.ReadFile(filepath.Join(first.Parent.LocalPath, constants.HfArtifactReadyMarkerFileName))
			require.NoError(t, err)
			validations := 0
			validate := func(string) (bool, error) { validations++; return false, validationErr }
			download := func(string) error { t.Fatal("unrecorded users must block file replacement"); return nil }
			for attempt := 0; attempt < 2; attempt++ {
				_, err := s.runHfArtifactDownload(context.Background(), task, first, true, validate, download)
				require.Error(t, err)
				if validationErr != nil {
					require.ErrorIs(t, err, validationErr)
				} else {
					require.ErrorContains(t, err, "unrecorded child")
				}
			}
			require.Equal(t, 2, validations)
			after, err := os.ReadFile(filepath.Join(first.Parent.LocalPath, constants.HfArtifactReadyMarkerFileName))
			require.NoError(t, err)
			require.Equal(t, marker, after)
			require.FileExists(t, filepath.Join(first.Parent.LocalPath, "config.json"))
			assertChildSymlinkTarget(t, first.ChildModelPath, first.Parent.LocalPath)
			assertChildSymlinkTarget(t, second.ChildModelPath, first.Parent.LocalPath)
			parent, found, err := h.repository.Get(context.Background(), first.Parent.Identity)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, HfArtifactStatusFailed, parent.Status)
			require.Empty(t, parent.LockID)
			// A transient inspection error or repaired disk must not permanently
			// strand the child after the first failed reconstruction attempt.
			result, err := s.runHfArtifactDownload(context.Background(), task, first, true,
				func(string) (bool, error) { return true, nil }, download)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome)
			_, found, err = h.repository.GetParentForChild(context.Background(), first.ChildModelKey)
			require.NoError(t, err)
			require.True(t, found)
		})
	}
}

func TestGopherHfArtifactColdRecoveryWaitsForNormalWorkerAndLock(t *testing.T) {
	s, task, input, _ := newTestColdHfArtifactGopher(t)
	noValidate := func(string) (bool, error) {
		t.Fatal("validation must wait for a normal worker and ownership")
		return false, nil
	}
	noDownload := func(string) error { t.Fatal("a blocked task must not download"); return nil }
	result, err := s.runHfArtifactDownload(context.Background(), task, input, false, noValidate, noDownload)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	require.True(t, task.NormalPriorityOnly)
	lock, acquired, err := tryHfArtifactParentFileLock(input.Parent, input.ModelStoreRoot)
	require.NoError(t, err)
	require.True(t, acquired)
	t.Cleanup(func() { require.NoError(t, lock.Close()) })
	result, err = s.runHfArtifactDownload(context.Background(), task, input, true, noValidate, noDownload)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskRetry, result.Outcome)
	_, found, err := s.sharedHfArtifactHandler().repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	require.False(t, found, "a contending task must not publish parent ownership")
}

func TestHfArtifactRepairPreservesUnrecordedSibling(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	orphan := filepath.Join(first.ModelStoreRoot, "unrecorded-sibling")
	require.NoError(t, os.Symlink(first.Parent.LocalPath, orphan))
	file := filepath.Join(first.Parent.LocalPath, "config.json")
	require.NoError(t, os.WriteFile(file, []byte("corrupt"), 0o644))
	markerPath := filepath.Join(first.Parent.LocalPath, constants.HfArtifactReadyMarkerFileName)
	marker, err := os.ReadFile(markerPath)
	require.NoError(t, err)
	ready := map[string]ModelStatus{first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusReady}
	failed := map[string]ModelStatus{first.ChildModelKey: ModelStatusFailed, second.ChildModelKey: ModelStatusFailed}
	var published []map[string]ModelStatus
	h.updateChildStatuses = func(_ context.Context, statuses map[string]ModelStatus) error {
		published = append(published, statuses)
		return nil
	}
	noDownload := func(string) error { t.Fatal("a partially recovered parent still has unknown users"); return nil }
	for attempt := 0; attempt < 2; attempt++ {
		_, err = h.handleDownloadOverride(context.Background(), first,
			func(string) (bool, error) { return false, nil }, noDownload)
		require.ErrorContains(t, err, "unrecorded child")
		assertTestRepairChildStatuses(t, h, failed)
		require.Len(t, published, attempt+1)
		require.Equal(t, failed, published[attempt])
		parent := testRepairParent(t, h, first)
		require.Equal(t, HfArtifactStatusFailed, parent.Status)
		require.Empty(t, parent.LockID)
		require.Equal(t, ready, parent.ChildStatusesBeforeRepair)
		bytes, err := os.ReadFile(file)
		require.NoError(t, err)
		require.Equal(t, "corrupt", string(bytes))
		afterMarker, err := os.ReadFile(markerPath)
		require.NoError(t, err)
		require.Equal(t, marker, afterMarker)
		assertChildSymlinkTarget(t, orphan, first.Parent.LocalPath)
	}
	// After an external file repair, successful validation must restore the
	// saved child statuses even though the unrecorded sibling still exists.
	require.NoError(t, os.WriteFile(file, []byte("{}"), 0o644))
	result, err := h.handleDownloadOverride(context.Background(), first,
		func(string) (bool, error) { return true, nil }, noDownload)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	assertTestRepairChildStatuses(t, h, ready)
	require.Equal(t, ready, published[len(published)-1])
	require.Empty(t, testRepairParent(t, h, first).ChildStatusesBeforeRepair)
	assertChildSymlinkTarget(t, orphan, first.Parent.LocalPath)
}

func TestGopherHfArtifactColdRecoveryPreservesDanglingChildren(t *testing.T) {
	s, task, first, second := newTestColdHfArtifactGopher(t)
	require.NoError(t, os.RemoveAll(first.Parent.LocalPath))
	validated := false
	_, err := s.runHfArtifactDownload(context.Background(), task, first, true,
		func(string) (bool, error) { validated = true; return false, nil },
		func(string) error { t.Fatal("dangling unrecorded children must prevent replacement"); return nil })
	require.True(t, validated)
	require.ErrorContains(t, err, "unrecorded child")
	assertChildSymlinkTarget(t, first.ChildModelPath, first.Parent.LocalPath)
	assertChildSymlinkTarget(t, second.ChildModelPath, first.Parent.LocalPath)
	require.NoDirExists(t, first.Parent.LocalPath)
}

func TestGopherHfArtifactColdRecoveryDoesNotRestoreDeletedChild(t *testing.T) {
	s, task, input, _ := newTestColdHfArtifactGopher(t)
	result, err := s.runHfArtifactDownload(context.Background(), task, input, true,
		func(string) (bool, error) {
			s.configMapReconciler.invalidateModelUIDAndEvictCache(input.ChildModelKey, input.ChildModelUID)
			return true, nil
		},
		func(string) error { t.Fatal("a deleted child must not write model files"); return nil })
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	_, found, err := s.sharedHfArtifactHandler().repository.GetParentForChild(context.Background(), input.ChildModelKey)
	require.NoError(t, err)
	require.False(t, found)
	parent, found, err := s.sharedHfArtifactHandler().repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	require.True(t, found)
	require.Empty(t, parent.Children)
	require.Empty(t, parent.LockID)
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
}
