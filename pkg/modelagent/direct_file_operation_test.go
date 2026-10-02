package modelagent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/modelparser"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	"sigs.k8s.io/ome/pkg/xet"
)

func newDirectOperationTestGopher(t *testing.T, uri string) (*Gopher, *GopherTask) {
	t.Helper()
	g, task := newCancellationCoverageGopher(t, uri)
	g.modelRootDir = t.TempDir()
	task.BaseModel.Spec.Storage.Path = stringPtr(filepath.Join(g.modelRootDir, "model"))
	g.gopherChan = make(chan *GopherTask, 10)
	g.samePathWaitDelay = time.Millisecond
	require.NoError(t, os.MkdirAll(*task.BaseModel.Spec.Storage.Path, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(*task.BaseModel.Spec.Storage.Path, "weights"), []byte("model"), 0o600))
	require.NoError(t, g.configMapReconciler.ReconcileModelMetadata(context.Background(), &ConfigMapMetadataOp{
		BaseModel: task.BaseModel, ModelMetadata: ModelMetadata{Artifact: Artifact{ParentPath: map[string]string{
			getModelID(task.BaseModel, nil): *task.BaseModel.Spec.Storage.Path,
		}}},
	}))
	return g, task
}

func TestIsDirectFileTaskUsesArtifactRouting(t *testing.T) {
	for _, tc := range []struct {
		name   string
		uri    string
		shared bool
		want   bool
	}{
		{name: "direct HF", uri: "hf://org/model", want: true},
		{name: "direct OCI", uri: "oci://n/ns/b/bucket/o/model", want: true},
		{name: "direct ModelPack", uri: "modelpack://registry.example.com/org/model:tag", want: true},
		{name: "shared HF", uri: "hf://org/model", shared: true},
		{name: "shared OCI", uri: "oci://n/ns/b/bucket/o/model", shared: true},
		{name: "local", uri: "local:///model"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := &GopherTask{
				BaseModel:      &v1beta1.BaseModel{Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr(tc.uri)}}},
				SharedArtifact: tc.shared,
			}
			require.Equal(t, tc.want, isDirectFileTask(task))
		})
	}
}

func TestEnqueueDeleteWithoutModelDoesNotPanic(t *testing.T) {
	g := &Gopher{}
	task := &GopherTask{TaskType: Delete}

	require.NotPanics(t, func() { g.enqueueTask(task) })
	require.Equal(t, 1, g.taskQueue.len())
	queued, ok := g.taskQueue.popHighPriority()
	require.True(t, ok)
	require.Same(t, task, queued)
	require.EqualError(t, g.processTask(queued), "gopher got empty task")
}

// Exercise the real dispatcher and HF adapter with only external byte transfer
// replaced. OCI's callback deliberately has the same uncancellable lifetime as
// BulkDownload; the dispatcher, not the test writer, owns the operation lock.
func runDirectTestDownload(g *Gopher, task *GopherTask, write func(context.Context, string) error) error {
	source := directHfSource{
		resolve: func(context.Context, string, string, string, string) (string, error) {
			return strings.Repeat("a", 40), nil
		},
		download: func(ctx context.Context, _ *GopherTask, config *xet.DownloadConfig) error {
			return write(ctx, config.LocalDir)
		},
	}
	return g.processTaskWithSourceAdapters(task, true,
		func(ctx context.Context, task *GopherTask, spec v1beta1.BaseModelSpec, allow bool) (bool, error) {
			return source.process(ctx, g, task, spec, allow)
		},
		func(ctx context.Context, _ *ociobjectstore.ObjectURI, path string, _ *GopherTask) error {
			return write(ctx, path)
		},
	)
}

func TestDirectDeleteWaitsForCanceledWriterReturn(t *testing.T) {
	for _, uri := range []string{"hf://org/model", "oci://n/ns/b/bucket/o/model"} {
		t.Run(uri, func(t *testing.T) {
			g, task := newDirectOperationTestGopher(t, uri)
			g.modelConfigParser = modelparser.NewModelConfigParser(nil, g.logger)
			started := make(chan context.Context, 1)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			done := make(chan error, 1)
			go func() {
				done <- runDirectTestDownload(g, task, func(ctx context.Context, path string) error {
					started <- ctx
					<-release // Ignore cancellation until the real writer returns.
					return os.WriteFile(filepath.Join(path, "late-weights"), []byte("late"), 0o600)
				})
			}()
			writerCtx := <-started
			task.BaseModel.DeletionTimestamp = &metav1.Time{Time: time.Now()}
			deletion := &GopherTask{TaskType: Delete, BaseModel: task.BaseModel}
			require.NoError(t, g.processTask(deletion))
			require.ErrorIs(t, writerCtx.Err(), context.Canceled)
			require.Same(t, deletion, receiveDirectRetry(t, g))
			require.FileExists(t, filepath.Join(*task.BaseModel.Spec.Storage.Path, "weights"))
			lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: g.modelRootDir, ChildModelPath: *task.BaseModel.Spec.Storage.Path})
			require.NoError(t, err)
			if acquired {
				_ = lock.Close()
			}
			require.False(t, acquired, "cancellation must not release the writer's file lock")
			unblock()
			require.ErrorIs(t, <-done, context.Canceled)
			require.FileExists(t, filepath.Join(*task.BaseModel.Spec.Storage.Path, "late-weights"))
			require.NoError(t, g.processTask(deletion))
			require.NoDirExists(t, *task.BaseModel.Spec.Storage.Path)
			require.NoError(t, runDirectTestDownload(g, task, func(context.Context, string) error {
				t.Error("a delayed retry restored a completed deletion")
				return nil
			}))
			exists, _, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), getModelID(task.BaseModel, nil))
			require.NoError(t, err)
			require.False(t, exists, "canceled writer must not restore Ready or metadata after cleanup")
		})
	}
}

func TestDirectFileLockHeldThroughReady(t *testing.T) {
	for _, uri := range []string{"hf://org/model", "oci://n/ns/b/bucket/o/model"} {
		t.Run(uri, func(t *testing.T) {
			g, task := newDirectOperationTestGopher(t, uri)
			g.modelConfigParser = modelparser.NewModelConfigParser(nil, g.logger)
			ready := make(chan struct{})
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			g.configMapReconciler.kubeClient.(*k8sfake.Clientset).PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				var entry ModelEntry
				_ = json.Unmarshal([]byte(cm.Data[getModelID(task.BaseModel, nil)]), &entry)
				if entry.Status == ModelStatusReady {
					close(ready)
					<-release
				}
				return false, nil, nil
			})
			done := make(chan error, 1)
			go func() { done <- runDirectTestDownload(g, task, func(context.Context, string) error { return nil }) }()
			select {
			case <-ready:
			case err := <-done:
				t.Fatalf("writer returned before Ready: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("writer did not reach Ready")
			}
			// An independent agent has no in-process tracker state to help it.
			deleter, deletion := newDirectOperationTestGopher(t, uri)
			deleter.modelRootDir = g.modelRootDir
			deletion.TaskType, deletion.BaseModel = Delete, task.BaseModel
			deleter.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel}}
			require.NoError(t, deleter.processTask(deletion))
			require.Same(t, deletion, receiveDirectRetry(t, deleter))
			require.FileExists(t, filepath.Join(*task.BaseModel.Spec.Storage.Path, "weights"))
			unblock()
			require.NoError(t, <-done)
			assertCancellationCoverageStatus(t, g, task, ModelStatusReady)
		})
	}
}

func receiveDirectRetry(t *testing.T, g *Gopher) *GopherTask {
	t.Helper()
	select {
	case task := <-g.gopherChan:
		return task
	case <-time.After(5 * time.Second):
		t.Fatal("busy file operation was not requeued")
		return nil
	}
}

func TestDirectDeleteRetriesCompatibleFileLock(t *testing.T) {
	for _, uri := range []string{"oci://n/ns/b/bucket/o/model", "modelpack://ghcr.io/org/model:tag"} {
		t.Run(uri, func(t *testing.T) {
			g, task := newDirectOperationTestGopher(t, uri)
			task.TaskType = Delete
			lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{
				ModelStoreRoot: g.modelRootDir, ChildModelPath: *task.BaseModel.Spec.Storage.Path,
			})
			require.NoError(t, err)
			require.True(t, acquired)
			defer lock.Close()
			require.NoError(t, g.processTask(task))
			require.FileExists(t, filepath.Join(*task.BaseModel.Spec.Storage.Path, "weights"), "ordinary Delete must not remove files owned by another operation")
			require.Same(t, task, receiveDirectRetry(t, g))
			require.NoError(t, lock.Close())
			require.NoError(t, g.processTask(task))
			require.NoDirExists(t, *task.BaseModel.Spec.Storage.Path)
		})
	}
}

func TestSharedRoutedDirectFallbackDeleteKeepsFileLock(t *testing.T) {
	for _, uri := range []string{"hf://org/model", "oci://n/ns/b/bucket/o/model", "modelpack://ghcr.io/org/model:tag"} {
		t.Run(uri, func(t *testing.T) {
			g, task := newDirectOperationTestGopher(t, uri)
			task.TaskType = Delete
			task.SharedArtifact = true
			task.BaseModel.DeletionTimestamp = &metav1.Time{Time: time.Now()}
			lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{
				ModelStoreRoot: g.modelRootDir, ChildModelPath: *task.BaseModel.Spec.Storage.Path,
			})
			require.NoError(t, err)
			require.True(t, acquired)
			defer lock.Close()
			require.NoError(t, g.processTask(task))
			require.Same(t, task, receiveDirectRetry(t, g))
			require.FileExists(t, filepath.Join(*task.BaseModel.Spec.Storage.Path, "weights"))
			require.NoError(t, lock.Close())
			require.NoError(t, g.processTask(task))
			require.NoDirExists(t, *task.BaseModel.Spec.Storage.Path)
		})
	}
}

func TestDirectDownloadRetriesCompatibleFileLock(t *testing.T) {
	for _, uri := range []string{"hf://org/model", "oci://n/ns/b/bucket/o/model"} {
		t.Run(uri, func(t *testing.T) {
			g, task := newDirectOperationTestGopher(t, uri)
			g.modelConfigParser = modelparser.NewModelConfigParser(nil, g.logger)
			lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: g.modelRootDir, ChildModelPath: *task.BaseModel.Spec.Storage.Path})
			require.NoError(t, err)
			require.True(t, acquired)
			defer lock.Close()
			writes := 0
			write := func(context.Context, string) error { writes++; return nil }
			require.NoError(t, runDirectTestDownload(g, task, write))
			require.Same(t, task, receiveDirectRetry(t, g))
			require.Zero(t, writes)
			require.NoError(t, lock.Close())
			require.NoError(t, runDirectTestDownload(g, task, write))
			require.Equal(t, 1, writes)
			assertCancellationCoverageStatus(t, g, task, ModelStatusReady)
		})
	}
}

func TestDirectDeleteRetryRetainsNewerDownload(t *testing.T) {
	for _, uri := range []string{"hf://org/model", "oci://n/ns/b/bucket/o/model"} {
		t.Run(uri, func(t *testing.T) {
			g, oldDownload := newDirectOperationTestGopher(t, uri)
			_, finish, proceed, err := g.beginTask(oldDownload)
			require.NoError(t, err)
			require.True(t, proceed)
			defer finish(false)
			deletion := &GopherTask{TaskType: Delete, BaseModel: oldDownload.BaseModel}
			g.enqueueTask(deletion)
			queuedDeletion, ok := g.taskQueue.popHighPriority()
			require.True(t, ok)
			require.Same(t, deletion, queuedDeletion)
			require.NoError(t, g.processTask(queuedDeletion))

			// Affinity can return while the canceled writer is still finishing.
			newerDownload := &GopherTask{TaskType: Download, BaseModel: oldDownload.BaseModel}
			g.enqueueTask(newerDownload)
			require.Greater(t, newerDownload.Sequence, deletion.Sequence)
			require.Equal(t, 1, g.taskQueue.len())
			g.enqueueTask(receiveDirectRetry(t, g))
			require.Equal(t, 2, g.taskQueue.len(), "older Delete retry discarded a newer Download")

			finish(false)
			queuedDeletion, ok = g.taskQueue.popHighPriority()
			require.True(t, ok)
			require.Same(t, deletion, queuedDeletion)
			require.NoError(t, g.processTask(queuedDeletion))
			var queuedDownload *GopherTask
			if isObjectStorageDownloadTask(newerDownload) {
				queuedDownload, ok = g.taskQueue.popHighPriority()
			} else {
				queuedDownload, ok = g.taskQueue.popNormal()
			}
			require.True(t, ok)
			require.Same(t, newerDownload, queuedDownload)
			_, finishNew, proceed, err := g.beginTask(queuedDownload)
			defer finishNew(false)
			require.NoError(t, err)
			require.True(t, proceed, "newer download must remain eligible after the older delete completes")
		})
	}
}

func TestDirectDeleteLocksSecondaryParent(t *testing.T) {
	g, task := newDirectOperationTestGopher(t, "hf://org/model")
	task.TaskType = Delete
	task.BaseModel.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	parentPath := filepath.Join(g.modelRootDir, "parent")
	require.NoError(t, os.Mkdir(parentPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(parentPath, "weights"), []byte("parent"), 0o600))
	require.NoError(t, os.RemoveAll(*task.BaseModel.Spec.Storage.Path))
	require.NoError(t, os.Symlink(parentPath, *task.BaseModel.Spec.Storage.Path))
	require.NoError(t, g.configMapReconciler.ReconcileModelMetadata(context.Background(), &ConfigMapMetadataOp{
		BaseModel: task.BaseModel, ModelMetadata: ModelMetadata{Artifact: Artifact{ParentPath: map[string]string{"default.basemodel.parent": parentPath}}},
	}))
	lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: g.modelRootDir, ChildModelPath: parentPath})
	require.NoError(t, err)
	require.True(t, acquired)
	defer lock.Close()
	require.NoError(t, g.processTask(task))
	require.Same(t, task, receiveDirectRetry(t, g))
	require.FileExists(t, filepath.Join(parentPath, "weights"))
	require.NoError(t, lock.Close())
	require.NoError(t, g.processTask(task))
	require.NoDirExists(t, parentPath)
}

func TestDirectDeleteRejectsStaleUID(t *testing.T) {
	g, task := newDirectOperationTestGopher(t, "oci://n/ns/b/bucket/o/model")
	task.TaskType = Delete
	replacement := task.BaseModel.DeepCopy()
	replacement.UID = "replacement"
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{replacement}}
	require.NoError(t, g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: replacement, ModelStateOnNode: Ready}))
	require.NoError(t, g.processTask(task))
	require.FileExists(t, filepath.Join(*task.BaseModel.Spec.Storage.Path, "weights"))
	assertCancellationCoverageStatus(t, g, &GopherTask{BaseModel: replacement}, ModelStatusReady)
}

func TestDirectDownloadRejectsStaleUID(t *testing.T) {
	g, task := newDirectOperationTestGopher(t, "oci://n/ns/b/bucket/o/model")
	replacement := task.BaseModel.DeepCopy()
	replacement.UID = "replacement"
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{replacement}}
	require.NoError(t, runDirectTestDownload(g, task, func(context.Context, string) error {
		t.Error("stale UID started a writer")
		return nil
	}))
}

func TestDirectFileLockNormalizesPathAliases(t *testing.T) {
	g, task := newDirectOperationTestGopher(t, "oci://n/ns/b/bucket/o/model")
	alias := filepath.Join(t.TempDir(), "alias")
	require.NoError(t, os.Symlink(g.modelRootDir, alias))
	cwd, err := os.Getwd()
	require.NoError(t, err)
	relative, err := filepath.Rel(cwd, *task.BaseModel.Spec.Storage.Path)
	require.NoError(t, err)
	for _, path := range []string{filepath.Join(alias, "model"), relative} {
		ctx, release := directFileOperationContext(context.Background())
		acquired, err := g.tryLockDirectModelPath(ctx, path)
		require.NoError(t, err)
		require.True(t, acquired)
		lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: g.modelRootDir, ChildModelPath: *task.BaseModel.Spec.Storage.Path})
		if acquired {
			_ = lock.Close()
		}
		release()
		require.NoError(t, err)
		require.False(t, acquired, "aliases must use the same lock inode")
	}
}

func TestDirectDeletePreservesSharedSibling(t *testing.T) {
	shared, sibling, input := newTestHfArtifactGopher(t)
	require.NoError(t, runTestHfArtifactDownload(shared.sharedHfArtifactHandler(), input))
	g, task := newDirectOperationTestGopher(t, "oci://n/ns/b/bucket/o/model")
	task.TaskType = Delete
	task.BaseModel.Name, task.BaseModel.UID = "ordinary", "ordinary-uid"
	task.BaseModel.Spec.Storage.Path = &input.ChildModelPath
	g.modelRootDir = input.ModelStoreRoot
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel, sibling.BaseModel}}
	require.NoError(t, g.processTask(task))
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
	require.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"))
}

func TestDirectDeleteRetainsFilesAndRemovesBookkeeping(t *testing.T) {
	for _, retention := range []string{"reserve", "sibling", "local", "pvc"} {
		t.Run(retention, func(t *testing.T) {
			g, task := newDirectOperationTestGopher(t, "oci://n/ns/b/bucket/o/model")
			task.TaskType = Delete
			switch retention {
			case "reserve":
				task.BaseModel.Labels = map[string]string{constants.ReserveModelArtifact: "true"}
			case "sibling":
				sibling := task.BaseModel.DeepCopy()
				sibling.Name, sibling.UID = "sibling", "sibling-uid"
				g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel, sibling}}
			case "local":
				task.BaseModel.Spec.Storage.StorageUri = stringPtr("local:///model")
			case "pvc":
				task.BaseModel.Spec.Storage.StorageUri = stringPtr("pvc://model")
			}
			require.NoError(t, g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
			require.NoError(t, g.processTask(task))
			require.FileExists(t, filepath.Join(*task.BaseModel.Spec.Storage.Path, "weights"))
			exists, _, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), getModelID(task.BaseModel, nil))
			require.NoError(t, err)
			require.False(t, exists)
		})
	}
}
