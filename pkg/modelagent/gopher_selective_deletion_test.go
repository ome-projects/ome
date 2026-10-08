package modelagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func newSelectiveDeletionGopher(t *testing.T, uri string) (*Gopher, *GopherTask) {
	t.Helper()
	g, original := newCancellationCoverageGopher(t, uri)
	g.modelRootDir = t.TempDir()
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gpt-oss-20b", UID: "parent-uid",
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
		Spec: original.BaseModel.Spec,
	}
	model.Spec.Storage.Path = stringPtr(filepath.Join(g.modelRootDir, model.Name))
	g.baseModelLister = &mockBaseModelLister{}
	g.clusterBaseModelLister = &mockClusterBaseModelLister{models: []*v1beta1.ClusterBaseModel{model}}
	require.NoError(t, g.configMapReconciler.ReconcileModelMetadata(context.Background(), &ConfigMapMetadataOp{
		ClusterBaseModel: model,
		ModelMetadata: ModelMetadata{Artifact: Artifact{ParentPath: map[string]string{
			getModelID(nil, model): *model.Spec.Storage.Path,
		}}},
	}))
	require.NoError(t, g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{
		ClusterBaseModel: model, ModelStateOnNode: Ready,
	}))
	return g, &GopherTask{TaskType: Delete, ClusterBaseModel: model}
}

func writeSelectiveDeletionFile(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(path), 0o600))
}

func TestModelDeletionPreservesReferencedDescendants(t *testing.T) {
	for _, uri := range []string{"hf://openai/gpt-oss-20b", "oci://n/ns/b/models/o/gpt-oss-20b", "modelpack://registry.example.com/org/model:tag"} {
		t.Run(uri, func(t *testing.T) {
			for _, kind := range []string{"ClusterBaseModel", "BaseModel"} {
				t.Run(kind, func(t *testing.T) {
					g, task := newSelectiveDeletionGopher(t, uri)
					target := *task.ClusterBaseModel.Spec.Storage.Path
					protected := filepath.Join(target, "releases", "rev-002")
					child := &v1beta1.ClusterBaseModel{
						ObjectMeta: metav1.ObjectMeta{Name: "gpt-oss-20b-rev-002", UID: "child-uid"},
						Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{Path: stringPtr(protected)}},
					}
					if kind == "ClusterBaseModel" {
						g.clusterBaseModelLister.(*mockClusterBaseModelLister).models = append(
							g.clusterBaseModelLister.(*mockClusterBaseModelLister).models, child)
					} else {
						g.baseModelLister.(*mockBaseModelLister).models = []*v1beta1.BaseModel{{
							ObjectMeta: metav1.ObjectMeta{Name: task.ClusterBaseModel.Name, Namespace: "another-namespace"},
							Spec:       child.Spec,
						}}
					}
					kept := filepath.Join(protected, "weights.safetensors")
					removed := []string{
						filepath.Join(target, "weights.safetensors"),
						filepath.Join(target, "config.json"),
						filepath.Join(target, "releases", "rev-001", "weights.safetensors"),
						filepath.Join(target, "unused", "file"),
					}
					writeSelectiveDeletionFile(t, kept)
					for _, path := range removed {
						writeSelectiveDeletionFile(t, path)
					}

					require.NoError(t, g.processTask(task))
					contents, err := os.ReadFile(kept)
					require.NoError(t, err, "deleting the parent must preserve a live revision")
					require.Equal(t, kept, string(contents))
					for _, path := range removed {
						require.NoFileExists(t, path)
					}
					require.NoDirExists(t, filepath.Join(target, "releases", "rev-001"))
					require.NoDirExists(t, filepath.Join(target, "unused"))
					exists, _, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), getModelID(nil, task.ClusterBaseModel))
					require.NoError(t, err)
					require.False(t, exists)
					node, err := g.nodeLabelReconciler.kubeClient.CoreV1().Nodes().Get(context.Background(), g.nodeLabelReconciler.nodeName, metav1.GetOptions{})
					require.NoError(t, err)
					require.NotContains(t, node.Labels, constants.GetClusterBaseModelLabel(task.ClusterBaseModel.Name))
					require.NoError(t, g.processTask(task), "repeated cleanup must be idempotent")
					require.FileExists(t, kept)
				})
			}
		})
	}
}

func TestModelDeletionReferenceBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name       string
		suffix     string
		keepTarget bool
	}{
		{name: "same path with trailing separator", suffix: "/", keepTarget: true},
		{name: "same path after cleaning", suffix: "/releases/..", keepTarget: true},
		{name: "sibling sharing a string prefix", suffix: "-bf16"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, task := newSelectiveDeletionGopher(t, "oci://n/ns/b/models/o/model")
			target := *task.ClusterBaseModel.Spec.Storage.Path
			reference := target + tc.suffix
			g.baseModelLister.(*mockBaseModelLister).models = []*v1beta1.BaseModel{{
				ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "other"},
				Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{Path: stringPtr(reference)}},
			}}
			weights := filepath.Join(target, "weights")
			consumerWeights := filepath.Join(reference, "weights")
			writeSelectiveDeletionFile(t, weights)
			writeSelectiveDeletionFile(t, consumerWeights)
			require.NoError(t, g.processTask(task))
			require.FileExists(t, consumerWeights)
			if tc.keepTarget {
				require.FileExists(t, weights)
			} else {
				require.NoDirExists(t, target)
			}
		})
	}
}

func TestModelDeletionProtectsMultipleAndMissingRevisions(t *testing.T) {
	g, task := newSelectiveDeletionGopher(t, "oci://n/ns/b/models/o/model")
	target := *task.ClusterBaseModel.Spec.Storage.Path
	protected := []string{
		filepath.Join(target, "releases", "rev-002"),
		filepath.Join(target, "releases", "rev-003", "nested"),
		filepath.Join(target, "downloads", "pending-revision"),
	}
	for i, path := range protected {
		g.baseModelLister.(*mockBaseModelLister).models = append(g.baseModelLister.(*mockBaseModelLister).models, &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: filepath.Base(path), Namespace: "other"},
			Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{Path: stringPtr(path + "/")}},
		})
		if i < 2 {
			writeSelectiveDeletionFile(t, filepath.Join(path, "weights"))
		}
	}
	removed := []string{
		filepath.Join(target, "weights"),
		filepath.Join(target, "releases", "rev-003", "unused"),
		filepath.Join(target, "downloads", "old-partial"),
	}
	for _, path := range removed {
		writeSelectiveDeletionFile(t, path)
	}
	require.NoError(t, g.processTask(task))
	for _, path := range protected[:2] {
		require.FileExists(t, filepath.Join(path, "weights"))
	}
	require.DirExists(t, filepath.Dir(protected[2]), "retain the container for a revision that is not downloaded yet")
	for _, path := range removed {
		require.NoFileExists(t, path)
	}
}

func TestModelDeletionPreservesSymlinkConsumers(t *testing.T) {
	g, task := newSelectiveDeletionGopher(t, "oci://n/ns/b/models/o/model")
	target := *task.ClusterBaseModel.Spec.Storage.Path
	protected := filepath.Join(target, "releases", "rev-002")
	kept := filepath.Join(protected, "weights")
	writeSelectiveDeletionFile(t, kept)
	consumer := filepath.Join(g.modelRootDir, "consumer")
	require.NoError(t, os.Symlink(protected, consumer))
	g.baseModelLister.(*mockBaseModelLister).models = []*v1beta1.BaseModel{{
		ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "other"},
		Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{Path: stringPtr(consumer)}},
	}}
	outside := filepath.Join(g.modelRootDir, "outside", "weights")
	writeSelectiveDeletionFile(t, outside)
	unusedLink := filepath.Join(target, "unused-link")
	require.NoError(t, os.Symlink(filepath.Dir(outside), unusedLink))
	removed := filepath.Join(target, "weights")
	writeSelectiveDeletionFile(t, removed)
	require.NoError(t, g.processTask(task))
	require.FileExists(t, kept)
	require.FileExists(t, filepath.Join(consumer, "weights"))
	require.FileExists(t, outside, "cleanup must not follow an unreferenced symlink")
	_, err := os.Lstat(unusedLink)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoFileExists(t, removed)
}

func TestModelDeletionRetainsInternalArtifactAndLockDirectories(t *testing.T) {
	g, task := newSelectiveDeletionGopher(t, "oci://n/ns/b/models/o/model")
	target := *task.ClusterBaseModel.Spec.Storage.Path
	kept := []string{
		filepath.Join(target, hfArtifactLockDirectory, "active.lock"),
		filepath.Join(target, constants.ModelArtifactsDirectory, "shared", "weights"),
	}
	for _, path := range kept {
		writeSelectiveDeletionFile(t, path)
	}
	lockInfo, err := os.Stat(kept[0])
	require.NoError(t, err)
	removed := filepath.Join(target, "weights")
	writeSelectiveDeletionFile(t, removed)
	require.NoError(t, g.processTask(task))
	for _, path := range kept {
		require.FileExists(t, path)
	}
	currentInfo, err := os.Stat(kept[0])
	require.NoError(t, err)
	require.True(t, os.SameFile(lockInfo, currentInfo), "an active lock must keep its inode")
	require.NoFileExists(t, removed)
}

func TestModelDeletionReferenceLookupFailureKeepsAllFiles(t *testing.T) {
	for _, kind := range []string{"BaseModel", "ClusterBaseModel"} {
		t.Run(kind, func(t *testing.T) {
			g, task := newSelectiveDeletionGopher(t, "oci://n/ns/b/models/o/model")
			weights := filepath.Join(*task.ClusterBaseModel.Spec.Storage.Path, "weights")
			writeSelectiveDeletionFile(t, weights)
			lookupErr := errors.New("cache unavailable")
			if kind == "BaseModel" {
				g.baseModelLister.(*mockBaseModelLister).err = lookupErr
			} else {
				g.clusterBaseModelLister.(*mockClusterBaseModelLister).err = lookupErr
			}
			require.ErrorIs(t, g.deleteModel(context.Background(), *task.ClusterBaseModel.Spec.Storage.Path, task), lookupErr)
			require.FileExists(t, weights)
		})
	}
}

func TestLegacyParentCleanupPreservesReferencedRevision(t *testing.T) {
	g, task := newSelectiveDeletionGopher(t, "hf://openai/gpt-oss-20b")
	parent := *task.ClusterBaseModel.Spec.Storage.Path
	child := filepath.Join(g.modelRootDir, "legacy-child")
	kept := filepath.Join(parent, "releases", "rev-002", "weights")
	removed := filepath.Join(parent, "weights")
	writeSelectiveDeletionFile(t, kept)
	writeSelectiveDeletionFile(t, removed)
	require.NoError(t, os.Symlink(parent, child))
	task.ClusterBaseModel.Spec.Storage.Path = stringPtr(child)
	require.NoError(t, g.configMapReconciler.ReconcileModelMetadata(context.Background(), &ConfigMapMetadataOp{
		ClusterBaseModel: task.ClusterBaseModel,
		ModelMetadata:    ModelMetadata{Artifact: Artifact{ParentPath: map[string]string{"clusterbasemodel.deleted-parent": parent}}},
	}))
	g.baseModelLister.(*mockBaseModelLister).models = []*v1beta1.BaseModel{{
		ObjectMeta: metav1.ObjectMeta{Name: "revision", Namespace: "other"},
		Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{Path: stringPtr(filepath.Dir(kept))}},
	}}
	require.NoError(t, g.processTask(task))
	_, err := os.Lstat(child)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.FileExists(t, kept)
	require.NoFileExists(t, removed)
}

func TestModelDeletionProtectsDefaultStoragePath(t *testing.T) {
	g, task := newSelectiveDeletionGopher(t, "oci://n/ns/b/models/o/model")
	target := *task.ClusterBaseModel.Spec.Storage.Path
	g.modelRootDir = target
	consumer := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "other"},
		Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("hf://org/model")}},
	}
	g.baseModelLister.(*mockBaseModelLister).models = []*v1beta1.BaseModel{consumer}
	kept := filepath.Join(getDestPath(&consumer.Spec, g.modelRootDir), "weights")
	removed := filepath.Join(target, "weights")
	writeSelectiveDeletionFile(t, kept)
	writeSelectiveDeletionFile(t, removed)
	require.NoError(t, g.processTask(task))
	require.FileExists(t, kept)
	require.NoFileExists(t, removed)
}

func TestModelDeletionPreservesActiveDescendantDownload(t *testing.T) {
	g, task := newSelectiveDeletionGopher(t, "oci://n/ns/b/models/o/model")
	target := *task.ClusterBaseModel.Spec.Storage.Path
	protected := filepath.Join(target, "releases", "rev-002")
	writer, download := newCancellationCoverageGopher(t, "oci://n/ns/b/models/o/revision")
	writer.modelRootDir = g.modelRootDir
	download.BaseModel.Spec.Storage.Path = stringPtr(protected)
	g.baseModelLister.(*mockBaseModelLister).models = []*v1beta1.BaseModel{download.BaseModel}
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	done := make(chan error, 1)
	go func() {
		done <- runDirectTestDownload(writer, download, func(ctx context.Context, path string) error {
			if err := os.MkdirAll(path, 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(path, "partial"), []byte("partial"), 0o600); err != nil {
				return err
			}
			started <- ctx
			<-release
			return os.WriteFile(filepath.Join(path, "weights"), []byte("complete"), 0o600)
		})
	}()
	var writerCtx context.Context
	select {
	case writerCtx = <-started:
	case err := <-done:
		t.Fatalf("download returned before writing: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("download did not start")
	}
	removed := filepath.Join(target, "weights")
	writeSelectiveDeletionFile(t, removed)
	require.NoError(t, g.processTask(task))
	require.NoError(t, writerCtx.Err())
	require.FileExists(t, filepath.Join(protected, "partial"))
	require.NoFileExists(t, removed)
	lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{
		ModelStoreRoot: g.modelRootDir, ChildModelPath: protected,
	})
	require.NoError(t, err)
	if acquired {
		_ = lock.Close()
	}
	require.False(t, acquired, "parent cleanup must not unlink the active descendant lock")
	unblock()
	require.NoError(t, <-done)
	require.FileExists(t, filepath.Join(protected, "weights"))
}
