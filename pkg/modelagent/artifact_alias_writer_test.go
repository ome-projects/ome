package modelagent

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/xet"
)

func TestDirectEvictionSerializesAliasWriter(t *testing.T) {
	for _, spelling := range []string{"absolute", "space", "relative", "nil", "empty"} {
		for _, order := range []string{"eviction first", "writer first", "external legacy"} {
			if order == "external legacy" && (spelling == "nil" || spelling == "empty") {
				continue // A missing destination intentionally falls back inside the configured store.
			}
			t.Run(spelling+"/"+order, func(t *testing.T) {
				ctx := context.Background()
				evictor, task, path := newEvictionTestModel(t)
				node, err := evictor.kubeClient.CoreV1().Nodes().Get(ctx, evictor.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				node.Labels["kubernetes.io/hostname"] = node.Name
				_, err = evictor.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
				require.NoError(t, err)
				if spelling == "nil" || spelling == "empty" {
					fallbackFamily := filepath.Join(evictor.modelRootDir, "hf:")
					require.NoError(t, os.Rename(path, fallbackFamily))
					path = fallbackFamily
					task.BaseModel.Spec.Storage.Path = &path
					_, err = evictor.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
					require.NoError(t, err)
					cm, err := evictor.configMapReconciler.getConfigMap(ctx)
					require.NoError(t, err)
					entry := directArtifactEntry(t, evictor, task)
					entry.Config.Artifact.ParentPath[getModelID(task.BaseModel, nil)] = path
					_, err = writeModelEntry(cm.Data, getModelID(task.BaseModel, nil), entry)
					require.NoError(t, err)
					_, err = evictor.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
					require.NoError(t, err)
				}
				alias := filepath.Join(t.TempDir(), "alias")
				target := path
				if order == "external legacy" {
					target = t.TempDir()
				}
				require.NoError(t, os.Symlink(target, alias))
				destination := filepath.Join(alias, "nested")
				other := task.BaseModel.DeepCopy()
				other.Name, other.UID = "alias-writer", "alias-writer"
				other.Annotations = map[string]string{ConfigParsingAnnotation: "true"}
				canonicalDestination := filepath.Join(path, "nested")
				switch spelling {
				case "space":
					destination += " "
					canonicalDestination += " "
				case "relative":
					cwd, err := os.Getwd()
					require.NoError(t, err)
					destination, err = filepath.Rel(cwd, destination)
					require.NoError(t, err)
				case "nil", "empty":
					destination = evictor.modelRootDir + "/" + *other.Spec.Storage.StorageUri
					canonicalDestination = filepath.Join(path, "org", "model")
				}
				other.Spec.Storage.Path = &destination
				if spelling == "nil" {
					other.Spec.Storage.Path = nil
				} else if spelling == "empty" {
					other.Spec.Storage.Path = stringPtr("")
				}
				download := &GopherTask{TaskType: Download, BaseModel: other}
				api := evictor.modelClient.(*omefake.Clientset)
				otherAPI := omefake.NewSimpleClientset()
				otherAPI.ReactionChain = nil
				otherAPI.AddReactor("*", "*", k8stesting.ObjectReaction(api.Tracker()))
				writer := &Gopher{
					configMapReconciler: NewConfigMapReconciler(evictor.configMapReconciler.nodeName, evictor.configMapReconciler.namespace, evictor.kubeClient, evictor.logger),
					nodeLabelReconciler: NewNodeLabelReconciler(node.Name, evictor.kubeClient, 1, evictor.logger),
					kubeClient:          evictor.kubeClient, modelClient: otherAPI, modelRootDir: evictor.modelRootDir, logger: evictor.logger,
					gopherChan: make(chan *GopherTask, 1), samePathWaitDelay: time.Millisecond,
				}
				writerCtx, release := withDirectArtifactDownloadOperation(ctx)
				defer release()
				downloads := 0
				source := directHfSource{
					resolve: func(context.Context, string, string, string, string) (string, error) {
						return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil
					},
					download: func(_ context.Context, _ *GopherTask, cfg *xet.DownloadConfig) error {
						require.Equal(t, destination, cfg.LocalDir, "source destination spelling must be preserved")
						downloads++
						require.NoError(t, os.MkdirAll(cfg.LocalDir, 0700))
						return os.WriteFile(filepath.Join(cfg.LocalDir, "weights"), []byte("alias writer"), 0600)
					},
				}
				write := func() bool {
					t.Helper()
					admittedCtx, finish, proceed, err := writer.beginTask(download)
					require.NoError(t, err)
					require.True(t, proceed)
					defer finish(false)
					require.NoError(t, writer.safeNodeLabelReconciliation(admittedCtx, &NodeLabelOp{ModelStateOnNode: Updating, BaseModel: other}))
					waiting, err := source.process(writerCtx, writer, download, other.Spec, true)
					require.NoError(t, err)
					return waiting
				}
				if order == "writer first" {
					require.NoError(t, api.Tracker().Add(other))
					require.False(t, write())
					require.NotNil(t, directArtifactDownloadOperationFromContext(writerCtx).release)
					require.ErrorContains(t, evictor.evictDirectArtifact(ctx, task), "active writer")
				} else {
					lists := 0
					api.PrependReactor("list", "clusterbasemodels", func(k8stesting.Action) (bool, runtime.Object, error) {
						lists++
						if lists != 2 {
							return false, nil, nil
						}
						require.NoError(t, api.Tracker().Add(other))
						waiting := write()
						if order == "external legacy" {
							require.False(t, waiting)
						} else {
							require.True(t, waiting)
							require.Zero(t, downloads)
						}
						require.Nil(t, directArtifactDownloadOperationFromContext(writerCtx).release)
						return true, &v1beta1.ClusterBaseModelList{}, nil
					})
					require.NoError(t, evictor.evictDirectArtifact(ctx, task))
					require.Equal(t, 2, lists)
					if order != "external legacy" {
						require.NoDirExists(t, path)
						select {
						case retry := <-writer.gopherChan:
							require.Same(t, download, retry)
						case <-time.After(time.Second):
							t.Fatal("alias writer was not requeued")
						}
						// The fixed alias is dangling now. The real source must recreate
						// its managed destination safely, not keep retrying forever.
						require.False(t, write())
					}
				}
				require.Equal(t, 1, downloads)
				require.FileExists(t, filepath.Join(destination, "weights"))
				if order == "external legacy" {
					return
				}
				ready, err := writer.finishDownloadStatus(writerCtx, download, &NodeLabelOp{ModelStateOnNode: Ready, BaseModel: other})
				require.NoError(t, err)
				require.True(t, ready)
				require.Equal(t, canonicalDestination, directArtifactEntry(t, writer, download).DirectArtifactPath)
				lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: evictor.modelRootDir, ChildModelPath: path})
				require.NoError(t, err)
				if acquired {
					require.NoError(t, lock.Close())
				}
				require.False(t, acquired, "the alias writer holds the family lock through Ready")
				release()
				lock, acquired, err = tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: evictor.modelRootDir, ChildModelPath: path})
				require.NoError(t, err)
				require.True(t, acquired)
				require.NoError(t, lock.Close())
			})
		}
	}
}

func TestDirectAliasWriterCannotBypassManagedBoundaries(t *testing.T) {
	for _, target := range []string{"store root", "shared", "locks", "cycle", "leaf link", "managed ancestor", "managed parent traversal", "hidden managed ancestor", "hidden outward ancestor", "hidden same-family ancestor"} {
		t.Run(target, func(t *testing.T) {
			g, task, path := newDirectArtifactTestModel(t)
			alias := filepath.Join(t.TempDir(), "alias")
			destination := filepath.Join(alias, "nested")
			switch target {
			case "store root":
				path, destination = g.modelRootDir, alias
			case "shared":
				path = filepath.Join(g.modelRootDir, constants.ModelArtifactsDirectory)
			case "locks":
				path = filepath.Join(g.modelRootDir, hfArtifactLockDirectory)
			case "cycle":
				path = alias
			case "leaf link":
				destination = alias
			case "managed ancestor", "managed parent traversal":
				alias = filepath.Join(path, "alias")
				path = filepath.Join(g.modelRootDir, "other-family", "child")
				require.NoError(t, os.MkdirAll(path, 0700))
				destination = alias + "/nested"
				if target == "managed parent traversal" {
					destination = alias + "/../nested"
				}
			case "hidden managed ancestor", "hidden outward ancestor", "hidden same-family ancestor":
				intermediate := filepath.Join(path, "alias")
				linked := filepath.Join(g.modelRootDir, "other-family")
				if target == "hidden outward ancestor" {
					linked = t.TempDir()
				} else if target == "hidden same-family ancestor" {
					linked = filepath.Join(path, "data")
				}
				require.NoError(t, os.MkdirAll(linked, 0700))
				require.NoError(t, os.Symlink(linked, intermediate))
				path = intermediate
			}
			require.NoError(t, os.Symlink(path, alias))
			task.BaseModel.Spec.Storage.Path = &destination
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			unlock, acquired, err := g.acquireDirectArtifactDownload(context.Background(), task)
			if acquired {
				unlock()
			}
			require.Error(t, err)
			require.False(t, acquired)
		})
	}
}

func TestDirectAliasWriterFailedAdmissionPreservesMissingDestination(t *testing.T) {
	g, task, path := newDirectArtifactTestModel(t)
	canonical := filepath.Join(path, "absent", "child")
	alias := filepath.Join(t.TempDir(), "alias")
	require.NoError(t, os.Symlink(filepath.Dir(canonical), alias))
	destination := filepath.Join(alias, "child")
	task.BaseModel.Spec.Storage.Path = &destination
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	ctx := context.Background()
	cm, err := g.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, err)
	entry := directArtifactEntry(t, g, task)
	entry.DirectArtifactPendingEviction = &DirectArtifactPendingEviction{ModelUID: task.BaseModel.UID, Path: path}
	_, err = writeModelEntry(cm.Data, getModelID(task.BaseModel, nil), entry)
	require.NoError(t, err)
	_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	_, acquired, err := g.acquireDirectArtifactDownload(ctx, task)
	require.Error(t, err)
	require.False(t, acquired)
	require.NoDirExists(t, canonical, "admission must complete before creating the aliased destination")
	lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: g.modelRootDir, ChildModelPath: path})
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, lock.Close())
}

func TestDirectHfWriterReplacesManagedDanglingLeaf(t *testing.T) {
	for _, spelling := range []string{"relative", "space"} {
		t.Run(spelling, func(t *testing.T) {
			g, task, input, source, downloads := newTestDirectHfSource(t)
			task.BaseModel.Spec.Storage.DownloadPolicy = nil
			path := input.ChildModelPath
			if spelling == "space" {
				path += " "
			}
			external := filepath.Join(t.TempDir(), "absent")
			require.NoError(t, os.Symlink(external, path))
			destination := path
			if spelling == "relative" {
				cwd, err := os.Getwd()
				require.NoError(t, err)
				destination, err = filepath.Rel(cwd, path)
				require.NoError(t, err)
			}
			task.BaseModel.Spec.Storage.Path = &destination
			ctx, release := withDirectArtifactDownloadOperation(context.Background())
			defer release()
			waiting, err := source.process(ctx, g, task, task.BaseModel.Spec, true)
			require.NoError(t, err)
			require.False(t, waiting, "a managed leaf link is unlinked, not mkdir'd through")
			require.Equal(t, 1, *downloads)
			require.FileExists(t, filepath.Join(path, "model.safetensors"))
			require.NoDirExists(t, external)
			require.NotNil(t, directArtifactDownloadOperationFromContext(ctx).release)
		})
	}
}
