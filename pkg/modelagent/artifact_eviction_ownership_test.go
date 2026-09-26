package modelagent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/modelparser"
	"sigs.k8s.io/ome/pkg/xet"
)

func TestDirectEvictionPreservesPendingSharedClaim(t *testing.T) {
	g, task, path := newEvictionTestModel(t)
	other := testHfArtifactTaskInput(t, g.modelRootDir, "retired")
	require.NoError(t, g.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
		return writeModelEntry(cm.Data, other.ChildModelKey, ModelEntry{
			Name: "retired", ModelUID: other.ChildModelUID,
			HfArtifactPendingDeletion: &HfArtifactPendingDeletion{
				Identity: other.Parent.Identity, ModelUID: other.ChildModelUID,
				ChildPath: path, ParentPath: other.Parent.LocalPath,
			},
		})
	}))
	// A persisted cleanup obligation survives the other CR and its placement.
	// Neither a direct nor a shared remover may silently ignore that claim.
	require.Error(t, g.evictDirectArtifact(context.Background(), task))
	require.FileExists(t, filepath.Join(path, "weights"))
}

func TestDirectEvictionPreservesPersistedLegacyAlias(t *testing.T) {
	for _, change := range []string{"deleted", "path moved", "placement changed"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			g, target, path := newEvictionTestModel(t)
			alias := filepath.Join(t.TempDir(), "alias")
			require.NoError(t, os.Symlink(path, alias))
			legacyPath := filepath.Join(alias, "nested")
			other := target.BaseModel.DeepCopy()
			other.Name, other.UID, other.Annotations = "legacy-owner", "legacy-uid", nil
			other.Spec.Storage.Path, other.Spec.Storage.DownloadPolicy = &legacyPath, nil
			_, err := g.modelClient.OmeV1beta1().BaseModels(other.Namespace).Create(ctx, other, metav1.CreateOptions{})
			require.NoError(t, err)
			g.modelConfigParser = modelparser.NewModelConfigParser(g.modelClient, g.logger)
			writer := &GopherTask{TaskType: Download, BaseModel: other}
			source := directHfSource{
				resolve: func(context.Context, string, string, string, string) (string, error) {
					return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil
				},
				download: func(_ context.Context, _ *GopherTask, cfg *xet.DownloadConfig) error {
					require.Equal(t, legacyPath, cfg.LocalDir)
					require.NoError(t, os.MkdirAll(cfg.LocalDir, 0700))
					require.NoError(t, os.WriteFile(filepath.Join(cfg.LocalDir, "weights"), []byte("legacy model"), 0600))
					return os.WriteFile(filepath.Join(cfg.LocalDir, "config.json"), []byte(`{"model_type":"llama","architectures":["LlamaForCausalLM"],"hidden_size":8,"intermediate_size":16,"num_hidden_layers":1,"num_attention_heads":1,"vocab_size":10}`), 0600)
				},
			}
			waiting, err := source.process(ctx, g, writer, other.Spec, true)
			require.NoError(t, err)
			require.False(t, waiting)
			persisted := directArtifactEntry(t, g, writer)
			require.NotNil(t, persisted.Config)
			require.Equal(t, legacyPath, persisted.Config.Artifact.ParentPath[getModelID(other, nil)])
			if change == "deleted" {
				require.NoError(t, g.modelClient.OmeV1beta1().BaseModels(other.Namespace).Delete(ctx, other.Name, metav1.DeleteOptions{}))
			} else {
				live, err := g.modelClient.OmeV1beta1().BaseModels(other.Namespace).Get(ctx, other.Name, metav1.GetOptions{})
				require.NoError(t, err)
				if change == "path moved" {
					newPath := filepath.Join(t.TempDir(), "new-location")
					live.Spec.Storage.Path = &newPath
				} else {
					live.Spec.Storage.NodeSelector = map[string]string{"kubernetes.io/hostname": "another-node"}
				}
				_, err = g.modelClient.OmeV1beta1().BaseModels(other.Namespace).Update(ctx, live, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			// Only durable metadata survives this restart, not the writer's guards.
			g.configMapReconciler = NewConfigMapReconciler(g.configMapReconciler.nodeName, g.configMapReconciler.namespace, g.kubeClient, g.logger)
			require.Error(t, g.evictDirectArtifact(ctx, target))
			require.FileExists(t, filepath.Join(path, "nested", "weights"))
			require.Equal(t, legacyPath, directArtifactEntry(t, g, writer).Config.Artifact.ParentPath[getModelID(other, nil)])
		})
	}
}

func TestDirectEvictionResolvesPersistedMissingPaths(t *testing.T) {
	for _, layout := range []string{"unrelated missing", "unrelated spaces", "unrelated repeated slash", "unrelated relative", "missing child of alias", "alias leaf", "dangling alias", "symlink loop", "raw overlap"} {
		t.Run(layout, func(t *testing.T) {
			ctx := context.Background()
			g, task, path := newEvictionTestModel(t)
			alias := filepath.Join(t.TempDir(), "alias")
			reference := filepath.Join(alias, "missing", "child")
			switch layout {
			case "unrelated spaces":
				reference += " "
			case "unrelated repeated slash":
				reference = alias + "/hf://org/model"
			case "unrelated relative":
				cwd, err := os.Getwd()
				require.NoError(t, err)
				reference, err = filepath.Rel(cwd, reference)
				require.NoError(t, err)
			case "missing child of alias", "alias leaf":
				require.NoError(t, os.Symlink(path, alias))
				if layout == "alias leaf" {
					reference = alias
				}
			case "dangling alias":
				require.NoError(t, os.Symlink(filepath.Join(t.TempDir(), "missing"), alias))
			case "symlink loop":
				require.NoError(t, os.Symlink(alias, alias))
			case "raw overlap":
				// A raw in-tree reference still protects the tree when its current
				// symlink target happens to resolve somewhere unrelated.
				reference = filepath.Join(path, "link")
				require.NoError(t, os.Symlink(t.TempDir(), reference))
			}
			cm, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			_, err = writeModelEntry(cm.Data, "default.basemodel.legacy", ModelEntry{
				Name: "legacy", ModelUID: "legacy", Status: ModelStatusReady,
				Config: &ModelConfig{Artifact: Artifact{ParentPath: map[string]string{"default.basemodel.legacy": reference}}},
			})
			require.NoError(t, err)
			_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
			require.NoError(t, err)
			err = g.evictDirectArtifact(ctx, task)
			if strings.HasPrefix(layout, "unrelated") {
				require.NoError(t, err, "an unrelated absent path must not block eviction")
				require.NoDirExists(t, path)
			} else {
				require.Error(t, err, "overlapping or unsafe persisted paths must protect the artifact")
				require.FileExists(t, filepath.Join(path, "weights"))
			}
		})
	}
}

func TestCompletedDirectEvictionDoesNotBlockReusedPath(t *testing.T) {
	for _, state := range []string{"completed", "pending", "new intent"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			g, previous, path := newEvictionTestModel(t)
			require.NoError(t, g.evictDirectArtifact(ctx, previous))
			other := previous.BaseModel.DeepCopy()
			other.Name, other.UID = "other", "other"
			_, err := g.modelClient.OmeV1beta1().BaseModels(other.Namespace).Create(ctx, other, metav1.CreateOptions{})
			require.NoError(t, err)
			require.NoError(t, os.Mkdir(path, 0700))
			require.NoError(t, os.WriteFile(filepath.Join(path, "weights"), []byte("other"), 0600))
			cm, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			_, err = writeModelEntry(cm.Data, getModelID(other, nil), ModelEntry{Name: other.Name, ModelUID: other.UID, Status: ModelStatusReady, DirectArtifactPath: path})
			require.NoError(t, err)
			if state == "pending" {
				entry, err := existingModelEntry(cm.Data, getModelID(previous.BaseModel, nil))
				require.NoError(t, err)
				entry.DirectArtifactPendingEviction = &DirectArtifactPendingEviction{ModelUID: previous.BaseModel.UID, Path: path}
				_, err = writeModelEntry(cm.Data, getModelID(previous.BaseModel, nil), entry)
				require.NoError(t, err)
			} else if state == "new intent" {
				previous.BaseModel.Annotations = nil
				_, err = g.modelClient.OmeV1beta1().BaseModels(previous.BaseModel.Namespace).Update(ctx, previous.BaseModel, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
			require.NoError(t, err)
			// Completed acknowledgement, including its old path, survives restart.
			g.configMapReconciler = NewConfigMapReconciler(g.configMapReconciler.nodeName, g.configMapReconciler.namespace, g.kubeClient, g.logger)
			err = g.evictDirectArtifact(ctx, &GopherTask{TaskType: Evict, BaseModel: other})
			if state != "completed" {
				require.Error(t, err, "pending cleanup and renewed live intent still protect the path")
				require.FileExists(t, filepath.Join(path, "weights"))
				return
			}
			require.NoError(t, err, "completed eviction is history, not another local owner")
			require.NoDirExists(t, path)
			require.Equal(t, ModelStatusEvicted, directArtifactEntry(t, g, previous).Status)
		})
	}
}

func TestDirectEvictionOtherNodeReferences(t *testing.T) {
	for _, placement := range []string{"selector", "affinity"} {
		for _, claim := range []string{"none", "entry", "path", "pending"} {
			t.Run(placement+"/"+claim, func(t *testing.T) {
				ctx := context.Background()
				g, task, path := newEvictionTestModel(t)
				other := task.BaseModel.DeepCopy()
				other.Name, other.UID, other.Annotations = "other", "other", nil
				if placement == "selector" {
					other.Spec.Storage.NodeSelector = map[string]string{"kubernetes.io/hostname": "different-node"}
				} else {
					other.Spec.Storage.NodeAffinity = &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{{
								MatchExpressions: []corev1.NodeSelectorRequirement{{
									Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: []string{"different-node"},
								}},
							}},
						},
					}
				}
				_, err := g.modelClient.OmeV1beta1().BaseModels(other.Namespace).Create(ctx, other, metav1.CreateOptions{})
				require.NoError(t, err)
				if claim != "none" {
					entry := ModelEntry{Name: other.Name, ModelUID: other.UID, Status: ModelStatusUpdating}
					if claim == "path" {
						entry.DirectArtifactPath = path
					} else if claim == "pending" {
						entry.DirectArtifactPendingEviction = &DirectArtifactPendingEviction{ModelUID: other.UID, Path: path}
					}
					cm, err := g.configMapReconciler.getConfigMap(ctx)
					require.NoError(t, err)
					_, err = writeModelEntry(cm.Data, getModelID(other, nil), entry)
					require.NoError(t, err)
					_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
					require.NoError(t, err)
				}
				err = g.evictDirectArtifact(ctx, task)
				if claim != "none" {
					require.Error(t, err, "persisted local ownership survives placement changes")
					require.FileExists(t, filepath.Join(path, "weights"))
					return
				}
				require.NoError(t, err, "an unpersisted off-node Model has no local claim")
				require.NoDirExists(t, path)
			})
		}
	}
}

func TestDirectEvictionSerializesConcurrentNestedWriter(t *testing.T) {
	ctx := context.Background()
	evictor, task, path := newEvictionTestModel(t)
	node, err := evictor.kubeClient.CoreV1().Nodes().Get(ctx, evictor.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	node.Labels["kubernetes.io/hostname"] = node.Name
	_, err = evictor.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	nestedPath := filepath.Join(path, "nested")
	other := task.BaseModel.DeepCopy()
	other.Name, other.UID = "nested", "nested"
	other.Annotations = map[string]string{ConfigParsingAnnotation: "true"}
	other.Spec.Storage.Path = &nestedPath
	download := &GopherTask{TaskType: Download, BaseModel: other}
	api := evictor.modelClient.(*omefake.Clientset)
	// Separate client and Gopher locks, with only API storage and disk shared.
	otherAPI := omefake.NewSimpleClientset()
	otherAPI.ReactionChain = nil
	otherAPI.AddReactor("*", "*", k8stesting.ObjectReaction(api.Tracker()))
	writer := &Gopher{
		configMapReconciler: NewConfigMapReconciler(evictor.configMapReconciler.nodeName, evictor.configMapReconciler.namespace, evictor.kubeClient, evictor.logger),
		nodeLabelReconciler: NewNodeLabelReconciler(evictor.configMapReconciler.nodeName, evictor.kubeClient, 1, evictor.logger),
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
			downloads++
			require.NoError(t, os.MkdirAll(cfg.LocalDir, 0700))
			return os.WriteFile(filepath.Join(cfg.LocalDir, "weights"), []byte("new model"), 0600)
		},
	}
	lists := 0
	api.PrependReactor("list", "clusterbasemodels", func(k8stesting.Action) (bool, runtime.Object, error) {
		lists++
		if lists != 2 {
			return false, nil, nil
		}
		// The last list snapshot cannot see this newly admitted writer. Only
		// cross-process filesystem serialization can close the check/use gap.
		require.NoError(t, api.Tracker().Add(other))
		admittedCtx, finish, proceed, err := writer.beginTask(download)
		require.NoError(t, err)
		require.True(t, proceed)
		defer finish(false)
		require.NoError(t, writer.safeNodeLabelReconciliation(admittedCtx, &NodeLabelOp{ModelStateOnNode: Updating, BaseModel: other}))
		waiting, err := source.process(writerCtx, writer, download, other.Spec, true)
		require.NoError(t, err)
		require.True(t, waiting, "the ancestor eviction already owns this path family")
		require.Zero(t, downloads)
		require.Nil(t, directArtifactDownloadOperationFromContext(writerCtx).release)
		return true, &v1beta1.ClusterBaseModelList{}, nil
	})
	require.NoError(t, evictor.evictDirectArtifact(ctx, task))
	require.Equal(t, 2, lists)
	require.NoDirExists(t, path)
	select {
	case retry := <-writer.gopherChan:
		require.Same(t, download, retry)
	case <-time.After(time.Second):
		t.Fatal("nested writer was not requeued")
	}
	waiting, err := source.process(writerCtx, writer, download, other.Spec, true)
	require.NoError(t, err)
	require.False(t, waiting)
	require.Equal(t, 1, downloads)
	require.FileExists(t, filepath.Join(nestedPath, "weights"))
	require.DirExists(t, filepath.Join(evictor.modelRootDir, hfArtifactLockDirectory))
}

func TestNestedDirectWriterFailedAdmissionReleasesFamilyLock(t *testing.T) {
	g, task, path := newDirectArtifactTestModel(t)
	nestedPath := filepath.Join(path, "nested")
	task.BaseModel.Spec.Storage.Path = &nestedPath
	live := task.BaseModel.DeepCopy()
	live.UID = "replacement"
	g.modelClient = omefake.NewSimpleClientset(live)
	ctx, release := withDirectArtifactDownloadOperation(context.Background())
	defer release()
	_, acquired, err := g.acquireDirectArtifactDownload(ctx, task)
	require.Error(t, err, "the stale UID must fail admission after taking the lock")
	require.False(t, acquired)
	require.Nil(t, directArtifactDownloadOperationFromContext(ctx).release)
	lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: g.modelRootDir, ChildModelPath: path})
	require.NoError(t, err)
	require.True(t, acquired, "failed nested admission must release its ancestor family")
	require.NoError(t, lock.Close())
}
