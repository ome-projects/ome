package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/xet"
)

func TestDirectArtifactOrdinaryOCIWaitsForPathLock(t *testing.T) {
	g, task, path := newDirectArtifactTestModel(t)
	task.TaskType = Download
	task.BaseModel.Annotations = nil
	task.BaseModel.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/models/o/model")
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	g.metrics = NewMetrics(prometheus.NewRegistry())
	g.gopherChan = make(chan *GopherTask, 1)
	g.samePathWaitDelay = time.Millisecond
	g.downloadRetry = 1
	lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: g.modelRootDir, ChildModelPath: path})
	require.NoError(t, err)
	require.True(t, acquired)
	t.Cleanup(func() { require.NoError(t, lock.Close()) })
	require.NoError(t, g.processTask(task))
	select {
	case retry := <-g.gopherChan:
		require.Same(t, task, retry)
	case <-time.After(time.Second):
		t.Fatal("ordinary OCI writer did not wait for the child lock")
	}
	require.FileExists(t, filepath.Join(path, "weights"))
}

func TestRestoredArtifactCreatesDestinationAncestors(t *testing.T) {
	ctx := context.Background()
	g, task, _ := newDirectArtifactTestModel(t)
	path := filepath.Join(g.modelRootDir, "new-tenant", "model")
	task.BaseModel.Spec.Storage.Path = &path
	delete(task.BaseModel.Annotations, constants.ModelArtifactResidencyAnnotation)
	task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-2"
	task.TaskType = Download
	_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	unlock, acquired, err := g.acquireDirectArtifactPath(task)
	require.NoError(t, err)
	require.True(t, acquired)
	defer unlock()
	err = g.downloadRestoredArtifact(ctx, task, path, func(stage string) error {
		return os.WriteFile(filepath.Join(stage, "weights"), []byte("complete"), 0600)
	})
	require.NoError(t, err, "restoration on a new node must create destination ancestors")
	require.FileExists(t, filepath.Join(path, "weights"))
}

func TestDirectArtifactOrdinaryHfWaitsForPathLock(t *testing.T) {
	g, task, input, source, downloads := newTestDirectHfSource(t)
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	lock, acquired, err := tryHfArtifactChildFileLock(input)
	require.NoError(t, err)
	require.True(t, acquired)
	t.Cleanup(func() { require.NoError(t, lock.Close()) })
	waiting, err := source.process(context.Background(), g, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	require.True(t, waiting)
	require.Zero(t, *downloads)
}

func TestDirectArtifactOrdinaryHfRejectsStaleWork(t *testing.T) {
	for _, change := range []string{"uid", "request", "intent", "source", "path", "deleted"} {
		t.Run(change, func(t *testing.T) {
			g, task, _, source, downloads := newTestDirectHfSource(t)
			task.BaseModel.Spec.Storage.DownloadPolicy = nil
			live := task.BaseModel.DeepCopy()
			switch change {
			case "uid":
				live.UID = "replacement"
			case "request":
				live.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "new-request"
			case "intent":
				live.Annotations[constants.ModelArtifactResidencyAnnotation] = constants.ModelArtifactResidencyEvicted
			case "source":
				live.Spec.Storage.StorageUri = stringPtr("hf://other/model@main")
			case "path":
				live.Spec.Storage.Path = stringPtr(filepath.Join(g.modelRootDir, "other"))
			}
			g.modelClient = omefake.NewSimpleClientset(live)
			if change == "deleted" {
				g.modelClient = omefake.NewSimpleClientset()
			}
			_, err := source.process(context.Background(), g, task, task.BaseModel.Spec, true)
			require.Error(t, err)
			require.Zero(t, *downloads)
		})
	}
}

func TestDirectArtifactOrdinaryHfSettlesReceiptBeforeDownload(t *testing.T) {
	g, task, input, source, _ := newTestDirectHfSource(t)
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
	_, err := g.kubeClient.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, UID: "node-uid"}}, metav1.CreateOptions{})
	require.NoError(t, err)
	path, err := safeArtifactEvictionPath(g.modelRootDir, input.ChildModelPath)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(path, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(path, "partial"), []byte("partial"), 0600))
	require.NoError(t, g.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
		return writeModelEntry(cm.Data, getModelID(task.BaseModel, nil), ModelEntry{ModelUID: task.BaseModel.UID, Status: ModelStatusUpdating})
	}))
	seedDirectArtifactReceipt(t, g, task, ArtifactPendingEviction{ModelUID: task.BaseModel.UID, Path: path})
	called := false
	source.download = func(_ context.Context, _ *GopherTask, config *xet.DownloadConfig) error {
		called = true
		_, err := os.Lstat(filepath.Join(path, "partial"))
		require.True(t, os.IsNotExist(err), "owned cleanup must finish before download")
		require.Nil(t, directArtifactEntry(t, g, task).ArtifactPendingEviction)
		return os.MkdirAll(config.LocalDir, 0700)
	}
	_, err = source.process(context.Background(), g, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	require.True(t, called)
}

func TestDirectArtifactOrdinaryWriterBoundaries(t *testing.T) {
	for _, scenario := range []string{"missing-client", "missing-uid", "outside-root"} {
		t.Run(scenario, func(t *testing.T) {
			g, task, _ := newDirectArtifactTestModel(t)
			task.TaskType = Download
			task.BaseModel.Annotations = nil
			g.modelClient = nil
			if scenario == "missing-uid" {
				task.BaseModel.UID = ""
			}
			if scenario == "outside-root" {
				task.BaseModel.Spec.Storage.Path = stringPtr(filepath.Join(t.TempDir(), "legacy"))
			}
			release, acquired, err := g.acquireDirectArtifactDownload(context.Background(), task)
			if release != nil {
				defer release()
			}
			if scenario == "outside-root" {
				require.NoError(t, err)
				require.True(t, acquired, "unsupported legacy paths retain ordinary behavior")
			} else {
				require.Error(t, err)
				require.False(t, acquired)
			}
		})
	}
}

func TestDirectArtifactOrdinaryHfHoldsLockThroughReady(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			g, task, input, source, _ := newTestDirectHfSource(t)
			task.BaseModel.Spec.Storage.DownloadPolicy = nil
			g.samePathWaitDelay = time.Millisecond
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
			_, err := g.kubeClient.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, UID: "node-uid"}}, metav1.CreateOptions{})
			require.NoError(t, err)
			ctx, release := withDirectArtifactDownloadOperation(context.Background())
			defer release()
			waiting, err := source.process(ctx, g, task, task.BaseModel.Spec, true)
			require.NoError(t, err)
			require.False(t, waiting)
			assertLocked := func() {
				t.Helper()
				lock, acquired, err := tryHfArtifactChildFileLock(input)
				require.NoError(t, err)
				if acquired {
					require.NoError(t, lock.Close())
				}
				require.False(t, acquired, "source return must not release the publication lock")
			}
			assertLocked()
			if changed {
				live := task.BaseModel.DeepCopy()
				live.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "new-request"
				_, err := g.modelClient.OmeV1beta1().BaseModels(live.Namespace).Update(ctx, live, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			g.kubeClient.(*k8sfake.Clientset).PrependReactor("update", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
				assertLocked()
				return false, nil, nil
			})
			ready, err := g.finishDownloadStatus(ctx, task, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready})
			require.NoError(t, err)
			if changed {
				require.False(t, ready)
				select {
				case retry := <-g.gopherChan:
					require.Same(t, task, retry)
				case <-time.After(time.Second):
					t.Fatal("stale publication did not defer readiness")
				}
			} else {
				require.True(t, ready)
			}
			assertLocked()
			release()
			lock, acquired, err := tryHfArtifactChildFileLock(input)
			require.NoError(t, err)
			require.True(t, acquired)
			require.NoError(t, lock.Close())
		})
	}
}

func TestDirectArtifactOrdinaryReadyRetriesUnavailableState(t *testing.T) {
	for _, failure := range []string{"read-error", "corrupt-entry", "canceled"} {
		t.Run(failure, func(t *testing.T) {
			g, task, _, source, _ := newTestDirectHfSource(t)
			task.BaseModel.Spec.Storage.DownloadPolicy = nil
			g.samePathWaitDelay = time.Millisecond
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx, release := withDirectArtifactDownloadOperation(ctx)
			defer release()
			_, err := g.kubeClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, UID: "node-uid"}}, metav1.CreateOptions{})
			require.NoError(t, err)
			waiting, err := source.process(ctx, g, task, task.BaseModel.Spec, true)
			require.NoError(t, err)
			require.False(t, waiting)
			cm, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			failures := 0
			g.kubeClient.(*k8sfake.Clientset).PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
				if failures != 0 {
					return false, nil, nil
				}
				failures++
				switch failure {
				case "canceled":
					cancel()
					return true, nil, ctx.Err()
				case "read-error":
					return true, nil, fmt.Errorf("publication state temporarily unavailable")
				default:
					corrupt := cm.DeepCopy()
					corrupt.Data[getModelID(task.BaseModel, nil)] = "{"
					return true, corrupt, nil
				}
			})
			op := &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}
			published, err := g.finishDownloadStatus(ctx, task, op)
			require.False(t, published)
			require.Equal(t, 1, failures)
			node, readErr := g.kubeClient.CoreV1().Nodes().Get(context.Background(), cm.Name, metav1.GetOptions{})
			require.NoError(t, readErr)
			require.NotEqual(t, "Ready", node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
			if failure == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
				require.True(t, task.SamePathWaitStartedAt.IsZero(), "cancellation must not requeue publication")
				return
			}
			require.NoError(t, err)
			select {
			case retry := <-g.gopherChan:
				require.Same(t, task, retry)
			case <-time.After(time.Second):
				t.Fatal("unavailable publication state must queue a retry")
			}
			published, err = g.finishDownloadStatus(ctx, task, op)
			require.NoError(t, err)
			require.True(t, published)
		})
	}
}

func newDirectArtifactTestModel(t *testing.T) (*Gopher, *GopherTask, string) {
	t.Helper()
	g, task, path := newArtifactEvictionTestModel(t)
	root, err := canonicalHfArtifactStoreRoot(g.modelRootDir)
	require.NoError(t, err)
	path, err = safeArtifactEvictionPath(g.modelRootDir, path)
	require.NoError(t, err)
	g.modelRootDir = root
	task.BaseModel.Spec.Storage.Path = &path
	_, err = g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	cm, err := g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	cm.Data[getModelID(task.BaseModel, nil)] = `{"status":"Ready","modelUID":"uid"}`
	_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	return g, task, path
}

func TestDirectArtifactEvictionWaitsForPathLock(t *testing.T) {
	g, task, path := newDirectArtifactTestModel(t)
	lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: g.modelRootDir, ChildModelPath: path})
	require.NoError(t, err)
	require.True(t, acquired)
	t.Cleanup(func() { require.NoError(t, lock.Close()) })
	_, _ = g.processArtifactEviction(context.Background(), task)
	require.FileExists(t, filepath.Join(path, "weights"), "another process owns the child path")
}

func TestDirectArtifactAcquireRejectsUnsafePaths(t *testing.T) {
	for _, scenario := range []string{"unknown-uid", "empty-path", "root", "outside", "symlink-ancestor", "symlink-leaf", "staging", "shared-parent"} {
		t.Run(scenario, func(t *testing.T) {
			g, task, path := newDirectArtifactTestModel(t)
			outside := t.TempDir()
			switch scenario {
			case "unknown-uid":
				task.BaseModel.UID = ""
			case "empty-path":
				path = ""
			case "root":
				path = g.modelRootDir
			case "outside":
				path = outside
			case "symlink-ancestor":
				alias := filepath.Join(g.modelRootDir, "alias")
				require.NoError(t, os.Symlink(outside, alias))
				path = filepath.Join(alias, "model")
			case "symlink-leaf":
				path = filepath.Join(g.modelRootDir, "alias")
				require.NoError(t, os.Symlink(outside, path))
			case "staging":
				path = filepath.Join(g.modelRootDir, directArtifactStagingDirectory, "model")
			case "shared-parent":
				path = filepath.Join(g.modelRootDir, constants.ModelArtifactsDirectory, "model")
			}
			task.BaseModel.Spec.Storage.Path = &path
			release, acquired, err := g.acquireDirectArtifactPath(task)
			require.Error(t, err)
			require.False(t, acquired)
			require.Nil(t, release)
			require.DirExists(t, outside)
		})
	}
}

func TestDirectArtifactEvictionReceiptCASPreservesUnrelatedFiles(t *testing.T) {
	g, task, path := newDirectArtifactTestModel(t)
	key := getModelID(task.BaseModel, nil)
	parent := filepath.Join(g.modelRootDir, "unrelated-parent")
	require.NoError(t, os.Mkdir(parent, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(parent, "keep"), []byte("keep"), 0600))
	require.NoError(t, os.Symlink(parent, filepath.Join(path, "external-link")))
	client := g.kubeClient.(*k8sfake.Clientset)
	conflicted := false
	client.PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		cm := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		entry, err := existingModelEntry(cm.Data, key)
		require.NoError(t, err)
		if entry.ArtifactPendingEviction != nil && !conflicted {
			conflicted = true
			require.FileExists(t, filepath.Join(path, "weights"))
			stored, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), cm.Namespace, cm.Name)
			require.NoError(t, err)
			other := stored.(*corev1.ConfigMap).DeepCopy()
			other.Data["unrelated"] = `{"status":"Ready","name":"keep"}`
			require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), other, other.Namespace))
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, cm.Name, fmt.Errorf("concurrent update"))
		}
		return false, nil, nil
	})
	waiting, err := g.processArtifactEviction(context.Background(), task)
	require.NoError(t, err)
	require.False(t, waiting)
	require.True(t, conflicted)
	require.FileExists(t, filepath.Join(parent, "keep"))
	entry := directArtifactEntry(t, g, task)
	require.Equal(t, ModelStatusEvicted, entry.Status)
	require.Nil(t, entry.ArtifactPendingEviction)
	cm, err := g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	require.Equal(t, `{"status":"Ready","name":"keep"}`, cm.Data["unrelated"])
}

func TestDirectArtifactEvictionRequiresDurableReceipt(t *testing.T) {
	g, task, path := newDirectArtifactTestModel(t)
	g.kubeClient.(*k8sfake.Clientset).PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		cm := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
		require.NoError(t, err)
		if entry.ArtifactPendingEviction != nil {
			return true, nil, fmt.Errorf("receipt storage unavailable")
		}
		return false, nil, nil
	})
	_, err := g.processArtifactEviction(context.Background(), task)
	require.Error(t, err)
	require.FileExists(t, filepath.Join(path, "weights"))
}

func TestDirectArtifactEvictionSelfOwnedHfMetadata(t *testing.T) {
	for _, scenario := range []string{"self", "self-resume", "self-root-alias", "foreign-key", "foreign-path", "extra-parent", "descendants", "empty-path", "outside-root", "path-alias"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			g, task, path := newDirectArtifactTestModel(t)
			key := getModelID(task.BaseModel, task.ClusterBaseModel)
			artifact := Artifact{Sha: "revision", ParentPath: map[string]string{key: path}}
			outside := t.TempDir()
			protected := filepath.Join(outside, "keep")
			require.NoError(t, os.WriteFile(protected, []byte("keep"), 0600))
			switch scenario {
			case "self-root-alias":
				// OS aliases above the configured root have the same bounded path.
				alias := filepath.Join(t.TempDir(), "ancestor")
				require.NoError(t, os.Symlink(filepath.Dir(g.modelRootDir), alias))
				g.modelRootDir = filepath.Join(alias, filepath.Base(g.modelRootDir))
				task.BaseModel.Spec.Storage.Path = stringPtr(filepath.Join(g.modelRootDir, "model"))
				artifact.ParentPath[key] = *task.BaseModel.Spec.Storage.Path
				_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
				require.NoError(t, err)
			case "foreign-key":
				artifact.ParentPath = map[string]string{"customer.basemodel.other": path}
			case "foreign-path":
				artifact.ParentPath[key] = filepath.Join(g.modelRootDir, "other")
			case "extra-parent":
				artifact.ParentPath["customer.basemodel.other"] = path
			case "descendants":
				artifact.ChildrenPaths = []string{filepath.Join(g.modelRootDir, "child")}
			case "empty-path":
				artifact.ParentPath[key] = ""
			case "outside-root":
				artifact.ParentPath[key] = outside
			case "path-alias":
				alias := filepath.Join(g.modelRootDir, "alias")
				require.NoError(t, os.Symlink(path, alias))
				artifact.ParentPath[key] = alias
			}
			require.NoError(t, g.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
				entry, err := existingModelEntry(cm.Data, key)
				if err != nil {
					return false, err
				}
				entry.Config = &ModelConfig{Artifact: artifact}
				return writeModelEntry(cm.Data, key, entry)
			}))
			var err error
			if scenario == "self-resume" {
				seedDirectArtifactReceipt(t, g, task, ArtifactPendingEviction{ModelUID: task.BaseModel.UID, Path: path})
				release, acquired, lockErr := g.acquireDirectArtifactPath(task)
				require.NoError(t, lockErr)
				require.True(t, acquired)
				defer release()
				err = g.resumeDirectArtifactEviction(ctx, task)
			} else {
				_, err = g.processArtifactEviction(ctx, task)
			}
			switch scenario {
			case "self", "self-resume", "self-root-alias":
				require.NoError(t, err)
				_, statErr := os.Lstat(path)
				require.True(t, os.IsNotExist(statErr))
				require.Nil(t, directArtifactEntry(t, g, task).ArtifactPendingEviction)
			default:
				require.Error(t, err)
				require.FileExists(t, filepath.Join(path, "weights"))
			}
			require.FileExists(t, protected)
		})
	}
}

func TestDirectArtifactEvictionPersistsReceiptBeforeRemovingFiles(t *testing.T) {
	g, task, path := newDirectArtifactTestModel(t)
	key := getModelID(task.BaseModel, nil)
	recorded := false
	g.kubeClient.(*k8sfake.Clientset).PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		cm := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		var entry map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(cm.Data[key]), &entry))
		if entry["artifactPendingEviction"] != nil && !recorded {
			require.FileExists(t, filepath.Join(path, "weights"))
			recorded = true
		}
		if string(entry["status"]) == `"Evicted"` {
			return true, nil, fmt.Errorf("simulate crash after file removal")
		}
		return false, nil, nil
	})
	_, _ = g.processArtifactEviction(context.Background(), task)
	require.True(t, recorded, "direct cleanup must persist ownership before RemoveAll")
	_, err := os.Lstat(path)
	require.True(t, os.IsNotExist(err))
	cm, err := g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	require.Contains(t, cm.Data[key], "artifactPendingEviction", "failed completion must retain the receipt")
}

func TestDirectArtifactEvictionRechecksConsumersUnderPathLock(t *testing.T) {
	g, task, path := newDirectArtifactTestModel(t)
	checkedUnderLock := false
	g.modelClient.(*omefake.Clientset).PrependReactor("list", "inferenceservices", func(k8stesting.Action) (bool, runtime.Object, error) {
		lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: g.modelRootDir, ChildModelPath: path})
		require.NoError(t, err)
		if acquired {
			require.NoError(t, lock.Close())
			return false, nil, nil
		}
		checkedUnderLock = true
		return true, &v1beta1.InferenceServiceList{Items: []v1beta1.InferenceService{{
			ObjectMeta: metav1.ObjectMeta{Namespace: task.BaseModel.Namespace, Name: "new-consumer"},
			Spec:       v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: task.BaseModel.Name}},
		}}}, nil
	})
	_, _ = g.processArtifactEviction(context.Background(), task)
	require.True(t, checkedUnderLock)
	require.FileExists(t, filepath.Join(path, "weights"))
}

func seedDirectArtifactReceipt(t *testing.T, g *Gopher, task *GopherTask, receipt ArtifactPendingEviction) {
	t.Helper()
	err := g.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
		key := getModelID(task.BaseModel, task.ClusterBaseModel)
		entry, err := existingModelEntry(cm.Data, key)
		if err != nil {
			return false, err
		}
		entry.ArtifactPendingEviction = &receipt
		return writeModelEntry(cm.Data, key, entry)
	})
	require.NoError(t, err)
}

func directArtifactEntry(t *testing.T, g *Gopher, task *GopherTask) ModelEntry {
	t.Helper()
	cm, err := g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, task.ClusterBaseModel))
	require.NoError(t, err)
	return entry
}

func TestDirectArtifactEvictionResumesAfterRestartAndWithdrawal(t *testing.T) {
	for _, absent := range []bool{false, true} {
		t.Run(fmt.Sprint(absent), func(t *testing.T) {
			g, task, path := newDirectArtifactTestModel(t)
			seedDirectArtifactReceipt(t, g, task, ArtifactPendingEviction{ModelUID: task.BaseModel.UID, Path: path})
			if absent {
				require.NoError(t, os.RemoveAll(path))
			}
			delete(task.BaseModel.Annotations, constants.ModelArtifactResidencyAnnotation)
			_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			// A fresh reconciler has no process-local ownership cache.
			g.configMapReconciler = NewConfigMapReconciler("node1", g.configMapReconciler.namespace, g.kubeClient, g.logger)
			release, acquired, err := g.acquireDirectArtifactPath(task)
			require.NoError(t, err)
			require.True(t, acquired)
			defer release()
			require.NoError(t, g.resumeDirectArtifactEviction(context.Background(), task))
			_, err = os.Lstat(path)
			require.True(t, os.IsNotExist(err))
			require.Nil(t, directArtifactEntry(t, g, task).ArtifactPendingEviction)
		})
	}
}

func TestDirectArtifactEvictionResumesWithWaitingPods(t *testing.T) {
	for _, node := range []string{"", "node2", "node1"} {
		t.Run("pod-node="+node, func(t *testing.T) {
			ctx := context.Background()
			g, task, path := newDirectArtifactTestModel(t)
			seedDirectArtifactReceipt(t, g, task, ArtifactPendingEviction{ModelUID: task.BaseModel.UID, Path: path})
			delete(task.BaseModel.Annotations, constants.ModelArtifactResidencyAnnotation)
			task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-2"
			_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			label := constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)
			_, err = g.kubeClient.CoreV1().Pods(task.BaseModel.Namespace).Create(ctx, &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "waiting-endpoint"},
				Spec: corev1.PodSpec{NodeName: node, NodeSelector: map[string]string{label: string(Ready)},
					Volumes: []corev1.Volume{{Name: "model", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: path}}}},
				},
				Status: corev1.PodStatus{Phase: corev1.PodPending},
			}, metav1.CreateOptions{})
			require.NoError(t, err)
			release, acquired, err := g.acquireDirectArtifactPath(task)
			require.NoError(t, err)
			require.True(t, acquired)
			defer release()
			err = g.resumeDirectArtifactEviction(ctx, task)
			if node == "node1" {
				require.ErrorContains(t, err, "consuming pod")
				require.FileExists(t, filepath.Join(path, "weights"))
				require.NotNil(t, directArtifactEntry(t, g, task).ArtifactPendingEviction)
			} else {
				require.NoError(t, err)
				require.NoDirExists(t, path)
				require.Nil(t, directArtifactEntry(t, g, task).ArtifactPendingEviction)
			}
		})
	}
}

func TestDirectArtifactEvictionRejectsAmbiguousReceipt(t *testing.T) {
	for _, scenario := range []string{"empty-uid", "other-uid", "empty-path", "outside-root", "root", "symlink", "consumer", "other-model"} {
		t.Run(scenario, func(t *testing.T) {
			g, task, path := newDirectArtifactTestModel(t)
			outside := t.TempDir()
			protected := filepath.Join(outside, "protected")
			require.NoError(t, os.WriteFile(protected, []byte("keep"), 0600))
			receipt := ArtifactPendingEviction{ModelUID: task.BaseModel.UID, Path: path}
			switch scenario {
			case "empty-uid":
				receipt.ModelUID = ""
			case "other-uid":
				receipt.ModelUID = "replaced"
			case "empty-path":
				receipt.Path = ""
			case "outside-root":
				receipt.Path = outside
			case "root":
				receipt.Path = g.modelRootDir
			case "symlink":
				require.NoError(t, os.RemoveAll(path))
				require.NoError(t, os.Symlink(outside, path))
			case "consumer":
				_, err := g.kubeClient.CoreV1().Pods("default").Create(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "consumer"}, Spec: corev1.PodSpec{NodeName: "node1", Volumes: []corev1.Volume{{Name: "model", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: path}}}}}}, metav1.CreateOptions{})
				require.NoError(t, err)
			case "other-model":
				other := task.BaseModel.DeepCopy()
				other.Name, other.UID = "other", "other"
				_, err := g.modelClient.OmeV1beta1().BaseModels(other.Namespace).Create(context.Background(), other, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			seedDirectArtifactReceipt(t, g, task, receipt)
			// The production caller owns this same lock before resume.
			lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: g.modelRootDir, ChildModelPath: path})
			require.NoError(t, err)
			require.True(t, acquired)
			defer lock.Close()
			require.Error(t, g.resumeDirectArtifactEviction(context.Background(), task))
			require.NotNil(t, directArtifactEntry(t, g, task).ArtifactPendingEviction)
			require.FileExists(t, protected)
			if scenario != "symlink" {
				require.FileExists(t, filepath.Join(path, "weights"))
			}
		})
	}
}

func TestDownloadRestoredArtifactPublication(t *testing.T) {
	for _, scenario := range []string{"success", "existing", "canceled", "stale-request", "changed-source", "replacement-target", "download-error", "deleted"} {
		t.Run(scenario, func(t *testing.T) {
			g, task, path := newDirectArtifactTestModel(t)
			delete(task.BaseModel.Annotations, constants.ModelArtifactResidencyAnnotation)
			task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-1"
			task.TaskType = Download
			_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			if scenario != "existing" {
				require.NoError(t, os.RemoveAll(path))
			}
			release, acquired, err := g.acquireDirectArtifactPath(task)
			require.NoError(t, err)
			require.True(t, acquired)
			defer release()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var stage string
			err = g.downloadRestoredArtifact(ctx, task, path, func(destination string) error {
				if scenario == "existing" {
					require.Equal(t, path, destination)
					require.FileExists(t, filepath.Join(destination, "weights"))
					return nil
				}
				require.NotEqual(t, path, destination)
				stage = destination
				require.DirExists(t, destination)
				require.NoError(t, os.WriteFile(filepath.Join(destination, "new-weights"), []byte("complete"), 0600))
				// Another process can start while this attempt still owns its stage.
				require.NoError(t, g.cleanupArtifactStaging())
				require.FileExists(t, filepath.Join(destination, "new-weights"))
				switch scenario {
				case "canceled":
					cancel()
				case "stale-request", "changed-source":
					latest := task.BaseModel.DeepCopy()
					if scenario == "stale-request" {
						latest.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-2"
					} else {
						latest.Spec.Storage.StorageUri = stringPtr("hf://other/model")
					}
					_, updateErr := g.modelClient.OmeV1beta1().BaseModels(latest.Namespace).Update(context.Background(), latest, metav1.UpdateOptions{})
					require.NoError(t, updateErr)
				case "replacement-target":
					require.NoError(t, os.Mkdir(path, 0700))
					require.NoError(t, os.WriteFile(filepath.Join(path, "other-owner"), []byte("keep"), 0600))
				case "download-error":
					return fmt.Errorf("incomplete download")
				case "deleted":
					require.NoError(t, g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Delete(ctx, task.BaseModel.Name, metav1.DeleteOptions{}))
				}
				return nil
			})
			if stage != "" {
				require.NoDirExists(t, stage, "completed or abandoned attempts must not leave unpublished files")
			}
			switch scenario {
			case "success":
				require.NoError(t, err)
				require.FileExists(t, filepath.Join(path, "new-weights"))
			case "existing":
				require.NoError(t, err)
				require.FileExists(t, filepath.Join(path, "weights"))
			case "replacement-target":
				require.Error(t, err)
				require.FileExists(t, filepath.Join(path, "other-owner"))
				require.NoFileExists(t, filepath.Join(path, "new-weights"))
			default:
				require.Error(t, err)
				_, statErr := os.Lstat(path)
				require.True(t, os.IsNotExist(statErr))
			}
		})
	}
}
