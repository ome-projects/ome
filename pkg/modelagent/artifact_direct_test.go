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
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestDirectArtifactOrdinaryOCIWaitsForPathLock(t *testing.T) {
	g, task, path := newDirectArtifactTestModel(t)
	task.TaskType = Download
	task.BaseModel.Annotations = nil
	task.BaseModel.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/models/o/model")
	// Broken lock admission must fail locally instead of reaching instance-principal discovery.
	task.BaseModel.Spec.Storage.Parameters = &map[string]string{"auth": "unsupported-test-auth"}
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

func TestDirectArtifactHfAcquisitionRetriesUnavailableAPI(t *testing.T) {
	for _, fault := range []string{"model-read", "handoff-read", "canceled"} {
		t.Run(fault, func(t *testing.T) {
			g, task, input, source, downloads := newTestDirectHfSource(t)
			defer g.taskQueue.close()
			task.BaseModel.Spec.Storage.DownloadPolicy = nil
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			g.samePathWaitDelay = time.Millisecond
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			failed, handoff := false, false
			g.modelClient.(*omefake.Clientset).PrependReactor("get", "basemodels", func(k8stesting.Action) (bool, runtime.Object, error) {
				if failed {
					return false, nil, nil
				}
				if fault == "handoff-read" {
					handoff = true
					return false, nil, nil
				}
				failed = true
				if fault == "canceled" {
					cancel()
					return true, nil, ctx.Err()
				}
				return true, nil, apierrors.NewServiceUnavailable("one-shot Model GET failure")
			})
			g.kubeClient.(*k8sfake.Clientset).PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
				if handoff && !failed {
					failed = true
					return true, nil, apierrors.NewServiceUnavailable("one-shot handoff read failure")
				}
				return false, nil, nil
			})
			waiting, err := source.process(ctx, g, task, task.BaseModel.Spec, true)
			require.True(t, failed)
			require.Zero(t, *downloads)
			lock, acquired, lockErr := tryHfArtifactChildFileLock(input)
			require.NoError(t, lockErr)
			require.True(t, acquired, "failed acquisition must release its lock")
			require.NoError(t, lock.Close())
			if fault == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
				require.False(t, waiting)
				require.True(t, task.SamePathWaitStartedAt.IsZero())
				require.Empty(t, g.gopherChan)
				return
			}
			require.NoError(t, err)
			require.True(t, waiting)
			select {
			case retry := <-g.gopherChan:
				require.Same(t, task, retry)
				waiting, err = source.process(ctx, g, retry, retry.BaseModel.Spec, true)
				require.NoError(t, err)
				require.False(t, waiting)
				require.Equal(t, 1, *downloads)
			case <-time.After(time.Second):
				t.Fatal("transient HF acquisition failure dropped the download task")
			}
		})
	}
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
	for _, change := range []string{"uid", "source", "path", "deleted"} {
		t.Run(change, func(t *testing.T) {
			g, task, _, source, downloads := newTestDirectHfSource(t)
			task.BaseModel.Spec.Storage.DownloadPolicy = nil
			live := task.BaseModel.DeepCopy()
			switch change {
			case "uid":
				live.UID = "replacement"
			case "source":
				live.Spec.Storage.StorageUri = stringPtr("hf://other/model@main")
			case "path":
				live.Spec.Storage.Path = stringPtr(filepath.Join(g.modelRootDir, "other"))
			}
			g.modelClient = omefake.NewSimpleClientset(live)
			if change == "deleted" {
				g.modelClient = omefake.NewSimpleClientset()
			}
			waiting, err := source.process(context.Background(), g, task, task.BaseModel.Spec, true)
			require.NoError(t, err)
			require.True(t, waiting)
			require.Zero(t, *downloads)
		})
	}
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
			_, err := g.kubeClient.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, UID: "node-uid", Labels: map[string]string{"kubernetes.io/hostname": g.configMapReconciler.nodeName}}}, metav1.CreateOptions{})
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
				live.UID = "replacement-uid"
				_, err := g.modelClient.OmeV1beta1().BaseModels(live.Namespace).Update(ctx, live, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			g.kubeClient.(*k8sfake.Clientset).PrependReactor("patch", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
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
			_, err := g.kubeClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, UID: "node-uid", Labels: map[string]string{"kubernetes.io/hostname": g.configMapReconciler.nodeName}}}, metav1.CreateOptions{})
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
	root := t.TempDir()
	path := filepath.Join(root, "model")
	require.NoError(t, os.Mkdir(path, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(path, "weights"), []byte("weights"), 0600))
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "customer", UID: "uid"}, Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("hf://org/model"), Path: &path}}}
	key := getModelID(model, nil)
	raw, err := json.Marshal(ModelEntry{Name: model.Name, ModelUID: model.UID, Status: ModelStatusReady})
	require.NoError(t, err)
	g := newGopherForProcessTask(makeConfigMap("node1", map[string]string{key: string(raw)}), map[string]string{constants.GetBaseModelLabel(model.Namespace, model.Name): "Ready"})
	g.modelRootDir = root
	g.modelVerificationLimiter = newVerificationLimiter(1)
	g.modelClient = omefake.NewSimpleClientset(model)
	g.kubeClient = g.configMapReconciler.kubeClient
	g.artifactRouting.known = true
	_, err = g.kubeClient.CoreV1().Nodes().Get(context.Background(), g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	root, err = canonicalHfArtifactStoreRoot(root)
	require.NoError(t, err)
	path, err = ownedArtifactPath(g.modelRootDir, path)
	require.NoError(t, err)
	g.modelRootDir = root
	model.Spec.Storage.Path = &path
	_, err = g.modelClient.OmeV1beta1().BaseModels(model.Namespace).Update(context.Background(), model, metav1.UpdateOptions{})
	require.NoError(t, err)
	return g, &GopherTask{TaskType: Download, BaseModel: model}, path
}

func directArtifactEntry(t *testing.T, g *Gopher, task *GopherTask) ModelEntry {
	t.Helper()
	cm, err := g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, task.ClusterBaseModel))
	require.NoError(t, err)
	return entry
}
