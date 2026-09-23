package modelagent

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	modelslister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func newAckReplaySharedModel(t *testing.T) (*Gopher, *GopherTask, context.Context) {
	t.Helper()
	g, task, input := newTestHfArtifactGopher(t)
	setAckReplayLiveModel(t, g, task)
	ctx, release := withDirectArtifactDownloadOperation(context.Background())
	t.Cleanup(release)
	t.Cleanup(g.taskQueue.close)
	result, err := g.runHfArtifactDownload(ctx, task, input, true, nil, func(path string) error {
		return os.MkdirAll(path, 0755)
	})
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	task.SharedArtifact = true
	g.metrics = NewMetrics(prometheus.NewRegistry())
	g.samePathWaitDelay = time.Millisecond
	g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
	_, err = g.kubeClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: g.configMapReconciler.nodeName, UID: "node-uid",
	}}, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, g.configMapReconciler.InitializeNodeUID("node-uid"))
	require.NoError(t, g.nodeLabelReconciler.InitializeNodeUID("node-uid"))
	return g, task, ctx
}

func setAckReplayLiveModel(t *testing.T, g *Gopher, task *GopherTask) {
	t.Helper()
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, index.Add(task.BaseModel))
	g.baseModelLister = modelslister.NewBaseModelLister(index)
}

func TestArtifactAckRejectsOldNoRequestSharedCompletion(t *testing.T) {
	g, old, _ := newAckReplaySharedModel(t)
	attempt, finishOld, proceed, err := g.beginTask(old)
	require.NoError(t, err)
	require.True(t, proceed)
	defer finishOld(false)
	ctx, releaseOld := withDirectArtifactDownloadOperation(attempt)
	defer releaseOld()
	parent, found, err := g.sharedHfArtifactHandler().repository.GetParentForChild(ctx, getModelID(old.BaseModel, nil))
	require.NoError(t, err)
	require.True(t, found)
	input := g.hfArtifactInputForChild(old, parent)
	result, err := g.runHfArtifactDownload(ctx, old, input, true, nil, nil)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)

	current := &GopherTask{TaskType: Download, BaseModel: old.BaseModel.DeepCopy()}
	current.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-2"
	// Separate trackers and operation guards, sharing the real host file locks and API.
	other := &Gopher{
		modelRootDir: g.modelRootDir, kubeClient: g.kubeClient, logger: g.logger,
		configMapReconciler: g.configMapReconciler, nodeLabelReconciler: g.nodeLabelReconciler,
		clusterBaseModelLister: g.clusterBaseModelLister, gopherChan: make(chan *GopherTask, 10),
	}
	setAckReplayLiveModel(t, other, current)
	g.modelClient = other.modelClient // Keep the first agent's informer stale.
	newer, finishNew, proceed, err := other.beginTask(current)
	require.NoError(t, err)
	require.True(t, proceed, "the second agent has an independent task sequence")
	defer finishNew(false)
	newer, releaseNew := withDirectArtifactDownloadOperation(newer)
	defer releaseNew()
	result, err = other.runHfArtifactDownload(newer, current, input, true, func(path string) (bool, error) {
		info, err := os.Stat(path)
		return err == nil && info.IsDir(), err
	}, nil)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome, "host locks are released when the first shared handler returns")
	published, err := other.finishDownloadStatus(newer, current, &NodeLabelOp{BaseModel: current.BaseModel, ModelStateOnNode: Ready})
	require.NoError(t, err)
	require.True(t, published)
	assertAck := func() {
		t.Helper()
		node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
		require.NoError(t, err)
		cm, err := g.configMapReconciler.getConfigMap(ctx)
		require.NoError(t, err)
		require.True(t, modelRehydrationAcknowledged(current, node, cm), "old completion must not erase the current acknowledgement")
	}
	assertAck()
	skip, _, err := g.shouldSkipStaleDownloadTask(ctx, old)
	require.NoError(t, err)
	require.False(t, skip, "the stale informer alone cannot fence the first agent")
	published, err = g.finishDownloadStatus(ctx, old, &NodeLabelOp{BaseModel: old.BaseModel, ModelStateOnNode: Ready})
	require.NoError(t, err)
	assertAck()
	require.False(t, published)
	select {
	case retry := <-g.gopherChan:
		require.Same(t, old, retry)
	case <-time.After(time.Second):
		t.Fatal("rejected status publication was not requeued")
	}
}

func TestArtifactAckCurrentNoRequestSharedCompletion(t *testing.T) {
	g, task, ctx := newAckReplaySharedModel(t)
	published, err := g.finishDownloadStatus(ctx, task, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready})
	require.NoError(t, err)
	require.True(t, published)
}

func TestArtifactAckRejectsStaleNoRequestProgress(t *testing.T) {
	for _, source := range []string{"shared", "direct-hf", "direct-oci"} {
		for _, status := range []ModelStateOnNode{Updating, Failed} {
			t.Run(source+"/"+string(status), func(t *testing.T) {
				var g *Gopher
				var old *GopherTask
				if source == "shared" {
					g, old, _ = newAckReplaySharedModel(t)
				} else {
					g, old, _ = newDirectArtifactTestModel(t)
					old.TaskType = Download
					old.BaseModel.Annotations = map[string]string{}
					if source == "direct-oci" {
						old.BaseModel.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/models/o/model")
					}
					setAckReplayLiveModel(t, g, old)
				}
				current := &GopherTask{TaskType: Download, BaseModel: old.BaseModel.DeepCopy()}
				current.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-2"
				g.modelClient = omefake.NewSimpleClientset(current.BaseModel)
				ctx, release := withDirectArtifactDownloadOperation(context.Background())
				defer release()
				require.False(t, directArtifactDownloadOperationFromContext(ctx).sharedCompleted)
				require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: current.BaseModel, ModelStateOnNode: Ready}))
				skip, _, err := g.shouldSkipStaleDownloadTask(ctx, old)
				require.NoError(t, err)
				require.False(t, skip)
				err = g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: old.BaseModel, ModelStateOnNode: status})
				require.ErrorContains(t, err, "artifact request changed")
				node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				cm, err := g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				require.True(t, modelRehydrationAcknowledged(current, node, cm))
			})
		}
	}
}

func TestArtifactStatusPreservesUnsupportedLegacyWithoutLiveClient(t *testing.T) {
	for _, status := range []ModelStateOnNode{Updating, Ready, Failed} {
		t.Run(string(status), func(t *testing.T) {
			g, task, _ := newDirectArtifactTestModel(t)
			task.TaskType = Download
			task.BaseModel.Annotations = nil
			task.BaseModel.Spec.Storage.Path = stringPtr(t.TempDir())
			g.modelClient = nil
			ctx, release := withDirectArtifactDownloadOperation(context.Background())
			defer release()
			require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: status}))
		})
	}
}

func TestArtifactAckRejectsStaleSharedOverrideBeforeRepair(t *testing.T) {
	g, old, _ := newAckReplaySharedModel(t)
	old.TaskType = DownloadOverride
	old.BaseModel.Spec.Storage.Parameters = &map[string]string{"auth": "unsupported-test-auth"}
	setAckReplayLiveModel(t, g, old)
	current := &GopherTask{TaskType: Download, BaseModel: old.BaseModel.DeepCopy()}
	current.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-2"
	g.modelClient = omefake.NewSimpleClientset(current.BaseModel)
	ctx := context.Background()
	require.NoError(t, g.loadArtifactRouting(ctx))
	require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: current.BaseModel, ModelStateOnNode: Ready}))
	// The informer still admits this old override. An initial Updating rejection
	// must stop it before validation/repair can change the shared parent state.
	err := g.processTask(old)
	node, nodeErr := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, nodeErr)
	cm, cmErr := g.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, cmErr)
	require.True(t, modelRehydrationAcknowledged(current, node, cm))
	require.NoError(t, err, "stale task must defer before reaching the unsupported-auth validator")
	select {
	case retry := <-g.gopherChan:
		require.Same(t, old, retry)
	case <-time.After(time.Second):
		t.Fatal("rejected override was not requeued")
	}
}

func TestArtifactAckRejectsNoRequestChangeDuringSharedValidation(t *testing.T) {
	for _, change := range []string{"request", "source", "uid", "deletion"} {
		t.Run(change, func(t *testing.T) {
			g, task, ctx := newAckReplaySharedModel(t)
			require.Empty(t, artifactRehydrationID(task))
			require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
			task.TaskType = DownloadOverride
			key := getModelID(task.BaseModel, nil)
			handler := g.sharedHfArtifactHandler()
			parent, found, err := handler.repository.GetParentForChild(ctx, key)
			require.NoError(t, err)
			require.True(t, found)
			weights := filepath.Join(parent.LocalPath, "weights")
			require.NoError(t, os.WriteFile(weights, []byte("original"), 0600))
			markerPath := filepath.Join(parent.LocalPath, constants.HfArtifactReadyMarkerFileName)
			marker, err := os.ReadFile(markerPath)
			require.NoError(t, err)
			before, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)

			validations, downloads := 0, 0
			_, err = g.runHfArtifactDownload(ctx, task, g.hfArtifactInputForChild(task, parent), true, func(string) (bool, error) {
				validations++
				current := task.BaseModel.DeepCopy()
				switch change {
				case "request":
					current.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-2"
				case "source":
					current.Spec.Storage.StorageUri = stringPtr(*current.Spec.Storage.StorageUri + "/replacement")
				case "uid":
					current.UID = "replacement-uid"
				case "deletion":
					now := metav1.Now()
					current.DeletionTimestamp = &now
				}
				// Keep the informer stale while the live model changes during validation.
				_, updateErr := g.modelClient.OmeV1beta1().BaseModels(current.Namespace).Update(ctx, current, metav1.UpdateOptions{})
				require.NoError(t, updateErr)
				return false, nil
			}, func(string) error {
				downloads++
				return os.WriteFile(weights, []byte("repaired"), 0600)
			})
			require.Equal(t, 1, validations)
			require.Zero(t, downloads, "a superseded ordinary task must not repair shared files")
			require.ErrorContains(t, err, "shared artifact task is no longer current")
			contents, err := os.ReadFile(weights)
			require.NoError(t, err)
			require.Equal(t, "original", string(contents))
			contents, err = os.ReadFile(markerPath)
			require.NoError(t, err)
			require.Equal(t, marker, contents, "stale validation must not remove or replace the Ready marker")
			after, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			require.Equal(t, before.Data[key], after.Data[key], "stale validation must not start child repair")
		})
	}
}

func TestArtifactReplayPreservesQueuedOverrideValidation(t *testing.T) {
	for _, acknowledgedAtDispatch := range []bool{false, true} {
		name := "repair-missing-ack"
		if acknowledgedAtDispatch {
			name = "already-acknowledged"
		}
		t.Run(name, func(t *testing.T) {
			g, task, ctx := newAckReplaySharedModel(t)
			task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-1"
			setAckReplayLiveModel(t, g, task)
			require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
			changed := task.BaseModel.DeepCopy()
			changed.Annotations["refresh"] = "new"
			override := &GopherTask{TaskType: DownloadOverride, BaseModel: changed}
			setAckReplayLiveModel(t, g, override)
			node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			requestKey, err := constants.ArtifactReadyLabelKey(changed.UID)
			require.NoError(t, err)
			delete(node.Labels, requestKey)
			_, err = g.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			require.NoError(t, err)
			g.enqueueTask(override)
			scout := &Scout{
				baseModelLister: g.baseModelLister, clusterBaseModelLister: g.clusterBaseModelLister,
				kubeClient: g.kubeClient, nodeName: g.configMapReconciler.nodeName,
				configMapNamespace: g.configMapReconciler.namespace, gopherChan: g.gopherChan, logger: g.logger,
			}
			require.NoError(t, scout.reconcileArtifactRequests(ctx))
			replay := <-g.gopherChan
			require.True(t, replay.ArtifactRequestReplay)
			require.Equal(t, Download, replay.TaskType)
			g.enqueueTask(replay)
			if acknowledgedAtDispatch {
				require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: changed, ModelStateOnNode: Ready}))
			}
			first, ok := g.taskQueue.popHighPriority()
			require.True(t, ok)
			require.Same(t, replay, first)
			// No OCI client is configured: replay must remain cheap Ready reuse.
			require.NoError(t, g.processTask(first))
			ready, err := g.artifactRequestAlreadyReady(ctx, replay)
			require.NoError(t, err)
			require.True(t, ready)
			require.Empty(t, g.gopherChan)
			next, ok := g.taskQueue.popNormal()
			require.True(t, ok)
			require.Same(t, override, next)
			nextCtx, finish, proceed, err := g.beginTask(next)
			defer finish(false)
			require.NoError(t, err)
			require.True(t, proceed, "periodic replay must not retire a queued explicit refresh")
			needsValidation, err := g.artifactRequestNeedsValidation(nextCtx, next)
			require.NoError(t, err)
			require.False(t, needsValidation, "the explicit override, not the acknowledged request, must cause validation")
			parent, found, err := g.sharedHfArtifactHandler().repository.GetParentForChild(nextCtx, getModelID(changed, nil))
			require.NoError(t, err)
			require.True(t, found)
			validations := 0
			result, err := g.runHfArtifactDownload(nextCtx, next, g.hfArtifactInputForChild(next, parent), true, func(string) (bool, error) {
				validations++
				return true, nil
			}, nil)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome)
			require.Equal(t, 1, validations)
		})
	}
}
