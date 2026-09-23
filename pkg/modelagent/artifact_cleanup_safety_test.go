package modelagent

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestSharedDeletionReceiptPreservesBoundConsumer(t *testing.T) {
	ctx := context.Background()
	g, task, input := newTestHfArtifactGopher(t)
	defer g.taskQueue.close()
	handler := g.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	parent, found, err := handler.repository.GetParentForChild(ctx, input.ChildModelKey)
	require.NoError(t, err)
	require.True(t, found)
	// Crash boundary: eviction committed its receipt but has not unlinked the child.
	_, err = handler.repository.removeModelReference(ctx, parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
	require.NoError(t, err)
	_, err = g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	_, err = g.kubeClient.CoreV1().Pods("default").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "surviving-consumer"},
		Spec: corev1.PodSpec{NodeName: g.configMapReconciler.nodeName,
			Volumes: []corev1.Volume{{Name: "model", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: input.ChildModelPath}}}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	used, err := g.pathHasLocalPodConsumers(ctx, input.Parent.LocalPath)
	require.NoError(t, err)
	require.True(t, used, "the live Pod is discoverable before unlink")
	// This is the cleanup entry point used by both HF and OCI restoration.
	handled, waiting, err := g.resumeHfArtifactChildDeletion(ctx, task, true)
	t.Logf("resume result: handled=%v waiting=%v err=%v", handled, waiting, err)
	assert.True(t, handler.files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath), "bound consumer must keep its child link")
	assert.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"), "bound consumer must keep parent bytes")
}

func TestSharedRepairProtectsConsumersFromOrdinarySibling(t *testing.T) {
	ctx := context.Background()
	g, task, input := newTestHfArtifactGopher(t)
	defer g.taskQueue.close()
	handler := g.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	task.TaskType = DownloadOverride
	_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	_, err = g.kubeClient.CoreV1().Pods("default").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "surviving-consumer"},
		Spec: corev1.PodSpec{NodeName: g.configMapReconciler.nodeName,
			Volumes: []corev1.Volume{{Name: "model", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: input.ChildModelPath}}}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	validate := func(string) (bool, error) { return false, nil }
	writes := 0
	download := func(path string) error {
		writes++
		return os.WriteFile(filepath.Join(path, "config.json"), []byte("rewritten"), 0600)
	}
	_, err = g.runHfArtifactDownload(ctx, task, input, true, validate, download)
	require.ErrorContains(t, err, "while a Pod uses")
	require.Zero(t, writes)
	parent, found, err := handler.repository.Get(ctx, input.Parent.Identity)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, HfArtifactStatusFailed, parent.Status)
	// A new ordinary sibling has no rehydration annotation but shares this parent.
	other := input
	other.ChildModelKey = "default.basemodel.model-2"
	other.ChildModelUID = "model-2-uid"
	other.ChildModelPath = filepath.Join(input.ModelStoreRoot, "model-2")
	seedTestChildModelEntry(t, handler.repository, other)
	otherTask := *task
	otherTask.BaseModel = task.BaseModel.DeepCopy()
	otherTask.BaseModel.Name = "model-2"
	otherTask.BaseModel.UID = other.ChildModelUID
	otherTask.BaseModel.Spec.Storage.Path = &other.ChildModelPath
	_, err = g.modelClient.OmeV1beta1().BaseModels(otherTask.BaseModel.Namespace).Create(ctx, otherTask.BaseModel, metav1.CreateOptions{})
	require.NoError(t, err)
	result, err := g.runHfArtifactDownload(ctx, &otherTask, other, true, validate, download)
	t.Logf("ordinary sibling: outcome=%v err=%v writes=%d", result.Outcome, err, writes)
	assert.Zero(t, writes, "ordinary sibling must honor the same local consumer protection")
}

func TestSharedEvictionRechecksWithdrawnIntentUnderLock(t *testing.T) {
	ctx := context.Background()
	g, oldTask, input := newTestHfArtifactGopher(t)
	defer g.taskQueue.close()
	handler := g.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	oldTask.TaskType = Evict
	oldTask.SharedArtifact = true
	oldTask.BaseModel.Annotations[constants.ModelArtifactResidencyAnnotation] = constants.ModelArtifactResidencyEvicted
	g.modelClient = omefake.NewSimpleClientset(oldTask.BaseModel)
	_, err := g.kubeClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, UID: "node-uid"}}, metav1.CreateOptions{})
	require.NoError(t, err)
	g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
	latest, _, err := g.prepareArtifactEviction(ctx, oldTask)
	require.NoError(t, err)
	require.NotNil(t, latest)
	require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: latest.BaseModel, ModelStateOnNode: Updating}))
	// Pause agent 1 after preflight, before acquiring the shared locks.
	// Agent 2 observes withdrawal, validates the parent, and completes restore-2.
	restore := *oldTask
	restore.TaskType = Download
	restore.BaseModel = oldTask.BaseModel.DeepCopy()
	delete(restore.BaseModel.Annotations, constants.ModelArtifactResidencyAnnotation)
	g.modelClient = omefake.NewSimpleClientset(restore.BaseModel)
	second := &Gopher{modelRootDir: g.modelRootDir, kubeClient: g.kubeClient, modelClient: g.modelClient,
		configMapReconciler: NewConfigMapReconciler(g.configMapReconciler.nodeName, g.configMapReconciler.namespace, g.kubeClient, g.logger),
		nodeLabelReconciler: NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger),
		baseModelLister:     g.baseModelLister, clusterBaseModelLister: g.clusterBaseModelLister,
		logger: g.logger, taskQueue: newGopherTaskQueue(), gopherChan: make(chan *GopherTask, 10)}
	defer second.taskQueue.close()
	result, err := second.runHfArtifactDownload(ctx, &restore, input, true, func(string) (bool, error) { return true, nil }, nil)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	require.NoError(t, second.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: restore.BaseModel, ModelStateOnNode: Ready}))
	published, err := second.finishDownloadStatus(ctx, &restore, &NodeLabelOp{BaseModel: restore.BaseModel, ModelStateOnNode: Ready})
	require.NoError(t, err)
	require.True(t, published)
	// No Pod is needed: withdrawn intent alone must fence the old eviction.
	// Resume precisely the shared-cleanup suffix of processArtifactEviction.
	handled, waiting, err := g.processSharedHfArtifactDelete(ctx, latest)
	t.Logf("old eviction after restore: handled=%v waiting=%v err=%v", handled, waiting, err)
	assert.True(t, handler.files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath), "withdrawn old task must not delete restored child")
	assert.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"), "completed restoration must keep its parent bytes")
}

func TestDirectEvictionReceiptSettlesPreviousPath(t *testing.T) {
	ctx := context.Background()
	g, task, oldPath := newDirectArtifactTestModel(t)
	seedDirectArtifactReceipt(t, g, task, ArtifactPendingEviction{ModelUID: task.BaseModel.UID, Path: oldPath})
	newPath := filepath.Join(g.modelRootDir, "new-path")
	task.BaseModel.Spec.Storage.Path = &newPath
	delete(task.BaseModel.Annotations, constants.ModelArtifactResidencyAnnotation)
	task.TaskType = Download
	_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	g.configMapReconciler = NewConfigMapReconciler(g.configMapReconciler.nodeName, g.configMapReconciler.namespace, g.kubeClient, g.logger)
	_, err = g.settleDirectArtifactEviction(ctx, task)
	require.NoError(t, err, "same UID's old-path receipt must be settled before downloading its new path")
	require.Nil(t, directArtifactEntry(t, g, task).ArtifactPendingEviction)
	require.NoDirExists(t, oldPath)
}

func TestDirectEvictionPreviousPathSafety(t *testing.T) {
	for _, scenario := range []string{"consumer", "old path lock", "stale source", "stale UID", "other model"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			g, task, oldPath := newDirectArtifactTestModel(t)
			seedDirectArtifactReceipt(t, g, task, ArtifactPendingEviction{ModelUID: task.BaseModel.UID, Path: oldPath})
			newPath := filepath.Join(g.modelRootDir, "new-path")
			task.BaseModel.Spec.Storage.Path = &newPath
			task.BaseModel.Annotations = nil
			task.TaskType = Download
			live := task.BaseModel.DeepCopy()
			switch scenario {
			case "consumer":
				_, err := g.kubeClient.CoreV1().Pods("default").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "consumer"}, Spec: corev1.PodSpec{
					NodeName: g.configMapReconciler.nodeName, Volumes: []corev1.Volume{{Name: "model", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: oldPath}}}},
				}}, metav1.CreateOptions{})
				require.NoError(t, err)
			case "old path lock":
				lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: g.modelRootDir, ChildModelPath: oldPath})
				require.NoError(t, err)
				require.True(t, acquired)
				defer lock.Close()
			case "stale source":
				live.Spec.Storage.StorageUri = stringPtr("hf://another/model@main")
			case "stale UID":
				live.UID = "replacement"
			}
			g.modelClient = omefake.NewSimpleClientset(live)
			if scenario == "other model" {
				other := live.DeepCopy()
				other.Name, other.UID = "other", "other"
				other.Spec.Storage.Path = &oldPath
				_, err := g.modelClient.OmeV1beta1().BaseModels(other.Namespace).Create(ctx, other, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			_, err := g.settleDirectArtifactEviction(ctx, task)
			require.Error(t, err)
			require.NotNil(t, directArtifactEntry(t, g, task).ArtifactPendingEviction)
			require.FileExists(t, filepath.Join(oldPath, "weights"))
		})
	}
}

func TestSharedEvictionRechecksChangedSourceAndUID(t *testing.T) {
	for _, change := range []string{"source", "uid", "annotation"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			g, task, input := newTestHfArtifactGopher(t)
			defer g.taskQueue.close()
			h := g.sharedHfArtifactHandler()
			require.NoError(t, runTestHfArtifactDownload(h, input))
			task.TaskType, task.SharedArtifact = Evict, true
			task.BaseModel.Annotations[constants.ModelArtifactResidencyAnnotation] = constants.ModelArtifactResidencyEvicted
			live := task.BaseModel.DeepCopy()
			switch change {
			case "source":
				live.Spec.Storage.StorageUri = stringPtr("hf://another/model@main")
			case "uid":
				live.UID = "replacement"
			case "annotation":
				live.Annotations["refresh"] = "new-request"
			}
			g.modelClient = omefake.NewSimpleClientset(live)
			_, waiting, err := g.processSharedHfArtifactDelete(ctx, task)
			require.True(t, waiting || err != nil)
			require.True(t, h.files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
			require.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"))
		})
	}
}
