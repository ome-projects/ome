package modelagent

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	modelslister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestGopherHfArtifactCustomRootPreservesReservedChild(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	s.modelRootDir = t.TempDir()
	handler := s.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	second := input
	second.ChildModelKey = "default.basemodel.model-2"
	second.ChildModelUID = "uid-2"
	second.ChildModelPath = filepath.Join(input.ModelStoreRoot, "model-2")
	seedTestChildModelEntry(t, handler.repository, second)
	require.NoError(t, runTestHfArtifactDownload(handler, second))
	task.TaskType = Delete
	task.BaseModel.Labels = map[string]string{constants.ReserveModelArtifact: "true"}
	_, waiting, err := s.processSharedHfArtifactDelete(context.Background(), task)
	require.NoError(t, err)
	require.False(t, waiting)
	task.BaseModel = task.BaseModel.DeepCopy()
	task.BaseModel.Name = "model-2"
	task.BaseModel.UID = second.ChildModelUID
	task.BaseModel.Labels = nil
	task.BaseModel.Spec.Storage.Path = &second.ChildModelPath
	_, waiting, err = s.processSharedHfArtifactDelete(context.Background(), task)
	require.NoError(t, err)
	require.False(t, waiting)
	assert.DirExists(t, input.Parent.LocalPath)
	assert.True(t, handler.files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
}

func TestGopherHfArtifactReadyStatusRequeuesDuringSiblingOperation(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	handler := s.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	client := s.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	_, err := client.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: s.configMapReconciler.nodeName}}, metav1.CreateOptions{})
	require.NoError(t, err)
	s.nodeLabelReconciler = NewNodeLabelReconciler(s.configMapReconciler.nodeName, client, 1, s.logger)
	s.samePathWaitDelay = time.Millisecond
	unlock, acquired := handler.tryParentOperation(input.Parent.Key)
	require.True(t, acquired)
	op := &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}
	require.NoError(t, s.finishDownloadStatus(task, op))
	unlock()
	select {
	case retry := <-s.gopherChan:
		require.NoError(t, s.finishDownloadStatus(retry, op))
	case <-time.After(time.Second):
		t.Fatal("completed child was not requeued for its Ready update")
	}
}

func TestGopherDeletePreflightErrorReleasesBarrier(t *testing.T) {
	s, task, _ := newTestHfArtifactGopher(t)
	task.TaskType = Delete
	task.SharedArtifact = true
	uri := "invalid://model"
	task.BaseModel.Spec.Storage.StorageUri = &uri
	// No lister entry is required for delete. Avoid label writes on the error
	// path by using a fixture with the node present.
	cm, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	withNode := newGopherForProcessTask(cm)
	withNode.logger = s.logger
	task.Sequence = withNode.taskTracker.ensureSequence(0)
	attempt, _ := withNode.taskTracker.beginDelete(gopherTaskModelKey(task), task.Sequence)
	withNode.taskTracker.finishDelete(attempt, true)
	require.Error(t, withNode.processTask(task))
	next := withNode.taskTracker.ensureSequence(0)
	download, result := withNode.taskTracker.beginDownload(gopherTaskModelKey(task), next, func() {})
	assert.Equal(t, gopherTaskProceed, result)
	withNode.taskTracker.finishDownload(download)
}

func TestGopherHfArtifactSourceTransitionClearsSharedReference(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	handler := s.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	uri := "hf://org/model"
	task.BaseModel.Spec.Storage.StorageUri = &uri
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	waiting, err := s.detachHfArtifactForDefaultDownload(context.Background(), task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	require.False(t, waiting)
	_, found, err := handler.repository.GetParentForChild(context.Background(), input.ChildModelKey)
	require.NoError(t, err)
	assert.False(t, found)
	assertChildPathMissing(t, input.ChildModelPath)
	require.NoError(t, os.MkdirAll(input.ChildModelPath, 0o755))
	task.TaskType = Delete
	handled, _, err := s.processSharedHfArtifactDelete(context.Background(), task)
	require.NoError(t, err)
	assert.False(t, handled, "ordinary HF files must use ordinary deletion")
}

func newTestHfArtifactGopher(t *testing.T) (*Gopher, *GopherTask, hfArtifactTaskInput) {
	t.Helper()
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	uri := "oci://n/ns/b/models/o/artifacts/" + input.Parent.Identity.ModelID + "/" + input.Parent.Identity.CommitSHA
	policy := v1beta1.ReuseIfExists
	task := &GopherTask{TaskType: Download, BaseModel: &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model-1", Namespace: "default", UID: input.ChildModelUID,
			Annotations: map[string]string{hfModelIDAnnotationKey: input.Parent.Identity.ModelID, hfSHAAnnotationKey: input.Parent.Identity.CommitSHA}},
		Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: &uri, Path: &input.ChildModelPath, DownloadPolicy: &policy}},
	}}
	gopher := &Gopher{configMapReconciler: repository.configMaps, modelRootDir: input.ModelStoreRoot,
		logger: zap.NewNop().Sugar(), gopherChan: make(chan *GopherTask, 10), taskQueue: newGopherTaskQueue(),
		baseModelLister:        modelslister.NewBaseModelLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})),
		clusterBaseModelLister: modelslister.NewClusterBaseModelLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{}))}
	return gopher, task, input
}

func TestGopherHfArtifactReadyReusePreservesReferenceWithoutParsing(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	handler := s.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	handled, waiting, err := s.processHfOCIArtifact(context.Background(), task, task.BaseModel.Spec, false)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.False(t, waiting)
	parent, found, err := handler.repository.GetParentForChild(context.Background(), input.ChildModelKey)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, input.Parent.Key, parent.Key)
	assert.True(t, handler.files.IsChildLinkedToParent(input.ChildModelPath, parent.LocalPath))
}

func TestGopherHfArtifactMissingParentDemotesWithoutAcquiring(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	result, err := s.runHfArtifactDownload(context.Background(), task, input, false, nil, func(string) error {
		t.Fatal("high-priority worker must not download")
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assert.True(t, task.NormalPriorityOnly)
	_, found, err := s.sharedHfArtifactHandler().repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestGopherHfArtifactNormalDownloadRepairsFailedParent(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	handler := s.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	parent, acquired, err := handler.repository.TryAcquireLockForRepair(context.Background(), input.Parent)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, handler.repository.MarkFailed(context.Background(), parent))
	validations := 0
	result, err := s.runHfArtifactDownload(context.Background(), task, input, true, func(string) (bool, error) {
		validations++
		return true, nil
	}, func(string) error {
		t.Fatal("healthy files need validation, not another download")
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assert.Equal(t, 1, validations)
}

func TestGopherHfArtifactKeepsLegacyDirectory(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	require.NoError(t, os.MkdirAll(input.ChildModelPath, 0o755))
	file := filepath.Join(input.ChildModelPath, "weights")
	require.NoError(t, os.WriteFile(file, []byte("legacy"), 0o644))
	result, err := s.runHfArtifactDownload(context.Background(), task, input, true, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskUseDefaultDownload, result.Outcome)
	assert.FileExists(t, file)
}

func TestGopherHfArtifactWaitTimeoutDoesNotFallback(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	s.samePathWaitTimeout = time.Millisecond
	task.SamePathWaitStartedAt = time.Now().Add(-time.Second)
	err := s.requeueHfArtifactTask(task, newHfArtifactRetryResult(input.Parent.Key, nil))
	assert.ErrorContains(t, err, "retry budget exhausted")
}

func TestGopherHfArtifactDeleteUsesStoredReferenceAfterGateRemoval(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	handler := s.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	task.TaskType = Delete
	task.BaseModel.Annotations = nil
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	handled, waiting, err := s.processSharedHfArtifactDelete(context.Background(), task)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.False(t, waiting)
	assert.NoDirExists(t, input.Parent.LocalPath)
	assertChildPathMissing(t, input.ChildModelPath)
}

func TestGopherHfArtifactStatusWaitsForParentRepair(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	handler := s.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	unlock, acquired := handler.tryParentOperation(input.Parent.Key)
	require.True(t, acquired)
	for _, status := range []ModelStateOnNode{Ready, Updating} {
		_, err := s.lockHfChildStatus(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: status})
		assert.ErrorContains(t, err, "active operation")
	}
	unlock()
	parent, acquired, err := handler.repository.TryAcquireLockForRepair(context.Background(), input.Parent)
	require.NoError(t, err)
	require.True(t, acquired)
	_, err = handler.repository.MarkChildrenFailedForRepair(context.Background(), parent)
	require.NoError(t, err)
	for _, status := range []ModelStateOnNode{Ready, Updating} {
		_, err := s.lockHfChildStatus(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: status})
		assert.ErrorContains(t, err, "not ready")
	}
}

func TestGopherHfArtifactDoesNotShareAcrossUnrelatedRoots(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	handler := s.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	other := testHfArtifactTaskInput(t, t.TempDir(), "model-2")
	seedTestChildModelEntry(t, handler.repository, other)
	result, err := s.runHfArtifactDownload(context.Background(), task, other, true, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskUseDefaultDownload, result.Outcome)
	assertChildPathMissing(t, other.ChildModelPath)
	assert.DirExists(t, input.Parent.LocalPath)
}

func TestGopherHfArtifactPolicyChangeDetachesOnlyOnNormalWorker(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	handler := s.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	handled, waiting, err := s.processHfOCIArtifact(context.Background(), task, task.BaseModel.Spec, false)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.True(t, waiting)
	assert.True(t, handler.files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
	handled, waiting, err = s.processHfOCIArtifact(context.Background(), task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	assert.False(t, handled, "normal worker now uses the default download")
	assert.False(t, waiting)
	assertChildPathMissing(t, input.ChildModelPath)
	assert.NoDirExists(t, input.Parent.LocalPath)
}

func TestGopherHfArtifactReserveLabelPreservesFilesWithoutReference(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	handler := s.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	task.TaskType = Delete
	task.BaseModel.Labels = map[string]string{constants.ReserveModelArtifact: "true"}
	handled, waiting, err := s.processSharedHfArtifactDelete(context.Background(), task)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.False(t, waiting)
	assert.True(t, handler.files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
	parent, found, err := handler.repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	require.True(t, found)
	assert.Empty(t, parent.Children)
	assert.Empty(t, parent.LockID)
	assert.Equal(t, HfArtifactStatusReady, parent.Status)
}

func TestGopherHfArtifactLegacyPathDoesNotEnableReuse(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	task.BaseModel.Annotations = nil
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	handled, waiting, err := s.processHfOCIArtifact(context.Background(), task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	assert.False(t, handled)
	assert.False(t, waiting)
	_, found, err := s.sharedHfArtifactHandler().repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestGopherHfArtifactWillNotOverwriteUnrecordedSharedSymlink(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	require.NoError(t, writeTestHfArtifactFiles(input.Parent.LocalPath))
	require.NoError(t, s.sharedHfArtifactHandler().files.CreateChildSymlink(input.ChildModelPath, input.Parent.LocalPath))
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	handled, _, err := s.processHfOCIArtifact(context.Background(), task, task.BaseModel.Spec, true)
	assert.True(t, handled)
	assert.ErrorContains(t, err, "no persisted child reference")
	assert.True(t, s.sharedHfArtifactHandler().files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
}
