package modelagent

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	"sigs.k8s.io/ome/pkg/xet"
)

func isolationTestTask(name, uri string, priority v1beta1.ModelDownloadPriority, taskType GopherTaskType) *GopherTask {
	return &GopherTask{
		TaskType: taskType, DownloadPriority: priority,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "service-ns", UID: types.UID(name)},
			Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: &uri, DownloadPriority: &priority}},
		},
	}
}

func TestRemoteDownloadsUseDownloadWorkers(t *testing.T) {
	for _, uri := range []string{"hf://org/model", "oci://n/ns/b/bucket/o/model"} {
		for _, taskType := range []GopherTaskType{Download, DownloadOverride} {
			t.Run(uri+"/"+string(taskType), func(t *testing.T) {
				queue := newGopherTaskQueue()
				task := isolationTestTask("model", uri, v1beta1.ModelDownloadPriorityHigh, taskType)
				// OCI Download may probe for reuse first, but its fallback must
				// retain priority within the download pool, not return to cleanup.
				task.NormalPriorityOnly = taskType == Download && uri != "hf://org/model"
				queue.enqueue(task)
				queue.close()
				_, ok := queue.popHighPriority()
				require.False(t, ok, "a remote download must not occupy the cleanup pool")
				actual, ok := queue.popNormal()
				require.True(t, ok)
				assert.Same(t, task, actual)
			})
		}
	}
}

func TestDownloadWorkersPreserveFallbackPriority(t *testing.T) {
	queue := newGopherTaskQueue(1)
	priorities := []v1beta1.ModelDownloadPriority{
		v1beta1.ModelDownloadPriorityBackground,
		v1beta1.ModelDownloadPriorityStandard,
		v1beta1.ModelDownloadPriorityHigh,
	}
	for _, priority := range priorities {
		task := isolationTestTask(string(priority), "hf://org/model", priority, Download)
		task.NormalPriorityOnly = true
		queue.enqueue(task)
	}
	for i := len(priorities) - 1; i >= 0; i-- {
		actual, ok := queue.popNormal()
		require.True(t, ok)
		assert.Equal(t, priorities[i], actual.DownloadPriority)
	}
	assert.Zero(t, queue.len())
	queue.close()
}

func TestCleanupWorkerDefersEveryRemoteDownloadPath(t *testing.T) {
	originalFetch := fetchAttributeFromHfModelMetaData
	fetchAttributeFromHfModelMetaData = func(context.Context, string, string) (interface{}, error) {
		panic("cleanup worker reached Hugging Face network access")
	}
	t.Cleanup(func() { fetchAttributeFromHfModelMetaData = originalFetch })
	for _, kind := range []string{"BaseModel", "ClusterBaseModel"} {
		for _, uri := range []string{"hf://org/model", "oci://n/ns/b/bucket/o/model"} {
			for _, taskType := range []GopherTaskType{Download, DownloadOverride} {
				t.Run(kind+"/"+uri+"/"+string(taskType), func(t *testing.T) {
					task := isolationTestTask("model", uri, v1beta1.ModelDownloadPriorityHigh, taskType)
					task.BaseModel.Spec.Storage.Path = stringPtr(filepath.Join(t.TempDir(), "weights"))
					g := newGopherForProcessTask(makeConfigMap("node-1", map[string]string{}))
					g.taskQueue = newGopherTaskQueue()
					g.downloadRetry = 1
					g.objectStorageDownload = func(context.Context, *ociobjectstore.ObjectURI, string, *GopherTask) error {
						panic("cleanup worker reached OCI network download")
					}
					if kind == "ClusterBaseModel" {
						task.ClusterBaseModel = &v1beta1.ClusterBaseModel{ObjectMeta: task.BaseModel.ObjectMeta, Spec: task.BaseModel.Spec}
						task.BaseModel = nil
						g.clusterBaseModelLister = &mockClusterBaseModelLister{models: []*v1beta1.ClusterBaseModel{task.ClusterBaseModel}}
					} else {
						g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel}}
					}
					// Both backend stubs fail before any network request. Execution
					// must hand off before reaching either download implementation.
					require.NotPanics(t, func() { require.NoError(t, g.processTaskWithOptions(task, false)) })
					require.Equal(t, 1, g.taskQueue.len())
					g.taskQueue.close()
					_, ok := g.taskQueue.popHighPriority()
					require.False(t, ok)
					actual, ok := g.taskQueue.popNormal()
					require.True(t, ok)
					assert.Same(t, task, actual)
					assert.Equal(t, v1beta1.ModelDownloadPriorityHigh, actual.DownloadPriority)
					assert.Equal(t, taskType, actual.TaskType)
				})
			}
		}
	}
}

func TestBlockedHuggingFaceDownloadDoesNotBlockUnrelatedDelete(t *testing.T) {
	originalFetch := fetchAttributeFromHfModelMetaData
	fetchAttributeFromHfModelMetaData = func(context.Context, string, string) (interface{}, error) { return "sha", nil }
	t.Cleanup(func() { fetchAttributeFromHfModelMetaData = originalFetch })

	download := isolationTestTask("wanted", "hf://org/wanted", v1beta1.ModelDownloadPriorityHigh, Download)
	download.BaseModel.Spec.Storage.Path = stringPtr(filepath.Join(t.TempDir(), "download"))
	deleted := isolationTestTask("obsolete", "oci://n/ns/b/bucket/o/obsolete", v1beta1.ModelDownloadPriorityStandard, Delete)
	artifact := filepath.Join(t.TempDir(), "obsolete")
	require.NoError(t, os.Mkdir(artifact, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(artifact, "weights"), []byte("obsolete weights"), 0600))
	deleted.BaseModel.Spec.Storage.Path = &artifact
	deletionTime := metav1.Now()
	deleted.BaseModel.DeletionTimestamp = &deletionTime

	g := newGopherForProcessTask(makeConfigMap("node-1", map[string]string{}))
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{download.BaseModel, deleted.BaseModel}}
	g.clusterBaseModelLister = &mockClusterBaseModelLister{}
	g.metrics = NewMetrics(prometheus.NewRegistry())
	g.xetConfig = &xet.Config{}
	g.taskQueue = newGopherTaskQueue(1)
	started, release := make(chan struct{}), make(chan struct{})
	g.snapshotDownload = func(context.Context, *xet.DownloadConfig, xet.ProgressHandler, time.Duration) (string, error) {
		close(started)
		<-release
		return "", context.Canceled
	}
	downloadDone, cleanupDone := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		close(release)
		g.taskQueue.close()
		for _, done := range []chan struct{}{downloadDone, cleanupDone} {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("model-agent worker did not stop")
			}
		}
	})
	go func() { g.runWorker(); close(downloadDone) }()
	go func() { g.runHighPriorityWorker(); close(cleanupDone) }()
	g.enqueueTask(download)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("HF weight download never started")
	}
	g.enqueueTask(deleted)
	require.Eventually(t, func() bool {
		_, err := os.Stat(artifact)
		return os.IsNotExist(err)
	}, 5*time.Second, 10*time.Millisecond, "cleanup must finish while the unrelated HF transfer remains blocked")
}

func TestDownloadPriorityDisplacementRetainsFIFO(t *testing.T) {
	queue := newGopherTaskQueue(2)
	for _, name := range []string{"one", "two", "three", "four"} {
		queue.enqueue(isolationTestTask(name, "hf://org/model", v1beta1.ModelDownloadPriorityBackground, Download))
	}
	for _, name := range []string{"urgent-one", "urgent-two"} {
		queue.enqueue(isolationTestTask(name, "hf://org/model", v1beta1.ModelDownloadPriorityHigh, Download))
	}
	require.Empty(t, queue.high, "download urgency must not use cleanup workers")
	for _, name := range []string{"urgent-one", "urgent-two", "one", "two", "three", "four"} {
		task, ok := queue.popNormal()
		require.True(t, ok)
		assert.Equal(t, name, task.BaseModel.Name)
	}
	assert.Zero(t, queue.len())
	queue.close()
}

func TestDownloadFallbackPrioritySurvivesReuseRetry(t *testing.T) {
	for _, retryFirst := range []bool{false, true} {
		queue := newGopherTaskQueue()
		urgent := isolationTestTask("model", "oci://n/ns/b/bucket/o/model", v1beta1.ModelDownloadPriorityHigh, Download)
		urgent.NormalPriorityOnly = true
		retry := isolationTestTask("model", "oci://n/ns/b/bucket/o/model", v1beta1.ModelDownloadPriorityStandard, Download)
		retry.SamePathWaitStartedAt = time.Now()
		if retryFirst {
			queue.enqueue(retry)
		}
		queue.enqueue(urgent)
		if !retryFirst {
			queue.enqueue(retry)
		}
		queue.close()
		_, ok := queue.popHighPriority()
		require.False(t, ok, "execution lane must not override download urgency when coalescing continuations")
		actual, ok := queue.popNormal()
		require.True(t, ok)
		assert.Same(t, urgent, actual)
		assert.Zero(t, queue.len())
	}
}
