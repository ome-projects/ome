package modelagent

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestGopherTaskQueueUsesSeparateFIFOs(t *testing.T) {
	queue := newGopherTaskQueue()
	download1 := &GopherTask{
		TaskType: Download,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "download-1", Namespace: "service-ns", UID: "download-1-uid"},
			Spec: v1beta1.BaseModelSpec{
				Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("hf://repo/model-1")},
			},
		},
	}
	download2 := &GopherTask{
		TaskType: Download,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "download-2", Namespace: "service-ns", UID: "download-2-uid"},
			Spec: v1beta1.BaseModelSpec{
				Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("hf://repo/model-2")},
			},
		},
	}
	wait1 := &GopherTask{
		TaskType:              Download,
		SamePathWaitStartedAt: time.Now(),
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "wait-1", Namespace: "service-ns", UID: "wait-1-uid"},
		},
	}
	wait2 := &GopherTask{
		TaskType:              Download,
		SamePathWaitStartedAt: time.Now(),
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "wait-2", Namespace: "service-ns", UID: "wait-2-uid"},
		},
	}

	queue.enqueue(download1)
	queue.enqueue(wait1)
	queue.enqueue(download2)
	queue.enqueue(wait2)

	task, ok := queue.popNormal()
	require.True(t, ok)
	assert.Equal(t, "download-1", task.BaseModel.Name)
	task, ok = queue.popNormal()
	require.True(t, ok)
	assert.Equal(t, "download-2", task.BaseModel.Name)
	task, ok = queue.popHighPriority()
	require.True(t, ok)
	assert.Equal(t, "wait-1", task.BaseModel.Name)
	task, ok = queue.popHighPriority()
	require.True(t, ok)
	assert.Equal(t, "wait-2", task.BaseModel.Name)
}

func TestGopherTaskQueueRoutesObjectStorageDownloadToHighPriority(t *testing.T) {
	queue := newGopherTaskQueue()
	task := &GopherTask{
		TaskType: Download,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "oci-model", Namespace: "service-ns", UID: "oci-model-uid"},
			Spec: v1beta1.BaseModelSpec{
				Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("oci://n/ns/b/bucket/o/model")},
			},
		},
	}

	queue.enqueue(task)

	queued, ok := queue.popHighPriority()
	require.True(t, ok)
	assert.Equal(t, "oci-model", queued.BaseModel.Name)
}

func TestGopherTaskQueueKeepsDownloadOverrideNormal(t *testing.T) {
	queue := newGopherTaskQueue()
	task := &GopherTask{
		TaskType: DownloadOverride,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "oci-model", Namespace: "service-ns", UID: "oci-model-uid"},
			Spec: v1beta1.BaseModelSpec{
				Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("oci://n/ns/b/bucket/o/model")},
			},
		},
	}

	queue.enqueue(task)

	queued, ok := queue.popNormal()
	require.True(t, ok)
	assert.Equal(t, "oci-model", queued.BaseModel.Name)
}

func TestGopherTaskQueuePrioritizesNormalDownloadBeforeRevalidationReplay(t *testing.T) {
	queue := newGopherTaskQueue()
	validation := &GopherTask{
		TaskType:           Download,
		NormalPriorityOnly: true,
		RevalidationReplay: true,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "validation", Namespace: "service-ns", UID: "validation-uid"},
		},
	}
	download := &GopherTask{
		TaskType:           Download,
		NormalPriorityOnly: true,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "download", Namespace: "service-ns", UID: "download-uid"},
		},
	}

	queue.enqueue(validation)
	queue.enqueue(download)

	task, ok := queue.popNormal()
	require.True(t, ok)
	assert.Equal(t, "download", task.BaseModel.Name)
	task, ok = queue.popNormal()
	require.True(t, ok)
	assert.Equal(t, "validation", task.BaseModel.Name)
}

func TestGopherTaskQueueDeleteSupersedesPendingDownloadsForSameModel(t *testing.T) {
	queue := newGopherTaskQueue()
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "service-ns", UID: "model-uid"},
	}

	queue.enqueue(&GopherTask{TaskType: Download, BaseModel: model})
	queue.enqueue(&GopherTask{TaskType: DownloadOverride, BaseModel: model})
	queue.enqueue(&GopherTask{TaskType: Delete, BaseModel: model})

	task, ok := queue.popHighPriority()
	require.True(t, ok)
	assert.Equal(t, Delete, task.TaskType)
	assert.Equal(t, 0, queue.len())
}

func TestGopherTaskQueueDeletePreemptsHighPriorityFIFO(t *testing.T) {
	queue := newGopherTaskQueue()
	wait := &GopherTask{
		TaskType:              Download,
		SamePathWaitStartedAt: time.Now(),
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "wait", Namespace: "service-ns", UID: "wait-uid"},
		},
	}
	deleteTask := &GopherTask{
		TaskType: Delete,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "delete", Namespace: "service-ns", UID: "delete-uid"},
		},
	}

	queue.enqueue(wait)
	queue.enqueue(deleteTask)

	task, ok := queue.popHighPriority()
	require.True(t, ok)
	assert.Equal(t, Delete, task.TaskType)
	task, ok = queue.popHighPriority()
	require.True(t, ok)
	assert.Equal(t, "wait", task.BaseModel.Name)
}

func TestGopherTaskQueuePopHighPriorityOnly(t *testing.T) {
	queue := newGopherTaskQueue()
	download := &GopherTask{
		TaskType: Download,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "download", Namespace: "service-ns", UID: "download-uid"},
		},
	}
	wait := &GopherTask{
		TaskType:              Download,
		SamePathWaitStartedAt: time.Now(),
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "wait", Namespace: "service-ns", UID: "wait-uid"},
		},
	}

	queue.enqueue(download)
	queue.enqueue(wait)

	task, ok := queue.popHighPriority()
	require.True(t, ok)
	assert.Equal(t, "wait", task.BaseModel.Name)
	assert.Equal(t, 1, queue.len())

	task, ok = queue.popNormal()
	require.True(t, ok)
	assert.Equal(t, "download", task.BaseModel.Name)
}

func TestGopherTaskQueueDemotedSamePathWaitUsesNormalQueue(t *testing.T) {
	queue := newGopherTaskQueue()
	demoted := &GopherTask{
		TaskType:              Download,
		SamePathWaitStartedAt: time.Now(),
		NormalPriorityOnly:    true,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "demoted", Namespace: "service-ns", UID: "demoted-uid"},
		},
	}
	deleteTask := &GopherTask{
		TaskType: Delete,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "delete", Namespace: "service-ns", UID: "delete-uid"},
		},
	}

	queue.enqueue(demoted)
	queue.enqueue(deleteTask)

	task, ok := queue.popHighPriority()
	require.True(t, ok)
	assert.Equal(t, Delete, task.TaskType)
	task, ok = queue.popNormal()
	require.True(t, ok)
	assert.Equal(t, "demoted", task.BaseModel.Name)
}

func TestGopherTaskQueueDeleteSupersedesPendingRevalidationReplayForSameModel(t *testing.T) {
	queue := newGopherTaskQueue()
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "service-ns", UID: "model-uid"},
	}

	queue.enqueue(&GopherTask{
		TaskType:           Download,
		BaseModel:          model,
		NormalPriorityOnly: true,
		RevalidationReplay: true,
	})
	queue.enqueue(&GopherTask{TaskType: Delete, BaseModel: model})

	task, ok := queue.popHighPriority()
	require.True(t, ok)
	assert.Equal(t, Delete, task.TaskType)
	assert.Equal(t, 0, queue.len())
}

func TestGopherTaskQueueEnqueueWakesMatchingBlockedWorker(t *testing.T) {
	queue := newGopherTaskQueue()
	normalDone := make(chan struct{})
	highDone := make(chan *GopherTask, 1)

	go func() {
		if _, ok := queue.popNormal(); ok {
			close(normalDone)
		}
	}()
	go func() {
		task, ok := queue.popHighPriority()
		if ok {
			highDone <- task
		}
	}()

	time.Sleep(10 * time.Millisecond)
	waitTask := &GopherTask{
		TaskType:              Download,
		SamePathWaitStartedAt: time.Now(),
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "wait", Namespace: "service-ns", UID: "wait-uid"},
		},
	}
	queue.enqueue(waitTask)

	select {
	case task := <-highDone:
		assert.Equal(t, "wait", task.BaseModel.Name)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected blocked high-priority worker to wake for high-priority task")
	}

	select {
	case <-normalDone:
		t.Fatal("normal worker should not consume high-priority task")
	default:
	}
	queue.close()
}

func TestGopherTaskQueueKeepsRepeatedEvictions(t *testing.T) {
	for _, changedMetadata := range []bool{false, true} {
		t.Run(fmt.Sprint(changedMetadata), func(t *testing.T) {
			s, first := newEvictionQueueTestGopher()
			second := *first
			second.BaseModel = first.BaseModel.DeepCopy()
			if changedMetadata {
				second.BaseModel.ResourceVersion = "newer"
				second.BaseModel.Annotations = map[string]string{"request": "newer"}
			}
			s.enqueueTask(first)
			s.enqueueTask(&second)
			require.Equal(t, 2, s.taskQueue.len(), "each eviction must retain its queued task")
			queued, ok := s.taskQueue.popHighPriority()
			require.True(t, ok)
			require.Same(t, first, queued)
			_, finish, admitted, err := s.beginTask(queued)
			require.NoError(t, err)
			require.True(t, admitted)
			finish(false)
			queued, ok = s.taskQueue.popHighPriority()
			require.True(t, ok)
			require.Same(t, &second, queued)
			_, finish, admitted, err = s.beginTask(queued)
			require.NoError(t, err)
			require.True(t, admitted, "later eviction must progress after earlier completion")
			finish(false)
		})
	}
}

func TestGopherTaskQueueRepeatedEvictionWaitsForActiveCleanup(t *testing.T) {
	s, first := newEvictionQueueTestGopher()
	s.enqueueTask(first)
	_, _ = s.taskQueue.popHighPriority()
	_, finishFirst, admitted, err := s.beginTask(first)
	require.NoError(t, err)
	require.True(t, admitted)
	second := *first
	second.Sequence = 0
	s.enqueueTask(&second)
	_, _ = s.taskQueue.popHighPriority()
	_, _, admitted, err = s.beginTask(&second)
	require.NoError(t, err)
	require.False(t, admitted, "worker must requeue while cleanup owns the model")
	finishFirst(false)
	retry := waitForEvictionQueueRetry(t, s)
	s.enqueueTask(retry)
	_, _ = s.taskQueue.popHighPriority()
	_, finishSecond, admitted, err := s.beginTask(retry)
	require.NoError(t, err)
	require.True(t, admitted)
	finishSecond(false)
}

func TestGopherTaskQueueDeleteSupersedesEvictionReservation(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprint(active), func(t *testing.T) {
			s, eviction := newEvictionQueueTestGopher()
			s.enqueueTask(eviction)
			var finishEviction func(bool)
			if active {
				_, _ = s.taskQueue.popHighPriority()
				_, finish, admitted, err := s.beginTask(eviction)
				require.NoError(t, err)
				require.True(t, admitted)
				finishEviction = finish
			}
			download := *eviction
			download.TaskType, download.Sequence = Download, 0
			s.enqueueTask(&download)
			deletion := *eviction
			deletion.TaskType, deletion.Sequence = Delete, 0
			s.enqueueTask(&deletion)
			require.Equal(t, 1, s.taskQueue.len(), "Delete removes queued eviction and download intent")
			queued, _ := s.taskQueue.popHighPriority()
			require.Same(t, &deletion, queued)
			_, finishDelete, admitted, err := s.beginTask(queued)
			require.NoError(t, err)
			if active {
				require.False(t, admitted, "Delete must wait for all active eviction cleanup")
				finishEviction(true) // Even a pending-cleanup retry must yield to Delete.
				retry := waitForEvictionQueueRetry(t, s)
				s.enqueueTask(retry)
				queued, _ = s.taskQueue.popHighPriority()
				_, finishDelete, admitted, err = s.beginTask(queued)
				require.NoError(t, err)
			}
			require.True(t, admitted, "Delete must not wait on a pruned eviction reservation")
			finishDelete(false)
			_, _, admitted, err = s.beginTask(eviction)
			require.NoError(t, err)
			require.False(t, admitted, "superseded eviction retries must remain stale")
			download.Sequence = 0
			s.enqueueTask(&download)
			queued, _ = s.taskQueue.popNormal()
			_, finishDownload, admitted, err := s.beginTask(queued)
			require.NoError(t, err)
			require.True(t, admitted, "later downloads must not inherit the old reservation")
			finishDownload(false)
		})
	}
}

func newEvictionQueueTestGopher() (*Gopher, *GopherTask) {
	gopher := &Gopher{
		artifactRouting: gopherArtifactRouting{known: true}, taskQueue: newGopherTaskQueue(),
		gopherChan: make(chan *GopherTask, 10), samePathWaitDelay: time.Millisecond, logger: zap.NewNop().Sugar(),
	}
	task := &GopherTask{TaskType: Evict, SharedArtifact: true, BaseModel: &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", UID: "model-uid"},
	}}
	return gopher, task
}

func waitForEvictionQueueRetry(t *testing.T, s *Gopher) *GopherTask {
	t.Helper()
	select {
	case retry := <-s.gopherChan:
		return retry
	case <-time.After(time.Second):
		t.Fatal("waiting task did not release its worker and requeue")
		return nil
	}
}
