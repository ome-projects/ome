package modelagent

import (
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

func TestGopherTaskQueueHighPrioritySupersedesBackgroundForSameModel(t *testing.T) {
	queue := newGopherTaskQueue()
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "service-ns", UID: "model-uid"},
	}

	require.True(t, queue.enqueue(&GopherTask{
		TaskType:         Download,
		BaseModel:        model,
		DownloadPriority: v1beta1.ModelDownloadPriorityBackground,
	}).accepted)
	require.True(t, queue.enqueue(&GopherTask{
		TaskType:         Download,
		BaseModel:        model,
		DownloadPriority: v1beta1.ModelDownloadPriorityHigh,
	}).accepted)

	task, ok := queue.popHighPriority()
	require.True(t, ok)
	assert.Equal(t, v1beta1.ModelDownloadPriorityHigh, task.DownloadPriority)
	assert.Equal(t, 0, queue.len())
}

func TestGopherTaskQueueRejectsBackgroundWhenCapacityIsFull(t *testing.T) {
	queue := newGopherTaskQueue(1)
	first := &GopherTask{TaskType: Download, BaseModel: &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: "service-ns", UID: "first-uid"},
	}}
	second := &GopherTask{TaskType: Download, BaseModel: &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "second", Namespace: "service-ns", UID: "second-uid"},
	}}

	assert.True(t, queue.enqueue(first).accepted)
	assert.False(t, queue.enqueue(second).accepted)
	assert.Equal(t, 1, queue.len())
}

func TestGopherTaskQueueHighPriorityPreservesDisplacedBackgroundAtCapacity(t *testing.T) {
	queue := newGopherTaskQueue(1)
	background := &GopherTask{TaskType: Download, DownloadPriority: v1beta1.ModelDownloadPriorityBackground, BaseModel: &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "background", Namespace: "service-ns", UID: "background-uid"},
	}}
	demand := &GopherTask{TaskType: Download, DownloadPriority: v1beta1.ModelDownloadPriorityHigh, BaseModel: &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "demand", Namespace: "service-ns", UID: "demand-uid"},
	}}

	assert.True(t, queue.enqueue(background).accepted)
	result := queue.enqueue(demand)
	require.True(t, result.accepted)
	assert.Same(t, background, result.displaced)
	task, ok := queue.popHighPriority()
	require.True(t, ok)
	assert.Equal(t, "demand", task.BaseModel.Name)
	require.True(t, queue.enqueueWhenAvailable(result.displaced))
	task, ok = queue.popNormal()
	require.True(t, ok)
	assert.Equal(t, "background", task.BaseModel.Name)
	assert.Equal(t, 0, queue.len())
}

func TestGopherEnqueueTaskRetriesDisplacedWorkAfterCapacityIsAvailable(t *testing.T) {
	queue := newGopherTaskQueue(1)
	background := &GopherTask{TaskType: Download, DownloadPriority: v1beta1.ModelDownloadPriorityBackground, BaseModel: &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "background", Namespace: "service-ns", UID: "background-uid"},
	}}
	demand := &GopherTask{TaskType: Download, DownloadPriority: v1beta1.ModelDownloadPriorityHigh, BaseModel: &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "demand", Namespace: "service-ns", UID: "demand-uid"},
	}}
	require.True(t, queue.enqueue(background).accepted)
	gopher := &Gopher{taskQueue: queue, logger: zap.NewNop().Sugar()}

	enqueued := make(chan struct{})
	go func() {
		gopher.enqueueTask(demand)
		close(enqueued)
	}()

	task, ok := queue.popHighPriority()
	require.True(t, ok)
	assert.Equal(t, "demand", task.BaseModel.Name)
	select {
	case <-enqueued:
	case <-time.After(time.Second):
		t.Fatal("displaced work was not re-enqueued after capacity became available")
	}
	task, ok = queue.popNormal()
	require.True(t, ok)
	assert.Equal(t, "background", task.BaseModel.Name)
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
