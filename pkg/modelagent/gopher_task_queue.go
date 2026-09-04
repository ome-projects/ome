package modelagent

import (
	"sync"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

type gopherTaskQueue struct {
	mutex              sync.Mutex
	cond               *sync.Cond
	high               []*GopherTask
	normalDownload     []*GopherTask
	normalRevalidation []*GopherTask
	capacity           int
	closed             bool
}

type gopherTaskEnqueueResult struct {
	accepted  bool
	displaced *GopherTask
}

const defaultGopherTaskQueueCapacity = 4096

func newGopherTaskQueue(capacity ...int) *gopherTaskQueue {
	configuredCapacity := defaultGopherTaskQueueCapacity
	if len(capacity) > 0 && capacity[0] > 0 {
		configuredCapacity = capacity[0]
	}
	queue := &gopherTaskQueue{capacity: configuredCapacity}
	queue.cond = sync.NewCond(&queue.mutex)
	return queue
}

func (q *gopherTaskQueue) setCapacity(capacity int) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if capacity <= 0 {
		capacity = defaultGopherTaskQueueCapacity
	}
	q.capacity = capacity
}

func (q *gopherTaskQueue) enqueue(task *GopherTask) gopherTaskEnqueueResult {
	if task == nil {
		return gopherTaskEnqueueResult{}
	}
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return q.enqueueLocked(task)
}

func (q *gopherTaskQueue) enqueueLocked(task *GopherTask) gopherTaskEnqueueResult {
	if q.closed {
		return gopherTaskEnqueueResult{}
	}
	var displaced *GopherTask
	if task.TaskType == Delete {
		// Delete preempts pending work for the same model and should run before
		// reuse-wait tasks, so it is the only non-FIFO insertion.
		q.high = removeSupersededTasks(q.high, task)
		q.normalDownload = removeSupersededTasks(q.normalDownload, task)
		q.normalRevalidation = removeSupersededTasks(q.normalRevalidation, task)
		q.high = append([]*GopherTask{task}, q.high...)
	} else if isFreshModelDownloadTask(task) {
		// A model status or spec update can change its effective priority. Keep
		// exactly one queued task for the model UID and move that task between
		// queues without affecting an active download.
		q.high = removeSupersededTasks(q.high, task)
		q.normalDownload = removeSupersededTasks(q.normalDownload, task)
		q.normalRevalidation = removeSupersededTasks(q.normalRevalidation, task)
		switch effectiveTaskPriority(task) {
		case v1beta1.ModelDownloadPriorityHigh:
			var roomAvailable bool
			displaced, roomAvailable = q.makeRoomForPriorityTask()
			if !roomAvailable {
				return gopherTaskEnqueueResult{}
			}
			q.high = append(q.high, task)
		case v1beta1.ModelDownloadPriorityBackground:
			if !q.hasCapacity() {
				return gopherTaskEnqueueResult{}
			}
			q.normalRevalidation = append(q.normalRevalidation, task)
		default:
			if !q.hasCapacity() {
				return gopherTaskEnqueueResult{}
			}
			q.normalDownload = append(q.normalDownload, task)
		}
	} else if shouldUseHighPriorityQueue(task) {
		if !q.hasCapacity() {
			return gopherTaskEnqueueResult{}
		}
		q.high = append(q.high, task)
	} else if task.RevalidationReplay {
		if !q.hasCapacity() {
			return gopherTaskEnqueueResult{}
		}
		q.normalRevalidation = append(q.normalRevalidation, task)
	} else {
		if !q.hasCapacity() {
			return gopherTaskEnqueueResult{}
		}
		q.normalDownload = append(q.normalDownload, task)
	}
	q.cond.Broadcast()
	return gopherTaskEnqueueResult{accepted: true, displaced: displaced}
}

func (q *gopherTaskQueue) enqueueWhenAvailable(task *GopherTask) bool {
	if task == nil {
		return true
	}
	q.mutex.Lock()
	defer q.mutex.Unlock()
	pending := task
	for !q.closed {
		result := q.enqueueLocked(pending)
		if result.accepted {
			if result.displaced == nil {
				return true
			}
			pending = result.displaced
			continue
		}
		q.cond.Wait()
	}
	return false
}

func (q *gopherTaskQueue) hasCapacity() bool {
	return q.capacity <= 0 || q.lenLocked() < q.capacity
}

func (q *gopherTaskQueue) makeRoomForPriorityTask() (*GopherTask, bool) {
	if q.hasCapacity() {
		return nil, true
	}
	if len(q.normalRevalidation) > 0 {
		displaced := q.normalRevalidation[len(q.normalRevalidation)-1]
		q.normalRevalidation = q.normalRevalidation[:len(q.normalRevalidation)-1]
		return displaced, true
	}
	if len(q.normalDownload) > 0 {
		displaced := q.normalDownload[len(q.normalDownload)-1]
		q.normalDownload = q.normalDownload[:len(q.normalDownload)-1]
		return displaced, true
	}
	return nil, false
}

func (q *gopherTaskQueue) popNormal() (*GopherTask, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	for len(q.normalDownload) == 0 && len(q.normalRevalidation) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.normalDownload) > 0 {
		task := q.normalDownload[0]
		q.normalDownload = q.normalDownload[1:]
		q.cond.Broadcast()
		return task, true
	}
	if len(q.normalRevalidation) > 0 {
		task := q.normalRevalidation[0]
		q.normalRevalidation = q.normalRevalidation[1:]
		q.cond.Broadcast()
		return task, true
	}
	return nil, false
}

func (q *gopherTaskQueue) popHighPriority() (*GopherTask, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	for len(q.high) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.high) > 0 {
		task := q.high[0]
		q.high = q.high[1:]
		q.cond.Broadcast()
		return task, true
	}
	return nil, false
}

func (q *gopherTaskQueue) close() {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.closed = true
	q.cond.Broadcast()
}

func (q *gopherTaskQueue) len() int {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return q.lenLocked()
}

func (q *gopherTaskQueue) lenLocked() int {
	return len(q.high) + len(q.normalDownload) + len(q.normalRevalidation)
}

func shouldUseHighPriorityQueue(task *GopherTask) bool {
	return task.TaskType == Delete ||
		isObjectStorageDownloadTask(task) ||
		(!task.NormalPriorityOnly && !task.SamePathWaitStartedAt.IsZero())
}

func isFreshModelDownloadTask(task *GopherTask) bool {
	return task != nil &&
		(task.TaskType == Download || task.TaskType == DownloadOverride) &&
		!task.NormalPriorityOnly &&
		!task.RevalidationReplay &&
		task.SamePathWaitStartedAt.IsZero()
}

func effectiveTaskPriority(task *GopherTask) v1beta1.ModelDownloadPriority {
	if task.DownloadPriority == v1beta1.ModelDownloadPriorityHigh {
		return v1beta1.ModelDownloadPriorityHigh
	}
	if task.DownloadPriority == v1beta1.ModelDownloadPriorityBackground {
		return v1beta1.ModelDownloadPriorityBackground
	}
	// Preserve the existing Object Storage fast path unless the model author
	// explicitly selected Background.
	if isObjectStorageDownloadTask(task) {
		return v1beta1.ModelDownloadPriorityHigh
	}
	return v1beta1.ModelDownloadPriorityStandard
}

func isObjectStorageDownloadTask(task *GopherTask) bool {
	if task == nil || task.TaskType != Download || task.NormalPriorityOnly || task.RevalidationReplay {
		return false
	}
	var storageSpec *v1beta1.StorageSpec
	if task.BaseModel != nil {
		storageSpec = task.BaseModel.Spec.Storage
	} else if task.ClusterBaseModel != nil {
		storageSpec = task.ClusterBaseModel.Spec.Storage
	}
	if storageSpec == nil || storageSpec.StorageUri == nil {
		return false
	}
	storageType, err := storage.GetStorageType(*storageSpec.StorageUri)
	return err == nil && storageType == storage.StorageTypeOCI
}

func removeSupersededTasks(tasks []*GopherTask, deleteTask *GopherTask) []*GopherTask {
	modelUID := getModelUID(deleteTask)
	if modelUID == "" {
		return tasks
	}
	kept := tasks[:0]
	for _, task := range tasks {
		if task.TaskType != Delete && getModelUID(task) == modelUID {
			continue
		}
		kept = append(kept, task)
	}
	return kept
}
