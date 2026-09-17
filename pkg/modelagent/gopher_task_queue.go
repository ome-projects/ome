package modelagent

import (
	"sync"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

type gopherTaskQueue struct {
	mutex               sync.Mutex
	cond                *sync.Cond
	high                []*GopherTask
	normalDownload      []*GopherTask
	normalRevalidation  []*GopherTask
	pendingHigh         []*GopherTask
	pendingDownload     []*GopherTask
	pendingRevalidation []*GopherTask
	capacity            int
	closed              bool
}

type gopherTaskEnqueueResult struct {
	accepted bool
	deferred bool
}

type gopherTaskQueueLane int

const (
	gopherTaskQueueLaneHigh gopherTaskQueueLane = iota
	gopherTaskQueueLaneDownload
	gopherTaskQueueLaneRevalidation
)

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
	q.rebalanceLocked()
	q.cond.Broadcast()
}

func (q *gopherTaskQueue) enqueue(task *GopherTask) gopherTaskEnqueueResult {
	if task == nil {
		return gopherTaskEnqueueResult{}
	}
	q.mutex.Lock()
	defer q.mutex.Unlock()
	result := q.enqueueLocked(task)
	if result.accepted {
		q.cond.Broadcast()
	}
	return result
}

func (q *gopherTaskQueue) enqueueLocked(task *GopherTask) gopherTaskEnqueueResult {
	if q.closed {
		return gopherTaskEnqueueResult{}
	}
	if retained := q.retainedTaskLocked(task); retained != nil {
		return gopherTaskEnqueueResult{accepted: true, deferred: q.isPendingLocked(retained)}
	}
	q.removeSupersededLocked(task)
	if task.TaskType == Delete {
		// Deletion is a safety operation. Preserve its existing ability to exceed
		// the runnable limit and place it ahead of other high-priority work.
		q.high = append([]*GopherTask{task}, q.high...)
		return gopherTaskEnqueueResult{accepted: true}
	}

	q.appendPendingLocked(task, taskQueueLane(task))
	q.rebalanceLocked()
	return gopherTaskEnqueueResult{
		accepted: true,
		deferred: q.isPendingLocked(task),
	}
}

// retainedTaskLocked coalesces retries into the strongest queued intent without
// moving that intent to the back of its FIFO or replacing its wait deadline.
// Fresh informer observations still supersede old downloads, including when
// the user lowers priority or the controller removes serving demand.
func (q *gopherTaskQueue) retainedTaskLocked(incoming *GopherTask) *GopherTask {
	uid := getModelUID(incoming)
	if uid == "" || incoming.TaskType == Delete || isFreshModelDownloadTask(incoming) {
		return nil
	}
	selected := incoming
	retained := false
	for _, tasks := range [][]*GopherTask{
		q.high, q.normalDownload, q.normalRevalidation,
		q.pendingHigh, q.pendingDownload, q.pendingRevalidation,
	} {
		for _, queued := range tasks {
			if getModelUID(queued) == uid && continuationSupersededBy(queued, selected) {
				selected = queued
				retained = true
			}
		}
	}
	if !retained {
		return nil
	}
	return selected
}

func continuationSupersededBy(queued, incoming *GopherTask) bool {
	if queued.TaskType == Delete || incoming.TaskType == Delete {
		return queued.TaskType == Delete
	}
	if isFreshModelDownloadTask(queued) || isFreshModelDownloadTask(incoming) {
		return isFreshModelDownloadTask(queued)
	}
	if queuedLane, incomingLane := taskQueueLane(queued), taskQueueLane(incoming); queuedLane != incomingLane {
		return queuedLane < incomingLane
	}
	if queued.TaskType != incoming.TaskType {
		return queued.TaskType == DownloadOverride
	}
	return true
}

func (q *gopherTaskQueue) removeSupersededLocked(task *GopherTask) {
	if task.TaskType == Delete {
		q.high = removeTasksForModelUID(q.high, task)
		q.normalDownload = removeTasksForModelUID(q.normalDownload, task)
		q.normalRevalidation = removeTasksForModelUID(q.normalRevalidation, task)
		q.pendingHigh = removeTasksForModelUID(q.pendingHigh, task)
		q.pendingDownload = removeTasksForModelUID(q.pendingDownload, task)
		q.pendingRevalidation = removeTasksForModelUID(q.pendingRevalidation, task)
		return
	}
	// Only a fresh observation or stronger continuation reaches this point.
	// Coalesce across runnable and deferred lanes so displacement cannot leave
	// duplicate retries. Delete tasks and different object UIDs remain distinct.
	q.high = removeSupersededTasks(q.high, task)
	q.normalDownload = removeSupersededTasks(q.normalDownload, task)
	q.normalRevalidation = removeSupersededTasks(q.normalRevalidation, task)
	q.pendingHigh = removeSupersededTasks(q.pendingHigh, task)
	q.pendingDownload = removeSupersededTasks(q.pendingDownload, task)
	q.pendingRevalidation = removeSupersededTasks(q.pendingRevalidation, task)
}

func (q *gopherTaskQueue) hasCapacity() bool {
	return q.capacity <= 0 || q.runnableLenLocked() < q.capacity
}

func (q *gopherTaskQueue) appendPendingLocked(task *GopherTask, lane gopherTaskQueueLane) {
	switch lane {
	case gopherTaskQueueLaneHigh:
		q.pendingHigh = append(q.pendingHigh, task)
	case gopherTaskQueueLaneDownload:
		q.pendingDownload = append(q.pendingDownload, task)
	case gopherTaskQueueLaneRevalidation:
		q.pendingRevalidation = append(q.pendingRevalidation, task)
	}
}

func (q *gopherTaskQueue) rebalanceLocked() {
	if q.closed {
		return
	}
	for len(q.pendingHigh) > 0 {
		if !q.hasCapacity() && !q.deferLowestPriorityRunnableLocked() {
			break
		}
		q.high = append(q.high, q.pendingHigh[0])
		q.pendingHigh = q.pendingHigh[1:]
	}
	for q.hasCapacity() {
		switch {
		case len(q.pendingDownload) > 0:
			q.normalDownload = append(q.normalDownload, q.pendingDownload[0])
			q.pendingDownload = q.pendingDownload[1:]
		case len(q.pendingRevalidation) > 0:
			q.normalRevalidation = append(q.normalRevalidation, q.pendingRevalidation[0])
			q.pendingRevalidation = q.pendingRevalidation[1:]
		default:
			return
		}
	}
}

func (q *gopherTaskQueue) deferLowestPriorityRunnableLocked() bool {
	if len(q.normalRevalidation) > 0 {
		deferred := q.normalRevalidation[len(q.normalRevalidation)-1]
		q.normalRevalidation = q.normalRevalidation[:len(q.normalRevalidation)-1]
		q.pendingRevalidation = append(q.pendingRevalidation, deferred)
		return true
	}
	if len(q.normalDownload) > 0 {
		deferred := q.normalDownload[len(q.normalDownload)-1]
		q.normalDownload = q.normalDownload[:len(q.normalDownload)-1]
		q.pendingDownload = append(q.pendingDownload, deferred)
		return true
	}
	return false
}

func (q *gopherTaskQueue) isPendingLocked(target *GopherTask) bool {
	return containsTask(q.pendingHigh, target) ||
		containsTask(q.pendingDownload, target) ||
		containsTask(q.pendingRevalidation, target)
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
		q.rebalanceLocked()
		q.cond.Broadcast()
		return task, true
	}
	if len(q.normalRevalidation) > 0 {
		task := q.normalRevalidation[0]
		q.normalRevalidation = q.normalRevalidation[1:]
		q.rebalanceLocked()
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
		q.rebalanceLocked()
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
	return q.runnableLenLocked() + len(q.pendingHigh) +
		len(q.pendingDownload) + len(q.pendingRevalidation)
}

func (q *gopherTaskQueue) runnableLenLocked() int {
	return len(q.high) + len(q.normalDownload) + len(q.normalRevalidation)
}

func containsTask(tasks []*GopherTask, target *GopherTask) bool {
	for _, task := range tasks {
		if task == target {
			return true
		}
	}
	return false
}

func taskQueueLane(task *GopherTask) gopherTaskQueueLane {
	if isFreshModelDownloadTask(task) {
		switch effectiveTaskPriority(task) {
		case v1beta1.ModelDownloadPriorityHigh:
			return gopherTaskQueueLaneHigh
		case v1beta1.ModelDownloadPriorityBackground:
			return gopherTaskQueueLaneRevalidation
		default:
			return gopherTaskQueueLaneDownload
		}
	}
	if shouldUseHighPriorityQueue(task) {
		return gopherTaskQueueLaneHigh
	}
	if task.RevalidationReplay {
		return gopherTaskQueueLaneRevalidation
	}
	return gopherTaskQueueLaneDownload
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

func removeTasksForModelUID(tasks []*GopherTask, target *GopherTask) []*GopherTask {
	modelUID := getModelUID(target)
	if modelUID == "" {
		return tasks
	}
	kept := tasks[:0]
	for _, task := range tasks {
		if getModelUID(task) != modelUID {
			kept = append(kept, task)
		}
	}
	return kept
}
