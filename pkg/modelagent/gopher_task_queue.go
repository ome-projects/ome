package modelagent

import (
	"sync"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

type gopherTaskQueue struct {
	mutex                    sync.Mutex
	cond                     *sync.Cond
	high                     []*GopherTask
	urgentDownload           []*GopherTask
	normalDownload           []*GopherTask
	normalRevalidation       []*GopherTask
	pendingHigh              []*GopherTask
	pendingUrgent            []*GopherTask
	pendingDownload          []*GopherTask
	pendingRevalidation      []*GopherTask
	capacity                 int
	closed                   bool
	downloadSchedulingPolicy string
}

type gopherTaskEnqueueResult struct {
	accepted bool
	deferred bool
}

type gopherTaskQueueLane int

const (
	// Worker selection and download urgency are separate. Only the first lane
	// is consumed by cleanup/reuse workers; the others share download workers.
	gopherTaskQueueLaneHigh gopherTaskQueueLane = iota
	gopherTaskQueueLaneUrgentDownload
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
	if task.TaskType == Reprioritize {
		if q.downloadSchedulingPolicy != DownloadSchedulingPolicyFIFO {
			q.reprioritizeLocked(task)
		}
		return gopherTaskEnqueueResult{accepted: true}
	}
	if q.downloadSchedulingPolicy == DownloadSchedulingPolicyFIFO {
		// Normalize a copy: neither explicit priority nor persisted serving
		// demand may affect FIFO routing, coalescing, or reuse eligibility.
		copy := *task
		copy.DownloadPriority = v1beta1.ModelDownloadPriorityStandard
		task = &copy
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

	q.appendPendingLocked(task, q.taskQueueLane(task))
	q.rebalanceLocked()
	return gopherTaskEnqueueResult{
		accepted: true,
		deferred: q.isPendingLocked(task),
	}
}

// reprioritizeLocked changes existing downloads only. An absent UID is a no-op:
// active and completed downloads must not be restarted by a scheduling update.
func (q *gopherTaskQueue) reprioritizeLocked(update *GopherTask) {
	uid := getModelUID(update)
	if uid == "" {
		return
	}
	var moved []*GopherTask
	for _, tasks := range q.allQueuesLocked() {
		kept := (*tasks)[:0]
		for _, task := range *tasks {
			if getModelUID(task) == uid && (task.TaskType == Download || task.TaskType == DownloadOverride) {
				updated := *task
				updated.DownloadPriority = update.DownloadPriority
				if taskQueueLane(&updated) != taskQueueLane(task) {
					moved = append(moved, &updated)
					continue
				}
				task = &updated
			}
			kept = append(kept, task)
		}
		*tasks = kept
	}
	for _, task := range moved {
		q.appendPendingLocked(task, taskQueueLane(task))
	}
	q.rebalanceLocked()
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
	for _, tasks := range q.allQueuesLocked() {
		for _, queued := range *tasks {
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
	if queuedPriority, incomingPriority := downloadTaskLane(queued), downloadTaskLane(incoming); queuedPriority != incomingPriority {
		return queuedPriority < incomingPriority
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
	// Only a fresh observation or stronger continuation reaches this point.
	// Coalesce across runnable and deferred lanes so displacement cannot leave
	// duplicate retries. Delete tasks and different object UIDs remain distinct.
	for _, tasks := range q.allQueuesLocked() {
		if task.TaskType == Delete {
			*tasks = removeTasksForModelUID(*tasks, task)
		} else {
			*tasks = removeSupersededTasks(*tasks, task)
		}
	}
}

// These accessors require mutex to be held and return lanes in priority order.
func (q *gopherTaskQueue) runnableQueuesLocked() []*[]*GopherTask {
	return []*[]*GopherTask{&q.high, &q.urgentDownload, &q.normalDownload, &q.normalRevalidation}
}

func (q *gopherTaskQueue) pendingQueuesLocked() []*[]*GopherTask {
	return []*[]*GopherTask{&q.pendingHigh, &q.pendingUrgent, &q.pendingDownload, &q.pendingRevalidation}
}

func (q *gopherTaskQueue) allQueuesLocked() []*[]*GopherTask {
	return append(q.runnableQueuesLocked(), q.pendingQueuesLocked()...)
}

func (q *gopherTaskQueue) hasCapacity() bool {
	return q.capacity <= 0 || q.runnableLenLocked() < q.capacity
}

func (q *gopherTaskQueue) appendPendingLocked(task *GopherTask, lane gopherTaskQueueLane) {
	tasks := q.pendingQueuesLocked()[lane]
	*tasks = append(*tasks, task)
}

func (q *gopherTaskQueue) rebalanceLocked() {
	if q.closed {
		return
	}
	runnable := q.runnableQueuesLocked()
	for lane, pending := range q.pendingQueuesLocked() {
		for len(*pending) > 0 {
			if !q.hasCapacity() && !q.deferLowerPriorityRunnableLocked(lane) {
				break
			}
			*runnable[lane] = append(*runnable[lane], (*pending)[0])
			*pending = (*pending)[1:]
		}
	}
}

func (q *gopherTaskQueue) deferLowerPriorityRunnableLocked(incomingLane int) bool {
	runnable, pending := q.runnableQueuesLocked(), q.pendingQueuesLocked()
	for lane := len(runnable) - 1; lane > incomingLane; lane-- {
		tasks := runnable[lane]
		if len(*tasks) > 0 {
			deferred := (*tasks)[len(*tasks)-1]
			*tasks = (*tasks)[:len(*tasks)-1]
			// Displaced runnable work predates this lane's pending work.
			*pending[lane] = append([]*GopherTask{deferred}, *pending[lane]...)
			return true
		}
	}
	return false
}

func (q *gopherTaskQueue) isPendingLocked(target *GopherTask) bool {
	for _, tasks := range q.pendingQueuesLocked() {
		if containsTask(*tasks, target) {
			return true
		}
	}
	return false
}

func (q *gopherTaskQueue) popNormal() (*GopherTask, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	for len(q.urgentDownload) == 0 && len(q.normalDownload) == 0 && len(q.normalRevalidation) == 0 && !q.closed {
		q.cond.Wait()
	}
	for _, tasks := range q.runnableQueuesLocked()[1:] {
		if len(*tasks) > 0 {
			task := (*tasks)[0]
			*tasks = (*tasks)[1:]
			q.rebalanceLocked()
			q.cond.Broadcast()
			return task, true
		}
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
	count := 0
	for _, tasks := range q.allQueuesLocked() {
		count += len(*tasks)
	}
	return count
}

func (q *gopherTaskQueue) runnableLenLocked() int {
	return len(q.high) + len(q.urgentDownload) + len(q.normalDownload) + len(q.normalRevalidation)
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
	if shouldUseHighPriorityQueue(task) {
		return gopherTaskQueueLaneHigh
	}
	return downloadTaskLane(task)
}

func (q *gopherTaskQueue) taskQueueLane(task *GopherTask) gopherTaskQueueLane {
	if q.downloadSchedulingPolicy == DownloadSchedulingPolicyFIFO && !shouldUseHighPriorityQueue(task) {
		// Startup revalidation and Background work share the FIFO download
		// lane. Cleanup and local reuse still have their dedicated workers.
		return gopherTaskQueueLaneDownload
	}
	return taskQueueLane(task)
}

func downloadTaskLane(task *GopherTask) gopherTaskQueueLane {
	if task.RevalidationReplay {
		return gopherTaskQueueLaneRevalidation
	}
	switch effectiveTaskPriority(task) {
	case v1beta1.ModelDownloadPriorityHigh:
		return gopherTaskQueueLaneUrgentDownload
	case v1beta1.ModelDownloadPriorityBackground:
		return gopherTaskQueueLaneRevalidation
	default:
		return gopherTaskQueueLaneDownload
	}
}

func shouldUseHighPriorityQueue(task *GopherTask) bool {
	return task.TaskType == Delete ||
		(isObjectStorageDownloadTask(task) && effectiveTaskPriority(task) != v1beta1.ModelDownloadPriorityBackground) ||
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
