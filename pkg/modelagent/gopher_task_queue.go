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
	closed             bool
	// Entries survive pop and delayed retries. Active counts also cover a
	// demoted retry starting before its previous worker returns.
	pending map[*GopherTask]*gopherPendingTask
}

type gopherPendingTask struct {
	active int
	queued bool
}

func newGopherTaskQueue() *gopherTaskQueue {
	queue := &gopherTaskQueue{pending: make(map[*GopherTask]*gopherPendingTask)}
	queue.cond = sync.NewCond(&queue.mutex)
	return queue
}

func (q *gopherTaskQueue) enqueue(task *GopherTask) {
	if task == nil {
		return
	}
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.closed {
		return
	}
	if _, exists := q.pending[task]; task.ArtifactReportRecovery && !exists && q.hasPendingModelTask(task, false) {
		return
	}
	if q.pending[task] == nil {
		q.pending[task] = &gopherPendingTask{}
	}
	q.pending[task].queued = true
	if task.TaskType == Delete || task.TaskType == Evict {
		// Delete preempts pending work for the same model and should run before
		// reuse-wait tasks, so it is the only non-FIFO insertion.
		q.high = q.pruneSupersededTasksLocked(q.high, task)
		q.normalDownload = q.pruneSupersededTasksLocked(q.normalDownload, task)
		q.normalRevalidation = q.pruneSupersededTasksLocked(q.normalRevalidation, task)
		if task.TaskType == Delete {
			q.high = append([]*GopherTask{task}, q.high...)
		} else {
			q.high = append(q.high, task)
		}
	} else if shouldUseHighPriorityQueue(task) {
		q.high = append(q.high, task)
	} else if task.RevalidationReplay {
		q.normalRevalidation = append(q.normalRevalidation, task)
	} else {
		q.normalDownload = append(q.normalDownload, task)
	}
	q.cond.Broadcast()
}

// Caller holds q.mutex. Explicit queued, active, and delayed work all outrank
// periodic recovery, even if a recovery task has a later sequence number.
func (q *gopherTaskQueue) hasPendingModelTask(task *GopherTask, explicitOnly bool) bool {
	for pending := range q.pending {
		if pending != task && getModelUID(pending) == getModelUID(task) && (!explicitOnly || !pending.ArtifactReportRecovery) {
			return true
		}
	}
	return false
}

func (q *gopherTaskQueue) startTask(task *GopherTask) bool {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.closed {
		return false
	}
	if task.ArtifactReportRecovery && q.hasPendingModelTask(task, true) {
		if pending := q.pending[task]; pending == nil || pending.active == 0 {
			delete(q.pending, task)
		} else {
			pending.queued = false
		}
		return false
	}
	if q.pending[task] == nil {
		q.pending[task] = &gopherPendingTask{}
	}
	q.pending[task].active++
	q.pending[task].queued = false
	return true
}

func (q *gopherTaskQueue) retainTask(task *GopherTask) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.closed {
		return
	}
	if q.pending[task] == nil {
		q.pending[task] = &gopherPendingTask{}
	}
	q.pending[task].queued = true
}

func (q *gopherTaskQueue) finishTask(task *GopherTask) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	pending := q.pending[task]
	if pending == nil {
		return
	}
	pending.active--
	if pending.active == 0 && !pending.queued {
		delete(q.pending, task)
	}
}

func (q *gopherTaskQueue) cancelRetainedTask(task *GopherTask) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if pending := q.pending[task]; pending != nil {
		pending.queued = false
		if pending.active == 0 {
			delete(q.pending, task)
		}
	}
}

// Caller holds q.mutex. Pruning also releases queued reservations.
func (q *gopherTaskQueue) pruneSupersededTasksLocked(tasks []*GopherTask, deletion *GopherTask) []*GopherTask {
	kept := tasks[:0]
	for _, task := range tasks {
		if !isTaskSupersededByDeletion(task, deletion) {
			kept = append(kept, task)
			continue
		}
		// Active and delayed attempts remain reserved until their own completion.
		if pending := q.pending[task]; pending != nil {
			pending.queued = false
			if pending.active == 0 {
				delete(q.pending, task)
			}
		}
	}
	return kept
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
		return task, true
	}
	if len(q.normalRevalidation) > 0 {
		task := q.normalRevalidation[0]
		q.normalRevalidation = q.normalRevalidation[1:]
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
		return task, true
	}
	return nil, false
}

func (q *gopherTaskQueue) close() {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.closed = true
	clear(q.pending)
	q.cond.Broadcast()
}

func (q *gopherTaskQueue) len() int {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return len(q.high) + len(q.normalDownload) + len(q.normalRevalidation)
}

func shouldUseHighPriorityQueue(task *GopherTask) bool {
	return task.TaskType == Delete || task.TaskType == Evict || isObjectStorageDownloadTask(task) || (!task.NormalPriorityOnly && !task.RevalidationReplay && !task.SamePathWaitStartedAt.IsZero())
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

func isTaskSupersededByDeletion(task, deletion *GopherTask) bool {
	modelUID := getModelUID(deletion)
	if modelUID == "" || task.TaskType == Delete || getModelUID(task) != modelUID {
		return false
	}
	// Repeated evictions retain their owner and run in sequence. Only Delete
	// supersedes queued eviction intent.
	if deletion.TaskType == Evict && task.TaskType == Evict {
		return false
	}
	// Direct and shared deletes preserve work admitted after their intent.
	return !usesArtifactTaskCoordinator(deletion) || deletion.Sequence == 0 || task.Sequence == 0 || task.Sequence <= deletion.Sequence
}
