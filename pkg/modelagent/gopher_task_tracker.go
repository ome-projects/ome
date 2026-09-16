package modelagent

import (
	"context"
	"sync"
)

type gopherTaskStartResult string

const (
	gopherTaskProceed gopherTaskStartResult = "Proceed"
	gopherTaskWait    gopherTaskStartResult = "Wait"
	gopherTaskStale   gopherTaskStartResult = "Stale"
)

// gopherTaskTracker serializes task attempts for each model UID. Its zero
// value is ready for use. Main owns queueing, wait deadlines, and status writes.
// Sequence fences survive finished attempts until this coordinator is discarded.
type gopherTaskTracker struct {
	mutex        sync.Mutex
	nextSequence uint64
	models       map[string]*gopherModelTaskState
}

type gopherModelTaskState struct {
	download               *gopherDownloadAttempt
	legacyAttempts         map[*gopherLegacyAttempt]struct{}
	latestLegacyDownload   *gopherLegacyAttempt
	newestDownloadSequence uint64
	finishedDeleteSequence uint64
	deleteSequence         uint64
	deleteAttempt          *gopherDeleteAttempt
}

// Attempts identify individual attempts, not logical tasks. Even a retry of the
// same sequence gets a new attempt, fencing a delayed finalizer from that retry.
type gopherDownloadAttempt struct {
	modelUID string
	sequence uint64
	cancel   context.CancelFunc
}

type gopherDeleteAttempt struct {
	modelUID string
	sequence uint64
}

type gopherLegacyAttempt struct {
	modelUID string
	cancel   context.CancelFunc
}

// ensureSequence is called before enqueue. Keep the returned sequence on the
// task across all demotions and requeues; zero requests a new logical task.
func (tracker *gopherTaskTracker) ensureSequence(existing uint64) uint64 {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	if existing > tracker.nextSequence {
		tracker.nextSequence = existing
	}
	if existing != 0 {
		return existing
	}
	tracker.nextSequence++
	return tracker.nextSequence
}

// beginDownload must precede Updating writes. A Wait or Stale result owns no
// attempt and never replaces or cancels the active download's cancel function.
func (tracker *gopherTaskTracker) beginDownload(modelUID string, sequence uint64, cancel context.CancelFunc) (*gopherDownloadAttempt, gopherTaskStartResult) {
	if modelUID == "" || sequence == 0 {
		return nil, gopherTaskStale
	}
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	state := tracker.modelStateLocked(modelUID)
	if sequence <= state.finishedDeleteSequence || sequence < state.newestDownloadSequence {
		return nil, gopherTaskStale
	}
	if state.deleteSequence != 0 {
		if sequence <= state.deleteSequence {
			return nil, gopherTaskStale
		}
		return nil, gopherTaskWait
	}
	if state.download != nil || len(state.legacyAttempts) != 0 {
		return nil, gopherTaskWait
	}
	attempt := &gopherDownloadAttempt{modelUID: modelUID, sequence: sequence, cancel: cancel}
	state.download = attempt
	// Retain newer admitted intent even after completion (including failure).
	// An older delayed delete must never become eligible when the slot clears.
	state.newestDownloadSequence = sequence
	return attempt, gopherTaskProceed
}

// finishDownload releases the slot only after all progress workers, reference
// writes, and status finalization have finished, not merely after cancellation
// or byte transfer. It intentionally retains the newest download sequence.
func (tracker *gopherTaskTracker) finishDownload(attempt *gopherDownloadAttempt) {
	if attempt == nil {
		return
	}
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	if state := tracker.models[attempt.modelUID]; state != nil && state.download == attempt {
		state.download = nil
	}
}

// beginDelete reserves the barrier before cancelling older active work. Wait
// is nonblocking: callers use their bounded requeue policy, never a fixed sleep.
// A non-nil attempt must be finished even when the result is Wait. A nil attempt
// means another attempt owns the barrier, or this task is stale.
func (tracker *gopherTaskTracker) beginDelete(modelUID string, sequence uint64) (*gopherDeleteAttempt, gopherTaskStartResult) {
	if modelUID == "" || sequence == 0 {
		return nil, gopherTaskStale
	}
	tracker.mutex.Lock()
	state := tracker.modelStateLocked(modelUID)
	if sequence <= state.newestDownloadSequence || sequence <= state.finishedDeleteSequence ||
		(state.deleteSequence != 0 && sequence < state.deleteSequence) {
		tracker.mutex.Unlock()
		return nil, gopherTaskStale
	}
	if state.deleteSequence != 0 && (sequence != state.deleteSequence || state.deleteAttempt != nil) {
		tracker.mutex.Unlock()
		return nil, gopherTaskWait
	}
	attempt := &gopherDeleteAttempt{modelUID: modelUID, sequence: sequence}
	state.deleteSequence = sequence
	state.deleteAttempt = attempt
	active := state.download
	legacyActive := len(state.legacyAttempts) != 0
	tracker.mutex.Unlock()

	if active != nil {
		// Cancel outside the mutex: cancellation may trigger finalization that
		// immediately calls back into this coordinator.
		if active.cancel != nil {
			active.cancel()
		}
		return attempt, gopherTaskWait
	}
	if legacyActive {
		tracker.cancelLegacyDownload(modelUID)
		return attempt, gopherTaskWait
	}
	return attempt, gopherTaskProceed
}

// Legacy tasks retain their existing overlap and latest-download
// cancellation behavior. Tracking all attempts only guards a later transition
// to shared ownership; it does not serialize ordinary downloads.
func (tracker *gopherTaskTracker) beginLegacyTask(modelUID string, cancel context.CancelFunc) *gopherLegacyAttempt {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	state := tracker.modelStateLocked(modelUID)
	if state.legacyAttempts == nil {
		state.legacyAttempts = make(map[*gopherLegacyAttempt]struct{})
	}
	attempt := &gopherLegacyAttempt{modelUID: modelUID, cancel: cancel}
	state.legacyAttempts[attempt] = struct{}{}
	if cancel != nil {
		state.latestLegacyDownload = attempt
	}
	return attempt
}

func (tracker *gopherTaskTracker) finishLegacyTask(attempt *gopherLegacyAttempt) {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	state := tracker.models[attempt.modelUID]
	if state == nil {
		return
	}
	delete(state.legacyAttempts, attempt)
	if state.latestLegacyDownload == attempt {
		state.latestLegacyDownload = nil
	}
	if len(state.legacyAttempts) == 0 && state.newestDownloadSequence == 0 && state.deleteSequence == 0 && state.finishedDeleteSequence == 0 {
		delete(tracker.models, attempt.modelUID)
	}
}

func (tracker *gopherTaskTracker) cancelLegacyDownload(modelUID string) {
	tracker.mutex.Lock()
	state := tracker.models[modelUID]
	var cancel context.CancelFunc
	if state != nil && state.latestLegacyDownload != nil {
		cancel = state.latestLegacyDownload.cancel
	}
	tracker.mutex.Unlock()
	if cancel != nil {
		cancel()
	}
}

// finishDelete releases the attempt. Set keepBarrier only after successfully
// scheduling a retry, including shared-parent cleanup retries. On completion,
// error, shutdown, or exhausted wait budget, false releases the barrier and
// fences older download tasks. A later task can then acquire the model slot.
func (tracker *gopherTaskTracker) finishDelete(attempt *gopherDeleteAttempt, keepBarrier bool) {
	if attempt == nil {
		return
	}
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	state := tracker.models[attempt.modelUID]
	if state == nil || state.deleteAttempt != attempt {
		return
	}
	state.deleteAttempt = nil
	if keepBarrier {
		return
	}
	state.finishedDeleteSequence = attempt.sequence
	state.deleteSequence = 0
}

func (tracker *gopherTaskTracker) modelStateLocked(modelUID string) *gopherModelTaskState {
	if tracker.models == nil {
		tracker.models = make(map[string]*gopherModelTaskState)
	}
	state := tracker.models[modelUID]
	if state == nil {
		state = &gopherModelTaskState{}
		tracker.models[modelUID] = state
	}
	return state
}
