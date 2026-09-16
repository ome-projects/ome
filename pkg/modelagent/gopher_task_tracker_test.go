package modelagent

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGopherTaskTrackerSequencesSurviveRequeue(t *testing.T) {
	var tracker gopherTaskTracker
	first := tracker.ensureSequence(0)
	require.NotZero(t, first)
	require.Equal(t, first, tracker.ensureSequence(first))
	require.Greater(t, tracker.ensureSequence(0), first)
	require.EqualValues(t, 100, tracker.ensureSequence(100))
	require.Equal(t, first, tracker.ensureSequence(first))
	require.EqualValues(t, 101, tracker.ensureSequence(0))
}

func TestGopherTaskTrackerBusyDownloadKeepsCancelHandle(t *testing.T) {
	var tracker gopherTaskTracker
	var firstCancelled, secondCancelled atomic.Int32
	first, outcome := tracker.beginDownload("model", 1, func() { firstCancelled.Add(1) })
	require.Equal(t, gopherTaskProceed, outcome)
	second, outcome := tracker.beginDownload("model", 2, func() { secondCancelled.Add(1) })
	require.Equal(t, gopherTaskWait, outcome)
	require.Nil(t, second)

	deletion, outcome := tracker.beginDelete("model", 3)
	require.Equal(t, gopherTaskWait, outcome)
	require.NotNil(t, deletion)
	require.EqualValues(t, 1, firstCancelled.Load())
	require.Zero(t, secondCancelled.Load())
	tracker.finishDelete(deletion, true)
	tracker.finishDownload(first)
	deletion, outcome = tracker.beginDelete("model", 3)
	require.Equal(t, gopherTaskProceed, outcome)
	tracker.finishDelete(deletion, false)
}

func TestGopherTaskTrackerDeleteWaitsForFullFinalization(t *testing.T) {
	var tracker gopherTaskTracker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	download, outcome := tracker.beginDownload("model", 1, cancel)
	require.Equal(t, gopherTaskProceed, outcome)
	deletion, outcome := tracker.beginDelete("model", 2)
	require.Equal(t, gopherTaskWait, outcome)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	tracker.finishDelete(deletion, true)

	// Cancellation or completed byte transfer is not full task finalization.
	deletion, outcome = tracker.beginDelete("model", 2)
	require.Equal(t, gopherTaskWait, outcome)
	tracker.finishDelete(deletion, true)
	newer, outcome := tracker.beginDownload("model", 3, func() {})
	require.Equal(t, gopherTaskWait, outcome)
	require.Nil(t, newer)

	tracker.finishDownload(download)
	deletion, outcome = tracker.beginDelete("model", 2)
	require.Equal(t, gopherTaskProceed, outcome)
	tracker.finishDelete(deletion, false)
	newer, outcome = tracker.beginDownload("model", 3, func() {})
	require.Equal(t, gopherTaskProceed, outcome)
	tracker.finishDownload(newer)
}

func TestGopherTaskTrackerRejectsDeleteOlderThanCompletedDownload(t *testing.T) {
	var tracker gopherTaskTracker
	deleteSequence := tracker.ensureSequence(0)
	downloadSequence := tracker.ensureSequence(0)
	download, outcome := tracker.beginDownload("model", downloadSequence, func() {})
	require.Equal(t, gopherTaskProceed, outcome)
	tracker.finishDownload(download)

	deletion, outcome := tracker.beginDelete("model", deleteSequence)
	require.Equal(t, gopherTaskStale, outcome)
	require.Nil(t, deletion)
	newer, outcome := tracker.beginDownload("model", tracker.ensureSequence(0), func() {})
	require.Equal(t, gopherTaskProceed, outcome, "stale deletion must not leave a barrier")
	tracker.finishDownload(newer)
}

func TestGopherTaskTrackerRejectsDeleteOlderThanActiveDownload(t *testing.T) {
	var tracker gopherTaskTracker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	download, outcome := tracker.beginDownload("model", 2, cancel)
	require.Equal(t, gopherTaskProceed, outcome)
	deletion, outcome := tracker.beginDelete("model", 1)
	require.Equal(t, gopherTaskStale, outcome)
	require.Nil(t, deletion)
	require.NoError(t, ctx.Err())
	tracker.finishDownload(download)
}

func TestGopherTaskTrackerSharedParentRequeueKeepsBarrier(t *testing.T) {
	var tracker gopherTaskTracker
	deletion, outcome := tracker.beginDelete("model", 2)
	require.Equal(t, gopherTaskProceed, outcome)
	tracker.finishDelete(deletion, true)

	_, outcome = tracker.beginDownload("model", 1, func() {})
	require.Equal(t, gopherTaskStale, outcome)
	_, outcome = tracker.beginDownload("model", 3, func() {})
	require.Equal(t, gopherTaskWait, outcome)
	deletion, outcome = tracker.beginDelete("model", 2)
	require.Equal(t, gopherTaskProceed, outcome)
	tracker.finishDelete(deletion, false)
	_, outcome = tracker.beginDownload("model", 1, func() {})
	require.Equal(t, gopherTaskStale, outcome)
	_, outcome = tracker.beginDelete("model", 2)
	require.Equal(t, gopherTaskStale, outcome)
	download, outcome := tracker.beginDownload("model", 3, func() {})
	require.Equal(t, gopherTaskProceed, outcome)
	tracker.finishDownload(download)
}

func TestGopherTaskTrackerTerminalWaitReleasesBarrier(t *testing.T) {
	var tracker gopherTaskTracker
	download, _ := tracker.beginDownload("model", 1, func() {})
	deletion, outcome := tracker.beginDelete("model", 2)
	require.Equal(t, gopherTaskWait, outcome)
	// Main abandons the delete when its bounded requeue budget is exhausted.
	tracker.finishDelete(deletion, false)
	_, outcome = tracker.beginDownload("model", 3, func() {})
	require.Equal(t, gopherTaskWait, outcome, "active finalization still owns the slot")
	tracker.finishDownload(download)
	download, outcome = tracker.beginDownload("model", 3, func() {})
	require.Equal(t, gopherTaskProceed, outcome)
	tracker.finishDownload(download)
}

func TestGopherTaskTrackerDeleteAttemptsAreExclusive(t *testing.T) {
	var tracker gopherTaskTracker
	first, outcome := tracker.beginDelete("model", 2)
	require.Equal(t, gopherTaskProceed, outcome)
	for _, sequence := range []uint64{2, 3} {
		second, result := tracker.beginDelete("model", sequence)
		require.Equal(t, gopherTaskWait, result)
		require.Nil(t, second)
		tracker.finishDelete(second, false)
	}
	_, outcome = tracker.beginDelete("model", 1)
	require.Equal(t, gopherTaskStale, outcome)
	tracker.finishDelete(first, false)
	second, outcome := tracker.beginDelete("model", 3)
	require.Equal(t, gopherTaskProceed, outcome)
	tracker.finishDelete(second, false)
}

func TestGopherTaskTrackerOldDeleteAttemptCannotFinishRetry(t *testing.T) {
	var tracker gopherTaskTracker
	first, _ := tracker.beginDelete("model", 1)
	tracker.finishDelete(first, true)
	retry, outcome := tracker.beginDelete("model", 1)
	require.Equal(t, gopherTaskProceed, outcome)
	require.NotSame(t, first, retry)
	tracker.finishDelete(first, false)
	_, outcome = tracker.beginDownload("model", 2, func() {})
	require.Equal(t, gopherTaskWait, outcome)
	tracker.finishDelete(retry, false)
	download, outcome := tracker.beginDownload("model", 2, func() {})
	require.Equal(t, gopherTaskProceed, outcome)
	tracker.finishDownload(download)
}

func TestGopherTaskTrackerOldDownloadAttemptCannotFinishRetry(t *testing.T) {
	var tracker gopherTaskTracker
	first, _ := tracker.beginDownload("model", 1, func() {})
	tracker.finishDownload(first)
	retry, outcome := tracker.beginDownload("model", 1, func() {})
	require.Equal(t, gopherTaskProceed, outcome)
	tracker.finishDownload(first)
	deletion, outcome := tracker.beginDelete("model", 2)
	require.Equal(t, gopherTaskWait, outcome)
	tracker.finishDelete(deletion, true)
	tracker.finishDownload(retry)
	deletion, outcome = tracker.beginDelete("model", 2)
	require.Equal(t, gopherTaskProceed, outcome)
	tracker.finishDelete(deletion, false)
}

func TestGopherTaskTrackerCancellationCanFinalizeWithoutLockDeadlock(t *testing.T) {
	var tracker gopherTaskTracker
	var download *gopherDownloadAttempt
	download, _ = tracker.beginDownload("model", 1, func() { tracker.finishDownload(download) })
	deletion, outcome := tracker.beginDelete("model", 2)
	require.Equal(t, gopherTaskWait, outcome)
	tracker.finishDelete(deletion, true)
	deletion, outcome = tracker.beginDelete("model", 2)
	require.Equal(t, gopherTaskProceed, outcome)
	tracker.finishDelete(deletion, false)
}

func TestGopherTaskTrackerConcurrentDownloadHasSingleOwner(t *testing.T) {
	var tracker gopherTaskTracker
	var workers sync.WaitGroup
	start := make(chan struct{})
	winners := make(chan *gopherDownloadAttempt, 8)
	for i := 0; i < cap(winners); i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			attempt, outcome := tracker.beginDownload("model", 1, func() {})
			if outcome == gopherTaskProceed {
				winners <- attempt
			}
		}()
	}
	close(start)
	workers.Wait()
	close(winners)
	require.Len(t, winners, 1)
	for winner := range winners {
		tracker.finishDownload(winner)
	}
}

func TestGopherTaskTrackerIsolatedUIDsAndInvalidInput(t *testing.T) {
	var tracker gopherTaskTracker
	for _, input := range []struct {
		uid      string
		sequence uint64
	}{{"", 1}, {"model", 0}} {
		attempt, outcome := tracker.beginDownload(input.uid, input.sequence, func() {})
		require.Nil(t, attempt)
		require.Equal(t, gopherTaskStale, outcome)
		deletion, outcome := tracker.beginDelete(input.uid, input.sequence)
		require.Nil(t, deletion)
		require.Equal(t, gopherTaskStale, outcome)
	}
	deletion, _ := tracker.beginDelete("old-uid", 2)
	download, outcome := tracker.beginDownload("new-uid", 1, func() {})
	require.Equal(t, gopherTaskProceed, outcome)
	tracker.finishDelete(deletion, false)
	tracker.finishDownload(download)
	tracker.finishDownload(nil)
	tracker.finishDelete(nil, false)
}
