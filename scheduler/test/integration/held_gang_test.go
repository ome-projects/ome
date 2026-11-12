package integration

import (
	"testing"
	"time"
)

// A gang the cluster cannot place yet is watched for holdWindow from the
// moment its pods exist. The scheduler tries each pod, finds the gang cannot
// form, and holds both until a registered event or a backoff says otherwise:
// a few attempts in all. A gang that cycles instead shows hundreds of attempts
// and status patches per second here, and loses a pod when the scheduler's
// queue pops a copy it still tracks as in flight.
const (
	holdWindow      = 4 * time.Second
	holdPollPeriod  = 50 * time.Millisecond
	maxHeldAttempts = 10
	maxHeldPatches  = 10
)

// watchHeldGang runs create, then samples the scheduler's work every
// holdPollPeriod for window. It returns the total since before create ran,
// with how long the scheduler kept working: the span from the first sample
// that moved to the last one that did.
func watchHeldGang(t *testing.T, patches *statusPatchCounter, window time.Duration, create func()) (total loopSample, busy time.Duration) {
	t.Helper()
	start := sampleLoop(t, patches)
	last := start
	began := time.Now()
	create()
	var firstMove, lastMove time.Time
	for time.Since(began) < window {
		time.Sleep(holdPollPeriod)
		now := sampleLoop(t, patches)
		if now != last {
			if firstMove.IsZero() {
				firstMove = time.Now()
			}
			lastMove = time.Now()
			last = now
		}
	}
	if !firstMove.IsZero() {
		busy = lastMove.Sub(firstMove) + holdPollPeriod
	}
	return last.minus(start), busy
}

// TestHeldGangWaitsForAChange: three gangs balance two racks 2/1 and fill rack
// b; a fourth gang's leader is vetoed in rack a by its spread constraint while
// rack b has no room. The worker takes rack a and waits at the gate, the leader
// is vetoed there, and the gang unwinds. Nothing in the cluster changes after
// that, so the scheduler must hold both pods after a few attempts, send a few
// status patches at most, and discard no pod.
func TestHeldGangWaitsForAChange(t *testing.T) {
	patches := &statusPatchCounter{}
	tc := startScheduler(t, patches.wrap(globalKubeConfig), gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "tsc-held"
	createNamespace(t, tc, ns)
	balanceThreeGangs(t, tc, ns, "th-")

	held, busy := watchHeldGang(t, patches, holdWindow, func() { createHeldGang(t, tc, ns) })
	t.Logf("held gang over %s, scheduler busy for %s: %s", holdWindow, busy.Round(time.Millisecond), held)
	if busy > 0 {
		t.Logf("while busy: %.0f attempts/s, %.0f status patches/s, %.0f gate waits/s, %.0f unwinds/s",
			held.attempts/busy.Seconds(), float64(held.patches)/busy.Seconds(), held.gateWaits/busy.Seconds(), held.unwinds/busy.Seconds())
	}
	ensureNotBound(t, tc, ns, "b3-0", 0)

	if held.attempts > maxHeldAttempts {
		t.Errorf("held gang was tried %.0f times in %s with nothing changed, want at most %d", held.attempts, holdWindow, maxHeldAttempts)
	}
	if held.patches > maxHeldPatches {
		t.Errorf("held gang caused %d pod status patches in %s with nothing changed, want at most %d", held.patches, holdWindow, maxHeldPatches)
	}
	if held.discards != 0 {
		t.Errorf("the scheduler discarded a held pod %d times in %s; such a pod is in no queue until it is deleted, recreated or the scheduler restarts",
			held.discards, holdWindow)
	}
}
