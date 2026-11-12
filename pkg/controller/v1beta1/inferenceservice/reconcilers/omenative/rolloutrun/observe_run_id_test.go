package rolloutrun

import (
	"encoding/json"
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// The run id names one run in every event and in status.rollout. Two runs
// of one service never share it, whatever the clock does between the close
// of one run and the open of the next.

// rejectAndResume plays one rollback and one resume of an inline canary
// toward newRev with the clock at rest: the unit rejects the revision and
// the open run closes RolledBack; then the operator resumes, the canary
// layer clears the rejection and starts the unit again, and a run toward
// the same revision under the same plan opens in the same second. It
// returns the id of the run that closed and the id of the run that opened.
func rejectAndResume(t *testing.T, isvc *v1beta1.InferenceService) (closed, opened string) {
	t.Helper()
	closed = isvc.Status.Rollout.ActiveRun.RunID
	setEngineUnit(isvc, v1beta1.RolloutPhaseRolledBack, &v1beta1.CanaryStatus{
		CurrentStep:            1,
		RolledBackRevisionHash: hashOf(t, newRev),
		StableRevisionHash:     hashOf(t, oldRev),
	})
	if out := pass(t, isvc, divergedIR(oldRev, newRev)); !out.StateChanged || v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("the rollback must close the run: %+v", out)
	}
	if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunRolledBack {
		t.Fatalf("lastRun = %+v, want RolledBack", last)
	}
	setEngineUnit(isvc, v1beta1.RolloutPhasePending, &v1beta1.CanaryStatus{})
	if out := pass(t, isvc, divergedIR(oldRev, newRev)); !out.Opened {
		t.Fatalf("the resume must open a run toward the revision again: %+v", out)
	}
	return closed, isvc.Status.Rollout.ActiveRun.RunID
}

func TestARunOpenedInTheSecondItsPredecessorClosedCarriesADifferentID(t *testing.T) {
	isvc, _ := openInlineCanary(t)
	first, second := rejectAndResume(t, isvc)
	if second == first {
		t.Fatalf("the run opened on the resume carries the id of the run the rollback closed: %s", first)
	}
	// A second rollback and resume in the same second: the three runs are
	// three names.
	if _, third := rejectAndResume(t, isvc); third == first || third == second {
		t.Fatalf("the third run shares an id with an earlier one: %s %s %s", first, second, third)
	}
}

func TestARunIDIsMadeOnceAndTheCloseRecordsIt(t *testing.T) {
	isvc, ir := openInlineCanary(t)
	id := isvc.Status.Rollout.ActiveRun.RunID
	// Every pass builds a fresh client over the status the last pass left,
	// as a manager that restarted would; a later pass reads the id from
	// status and makes no second one.
	if out := passAt(t, isvc, time.Unix(1060, 0), ir); out.Opened || isvc.Status.Rollout.ActiveRun.RunID != id {
		t.Fatalf("a pass over the open run changed its id: %+v %s -> %s", out, id, isvc.Status.Rollout.ActiveRun.RunID)
	}
	closed, opened := rejectAndResume(t, isvc)
	if closed != id {
		t.Fatalf("the run closed under id %s, want %s", closed, id)
	}
	if last := isvc.Status.Rollout.LastRun; last.RunID != id {
		t.Fatalf("lastRun.runID = %q, want the closed run's id %s", last.RunID, id)
	}
	// The record survives the API round trip, so the next open folds it
	// after a restart as well as within one pass.
	raw, err := json.Marshal(isvc.Status.Rollout)
	if err != nil {
		t.Fatal(err)
	}
	var stored v1beta1.RolloutStatus
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.LastRun.RunID != id || stored.ActiveRun.RunID != opened {
		t.Fatalf("stored run ids = %q (closed) %q (open), want %q %q", stored.LastRun.RunID, stored.ActiveRun.RunID, id, opened)
	}
}
