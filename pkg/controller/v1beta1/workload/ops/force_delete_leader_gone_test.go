package ops

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestForceDeleteOfATerminatingPod_AShortenedWorkerIsReadOnItsKubeletsCount:
// the worker of a gang shows 30 s of its 480 s, the grace a delete after its
// first one named. Its node is gone, the policy's slack is 10 s, and the
// replica sets lifecycle.leaderGoneGracePeriod to 30 s. 20 s past the
// deletion time the API server shows, the worker's kubelet may still be
// counting: the pod stays, and the next look is 40 s after that deletion
// time. 45 s past it the pod is removed, and the Warning says 15 s past the
// deadline, the time OME waited past it.
func TestForceDeleteOfATerminatingPod_AShortenedWorkerIsReadOnItsKubeletsCount(t *testing.T) {
	policy := &workload.ForceDeletePolicy{OverdueSlack: 10 * time.Second, NodeUnreachableThreshold: time.Minute}
	// look runs the force-delete on the worker as it stands past after its
	// deletion time, and returns the next look, the deletes and the events.
	look := func(past time.Duration) (time.Time, []recordedDeleteOpts, []string) {
		t.Helper()
		worker := endingIn(memberPod("svc-engine-0-worker-0", workload.RunnerWorker, 1), 30, -past)
		var deletes []recordedDeleteOpts
		funcs := fdDeleteRecorder(&deletes)
		rec := record.NewFakeRecorder(4)
		input := fdInput(fdISVC("svc"), policy)
		input.DesiredSpec.Lifecycle.LeaderGoneGracePeriod = &metav1.Duration{Duration: leaderGoneBound}
		deps := workload.Deps{Client: fdFakeClient(t, &funcs, fdStoredCopy(worker)), Recorder: rec}
		next, err := escalateStuckTerminatingWithDeadline(context.Background(), deps, input, worker, 0)
		if err != nil {
			t.Fatal(err)
		}
		return next, deletes, fdDrainEvents(rec)
	}

	next, deletes, events := look(20 * time.Second)
	if len(deletes) != 0 || len(events) != 0 {
		t.Fatalf("20 s past the deletion time: deletes %+v, events %v, want none while the kubelet may still be counting", deletes, events)
	}
	if want := fdNow.Add(20*time.Second + time.Nanosecond); !next.Equal(want) {
		t.Fatalf("the next look is %s from now, want %s: 40 s after the deletion time", next.Sub(fdNow), want.Sub(fdNow))
	}

	_, deletes, events = look(45 * time.Second)
	if len(deletes) != 1 || deletes[0].grace == nil || *deletes[0].grace != 0 {
		t.Fatalf("45 s past the deletion time: deletes %+v, want one with a grace of 0", deletes)
	}
	const waited = "node-gone, 15s past the pod's own deletion deadline"
	if len(events) != 1 || !strings.Contains(events[0], waited) {
		t.Fatalf("events %v, want one that says %q", events, waited)
	}
}
