package canary

import (
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rollout"
)

// eventWith reports whether one of the recorded events carries the reason
// and every fragment.
func eventWith(events []string, reason string, fragments ...string) bool {
	for _, ev := range events {
		if !strings.Contains(ev, reason) {
			continue
		}
		complete := true
		for _, f := range fragments {
			if !strings.Contains(ev, f) {
				complete = false
			}
		}
		if complete {
			return true
		}
	}
	return false
}

// Four engines, a two-step canary, the last step done: the release lets the
// one Instance kept on version 1 roll, and that Instance never comes back on
// version 2. The pass that projects the release clocks the cutover's wait.
// Inside the ready timeout the unit stays Promoting at partition 0; past it
// the unit stops Failed with the done mark kept, names the Instance, and
// leaves the traffic on version 2, the only revision with a serving pod.
func TestReconcile_CutoverStopsOnTheReadyTimeoutNamingTheInstance(t *testing.T) {
	isvc, in := fixtureDoneRolling(t)
	rec := record.NewFakeRecorder(16)
	in.Recorder = rec
	in.InstancesOffTarget = func() map[v1beta1.ComponentType][]string {
		return map[v1beta1.ComponentType][]string{v1beta1.EngineComponent: {"Instance 0 (Updating)"}}
	}
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.CapacityWaitSince != nil {
		t.Fatalf("the release pass clocks nothing, its projection lands on the next pass, got %v", cs.CapacityWaitSince)
	}

	// The pass that projects the release finds the Instance rolling and
	// clocks the wait.
	in.Now = in.Now.Add(in.Requeue)
	waitSince := in.Now
	res := mustReconcile(t, isvc, in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePromoting || res.Partition != 0 || cs.CapacityWaitSince == nil || !cs.CapacityWaitSince.Time.Equal(waitSince) {
		t.Fatalf("the first pass after the release clocks the wait and keeps partition 0, got phase=%q partition=%d clock=%v", phaseOf(isvc), res.Partition, cs.CapacityWaitSince)
	}

	in.Now = waitSince.Add(cellReadyTimeout - time.Minute)
	res = mustReconcile(t, isvc, in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePromoting || cs.Failed != nil || res.Partition != 0 {
		t.Fatalf("inside the ready timeout the cutover waits at partition 0, got phase=%q failed=%+v partition=%d", phaseOf(isvc), cs.Failed, res.Partition)
	}
	if cs.CapacityWaitSince == nil || !cs.CapacityWaitSince.Time.Equal(waitSince) {
		t.Fatalf("the wait keeps the clock the projection started at %v, got %v", waitSince, cs.CapacityWaitSince)
	}
	expectNoEvent(t, rec, EventReasonCanaryCutoverTimeout)

	in.Now = waitSince.Add(cellReadyTimeout + time.Second)
	res = mustReconcile(t, isvc, in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed || cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureCapacityTimeout {
		t.Fatalf("past the ready timeout the cutover stops Failed (CapacityTimeout), got phase=%q failed=%+v", phaseOf(isvc), cs.Failed)
	}
	if int(cs.CurrentStep) != len(isvc.Spec.Rollout.Groups[0].Canary.Steps) {
		t.Fatalf("the stop keeps the done mark, got step %d", cs.CurrentStep)
	}
	if res.Partition != in.DesiredReplicas || res.RequeueAfter != failedRequeue {
		t.Fatalf("the stop holds every Instance where it stands and waits at the stopped cadence, got %+v", res)
	}
	if got := revisionPercent(isvc, v1beta1.EngineComponent, "new"); got != 100 {
		t.Fatalf("the traffic stays on the new revision, the only one with a serving pod, got %d%%", got)
	}
	events := drainEvents(rec)
	if !eventWith(events, EventReasonCanaryCutoverTimeout, "Warning", "after its last step", "3 of 4 engine Instances", "revision new", cellReadyTimeout.String(), "engine: Instance 0 (Updating)") {
		t.Fatalf("the stop names the Instance that never came back, got %v", events)
	}
	if !eventWith(events, EventReasonCanaryRollHeld, "Warning", "CapacityTimeout") {
		t.Fatalf("the stop reports the held roll, got %v", events)
	}

	// The next pass reads the stop: it stands, with no second report.
	res = mustReconcile(t, isvc, in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed || !res.Active || res.RequeueAfter != failedRequeue {
		t.Fatalf("the stop stands at the stopped cadence, got phase=%q result=%+v", phaseOf(isvc), res)
	}
	expectNoEvent(t, rec, EventReasonCanaryCutoverTimeout)
	expectNoEvent(t, rec, EventReasonCanaryRollHeld)
}

// With no ready timeout configured anywhere the cutover waits as long as it
// takes: nothing in the code bounds it, and nothing stops the unit.
func TestReconcile_CutoverWaitsWithNoReadyTimeoutConfigured(t *testing.T) {
	isvc, in := fixtureDoneRolling(t)
	in.DefaultReadyTimeout = 0
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	for _, d := range []time.Duration{in.Requeue, 24 * time.Hour} {
		in.Now = in.Now.Add(d)
		res := mustReconcile(t, isvc, in)
		if phaseOf(isvc) != v1beta1.RolloutPhasePromoting || cs.Failed != nil || !res.Active || res.Partition != 0 {
			t.Fatalf("with no ready timeout the cutover waits at partition 0, got phase=%q failed=%+v result=%+v", phaseOf(isvc), cs.Failed, res)
		}
		if res.RequeueAfter == 0 && !res.Requeue {
			t.Fatalf("the wait asks to come back, got %+v", res)
		}
	}
}

// A cutover that lands inside the ready timeout completes as before: the
// promoted revisions are recorded, the unit reads Stable, the wait's clock is
// dropped with the record, and no stop is reported.
func TestReconcile_CutoverCompletingInsideTheReadyTimeoutClearsItsWait(t *testing.T) {
	isvc, in := fixtureDoneRolling(t)
	rec := record.NewFakeRecorder(16)
	in.Recorder = rec
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	in.Now = in.Now.Add(in.Requeue)
	mustReconcile(t, isvc, in) // the projection clocks the wait
	if cs.CapacityWaitSince == nil {
		t.Fatal("the pass after the release clocks the wait")
	}
	in.Now = in.Now.Add(cellReadyTimeout - time.Minute)
	in.PerRevisionPods = map[string]int32{"new": 4}
	res := mustReconcile(t, isvc, in)
	if !res.Complete || phaseOf(isvc) != v1beta1.RolloutPhaseStable || cs.Failed != nil {
		t.Fatalf("the cutover completes inside the timeout, got complete=%v phase=%q failed=%+v", res.Complete, phaseOf(isvc), cs.Failed)
	}
	if cs.CapacityWaitSince != nil {
		t.Fatalf("the completion drops the wait's clock, got %v", cs.CapacityWaitSince)
	}
	expectNoEvent(t, rec, EventReasonCanaryCutoverTimeout)
}

// On a prefill/decode unit the engine's cutover lands and the decoder's
// released Instance never comes back: the stop names the decoder's Instance
// and both members keep their traffic on the new revision.
func TestReconcile_PrefillDecodeCutoverStopNamesTheDecoderInstance(t *testing.T) {
	isvc, in := fixtureDoneRollingPD(t)
	rec := record.NewFakeRecorder(16)
	in.Recorder = rec
	evDecoderLastInstanceLeftShort(isvc, &in)
	in.InstancesOffTarget = func() map[v1beta1.ComponentType][]string {
		return map[v1beta1.ComponentType][]string{v1beta1.DecoderComponent: {"Instance 0 (Updating)"}}
	}
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	in.Now = in.Now.Add(in.Requeue)
	mustReconcile(t, isvc, in) // the projection clocks the wait
	if cs.CapacityWaitSince == nil {
		t.Fatal("the pass after the release clocks the wait for the released Instances")
	}
	in.Now = cs.CapacityWaitSince.Time.Add(cellReadyTimeout + time.Second)
	res := mustReconcile(t, isvc, in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed || cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureCapacityTimeout || res.Partition != in.DesiredReplicas {
		t.Fatalf("the decoder's released Instance past the ready timeout stops the unit, got phase=%q failed=%+v result=%+v", phaseOf(isvc), cs.Failed, res)
	}
	if got := revisionPercent(isvc, v1beta1.EngineComponent, "new"); got != 100 {
		t.Fatalf("the engine's traffic stays on the new revision, got %d%%", got)
	}
	if got := revisionPercent(isvc, v1beta1.DecoderComponent, "decnew"); got != 100 {
		t.Fatalf("the decoder's traffic stays on the new revision, got %d%%", got)
	}
	expectEvent(t, rec, EventReasonCanaryCutoverTimeout, "Warning", "4 of 4 engine Instances", "decoder: Instance 0 (Updating)")
}
