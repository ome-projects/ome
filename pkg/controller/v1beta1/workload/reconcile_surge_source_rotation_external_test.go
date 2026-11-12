package workload_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// requireSourceServes fails unless the one pod of Instance idx on image is
// in rotation: the serving gate reads True and no surge drain holds it.
func (h *recoveryHarness) requireSourceServes(idx int32, image, when string) {
	h.t.Helper()
	pods := h.podsOfOnImage(idx, image)
	if len(pods) != 1 {
		h.dumpState(when)
		h.t.Fatalf("%s: %d pod(s) of Instance %d on %s, want the one source", when, len(pods), idx, image)
	}
	if !podreadiness.IsServing(pods[0]) || podreadiness.HeldNotServingBy(pods[0], podreadiness.WriterUpdateSurgeDrain) {
		h.dumpState(when)
		h.t.Fatalf("%s: the source %s is out of rotation: conditions=%+v", when, pods[0].Name, pods[0].Status.Conditions)
	}
}

// crashReplacement makes the one replacement of Instance 0 on image stay
// down in CrashLoopBackOff from the next kubelet pass on, and returns it.
func (h *recoveryHarness) crashReplacement(image string) *corev1.Pod {
	h.t.Helper()
	pods := h.podsOfOnImage(0, image)
	if len(pods) != 1 {
		h.dumpState("replacement to crash")
		h.t.Fatalf("%d replacement pod(s) on %s, want one", len(pods), image)
	}
	h.wedgePod(pods[0], "CrashLoopBackOff")
	return pods[0]
}

// A single-pod surge takes its source out of rotation once the replacement
// serves. The replacement then crashes and stays down past the grace, so
// the attempt ends and the retry schedule counts it. The source pod is
// healthy the whole time: the ending puts it back in rotation before the
// row is written Failed, the retry drains it again behind its own
// replacement, and when that one dies too and the schedule holds the
// revision, the source serves under the hold for as long as it stands.
// Nothing is rebuilt while the revision holds.
func TestSurgeAfterTakingOver_AReplacementThatKeepsDyingHandsTheSourceBackAtEveryEnding(t *testing.T) {
	h := surgeHandedOver(t)
	h.retryPolicy = &workloadtypes.RetryPolicy{MaxAttempts: 2, InitialDelay: 20 * time.Second, MaxDelay: time.Minute, Multiplier: 2}
	source := h.podsOfOnImage(0, goodImage)
	if len(source) != 1 {
		t.Fatalf("%d source pod(s), want one", len(source))
	}
	sourceKept := func() {
		if pods := h.podsOfOnImage(0, goodImage); len(pods) != 1 || pods[0].UID != source[0].UID {
			h.dumpState("source replaced")
			t.Fatalf("the source pod was deleted or rebuilt; it serves the Instance until a replacement takes over for good")
		}
	}
	firstReplacement := h.crashReplacement(fixedImage)

	ended := func(attempts int32) func() bool {
		return func() bool {
			s, b := h.instance(0), h.findBlock(h.revFixed.Name)
			return s != nil && s.Phase == workloadtypes.InstancePhaseFailed && s.Operation == nil &&
				b != nil && b.AttemptsStarted == attempts
		}
	}
	if !h.runWithInvariant(20, ended(1), sourceKept) {
		h.dumpState("first ending")
		t.Fatalf("the crashed replacement did not end the first attempt: %+v", h.instance(0))
	}
	h.requireSourceServes(0, goodImage, "after the first ending")

	// The retry removes the pod the first attempt died on, builds its own
	// replacement and, once that serves, takes the source out again.
	handedOverAgain := func() bool {
		s := h.instance(0)
		if s == nil || s.Operation == nil || s.Operation.Step != workloadtypes.UpdateStepSurgeDrain {
			return false
		}
		pods := h.podsOfOnImage(0, fixedImage)
		return len(pods) == 1 && pods[0].UID != firstReplacement.UID && podreadiness.IsServing(pods[0])
	}
	servedThroughTheWait := func() {
		sourceKept()
		if s := h.instance(0); s != nil && s.Operation == nil && h.instancesServingOn(goodImage) != 1 {
			h.dumpState("source out of rotation between attempts")
			t.Fatalf("the source left rotation while no attempt was open: %+v", s)
		}
	}
	if !h.runWithInvariant(30, handedOverAgain, servedThroughTheWait) {
		h.dumpState("second takeover")
		t.Fatalf("the retry never took over serving with its own replacement: %+v", h.instance(0))
	}
	if pods := h.podsOfOnImage(0, goodImage); len(pods) != 1 || podreadiness.IsServing(pods[0]) {
		t.Fatalf("the source is still in rotation behind the retry's serving replacement: %d pod(s)", len(pods))
	}

	h.crashReplacement(fixedImage)
	if !h.runWithInvariant(20, func() bool { return ended(2)() && h.heldOn(h.revFixed.Name) }, sourceKept) {
		h.dumpState("held")
		t.Fatalf("the second crash did not hold the revision with the Instance Failed: %+v block=%+v", h.instance(0), h.findBlock(h.revFixed.Name))
	}
	h.requireSourceServes(0, goodImage, "under the hold")
	if got := h.instancesServingOn(goodImage); got != 1 {
		t.Fatalf("%d Instance(s) serve on the starting revision under the hold, want 1", got)
	}

	// The clock passes under the hold: the source keeps serving, the row
	// stays Failed with no attempt, and nothing is built.
	live := len(h.livePods())
	h.runWithInvariant(15, func() bool { return false }, sourceKept)
	h.requireSourceServes(0, goodImage, "after the clock under the hold")
	if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhaseFailed || s.Operation != nil || s.RunningRevision != h.revV1.Name {
		t.Fatalf("the row left Failed with no attempt on the starting revision under the hold: %+v", s)
	}
	if got := len(h.livePods()); got != live {
		h.dumpState("built under the hold")
		t.Fatalf("live pods went %d to %d under the hold; nothing is built while the revision holds", live, got)
	}
}

// A gang surge hands over and its replacement gang is deleted from outside
// while the source gang is still draining. The schedule allows one attempt,
// so the loss holds the revision: the pair ends with the source Failed and
// no attempt, and the source's members, out of rotation since the
// takeover, serve again before that write lands.
func TestGangSurgeAfterTakingOver_AHeldLossHandsTheStandingSourceBack(t *testing.T) {
	h, surgeIdx := gangSurgeHandedOver(t)
	h.retryPolicy = &workloadtypes.RetryPolicy{MaxAttempts: 1, InitialDelay: 20 * time.Second, MaxDelay: time.Minute, Multiplier: 2}
	sourceUIDs := map[string]bool{}
	for _, pod := range h.podsOf(0) {
		sourceUIDs[string(pod.UID)] = true
	}
	if len(sourceUIDs) != 2 {
		t.Fatalf("%d source member(s), want the leader and the worker", len(sourceUIDs))
	}
	sourceKept := func() {
		pods := h.podsOf(0)
		if len(pods) != 2 || !sourceUIDs[string(pods[0].UID)] || !sourceUIDs[string(pods[1].UID)] {
			h.dumpState("source gang changed")
			t.Fatalf("the source gang was deleted or rebuilt while its surge ended: %d member(s)", len(pods))
		}
	}

	h.losePods(surgeIdx)
	held := func() bool {
		s := h.instance(0)
		return s != nil && s.Phase == workloadtypes.InstancePhaseFailed && s.Operation == nil &&
			h.heldOn(h.revFixed.Name) && h.instance(surgeIdx) == nil
	}
	if !h.runWithInvariant(10, held, sourceKept) {
		h.dumpState("not held")
		t.Fatalf("the loss did not end the pair with the revision held: %+v block=%+v", h.instance(0), h.findBlock(h.revFixed.Name))
	}
	for _, pod := range h.podsOf(0) {
		if !podreadiness.IsServing(pod) || podreadiness.HeldNotServingBy(pod, podreadiness.WriterUpdateSurgeDrain) {
			h.dumpState("source member out of rotation")
			t.Fatalf("source member %s is out of rotation after the pair ended: conditions=%+v", pod.Name, pod.Status.Conditions)
		}
	}
	if !h.instanceInRotation(0) {
		t.Fatalf("the source gang does not serve as a whole after the pair ended")
	}
	h.settle(6)
	sourceKept()
	if !h.instanceInRotation(0) || !held() {
		h.dumpState("after settling")
		t.Fatalf("the source gang left rotation or the hold moved after the pair ended: %+v", h.instance(0))
	}
}
