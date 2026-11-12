package workload_test

import (
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// A repair's new pod on a node whose kubelet stops before the pod starts,
// under a force-delete policy. Two single-pod Instances serve version 1 and
// roll to version 2 under RecreatePod with one Instance out of rotation at
// a time. Instance 0 serves version 2 first. Its pod is deleted by hand, so
// a repair creates a replacement. The scheduler binds the replacement to
// the node under Instance 1's pod, and the kubelet there stops in the same
// second, before the node reads NotReady.
//
// The rule under test: a pod of the rebuilt set that never ran a container
// served nothing and holds no state, so once the node under it has read
// NotReady for the policy's threshold the repair removes the object and
// creates the name again on a live node. A pod that ran a container keeps
// its name until its kubelet returns, since the container may still run.

// newLostNodeRebuildHarness is the node-loss harness one pass after the
// kubelet under the replacement stopped. ranFirst lets the replacement run
// its container (never Ready) before the node stops. It returns the
// harness, the replacement pod as bound, and the name of the lost node.
func newLostNodeRebuildHarness(t *testing.T, ranFirst bool) (*recoveryHarness, *corev1.Pod, string) {
	t.Helper()
	story := nodeLossStory{name: "single pod under RecreateInstanceOnPodRestart", policy: workloadtypes.RestartPolicyRecreateInstance}
	h := newNodeLossHarness(t, story, &workloadtypes.ForceDeletePolicy{
		NodeUnreachableThreshold: nodeLossThreshold,
		OverdueSlack:             nodeLossSlack,
	})

	one := intstr.FromInt32(1)
	h.lifecycle.UpdateStrategy = &workloadtypes.UpdateStrategy{
		Type:          workloadtypes.UpdateStrategyRecreatePod,
		RollingUpdate: &workloadtypes.RollingUpdate{MaxUnavailable: &one},
	}
	h.setTarget(h.revFixed, fixedImage)
	if !h.run(30, func() bool { return h.instanceReadyOn(0, h.revFixed) }) {
		h.dumpState("first version 2 pod")
		t.Fatalf("Instance 0 never served version 2")
	}
	peer := h.podsOf(1)
	if len(peer) != 1 || peer[0].Spec.NodeName == "" {
		t.Fatalf("Instance 1 has %d pod(s), want one bound pod", len(peer))
	}
	lostNode := peer[0].Spec.NodeName

	// The first version 2 pod is deleted by hand. Once its kubelet has let
	// it go the pass opens the repair and creates the replacement, which
	// no scheduler has bound yet.
	h.losePods(0)
	opened := func() bool {
		s, pods := h.instance(0), h.podsOf(0)
		return s != nil && s.Phase == workloadtypes.InstancePhaseRestarting && len(pods) == 1 && pods[0].Spec.NodeName == ""
	}
	if !h.run(6, opened) {
		h.dumpState("repair opened")
		t.Fatalf("after the loss want one unbound replacement under an open repair, got row %+v and %d pod(s)", h.instance(0), len(h.podsOf(0)))
	}
	replacement := h.podsOf(0)[0]

	// The scheduler binds the replacement to the node under Instance 1's
	// pod while that node still reads Ready.
	now := h.clk.Now()
	replacement.Spec.NodeName = lostNode
	replacement.CreationTimestamp = metav1.NewTime(now)
	if err := h.c.Update(h.ctx, replacement); err != nil {
		t.Fatalf("bind %s to %s: %v", replacement.Name, lostNode, err)
	}
	setPodScheduled(replacement, corev1.ConditionTrue, "", "", now)
	if err := h.c.Status().Update(h.ctx, replacement); err != nil {
		t.Fatalf("mark %s scheduled: %v", replacement.Name, err)
	}
	if ranFirst {
		// The kubelet starts the container, which never passes readiness,
		// and reports that before it stops.
		h.readinessFails[replacement.UID] = true
		h.step()
	}

	// The kubelet under the node stops before the node reads NotReady.
	h.failNode(lostNode)
	bound := h.podsOf(0)
	if len(bound) != 1 || bound[0].Spec.NodeName != lostNode || bound[0].UID != replacement.UID {
		h.dumpState("replacement on the lost node")
		t.Fatalf("the replacement is not the one bound pod on %s", lostNode)
	}
	return h, bound[0], lostNode
}

// instanceReadyOn reports whether Instance idx is Ready on rev with no
// operation in flight.
func (h *recoveryHarness) instanceReadyOn(idx int32, rev *appsv1.ControllerRevision) bool {
	s := h.instance(idx)
	return s != nil && s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil && s.RunningRevision == rev.Name
}

// instanceServingOff reports whether Instance idx is Ready on rev with
// every pod serving and PodReady on a node other than node.
func (h *recoveryHarness) instanceServingOff(idx int32, rev *appsv1.ControllerRevision, node string) bool {
	if !h.instanceReadyOn(idx, rev) {
		return false
	}
	pods := h.podsOf(idx)
	if len(pods) != len(h.desired.Runners) {
		return false
	}
	for _, pod := range pods {
		if pod.Spec.NodeName == node || !podreadiness.IsServing(pod) || !podConditionTrue(pod, corev1.PodReady) {
			return false
		}
	}
	return true
}

// podUIDs is the set of UIDs of pods.
func podUIDs(pods []*corev1.Pod) map[types.UID]bool {
	out := make(map[types.UID]bool, len(pods))
	for _, pod := range pods {
		out[pod.UID] = true
	}
	return out
}

// sawEventNaming reports whether an event with reason carries every one
// of the given words.
func (h *recoveryHarness) sawEventNaming(reason workloadtypes.EventReason, words ...string) bool {
	for _, e := range h.events {
		if !strings.Contains(e, string(reason)) {
			continue
		}
		named := true
		for _, w := range words {
			named = named && strings.Contains(e, w)
		}
		if named {
			return true
		}
	}
	return false
}

func TestRepairRebuild_PodThatNeverRanOnALostNodeIsRebuiltElsewhereAtTheThreshold(t *testing.T) {
	h, replacement, lostNode := newLostNodeRebuildHarness(t, false)
	stoppedAt := h.clk.Now()

	// The threshold is three passes of the harness clock; the pass that
	// reads it elapsed removes the pod, so a fourth pass at most.
	removed := func() bool {
		return h.sawEventNaming(workloadtypes.EventReasonPodForceDeleted, replacement.Name, "never ran a container")
	}
	passes := 0
	for ; passes < 10 && !removed(); passes++ {
		h.step()
	}
	if !removed() {
		h.dumpState("replacement past the threshold")
		t.Fatalf("the repair's pod %s never ran a container on %s, whose kubelet stopped, and was not removed within %v of the node's %v threshold: the Instance waits; events: %v",
			replacement.Name, lostNode, h.clk.Now().Sub(stoppedAt), nodeLossThreshold, h.events)
	}
	if passes > 4 {
		t.Fatalf("the replacement was removed %d passes (%v) after its node stopped, want within the %v threshold plus one pass", passes, h.clk.Now().Sub(stoppedAt), nodeLossThreshold)
	}
	for _, pod := range h.podsOnNode(lostNode) {
		if pod.UID == replacement.UID {
			t.Fatalf("the replacement %s is still bound to %s after its removal", pod.Name, lostNode)
		}
	}

	// The same repair creates the name again, and the scheduler places it
	// on a live node: Instance 0 serves version 2 while the kubelet of the
	// lost node is still down.
	if !h.run(40, func() bool { return h.instanceServingOff(0, h.revFixed, lostNode) }) {
		h.dumpState("rebuild off the lost node")
		t.Fatalf("Instance 0 never came back Ready and serving version 2 on another node while the kubelet of %s was down", lostNode)
	}
	for _, pod := range h.podsOf(0) {
		if pod.UID == replacement.UID {
			t.Fatalf("Instance 0 serves on the very pod %s that never ran; the repair had to create a new one", pod.Name)
		}
	}

	// Nothing else of the Component waits on the lost node: the roll
	// finishes on version 2 and the lost node holds no pod of it.
	if !h.run(60, func() bool { return h.settledOn(h.revFixed, 2) }) {
		h.dumpState("roll end")
		t.Fatalf("the Component did not settle on version 2 while the kubelet of %s was down", lostNode)
	}
	if on := h.podsOnNode(lostNode); len(on) != 0 {
		t.Fatalf("%d pod object(s) still bound to %s once the Component settled", len(on), lostNode)
	}

	// The kubelet returns and finds nothing of this Component to run; no
	// Instance is rebuilt again.
	settled := podUIDs(h.livePods())
	h.recoverNode(lostNode)
	for i := 0; i < 3; i++ {
		h.step()
	}
	if !h.settledOn(h.revFixed, 2) {
		h.dumpState("after the node returned")
		t.Fatalf("the Component did not stay settled on version 2 after the node returned")
	}
	for _, pod := range h.livePods() {
		if !settled[pod.UID] {
			t.Fatalf("pod %s was replaced after the node returned", pod.Name)
		}
	}
}

func TestRepairRebuild_PodThatRanOnALostNodeKeepsWaitingForItsKubelet(t *testing.T) {
	h, replacement, lostNode := newLostNodeRebuildHarness(t, true)
	if replacement.Status.Phase != corev1.PodRunning {
		t.Fatalf("the replacement must have run its container before the node stopped, got phase %q", replacement.Status.Phase)
	}
	stoppedAt := h.clk.Now()

	// Long past the threshold the replacement is untouched: it may still be
	// running on the node, so only its kubelet or the operator's own
	// eviction may end it.
	for h.clk.Now().Sub(stoppedAt) < 10*nodeLossThreshold {
		h.step()
		if h.sawEventNaming(workloadtypes.EventReasonPodForceDeleted, replacement.Name) {
			t.Fatalf("the replacement %s ran a container and must keep its name while its node is down; events: %v", replacement.Name, h.events)
		}
	}
	pods := h.podsOf(0)
	if len(pods) != 1 || pods[0].UID != replacement.UID || pods[0].DeletionTimestamp != nil || pods[0].Spec.NodeName != lostNode {
		h.dumpState("replacement that ran")
		t.Fatalf("Instance 0 must keep the replacement that ran on %s untouched, got %d pod(s)", lostNode, len(pods))
	}
	if s := h.instance(0); s == nil || s.Operation == nil || s.Operation.Type != workloadtypes.InstanceOperationRestart {
		t.Fatalf("Instance 0 must still carry its repair while the replacement waits for its kubelet, got %+v", h.instance(0))
	}

	// The kubelet returns and the container passes readiness: the repair
	// finishes on the very pod it waited for.
	delete(h.readinessFails, replacement.UID)
	h.recoverNode(lostNode)
	if !h.run(20, func() bool { return h.instanceReadyOn(0, h.revFixed) }) {
		h.dumpState("after the node returned")
		t.Fatalf("Instance 0 did not finish its repair once the kubelet under its replacement returned")
	}
	for _, pod := range h.podsOf(0) {
		if pod.UID != replacement.UID {
			t.Fatalf("Instance 0 was rebuilt on %s instead of finishing on the replacement that ran", pod.Name)
		}
	}
}

func TestRepairRebuild_PassBeforeTheThresholdRequeuesAtTheThreshold(t *testing.T) {
	h, _, lostNode := newLostNodeRebuildHarness(t, false)
	stoppedAt := h.clk.Now()

	// Well inside the threshold the pass keeps the repair's own cadence.
	res, err := h.pass()
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if res.RequeueAfter != testRequeueIntervals.Operation {
		t.Fatalf("a pass %v after the node stopped asked to come back in %v, want the repair's cadence %v", h.clk.Now().Sub(stoppedAt), res.RequeueAfter, testRequeueIntervals.Operation)
	}

	// Closer to the threshold than the cadence, the pass comes back at the
	// threshold itself: a node that stopped reporting emits no event when
	// it comes of age, so nothing else would run the pass at that moment.
	// The threshold counts from the condition time the Node carries,
	// which the API stores to the second.
	node := &corev1.Node{}
	if err := h.c.Get(h.ctx, types.NamespacedName{Name: lostNode}, node); err != nil {
		t.Fatalf("get node %s: %v", lostNode, err)
	}
	var comesOfAge time.Time
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			comesOfAge = cond.LastTransitionTime.Add(nodeLossThreshold)
		}
	}
	if comesOfAge.IsZero() {
		t.Fatalf("node %s carries no Ready condition", lostNode)
	}
	remaining := testRequeueIntervals.Operation / 2
	h.clk.Step(comesOfAge.Add(-remaining).Sub(h.clk.Now()))
	res, err = h.pass()
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if res.RequeueAfter != remaining {
		t.Fatalf("a pass %v before the threshold asked to come back in %v, want %v", remaining, res.RequeueAfter, remaining)
	}
}
