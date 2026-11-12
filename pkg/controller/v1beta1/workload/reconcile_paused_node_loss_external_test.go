package workload_test

import (
	"maps"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// pausedNodeLossPolicies are the restart policies a lost node under a
// paused Component is read against. They decide what follows the freed
// names: under None the row is written Pending and rebuilt when the pause
// clears; under RecreateInstanceOnPodRestart the repair rebuilds the
// Instance at once, since a plain pause holds the roll and not the repair.
func pausedNodeLossPolicies() []nodeLossStory {
	return []nodeLossStory{
		{"single pod under None", false, workloadtypes.RestartPolicyNone},
		{"single pod under RecreateInstanceOnPodRestart", false, workloadtypes.RestartPolicyRecreateInstance},
	}
}

// newPausedNodeLossHarness is the harness with four single-pod Instances
// converged on v1 over four nodes, one pod per node in creation order, so
// Instance 0 sits on node-a and the other three on nodes the loss never
// touches. The force-delete policy is on.
func newPausedNodeLossHarness(t *testing.T, story nodeLossStory) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarnessFor(t, story.multiPod, constants.MainContainerName)
	h.replicas = 4
	restart := story.policy
	h.lifecycle.RestartPolicy = &restart
	h.podGrace = 30 * time.Second
	h.forceDelete = &workloadtypes.ForceDeletePolicy{
		NodeUnreachableThreshold: nodeLossThreshold,
		OverdueSlack:             nodeLossSlack,
	}
	h.recorder = record.NewFakeRecorder(256)
	h.useNodes(nodeLossLostNode, "node-b", "node-c", "node-d")
	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.settledOn(h.revV1, 4) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never converged on v1")
	}
	if on := h.podsOnNode(nodeLossLostNode); len(on) != 1 || !podBelongsToInstance(on[0], 0) {
		h.dumpState("placement")
		t.Fatalf("node model placed %d pod(s) on %s, want exactly one pod of Instance 0", len(on), nodeLossLostNode)
	}
	return h
}

func podBelongsToInstance(pod *corev1.Pod, idx int32) bool {
	got, ok := query.InstanceIdxFromLabels(pod)
	return ok && got == idx
}

// requireLiveInstancesUntouched is the per-step invariant of both paused
// node-loss stories: Instances 1 to 3 keep the very pods they had, each
// serving and PodReady, nothing renders a revision other than v1, and no
// graceful delete is ever issued on the lost node.
func (h *recoveryHarness) requireLiveInstancesUntouched(live map[int32]map[string]bool) {
	h.t.Helper()
	for idx, want := range live {
		pods := h.podsOf(idx)
		if !maps.Equal(podIdentities(pods), want) {
			h.dumpState("live Instances")
			h.t.Fatalf("Instance %d has %d live pod(s) and not its original %d: a pause must start nothing on an Instance the loss did not touch, and must replace none of its pods", idx, len(pods), len(want))
		}
		for _, pod := range pods {
			if !podreadiness.IsServing(pod) || !podConditionTrue(pod, corev1.PodReady) {
				h.t.Fatalf("pod %s of Instance %d left the rotation", pod.Name, idx)
			}
		}
	}
	if n := h.podsOnRevision(h.revFixed); n != 0 {
		h.t.Fatalf("%d pod(s) render version 2 while the Component is paused: the pause must hold the roll", n)
	}
	h.requireNoGracefulDeleteOn(nodeLossLostNode)
}

// podsOnRevision counts the live pods carrying rev's revision hash.
func (h *recoveryHarness) podsOnRevision(rev *appsv1.ControllerRevision) int {
	h.t.Helper()
	hash := query.RevisionOf(rev).Hash()
	n := 0
	for _, pod := range h.livePods() {
		if pod.Labels[query.LabelRevisionHash] == hash {
			n++
		}
	}
	return n
}

// liveIdentities is the pod identity of every Instance in idx, the way
// the kubelet model tells a rebuilt pod from the one it replaced.
func (h *recoveryHarness) liveIdentities(idx ...int32) map[int32]map[string]bool {
	out := make(map[int32]map[string]bool, len(idx))
	for _, i := range idx {
		out[i] = podIdentities(h.podsOf(i))
	}
	return out
}

// idlePassAdvance is how far the harness moves its clock for one pass
// that asks for no requeue: the least a pass costs, and so the most a
// force delete due at a boundary may run late by.
func (h *recoveryHarness) idlePassAdvance() time.Duration {
	h.t.Helper()
	before := h.clk.Now()
	h.step()
	return h.clk.Now().Sub(before)
}

// TestNodeLoss_PausedComponentFreesTheLostNodesPods: four engines serve
// version 1 and the operator pauses the Component. The node under
// Instance 0 then stops reporting. The force-delete policy must free the
// pods on it at the node threshold while the pause stands, as it does for
// an unpaused Component, and the pause must still start nothing new: the
// three Instances on live nodes keep their pods. What follows the freed
// names is the pause's own rule for a lost pod. Under restart policy None
// the row is written Pending with its revision kept and nothing is rebuilt
// until the pause clears; under RecreateInstanceOnPodRestart the repair
// rebuilds the Instance on another node while the pause stands.
func TestNodeLoss_PausedComponentFreesTheLostNodesPods(t *testing.T) {
	for _, story := range pausedNodeLossPolicies() {
		t.Run(story.name, func(t *testing.T) {
			h := newPausedNodeLossHarness(t, story)
			h.desired.Paused = true
			h.step()
			onePass := h.idlePassAdvance()
			live := h.liveIdentities(1, 2, 3)
			before := h.instance(0).Incarnation
			untouched := func() { h.requireLiveInstancesUntouched(live) }

			h.failNode(nodeLossLostNode)
			failedAt := h.clk.Now()

			freed := func() bool { return h.sawEvent(workloadtypes.EventReasonPodForceDeleted) }
			if !h.runWithInvariant(10, freed, untouched) {
				h.dumpState("paused, node down past the threshold")
				t.Fatalf("the pod on %s was never force-deleted while the Component was paused: a pause must not keep a lost node's pods in place", nodeLossLostNode)
			}
			if elapsed := h.clk.Now().Sub(failedAt); elapsed > nodeLossThreshold+onePass {
				t.Fatalf("the pod was freed %v after the node stopped reporting, want within the %v threshold plus one pass", elapsed, nodeLossThreshold)
			}
			if on := h.podsOnNode(nodeLossLostNode); len(on) != 0 {
				t.Fatalf("%d pod object(s) still bound to %s after the force delete", len(on), nodeLossLostNode)
			}

			switch story.policy {
			case workloadtypes.RestartPolicyNone:
				// The pause holds the rebuild: the row is written Pending
				// with its revision kept and no pod is created until the
				// pause clears.
				for i := 0; i < 3; i++ {
					h.step()
					untouched()
				}
				if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhasePending || s.Operation != nil || s.RunningRevision != h.revV1.Name {
					h.dumpState("paused after the force delete")
					t.Fatalf("Instance 0 reads %+v, want Pending with no operation and its v1 revision kept while the pause stands", s)
				}
				if pods := h.podsOf(0); len(pods) != 0 {
					t.Fatalf("the paused pass rebuilt Instance 0 (%d pod(s)); under policy None the rebuild waits for the unpause", len(pods))
				}
				h.desired.Paused = false
				if !h.runWithInvariant(40, func() bool { return h.servingElsewhere(nodeLossLostNode) }, untouched) {
					h.dumpState("rebuild after the unpause")
					t.Fatalf("Instance 0 never came back Ready and serving on another node after the unpause")
				}
				if s := h.instance(0); s.Incarnation != before {
					t.Fatalf("policy None re-materializes the missing pod in place at the same generation, got %d (was %d)", s.Incarnation, before)
				}
			case workloadtypes.RestartPolicyRecreateInstance:
				// A plain pause holds the roll and not the repair: the
				// Instance is rebuilt on another node while the pause stands.
				if !h.runWithInvariant(40, func() bool { return h.servingElsewhere(nodeLossLostNode) }, untouched) {
					h.dumpState("paused rebuild off the lost node")
					t.Fatalf("Instance 0 never came back Ready and serving on another node while the Component was paused")
				}
				if s := h.instance(0); s.Incarnation <= before {
					t.Fatalf("RecreateInstanceOnPodRestart rebuilds the whole Instance at the next generation, got %d (was %d)", s.Incarnation, before)
				}
				h.desired.Paused = false
			}
			h.requirePodsRender(0, h.revV1, goodImage)
			if !h.run(30, func() bool { return h.settledOn(h.revV1, 4) }) {
				h.dumpState("after the unpause")
				t.Fatalf("the Component did not settle on v1 after the unpause")
			}
		})
	}
}

// TestNodeLoss_PauseStillHoldsTheRollWhileALostNodesPodsAreFreed: four
// engines serve version 1, version 2 is pushed and the operator pauses
// the Component before the roll starts. The node under Instance 0 then
// stops reporting and the pause lasts past the node threshold. Whatever
// the sweep does about the lost node's pod, the pause holds the roll
// itself: no pod renders version 2 while the pause stands, the three
// Instances on live nodes keep their pods, and a rebuild under the pause
// stays on version 1. When the pause clears the roll runs to the end.
func TestNodeLoss_PauseStillHoldsTheRollWhileALostNodesPodsAreFreed(t *testing.T) {
	for _, story := range pausedNodeLossPolicies() {
		t.Run(story.name, func(t *testing.T) {
			h := newPausedNodeLossHarness(t, story)
			v2 := h.pushRevision(fixedImage)
			h.desired.Paused = true
			h.step()
			live := h.liveIdentities(1, 2, 3)
			untouched := func() { h.requireLiveInstancesUntouched(live) }

			h.failNode(nodeLossLostNode)
			for i := 0; i < 12; i++ {
				h.step()
				untouched()
			}
			if s := h.instance(0); s == nil || s.RunningRevision != h.revV1.Name {
				h.dumpState("paused past the threshold")
				t.Fatalf("Instance 0 reads %+v, want its v1 revision kept while the pause stands", s)
			}
			for _, pod := range h.podsOf(0) {
				if pod.Labels[query.LabelRevisionHash] != query.RevisionOf(h.revV1).Hash() {
					t.Fatalf("pod %s of Instance 0 renders %s while the Component is paused; a rebuild under a pause stays on the revision the Instance ran", pod.Name, pod.Labels[query.LabelRevisionHash])
				}
			}

			h.desired.Paused = false
			if !h.run(60, func() bool { return h.settledOn(v2, 4) }) {
				h.dumpState("roll after the unpause")
				t.Fatalf("the roll to version 2 did not finish after the unpause")
			}
		})
	}
}
