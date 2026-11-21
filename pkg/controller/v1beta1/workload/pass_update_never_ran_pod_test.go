package workload_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// bindNeverVisitedPodsTo binds every pod of the Component the kubelet
// model has not visited yet to node, as the API server and the scheduler
// leave a pod bound to a node whose kubelet never picks it up: created
// now, Pending, scheduled, no container status. It reports the pods bound.
func (h *recoveryHarness) bindNeverVisitedPodsTo(node string) []string {
	h.t.Helper()
	var bound []string
	for _, pod := range h.livePods() {
		if !pod.CreationTimestamp.IsZero() || pod.Spec.NodeName != "" {
			continue
		}
		pod.Spec.NodeName = node
		pod.CreationTimestamp = metav1.NewTime(h.clk.Now())
		if err := h.c.Update(h.ctx, pod); err != nil {
			h.t.Fatalf("bind pod %s to %s: %v", pod.Name, node, err)
		}
		pod.Status.Phase = corev1.PodPending
		setPodScheduled(pod, corev1.ConditionTrue, "", "", h.clk.Now())
		if err := h.c.Status().Update(h.ctx, pod); err != nil {
			h.t.Fatalf("write pod %s pending on %s: %v", pod.Name, node, err)
		}
		bound = append(bound, pod.Name)
	}
	return bound
}

// bindTheNextNewPodsTo runs passes until the Component has pods the
// kubelet model has not visited, binds them to node, and reports them;
// nil when no new pod appears within passes.
func (h *recoveryHarness) bindTheNextNewPodsTo(node string, passes int) []string {
	h.t.Helper()
	for i := 0; i < passes; i++ {
		h.step()
		if bound := h.bindNeverVisitedPodsTo(node); len(bound) > 0 {
			return bound
		}
	}
	return nil
}

// neverRanShape is one attempt kind the test drives: the strategy,
// whether the Instances are gangs, and whether the new pod comes from the
// refill of a lost Instance (the Create pass) instead of a push.
type neverRanShape struct {
	name     string
	strategy workloadtypes.UpdateStrategyType
	gang     bool
	refill   bool
}

// An attempt's new pod is bound to a node whose kubelet stops before the
// pod ever runs: a surge replacement, the rebuilt pod of a recreate, a
// member of a replacement gang, or the pod the Create pass builds for an
// Instance that lost its own. The pod never reports, so nothing on it
// changes. Once the node has been NotReady for the force-delete policy's
// threshold the pass removes the pod, as a repair removes a never-run pod
// of its own set, and the attempt builds it again on a live node; the
// roll, or the refill, finishes there.
func TestUpdate_ANewPodThatNeverRanOnANodeThatStoppedReportingIsBuiltAgainElsewhere(t *testing.T) {
	shapes := []neverRanShape{
		{"surge", workloadtypes.UpdateStrategySurgeThenDrain, false, false},
		{"recreate", workloadtypes.UpdateStrategyRecreatePod, false, false},
		{"gang-surge", workloadtypes.UpdateStrategySurgeThenDrain, true, false},
		{"refill", workloadtypes.UpdateStrategyRecreatePod, false, true},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) { neverRanPodOfALostNodeIsBuiltAgain(t, shape) })
	}
}

// neverRanPodOfALostNodeIsBuiltAgain drives one shape: two Instances
// Ready on v1, the attempt's new pod bound to a node that then stops
// reporting, the pod removed at the policy's threshold, the attempt
// finished on a live node.
func neverRanPodOfALostNodeIsBuiltAgain(t *testing.T, shape neverRanShape) {
	h := newRecoveryHarness(t, shape.gang)
	h.replicas = 2
	h.useRollingBudget(shape.strategy)
	h.forceDelete = &workloadtypes.ForceDeletePolicy{NodeUnreachableThreshold: nodeLossThreshold, OverdueSlack: nodeLossSlack}
	h.recorder = record.NewFakeRecorder(256)
	h.useNodes("node-a", "node-b")
	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.settledOn(h.revV1, 2) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never converged on v1 for two Instances")
	}
	want := h.revFixed
	if shape.refill {
		want = h.revV1
		h.losePods(0)
	} else {
		h.setTarget(h.revFixed, fixedImage)
	}

	// The attempt's new pod appears within a few passes; under RecreatePod
	// only after the old pod left.
	lost := "node-lost"
	h.addNodes(lost)
	bound := h.bindTheNextNewPodsTo(lost, 12)
	if len(bound) == 0 {
		h.dumpState("no new pod to bind")
		t.Fatalf("the %s attempt created no new pod within twelve passes", shape.name)
	}
	h.failNode(lost)
	failedAt := h.clk.Now()

	freed := func() bool { return h.sawEvent(workloadtypes.EventReasonPodForceDeleted) }
	if !h.run(20, freed) {
		h.dumpState("never-run pod kept on the lost node")
		t.Fatalf("the new pod %v of the %s attempt, which never ran on %s, was not removed after the node stopped reporting for the policy's threshold", bound, shape.name, lost)
	}
	if elapsed := h.clk.Now().Sub(failedAt); elapsed > nodeLossThreshold+2*recoveryStepFloor {
		t.Fatalf("the never-run pod was removed %v after the node stopped reporting, want within the %v threshold plus two passes", elapsed, nodeLossThreshold)
	}
	if !h.run(40, func() bool { return h.settledOn(want, 2) }) {
		h.dumpState("after the never-run pod was removed")
		t.Fatalf("the %s attempt did not finish on %s after the never-run pod was removed", shape.name, want.Name)
	}
	if on := h.podsOnNode(lost); len(on) != 0 {
		t.Fatalf("%d pod(s) still sit on %s after the roll finished", len(on), lost)
	}
}

// pendingPodOnImage reports a bound Pending pod of image that is not on
// its way out, or nil.
func (h *recoveryHarness) pendingPodOnImage(image string) *corev1.Pod {
	for _, pod := range h.podsOnImage(image) {
		if pod.Spec.NodeName != "" && pod.Status.Phase == corev1.PodPending && pod.DeletionTimestamp == nil {
			return pod
		}
	}
	return nil
}

// podNamed reads the Component's live pod called name, or nil.
func (h *recoveryHarness) podNamed(name string) *corev1.Pod {
	for _, pod := range h.livePods() {
		if pod.Name == name {
			return pod
		}
	}
	return nil
}

// A surge replacement on a node that keeps reporting Ready waits on its
// image for longer than the force-delete threshold plus its slack. A slow
// pull is not a lost node: the pod is never removed, and once the pull
// ends the roll finishes on that same pod.
func TestUpdate_ANeverRunPodOnANodeThatKeepsReportingIsKeptThroughASlowPull(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.replicas = 2
	h.useRollingBudget(workloadtypes.UpdateStrategySurgeThenDrain)
	h.lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 30 * time.Minute}
	h.forceDelete = &workloadtypes.ForceDeletePolicy{NodeUnreachableThreshold: nodeLossThreshold, OverdueSlack: nodeLossSlack}
	h.recorder = record.NewFakeRecorder(256)
	h.useNodes("node-a", "node-b")
	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.settledOn(h.revV1, 2) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never converged on v1 for two Instances")
	}
	h.armWedge("ContainerCreating", false)
	h.setTarget(h.revFixed, fixedImage)
	var pulling *corev1.Pod
	if !h.run(12, func() bool { pulling = h.pendingPodOnImage(fixedImage); return pulling != nil }) {
		h.dumpState("no pulling pod")
		t.Fatalf("the roll created no new pod that waits on its image within twelve passes")
	}
	name, uid, node := pulling.Name, pulling.UID, pulling.Spec.NodeName
	if h.deadNodes[node] {
		t.Fatalf("the pulling pod %s sits on %s, which is not a reporting node", name, node)
	}
	start := h.clk.Now()
	for h.clk.Now().Sub(start) < nodeLossThreshold+nodeLossSlack+2*recoveryStepFloor {
		h.step()
		if h.sawEvent(workloadtypes.EventReasonPodForceDeleted) {
			t.Fatalf("the pod %s, pulling its image on the reporting node %s, was force-deleted %v into the pull: a slow pull read as a lost node", name, node, h.clk.Now().Sub(start))
		}
		if got := h.podNamed(name); got == nil || got.UID != uid {
			t.Fatalf("the pod %s on the reporting node %s was replaced %v into the pull", name, node, h.clk.Now().Sub(start))
		}
	}
	h.clearWedge()
	if !h.run(40, func() bool { return h.settledOn(h.revFixed, 2) }) {
		h.dumpState("after the pull ended")
		t.Fatalf("the roll did not finish after the pull ended")
	}
	if got := h.podNamed(name); got == nil || got.UID != uid {
		t.Fatalf("the roll finished on a different pod than %s, the one that pulled slowly", name)
	}
}
