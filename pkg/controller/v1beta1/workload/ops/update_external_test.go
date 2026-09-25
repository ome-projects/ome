package ops_test

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const (
	alienRunningRevision = "llama-70b-engine-aaaaaaaa"
	alienPodRevision     = "llama-70b-engine-bbbbbbbb"
)

// alienFixture is a Ready Instance whose pod carries a third revision —
// neither the Instance's running one nor the current roll target.
func alienFixture(t *testing.T, serving bool) (*v1beta1.InferenceService, client.Client, *corev1.Pod, *appsv1.ControllerRevision, *record.FakeRecorder) {
	t.Helper()
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     1,
		Phase:           v1beta1.OMENativeInstanceReady,
		RunningRevision: alienRunningRevision,
	})
	pod := podForInstance(isvc, 0, true /* ready */, serving)
	pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(alienPodRevision)
	c := newFakeClient(t, isvc, ir, pod)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-cccccccc", Namespace: "prod"},
	}
	return isvc, c, pod, target, record.NewFakeRecorder(8)
}

func TestCleanupWreckage_ServingAlienLeavesRotationBeforeItIsDeleted(t *testing.T) {
	isvc, c, pod, target, events := alienFixture(t, true /* serving */)
	plan := buildPlanSinglePodEngine(1)
	sweep := func() {
		t.Helper()
		input := buildTestInput(isvc, c, workload.ComponentEngine)
		live := &corev1.Pod{}
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), live); err != nil {
			t.Fatalf("re-read alien: %v", err)
		}
		if _, err := ops.CleanupWreckage(context.Background(), workload.Deps{Client: c, Recorder: events},
			input, plan, plan.Instances[0], target, []*corev1.Pod{live}); err != nil {
			t.Fatalf("CleanupWreckage: %v", err)
		}
	}

	sweep()
	afterFirst := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), afterFirst); err != nil {
		t.Fatalf("the alien was deleted in the pass that unrouted it: %v", err)
	}
	if podreadiness.IsServing(afterFirst) {
		t.Fatalf("the alien is still in rotation after the first pass")
	}

	sweep()
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err == nil {
		t.Errorf("the alien survived the pass after it left rotation")
	}
}

func TestCleanupWreckage_VanishedAlienIsNotAnError(t *testing.T) {
	isvc, c, pod, target, events := alienFixture(t, true /* serving */)
	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatalf("delete the alien out from under the sweep: %v", err)
	}
	plan := buildPlanSinglePodEngine(1)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	if _, err := ops.CleanupWreckage(context.Background(), workload.Deps{Client: c, Recorder: events},
		input, plan, plan.Instances[0], target, []*corev1.Pod{pod}); err != nil {
		t.Fatalf("a pod that vanished mid-sweep must not fail the pass: %v", err)
	}
}

func TestCleanupWreckage_UnroutedAlienIsDeletedImmediately(t *testing.T) {
	isvc, c, pod, target, events := alienFixture(t, false /* serving */)
	plan := buildPlanSinglePodEngine(1)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	if _, err := ops.CleanupWreckage(context.Background(), workload.Deps{Client: c, Recorder: events},
		input, plan, plan.Instances[0], target, []*corev1.Pod{pod}); err != nil {
		t.Fatalf("CleanupWreckage: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err == nil {
		t.Errorf("an alien that was never in rotation has nothing to wait for; it must go in this pass")
	}
}
