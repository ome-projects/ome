package ops

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// mnPod is a Running pod of Instance idx bound to node, as the pass's
// view presents a pod whose Node object is gone: nothing on it says so.
func mnPod(name, node string, idx string, uid k8stypes.UID) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "prod",
			UID:       uid,
			Labels:    map[string]string{query.LabelInstanceIdx: idx},
		},
		Spec:   corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
}

func TestFreePodsOnMissingNodes_RemovesEachPodWithGraceZeroUnderItsUIDAndRecordsIt(t *testing.T) {
	isvc := fdISVC("svc")
	first := mnPod("svc-engine-0-main-0", "node-gone", "0", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeee0001")
	second := mnPod("svc-engine-1-main-0", "node-gone", "1", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeee0002")
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	c := fdFakeClient(t, &funcs, isvc, first, second)
	rec := record.NewFakeRecorder(16)
	deps := workload.Deps{Client: c, Recorder: rec}
	input := fdInput(isvc, nil)
	input.Swept = &workload.SweptPods{}

	if err := FreePodsOnMissingNodes(context.Background(), deps, input, []*corev1.Pod{first, second}); err != nil {
		t.Fatalf("free: %v", err)
	}
	if len(deletes) != 2 {
		t.Fatalf("got %d pod deletes, want one per pod", len(deletes))
	}
	for i, pod := range []*corev1.Pod{first, second} {
		got := deletes[i]
		if got.name != pod.Name || got.grace == nil || *got.grace != 0 || got.uid == nil || *got.uid != pod.UID {
			t.Fatalf("delete %d = %+v; want %s with grace 0 under its UID", i, got, pod.Name)
		}
		if !input.Swept.Swept(pod.UID) {
			t.Fatalf("pod %s was not recorded as swept this pass", pod.Name)
		}
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err == nil {
			t.Fatalf("pod %s still exists after the free", pod.Name)
		}
	}
	events := fdDrainEvents(rec)
	if n := fdCountEvents(events, workload.EventReasonPodForceDeleted); n != 2 {
		t.Fatalf("got %d %s events, want 2: %v", n, workload.EventReasonPodForceDeleted, events)
	}
	for _, e := range events {
		if !strings.Contains(e, "bound to node node-gone, which no longer exists") {
			t.Fatalf("event does not name the missing node: %s", e)
		}
	}
	entries := fdAuditEntries(t, c, isvc)
	if len(entries) != 2 {
		t.Fatalf("got %d audit rows, want one per pod", len(entries))
	}
	for _, e := range entries {
		if e.Reason != audit.ReasonForceDelete || e.Outcome != audit.OutcomeForceDeleteUnreachable || e.FromNode != "node-gone" {
			t.Fatalf("audit row %+v; want a force-delete row naming node-gone", e)
		}
	}

	// A second pass over the same reading issues nothing: the pods are
	// swept, and the objects are gone.
	if err := FreePodsOnMissingNodes(context.Background(), deps, input, []*corev1.Pod{first, second}); err != nil {
		t.Fatalf("free again: %v", err)
	}
	if len(deletes) != 2 {
		t.Fatalf("a swept pod was deleted again: %d deletes", len(deletes))
	}
}

// A pod that left on its own is done with no event, and a delete that the
// apiserver refuses because another pod now holds the name is done too:
// the UID precondition on every delete is what keeps a successor safe,
// and the fake apiserver answers a vanished pod with NotFound.
func TestFreePodsOnMissingNodes_APodAlreadyGoneOrReplacedIsDone(t *testing.T) {
	isvc := fdISVC("svc")
	vanished := mnPod("svc-engine-0-main-0", "node-gone", "0", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeee0001")
	replaced := mnPod("svc-engine-1-main-0", "node-gone", "1", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeee0002")
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	refuse := funcs.Delete
	funcs.Delete = func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		if obj.GetName() == replaced.Name {
			_ = refuse(ctx, cl, obj, opts...)
			return apierrors.NewConflict(corev1.Resource("pods"), obj.GetName(), nil)
		}
		return refuse(ctx, cl, obj, opts...)
	}
	c := fdFakeClient(t, &funcs, isvc)
	rec := record.NewFakeRecorder(16)
	input := fdInput(isvc, nil)
	input.Swept = &workload.SweptPods{}

	if err := FreePodsOnMissingNodes(context.Background(), workload.Deps{Client: c, Recorder: rec}, input, []*corev1.Pod{vanished, replaced}); err != nil {
		t.Fatalf("free: %v", err)
	}
	if len(deletes) != 2 || deletes[1].uid == nil || *deletes[1].uid != replaced.UID {
		t.Fatalf("deletes = %+v; want one per pod, each under the pod's own UID", deletes)
	}
	if n := fdCountEvents(fdDrainEvents(rec), workload.EventReasonPodForceDeleted); n != 0 {
		t.Fatalf("got %d events for pods that needed no delete, want none", n)
	}
	if input.Swept.Swept(vanished.UID) || input.Swept.Swept(replaced.UID) {
		t.Fatalf("a pod that was not deleted here must not read as swept")
	}
}
