package types

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestOperationNodeUnknown(t *testing.T) {
	if OperationNodeUnknown(nil) || OperationNodeUnknown(&InstanceOperation{}) {
		t.Error("only the node-unknown token reports the hold")
	}
	if !OperationNodeUnknown(&InstanceOperation{Waiting: WaitingReasonNodeUnknown}) {
		t.Error("the token reads as node-unknown")
	}
}

func TestPodPhaseUnknown(t *testing.T) {
	if PodPhaseUnknown(nil) {
		t.Error("no pod is not an unreported one")
	}
	if PodPhaseUnknown(&corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning}}) {
		t.Error("a reported pod is not Unknown")
	}
	if !PodPhaseUnknown(&corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodUnknown}}) {
		t.Error("phase Unknown reads as unreported")
	}
}

// The record's moment is the pod's creation, not now: re-observing the
// same wait must produce an identical record or the pass writes on every
// tick.
func TestNodeUnknownTerminationIsStableAcrossObservations(t *testing.T) {
	created := metav1.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "svc-engine-0", CreationTimestamp: created},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
	}
	first, second := NodeUnknownTermination(pod), NodeUnknownTermination(pod)
	if *first != *second {
		t.Fatalf("two observations differ: %+v vs %+v", *first, *second)
	}
	if !first.Time.Equal(&created) {
		t.Errorf("record moment: got %v want the pod's creation %v", first.Time, created)
	}
	if !strings.Contains(first.Message, "node-a") {
		t.Errorf("message must name the node that went quiet: %q", first.Message)
	}
	unbound := NodeUnknownTermination(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p"}})
	if !strings.Contains(unbound.Message, "<unbound>") {
		t.Errorf("an unscheduled pod has no node to name: %q", unbound.Message)
	}
	if NodeUnknownTermination(nil) != nil {
		t.Error("no pod, no record")
	}
}
