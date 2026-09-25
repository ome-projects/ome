package types

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestOperationSourceUnrouted(t *testing.T) {
	if OperationSourceUnrouted(nil) || OperationSourceUnrouted(&InstanceOperation{}) {
		t.Error("only the source-unrouted token reports the state")
	}
	if !OperationSourceUnrouted(&InstanceOperation{Waiting: WaitingReasonSourceUnrouted}) {
		t.Error("the token reads as source-unrouted")
	}
}

// TestSourceUnroutedDoesNotParkTheDeadline pins the design choice behind
// the token: every other Waiting token names a wait an operator or
// another controller owns and parks InstanceReadyTimeout, while this one
// names a workload that is getting worse. The clock must keep running.
func TestSourceUnroutedDoesNotParkTheDeadline(t *testing.T) {
	op := &InstanceOperation{
		Type:    InstanceOperationUpdate,
		Step:    UpdateStepSurge,
		Waiting: WaitingReasonSourceUnrouted,
	}
	if OperationExternallyHeld(op) {
		t.Fatalf("a SourceUnrouted report must not park the attempt deadline")
	}
	if !OperationSourceUnrouted(op) {
		t.Fatalf("OperationSourceUnrouted must recognize its own token")
	}
}

// The record's moment is the pod's creation, not now: re-observing the
// same state must produce an identical record or the pass writes on
// every tick.
func TestSourceUnroutedTerminationIsStableAcrossObservations(t *testing.T) {
	created := metav1.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "svc-engine-0", CreationTimestamp: created}}
	first, second := SourceUnroutedTermination(pod), SourceUnroutedTermination(pod)
	if *first != *second {
		t.Fatalf("two observations differ: %+v vs %+v", *first, *second)
	}
	if first.Reason != WaitingReasonSourceUnrouted || first.PodName != pod.Name {
		t.Errorf("record must name the token and the pod: %+v", *first)
	}
	if !first.Time.Equal(&created) {
		t.Errorf("record moment: got %v want the pod's creation %v", first.Time, created)
	}
	if !strings.Contains(first.Message, pod.Name) {
		t.Errorf("message must name the pod: %q", first.Message)
	}
	if SourceUnroutedTermination(nil) != nil {
		t.Error("no pod, no record")
	}
}
