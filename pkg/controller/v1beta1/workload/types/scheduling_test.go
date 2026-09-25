package types

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const schedulerMessage = "0/3 nodes are available: 3 Insufficient nvidia.com/gpu"

// unschedulablePod builds a Pending pod the scheduler has ruled out,
// carrying the scheduler's own message and the moment it did so.
func unschedulablePod(name string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type:               corev1.PodScheduled,
				Status:             corev1.ConditionFalse,
				Reason:             WaitingReasonUnschedulable,
				Message:            schedulerMessage,
				LastTransitionTime: metav1.NewTime(since),
			}},
		},
	}
}

// TestPodUnschedulable pins the detector: only PodScheduled=False with
// the scheduler's Unschedulable reason counts, and it yields the
// scheduler's message plus the condition's last transition (the start of
// the hold, which a grace window is measured from).
func TestPodUnschedulable(t *testing.T) {
	since := time.Now().Add(-5 * time.Minute)
	message, at, ok := PodUnschedulable(unschedulablePod("engine-0-default-0", since))
	if !ok {
		t.Fatalf("unschedulable pod: got ok=false want true")
	}
	if message != schedulerMessage {
		t.Errorf("message: got %q want %q", message, schedulerMessage)
	}
	if !at.Time.Equal(since) {
		t.Errorf("since: got %v want the condition transition %v", at.Time, since)
	}

	scheduled := &corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
		{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
	}}}
	if _, _, ok := PodUnschedulable(scheduled); ok {
		t.Errorf("scheduled pod: got ok=true want false")
	}
	if _, _, ok := PodUnschedulable(&corev1.Pod{}); ok {
		t.Errorf("pod with no PodScheduled condition: got ok=true want false")
	}
	if _, _, ok := PodUnschedulable(nil); ok {
		t.Errorf("nil pod: got ok=true want false")
	}
}

// TestFirstUnschedulablePod: one unplaceable pod holds the whole
// Instance, and which one is chosen by NAME rather than by the
// informer's map iteration order — a gang with several unplaceable
// members must record the same pod every pass or the hold's edge
// trigger never settles.
func TestFirstUnschedulablePod(t *testing.T) {
	since := time.Now().Add(-5 * time.Minute)
	b := unschedulablePod("engine-0-worker-1", since)
	a := unschedulablePod("engine-0-worker-0", since)
	pod, message, at := FirstUnschedulablePod([]*corev1.Pod{b, a})
	if pod == nil || pod.Name != a.Name {
		t.Errorf("chosen pod: got %v want the lowest name %s", pod, a.Name)
	}
	if message != schedulerMessage || !at.Time.Equal(since) {
		t.Errorf("message/since: got %q %v want the scheduler's own", message, at)
	}
	scheduled := &corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
		{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
	}}}
	if pod, _, _ := FirstUnschedulablePod([]*corev1.Pod{scheduled}); pod != nil {
		t.Errorf("placed pod: got %v want none", pod)
	}
}

// TestPodAdmissionGated pins the gated-pod detector: a pod is "gated"
// (queued for admission, e.g. by Kueue) iff it still carries a scheduling
// gate. An un-gated or nil pod is not.
func TestPodAdmissionGated(t *testing.T) {
	gated := &corev1.Pod{Spec: corev1.PodSpec{
		SchedulingGates: []corev1.PodSchedulingGate{{Name: "kueue.x-k8s.io/admission"}},
	}}
	if !PodAdmissionGated(gated) {
		t.Errorf("pod with a scheduling gate: got false want true")
	}
	if PodAdmissionGated(&corev1.Pod{}) {
		t.Errorf("pod with no scheduling gate: got true want false")
	}
	if PodAdmissionGated(nil) {
		t.Errorf("nil pod: got true want false")
	}
}
