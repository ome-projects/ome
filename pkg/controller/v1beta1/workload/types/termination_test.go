package types

// InstanceTermination.Time must record WHEN THE FAILURE HAPPENED (kubelet's
// FinishedAt), not when the controller observed it. LastFailure is part of the
// InstanceStatus that the no-op status-write guard DeepEquals before writing,
// so an observation timestamp makes a permanently-failed Instance produce a
// differing status on every reconcile — defeating the guard and driving a
// self-sustaining write -> watch -> reconcile loop.

import (
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	// failedAt is when kubelet says the container died.
	failedAt = metav1.NewTime(time.Date(2026, 8, 19, 4, 28, 38, 0, time.UTC))
	// observedAt is when a reconcile happened to look. Always later, and
	// different per observation.
	observedAt = metav1.NewTime(time.Date(2026, 8, 21, 15, 40, 0, 0, time.UTC))
)

func podWithStatus(name string, cs corev1.ContainerStatus) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "inf-prod"},
		Status:     corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{cs}},
	}
}

// Branch 1: a container terminated with a non-zero exit code.
func TestPodTermination_UsesFinishedAtNotNow(t *testing.T) {
	pod := podWithStatus("router-0-default-0", corev1.ContainerStatus{
		Name: "ome-container",
		State: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{
				ExitCode:   1,
				Reason:     "Error",
				FinishedAt: failedAt,
			},
		},
	})

	got := PodTermination(pod, observedAt)
	if got == nil {
		t.Fatal("expected a termination record")
	}
	if !got.Time.Equal(&failedAt) {
		t.Fatalf("Time = %v, want kubelet FinishedAt %v (got the observation time instead?)", got.Time, failedAt)
	}
	if got.ExitCode == nil || *got.ExitCode != 1 {
		t.Fatalf("ExitCode = %v, want 1", got.ExitCode)
	}
}

// Branch 2: live state is Waiting with CrashLoopBackOff, and the crash that
// caused it is in LastTerminationState. Exit code is zero there, so branch 1
// does not match.
func TestPodTermination_CrashLoopBackOffUsesLastTerminationFinishedAt(t *testing.T) {
	pod := podWithStatus("router-0-default-0", corev1.ContainerStatus{
		Name: "ome-container",
		State: corev1.ContainerState{
			Waiting: &corev1.ContainerStateWaiting{
				Reason:  "CrashLoopBackOff",
				Message: "back-off 5m0s restarting failed container",
			},
		},
		LastTerminationState: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{
				ExitCode:   0,
				FinishedAt: failedAt,
			},
		},
	})

	got := PodTermination(pod, observedAt)
	if got == nil {
		t.Fatal("expected a termination record")
	}
	if got.Reason != "CrashLoopBackOff" {
		t.Fatalf("Reason = %q, want CrashLoopBackOff", got.Reason)
	}
	if !got.Time.Equal(&failedAt) {
		t.Fatalf("Time = %v, want last-termination FinishedAt %v", got.Time, failedAt)
	}
}

// Write-storm guard: observing an unchanged failed pod twice, at two
// different times, must produce byte-identical records. If this fails, the
// status DeepEqual guard cannot converge.
func TestPodTermination_StableAcrossObservations(t *testing.T) {
	cases := map[string]corev1.ContainerStatus{
		"non-zero exit": {
			Name: "ome-container",
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode: 1, Reason: "Error", FinishedAt: failedAt,
				},
			},
		},
		"crashloopbackoff": {
			Name: "ome-container",
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
			},
			LastTerminationState: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{FinishedAt: failedAt},
			},
		},
		"exit zero but terminated": {
			Name: "ome-container",
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, FinishedAt: failedAt},
			},
		},
	}

	for name, cs := range cases {
		t.Run(name, func(t *testing.T) {
			pod := podWithStatus("router-0-default-0", cs)

			first := PodTermination(pod, observedAt)
			second := PodTermination(pod, metav1.NewTime(observedAt.Add(437*time.Millisecond)))

			if first == nil || second == nil {
				t.Fatalf("expected records, got %v / %v", first, second)
			}
			if !reflect.DeepEqual(first, second) {
				t.Fatalf("record changed across observations of an unchanged pod:\n first  = %+v\n second = %+v", first, second)
			}
		})
	}
}

// The fallback must stay intact: with no kubelet timestamp there is no event
// time to report, so the observation time is correct.
func TestPodTermination_FallsBackToNowWhenFinishedAtZero(t *testing.T) {
	pod := podWithStatus("router-0-default-0", corev1.ContainerStatus{
		Name: "ome-container",
		State: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"},
		},
	})

	got := PodTermination(pod, observedAt)
	if got == nil {
		t.Fatal("expected a termination record")
	}
	if !got.Time.Equal(&observedAt) {
		t.Fatalf("Time = %v, want fallback to now %v", got.Time, observedAt)
	}
}

// Branch 4 has no container-level timestamp available, so it keeps `now`.
func TestPodTermination_PodLevelFallbackUsesNow(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "router-0-default-0", Namespace: "inf-prod"},
		Status:     corev1.PodStatus{Phase: corev1.PodFailed, Message: "evicted"},
	}

	got := PodTermination(pod, observedAt)
	if got == nil {
		t.Fatal("expected a termination record")
	}
	if got.Reason != "PodFailed" {
		t.Fatalf("Reason = %q, want PodFailed", got.Reason)
	}
	if !got.Time.Equal(&observedAt) {
		t.Fatalf("Time = %v, want now %v", got.Time, observedAt)
	}
}

// PodTerminationWithReason must not undo the FinishedAt selection.
func TestPodTerminationWithReason_PreservesFinishedAt(t *testing.T) {
	pod := podWithStatus("router-0-default-0", corev1.ContainerStatus{
		Name: "ome-container",
		State: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, FinishedAt: failedAt},
		},
	})

	got := PodTerminationWithReason(pod, "CrashLoopBackOff", observedAt)
	if got == nil {
		t.Fatal("expected a termination record")
	}
	if !got.Time.Equal(&failedAt) {
		t.Fatalf("Time = %v, want kubelet FinishedAt %v", got.Time, failedAt)
	}
}
