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

func int32p(v int32) *int32 { return &v }

func podWith(name string, phase corev1.PodPhase, css ...corev1.ContainerStatus) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status:     corev1.PodStatus{Phase: phase, ContainerStatuses: css},
	}
}

func TestPodTermination_NilPod(t *testing.T) {
	if got := PodTermination(nil, metav1.Now()); got != nil {
		t.Fatalf("PodTermination(nil) = %+v, want nil", got)
	}
}

// A non-zero terminated exit code is the highest-precedence signal — the
// canonical crash the operator most wants to see (OOMKilled, exit 137).
func TestPodTermination_NonZeroTerminatedWins(t *testing.T) {
	pod := podWith("engine-0", corev1.PodFailed, corev1.ContainerStatus{
		Name: "main",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			Reason:   "OOMKilled",
			ExitCode: 137,
			Message:  "out of memory",
		}},
	})
	got := PodTermination(pod, metav1.Now())
	if got == nil {
		t.Fatal("PodTermination = nil, want a record")
	}
	if got.PodName != "engine-0" || got.ContainerName != "main" || got.Reason != "OOMKilled" {
		t.Errorf("identity: %+v", got)
	}
	if got.ExitCode == nil || *got.ExitCode != 137 {
		t.Errorf("ExitCode: got %v want 137", got.ExitCode)
	}
	if got.Message != "out of memory" {
		t.Errorf("Message: got %q", got.Message)
	}
	if got.ShortString() != "pod engine-0 container main failed (OOMKilled, exit 137)" {
		t.Errorf("ShortString: %q", got.ShortString())
	}
}

// CrashLoopBackOff surfaces the crash in LastTerminationState while the
// live State is Waiting — the extractor must read LastTerminationState's
// non-zero exit code in preference to the bare waiting reason.
func TestPodTermination_CrashLoopReadsLastTerminationState(t *testing.T) {
	pod := podWith("engine-0", corev1.PodRunning, corev1.ContainerStatus{
		Name:  "main",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			Reason:   "Error",
			ExitCode: 1,
		}},
	})
	got := PodTermination(pod, metav1.Now())
	if got == nil || got.Reason != "Error" || got.ExitCode == nil || *got.ExitCode != 1 {
		t.Fatalf("want Error/exit 1 from LastTerminationState, got %+v", got)
	}
}

// A terminal waiting state with no terminated history (ImagePullBackOff)
// yields a reason with a nil ExitCode and a "stuck" ShortString.
func TestPodTermination_TerminalWaitingNoExitCode(t *testing.T) {
	pod := podWith("engine-0", corev1.PodPending, corev1.ContainerStatus{
		Name:  "main",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "back-off pulling"}},
	})
	got := PodTermination(pod, metav1.Now())
	if got == nil {
		t.Fatal("want a record")
	}
	if got.Reason != "ImagePullBackOff" || got.ExitCode != nil {
		t.Errorf("want ImagePullBackOff/nil exit, got %+v", got)
	}
	if got.ShortString() != "pod engine-0 container main stuck (ImagePullBackOff)" {
		t.Errorf("ShortString: %q", got.ShortString())
	}
}

// A non-terminal waiting reason (ContainerCreating) is not a failure
// signal; with no other signal and a non-Failed phase, return nil.
func TestPodTermination_TransientWaitingIgnored(t *testing.T) {
	pod := podWith("engine-0", corev1.PodPending, corev1.ContainerStatus{
		Name:  "main",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}},
	})
	if got := PodTermination(pod, metav1.Now()); got != nil {
		t.Fatalf("ContainerCreating must not produce a record, got %+v", got)
	}
}

// Pod phase Failed with no per-container detail falls back to a
// pod-level PodFailed record.
func TestPodTermination_PodLevelFallback(t *testing.T) {
	pod := podWith("engine-0", corev1.PodFailed)
	pod.Status.Message = "node shutdown"
	got := PodTermination(pod, metav1.Now())
	if got == nil || got.Reason != "PodFailed" || got.ContainerName != "" {
		t.Fatalf("want pod-level PodFailed, got %+v", got)
	}
	if got.Message != "node shutdown" {
		t.Errorf("Message: got %q", got.Message)
	}
	if got.ShortString() != "pod engine-0 failed (PodFailed)" {
		t.Errorf("ShortString: %q", got.ShortString())
	}
}

// A Running pod with no termination signal yields nil (no false-positive
// capture during healthy operation).
func TestPodTermination_HealthyRunningNil(t *testing.T) {
	pod := podWith("engine-0", corev1.PodRunning, corev1.ContainerStatus{
		Name:  "main",
		Ready: true,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	})
	if got := PodTermination(pod, metav1.Now()); got != nil {
		t.Fatalf("healthy running pod must yield nil, got %+v", got)
	}
}

// Init-container failures are captured with the same precedence after
// regular containers.
func TestPodTermination_InitContainerCrash(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-0"},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name: "init-model",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					Reason:   "Error",
					ExitCode: 2,
				}},
			}},
		},
	}
	got := PodTermination(pod, metav1.Now())
	if got == nil || got.ContainerName != "init-model" || got.ExitCode == nil || *got.ExitCode != 2 {
		t.Fatalf("want init-model/exit 2, got %+v", got)
	}
}

// kubelet occasionally leaves the terminated Reason blank; the extractor
// must still produce a non-empty reason ("Error") so the record is
// never reason-less.
func TestPodTermination_BlankReasonFallsBackToError(t *testing.T) {
	pod := podWith("engine-0", corev1.PodFailed, corev1.ContainerStatus{
		Name:  "main",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 139}},
	})
	got := PodTermination(pod, metav1.Now())
	if got == nil || got.Reason != "Error" {
		t.Fatalf("want fallback reason Error, got %+v", got)
	}
}

// PodTerminationWithReason fills a missing reason from the override but
// keeps container/exit detail when the extractor found it.
func TestPodTerminationWithReason(t *testing.T) {
	// No per-container detail at all → override supplies the whole record.
	bare := podWith("engine-0", corev1.PodPending)
	got := PodTerminationWithReason(bare, "ImagePullBackOff", metav1.Now())
	if got == nil || got.PodName != "engine-0" || got.Reason != "ImagePullBackOff" {
		t.Fatalf("override path: got %+v", got)
	}

	// Extractor already produced detail with a reason → override does not
	// clobber it.
	rich := podWith("engine-1", corev1.PodFailed, corev1.ContainerStatus{
		Name:  "main",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137}},
	})
	got = PodTerminationWithReason(rich, "ImagePullBackOff", metav1.Now())
	if got.Reason != "OOMKilled" || got.ExitCode == nil || *got.ExitCode != 137 {
		t.Fatalf("override must not clobber richer record, got %+v", got)
	}
}

func TestInstanceTermination_ShortStringNil(t *testing.T) {
	var t0 *InstanceTermination
	if got := t0.ShortString(); got != "" {
		t.Fatalf("nil ShortString = %q, want empty", got)
	}
}

func TestInstanceTermination_ShortStringUnknownPod(t *testing.T) {
	tm := &InstanceTermination{Reason: "OOMKilled", ExitCode: int32p(137)}
	if got := tm.ShortString(); got != "pod <unknown> failed (OOMKilled, exit 137)" {
		t.Fatalf("ShortString = %q", got)
	}
}
