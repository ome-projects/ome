package query

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// podInPhase builds a pod in the given phase. ready stamps a
// ContainersReady=True condition — on a terminal pod that models a stale
// condition the kubelet never cleared.
func podInPhase(name string, phase corev1.PodPhase, ready bool) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod"}}
	pod.Status.Phase = phase
	if ready {
		pod.Status.Conditions = []corev1.PodCondition{{
			Type: corev1.ContainersReady, Status: corev1.ConditionTrue,
		}}
	}
	return pod
}

func TestIsTerminalPod(t *testing.T) {
	cases := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{"nil", nil, false},
		{"unset phase", podInPhase("p", "", false), false},
		{"pending", podInPhase("p", corev1.PodPending, false), false},
		{"running", podInPhase("p", corev1.PodRunning, true), false},
		{"unknown", podInPhase("p", corev1.PodUnknown, false), false},
		{"failed", podInPhase("p", corev1.PodFailed, false), true},
		{"succeeded", podInPhase("p", corev1.PodSucceeded, false), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsTerminalPod(tc.pod); got != tc.want {
				t.Fatalf("IsTerminalPod = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestExcludeTerminalPods(t *testing.T) {
	live := podInPhase("live", corev1.PodRunning, true)
	pending := podInPhase("pending", corev1.PodPending, false)
	got := ExcludeTerminalPods([]*corev1.Pod{
		podInPhase("failed", corev1.PodFailed, false),
		live,
		nil,
		podInPhase("succeeded", corev1.PodSucceeded, false),
		pending,
	})
	if len(got) != 2 || got[0] != live || got[1] != pending {
		t.Fatalf("ExcludeTerminalPods kept %d pods, want exactly [live pending]", len(got))
	}
	if out := ExcludeTerminalPods(nil); out == nil || len(out) != 0 {
		t.Fatalf("ExcludeTerminalPods(nil) = %v, want an empty non-nil slice", out)
	}
}

// A terminal pod is never runtime-ready, even when it still carries a
// ContainersReady=True condition from before it died.
func TestAllPodsRuntimeReady_TerminalPodNeverReady(t *testing.T) {
	ready := podInPhase("ready", corev1.PodRunning, true)
	cases := []struct {
		name string
		pods []*corev1.Pod
		want bool
	}{
		{"empty", nil, false},
		{"all ready", []*corev1.Pod{ready, podInPhase("ready-2", corev1.PodRunning, true)}, true},
		{"one not ready", []*corev1.Pod{ready, podInPhase("booting", corev1.PodRunning, false)}, false},
		{"failed with stale ready condition", []*corev1.Pod{ready, podInPhase("dead", corev1.PodFailed, true)}, false},
		{"succeeded with stale ready condition", []*corev1.Pod{podInPhase("done", corev1.PodSucceeded, true)}, false},
		{"only terminal", []*corev1.Pod{podInPhase("dead", corev1.PodFailed, false)}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := AllPodsRuntimeReady(tc.pods); got != tc.want {
				t.Fatalf("AllPodsRuntimeReady = %v, want %v", got, tc.want)
			}
		})
	}
}
