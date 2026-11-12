package query

import (
	"math/rand/v2"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func phasePod(name string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}, Status: corev1.PodStatus{Phase: phase}}
}

// TestLivePodCount_OnEveryPicture holds the count of pods outside a
// terminal phase: Running and Pending pods count, Failed and Succeeded
// pods and nil entries do not.
func TestLivePodCount_OnEveryPicture(t *testing.T) {
	pictures := []struct {
		name string
		pods []*corev1.Pod
		want int
	}{
		{name: "no pod", want: 0},
		{name: "an empty list", pods: []*corev1.Pod{}, want: 0},
		{name: "a Running pod", pods: []*corev1.Pod{phasePod("a", corev1.PodRunning)}, want: 1},
		{name: "a Pending pod", pods: []*corev1.Pod{phasePod("a", corev1.PodPending)}, want: 1},
		{name: "a pod with no phase", pods: []*corev1.Pod{phasePod("a", "")}, want: 1},
		{name: "a Failed pod", pods: []*corev1.Pod{phasePod("a", corev1.PodFailed)}, want: 0},
		{name: "a Succeeded pod", pods: []*corev1.Pod{phasePod("a", corev1.PodSucceeded)}, want: 0},
		{name: "a nil entry", pods: []*corev1.Pod{nil}, want: 0},
		{name: "two live pods around a terminal one and a nil entry", pods: []*corev1.Pod{phasePod("a", corev1.PodRunning), phasePod("b", corev1.PodFailed), nil, phasePod("c", corev1.PodPending)}, want: 2},
	}
	for _, pic := range pictures {
		t.Run(pic.name, func(t *testing.T) {
			if got := LivePodCount(pic.pods); got != pic.want {
				t.Errorf("LivePodCount = %d, want %d", got, pic.want)
			}
			if got := len(ExcludeTerminalPods(pic.pods)); got != pic.want {
				t.Errorf("len(ExcludeTerminalPods) = %d, want %d", got, pic.want)
			}
		})
	}
}

// TestLivePodCount_AgreesWithTheListOnRandomLists holds the count to the
// length of the list ExcludeTerminalPods keeps, over random lists of pods
// in every phase with nil entries among them.
func TestLivePodCount_AgreesWithTheListOnRandomLists(t *testing.T) {
	rng := rand.New(rand.NewPCG(30, 1107))
	phases := []corev1.PodPhase{corev1.PodRunning, corev1.PodPending, corev1.PodFailed, corev1.PodSucceeded, corev1.PodUnknown, ""}
	draws, live := 100000, 0
	for i := 0; i < draws; i++ {
		var pods []*corev1.Pod
		for n := rng.IntN(10); n > 0; n-- {
			if rng.IntN(8) == 0 {
				pods = append(pods, nil)
				continue
			}
			pods = append(pods, phasePod("p", phases[rng.IntN(len(phases))]))
		}
		got, want := LivePodCount(pods), len(ExcludeTerminalPods(pods))
		if got != want {
			t.Fatalf("draw %d: LivePodCount = %d, len(ExcludeTerminalPods) = %d", i, got, want)
		}
		live += got
	}
	t.Logf("%d draws, %d live pods counted", draws, live)
}
