package status_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
)

// podRevisionsOneAppendAtATime is the reading of each Instance's pod
// revisions with one list per Instance, grown by append: the form the
// one-array reading replaces, kept here as its reference.
func podRevisionsOneAppendAtATime(byInstance map[int32][]*corev1.Pod) status.PodRevisions {
	revs := make(status.PodRevisions, len(byInstance))
	for idx, pods := range byInstance {
		for _, pod := range query.ExcludeTerminalPods(pods) {
			if rev := query.RevisionFromPod(pod); !rev.IsZero() {
				revs[idx] = append(revs[idx], rev)
			}
		}
	}
	return revs
}

// randomPodBuckets draws a Component's pods bucketed by Instance: zero to
// nine pods per Instance, each Running, Failed or Succeeded, labelled with
// one of three revisions or with none, with a nil entry now and then.
func randomPodBuckets(r *rand.Rand) map[int32][]*corev1.Pod {
	revisions := []string{"", "aaaaaaaa", "bbbbbbbb", "cccccccc"}
	phases := []corev1.PodPhase{corev1.PodRunning, corev1.PodRunning, corev1.PodPending, corev1.PodFailed, corev1.PodSucceeded}
	instances := r.Intn(12)
	byInstance := make(map[int32][]*corev1.Pod, instances)
	for i := 0; i < instances; i++ {
		idx := int32(r.Intn(40))
		var pods []*corev1.Pod
		for n, count := 0, r.Intn(10); n < count; n++ {
			if r.Intn(20) == 0 {
				pods = append(pods, nil)
				continue
			}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d-%d", idx, n)}}
			pod.Status.Phase = phases[r.Intn(len(phases))]
			if hash := revisions[r.Intn(len(revisions))]; hash != "" {
				pod.Labels = map[string]string{query.LabelRevisionHash: hash}
			}
			pods = append(pods, pod)
		}
		byInstance[idx] = pods
	}
	return byInstance
}

// TestPodRevisionsByInstance_AgreesWithOneListPerInstanceOnRandomBuckets
// holds the one-array reading to the one-list-per-Instance form on random
// buckets: the same Instances with an entry, the same revisions in the
// same order, and each entry capped at its own length.
func TestPodRevisionsByInstance_AgreesWithOneListPerInstanceOnRandomBuckets(t *testing.T) {
	r := rand.New(rand.NewSource(31))
	entries, revisions := 0, 0
	for draw := 0; draw < 50000; draw++ {
		byInstance := randomPodBuckets(r)
		got, want := status.PodRevisionsByInstance(byInstance), podRevisionsOneAppendAtATime(byInstance)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("draw %d: PodRevisionsByInstance = %v, want %v", draw, got, want)
		}
		for idx, revs := range got {
			if cap(revs) != len(revs) {
				t.Fatalf("draw %d: the entry of Instance %d has room for %d past its %d revisions; an append would write into the next entry", draw, idx, cap(revs)-len(revs), len(revs))
			}
			entries++
			revisions += len(revs)
		}
	}
	t.Logf("50000 draws: %d entries, %d revisions", entries, revisions)
}
