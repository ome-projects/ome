package query

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// indexedPod is a pod whose Instance index label carries the raw value
// given; "" leaves the label off.
func indexedPod(name, index string) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{}}}
	if index != "" {
		pod.Labels[LabelInstanceIdx] = index
	}
	return pod
}

// bucketNames is the names of a bucket's pods, in bucket order.
func bucketNames(bucket []*corev1.Pod) []string {
	names := make([]string, 0, len(bucket))
	for _, pod := range bucket {
		names = append(names, pod.Name)
	}
	return names
}

// indexLists are the two kinds of list the bucketing reads: one whose
// indexes lie close together, as a Component's do, and one with an index
// far from the rest.
var indexLists = map[string]struct {
	far string
}{
	"indexes close together":     {far: "3"},
	"an index far from the rest": {far: "2147483647"},
}

// TestBucketPodsByInstanceIdx_KeepsEachInstancesPodsInListOrder: every pod
// with a well-formed index label lands in its Instance's bucket in the
// order the list gives, whatever order the Instances come in; a pod with
// no label, one whose label is not an int32 and a nil entry are left out,
// and an Instance with no pod has no bucket.
func TestBucketPodsByInstanceIdx_KeepsEachInstancesPodsInListOrder(t *testing.T) {
	for name, list := range indexLists {
		t.Run(name, func(t *testing.T) {
			pods := []*corev1.Pod{
				indexedPod("a", "2"), indexedPod("b", "0"), indexedPod("c", "2"), nil,
				indexedPod("d", "x"), indexedPod("e", ""), indexedPod("f", "0"), indexedPod("g", "-1"),
				indexedPod("h", list.far), indexedPod("i", "2147483648"), indexedPod("j", "2"),
			}
			got := BucketPodsByInstanceIdx(pods)
			want := map[string][]string{"2": {"a", "c", "j"}, "0": {"b", "f"}, "-1": {"g"}, list.far: {"h"}}
			if len(got) != len(want) {
				t.Errorf("%d buckets, want %d", len(got), len(want))
			}
			for index, names := range want {
				idx, _ := InstanceIdxFromLabels(indexedPod("", index))
				if diff := cmp.Diff(names, bucketNames(got[idx])); diff != "" {
					t.Errorf("bucket %d (-want +got):\n%s", idx, diff)
				}
			}
		})
	}
}

// TestBucketPodsByInstanceIdx_ABucketGrownByACallerLeavesTheOthersWhole: a
// caller that appends to one Instance's bucket changes no other bucket and
// no entry of the list it bucketed, whichever bucket it grows.
func TestBucketPodsByInstanceIdx_ABucketGrownByACallerLeavesTheOthersWhole(t *testing.T) {
	for name, list := range indexLists {
		t.Run(name, func(t *testing.T) {
			indexes := []string{"0", "1", list.far}
			var pods []*corev1.Pod
			for round := 0; round < 2; round++ {
				for _, index := range indexes {
					pods = append(pods, indexedPod(fmt.Sprintf("pod-%s-%d", index, round), index))
				}
			}
			listed := bucketNames(pods)
			for _, grown := range indexes {
				got := BucketPodsByInstanceIdx(pods)
				grownIdx, _ := InstanceIdxFromLabels(indexedPod("", grown))
				got[grownIdx] = append(got[grownIdx], indexedPod("extra", grown))
				for _, index := range indexes {
					if index == grown {
						continue
					}
					idx, _ := InstanceIdxFromLabels(indexedPod("", index))
					want := []string{fmt.Sprintf("pod-%s-0", index), fmt.Sprintf("pod-%s-1", index)}
					if diff := cmp.Diff(want, bucketNames(got[idx])); diff != "" {
						t.Errorf("bucket %s grown: bucket %s (-want +got):\n%s", grown, index, diff)
					}
				}
				if diff := cmp.Diff(listed, bucketNames(pods)); diff != "" {
					t.Errorf("bucket %s grown: the list (-want +got):\n%s", grown, diff)
				}
			}
		})
	}
}

// podsOneAtATime is the grouping as a reader would write it, one append
// per pod: the reference the bucketing is held to.
func podsOneAtATime(pods []*corev1.Pod) map[int32][]*corev1.Pod {
	by := map[int32][]*corev1.Pod{}
	for _, pod := range pods {
		if idx, ok := InstanceIdxFromLabels(pod); ok {
			by[idx] = append(by[idx], pod)
		}
	}
	return by
}

// TestBucketPodsByInstanceIdx_AgreesWithTheOneAtATimeGroupingOnRandomLists:
// over 300,000 random lists of up to 48 entries drawn from pods of eleven
// index labels (well-formed, absent, not an int32, the int32 limits) and
// nil entries, close together or with one far from the rest, the buckets
// equal the grouping made one append at a time, bucket for bucket and pod
// for pod.
func TestBucketPodsByInstanceIdx_AgreesWithTheOneAtATimeGroupingOnRandomLists(t *testing.T) {
	pool := []*corev1.Pod{nil}
	for i, index := range []string{"0", "1", "2", "3", "7", "-1", "2147483647", "2147483648", "x", "", ""} {
		pool = append(pool, indexedPod(fmt.Sprintf("pod-%d", i), index))
	}
	const cases = 300000
	rng := rand.New(rand.NewSource(17))
	var buckets, bucketed int
	for n := 0; n < cases; n++ {
		list := make([]*corev1.Pod, rng.Intn(49))
		for i := range list {
			list[i] = pool[rng.Intn(len(pool))]
		}
		want := podsOneAtATime(list)
		got := BucketPodsByInstanceIdx(list)
		if len(got) != len(want) {
			t.Fatalf("case %d: %d buckets, want %d", n, len(got), len(want))
		}
		for idx, wantBucket := range want {
			gotBucket := got[idx]
			if len(gotBucket) != len(wantBucket) {
				t.Fatalf("case %d: bucket %d has %d pods, want %d", n, idx, len(gotBucket), len(wantBucket))
			}
			for i := range wantBucket {
				if gotBucket[i] != wantBucket[i] {
					t.Fatalf("case %d: bucket %d position %d holds %v, want %v", n, idx, i, gotBucket[i], wantBucket[i])
				}
			}
			buckets++
			bucketed += len(wantBucket)
		}
	}
	t.Logf("%d lists agreed over %d buckets and %d bucketed pods", cases, buckets, bucketed)
}
