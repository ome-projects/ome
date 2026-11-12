package ops

import (
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// rolledSetMember is a pod as the rolled-set reading tells its members
// apart: the revision its label names, and whether it is being deleted.
type rolledSetMember struct {
	name     string
	revision string
	deleting bool
}

func (m rolledSetMember) pod() *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: m.name, Labels: map[string]string{}}}
	if m.revision != "" {
		pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(m.revision)
	}
	if m.deleting {
		pod.DeletionTimestamp = &metav1.Time{Time: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	}
	return pod
}

func rolledSetMembers(members ...rolledSetMember) []*corev1.Pod {
	pods := make([]*corev1.Pod, 0, len(members))
	for _, m := range members {
		if m.name == "" {
			pods = append(pods, nil)
			continue
		}
		pods = append(pods, m.pod())
	}
	return pods
}

func podNames(pods []*corev1.Pod) []string {
	names := make([]string, 0, len(pods))
	for _, pod := range pods {
		names = append(names, pod.Name)
	}
	return names
}

// TestRolledSetPods_KeepsThePodsOnTheRevisionInListOrder holds the set the
// roll reads an Instance's pods as: the live pods carrying the target
// revision's label or no revision label at all, in the order the list has
// them; a pod of another revision, a pod being deleted and a nil entry are
// not members. With no target revision only the unlabelled pods are.
func TestRolledSetPods_KeepsThePodsOnTheRevisionInListOrder(t *testing.T) {
	const target, other = "svc-engine-rev-b", "svc-engine-rev-a"
	cases := []struct {
		name   string
		target string
		pods   []*corev1.Pod
		want   []string
	}{
		{name: "every pod on the revision, in list order", target: target,
			pods: rolledSetMembers(rolledSetMember{name: "a", revision: target}, rolledSetMember{name: "b", revision: target}, rolledSetMember{name: "c", revision: target}),
			want: []string{"a", "b", "c"}},
		{name: "a pod of another revision is not a member", target: target,
			pods: rolledSetMembers(rolledSetMember{name: "a", revision: target}, rolledSetMember{name: "x", revision: other}, rolledSetMember{name: "b", revision: target}),
			want: []string{"a", "b"}},
		{name: "a pod with no revision label is a member", target: target,
			pods: rolledSetMembers(rolledSetMember{name: "a"}, rolledSetMember{name: "b", revision: target}),
			want: []string{"a", "b"}},
		{name: "a pod being deleted is not a member, on the revision or not", target: target,
			pods: rolledSetMembers(rolledSetMember{name: "a", revision: target, deleting: true}, rolledSetMember{name: "b", revision: target}, rolledSetMember{name: "c", deleting: true}),
			want: []string{"b"}},
		{name: "a nil entry is not a member", target: target,
			pods: rolledSetMembers(rolledSetMember{}, rolledSetMember{name: "a", revision: target}),
			want: []string{"a"}},
		{name: "no pod on the revision is an empty set", target: target,
			pods: rolledSetMembers(rolledSetMember{name: "x", revision: other}, rolledSetMember{name: "y", revision: other}),
			want: []string{}},
		{name: "no target revision keeps the unlabelled pods alone", target: "",
			pods: rolledSetMembers(rolledSetMember{name: "a", revision: target}, rolledSetMember{name: "b"}),
			want: []string{"b"}},
		{name: "no pods is an empty set", target: target, want: []string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := podNames(rolledSetPods(tc.target, tc.pods)); !slices.Equal(got, tc.want) {
				t.Errorf("rolledSetPods = %v, want %v", got, tc.want)
			}
		})
	}
}

// rolledSetPodsCopied is the set read one append per member: the live
// pods carrying the target's revision or no revision, in list order. The
// reading rolledSetPods is held to.
func rolledSetPodsCopied(target string, pods []*corev1.Pod) []*corev1.Pod {
	targetRev := query.RevisionFromName(target)
	own := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		if podRev := query.RevisionFromPod(pod); !podRev.IsZero() && !podRev.Same(targetRev) {
			continue
		}
		own = append(own, pod)
	}
	return own
}

// TestRolledSetPods_AgreesWithTheCopiedSetOnRandomLists draws 300,000 pod
// lists of up to twelve entries from a pool of pods on the target, on
// another revision, on a revision whose hash is the target's under another
// name, with no revision label, being deleted or nil, against a target
// named, named with another revision's hash, or empty, and holds the set
// equal to the copied reading pod for pod, in order.
func TestRolledSetPods_AgreesWithTheCopiedSetOnRandomLists(t *testing.T) {
	const target = "svc-engine-rev-b"
	targets := []string{target, "svc-decoder-rev-b", "svc-engine-rev-a", ""}
	pool := []*corev1.Pod{
		nil,
		rolledSetMember{name: "on-target", revision: target}.pod(),
		rolledSetMember{name: "on-target-deleting", revision: target, deleting: true}.pod(),
		rolledSetMember{name: "other-revision", revision: "svc-engine-rev-a"}.pod(),
		rolledSetMember{name: "other-revision-deleting", revision: "svc-engine-rev-a", deleting: true}.pod(),
		rolledSetMember{name: "same-hash-other-name", revision: "svc-decoder-rev-b"}.pod(),
		rolledSetMember{name: "no-revision", revision: ""}.pod(),
		rolledSetMember{name: "no-revision-deleting", revision: "", deleting: true}.pod(),
	}
	r := rand.New(rand.NewPCG(25, 1))
	var members, lists, shared int
	for draw := 0; draw < 300000; draw++ {
		pods := make([]*corev1.Pod, r.IntN(13))
		for i := range pods {
			pods[i] = pool[r.IntN(len(pool))]
		}
		chosen := targets[r.IntN(len(targets))]
		got, want := rolledSetPods(chosen, pods), rolledSetPodsCopied(chosen, pods)
		if len(got) != len(want) {
			t.Fatalf("draw %d, target %q, list %v: %d members, want %d", draw, chosen, podNames(pods), len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("draw %d, target %q, list %v: member %d is %s, want %s", draw, chosen, podNames(pods), i, got[i].Name, want[i].Name)
			}
		}
		if len(pods) > 0 && len(got) == len(pods) {
			shared++
		}
		members += len(got)
		lists++
	}
	t.Logf("%d lists, %d members, %d lists whose every pod is a member", lists, members, shared)
	if shared == 0 || shared == lists {
		t.Fatalf("the draws took one path alone: %d of %d lists had every pod a member", shared, lists)
	}
}

// TestRolledSetPods_AllocatesNothingWhenEveryPodIsAMember: an Instance at
// rest hands the roll a list whose every pod is on the target, and the roll
// reads it as it is; a list with a pod to leave out is copied.
func TestRolledSetPods_AllocatesNothingWhenEveryPodIsAMember(t *testing.T) {
	const target = "svc-engine-rev-b"
	atRest := rolledSetMembers(rolledSetMember{name: "leader", revision: target}, rolledSetMember{name: "worker-0", revision: target}, rolledSetMember{name: "worker-1"})
	if allocs := testing.AllocsPerRun(100, func() { rolledSetPods(target, atRest) }); allocs != 0 {
		t.Errorf("a list whose every pod is a member cost %.0f allocations per reading, want none", allocs)
	}
	if got := rolledSetPods(target, atRest); len(got) != len(atRest) || &got[0] != &atRest[0] {
		t.Errorf("a list whose every pod is a member is read as a copy; want the list itself")
	}
	mixed := rolledSetMembers(rolledSetMember{name: "leader", revision: target}, rolledSetMember{name: "stale", revision: "svc-engine-rev-a"})
	if got := rolledSetPods(target, mixed); len(got) != 1 || &got[0] == &mixed[0] {
		t.Errorf("a list with a pod to leave out = %v sharing the list %v; want the members in a list of their own", podNames(got), &got[0] == &mixed[0])
	}
}
