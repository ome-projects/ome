package evidence_test

// The force-delete predicate, branch by branch. Only three classes are
// actionable, and each of the refusals is a distinct reason the cluster
// has not yet proved that no kubelet is left to run the pod's
// containers. The escalation that consumes these verdicts is tested with
// the escalation.

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

var tNow = time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)

// TestAttemptSuspectNode: the suspect node is read off the attempt's own
// pods. A single-pod surge shares its bucket with the serving source, so
// a source on another node — or one not yet bound — must neither veto
// the reading nor be the node it names; a pod being removed, or one
// without a revision label, is read exactly as SingleLiveNode reads it.
func TestAttemptSuspectNode(t *testing.T) {
	const oldRev, newRev = "own-engine-oldhash", "own-engine-newhash"
	on := func(rev, node string) *corev1.Pod {
		pod := &corev1.Pod{Spec: corev1.PodSpec{NodeName: node}}
		if rev != "" {
			pod.Labels = map[string]string{query.LabelRevisionHash: query.RevisionFromName(rev).Hash()}
		}
		return pod
	}
	leaving := func(rev, node string) *corev1.Pod {
		pod := on(rev, node)
		pod.DeletionTimestamp = &metav1.Time{Time: tNow}
		return pod
	}
	for _, tc := range []struct {
		name string
		pods []*corev1.Pod
		want string
	}{
		{name: "source and target on different nodes name the target's", pods: []*corev1.Pod{on(oldRev, "a"), on(newRev, "b")}, want: "b"},
		{name: "an unbound source does not veto the target's node", pods: []*corev1.Pod{on(oldRev, ""), on(newRev, "b")}, want: "b"},
		{name: "only the source present names nothing", pods: []*corev1.Pod{on(oldRev, "a")}, want: ""},
		{name: "a lone pod carrying the target label names its node", pods: []*corev1.Pod{on(newRev, "a")}, want: "a"},
		{name: "unlabeled pods stay in scope", pods: []*corev1.Pod{on("", "a")}, want: "a"},
		{name: "unlabeled pods spanning nodes still name nothing", pods: []*corev1.Pod{on("", "a"), on("", "b")}, want: ""},
		{name: "a drained old pod on its way out is still the attempt's", pods: []*corev1.Pod{leaving(oldRev, "a")}, want: "a"},
		{name: "a leaving source and a target on another node span nodes", pods: []*corev1.Pod{leaving(oldRev, "a"), on(newRev, "b")}, want: ""},
		{name: "a target gang on one node resolves", pods: []*corev1.Pod{on(oldRev, "a"), on(newRev, "b"), on(newRev, "b")}, want: "b"},
		{name: "a target gang spanning nodes names nothing", pods: []*corev1.Pod{on(newRev, "b"), on(newRev, "c")}, want: ""},
		{name: "no target revision leaves every pod in scope", pods: []*corev1.Pod{on(oldRev, "a"), on(newRev, "b")}, want: ""},
		{name: "no pods", pods: nil, want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rev := newRev
			if tc.name == "no target revision leaves every pod in scope" {
				rev = ""
			}
			if got := evidence.AttemptSuspectNode(tc.pods, rev); got != tc.want {
				t.Errorf("AttemptSuspectNode: got %q want %q", got, tc.want)
			}
		})
	}
}

func tPolicy() *types.ForceDeletePolicy {
	return &types.ForceDeletePolicy{
		OverdueSlack:             2 * time.Minute,
		NodeUnreachableThreshold: 5 * time.Minute,
	}
}

// tTerminatingPod is a pod whose deletion deadline has already passed by
// deletedAt, on node.
func tTerminatingPod(node string, deletedAt time.Time) *corev1.Pod {
	ts := metav1.NewTime(deletedAt)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "prod",
			Name:              "wedge-0",
			DeletionTimestamp: &ts,
			Finalizers:        nil,
		},
		Spec: corev1.PodSpec{NodeName: node},
	}
}

func tNode(name string, ready corev1.ConditionStatus, age time.Duration, taint bool) *corev1.Node {
	transition := metav1.NewTime(tNow.Add(-age))
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type:               corev1.NodeReady,
			Status:             ready,
			LastTransitionTime: transition,
		}}},
	}
	if taint {
		n.Spec.Taints = []corev1.Taint{{
			Key:       corev1.TaintNodeUnreachable,
			Effect:    corev1.TaintEffectNoExecute,
			TimeAdded: &transition,
		}}
	}
	return n
}

func tReader(t *testing.T, objs ...client.Object) client.Reader {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

// TestStuckTerminating_Branches walks the pod half of the predicate: an
// absent policy reads nothing at all, a pod inside its own deletion
// window is left alone, and a finalizer-pinned pod is reported rather
// than acted on because force-delete could not remove it anyway.
func TestStuckTerminating_Branches(t *testing.T) {
	overdue := tNow.Add(-10 * time.Minute)
	for _, tc := range []struct {
		name       string
		pod        *corev1.Pod
		policy     *types.ForceDeletePolicy
		want       evidence.TerminatingClass
		actionable bool
	}{
		{
			name:   "no policy: the escalation does not exist",
			pod:    tTerminatingPod("gone-node", overdue),
			policy: nil,
			want:   evidence.NotConfigured,
		},
		{
			name:   "inside the pod's own grace plus the slack",
			pod:    tTerminatingPod("gone-node", tNow.Add(9*time.Minute)),
			policy: tPolicy(),
			want:   evidence.WithinGrace,
		},
		{
			name:   "exactly at the slack boundary is still within grace",
			pod:    tTerminatingPod("gone-node", tNow.Add(-2*time.Minute)),
			policy: tPolicy(),
			want:   evidence.WithinGrace,
		},
		{
			name: "overdue but pinned by a foreign finalizer: report only",
			pod: func() *corev1.Pod {
				p := tTerminatingPod("gone-node", overdue)
				p.Finalizers = []string{"example.com/keep"}
				return p
			}(),
			policy: tPolicy(),
			want:   evidence.ForeignFinalizers,
		},
		{
			name:       "overdue with no node object: the node is gone",
			pod:        tTerminatingPod("gone-node", overdue),
			policy:     tPolicy(),
			want:       evidence.NodeGone,
			actionable: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// No node seeded: every branch above either stops before the
			// node read or expects the NotFound one.
			got := evidence.StuckTerminating(context.Background(), tReader(t), tc.pod, tc.policy, tNow)
			if got.Kind != tc.want {
				t.Fatalf("kind: got %s want %s", got.Kind, tc.want)
			}
			if got.Kind.Actionable() != tc.actionable {
				t.Errorf("actionable: got %v want %v", got.Kind.Actionable(), tc.actionable)
			}
		})
	}
}

// TestNodeDeath_Branches walks the node half: a current Ready=True vetoes
// every death branch including a stale unreachable taint, evidence
// younger than the threshold is a blip, and only a gone, long-tainted or
// long-NotReady node proves no kubelet is left.
func TestNodeDeath_Branches(t *testing.T) {
	for _, tc := range []struct {
		name       string
		node       *corev1.Node
		nodeName   string
		want       evidence.TerminatingClass
		actionable bool
	}{
		{
			name:     "unscheduled pod has no node whose death could be proven",
			nodeName: "",
			want:     evidence.Unscheduled,
		},
		{
			name:     "Ready=True is the kubelet-slow case",
			node:     tNode("live", corev1.ConditionTrue, 10*time.Second, false),
			nodeName: "live",
			want:     evidence.NodeHealthy,
		},
		{
			name:     "Ready=True vetoes a stale unreachable taint",
			node:     tNode("recovered", corev1.ConditionTrue, 10*time.Minute, true),
			nodeName: "recovered",
			want:     evidence.NodeHealthy,
		},
		{
			name:     "NotReady younger than the threshold is a blip",
			node:     tNode("dying", corev1.ConditionFalse, 2*time.Minute, false),
			nodeName: "dying",
			want:     evidence.NodeNotDeadLongEnough,
		},
		{
			name:       "unreachable taint older than the threshold",
			node:       tNode("dead", corev1.ConditionUnknown, 10*time.Minute, true),
			nodeName:   "dead",
			want:       evidence.NodeUnreachableTaint,
			actionable: true,
		},
		{
			name:       "NotReady older than the threshold",
			node:       tNode("dead", corev1.ConditionFalse, 10*time.Minute, false),
			nodeName:   "dead",
			want:       evidence.NodeNotReady,
			actionable: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var objs []client.Object
			if tc.node != nil {
				objs = append(objs, tc.node)
			}
			pod := tTerminatingPod(tc.nodeName, tNow.Add(-10*time.Minute))
			got := evidence.NodeDeath(context.Background(), tReader(t, objs...), pod, tPolicy(), tNow, 0)
			if got.Kind != tc.want {
				t.Fatalf("kind: got %s want %s", got.Kind, tc.want)
			}
			if got.Kind.Actionable() != tc.actionable {
				t.Errorf("actionable: got %v want %v", got.Kind.Actionable(), tc.actionable)
			}
		})
	}
}

// TestSingleLiveNode: the relocation branch may only be steered off one
// suspect node, so a multi-node attempt, an unscheduled pod or an empty
// set all name nothing.
func TestSingleLiveNode(t *testing.T) {
	on := func(nodes ...string) []*corev1.Pod {
		pods := make([]*corev1.Pod, 0, len(nodes))
		for _, n := range nodes {
			pods = append(pods, &corev1.Pod{Spec: corev1.PodSpec{NodeName: n}})
		}
		return pods
	}
	for _, tc := range []struct {
		name string
		pods []*corev1.Pod
		want string
	}{
		{name: "no pods", pods: nil, want: ""},
		{name: "one pod on one node", pods: on("a"), want: "a"},
		{name: "a whole gang on one node", pods: on("a", "a", "a"), want: "a"},
		{name: "pods spanning nodes", pods: on("a", "b"), want: ""},
		{name: "an unscheduled pod", pods: on("a", ""), want: ""},
		{name: "a nil pod", pods: []*corev1.Pod{nil}, want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := evidence.SingleLiveNode(tc.pods); got != tc.want {
				t.Errorf("SingleLiveNode: got %q want %q", got, tc.want)
			}
		})
	}
}

// TestSweepFreesUnknownPod holds the pre-sweep reading to the sweep's own
// arms: no policy frees nothing; a pod nobody asked to delete is freed on
// node death alone; a Terminating pod is freed only past its overdue
// window and never while a finalizer pins it; a live node frees nothing.
func TestSweepFreesUnknownPod(t *testing.T) {
	policy := &types.ForceDeletePolicy{OverdueSlack: time.Minute, NodeUnreachableThreshold: 5 * time.Minute}
	reader := tReader(t, tNode("live", corev1.ConditionTrue, 10*time.Second, false))
	quiet := func(node string, deleted *metav1.Time, finalizers ...string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-0", DeletionTimestamp: deleted, Finalizers: finalizers},
			Spec:       corev1.PodSpec{NodeName: node},
			Status:     corev1.PodStatus{Phase: corev1.PodUnknown},
		}
	}
	overdue := metav1.NewTime(tNow.Add(-10 * time.Minute))
	for _, tc := range []struct {
		name   string
		pod    *corev1.Pod
		policy *types.ForceDeletePolicy
		want   bool
	}{
		{"no policy", quiet("gone", nil), nil, false},
		{"node gone", quiet("gone", nil), policy, true},
		{"node live", quiet("live", nil), policy, false},
		{"terminating, node gone, overdue", quiet("gone", &overdue), policy, true},
		{"terminating, node gone, pinned", quiet("gone", &overdue, "example.com/keep"), policy, false},
		{"terminating, node gone, within grace", quiet("gone", ptrTime(tNow)), policy, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := evidence.SweepFreesUnknownPod(context.Background(), reader, tc.pod, tc.policy, tNow)
			if err != nil {
				t.Fatalf("SweepFreesUnknownPod: %v", err)
			}
			if got != tc.want {
				t.Errorf("SweepFreesUnknownPod = %v, want %v", got, tc.want)
			}
		})
	}
}

func ptrTime(at time.Time) *metav1.Time {
	t := metav1.NewTime(at)
	return &t
}

func TestUnknownPhaseTargetPods(t *testing.T) {
	pod := func(name string, phase corev1.PodPhase) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Status:     corev1.PodStatus{Phase: phase},
		}
	}
	names := func(targets ...string) map[string]struct{} {
		set := make(map[string]struct{}, len(targets))
		for _, t := range targets {
			set[t] = struct{}{}
		}
		return set
	}
	got := evidence.UnknownPhaseTargetPods(
		[]*corev1.Pod{
			pod("c", corev1.PodUnknown),
			pod("a", corev1.PodUnknown),
			pod("b", corev1.PodRunning),
			pod("d", corev1.PodUnknown),
		},
		names("a", "b", "c"),
	)
	if len(got) != 2 || got[0].Name != "a" || got[1].Name != "c" {
		t.Fatalf("want the Unknown targets in name order [a c], got %v", got)
	}
	if evidence.UnknownPhaseTargetPods(nil, names("a")) != nil {
		t.Error("no pods: want nil")
	}
	if evidence.UnknownPhaseTargetPods([]*corev1.Pod{pod("a", corev1.PodUnknown)}, nil) != nil {
		t.Error("no targets: want nil")
	}
}
