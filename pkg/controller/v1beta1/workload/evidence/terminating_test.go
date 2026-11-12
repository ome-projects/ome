package evidence

// The force-delete predicate, branch by branch. Only four classes are
// actionable, and each refusal is a distinct reason OME keeps waiting on
// the pod's kubelet. The escalation that acts on these classes is tested
// with the escalation.

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

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
			if got := AttemptSuspectNode(tc.pods, rev); got != tc.want {
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

// tReaders reads through r as both the cache and the API server.
func tReaders(r client.Reader) types.NodeReaders {
	return types.NodeReaders{Cache: r, Live: r}
}

// tClassify classifies one Terminating pod through readers at tNow.
func tClassify(readers types.NodeReaders, pod *corev1.Pod, policy *types.ForceDeletePolicy) TerminatingResult {
	return StuckTerminating(context.Background(), readers, pod, policy, tNow)
}

// tNodeReads counts the Node reads made through the reader it wraps, and
// fails each one with err when err is set.
type tNodeReads struct {
	client.Reader
	err   error
	reads int
}

func (r *tNodeReads) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Node); ok {
		r.reads++
		if r.err != nil {
			return r.err
		}
	}
	return r.Reader.Get(ctx, key, obj, opts...)
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
		want       TerminatingClass
		actionable bool
	}{
		{
			name:   "no policy: the escalation does not exist",
			pod:    tTerminatingPod("gone-node", overdue),
			policy: nil,
			want:   NotConfigured,
		},
		{
			name:   "inside the pod's own grace plus the slack",
			pod:    tTerminatingPod("gone-node", tNow.Add(9*time.Minute)),
			policy: tPolicy(),
			want:   WithinGrace,
		},
		{
			name:   "exactly at the slack boundary is still within grace",
			pod:    tTerminatingPod("gone-node", tNow.Add(-2*time.Minute)),
			policy: tPolicy(),
			want:   WithinGrace,
		},
		{
			name: "overdue but pinned by a foreign finalizer: report only",
			pod: func() *corev1.Pod {
				p := tTerminatingPod("gone-node", overdue)
				p.Finalizers = []string{"example.com/keep"}
				return p
			}(),
			policy: tPolicy(),
			want:   ForeignFinalizers,
		},
		{
			name:       "overdue with no node object: the node is gone",
			pod:        tTerminatingPod("gone-node", overdue),
			policy:     tPolicy(),
			want:       NodeGone,
			actionable: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// No node seeded: every branch above either stops before the
			// node read or expects the NotFound one.
			got := tClassify(tReaders(tReader(t)), tc.pod, tc.policy)
			if got.Kind != tc.want {
				t.Fatalf("kind: got %s want %s", got.Kind, tc.want)
			}
			if got.Kind.Actionable() != tc.actionable {
				t.Errorf("actionable: got %v want %v", got.Kind.Actionable(), tc.actionable)
			}
		})
	}
}

// TestUnreachableInsideGrace: a pod inside its grace period goes early
// only under SkipGracePeriodOnUnreachableNode, and only once its node's
// kubelet has stopped reporting for at least the threshold, the
// threshold itself included. When the node shows both signs, the outage
// counts from the more recent one, which also names the class. NodeReady
// Unknown reads as its own class, never as the NotReady of a running
// kubelet. A NodeReady the kubelet still posts, True or False, keeps the
// pod waiting, as do a gone node, a failed read, a pod bound to no node
// and finalizers. The cached Node screens every pod. The live Node is
// read only when the cached one already shows the threshold passed, and
// the live one decides. Signs younger than the threshold move the requeue
// to the moment they come of age, if that is sooner.
func TestUnreachableInsideGrace(t *testing.T) {
	keep := tPolicy()
	skip := tPolicy()
	skip.SkipGracePeriodOnUnreachableNode = true
	threshold := skip.NodeUnreachableThreshold
	deletedAt := tNow.Add(9 * time.Minute)
	graceEnds := deletedAt.Add(skip.OverdueSlack + time.Nanosecond)
	unreachable := tNode("lost", corev1.ConditionUnknown, 10*time.Minute, true)
	unknown := tNode("lost", corev1.ConditionUnknown, 10*time.Minute, false)
	recovered := tNode("lost", corev1.ConditionTrue, 10*time.Second, false)
	undated := tNode("lost", corev1.ConditionUnknown, time.Minute, true)
	undated.Spec.Taints[0].TimeAdded = nil
	undated.Status.Conditions = nil
	taintOnly := tNode("lost", corev1.ConditionUnknown, 10*time.Minute, true)
	taintOnly.Status.Conditions = nil
	// bothSigns is a node whose NodeReady went Unknown readyFor ago and
	// whose unreachable taint was added taintFor ago.
	bothSigns := func(readyFor, taintFor time.Duration) *corev1.Node {
		node := tNode("lost", corev1.ConditionUnknown, readyFor, true)
		added := metav1.NewTime(tNow.Add(-taintFor))
		node.Spec.Taints[0].TimeAdded = &added
		return node
	}
	for _, tc := range []struct {
		name   string
		policy *types.ForceDeletePolicy
		// cached and live are the Node the cache and the API server hold,
		// nil for none. The pod is bound to that node, or to nodeName.
		cached, live          *corev1.Node
		nodeName              string
		deletedAt             time.Time
		finalizers            []string
		cacheErr, liveErr     error
		want                  TerminatingClass
		requeueAt             time.Time
		cacheReads, liveReads int
	}{
		{name: "without the switch the pod waits and no node is read", policy: keep, cached: unreachable, live: unreachable, want: WithinGrace, requeueAt: graceEnds},
		{name: "an unreachable taint past the threshold", policy: skip, cached: unreachable, live: unreachable, want: NodeUnreachableTaint, cacheReads: 1, liveReads: 1},
		{name: "NodeReady Unknown past the threshold", policy: skip, cached: unknown, live: unknown, want: NodeReadyUnknown, cacheReads: 1, liveReads: 1},
		{
			name: "an unreachable taint exactly the threshold old", policy: skip,
			cached: tNode("lost", corev1.ConditionUnknown, threshold, true), live: tNode("lost", corev1.ConditionUnknown, threshold, true),
			want: NodeUnreachableTaint, cacheReads: 1, liveReads: 1,
		},
		{
			name: "NodeReady Unknown exactly the threshold old", policy: skip,
			cached: tNode("lost", corev1.ConditionUnknown, threshold, false), live: tNode("lost", corev1.ConditionUnknown, threshold, false),
			want: NodeReadyUnknown, cacheReads: 1, liveReads: 1,
		},
		{
			// One second is the resolution the API stores these times at.
			name: "an unreachable taint one second short of the threshold", policy: skip,
			cached: tNode("lost", corev1.ConditionUnknown, threshold-time.Second, true), live: unreachable,
			want: WithinGrace, requeueAt: tNow.Add(time.Second), cacheReads: 1,
		},
		{
			name: "NodeReady Unknown one second short of the threshold", policy: skip,
			cached: tNode("lost", corev1.ConditionUnknown, threshold-time.Second, false), live: unknown,
			want: WithinGrace, requeueAt: tNow.Add(time.Second), cacheReads: 1,
		},
		{
			name: "signs that come of age after the grace period requeue at its end", policy: skip,
			cached: tNode("lost", corev1.ConditionUnknown, time.Minute, false), live: unknown, deletedAt: tNow.Add(-time.Minute),
			want: WithinGrace, requeueAt: tNow.Add(-time.Minute).Add(skip.OverdueSlack + time.Nanosecond), cacheReads: 1,
		},
		{name: "an unreachable taint without a time cannot prove its age", policy: skip, cached: undated, live: unreachable, want: WithinGrace, requeueAt: graceEnds, cacheReads: 1},
		{name: "an unreachable taint past the threshold on a node with no NodeReady", policy: skip, cached: taintOnly, live: taintOnly, want: NodeUnreachableTaint, cacheReads: 1, liveReads: 1},
		{
			name: "an old taint does not age a fresh NodeReady Unknown", policy: skip,
			cached: bothSigns(time.Minute, 10*time.Minute), live: bothSigns(time.Minute, 10*time.Minute),
			want: WithinGrace, requeueAt: tNow.Add(threshold - time.Minute), cacheReads: 1,
		},
		{
			name: "a fresh taint holds back an old NodeReady Unknown", policy: skip,
			cached: bothSigns(10*time.Minute, time.Minute), live: bothSigns(10*time.Minute, time.Minute),
			want: WithinGrace, requeueAt: tNow.Add(threshold - time.Minute), cacheReads: 1,
		},
		{
			name: "with both signs past the threshold the more recent one names the class", policy: skip,
			cached: bothSigns(6*time.Minute, 10*time.Minute), live: bothSigns(6*time.Minute, 10*time.Minute),
			want: NodeReadyUnknown, cacheReads: 1, liveReads: 1,
		},
		{
			name: "NodeReady False past the threshold: the kubelet still runs and can stop the pod", policy: skip,
			cached: tNode("lost", corev1.ConditionFalse, 10*time.Minute, false), live: unreachable,
			want: WithinGrace, requeueAt: graceEnds, cacheReads: 1,
		},
		{
			name: "NodeReady False outweighs an unreachable taint", policy: skip,
			cached: tNode("lost", corev1.ConditionFalse, 10*time.Minute, true), live: unreachable,
			want: WithinGrace, requeueAt: graceEnds, cacheReads: 1,
		},
		{name: "a Ready node can still stop the pod", policy: skip, cached: recovered, live: unreachable, want: WithinGrace, requeueAt: graceEnds, cacheReads: 1},
		{
			name: "NodeReady True outweighs an unreachable taint", policy: skip,
			cached: tNode("lost", corev1.ConditionTrue, 10*time.Minute, true), live: unreachable,
			want: WithinGrace, requeueAt: graceEnds, cacheReads: 1,
		},
		{name: "a gone node carries no age", policy: skip, nodeName: "gone", want: WithinGrace, requeueAt: graceEnds, cacheReads: 1},
		{name: "a cache behind a recovered node is corrected by the live read", policy: skip, cached: unreachable, live: recovered, want: WithinGrace, requeueAt: graceEnds, cacheReads: 1, liveReads: 1},
		{
			name: "a live outage younger than the threshold requeues when it comes of age", policy: skip,
			cached: unreachable, live: tNode("lost", corev1.ConditionUnknown, 2*time.Minute, false),
			want: WithinGrace, requeueAt: tNow.Add(threshold - 2*time.Minute), cacheReads: 1, liveReads: 1,
		},
		{name: "a cache behind a newly unreachable node only delays the pod", policy: skip, cached: recovered, live: unreachable, want: WithinGrace, requeueAt: graceEnds, cacheReads: 1},
		{name: "a node the API server no longer holds carries no age", policy: skip, cached: unreachable, want: WithinGrace, requeueAt: graceEnds, cacheReads: 1, liveReads: 1},
		{
			name: "a failed cached read keeps the pod waiting and is passed on", policy: skip,
			cached: unreachable, live: unreachable, cacheErr: errors.New("cache not synced"),
			want: WithinGrace, requeueAt: graceEnds, cacheReads: 1,
		},
		{
			name: "a failed live read keeps the pod waiting and is passed on", policy: skip,
			cached: unreachable, live: unreachable, liveErr: errors.New("apiserver unavailable"),
			want: WithinGrace, requeueAt: graceEnds, cacheReads: 1, liveReads: 1,
		},
		{name: "a pod bound to no node reads nothing", policy: skip, want: WithinGrace, requeueAt: graceEnds},
		{
			name: "finalizers keep the pod waiting without a read", policy: skip,
			cached: unreachable, live: unreachable, finalizers: []string{"example.com/keep"},
			want: WithinGrace, requeueAt: graceEnds,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodeName := tc.nodeName
			var cachedObjs, liveObjs []client.Object
			if tc.cached != nil {
				cachedObjs, nodeName = append(cachedObjs, tc.cached), tc.cached.Name
			}
			if tc.live != nil {
				liveObjs, nodeName = append(liveObjs, tc.live), tc.live.Name
			}
			deleted := deletedAt
			if !tc.deletedAt.IsZero() {
				deleted = tc.deletedAt
			}
			pod := tTerminatingPod(nodeName, deleted)
			pod.Finalizers = tc.finalizers
			cache := &tNodeReads{Reader: tReader(t, cachedObjs...), err: tc.cacheErr}
			live := &tNodeReads{Reader: tReader(t, liveObjs...), err: tc.liveErr}

			got := tClassify(types.NodeReaders{Cache: cache, Live: live}, pod, tc.policy)
			if got.Kind != tc.want {
				t.Fatalf("kind: got %s want %s", got.Kind, tc.want)
			}
			if !got.RequeueAt.Equal(tc.requeueAt) {
				t.Errorf("requeueAt: got %v want %v", got.RequeueAt, tc.requeueAt)
			}
			if cache.reads != tc.cacheReads || live.reads != tc.liveReads {
				t.Errorf("node reads: cache %d, live %d; want cache %d, live %d", cache.reads, live.reads, tc.cacheReads, tc.liveReads)
			}
			if failed := tc.cacheErr != nil || tc.liveErr != nil; (got.NodeReadErr != nil) != failed {
				t.Errorf("NodeReadErr: got %v, want one only when a read failed", got.NodeReadErr)
			}
			if got.Kind.Actionable() && (got.Overdue != tNow.Sub(deleted) || got.NodeName != nodeName) {
				t.Errorf("overdue/node: got %v/%q want %v/%q, negative inside the grace period", got.Overdue, got.NodeName, tNow.Sub(deleted), nodeName)
			}
			if got.GraceSkipped != got.Kind.Actionable() {
				t.Errorf("GraceSkipped: got %v, want it set exactly when a pod inside its grace period may go", got.GraceSkipped)
			}
		})
	}
}

// TestAllPodsTerminatingOverdue: a teardown reads as stalled only when
// every pod in it is deleted and past the deadline the pod itself
// declared. One live pod, one pod inside its grace, or no pods at all
// is a teardown the drive still owns or has finished.
func TestAllPodsTerminatingOverdue(t *testing.T) {
	overdue := tTerminatingPod("node-a", tNow.Add(-time.Minute))
	inGrace := tTerminatingPod("node-a", tNow.Add(time.Minute))
	atDeadline := tTerminatingPod("node-a", tNow)
	live := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "live"}, Spec: corev1.PodSpec{NodeName: "node-a"}}
	for _, tc := range []struct {
		name string
		pods []*corev1.Pod
		want bool
	}{
		{name: "no pods", pods: nil, want: false},
		{name: "one pod past its deadline", pods: []*corev1.Pod{overdue}, want: true},
		{name: "every pod past its deadline", pods: []*corev1.Pod{overdue, overdue}, want: true},
		{name: "a pod inside its grace", pods: []*corev1.Pod{overdue, inGrace}, want: false},
		{name: "a pod exactly at its deadline", pods: []*corev1.Pod{atDeadline}, want: false},
		{name: "a pod not Terminating", pods: []*corev1.Pod{overdue, live}, want: false},
		{name: "a nil entry", pods: []*corev1.Pod{overdue, nil}, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := AllPodsTerminatingOverdue(tc.pods, tNow); got != tc.want {
				t.Fatalf("AllPodsTerminatingOverdue = %v, want %v", got, tc.want)
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
		want       TerminatingClass
		actionable bool
	}{
		{
			name:     "unscheduled pod has no node whose death could be proven",
			nodeName: "",
			want:     Unscheduled,
		},
		{
			name:     "Ready=True is the kubelet-slow case",
			node:     tNode("live", corev1.ConditionTrue, 10*time.Second, false),
			nodeName: "live",
			want:     NodeHealthy,
		},
		{
			name:     "Ready=True vetoes a stale unreachable taint",
			node:     tNode("recovered", corev1.ConditionTrue, 10*time.Minute, true),
			nodeName: "recovered",
			want:     NodeHealthy,
		},
		{
			name:     "NotReady younger than the threshold is a blip",
			node:     tNode("dying", corev1.ConditionFalse, 2*time.Minute, false),
			nodeName: "dying",
			want:     NodeNotDeadLongEnough,
		},
		{
			name:       "unreachable taint older than the threshold",
			node:       tNode("dead", corev1.ConditionUnknown, 10*time.Minute, true),
			nodeName:   "dead",
			want:       NodeUnreachableTaint,
			actionable: true,
		},
		{
			name:       "NotReady older than the threshold",
			node:       tNode("dead", corev1.ConditionFalse, 10*time.Minute, false),
			nodeName:   "dead",
			want:       NodeNotReady,
			actionable: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var objs []client.Object
			if tc.node != nil {
				objs = append(objs, tc.node)
			}
			pod := tTerminatingPod(tc.nodeName, tNow.Add(-10*time.Minute))
			got := NodeDeath(context.Background(), tReader(t, objs...), pod, tPolicy(), tNow, 0)
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
			if got := SingleLiveNode(tc.pods); got != tc.want {
				t.Errorf("SingleLiveNode: got %q want %q", got, tc.want)
			}
		})
	}
}

// TestReadSilentPod holds the pre-sweep reading to the sweep's own arms
// and to what each reading means for the name: no policy reads nothing
// and holds; a pod nobody asked to delete is freed on node death alone;
// a Terminating pod is freed only past its overdue window, or inside it
// under SkipGracePeriodOnUnreachableNode once its node has been
// unreachable for the threshold, and never while a finalizer pins it; a
// live node frees nothing — and holds nothing either unless the pod's
// own phase says Unknown, because a pod silent only by its withdrawn
// Ready on a node posting Ready is the kubelet's lag behind a
// readiness-gate write. Only a pod inside its grace period has its Node
// read from the cache first, so a cache that still shows a recovered
// node unreachable frees no pod nobody has deleted and no overdue pod.
func TestReadSilentPod(t *testing.T) {
	policy := &types.ForceDeletePolicy{OverdueSlack: time.Minute, NodeUnreachableThreshold: 5 * time.Minute}
	skip := *policy
	skip.SkipGracePeriodOnUnreachableNode = true
	nodes := func(recovered *corev1.Node) []client.Object {
		return []client.Object{
			tNode("live", corev1.ConditionTrue, 10*time.Second, false),
			tNode("dying", corev1.ConditionUnknown, time.Minute, false),
			tNode("lost", corev1.ConditionUnknown, 10*time.Minute, false),
			recovered,
		}
	}
	// The cache and the API server agree on every node but one, which
	// recovered: the cache still shows it unreachable, the API server
	// shows it Ready.
	readers := types.NodeReaders{
		Cache: tReader(t, nodes(tNode("recovered", corev1.ConditionUnknown, 10*time.Minute, true))...),
		Live:  tReader(t, nodes(tNode("recovered", corev1.ConditionTrue, 10*time.Second, false))...),
	}
	quiet := func(phase corev1.PodPhase, node string, deleted *metav1.Time, finalizers ...string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-0", DeletionTimestamp: deleted, Finalizers: finalizers},
			Spec:       corev1.PodSpec{NodeName: node},
			Status:     corev1.PodStatus{Phase: phase},
		}
	}
	overdue := metav1.NewTime(tNow.Add(-10 * time.Minute))
	for _, tc := range []struct {
		name   string
		pod    *corev1.Pod
		policy *types.ForceDeletePolicy
		want   TerminatingClass
		held   bool
	}{
		{"no policy", quiet(corev1.PodUnknown, "gone", nil), nil, NotConfigured, true},
		{"node gone", quiet(corev1.PodUnknown, "gone", nil), policy, NodeGone, false},
		{"node live, phase Unknown", quiet(corev1.PodUnknown, "live", nil), policy, NodeHealthy, true},
		{"node live, Ready withdrawn", quiet(corev1.PodRunning, "live", nil), policy, NodeHealthy, false},
		{"node dying, Ready withdrawn", quiet(corev1.PodRunning, "dying", nil), policy, NodeNotDeadLongEnough, true},
		{"unscheduled, Ready withdrawn", quiet(corev1.PodRunning, "", nil), policy, Unscheduled, false},
		{"terminating, node gone, overdue", quiet(corev1.PodUnknown, "gone", &overdue), policy, NodeGone, false},
		{"terminating, node gone, pinned", quiet(corev1.PodUnknown, "gone", &overdue, "example.com/keep"), policy, ForeignFinalizers, true},
		{"terminating, node gone, within grace", quiet(corev1.PodRunning, "gone", ptrTime(tNow)), policy, WithinGrace, true},
		{"terminating within grace, node unreachable past the threshold", quiet(corev1.PodRunning, "lost", ptrTime(tNow)), policy, WithinGrace, true},
		{"terminating within grace, node unreachable past the threshold, grace skipped", quiet(corev1.PodRunning, "lost", ptrTime(tNow)), &skip, NodeReadyUnknown, false},
		{"not deleted, the cache still shows a recovered node unreachable", quiet(corev1.PodUnknown, "recovered", nil), &skip, NodeHealthy, true},
		{"terminating, overdue, the cache still shows a recovered node unreachable", quiet(corev1.PodUnknown, "recovered", &overdue), &skip, NodeHealthy, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReadUnreportedPod(context.Background(), readers, tc.pod, tc.policy, tNow)
			if err != nil {
				t.Fatalf("ReadUnreportedPod: %v", err)
			}
			if got != tc.want {
				t.Errorf("ReadUnreportedPod = %s, want %s", got, tc.want)
			}
			if held := UnreportedPodHeld(tc.pod, got); held != tc.held {
				t.Errorf("UnreportedPodHeld = %v, want %v", held, tc.held)
			}
		})
	}
}

func ptrTime(at time.Time) *metav1.Time {
	t := metav1.NewTime(at)
	return &t
}

// TestSilentKubeletTargetPods: the targets whose kubelet has stopped
// reporting them, in name order. Phase Unknown counts with or without a
// policy. A Running pod whose Ready is False while its own report says
// Ready — ContainersReady True, every gate satisfied — counts only under
// a policy, which is what allows the Node read that separates a node that
// stopped reporting from the kubelet's lag; a pod whose gate is
// unsatisfied, whose containers are not ready, or which is PodReady never
// counts.
func TestSilentKubeletTargetPods(t *testing.T) {
	policy := &types.ForceDeletePolicy{OverdueSlack: time.Minute, NodeUnreachableThreshold: 5 * time.Minute}
	const gate = corev1.PodConditionType("example.com/serving")
	pod := func(name string, phase corev1.PodPhase) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Status:     corev1.PodStatus{Phase: phase},
		}
	}
	withdrawn := func(name string, containersReady, podReady, gateSatisfied bool) *corev1.Pod {
		status := func(ok bool) corev1.ConditionStatus {
			if ok {
				return corev1.ConditionTrue
			}
			return corev1.ConditionFalse
		}
		p := pod(name, corev1.PodRunning)
		p.Spec.ReadinessGates = []corev1.PodReadinessGate{{ConditionType: gate}}
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: status(containersReady)},
			{Type: corev1.PodReady, Status: status(podReady)},
			{Type: gate, Status: status(gateSatisfied)},
		}
		return p
	}
	names := func(targets ...string) map[string]struct{} {
		set := make(map[string]struct{}, len(targets))
		for _, t := range targets {
			set[t] = struct{}{}
		}
		return set
	}
	got := UnreportedTargetPods(
		[]*corev1.Pod{
			pod("c", corev1.PodUnknown),
			pod("a", corev1.PodUnknown),
			pod("b", corev1.PodRunning),
			pod("d", corev1.PodUnknown),
		},
		names("a", "b", "c"), nil,
	)
	if len(got) != 2 || got[0].Name != "a" || got[1].Name != "c" {
		t.Fatalf("want the Unknown targets in name order [a c], got %v", got)
	}
	if UnreportedTargetPods(nil, names("a"), nil) != nil {
		t.Error("no pods: want nil")
	}
	if UnreportedTargetPods([]*corev1.Pod{pod("a", corev1.PodUnknown)}, nil, nil) != nil {
		t.Error("no targets: want nil")
	}

	shapes := []*corev1.Pod{
		withdrawn("withdrawn", true, false, true),
		withdrawn("draining", true, false, false),
		withdrawn("probe-failing", false, false, true),
		withdrawn("ready", true, true, true),
	}
	all := names("withdrawn", "draining", "probe-failing", "ready")
	if got := UnreportedTargetPods(shapes, all, nil); got != nil {
		t.Errorf("without a policy no Running pod is read as silent, got %v", got)
	}
	got = UnreportedTargetPods(shapes, all, policy)
	if len(got) != 1 || got[0].Name != "withdrawn" {
		t.Fatalf("under a policy only the withdrawn-Ready shape is silent, got %v", got)
	}
}

// TestUnreportedPod_IsTheTargetSweepsReadingOfOnePod: the per-pod reading
// behind UnreportedTargetPods, and the reading over a set, agree with the
// target sweep on every shape with and without a policy. Phase Unknown is
// unreported either way; a Running pod whose Ready is False while its own
// report says Ready only under a policy; a draining, probe-failing, Ready,
// Pending or absent pod never.
func TestUnreportedPod_IsTheTargetSweepsReadingOfOnePod(t *testing.T) {
	policy := &types.ForceDeletePolicy{OverdueSlack: time.Minute, NodeUnreachableThreshold: 5 * time.Minute}
	const gate = corev1.PodConditionType("example.com/serving")
	status := func(ok bool) corev1.ConditionStatus {
		if ok {
			return corev1.ConditionTrue
		}
		return corev1.ConditionFalse
	}
	shape := func(name string, phase corev1.PodPhase, containersReady, podReady, gateSatisfied bool) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       corev1.PodSpec{ReadinessGates: []corev1.PodReadinessGate{{ConditionType: gate}}},
			Status: corev1.PodStatus{Phase: phase, Conditions: []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: status(containersReady)},
				{Type: corev1.PodReady, Status: status(podReady)},
				{Type: gate, Status: status(gateSatisfied)},
			}},
		}
	}
	shapes := []*corev1.Pod{
		shape("unknown", corev1.PodUnknown, false, false, false),
		shape("ready-taken-away", corev1.PodRunning, true, false, true),
		shape("draining", corev1.PodRunning, true, false, false),
		shape("probe-failing", corev1.PodRunning, false, false, true),
		shape("ready", corev1.PodRunning, true, true, true),
		shape("pending", corev1.PodPending, false, false, false),
		nil,
	}
	all := map[string]struct{}{}
	for _, pod := range shapes {
		if pod != nil {
			all[pod.Name] = struct{}{}
		}
	}
	unreported := map[*types.ForceDeletePolicy]map[string]bool{
		nil:    {"unknown": true},
		policy: {"unknown": true, "ready-taken-away": true},
	}
	for _, p := range []*types.ForceDeletePolicy{nil, policy} {
		for _, pod := range shapes {
			name := "nil"
			if pod != nil {
				name = pod.Name
			}
			got := UnreportedPod(pod, p)
			if want := unreported[p][name]; got != want {
				t.Errorf("policy=%v %s: UnreportedPod = %v, want %v", p != nil, name, got, want)
			}
			if swept := len(UnreportedTargetPods([]*corev1.Pod{pod}, all, p)) == 1; swept != got {
				t.Errorf("policy=%v %s: the target sweep reads the pod as unreported=%v, UnreportedPod says %v", p != nil, name, swept, got)
			}
		}
		if !AnyUnreportedPod(shapes, p) {
			t.Errorf("policy=%v: AnyUnreportedPod over every shape = false, want true", p != nil)
		}
		reporting := shapes[2:]
		if AnyUnreportedPod(reporting, p) || UnreportedTargetPods(reporting, all, p) != nil {
			t.Errorf("policy=%v: the reporting shapes read as unreported", p != nil)
		}
	}
	if AnyUnreportedPod(nil, policy) {
		t.Error("no pods: AnyUnreportedPod = true, want false")
	}
}

// TestPodNeverRanAContainer: a pod reads as one its kubelet has run
// nothing of only while it is Pending, not on its way out, and no
// container status, init or regular, shows a run that started, ended or
// restarted; a bound pod with no status at all is the shape a kubelet
// that stopped before the first start leaves behind.
func TestPodNeverRanAContainer(t *testing.T) {
	started := true
	running := corev1.ContainerStatus{Name: "runner", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}
	cases := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{name: "nil", pod: nil, want: false},
		{name: "bound with no status yet", pod: &corev1.Pod{Spec: corev1.PodSpec{NodeName: "node-a"}, Status: corev1.PodStatus{Phase: corev1.PodPending}}, want: true},
		{name: "no phase written yet", pod: &corev1.Pod{Spec: corev1.PodSpec{NodeName: "node-a"}}, want: true},
		{name: "image still pulling", pod: &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{
			Name: "runner", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}}}}}, want: true},
		{name: "init container ran", pod: &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending, InitContainerStatuses: []corev1.ContainerStatus{{
			Name: "init", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}}}}, want: false},
		{name: "running", pod: &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{running}}}, want: false},
		{name: "started but the phase still Pending", pod: &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{
			Name: "runner", Started: &started, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}}}}, want: false},
		{name: "restarted once", pod: &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{
			Name: "runner", RestartCount: 1, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}}}}, want: false},
		{name: "a previous run on record", pod: &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{
			Name: "runner", LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}}}}}, want: false},
		{name: "terminal", pod: &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed}}, want: false},
		{name: "unknown to its kubelet", pod: &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodUnknown}}, want: false},
		{name: "on its way out", pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &metav1.Time{Time: time.Now()}}, Status: corev1.PodStatus{Phase: corev1.PodPending}}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := PodNeverRanAContainer(tc.pod); got != tc.want {
				t.Fatalf("PodNeverRanAContainer = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestNodeDeathScreened: the cache screens and the live Node decides. A
// node the cache shows Ready, or stopped for less than the threshold, is
// read from the cache alone with its recheck time; a node the cache
// lacks reads gone with no live read; only a cached node past the
// threshold is read live, and the live copy's answer stands: stopped,
// Ready again, or gone. A cache that cannot be read is an error, not a reading.
func TestNodeDeathScreened(t *testing.T) {
	policy := &types.ForceDeletePolicy{NodeUnreachableThreshold: 5 * time.Minute}
	lost := tNode("n", corev1.ConditionUnknown, 10*time.Minute, false)
	for _, tc := range []struct {
		name         string
		node         string
		cached, live *corev1.Node
		cacheErr     error
		want         TerminatingClass
		requeueAt    time.Time
		liveReads    int
	}{
		{name: "unbound pod", want: Unscheduled},
		{name: "the cache lacks the node", node: "n", live: lost, want: NodeGone},
		{name: "the cache shows Ready", node: "n", cached: tNode("n", corev1.ConditionTrue, time.Hour, false), live: lost, want: NodeHealthy, requeueAt: tNow.Add(5 * time.Minute)},
		{name: "the cache shows the node stopped inside the threshold", node: "n", cached: tNode("n", corev1.ConditionUnknown, time.Minute, false), live: lost, want: NodeNotDeadLongEnough, requeueAt: tNow.Add(4 * time.Minute)},
		{name: "the cache shows the threshold passed and the live node agrees", node: "n", cached: lost, live: lost, want: NodeNotReady, liveReads: 1},
		{name: "the cache shows the threshold passed and the live node carries the unreachable taint", node: "n", cached: lost, live: tNode("n", corev1.ConditionUnknown, 10*time.Minute, true), want: NodeUnreachableTaint, liveReads: 1},
		{name: "the cache shows the threshold passed and the live node reports Ready", node: "n", cached: lost, live: tNode("n", corev1.ConditionTrue, 10*time.Second, false), want: NodeHealthy, requeueAt: tNow.Add(5 * time.Minute), liveReads: 1},
		{name: "the cache shows the threshold passed and the live node is gone", node: "n", cached: lost, want: NodeGone, liveReads: 1},
		{name: "the cache cannot be read", node: "n", cached: lost, live: lost, cacheErr: errors.New("cache stopped"), want: NodeReadError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objs := func(n *corev1.Node) []client.Object {
				if n == nil {
					return nil
				}
				return []client.Object{n}
			}
			cache := &tNodeReads{Reader: tReader(t, objs(tc.cached)...), err: tc.cacheErr}
			live := &tNodeReads{Reader: tReader(t, objs(tc.live)...)}
			pod := &corev1.Pod{Spec: corev1.PodSpec{NodeName: tc.node}}

			got := NodeDeathScreened(context.Background(), types.NodeReaders{Cache: cache, Live: live}, pod, policy, tNow)
			if got.Kind != tc.want {
				t.Fatalf("NodeDeathScreened = %s, want %s", got.Kind, tc.want)
			}
			if !got.RequeueAt.Equal(tc.requeueAt) {
				t.Errorf("RequeueAt = %v, want %v", got.RequeueAt, tc.requeueAt)
			}
			if live.reads != tc.liveReads {
				t.Errorf("live node reads = %d, want %d", live.reads, tc.liveReads)
			}
			if (got.NodeReadErr != nil) != (tc.cacheErr != nil) {
				t.Errorf("NodeReadErr = %v, want one exactly when the cache fails (%v)", got.NodeReadErr, tc.cacheErr)
			}
		})
	}
}
