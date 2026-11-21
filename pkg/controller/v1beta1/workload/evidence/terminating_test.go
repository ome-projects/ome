package evidence

// The force-delete predicate, branch by branch. Only four classes are
// actionable, and each refusal is a distinct reason OME keeps waiting on
// the pod's kubelet. The escalation that acts on these classes is tested
// with the escalation.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	ometesting "sigs.k8s.io/ome/pkg/utils/testing"
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
	return StuckTerminating(context.Background(), readers, pod, policy, types.Lifecycle{}, tNow)
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
			if got := AllPodsTerminatingOverdue(tc.pods, types.Lifecycle{}, tNow); got != tc.want {
				t.Fatalf("AllPodsTerminatingOverdue = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestAllPodsTerminatingOverdueFor: a teardown reads as one nothing has
// removed only when every pod in it has stood past its deletion time for
// the whole wait. A pod read a moment after its deletion time moved is
// not one yet. A wait of zero asks only for a deletion time behind the
// clock.
func TestAllPodsTerminatingOverdueFor(t *testing.T) {
	const wait = 5 * time.Second
	aMomentPast := tTerminatingPod("node-a", tNow.Add(-500*time.Millisecond))
	shortOfTheWait := tTerminatingPod("node-a", tNow.Add(-wait+time.Millisecond))
	theWaitPast := tTerminatingPod("node-a", tNow.Add(-wait))
	minutesPast := tTerminatingPod("node-a", tNow.Add(-10*time.Minute))
	inGrace := tTerminatingPod("node-a", tNow.Add(time.Minute))
	atItsTime := tTerminatingPod("node-a", tNow)
	running := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "running"}, Spec: corev1.PodSpec{NodeName: "node-a"}}
	for _, tc := range []struct {
		name string
		pods []*corev1.Pod
		wait time.Duration
		want bool
	}{
		{name: "no pods", pods: nil, wait: wait, want: false},
		{name: "a pod a moment past its deletion time", pods: []*corev1.Pod{aMomentPast}, wait: wait, want: false},
		{name: "a pod one millisecond short of the wait", pods: []*corev1.Pod{shortOfTheWait}, wait: wait, want: false},
		{name: "a pod past its deletion time by exactly the wait", pods: []*corev1.Pod{theWaitPast}, wait: wait, want: true},
		{name: "a pod minutes past its deletion time", pods: []*corev1.Pod{minutesPast}, wait: wait, want: true},
		{name: "every pod past the wait", pods: []*corev1.Pod{theWaitPast, minutesPast}, wait: wait, want: true},
		{name: "one pod of two short of the wait", pods: []*corev1.Pod{minutesPast, shortOfTheWait}, wait: wait, want: false},
		{name: "a pod inside its grace", pods: []*corev1.Pod{minutesPast, inGrace}, wait: wait, want: false},
		{name: "a pod exactly at its deletion time", pods: []*corev1.Pod{atItsTime}, wait: wait, want: false},
		{name: "a pod not Terminating", pods: []*corev1.Pod{minutesPast, running}, wait: wait, want: false},
		{name: "a nil entry", pods: []*corev1.Pod{minutesPast, nil}, wait: wait, want: false},
		{name: "no wait, a pod a moment past its deletion time", pods: []*corev1.Pod{aMomentPast}, wait: 0, want: true},
		{name: "no wait, a pod exactly at its deletion time", pods: []*corev1.Pod{atItsTime}, wait: 0, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := AllPodsTerminatingOverdueFor(tc.pods, types.Lifecycle{}, tNow, tc.wait); got != tc.want {
				t.Fatalf("AllPodsTerminatingOverdueFor = %v, want %v", got, tc.want)
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
			got, err := ReadUnreportedPod(context.Background(), readers, tc.pod, tc.policy, types.Lifecycle{}, tNow)
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

// The pod of the test below is the old pod of a move. OME deletes it with
// the grace its template names, the kubelet stops it and frees what it
// held, and the kubelet then removes it.
const (
	// oldPodGrace is the time the pod template gives a pod to stop.
	oldPodGrace = 60 * time.Second
	// oldPodStopsIn is how long the kubelet takes to stop the pod and
	// clean up after it.
	oldPodStopsIn = 2 * time.Second
	// oldPodAnswerWait bounds every wait on the API server.
	oldPodAnswerWait = time.Minute
	// oldPodHolder is a finalizer another controller put on the pod.
	oldPodHolder = "example.com/hold"
	// oldPodRequeue is the operation requeue the chart sets: how long a
	// pod stands past its deletion time before OME says it stays.
	oldPodRequeue = 5 * time.Second
)

// TestAllPodsTerminatingOverdue_ReadsAPodBetweenTheTwoWritesOfItsLastDelete
// asks a real API server what a pod looks like at the end of a healthy
// teardown.
//
// A kubelet ends every teardown with one more delete call, with a grace
// of zero and the pod's UID. The API server answers that call in two
// writes. The first moves the pod's deletion time from 60 seconds ahead
// to the present second and sets the grace to zero. The second removes
// the pod. A watcher sees both writes. A pod that a finalizer holds
// stays in the state the first write left.
//
// So a pod that used 2 of its 60 seconds reads, for a moment, as a pod
// whose deletion time is behind the clock, and AllPodsTerminatingOverdue
// reports it. AllPodsTerminatingOverdueFor, asked whether the pod has
// stood there for a requeue, does not.
//
// The test needs the control plane binaries KUBEBUILDER_ASSETS names and
// is skipped without them.
func TestAllPodsTerminatingOverdue_ReadsAPodBetweenTheTwoWritesOfItsLastDelete(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: export KUBEBUILDER_ASSETS=\"$(bin/setup-envtest use <kubernetes-version> -p path)\"")
	}
	_, cfg := ometesting.StartOnBusyHost(t, func() *envtest.Environment { return &envtest.Environment{} })
	api, err := client.NewWithWatch(cfg, client.Options{})
	if err != nil {
		t.Fatalf("build the client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*oldPodAnswerWait)
	defer cancel()

	t.Run("a watcher sees the deletion time move before the pod is gone", func(t *testing.T) {
		pod := createBoundPod(ctx, t, api, "old-pod-leaving")
		events, err := api.Watch(ctx, &corev1.PodList{}, client.InNamespace(pod.Namespace), client.MatchingFields{"metadata.name": pod.Name})
		if err != nil {
			t.Fatalf("watch the pod: %v", err)
		}
		defer events.Stop()
		asked := deleteAsTheEngineDoes(ctx, t, api, pod)
		time.Sleep(oldPodStopsIn)
		sendTheKubeletsLastDelete(ctx, t, api, pod)

		rewritten := podBetweenTheTwoWrites(t, events)
		requireDeletionTimeBehindTheClock(t, rewritten, asked)
	})

	t.Run("a pod that a finalizer holds stays as the first write left it", func(t *testing.T) {
		pod := createBoundPod(ctx, t, api, "old-pod-held", oldPodHolder)
		asked := deleteAsTheEngineDoes(ctx, t, api, pod)
		time.Sleep(oldPodStopsIn)
		sendTheKubeletsLastDelete(ctx, t, api, pod)

		held := &corev1.Pod{}
		if err := api.Get(ctx, client.ObjectKeyFromObject(pod), held); err != nil {
			t.Fatalf("the finalizer keeps the pod after the last delete: %v", err)
		}
		requireDeletionTimeBehindTheClock(t, held, asked)

		held.Finalizers = nil
		if err := api.Update(ctx, held); err != nil {
			t.Fatalf("release the finalizer: %v", err)
		}
		if err := api.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
			t.Fatalf("the pod leaves with its finalizer: got %v, want not found", err)
		}
	})
}

// createBoundPod creates a pod bound to a node, with the template's grace
// and the finalizers named. No kubelet runs it: the test plays the
// kubelet's calls itself.
func createBoundPod(ctx context.Context, t *testing.T, api client.Client, name string, finalizers ...string) *corev1.Pod {
	t.Helper()
	grace := int64(oldPodGrace / time.Second)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: name, Finalizers: finalizers},
		Spec: corev1.PodSpec{
			NodeName:                      "node-a",
			TerminationGracePeriodSeconds: &grace,
			Containers:                    []corev1.Container{{Name: "main", Image: "registry.example.com/runtime:v1"}},
		},
	}
	if err := api.Create(ctx, pod); err != nil {
		t.Fatalf("create pod %s: %v", name, err)
	}
	return pod
}

// deleteAsTheEngineDoes deletes the pod with no grace of its own, as the
// move deletes its old pod, and reports when it asked. The API server
// answers with the pod's own grace: the deletion time stands a whole
// grace ahead, and the engine reads the pod as inside it.
func deleteAsTheEngineDoes(ctx context.Context, t *testing.T, api client.Client, pod *corev1.Pod) time.Time {
	t.Helper()
	asked := time.Now()
	if err := api.Delete(ctx, pod); err != nil {
		t.Fatalf("delete pod %s: %v", pod.Name, err)
	}
	leaving := &corev1.Pod{}
	if err := api.Get(ctx, client.ObjectKeyFromObject(pod), leaving); err != nil {
		t.Fatalf("read pod %s after its delete: %v", pod.Name, err)
	}
	if leaving.DeletionTimestamp == nil || leaving.DeletionGracePeriodSeconds == nil {
		t.Fatalf("a deleted pod carries a deletion time and a grace; got %v and %v", leaving.DeletionTimestamp, leaving.DeletionGracePeriodSeconds)
	}
	if got := time.Duration(*leaving.DeletionGracePeriodSeconds) * time.Second; got != oldPodGrace {
		t.Fatalf("the first delete keeps the pod's own grace: got %s, want %s", got, oldPodGrace)
	}
	if ahead := leaving.DeletionTimestamp.Sub(asked); ahead < oldPodGrace-2*time.Second || ahead > oldPodGrace+2*time.Second {
		t.Fatalf("the first delete sets the deletion time one grace ahead: it stands %s after the request, want about %s", ahead, oldPodGrace)
	}
	if AllPodsTerminatingOverdue([]*corev1.Pod{leaving}, types.Lifecycle{}, time.Now()) {
		t.Fatalf("a pod inside its grace is not overdue; deletion time %s", leaving.DeletionTimestamp)
	}
	t.Logf("delete asked at %s: deletion time %s, grace %ds", asked.Format(time.RFC3339Nano), leaving.DeletionTimestamp.Format(time.RFC3339), *leaving.DeletionGracePeriodSeconds)
	return asked
}

// sendTheKubeletsLastDelete sends what a kubelet sends once the pod has
// stopped and its resources are freed: a delete with a grace of zero that
// names the pod's UID.
func sendTheKubeletsLastDelete(ctx context.Context, t *testing.T, api client.Client, pod *corev1.Pod) {
	t.Helper()
	uid := pod.UID
	if err := api.Delete(ctx, pod, client.GracePeriodSeconds(0), client.Preconditions{UID: &uid}); err != nil {
		t.Fatalf("the last delete of pod %s: %v", pod.Name, err)
	}
	t.Logf("last delete sent at %s", time.Now().Format(time.RFC3339Nano))
}

// podBetweenTheTwoWrites reads the watch until the pod is gone and
// returns the pod as the write before the removal left it: still there,
// with a grace of zero.
func podBetweenTheTwoWrites(t *testing.T, events watch.Interface) *corev1.Pod {
	t.Helper()
	var rewritten *corev1.Pod
	timeout := time.After(oldPodAnswerWait)
	for {
		select {
		case event, open := <-events.ResultChan():
			if !open {
				t.Fatalf("the watch closed before the pod was gone")
			}
			pod, isPod := event.Object.(*corev1.Pod)
			if !isPod {
				t.Fatalf("the watch sent %T, want a pod", event.Object)
			}
			t.Logf("watch %s: %s", event.Type, deletionOf(pod))
			if event.Type == watch.Modified && pod.DeletionGracePeriodSeconds != nil && *pod.DeletionGracePeriodSeconds == 0 {
				rewritten = pod
			}
			if event.Type != watch.Deleted {
				continue
			}
			if rewritten == nil {
				t.Fatalf("the pod was removed with no write before it that set the grace to zero: the last delete was one write")
			}
			return rewritten
		case <-timeout:
			t.Fatalf("the pod was not removed within %s of its last delete", oldPodAnswerWait)
		}
	}
}

// deletionOf writes a pod's deletion time and grace for the test log.
func deletionOf(pod *corev1.Pod) string {
	if pod.DeletionTimestamp == nil || pod.DeletionGracePeriodSeconds == nil {
		return "not deleted"
	}
	return fmt.Sprintf("deletion time %s, grace %ds", pod.DeletionTimestamp.Format(time.RFC3339), *pod.DeletionGracePeriodSeconds)
}

// requireDeletionTimeBehindTheClock fails unless the pod stands as the
// first write of the last delete leaves it: a grace of zero, and a
// deletion time no earlier than the second the engine asked and no later
// than now. The engine's reading then takes a pod that used 2 of its 60
// seconds for a pod past its deletion time, and not for one that has
// stood past it for a requeue.
func requireDeletionTimeBehindTheClock(t *testing.T, pod *corev1.Pod, asked time.Time) {
	t.Helper()
	now := time.Now()
	if pod.DeletionTimestamp == nil || pod.DeletionGracePeriodSeconds == nil || *pod.DeletionGracePeriodSeconds != 0 {
		t.Fatalf("the last delete leaves a deletion time and a grace of zero; got %v and %v", pod.DeletionTimestamp, pod.DeletionGracePeriodSeconds)
	}
	deletion := pod.DeletionTimestamp.Time
	if deletion.Before(asked.Truncate(time.Second)) || deletion.After(now) {
		t.Fatalf("the deletion time moved to %s: want it between the first request at %s and now, %s",
			deletion.Format(time.RFC3339), asked.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	}
	t.Logf("read at %s: deletion time %s, %s behind the clock, %s after the first request",
		now.Format(time.RFC3339Nano), deletion.Format(time.RFC3339), now.Sub(deletion).Round(time.Millisecond), deletion.Sub(asked.Truncate(time.Second)))
	if !AllPodsTerminatingOverdue([]*corev1.Pod{pod}, types.Lifecycle{}, now) {
		t.Fatalf("the engine's reading takes the pod for one past its deletion time; deletion time %s, read at %s",
			deletion.Format(time.RFC3339), now.Format(time.RFC3339Nano))
	}
	if AllPodsTerminatingOverdueFor([]*corev1.Pod{pod}, types.Lifecycle{}, now, oldPodRequeue) {
		t.Fatalf("a pod read %s after its deletion time moved has not stood past it for a requeue of %s",
			now.Sub(deletion).Round(time.Millisecond), oldPodRequeue)
	}
}

// tLeaderGoneLifecycle is a replica's lifecycle with leaderGoneGracePeriod
// at grace, and with the field unset when grace is zero.
func tLeaderGoneLifecycle(grace time.Duration) types.Lifecycle {
	if grace == 0 {
		return types.Lifecycle{}
	}
	return types.Lifecycle{LeaderGoneGracePeriod: &metav1.Duration{Duration: grace}}
}

// tShortenedWorker is a Terminating worker on node-a that shows 30 s of the
// 480 s its spec carries, the grace a delete after its first one named. Its
// deletion time is deletionTime.
func tShortenedWorker(deletionTime time.Time) *corev1.Pod {
	own, shown := int64(480), int64(30)
	pod := tTerminatingPod("node-a", deletionTime)
	pod.Labels = map[string]string{query.LabelRunner: types.RunnerWorker}
	pod.Spec.TerminationGracePeriodSeconds = &own
	pod.DeletionGracePeriodSeconds = &shown
	return pod
}

// TestAllPodsTerminatingOverdue_AShortenedWorkerIsReadOnItsKubeletsCount: a
// worker shows 30 s of its 480 s. With lifecycle.leaderGoneGracePeriod at
// 30 s it does not read as past its time until 30 s after the deletion time
// the API server shows, and the wait of the second reading counts from
// there. With the field unset both readings count from the deletion time.
func TestAllPodsTerminatingOverdue_AShortenedWorkerIsReadOnItsKubeletsCount(t *testing.T) {
	const wait = 5 * time.Second
	cases := []struct {
		name string
		// past is how long ago the worker's deletion time was.
		past       time.Duration
		field      time.Duration
		want       bool
		wantWaited bool
	}{
		{name: "the field unset, 5 s past the deletion time", past: 5 * time.Second, want: true, wantWaited: true},
		{name: "the field at 30 s, 5 s past the deletion time", past: 5 * time.Second, field: 30 * time.Second},
		{name: "the field at 30 s, 30 s past the deletion time", past: 30 * time.Second, field: 30 * time.Second},
		{name: "the field at 30 s, 31 s past the deletion time", past: 31 * time.Second, field: 30 * time.Second, want: true},
		{name: "the field at 30 s, 35 s past the deletion time", past: 35 * time.Second, field: 30 * time.Second, want: true, wantWaited: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pods := []*corev1.Pod{tShortenedWorker(tNow.Add(-tc.past))}
			lifecycle := tLeaderGoneLifecycle(tc.field)
			if got := AllPodsTerminatingOverdue(pods, lifecycle, tNow); got != tc.want {
				t.Errorf("AllPodsTerminatingOverdue = %v, want %v", got, tc.want)
			}
			if got := AllPodsTerminatingOverdueFor(pods, lifecycle, tNow, wait); got != tc.wantWaited {
				t.Errorf("AllPodsTerminatingOverdueFor with a wait of %s = %v, want %v", wait, got, tc.wantWaited)
			}
		})
	}
}

// TestForceDeleteReading_AShortenedWorkerIsReadOnItsKubeletsCount: a worker
// shows 30 s of its 480 s and stands 20 s past the deletion time the API
// server shows, on a node whose object is gone. The policy's slack is 10 s.
// With lifecycle.leaderGoneGracePeriod unset the pod is past the slack and
// the missing node decides. With the field at 30 s the pod is inside its
// grace until 40 s after that deletion time, and the reading names that
// instant for the next look. ReadUnreportedPod reads the same.
func TestForceDeleteReading_AShortenedWorkerIsReadOnItsKubeletsCount(t *testing.T) {
	policy := &types.ForceDeletePolicy{OverdueSlack: 10 * time.Second, NodeUnreachableThreshold: time.Minute}
	deletionTime := tNow.Add(-20 * time.Second)
	pod := tShortenedWorker(deletionTime)
	readers := tReaders(tReader(t))

	unset := StuckTerminating(context.Background(), readers, pod, policy, types.Lifecycle{}, tNow)
	if unset.Kind != NodeGone {
		t.Fatalf("with the field unset the reading is %s, want %s", unset.Kind, NodeGone)
	}
	set := StuckTerminating(context.Background(), readers, pod, policy, tLeaderGoneLifecycle(30*time.Second), tNow)
	if set.Kind != WithinGrace {
		t.Fatalf("with the field at 30 s the reading is %s, want %s", set.Kind, WithinGrace)
	}
	if want := deletionTime.Add(40*time.Second + time.Nanosecond); !set.RequeueAt.Equal(want) {
		t.Fatalf("the next look is %s after the deletion time, want %s", set.RequeueAt.Sub(deletionTime), want.Sub(deletionTime))
	}

	for _, tc := range []struct {
		name      string
		lifecycle types.Lifecycle
		want      TerminatingClass
	}{
		{name: "the field unset", want: NodeGone},
		{name: "the field at 30 s", lifecycle: tLeaderGoneLifecycle(30 * time.Second), want: WithinGrace},
	} {
		got, err := ReadUnreportedPod(context.Background(), readers, pod, policy, tc.lifecycle, tNow)
		if err != nil || got != tc.want {
			t.Errorf("ReadUnreportedPod with %s = %s, %v, want %s", tc.name, got, err, tc.want)
		}
	}
}

// TestForceDeleteReading_OverdueCountsFromTheDeadlineTheReadingWaitedFor: a
// worker shows 30 s of its 480 s, and the policy's slack is 10 s. With
// lifecycle.leaderGoneGracePeriod at 30 s the reading waits until 40 s after
// the deletion time the API server shows. At 45 s, on a node that is gone,
// Overdue is 15 s: the time the reading waited past its deadline, where the
// deletion time alone would say 45 s. A pod let go early on an unreachable
// node, 20 s after that deletion time, reads 10 s short of its deadline. With
// the field unset both count from the deletion time.
func TestForceDeleteReading_OverdueCountsFromTheDeadlineTheReadingWaitedFor(t *testing.T) {
	keep := &types.ForceDeletePolicy{OverdueSlack: 10 * time.Second, NodeUnreachableThreshold: time.Minute}
	skip := &types.ForceDeletePolicy{OverdueSlack: 10 * time.Second, NodeUnreachableThreshold: time.Minute, SkipGracePeriodOnUnreachableNode: true}
	gone := tReaders(tReader(t))
	unreachable := tReaders(tReader(t, tNode("node-a", corev1.ConditionUnknown, 10*time.Minute, false)))
	for _, tc := range []struct {
		name    string
		readers types.NodeReaders
		policy  *types.ForceDeletePolicy
		// past is how long ago the worker's deletion time was.
		past    time.Duration
		field   time.Duration
		want    TerminatingClass
		overdue time.Duration
		skipped bool
	}{
		{name: "the field unset, the node gone, 45 s past the deletion time", readers: gone, policy: keep, past: 45 * time.Second, want: NodeGone, overdue: 45 * time.Second},
		{name: "the field at 30 s, the node gone, 45 s past the deletion time", readers: gone, policy: keep, past: 45 * time.Second, field: 30 * time.Second, want: NodeGone, overdue: 15 * time.Second},
		{name: "the field unset, the node unreachable, 5 s past the deletion time", readers: unreachable, policy: skip, past: 5 * time.Second, want: NodeReadyUnknown, overdue: 5 * time.Second, skipped: true},
		{name: "the field at 30 s, the node unreachable, 20 s past the deletion time", readers: unreachable, policy: skip, past: 20 * time.Second, field: 30 * time.Second, want: NodeReadyUnknown, overdue: -10 * time.Second, skipped: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := tShortenedWorker(tNow.Add(-tc.past))
			got := StuckTerminating(context.Background(), tc.readers, pod, tc.policy, tLeaderGoneLifecycle(tc.field), tNow)
			if got.Kind != tc.want || got.GraceSkipped != tc.skipped {
				t.Fatalf("the reading is %s with GraceSkipped %v, want %s with %v", got.Kind, got.GraceSkipped, tc.want, tc.skipped)
			}
			if got.Overdue != tc.overdue {
				t.Fatalf("Overdue = %s, want %s", got.Overdue, tc.overdue)
			}
		})
	}
}
