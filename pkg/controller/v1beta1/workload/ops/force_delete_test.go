package ops

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clockutils "k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// fdNow is the pinned "wall clock" every force-delete test runs at.
var fdNow = time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)

type fdSteppingClock struct {
	clockutils.RealClock
	next time.Time
	step time.Duration
}

func (c *fdSteppingClock) Now() time.Time {
	now := c.next
	c.next = c.next.Add(c.step)
	return now
}

func fdPolicy() *workload.ForceDeletePolicy {
	return &workload.ForceDeletePolicy{
		OverdueSlack:             2 * time.Minute,
		NodeUnreachableThreshold: 5 * time.Minute,
	}
}

// fdFakeClient builds a fake client with the schemes the force-delete
// tests touch, optionally wrapped in interceptor funcs.
func fdFakeClient(t *testing.T, funcs *interceptor.Funcs, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add discoveryv1: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	b := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(objs...)
	if funcs != nil {
		b = b.WithInterceptorFuncs(*funcs)
	}
	return b.Build()
}

// recordedDeleteOpts is one Delete RPC as the escalation issued it —
// the options are reconstructed by applying every client.DeleteOption
// to a fresh client.DeleteOptions, which is exactly how the real client
// serializes them, so asserting on grace/uid here asserts what the
// apiserver would have received.
type recordedDeleteOpts struct {
	name  string
	grace *int64
	uid   *k8stypes.UID
}

// fdDeleteRecorder returns interceptor funcs that record every Pod deletion
// RPC's applied options before delegating to the inner client.
func fdDeleteRecorder(rec *[]recordedDeleteOpts) interceptor.Funcs {
	return interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*corev1.Pod); ok {
				do := &client.DeleteOptions{}
				for _, o := range opts {
					o.ApplyToDelete(do)
				}
				var uid *k8stypes.UID
				if do.Preconditions != nil {
					uid = do.Preconditions.UID
				}
				*rec = append(*rec, recordedDeleteOpts{name: obj.GetName(), grace: do.GracePeriodSeconds, uid: uid})
			}
			return cl.Delete(ctx, obj, opts...)
		},
	}
}

// fdTerminatingPod fabricates the pod object as the live List would
// present it: Terminating since deletionTS, optionally finalizer-pinned.
func fdTerminatingPod(name, node string, deletionTS time.Time, finalizers ...string) *corev1.Pod {
	dt := metav1.NewTime(deletionTS)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "prod",
			UID:               k8stypes.UID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeee0001"),
			DeletionTimestamp: &dt,
			Finalizers:        finalizers,
		},
		Spec: corev1.PodSpec{
			NodeName:   node,
			Containers: []corev1.Container{{Name: "main", Image: "test:v1"}},
		},
	}
}

// fdStoredCopy strips the fields the fake client refuses to store
// (DeletionTimestamp without finalizers) so the same pod can be seeded
// as a deletable object while escalate receives the Terminating view.
func fdStoredCopy(pod *corev1.Pod) *corev1.Pod {
	stored := pod.DeepCopy()
	stored.DeletionTimestamp = nil
	stored.Finalizers = nil
	return stored
}

func fdNodeReady(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type:               corev1.NodeReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.NewTime(fdNow.Add(-24 * time.Hour)),
		}}},
	}
}

func fdNodeNotReady(name string, since time.Duration) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type:               corev1.NodeReady,
			Status:             corev1.ConditionUnknown,
			LastTransitionTime: metav1.NewTime(fdNow.Add(-since)),
		}}},
	}
}

func fdNodeUnreachable(name string, since time.Duration) *corev1.Node {
	added := metav1.NewTime(fdNow.Add(-since))
	node := fdNodeNotReady(name, since)
	node.Spec.Taints = []corev1.Taint{{
		Key:       corev1.TaintNodeUnreachable,
		Effect:    corev1.TaintEffectNoExecute,
		TimeAdded: &added,
	}}
	return node
}

func fdISVC(name string) *v1beta1.InferenceService {
	return legacyMinimalISVC(name, "prod", 1)
}

func fdInput(isvc *v1beta1.InferenceService, policy *workload.ForceDeletePolicy) workload.ReconcileInput {
	return workload.ReconcileInput{
		OwnerObject: isvc,
		OwnerGVK:    v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		EventTarget: isvc,
		Key: workload.Key{
			Namespace: isvc.Namespace,
			OwnerName: isvc.Name,
			Component: workload.ComponentEngine,
		},
		ForceDelete: policy,
		Clock:       clocktesting.NewFakeClock(fdNow),
	}
}

// fdDrainEvents empties the fake recorder's channel.
func fdDrainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func fdCountEvents(events []string, reason workload.EventReason) int {
	n := 0
	for _, e := range events {
		if strings.Contains(e, string(reason)) {
			n++
		}
	}
	return n
}

func fdLedgerEntries(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) []audit.Entry {
	t.Helper()
	ledger, err := audit.LoadLedgerForOwner(context.Background(), c, isvc)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	return ledger.Entries
}

// overdueTS is a DeletionTimestamp far enough in the past to be
// overdue under fdPolicy's 2m slack.
var overdueTS = fdNow.Add(-10 * time.Minute)

func TestForceDeleteRequeueTracksExactPolicyBoundaries(t *testing.T) {
	isvc := fdISVC("deadline-requeue")
	policy := fdPolicy()

	t.Run("pod overdue slack", func(t *testing.T) {
		pod := fdTerminatingPod("within-grace", "unread-node", fdNow.Add(-time.Minute))
		nodeGets := 0
		funcs := interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Node); ok {
				nodeGets++
			}
			return cl.Get(ctx, key, obj, opts...)
		}}
		c := fdFakeClient(t, &funcs)
		next, err := escalateStuckTerminatingWithDeadline(context.Background(), workload.Deps{Client: c}, fdInput(isvc, policy), pod, 0)
		if err != nil {
			t.Fatal(err)
		}
		want := fdNow.Add(time.Minute + time.Nanosecond)
		if !next.Equal(want) || nodeGets != 0 {
			t.Fatalf("next/node reads = %s/%d, want %s/0", next, nodeGets, want)
		}
	})

	t.Run("node unreachable threshold", func(t *testing.T) {
		pod := fdTerminatingPod("young-unreachable", "node-a", overdueTS)
		node := fdNodeNotReady("node-a", 2*time.Minute)
		c := fdFakeClient(t, nil, node)
		next, err := escalateStuckTerminatingWithDeadline(context.Background(), workload.Deps{Client: c}, fdInput(isvc, policy), pod, 0)
		if err != nil {
			t.Fatal(err)
		}
		want := fdNow.Add(3 * time.Minute)
		if !next.Equal(want) {
			t.Fatalf("next = %s, want %s", next, want)
		}
	})

	for _, test := range []struct {
		name string
		node *corev1.Node
	}{
		{name: "ready node recheck", node: fdNodeReady("node-a")},
		{name: "node without unreachable evidence recheck", node: &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node-a"},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			recheckPolicy := *policy
			recheckPolicy.NodeUnreachableThreshold = 7*time.Minute + 13*time.Second
			pod := fdTerminatingPod("overdue", "node-a", overdueTS)
			c := fdFakeClient(t, nil, test.node)
			next, err := escalateStuckTerminatingWithDeadline(context.Background(), workload.Deps{Client: c}, fdInput(isvc, &recheckPolicy), pod, 0)
			if err != nil {
				t.Fatal(err)
			}
			want := fdNow.Add(recheckPolicy.NodeUnreachableThreshold)
			if !next.Equal(want) {
				t.Fatalf("next = %s, want %s", next, want)
			}
		})
	}
}

func TestDeleteBatchComparesAbsolutePolicyDeadlinesAcrossLongPass(t *testing.T) {
	owner := deleteBatchOwner()
	status := deleteOwnedStatus(0, fdNow.Add(-time.Minute))
	first := fdTerminatingPod("first-deadline", "unread-node", fdNow.Add(-time.Minute))
	second := fdTerminatingPod("second-deadline", "unread-node", fdNow.Add(-30*time.Second))
	clock := &fdSteppingClock{next: fdNow, step: 40 * time.Second}
	store := newDeleteMutationStore(owner, []workload.InstanceStatus{status})
	input := deleteBatchInput(owner, []workload.InstanceStatus{status})
	input.Clock = clock
	input.ForceDelete = fdPolicy()
	input.ScaleDownRequeueInterval = 0
	input.ApplyInstanceMutationsWithRetryBlock = store.apply
	client := newDeleteFailureBaseClient(t)

	result, err := DeleteBatch(context.Background(), workload.Deps{
		Client: client, Expectations: workload.NewExpectations(),
	}, input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{0: {first, second}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.InProgress || !result.PolicyDeadlineDue || result.RequeueAfter != 0 {
		t.Fatalf("result = %+v, want earliest absolute deadline already due", result)
	}
}

// Invariant 1: config absent → the predicate never evaluates; zero
// Node reads, pod untouched.
func TestForceDelete_NilPolicy_NoNodeReadsNoAction(t *testing.T) {
	nodeGets := 0
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	funcs.Get = func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if _, ok := obj.(*corev1.Node); ok {
			nodeGets++
		}
		return cl.Get(ctx, key, obj, opts...)
	}
	pod := fdTerminatingPod("wedge-0", "gone-node", overdueTS)
	c := fdFakeClient(t, &funcs, fdStoredCopy(pod))
	isvc := fdISVC("llama")

	// escalate with nil policy: hard no-op.
	if err := escalateStuckTerminating(context.Background(), workload.Deps{Client: c}, fdInput(isvc, nil), pod, 0); err != nil {
		t.Fatalf("escalate: %v", err)
	}
	// evidence helper with nil policy: NotConfigured before any read.
	if res := stuckTerminatingEvidence(context.Background(), c, pod, nil, fdNow); res.kind != evidenceNotConfigured {
		t.Errorf("evidence kind: got %s want not-configured", res.kind)
	}
	if nodeGets != 0 {
		t.Errorf("node reads with nil policy: got %d want 0", nodeGets)
	}
	if len(deletes) != 0 {
		t.Errorf("deletes with nil policy: got %d want 0", len(deletes))
	}
}

// Invariant 2: a pod inside its own grace window (long
// terminationGracePeriodSeconds ⇒ late DeletionTimestamp) is untouched
// even when its node is already gone.
func TestForceDelete_WithinOwnGrace_NodeGone_Untouched(t *testing.T) {
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	// A 10-minute-drain pod deleted moments ago: DeletionTimestamp is
	// still ~10m in the future (request time + its own grace period).
	pod := fdTerminatingPod("wedge-0", "gone-node", fdNow.Add(9*time.Minute))
	c := fdFakeClient(t, &funcs, fdStoredCopy(pod)) // node NOT seeded → NotFound if read
	isvc := fdISVC("llama")

	res := stuckTerminatingEvidence(context.Background(), c, pod, fdPolicy(), fdNow)
	if res.kind != evidenceWithinGrace {
		t.Errorf("evidence kind: got %s want within-grace", res.kind)
	}
	if err := escalateStuckTerminating(context.Background(), workload.Deps{Client: c}, fdInput(isvc, fdPolicy()), pod, 0); err != nil {
		t.Fatalf("escalate: %v", err)
	}
	if len(deletes) != 0 {
		t.Errorf("deletes: got %d want 0", len(deletes))
	}

	// Boundary: exactly at DeletionTimestamp+OverdueSlack is still
	// within grace (predicate requires now strictly after).
	boundary := fdTerminatingPod("wedge-1", "gone-node", fdNow.Add(-fdPolicy().OverdueSlack))
	if res := stuckTerminatingEvidence(context.Background(), c, boundary, fdPolicy(), fdNow); res.kind != evidenceWithinGrace {
		t.Errorf("boundary evidence kind: got %s want within-grace", res.kind)
	}
}

// Invariant 3: overdue on a Ready node is the kubelet-slow case —
// force-deleting risks a double-running container. Untouched.
func TestForceDelete_Overdue_NodeReady_Untouched(t *testing.T) {
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	pod := fdTerminatingPod("wedge-0", "healthy-node", overdueTS)
	c := fdFakeClient(t, &funcs, fdStoredCopy(pod), fdNodeReady("healthy-node"))
	isvc := fdISVC("llama")

	res := stuckTerminatingEvidence(context.Background(), c, pod, fdPolicy(), fdNow)
	if res.kind != evidenceNodeHealthy {
		t.Errorf("evidence kind: got %s want node-healthy", res.kind)
	}
	if want := fdNow.Add(fdPolicy().NodeUnreachableThreshold); !res.requeueAt.Equal(want) {
		t.Errorf("requeueAt: got %s want %s", res.requeueAt, want)
	}
	if err := escalateStuckTerminating(context.Background(), workload.Deps{Client: c}, fdInput(isvc, fdPolicy()), pod, 0); err != nil {
		t.Fatalf("escalate: %v", err)
	}
	if len(deletes) != 0 {
		t.Errorf("deletes: got %d want 0", len(deletes))
	}
}

// Invariant 4: unreachable evidence younger than the threshold is a
// blip, not a death. Untouched — for both the NotReady condition and
// the unreachable taint.
func TestForceDelete_Overdue_EvidenceYoungerThanThreshold_Untouched(t *testing.T) {
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	pod := fdTerminatingPod("wedge-0", "dying-node", overdueTS)
	c := fdFakeClient(t, &funcs, fdStoredCopy(pod), fdNodeNotReady("dying-node", 2*time.Minute))
	isvc := fdISVC("llama")

	res := stuckTerminatingEvidence(context.Background(), c, pod, fdPolicy(), fdNow)
	if res.kind != evidenceNodeNotDeadLongEnough {
		t.Errorf("evidence kind: got %s want node-not-dead-long-enough", res.kind)
	}
	if err := escalateStuckTerminating(context.Background(), workload.Deps{Client: c}, fdInput(isvc, fdPolicy()), pod, 0); err != nil {
		t.Fatalf("escalate: %v", err)
	}
	if len(deletes) != 0 {
		t.Errorf("deletes: got %d want 0", len(deletes))
	}

	// Young taint (2m < 5m threshold) on a separate client.
	cTaint := fdFakeClient(t, nil, fdNodeUnreachable("dying-node", 2*time.Minute))
	if res := stuckTerminatingEvidence(context.Background(), cTaint, pod, fdPolicy(), fdNow); res.kind != evidenceNodeNotDeadLongEnough {
		t.Errorf("young-taint evidence kind: got %s want node-not-dead-long-enough", res.kind)
	}
	// Taint with nil TimeAdded can't prove the threshold elapsed.
	nilAdded := fdNodeUnreachable("dying-node", 10*time.Minute)
	nilAdded.Spec.Taints[0].TimeAdded = nil
	nilAdded.Status.Conditions[0].LastTransitionTime = metav1.NewTime(fdNow.Add(-time.Minute))
	cNil := fdFakeClient(t, nil, nilAdded)
	if res := stuckTerminatingEvidence(context.Background(), cNil, pod, fdPolicy(), fdNow); res.kind != evidenceNodeNotDeadLongEnough {
		t.Errorf("nil-TimeAdded evidence kind: got %s want node-not-dead-long-enough", res.kind)
	}
}

// Invariant 5: overdue + unreachable taint older than the threshold →
// force-deleted with BOTH grace 0 and the pod-UID precondition on the
// recorded DeleteOptions.
func TestForceDelete_Overdue_OldUnreachableTaint_DeletedWithOptions(t *testing.T) {
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	pod := fdTerminatingPod("wedge-0", "dead-node", overdueTS)
	c := fdFakeClient(t, &funcs, fdStoredCopy(pod), fdNodeUnreachable("dead-node", 10*time.Minute))
	isvc := fdISVC("llama")

	res := stuckTerminatingEvidence(context.Background(), c, pod, fdPolicy(), fdNow)
	if res.kind != evidenceNodeUnreachableTaint {
		t.Fatalf("evidence kind: got %s want node-unreachable-taint", res.kind)
	}
	if err := escalateStuckTerminating(context.Background(), workload.Deps{Client: c}, fdInput(isvc, fdPolicy()), pod, 0); err != nil {
		t.Fatalf("escalate: %v", err)
	}
	if len(deletes) != 1 {
		t.Fatalf("deletes: got %d want 1", len(deletes))
	}
	d := deletes[0]
	if d.name != "wedge-0" {
		t.Errorf("deleted pod: got %q", d.name)
	}
	if d.grace == nil || *d.grace != 0 {
		t.Errorf("GracePeriodSeconds: got %v want 0", d.grace)
	}
	if d.uid == nil || *d.uid != pod.UID {
		t.Errorf("Preconditions.UID: got %v want %s", d.uid, pod.UID)
	}
	// Object actually gone.
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); !apierrors.IsNotFound(err) {
		t.Errorf("expected pod gone, got err=%v", err)
	}
}

// Invariant 5b (recovered-node regression): a node can carry a
// lingering unreachable taint with an old TimeAdded after it recovered
// — kubelet rejoined and posted Ready=True itself while a degraded
// kube-controller-manager never removed the taint. Ready=True must
// veto the taint: not actionable, zero deletes. The identical stale
// taint with Ready=Unknown old stays actionable — the veto must not
// weaken the genuine dead-node case.
func TestForceDelete_StaleTaintOnRecoveredNode_ReadyTrueVetoes(t *testing.T) {
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	pod := fdTerminatingPod("wedge-0", "recovered-node", overdueTS)
	// Unreachable taint 10m old (over the 5m threshold), but the node
	// posts Ready=True with a fresh heartbeat.
	node := fdNodeReady("recovered-node")
	added := metav1.NewTime(fdNow.Add(-10 * time.Minute))
	node.Spec.Taints = []corev1.Taint{{
		Key:       corev1.TaintNodeUnreachable,
		Effect:    corev1.TaintEffectNoExecute,
		TimeAdded: &added,
	}}
	node.Status.Conditions[0].LastHeartbeatTime = metav1.NewTime(fdNow.Add(-10 * time.Second))
	c := fdFakeClient(t, &funcs, fdStoredCopy(pod), node)
	isvc := fdISVC("llama")

	res := stuckTerminatingEvidence(context.Background(), c, pod, fdPolicy(), fdNow)
	if res.kind != evidenceNodeHealthy {
		t.Errorf("evidence kind: got %s want node-healthy (Ready=True must veto the stale taint)", res.kind)
	}
	if want := fdNow.Add(fdPolicy().NodeUnreachableThreshold); !res.requeueAt.Equal(want) {
		t.Errorf("requeueAt: got %s want %s", res.requeueAt, want)
	}
	if err := escalateStuckTerminating(context.Background(), workload.Deps{Client: c}, fdInput(isvc, fdPolicy()), pod, 0); err != nil {
		t.Fatalf("escalate: %v", err)
	}
	if len(deletes) != 0 {
		t.Errorf("deletes: got %d want 0 (pod on a live node)", len(deletes))
	}

	// Companion: same-age taint, Ready=Unknown just as old — the genuine
	// dead node. Still actionable.
	cDead := fdFakeClient(t, nil, fdNodeUnreachable("recovered-node", 10*time.Minute))
	if res := stuckTerminatingEvidence(context.Background(), cDead, pod, fdPolicy(), fdNow); res.kind != evidenceNodeUnreachableTaint {
		t.Errorf("dead-node evidence kind: got %s want node-unreachable-taint", res.kind)
	}
}

// Invariant 6: overdue + Node object gone → force-deleted.
func TestForceDelete_Overdue_NodeGone_Deleted(t *testing.T) {
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	pod := fdTerminatingPod("wedge-0", "gone-node", overdueTS)
	c := fdFakeClient(t, &funcs, fdStoredCopy(pod)) // no node object
	isvc := fdISVC("llama")

	res := stuckTerminatingEvidence(context.Background(), c, pod, fdPolicy(), fdNow)
	if res.kind != evidenceNodeGone {
		t.Fatalf("evidence kind: got %s want node-gone", res.kind)
	}
	if err := escalateStuckTerminating(context.Background(), workload.Deps{Client: c}, fdInput(isvc, fdPolicy()), pod, 0); err != nil {
		t.Fatalf("escalate: %v", err)
	}
	if len(deletes) != 1 {
		t.Fatalf("deletes: got %d want 1", len(deletes))
	}

	// Overdue + NotReady long enough is the third actionable branch.
	pod2 := fdTerminatingPod("wedge-1", "notready-node", overdueTS)
	c2 := fdFakeClient(t, nil, fdNodeNotReady("notready-node", 10*time.Minute))
	if res := stuckTerminatingEvidence(context.Background(), c2, pod2, fdPolicy(), fdNow); res.kind != evidenceNodeNotReady {
		t.Errorf("evidence kind: got %s want node-not-ready", res.kind)
	}

	// Unscheduled pod: nothing to prove dead — never actionable.
	pod3 := fdTerminatingPod("wedge-2", "", overdueTS)
	if res := stuckTerminatingEvidence(context.Background(), c2, pod3, fdPolicy(), fdNow); res.kind != evidenceUnscheduled {
		t.Errorf("evidence kind: got %s want unscheduled", res.kind)
	}
}

// Invariant 7: a foreign finalizer blocks the delete — one Warning,
// no delete, and no repeat spam on subsequent passes (dedup on pod UID
// via the persisted ledger marker).
func TestForceDelete_ForeignFinalizer_ReportOnceNoSpam(t *testing.T) {
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	pod := fdTerminatingPod("wedge-0", "gone-node", overdueTS, "someone.else/finalizer")
	// Pod not seeded — the report path never touches the pod object.
	c := fdFakeClient(t, &funcs)
	isvc := fdISVC("llama")
	rec := record.NewFakeRecorder(16)
	deps := workload.Deps{Client: c, Recorder: rec}
	input := fdInput(isvc, fdPolicy())

	for pass := 0; pass < 2; pass++ {
		if err := escalateStuckTerminating(context.Background(), deps, input, pod, 0); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
	}
	if len(deletes) != 0 {
		t.Errorf("deletes: got %d want 0", len(deletes))
	}
	events := fdDrainEvents(rec)
	if n := fdCountEvents(events, workload.EventReasonPodDeleteBlockedByFinalizer); n != 1 {
		t.Errorf("finalizer warnings: got %d want 1 (events=%v)", n, events)
	}
	entries := fdLedgerEntries(t, c, isvc)
	if len(entries) != 1 {
		t.Fatalf("ledger entries: got %d want 1", len(entries))
	}
	e := entries[0]
	if e.RequestUUID != string(pod.UID) || e.Reason != audit.ReasonForceDelete ||
		e.Outcome != audit.OutcomeForceDeleteFinalizerReport || e.Phase != audit.PhaseCompleted {
		t.Errorf("dedup marker shape: %+v", e)
	}
}

// Invariant 8: UID-precondition conflict and pod-already-gone are both
// success — the wedge object is gone or a successor exists; no error,
// no event, no ledger entry.
func TestForceDelete_PreconditionConflictAndNotFound_Success(t *testing.T) {
	isvc := fdISVC("llama")
	pod := fdTerminatingPod("wedge-0", "gone-node", overdueTS)

	// (a) precondition mismatch → 409 Conflict from the apiserver.
	conflictFuncs := interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			return apierrors.NewConflict(corev1.Resource("pods"), obj.GetName(),
				apierrors.NewBadRequest("the UID in the precondition does not match"))
		},
	}
	cConflict := fdFakeClient(t, &conflictFuncs, fdStoredCopy(pod))
	recConflict := record.NewFakeRecorder(16)
	if err := escalateStuckTerminating(context.Background(), workload.Deps{Client: cConflict, Recorder: recConflict}, fdInput(isvc, fdPolicy()), pod, 0); err != nil {
		t.Fatalf("conflict must be success: %v", err)
	}
	if events := fdDrainEvents(recConflict); len(events) != 0 {
		t.Errorf("conflict path events: %v", events)
	}
	if entries := fdLedgerEntries(t, cConflict, isvc); len(entries) != 0 {
		t.Errorf("conflict path ledger entries: %d", len(entries))
	}

	// (b) pod already gone → NotFound.
	cGone := fdFakeClient(t, nil) // pod absent, node absent
	recGone := record.NewFakeRecorder(16)
	if err := escalateStuckTerminating(context.Background(), workload.Deps{Client: cGone, Recorder: recGone}, fdInput(isvc, fdPolicy()), pod, 0); err != nil {
		t.Fatalf("NotFound must be success: %v", err)
	}
	if events := fdDrainEvents(recGone); len(events) != 0 {
		t.Errorf("NotFound path events: %v", events)
	}
	if entries := fdLedgerEntries(t, cGone, isvc); len(entries) != 0 {
		t.Errorf("NotFound path ledger entries: %d", len(entries))
	}
}

// Invariant 9: an action records exactly one event and one terminal
// ledger entry; the crash-replay pass (pod gone) never double-ledgers.
func TestForceDelete_ActionRecordsOnce_NoDoubleLedgerOnReplay(t *testing.T) {
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	pod := fdTerminatingPod("wedge-0", "gone-node", overdueTS)
	c := fdFakeClient(t, &funcs, fdStoredCopy(pod))
	isvc := fdISVC("llama")
	rec := record.NewFakeRecorder(16)
	deps := workload.Deps{Client: c, Recorder: rec}
	input := fdInput(isvc, fdPolicy())

	if err := escalateStuckTerminating(context.Background(), deps, input, pod, 3); err != nil {
		t.Fatalf("escalate: %v", err)
	}
	// Replay: the pod object is gone (delete succeeded), so a repeated
	// evaluation hits NotFound and records nothing new.
	if err := escalateStuckTerminating(context.Background(), deps, input, pod, 3); err != nil {
		t.Fatalf("replay: %v", err)
	}

	events := fdDrainEvents(rec)
	if n := fdCountEvents(events, workload.EventReasonPodForceDeleted); n != 1 {
		t.Errorf("PodForceDeleted events: got %d want 1 (events=%v)", n, events)
	}
	entries := fdLedgerEntries(t, c, isvc)
	if len(entries) != 1 {
		t.Fatalf("ledger entries: got %d want 1: %+v", len(entries), entries)
	}
	e := entries[0]
	if e.RequestUUID != string(pod.UID) {
		t.Errorf("RequestUUID: got %q want pod UID %q", e.RequestUUID, pod.UID)
	}
	if e.Component != string(workload.ComponentEngine) || e.SourceInstance != 3 {
		t.Errorf("Component/SourceInstance: got %q/%d", e.Component, e.SourceInstance)
	}
	if e.Phase != audit.PhaseCompleted || e.Reason != audit.ReasonForceDelete || e.Outcome != audit.OutcomeForceDeleteUnreachable {
		t.Errorf("Phase/Reason/Outcome: got %q/%q/%q", e.Phase, e.Reason, e.Outcome)
	}
	if e.FromNode != "gone-node" {
		t.Errorf("FromNode: got %q", e.FromNode)
	}
	if e.StartedAt == "" || e.CompletedAt == "" {
		t.Errorf("timestamps: StartedAt=%q CompletedAt=%q", e.StartedAt, e.CompletedAt)
	}
}

// The teardown sweep applies the per-pod predicate to a whole slice, so
// it must not widen it: a Terminating pod on a long-dead node goes, a
// Terminating pod on a Ready node stays (the kubelet is merely slow),
// and a pod nobody deleted is never even classified.
func TestForceDeletePods_SweepsOnlyActionableTerminatingPods(t *testing.T) {
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)

	wedged := fdTerminatingPod("wedge-0", "dead-node", overdueTS)
	wedged.UID = k8stypes.UID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeee0001")
	slow := fdTerminatingPod("slow-1", "live-node", overdueTS)
	slow.UID = k8stypes.UID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeee0002")
	serving := fdTerminatingPod("serving-2", "dead-node", overdueTS)
	serving.UID = k8stypes.UID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeee0003")
	serving.DeletionTimestamp = nil

	c := fdFakeClient(t, &funcs,
		fdStoredCopy(wedged), fdStoredCopy(slow), fdStoredCopy(serving),
		fdNodeUnreachable("dead-node", 10*time.Minute), fdNodeReady("live-node"))

	pods := []*corev1.Pod{wedged, slow, serving}
	if err := escalateStuckTerminatingPods(context.Background(), workload.Deps{Client: c}, fdInput(fdISVC("llama"), fdPolicy()), pods, 0); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(deletes) != 1 {
		t.Fatalf("deletes: got %d want 1 (%+v)", len(deletes), deletes)
	}
	if deletes[0].name != "wedge-0" {
		t.Errorf("deleted pod: got %q want wedge-0", deletes[0].name)
	}
	if deletes[0].grace == nil || *deletes[0].grace != 0 {
		t.Errorf("GracePeriodSeconds: got %v want 0", deletes[0].grace)
	}
}

// Unconfigured policy disables the sweep entirely — no node is read for
// any pod in the slice, matching the single-pod contract.
func TestForceDeletePods_NilPolicy_NoNodeReadsNoAction(t *testing.T) {
	nodeGets := 0
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	funcs.Get = func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if _, ok := obj.(*corev1.Node); ok {
			nodeGets++
		}
		return cl.Get(ctx, key, obj, opts...)
	}
	pod := fdTerminatingPod("wedge-0", "gone-node", overdueTS)
	c := fdFakeClient(t, &funcs, fdStoredCopy(pod))

	if err := escalateStuckTerminatingPods(context.Background(), workload.Deps{Client: c}, fdInput(fdISVC("llama"), nil), []*corev1.Pod{pod}, 0); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if nodeGets != 0 {
		t.Errorf("node reads with nil policy: got %d want 0", nodeGets)
	}
	if len(deletes) != 0 {
		t.Errorf("deletes with nil policy: got %d want 0", len(deletes))
	}
}

// A failed Node read is the fail-safe branch: the sweep surfaces the
// error rather than guessing, so the caller can requeue with no pod
// touched on evidence it never obtained.
func TestForceDeletePods_NodeReadError_Propagates(t *testing.T) {
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	funcs.Get = func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if _, ok := obj.(*corev1.Node); ok {
			return apierrors.NewServiceUnavailable("apiserver down")
		}
		return cl.Get(ctx, key, obj, opts...)
	}
	pod := fdTerminatingPod("wedge-0", "dead-node", overdueTS)
	c := fdFakeClient(t, &funcs, fdStoredCopy(pod), fdNodeUnreachable("dead-node", 10*time.Minute))

	err := escalateStuckTerminatingPods(context.Background(), workload.Deps{Client: c}, fdInput(fdISVC("llama"), fdPolicy()), []*corev1.Pod{pod}, 0)
	if err == nil {
		t.Fatal("expected node-read error to propagate, got nil")
	}
	if !strings.Contains(err.Error(), "wedge-0") {
		t.Errorf("error should name the pod: %v", err)
	}
	if len(deletes) != 0 {
		t.Errorf("deletes on unreadable evidence: got %d want 0", len(deletes))
	}
}
