package podreadiness

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func newReadinessTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&corev1.Pod{}).
		Build()
}

func mustParseList(t *testing.T, raw string) messageList {
	t.Helper()
	list, err := parseList(raw)
	if err != nil {
		t.Fatalf("parseList(%q): %v", raw, err)
	}
	return list
}

func newReadinessTestPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "x"}},
			ReadinessGates: []corev1.PodReadinessGate{{
				ConditionType: ConditionType,
			}},
		},
	}
}

func TestFreshPod_RemoveNotReadyKey_WritesStatusTrue(t *testing.T) {
	// Pod has no condition. The Lifecycle writer's Remove is the Ready
	// promotion — it creates the condition with Status=True.
	pod := newReadinessTestPod("p")
	c := newReadinessTestClient(t, pod)

	if err := RemoveNotReadyKey(context.Background(), c, c, pod, Message{UserAgent: WriterLifecycle, Key: KeyLifecycleInstanceReady}); err != nil {
		t.Fatalf("RemoveNotReadyKey: %v", err)
	}

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get: %v", err)
	}
	cond := findCondition(got, ConditionType)
	if cond == nil {
		t.Fatalf("condition not written")
	}
	if cond.Status != corev1.ConditionTrue {
		t.Errorf("expected Status=True, got %s", cond.Status)
	}
	if cond.Message != "" {
		t.Errorf("expected empty message, got %q", cond.Message)
	}
}

func TestMarkPodServingWithChangeReportsOnlyOwnedTransition(t *testing.T) {
	t.Run("fresh condition", func(t *testing.T) {
		pod := newReadinessTestPod("p-change-fresh")
		c := newReadinessTestClient(t, pod)

		changed, err := MarkPodServingWithChange(context.Background(), c, c, pod, WriterLifecycle, KeyLifecycleInstanceReady)
		if err != nil {
			t.Fatalf("first MarkPodServingWithChange: %v", err)
		}
		if !changed {
			t.Fatal("fresh Pod serving promotion must report a committed change")
		}
		changed, err = MarkPodServingWithChange(context.Background(), c, c, pod, WriterLifecycle, KeyLifecycleInstanceReady)
		if err != nil {
			t.Fatalf("idempotent MarkPodServingWithChange: %v", err)
		}
		if changed {
			t.Fatal("already-serving Pod must not report a change owned by this call")
		}
	})

	t.Run("held lifecycle key", func(t *testing.T) {
		pod := newReadinessTestPod("p-change-held")
		c := newReadinessTestClient(t, pod)
		if err := AddNotReadyKey(context.Background(), c, c, pod, Message{UserAgent: WriterLifecycle, Key: KeyLifecycleInstanceReady}); err != nil {
			t.Fatalf("AddNotReadyKey: %v", err)
		}

		changed, err := MarkPodServingWithChange(context.Background(), c, c, pod, WriterLifecycle, KeyLifecycleInstanceReady)
		if err != nil {
			t.Fatalf("MarkPodServingWithChange: %v", err)
		}
		if !changed {
			t.Fatal("removing the held Lifecycle key must report a committed change")
		}
	})

	t.Run("unrelated writer key", func(t *testing.T) {
		pod := newReadinessTestPod("p-change-unrelated")
		c := newReadinessTestClient(t, pod)
		other := Message{UserAgent: WriterDeleteDrain, Key: "delete-0"}
		if err := AddNotReadyKey(context.Background(), c, c, pod, other); err != nil {
			t.Fatalf("AddNotReadyKey: %v", err)
		}

		changed, err := MarkPodServingWithChange(context.Background(), c, c, pod, WriterLifecycle, KeyLifecycleInstanceReady)
		if err != nil {
			t.Fatalf("MarkPodServingWithChange: %v", err)
		}
		if changed {
			t.Fatal("an unrelated writer's hold must not be reported as a Lifecycle-owned transition")
		}
		got := &corev1.Pod{}
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
			t.Fatalf("get Pod: %v", err)
		}
		if !ContainsNotReadyKey(got, other) {
			t.Fatal("unrelated writer hold was not preserved")
		}
	})

	t.Run("inconsistent false condition", func(t *testing.T) {
		pod := newReadinessTestPod("p-change-inconsistent")
		pod.Status.Conditions = []corev1.PodCondition{{
			Type:   ConditionType,
			Status: corev1.ConditionFalse,
		}}
		c := newReadinessTestClient(t, pod)

		changed, err := MarkPodServingWithChange(context.Background(), c, c, pod, WriterLifecycle, KeyLifecycleInstanceReady)
		if err != nil {
			t.Fatalf("MarkPodServingWithChange: %v", err)
		}
		if !changed {
			t.Fatal("self-healing False to True must report an owned serving transition")
		}
	})
}

func TestAddNotReadyKey_SetsStatusFalse(t *testing.T) {
	pod := newReadinessTestPod("p")
	c := newReadinessTestClient(t, pod)

	if err := AddNotReadyKey(context.Background(), c, c, pod, Message{UserAgent: "Delete-drain", Key: "0"}); err != nil {
		t.Fatalf("AddNotReadyKey: %v", err)
	}

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get: %v", err)
	}
	cond := findCondition(got, ConditionType)
	if cond == nil || cond.Status != corev1.ConditionFalse {
		t.Fatalf("expected Status=False, got %+v", cond)
	}
	list := mustParseList(t, cond.Message)
	if len(list) != 1 || list[0].UserAgent != "Delete-drain" || list[0].Key != "0" {
		t.Errorf("unexpected message list: %v", list)
	}
}

func TestMultiWriter_TwoKeysHoldNotReady(t *testing.T) {
	pod := newReadinessTestPod("p")
	c := newReadinessTestClient(t, pod)

	if err := AddNotReadyKey(context.Background(), c, c, pod, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("Add A: %v", err)
	}
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if err := AddNotReadyKey(context.Background(), c, c, got, Message{UserAgent: "B", Key: "2"}); err != nil {
		t.Fatalf("Add B: %v", err)
	}

	// Removing only A — pod must stay NotReady because B still holds it.
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if err := RemoveNotReadyKey(context.Background(), c, c, got, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("Remove A: %v", err)
	}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	cond := findCondition(got, ConditionType)
	if cond.Status != corev1.ConditionFalse {
		t.Errorf("expected Status=False after removing one of two keys, got %s", cond.Status)
	}
	list := mustParseList(t, cond.Message)
	if len(list) != 1 || list[0].UserAgent != "B" {
		t.Errorf("expected only B remaining, got %v", list)
	}

	// Now remove B — pod must flip Ready.
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if err := RemoveNotReadyKey(context.Background(), c, c, got, Message{UserAgent: "B", Key: "2"}); err != nil {
		t.Fatalf("Remove B: %v", err)
	}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	cond = findCondition(got, ConditionType)
	if cond.Status != corev1.ConditionTrue {
		t.Errorf("expected Status=True after removing last key, got %s", cond.Status)
	}
}

func TestAddNotReadyKey_Idempotent(t *testing.T) {
	pod := newReadinessTestPod("p")
	c := newReadinessTestClient(t, pod)

	if err := AddNotReadyKey(context.Background(), c, c, pod, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("first Add: %v", err)
	}
	first := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), first)
	firstRV := first.ResourceVersion

	// Second Add of the same key must be a no-op (no patch issued).
	if err := AddNotReadyKey(context.Background(), c, c, first, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("second Add: %v", err)
	}
	second := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), second)
	if second.ResourceVersion != firstRV {
		t.Errorf("ResourceVersion bumped on idempotent Add: %s -> %s", firstRV, second.ResourceVersion)
	}
}

func TestRemoveNotReadyKey_IdempotentOnUnknownKey(t *testing.T) {
	pod := newReadinessTestPod("p")
	c := newReadinessTestClient(t, pod)

	// Seed Status=True via the Lifecycle promotion.
	if err := RemoveNotReadyKey(context.Background(), c, c, pod, Message{UserAgent: WriterLifecycle, Key: KeyLifecycleInstanceReady}); err != nil {
		t.Fatalf("seed Remove: %v", err)
	}
	first := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), first)
	firstRV := first.ResourceVersion

	// Removing a key that was never added must be a no-op — condition
	// already True, key isn't in the list.
	if err := RemoveNotReadyKey(context.Background(), c, c, first, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("second Remove: %v", err)
	}
	second := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), second)
	if second.ResourceVersion != firstRV {
		t.Errorf("ResourceVersion bumped on idempotent Remove: %s -> %s", firstRV, second.ResourceVersion)
	}
}

func TestContainsNotReadyKey(t *testing.T) {
	pod := newReadinessTestPod("p")
	c := newReadinessTestClient(t, pod)

	if got := ContainsNotReadyKey(pod, Message{UserAgent: "A", Key: "1"}); got {
		t.Errorf("fresh pod has no keys; ContainsNotReadyKey should be false")
	}

	if err := AddNotReadyKey(context.Background(), c, c, pod, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if !ContainsNotReadyKey(got, Message{UserAgent: "A", Key: "1"}) {
		t.Errorf("expected ContainsNotReadyKey=true after Add")
	}
	if ContainsNotReadyKey(got, Message{UserAgent: "B", Key: "2"}) {
		t.Errorf("expected ContainsNotReadyKey=false for unrelated key")
	}
}

func TestIsServing(t *testing.T) {
	if IsServing(nil) {
		t.Errorf("nil pod should not be serving")
	}
	pod := newReadinessTestPod("p")
	if IsServing(pod) {
		t.Errorf("fresh pod should not be serving (no condition)")
	}
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:   ConditionType,
		Status: corev1.ConditionFalse,
	}}
	if IsServing(pod) {
		t.Errorf("Status=False should not be serving")
	}
	pod.Status.Conditions[0].Status = corev1.ConditionTrue
	if !IsServing(pod) {
		t.Errorf("Status=True should be serving")
	}
}

func TestAddRemove_PreservesKubeletConditions(t *testing.T) {
	// Patches must NOT clobber kubelet-managed conditions like
	// ContainersReady — the strategic-merge patch keyed by `type` leaves
	// other condition slots alone.
	pod := newReadinessTestPod("p")
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
	}
	c := newReadinessTestClient(t, pod)

	if err := AddNotReadyKey(context.Background(), c, c, pod, Message{UserAgent: "Delete-drain", Key: "0"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)

	if findCondition(got, corev1.PodScheduled) == nil {
		t.Errorf("kubelet's PodScheduled condition was clobbered")
	}
	if findCondition(got, corev1.ContainersReady) == nil {
		t.Errorf("kubelet's ContainersReady condition was clobbered")
	}
	if findCondition(got, ConditionType) == nil {
		t.Errorf("our condition was not written")
	}
}

func TestAddNotReadyKey_NilPodRejected(t *testing.T) {
	c := newReadinessTestClient(t)
	if err := AddNotReadyKey(context.Background(), c, c, nil, Message{UserAgent: "x", Key: "y"}); err == nil {
		t.Fatalf("expected error on nil pod")
	}
}

func TestRemoveNotReadyKey_NilPodRejected(t *testing.T) {
	c := newReadinessTestClient(t)
	if err := RemoveNotReadyKey(context.Background(), c, c, nil, Message{UserAgent: "x", Key: "y"}); err == nil {
		t.Fatalf("expected error on nil pod")
	}
}

// TestRemoveLastKey_ClearsMessageField pins that removing the last
// writer leaves the persisted pod's message field actually empty.
//
// PodCondition.Message has `json:",omitempty"`, so the typed
// corev1.PodCondition.MarshalJSON drops the field when empty. A patch
// issued through the typed marshal with Status=True + Message="" would
// carry no `message` key, so the apiserver would preserve the prior
// Status=False message list. The pod would end up Status=True with a
// stale writer still in the message — the next in-place update's
// AddNotReadyKey would find its (UserAgent, Key) tuple already present,
// short-circuit without issuing a Status=False patch, and
// drain.IsPodDrained would observe the pod still in rotation, stalling
// the Instance at Phase=Updating with readyReplicas reading 0 for the
// entire post-patch window.
//
// Assertion: after the last writer is removed the message field is
// actually empty on the persisted pod, which the patchCondition
// hand-marshal guarantees.
func TestRemoveLastKey_ClearsMessageField(t *testing.T) {
	pod := newReadinessTestPod("p")
	c := newReadinessTestClient(t, pod)

	// Add a writer to populate Status=False + non-empty message.
	if err := AddNotReadyKey(context.Background(), c, c, pod, Message{UserAgent: "Update-in-place", Key: "0-1"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	mid := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), mid)
	if cond := findCondition(mid, ConditionType); cond == nil || cond.Status != corev1.ConditionFalse || cond.Message == "" {
		t.Fatalf("seed: expected Status=False with non-empty message, got %+v", cond)
	}

	// Remove the writer — list becomes empty, Status flips True.
	if err := RemoveNotReadyKey(context.Background(), c, c, mid, Message{UserAgent: "Update-in-place", Key: "0-1"}); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	cond := findCondition(got, ConditionType)
	if cond == nil {
		t.Fatalf("condition not found after Remove")
	}
	if cond.Status != corev1.ConditionTrue {
		t.Errorf("expected Status=True after removing last writer, got %s", cond.Status)
	}
	if cond.Message != "" {
		t.Errorf("expected empty message after removing last writer, got %q (stale entry would deadlock the next in-place update)", cond.Message)
	}
}

// TestSecondInPlaceCycle_CanReDrain pins the end-to-end shape: two
// back-to-back in-place updates on the same
// (Instance, Incarnation) must each be able to flip the pod to
// Status=False. The first cycle drains, ContainersReady stays True,
// we re-mark Serving; the second cycle issues the same {UserAgent,
// Key} tuple — it MUST take effect, not short-circuit on a stale
// message list.
func TestSecondInPlaceCycle_CanReDrain(t *testing.T) {
	pod := newReadinessTestPod("p")
	c := newReadinessTestClient(t, pod)
	key := Message{UserAgent: WriterUpdateInPlace, Key: "0-1"}

	// Cycle 1: drain → un-drain.
	if err := AddNotReadyKey(context.Background(), c, c, pod, key); err != nil {
		t.Fatalf("cycle 1 Add: %v", err)
	}
	mid := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), mid)
	if err := RemoveNotReadyKey(context.Background(), c, c, mid, key); err != nil {
		t.Fatalf("cycle 1 Remove: %v", err)
	}

	// Cycle 2: drain again — MUST flip Status to False.
	cycle2Start := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), cycle2Start)
	if !IsServing(cycle2Start) {
		t.Fatalf("cycle 2 precondition: expected Serving=True after cycle 1 finished")
	}
	if err := AddNotReadyKey(context.Background(), c, c, cycle2Start, key); err != nil {
		t.Fatalf("cycle 2 Add: %v", err)
	}
	after := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), after)
	cond := findCondition(after, ConditionType)
	if cond == nil {
		t.Fatalf("cycle 2: condition missing")
	}
	if cond.Status != corev1.ConditionFalse {
		t.Errorf("cycle 2 expected Status=False after Add, got %s — second-cycle drain would never converge", cond.Status)
	}
	list := mustParseList(t, cond.Message)
	if len(list) != 1 || list[0] != key {
		t.Errorf("cycle 2 expected single {%s, %s} entry, got %v", key.UserAgent, key.Key, list)
	}
}

// TestAddNotReadyKey_RecoversInconsistentStatusTrueWithStaleMessage
// pins the self-healing branch in AddNotReadyKey. A pod may be sitting
// at the paradoxical {Status=True, Message=[stale-writer]} state if a
// last-writer-removed patch ever omitted Message via json:",omitempty".
// The next in-place update cycle MUST be able to drain such pods —
// otherwise the only recovery path is operator-triggered pod restarts.
//
// Setup: synthesize a pod in the paradoxical state. Call
// AddNotReadyKey for the same {UserAgent, Key} tuple that's already
// in the list (mirroring a follow-up update reusing the same
// {Instance, Incarnation}). Assert Status flips to False so drain
// can proceed.
func TestAddNotReadyKey_RecoversInconsistentStatusTrueWithStaleMessage(t *testing.T) {
	staleMsg := Message{UserAgent: WriterUpdateInPlace, Key: "0-1"}
	staleList := messageList{staleMsg}
	pod := newReadinessTestPod("p")
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:    ConditionType,
		Status:  corev1.ConditionTrue, // paradox: True but list non-empty
		Reason:  "Serving",
		Message: staleList.dump(),
	}}
	c := newReadinessTestClient(t, pod)

	if err := AddNotReadyKey(context.Background(), c, c, pod, staleMsg); err != nil {
		t.Fatalf("AddNotReadyKey: %v", err)
	}

	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	cond := findCondition(got, ConditionType)
	if cond == nil {
		t.Fatalf("condition missing after Add")
	}
	if cond.Status != corev1.ConditionFalse {
		t.Errorf("expected Status=False after self-heal Add on paradoxical condition, got %s — drain would never converge for an in-place rollout reusing the same key", cond.Status)
	}
	list := mustParseList(t, cond.Message)
	if len(list) != 1 || list[0] != staleMsg {
		t.Errorf("expected single {%s, %s} entry after self-heal, got %v", staleMsg.UserAgent, staleMsg.Key, list)
	}
}

// TestAddNotReadyKey_ConflictRetryPreservesConcurrentWriter pins the
// RV-pinned patch: a competing writer landing between our re-read and
// our patch must force a 409 so RetryOnConflict recomputes against the
// new base. Without the resourceVersion in the patch body the stale
// patch applies cleanly and silently drops the competing writer's hold.
func TestAddNotReadyKey_ConflictRetryPreservesConcurrentWriter(t *testing.T) {
	pod := newReadinessTestPod("p")
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	competitor := Message{UserAgent: WriterMigrateSourceDrain, Key: "uuid-1"}
	raced := false
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pod).
		WithStatusSubresource(&corev1.Pod{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, cl client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if !raced {
					raced = true
					live := &corev1.Pod{}
					if err := cl.Get(ctx, client.ObjectKeyFromObject(pod), live); err != nil {
						t.Fatalf("interceptor get: %v", err)
					}
					if err := AddNotReadyKey(ctx, cl, cl, live, competitor); err != nil {
						t.Fatalf("interceptor competing Add: %v", err)
					}
				}
				return cl.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	ours := Message{UserAgent: WriterUpdateInPlace, Key: "0-1"}
	if err := AddNotReadyKey(context.Background(), c, c, pod, ours); err != nil {
		t.Fatalf("AddNotReadyKey: %v", err)
	}

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get: %v", err)
	}
	cond := findCondition(got, ConditionType)
	if cond == nil || cond.Status != corev1.ConditionFalse {
		t.Fatalf("expected Status=False, got %+v", cond)
	}
	list := mustParseList(t, cond.Message)
	if len(list) != 2 {
		t.Fatalf("expected both writers' holds to survive the race, got %v", list)
	}
	seen := map[Message]bool{}
	for _, m := range list {
		seen[m] = true
	}
	if !seen[ours] || !seen[competitor] {
		t.Errorf("stale-base patch dropped a concurrent writer's hold: %v", list)
	}
}

func TestPatchCondition_StaleBaseRejected(t *testing.T) {
	pod := newReadinessTestPod("p")
	c := newReadinessTestClient(t, pod)

	stale := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), stale); err != nil {
		t.Fatalf("get: %v", err)
	}
	// Bump the stored pod so the copy above goes stale.
	if err := AddNotReadyKey(context.Background(), c, c, pod, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	err := patchCondition(context.Background(), c, stale, corev1.PodCondition{
		Type:               ConditionType,
		Status:             corev1.ConditionTrue,
		Reason:             "Serving",
		LastTransitionTime: metav1.Now(),
	})
	if !apierrors.IsConflict(err) {
		t.Fatalf("expected conflict from stale-base patch, got %v", err)
	}
}

// TestRemoveNotReadyKey_PodGone_Errors pins the promote-to-serving
// contract: a deleted pod must surface NotFound, not silently succeed.
// A surge pod evicted between the reconcile-start snapshot and the
// promote patch would otherwise let the caller "promote" nothing and
// drain the old pod in the same pass — an availability outage (a full
// outage at replicas=1) with zero signal.
func TestRemoveNotReadyKey_PodGone_Errors(t *testing.T) {
	pod := newReadinessTestPod("gone")
	c := newReadinessTestClient(t)
	err := RemoveNotReadyKey(context.Background(), c, c, pod, Message{UserAgent: WriterLifecycle, Key: KeyLifecycleInstanceReady})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected NotFound for a deleted pod on the promote path, got %v", err)
	}
}

// TestMarkPodServing_PodGone_Errors pins the same contract through the
// wrapper every promote caller uses.
func TestMarkPodServing_PodGone_Errors(t *testing.T) {
	pod := newReadinessTestPod("gone")
	c := newReadinessTestClient(t)
	err := MarkPodServing(context.Background(), c, c, pod, WriterLifecycle, KeyLifecycleInstanceReady)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected NotFound from MarkPodServing on a deleted pod, got %v", err)
	}
}

// TestRemoveNotReadyKeyIgnoreNotFound_PodGone_ReturnsNil pins the
// drain-hold release contract: the hold dies with the pod, so a
// deleted pod is a clean no-op (migration-expiry un-drain of source
// pods that may already be gone).
func TestRemoveNotReadyKeyIgnoreNotFound_PodGone_ReturnsNil(t *testing.T) {
	pod := newReadinessTestPod("gone")
	c := newReadinessTestClient(t)
	if err := RemoveNotReadyKeyIgnoreNotFound(context.Background(), c, c, pod, Message{UserAgent: WriterMigrateSourceDrain, Key: "uuid-1"}); err != nil {
		t.Fatalf("expected nil for a deleted pod on drain-hold release, got %v", err)
	}
}

// TestAddNotReadyKey_LaggingCacheConvergesViaLiveReader pins the
// live-reader re-read: the RV-pinned patch base must come from the
// reader, not the (possibly lagging) cached client. A cache that keeps
// serving the resourceVersion from before another writer's patch would
// otherwise feed the same stale base to every RetryOnConflict attempt —
// the loop 409s until the budget is exhausted and the whole pass fails
// instead of converging.
func TestAddNotReadyKey_LaggingCacheConvergesViaLiveReader(t *testing.T) {
	pod := newReadinessTestPod("p")
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	live := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pod).
		WithStatusSubresource(&corev1.Pod{}).
		Build()

	// Freeze a "cache" snapshot, then land a competing writer so the
	// stored pod's resourceVersion moves past it.
	staleSnapshot := &corev1.Pod{}
	if err := live.Get(context.Background(), client.ObjectKeyFromObject(pod), staleSnapshot); err != nil {
		t.Fatalf("snapshot get: %v", err)
	}
	competitor := Message{UserAgent: WriterMigrateSourceDrain, Key: "uuid-1"}
	if err := AddNotReadyKey(context.Background(), live, live, pod, competitor); err != nil {
		t.Fatalf("competing Add: %v", err)
	}

	// lagging serves the frozen snapshot on every Get; writes pass through.
	lagging := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pod).
		WithStatusSubresource(&corev1.Pod{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				staleSnapshot.DeepCopyInto(obj.(*corev1.Pod))
				return nil
			},
			SubResourcePatch: func(ctx context.Context, cl client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				return live.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	ours := Message{UserAgent: WriterUpdateInPlace, Key: "0-1"}
	if err := AddNotReadyKey(context.Background(), lagging, live, pod, ours); err != nil {
		t.Fatalf("AddNotReadyKey against a lagging cache must converge via the live reader, got %v", err)
	}

	got := &corev1.Pod{}
	if err := live.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get: %v", err)
	}
	cond := findCondition(got, ConditionType)
	if cond == nil || cond.Status != corev1.ConditionFalse {
		t.Fatalf("expected Status=False, got %+v", cond)
	}
	list := mustParseList(t, cond.Message)
	seen := map[Message]bool{}
	for _, m := range list {
		seen[m] = true
	}
	if len(list) != 2 || !seen[ours] || !seen[competitor] {
		t.Errorf("expected both writers' holds after converging, got %v", list)
	}
}

// TestMalformedWriterList_FailsSafe pins the corrupt-state behavior:
// a writer list that doesn't parse must surface an error, not decode
// to "no writers hold the gate" and release other writers' drain holds.
func TestMalformedWriterList_FailsSafe(t *testing.T) {
	const garbage = "{not-json"
	pod := newReadinessTestPod("p")
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:    ConditionType,
		Status:  corev1.ConditionFalse,
		Reason:  "NotReady",
		Message: garbage,
	}}
	c := newReadinessTestClient(t, pod)
	msg := Message{UserAgent: WriterMigrateSourceDrain, Key: "uuid-1"}

	if err := RemoveNotReadyKey(context.Background(), c, c, pod, msg); err == nil {
		t.Errorf("Remove on malformed list must error, not release the gate")
	}
	if err := AddNotReadyKey(context.Background(), c, c, pod, msg); err == nil {
		t.Errorf("Add on malformed list must error, not rebuild the list")
	}

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get: %v", err)
	}
	cond := findCondition(got, ConditionType)
	if cond == nil || cond.Status != corev1.ConditionFalse || cond.Message != garbage {
		t.Errorf("malformed condition must be left untouched, got %+v", cond)
	}
	if ContainsNotReadyKey(got, msg) {
		t.Errorf("ContainsNotReadyKey on malformed list must be false")
	}
}

func TestMessageListSerialization_Deterministic(t *testing.T) {
	// Two writers adding in opposite orders should produce the same
	// serialized list, so operators inspecting the pod see stable
	// output between reconciles when nothing changed.
	pod := newReadinessTestPod("p1")
	c := newReadinessTestClient(t, pod)
	if err := AddNotReadyKey(context.Background(), c, c, pod, Message{UserAgent: "B", Key: "2"}); err != nil {
		t.Fatalf("Add B: %v", err)
	}
	p1 := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), p1)
	if err := AddNotReadyKey(context.Background(), c, c, p1, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("Add A: %v", err)
	}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), p1)
	cond1 := findCondition(p1, ConditionType)

	pod2 := newReadinessTestPod("p2")
	c2 := newReadinessTestClient(t, pod2)
	if err := AddNotReadyKey(context.Background(), c2, c2, pod2, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("Add A: %v", err)
	}
	p2 := &corev1.Pod{}
	_ = c2.Get(context.Background(), client.ObjectKeyFromObject(pod2), p2)
	if err := AddNotReadyKey(context.Background(), c2, c2, p2, Message{UserAgent: "B", Key: "2"}); err != nil {
		t.Fatalf("Add B: %v", err)
	}
	_ = c2.Get(context.Background(), client.ObjectKeyFromObject(pod2), p2)
	cond2 := findCondition(p2, ConditionType)

	if cond1.Message != cond2.Message {
		t.Errorf("Message ordering not stable:\n  p1: %s\n  p2: %s", cond1.Message, cond2.Message)
	}
}

// TestIsPodAvailable pins the Deployment minReadySeconds rule: Available
// is PodReady plus a Ready age of at least the window, with the window's
// boundary inclusive and a zero window collapsing to plain Ready.
func TestIsPodAvailable(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	readyPod := func(readyAt time.Time) *corev1.Pod {
		pod := newReadinessTestPod("p")
		pod.Status.Conditions = []corev1.PodCondition{{
			Type:               corev1.PodReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.NewTime(readyAt),
		}}
		return pod
	}
	tests := []struct {
		name          string
		pod           *corev1.Pod
		window        int32
		wantAvailable bool
		wantRetry     time.Duration
	}{
		{name: "nil pod", pod: nil, window: 10},
		{name: "no Ready condition", pod: newReadinessTestPod("p"), window: 10},
		{
			name: "Ready False",
			pod: func() *corev1.Pod {
				pod := readyPod(now.Add(-time.Hour))
				pod.Status.Conditions[0].Status = corev1.ConditionFalse
				return pod
			}(),
			window: 10,
		},
		{name: "zero window is Ready", pod: readyPod(now), window: 0, wantAvailable: true},
		{name: "negative window is Ready", pod: readyPod(now), window: -5, wantAvailable: true},
		{name: "inside window", pod: readyPod(now.Add(-4 * time.Second)), window: 10, wantRetry: 6 * time.Second},
		{name: "exactly at window boundary", pod: readyPod(now.Add(-10 * time.Second)), window: 10, wantAvailable: true},
		{name: "past window", pod: readyPod(now.Add(-11 * time.Second)), window: 10, wantAvailable: true},
		{
			name: "Ready without transition time cannot prove age",
			pod: func() *corev1.Pod {
				pod := readyPod(now)
				pod.Status.Conditions[0].LastTransitionTime = metav1.Time{}
				return pod
			}(),
			window: 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			available, retry := IsPodAvailable(tt.pod, tt.window, now)
			if available != tt.wantAvailable {
				t.Fatalf("available: got %v want %v", available, tt.wantAvailable)
			}
			if retry != tt.wantRetry {
				t.Fatalf("retry: got %v want %v", retry, tt.wantRetry)
			}
		})
	}
}
