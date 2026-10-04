// Package podreadiness implements the multi-writer readiness gate
// protocol for OMENative's `ome.io/serving` pod condition.
//
// Pattern ported from RBG (`sigs.k8s.io/rbgs/pkg/inplace/pod/readiness`).
// The condition's Message field carries a JSON list of {UserAgent, Key}
// entries — one per writer that wants the pod NotReady right now.
// Status=True iff the list is empty AND containers are ready (the
// latter is kubelet's responsibility via the standard readiness gate
// machinery).
//
// Why multi-writer: OMENative has at least four overlapping reasons
// to want a pod NotReady — migration source drain, in-place update
// drain, restart drain, scale-down drain — that may be in flight on
// the same pod simultaneously. A single binary True/False gate has
// the writers race to overwrite each other; the multi-writer protocol
// lets them coexist as independent message-list entries.
//
// All writes use Status().Patch with strategic merge on the condition
// list (patch-merge-key=type), so kubelet's concurrent writes to
// PodScheduled / ContainersReady / PodReady are preserved. Every patch
// pins metadata.resourceVersion from the read it was computed against,
// so a stale base 409s instead of silently overwriting another
// writer's hold; retry.RetryOnConflict re-reads and re-applies. The
// in-loop re-read goes through the caller-supplied live reader (the
// AuthoritativeReader role): a watch-backed cache lagging the
// controller's own preceding status patch would re-serve the stale
// resourceVersion on every retry and the loop could never converge.
//
// # Gang readiness equivalence (multi-pod)
//
// Multi-pod Instances require gang-readiness: each pod should remain
// NotReady until every sibling in the Instance reaches ContainersReady.
// The multi-pod path carries no separately-keyed gang writer; the
// gang-readiness contract is enforced instead by the Instance-create
// flow, which calls MarkPodServing on the Instance's pods ONLY once
// every leader and every worker reports ContainersReady=True. Before
// that gate no pod in the gang has serving=True, so none receives
// traffic.
//
// That gate is observably equivalent to a per-pod gang writer:
//   - A new pod's WriterLifecycle/KeyLifecycleInstanceReady stays in
//     the message list until the controller writes the MarkPodServing
//     removal in the create flow's Ready promotion. (The condition
//     starts non-existent on a fresh pod, which is treated as "writer
//     is implicitly holding"; the first MarkPodServing creates it with
//     Status=True.)
//   - The gate covers the WHOLE Instance, so the flip happens
//     atomically across leader and workers. There is no partial
//     "leader serving, workers still warming" window.
//
// A per-pod gang hold — wanted, for instance, by a partial in-place
// update on a multi-pod gang — is a separately-keyed writer the
// multi-writer protocol already admits without re-architecting.
package podreadiness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ConditionType is the controller-owned readiness gate OMENative
// stamps on every managed pod. The same value Render appends to
// pod.spec.readinessGates so kubelet AND's it into PodReady.
const ConditionType corev1.PodConditionType = "ome.io/serving"

// Message identifies one writer that wants the pod NotReady. The
// pair {UserAgent, Key} is unique per logical reason — e.g.,
// {"Update-in-place", "0-3"} for an in-place update on Instance 0
// incarnation 3.
type Message struct {
	UserAgent string `json:"userAgent"`
	Key       string `json:"key"`
}

// ErrPodIdentityChanged means a same-name Pod's UID differs from the caller's
// observation. The replacement must not receive the stale effect.
var ErrPodIdentityChanged = errors.New("pod identity changed")

// AddNotReadyKey appends msg to the readiness condition's message list
// and sets Status=False. No-op if msg is already present AND the
// condition Status is already False (the writer slot is already held).
// Patches only the single condition slot so kubelet's concurrent
// status writes are preserved; wrapped in retry.RetryOnConflict.
//
// reader is the live reader the RV-pinned patch base is read through
// (nil falls back to c, acceptable only when c does not lag the API
// server — e.g. tests).
//
// Self-healing branch: when the message list already contains msg but
// Status is True we still issue a patch to flip Status back to False.
// This resyncs a divergent shape — stale {UserAgent, Key} entries left
// in the message while Status has already flipped True. Without this
// recovery, a fresh in-place update reusing the same {idx, incarnation}
// key would short-circuit as "already held", no NotReady patch fires,
// drain.IsPodDrained sees the pod still Ready in the EndpointSlice,
// and the Instance hangs at Phase=Updating forever. With this branch,
// the first Add after a controller restart resyncs Status to match
// the message-list reality.
func AddNotReadyKey(ctx context.Context, c client.Client, reader client.Reader, pod *corev1.Pod, msg Message) error {
	if pod == nil {
		return fmt.Errorf("AddNotReadyKey: nil pod")
	}
	if reader == nil {
		reader = c
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &corev1.Pod{}
		if err := reader.Get(ctx, client.ObjectKeyFromObject(pod), fresh); err != nil {
			return fmt.Errorf("re-read pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		if pod.UID != "" && fresh.UID != pod.UID {
			return fmt.Errorf("%w: pod %s/%s", ErrPodIdentityChanged, pod.Namespace, pod.Name)
		}
		cond := findCondition(fresh, ConditionType)
		var base string
		var existingStatus corev1.ConditionStatus
		if cond != nil {
			base = cond.Message
			existingStatus = cond.Status
		}
		changed, list, err := addMessage(base, msg)
		if err != nil {
			return fmt.Errorf("pod %s/%s: %w", fresh.Namespace, fresh.Name, err)
		}
		if !changed && existingStatus == corev1.ConditionFalse {
			// Nothing to do — key already in the list AND Status is
			// already False (writer slot already held; everything
			// consistent).
			return nil
		}
		return patchCondition(ctx, c, fresh, corev1.PodCondition{
			Type:               ConditionType,
			Status:             corev1.ConditionFalse,
			Reason:             "NotReady",
			Message:            list.dump(),
			LastTransitionTime: transitionTime(cond, corev1.ConditionFalse),
		})
	})
}

// transitionTime is the LastTransitionTime to stamp when writing status
// onto cond. A write that only reshuffles the writer list must carry the
// old timestamp forward, or every writer that joins or leaves resets the
// "how long has this pod been NotReady" clock consumers read.
func transitionTime(cond *corev1.PodCondition, status corev1.ConditionStatus) metav1.Time {
	if cond != nil && cond.Status == status {
		return cond.LastTransitionTime
	}
	return metav1.Now()
}

// RemoveNotReadyKey removes msg from the readiness condition's
// message list. If the list becomes empty (or the condition doesn't
// exist yet — fresh pod case), sets Status=True. Otherwise keeps
// Status=False with the shrunken list.
//
// A pod that no longer exists is an ERROR (NotFound). This is the
// promote-to-serving contract: callers flip a pod into rotation and
// then act on that promotion (e.g. drain its predecessor in the same
// pass), so a silently-vanished pod must abort the caller before it
// removes the only serving replica. Drain-hold release paths, where
// the hold genuinely dies with the pod, use
// RemoveNotReadyKeyIgnoreNotFound instead.
//
// Pod names here are slot-based, so a same-name pod carrying a
// different UID is a replacement, not the caller's pod: that is
// ErrPodIdentityChanged, for the same reason.
//
// reader is the live reader the RV-pinned patch base is read through
// (nil falls back to c). Wrapped in retry.RetryOnConflict; patches
// only the single condition slot.
func RemoveNotReadyKey(ctx context.Context, c client.Client, reader client.Reader, pod *corev1.Pod, msg Message) error {
	_, err := removeNotReadyKey(ctx, c, reader, pod, msg, false)
	return err
}

// RemoveNotReadyKeyIgnoreNotFound is RemoveNotReadyKey for drain-hold
// release: a pod that no longer exists — vanished outright, or replaced
// under the same name by a pod with a different UID — is a no-op (nil)
// because the hold dies with the pod. Never use it on a
// promote-to-serving path —
// tolerating NotFound there turns "replacement is in rotation" into
// "replacement may be gone" and lets the caller drain its predecessor
// with nothing serving.
func RemoveNotReadyKeyIgnoreNotFound(ctx context.Context, c client.Client, reader client.Reader, pod *corev1.Pod, msg Message) error {
	_, err := removeNotReadyKey(ctx, c, reader, pod, msg, true)
	return err
}

func removeNotReadyKey(ctx context.Context, c client.Client, reader client.Reader, pod *corev1.Pod, msg Message, ignoreNotFound bool) (bool, error) {
	if pod == nil {
		return false, fmt.Errorf("RemoveNotReadyKey: nil pod")
	}
	if reader == nil {
		reader = c
	}
	changed := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		changed = false
		fresh := &corev1.Pod{}
		if err := reader.Get(ctx, client.ObjectKeyFromObject(pod), fresh); err != nil {
			if ignoreNotFound && apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("re-read pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		if pod.UID != "" && fresh.UID != pod.UID {
			// A same-name replacement occupies the slot. The caller's
			// hold died with its pod, so releasing it here would clear
			// a gate the replacement never earned. For the tolerant
			// variant that is the same no-op as a vanished pod.
			if ignoreNotFound {
				return nil
			}
			return fmt.Errorf("%w: pod %s/%s", ErrPodIdentityChanged, pod.Namespace, pod.Name)
		}
		cond := findCondition(fresh, ConditionType)
		if cond == nil {
			// An absent condition is the implicit Lifecycle hold on a
			// fresh pod. Only the promote path may resolve it: that IS
			// its Ready promotion. A drain-hold release has nothing to
			// release here — its hold was never recorded — and writing
			// Status=True would promote a pod no writer promoted,
			// slipping past the Instance-wide gang gate.
			if ignoreNotFound {
				return nil
			}
			// Removing a key that isn't present is vacuously "no
			// writers hold it NotReady" → write Status=True.
			if err := patchCondition(ctx, c, fresh, corev1.PodCondition{
				Type:               ConditionType,
				Status:             corev1.ConditionTrue,
				Reason:             "Serving",
				LastTransitionTime: metav1.Now(),
			}); err != nil {
				return err
			}
			changed = true
			return nil
		}
		messageChanged, list, err := removeMessage(cond.Message, msg)
		if err != nil {
			return fmt.Errorf("pod %s/%s: %w", fresh.Namespace, fresh.Name, err)
		}
		if !messageChanged && cond.Status == corev1.ConditionTrue {
			// Key wasn't in the list AND condition already True.
			// Nothing to do.
			return nil
		}
		status := corev1.ConditionTrue
		reason := "Serving"
		message := ""
		if len(list) > 0 {
			status = corev1.ConditionFalse
			reason = "NotReady"
			message = list.dump()
		}
		// If only the status field needs updating, still issue a
		// patch — the list dedup is in the helper.
		if err := patchCondition(ctx, c, fresh, corev1.PodCondition{
			Type:               ConditionType,
			Status:             status,
			Reason:             reason,
			Message:            message,
			LastTransitionTime: transitionTime(cond, status),
		}); err != nil {
			return err
		}
		changed = messageChanged || cond.Status != status
		return nil
	})
	return changed, err
}

// ContainsNotReadyKey reports whether the readiness condition holds
// msg as a live hold — msg is in the message list AND Status is False.
// Read-side check used by callers that want to skip a redundant Add.
//
// The Status=False requirement makes this "is the hold in effect", not
// "is the entry present". A condition in the paradoxical Status=True
// state with a non-empty list therefore reports false. That is what an
// Add caller wants, since AddNotReadyKey repairs the paradox on its
// next write. A Remove caller must NOT use this to skip its call: the
// stale entry is exactly what needs removing, and skipping leaves it.
func ContainsNotReadyKey(pod *corev1.Pod, msg Message) bool {
	if pod == nil {
		return false
	}
	cond := findCondition(pod, ConditionType)
	if cond == nil || cond.Status == corev1.ConditionTrue || cond.Message == "" {
		return false
	}
	list, err := parseList(cond.Message)
	if err != nil {
		return false
	}
	for _, m := range list {
		if m == msg {
			return true
		}
	}
	return false
}

// IsServing reports whether the condition exists with Status=True —
// true iff no writer currently holds the pod NotReady.
func IsServing(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	cond := findCondition(pod, ConditionType)
	return cond != nil && cond.Status == corev1.ConditionTrue
}

// HeldNotServing reports whether a writer currently holds the pod out of
// rotation: the condition exists with Status != True. A pod without the
// condition is not held, which is what separates this from !IsServing: a
// reader keyed on kubelet PodReady can honor the hold while PodReady still
// reads True, since kubelet re-evaluates the gate asynchronously.
func HeldNotServing(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	cond := findCondition(pod, ConditionType)
	return cond != nil && cond.Status != corev1.ConditionTrue
}

// ReadyAndServing reports whether the pod is serving: its Ready condition
// and the serving gate are both True. The gate is read on its own because
// a drain flips it before the kubelet folds it into Ready; Ready is read on
// its own because the node lifecycle controller revokes it on a pod whose
// kubelet stopped reporting, leaving the container statuses and the gate at
// their last values. Until both agree the pod is in no Service's endpoints.
func ReadyAndServing(pod *corev1.Pod) bool {
	return IsPodReady(pod) && IsServing(pod)
}

// IsContainersReady reports whether the kubelet-owned ContainersReady
// condition is True — i.e., all containers in the pod have passed their
// readiness probes.
//
// Distinct from corev1.PodReady (which ANDs ContainersReady with every
// readiness gate including ome.io/serving). Gating MarkPodServing on
// PodReady would deadlock: kubelet won't flip PodReady=True until
// ome.io/serving=True, and OMENative won't write ome.io/serving=True
// until PodReady=True. ContainersReady reflects only probe-level
// readiness and is the correct signal that the runtime is healthy
// enough to receive traffic.
func IsContainersReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.ContainersReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// IsPodReady reports whether the kubelet-owned PodReady condition is True —
// containers ready AND every readiness gate (including ome.io/serving)
// satisfied. This is the signal that the pod is actually in Service rotation,
// distinct from IsContainersReady (probes only) and IsServing (the gate
// condition alone, before kubelet re-evaluates PodReady). Use this to confirm
// a freshly-served replacement is carrying traffic before draining its source.
func IsPodReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// IsPodAvailable reports whether the pod is Available: PodReady is True and
// the condition's lastTransitionTime plus minReadySeconds is not after now
// (the Deployment minReadySeconds rule). A window <= 0 makes Available the
// same as Ready. The duration is how much longer the pod must stay Ready
// before it becomes Available; it is 0 when the pod already is, when it is
// not Ready, and when a Ready condition carries no lastTransitionTime (its
// age cannot be proven, so no later instant makes it Available). The status
// aggregator folds the smallest pending duration into the reconcile requeue
// so the Available counters refresh when the window elapses; the update
// strategies discard it because an in-flight Update already polls.
func IsPodAvailable(pod *corev1.Pod, minReadySeconds int32, now time.Time) (bool, time.Duration) {
	if pod == nil {
		return false, 0
	}
	var ready *corev1.PodCondition
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			ready = &pod.Status.Conditions[i]
			break
		}
	}
	if ready == nil || ready.Status != corev1.ConditionTrue {
		return false, 0
	}
	if minReadySeconds <= 0 {
		return true, 0
	}
	if ready.LastTransitionTime.IsZero() {
		return false, 0
	}
	window := time.Duration(minReadySeconds) * time.Second
	availableAt := ready.LastTransitionTime.Add(window)
	if !now.Before(availableAt) {
		return true, 0
	}
	return false, availableAt.Sub(now)
}

// MarkPodServing removes the {userAgent, key} entry from the
// ome.io/serving condition's writer list. If the resulting list is
// empty (or the condition didn't exist yet — fresh pod case), the
// condition is written with Status=True and the pod becomes eligible
// for Service rotation.
//
// Idempotent: removing a key that isn't present is a no-op (no patch
// issued) when the condition is already Status=True. A deleted pod is
// an error (see RemoveNotReadyKey): success means the pod IS in
// rotation, so promote-then-drain callers may safely act on it.
func MarkPodServing(ctx context.Context, c client.Client, reader client.Reader, pod *corev1.Pod, userAgent, key string) error {
	return RemoveNotReadyKey(ctx, c, reader, pod, Message{UserAgent: userAgent, Key: key})
}

// MarkPodServingWithChange is MarkPodServing plus whether this call committed
// a condition change. Callers use the result when they may need to compensate
// only the serving transition they own.
func MarkPodServingWithChange(ctx context.Context, c client.Client, reader client.Reader, pod *corev1.Pod, userAgent, key string) (bool, error) {
	return removeNotReadyKey(ctx, c, reader, pod, Message{UserAgent: userAgent, Key: key}, false)
}

// MarkPodNotServing adds the {userAgent, key} entry to the
// ome.io/serving condition's writer list. Status becomes False (because
// the list is non-empty). The pod is removed from Service rotation via
// kube-proxy's standard PodReady gate machinery.
//
// Idempotent: re-adding the same key is a no-op.
func MarkPodNotServing(ctx context.Context, c client.Client, reader client.Reader, pod *corev1.Pod, userAgent, key string) error {
	return AddNotReadyKey(ctx, c, reader, pod, Message{UserAgent: userAgent, Key: key})
}

// Writer userAgents used across the OMENative ops. Centralized here so
// a stray drift in any one site is easy to catch in `git grep`.
const (
	// WriterLifecycle flips a fresh pod's gate to Status=True once all
	// Instance siblings reach ContainersReady. The key
	// (KeyLifecycleInstanceReady) is the same across all sites — the
	// userAgent + key tuple is the lookup; we never need to differentiate.
	WriterLifecycle = "Lifecycle"

	// WriterUpdateInPlace drains a pod for in-place container image
	// update. Same key on Add (drain) and Remove (un-drain) on the
	// same pod.
	WriterUpdateInPlace = "Update-in-place"

	// WriterUpdateRecreateDrain drains the OLD pods for a recreate
	// update. The NEW pods are fresh and go through WriterLifecycle.
	WriterUpdateRecreateDrain = "Update-recreate-drain"

	// WriterRestartDrain drains the OLD pods for an Instance restart.
	// The NEW pods are fresh and go through WriterLifecycle.
	WriterRestartDrain = "Restart-drain"

	// WriterDeleteDrain drains a pod for scale-down deletion. The key
	// is removed by virtue of the pod being deleted; no explicit
	// RemoveNotReadyKey is required.
	WriterDeleteDrain = "Delete-drain"

	// WriterMigrateSourceDrain drains the source pods of a surge
	// migration. The key is removed by virtue of the source pods being
	// deleted at the end of the migration.
	WriterMigrateSourceDrain = "Migrate-source-drain"

	// WriterUpdateSurgeDrain drains the OLD pod during a SurgeThenDrain
	// rollout — after the surge pod (at the other ordinal slot) reaches
	// Ready, the old pod is drained via this writer then deleted. The
	// surge pod is fresh and goes through WriterLifecycle.
	WriterUpdateSurgeDrain = "Update-surge-drain"
)

// KeyLifecycleInstanceReady is the universal Lifecycle key. Matches
// RBG's convention. The pair (WriterLifecycle, KeyLifecycleInstanceReady)
// always means "the gate is unheld; if no other writer holds it, status
// is True".
const KeyLifecycleInstanceReady = "InstanceReady"

// patchCondition issues a strategic-merge patch updating only the
// single ome.io/serving condition. Kubelet's concurrent writes to
// the other Pod.Status.Conditions entries (PodScheduled, Initialized,
// ContainersReady, Ready, ...) are preserved because patch-merge-key
// on PodCondition is `type`.
//
// The patch is hand-marshaled (not via corev1.PodCondition) because
// PodCondition.Message has `json:",omitempty"` — when the controller
// removes the last writer and writes Status=True with Message="" the
// typed Marshal omits the message field entirely, so strategic-merge
// keeps the stale message list from the previous Status=False patch.
// The next AddNotReadyKey call then sees the orphaned key still in
// the list, addMessage returns changed=false, no patch fires, the
// pod stays Status=True with a stale writer in the message, and
// drain.IsPodDrained never observes the pod leaving rotation — the
// in-place update stalls forever in Phase=Updating. Forcing the
// "message" field into the patch payload (even when empty) makes
// strategic-merge clear the stale list so the next writer cycle
// starts from a clean slate.
//
// The patch pins pod's resourceVersion (the fresh read the new list
// was computed from) so a stale base gets a 409 instead of silently
// dropping a concurrent writer's entry; the callers' RetryOnConflict
// then re-reads and recomputes.
func patchCondition(ctx context.Context, c client.Client, pod *corev1.Pod, cond corev1.PodCondition) error {
	condMap := map[string]any{
		"type":               string(cond.Type),
		"status":             string(cond.Status),
		"reason":             cond.Reason,
		"message":            cond.Message,
		"lastTransitionTime": cond.LastTransitionTime,
	}
	patch := map[string]any{
		"metadata": map[string]any{
			"resourceVersion": pod.ResourceVersion,
		},
		"status": map[string]any{
			"conditions": []any{condMap},
		},
	}
	raw, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal condition patch for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	if err := c.Status().Patch(ctx, pod, client.RawPatch(types.StrategicMergePatchType, raw)); err != nil {
		return fmt.Errorf("patch %s on pod %s/%s: %w", ConditionType, pod.Namespace, pod.Name, err)
	}
	return nil
}

// findCondition returns a pointer to the condition matching condType
// in pod.Status.Conditions, or nil. Used by the multi-writer helpers
// to extract the current message list.
func findCondition(pod *corev1.Pod, condType corev1.PodConditionType) *corev1.PodCondition {
	if pod == nil {
		return nil
	}
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == condType {
			return &pod.Status.Conditions[i]
		}
	}
	return nil
}

// addMessage appends msg to the list parsed from base (an empty
// string is treated as an empty list). Returns changed=false if msg
// is already in the list.
func addMessage(base string, msg Message) (bool, messageList, error) {
	list, err := parseList(base)
	if err != nil {
		return false, nil, err
	}
	for _, m := range list {
		if m == msg {
			return false, list, nil
		}
	}
	list = append(list, msg)
	return true, list, nil
}

// removeMessage drops msg from the list parsed from base. Returns
// changed=false if msg isn't in the list.
func removeMessage(base string, msg Message) (bool, messageList, error) {
	list, err := parseList(base)
	if err != nil {
		return false, nil, err
	}
	var out messageList
	var removed bool
	for _, m := range list {
		if m == msg {
			removed = true
			continue
		}
		out = append(out, m)
	}
	return removed, out, nil
}

// parseList decodes a message list from its JSON serialization. An
// empty string yields nil ("no writers hold the gate"). A malformed
// string is an error, NOT an empty list — treating corruption as
// "no writers" would let RemoveNotReadyKey flip Status=True and
// release every other writer's drain hold with zero signal.
func parseList(raw string) (messageList, error) {
	if raw == "" {
		return nil, nil
	}
	var list messageList
	if err := json.Unmarshal([]byte(raw), &list); err != nil {
		return nil, fmt.Errorf("malformed %s writer list %q: %w", ConditionType, raw, err)
	}
	return list, nil
}

// messageList is a sortable slice of Messages. Stored in a stable
// order so the serialized form is deterministic — operators
// inspecting `kubectl get pod -o yaml` see the same list across
// reconciles when nothing changed.
type messageList []Message

func (l messageList) Len() int      { return len(l) }
func (l messageList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l messageList) Less(i, j int) bool {
	if l[i].UserAgent == l[j].UserAgent {
		return l[i].Key < l[j].Key
	}
	return l[i].UserAgent < l[j].UserAgent
}

// dump serializes the list in stable {UserAgent, Key} sort order.
func (l messageList) dump() string {
	if len(l) == 0 {
		return ""
	}
	sort.Sort(l)
	raw, _ := json.Marshal(l)
	return string(raw)
}
