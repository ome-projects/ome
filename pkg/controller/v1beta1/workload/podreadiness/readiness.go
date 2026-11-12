// Package podreadiness reads and writes the serving gate: the pod condition
// ome.io/serving that every rendered pod lists as a readiness gate, so the
// kubelet folds it into PodReady and a pod leaves its Service's endpoints
// while the gate is False.
//
// Several writers can hold one pod out of rotation at once: a drain for an
// in-place update, a restart, a scale-down or a migration, and the
// lifecycle hold on a fresh pod. The gate's message carries the JSON list
// of {userAgent, key} entries, one per writer holding it, and the gate is
// True exactly when the list is empty. A pod without the gate is held by
// the lifecycle writer until the Instance's create flow promotes it, once
// every pod of the Instance reports ContainersReady; that promotion covers
// the whole Instance, so a gang never serves in part.
//
// Every write patches the one condition with a strategic merge keyed on
// the condition type, so the kubelet's own conditions are kept, and names
// the resourceVersion of the fresh read it was computed from, so a
// concurrent writer's entry is never dropped: a stale base is a conflict,
// and the write re-reads through the live reader and applies again.
package podreadiness

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ConditionType is the serving gate: the controller-owned pod condition the
// rendered pods list in spec.readinessGates.
const ConditionType corev1.PodConditionType = "ome.io/serving"

// Message identifies one writer holding a pod out of rotation. The pair
// {UserAgent, Key} is unique per reason, such as the in-place update of
// one Instance.
type Message struct {
	UserAgent string `json:"userAgent"`
	Key       string `json:"key"`
}

// ErrPodIdentityChanged means a pod with another UID now holds the name of
// the caller's pod. The replacement never carried the caller's hold.
var ErrPodIdentityChanged = errors.New("pod identity changed")

// Writer userAgents, one per reason a pod is held out of rotation.
const (
	// WriterLifecycle is the implicit hold on a fresh pod; its one key
	// (KeyLifecycleInstanceReady) is removed when the Instance is promoted.
	WriterLifecycle = "Lifecycle"

	// WriterUpdateInPlace drains a pod for an in-place update and releases
	// the same key once the pod is back.
	WriterUpdateInPlace = "Update-in-place"

	// WriterUpdateRecreateDrain drains the old pods of a recreate update;
	// the new pods carry the lifecycle hold instead.
	WriterUpdateRecreateDrain = "Update-recreate-drain"

	// WriterRestartDrain drains the old pods of an Instance restart; the new
	// pods carry the lifecycle hold instead.
	WriterRestartDrain = "Restart-drain"

	// WriterDeleteDrain drains a pod a scale-down deletes; the hold ends
	// with the pod.
	WriterDeleteDrain = "Delete-drain"

	// WriterMigrateSourceDrain drains the source pods of a surge migration;
	// the hold ends with the pods.
	WriterMigrateSourceDrain = "Migrate-source-drain"

	// WriterUpdateSurgeDrain drains the old pod of a SurgeThenDrain update
	// once its replacement serves; the replacement carries the lifecycle
	// hold instead.
	WriterUpdateSurgeDrain = "Update-surge-drain"
)

// KeyLifecycleInstanceReady is the lifecycle writer's one key.
const KeyLifecycleInstanceReady = "InstanceReady"

// AddNotReadyKey records msg as a writer holding the pod out of rotation
// and writes the gate False. A hold already recorded and in effect writes
// nothing; a gate that reads True while its list still carries msg is
// written False again, so a key reused by the same writer takes effect.
// The pod is re-read through reader (c when nil) on every attempt; a pod
// gone or replaced under its name is an error.
func AddNotReadyKey(ctx context.Context, c client.Client, reader client.Reader, pod *corev1.Pod, msg Message) error {
	if pod == nil {
		return fmt.Errorf("AddNotReadyKey: nil pod")
	}
	if reader == nil {
		reader = c
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh, gate, err := freshGate(ctx, reader, pod)
		if err != nil {
			return err
		}
		stored := corev1.PodCondition{}
		if gate != nil {
			stored = *gate
		}
		added, list, err := addMessage(stored.Message, msg)
		if err != nil {
			return fmt.Errorf("pod %s/%s: %w", fresh.Namespace, fresh.Name, err)
		}
		if !added && stored.Status == corev1.ConditionFalse {
			return nil
		}
		return patchCondition(ctx, c, fresh, gateCondition(list, gate))
	})
}

// RemoveNotReadyKey removes msg from the writers holding the pod and writes
// the gate True once none is left, False with the shorter list otherwise.
// A pod without the gate is written True. Success means the pod is in
// rotation, so a caller may act on that and drain the pod it replaces; a
// pod gone, or replaced under its name by a pod with another UID
// (ErrPodIdentityChanged), is therefore an error. The pod is re-read
// through reader (c when nil) on every attempt.
func RemoveNotReadyKey(ctx context.Context, c client.Client, reader client.Reader, pod *corev1.Pod, msg Message) error {
	_, err := removeNotReadyKey(ctx, c, reader, pod, msg, promotion)
	return err
}

// RemoveNotReadyKeyIgnoreNotFound is RemoveNotReadyKey for releasing a
// drain hold. A hold dies with its pod, so a pod gone, replaced under its
// name, or without the gate (the hold was never recorded on it) writes
// nothing and returns nil. A promotion never uses it: success would not
// mean the pod serves.
func RemoveNotReadyKeyIgnoreNotFound(ctx context.Context, c client.Client, reader client.Reader, pod *corev1.Pod, msg Message) error {
	_, err := removeNotReadyKey(ctx, c, reader, pod, msg, holdRelease)
	return err
}

// releaseRule is what a release does when the pod it was asked to write is
// gone, is replaced under its name by a pod with another UID, or carries no
// gate yet.
type releaseRule int

const (
	// promotion acts on the pod being in rotation afterwards: a pod gone or
	// replaced is an error, and a pod without the gate is written True.
	promotion releaseRule = iota
	// holdRelease ends a hold that died with its pod and was never recorded
	// on a pod without the gate: nothing is written.
	holdRelease
)

// removeNotReadyKey removes msg under rule and reports whether it changed
// the gate: an entry removed or the status moved.
func removeNotReadyKey(ctx context.Context, c client.Client, reader client.Reader, pod *corev1.Pod, msg Message, rule releaseRule) (bool, error) {
	if pod == nil {
		return false, fmt.Errorf("RemoveNotReadyKey: nil pod")
	}
	if reader == nil {
		reader = c
	}
	changed := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		changed = false
		fresh, gate, err := freshGate(ctx, reader, pod)
		if err != nil {
			if rule == holdRelease && (apierrors.IsNotFound(err) || errors.Is(err, ErrPodIdentityChanged)) {
				return nil
			}
			return err
		}
		if gate == nil {
			if rule == holdRelease {
				return nil
			}
			if err := patchCondition(ctx, c, fresh, gateCondition(nil, nil)); err != nil {
				return err
			}
			changed = true
			return nil
		}
		removed, list, err := removeMessage(gate.Message, msg)
		if err != nil {
			return fmt.Errorf("pod %s/%s: %w", fresh.Namespace, fresh.Name, err)
		}
		if !removed && gate.Status == corev1.ConditionTrue {
			return nil
		}
		next := gateCondition(list, gate)
		if err := patchCondition(ctx, c, fresh, next); err != nil {
			return err
		}
		changed = removed || gate.Status != next.Status
		return nil
	})
	return changed, err
}

// freshGate re-reads the pod through reader as the base of a patch and
// returns the gate it carries, nil when it has none. A pod that cannot be
// read is an error that still answers apierrors.IsNotFound; a pod with
// another UID under the same name is ErrPodIdentityChanged.
func freshGate(ctx context.Context, reader client.Reader, pod *corev1.Pod) (*corev1.Pod, *corev1.PodCondition, error) {
	fresh := &corev1.Pod{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(pod), fresh); err != nil {
		return nil, nil, fmt.Errorf("re-read pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	if pod.UID != "" && fresh.UID != pod.UID {
		return nil, nil, fmt.Errorf("%w: pod %s/%s", ErrPodIdentityChanged, pod.Namespace, pod.Name)
	}
	return fresh, findCondition(fresh, ConditionType), nil
}

// gateCondition is the gate as the writer list leaves it: False with the
// list while a writer holds the pod, True with no message once none does.
// The transition time carries over from the stored gate when the status
// does not move, so a writer joining or leaving does not reset how long
// the pod has been out of rotation.
func gateCondition(list messageList, stored *corev1.PodCondition) corev1.PodCondition {
	cond := corev1.PodCondition{Type: ConditionType, Status: corev1.ConditionTrue, Reason: "Serving"}
	if len(list) > 0 {
		cond.Status = corev1.ConditionFalse
		cond.Reason = "NotReady"
		cond.Message = list.dump()
	}
	cond.LastTransitionTime = transitionTime(stored, cond.Status)
	return cond
}

// transitionTime is the time to record for a gate written with status: the
// stored gate's own time when it already had that status, else now.
func transitionTime(stored *corev1.PodCondition, status corev1.ConditionStatus) metav1.Time {
	if stored != nil && stored.Status == status {
		return stored.LastTransitionTime
	}
	return metav1.Now()
}

// ContainsNotReadyKey reports whether msg holds the pod out of rotation
// now: the gate is not True and its list carries msg. An entry left on a
// gate that reads True is not a hold, and AddNotReadyKey rewrites it; so a
// writer about to add may skip on true, and a writer about to remove may
// not skip on false, since that entry is what it removes.
func ContainsNotReadyKey(pod *corev1.Pod, msg Message) bool {
	return slices.Contains(holdingWriters(pod), msg)
}

// IsServing reports whether the gate is written True: no writer holds the
// pod out of rotation.
func IsServing(pod *corev1.Pod) bool {
	gate := findCondition(pod, ConditionType)
	return gate != nil && gate.Status == corev1.ConditionTrue
}

// HeldNotServing reports whether a writer holds the pod out of rotation:
// the gate exists and is not True. A pod without the gate is not held, so a
// reader of PodReady can honour a hold the kubelet has not folded in yet.
func HeldNotServing(pod *corev1.Pod) bool {
	gate := findCondition(pod, ConditionType)
	return gate != nil && gate.Status != corev1.ConditionTrue
}

// HeldNotServingBy reports whether the named writer is among those holding
// the pod out of rotation.
func HeldNotServingBy(pod *corev1.Pod, userAgent string) bool {
	return slices.ContainsFunc(holdingWriters(pod), func(m Message) bool { return m.UserAgent == userAgent })
}

// holdingWriters lists the writers holding the pod out of rotation: the
// gate's list while the gate is not True. It is empty for a pod without the
// gate, a gate that reads True, and a list that cannot be parsed.
func holdingWriters(pod *corev1.Pod) messageList {
	if !HeldNotServing(pod) {
		return nil
	}
	list, err := parseList(findCondition(pod, ConditionType).Message)
	if err != nil {
		return nil
	}
	return list
}

// ReadyAndServing reports whether the pod serves: PodReady and the gate are
// both True. The gate is read on its own because a drain moves it before
// the kubelet folds it into PodReady; PodReady is read on its own because
// the node lifecycle controller revokes it on a pod whose kubelet stopped
// reporting, leaving the container statuses and the gate at their last
// values. Until both agree the pod is in no Service's endpoints.
func ReadyAndServing(pod *corev1.Pod) bool {
	return IsPodReady(pod) && IsServing(pod)
}

// IsContainersReady reports whether the kubelet's ContainersReady condition
// is True: every container passes its readiness probe. It is the signal a
// promotion waits for; PodReady cannot be, since the kubelet keeps PodReady
// False until the gate is True.
func IsContainersReady(pod *corev1.Pod) bool {
	cond := findCondition(pod, corev1.ContainersReady)
	return cond != nil && cond.Status == corev1.ConditionTrue
}

// IsPodReady reports whether the kubelet's PodReady condition is True:
// the containers are ready and every readiness gate, this package's
// included, is satisfied, so the pod is in its Service's rotation.
func IsPodReady(pod *corev1.Pod) bool {
	cond := findCondition(pod, corev1.PodReady)
	return cond != nil && cond.Status == corev1.ConditionTrue
}

// IsPodAvailable reports whether the pod is Available: PodReady is True and
// its lastTransitionTime plus minReadySeconds is not after now (the
// Deployment minReadySeconds rule). A window of zero or less makes
// Available the same as Ready. The duration is how much longer the pod must
// stay Ready before it is Available: 0 when it already is, when it is not
// Ready, and when its Ready condition carries no lastTransitionTime (its age
// cannot be proven, so no later instant makes it Available). The status
// aggregator folds the smallest pending duration into its requeue; the
// update strategies discard it, since an open update polls already.
func IsPodAvailable(pod *corev1.Pod, minReadySeconds int32, now time.Time) (bool, time.Duration) {
	ready := findCondition(pod, corev1.PodReady)
	if ready == nil || ready.Status != corev1.ConditionTrue {
		return false, 0
	}
	if minReadySeconds <= 0 {
		return true, 0
	}
	if ready.LastTransitionTime.IsZero() {
		return false, 0
	}
	availableAt := ready.LastTransitionTime.Add(time.Duration(minReadySeconds) * time.Second)
	if !now.Before(availableAt) {
		return true, 0
	}
	return false, availableAt.Sub(now)
}

// MarkPodServing is RemoveNotReadyKey for the {userAgent, key} writer:
// success means the pod is in rotation.
func MarkPodServing(ctx context.Context, c client.Client, reader client.Reader, pod *corev1.Pod, userAgent, key string) error {
	return RemoveNotReadyKey(ctx, c, reader, pod, Message{UserAgent: userAgent, Key: key})
}

// MarkPodServingWithChange is MarkPodServing plus whether this call changed
// the gate, for a caller that compensates only the transition it made.
func MarkPodServingWithChange(ctx context.Context, c client.Client, reader client.Reader, pod *corev1.Pod, userAgent, key string) (bool, error) {
	return removeNotReadyKey(ctx, c, reader, pod, Message{UserAgent: userAgent, Key: key}, promotion)
}

// MarkPodNotServing is AddNotReadyKey for the {userAgent, key} writer: the
// pod leaves its Service's rotation through the kubelet's PodReady gate.
func MarkPodNotServing(ctx context.Context, c client.Client, reader client.Reader, pod *corev1.Pod, userAgent, key string) error {
	return AddNotReadyKey(ctx, c, reader, pod, Message{UserAgent: userAgent, Key: key})
}

// patchCondition writes cond as the pod's gate with a strategic merge keyed
// on the condition type, so the kubelet's conditions are kept. The patch is
// built by hand so that an empty message is sent and clears the stored
// list; the typed condition omits an empty message and the merge would
// keep the last list. The patch names pod's resourceVersion, so a base that
// is no longer current is a conflict and the caller re-reads.
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

// findCondition returns the pod's condition of the given type, nil when the
// pod is nil or carries none.
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

// addMessage adds msg to the list parsed from base and reports whether the
// list grew; msg already listed leaves it as it is.
func addMessage(base string, msg Message) (bool, messageList, error) {
	list, err := parseList(base)
	if err != nil {
		return false, nil, err
	}
	if slices.Contains(list, msg) {
		return false, list, nil
	}
	return true, append(list, msg), nil
}

// removeMessage drops msg from the list parsed from base and reports
// whether it was there.
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

// parseList decodes a writer list from its JSON form. An empty string is
// the empty list. A malformed string is an error, never the empty list: read
// as "no writer holds the gate", it would let a release write the gate True
// and release every other writer's hold.
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

// messageList is the writers holding a gate. Its serialized form is sorted
// by userAgent, then key, so the stored message is the same across passes
// when nothing changed.
type messageList []Message

// dump serializes the list in its sorted order; an empty list is "".
func (l messageList) dump() string {
	if len(l) == 0 {
		return ""
	}
	slices.SortFunc(l, func(a, b Message) int {
		return cmp.Or(strings.Compare(a.UserAgent, b.UserAgent), strings.Compare(a.Key, b.Key))
	})
	raw, _ := json.Marshal(l)
	return string(raw)
}

// NodeNotReadyReason is the reason the node lifecycle controller writes on
// a pod's Ready condition when it revokes it: the pod's node stopped
// reporting, so the kubelet-owned container statuses and the serving gate
// are stale and the pod is in no Service's endpoints.
const NodeNotReadyReason = "NodeNotReady"

// ReadyRevokedByNode reports whether the node lifecycle controller revoked
// the pod's Ready condition: Ready is False under NodeNotReadyReason. The
// kubelet is not reporting, so ContainersReady and the serving gate keep
// their last values and prove nothing about the pod; a Ready condition the
// kubelet has merely not folded the gate into yet carries no such reason.
func ReadyRevokedByNode(pod *corev1.Pod) bool {
	cond := findCondition(pod, corev1.PodReady)
	return cond != nil && cond.Status == corev1.ConditionFalse && cond.Reason == NodeNotReadyReason
}
