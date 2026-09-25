// Package query holds the read-side primitives shared between the
// workload pipeline (workload/status/aggregate.go, workload/ops/...)
// and the per-Component dispatch surfaces still on the ISVC controller
// (omenative/watches.go). Lives as a leaf so the workload package can
// import it without closing a cycle.
package query

import (
	"context"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// OMENativePodIndexField is the controller-runtime cache field-index
// name keyed on the (isvc, component) tuple every OMENative pod carries.
// Without it, a label-selector List scans every cached pod (~6k on a
// large cluster) per call; with it MatchingFields resolves an Instance's
// pods through the index in O(matched). Registered on the manager cache
// in cmd/manager (see RegisterOMENativePodIndex); the live API reader and
// the index-less fake client both fall back to the label selector below,
// which returns the identical set.
//
// The field name is an internal index identifier (a code constant like
// the label keys above), not a behavioral/user-facing value — no config
// surface is required.
const OMENativePodIndexField = "ome.io/omenative-pod"

// OMENativePodIndexValue builds the index value for one Instance's pods.
// Only pods OMENative manages (LabelManagedBy == ManagedByOMENative) are
// indexed; the extractor below returns nil for everything else so the
// index set matches the label selector exactly.
func OMENativePodIndexValue(isvcName string, component workload.ComponentType) string {
	return isvcName + "/" + string(component)
}

// OMENativePodIndexExtractor is the cache IndexerFunc registered for the
// OMENativePodIndexField. It mirrors the label selector in
// ListOMENativePodsByName: a pod is indexed only when it carries the
// managed-by label, and its index value is the (isvc, component) tuple.
func OMENativePodIndexExtractor(obj client.Object) []string {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	if pod.Labels[LabelManagedBy] != ManagedByOMENative {
		return nil
	}
	isvcName := pod.Labels[constants.InferenceServicePodLabelKey]
	component := pod.Labels[constants.OMEComponentLabel]
	if isvcName == "" || component == "" {
		return nil
	}
	return []string{OMENativePodIndexValue(isvcName, workload.ComponentType(component))}
}

// indexUnavailable reports whether a MatchingFields List failed because
// the OMENativePodIndexField isn't backed by an index for the given
// reader — true for the live API reader (the apiserver has no custom Pod
// field selector) and for an index-less cache/fake client. Callers fall
// back to the label-selector path, which returns the identical set.
func indexUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// Cache: "Index with name field:... does not exist" / "no index ...".
	// API server: rejects the unsupported field selector ("field label
	// not supported" / "Unable to find ... field selector").
	return strings.Contains(msg, "index") ||
		strings.Contains(msg, "field label not supported") ||
		strings.Contains(msg, "field selector")
}

// ListOMENativePodsByName returns every pod the OMENative selector matches
// for (namespace, isvcName, component). Takes a Reader so callers can use
// either the cached client or the live reader (destructive paths need live
// — see LiveListPodsForInstance below).
//
// useIndex selects the read strategy and MUST match the reader's
// capability:
//
//   - useIndex=true  — cached client (mgr.GetClient()) that has the
//     OMENativePodIndexField registered. Resolves the Instance's pods
//     through the field index in O(matched) instead of scanning every
//     cached pod per call. Pass true ONLY for the cached client.
//   - useIndex=false — live API reader (APIReader) or any reader without
//     the field index. The apiserver has no custom Pod field selector, so
//     a MatchingFields probe ALWAYS fails there and would force a second,
//     fallback List on every call. Skipping straight to the label selector
//     does ONE List. Pass false for every live-reader / index-less caller.
//
// Both modes return the identical pod set (the index extractor mirrors the
// label selector below) — useIndex only picks how the set is fetched.
//
// Adapters with a typed owner handle (e.g. ISVC, IR) compose the namespace
// + name + component arguments from their own source-of-truth shape; this
// helper stays owner-agnostic so the workload pipeline can call it from
// either adapter path.
func ListOMENativePodsByName(ctx context.Context, c client.Reader, namespace, isvcName string, component workload.ComponentType, useIndex bool) ([]*corev1.Pod, error) {
	pods := &corev1.PodList{}
	if useIndex {
		// Fast path: cache field index keyed on (isvc, component). Avoids
		// scanning every cached pod per call on large clusters. Falls back
		// to the label selector only if the index is somehow missing (e.g.
		// an index-less fake client in tests) — both return the identical
		// set.
		err := c.List(ctx, pods, client.InNamespace(namespace),
			client.MatchingFields{OMENativePodIndexField: OMENativePodIndexValue(isvcName, component)})
		if err == nil {
			return collectPods(pods), nil
		}
		if !indexUnavailable(err) {
			return nil, err
		}
		pods = &corev1.PodList{}
	}
	// Label-selector path: the only path for live / index-less readers
	// (skips the doomed MatchingFields probe), and the fallback when an
	// index-backed reader unexpectedly lacks the index.
	sel := labels.SelectorFromSet(labels.Set{
		constants.InferenceServicePodLabelKey: isvcName,
		constants.OMEComponentLabel:           string(component),
		LabelManagedBy:                        ManagedByOMENative,
	})
	if err := c.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return nil, err
	}
	return collectPods(pods), nil
}

// collectPods flattens a PodList into a slice of pointers into its backing
// array.
func collectPods(pods *corev1.PodList) []*corev1.Pod {
	out := make([]*corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		out = append(out, &pods.Items[i])
	}
	return out
}

// RegisterOMENativePodIndex installs the OMENativePodIndexField field
// index on the supplied indexer (mgr.GetFieldIndexer()). Call once during
// manager setup, before Start, so ListOMENativePodsByName's cache fast
// path resolves an Instance's pods through the index instead of scanning
// every cached pod. Idempotent re-registration is the caller's concern;
// controller-runtime errors on a duplicate field name.
func RegisterOMENativePodIndex(ctx context.Context, indexer client.FieldIndexer) error {
	return indexer.IndexField(ctx, &corev1.Pod{}, OMENativePodIndexField, OMENativePodIndexExtractor)
}

// LiveListPodsForInstance returns the pods matching the (component,
// idx) tuple via the supplied live API reader. Use this from any
// destructive code path — Delete, the zero-old-pods confirmation
// between recreate/restart Phase A and Phase B, Migrate's
// source-drain and source-delete steps — so the controller doesn't
// act on a stale cache view that says "pods gone" while the API
// server still holds them.
//
// Cache-driven ListOMENativePodsByName is fine for plan computation and
// status aggregation (stale reads are acceptable for non-destructive
// operations), but every step that mutates pod state needs a live read
// first.
//
// Takes primitive args (reader + namespace + isvcName + component) so
// the workload/query leaf doesn't take on a backward import to either
// workload.Deps / workload.Key or to a Component-spec carrier type —
// keeps the leaf importable by both adapter paths without closing a
// cycle. Adapters compose these from their own Deps + key.
func LiveListPodsForInstance(ctx context.Context, reader client.Reader, namespace, isvcName string, component workload.ComponentType, idx int32) ([]*corev1.Pod, error) {
	// Live API reader has no Pod field index — skip the doomed MatchingFields
	// probe (useIndex=false) and go straight to the label selector.
	pods, err := ListOMENativePodsByName(ctx, reader, namespace, isvcName, component, false)
	if err != nil {
		return nil, err
	}
	out := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if i, ok := InstanceIdxFromLabels(pod); ok && i == idx {
			out = append(out, pod)
		}
	}
	return out, nil
}

// LiveListPodsForComponent returns every pod the OMENative selector
// matches for (namespace, isvcName, component) via the supplied live
// reader — the per-Instance lister's selector minus the index filter.
// This is the teardown completion check: teardown is complete only when
// NO component pod remains, so a live pod whose InstanceStatus entry
// was lost (a statusless orphan) must still block completion — an
// InstanceStatus-driven check would miss exactly that pod.
//
// Same primitive-arg shape and live-reader rule as
// LiveListPodsForInstance (see comments there).
func LiveListPodsForComponent(ctx context.Context, reader client.Reader, namespace, isvcName string, component workload.ComponentType) ([]*corev1.Pod, error) {
	return ListOMENativePodsByName(ctx, reader, namespace, isvcName, component, false)
}

// BucketPodsByInstanceIdx groups pods by their ome.io/instance-index
// label. Pods missing the label are dropped — they were never created
// by OMENative.
func BucketPodsByInstanceIdx(pods []*corev1.Pod) map[int32][]*corev1.Pod {
	by := make(map[int32][]*corev1.Pod)
	for _, pod := range pods {
		idx, ok := InstanceIdxFromLabels(pod)
		if !ok {
			continue
		}
		by[idx] = append(by[idx], pod)
	}
	return by
}

// IndexPodsByName returns a name -> *Pod lookup. Used by Create's
// desired-vs-existing diff and Recreate's Phase-B race-on-create check.
func IndexPodsByName(pods []*corev1.Pod) map[string]*corev1.Pod {
	m := make(map[string]*corev1.Pod, len(pods))
	for _, pod := range pods {
		m[pod.Name] = pod
	}
	return m
}

// InstanceIdxFromLabels returns (0, false) when the ome.io/instance-index
// label is absent or malformed.
func InstanceIdxFromLabels(pod *corev1.Pod) (int32, bool) {
	if pod == nil {
		return 0, false
	}
	raw, ok := pod.Labels[LabelInstanceIdx]
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, false
	}
	return int32(n), true
}

// InstanceIncarnationFromLabels returns (0, false) when the
// ome.io/instance-incarnation label is absent or malformed.
func InstanceIncarnationFromLabels(pod *corev1.Pod) (int64, bool) {
	if pod == nil {
		return 0, false
	}
	raw, ok := pod.Labels[LabelInstanceIncarnation]
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// PodOrdinalFromLabels returns the pod-naming ordinal stamped via
// LabelPodOrdinal. Pre-feature pods lack the label and fall back to 0
// — matches their actual ordinal suffix in the pod name. Returns
// (0, false) only on malformed values so callers can distinguish
// "legacy pod, ordinal 0" from a parse error.
func PodOrdinalFromLabels(pod *corev1.Pod) (int32, bool) {
	if pod == nil {
		return 0, false
	}
	raw, ok := pod.Labels[LabelPodOrdinal]
	if !ok {
		return 0, true
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, false
	}
	return int32(n), true
}

// PartitionPodsByIncarnation splits pods by the ome.io/instance-incarnation
// label:
//   - old: value < current — safe to delete (prior materialization).
//   - fresh: value >= current — the new set, kept even if strictly above
//     (deleting future-labeled pods could lose work).
//   - unknown: label missing/malformed — orphan candidates. Callers refuse
//     to delete and emit FoundOrphan; an operator must re-classify the
//     pod before Restart can proceed. Without this guard, a stripped
//     label or third-party reuse of managed-by would silently tear down
//     foreign pods.
func PartitionPodsByIncarnation(pods []*corev1.Pod, current int64) (old, fresh, unknown []*corev1.Pod) {
	for _, pod := range pods {
		inc, ok := InstanceIncarnationFromLabels(pod)
		if !ok {
			unknown = append(unknown, pod)
			continue
		}
		if inc < current {
			old = append(old, pod)
			continue
		}
		fresh = append(fresh, pod)
	}
	return
}

// IsTerminalPod reports whether the pod has reached a terminal phase
// (Failed or Succeeded). The kubelet never runs another container in a
// terminal pod, so the object is dead weight that still occupies its
// stable name: every presence decision treats it as absent, and the
// owning operation must delete it before the name can be reused.
func IsTerminalPod(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	return pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded
}

const (
	// ReasonUnexpectedAdmissionError is the kubelet's pod-level reason
	// when it refuses a pod the scheduler already placed on its node for
	// any cause other than a specific resource shortfall.
	ReasonUnexpectedAdmissionError = "UnexpectedAdmissionError"
	// outOfResourceReasonPrefix is how the kubelet names a refusal for a
	// specific resource shortfall: "OutOf" concatenated with the resource
	// ("OutOfcpu", "OutOfmemory", "OutOfpods", "OutOfnvidia.com/gpu").
	// Matched by prefix because extended-resource names are
	// cluster-defined and cannot be enumerated.
	outOfResourceReasonPrefix = "OutOf"
)

// PodAdmissionRejected reports whether the kubelet refused to admit the
// pod after the scheduler had already placed it, and returns the reason
// it recorded.
//
// ENVIRONMENT-CAUSED: the node's own picture of its capacity disagreed
// with what the scheduler assumed, which says nothing about the pod
// template. No container ever ran, yet the object still holds its stable
// name, so the owning operation must delete it before that name can be
// used again.
func PodAdmissionRejected(pod *corev1.Pod) (string, bool) {
	if pod == nil || pod.Status.Phase != corev1.PodFailed {
		return "", false
	}
	reason := pod.Status.Reason
	if reason == ReasonUnexpectedAdmissionError || isOutOfResourceReason(reason) {
		return reason, true
	}
	return "", false
}

// isOutOfResourceReason matches the kubelet's OutOf<resource> family
// without swallowing every other reason that opens with the same
// letters. The suffix has to look like the resource name the kubelet
// concatenated: non-empty, starting with a lowercase letter, and built
// only from characters a (possibly domain-qualified) resource name may
// contain, as in nvidia.com/gpu.
func isOutOfResourceReason(reason string) bool {
	resource, ok := strings.CutPrefix(reason, outOfResourceReasonPrefix)
	if !ok || resource == "" {
		return false
	}
	if resource[0] < 'a' || resource[0] > 'z' {
		return false
	}
	for _, r := range resource {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
		case r == '-', r == '.', r == '/', r == '_':
		default:
			return false
		}
	}
	return true
}

// ExcludeTerminalPods returns the pods that are not terminal — the set a
// presence or liveness decision may count. Nil entries are dropped too.
func ExcludeTerminalPods(pods []*corev1.Pod) []*corev1.Pod {
	out := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if pod == nil || IsTerminalPod(pod) {
			continue
		}
		out = append(out, pod)
	}
	return out
}

// AllPodsRuntimeReady reports whether every pod has the ContainersReady
// PodCondition. Empty input returns false (nothing is Ready). A terminal
// pod is never runtime-ready whatever conditions it still carries: the
// presence pass ahead of every readiness gate treats it as a missing
// target, so answering true here would promote an incomplete set.
func AllPodsRuntimeReady(pods []*corev1.Pod) bool {
	if len(pods) == 0 {
		return false
	}
	for _, pod := range pods {
		if IsTerminalPod(pod) || !podreadiness.IsContainersReady(pod) {
			return false
		}
	}
	return true
}

// PodSetPromotable reports whether a pod set clears the one bar every path
// stamping an Instance Ready shares: the set is non-empty, no pod is
// terminal, every pod is PodReady — the readiness gates the controller
// writes are folded by kubelet into that condition, so a PodReady pod is
// eligible for its Service — and every pod has held Ready for the
// Component's minReadySeconds window. A window <= 0 reduces the bar to
// PodReady. Each extra predicate is an additional per-path requirement on
// every pod (the in-place roll confirms its runtime images there).
//
// The duration is how long until the last pod inside its window clears it,
// so a caller waiting on the window wakes when it elapses instead of
// polling; it is 0 when the set is promotable and when something other than
// the window holds it back. A pod that goes ContainersReady=false and back
// inside the window moves the Ready condition's lastTransitionTime, so the
// window restarts from that latest transition and the set is not promotable
// until the flapping pod has held Ready for a full window.
func PodSetPromotable(pods []*corev1.Pod, minReadySeconds int32, now time.Time, extra ...func(*corev1.Pod) bool) (bool, time.Duration) {
	if len(pods) == 0 {
		return false, 0
	}
	var wait time.Duration
	for _, pod := range pods {
		if IsTerminalPod(pod) || !podreadiness.IsContainersReady(pod) {
			return false, 0
		}
		for _, satisfied := range extra {
			if !satisfied(pod) {
				return false, 0
			}
		}
		available, remaining := podreadiness.IsPodAvailable(pod, minReadySeconds, now)
		if available {
			continue
		}
		if remaining <= 0 {
			// Not PodReady, or Ready with no provable age: no later instant
			// is known to promote this set.
			return false, 0
		}
		if remaining > wait {
			wait = remaining
		}
	}
	return wait == 0, wait
}

// PodSetFullyServing reports whether the pod set is fully healthy in
// the rotation: at least `desired` live (non-deleting) pods, every one
// of them ContainersReady AND carrying the serving gate. Deleting pods
// are excluded rather than disqualifying — a completed surge leaves the
// old pod draining next to the serving replacement. A terminal pod is
// never live and disqualifies the set outright: it is failure evidence
// that must reach the escalation pass even when stale conditions on it
// still read healthy, or when a serving sibling would otherwise cover
// the count. desired <= 0 never counts as serving (nothing is expected,
// so nothing can prove health).
func PodSetFullyServing(pods []*corev1.Pod, desired int32) bool {
	if desired <= 0 {
		return false
	}
	var live int32
	for _, p := range pods {
		if p == nil || p.DeletionTimestamp != nil {
			continue
		}
		if IsTerminalPod(p) || !podreadiness.IsContainersReady(p) || !podreadiness.IsServing(p) {
			return false
		}
		live++
	}
	return live >= desired
}

// AllTerminating reports whether every pod in the slice carries a
// DeletionTimestamp. Used by recreate / restart Phase B to allow
// moving on to Create even when the API server hasn't fully GC'd
// the previous incarnation's pods yet — terminating-but-not-yet-gone
// is a safe state because the previous Delete call already issued
// the foreground propagation.
//
// Empty slice returns true (nothing to terminate, trivially done).
func AllTerminating(pods []*corev1.Pod) bool {
	for _, pod := range pods {
		if pod.DeletionTimestamp == nil {
			return false
		}
	}
	return true
}

// LiveOldPodsClearedForRecreate live-reads pods for the Instance,
// partitions by incarnation against currentInc, and reports whether
// every OLD pod (incarnation < currentInc OR missing the label) is
// either gone or terminating. New-incarnation pods (those Phase B
// may have already created on a prior pass) are NOT inspected here —
// the Phase B caller does its own desired-vs-existing diff against
// them.
//
// The boundary check between Phase A (drain + delete) and Phase B
// (create) in recreate / restart ops. Without it, the cache may show
// stale old pods that Phase A already deleted (blocking Phase B
// indefinitely), or the cache may show zero pods while the API
// server still holds them (letting Phase B issue a stable-name
// Create that AlreadyExists-conflicts against the still-terminating
// previous incarnation). Tolerating terminating-but-not-yet-gone
// matches K8s foreground propagation semantics.
//
// Same primitive-arg shape as LiveListPodsForInstance (see comment
// there for the rationale).
func LiveOldPodsClearedForRecreate(ctx context.Context, reader client.Reader, namespace, isvcName string, component workload.ComponentType, idx int32, currentInc int64) (bool, error) {
	pods, err := LiveListPodsForInstance(ctx, reader, namespace, isvcName, component, idx)
	if err != nil {
		return false, err
	}
	// Orphans don't block recreate — they aren't the prior incarnation.
	// Restart's caller routes them through the FoundOrphan event path.
	oldPods, _, _ := PartitionPodsByIncarnation(pods, currentInc)
	return AllTerminating(oldPods), nil
}
