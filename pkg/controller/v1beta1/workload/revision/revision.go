// Package revision owns the ControllerRevision history for a
// per-workload pod template — deterministic hashing, collision-aware
// create, retention.
//
// Boundary: this package is a leaf. It depends on Kubernetes API
// machinery and the workload package's own type set; it does not
// import v1beta1 — adapters convert from CRD-typed status into
// workload.InstanceStatus before calling CollectLiveRevisionNames. The labels stamped on emitted CRs
// are taken from the caller-supplied revision.Key so the same labels
// carry through the pod-selector composition the rest of the workload
// pipeline does.
//
// Imports the audit and query subpackages solely to share constants:
// audit's MigrationRequestAnnotationPrefix (the controller-written
// lifecycle annotation that must NOT participate in the revision hash)
// and query's LabelPairingProtocol (the annotation key that surfaces a
// revision's pairing protocol on the emitted ControllerRevision).
package revision

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Key is the small subset of workload.Key this package needs. Defining
// it locally (rather than importing the workload top-level) keeps this
// leaf importable by both the workload top-level package and by
// adapters that hold their own composition of these fields. Adapters
// typically build it from the workload.Key they already have in scope.
type Key struct {
	Namespace string
	Name      string
	Labels    map[string]string
}

// DataPayload is the canonical content hashed into the
// ControllerRevision name. Only steady-state fields are hashed;
// transient operation overlays (operation-id, drain annotations,
// migration anti-affinity, readiness-gate status) are deliberately
// excluded so an in-flight operation cannot manufacture a fresh
// revision just by stamping a temporary annotation on a pod.
//
// PodSpec carries the leader / single-pod template.
//
// PodMeta carries the pod-template-level labels and annotations the
// user can edit independently of PodSpec. The field is part of the JSON
// shape from day one (no omitempty) so adding non-nil values later
// doesn't invalidate every previously-recorded revision name.
//
// WorkerPodSpec carries the multi-pod Component's worker template (set
// only when the Component declares both Leader and Worker). It uses
// `omitempty` so single-pod revisions — which have never carried a
// worker — keep their pre-multi-node JSON shape and hash. Without
// omitempty, every single-pod ISVC would see a phantom rollout on the
// first reconcile after the multi-pod field landed.
type DataPayload struct {
	PodSpec       *corev1.PodSpec    `json:"podSpec"`
	PodMeta       *metav1.ObjectMeta `json:"podMeta"`
	WorkerPodSpec *corev1.PodSpec    `json:"workerPodSpec,omitempty"`
	// TopologyKey records explicit topology intent for multi-pod revisions.
	// It is omitted when topology is unset so existing topology-free revisions
	// retain their canonical payload and hash.
	TopologyKey *string `json:"topologyKey,omitempty"`
	// PairingProtocol records the P/D wire-compatibility token the revision
	// was minted under, so a protocol change mints a new revision. Unlike
	// TopologyKey it applies to single-pod Components too (a PD engine is
	// often single-pod). Omitted when unset so protocol-free revisions retain
	// their canonical payload and hash.
	PairingProtocol *string `json:"pairingProtocol,omitempty"`
}

// dataPayloadJSON is the wire shape DataPayload marshals to. It exists so the
// canonical bytes belong to this package rather than to whichever apimachinery
// version the binary is compiled against: metav1.ObjectMeta's JSON shape is
// not stable across Kubernetes releases, and any shift in it renames every
// live ControllerRevision and rolls the fleet with no other signal.
type dataPayloadJSON struct {
	PodSpec         *corev1.PodSpec   `json:"podSpec"`
	PodMeta         *templateMetaJSON `json:"podMeta"`
	WorkerPodSpec   *corev1.PodSpec   `json:"workerPodSpec,omitempty"`
	TopologyKey     *string           `json:"topologyKey,omitempty"`
	PairingProtocol *string           `json:"pairingProtocol,omitempty"`
}

// templateMetaJSON is the pod-template metadata exactly as it is hashed: an
// explicit null creationTimestamp ahead of labels and annotations. Only labels
// and annotations carry meaning (see TemplateMeta); the null timestamp is
// inert padding that the canonical bytes carry so revision names stay stable
// against apimachinery's own serialization of an empty timestamp.
type templateMetaJSON struct {
	CreationTimestamp *metav1.Time      `json:"creationTimestamp"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
}

// MarshalJSON renders the pinned canonical shape. Unmarshalling stays on the
// default path: templateMetaJSON's field names are a subset of ObjectMeta's,
// so stored payloads decode straight back into PodMeta.
func (p DataPayload) MarshalJSON() ([]byte, error) {
	out := dataPayloadJSON{
		PodSpec:         p.PodSpec,
		WorkerPodSpec:   p.WorkerPodSpec,
		TopologyKey:     p.TopologyKey,
		PairingProtocol: p.PairingProtocol,
	}
	if p.PodMeta != nil {
		out.PodMeta = &templateMetaJSON{
			Labels:      p.PodMeta.Labels,
			Annotations: p.PodMeta.Annotations,
		}
	}
	return json.Marshal(out)
}

// Hash returns the deterministic hash that becomes the
// ControllerRevision name suffix, plus the canonical serialized bytes
// stored in CR.Data.Raw. collisionCount is folded into the hash so a
// retry after a same-name collision produces a different name.
//
// Single-pod variant — does not include a worker template. Multi-pod
// Components must use HashWithWorker so worker-only spec changes trigger
// a new revision. See HashWithWorker for the multi-pod path; this
// function is the single-pod / no-worker shorthand.
//
// nil collisionCount is normalized to 0 before folding — the status
// field is *int32 and callers may legitimately pass either nil or
// ptr.To(int32(0)) for "no salt yet"; both must produce the same hash
// or a defaulter that normalizes the pointer would flip every CR hash
// on the next reconcile.
//
// templateMeta carries user-intent labels and annotations from the
// rendered Component metadata so template-meta changes trigger a new
// revision and a rollout. nil hashes the same as empty ObjectMeta —
// both serialize to the same `podMeta` JSON shape.
//
// ownerUID partitions the hash space per-owner so a deleted-and-recreated
// owner of the same name (new UID) does NOT compute the same CR name as
// the deleted predecessor. Without this partition, cascade-GC racing with
// recreation can leave the new owner pointing at a freshly-created
// same-name CR (or, if GC is slower, silently flag a foreign-owner
// collision) — both shapes drop the OMENative-on-ISVC contract that "new
// owner ⇒ new revision history" on the floor. Empty UID hashes
// identically to a zero-value UID; callers that legitimately have no
// owner identity yet (test fixtures predating the fix) can pass "".
func Hash(template *corev1.PodSpec, templateMeta *metav1.ObjectMeta, collisionCount *int32, ownerUID types.UID) (string, []byte, error) {
	return HashWithWorker(template, nil, templateMeta, collisionCount, ownerUID)
}

// HashWithWorker is Hash with an explicit worker PodSpec slot. Use for
// multi-pod Components (Leader + Worker); pass workerSpec=nil for
// single-pod / no-worker Components, in which case the canonical bytes
// and hash are identical to Hash(template, templateMeta, collisionCount, ownerUID).
//
// The worker template participates in the revision hash so a worker-only
// image / env / resource bump triggers a new revision and a rollout. The
// alternative — rendering the worker payload at create time without
// hashing it — leaves worker-only spec changes silently un-rolled,
// which violates the revision pipeline's contract: every template
// change must produce a new revision and trigger a rollout.
//
// ownerUID is folded into the hash so two owners with the same name +
// spec — produced by delete-and-recreate cycles — emit distinct CR names.
// See Hash for the rationale.
func HashWithWorker(template, workerSpec *corev1.PodSpec, templateMeta *metav1.ObjectMeta, collisionCount *int32, ownerUID types.UID) (string, []byte, error) {
	return HashWithWorkerAndTopology(template, workerSpec, templateMeta, "", collisionCount, ownerUID)
}

// HashWithWorkerAndTopology extends HashWithWorker with the topology key used
// to render immutable worker affinity and the matching PodGroup contract.
// Multi-pod revisions record non-empty topology intent. An empty topology key
// preserves the legacy topology-free payload and hash; single-pod hashes remain
// unchanged regardless of the supplied key.
func HashWithWorkerAndTopology(template, workerSpec *corev1.PodSpec, templateMeta *metav1.ObjectMeta, topologyKey string, collisionCount *int32, ownerUID types.UID) (string, []byte, error) {
	return HashWithWorkerTopologyAndPairing(template, workerSpec, templateMeta, topologyKey, "", collisionCount, ownerUID)
}

// HashWithWorkerTopologyAndPairing extends HashWithWorkerAndTopology with the
// P/D pairing protocol, so a protocol change mints a new revision for the
// Component. Unlike topology, pairing applies to single-pod Components too. An
// empty protocol preserves the protocol-free payload and hash.
func HashWithWorkerTopologyAndPairing(template, workerSpec *corev1.PodSpec, templateMeta *metav1.ObjectMeta, topologyKey, pairingProtocol string, collisionCount *int32, ownerUID types.UID) (string, []byte, error) {
	if template == nil {
		return "", nil, fmt.Errorf("nil pod template")
	}
	// Topology is meaningful only for a leader+worker workload. The public API
	// documents it as ignored for single-pod Components, so it must not produce
	// a phantom revision when no worker template exists.
	var topologyKeyPayload *string
	if workerSpec != nil && topologyKey != "" {
		topologyKeyPayload = &topologyKey
	}
	var pairingPayload *string
	if pairingProtocol != "" {
		pairingPayload = &pairingProtocol
	}
	raw, err := json.Marshal(DataPayload{
		PodSpec:         template,
		PodMeta:         TemplateMeta(templateMeta),
		WorkerPodSpec:   workerSpec,
		TopologyKey:     topologyKeyPayload,
		PairingProtocol: pairingPayload,
	})
	if err != nil {
		return "", nil, fmt.Errorf("marshal revision payload: %w", err)
	}
	h := fnv.New32a()
	_, _ = h.Write(raw)
	cc := int32(0)
	if collisionCount != nil {
		cc = *collisionCount
	}
	// Delimit the CollisionCount so it can't concatenate ambiguously with
	// the trailing bytes of `raw` — raw ending in "1" plus cc=0 must not
	// hash the same as raw ending in "10" plus a different cc.
	_, _ = fmt.Fprintf(h, "|cc=%d|", cc)
	// Delimit the owner UID the same way. An owner with empty UID hashes
	// identically to one whose UID happens to render as the empty
	// string — both serialize to "|uid=|" — so test fixtures that don't
	// carry a UID stay backward-compatible with the legacy hash provided
	// they consistently pass an empty UID. Real reconciles always supply
	// the live owner UID via EnsureControllerRevision.
	_, _ = fmt.Fprintf(h, "|uid=%s|", string(ownerUID))
	return fmt.Sprintf("%08x", h.Sum32()), raw, nil
}

// TemplateMeta strips ObjectMeta to user-intent fields (labels +
// annotations) only. Operational fields (UID, ResourceVersion, etc.)
// are excluded so the hash stays stable when only those change. Returns
// nil when neither labels nor annotations are set — preserves the
// legacy `"podMeta":null` shape so workloads that don't yet carry
// template metadata don't see a phantom rollout when this code first
// deploys.
//
// Filters out lifecycle annotations that the controller writes back to
// the owner (migration requests) so adding/removing them does not bump
// the revision hash. Without this, a migration request landing on the
// owner would drift every Component's revision hash — including
// Components that aren't the migration target — and force a phantom
// rollout that envtest cannot complete.
//
// Not the only annotation exclusion, and the smaller one. The
// InferenceReplica reconciler first drops every key named by
// ome.io/revision-excluded-annotation-keys — the irprojector stamps it
// with every ISVC annotation NOT declared under
// spec.<component>.annotations. So ISVC-level annotations never reach
// this function; only component-declared ones do. Predicting an
// annotation-driven rollout means reading both. Labels have no dynamic
// exclusion: for them the static list below is the whole story.
func TemplateMeta(src *metav1.ObjectMeta) *metav1.ObjectMeta {
	if src == nil || (len(src.Labels) == 0 && len(src.Annotations) == 0) {
		return nil
	}
	filtered := src.Annotations
	if hasDerivedAnnotation(src.Annotations) {
		filtered = make(map[string]string, len(src.Annotations))
		for k, v := range src.Annotations {
			if isDerivedAnnotation(k) {
				continue
			}
			filtered[k] = v
		}
	}
	labels := src.Labels
	if hasDerivedLabel(src.Labels) {
		labels = make(map[string]string, len(src.Labels))
		for k, v := range src.Labels {
			if isDerivedLabel(k) {
				continue
			}
			labels[k] = v
		}
	}
	if len(labels) == 0 && len(filtered) == 0 {
		return nil
	}
	return &metav1.ObjectMeta{
		Labels:      labels,
		Annotations: filtered,
	}
}

// Labels naming the queue a pod is admitted against, and the priority it is
// admitted at. Duplicated as literals to keep this package a leaf, like the
// rollout verbs above.
const (
	kueueQueueNameLabel     = "kueue.x-k8s.io/queue-name"
	kueuePriorityClassLabel = "kueue.x-k8s.io/priority-class"
)

// isSchedulingLabel reports whether the label is one the admission plane owns
// rather than the user, and therefore must not feed the pod-template revision
// hash — the label mirror of isLifecycleAnnotation.
//
// A queue assignment is derived from cluster state, not from the spec: it moves
// when quota is re-authored or when the operator turns queue stamping on or off.
// Hashing it makes each of those a full rollout of every workload, which is a
// steep price for a value the running pods cannot act on anyway. Kueue reads
// these at admission, so a pod already admitted keeps its queue whatever the
// label later says, and the new value takes effect when the pod is next created.
//
// The consequence to know: re-pointing a workload at another queue does not
// recreate its pods on its own. It takes effect on the next rollout.
func isSchedulingLabel(key string) bool {
	switch key {
	case kueueQueueNameLabel, kueuePriorityClassLabel:
		return true
	}
	return false
}

// isPlacementMarker reports whether the label is one the multi-cluster control
// plane stamps on a derived workload to record its provenance: which source
// InferenceService it was derived from, and which control plane derived it.
//
// It is identity bookkeeping, not workload configuration — the values are a UID
// and a control-plane name, neither of which a running pod can act on. Hashing
// it would make taking an already-running workload under control-plane
// management a full rollout of that workload, which is precisely the downtime a
// live migration exists to avoid. The markers are also written by the placer
// rather than by whoever authored the spec, so they are not user intent.
func isPlacementMarker(key string) bool {
	switch key {
	case constants.PlacementOrigin, constants.PlacementControlPlane:
		return true
	}
	return false
}

// isDerivedLabel reports whether a label is written by a controller from
// cluster state rather than authored by the user, and so must not feed the
// pod-template revision hash. Two families qualify: the admission plane's queue
// assignment and the control plane's placement provenance.
func isDerivedLabel(key string) bool {
	return isSchedulingLabel(key) || isPlacementMarker(key)
}

// hasDerivedLabel reports whether any key is controller-written. Fast path so
// the common case skips the copy in TemplateMeta.
func hasDerivedLabel(labels map[string]string) bool {
	for k := range labels {
		if isDerivedLabel(k) {
			return true
		}
	}
	return false
}

// isSchedulingAnnotation reports whether the annotation selects where the
// admission plane places the workload. The annotation mirror of
// isSchedulingLabel: this one names the queue, that one carries it.
func isSchedulingAnnotation(key string) bool {
	return key == constants.LocalQueue
}

// isDerivedAnnotation reports whether an annotation is controller-owned rather
// than authored intent, and so must not feed the pod-template revision hash.
func isDerivedAnnotation(key string) bool {
	return isLifecycleAnnotation(key) || isSchedulingAnnotation(key)
}

// hasDerivedAnnotation reports whether any key is controller-owned. Fast path
// so the common case skips the copy in TemplateMeta.
func hasDerivedAnnotation(annotations map[string]string) bool {
	for k := range annotations {
		if isDerivedAnnotation(k) {
			return true
		}
	}
	return false
}

// pausedRolloutAnnotation is the operator-set ISVC annotation that
// freezes coordination groups at their current phase
// (constants.PausedRolloutAnnotation). Duplicated here as a literal —
// like the canary verbs below — to keep this package a leaf.
const pausedRolloutAnnotation = "ome.io/rollout-paused"

// rolloutPromoteAnnotation and rolloutRollbackAnnotation are the canary
// operator verbs (constants.RolloutPromoteAnnotation /
// RolloutRollbackAnnotation). Like the pause signal they are written to
// and consumed off the ISVC mid-rollout; duplicated here as literals to
// keep this package a leaf.
const (
	rolloutPromoteAnnotation  = "ome.io/rollout-promote"
	rolloutRollbackAnnotation = "ome.io/rollout-rollback"
)

// isLifecycleAnnotation reports whether the annotation key is one the
// controller reads or writes as part of normal reconcile (migration
// requests, rollout-paused signal, the canary promote/rollback verbs)
// and therefore must not feed into the pod-template revision hash.
// Without this filter, adding or removing the annotation would flip the
// ControllerRevision name and drive a phantom rollout on a spec the user
// never edited — e.g. an operator's `rollout-promote` would mint a new
// revision and roll to it instead of the one being promoted.
// Object-scoped annotations are filtered by the InferenceReplica reconciler before this call.
func isLifecycleAnnotation(key string) bool {
	if strings.HasPrefix(key, audit.MigrationRequestAnnotationPrefix) {
		return true
	}
	switch key {
	case pausedRolloutAnnotation, rolloutPromoteAnnotation, rolloutRollbackAnnotation:
		return true
	}
	return false
}

// revisionAnnotations derives the annotations stamped on a freshly-created
// ControllerRevision from its canonical payload bytes. The pairing protocol is
// surfaced as an annotation so readers on hot paths (traffic building, the
// pair-floor gate) can resolve revision→protocol from object metadata without
// decoding Data.Raw. Runs only at CR create, so the decode cost is per-mint,
// not per-reconcile. Returns nil when the payload carries no annotation-worthy
// fields, keeping protocol-free CRs byte-identical to their legacy shape.
func revisionAnnotations(raw []byte) map[string]string {
	var p DataPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil
	}
	if p.PairingProtocol == nil || *p.PairingProtocol == "" {
		return nil
	}
	return map[string]string{query.LabelPairingProtocol: *p.PairingProtocol}
}

// PairingProtocolFromRevision returns the P/D pairing protocol a
// ControllerRevision was minted under: the ome.io/pairing-protocol annotation
// stamped at create, falling back to decoding the stored payload for CRs
// created without the annotation. "" means the revision pairs with anything —
// which is definitionally correct for every pre-pairing revision, so the
// fallback swallows decode errors rather than failing a read that could only
// ever concern a revision minted without a protocol.
func PairingProtocolFromRevision(cr *appsv1.ControllerRevision) string {
	if cr == nil {
		return ""
	}
	if v, ok := cr.Annotations[query.LabelPairingProtocol]; ok {
		return v
	}
	p, err := PayloadFromControllerRevision(cr)
	if err != nil || p == nil || p.PairingProtocol == nil {
		return ""
	}
	return *p.PairingProtocol
}

// PayloadFromControllerRevision unmarshals a ControllerRevision's Data into the
// DataPayload (pod template + meta + worker template). Returns (nil, nil) when
// the CR or its data is empty. Used by callers that need to render from a stored
// revision (e.g. a canary rollback rolling Instances back to the stable CR).
func PayloadFromControllerRevision(cr *appsv1.ControllerRevision) (*DataPayload, error) {
	if cr == nil || len(cr.Data.Raw) == 0 {
		return nil, nil
	}
	var p DataPayload
	if err := json.Unmarshal(cr.Data.Raw, &p); err != nil {
		return nil, fmt.Errorf("unmarshal ControllerRevision %s data: %w", cr.Name, err)
	}
	return &p, nil
}

// Name composes the ControllerRevision object name from the Key and
// the hash — `<key.Name>-<hash>`. Deterministic, so "reuse-if-exists"
// works.
//
// The ISVC adapter sets key.Name = "<isvc>-<component>", producing
// `<isvc>-<component>-<hash>`. IR-adapter callers set key.Name the same
// way (since one IR is one (ISVC, Component) tuple), so cross-adapter
// CRs share the same name and the in-place migration window stays
// seamless.
func Name(key Key, hash string) string {
	return fmt.Sprintf("%s-%s", key.Name, hash)
}

// Labels are stamped on every ControllerRevision so the list selector
// picks them up. Taken directly from the Key's seed label set, which
// the adapter populated with the {ome.io/inferenceservice, component,
// managed-by} trio so the existing pod selector also picks up the CRs.
func Labels(key Key) map[string]string {
	out := make(map[string]string, len(key.Labels))
	for k, v := range key.Labels {
		out[k] = v
	}
	return out
}

// EnsureControllerRevision creates or reuses a CR matching the
// workload's pod template. `reads` must be the live API reader: a
// stale cache could return NotFound for an object the apiserver
// already holds, causing re-Create over a concurrent writer.
//
// collision=true signals a hash collision (same name, different Data,
// or foreign ownership). Caller bumps CollisionCount and retries with a
// different salt so the next name lands.
//
// New CRs are stamped with Labels() and a controller OwnerReference
// for the workload owner; existing CRs keep whatever labels/owners they
// already have.
//
// scopeUID partitions the revision history per parent identity so a
// deleted-and-recreated parent of the same name does not silently
// inherit the predecessor's CR. For the ISVC adapter this is the ISVC
// UID; for the IR adapter this is the ISVC UID resolved from the IR's
// controller OwnerReference so BOTH paths address the same per-ISVC
// revision space (the IR's own UID is intentionally NOT used — that
// would diverge the IR-managed path's hash from the direct path's hash
// and break the shadow-mode equivalence the cutover relies on).
//
// Single-pod variant — does not hash a worker template. Multi-pod
// callers use EnsureControllerRevisionWithWorker so worker-only spec
// changes trigger a new CR.
func EnsureControllerRevision(
	ctx context.Context,
	c client.Client,
	reads client.Reader,
	owner client.Object,
	ownerGVK schema.GroupVersionKind,
	key Key,
	template *corev1.PodSpec,
	templateMeta *metav1.ObjectMeta,
	collisionCount *int32,
	scopeUID types.UID,
) (cr *appsv1.ControllerRevision, collision bool, err error) {
	return EnsureControllerRevisionWithWorker(ctx, c, reads, owner, ownerGVK, key, template, nil, templateMeta, collisionCount, scopeUID)
}

// EnsureControllerRevisionWithWorker is the multi-pod-aware sibling of
// EnsureControllerRevision: it folds workerSpec into the revision hash
// so a worker-only spec change produces a new CR and triggers a rollout.
// Pass workerSpec=nil to get behavior identical to
// EnsureControllerRevision (same name, same Data.Raw).
//
// scopeUID semantics — see EnsureControllerRevision.
func EnsureControllerRevisionWithWorker(
	ctx context.Context,
	c client.Client,
	reads client.Reader,
	owner client.Object,
	ownerGVK schema.GroupVersionKind,
	key Key,
	template *corev1.PodSpec,
	workerSpec *corev1.PodSpec,
	templateMeta *metav1.ObjectMeta,
	collisionCount *int32,
	scopeUID types.UID,
) (cr *appsv1.ControllerRevision, collision bool, err error) {
	if owner == nil {
		return nil, false, fmt.Errorf("EnsureControllerRevision: nil owner")
	}
	if c == nil {
		return nil, false, fmt.Errorf("EnsureControllerRevision: nil client")
	}
	if reads == nil {
		return nil, false, fmt.Errorf("EnsureControllerRevision: nil reader")
	}

	hash, raw, err := HashWithWorker(template, workerSpec, templateMeta, collisionCount, scopeUID)
	if err != nil {
		return nil, false, err
	}
	return EnsureControllerRevisionFromHash(ctx, c, reads, owner, ownerGVK, key, hash, raw)
}

// EnsureControllerRevisionFromHash is the create-or-reuse half of
// EnsureControllerRevisionWithWorker with the (hash, raw) precomputed by
// the caller. It exists so a caller that can cheaply detect an unchanged
// pod template (e.g. an unchanged generation + collisionCount) can skip
// the per-reconcile json.Marshal + FNV in HashWithWorker and still reuse
// the exact same create / reuse / collision logic. hash + raw MUST be the
// byte-identical output of HashWithWorker for the same inputs — the
// deterministic-name Get and the bytes.Equal collision check both depend
// on it.
//
// All ownership / collision / race-on-Create semantics are identical to
// EnsureControllerRevisionWithWorker; see that function for the rationale.
func EnsureControllerRevisionFromHash(
	ctx context.Context,
	c client.Client,
	reads client.Reader,
	owner client.Object,
	ownerGVK schema.GroupVersionKind,
	key Key,
	hash string,
	raw []byte,
) (cr *appsv1.ControllerRevision, collision bool, err error) {
	if owner == nil {
		return nil, false, fmt.Errorf("EnsureControllerRevision: nil owner")
	}
	if c == nil {
		return nil, false, fmt.Errorf("EnsureControllerRevision: nil client")
	}
	if reads == nil {
		return nil, false, fmt.Errorf("EnsureControllerRevision: nil reader")
	}

	name := Name(key, hash)
	objKey := client.ObjectKey{Namespace: key.Namespace, Name: name}

	existing := &appsv1.ControllerRevision{}
	if getErr := reads.Get(ctx, objKey, existing); getErr == nil {
		// Foreign ownership = collision: a CR left over from a
		// previously-deleted-and-recreated owner of the same name would
		// otherwise be silently adopted, mixing histories.
		if !controllerOwnedBy(existing.OwnerReferences, owner.GetUID()) {
			return existing, true, nil
		}
		if !bytes.Equal(existing.Data.Raw, raw) {
			return existing, true, nil
		}
		return existing, false, nil
	} else if !apierrors.IsNotFound(getErr) {
		return nil, false, fmt.Errorf("get ControllerRevision %s: %w", name, getErr)
	}

	nextRev, err := nextRevisionNumber(ctx, reads, key)
	if err != nil {
		return nil, false, err
	}

	cr = &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       key.Namespace,
			Labels:          Labels(key),
			Annotations:     revisionAnnotations(raw),
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(owner, ownerGVK)},
		},
		Data:     runtime.RawExtension{Raw: raw},
		Revision: nextRev,
	}
	if cerr := c.Create(ctx, cr); cerr != nil {
		if !apierrors.IsAlreadyExists(cerr) {
			return nil, false, fmt.Errorf("create ControllerRevision %s: %w", name, cerr)
		}
		// Lost the race; re-read via live reader (cache may still show
		// NotFound) and apply the same ownership / match-or-collision
		// gating as the existing-on-Get branch.
		if getErr := reads.Get(ctx, objKey, cr); getErr != nil {
			return nil, false, fmt.Errorf("re-get after AlreadyExists: %w", getErr)
		}
		if !controllerOwnedBy(cr.OwnerReferences, owner.GetUID()) {
			return cr, true, nil
		}
		if !bytes.Equal(cr.Data.Raw, raw) {
			return cr, true, nil
		}
	}
	return cr, false, nil
}

// controllerOwnedBy reports whether refs contains a Controller=true
// OwnerReference pointing at uid. Only a true controller ref asserts
// the parent claim we care about.
func controllerOwnedBy(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.Controller != nil && *ref.Controller && ref.UID == uid {
			return true
		}
	}
	return false
}

// RetainControllerRevisions sweeps the workload-scoped ControllerRevisions
// and keeps the most recent maxNonLive non-live ones, in addition to any
// revision named in liveNames. Older non-live revisions are deleted. Order
// is by .Revision (highest = newest).
//
// reads is the live API reader used for the listing step so retention
// decisions aren't made against a stale cache.
//
// liveNames typically holds CurrentRevision and UpdateRevision plus
// every InstanceStatus.RunningRevision/TargetRevision. Empty entries
// are ignored. The recommended default for maxNonLive is 10.
func RetainControllerRevisions(
	ctx context.Context,
	c client.Client,
	reads client.Reader,
	key Key,
	maxNonLive int,
	liveNames ...string,
) error {
	if maxNonLive < 0 {
		return fmt.Errorf("RetainControllerRevisions: maxNonLive must be >= 0")
	}
	existing, err := listControllerRevisions(ctx, reads, key)
	if err != nil {
		return err
	}

	live := make(map[string]struct{}, len(liveNames))
	for _, n := range liveNames {
		if n != "" {
			live[n] = struct{}{}
		}
	}

	nonLive := make([]appsv1.ControllerRevision, 0, len(existing))
	for _, cr := range existing {
		if _, ok := live[cr.Name]; ok {
			continue
		}
		nonLive = append(nonLive, cr)
	}
	if len(nonLive) <= maxNonLive {
		return nil
	}

	// .Revision is monotonic by construction (nextRevisionNumber returns
	// max+1) so it suffices as the sole sort key. No CreationTimestamp
	// tie-breaker is needed unless we later relax the monotonicity
	// guarantee — e.g., allow CollisionCount resets that could reuse a
	// revision number, which the spec currently does not permit.
	sort.Slice(nonLive, func(i, j int) bool {
		return nonLive[i].Revision > nonLive[j].Revision
	})

	// Don't bail on the first delete failure: a transient API error on
	// the oldest CR would otherwise leave every newer non-live CR behind
	// and starve retention until the failing one is hand-deleted. Collect
	// errors and continue.
	var sweepErr error
	for i := maxNonLive; i < len(nonLive); i++ {
		cr := nonLive[i]
		if derr := c.Delete(ctx, &cr); derr != nil && !apierrors.IsNotFound(derr) {
			sweepErr = errors.Join(sweepErr, fmt.Errorf("delete ControllerRevision %s: %w", cr.Name, derr))
		}
	}
	return sweepErr
}

// listControllerRevisions returns every ControllerRevision matching the
// workload's seed label set.
func listControllerRevisions(
	ctx context.Context,
	reads client.Reader,
	key Key,
) ([]appsv1.ControllerRevision, error) {
	list := &appsv1.ControllerRevisionList{}
	sel := labels.SelectorFromSet(labels.Set(Labels(key)))
	if err := reads.List(ctx, list,
		client.InNamespace(key.Namespace),
		client.MatchingLabelsSelector{Selector: sel},
	); err != nil {
		return nil, fmt.Errorf("list ControllerRevisions: %w", err)
	}
	return list.Items, nil
}

// nextRevisionNumber returns max(.Revision)+1 across the workload's
// existing CRs, or 1 when none exist. Reads via the supplied reader so
// the caller can force a live read when the monotonicity guarantee
// matters (two concurrent reconciles reading from a stale cache could
// otherwise both compute the same max+1).
func nextRevisionNumber(
	ctx context.Context,
	reads client.Reader,
	key Key,
) (int64, error) {
	existing, err := listControllerRevisions(ctx, reads, key)
	if err != nil {
		return 0, err
	}
	var maxRev int64
	for _, cr := range existing {
		if cr.Revision > maxRev {
			maxRev = cr.Revision
		}
	}
	return maxRev + 1, nil
}

// CollectLiveRevisionNames is the union retention must protect:
// CurrentRevision + UpdateRevision plus every
// InstanceStatus.RunningRevision/TargetRevision. Without the per-instance
// union, an in-flight migration whose source RunningRevision points to
// an older CR could have that CR swept mid-flight, then the migration
// resume path returns nil and the migration bails forever.
//
// Takes the slice of InstanceStatuses + the two revision pointers
// directly so the call works against either an LifecycleStatus
// (ISVC path) or an InferenceReplicaStatus (IR path) without needing a
// type-specific reader. Either adapter projects its status surface into
// these primitive args.
func CollectLiveRevisionNames(current, update string, instances []workload.InstanceStatus) []string {
	seen := map[string]struct{}{}
	add := func(name string) {
		if name == "" {
			return
		}
		seen[name] = struct{}{}
	}
	add(current)
	add(update)
	for _, inst := range instances {
		add(inst.RunningRevision)
		add(inst.TargetRevision)
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	return out
}
