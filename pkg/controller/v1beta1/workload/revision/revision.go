// Package revision owns the ControllerRevision history for a
// per-workload pod template — deterministic hashing, collision-aware
// create, retention.
//
// Boundary: this package is a leaf. It depends on Kubernetes API
// machinery and the workload package's own type set; it does not
// import v1beta1 — an adapter hands LiveRevisionNames an iterator over
// its own row type and converts nothing. The labels stamped on emitted CRs
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
	"iter"
	"sort"
	"strconv"
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
	// WorkerSize records the worker pods per Instance beside a worker template,
	// so a width change mints a new revision and every reader renders the gang
	// at the width its revision names. Absent on single-pod revisions.
	WorkerSize *int32 `json:"workerSize,omitempty"`
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
	WorkerSize      *int32            `json:"workerSize,omitempty"`
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
		WorkerSize:      p.WorkerSize,
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
// owner ⇒ new revision history" on the floor. An empty ownerUID is
// accepted and leaves the hash unpartitioned, which suits only a caller
// with no owner identity; a caller that persists revision history passes
// its owner's UID.
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
// empty protocol preserves the protocol-free payload and hash. No width is
// recorded; a multi-pod caller passes its worker count through HashTemplate.
func HashWithWorkerTopologyAndPairing(template, workerSpec *corev1.PodSpec, templateMeta *metav1.ObjectMeta, topologyKey, pairingProtocol string, collisionCount *int32, ownerUID types.UID) (string, []byte, error) {
	return HashTemplate(Template{
		PodSpec:         template,
		WorkerPodSpec:   workerSpec,
		Meta:            templateMeta,
		TopologyKey:     topologyKey,
		PairingProtocol: pairingProtocol,
	}, collisionCount, ownerUID)
}

// Template is everything a revision hashes: the pod templates, the
// template metadata the user authored, and the gang shape they render in.
type Template struct {
	// PodSpec is the leader or single-pod template. Required.
	PodSpec *corev1.PodSpec
	// WorkerPodSpec is the worker template; nil for a single-pod Component.
	WorkerPodSpec *corev1.PodSpec
	// Meta is the pod-template labels and annotations; derived keys are
	// dropped (TemplateMeta).
	Meta *metav1.ObjectMeta
	// TopologyKey is the gang co-location key; ignored without a worker
	// template.
	TopologyKey string
	// PairingProtocol is the P/D wire-compatibility token; empty pairs with
	// anything.
	PairingProtocol string
	// WorkerSize is the worker pods per Instance; recorded only beside a
	// worker template and only when positive.
	WorkerSize int32
}

// HashTemplate returns the deterministic hash that becomes the
// ControllerRevision name suffix, plus the canonical bytes stored in
// CR.Data.Raw. Every field a revision owns enters here, so a change to any
// of them is a new revision; collisionCount and ownerUID salt the name as
// Hash describes.
func HashTemplate(t Template, collisionCount *int32, ownerUID types.UID) (string, []byte, error) {
	if t.PodSpec == nil {
		return "", nil, fmt.Errorf("nil pod template")
	}
	// Topology and width describe a leader+worker gang. The public API
	// documents topology as ignored for single-pod Components, and a
	// Component without a worker template has no width, so neither may
	// produce a phantom revision when no worker template exists.
	var topologyKeyPayload *string
	if t.WorkerPodSpec != nil && t.TopologyKey != "" {
		key := t.TopologyKey
		topologyKeyPayload = &key
	}
	var workerSizePayload *int32
	if t.WorkerPodSpec != nil && t.WorkerSize > 0 {
		size := t.WorkerSize
		workerSizePayload = &size
	}
	var pairingPayload *string
	if t.PairingProtocol != "" {
		protocol := t.PairingProtocol
		pairingPayload = &protocol
	}
	raw, err := json.Marshal(DataPayload{
		PodSpec:         t.PodSpec,
		PodMeta:         TemplateMeta(t.Meta),
		WorkerPodSpec:   t.WorkerPodSpec,
		TopologyKey:     topologyKeyPayload,
		PairingProtocol: pairingPayload,
		WorkerSize:      workerSizePayload,
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
	return key == constants.PlacementExecution || isLifecycleAnnotation(key) || isSchedulingAnnotation(key)
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

// rolloutPromoteAnnotation, rolloutPromoteForceAnnotation and
// rolloutRollbackAnnotation are the canary operator verbs
// (constants.RolloutPromoteAnnotation / RolloutPromoteForceAnnotation /
// RolloutRollbackAnnotation). Like the pause signal they are written to
// and consumed off the ISVC mid-rollout; duplicated here as literals to
// keep this package a leaf.
const (
	rolloutPromoteAnnotation      = "ome.io/rollout-promote"
	rolloutPromoteForceAnnotation = "ome.io/rollout-promote-force"
	rolloutRollbackAnnotation     = "ome.io/rollout-rollback"
)

// isLifecycleAnnotation reports whether the annotation key is one the
// controller reads or writes as part of normal reconcile (migration
// requests, rollout-paused signal, the canary promote/rollback verbs,
// the InferenceReplica release-held and reset-instances verbs) and
// therefore must not feed into the pod-template revision hash. Without
// this filter, adding or removing the annotation would flip the
// ControllerRevision name and drive a phantom rollout on a spec the user
// never edited — e.g. an operator's `rollout-promote` would mint a new
// revision and roll to it instead of the one being promoted, and a
// `reset-instances` meant to rebuild two Failed Instances would roll
// every Instance.
// Object-scoped annotations are filtered by the InferenceReplica reconciler before this call.
func isLifecycleAnnotation(key string) bool {
	if strings.HasPrefix(key, audit.MigrationRequestAnnotationPrefix) {
		return true
	}
	switch key {
	case pausedRolloutAnnotation, rolloutPromoteAnnotation, rolloutPromoteForceAnnotation, rolloutRollbackAnnotation,
		constants.ReleaseHeldRevisionAnnotationKey, constants.ResetInstancesAnnotationKey:
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
	out := map[string]string{}
	if p.PairingProtocol != nil && *p.PairingProtocol != "" {
		out[query.LabelPairingProtocol] = *p.PairingProtocol
	}
	if p.WorkerSize != nil && *p.WorkerSize > 0 {
		out[query.AnnotationWorkerSize] = strconv.FormatInt(int64(*p.WorkerSize), 10)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// WorkerSizeFromRevision returns the worker pods per Instance a
// ControllerRevision was minted at: the ome.io/worker-size annotation
// stamped at create, else the stored payload. ok is false when the
// revision records no width — a single-pod revision, or a multi-pod revision
// whose payload carries no worker count — and the caller renders the
// desired width.
func WorkerSizeFromRevision(cr *appsv1.ControllerRevision) (size int32, ok bool) {
	if cr == nil {
		return 0, false
	}
	if v, found := cr.Annotations[query.AnnotationWorkerSize]; found {
		n, err := strconv.ParseInt(v, 10, 32)
		if err == nil && n > 0 {
			return int32(n), true
		}
	}
	p, err := PayloadFromControllerRevision(cr)
	if err != nil || p == nil || p.WorkerSize == nil || *p.WorkerSize <= 0 {
		return 0, false
	}
	return *p.WorkerSize, true
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
// The InferenceReplica reconciler sets key.Name = "<prefix>-<component>"
// (the InferenceService name for a projected replica, the replica's own
// name for a standalone one), producing `<prefix>-<component>-<hash>`.
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
// scopeUID feeds the hash and therefore the revision name: the
// InferenceService's UID for a projected replica, the replica's own UID
// for a standalone one, so two pod sets never share a revision name even
// when their templates match. Listing, numbering and retention select by
// the key's labels.
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
	return EnsureControllerRevisionFromHash(ctx, c, reads, owner, ownerGVK, key, hash, raw, nil)
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
//
// scope is the object the owner's revision history belongs to beyond the
// owner itself: the InferenceService that projects a replica (ScopeOwner).
// A new revision names it as a plain owner, so the history stays when the
// replica is deleted and leaves with the service; an existing revision the
// scope owns is reused whichever replica minted it (RetainedBy), and one
// the owner controls without the scope gains it. nil scopes the history to
// the owner alone.
func EnsureControllerRevisionFromHash(
	ctx context.Context,
	c client.Client,
	reads client.Reader,
	owner client.Object,
	ownerGVK schema.GroupVersionKind,
	key Key,
	hash string,
	raw []byte,
	scope *metav1.OwnerReference,
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

	if confirmed := confirmedFromCache(ctx, c, objKey, owner, scope, raw); confirmed != nil {
		return confirmed, false, nil
	}
	existing := &appsv1.ControllerRevision{}
	if getErr := reads.Get(ctx, objKey, existing); getErr == nil {
		// Foreign ownership = collision: a CR left over from a
		// previously-deleted-and-recreated owner of the same name would
		// otherwise be silently adopted, mixing histories. A revision the
		// scope owns is this history, whichever replica of the service
		// minted it.
		if !RetainedBy(existing, owner.GetUID(), scope) {
			return existing, true, nil
		}
		if !bytes.Equal(existing.Data.Raw, raw) {
			return existing, true, nil
		}
		if err := EnsureScopeOwner(ctx, c, existing, scope); err != nil {
			return nil, false, err
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
			OwnerReferences: historyOwners(owner, ownerGVK, scope),
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
		if !RetainedBy(cr, owner.GetUID(), scope) {
			return cr, true, nil
		}
		if !bytes.Equal(cr.Data.Raw, raw) {
			return cr, true, nil
		}
	}
	return cr, false, nil
}

// confirmedFromCache reads the revision as the cache holds it and returns
// that copy when the pass would confirm it without a write: the owner's or
// its scope's, recording the same payload, and already naming the scope
// owner. Nil for any other answer, a missing copy included: a collision, a
// scope owner to add and a revision to mint are decided on the live object.
// A revision deleted or re-owned after the copy was taken raises a revision
// event on its owner, whose next pass reads the live object.
func confirmedFromCache(ctx context.Context, cache client.Reader, key client.ObjectKey, owner client.Object, scope *metav1.OwnerReference, raw []byte) *appsv1.ControllerRevision {
	cr := &appsv1.ControllerRevision{}
	if err := cache.Get(ctx, key, cr); err != nil {
		return nil
	}
	if !RetainedBy(cr, owner.GetUID(), scope) || !bytes.Equal(cr.Data.Raw, raw) {
		return nil
	}
	if scope != nil && scope.UID != "" && !ownedBy(cr.OwnerReferences, scope.UID) {
		return nil
	}
	return cr
}

// RetainControllerRevisions keeps the revisions liveNames protect and the
// newest maxNonLive others, and deletes the rest. c is the manager's client
// (reads from the cache, writes to the apiserver); reads is the live reader.
// The cached history decides only that there is nothing to delete; a cached
// history over the limit sends the sweep to the live list, which decides
// every deletion; a revision in use is protected by name on both lists.
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
	live := liveNameSet(liveNames)
	if cached, err := listControllerRevisions(ctx, c, key); err == nil && len(overTheLimit(cached, live, maxNonLive)) == 0 {
		return nil
	}
	existing, err := listControllerRevisions(ctx, reads, key)
	if err != nil {
		return err
	}

	// Don't bail on the first delete failure: a transient API error on
	// the oldest CR would otherwise leave every newer non-live CR behind
	// and starve retention until the failing one is hand-deleted. Collect
	// errors and continue.
	var sweepErr error
	for _, cr := range overTheLimit(existing, live, maxNonLive) {
		if derr := c.Delete(ctx, &cr); derr != nil && !apierrors.IsNotFound(derr) {
			sweepErr = errors.Join(sweepErr, fmt.Errorf("delete ControllerRevision %s: %w", cr.Name, derr))
		}
	}
	return sweepErr
}

// liveNameSet is the set of revision names the sweep protects; an empty
// name is ignored.
func liveNameSet(names []string) map[string]struct{} {
	live := make(map[string]struct{}, len(names))
	for _, n := range names {
		if n != "" {
			live[n] = struct{}{}
		}
	}
	return live
}

// overTheLimit is what the sweep deletes from one listing of the history:
// the revisions no live name protects, past the maxNonLive newest of them,
// oldest last. Nil when the history is within the limit.
func overTheLimit(history []appsv1.ControllerRevision, live map[string]struct{}, maxNonLive int) []appsv1.ControllerRevision {
	nonLive := make([]appsv1.ControllerRevision, 0, len(history))
	for _, cr := range history {
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
	return nonLive[maxNonLive:]
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

// LiveRevisionNames is the union retention must protect: the current and
// update revisions and the two names every row carries, the revision the
// Instance runs and the one it moves toward, each once, empty names left
// out. A row's revision is kept whatever the history's limit: a move reads
// its source's pod template from the revision the row names, and a swept
// one would leave it no template. rows yields each row's two names, so a
// caller walks its own row type without copying it into another.
func LiveRevisionNames(current, update string, rows iter.Seq2[string, string]) []string {
	seen := map[string]struct{}{}
	addLiveName(seen, current)
	addLiveName(seen, update)
	for running, target := range rows {
		addLiveName(seen, running)
		addLiveName(seen, target)
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

// addLiveName records a named revision; an empty name is no revision.
func addLiveName(seen map[string]struct{}, name string) {
	if name != "" {
		seen[name] = struct{}{}
	}
}

// RowRevisions yields, per engine row, the revision the Instance runs and
// the one it moves toward.
func RowRevisions(instances []workload.InstanceStatus) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for i := range instances {
			if !yield(instances[i].RunningRevision, instances[i].TargetRevision) {
				return
			}
		}
	}
}

// ReferencedRevisionNames returns, sorted, every revision name the rows
// record a pod set under: the revision each runs, converges to, or its
// attempt pinned. except is left out; the roll target renders the desired
// width and needs no lookup.
func ReferencedRevisionNames(statuses []workload.InstanceStatus, except string) []string {
	seen := map[string]struct{}{}
	add := func(name string) {
		if name != "" && name != except {
			seen[name] = struct{}{}
		}
	}
	for i := range statuses {
		add(statuses[i].RunningRevision)
		add(statuses[i].TargetRevision)
		if statuses[i].Operation != nil {
			add(statuses[i].Operation.TargetRevision)
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// WorkerSizesByRevision reads the worker pods per Instance each named
// ControllerRevision was minted at. A revision that is gone, or that
// records no width, is left out, so the Instances on it render the desired
// width.
func WorkerSizesByRevision(ctx context.Context, reads client.Reader, namespace string, names []string) (map[string]int32, error) {
	if reads == nil || len(names) == 0 {
		return nil, nil
	}
	out := map[string]int32{}
	for _, name := range names {
		cr := &appsv1.ControllerRevision{}
		if err := reads.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cr); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("read revision %s/%s: %w", namespace, name, err)
		}
		if size, ok := WorkerSizeFromRevision(cr); ok {
			out[name] = size
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// HasReadyRowOn reports whether any Instance is recorded Ready on the
// revision, so a width can be observed on its gangs at all.
func HasReadyRowOn(rows []workload.InstanceStatus, revisionName string) bool {
	for i := range rows {
		if rows[i].Phase == workload.InstancePhaseReady && rows[i].RunningRevision == revisionName {
			return true
		}
	}
	return false
}

// ObservedWorkerWidth is the worker count the gangs on a revision run: the
// most worker pods any Ready Instance recorded on it holds, counted from the
// live pods that carry its hash. known is false when no Ready Instance on
// the revision holds a worker pod, so nothing is observed.
func ObservedWorkerWidth(rows []workload.InstanceStatus, pods []*corev1.Pod, revisionName string) (width int32, known bool) {
	hash := query.RevisionFromName(revisionName).Hash()
	ready := map[string]struct{}{}
	for i := range rows {
		if rows[i].Phase == workload.InstancePhaseReady && rows[i].RunningRevision == revisionName {
			ready[strconv.FormatInt(int64(rows[i].Index), 10)] = struct{}{}
		}
	}
	if len(ready) == 0 {
		return 0, false
	}
	count := map[string]int32{}
	for _, pod := range query.ExcludeTerminalPods(pods) {
		if pod.DeletionTimestamp != nil {
			continue
		}
		idx := pod.Labels[query.LabelInstanceIdx]
		if _, ok := ready[idx]; !ok || pod.Labels[query.LabelRunner] != workload.RunnerWorker || pod.Labels[query.LabelRevisionHash] != hash {
			continue
		}
		count[idx]++
	}
	for _, n := range count {
		known = true
		if n > width {
			width = n
		}
	}
	return width, known
}

// WidthFreeTwin names the revision the same templates hash to with no
// worker count recorded: the hash and canonical bytes of the template
// rendered without its width.
type WidthFreeTwin struct {
	Hash string
	Raw  []byte
}

// WidthFreeTwinOf renders t without its worker count. ok is false when t
// has no width, so no revision minted without one can stand in for it.
func WidthFreeTwinOf(t Template, collisionCount *int32, ownerUID types.UID) (twin WidthFreeTwin, ok bool, err error) {
	if t.WorkerPodSpec == nil || t.WorkerSize <= 0 {
		return WidthFreeTwin{}, false, nil
	}
	free := t
	free.WorkerSize = 0
	hash, raw, err := HashTemplate(free, collisionCount, ownerUID)
	if err != nil {
		return WidthFreeTwin{}, false, err
	}
	return WidthFreeTwin{Hash: hash, Raw: raw}, true, nil
}

// AdoptRevisionWithoutWidth returns the ControllerRevision a multi-pod
// Component keeps as its target instead of minting the hashed one: the
// revision under the twin's name, when no revision exists under the hashed
// name yet, the twin is this owner's and records exactly the width-free
// rendering of the same templates, and the gangs on it run the desired
// width (observed; nothing observed reads as the desired width). nil means
// the caller mints as usual: a width the gangs do not run mints and rolls
// them to it, and a template change mints a new name, so revisions minted
// without a width phase out on their own.
//
// recordedTarget is the update revision the owner's status names. Once it
// names the twin, the twin is confirmed from the cache alone and nothing is
// read live; the live reads run only the first time, while the status still
// names another revision. scope is the history's scope beyond the owner, as
// for EnsureControllerRevisionFromHash: a twin a previous replica of the
// same service minted is this owner's too.
func AdoptRevisionWithoutWidth(ctx context.Context, cache, live client.Reader, owner client.Object, scope *metav1.OwnerReference, key Key, hash string, twin WidthFreeTwin, workerSize int32, recordedTarget string, observed func(revisionName string) (int32, bool)) (*appsv1.ControllerRevision, error) {
	if twin.Hash == "" || workerSize <= 0 || owner == nil || cache == nil || live == nil {
		return nil, nil
	}
	name := Name(key, twin.Hash)
	twinKey := client.ObjectKey{Namespace: key.Namespace, Name: name}
	keeps := func(cr *appsv1.ControllerRevision) bool {
		if !RetainedBy(cr, owner.GetUID(), scope) || !bytes.Equal(cr.Data.Raw, twin.Raw) {
			return false
		}
		if observed != nil {
			if width, known := observed(name); known && width != workerSize {
				return false
			}
		}
		return true
	}
	if recordedTarget == name {
		cached := &appsv1.ControllerRevision{}
		err := cache.Get(ctx, twinKey, cached)
		if err == nil {
			if keeps(cached) {
				return cached, nil
			}
			return nil, nil
		}
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("get ControllerRevision %s: %w", name, err)
		}
	}
	hashedKey := client.ObjectKey{Namespace: key.Namespace, Name: Name(key, hash)}
	for _, reads := range []client.Reader{cache, live} {
		err := reads.Get(ctx, hashedKey, &appsv1.ControllerRevision{})
		if err == nil {
			return nil, nil
		}
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("get ControllerRevision %s: %w", hashedKey.Name, err)
		}
	}
	candidate := &appsv1.ControllerRevision{}
	if err := live.Get(ctx, twinKey, candidate); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get ControllerRevision %s: %w", name, err)
	}
	if !keeps(candidate) {
		return nil, nil
	}
	return candidate, nil
}
