// Package audit owns the per-owner migration ledger persisted as a
// ConfigMap. The ledger lives outside the controller process so UUID
// dedup, in-flight resume, and capacity gating survive controller
// restarts and requester retries.
//
// Boundary: this package depends only on Kubernetes API machinery plus
// the workload/types value types (for the MigrationRecord shape
// ValidateCapacity reads) and is owner-agnostic — callers pass the
// owner metav1.Object and its GroupVersionKind explicitly, so this
// package does not import the project's v1beta1 type set. The
// "ome.io/managed-by=OMENative" label it stamps on the ConfigMap
// matches the value other workload subpackages use; the owner-specific
// GVK is passed through from the adapter rather than recomputed here.
package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const (
	// MigrationRequestAnnotationPrefix is the canonical key prefix a migration
	// requester uses to write migration requests onto the InferenceService. The
	// suffix is the request UUID and is what's recorded in the audit
	// ledger for dedup.
	MigrationRequestAnnotationPrefix = "ome.io/migration-request-v1-"

	// SchemaV1 is the only schemaVersion currently understood. Requests
	// carrying any other value are rejected as UnsupportedSchemaVersion.
	SchemaV1 = "v1"

	// ConfigMapNameSuffix is appended to the owner name to form the audit
	// ConfigMap name. One CM per owner keeps blast radius small and
	// matches the parent's namespace + lifecycle automatically via
	// OwnerReference cascade.
	ConfigMapNameSuffix = "-ome-migration-audit"

	// LedgerKey is the data key inside the audit ConfigMap.
	LedgerKey = "history.json"

	// Phase* are the lifecycle phases recorded against each migration
	// request.
	PhaseStarted   = "Started"
	PhaseCompleted = "Completed"
	PhaseFailed    = "Failed"

	// labelManagedBy / managedByValue are stamped on the audit
	// ConfigMap so cluster-side selectors (and the controller's own
	// label-based queries) recognize it as OMENative-owned. Duplicated
	// from render.go's constants — same string, same intent.
	labelManagedBy = "ome.io/managed-by"
	managedByValue = "OMENative"
)

// ErrUnsupportedSchemaVersion is returned by ParseMigrationRequest when
// the request's schemaVersion isn't one we know how to handle. Callers
// should record this and reject the request rather than retrying.
var ErrUnsupportedSchemaVersion = fmt.Errorf("UnsupportedSchemaVersion")

// MigrationRequest is the JSON shape a migration requester writes into the
// ome.io/migration-request-v1-<uuid> annotation value. Fields match
// the canonical request schema; unknown additive fields are silently
// dropped by json.Unmarshal (the version-skew rule).
type MigrationRequest struct {
	SchemaVersion   string   `json:"schemaVersion"`
	Component       string   `json:"component"`
	Instance        int32    `json:"instance"`
	FromNode        string   `json:"from_node"`
	HintTargetNodes []string `json:"hint_target_nodes,omitempty"`
	Reason          string   `json:"reason,omitempty"`
	RequestedAt     string   `json:"requested_at,omitempty"`
	RequestedBy     string   `json:"requested_by,omitempty"`
}

// Entry is one row of the per-owner migration ledger. Stored in the
// audit ConfigMap; persists across controller restarts so UUID dedup
// outlives the in-memory expectations cache.
type Entry struct {
	RequestUUID    string `json:"requestUUID"`
	Component      string `json:"component"`
	SourceInstance int32  `json:"sourceInstance"`
	SurgeInstance  int32  `json:"surgeInstance,omitempty"`
	Phase          string `json:"phase"`
	Reason         string `json:"reason,omitempty"`
	FromNode       string `json:"fromNode,omitempty"`
	// HintTargetNodes carries the request's preferred placement targets
	// so a Started row imported after an upgrade resumes with the
	// requester's hints intact (unknown to older builds — additive).
	HintTargetNodes []string `json:"hintTargetNodes,omitempty"`
	StartedAt       string   `json:"startedAt"`
	CompletedAt     string   `json:"completedAt,omitempty"`
	Outcome         string   `json:"outcome,omitempty"`
}

// CompletedAtTime parses the entry's RFC3339 CompletedAt timestamp.
// ok=false when the field is unset or malformed.
func (e Entry) CompletedAtTime() (time.Time, bool) {
	return parseTime(e.CompletedAt)
}

// Ledger is the in-memory shape of the audit ConfigMap's history.json
// payload.
type Ledger struct {
	Entries []Entry `json:"entries"`

	// loadedResourceVersion pins persists to the ConfigMap state this
	// ledger was loaded from. PersistLedgerForOwner writes against it so
	// a concurrent writer (sibling IR on the shared parent CM) surfaces
	// as a 409 for the caller's reconcile/retry loop to reload and
	// re-apply, instead of being silently blob-overwritten. Empty means
	// the ConfigMap did not exist at load time.
	loadedResourceVersion string
}

// ExtractRequestUUID returns the UUID suffix of a migration-request
// annotation key, or "" if the key isn't a migration request annotation.
// Used to scan an owner's annotations for pending requests.
func ExtractRequestUUID(key string) string {
	if !strings.HasPrefix(key, MigrationRequestAnnotationPrefix) {
		return ""
	}
	return strings.TrimPrefix(key, MigrationRequestAnnotationPrefix)
}

// ParseMigrationRequest unmarshals an annotation value into a
// MigrationRequest and validates its schemaVersion. Returns
// ErrUnsupportedSchemaVersion when the schema is unknown; the
// reconciler records that as a rejected request and stops driving
// the UUID.
func ParseMigrationRequest(raw string) (*MigrationRequest, error) {
	var req MigrationRequest
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		return nil, fmt.Errorf("parse migration request: %w", err)
	}
	if req.SchemaVersion != SchemaV1 {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedSchemaVersion, req.SchemaVersion)
	}
	return &req, nil
}

// ConfigMapNameForOwner returns the deterministic audit ConfigMap name
// for the owner — one ConfigMap per owner object. Adapters compose the
// owner-typed handle into a metav1.Object; the workload package itself
// never sees the owner-CRD shape.
func ConfigMapNameForOwner(owner metav1.Object) string {
	return owner.GetName() + ConfigMapNameSuffix
}

// LoadLedgerForOwner fetches the per-owner audit ConfigMap and parses
// its history.json payload. Returns an empty ledger (and no error) when
// the ConfigMap doesn't exist yet — first migration on this owner.
// The returned ledger remembers the ConfigMap revision it was loaded
// at; PersistLedgerForOwner writes against that revision (CAS).
// Owner is held as metav1.Object so this helper is owner-agnostic; the
// ISVC adapter and the IR adapter use the same persistence path.
func LoadLedgerForOwner(ctx context.Context, reads client.Reader, owner metav1.Object) (*Ledger, error) {
	cm := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: owner.GetNamespace(), Name: ConfigMapNameForOwner(owner)}
	if err := reads.Get(ctx, key, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return &Ledger{}, nil
		}
		return nil, fmt.Errorf("get audit configmap %s: %w", key.Name, err)
	}
	raw, ok := cm.Data[LedgerKey]
	if !ok || raw == "" {
		return &Ledger{loadedResourceVersion: cm.ResourceVersion}, nil
	}
	var l Ledger
	if err := json.Unmarshal([]byte(raw), &l); err != nil {
		return nil, fmt.Errorf("unmarshal audit ledger %s: %w", key.Name, err)
	}
	l.loadedResourceVersion = cm.ResourceVersion
	return &l, nil
}

// HasCompletedOrFailedRequest reports whether the ledger already
// terminally records this UUID as a MIGRATION. Both Completed and
// Failed are terminal — a re-delivered request with the same UUID is a
// no-op regardless of outcome. ForceDelete rows share the RequestUUID
// keyspace (they key on the pod UID) but are not migrations, so they
// are excluded: a migration request reusing a force-deleted pod's UID
// must not be swallowed as already-terminal. Exclusion (rather than a
// positive reason match) because migration rows carry the requester's
// free-form Reason, often empty.
func (l *Ledger) HasCompletedOrFailedRequest(uuid string) bool {
	for _, e := range l.Entries {
		if e.RequestUUID != uuid || e.Reason == ReasonForceDelete {
			continue
		}
		if e.Phase == PhaseCompleted || e.Phase == PhaseFailed {
			return true
		}
	}
	return false
}

// InFlightEntry returns a copy of the in-progress entry for uuid, or
// nil when the ledger has no Started-but-not-yet-terminal entry for
// this UUID. Used by the migration state machine to resume across
// reconciles. A copy — not an interior pointer — because trim and
// UpsertEntry compact Entries in place, which would re-point an
// aliasing pointer at a different row; write changes back through
// UpsertEntry.
func (l *Ledger) InFlightEntry(uuid string) *Entry {
	for i := range l.Entries {
		if l.Entries[i].RequestUUID != uuid {
			continue
		}
		if l.Entries[i].Phase == PhaseStarted {
			e := l.Entries[i]
			return &e
		}
	}
	return nil
}

// InFlightEntryOrSeed returns the in-flight Started entry, synthesizing
// one if missing — covers the case where the ledger was lost between
// writes. Keeps the terminal append in Migrate's tail simple.
func (l *Ledger) InFlightEntryOrSeed(uuid string, req *MigrationRequest, surgeIdx int32) *Entry {
	if e := l.InFlightEntry(uuid); e != nil {
		return e
	}
	e := NewStartedEntry(req, uuid, surgeIdx)
	return &e
}

// maxTerminalEntries caps the number of terminal (Completed / Failed)
// entries the ledger retains. ConfigMaps are capped at 1 MiB; a terminal
// entry is ~280 bytes, so ~3700 entries fit and 200 is well clear of the
// cap while still preserving plenty of dedup history. In-flight Started
// entries are NEVER truncated — dropping them would lose the resume
// anchor for an active migration.
const maxTerminalEntries = 200

// MaxTerminalEntries exposes the trim cap for tests that need to assert
// the ring-buffer behavior. Source of truth is the unexported constant.
const MaxTerminalEntries = maxTerminalEntries

// UpsertEntry adds the entry if its UUID is new, or replaces the
// existing same-UUID entry in place. Last-writer-wins on Phase / Outcome
// for the lifetime of one migration. After mutation the ledger is
// ring-buffer-trimmed: every Started entry is kept, and only the most
// recent maxTerminalEntries terminal entries survive.
func (l *Ledger) UpsertEntry(entry Entry) {
	for i := range l.Entries {
		if l.Entries[i].RequestUUID == entry.RequestUUID {
			l.Entries[i] = entry
			l.trim()
			return
		}
	}
	l.Entries = append(l.Entries, entry)
	l.trim()
}

// trim drops the oldest terminal entries when the count exceeds the
// configured cap. Started entries are protected — losing one would
// strand an in-flight migration that crashed before its destructive
// step recorded a terminal Completed/Failed row.
func (l *Ledger) trim() {
	var terminal []int
	for i, e := range l.Entries {
		if e.Phase == PhaseCompleted || e.Phase == PhaseFailed {
			terminal = append(terminal, i)
		}
	}
	overflow := len(terminal) - maxTerminalEntries
	if overflow <= 0 {
		return
	}
	drop := make(map[int]struct{}, overflow)
	for _, i := range terminal[:overflow] {
		drop[i] = struct{}{}
	}
	kept := l.Entries[:0]
	for i, e := range l.Entries {
		if _, gone := drop[i]; gone {
			continue
		}
		kept = append(kept, e)
	}
	l.Entries = kept
}

// PersistLedgerForOwner writes the ledger back to the per-owner audit
// ConfigMap. Creates the CM on first call stamped with the OMENative-
// managed-by label + a controller OwnerReference back to the owner so
// the cluster GC sweeps it when the owner is deleted.
//
// Writes are compare-and-swap against the ConfigMap state the ledger
// was loaded from: a concurrent writer's persist (or a create race)
// returns a Conflict error instead of silently erasing the other
// writer's entries. Callers inherit the retry via their reconcile
// requeue (or an explicit retry.RetryOnConflict around load+mutate+
// persist): reload, re-apply the mutation, persist again. A successful
// persist re-pins the ledger to the written revision, so sequential
// persists of the same in-memory ledger within one pass keep working.
//
// Owner is held as metav1.Object and the OwnerReference GVK is passed
// explicitly so the workload package stays free of CRD-specific imports;
// the adapter caller knows which owner Kind it owns.
func PersistLedgerForOwner(
	ctx context.Context,
	c client.Client,
	owner metav1.Object,
	gvk schema.GroupVersionKind,
	l *Ledger,
) error {
	raw, err := json.Marshal(l)
	if err != nil {
		return fmt.Errorf("marshal audit ledger: %w", err)
	}
	key := client.ObjectKey{Namespace: owner.GetNamespace(), Name: ConfigMapNameForOwner(owner)}

	if l.loadedResourceVersion == "" {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:            key.Name,
				Namespace:       key.Namespace,
				Labels:          map[string]string{labelManagedBy: managedByValue},
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(owner, gvk)},
			},
			Data: map[string]string{LedgerKey: string(raw)},
		}
		if err := c.Create(ctx, cm); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return apierrors.NewConflict(corev1.Resource("configmaps"), key.Name,
					fmt.Errorf("audit configmap created concurrently since ledger load"))
			}
			return fmt.Errorf("create audit configmap %s: %w", key.Name, err)
		}
		l.loadedResourceVersion = cm.ResourceVersion
		return nil
	}

	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, key, cm); err != nil {
		if apierrors.IsNotFound(err) {
			// Deleted since load (owner teardown cascade); a reload
			// decides whether recreating is still meaningful.
			return apierrors.NewConflict(corev1.Resource("configmaps"), key.Name,
				fmt.Errorf("audit configmap deleted since ledger load"))
		}
		return fmt.Errorf("get audit configmap %s: %w", key.Name, err)
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[LedgerKey] = string(raw)
	// Claim ownership when we land on a pre-existing CM without our
	// controller ref; without this we'd happily write to a CM the
	// cluster GC won't sweep when the owner is deleted.
	claimControllerRef(ctx, cm, owner, gvk)
	// CAS anchor: write against the revision the ledger was loaded at,
	// not whatever Get just returned.
	cm.ResourceVersion = l.loadedResourceVersion
	if err := c.Update(ctx, cm); err != nil {
		return fmt.Errorf("update audit configmap %s: %w", key.Name, err)
	}
	l.loadedResourceVersion = cm.ResourceVersion
	return nil
}

// claimControllerRef ensures the ConfigMap carries exactly one
// controller OwnerReference — the current owner's. A stale
// predecessor's controller ref (owner deleted and recreated under the
// same name, new UID) is replaced rather than appended beside: two
// Controller=true refs make the apiserver reject every subsequent
// write until GC sweeps the CM.
func claimControllerRef(ctx context.Context, cm *corev1.ConfigMap, owner metav1.Object, gvk schema.GroupVersionKind) {
	if hasControllerRefForOwner(cm, owner) {
		return
	}
	kept := make([]metav1.OwnerReference, 0, len(cm.OwnerReferences)+1)
	stale := 0
	for _, ref := range cm.OwnerReferences {
		if ref.Controller != nil && *ref.Controller {
			stale++
			continue
		}
		kept = append(kept, ref)
	}
	cm.OwnerReferences = append(kept, *metav1.NewControllerRef(owner, gvk))
	if stale > 0 {
		log.FromContext(ctx).Info("adopting audit configmap from stale controller owner",
			"configmap", cm.Namespace+"/"+cm.Name,
			"owner", owner.GetName(), "ownerUID", owner.GetUID(), "staleRefs", stale)
	}
}

// hasControllerRefForOwner reports whether obj already has a controller
// OwnerReference pointing at the given owner (matched on UID).
func hasControllerRefForOwner(obj metav1.Object, owner metav1.Object) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Controller != nil && *ref.Controller && ref.UID == owner.GetUID() {
			return true
		}
	}
	return false
}

// NewStartedEntry constructs a freshly-Started ledger row from a
// validated request. StartedAt prefers req.RequestedAt so duration
// graphs (CompletedAt - StartedAt) reflect the requester's accept-time rather
// than the controller's processing-time; falls back to now when the
// request omits the field.
func NewStartedEntry(req *MigrationRequest, uuid string, surgeIdx int32) Entry {
	startedAt := req.RequestedAt
	if startedAt == "" {
		startedAt = metav1.Now().UTC().Format(time.RFC3339)
	}
	return Entry{
		RequestUUID:     uuid,
		Component:       req.Component,
		SourceInstance:  req.Instance,
		SurgeInstance:   surgeIdx,
		Phase:           PhaseStarted,
		Reason:          req.Reason,
		FromNode:        req.FromNode,
		HintTargetNodes: append([]string(nil), req.HintTargetNodes...),
		StartedAt:       startedAt,
	}
}

// NewTerminalEntry rewrites a Started entry with the terminal phase and
// outcome. Returns a copy ready for UpsertEntry; does not write.
func NewTerminalEntry(prev Entry, phase, outcome string) Entry {
	out := prev
	out.Phase = phase
	out.Outcome = outcome
	out.CompletedAt = metav1.Now().UTC().Format(time.RFC3339)
	return out
}

// ValidateCapacity reports whether the migration request identified by
// requestUUID is admissible against the owner's status.migrations
// records — the single source of truth for migration work. Returns
// ok=true when admission is allowed; ok=false with a short reason
// string when the request must be rejected.
//
// The request's OWN record (already born Accepted by the accept pass)
// is excluded from both counts — capacity gates the request against
// the other work in flight, not against itself.
//
// Both caps bound EXECUTION, not queued intent — a queued-Accepted
// record holds no resources (the dispatcher executes serially), so a
// batch submission must never trip the caps for the records behind it:
//
//   - in-flight = non-terminal records whose surge is ALLOCATED
//     (SurgeInstance set). Terminal records (Completed / Failed /
//     Relocated) free their slot structurally — the wedged-Started-row
//     capacity leak cannot exist.
//   - per-window = records of ANY phase with AllocatedAt inside the
//     policy's trailing window. A nil AllocatedAt (queued, or an Auto
//     record that never allocates a surge) never counts.
//
// The two caps are independent — either tripping rejects the request.
// `now` is injected so tests can pin a deterministic time and walk the
// trailing window past the relevant records.
//
// A nil policy (no lifecycle.audit) is not admissible: the caps are the
// only bound on how much destructive migration work one requester can
// start. The caller holds the request on that answer rather than
// failing it, so the reason names the missing configuration.
func ValidateCapacity(policy *types.MigrationAuditPolicy, records []types.MigrationRecord, requestUUID string, now time.Time) (ok bool, reason string) {
	if policy == nil {
		return false, "migration capacity policy unconfigured (no lifecycle.audit)"
	}
	inFlight := int32(0)
	allocatedInWindow := int32(0)
	windowStart := now.Add(-policy.Window)

	for i := range records {
		r := &records[i]
		if r.RequestUUID == requestUUID {
			continue
		}
		if !r.Phase.Terminal() && r.SurgeAllocated() {
			inFlight++
		}
		if r.AllocatedAt != nil && r.AllocatedAt.Time.After(windowStart) {
			allocatedInWindow++
		}
	}

	if inFlight >= policy.MaxInFlight {
		return false, fmt.Sprintf("in-flight migration cap reached (%d/%d)", inFlight, policy.MaxInFlight)
	}
	if allocatedInWindow >= policy.MaxPerWindow {
		return false, fmt.Sprintf("migration rate cap reached (%d/%d in the last %s)", allocatedInWindow, policy.MaxPerWindow, policy.Window)
	}
	return true, ""
}

// parseTime parses the RFC3339-formatted StartedAt / CompletedAt strings
// the audit ledger uses. Returns (zero, false) on parse error so the
// caller can choose to skip rather than treat a malformed timestamp as
// "very old" or "very recent".
func parseTime(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t, true
	}
	return time.Time{}, false
}
