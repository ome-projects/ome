package inferencereplica

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// unallocatedSurgeIndex is the surge index of an audit row whose surge the
// executor has not allocated yet: the intake admits the request, and the
// executor writes the real index when it allocates the surge. -1, because 0
// is a valid Instance index.
const unallocatedSurgeIndex = int32(-1)

// requestOutcome is what one pass does with one migration request
// annotation on the parent InferenceService.
type requestOutcome int

const (
	// requestKnown: this replica already carries the request, or the
	// audit record holds it finished; the annotation is a re-delivery.
	requestKnown requestOutcome = iota
	// requestSibling: the request names another Component the parent
	// declares; that Component's replica answers it.
	requestSibling
	// requestInvalid: the request cannot be a migration here; it is
	// refused with a reason.
	requestInvalid
	// requestRefused: a valid request while the Component's
	// MigrationPolicy mode is Never.
	requestRefused
	// requestAccepted: a valid request this pass takes on.
	requestAccepted
	// requestWaits: a valid request behind this pass's one accept; it
	// stays on the parent for a later pass.
	requestWaits
)

// migrationRequest is one request annotation after classification.
type migrationRequest struct {
	key     string
	uuid    string
	outcome requestOutcome
	// body is the parsed request; nil when the value did not parse.
	body *audit.MigrationRequest
	// parseErr is why the value did not parse; an unsupported schema
	// version picks its own event reason.
	parseErr error
	// reason says why an invalid request is refused.
	reason string
}

func (request migrationRequest) invalid(reason string) migrationRequest {
	request.outcome = requestInvalid
	request.reason = reason
	return request
}

// statusEntry is the status record every answered valid request starts
// from: a Manual migration of the requested Instance off its node.
func (request migrationRequest) statusEntry(now metav1.Time) v1beta1.MigrationStatus {
	return v1beta1.MigrationStatus{
		RequestUUID:    request.uuid,
		Trigger:        v1beta1.MigrationTriggerManual,
		SourceInstance: request.body.Instance,
		FromNode:       request.body.FromNode,
		Reason:         request.body.Reason,
		StartedAt:      now,
	}
}

// migrationIntake answers the parent's migration request annotations
// for one pass: it classifies each request, records the answer on the
// replica's status and the audit record, and collects the annotations
// to remove at the end of the pass.
type migrationIntake struct {
	reconciler           *Reconciler
	log                  logr.Logger
	ir                   *v1beta1.InferenceReplica
	parent               *v1beta1.InferenceService
	mode                 workloadtypes.MigrationMode
	instanceReadyTimeout time.Duration
	auditRecord          *audit.Ledger
	auditRecordChanged   bool
	// accepted is set once a request is accepted: one Manual request per
	// pass, matching the one migration dispatch per pass.
	accepted bool
	// answered lists the annotations to remove from the parent.
	answered []string
}

// consumeMigrationRequests answers every migration request annotation
// on the parent that is this Component's to answer: a valid request
// becomes an Accepted status entry and a Started audit row, a valid one
// under mode Never a Failed entry that is terminal at birth, an invalid
// one a Failed audit row, and a re-delivered one nothing. The annotation
// is removed in each of those cases. A sibling Component's request and a
// valid request behind this pass's one accept stay for a later pass.
//
// Write order per answered request: the status entry first, then the
// audit row, then the annotation removals, batched into one
// acknowledgeRequest on the parent, so a crash between them is answered
// as a re-delivery next pass.
func (r *Reconciler) consumeMigrationRequests(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, mode workloadtypes.MigrationMode, instanceReadyTimeout time.Duration) (requeue bool, err error) {
	if parent == nil || len(parent.Annotations) == 0 {
		return false, nil
	}
	keys := migrationRequestKeys(parent)
	if len(keys) == 0 {
		return false, nil
	}

	// The audit record is parent-owned on this path (the same rule as
	// buildReconcileInput's LedgerOwner). Live read: sibling replicas of
	// the same parent write it concurrently, and a cache-lagged snapshot
	// persisted wholesale below would drop their rows.
	auditRecord, err := audit.LoadLedgerForOwner(ctx, r.APIReader, parent)
	if err != nil {
		return false, fmt.Errorf("load migration audit ledger: %w", err)
	}
	intake := &migrationIntake{
		reconciler:           r,
		log:                  log,
		ir:                   ir,
		parent:               parent,
		mode:                 mode,
		instanceReadyTimeout: instanceReadyTimeout,
		auditRecord:          auditRecord,
	}
	for _, k := range keys {
		if err := intake.answer(ctx, intake.classify(k, parent.Annotations[k])); err != nil {
			return false, err
		}
	}
	if intake.auditRecordChanged {
		if perr := audit.PersistLedgerForOwner(ctx, r.Client, parent, isvcGVK, auditRecord); perr != nil {
			return false, fmt.Errorf("persist migration audit ledger: %w", perr)
		}
	}
	if len(intake.answered) == 0 {
		return false, nil
	}
	parentKey := client.ObjectKeyFromObject(parent)
	return r.acknowledgeRequest(ctx, parent, "migration-request annotations", func(ctx context.Context) (client.Object, error) {
		fresh := &v1beta1.InferenceService{}
		return fresh, r.APIReader.Get(ctx, parentKey, fresh)
	}, intake.answered...)
}

// migrationRequestKeys lists the parent's migration request annotation
// keys in sorted order, so every pass answers them in the same order.
func migrationRequestKeys(parent *v1beta1.InferenceService) []string {
	keys := make([]string, 0, len(parent.Annotations))
	for k := range parent.Annotations {
		if isMigrationRequestKey(k) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// isMigrationRequestKey says whether an annotation key is a migration
// request.
func isMigrationRequestKey(key string) bool {
	return strings.HasPrefix(key, audit.MigrationRequestAnnotationPrefix)
}

// classify reads one annotation and decides its outcome. The checks
// repeat the webhook's, because annotations written before the webhook
// or around it arrive here too.
func (in *migrationIntake) classify(key, raw string) migrationRequest {
	request := migrationRequest{key: key, uuid: audit.ExtractRequestUUID(key)}
	if request.uuid != "" && (hasMigrationRecord(in.ir, request.uuid) || in.auditRecord.HasCompletedOrFailedRequest(request.uuid)) {
		request.outcome = requestKnown
		return request
	}
	request.body, request.parseErr = audit.ParseMigrationRequest(raw)
	switch {
	case request.uuid == "":
		return request.invalid("annotation key carries no request UUID")
	case request.parseErr != nil:
		return request.invalid(request.parseErr.Error())
	case request.body.Component != string(in.ir.Spec.Component):
		if parentHasComponent(in.parent, request.body.Component) {
			request.outcome = requestSibling
			return request
		}
		return request.invalid(fmt.Sprintf("component %q does not exist on this InferenceService", request.body.Component))
	case request.body.Instance < 0:
		return request.invalid(fmt.Sprintf("instance must be >= 0, got %d", request.body.Instance))
	case in.mode == workloadtypes.MigrationModeNever:
		request.outcome = requestRefused
	case in.accepted:
		request.outcome = requestWaits
	default:
		request.outcome = requestAccepted
	}
	return request
}

// answer records the outcome and, for every request this pass settles,
// queues its annotation for removal.
func (in *migrationIntake) answer(ctx context.Context, request migrationRequest) error {
	switch request.outcome {
	case requestSibling, requestWaits:
		return nil
	case requestKnown:
		// Only the re-delivered annotation is left to remove.
	case requestInvalid:
		in.recordRejected(request)
	case requestRefused:
		if err := in.recordRefused(ctx, request); err != nil {
			return err
		}
	case requestAccepted:
		if err := in.recordAccepted(ctx, request); err != nil {
			return err
		}
	}
	in.answered = append(in.answered, request.key)
	return nil
}

// recordRejected answers a request that cannot be a migration here: a
// Failed audit row when the key carries a UUID (a key without one has
// nothing to deduplicate on) and a Warning event on the parent.
func (in *migrationIntake) recordRejected(request migrationRequest) {
	if request.uuid != "" {
		in.recordAudit(failedAuditRow(request.uuid, request.body, request.reason, in.reconciler.now()))
	}
	// An unsupported schema version keeps its own event reason, so
	// version skew can be alerted on by itself.
	eventReason := workloadtypes.EventReasonMigrationRequestRejected
	if errors.Is(request.parseErr, audit.ErrUnsupportedSchemaVersion) {
		eventReason = workloadtypes.EventReasonUnsupportedSchemaVersion
	}
	in.rejectedEvent(eventReason, request.uuid, request.reason)
}

// recordRefused answers a valid request while the Component's
// MigrationPolicy mode is Never: a Failed status entry that is terminal
// at birth, a Failed audit row and a Warning event, so the refusal shows
// in every place an accepted migration would, in the same pass.
func (in *migrationIntake) recordRefused(ctx context.Context, request migrationRequest) error {
	const message = "migrations disabled by MigrationPolicy Mode=Never"
	now := metav1.NewTime(in.reconciler.now())
	entry := request.statusEntry(now)
	entry.Phase = v1beta1.MigrationPhaseFailed
	entry.Message = message
	entry.CompletedAt = &now
	entry.Deadline = now
	if err := in.appendStatusEntry(ctx, entry); err != nil {
		return err
	}
	in.recordAudit(failedAuditRow(request.uuid, request.body, message, now.Time))
	in.rejectedEvent(workloadtypes.EventReasonMigrationRequestRejected, request.uuid, message)
	in.log.V(1).Info("Migration request rejected: MigrationPolicy Mode=Never",
		"uuid", request.uuid, "sourceInstance", request.body.Instance, "fromNode", request.body.FromNode)
	return nil
}

// recordAccepted takes a valid request on: an Accepted status entry with
// the deadline the moved Instance has to become Ready by, and a Started
// audit row whose surge index the executor fills in when it allocates
// the surge.
func (in *migrationIntake) recordAccepted(ctx context.Context, request migrationRequest) error {
	now := metav1.NewTime(in.reconciler.now())
	entry := request.statusEntry(now)
	entry.HintTargetNodes = copiedNames(request.body.HintTargetNodes)
	entry.Phase = v1beta1.MigrationPhaseAccepted
	entry.Deadline = metav1.NewTime(now.Add(in.instanceReadyTimeout))
	if err := in.appendStatusEntry(ctx, entry); err != nil {
		return err
	}
	in.recordAudit(audit.NewStartedEntry(request.body, request.uuid, unallocatedSurgeIndex))
	in.accepted = true
	in.log.V(1).Info("Migration request accepted into status.migrations",
		"uuid", request.uuid, "sourceInstance", request.body.Instance, "fromNode", request.body.FromNode)
	return nil
}

// appendStatusEntry commits one entry to the replica's status.migrations.
func (in *migrationIntake) appendStatusEntry(ctx context.Context, entry v1beta1.MigrationStatus) error {
	r := in.reconciler
	if err := appendMigrationStatus(ctx, r.statusWriter(), r.liveReader(), in.ir, entry); err != nil {
		return fmt.Errorf("append migration status entry (uuid=%s): %w", entry.RequestUUID, err)
	}
	return nil
}

// recordAudit adds or replaces one row of the audit record and notes that
// the record is to be written at the end of the pass.
func (in *migrationIntake) recordAudit(row audit.Entry) {
	in.auditRecord.UpsertEntry(row)
	in.auditRecordChanged = true
}

// rejectedEvent warns on the parent, where the operator wrote the request.
func (in *migrationIntake) rejectedEvent(reason workloadtypes.EventReason, uuid, text string) {
	if in.reconciler.Recorder == nil {
		return
	}
	in.reconciler.Recorder.Eventf(in.parent, corev1.EventTypeWarning, string(reason),
		"OMENative migration uuid=%s rejected: %s", uuid, text)
}

// failedAuditRow is the audit row of a request that never started: a
// terminal Failed row that opens and closes at the same time.
func failedAuditRow(uuid string, body *audit.MigrationRequest, outcome string, at time.Time) audit.Entry {
	when := at.UTC().Format(time.RFC3339)
	entry := audit.Entry{
		RequestUUID: uuid,
		Phase:       audit.PhaseFailed,
		StartedAt:   when,
		CompletedAt: when,
		Outcome:     outcome,
	}
	if body != nil {
		entry.Component = body.Component
		entry.SourceInstance = body.Instance
		entry.FromNode = body.FromNode
	}
	return entry
}

// acknowledgeRequest removes the answered request annotations from the
// object, so a request is delivered once. The request's effect commits
// first and this removal runs second: a crash between the two delivers
// the request again, and every answer is a no-op on a re-delivery. The
// removal is one Update against a live read under conflict retry,
// because the pass-start snapshot goes stale on a busy object and a
// skipped removal would defer the next answer by a whole pass. It is
// mirrored onto the caller's object, so later steps of this pass see the
// request as answered. Exhausted retries ask for a requeue and the
// request is answered again next pass; an object that is gone has
// nothing left to acknowledge. readLive returns the object as the API
// server holds it now; request names the annotations in the error text.
func (r *Reconciler) acknowledgeRequest(ctx context.Context, obj client.Object, request string, readLive func(context.Context) (client.Object, error), keys ...string) (requeue bool, err error) {
	uerr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh, err := readLive(ctx)
		if err != nil {
			return err
		}
		annotations := fresh.GetAnnotations()
		changed := false
		for _, k := range keys {
			if _, ok := annotations[k]; ok {
				delete(annotations, k)
				changed = true
			}
		}
		if !changed {
			return nil
		}
		return r.Update(ctx, fresh)
	})
	if uerr != nil {
		if apierrors.IsConflict(uerr) {
			return true, nil
		}
		if apierrors.IsNotFound(uerr) {
			return false, nil
		}
		return false, fmt.Errorf("delete consumed %s: %w", request, uerr)
	}
	annotations := obj.GetAnnotations()
	for _, k := range keys {
		delete(annotations, k)
	}
	return false, nil
}

// appendMigrationStatus adds one record to the replica's status under
// conflict retry and mirrors the committed records onto the pass's copy. A
// record with the same request UUID already present writes nothing.
func appendMigrationStatus(ctx context.Context, writer statusWriter, reads client.Reader, ir *v1beta1.InferenceReplica, entry v1beta1.MigrationStatus) error {
	write := newMigrationRecordWrite(writer, reads, ir)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error { return write.appendOnce(ctx, entry) }); err != nil {
		return err
	}
	write.mirrorOnto(ir)
	return nil
}

// migrationToWorkload reads one API migration record as the engine's record.
// The two records share no memory: every pointer and list is copied. The
// trigger and the phase cross as the strings they are, so a value this
// manager does not know is written back unchanged.
func migrationToWorkload(v v1beta1.MigrationStatus) workloadtypes.MigrationRecord {
	return workloadtypes.MigrationRecord{
		RequestUUID:     v.RequestUUID,
		Trigger:         workloadtypes.MigrationTrigger(v.Trigger),
		SourceInstance:  v.SourceInstance,
		SurgeInstance:   copiedPointer(v.SurgeInstance),
		AllocatedAt:     copiedPointer(v.AllocatedAt),
		FromNode:        v.FromNode,
		HintTargetNodes: copiedNames(v.HintTargetNodes),
		Phase:           workloadtypes.MigrationPhase(v.Phase),
		Attempt:         v.Attempt,
		Reason:          v.Reason,
		Message:         v.Message,
		StartedAt:       v.StartedAt,
		Deadline:        v.Deadline,
		CompletedAt:     copiedPointer(v.CompletedAt),
		Succeeded:       copiedPointer(v.Succeeded),
	}
}

// migrationFromWorkload reads one engine migration record as the API record,
// field for field as migrationToWorkload reads the other way.
func migrationFromWorkload(w workloadtypes.MigrationRecord) v1beta1.MigrationStatus {
	return v1beta1.MigrationStatus{
		RequestUUID:     w.RequestUUID,
		Trigger:         v1beta1.MigrationTrigger(w.Trigger),
		SourceInstance:  w.SourceInstance,
		SurgeInstance:   copiedPointer(w.SurgeInstance),
		AllocatedAt:     copiedPointer(w.AllocatedAt),
		FromNode:        w.FromNode,
		HintTargetNodes: copiedNames(w.HintTargetNodes),
		Phase:           v1beta1.MigrationPhase(w.Phase),
		Attempt:         w.Attempt,
		Reason:          w.Reason,
		Message:         w.Message,
		StartedAt:       w.StartedAt,
		Deadline:        w.Deadline,
		CompletedAt:     copiedPointer(w.CompletedAt),
		Succeeded:       copiedPointer(w.Succeeded),
	}
}

// migrationsFromIR reads the replica's migration records as the engine's
// records, the list the engine selects migration work from; nil when the
// replica carries none.
func migrationsFromIR(ir *v1beta1.InferenceReplica) []workloadtypes.MigrationRecord {
	if len(ir.Status.Migrations) == 0 {
		return nil
	}
	out := make([]workloadtypes.MigrationRecord, len(ir.Status.Migrations))
	for i := range ir.Status.Migrations {
		out[i] = migrationToWorkload(ir.Status.Migrations[i])
	}
	return out
}

// buildMutateMigration is the engine's MutateMigration capability: under
// conflict retry, the replica is read live, the callback runs on the engine's
// copy of the record named by requestUUID, one write follows when the
// callback asks for it, and the committed records are mirrored onto the
// pass's copy of the replica. A record that is not there is left alone; a
// replica that is gone, or replaced under the same name, reads as
// ErrStatusOwnerGone.
func buildMutateMigration(writer statusWriter, reads client.Reader, ir *v1beta1.InferenceReplica) func(ctx context.Context, requestUUID string, mutate func(*workloadtypes.MigrationRecord) bool) error {
	return func(ctx context.Context, requestUUID string, mutate func(*workloadtypes.MigrationRecord) bool) error {
		write := newMigrationRecordWrite(writer, reads, ir)
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			return write.mutateOnce(ctx, requestUUID, mutate)
		})
		if err != nil {
			return err
		}
		write.mirrorOnto(ir)
		return nil
	}
}

// migrationRecordWrite is one conflict-retried write of a replica's
// migration records: the replica it targets by key and identity, and after
// an attempt, whether the attempt committed rows and which.
type migrationRecordWrite struct {
	writer    statusWriter
	reads     client.Reader
	key       client.ObjectKey
	ownerUID  types.UID
	wrote     bool
	committed []v1beta1.MigrationStatus
}

func newMigrationRecordWrite(writer statusWriter, reads client.Reader, ir *v1beta1.InferenceReplica) *migrationRecordWrite {
	return &migrationRecordWrite{writer: writer, reads: reads, key: client.ObjectKeyFromObject(ir), ownerUID: ir.UID}
}

// readOwner reads the replica live and decodes its rows. A replica that is
// gone, or another object under the same name, reads as ErrStatusOwnerGone.
func (w *migrationRecordWrite) readOwner(ctx context.Context) (*v1beta1.InferenceReplica, irstatus.Encoding, error) {
	fresh := &v1beta1.InferenceReplica{}
	source, err := irstatus.GetDecoded(ctx, w.reads, w.key, fresh)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, "", workloadtypes.ErrStatusOwnerGone
		}
		return nil, "", fmt.Errorf("re-read IR: %w", err)
	}
	if w.ownerUID == "" || fresh.UID != w.ownerUID {
		return nil, "", workloadtypes.ErrStatusOwnerGone
	}
	return fresh, source, nil
}

// write persists the rows of a freshly read replica. A replica deleted
// between the read and the write reads as ErrStatusOwnerGone.
func (w *migrationRecordWrite) write(ctx context.Context, fresh *v1beta1.InferenceReplica, source irstatus.Encoding) error {
	if err := updateInferenceReplicaStatus(ctx, w.writer, fresh, source); err != nil {
		if apierrors.IsNotFound(err) {
			return workloadtypes.ErrStatusOwnerGone
		}
		return fmt.Errorf("update IR status: %w", err)
	}
	return nil
}

// mirrorOnto copies the committed rows onto the pass's copy of the replica,
// so later work in the same pass reads the written state; an attempt that
// committed nothing leaves the copy as it is.
func (w *migrationRecordWrite) mirrorOnto(ir *v1beta1.InferenceReplica) {
	if !w.wrote {
		return
	}
	ir.Status.Migrations = copiedMigrations(w.committed)
}

// appendOnce is one attempt of the append: a fresh read and one write when
// the record is not there yet. A record already present is kept as it is.
func (w *migrationRecordWrite) appendOnce(ctx context.Context, entry v1beta1.MigrationStatus) error {
	w.wrote = false
	fresh, source, err := w.readOwner(ctx)
	if err != nil {
		return err
	}
	if migrationEntryIndex(fresh.Status.Migrations, entry.RequestUUID) == -1 {
		fresh.Status.Migrations = append(fresh.Status.Migrations, entry)
		if err := w.write(ctx, fresh, source); err != nil {
			return err
		}
	}
	w.wrote = true
	w.committed = fresh.Status.Migrations
	return nil
}

// mutateOnce is one attempt of the mutate capability: a fresh read, the
// callback on the record's workload mirror, and one write when the callback
// asks for it. A missing record is a clean no-op. The record's key is fixed:
// a callback cannot re-key it.
func (w *migrationRecordWrite) mutateOnce(ctx context.Context, requestUUID string, mutate func(*workloadtypes.MigrationRecord) bool) error {
	w.wrote = false
	fresh, source, err := w.readOwner(ctx)
	if err != nil {
		return err
	}
	pos := migrationEntryIndex(fresh.Status.Migrations, requestUUID)
	if pos == -1 {
		return nil
	}
	record := migrationToWorkload(fresh.Status.Migrations[pos])
	if !mutate(&record) {
		return nil
	}
	record.RequestUUID = requestUUID
	fresh.Status.Migrations[pos] = migrationFromWorkload(record)
	if err := w.write(ctx, fresh, source); err != nil {
		return err
	}
	w.wrote = true
	w.committed = fresh.Status.Migrations
	return nil
}

// migrationEntryIndex is the position of the record for uuid among entries,
// or -1 when there is none.
func migrationEntryIndex(entries []v1beta1.MigrationStatus, uuid string) int {
	for i := range entries {
		if entries[i].RequestUUID == uuid {
			return i
		}
	}
	return -1
}

// copiedMigrations deep-copies a record slice, so the pass's copy never
// aliases the rows of the written object.
func copiedMigrations(entries []v1beta1.MigrationStatus) []v1beta1.MigrationStatus {
	out := make([]v1beta1.MigrationStatus, len(entries))
	for i := range entries {
		out[i] = *entries[i].DeepCopy()
	}
	return out
}

// copiedPointer is a pointer to a copy of the value p points to, nil for nil.
func copiedPointer[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// copiedNames is a copy of a name list, nil when the list is empty.
func copiedNames(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	return append([]string(nil), names...)
}

// buildAppendMigration is the engine's AppendMigration capability: the record
// is added through appendMigrationStatus, so a record with the same request
// UUID already present writes nothing.
func buildAppendMigration(writer statusWriter, reads client.Reader, ir *v1beta1.InferenceReplica) func(ctx context.Context, rec workloadtypes.MigrationRecord) error {
	return func(ctx context.Context, rec workloadtypes.MigrationRecord) error {
		return appendMigrationStatus(ctx, writer, reads, ir, migrationFromWorkload(rec))
	}
}

// syncMigrationEntries keeps the replica's migration records in step with
// the audit record before the pass answers new requests. An audit row an
// operator started for this Component and nothing finished becomes an
// Accepted record when the replica has none for it, so a migration under
// way across a manager restart goes on through its record. A finished
// record older than recordWindow is dropped, but only when the audit record
// holds it finished too: a record the audit record still shows started
// would be imported again as new work, so it is kept and counted in one
// V(1) line per pass. A zero window drops nothing. One status write covers
// the import and the trim, and the committed records are mirrored onto the
// pass's copy. An unreadable audit record defers both to the next pass, and
// a replica at rest is neither read nor written here
// (migrationRecordsNeedLiveRead).
func (r *Reconciler) syncMigrationEntries(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, instanceReadyTimeout, recordWindow time.Duration) error {
	if r.Client == nil || ir == nil {
		return nil
	}
	auditRecord := r.loadMigrationAuditRecord(ctx, log, ir, parent)
	imports := importableLedgerMigrations(auditRecord, string(ir.Spec.Component), metav1.NewTime(r.now()), instanceReadyTimeout)
	if !migrationRecordsNeedLiveRead(ir, parent, imports) {
		return nil
	}
	sync := &migrationRecordSync{
		migrationRecordWrite: newMigrationRecordWrite(r.statusWriter(), r.liveReader(), ir),
		auditRecord:          auditRecord,
		imports:              imports,
		window:               recordWindow,
		now:                  r.now,
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error { return sync.syncOnce(ctx) }); err != nil {
		return err
	}
	if sync.agedKept > 0 {
		log.V(1).Info("migration trim: retained aged terminal entries with no terminal ledger row (backstop — the ledger must remember what status forgets)",
			"count", sync.agedKept)
	}
	sync.mirrorOnto(ir)
	return nil
}

// loadMigrationAuditRecord reads the audit record of the replica's owner,
// the parent service when there is one. An unreadable record reads as
// empty, so nothing is imported or trimmed this pass and the next pass
// reads it again.
func (r *Reconciler) loadMigrationAuditRecord(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService) *audit.Ledger {
	owner := client.Object(ir)
	if parent != nil {
		owner = parent
	}
	auditRecord, err := audit.LoadLedgerForOwner(ctx, r.Client, owner)
	if err != nil {
		log.V(1).Info("migration upgrade import skipped: ledger load failed; retrying next pass", "error", err.Error())
		return &audit.Ledger{}
	}
	return auditRecord
}

// migrationRecordSync is one conflict-retried sync of a replica's migration
// records with its audit record: the rows to import, the window terminal
// records age out of, the clock the window is measured from, and the count
// of aged records kept because the audit record does not hold them.
type migrationRecordSync struct {
	*migrationRecordWrite
	auditRecord *audit.Ledger
	imports     []v1beta1.MigrationStatus
	window      time.Duration
	now         func() time.Time
	agedKept    int
}

// syncOnce is one attempt: a fresh read, the trim, the import, and one write
// when either changed a row. An attempt that changes nothing still reports
// the fresh rows as committed, so the pass's copy follows the live object.
func (s *migrationRecordSync) syncOnce(ctx context.Context) error {
	s.wrote = false
	fresh, source, err := s.readOwner(ctx)
	if err != nil {
		return err
	}
	trimmed := s.trimAgedRecords(fresh)
	imported := s.importAuditRows(fresh)
	if trimmed || imported {
		if err := s.write(ctx, fresh, source); err != nil {
			return err
		}
	}
	s.wrote = true
	s.committed = fresh.Status.Migrations
	return nil
}

// trimAgedRecords drops the terminal records whose completion is older than
// the window; a zero window is unconfigured and ages nothing out. A
// Completed or Failed record goes only when the audit record holds it
// terminally, because status may forget only what the audit record
// remembers: a dropped record the audit record still shows as Started would
// be imported again as fresh work. A Relocated record is born terminal and
// goes on the window alone. Reports whether a record was dropped; the aged
// records kept are counted.
func (s *migrationRecordSync) trimAgedRecords(fresh *v1beta1.InferenceReplica) bool {
	cutoff := s.now().Add(-s.window)
	s.agedKept = 0
	changed := false
	kept := fresh.Status.Migrations[:0]
	for i := range fresh.Status.Migrations {
		e := fresh.Status.Migrations[i]
		if s.agedOut(e, cutoff) {
			if e.Phase == v1beta1.MigrationPhaseRelocated || s.auditRecord.HasCompletedOrFailedRequest(e.RequestUUID) {
				changed = true
				continue
			}
			s.agedKept++
		}
		kept = append(kept, e)
	}
	fresh.Status.Migrations = kept
	return changed
}

// agedOut says whether a terminal record completed before the cutoff.
func (s *migrationRecordSync) agedOut(e v1beta1.MigrationStatus, cutoff time.Time) bool {
	return s.window > 0 && e.Phase.Terminal() && e.CompletedAt != nil && e.CompletedAt.Time.Before(cutoff)
}

// importAuditRows appends the audit rows that have no record yet. Reports
// whether one was appended.
func (s *migrationRecordSync) importAuditRows(fresh *v1beta1.InferenceReplica) bool {
	changed := false
	for i := range s.imports {
		if migrationEntryIndex(fresh.Status.Migrations, s.imports[i].RequestUUID) != -1 {
			continue
		}
		fresh.Status.Migrations = append(fresh.Status.Migrations, *s.imports[i].DeepCopy())
		changed = true
	}
	return changed
}

// migrationRecordsNeedLiveRead says whether the sync must read the replica
// live: the pass's copy carries a migration record, the audit record holds
// a row to import, or the parent carries a migration request. A copy with
// a record is read live on every pass, because the cache can be one write
// behind the record's phase and the migrate pass must act on the live one.
// A request on the parent is classified against the records the replica
// carries live. A replica at rest meets none of the three.
func migrationRecordsNeedLiveRead(ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, imports []v1beta1.MigrationStatus) bool {
	return len(ir.Status.Migrations) > 0 || len(imports) > 0 || hasMigrationRequest(parent)
}

// hasMigrationRequest says whether the parent carries a migration request
// annotation.
func hasMigrationRequest(parent *v1beta1.InferenceService) bool {
	if parent == nil {
		return false
	}
	for k := range parent.Annotations {
		if isMigrationRequestKey(k) {
			return true
		}
	}
	return false
}

// importableLedgerMigrations lists the audit rows the sync turns into
// records: this Component's started rows of operator work (not a
// relocation, not a force delete) whose request has no finished row. The
// caller skips the rows the replica already records.
func importableLedgerMigrations(ledger *audit.Ledger, component string, now metav1.Time, instanceReadyTimeout time.Duration) []v1beta1.MigrationStatus {
	if ledger == nil {
		return nil
	}
	var out []v1beta1.MigrationStatus
	for i := range ledger.Entries {
		row := &ledger.Entries[i]
		if row.Phase != audit.PhaseStarted || row.Component != component {
			continue
		}
		if row.Reason == audit.ReasonAutoRecover || row.Reason == audit.ReasonForceDelete {
			continue
		}
		if ledger.HasCompletedOrFailedRequest(row.RequestUUID) {
			continue
		}
		entry := v1beta1.MigrationStatus{
			RequestUUID:     row.RequestUUID,
			Trigger:         v1beta1.MigrationTriggerManual,
			Phase:           v1beta1.MigrationPhaseAccepted,
			SourceInstance:  row.SourceInstance,
			FromNode:        row.FromNode,
			HintTargetNodes: copiedNames(row.HintTargetNodes),
			Reason:          row.Reason,
			StartedAt:       now,
			Deadline:        metav1.NewTime(now.Add(instanceReadyTimeout)),
		}
		if started, ok := parseLedgerTime(row.StartedAt); ok {
			entry.StartedAt = metav1.NewTime(started)
		}
		// A row with a surge index resumes on that Instance and counts as
		// executing since its start time, the allocation time the capacity
		// gate reads; a row at unallocatedSurgeIndex imports with no surge,
		// so the executor allocates one.
		if row.SurgeInstance >= 0 {
			s := row.SurgeInstance
			entry.SurgeInstance = &s
			allocated := entry.StartedAt
			entry.AllocatedAt = &allocated
		}
		out = append(out, entry)
	}
	return out
}

// parseLedgerTime parses the ledger's RFC3339 timestamps; ok=false on
// empty/malformed input (caller falls back to now).
func parseLedgerTime(raw string) (time.Time, bool) {
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

// hasMigrationRecord reports whether the replica carries a record for uuid.
func hasMigrationRecord(ir *v1beta1.InferenceReplica, uuid string) bool {
	return migrationEntryIndex(ir.Status.Migrations, uuid) != -1
}

// parentHasComponent reports whether the parent ISVC declares the named
// component: engine always; any other role per spec presence (same
// enumeration rule as the ISVC webhook's migration-request validation).
func parentHasComponent(parent *v1beta1.InferenceService, component string) bool {
	c := v1beta1.ComponentType(component)
	return c == v1beta1.EngineComponent || v1beta1.SpecOf(&parent.Spec, c) != nil
}

// now is the pass's clock: the injected clock's time, or the wall clock
// when no clock is wired (mirrors ReconcileInput.Now).
func (r *Reconciler) now() time.Time {
	return clockNow(r.Clock)
}
