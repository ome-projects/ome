package replay

import (
	"context"
	"errors"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/rollout"
)

// eventApplier turns one timeline event into the cluster, spec or replica
// change its vocabulary entry names, and returns the detail the trace records.
type eventApplier func(ctx context.Context, d *driver, ev TimelineEvent) (string, error)

// eventAppliers is the set of timeline events the driver can stage. An event
// the vocabulary declares but this map does not carry fails to load.
var eventAppliers map[string]eventApplier

// producedEvents are the events the engines produce inside the pass; a
// scenario stages the facts that make them happen instead of naming them.
var producedEvents = map[string]string{
	"canary.progress":        "the canary executor advances the step; stage the capacity and verb facts that advance it",
	"canary.completed":       "the canary executor completes the ladder; stage the facts that complete it",
	"canary.rolledBack":      "the canary executor records the revert; stage verb.rollback and the pod facts",
	"canary.failed":          "the canary executor parks the unit; stage the timer or crash facts",
	"canary.parked":          "the canary dispatch parks a rollback with no stable revision; stage stable.crMissing and verb.rollback",
	"canary.resumed":         "the canary executor re-arms on verb.resume",
	"coord.staged":           "the coordination pass rests a group Staged; stage member.staged",
	"coord.settled":          "the coordination pass reports a group Idle; stage member.converged",
	"ratio.met":              "the ratio evaluation is the coordination pass's own",
	"ratio.lost":             "the ratio evaluation is the coordination pass's own",
	"ratio.anchorMissing":    "the anchor is absent until the coordination pass snapshots it",
	"seq.nextTurn":           "the Sequential hand-off is the coordination pass's own; stage member.converged on the active member",
	"flush.staleOverwrite":   "the flush overwrites the live group entry on its own; stage ctrl.staleRead",
	"run.adopted":            "the run layer adopts an in-flight ladder on its own; stage ctrl.runLost and a resync",
	"target.new":             "the run layer presents a new target on its own; stage spec.revision",
	"canary.armed":           "the canary executor arms the unit on its own",
	"canary.cutoverCleared":  "the canary executor clears the stable identity on its own",
	"run.open":               "the run layer opens the run on its own",
	"run.retarget":           "the run layer retargets on its own; stage spec.revision",
	"run.close":              "the run layer closes the run on its own",
	"write.coordRolledOut":   "a coordination write the pass makes on its own",
	"write.coordReady":       "a coordination write the pass makes on its own",
	"write.canaryReady":      "a canary write the pass makes on its own",
	"write.canaryPromoted":   "a canary write the pass makes on its own",
	"write.canaryRolledBack": "a canary write the pass makes on its own",
	"write.canarySettled":    "a canary write the pass makes on its own",
	"write.modeSwitch":       "the cleanup's status write; the operator's switch is staged as spec.modeSwitch",
	"read.pinStable":         "a read the run layer makes on its own",
	"read.rollbackStable":    "a read the canary dispatch makes on its own",
	"read.stableCR":          "a read the canary dispatch makes on its own",
	"read.rollbackCR":        "a read the replica controller makes, which the replay does not run",
	"read.statusTrails":      "a read every stage makes on its own; stage ir.stale",
	"cr.collision":           "a hash collision is minted by the replica controller, which the replay does not run",
	"scope.rotated":          "a replica recreated under a new owner UID is not modelled; a projected replica deleted and projected again under the same service is staged as replica.reprojected",
	"ir.readError":           "the fake apiserver answers every replica read",
	"analysis.unavailable":   "the gate's miss is the pass's own: stage no analysis.sample anywhere for no sampler, none for a member for no fresh sample",
	"member.staged":          "a staged member needs the user partition flow, which is not modelled",
	"ir.staged":              "the replica rests at its staged shape on its own; stage the partition and ir.straggling, and the current revision stays behind the update revision by the engine's rule",
	"timer.pauseElapsed":     "a timed soak is not modelled",
	"timer.drainElapsed":     "a drain window is not modelled",
	"pod.canaryRestarted":    "the restart reading is not modelled",
	"pod.canaryDeleted":      "a canary pod on its way out is not modelled",
	"pod.replacementReady":   "the released floor's replacement is not modelled",
	"verb.pause":             "the pause is staged as spec.pause",
	"verb.unpause":           "the unpause is staged as spec.unpause",
	"spec.groupEdited":       "a group edit is staged as spec.planEdit",
	"policy.changed":         "a policy body edit is not modelled",
}

// Produced reports whether the engines produce an event inside the pass,
// so no scenario stages it, with the note saying what a scenario stages
// instead.
func Produced(id string) (note string, ok bool) {
	note, ok = producedEvents[id]
	return note, ok
}

// Stageable reports whether a scenario may stage the event: an applier
// exists for it.
func Stageable(id string) bool {
	_, ok := eventAppliers[id]
	return ok
}

// staleReadEvents are the events that stage a pass over the previous
// tick's cache; a run that stages one snapshots the store every tick.
var staleReadEvents = map[string]struct{}{
	"ctrl.staleRead":   {},
	"pass.staleRecord": {},
	"read.staleCopy":   {},
}

func init() {
	eventAppliers = map[string]eventApplier{
		"ctrl.resync":              applyResync,
		"ctrl.crash":               applyCtrlCrash,
		"pass.resync":              applyResync,
		"spec.revision":            applyTargetChange,
		"member.targetChanged":     applyTargetChange,
		"replica.targetBumped":     applyTargetChangeOrRuntimeEdit,
		"runtime.drifted":          applyRuntimeDrift,
		"runtime.synced":           applyRuntimeSync,
		"runtime.editRefused":      applyRuntimeEditRefused,
		"member.rollback":          applyMemberRollback,
		"ir.converged":             applyConverged,
		"ir.straggling":            applyStraggling,
		"ir.stale":                 applyStale,
		"member.surgePodCreated":   applySurgePod,
		"member.readyOnTarget":     applyReadyOnTarget,
		"member.oldPodsGone":       applyOldPodsGone,
		"member.converged":         applyPromote,
		"replica.currentPromoted":  applyPromote,
		"replica.currentWithdrawn": applyWithdraw,
		"member.capacityLost":      applyCapacityLost,
		"member.failed":            applyFailed,
		"member.surgeFailed":       applySurgeFailed,
		"member.held":              applyHeld,
		"member.recovered":         applyRowPhase(v1beta1.OMENativeInstanceReady),
		"member.dark":              applyDark,
		"ir.missing":               applyReplicaMissing,
		"capacity.reached":         applyCapacityReached,
		"capacity.dropped":         applyCapacityDropped,
		"pod.stableGone":           applyStablePodGone,
		"pod.canaryCrashed":        applyCanaryCrash,
		"analysis.sample":          applyAnalysisSample,
		"record.legacy":            applyRecordLegacy,
		"pod.rejectedGone":         applyRejectedGone,
		"pod.lastStableRolled":     applyLastStableRolled,
		"stable.crMissing":         applyCollected,
		"cr.collected":             applyCollected,
		"replica.reprojected":      applyReprojected,
		"verb.repin":               applyRepin,
		"plan.repin":               applyRepin,
		"verb.rollback":            applyAnnotation(constants.RolloutRollbackAnnotation, "true"),
		"verb.promote":             applyPromoteVerb(constants.RolloutPromoteAnnotation),
		"verb.promoteForce":        applyPromoteVerb(constants.RolloutPromoteForceAnnotation),
		"verb.resume":              applyPromoteVerb(constants.RolloutResumeAnnotation),
		"spec.pause":               applyAnnotation(constants.PausedRolloutAnnotation, "true"),
		"spec.unpause":             applyAnnotation(constants.PausedRolloutAnnotation, ""),
		"plan.drift":               applyPlanEdit,
		"spec.planEdit":            applyPlanEdit,
		"spec.modeSwitch":          applyModeSwitch,
		"plan.unresolved":          applyPolicyRemoved,
		"policy.unresolved":        applyPolicyRemoved,
		"policy.resolved":          applyPolicyResolved,
		"spec.replicas":            applyReplicas,
		"spec.partition":           applyPartition,
		"ctrl.staleRead":           applyStaleRead,
		"pass.staleRecord":         applyStaleRead,
		"read.staleCopy":           applyStaleRead,
		"ctrl.runLost":             applyRunLost,
		"run.lost":                 applyRunLost,
		"api.flushFailed":          applyFlushFailed,
		"api.conflict":             applyConflict,
		"api.serviceWriteFailed":   applyServiceWriteFailed,
		"timer.soak":               applySoakTimer,
		"timer.readyTimeout":       applyReadyTimeoutTimer,
		PeerServicesCreated:        applyPeerServicesCreated,
	}
}

// eventArgKeys is the argument each event reads. An event absent from the
// map takes no arguments.
var eventArgKeys = map[string][]string{
	ClockAdvance:               {"duration"},
	PeerServicesCreated:        {"component"},
	"spec.revision":            {"component", "image", "protocol"},
	"member.targetChanged":     {"component", "image", "protocol"},
	"replica.targetBumped":     {"component", "image", "protocol"},
	"runtime.drifted":          {"component", "image"},
	"runtime.synced":           {"value"},
	"runtime.editRefused":      {"component", "image", "reason"},
	"member.rollback":          {"component"},
	"ir.converged":             {"component"},
	"ir.straggling":            {"component", "trailing"},
	"ir.stale":                 {"component", "lag"},
	"member.surgePodCreated":   {"component", "index", "replacement", "ordinal", "waiting"},
	"member.readyOnTarget":     {"component", "index", "listedAfter"},
	"member.oldPodsGone":       {"component", "index"},
	"member.converged":         {"component"},
	"replica.currentPromoted":  {"component"},
	"replica.currentWithdrawn": {"component"},
	"member.capacityLost":      {"component", "index", "gone", "image"},
	"member.failed":            {"component", "index", "reason"},
	"member.surgeFailed":       {"component", "index", "reason"},
	"member.held":              {"component", "attempts"},
	"member.recovered":         {"component", "index"},
	"member.dark":              {"component"},
	"ir.missing":               {"component"},
	"capacity.reached":         {"component"},
	"capacity.dropped":         {"component", "index"},
	"pod.stableGone":           {"component", "index", "gone"},
	"pod.canaryCrashed":        {"component", "index"},
	"analysis.sample":          {"component", "outcome"},
	"record.legacy":            {"component"},
	"pod.rejectedGone":         {"component"},
	"pod.lastStableRolled":     {"component"},
	"stable.crMissing":         {"component", "image"},
	"cr.collected":             {"component", "image"},
	"replica.reprojected":      {"component"},
	"verb.repin":               {"value"},
	"plan.repin":               {"value"},
	"verb.promote":             {"component", "image"},
	"verb.promoteForce":        {"component", "image"},
	"verb.resume":              {"component", "image"},
	"plan.drift":               {"groups"},
	"spec.planEdit":            {"groups"},
	"spec.modeSwitch":          {"component", "groups"},
	"plan.unresolved":          {"name"},
	"policy.unresolved":        {"name"},
	"policy.resolved":          {"name"},
	"spec.replicas":            {"component", "to"},
	"spec.partition":           {"component", "to"},
	"api.flushFailed":          {"count"},
	"api.conflict":             {"write", "count"},
	"api.serviceWriteFailed":   {"count"},
	"timer.soak":               {"slack"},
	"timer.readyTimeout":       {"slack"},
	"ctrl.crash":               {"leaseWait"},
}

// apply stages one event and records it.
func (d *driver) apply(ctx context.Context, ev TimelineEvent) error {
	if ev.ID == ClockAdvance {
		detail, err := d.advance("clock.advance", ev.Args.Duration)
		if err != nil {
			return err
		}
		d.trace.applied(ev, detail)
		return nil
	}
	applier, ok := eventAppliers[ev.ID]
	if !ok {
		return fmt.Errorf("replay: no applier for event %s", ev.ID)
	}
	detail, err := applier(ctx, d, ev)
	if err != nil {
		return err
	}
	d.trace.applied(ev, detail)
	return nil
}

// advance moves the injected clock forward.
func (d *driver) advance(field, value string) (string, error) {
	delta, err := ParseDuration(field, value)
	if err != nil {
		return "", err
	}
	if delta < 0 {
		return "", fmt.Errorf("replay: %s: the clock does not run backwards", field)
	}
	d.clock.Step(delta)
	return "by=" + delta.String(), nil
}

func requireMember(d *driver, ev TimelineEvent) (*member, error) {
	if ev.Args.Component == "" {
		return nil, fmt.Errorf("replay: event %s needs a component", ev.Key())
	}
	return d.member(ev.Args.Component)
}

func requireImage(ev TimelineEvent) (string, error) {
	if ev.Args.Image == "" {
		return "", fmt.Errorf("replay: event %s needs the image whose template mints the revision", ev.Key())
	}
	return ev.Args.Image, nil
}

func applyResync(context.Context, *driver, TimelineEvent) (string, error) {
	return "", nil
}

// applyCtrlCrash is the manager process restarting between two passes.
// The service, its replicas, the policies, the revisions and the pods are
// durable, and no stage keeps anything between passes that outranks them:
// the run layer, the canary executor and the coordination pass each read
// their state back from status, so the pass that follows is the
// new manager's first and reads what the apiserver holds. What the restart
// costs is time: leaseWait is how long the new manager waits for the
// leader Lease before that pass, and the clock moves by it.
func applyCtrlCrash(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Variant != "" {
		return "", fmt.Errorf("replay: ctrl.crash takes no variant")
	}
	wait, err := ParseDuration("ctrl.crash.leaseWait", ev.Args.LeaseWait)
	if err != nil {
		return "", err
	}
	if wait < 0 {
		return "", fmt.Errorf("replay: ctrl.crash.leaseWait: the clock does not run backwards")
	}
	if wait == 0 {
		return "", nil
	}
	d.clock.Step(wait)
	return "leaseWait=" + wait.String(), nil
}

// applyTargetChange edits a Component's template image: the service spec
// moves, the projection targets the new revision, and the replica records
// it as its update revision. With a protocol the revision the image mints
// carries it and spec.rollout.pairingProtocol moves to it.
func applyTargetChange(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	image, err := requireImage(ev)
	if err != nil {
		return "", err
	}
	cs := d.service.Components[string(m.component)]
	cs.Image = image
	d.service.Components[string(m.component)] = cs
	m.image = image
	detail := fmt.Sprintf("component=%s image=%s", m.component, image)
	// While the recorded runtime revision has drifted the controller ends
	// its pass before the projection, so the replica keeps its template and
	// mints no revision; the member records the pushed target on the first
	// publication after the projection carries it, as it does a runtime edit.
	if d.runtime != nil && d.runtime.drifted() {
		m.pendingTarget = image
		detail += " projection=after-sync"
	} else {
		m.retarget(image)
		ir, err := d.replica(ctx, m.component)
		if err != nil {
			return "", err
		}
		m.retargetCreateAttempts(image, d.stepStableImage(m, ir, image))
		m.retargetUnservedUpdateAttempts(image)
	}
	if ev.Args.Protocol != "" {
		if d.service.RevisionProtocols == nil {
			d.service.RevisionProtocols = map[string]string{}
		}
		d.service.RevisionProtocols[image] = ev.Args.Protocol
		protocol := ev.Args.Protocol
		d.service.PairingProtocol = &protocol
		detail += " protocol=" + protocol
	}
	generation, err := d.writeSpec(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s generation=%d", detail, generation), nil
}

// applyMemberRollback returns a member's template to its current revision
// while its pods of the abandoned revision still exist.
func applyMemberRollback(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	if m.current == "" || m.current == m.image {
		return "", fmt.Errorf("replay: member.rollback: %s targets its current revision already", m.component)
	}
	ev.Args.Image = m.current
	return applyTargetChange(ctx, d, ev)
}

// applyConverged settles a member on its roll target: every row Ready on it
// with one Ready pod, and the current revision promoted in the same
// publication. The roll target is the revision the replica's rollback
// target names while that revision is retained for it, as the replica
// controller rolls; else the spec target. The publication promotes by the
// engine's rule, so a Held retry record on the target keeps the current
// revision where it was.
func applyConverged(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	target, err := d.rollTargetImage(ctx, m)
	if err != nil {
		return "", err
	}
	m.current = target
	m.promoteStaged = true
	m.failedAttempts = 0
	m.pods = m.pods[:0]
	for i := range m.rows {
		m.rows[i].phase = v1beta1.OMENativeInstanceReady
		m.rows[i].running = target
		m.rows[i].served = true
		m.rows[i].target = ""
		m.rows[i].readySince = d.clock.Now()
		m.pods = append(m.pods, podState{index: m.rows[i].index, image: target, ready: true, serving: true})
	}
	return fmt.Sprintf("component=%s revision=%s", m.component, target), nil
}

// applyReprojected deletes a member's replica, as an operator's delete does:
// its pods leave with it, and the garbage collector keeps every
// ControllerRevision the service still owns and drops the rest. The member
// has no rows until the pass projects the replica again, and then comes up
// Pending on the replica's roll target before any pod runs.
func applyReprojected(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	ir, err := d.replica(ctx, m.component)
	if err != nil {
		return "", err
	}
	kept, dropped, err := d.collectAfterDelete(ctx, m, ir.UID)
	if err != nil {
		return "", err
	}
	if err := d.cluster.staging(func() error { return d.cluster.cli.Delete(ctx, ir) }); err != nil {
		return "", fmt.Errorf("replay: delete replica %s: %w", ir.Name, err)
	}
	pods := len(m.pods)
	m.rows, m.pods = nil, nil
	m.current, m.target = "", m.image
	m.reprojected = true
	return fmt.Sprintf("component=%s replica=%s pods=%d revisions-kept=%d revisions-collected=%d", m.component, ir.Name, pods, kept, dropped), nil
}

// collectAfterDelete is the garbage collector's pass over a member's
// ControllerRevisions once its replica is deleted: a revision another live
// owner holds stays with the deleted replica's reference removed, and one
// the replica alone owned is deleted. It reports how many stayed and how
// many went.
func (d *driver) collectAfterDelete(ctx context.Context, m *member, replicaUID k8stypes.UID) (kept, dropped int, err error) {
	for image, cr := range m.revisions {
		live := &appsv1.ControllerRevision{}
		if err := d.cluster.cli.Get(ctx, client.ObjectKeyFromObject(cr), live); err != nil {
			if apierrors.IsNotFound(err) {
				delete(m.revisions, image)
				continue
			}
			return 0, 0, fmt.Errorf("replay: read revision %s: %w", cr.Name, err)
		}
		owners := make([]metav1.OwnerReference, 0, len(live.OwnerReferences))
		for _, ref := range live.OwnerReferences {
			if ref.UID != replicaUID {
				owners = append(owners, ref)
			}
		}
		if len(owners) == 0 {
			if err := d.cluster.staging(func() error { return d.cluster.cli.Delete(ctx, live) }); err != nil {
				return 0, 0, fmt.Errorf("replay: collect revision %s: %w", live.Name, err)
			}
			delete(m.revisions, image)
			dropped++
			continue
		}
		live.OwnerReferences = owners
		if err := d.cluster.staging(func() error { return d.cluster.cli.Update(ctx, live) }); err != nil {
			return 0, 0, fmt.Errorf("replay: remove the deleted replica from revision %s: %w", live.Name, err)
		}
		m.revisions[image] = live
		kept++
	}
	return kept, dropped, nil
}

// applyStraggling stands every Instance of a member but the named number
// of its lowest ones Ready on the target, the rest on the previous
// revision, and names the target as the member's stable image. The
// publication decides the current revision by the engine's rule, which
// keeps the previous revision while an Instance trails the target.
func applyStraggling(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	if ev.Args.Trailing <= 0 || int(ev.Args.Trailing) >= len(m.rows) {
		return "", fmt.Errorf("replay: ir.straggling: trailing must be between 1 and %d", len(m.rows)-1)
	}
	previous := m.current
	m.current = m.target
	m.promoteStaged = true
	m.failedAttempts = 0
	m.pods = m.pods[:0]
	for i := range m.rows {
		running := m.target
		if i < int(ev.Args.Trailing) {
			running = previous
		}
		m.rows[i].phase = v1beta1.OMENativeInstanceReady
		m.rows[i].running = running
		m.rows[i].served = true
		m.rows[i].target = ""
		m.pods = append(m.pods, podState{index: m.rows[i].index, image: running, ready: true, serving: true})
	}
	return fmt.Sprintf("component=%s trailing=%d", m.component, ev.Args.Trailing), nil
}

// applyStale publishes the member's next status behind its generation, or
// with lag=status publishes nothing beyond a promotion the stored rows
// earned, so the last written status stands, rows included, as it does
// while the replica controller has not run since the spec moved.
func applyStale(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	switch ev.Args.Lag {
	case "", "generation":
		m.stale = true
	case "status":
		m.keepStatus = true
	default:
		return "", fmt.Errorf("replay: %s: lag must be generation or status, got %q", ev.Key(), ev.Args.Lag)
	}
	detail := "component=" + string(m.component)
	if ev.Args.Lag != "" {
		detail += " lag=" + ev.Args.Lag
	}
	return detail, nil
}

// applySurgePod creates an Instance's first pod on the target revision, not
// yet Ready, and marks the row updating toward it; without an index every
// Instance surges. A start on an Instance whose failed replacement still
// stands takes that pod's slot: the Instance keeps one replacement. An
// Instance whose row is an open Create attempt gets the attempt's pod
// instead, on the attempt's revision.
func applySurgePod(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	if ev.Args.Replacement != nil {
		return applyGangSurge(m, ev)
	}
	var indexes []int32
	if ev.Args.Index != nil {
		if m.row(*ev.Args.Index) == nil {
			return "", fmt.Errorf("replay: %s: no row %d", ev.Key(), *ev.Args.Index)
		}
		indexes = []int32{*ev.Args.Index}
	} else {
		for _, row := range m.rows {
			indexes = append(indexes, row.index)
		}
	}
	ordinal, detail := int32(1), ""
	if ev.Args.Ordinal != nil {
		ordinal, detail = *ev.Args.Ordinal, fmt.Sprintf(" ordinal=%d", *ev.Args.Ordinal)
	}
	for _, index := range indexes {
		row := m.row(index)
		image := m.target
		if row.createAttempt() {
			// The attempt's own pod: the row stays a Create attempt toward
			// its revision until the pod is Ready.
			image = row.target
		} else {
			row.phase = v1beta1.OMENativeInstanceUpdating
			row.target = m.target
		}
		row.waiting = ev.Args.Waiting
		if m.podOn(index, image) == nil {
			m.pods = append(m.pods, podState{index: index, ordinal: ordinal, image: image, ready: false})
		}
	}
	if ev.Args.Waiting != "" {
		detail += " waiting=" + ev.Args.Waiting
	}
	return fmt.Sprintf("component=%s instances=%d revision=%s%s", m.component, len(indexes), m.target, detail), nil
}

// applyGangSurge opens a gang surge on one Instance: the replacement gang
// is a row of its own at the named index, Creating toward the target with
// its pod not yet Ready, and the source row names that index, as the engine
// writes the pair. member.readyOnTarget on the replacement's index readies
// it; member.oldPodsGone on the source's index ends the pair, the
// replacement promoted in the source's place and the source row gone.
func applyGangSurge(m *member, ev TimelineEvent) (string, error) {
	if ev.Args.Index == nil {
		return "", fmt.Errorf("replay: %s: a replacement index needs the index of the Instance it replaces", ev.Key())
	}
	source := m.row(*ev.Args.Index)
	if source == nil {
		return "", fmt.Errorf("replay: %s: no row %d", ev.Key(), *ev.Args.Index)
	}
	replacement := *ev.Args.Replacement
	if m.row(replacement) != nil {
		return "", fmt.Errorf("replay: %s: row %d exists; a replacement is built at a free index", ev.Key(), replacement)
	}
	if source.replacement != nil {
		return "", fmt.Errorf("replay: %s: Instance %d of %s already has its replacement at %d", ev.Key(), source.index, m.component, *source.replacement)
	}
	sourceIndex := source.index
	source.phase = v1beta1.OMENativeInstanceUpdating
	source.target = m.target
	source.replacement = &replacement
	m.rows = append(m.rows, rowState{index: replacement, phase: v1beta1.OMENativeInstanceCreating, target: m.target, replaces: &sourceIndex})
	m.pods = append(m.pods, podState{index: replacement, ordinal: 1, image: m.target, ready: false})
	return fmt.Sprintf("component=%s instances=1 revision=%s replacement=%d", m.component, m.target, replacement), nil
}

// podOn is an Instance's pod on the given revision, nil when it has none.
func (m *member) podOn(index int32, image string) *podState {
	for i := range m.pods {
		if m.pods[i].index == index && m.pods[i].image == image {
			return &m.pods[i]
		}
	}
	return nil
}

// applySurgeFailed ends an Instance's surge without its replacement
// serving. The row reads Ready, the phase the replica publishes over the
// engine's Failed row while the old pod serves, keeps the target revision
// and carries no operation; the extra pod stays, never Ready, until a
// start on the Instance replaces it.
func applySurgeFailed(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	if ev.Args.Index == nil {
		return "", fmt.Errorf("replay: %s needs the row index", ev.Key())
	}
	row := m.row(*ev.Args.Index)
	if row == nil {
		return "", fmt.Errorf("replay: %s: no row %d", ev.Key(), *ev.Args.Index)
	}
	if row.target == "" || row.target == row.running {
		return "", fmt.Errorf("replay: %s: Instance %d of %s has no surge toward a target revision; stage member.surgePodCreated for it first", ev.Key(), row.index, m.component)
	}
	replacement := m.podOn(row.index, row.target)
	if replacement == nil {
		return "", fmt.Errorf("replay: %s: Instance %d of %s has no pod on the target revision; stage member.surgePodCreated for it first", ev.Key(), row.index, m.component)
	}
	if replacement.ready {
		return "", fmt.Errorf("replay: %s: the replacement of Instance %d of %s is Ready; a surge that served is not a failed one", ev.Key(), row.index, m.component)
	}
	if old := m.podOn(row.index, row.running); old == nil || !old.ready || !old.serving {
		return "", fmt.Errorf("replay: %s: Instance %d of %s has no serving pod on its running revision; the publication writes Ready over a failed replacement only while the old pod serves", ev.Key(), row.index, m.component)
	}
	row.phase = v1beta1.OMENativeInstanceReady
	row.served = true
	detail := fmt.Sprintf("component=%s index=%d revision=%s", m.component, row.index, row.target)
	return detail + m.recordFailure(row, ev.Args.Reason), nil
}

// recordFailure records an ended attempt on its row and on the member's
// retry record, as the replica controller does: the row's last failure
// names the reason, the open attempt waits on nothing any more, and a
// failure the environment caused (the scheduler could not place the pod)
// counts nothing on the record, so the next pass starts the attempt
// again at once, while any other failure is one the schedule counted. It
// returns the apply detail the reason adds, empty when none was named.
func (m *member) recordFailure(row *rowState, reason string) string {
	row.waiting = ""
	row.failureReason = reason
	if !types.IsEnvironmentCausedReason(reason) {
		m.failedAttempts++
	}
	if reason == "" {
		return ""
	}
	return " reason=" + reason
}

// applyFailed sets a row Failed, as the replica publishes an attempt that
// ended without serving, and records the failure (recordFailure).
func applyFailed(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	if ev.Args.Index == nil {
		return "", fmt.Errorf("replay: %s needs the row index", ev.Key())
	}
	row := m.row(*ev.Args.Index)
	if row == nil {
		return "", fmt.Errorf("replay: %s: no row %d", ev.Key(), *ev.Args.Index)
	}
	row.phase = v1beta1.OMENativeInstanceFailed
	detail := fmt.Sprintf("component=%s index=%d phase=%s", m.component, row.index, row.phase)
	return detail + m.recordFailure(row, ev.Args.Reason), nil
}

// applyHeld sets or clears a member's retry record for its target as the
// replica controller would publish it. Set, the record reads Held after
// the attempts the event names and no fresh start opens on the member;
// released, the operator's release removed it and the next attempt opens
// as a fresh start. The stand-in runs no retry schedule of its own, so the
// scenario names the moment.
func applyHeld(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	switch ev.Variant {
	case "set":
		if m.target == "" || m.target == m.current {
			return "", fmt.Errorf("replay: %s: %s has no push in flight whose record could be Held", ev.Key(), m.component)
		}
		if ev.Args.Attempts <= 0 {
			return "", fmt.Errorf("replay: %s needs the failed attempts the record counts", ev.Key())
		}
		m.heldAttempts = ev.Args.Attempts
		return fmt.Sprintf("component=%s revision=%s attempts=%d", m.component, m.target, m.heldAttempts), nil
	case "released":
		if m.heldAttempts == 0 {
			return "", fmt.Errorf("replay: %s: %s has no Held record to release", ev.Key(), m.component)
		}
		m.heldAttempts, m.failedAttempts = 0, 0
		return fmt.Sprintf("component=%s revision=%s", m.component, m.target), nil
	}
	return "", fmt.Errorf("replay: %s: the variant is set or released", ev.Key())
}

func (m *member) row(index int32) *rowState {
	for i := range m.rows {
		if m.rows[i].index == index {
			return &m.rows[i]
		}
	}
	return nil
}

// applyReadyOnTarget marks every target-revision pod of a member Ready, or
// the one Instance's when the event names an index.
func applyReadyOnTarget(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	if ev.Args.Index != nil && m.row(*ev.Args.Index) == nil {
		return "", fmt.Errorf("replay: %s: no row %d", ev.Key(), *ev.Args.Index)
	}
	listedAfter, err := ParseDuration("member.readyOnTarget.listedAfter", ev.Args.ListedAfter)
	if err != nil {
		return "", err
	}
	n := 0
	for i := range m.pods {
		if m.pods[i].image == m.target && (ev.Args.Index == nil || m.pods[i].index == *ev.Args.Index) {
			m.pods[i].ready, m.pods[i].serving = true, true
			m.pods[i].listedAfter = listedAfter
			n++
		}
	}
	if n == 0 {
		return "", fmt.Errorf("replay: member.readyOnTarget: %s has no pod on its target revision", m.component)
	}
	for i := range m.rows {
		if ev.Args.Index == nil || m.rows[i].index == *ev.Args.Index {
			m.rows[i].waiting = ""
		}
	}
	detail := fmt.Sprintf("component=%s pods=%d", m.component, n)
	if listedAfter > 0 {
		detail += " listedAfter=" + listedAfter.String()
	}
	return detail, nil
}

// applyOldPodsGone removes every pod of a member that is not on the target
// revision, or with an index the pods of that one Instance; each row it
// covers stands Ready on the target pod that replaced its old one, so a
// row with no target pod fails the run. A row whose gang surge built its
// replacement as a row of its own ends its pair instead: the replacement
// row is promoted Ready on the target and the source row is removed.
func applyOldPodsGone(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	covers := func(index int32) bool { return ev.Args.Index == nil || *ev.Args.Index == index }
	if ev.Args.Index != nil && m.row(*ev.Args.Index) == nil {
		return "", fmt.Errorf("replay: %s: no row %d", ev.Key(), *ev.Args.Index)
	}
	kept := m.pods[:0]
	removed := 0
	for _, p := range m.pods {
		if p.image == m.target || !covers(p.index) {
			kept = append(kept, p)
			continue
		}
		removed++
	}
	m.pods = kept
	ended := map[int32]bool{}
	for i := range m.rows {
		if !covers(m.rows[i].index) {
			continue
		}
		promoted := &m.rows[i]
		if m.rows[i].replacement != nil {
			promoted = m.row(*m.rows[i].replacement)
			if promoted == nil {
				return "", fmt.Errorf("replay: %s: Instance %d of %s names a replacement row %d that does not exist", ev.Key(), m.rows[i].index, m.component, *m.rows[i].replacement)
			}
			ended[m.rows[i].index] = true
		}
		if m.podOn(promoted.index, m.target) == nil {
			return "", fmt.Errorf("replay: %s: Instance %d of %s has no pod on the target revision; stage member.surgePodCreated for it first", ev.Key(), promoted.index, m.component)
		}
		onTarget, readyOnTarget := m.podsOn(promoted.index, m.target)
		promoted.served = onTarget > 0 && readyOnTarget == onTarget
		promoted.running = m.target
		promoted.target = ""
		promoted.phase = v1beta1.OMENativeInstanceReady
		promoted.readySince = d.clock.Now()
		promoted.replaces = nil
		m.promotedOn(m.target)
	}
	if len(ended) > 0 {
		rows := m.rows[:0]
		for _, r := range m.rows {
			if !ended[r.index] {
				rows = append(rows, r)
			}
		}
		m.rows = rows
	}
	detail := fmt.Sprintf("component=%s removed=%d", m.component, removed)
	if ev.Args.Index != nil {
		detail = fmt.Sprintf("component=%s instance=%d removed=%d", m.component, *ev.Args.Index, removed)
	}
	return detail, nil
}

// applyPromote stands for the replica controller's promotion write: the
// next publication lands the promotion the rows have earned by the engine's
// rule (every Instance Ready on the update revision and no retry record
// naming it) instead of leaving it for the publication after, and the
// member's stable image moves to the target.
func applyPromote(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	m.current = m.target
	m.failedAttempts = 0
	m.promoteStaged = true
	return fmt.Sprintf("component=%s revision=%s", m.component, m.target), nil
}

// applyWithdraw names the moment the replica controller empties its
// current revision. The publication empties it by the engine's rule, when
// the current revision names the update revision while an Instance is off
// it, and keeps the current revision otherwise; the member's stable image
// reads empty for the events that name it.
func applyWithdraw(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	m.current = ""
	return "component=" + string(m.component), nil
}

// applyCapacityLost takes a member's pods of one revision out of readiness,
// the target's or the current's, or with gone removes them: the rows keep
// their revision and wait for the replica to rebuild them, and the
// publication writes their phase from the pods (Restarting over pods that
// serve nothing, Pending over none). With an index
// only that Instance's pods of the revision are taken, the one pod a
// kubelet loses; without one the whole revision goes. An image names the
// revision outright where the variant cannot: after a push back the target
// and the current revision agree while an Instance still runs the revision
// the push came back from.
func applyCapacityLost(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	image := m.target
	if ev.Variant == "oldRevision" {
		image = m.current
	}
	if ev.Args.Image != "" {
		if image, err = m.resolveImage(ev.Args.Image); err != nil {
			return "", err
		}
	}
	covers := func(index int32) bool { return ev.Args.Index == nil || *ev.Args.Index == index }
	if ev.Args.Index != nil && m.row(*ev.Args.Index) == nil {
		return "", fmt.Errorf("replay: %s: no row %d", ev.Key(), *ev.Args.Index)
	}
	n := 0
	kept := m.pods[:0]
	for i := range m.pods {
		if m.pods[i].image != image || !covers(m.pods[i].index) {
			kept = append(kept, m.pods[i])
			continue
		}
		n++
		if ev.Args.Gone {
			continue
		}
		m.pods[i].ready, m.pods[i].serving = false, false
		kept = append(kept, m.pods[i])
	}
	m.pods = kept
	detail := fmt.Sprintf("component=%s revision=%s pods=%d", m.component, image, n)
	if ev.Args.Index != nil {
		if n == 0 {
			return "", fmt.Errorf("replay: %s: Instance %d of %s has no pod on that revision", ev.Key(), *ev.Args.Index, m.component)
		}
		detail = fmt.Sprintf("component=%s instance=%d revision=%s pods=%d", m.component, *ev.Args.Index, image, n)
	}
	if ev.Args.Gone {
		detail += " gone"
	}
	return detail, nil
}

// applyRowPhase writes one row's phase.
func applyRowPhase(phase v1beta1.OMENativeInstancePhase) eventApplier {
	return func(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
		m, err := requireMember(d, ev)
		if err != nil {
			return "", err
		}
		if ev.Args.Index == nil {
			return "", fmt.Errorf("replay: %s needs the row index", ev.Key())
		}
		row := m.row(*ev.Args.Index)
		if row == nil {
			return "", fmt.Errorf("replay: %s: no row %d", ev.Key(), *ev.Args.Index)
		}
		row.phase = phase
		if phase == v1beta1.OMENativeInstanceReady && row.running != "" {
			m.promotedOn(row.running)
		}
		return fmt.Sprintf("component=%s index=%d phase=%s", m.component, *ev.Args.Index, phase), nil
	}
}

// applyReplicaMissing deletes a member's InferenceReplica, as an operator
// or a garbage collector removing the object would. The member publishes
// nothing and syncs no pod until the projector has created the object
// again, which the next pass does.
func applyReplicaMissing(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	ir, err := d.replica(ctx, m.component)
	if err != nil {
		return "", err
	}
	if err := d.cluster.staging(func() error { return d.cluster.cli.Delete(ctx, ir) }); err != nil {
		return "", fmt.Errorf("replay: remove replica %s: %w", ir.Name, err)
	}
	m.missing = true
	return fmt.Sprintf("component=%s replica=%s", m.component, ir.Name), nil
}

// applyDark takes every pod of a member out of rotation; the publication
// writes its settled rows Restarting from the pods, and no row reads Failed.
func applyDark(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	for i := range m.pods {
		m.pods[i].ready, m.pods[i].serving = false, false
	}
	return "component=" + string(m.component), nil
}

// applyCapacityReached stages the step's canary capacity on a member: the
// Instances above the projected partition stand Ready on the target.
func applyCapacityReached(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	ir, err := d.replica(ctx, m.component)
	if err != nil {
		return "", err
	}
	partition := int32(0)
	if ir.Spec.Pacing != nil && ir.Spec.Pacing.Partition != nil {
		partition = *ir.Spec.Pacing.Partition
	}
	staged := 0
	for i := range m.rows {
		if m.rows[i].index < partition {
			continue
		}
		m.setInstance(m.rows[i].index, m.target, d.clock.Now())
		staged++
	}
	return fmt.Sprintf("component=%s partition=%d staged=%d revision=%s", m.component, partition, staged, m.target), nil
}

// setInstance stands one Instance Ready on a revision with one Ready pod.
func (m *member) setInstance(index int32, image string, now time.Time) {
	row := m.row(index)
	if row == nil {
		return
	}
	row.phase = v1beta1.OMENativeInstanceReady
	row.running = image
	row.target = ""
	row.readySince = now
	row.served = true
	m.removePodsOf(index)
	m.pods = append(m.pods, podState{index: index, image: image, ready: true, serving: true})
}

// applyCapacityDropped takes one Instance's target pod out of readiness.
func applyCapacityDropped(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	if ev.Args.Index == nil {
		return "", fmt.Errorf("replay: %s needs the index of the Instance that dips", ev.Key())
	}
	n := 0
	for i := range m.pods {
		if m.pods[i].index == *ev.Args.Index && m.pods[i].image == m.target {
			m.pods[i].ready, m.pods[i].serving = false, false
			n++
		}
	}
	if n == 0 {
		return "", fmt.Errorf("replay: %s: Instance %d has no pod on the target revision", ev.Key(), *ev.Args.Index)
	}
	return fmt.Sprintf("component=%s index=%d", m.component, *ev.Args.Index), nil
}

// applyStablePodGone takes one Instance's pod on the current revision, the
// stable side of a canary split, out of readiness (a crash loop, a lost
// probe) or, with gone, removes it (a deletion the replica has yet to
// rebuild); the row keeps its revision either way.
func applyStablePodGone(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	if ev.Args.Index == nil {
		return "", fmt.Errorf("replay: %s needs the index of the stable Instance", ev.Key())
	}
	n := 0
	kept := m.pods[:0]
	for i := range m.pods {
		if m.pods[i].index != *ev.Args.Index || m.pods[i].image != m.current {
			kept = append(kept, m.pods[i])
			continue
		}
		n++
		if ev.Args.Gone {
			continue
		}
		m.pods[i].ready, m.pods[i].serving = false, false
		kept = append(kept, m.pods[i])
	}
	m.pods = kept
	if n == 0 {
		return "", fmt.Errorf("replay: %s: Instance %d has no pod on the current revision", ev.Key(), *ev.Args.Index)
	}
	detail := fmt.Sprintf("component=%s index=%d", m.component, *ev.Args.Index)
	if ev.Args.Gone {
		detail += " gone"
	}
	return detail, nil
}

// applyCanaryCrash puts the runner of one Instance's target-revision pod
// into a crash loop after the Instance served: the kubelet restarted the
// runner once since the Instance entered Ready and the restarted run died
// again now, so the pod reports CrashLoopBackOff with that death recorded
// and is not Ready. The row keeps the Ready phase the replica publishes
// over a pod set that served and lost its runner. The crash reading dates
// the deaths against the row's readiness and the run's open, so the clock
// must have moved since the Instance served.
func applyCanaryCrash(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	if ev.Args.Index == nil {
		return "", fmt.Errorf("replay: %s needs the index of the Instance whose pod keeps dying", ev.Key())
	}
	row := m.row(*ev.Args.Index)
	if row == nil {
		return "", fmt.Errorf("replay: %s: no row %d", ev.Key(), *ev.Args.Index)
	}
	pod := m.podOn(row.index, m.target)
	if pod == nil {
		return "", fmt.Errorf("replay: %s: Instance %d of %s has no pod on the target revision", ev.Key(), row.index, m.component)
	}
	if row.readySince.IsZero() || !d.clock.Now().After(row.readySince) {
		return "", fmt.Errorf("replay: %s: Instance %d of %s has not served on the target revision before now; a crash loop is read only after the Instance served, so stage capacity.reached and move the clock first", ev.Key(), row.index, m.component)
	}
	pod.ready, pod.serving = false, false
	pod.crashedAt = d.clock.Now()
	return fmt.Sprintf("component=%s index=%d revision=%s", m.component, row.index, m.target), nil
}

// applyRecordLegacy writes a member's canary status the way an external
// status writer would, in the shape the variant names, so the next pass
// reads it back from the store. Only the Failed phase with an empty failure
// reason is staged: the primary's phase reads Failed while its record names
// no reason for it.
func applyRecordLegacy(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	if ev.Variant != "failedPhaseNoMarker" {
		return "", fmt.Errorf("replay: %s: only the failedPhaseNoMarker variant is staged", ev.Key())
	}
	isvc, err := d.liveService(ctx)
	if err != nil {
		return "", err
	}
	cs := rollout.CanaryStatusFor(&isvc.Status, m.component)
	if cs == nil {
		return "", fmt.Errorf("replay: %s: %s carries no canary record to write over", ev.Key(), m.component)
	}
	cs.Failed = nil
	entry := isvc.Status.Components[m.component]
	entry.RolloutPhase = v1beta1.RolloutPhaseFailed
	isvc.Status.Components[m.component] = entry
	if err := d.cluster.staging(func() error { return d.cluster.cli.Status().Update(ctx, isvc) }); err != nil {
		return "", fmt.Errorf("replay: write the %s status: %w", m.component, err)
	}
	return fmt.Sprintf("component=%s phase=%s failed=nil", m.component, v1beta1.RolloutPhaseFailed), nil
}

// applyRejectedGone returns a unit's members to their current revision:
// every pod of a rejected revision leaves and the rows stand on the
// current revision again. The unit variant names the whole unit; a
// component names one member.
func applyRejectedGone(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	members := d.components()
	if ev.Args.Component != "" {
		m, err := d.member(ev.Args.Component)
		if err != nil {
			return "", err
		}
		members = []v1beta1.ComponentType{m.component}
	}
	removed := 0
	for _, c := range members {
		m := d.members[c]
		if m.current == "" {
			continue
		}
		for i := range m.rows {
			if m.rows[i].running != m.current || m.rows[i].target != "" {
				removed++
			}
			m.setInstance(m.rows[i].index, m.current, d.clock.Now())
		}
	}
	return fmt.Sprintf("instances=%d", removed), nil
}

// applyLastStableRolled rolls every remaining Instance of a member onto its
// target revision.
func applyLastStableRolled(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	if ev.Variant == "short" {
		// A recreate deletes the released Instance's old pod before its
		// replacement exists: the row waits on the target with no pod while
		// every other Instance already stands on the target.
		recreating := 0
		for i := range m.rows {
			row := &m.rows[i]
			if row.running == m.target {
				continue
			}
			kept := m.pods[:0]
			for _, p := range m.pods {
				if p.index != row.index {
					kept = append(kept, p)
				}
			}
			m.pods = kept
			row.phase = v1beta1.OMENativeInstanceUpdating
			row.target = m.target
			recreating++
		}
		return fmt.Sprintf("component=%s revision=%s recreating=%d", m.component, m.target, recreating), nil
	}
	for i := range m.rows {
		m.setInstance(m.rows[i].index, m.target, d.clock.Now())
	}
	return fmt.Sprintf("component=%s revision=%s", m.component, m.target), nil
}

// applyCollected takes a member's revision away from the dispatch: the
// sweep garbage-collects a revision nothing names (cr.collected), or the
// replica loses control of a revision it still names (stable.crMissing),
// which is the other way the dispatch finds no retained revision it
// controls.
func applyCollected(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	image, err := requireImage(ev)
	if err != nil {
		return "", err
	}
	cr, ok := m.revisions[image]
	if !ok {
		return "", fmt.Errorf("replay: %s: %s never minted a revision for %s", ev.Key(), m.component, image)
	}
	if ev.ID == "stable.crMissing" {
		return d.disownRevision(ctx, m, image, cr)
	}
	if m.current == image || m.target == image {
		return "", fmt.Errorf("replay: %s: the replica's revision pair still names %s; the sweep keeps a named revision", ev.Key(), image)
	}
	for _, row := range m.rows {
		if row.running == image || row.target == image {
			return "", fmt.Errorf("replay: %s: a row of %s still names %s; the sweep keeps a named revision", ev.Key(), m.component, image)
		}
	}
	if err := d.cluster.staging(func() error { return d.cluster.cli.Delete(ctx, cr) }); err != nil && !apierrors.IsNotFound(err) {
		return "", fmt.Errorf("replay: collect revision %s: %w", cr.Name, err)
	}
	delete(m.revisions, image)
	if d.collected[m.component] == nil {
		d.collected[m.component] = map[string]bool{}
	}
	d.collected[m.component][image] = true
	return fmt.Sprintf("component=%s revision=%s collected", m.component, cr.Name), nil
}

// disownRevision rewrites a ControllerRevision's controller reference to
// another object and drops the service's ownership, so the revision is
// outside the Component's history: neither the replica controls it nor the
// service owns it.
func (d *driver) disownRevision(ctx context.Context, m *member, image string, cr *appsv1.ControllerRevision) (string, error) {
	live := &appsv1.ControllerRevision{}
	if err := d.cluster.cli.Get(ctx, client.ObjectKeyFromObject(cr), live); err != nil {
		return "", fmt.Errorf("replay: read revision %s: %w", cr.Name, err)
	}
	owners := live.OwnerReferences[:0]
	for _, ref := range live.OwnerReferences {
		if ref.Controller == nil || !*ref.Controller {
			continue
		}
		ref.Name += "-previous"
		ref.UID += "-previous"
		owners = append(owners, ref)
	}
	live.OwnerReferences = owners
	if err := d.cluster.staging(func() error { return d.cluster.cli.Update(ctx, live) }); err != nil {
		return "", fmt.Errorf("replay: disown revision %s: %w", cr.Name, err)
	}
	m.revisions[image] = live
	return fmt.Sprintf("component=%s revision=%s controlled-by-another-object", m.component, cr.Name), nil
}

// applyRepin writes the repin annotation: the value the scenario names, or
// "now".
func applyRepin(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	value := ev.Args.Value
	if value == "" {
		value = "now"
	}
	return "value=" + value, d.setAnnotation(ctx, constants.RolloutRepinAnnotation, value)
}

// applyAnnotation writes one fixed annotation value, or removes the key.
func applyAnnotation(key, value string) eventApplier {
	return func(ctx context.Context, d *driver, _ TimelineEvent) (string, error) {
		detail := key + "=" + value
		if value == "" {
			detail = key + "=removed"
		}
		return detail, d.setAnnotation(ctx, key, value)
	}
}

// applyPromoteVerb writes a verb that names a revision hash: the hash of
// the image the scenario names, minted for the member it names.
func applyPromoteVerb(key string) eventApplier {
	return func(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
		m, err := requireMember(d, ev)
		if err != nil {
			return "", err
		}
		image := ev.Args.Image
		if image == "" {
			image = m.target
		}
		hash, err := d.revisionHash(ctx, m, image)
		if err != nil {
			return "", err
		}
		return key + "=" + hash, d.setAnnotation(ctx, key, hash)
	}
}

// applyPlanEdit replaces spec.rollout.groups.
func applyPlanEdit(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	d.service.Groups = append([]v1beta1.RolloutGroup(nil), ev.Args.Groups...)
	generation, err := d.writeSpec(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("groups=%d generation=%d", len(d.service.Groups), generation), nil
}

// applyModeSwitch moves a Component out of the OMENative mode and, as
// admission requires of a member that is not OMENative, replaces
// spec.rollout.groups with groups that do not name it, in one write. From
// this tick the pass resolves the Component to RawDeployment: no replica
// is projected or published for it, and the cleanup stage removes what
// OMENative had projected and clears its OMENative status.
func applyModeSwitch(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	if d.departed[m.component] {
		return "", fmt.Errorf("replay: %s: %s has already left the OMENative mode", ev.Key(), m.component)
	}
	groups := d.service.Groups
	if ev.Args.Groups != nil {
		groups = append([]v1beta1.RolloutGroup(nil), ev.Args.Groups...)
	}
	for gi := range groups {
		for _, c := range groups[gi].Components {
			if c == m.component {
				return "", fmt.Errorf("replay: %s: groups[%d] still names %s; admission refuses a group member that is not OMENative, so the switch must take it out of every group", ev.Key(), gi, m.component)
			}
		}
	}
	d.departed[m.component] = true
	d.service.Groups = groups
	generation, err := d.writeSpec(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("component=%s mode=%s groups=%d generation=%d", m.component, constants.RawDeployment, len(groups), generation), nil
}

// applyPolicyRemoved deletes a referenced RolloutPolicy.
func applyPolicyRemoved(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Args.Name == "" {
		return "", fmt.Errorf("replay: %s needs the policy name", ev.Key())
	}
	policy := &v1beta1.RolloutPolicy{}
	key := client.ObjectKey{Namespace: d.opts.Namespace, Name: ev.Args.Name}
	if err := d.cluster.cli.Get(ctx, key, policy); err != nil {
		return "", fmt.Errorf("replay: %s: %w", ev.Key(), err)
	}
	d.removedPolicies[ev.Args.Name] = policy.DeepCopy()
	if err := d.cluster.staging(func() error { return d.cluster.cli.Delete(ctx, policy) }); err != nil {
		return "", fmt.Errorf("replay: remove policy %s: %w", ev.Args.Name, err)
	}
	return "policy=" + ev.Args.Name, nil
}

// applyPolicyResolved creates a RolloutPolicy again with the body it had
// when plan.unresolved or policy.unresolved removed it, as an operator
// restoring a deleted policy would.
func applyPolicyResolved(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Args.Name == "" {
		return "", fmt.Errorf("replay: %s needs the policy name", ev.Key())
	}
	removed, ok := d.removedPolicies[ev.Args.Name]
	if !ok {
		return "", fmt.Errorf("replay: %s: no earlier event removed policy %s", ev.Key(), ev.Args.Name)
	}
	policy := &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: removed.Namespace, Name: removed.Name},
		Spec:       *removed.Spec.DeepCopy(),
	}
	if err := d.cluster.staging(func() error { return d.cluster.cli.Create(ctx, policy) }); err != nil {
		return "", fmt.Errorf("replay: restore policy %s: %w", ev.Args.Name, err)
	}
	delete(d.removedPolicies, ev.Args.Name)
	return "policy=" + ev.Args.Name, nil
}

// applyReplicas rewrites a Component's desired replica count. A new
// Instance appears as a Create attempt with no pod yet, the row the replica
// controller's create pass writes for an Instance it has no row for: the
// attempt names the revision the pass chooses (scaleUpImage) and the
// running revision is recorded when the Instance is promoted Ready. A
// removed Instance takes its row and pods with it.
func applyReplicas(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	if ev.Args.To == nil || *ev.Args.To <= 0 {
		return "", fmt.Errorf("replay: %s needs a positive to", ev.Key())
	}
	to := *ev.Args.To
	cs := d.service.Components[string(m.component)]
	cs.Replicas = to
	d.service.Components[string(m.component)] = cs
	image, err := d.scaleUpImage(ctx, m)
	if err != nil {
		return "", err
	}
	for int32(len(m.rows)) < to {
		m.rows = append(m.rows, creatingRow(int32(len(m.rows)), image))
	}
	for int32(len(m.rows)) > to {
		last := m.rows[len(m.rows)-1].index
		m.rows = m.rows[:len(m.rows)-1]
		kept := m.pods[:0]
		for _, p := range m.pods {
			if p.index != last {
				kept = append(kept, p)
			}
		}
		m.pods = kept
	}
	generation, err := d.writeSpec(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("component=%s to=%d generation=%d", m.component, to, generation), nil
}

// applyPartition sets or clears a Component's user partition.
func applyPartition(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	cs := d.service.Components[string(m.component)]
	detail := "cleared"
	if ev.Variant == "cleared" {
		cs.Partition = nil
	} else {
		if ev.Args.To == nil {
			return "", fmt.Errorf("replay: %s needs to", ev.Key())
		}
		to := *ev.Args.To
		cs.Partition = &to
		detail = fmt.Sprintf("to=%d", to)
	}
	d.service.Components[string(m.component)] = cs
	generation, err := d.writeSpec(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("component=%s %s generation=%d", m.component, detail, generation), nil
}

// applyStaleRead stages one pass whose cached reads serve the store as the
// previous pass found it, while its live reads stay current. The variant
// is a claim about what the stale copy misses, checked once the tick's
// publications have landed. A copy that misses a repin lags the status
// alone: the verb's removal is a metadata write of its own, which the cache
// delivers apart from the status flush.
func applyStaleRead(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	if d.cluster.stale == nil {
		return "", fmt.Errorf("replay: %s: no cache snapshot to serve; a stale read needs a previous tick", ev.Key())
	}
	d.cluster.stalePass = true
	d.staleClaim = ev.Key()
	detail := fmt.Sprintf("cache=before-pass-%d", d.cluster.staleTick)
	if ev.Key() == "ctrl.staleRead[missesRepin]" {
		if err := d.overlayLiveMetadata(ctx); err != nil {
			return "", err
		}
		detail += " metadata=live"
	}
	return detail, nil
}

// overlayLiveMetadata gives the stale copy of the service the live
// object's annotations while its status stays as the previous pass found
// it.
func (d *driver) overlayLiveMetadata(ctx context.Context) error {
	live, err := d.liveService(ctx)
	if err != nil {
		return err
	}
	stale := &v1beta1.InferenceService{}
	if err := d.cluster.stale.Get(ctx, d.isvcKey(), stale); err != nil {
		return fmt.Errorf("replay: read the stale copy: %w", err)
	}
	stale.Annotations = copyMap(live.Annotations)
	writer, ok := d.cluster.stale.(client.Client)
	if !ok {
		return fmt.Errorf("replay: the stale view is not writable")
	}
	if err := writer.Update(ctx, stale); err != nil {
		return fmt.Errorf("replay: overlay the live metadata on the stale copy: %w", err)
	}
	return nil
}

// checkStaleClaim verifies the staged stale-read variant against the live
// object and the snapshot: a claim the cluster does not bear out fails
// the run rather than recording a lag that misses nothing.
func (d *driver) checkStaleClaim(ctx context.Context) error {
	claim := d.staleClaim
	d.staleClaim = ""
	if claim == "" {
		return nil
	}
	live, err := d.liveService(ctx)
	if err != nil {
		return err
	}
	stale := &v1beta1.InferenceService{}
	if err := d.cluster.stale.Get(ctx, d.isvcKey(), stale); err != nil {
		return fmt.Errorf("replay: read the stale copy: %w", err)
	}
	lr, sr := live.Status.Rollout, stale.Status.Rollout
	activeID := func(rs *v1beta1.RolloutStatus) string {
		if rs == nil || rs.ActiveRun == nil {
			return ""
		}
		return rs.ActiveRun.RunID
	}
	pinnedAt := func(rs *v1beta1.RolloutStatus) time.Time {
		if rs == nil || rs.ActiveRun == nil {
			return time.Time{}
		}
		return rs.ActiveRun.PinnedAt.Time
	}
	holds := true
	switch claim {
	case "ctrl.staleRead[missesOpen]":
		holds = activeID(lr) != "" && activeID(sr) != activeID(lr)
	case "ctrl.staleRead[missesClose]":
		holds = activeID(sr) != "" && activeID(sr) != activeID(lr) && lr != nil && lr.LastRun != nil
	case "ctrl.staleRead[missesRepin]":
		holds = activeID(lr) != "" && activeID(sr) == activeID(lr) && pinnedAt(lr).After(pinnedAt(sr))
	case "ctrl.staleRead[anchorMissing]":
		holds = anyGroup(live, func(g v1beta1.RolloutCoordinationGroupStatus) bool { return anchorOf(&g) != "nil" }) &&
			!anyGroup(stale, func(g v1beta1.RolloutCoordinationGroupStatus) bool { return anchorOf(&g) != "nil" })
	case "ctrl.staleRead[phaseRegressed]":
		holds = groupPhasesDiffer(live, stale)
	case "pass.staleRecord":
		holds = !canaryRecordsEqual(live, stale)
	case "read.staleCopy[canaryOwned]", "read.staleCopy[coordinationOwned]":
		holds = !revisionFieldsEqual(live, stale)
	case "read.staleCopy[coordinationOwnedEmpty]":
		holds = anyRevisionRecord(live) && !anyRevisionRecord(stale)
	}
	if !holds {
		return fmt.Errorf("replay: %s: the stale copy does not miss what the variant claims", claim)
	}
	return nil
}

func anyGroup(isvc *v1beta1.InferenceService, pred func(v1beta1.RolloutCoordinationGroupStatus) bool) bool {
	if isvc.Status.RolloutCoordination == nil {
		return false
	}
	for _, g := range isvc.Status.RolloutCoordination.Groups {
		if pred(g) {
			return true
		}
	}
	return false
}

func groupPhasesDiffer(live, stale *v1beta1.InferenceService) bool {
	before := groupsByName(stale.Status.RolloutCoordination)
	for name, g := range groupsByName(live.Status.RolloutCoordination) {
		if before[name].Phase != g.Phase || before[name].CompositePhase != g.CompositePhase {
			return true
		}
	}
	return false
}

func canaryRecordsEqual(live, stale *v1beta1.InferenceService) bool {
	for c, cs := range live.Status.Components {
		if cs.Canary == nil {
			continue
		}
		other := stale.Status.Components[c].Canary
		if other == nil || !sameRecord(other, cs.Canary) {
			return false
		}
	}
	return true
}

func sameRecord(a, b *v1beta1.CanaryStatus) bool {
	return a.TargetID == b.TargetID && a.CanaryRevisionHash == b.CanaryRevisionHash && a.StableRevisionHash == b.StableRevisionHash &&
		a.CurrentStep == b.CurrentStep && a.ObservedTrafficWeight == b.ObservedTrafficWeight && a.PromotedThrough == b.PromotedThrough &&
		a.PreStepHold == b.PreStepHold && a.RolledBackRevisionHash == b.RolledBackRevisionHash && (a.Failed == nil) == (b.Failed == nil)
}

// anyRevisionRecord reports whether any Component of the service carries one
// of the three rolled-out revision fields.
func anyRevisionRecord(isvc *v1beta1.InferenceService) bool {
	for _, cs := range isvc.Status.Components {
		if cs.LatestReadyRevision != "" || cs.LatestRolledoutRevision != "" || cs.PreviousRolledoutRevision != "" {
			return true
		}
	}
	return false
}

func revisionFieldsEqual(live, stale *v1beta1.InferenceService) bool {
	for c, cs := range live.Status.Components {
		other := stale.Status.Components[c]
		if cs.LatestReadyRevision != other.LatestReadyRevision || cs.LatestRolledoutRevision != other.LatestRolledoutRevision ||
			cs.PreviousRolledoutRevision != other.PreviousRolledoutRevision {
			return false
		}
	}
	return true
}

// applyRunLost drops the pinned run from the live status: the status
// loss a dropped flush, or a status written without the run fields, leaves.
func applyRunLost(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	live, err := d.liveService(ctx)
	if err != nil {
		return "", err
	}
	if live.Status.Rollout == nil || live.Status.Rollout.ActiveRun == nil {
		return "", fmt.Errorf("replay: %s: no run is pinned", ev.Key())
	}
	id := live.Status.Rollout.ActiveRun.RunID
	live.Status.Rollout.ActiveRun = nil
	if err := d.cluster.staging(func() error { return d.cluster.cli.Status().Update(ctx, live) }); err != nil {
		return "", fmt.Errorf("replay: drop the pinned run: %w", err)
	}
	return "run=" + id, nil
}

// applyFlushFailed arms a non-conflict failure on the next status writes.
func applyFlushFailed(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	count := ev.Args.Count
	if count == 0 {
		count = 1
	}
	d.cluster.flushFailures = count
	return fmt.Sprintf("count=%d", count), nil
}

// applyConflict arms a 409 on the write-th status write of the next pass.
func applyConflict(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Args.Write < 1 {
		return "", fmt.Errorf("replay: api.conflict needs write, the ordinal of the status write of the pass it answers")
	}
	count := ev.Args.Count
	if count == 0 {
		count = 1
	}
	d.cluster.armConflict(ev.Args.Write, count)
	return fmt.Sprintf("write=%d count=%d", ev.Args.Write, count), nil
}

// applyServiceWriteFailed arms a refusal on the next per-revision Service
// writes: an ordinary error, or a create the apiserver refuses because the
// namespace is terminating.
func applyServiceWriteFailed(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	count := ev.Args.Count
	if count == 0 {
		count = 1
	}
	var err error
	switch ev.Variant {
	case "namespaceTerminating":
		status := apierrors.NewForbidden(schema.GroupResource{Resource: "services"}, d.opts.ServiceName,
			errors.New("unable to create new content in namespace because it is being terminated"))
		status.ErrStatus.Details.Causes = append(status.ErrStatus.Details.Causes, metav1.StatusCause{
			Type:    corev1.NamespaceTerminatingCause,
			Message: "namespace is being terminated",
			Field:   "metadata.namespace",
		})
		err = status
	case "ensure":
		err = apierrors.NewInternalError(errors.New("etcdserver: request timed out"))
	default:
		return "", fmt.Errorf("replay: api.serviceWriteFailed needs a variant: ensure or namespaceTerminating")
	}
	d.cluster.armRejection("create", "services", count, err, ev.Key())
	return fmt.Sprintf("verb=create resource=services count=%d", count), nil
}

// applySoakTimer moves the clock onto the earliest Sequential soak
// deadline: the previous member's recorded finish plus the group's soak,
// plus the slack.
func applySoakTimer(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	slack, err := requireSlack(ev)
	if err != nil {
		return "", err
	}
	live, err := d.liveService(ctx)
	if err != nil {
		return "", err
	}
	var deadline time.Time
	for _, g := range groupsByName(live.Status.RolloutCoordination) {
		soak := d.soakOf(g.Name)
		if soak <= 0 || g.PreviousComponent == "" {
			continue
		}
		// The engine measures the wait from the finish the group's entry
		// records for the previous member and, while it records none, from
		// that member's Ready transition, the instant its replica recorded
		// the roll done.
		var finished time.Time
		if g.MemberFinished != nil && g.MemberFinished.Component == g.PreviousComponent {
			finished = g.MemberFinished.At.Time
		} else {
			ir, err := d.replica(ctx, g.PreviousComponent)
			if err != nil {
				return "", err
			}
			finished = readyTransitionOf(ir)
		}
		if finished.IsZero() {
			continue
		}
		at := finished.Add(soak)
		if deadline.IsZero() || at.Before(deadline) {
			deadline = at
		}
	}
	return d.moveOnto("timer.soak", deadline, slack)
}

// readyTransitionOf is when a replica's Ready condition last turned True,
// zero while it is not True.
func readyTransitionOf(ir *v1beta1.InferenceReplica) time.Time {
	if ir == nil {
		return time.Time{}
	}
	for _, cond := range ir.Status.Conditions {
		if cond.Type == "Ready" && cond.Status == metav1.ConditionTrue {
			return cond.LastTransitionTime.Time
		}
	}
	return time.Time{}
}

// soakOf is the soak the resolved group named by a status group carries:
// the largest soak declared on any group but the last.
func (d *driver) soakOf(string) time.Duration {
	var soak time.Duration
	for i, g := range d.service.Groups {
		if i == len(d.service.Groups)-1 || g.Soak == nil {
			continue
		}
		if g.Soak.Duration > soak {
			soak = g.Soak.Duration
		}
	}
	return soak
}

// applyReadyTimeoutTimer moves the clock onto the earliest capacity-wait
// deadline of a canary unit, plus the slack.
func applyReadyTimeoutTimer(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	slack, err := requireSlack(ev)
	if err != nil {
		return "", err
	}
	_, _, timeout, err := d.cadences()
	if err != nil {
		return "", err
	}
	live, err := d.liveService(ctx)
	if err != nil {
		return "", err
	}
	var deadline time.Time
	for _, cs := range live.Status.Components {
		if cs.Canary == nil || cs.Canary.CapacityWaitSince == nil {
			continue
		}
		window := timeout
		for _, g := range d.service.Groups {
			if g.Canary != nil && g.Canary.ReadyTimeout != nil && g.Canary.ReadyTimeout.Duration > 0 {
				window = g.Canary.ReadyTimeout.Duration
			}
		}
		if window <= 0 {
			continue
		}
		at := cs.Canary.CapacityWaitSince.Time.Add(window)
		if deadline.IsZero() || at.Before(deadline) {
			deadline = at
		}
	}
	return d.moveOnto("timer.readyTimeout", deadline, slack)
}

func requireSlack(ev TimelineEvent) (time.Duration, error) {
	if ev.Args.Slack == "" {
		return 0, fmt.Errorf("replay: %s needs slack: how far past the deadline the observation lands", ev.Key())
	}
	return ParseDuration(ev.Key()+".slack", ev.Args.Slack)
}

// moveOnto moves the clock onto a deadline plus slack; a deadline already
// behind the clock moves nothing, and no deadline fails the run.
func (d *driver) moveOnto(field string, deadline time.Time, slack time.Duration) (string, error) {
	if deadline.IsZero() {
		return "", fmt.Errorf("replay: %s: nothing to resolve; no deadline is running", field)
	}
	target := deadline.Add(slack)
	if !target.After(d.clock.Now()) {
		return "deadline=" + d.norm.at(deadline) + " already-elapsed", nil
	}
	d.clock.SetTime(target)
	return "deadline=" + d.norm.at(deadline) + " now=" + d.norm.at(target), nil
}
