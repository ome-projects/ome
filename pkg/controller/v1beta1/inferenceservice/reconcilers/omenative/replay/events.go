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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
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
	"write.modeSwitch":       "a Component never leaves OMENative in a replay",
	"read.pinStable":         "a read the run layer makes on its own",
	"read.rollbackStable":    "a read the canary dispatch makes on its own",
	"read.stableCR":          "a read the canary dispatch makes on its own",
	"read.rollbackCR":        "a read the replica controller makes, which the replay does not run",
	"read.statusTrails":      "a read every stage makes on its own; stage ir.stale",
	"cr.collision":           "a hash collision is minted by the replica controller, which the replay does not run",
	"scope.rotated":          "a replica is never deleted and re-projected in a replay",
	"ir.readError":           "the fake apiserver answers every replica read",
	"analysis.sample":        "the analysis sampler is not modelled",
	"analysis.unavailable":   "the analysis sampler is not modelled",
	"record.legacy":          "a persisted record that lacks an optional field is not modelled",
	"member.staged":          "a staged member needs the user partition flow, which is not modelled",
	"ir.missing":             "a replica is never removed in a replay; the projector would recreate it in the same pass",
	"timer.pauseElapsed":     "a timed soak is not modelled",
	"timer.drainElapsed":     "a drain window is not modelled",
	"pod.canaryCrashed":      "the crash reading is not modelled",
	"pod.canaryRestarted":    "the restart reading is not modelled",
	"pod.canaryDeleted":      "a canary pod on its way out is not modelled",
	"pod.replacementReady":   "the released floor's replacement is not modelled",
	"verb.pause":             "the pause is staged as spec.pause",
	"verb.unpause":           "the unpause is staged as spec.unpause",
	"spec.groupEdited":       "a group edit is staged as spec.planEdit",
	"policy.changed":         "a policy body edit is not modelled",
	"policy.resolved":        "a policy's return is not modelled",
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
		"pass.resync":              applyResync,
		"spec.revision":            applyTargetChange,
		"member.targetChanged":     applyTargetChange,
		"replica.targetBumped":     applyTargetChange,
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
		"member.failed":            applyRowPhase(v1beta1.OMENativeInstanceFailed),
		"member.recovered":         applyRowPhase(v1beta1.OMENativeInstanceReady),
		"member.dark":              applyDark,
		"capacity.reached":         applyCapacityReached,
		"capacity.dropped":         applyCapacityDropped,
		"pod.rejectedGone":         applyRejectedGone,
		"pod.lastStableRolled":     applyLastStableRolled,
		"stable.crMissing":         applyCollected,
		"cr.collected":             applyCollected,
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
		"plan.unresolved":          applyPolicyRemoved,
		"policy.unresolved":        applyPolicyRemoved,
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
	}
}

// eventArgKeys is the argument each event reads. An event absent from the
// map takes no arguments.
var eventArgKeys = map[string][]string{
	ClockAdvance:               {"duration"},
	"spec.revision":            {"component", "image"},
	"member.targetChanged":     {"component", "image"},
	"replica.targetBumped":     {"component", "image"},
	"member.rollback":          {"component"},
	"ir.converged":             {"component"},
	"ir.straggling":            {"component", "trailing"},
	"ir.stale":                 {"component"},
	"member.surgePodCreated":   {"component", "index"},
	"member.readyOnTarget":     {"component"},
	"member.oldPodsGone":       {"component"},
	"member.converged":         {"component"},
	"replica.currentPromoted":  {"component"},
	"replica.currentWithdrawn": {"component"},
	"member.capacityLost":      {"component"},
	"member.failed":            {"component", "index"},
	"member.recovered":         {"component", "index"},
	"member.dark":              {"component"},
	"capacity.reached":         {"component"},
	"capacity.dropped":         {"component", "index"},
	"pod.rejectedGone":         {"component"},
	"pod.lastStableRolled":     {"component"},
	"stable.crMissing":         {"component", "image"},
	"cr.collected":             {"component", "image"},
	"verb.repin":               {"value"},
	"plan.repin":               {"value"},
	"verb.promote":             {"component", "image"},
	"verb.promoteForce":        {"component", "image"},
	"verb.resume":              {"component", "image"},
	"plan.drift":               {"groups"},
	"spec.planEdit":            {"groups"},
	"plan.unresolved":          {"name"},
	"policy.unresolved":        {"name"},
	"spec.replicas":            {"component", "to"},
	"spec.partition":           {"component", "to"},
	"api.flushFailed":          {"count"},
	"api.conflict":             {"write", "count"},
	"api.serviceWriteFailed":   {"count"},
	"timer.soak":               {"slack"},
	"timer.readyTimeout":       {"slack"},
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

// applyTargetChange edits a Component's template image: the service spec
// moves, the projection targets the new revision, and the replica records
// it as its update revision.
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
	m.image, m.target = image, image
	generation, err := d.writeSpec(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("component=%s image=%s generation=%d", m.component, image, generation), nil
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

// applyConverged settles a member on its target: every row Ready on it
// with one Ready pod, and the current revision promoted.
func applyConverged(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	m.current = m.target
	m.pods = m.pods[:0]
	for i := range m.rows {
		m.rows[i].phase = v1beta1.OMENativeInstanceReady
		m.rows[i].running = m.target
		m.rows[i].target = ""
		m.rows[i].readySince = d.clock.Now()
		m.pods = append(m.pods, podState{index: m.rows[i].index, image: m.target, ready: true, serving: true})
	}
	return fmt.Sprintf("component=%s revision=%s", m.component, m.target), nil
}

// applyStraggling promotes a member's current revision while the named
// number of its lowest Instances still run the previous revision.
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
	m.pods = m.pods[:0]
	for i := range m.rows {
		running := m.target
		if i < int(ev.Args.Trailing) {
			running = previous
		}
		m.rows[i].phase = v1beta1.OMENativeInstanceReady
		m.rows[i].running = running
		m.rows[i].target = ""
		m.pods = append(m.pods, podState{index: m.rows[i].index, image: running, ready: true, serving: true})
	}
	return fmt.Sprintf("component=%s trailing=%d", m.component, ev.Args.Trailing), nil
}

// applyStale publishes the member's next status behind its generation.
func applyStale(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	m.stale = true
	return "component=" + string(m.component), nil
}

// applySurgePod creates an Instance's first pod on the target revision, not
// yet Ready, and marks the row updating toward it; without an index every
// Instance surges.
func applySurgePod(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
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
	for _, index := range indexes {
		row := m.row(index)
		row.phase = v1beta1.OMENativeInstanceUpdating
		row.target = m.target
		m.pods = append(m.pods, podState{index: index, ordinal: 1, image: m.target, ready: false})
	}
	return fmt.Sprintf("component=%s instances=%d revision=%s", m.component, len(indexes), m.target), nil
}

func (m *member) row(index int32) *rowState {
	for i := range m.rows {
		if m.rows[i].index == index {
			return &m.rows[i]
		}
	}
	return nil
}

// applyReadyOnTarget marks every target-revision pod of a member Ready.
func applyReadyOnTarget(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	n := 0
	for i := range m.pods {
		if m.pods[i].image == m.target {
			m.pods[i].ready, m.pods[i].serving = true, true
			n++
		}
	}
	if n == 0 {
		return "", fmt.Errorf("replay: member.readyOnTarget: %s has no pod on its target revision", m.component)
	}
	return fmt.Sprintf("component=%s pods=%d", m.component, n), nil
}

// applyOldPodsGone removes every pod of a member that is not on the target
// revision; each row stands Ready on the target pod that replaced its old
// one, so a row with no target pod fails the run.
func applyOldPodsGone(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	kept := m.pods[:0]
	removed := 0
	for _, p := range m.pods {
		if p.image == m.target {
			kept = append(kept, p)
			continue
		}
		removed++
	}
	m.pods = kept
	for i := range m.rows {
		if !m.hasPod(m.rows[i].index) {
			return "", fmt.Errorf("replay: %s: Instance %d of %s has no pod on the target revision; stage member.surgePodCreated for it first", ev.Key(), m.rows[i].index, m.component)
		}
		m.rows[i].running = m.target
		m.rows[i].target = ""
		m.rows[i].phase = v1beta1.OMENativeInstanceReady
		m.rows[i].readySince = d.clock.Now()
	}
	return fmt.Sprintf("component=%s removed=%d", m.component, removed), nil
}

func (m *member) hasPod(index int32) bool {
	for _, p := range m.pods {
		if p.index == index {
			return true
		}
	}
	return false
}

// applyPromote promotes the replica's current revision to its target.
func applyPromote(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	m.current = m.target
	return fmt.Sprintf("component=%s revision=%s", m.component, m.target), nil
}

// applyWithdraw withdraws the replica's current revision to empty.
func applyWithdraw(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	m.current = ""
	return "component=" + string(m.component), nil
}

// applyCapacityLost takes a member's pods of one revision out of readiness:
// the target's or the current's.
func applyCapacityLost(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	image := m.target
	if ev.Variant == "oldRevision" {
		image = m.current
	}
	n := 0
	for i := range m.pods {
		if m.pods[i].image == image {
			m.pods[i].ready, m.pods[i].serving = false, false
			n++
		}
	}
	for i := range m.rows {
		if m.rows[i].running == image && m.rows[i].phase == v1beta1.OMENativeInstanceReady {
			m.rows[i].phase = v1beta1.OMENativeInstancePending
		}
	}
	return fmt.Sprintf("component=%s revision=%s pods=%d", m.component, image, n), nil
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
		return fmt.Sprintf("component=%s index=%d phase=%s", m.component, *ev.Args.Index, phase), nil
	}
}

// applyDark takes every pod of a member out of rotation and demotes its
// rows, with no row reading Failed.
func applyDark(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	for i := range m.pods {
		m.pods[i].ready, m.pods[i].serving = false, false
	}
	for i := range m.rows {
		m.rows[i].phase = v1beta1.OMENativeInstancePending
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
	kept := m.pods[:0]
	for _, p := range m.pods {
		if p.index != index {
			kept = append(kept, p)
		}
	}
	m.pods = append(kept, podState{index: index, image: image, ready: true, serving: true})
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
// another object, so the Component's replica does not control it.
func (d *driver) disownRevision(ctx context.Context, m *member, image string, cr *appsv1.ControllerRevision) (string, error) {
	live := &appsv1.ControllerRevision{}
	if err := d.cluster.cli.Get(ctx, client.ObjectKeyFromObject(cr), live); err != nil {
		return "", fmt.Errorf("replay: read revision %s: %w", cr.Name, err)
	}
	for i := range live.OwnerReferences {
		if live.OwnerReferences[i].Controller != nil && *live.OwnerReferences[i].Controller {
			live.OwnerReferences[i].Name = live.OwnerReferences[i].Name + "-previous"
			live.OwnerReferences[i].UID = live.OwnerReferences[i].UID + "-previous"
		}
	}
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

// applyReplicas rewrites a Component's desired replica count. A new
// Instance appears as a Pending row with no pod yet; a removed one takes
// its row and pods with it.
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
	for int32(len(m.rows)) < to {
		m.rows = append(m.rows, rowState{index: int32(len(m.rows)), phase: v1beta1.OMENativeInstancePending, running: m.target})
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
// deadline: a group's lastTransitionTime plus its soak, plus the slack.
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
		if soak <= 0 || g.LastTransitionTime == nil {
			continue
		}
		at := g.LastTransitionTime.Time.Add(soak)
		if deadline.IsZero() || at.Before(deadline) {
			deadline = at
		}
	}
	return d.moveOnto("timer.soak", deadline, slack)
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
