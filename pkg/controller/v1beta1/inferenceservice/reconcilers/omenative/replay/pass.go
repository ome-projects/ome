package replay

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
)

// runPass is one InferenceService reconcile in the controller's OMENative
// order: the pass copy from the cache, the run layer, the canary bind and
// the boundary flush, the replica projection, the canary dispatch, the
// coordination pass, the aggregate, and the final flush with its verb
// consumption. Engine errors end the pass as they end the controller's,
// and are recorded rather than returned.
func (d *driver) runPass(ctx context.Context) error {
	cached := d.cluster.cachedClient()
	live := client.Reader(d.cluster.cli)
	isvc := &v1beta1.InferenceService{}
	if err := cached.Get(ctx, d.isvcKey(), isvc); err != nil {
		return fmt.Errorf("replay: read the pass copy: %w", err)
	}
	view := "live"
	if d.cluster.stalePass {
		view = fmt.Sprintf("before-pass-%d", d.cluster.staleTick)
	}
	d.trace.line("pass cache=%s generation=%d", view, isvc.Generation)
	base := canary.NewBase(isvc)
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	passCopy := isvc.Status.DeepCopy()
	reconciler := &inferenceservice.InferenceServiceReconciler{Client: cached, APIReader: live, Recorder: d.recorder, Log: logr.Discard()}

	var pending ctrl.Result
	var policies rollout.Policies
	var consume []string
	if isvc.Spec.Rollout != nil || isvc.Status.Rollout != nil {
		d.stage = "run"
		out, err := rolloutrun.Reconcile(ctx, rolloutrun.Inputs{
			Client:         cached,
			Reader:         live,
			Recorder:       d.recorder,
			ISVC:           isvc,
			Now:            d.clock.Now(),
			FeatureEnabled: d.cfg.RolloutPolicyEnabled,
			BoundProviders: d.boundProviders(),
		})
		d.traceRun(isvc, out, err)
		if err != nil {
			return d.endPass(ctrl.Result{}, err)
		}
		pending.RequeueAfter = out.RequeueAfter
		policies = out.Policies
		if out.Opened {
			d.stage = "bind"
			for _, g := range rollout.CanaryGroups(isvc, policies) {
				if err := canary.BindRun(ctx, cached, isvc, g, out.Adopted); err != nil {
					return d.endPass(ctrl.Result{}, err)
				}
				d.traceBind(isvc, g)
			}
		}
		if out.StateChanged {
			if err := d.flush(ctx, reconciler, isvc, "boundary", nil, base); err != nil {
				return d.endPass(ctrl.Result{}, err)
			}
			if base.Stale() {
				return d.endPass(ctrl.Result{Requeue: true}, nil)
			}
		}
	}

	d.stage = "project"
	for _, c := range d.components() {
		if _, err := d.project(ctx, cached, live, isvc, c, d.partitionFor(isvc, policies, c)); err != nil {
			return d.endPass(ctrl.Result{}, err)
		}
	}

	modes := d.modes()
	canaryGroups := rollout.CanaryGroups(isvc, policies)
	if len(canaryGroups) > 0 {
		d.stage = "dispatch"
		requeue, parked, readyTimeout, err := d.cadences()
		if err != nil {
			return err
		}
		for _, g := range canaryGroups {
			out, err := canary.Dispatch(ctx, canary.DispatchDeps{
				ISVC:                 isvc,
				Client:               cached,
				Reader:               irstatus.NewReader(live, irstatus.Decoder{}),
				Recorder:             d.recorder,
				Now:                  d.clock.Now(),
				DefaultReadyTimeout:  readyTimeout,
				Requeue:              requeue,
				ParkedRequeue:        parked,
				ComponentRunnerPorts: d.runnerPorts(),
				Policies:             policies,
				Group:                g,
			})
			if err != nil {
				return d.endPass(ctrl.Result{}, err)
			}
			d.traceDispatch(isvc, g, out)
			if out.RequeueAfter > pending.RequeueAfter {
				pending.RequeueAfter = out.RequeueAfter
			}
			if out.Requeue {
				pending.Requeue = true
			}
			consume = append(consume, out.Consume...)
		}
	}

	d.stage = "coordination"
	if _, err := coordination.Reconcile(ctx, coordination.ReconcileInputs{
		ISVC:                         isvc,
		Policies:                     policies,
		Client:                       cached,
		Reader:                       irstatus.NewReader(live, irstatus.Decoder{}),
		Recorder:                     d.recorder,
		Now:                          d.clock.Now(),
		TrafficWeightDeadbandPercent: d.cfg.TrafficWeightDeadbandPercent,
		DefaultRatioTolerancePercent: d.cfg.RatioTolerancePercent,
		ComponentDeploymentModes:     modes,
		ComponentRunnerPorts:         d.runnerPorts(),
	}); err != nil {
		return d.endPass(ctrl.Result{}, err)
	}
	d.traceCoordination(passCopy, isvc)
	d.traceRevisions(isvc)

	d.stage = "aggregate"
	if err := irprojector.AggregateIRStatus(ctx, cached, live, isvc, modes); err != nil {
		return d.endPass(ctrl.Result{}, err)
	}

	if err := d.flush(ctx, reconciler, isvc, "final", consume, base); err != nil {
		if apierrors.IsConflict(err) {
			return d.endPass(ctrl.Result{Requeue: true}, nil)
		}
		return d.endPass(ctrl.Result{}, err)
	}
	if base.Stale() {
		pending.Requeue = true
	}
	return d.endPass(pending, nil)
}

// endPass closes the pass in the trace. An engine error is the pass's
// result, not the driver's failure.
func (d *driver) endPass(result ctrl.Result, err error) error {
	d.stage = ""
	parts := []string{}
	switch {
	case result.RequeueAfter > 0:
		parts = append(parts, "after:"+result.RequeueAfter.String())
	case result.Requeue: //nolint:staticcheck // the bare backoff is a distinct controller outcome
		parts = append(parts, "requeue")
	default:
		parts = append(parts, "none")
	}
	if err != nil {
		parts = append(parts, "err:"+d.norm.text(err.Error()))
	}
	d.trace.line("pass result=%s", joinFields(parts))
	return nil
}

// modes resolves every declared Component to OMENative; a scenario never
// moves a Component to another backend.
func (d *driver) modes() map[v1beta1.ComponentType]constants.DeploymentModeType {
	out := map[v1beta1.ComponentType]constants.DeploymentModeType{}
	for _, c := range d.components() {
		out[c] = constants.OMENative
	}
	return out
}

// cadences reads the configured canary cadences and the capacity timeout.
func (d *driver) cadences() (requeue, parked, readyTimeout time.Duration, err error) {
	if requeue, err = ParseDuration("config.requeue", d.cfg.Requeue); err != nil {
		return
	}
	if parked, err = ParseDuration("config.parkedRequeue", d.cfg.ParkedRequeue); err != nil {
		return
	}
	readyTimeout, err = ParseDuration("config.defaultReadyTimeout", d.cfg.DefaultReadyTimeout)
	return
}

// partitionFor is the rollout-control partition the controller projects for
// one Component: the canary step's when a canary plan is effective, the
// full plan-gate hold when a canary-kind group has no executable body.
func (d *driver) partitionFor(isvc *v1beta1.InferenceService, policies rollout.Policies, c v1beta1.ComponentType) *int32 {
	ext := d.componentExt(c)
	if len(rollout.CanaryGroups(isvc, policies)) > 0 {
		return canary.StepPartition(isvc, policies, c, ext)
	}
	return canary.PlanGateHoldPartition(isvc, policies, c, ext)
}

// flush enters the controller's status flush and records what it did: the
// short-circuit against the cached copy, the write, a rebase, a pin the
// live object kept, the group fields it copied over the live entry, and
// the canary record it kept live. consume names the verbs the pass applied.
func (d *driver) flush(ctx context.Context, r *inferenceservice.InferenceServiceReconciler, isvc *v1beta1.InferenceService, which string, consume []string, base *canary.Base) error {
	liveBefore, err := d.liveService(ctx)
	if err != nil {
		return err
	}
	cachedCopy := &v1beta1.InferenceService{}
	if err := d.cluster.cachedClient().Get(ctx, d.isvcKey(), cachedCopy); err != nil {
		return fmt.Errorf("replay: read the cached copy: %w", err)
	}
	desired := isvc.Status.DeepCopy()
	shortCircuit := equality.Semantic.DeepEqual(cachedCopy.Status, *desired)
	writesBefore := d.cluster.statusWrites
	d.stage = "flush:" + which
	ferr := r.FlushStatusThenConsume(ctx, isvc, constants.OMENative, consume, base)
	liveAfter, err := d.liveService(ctx)
	if err != nil {
		return err
	}
	written := !equality.Semantic.DeepEqual(liveBefore.Status, liveAfter.Status)
	parts := []string{"flush", "which=" + which}
	switch {
	case ferr != nil && apierrors.IsConflict(ferr):
		parts = append(parts, "result=conflict")
	case ferr != nil:
		parts = append(parts, "result=failed")
	case shortCircuit:
		parts = append(parts, "result=short-circuit")
	case written:
		parts = append(parts, "result=written")
	default:
		parts = append(parts, "result=no-change")
	}
	if attempts := d.cluster.statusWrites - writesBefore; attempts > 0 || ferr != nil {
		parts = append(parts, fmt.Sprintf("statusWrites=%d", attempts))
	}
	if ferr == nil && !shortCircuit {
		parts = append(parts, "run="+d.runPreserve(desired, &cachedCopy.Status, &liveBefore.Status, &liveAfter.Status))
		parts = append(parts, "groups="+d.groupOverwrite(desired, &cachedCopy.Status, &liveBefore.Status, &liveAfter.Status))
		if base.Stale() {
			parts = append(parts, "canary=kept-live")
		}
	}
	if len(consume) > 0 {
		parts = append(parts, "consume="+listValue(consume))
	}
	if ferr != nil {
		parts = append(parts, "err="+d.norm.text(ferr.Error()))
	}
	d.trace.line("%s", joinFields(parts))
	return ferr
}

// runClock is the run state's monotonic timestamp as the preserve rule
// reads it: the latest boundary it records.
func runClock(rs *v1beta1.RolloutStatus) time.Time {
	var t time.Time
	if rs == nil {
		return t
	}
	if rs.LastRun != nil && rs.LastRun.ClosedAt != nil && rs.LastRun.ClosedAt.Time.After(t) {
		t = rs.LastRun.ClosedAt.Time
	}
	if rs.ActiveRun != nil {
		if rs.ActiveRun.OpenedAt.Time.After(t) {
			t = rs.ActiveRun.OpenedAt.Time
		}
		if rs.ActiveRun.PinnedAt.Time.After(t) {
			t = rs.ActiveRun.PinnedAt.Time
		}
	}
	return t
}

// runPreserve reports what the flush did with the run pair: nothing when
// the pass's and the live pair agree, the ordinary write when the pass's
// copy was current, and under a lagging copy either the live pair kept by
// its newer clock or the pass's pair written over the live one.
func (d *driver) runPreserve(desired, cached, liveBefore, liveAfter *v1beta1.InferenceServiceStatus) string {
	if equality.Semantic.DeepEqual(runPair(desired.Rollout), runPair(liveBefore.Rollout)) {
		return "same"
	}
	lagging := !equality.Semantic.DeepEqual(runPair(cached.Rollout), runPair(liveBefore.Rollout))
	keptLive := equality.Semantic.DeepEqual(runPair(liveAfter.Rollout), runPair(liveBefore.Rollout))
	switch {
	case keptLive && runClock(liveBefore.Rollout).After(runClock(desired.Rollout)):
		return "live-kept(newer-clock)"
	case keptLive:
		return "live-kept"
	case lagging:
		return "desired(overwrote-live)"
	default:
		return "desired"
	}
}

func runPair(rs *v1beta1.RolloutStatus) *v1beta1.RolloutStatus {
	if rs == nil {
		return nil
	}
	return &v1beta1.RolloutStatus{ActiveRun: rs.ActiveRun, LastRun: rs.LastRun}
}

// groupOverwrite reports, per group, what the flush did with the live
// entry: nothing, the ordinary update of a current copy, or under a
// lagging copy the pass's entry written over a newer live one.
func (d *driver) groupOverwrite(desired, cached, liveBefore, liveAfter *v1beta1.InferenceServiceStatus) string {
	before := groupsByName(liveBefore.RolloutCoordination)
	after := groupsByName(liveAfter.RolloutCoordination)
	read := groupsByName(cached.RolloutCoordination)
	want := groupsByName(desired.RolloutCoordination)
	names := map[string]struct{}{}
	for n := range before {
		names[n] = struct{}{}
	}
	for n := range want {
		names[n] = struct{}{}
	}
	if len(names) == 0 {
		return "none"
	}
	ordered := make([]string, 0, len(names))
	for n := range names {
		ordered = append(ordered, n)
	}
	sort.Strings(ordered)
	parts := make([]string, 0, len(ordered))
	for _, n := range ordered {
		b, hadBefore := before[n]
		a, hasAfter := after[n]
		r := read[n]
		switch {
		case !hadBefore && hasAfter:
			parts = append(parts, n+":added")
		case hadBefore && !hasAfter:
			parts = append(parts, n+":removed")
		case equality.Semantic.DeepEqual(b, a):
			parts = append(parts, n+":unchanged")
		case !equality.Semantic.DeepEqual(r, b):
			parts = append(parts, n+":overwrote-newer-live{"+d.groupDiff(&b, &a)+"}")
		default:
			parts = append(parts, n+":updated{"+d.groupDiff(&b, &a)+"}")
		}
	}
	return strings.Join(parts, ",")
}

func groupsByName(rc *v1beta1.RolloutCoordinationStatus) map[string]v1beta1.RolloutCoordinationGroupStatus {
	out := map[string]v1beta1.RolloutCoordinationGroupStatus{}
	if rc == nil {
		return out
	}
	for _, g := range rc.Groups {
		out[g.Name] = g
	}
	return out
}

// groupDiff names the group fields the coordination machine reads back that a write
// moved.
func (d *driver) groupDiff(before, after *v1beta1.RolloutCoordinationGroupStatus) string {
	var parts []string
	if before.Phase != after.Phase {
		parts = append(parts, fmt.Sprintf("phase:%s→%s", orNil(string(before.Phase)), orNil(string(after.Phase))))
	}
	if before.CompositePhase != after.CompositePhase {
		parts = append(parts, fmt.Sprintf("composite:%s→%s", orNil(before.CompositePhase), orNil(after.CompositePhase)))
	}
	if before.CurrentComponent != after.CurrentComponent {
		parts = append(parts, fmt.Sprintf("current:%s→%s", orNil(string(before.CurrentComponent)), orNil(string(after.CurrentComponent))))
	}
	if !equality.Semantic.DeepEqual(before.LastTransitionTime, after.LastTransitionTime) {
		parts = append(parts, fmt.Sprintf("transition:%s→%s", d.norm.atMeta(before.LastTransitionTime), d.norm.atMeta(after.LastTransitionTime)))
	}
	if ab, aa := anchorOf(before), anchorOf(after); ab != aa {
		parts = append(parts, fmt.Sprintf("anchor:%s→%s", ab, aa))
	}
	if len(parts) == 0 {
		return "other-fields"
	}
	return strings.Join(parts, " ")
}

// anchorOf renders a group's ratio anchor.
func anchorOf(g *v1beta1.RolloutCoordinationGroupStatus) string {
	if g.ObservedRatio == nil || len(g.ObservedRatio.Original) == 0 {
		return "nil"
	}
	return componentCounts(g.ObservedRatio.Original)
}

func componentCounts(m map[v1beta1.ComponentType]int32) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", k, m[v1beta1.ComponentType(k)]))
	}
	return "{" + strings.Join(parts, " ") + "}"
}

// traceRun records the run layer's outcome, the persisted run pair and the
// plan conditions as the pass holds them in memory.
func (d *driver) traceRun(isvc *v1beta1.InferenceService, out rolloutrun.Outcome, err error) {
	outcome := "none"
	switch {
	case err != nil:
		outcome = "error"
	case out.Opened && out.Adopted:
		outcome = "adopted"
	case out.Opened:
		outcome = "opened"
	case out.Parked:
		outcome = "parked"
	case out.StateChanged:
		outcome = "stateChanged"
	}
	parts := []string{"run", "outcome=" + outcome}
	if out.StateChanged {
		parts = append(parts, "stateChanged=true")
	}
	if out.RequeueAfter > 0 {
		parts = append(parts, "requeueAfter="+out.RequeueAfter.String())
	}
	parts = append(parts, "policies="+d.policiesValue(out.Policies))
	if err != nil {
		parts = append(parts, "err="+d.norm.text(err.Error()))
	}
	d.trace.line("%s", joinFields(parts))
	d.traceRunState(isvc)
}

// traceRunState prints the run pair and the two plan conditions.
func (d *driver) traceRunState(isvc *v1beta1.InferenceService) {
	rs := isvc.Status.Rollout
	if rs == nil || rs.ActiveRun == nil {
		d.trace.line("run active=nil")
	} else {
		a := rs.ActiveRun
		d.trace.line("run active id=%s openedAt=%s pinnedAt=%s targets=%s digests=%s",
			a.RunID, d.norm.atValue(a.OpenedAt), d.norm.atValue(a.PinnedAt), targetsValue(a.TargetRevisions), digestsValue(a.Plan.Groups))
	}
	if rs != nil && rs.LastRun != nil {
		l := rs.LastRun
		d.trace.line("run last outcome=%s openedAt=%s closedAt=%s targets=%s",
			l.Outcome, d.norm.atMeta(l.OpenedAt), d.norm.atMeta(l.ClosedAt), targetsValue(l.TargetRevisions))
	}
	d.trace.line("run conditions %s %s",
		conditionValue(isvc, v1beta1.RolloutPlanReadyCondition), conditionValue(isvc, v1beta1.RolloutPlanDriftCondition))
}

func (d *driver) policiesValue(p rollout.Policies) string {
	if !p.Enabled {
		return "disabled"
	}
	names := make([]string, 0, len(p.ByName))
	for n := range p.ByName {
		names = append(names, n)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, n := range names {
		state := "valid"
		if p.Invalid[n] != nil {
			state = "invalid"
		}
		parts = append(parts, n+":"+state)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func targetsValue(targets []v1beta1.RolloutRunTarget) string {
	parts := make([]string, 0, len(targets))
	for _, t := range targets {
		parts = append(parts, fmt.Sprintf("%s=%s/stable=%s", t.Component, orNil(t.Revision), orNil(t.StableRevision)))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func digestsValue(groups []v1beta1.RolloutRunGroup) string {
	parts := make([]string, 0, len(groups))
	for _, g := range groups {
		parts = append(parts, string(g.Source)+":"+g.PortableDigest)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// conditionValue renders one duck condition as Type=Status(Reason).
func conditionValue(isvc *v1beta1.InferenceService, condType string) string {
	cond := isvc.Status.GetCondition(apis.ConditionType(condType))
	if cond == nil {
		return condType + "=absent"
	}
	return fmt.Sprintf("%s=%s(%s)", condType, cond.Status, cond.Reason)
}

// traceBind records the canary record the bind left on a group's unit.
func (d *driver) traceBind(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup) {
	primary := rollout.PrimaryOf(g)
	d.trace.line("bind unit=%s record=%s phase=%s", primary,
		d.canaryRecord(rollout.CanaryStatusFor(&isvc.Status, primary)), orNil(string(isvc.Status.Components[primary].RolloutPhase)))
}

// canaryRecord renders a unit's record in one fixed field order; an absent
// record is nil and an unset field is omitted.
func (d *driver) canaryRecord(cs *v1beta1.CanaryStatus) string {
	if cs == nil {
		return "nil"
	}
	parts := []string{
		"targetID=" + orNil(cs.TargetID),
		"canary=" + orNil(cs.CanaryRevisionHash),
		"stable=" + orNil(cs.StableRevisionHash),
		fmt.Sprintf("step=%d", cs.CurrentStep),
		fmt.Sprintf("weight=%d", cs.ObservedTrafficWeight),
		"entered=" + d.norm.atMeta(cs.StepEnteredTime),
	}
	if cs.PromotedThrough != "" {
		parts = append(parts, "promotedThrough="+cs.PromotedThrough)
	}
	if cs.PreStepHold {
		parts = append(parts, "preStepHold=true")
	}
	if cs.RolledBackRevisionHash != "" {
		parts = append(parts, "rolledBack="+cs.RolledBackRevisionHash)
	}
	if cs.CapacityWaitSince != nil {
		parts = append(parts, "capacityWaitSince="+d.norm.atMeta(cs.CapacityWaitSince))
	}
	if cs.Failed != nil {
		parts = append(parts, fmt.Sprintf("failed=%s@%s", cs.Failed.Reason, d.norm.atMeta(cs.Failed.Time)))
	}
	return "{" + joinFields(parts) + "}"
}

// traceDispatch records the executor's state, record and result for one
// unit, then every member's traffic targets.
func (d *driver) traceDispatch(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup, out canary.Outcome) {
	primary := rollout.PrimaryOf(g)
	cs := rollout.CanaryStatusFor(&isvc.Status, primary)
	steps := 0
	if g.Canary != nil {
		steps = len(g.Canary.Steps)
	}
	phase := isvc.Status.Components[primary].RolloutPhase
	parts := []string{
		"dispatch", "unit=" + string(primary),
		"state=" + string(rollout.StateOf(cs, phase, steps)),
		"phase=" + orNil(string(phase)),
		"record=" + d.canaryRecord(cs),
	}
	switch {
	case out.RequeueAfter > 0:
		parts = append(parts, "requeue=after:"+out.RequeueAfter.String())
	case out.Requeue:
		parts = append(parts, "requeue=rate-limited")
	default:
		parts = append(parts, "requeue=none")
	}
	if len(out.Consume) > 0 {
		parts = append(parts, "consume="+listValue(out.Consume))
	}
	d.trace.line("%s", joinFields(parts))
	for _, c := range g.Components {
		d.trace.line("traffic component=%s %s", c, trafficValue(isvc.Status.Components[c].Traffic))
	}
}

func trafficValue(targets []v1beta1.ComponentTrafficTarget) string {
	if len(targets) == 0 {
		return "nil"
	}
	parts := make([]string, 0, len(targets))
	for _, t := range targets {
		entry := fmt.Sprintf("%s:%d", t.RevisionName, t.Percent)
		if t.LatestRevision {
			entry += "(latest)"
		}
		if t.PairingProtocol != "" {
			entry += "(protocol=" + t.PairingProtocol + ")"
		}
		parts = append(parts, entry)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// traceCoordination records every group's entry before and after the
// pass merged its observation, and the coordination condition.
func (d *driver) traceCoordination(passCopy *v1beta1.InferenceServiceStatus, isvc *v1beta1.InferenceService) {
	before := groupsByName(passCopy.RolloutCoordination)
	if isvc.Status.RolloutCoordination != nil {
		for _, g := range isvc.Status.RolloutCoordination.Groups {
			prev := before[g.Name]
			d.trace.line("coordination group=%s before=%s after=%s", g.Name, d.groupEntry(&prev), d.groupEntry(&g))
		}
	}
	d.trace.line("coordination conditions %s", conditionValue(isvc, v1beta1.RolloutCoordinationReady))
}

// groupEntry renders the group fields the coordination machine reads back.
func (d *driver) groupEntry(g *v1beta1.RolloutCoordinationGroupStatus) string {
	if g == nil || g.Name == "" {
		return "nil"
	}
	return fmt.Sprintf("{phase=%s composite=%s current=%s previous=%s transition=%s anchor=%s}",
		orNil(string(g.Phase)), orNil(g.CompositePhase), orNil(string(g.CurrentComponent)), orNil(string(g.PreviousComponent)),
		d.norm.atMeta(g.LastTransitionTime), anchorOf(g))
}

// traceRevisions records every Component's rolled-out revision record and
// projected phase as the pass holds them before the final flush.
func (d *driver) traceRevisions(isvc *v1beta1.InferenceService) {
	for _, c := range d.components() {
		cs := isvc.Status.Components[c]
		d.trace.line("revisions component=%s ready=%s latest=%s previous=%s phase=%s", c,
			orNil(revisionHashOfService(cs.LatestReadyRevision)), orNil(revisionHashOfService(cs.LatestRolledoutRevision)),
			orNil(revisionHashOfService(cs.PreviousRolledoutRevision)), orNil(string(cs.RolloutPhase)))
	}
}

// revisionHashOfService is the revision hash a per-revision Service name
// carries.
func revisionHashOfService(name string) string {
	if name == "" {
		return ""
	}
	if i := strings.LastIndex(name, "-rev-"); i >= 0 {
		return name[i+len("-rev-"):]
	}
	return query.RevisionHashFromControllerRevisionName(name)
}

func listValue(values []string) string {
	if len(values) == 0 {
		return "nil"
	}
	sorted := append([]string(nil), values...)
	sort.Strings(sorted)
	return "[" + strings.Join(sorted, ",") + "]"
}
