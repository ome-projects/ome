// Package rolloutrun owns the rollout RUN lifecycle: one traversal of the
// effective rollout plan for one target revision set. At open it renders the
// effective plan (each group's inline progression or its referenced
// RolloutPolicy body), re-validates the composition, and pins it into
// status.rollout.activeRun in the same in-memory status the executors' step
// state lives in — one status flush carries both, so the pinned plan and the
// step counters can never be observed out of sync. Executors consume the
// pinned plan (rollout.Effective); spec and policy edits are inert
// mid-run and take effect at the next open, except that the coordination
// gates read a group's ratio rule, a rolling group's budgets and the
// group's presence from the spec as it is now, the members staying as
// recorded (rollout.EffectiveCoordination). Failure to resolve a plan PARKS
// the rollout (condition + held update gates) — it never falls back to the
// default progression, because silently removing a declared gate is the
// failure class this layer exists to prevent.
package rolloutrun

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
	"sigs.k8s.io/ome/pkg/validation"
)

const (
	// shortRequeue re-checks a run that is opening or waiting on a consistent
	// IR snapshot.
	shortRequeue = 10 * time.Second
	// parkRequeue backstops watch-driven recovery from a parked plan (the
	// policy watch re-enqueues consumers when the missing policy appears).
	parkRequeue = time.Minute
)

// Inputs is the controller-side input for one run-layer pass.
type Inputs struct {
	// Client serves the cached reads of every pass (the referenced policies
	// the effective view resolves through) and the verb-annotation
	// consumption writes.
	Client client.Client
	// Reader serves the reads a run DECISION depends on (IR revision pairs,
	// and the referenced policies on the pass that pins them): acting on a
	// cache-lagged target would pin the wrong plan.
	Reader   client.Reader
	Recorder record.EventRecorder
	ISVC     *v1beta1.InferenceService
	Now      time.Time
	// FeatureEnabled is the rollout-policy chart gate: refs resolve only when
	// the policy surface is installed. Pinning itself is never gated.
	FeatureEnabled bool
	// BoundProviders is the set of metric-provider names bound on this
	// cluster; a composed analysis naming an unbound provider parks at open
	// (a deterministic config error must not start a roll whose gate cannot
	// sample).
	BoundProviders map[string]struct{}
	// RouteCapabilities is the declared capability set of the route provider
	// the InferenceService's external route resolves to. A canary whose
	// traffic percentages need a capability the provider declares it lacks
	// is held at open, and the TrafficIntentReady condition reports the
	// provider's standing every pass; a provider with no declared set gates
	// nothing and is reported as unverified.
	RouteCapabilities validation.RouteProviderCapabilities
	// ComponentDeploymentModes is the deployment mode of every Component the
	// spec declares, as the controller resolved it for this pass. A recorded
	// member whose Component is missing from the map, or runs under another
	// mode, has left the run: admission takes such a Component out of every
	// rollout group in the same write and the cleanup deletes its replica,
	// so nothing of it can retarget or converge again. nil reads every
	// member as OMENative.
	ComponentDeploymentModes map[v1beta1.ComponentType]constants.DeploymentModeType
}

// Outcome reports the pass result. Parked means the effective plan could not
// be resolved: no run opened, update gates stay closed, old revision keeps
// serving. StateChanged means the run BOUNDARY moved this pass (opened,
// closed, or repinned): the caller must flush status before anything fallible
// runs, because the pin is load-bearing for the update gates and the
// partition stamp — a dropped in-memory pin reopens the run next pass and
// flaps the projected spec.
type Outcome struct {
	Parked       bool
	StateChanged bool
	RequeueAfter time.Duration
	// Opened reports that this pass created a fresh ActiveRun. Adopted
	// distinguishes status-loss recovery, whose existing executor progress must
	// be bound to the run without being reset.
	Opened  bool
	Adopted bool
	// Closed is the run this pass closed ahead of the run it opened, with the
	// plan it pinned: a unit the fresh run keeps where it stands reads its
	// recorded step against that plan. Nil when the pass opened over no run,
	// or over a run closed on an earlier pass.
	Closed *v1beta1.RolloutRun
	// Policies is the set of referenced RolloutPolicy objects this pass
	// observed: the set the executors' effective view resolves through, so
	// they read every reference the way the run opener did.
	Policies rollout.Policies
}

// composedPlan is one rendered effective plan: the pinnable groups plus their
// per-group digests (digests double as the repin CAS value).
type composedPlan struct {
	groups  []v1beta1.RolloutRunGroup
	digests []string
}

// Reconcile drives the run lifecycle for one ISVC, mutating isvc.Status
// in-memory; the controller's status flush persists it together with the
// executors' step state. The referenced policies are observed once here
// through the cache and handed back in the Outcome for the executors' view;
// a pass that pins a plan composes it from a live reading.
func Reconcile(ctx context.Context, in Inputs) (Outcome, error) {
	policies, err := observePolicies(ctx, in, in.Client)
	if err != nil {
		return Outcome{}, err
	}
	out, err := reconcile(ctx, in, policies)
	out.Policies = policies
	return out, err
}

func reconcile(ctx context.Context, in Inputs, policies rollout.Policies) (Outcome, error) {
	isvc := in.ISVC
	if in.Now.IsZero() {
		in.Now = time.Now()
	}
	now := metav1.Time{Time: in.Now}

	specGroups := 0
	if isvc.Spec.Rollout != nil {
		specGroups = len(isvc.Spec.Rollout.Groups)
	}

	if err := writeResolutionView(isvc, policies); err != nil {
		return Outcome{}, err
	}
	reportTrafficIntent(in, now)

	active := activeRun(isvc)
	// closedRun is the run a retarget closed on this pass. Its close event
	// is written once the pass knows what follows it: a fresh run, a plan
	// that does not compose, or nothing left to roll.
	var closedRun *v1beta1.RolloutRun

	// Nothing declared and nothing open: there is nothing to decide.
	if specGroups == 0 && active == nil {
		return Outcome{}, nil
	}

	left := departedMembers(in, active)
	targets, fresh, err := observeGroupTargets(ctx, in.Reader, isvc, active, left)
	if err != nil {
		return Outcome{}, err
	}

	stableOverrides := map[v1beta1.ComponentType]string{}
	if active != nil {
		if _, ok := isvc.Annotations[constants.RolloutRepinAnnotation]; ok {
			changed, err := handleRepin(ctx, in, active, now)
			if err != nil {
				return Outcome{}, err
			}
			return Outcome{StateChanged: changed, RequeueAfter: shortRequeue}, nil
		}
		// A run whose every recorded member left the OMENative mode has nothing
		// left to read: no replica of those members will ever report the
		// recorded revision. It closes on the spec alone, so a group that took
		// the members' place can open its own run on the next pass.
		if len(left) > 0 && noRecordedMemberRemains(active, left) {
			closeAndRest(in, active, v1beta1.RolloutRunSuperseded, now,
				"run %s closed: %s left the OMENative mode and every rollout group, and no other recorded member remains", active.RunID, membersLabel(departedList(active, left)))
			return Outcome{StateChanged: true}, nil
		}
		if !fresh {
			return Outcome{RequeueAfter: shortRequeue}, nil
		}
		if retargeted(isvc, active, targets) {
			for _, target := range active.TargetRevisions {
				// An unknown stable stays unknown only for a component that
				// moved. A component still on its pinned revision is on its
				// stable; carrying the unknown would make it look retargeted
				// and arm a ladder it has no work for.
				moved := targets[target.Component].target != "" && targets[target.Component].target != target.Revision
				if target.StableRevision != "" || moved {
					stableOverrides[target.Component] = target.StableRevision
				}
			}
			if specGroups == 0 {
				// The spec declares no rollout any more, so no run opens
				// toward the new target: the Components roll on their own.
				closeAndRest(in, active, v1beta1.RolloutRunSuperseded, now,
					"run %s superseded by a new target revision; spec.rollout is gone, so no run opens", active.RunID)
				return Outcome{StateChanged: true}, nil
			}
			closeRun(isvc, active, v1beta1.RolloutRunSuperseded, now)
			recordRunClosed(isvc, v1beta1.RolloutRunSuperseded)
			closedRun, active = active, nil
			// fall through to the open path with a fresh render.
		} else if outcome, done := closedOutcome(isvc, active, targets, left); done {
			closeAndRest(in, active, outcome, now, "run %s closed: %s", active.RunID, outcome)
			return Outcome{StateChanged: true}, nil
		} else {
			if specGroups == 0 {
				reportGroupsRemoved(in, active, now)
				return Outcome{RequeueAfter: shortRequeue}, nil
			}
			if err := updateDrift(in, policies, active, now); err != nil {
				return Outcome{}, err
			}
			setPlanReady(isvc, corev1.ConditionTrue, v1beta1.RolloutPlanReasonPinned,
				fmt.Sprintf("run %s pinned", active.RunID), now)
			return Outcome{}, nil
		}
	}

	if !fresh {
		return Outcome{RequeueAfter: shortRequeue}, nil
	}
	diverged := divergedMember(isvc, targets)
	midFlight := canaryMidFlight(isvc, policies, targets)
	if !diverged && !midFlight {
		settleNoRun(isvc, now)
		if closedRun == nil {
			return Outcome{}, nil
		}
		// The close is a run boundary like any other: it is flushed before
		// anything fallible runs, and nothing follows it.
		emitRetargetClose(in, closedRun, "nothing diverges, so no run opens")
		return Outcome{StateChanged: true}, nil
	}

	// The plan is pinned from a live reading of the referenced policies: a
	// cached body can lag an edit, and the pin is what executes.
	live, err := observePolicies(ctx, in, in.Reader)
	if err != nil {
		return Outcome{}, err
	}
	plan, parkReason, parkMsg, err := composePlan(in, live)
	if err != nil {
		return Outcome{}, err
	}
	if parkReason != "" {
		if closedRun != nil {
			emitRetargetClose(in, closedRun, "the plan for the new target does not compose, so no run opens")
		}
		if setPlanReady(isvc, corev1.ConditionFalse, parkReason, parkMsg, now) {
			recordParked(isvc, parkReason)
			emit(in.Recorder, isvc, corev1.EventTypeWarning, EventPlanParked,
				"rollout parked (%s): %s — the new revision is held, the previous revision keeps serving", parkReason, parkMsg)
		}
		return Outcome{Parked: true, StateChanged: closedRun != nil, RequeueAfter: parkRequeue}, nil
	}
	if closedRun != nil {
		emitRetargetClose(in, closedRun, "opening a fresh run")
	}
	adopting := closedRun == nil && midFlight
	openRun(isvc, plan, targets, stableOverrides, adopting, now)
	recordRunOpened(isvc, plan, adopting)
	setPlanReady(isvc, corev1.ConditionTrue, v1beta1.RolloutPlanReasonPinned,
		fmt.Sprintf("run %s pinned", isvc.Status.Rollout.ActiveRun.RunID), now)
	setPlanDrift(isvc, false, v1beta1.RolloutPlanDriftReasonInSync, "", now)
	if adopting {
		emit(in.Recorder, isvc, corev1.EventTypeNormal, EventPlanRepinned,
			"run %s adopted the in-flight rollout in place (plan pinned at the current step; no restart, no traffic movement)", isvc.Status.Rollout.ActiveRun.RunID)
	} else {
		emit(in.Recorder, isvc, corev1.EventTypeNormal, EventRunOpened,
			"run %s opened (%s)", isvc.Status.Rollout.ActiveRun.RunID, combinedPlanDigest(plan.digests))
	}
	return Outcome{StateChanged: true, RequeueAfter: shortRequeue, Opened: true, Adopted: adopting, Closed: closedRun}, nil
}

// emitRetargetClose records the close of a run whose target moved on, with
// what the pass decided follows it.
func emitRetargetClose(in Inputs, closed *v1beta1.RolloutRun, then string) {
	emit(in.Recorder, in.ISVC, corev1.EventTypeNormal, EventRunClosed,
		"run %s superseded by a new target revision; %s", closed.RunID, then)
}

// closeAndRest closes the run with its outcome and records that nothing is
// open any more: the plan conditions read NoActiveRun and InSync.
func closeAndRest(in Inputs, active *v1beta1.RolloutRun, outcome v1beta1.RolloutRunOutcome, now metav1.Time, messageFmt string, args ...interface{}) {
	isvc := in.ISVC
	closeRun(isvc, active, outcome, now)
	recordRunClosed(isvc, outcome)
	recordDrift(isvc, false)
	emit(in.Recorder, isvc, corev1.EventTypeNormal, EventRunClosed, messageFmt, args...)
	setPlanReady(isvc, corev1.ConditionTrue, v1beta1.RolloutPlanReasonNoRun,
		fmt.Sprintf("last run closed %s", outcome), now)
	setPlanDrift(isvc, false, v1beta1.RolloutPlanDriftReasonInSync, "", now)
}

// reportGroupsRemoved records the drift of a run whose spec declares no
// rollout group any more. The run keeps the revisions it recorded and
// closes when every recorded member reaches them, rolls back or is
// superseded by a new target; a canary keeps its recorded steps, a rolling
// or blue-green group stops pacing its members at once while the pairing
// gates keep its pairable minimum; the rollback verb ends the run sooner
// and the repin verb closes it at once.
func reportGroupsRemoved(in Inputs, active *v1beta1.RolloutRun, now metav1.Time) {
	isvc := in.ISVC
	recordDrift(isvc, true)
	if setPlanDrift(isvc, true, v1beta1.RolloutPlanDriftReasonSpecNewerThanRun,
		"spec.rollout removed mid-run; the run keeps its recorded revisions and closes when every member reaches them or rolls back; a canary keeps its recorded steps, a rolling or blue-green group stops pacing at once (a pairing-protocol change in flight keeps its pairable minimum) — end the run sooner with ome.io/rollout-rollback, or close it at once with ome.io/rollout-repin", now) {
		emit(in.Recorder, isvc, corev1.EventTypeWarning, EventGroupRemoved,
			"spec.rollout was removed while run %s is open; the run keeps its recorded revisions and closes when every member reaches them or rolls back; a canary keeps its recorded steps, a rolling or blue-green group stops pacing at once and a pairing-protocol change in flight keeps its pairable minimum (ome.io/rollout-rollback ends the run sooner, ome.io/rollout-repin closes it at once)", active.RunID)
	}
	setPlanReady(isvc, corev1.ConditionTrue, v1beta1.RolloutPlanReasonPinned,
		fmt.Sprintf("run %s pinned", active.RunID), now)
}

// settleNoRun records the idle state: no run is pinned and none is needed.
func settleNoRun(isvc *v1beta1.InferenceService, now metav1.Time) {
	setPlanReady(isvc, corev1.ConditionTrue, v1beta1.RolloutPlanReasonNoRun, "no rollout in progress", now)
	setPlanDrift(isvc, false, v1beta1.RolloutPlanDriftReasonInSync, "", now)
}

func activeRun(isvc *v1beta1.InferenceService) *v1beta1.RolloutRun {
	if isvc.Status.Rollout == nil {
		return nil
	}
	return isvc.Status.Rollout.ActiveRun
}

// composePlan renders the effective plan from the live spec: per group, the
// resolver's body and provenance, then composed-body re-validation and
// provider resolution. A non-empty parkReason means the plan is unresolvable
// — the caller parks instead of opening. A spec with no rollout block
// renders the empty plan.
func composePlan(in Inputs, policies rollout.Policies) (composedPlan, string, string, error) {
	isvc := in.ISVC
	var plan composedPlan
	if isvc.Spec.Rollout == nil {
		return plan, "", "", nil
	}
	inflated := derivedProvenance(isvc)
	for gi := range isvc.Spec.Rollout.Groups {
		resolved, rerr := rollout.ResolveGroup(gi, &isvc.Spec.Rollout.Groups[gi], policies)
		if rerr != nil {
			var unresolved *rollout.Unresolved
			if errors.As(rerr, &unresolved) {
				return plan, unresolved.Reason, unresolved.Message, nil
			}
			return plan, "", "", rerr
		}
		composed := resolved.Group
		// Composed-body re-validation, the belt against skew: the body passed
		// its own admission, but this instance of it is what will execute.
		if composed.Canary != nil {
			if verr := validation.ValidateCanaryPlan(fmt.Sprintf("groups[%d].canary (composed)", gi), composed.Canary); verr != nil {
				return plan, v1beta1.RolloutPlanReasonPlanInvalid, verr.Error(), nil
			}
			// A canary on the externally routed unit programs its traffic
			// percentages onto the route. A provider declared unable to split
			// a route between revisions would run the steps with those
			// percentages dropped, so the run does not open.
			required := validation.RouteRequirementsOfGroup(fmt.Sprintf("groups[%d]", gi), &composed, validation.RouteEntrypoint(&isvc.Spec))
			if unsupported := validation.UnsupportedRouteCapabilities(required, in.RouteCapabilities); len(unsupported) > 0 {
				return plan, v1beta1.RolloutPlanReasonRouteProviderUnsupported,
					validation.RouteCapabilityUnsupportedError(unsupported[0], in.RouteCapabilities).Error(), nil
			}
			if pr := composed.Canary.Prometheus; pr != nil && pr.ProviderRef != nil {
				if _, bound := in.BoundProviders[pr.ProviderRef.Name]; !bound {
					return plan, v1beta1.RolloutPlanReasonProviderUnbound,
						fmt.Sprintf("groups[%d]: metric provider %q is not bound in this cluster's metricProviders configuration", gi, pr.ProviderRef.Name), nil
				}
			}
		}

		digest, derr := rolloutpolicy.ProgressionDigest(&composed)
		if derr != nil {
			return plan, "", "", derr
		}
		source, provRef := resolved.Source, resolved.PolicyRef
		if p, ok := inflated[gi]; ok && source == v1beta1.RolloutPlanSourceInline {
			// A derived ISVC's inline group inflated from a policy at derive
			// time: report the policy identity a locally-resolved ref would.
			source = v1beta1.RolloutPlanSourcePolicy
			if p.PolicyRef != nil {
				// The provenance annotation encodes only name@digest, so the
				// parsed ref's Progression is zero — which the status
				// subresource's enum rejects, failing every status write for
				// the life of the run. The composed body is authoritative.
				provRef = p.PolicyRef.DeepCopy()
				provRef.Progression = composed.DeclaredProgression()
			}
		}
		plan.groups = append(plan.groups, v1beta1.RolloutRunGroup{
			Source:           source,
			PolicyRef:        provRef,
			PolicyGeneration: resolved.PolicyGeneration,
			PortableDigest:   digest,
			Group:            composed,
		})
		plan.digests = append(plan.digests, digest)
	}
	return plan, "", "", nil
}

// openRun pins the plan. Engine step state is deliberately untouched: the
// canary and coordination engines own their counters and reset them under
// their own rules (which is what makes adopt-in-place a no-op for a roll
// already in flight).
//
// A Component whose observed target is the revision its own canary already
// rejected is pinned at its stable revision instead. The run is opened for the
// whole InferenceService, so any Component's retarget drags every other
// Component's target into the pin; re-presenting a rejected revision would
// re-arm a ladder already known to fail, and its rollback would then close the
// shared run and discard the progress of the group that opened it. A unit
// adopted mid-revert is the exception: it is pinned at the rejected revision,
// the target the lost run held, so the revert completes under the run and
// closes it RolledBack.
func openRun(isvc *v1beta1.InferenceService, plan composedPlan, targets map[v1beta1.ComponentType]targetPair, stableOverrides map[v1beta1.ComponentType]string, adopting bool, now metav1.Time) {
	rejected := stickyRejectHashes(isvc, targets)
	var pinned []v1beta1.RolloutRunTarget
	seen := map[v1beta1.ComponentType]bool{}
	for i := range plan.groups {
		g := &plan.groups[i].Group
		// Adoption is a unit's: only the Components of the unit whose ladder
		// the run adopts are mid-roll, so only they resolve their stable from
		// the records; every other unit resolves as in a fresh run.
		unitAdopting := adopting && unitLadderInFlight(isvc, g)
		// Adoption does not reset the unit, so the pin must be the target the
		// executor's hold is keyed on; the run's close then recognizes the
		// rollback as its own.
		reverting := unitAdopting && revertInFlight(isvc, g)
		for _, comp := range g.Components {
			if seen[comp] {
				continue
			}
			seen[comp] = true
			t := targets[comp]
			rev := t.target
			if rev == "" {
				rev = t.current
			}
			stable, carried := stableOverrides[comp]
			if !carried {
				stable = componentStableRevision(isvc, comp, rev, t, unitAdopting)
			}
			if hold := rejected[comp]; hold != "" && rev == hold && stable != "" && !reverting {
				rev = stable
			}
			pinned = append(pinned, v1beta1.RolloutRunTarget{
				Component:      comp,
				Revision:       rev,
				StableRevision: stable,
			})
		}
	}
	if isvc.Status.Rollout == nil {
		isvc.Status.Rollout = &v1beta1.RolloutStatus{}
	}
	isvc.Status.Rollout.ActiveRun = &v1beta1.RolloutRun{
		RunID:           runID(isvc, pinned, plan.digests, now.UTC().Format(time.RFC3339), closedRunID(isvc)),
		OpenedAt:        now,
		PinnedAt:        now,
		TargetRevisions: pinned,
		Plan:            v1beta1.RolloutRunPlan{Groups: plan.groups},
	}
}

// closeRun drops the pinned plan and keeps the bounded record. Engine state
// (done sentinel, sticky reject) survives — it is the engines', not the
// run's.
func closeRun(isvc *v1beta1.InferenceService, active *v1beta1.RolloutRun, outcome v1beta1.RolloutRunOutcome, now metav1.Time) {
	provenance := make([]v1beta1.RolloutRunProvenance, 0, len(active.Plan.Groups))
	for i := range active.Plan.Groups {
		g := &active.Plan.Groups[i]
		provenance = append(provenance, v1beta1.RolloutRunProvenance{
			Source:         g.Source,
			PolicyRef:      g.PolicyRef,
			PortableDigest: g.PortableDigest,
		})
	}
	opened := active.OpenedAt
	isvc.Status.Rollout.LastRun = &v1beta1.RolloutRunRecord{
		Outcome:         outcome,
		RunID:           active.RunID,
		OpenedAt:        &opened,
		ClosedAt:        &now,
		TargetRevisions: append([]v1beta1.RolloutRunTarget(nil), active.TargetRevisions...),
		Groups:          provenance,
	}
	isvc.Status.Rollout.ActiveRun = nil
}

// componentStableRevision resolves the revision a Component falls back to in
// the run: the InferenceReplica answers wherever it can, the status records
// only where it cannot. A current revision off the pin is the stable. A
// Component settled on the pin, its current revision the pinned one with no
// Instance trailing, is where it stands: the run pins it at that revision as
// both target and stable, so it sits the run out, and a record naming an
// earlier revision (one no Instance may run any more) is never promoted to
// its rollback target. The records answer for the Components of the unit
// whose ladder the run adopts (adopting), mid-roll by definition, and for a
// Component whose agreeing pair still trails Instances (a mid-roll revert): the unit's
// canary record names the stable its ladder shifts traffic from, and
// LatestRolledoutRevision names the revision that last owned the traffic,
// kept by coordination for its Components and by the canary executor for
// its members, so a secondary with no record of its own still resolves.
func componentStableRevision(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, pinnedRevision string, target targetPair, adopting bool) string {
	if target.current != "" && target.current != pinnedRevision {
		return target.current
	}
	if !adopting && target.current != "" && !target.straggling() {
		return target.current
	}
	if cs := rollout.CanaryStatusFor(&isvc.Status, comp); comp == rollout.CanaryUnit(comp) && cs != nil && cs.StableRevisionHash != "" {
		return cs.StableRevisionHash
	}
	if status, ok := isvc.Status.Components[comp]; ok {
		if hash := query.RevisionFromName(status.LatestRolledoutRevision).Hash(); hash != "" {
			return hash
		}
	}
	if !adopting {
		return target.current
	}
	return ""
}

// retargeted reports whether any pinned member's observed target moved off
// the run's pinned revision (the canary sticky reject is a hold, not a new
// target).
func retargeted(isvc *v1beta1.InferenceService, active *v1beta1.RolloutRun, targets map[v1beta1.ComponentType]targetPair) bool {
	rejected := stickyRejectHashes(isvc, targets)
	pinnedFor := map[v1beta1.ComponentType]string{}
	for _, t := range active.TargetRevisions {
		pinnedFor[t.Component] = t.Revision
	}
	for i := range active.Plan.Groups {
		for _, comp := range active.Plan.Groups[i].Group.Components {
			obs := targets[comp]
			pinnedRev := pinnedFor[comp]
			if obs.target == "" || pinnedRev == "" {
				continue
			}
			if obs.target != pinnedRev && obs.target != rejected[comp] {
				return true
			}
		}
	}
	return false
}

// closedOutcome decides whether the run reached a terminal state: RolledBack
// (the canary's sticky hold settled) or Completed — every pinned group's
// members converged to the pinned target (a canary unit also reports Stable),
// or every member short of the target rests at its own partition in a group
// that rests Staged (a lifecycle partition deliberately holds mixed
// revisions; keying completion on full convergence alone would keep such a
// run open forever). A member short of the target for another reason (an
// Instance still being created, a retry record keeping its current revision
// behind) keeps the run open: the open reads that member's divergence, so a
// close over it would open a run again on the next pass. A member in left is
// not read: it has left the OMENative mode, so the run is decided on the
// members that remain.
func closedOutcome(isvc *v1beta1.InferenceService, active *v1beta1.RolloutRun, targets map[v1beta1.ComponentType]targetPair, left map[v1beta1.ComponentType]bool) (v1beta1.RolloutRunOutcome, bool) {
	pinnedFor := map[v1beta1.ComponentType]string{}
	for _, t := range active.TargetRevisions {
		if left[t.Component] {
			continue
		}
		pinnedFor[t.Component] = t.Revision
	}
	pinnedSpec := active.Plan.AsRolloutSpec(isvc.Spec.Rollout)
	resolved := coordination.ResolveGroups(pinnedSpec, coordination.GroupDefaults{})

	for i := range active.Plan.Groups {
		g := &active.Plan.Groups[i].Group
		if g.Canary != nil {
			primary := primaryOfGroup(g)
			cs := rollout.CanaryStatusFor(&isvc.Status, primary)
			if cs == nil {
				// A unit with no stable revision to shift from never arms; it
				// is done once every member rests on the pinned revision.
				for _, comp := range g.Components {
					pinnedRev := pinnedFor[comp]
					if pinnedRev == "" || left[comp] {
						continue
					}
					obs := targets[comp]
					if obs.current != pinnedRev || obs.target != pinnedRev || (obs.replicas > 0 && obs.updated < obs.replicas) {
						return "", false
					}
				}
				continue
			}
			if cs.RolledBackRevisionHash != "" {
				switch decideRejectedUnit(isvc, g, cs, closeReading{recorded: pinnedFor, observed: targets, left: left}) {
				case rejectedUnitHoldsRunOpen:
					return "", false
				case rejectedUnitClosesRolledBack:
					return v1beta1.RolloutRunRolledBack, true
				case rejectedUnitInert:
				}
				continue
			}
			if stepShortOfEnd(isvc, primary, cs, g.Canary.Steps) {
				return "", false
			}
			// The done sentinel releases the last held instance; the unit
			// reads Stable once that instance has rolled. The run stays
			// pinned until then, because a plan resolved only through the pin
			// must still drive the unit to report it.
			if primaryPhase(isvc, g) != v1beta1.RolloutPhaseStable {
				return "", false
			}
			// The Stable read can lead the replica by a pass: Instances may still
			// trail, and current is promoted only once every Instance is Ready on
			// the pin. A close before the pair agrees would reopen at once.
			for _, comp := range g.Components {
				if left[comp] {
					continue
				}
				if trailsRecordedRevision(targets[comp], pinnedFor[comp]) {
					return "", false
				}
			}
			continue
		}
		for _, comp := range g.Components {
			pinnedRev := pinnedFor[comp]
			if pinnedRev == "" || left[comp] {
				continue
			}
			obs := targets[comp]
			if obs.current == pinnedRev && obs.target == pinnedRev && (obs.replicas == 0 || obs.updated >= obs.replicas) {
				continue
			}
			if !obs.stagedByPartition() || !groupStaged(isvc, resolved, comp) {
				return "", false
			}
		}
	}
	return v1beta1.RolloutRunCompleted, true
}

// rejectedUnitDecision is what a canary unit holding a rejection says about
// the close of the run.
type rejectedUnitDecision int

const (
	// rejectedUnitHoldsRunOpen: the revert is still draining, or an Instance
	// still trails the revision the run recorded for a member of the unit.
	rejectedUnitHoldsRunOpen rejectedUnitDecision = iota
	// rejectedUnitClosesRolledBack: the revert of the revision this run
	// recorded for the unit settled.
	rejectedUnitClosesRolledBack
	// rejectedUnitInert: the unit holds the rejection of a revision this run
	// does not name and rests on its stable revision; the other groups decide.
	rejectedUnitInert
)

// closeReading is what the close reads per member: the revision the run
// recorded for it, its replica's observed revision pair with its counters,
// and whether it left the OMENative mode.
type closeReading struct {
	recorded map[v1beta1.ComponentType]string
	observed map[v1beta1.ComponentType]targetPair
	left     map[v1beta1.ComponentType]bool
}

// decideRejectedUnit reads a canary unit holding a rejection for the close.
// Only a rollback of the revision this run recorded for the unit is this
// run's outcome: the run closes RolledBack once the revert settles. A unit
// holding the rejection of a revision this run does not name was recorded
// at its stable revision, and its rejection never closes the run: that
// would end every other group's rollout for a revision this run never
// offered. It still keeps the run open while its revert drains, and while
// an Instance trails the recorded revision after the operator set the
// template back to the stable: the roll back needs a recorded run, and a
// close over it would be followed by an adoption on the next pass, every
// pass until the Instances land.
func decideRejectedUnit(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup, cs *v1beta1.CanaryStatus, reading closeReading) rejectedUnitDecision {
	primary := primaryOfGroup(g)
	state := rollout.StateOf(cs, primaryPhase(isvc, g), len(g.Canary.Steps))
	if reading.recorded[primary] == cs.RolledBackRevisionHash {
		if state == rollout.CanaryStateRolledBack {
			return rejectedUnitClosesRolledBack
		}
		return rejectedUnitHoldsRunOpen
	}
	if state == rollout.CanaryStateRollingBack || reading.unitTrails(g) {
		return rejectedUnitHoldsRunOpen
	}
	return rejectedUnitInert
}

// unitTrails reports a member of the group whose replica's update revision
// is the revision the run recorded for it while an Instance still runs
// another. A member whose update revision is not the recorded one tells
// nothing about it: a settled rejection keeps the rejected revision as the
// replica's target with every Instance on the stable revision.
func (r closeReading) unitTrails(g *v1beta1.RolloutGroup) bool {
	for _, comp := range g.Components {
		recorded := r.recorded[comp]
		if recorded == "" || r.left[comp] {
			continue
		}
		obs := r.observed[comp]
		if obs.target == recorded && trailsRecordedRevision(obs, recorded) {
			return true
		}
	}
	return false
}

// trailsRecordedRevision reports a member whose Instances have not all
// reached the revision the run recorded for it: some still run another
// revision, or the replica has not promoted its current revision to it,
// which it does only once every Instance is Ready there.
func trailsRecordedRevision(obs targetPair, recorded string) bool {
	return obs.straggling() || (recorded != "" && obs.current != recorded)
}

// groupStaged reports whether the coordination status group owning comp
// rests in the Staged phase. Group identity goes through ResolveGroups on
// the pinned plan — the exact function the engine names its status groups
// with, so collapse semantics cannot skew the lookup.
func groupStaged(isvc *v1beta1.InferenceService, resolved []coordination.ResolvedGroup, comp v1beta1.ComponentType) bool {
	group, ok := coordination.MembershipFor(resolved, comp)
	if !ok || isvc.Status.RolloutCoordination == nil {
		return false
	}
	for i := range isvc.Status.RolloutCoordination.Groups {
		sg := &isvc.Status.RolloutCoordination.Groups[i]
		if sg.Name == group.Name {
			return sg.Phase == v1beta1.CoordinationPhaseStaged
		}
	}
	return false
}

// primaryPhase is the rollout phase of the canary group's primary Component
// (rollout.PrimaryOf — the step machine's Component).
func primaryPhase(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup) v1beta1.RolloutPhase {
	if p := rollout.PrimaryOf(g); p != "" {
		return isvc.Status.Components[p].RolloutPhase
	}
	return ""
}

// updateDrift compares each spec group's live render digest and members
// against the recorded group's and sets RolloutPlanDrift. A group whose
// members changed under an unchanged progression body is drift too: the
// recorded plan still governs the members it names, and a plan reread
// replaces it with the members the spec names now. A group's ratio rule and
// a rolling group's budgets are not plan content: the coordination gates
// read them from the spec, so the live digest is compared with the digest
// of the group as it executes and an edit of them is no drift, while a
// change of the progression kind or of a canary body is. A canary group
// disappearing from the spec gets a pointed event: only the rollback and
// repin verbs abort a recorded run.
func updateDrift(in Inputs, policies rollout.Policies, active *v1beta1.RolloutRun, now metav1.Time) error {
	isvc := in.ISVC
	specGroups := isvc.Spec.Rollout.Groups
	recorded := active.Plan.Groups
	executing := rollout.EffectiveCoordination(isvc, policies)

	if len(specGroups) != len(recorded) {
		canaryGone := pinnedHasCanary(recorded) && !specHasCanaryKind(isvc.Spec.Rollout)
		msg := "spec.rollout group shape changed mid-run; the run keeps its recorded revisions, a rolling or blue-green group that shares no member with any spec group stops pacing at once (a pairing-protocol change in flight keeps its pairable minimum), and the rest of the recorded plan continues until the next run or a repin"
		if canaryGone {
			msg = "the canary group was removed mid-run; the recorded canary continues — abort with ome.io/rollout-rollback, or repin with ome.io/rollout-repin"
		}
		recordDrift(isvc, true)
		if setPlanDrift(isvc, true, v1beta1.RolloutPlanDriftReasonSpecNewerThanRun, msg, now) && canaryGone {
			emit(in.Recorder, isvc, corev1.EventTypeWarning, EventGroupRemoved,
				"canary group removed while run %s is recorded; deleting the group does not abort a recorded run — use ome.io/rollout-rollback or ome.io/rollout-repin", active.RunID)
		}
		return nil
	}

	for gi := range specGroups {
		if !sameMembers(specGroups[gi].Components, recorded[gi].Group.Components) {
			setPlanDrift(isvc, true, v1beta1.RolloutPlanDriftReasonSpecNewerThanRun,
				fmt.Sprintf("groups[%d]: members changed from %s to %s; the edit applies at the next run (or via ome.io/rollout-repin)",
					gi, membersLabel(recorded[gi].Group.Components), membersLabel(specGroups[gi].Components)), now)
			recordDrift(isvc, true)
			return nil
		}
		live, err := liveSourceDigest(&specGroups[gi], policies)
		if err != nil {
			return err
		}
		executingDigest := recorded[gi].PortableDigest
		if gi < len(executing.Groups) {
			if digest, derr := rolloutpolicy.ProgressionDigest(&executing.Groups[gi]); derr == nil && digest != "" {
				executingDigest = digest
			}
		}
		if live == executingDigest {
			continue
		}
		reason := v1beta1.RolloutPlanDriftReasonSpecNewerThanRun
		if rolloutpolicy.GroupSource(&specGroups[gi]) == v1beta1.RolloutPlanSourcePolicy {
			reason = v1beta1.RolloutPlanDriftReasonPolicyNewerThanRun
		}
		liveLabel := live
		if liveLabel == "" {
			liveLabel = "(unresolvable)"
		}
		setPlanDrift(isvc, true, reason,
			fmt.Sprintf("groups[%d]: live render %s differs from recorded %s; the edit applies at the next run (or via ome.io/rollout-repin)",
				gi, liveLabel, executingDigest), now)
		recordDrift(isvc, true)
		return nil
	}
	setPlanDrift(isvc, false, v1beta1.RolloutPlanDriftReasonInSync, "", now)
	recordDrift(isvc, false)
	return nil
}

func pinnedHasCanary(groups []v1beta1.RolloutRunGroup) bool {
	for i := range groups {
		if groups[i].Group.Canary != nil {
			return true
		}
	}
	return false
}

func specHasCanaryKind(spec *v1beta1.RolloutSpec) bool {
	if spec == nil {
		return false
	}
	for i := range spec.Groups {
		if groupKind(&spec.Groups[i]) == v1beta1.RolloutProgressionCanary {
			return true
		}
	}
	return false
}

// handleRepin applies the one-shot repin verb: re-render from the current
// source, CAS-check the annotation's expected digest against the fresh
// render (guarding against a racing edit), replace the pinned plan
// preserving run identity and progress, and clamp the canary so a repin can
// only hold or tighten — a clamped step whose traffic exceeds the currently
// programmed weight enters the pre-step hold instead of raising exposure.
// The annotation is consumed in every branch: a rejected repin must not
// retry forever.
func handleRepin(ctx context.Context, in Inputs, active *v1beta1.RolloutRun, now metav1.Time) (bool, error) {
	isvc := in.ISVC
	want := isvc.Annotations[constants.RolloutRepinAnnotation]
	oldDigest := combinedPlanDigest(pinnedDigests(active))

	// A repin replaces the pin, so it renders from a live reading too.
	live, err := observePolicies(ctx, in, in.Reader)
	if err != nil {
		return false, err
	}
	plan, parkReason, parkMsg, err := composePlan(in, live)
	if err != nil {
		return false, err
	}
	if parkReason != "" {
		recordRepin(isvc, false)
		emit(in.Recorder, isvc, corev1.EventTypeWarning, EventRepinRejected,
			"repin rejected: the current source does not render (%s): %s", parkReason, parkMsg)
		return false, consumeAnnotation(ctx, in.Client, isvc, constants.RolloutRepinAnnotation)
	}
	fresh := combinedPlanDigest(plan.digests)
	// Idempotence is what makes the verb safe under cache lag: a pass that
	// still sees the annotation after a prior pass already swapped (the
	// removal patch outran the informer) re-renders to exactly the pinned
	// digests and members and must be pure cleanup — re-applying would
	// re-stamp PinnedAt and re-clamp for no reason, and claiming a state
	// change would trigger a spurious flush. The digests cover the
	// progression bodies alone, so the members are compared beside them.
	if fresh == oldDigest && sameGroupMembers(plan.groups, active.Plan.Groups) {
		return false, consumeAnnotation(ctx, in.Client, isvc, constants.RolloutRepinAnnotation)
	}
	if want != "now" && want != fresh {
		recordRepin(isvc, false)
		emit(in.Recorder, isvc, corev1.EventTypeWarning, EventRepinRejected,
			"repin rejected: expected render digest %s but the current render is %s (a concurrent edit landed; re-issue with the current digest)", want, fresh)
		return false, consumeAnnotation(ctx, in.Client, isvc, constants.RolloutRepinAnnotation)
	}

	// An empty re-render (no groups) closes the run instead of pinning an
	// empty plan.
	if len(plan.groups) == 0 {
		recordRepin(isvc, true)
		closeAndRest(in, active, v1beta1.RolloutRunSuperseded, now,
			"run %s closed by repin to an empty plan", active.RunID)
		return true, consumeAnnotation(ctx, in.Client, isvc, constants.RolloutRepinAnnotation)
	}

	ranUnder := canaryStepsByUnit(active)
	oldMembers := groupMembersLabel(active.Plan.Groups)
	active.Plan = v1beta1.RolloutRunPlan{Groups: plan.groups}
	active.PinnedAt = now
	clampCanary(isvc, active, ranUnder)
	recordRepin(isvc, true)
	change := fmt.Sprintf("%s -> %s", oldDigest, fresh)
	if fresh == oldDigest {
		change = fmt.Sprintf("the progression bodies are unchanged (%s), the members are now %s (were %s)", fresh, groupMembersLabel(plan.groups), oldMembers)
	}
	emit(in.Recorder, isvc, corev1.EventTypeNormal, EventPlanRepinned,
		"run %s repinned: %s", active.RunID, change)
	setPlanDrift(isvc, false, v1beta1.RolloutPlanDriftReasonInSync, "", now)
	return true, consumeAnnotation(ctx, in.Client, isvc, constants.RolloutRepinAnnotation)
}

// clampCanary keeps every canary unit's recorded step inside the step list
// the plan verb recorded for that unit, each against the list the unit ran
// under: a unit finished under its old list stays finished, an index past
// the new list's end lands on its last step, and the pre-step hold is set
// when the kept step would raise traffic, since the executor writes a
// step's traffic as soon as its capacity gate passes. A unit with no old
// list on record is clamped into its new list alone.
func clampCanary(isvc *v1beta1.InferenceService, active *v1beta1.RolloutRun, ranUnder map[v1beta1.ComponentType][]v1beta1.RolloutGroupStep) {
	for i := range active.Plan.Groups {
		g := &active.Plan.Groups[i].Group
		c := g.Canary
		if c == nil {
			continue
		}
		primary := primaryOfGroup(g)
		cs := rollout.CanaryStatusFor(&isvc.Status, primary)
		if cs == nil {
			continue
		}
		rollout.ClampStep(cs, primaryPhase(isvc, g), ranUnder[primary], c.Steps)
	}
}

// canaryStepsByUnit is each canary unit's step list in the recorded plan,
// by the unit's primary; empty when the plan carries no canary group.
func canaryStepsByUnit(active *v1beta1.RolloutRun) map[v1beta1.ComponentType][]v1beta1.RolloutGroupStep {
	out := map[v1beta1.ComponentType][]v1beta1.RolloutGroupStep{}
	for i := range active.Plan.Groups {
		g := &active.Plan.Groups[i].Group
		if g.Canary != nil {
			out[primaryOfGroup(g)] = g.Canary.Steps
		}
	}
	return out
}

func pinnedDigests(active *v1beta1.RolloutRun) []string {
	out := make([]string, 0, len(active.Plan.Groups))
	for i := range active.Plan.Groups {
		out = append(out, active.Plan.Groups[i].PortableDigest)
	}
	return out
}

// departedMembers is the set of recorded members whose Component has left the
// OMENative mode or the spec, read from the modes the controller resolved
// for this pass. With no modes handed in, no member has left.
func departedMembers(in Inputs, active *v1beta1.RolloutRun) map[v1beta1.ComponentType]bool {
	left := map[v1beta1.ComponentType]bool{}
	if active == nil || in.ComponentDeploymentModes == nil {
		return left
	}
	for _, target := range active.TargetRevisions {
		mode, declared := in.ComponentDeploymentModes[target.Component]
		if !declared || mode != constants.OMENative {
			left[target.Component] = true
		}
	}
	return left
}

// noRecordedMemberRemains reports whether every member the run recorded has left.
func noRecordedMemberRemains(active *v1beta1.RolloutRun, left map[v1beta1.ComponentType]bool) bool {
	for _, target := range active.TargetRevisions {
		if !left[target.Component] {
			return false
		}
	}
	return len(active.TargetRevisions) > 0
}

// departedList is the recorded members in left, in the run's order.
func departedList(active *v1beta1.RolloutRun, left map[v1beta1.ComponentType]bool) []v1beta1.ComponentType {
	var out []v1beta1.ComponentType
	for _, target := range active.TargetRevisions {
		if left[target.Component] {
			out = append(out, target.Component)
		}
	}
	return out
}

// sameMembers reports whether two member lists name the same set of
// Components, in any order.
func sameMembers(a, b []v1beta1.ComponentType) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[v1beta1.ComponentType]bool, len(a))
	for _, c := range a {
		seen[c] = true
	}
	for _, c := range b {
		if !seen[c] {
			return false
		}
	}
	return true
}

// sameGroupMembers reports whether a fresh render names, group by group,
// the members the recorded plan names.
func sameGroupMembers(fresh, recorded []v1beta1.RolloutRunGroup) bool {
	if len(fresh) != len(recorded) {
		return false
	}
	for i := range fresh {
		if !sameMembers(fresh[i].Group.Components, recorded[i].Group.Components) {
			return false
		}
	}
	return true
}

// membersLabel renders a member list for a condition or an event, as
// "[engine, decoder]".
func membersLabel(members []v1beta1.ComponentType) string {
	names := make([]string, 0, len(members))
	for _, c := range members {
		names = append(names, string(c))
	}
	return "[" + strings.Join(names, ", ") + "]"
}

// groupMembersLabel renders every group's members, as
// "groups[0]=[engine] groups[1]=[decoder]".
func groupMembersLabel(groups []v1beta1.RolloutRunGroup) string {
	parts := make([]string, 0, len(groups))
	for i := range groups {
		parts = append(parts, fmt.Sprintf("groups[%d]=%s", i, membersLabel(groups[i].Group.Components)))
	}
	return strings.Join(parts, " ")
}

// writeResolutionView refreshes status.rollout.groups[] — the always-current
// per-group source view, including the shadowed-policy preview when an
// inline arm outranks a ref — from the policies this pass observed.
func writeResolutionView(isvc *v1beta1.InferenceService, policies rollout.Policies) error {
	if isvc.Spec.Rollout == nil || len(isvc.Spec.Rollout.Groups) == 0 {
		if isvc.Status.Rollout != nil {
			isvc.Status.Rollout.Groups = nil
		}
		return nil
	}
	view := make([]v1beta1.RolloutGroupResolution, 0, len(isvc.Spec.Rollout.Groups))
	for gi := range isvc.Spec.Rollout.Groups {
		g := &isvc.Spec.Rollout.Groups[gi]
		entry := v1beta1.RolloutGroupResolution{
			Index:  int32(gi),
			Source: rolloutpolicy.GroupSource(g),
		}
		if g.PolicyRef != nil {
			entry.PolicyRef = g.PolicyRef.DeepCopy()
		}
		live, err := liveSourceDigest(g, policies)
		if err != nil {
			return err
		}
		entry.ObservedDigest = live
		// The preview surface: an inline arm shadows the ref; publish what
		// the policy would pin so the cutover diff is one field-compare.
		if entry.Source == v1beta1.RolloutPlanSourceInline && g.PolicyRef != nil {
			shadow := &v1beta1.ShadowedRolloutPolicyRef{Name: g.PolicyRef.Name}
			if policy := policies.ByName[g.PolicyRef.Name]; policy != nil {
				if d, derr := rolloutpolicy.PortableDigest(&policy.Spec); derr == nil {
					shadow.WouldPinDigest = d
				}
			}
			entry.ShadowedPolicyRef = shadow
		}
		view = append(view, entry)
	}
	if isvc.Status.Rollout == nil {
		isvc.Status.Rollout = &v1beta1.RolloutStatus{}
	}
	isvc.Status.Rollout.Groups = view
	return nil
}
