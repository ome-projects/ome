// Package rolloutrun owns the rollout RUN lifecycle: one traversal of the
// effective rollout plan for one target revision set. At open it renders the
// effective plan (each group's inline progression or its referenced
// RolloutPolicy body), re-validates the composition, and pins it into
// status.rollout.activeRun in the same in-memory status the executors' step
// state lives in — one status flush carries both, so the pinned plan and the
// step counters can never be observed out of sync. Executors consume the
// pinned plan (v1beta1.EffectiveRollout); spec and policy edits are inert
// mid-run and take effect at the next open. Failure to resolve a plan PARKS
// the rollout (condition + held update gates) — it never falls back to the
// default progression, because silently removing a declared gate is the
// failure class this layer exists to prevent.
package rolloutrun

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
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
	// Client serves cached reads (drift and resolution-view policy lookups —
	// they run every reconcile) and the verb-annotation consumption writes.
	Client client.Client
	// Reader serves the reads a run DECISION depends on (IR revision pairs,
	// policy bodies at open/repin): acting on a cache-lagged target would pin
	// the wrong plan.
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
}

// composedPlan is one rendered effective plan: the pinnable groups plus their
// per-group digests (digests double as the repin CAS value).
type composedPlan struct {
	groups  []v1beta1.RolloutRunGroup
	digests []string
}

// Reconcile drives the run lifecycle for one ISVC, mutating isvc.Status
// in-memory; the controller's status flush persists it together with the
// executors' step state.
func Reconcile(ctx context.Context, in Inputs) (Outcome, error) {
	isvc := in.ISVC
	if in.Now.IsZero() {
		in.Now = time.Now()
	}
	now := metav1.Time{Time: in.Now}

	specGroups := 0
	if isvc.Spec.Rollout != nil {
		specGroups = len(isvc.Spec.Rollout.Groups)
	}

	if err := writeResolutionView(ctx, in); err != nil {
		return Outcome{}, err
	}

	active := activeRun(isvc)
	retargeting := false

	// Every group deleted mid-run: the pinned plan continues (an inert edit,
	// like any other); deleting a group never aborts a run, so point at the
	// verbs that do.
	if specGroups == 0 {
		if active != nil {
			if setPlanDrift(isvc, true, v1beta1.RolloutPlanDriftReasonSpecNewerThanRun,
				"spec.rollout removed mid-run; the pinned plan continues — abort with ome.io/rollout-rollback, or repin to the empty plan with ome.io/rollout-repin", now) {
				emit(in.Recorder, isvc, corev1.EventTypeWarning, EventGroupRemoved,
					"spec.rollout was removed while run %s is pinned; the run continues on its pinned plan (use ome.io/rollout-rollback or ome.io/rollout-repin to abort)", active.RunID)
			}
			return Outcome{RequeueAfter: shortRequeue}, nil
		}
		return Outcome{}, nil
	}

	targets, fresh, err := observeGroupTargets(ctx, in.Reader, isvc)
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
		if !fresh {
			return Outcome{RequeueAfter: shortRequeue}, nil
		}
		if retargeted(isvc, active, targets) {
			retargeting = true
			for _, target := range active.TargetRevisions {
				stableOverrides[target.Component] = target.StableRevision
			}
			closeRun(isvc, active, v1beta1.RolloutRunSuperseded, now)
			recordRunClosed(isvc, v1beta1.RolloutRunSuperseded)
			emit(in.Recorder, isvc, corev1.EventTypeNormal, EventRunClosed,
				"run %s superseded by a new target revision; opening a fresh run", active.RunID)
			active = nil
			// fall through to the open path with a fresh render.
		} else if outcome, done := closedOutcome(isvc, active, targets); done {
			closeRun(isvc, active, outcome, now)
			recordRunClosed(isvc, outcome)
			recordDrift(isvc, false)
			emit(in.Recorder, isvc, corev1.EventTypeNormal, EventRunClosed, "run %s closed: %s", active.RunID, outcome)
			setPlanReady(isvc, corev1.ConditionTrue, v1beta1.RolloutPlanReasonNoRun,
				fmt.Sprintf("last run closed %s", outcome), now)
			setPlanDrift(isvc, false, v1beta1.RolloutPlanDriftReasonInSync, "", now)
			return Outcome{StateChanged: true}, nil
		} else {
			if err := updateDrift(ctx, in, active, now); err != nil {
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
	if !divergedMember(isvc, targets) && !canaryMidFlight(isvc) {
		setPlanReady(isvc, corev1.ConditionTrue, v1beta1.RolloutPlanReasonNoRun, "no rollout in progress", now)
		setPlanDrift(isvc, false, v1beta1.RolloutPlanDriftReasonInSync, "", now)
		return Outcome{}, nil
	}

	plan, parkReason, parkMsg, err := composePlan(ctx, in, in.Reader)
	if err != nil {
		return Outcome{}, err
	}
	if parkReason != "" {
		if setPlanReady(isvc, corev1.ConditionFalse, parkReason, parkMsg, now) {
			recordParked(isvc, parkReason)
			emit(in.Recorder, isvc, corev1.EventTypeWarning, EventPlanParked,
				"rollout parked (%s): %s — the new revision is held, the previous revision keeps serving", parkReason, parkMsg)
		}
		return Outcome{Parked: true, RequeueAfter: parkRequeue}, nil
	}

	adopting := !retargeting && isvc.Status.Canary != nil && canaryMidFlight(isvc)
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
	return Outcome{StateChanged: true, RequeueAfter: shortRequeue, Opened: true, Adopted: adopting}, nil
}

func activeRun(isvc *v1beta1.InferenceService) *v1beta1.RolloutRun {
	if isvc.Status.Rollout == nil {
		return nil
	}
	return isvc.Status.Rollout.ActiveRun
}

// composePlan renders the effective plan from the live spec: per group,
// inline-wins resolution, ref fetch through reads, composed-body
// re-validation, and provider resolution. A non-empty parkReason means the
// plan is unresolvable — the caller parks instead of opening.
func composePlan(ctx context.Context, in Inputs, reads client.Reader) (composedPlan, string, string, error) {
	isvc := in.ISVC
	inflated := derivedProvenance(isvc)
	var plan composedPlan
	for gi := range isvc.Spec.Rollout.Groups {
		g := &isvc.Spec.Rollout.Groups[gi]
		var policySpec *v1beta1.RolloutPolicySpec
		var provRef *v1beta1.RolloutPolicyRef
		var policyGen int64

		if g.PolicyRef != nil && g.Canary == nil && g.BlueGreen == nil && g.RollingUpdate == nil {
			if !in.FeatureEnabled {
				return plan, v1beta1.RolloutPlanReasonPlanInvalid,
					fmt.Sprintf("groups[%d].policyRef %q: the rollout policy feature is not enabled on this cluster, so the ref cannot resolve", gi, g.PolicyRef.Name), nil
			}
			policy := &v1beta1.RolloutPolicy{}
			err := reads.Get(ctx, client.ObjectKey{Namespace: isvc.Namespace, Name: g.PolicyRef.Name}, policy)
			switch {
			case err == nil:
			case client.IgnoreNotFound(err) == nil:
				return plan, v1beta1.RolloutPlanReasonPolicyNotFound,
					fmt.Sprintf("groups[%d].policyRef %q: RolloutPolicy not found in namespace %s", gi, g.PolicyRef.Name, isvc.Namespace), nil
			default:
				return plan, "", "", err
			}
			if verr := validation.ValidateRolloutPolicySpec(&policy.Spec); verr != nil {
				return plan, v1beta1.RolloutPlanReasonPolicyNotReady,
					fmt.Sprintf("groups[%d].policyRef %q: policy body is invalid: %v", gi, g.PolicyRef.Name, verr), nil
			}
			policySpec = &policy.Spec
			provRef = g.PolicyRef.DeepCopy()
			policyGen = policy.Generation
		}

		composed, cerr := rolloutpolicy.ComposeGroup(g, policySpec)
		if cerr != nil {
			reason := v1beta1.RolloutPlanReasonPlanInvalid
			if errors.Is(cerr, rolloutpolicy.ErrProgressionMismatch) {
				reason = v1beta1.RolloutPlanReasonProgressionMismatch
			}
			return plan, reason, fmt.Sprintf("groups[%d]: %v", gi, cerr), nil
		}
		// Composed-body re-validation, the belt against skew: the body passed
		// its own admission, but this instance of it is what will execute.
		if composed.Canary != nil {
			if verr := validation.ValidateCanaryPlan(fmt.Sprintf("groups[%d].canary (composed)", gi), composed.Canary); verr != nil {
				return plan, v1beta1.RolloutPlanReasonPlanInvalid, verr.Error(), nil
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
		source := v1beta1.RolloutPlanSourceInline
		if policySpec != nil {
			source = v1beta1.RolloutPlanSourcePolicy
		} else if p, ok := inflated[gi]; ok {
			// A derived ISVC's inline group inflated from a policy at derive
			// time: report the policy identity a locally-resolved ref would.
			source = v1beta1.RolloutPlanSourcePolicy
			provRef = p.PolicyRef
		}
		plan.groups = append(plan.groups, v1beta1.RolloutRunGroup{
			Source:           source,
			PolicyRef:        provRef,
			PolicyGeneration: policyGen,
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
func openRun(isvc *v1beta1.InferenceService, plan composedPlan, targets map[v1beta1.ComponentType]targetPair, stableOverrides map[v1beta1.ComponentType]string, adopting bool, now metav1.Time) {
	var pinned []v1beta1.RolloutRunTarget
	seen := map[v1beta1.ComponentType]bool{}
	for i := range plan.groups {
		for _, comp := range plan.groups[i].Group.Components {
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
				stable = componentStableRevision(isvc, comp, rev, t, adopting)
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
		RunID:           runID(isvc, pinned, plan.digests, now.UTC().Format(time.RFC3339)),
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
		OpenedAt:        &opened,
		ClosedAt:        &now,
		TargetRevisions: append([]v1beta1.RolloutRunTarget(nil), active.TargetRevisions...),
		Groups:          provenance,
	}
	isvc.Status.Rollout.ActiveRun = nil
}

// componentStableRevision resolves the last promoted revision for a Component.
// A distinct IR current revision is authoritative at run open. The scalar
// primary and per-Component rollout status cover adoption of an older run that
// has already advanced CurrentRevision to its target.
func componentStableRevision(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, pinnedRevision string, target targetPair, adopting bool) string {
	if target.current != "" && target.current != pinnedRevision {
		return target.current
	}
	if comp == primaryCanaryComponent(isvc) && isvc.Status.Canary != nil && isvc.Status.Canary.StableRevisionHash != "" {
		return isvc.Status.Canary.StableRevisionHash
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
// members converged to the pinned target, or the group rests Staged (a
// lifecycle partition deliberately holds mixed revisions; keying completion
// on full convergence alone would pin such a run's plan forever).
func closedOutcome(isvc *v1beta1.InferenceService, active *v1beta1.RolloutRun, targets map[v1beta1.ComponentType]targetPair) (v1beta1.RolloutRunOutcome, bool) {
	pinnedFor := map[v1beta1.ComponentType]string{}
	for _, t := range active.TargetRevisions {
		pinnedFor[t.Component] = t.Revision
	}
	pinnedSpec := active.Plan.AsRolloutSpec(isvc.Spec.Rollout)
	resolved := coordination.ResolveGroups(pinnedSpec, coordination.GroupDefaults{})
	cs := isvc.Status.Canary

	for i := range active.Plan.Groups {
		g := &active.Plan.Groups[i].Group
		if g.Canary != nil {
			if cs == nil {
				return "", false
			}
			if cs.RolledBackRevisionHash != "" {
				if primaryPhase(isvc, g) == v1beta1.RolloutPhaseRolledBack {
					return v1beta1.RolloutRunRolledBack, true
				}
				return "", false
			}
			if int(cs.CurrentStep) < len(g.Canary.Steps) {
				return "", false
			}
			// The done sentinel can lead the IR counters by a pass; closing
			// with stragglers would immediately reopen via the straggler
			// trigger — wait for real convergence instead of churning.
			for _, comp := range g.Components {
				obs := targets[comp]
				if obs.replicas > 0 && obs.updated < obs.replicas {
					return "", false
				}
			}
			continue
		}
		for _, comp := range g.Components {
			pinnedRev := pinnedFor[comp]
			if pinnedRev == "" {
				continue
			}
			obs := targets[comp]
			if obs.current == pinnedRev && obs.target == pinnedRev && (obs.replicas == 0 || obs.updated >= obs.replicas) {
				continue
			}
			if !groupStaged(isvc, resolved, comp) {
				return "", false
			}
		}
	}
	return v1beta1.RolloutRunCompleted, true
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
// (router > engine > decoder among the group's members — the step machine's
// Component).
func primaryPhase(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup) v1beta1.RolloutPhase {
	for _, c := range []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		for _, gc := range g.Components {
			if gc == c {
				return isvc.Status.Components[c].RolloutPhase
			}
		}
	}
	return ""
}

// updateDrift compares each spec group's live render digest against the
// pinned one and stamps RolloutPlanDrift. A canary group disappearing from
// the spec gets a pointed event: only the rollback and repin verbs abort a
// pinned run.
func updateDrift(ctx context.Context, in Inputs, active *v1beta1.RolloutRun, now metav1.Time) error {
	isvc := in.ISVC
	specGroups := isvc.Spec.Rollout.Groups
	pinned := active.Plan.Groups

	if len(specGroups) != len(pinned) {
		canaryGone := pinnedHasCanary(pinned) && !specHasCanaryKind(isvc.Spec.Rollout)
		msg := "spec.rollout group shape changed mid-run; the pinned plan continues until the next run or a repin"
		if canaryGone {
			msg = "the canary group was removed mid-run; the pinned canary continues — abort with ome.io/rollout-rollback, or repin with ome.io/rollout-repin"
		}
		recordDrift(isvc, true)
		if setPlanDrift(isvc, true, v1beta1.RolloutPlanDriftReasonSpecNewerThanRun, msg, now) && canaryGone {
			emit(in.Recorder, isvc, corev1.EventTypeWarning, EventGroupRemoved,
				"canary group removed while run %s is pinned; deleting the group does not abort a pinned run — use ome.io/rollout-rollback or ome.io/rollout-repin", active.RunID)
		}
		return nil
	}

	for gi := range specGroups {
		live, err := liveSourceDigest(ctx, in.Client, isvc, &specGroups[gi])
		if err != nil {
			return err
		}
		if live == pinned[gi].PortableDigest {
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
			fmt.Sprintf("groups[%d]: live render %s differs from pinned %s; the edit applies at the next run (or via ome.io/rollout-repin)",
				gi, liveLabel, pinned[gi].PortableDigest), now)
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

	plan, parkReason, parkMsg, err := composePlan(ctx, in, in.Reader)
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
	// digests and must be pure cleanup — re-applying would re-stamp
	// PinnedAt and re-clamp for no reason, and claiming a state change
	// would trigger a spurious flush.
	if fresh == oldDigest {
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
		closeRun(isvc, active, v1beta1.RolloutRunSuperseded, now)
		recordRepin(isvc, true)
		recordRunClosed(isvc, v1beta1.RolloutRunSuperseded)
		emit(in.Recorder, isvc, corev1.EventTypeNormal, EventRunClosed,
			"run %s closed by repin to an empty plan", active.RunID)
		return true, consumeAnnotation(ctx, in.Client, isvc, constants.RolloutRepinAnnotation)
	}

	oldStepCount := pinnedCanaryStepCount(active)
	active.Plan = v1beta1.RolloutRunPlan{Groups: plan.groups}
	active.PinnedAt = now
	clampCanary(isvc, active, oldStepCount)
	recordRepin(isvc, true)
	emit(in.Recorder, isvc, corev1.EventTypeNormal, EventPlanRepinned,
		"run %s repinned: %s -> %s", active.RunID, oldDigest, fresh)
	setPlanDrift(isvc, false, v1beta1.RolloutPlanDriftReasonInSync, "", now)
	return true, consumeAnnotation(ctx, in.Client, isvc, constants.RolloutRepinAnnotation)
}

// clampCanary bounds the persisted step counter into the repinned ladder and
// arms the pre-step hold when the clamped step would raise traffic: the
// executor programs traffic as soon as the capacity gate passes — before any
// pause — so a bare index-clamp onto a higher-traffic step (always the case
// when a shorter ladder clamps onto the final 100% step) would be immediate
// exposure, not a hold.
func clampCanary(isvc *v1beta1.InferenceService, active *v1beta1.RolloutRun, oldStepCount int) {
	cs := isvc.Status.Canary
	if cs == nil {
		return
	}
	for i := range active.Plan.Groups {
		c := active.Plan.Groups[i].Group.Canary
		if c == nil {
			continue
		}
		// Done stays done: a canary that finished under the OLD ladder must
		// not be resurrected by a repin. Done is judged against the old
		// plan's length — an in-flight index merely beyond the NEW ladder is
		// the shrink case and clamps to the last step instead.
		if oldStepCount > 0 && int(cs.CurrentStep) >= oldStepCount {
			cs.CurrentStep = int32(len(c.Steps))
			return
		}
		if cs.CurrentStep < 0 {
			cs.CurrentStep = 0
		}
		if int(cs.CurrentStep) >= len(c.Steps) {
			cs.CurrentStep = int32(len(c.Steps)) - 1
		}
		if c.Steps[cs.CurrentStep].Traffic > cs.ObservedTrafficWeight {
			cs.PreStepHold = true
		}
		return
	}
}

// pinnedCanaryStepCount is the pinned canary ladder's length, 0 when the
// pinned plan carries no canary group.
func pinnedCanaryStepCount(active *v1beta1.RolloutRun) int {
	for i := range active.Plan.Groups {
		if c := active.Plan.Groups[i].Group.Canary; c != nil {
			return len(c.Steps)
		}
	}
	return 0
}

func pinnedDigests(active *v1beta1.RolloutRun) []string {
	out := make([]string, 0, len(active.Plan.Groups))
	for i := range active.Plan.Groups {
		out = append(out, active.Plan.Groups[i].PortableDigest)
	}
	return out
}

// writeResolutionView refreshes status.rollout.groups[] — the always-current
// per-group source view, including the shadowed-policy preview when an
// inline arm outranks a ref. Cached reads: this runs every reconcile.
func writeResolutionView(ctx context.Context, in Inputs) error {
	isvc := in.ISVC
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
		live, err := liveSourceDigest(ctx, in.Client, isvc, g)
		if err != nil {
			return err
		}
		entry.ObservedDigest = live
		// The preview surface: an inline arm shadows the ref; publish what
		// the policy would pin so the cutover diff is one field-compare.
		if entry.Source == v1beta1.RolloutPlanSourceInline && g.PolicyRef != nil {
			shadow := &v1beta1.ShadowedRolloutPolicyRef{Name: g.PolicyRef.Name}
			policy := &v1beta1.RolloutPolicy{}
			if err := in.Client.Get(ctx, client.ObjectKey{Namespace: isvc.Namespace, Name: g.PolicyRef.Name}, policy); err == nil {
				if d, derr := rolloutpolicy.PortableDigest(&policy.Spec); derr == nil {
					shadow.WouldPinDigest = d
				}
			} else if client.IgnoreNotFound(err) != nil {
				return err
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
