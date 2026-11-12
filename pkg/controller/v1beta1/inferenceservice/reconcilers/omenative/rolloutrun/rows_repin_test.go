package rolloutrun

import (
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// Tests for RepinPending: a run is pinned and the repin annotation is
// present. The pass consumes the verb before it reads replica freshness,
// retargets or closes, so everything else waits one pass.

// The repin verb is consumed first and the pass returns: whatever else the
// pass could have judged (a moved target, a converged, trailing, stale or
// missing replica, a unit that finished, rolled back, failed, parked or was
// re-armed, a staged or idle group, a rollback verb) is judged on the pass
// after, against the repinned plan.
func TestTheRepinVerbIsConsumedBeforeTheRunIsJudged(t *testing.T) {
	type story struct {
		name string
		// canary selects the inline canary start; otherwise a blueGreen run
		// whose group is edited to a rolling update.
		canary bool
		apply  func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object
		// modes is the deployment modes the pass after the plan reread reads; nil
		// reads every member as OMENative.
		modes map[v1beta1.ComponentType]constants.DeploymentModeType
		// after is what the pass after the repin does.
		after func(t *testing.T, isvc *v1beta1.InferenceService, out Outcome)
	}
	stillPinned := func(t *testing.T, isvc *v1beta1.InferenceService, out Outcome) {
		t.Helper()
		if out.StateChanged || out.Opened || !v1beta1.RolloutRunActive(isvc) {
			t.Fatalf("pass after the repin = %+v active=%v, want the run kept", out, v1beta1.RolloutRunActive(isvc))
		}
	}
	closes := func(outcome v1beta1.RolloutRunOutcome) func(t *testing.T, isvc *v1beta1.InferenceService, out Outcome) {
		return func(t *testing.T, isvc *v1beta1.InferenceService, out Outcome) {
			t.Helper()
			if !out.StateChanged || v1beta1.RolloutRunActive(isvc) || isvc.Status.Rollout.LastRun == nil || isvc.Status.Rollout.LastRun.Outcome != outcome {
				t.Fatalf("pass after the repin = %+v rollout=%+v, want the %s close", out, isvc.Status.Rollout, outcome)
			}
		}
	}
	stories := []story{
		{
			name: "spec.revision", canary: true,
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object {
				return []runtime.Object{divergedIR(oldRev, thirdRev)}
			},
			after: func(t *testing.T, isvc *v1beta1.InferenceService, out Outcome) {
				if !out.Opened || isvc.Status.Rollout.LastRun == nil || isvc.Status.Rollout.LastRun.Outcome != v1beta1.RolloutRunSuperseded {
					t.Fatalf("pass after the repin = %+v, want the retarget", out)
				}
			},
		},
		{
			name: "spec.planEdit", canary: true,
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object {
				return []runtime.Object{divergedIR(oldRev, newRev)}
			},
			after: stillPinned,
		},
		{
			name: "ir.stale", canary: true,
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object {
				stale := divergedIR(oldRev, newRev)
				stale.Status.ObservedGeneration = 0
				return []runtime.Object{stale}
			},
			after: func(t *testing.T, isvc *v1beta1.InferenceService, out Outcome) {
				if out.RequeueAfter != shortRequeue || out.StateChanged {
					t.Fatalf("pass after the repin = %+v, want the ten-second wait", out)
				}
			},
		},
		{
			name: "ir.missing", canary: true,
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object { return nil },
			after: stillPinned,
		},
		{
			name: "ir.converged", canary: true,
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object {
				return []runtime.Object{settledIR(newRev)}
			},
			after: closes(v1beta1.RolloutRunCompleted),
		},
		{
			name: "ir.straggling", canary: true,
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object {
				return []runtime.Object{stragglingIR(newRev)}
			},
			after: stillPinned,
		},
		{
			name: "canary.rolledBack", canary: true,
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object {
				setEngineUnit(isvc, v1beta1.RolloutPhaseRolledBack, rejectedRecord(t))
				return []runtime.Object{divergedIR(oldRev, newRev)}
			},
			after: closes(v1beta1.RolloutRunRolledBack),
		},
		{
			name: "canary.failed", canary: true,
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object {
				cs := midLadderRecord(t)
				cs.Failed = &v1beta1.CanaryFailure{Reason: v1beta1.CanaryFailureCapacityTimeout}
				setEngineUnit(isvc, v1beta1.RolloutPhaseFailed, cs)
				return []runtime.Object{divergedIR(oldRev, newRev)}
			},
			after: stillPinned,
		},
		{
			name: "canary.parked", canary: true,
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object {
				cs := rejectedRecord(t)
				cs.Failed = &v1beta1.CanaryFailure{Reason: v1beta1.CanaryFailureStableRevisionMissing}
				setEngineUnit(isvc, v1beta1.RolloutPhaseFailed, cs)
				return []runtime.Object{divergedIR(oldRev, newRev)}
			},
			after: stillPinned,
		},
		{
			name: "canary.resumed", canary: true,
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object {
				setEngineUnit(isvc, v1beta1.RolloutPhasePending, midLadderRecord(t))
				return []runtime.Object{divergedIR(oldRev, newRev)}
			},
			after: stillPinned,
		},
		{
			name: "coord.staged",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object {
				setCoordinationPhase(isvc, v1beta1.CoordinationPhaseStaged)
				return []runtime.Object{stagedIR()}
			},
			after: closes(v1beta1.RolloutRunCompleted),
		},
		{
			name: "coord.settled",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object {
				setCoordinationPhase(isvc, v1beta1.CoordinationPhaseIdle)
				return []runtime.Object{divergedIR(oldRev, newRev)}
			},
			after: stillPinned,
		},
		{
			name: "verb.rollback", canary: true,
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object {
				annotate(isvc, constants.RolloutRollbackAnnotation, "true")
				return []runtime.Object{divergedIR(oldRev, newRev)}
			},
			after: func(t *testing.T, isvc *v1beta1.InferenceService, out Outcome) {
				stillPinned(t, isvc, out)
				if isvc.Annotations[constants.RolloutRollbackAnnotation] != "true" {
					t.Fatal("the run layer consumed the rollback verb")
				}
			},
		},
		{
			name: "ctrl.resync", canary: true,
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object {
				return []runtime.Object{divergedIR(oldRev, newRev)}
			},
			after: stillPinned,
		},
		{
			// The engine leaves the mode with the plan reread written in the same
			// pass: the verb is read first and the plan replaced with the
			// decoder group that took the engine's place; the next pass
			// reads the engine as departed and closes the run.
			name: "spec.modeSwitch", canary: true,
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object {
				isvc.Spec.Rollout.Groups = []v1beta1.RolloutGroup{{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, Canary: canaryBody(25, 100)}}
				return []runtime.Object{divergedIR(oldRev, newRev), settledDecoderIR(decoderCurrentRev)}
			},
			modes: modesWith(v1beta1.EngineComponent),
			after: closes(v1beta1.RolloutRunSuperseded),
		},
	}
	for _, s := range stories {
		t.Run(s.name, func(t *testing.T) {
			var isvc *v1beta1.InferenceService
			if s.canary {
				isvc, _ = openInlineCanary(t)
				isvc.Spec.Rollout.Groups[0].Canary = canaryBody(25, 100)
			} else {
				isvc = isvcFixture(inlineBlueGreen())
				pass(t, isvc, divergedIR(oldRev, newRev))
				isvc.Spec.Rollout.Groups[0] = v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}}
			}
			run := requirePinned(t, isvc)
			id, pinnedAt, oldDigest := run.RunID, run.PinnedAt, run.Plan.Groups[0].PortableDigest
			objects := s.apply(t, isvc)
			annotate(isvc, constants.RolloutRepinAnnotation, "now")

			out := passAt(t, isvc, time.Unix(2000, 0), objects...)
			if !out.StateChanged || out.Opened || out.Parked || out.RequeueAfter != shortRequeue {
				t.Fatalf("repin pass = %+v, want the applied repin as the only boundary", out)
			}
			if _, still := isvc.Annotations[constants.RolloutRepinAnnotation]; still {
				t.Fatal("the verb was not consumed")
			}
			run = requireSameRun(t, isvc, id)
			if !run.PinnedAt.After(pinnedAt.Time) || run.Plan.Groups[0].PortableDigest == oldDigest {
				t.Fatalf("run = %+v, want the plan replaced with PinnedAt advanced", run)
			}
			if isvc.Status.Rollout.LastRun != nil {
				t.Fatalf("the repin pass closed or retargeted the run: %+v", isvc.Status.Rollout.LastRun)
			}
			after, _ := passWith(t, isvc, func(in *Inputs) {
				in.Now = time.Unix(3000, 0)
				in.ComponentDeploymentModes = s.modes
			}, objects...)
			s.after(t, isvc, after)
		})
	}
}

// A repin whose fresh render equals the pinned digests is pure cleanup: the
// annotation is removed, nothing is re-stamped and no state change is
// claimed; the next pass has nothing left to read.
func TestAnUnchangedRepinIsPureCleanup(t *testing.T) {
	isvc, ir := openInlineCanary(t)
	before := isvc.Status.DeepCopy()
	annotate(isvc, constants.RolloutRepinAnnotation, "now")
	out, events := passWith(t, isvc, func(in *Inputs) { in.Now = time.Unix(2000, 0) }, ir)
	if out.StateChanged || out.Opened || len(events) != 0 {
		t.Fatalf("outcome = %+v events = %v, want no state change claimed", out, events)
	}
	if _, still := isvc.Annotations[constants.RolloutRepinAnnotation]; still {
		t.Fatal("the cleanup left the annotation")
	}
	if !reflect.DeepEqual(before.Rollout.ActiveRun, isvc.Status.Rollout.ActiveRun) {
		t.Fatalf("the cleanup re-stamped the pin:\nbefore %+v\nafter  %+v", before.Rollout.ActiveRun, isvc.Status.Rollout.ActiveRun)
	}
	if out, events := passWith(t, isvc, nil, ir); out.StateChanged || len(events) != 0 {
		t.Fatalf("the pass after the cleanup = %+v events = %v, want nothing", out, events)
	}
}

// A repin whose source does not render, or whose expected digest is not
// the fresh render's, is refused with a RolloutRepinRejected event: the pin
// is unchanged, no state change is claimed and the annotation is removed so
// a rejected repin never retries; the next pass has nothing left to read
// and reports the drift as usual.
func TestARefusedRepinIsConsumedWithoutTouchingThePin(t *testing.T) {
	refusals := map[string]func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object){
		"unrenderable": func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
			isvc, ir := openInlineCanary(t)
			isvc.Spec.Rollout.Groups[0] = refCanary(policyName)
			return isvc, []runtime.Object{ir}
		},
		"policy.unresolved": func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
			isvc, ir := openRefCanary(t, false)
			return isvc, []runtime.Object{ir}
		},
		"mismatched": func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
			isvc, ir := openInlineCanary(t)
			isvc.Spec.Rollout.Groups[0].Canary = canaryBody(25, 100)
			return isvc, []runtime.Object{ir}
		},
	}
	for name, setup := range refusals {
		t.Run(name, func(t *testing.T) {
			isvc, objects := setup(t)
			before := isvc.Status.Rollout.ActiveRun.DeepCopy()
			value := "now"
			if name == "mismatched" {
				value = "rp1:stale"
			}
			annotate(isvc, constants.RolloutRepinAnnotation, value)
			out, events := passWith(t, isvc, func(in *Inputs) { in.Now = time.Unix(2000, 0) }, objects...)
			if out.StateChanged || out.Opened || out.Parked {
				t.Fatalf("outcome = %+v, want the refusal to claim no state change", out)
			}
			if !hasEvent(events, EventRepinRejected) {
				t.Fatalf("events = %v, want %s", events, EventRepinRejected)
			}
			if _, still := isvc.Annotations[constants.RolloutRepinAnnotation]; still {
				t.Fatal("the refusal left the annotation")
			}
			if !reflect.DeepEqual(before, isvc.Status.Rollout.ActiveRun) {
				t.Fatalf("the refusal touched the pin:\nbefore %+v\nafter  %+v", before, isvc.Status.Rollout.ActiveRun)
			}
			out, events = passWith(t, isvc, nil, objects...)
			if out.StateChanged || out.Parked || hasEvent(events, EventRepinRejected) {
				t.Fatalf("the pass after the refusal = %+v events = %v, want the run on its pin with drift", out, events)
			}
			requireDrift(t, isvc, corev1.ConditionTrue, driftCondition(isvc).Reason)
		})
	}
}

// A repin with no spec group left renders the empty plan and closes the run
// Superseded in the pass that reads it: the annotation is removed, the plan
// conditions read NoActiveRun and InSync, and the passes after find nothing
// open and open nothing, since nothing is declared.
func TestARepinToTheEmptyPlanClosesTheRun(t *testing.T) {
	isvc, ir := openInlineCanary(t)
	isvc.Spec.Rollout = nil
	annotate(isvc, constants.RolloutRepinAnnotation, "now")
	out, events := passWith(t, isvc, nil, ir)
	if !out.StateChanged || v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("outcome = %+v active=%v, want the run closed by the repin", out, v1beta1.RolloutRunActive(isvc))
	}
	if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunSuperseded {
		t.Fatalf("lastRun = %+v, want the Superseded close", last)
	}
	if !hasEvent(events, "closed by repin to an empty plan") {
		t.Fatalf("events = %v, want the close named after the repin", events)
	}
	if _, ok := isvc.Annotations[constants.RolloutRepinAnnotation]; ok {
		t.Fatal("the repin annotation was not removed with the close")
	}
	requireNoRun(t, isvc, v1beta1.RolloutPlanReasonNoRun)
	requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)
	for i := 0; i < 2; i++ {
		if out := pass(t, isvc, ir); !emptyOutcome(out) || v1beta1.RolloutRunActive(isvc) {
			t.Fatalf("pass %d after the close = %+v active=%v, want nothing: no group is declared", i, out, v1beta1.RolloutRunActive(isvc))
		}
	}
}

// A reference that resolves again, with a new body, is what the next pass
// renders and pins when the annotation names now or that render's digest.
func TestARepinPinsTheBodyThatResolvesAgain(t *testing.T) {
	isvc, ir := openRefCanary(t, false)
	id := isvc.Status.Rollout.ActiveRun.RunID
	pass(t, isvc, ir) // the reference is gone: drift, no park
	requireDrift(t, isvc, corev1.ConditionTrue, v1beta1.RolloutPlanDriftReasonPolicyNewerThanRun)
	annotate(isvc, constants.RolloutRepinAnnotation, "now")
	out := pass(t, isvc, ir, canaryPolicy(policyName, 50, 100))
	if !out.StateChanged {
		t.Fatalf("outcome = %+v, want the repin applied", out)
	}
	run := requireSameRun(t, isvc, id)
	if run.Plan.Groups[0].Group.Canary.Steps[0].Traffic != 50 {
		t.Fatalf("pinned body = %+v, want the body that resolved again", run.Plan.Groups[0].Group.Canary)
	}
	requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)
}

// twoCanaryUnits is a router unit and an engine unit, each its own canary
// group with the given step lists, declared Concurrent.
func twoCanaryUnits(router, engine *v1beta1.GroupCanary) *v1beta1.InferenceService {
	one := 1
	ordering := v1beta1.RolloutGroupOrderingConcurrent
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llm-a", Namespace: "ns"},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &one}},
			Router: &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &one}},
			Rollout: &v1beta1.RolloutSpec{
				GroupOrdering: &ordering,
				Groups: []v1beta1.RolloutGroup{
					{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, Canary: router},
					{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: engine},
				},
			},
		},
	}
}

// setUnitRecord writes a unit's record and its primary's phase.
func setUnitRecord(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, phase v1beta1.RolloutPhase, cs *v1beta1.CanaryStatus) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	status := isvc.Status.Components[c]
	status.RolloutPhase = phase
	status.Canary = cs
	isvc.Status.Components[c] = status
}

// A plan verb clamps every canary unit against the list that unit ran
// under. Two units holding records whose lists shrink to a single 100
// percent step are both held before the raise; a router sitting the run out
// under a two-step list does not make an engine at the third step of its
// four-step list read as finished.
func TestAPlanReloadClampsEachCanaryUnitAgainstItsOwnList(t *testing.T) {
	router, engine := v1beta1.RouterComponent, v1beta1.EngineComponent
	type want struct {
		step int32
		hold bool
	}
	for _, tc := range []struct {
		name       string
		routerIR   *v1beta1.InferenceReplica
		router     *v1beta1.CanaryStatus
		editRouter *v1beta1.GroupCanary
		editEngine *v1beta1.GroupCanary
		engine     v1beta1.CanaryStatus
		wantRouter *want
		wantEngine want
	}{
		{
			name:       "two units with records are each held before their own raise",
			routerIR:   namedIR("llm-a-router", routerStableRev, routerNewRev),
			router:     &v1beta1.CanaryStatus{CurrentStep: 0, ObservedTrafficWeight: 50},
			editRouter: canaryBody(100),
			editEngine: canaryBody(10, 100),
			engine:     v1beta1.CanaryStatus{CurrentStep: 1, ObservedTrafficWeight: 30},
			wantRouter: &want{step: 0, hold: true},
			wantEngine: want{step: 1, hold: true},
		},
		{
			name:       "a router sitting the run out leaves the engine's third step where it is",
			routerIR:   namedIR("llm-a-router", routerStableRev, routerStableRev),
			editRouter: canaryBody(20, 100),
			editEngine: canaryBody(10, 30, 60, 100),
			engine:     v1beta1.CanaryStatus{CurrentStep: 2, ObservedTrafficWeight: 60},
			wantEngine: want{step: 2, hold: false},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isvc := twoCanaryUnits(canaryBody(50, 100), canaryBody(10, 30, 60, 100))
			engineIR := namedIR("llm-a-engine", engineStableRev, engineNewRev)
			if out := pass(t, isvc, tc.routerIR, engineIR); !out.Opened {
				t.Fatalf("fixture: %+v, want an open", out)
			}
			if tc.router != nil {
				tc.router.CanaryRevisionHash, tc.router.StableRevisionHash = hashOf(t, routerNewRev), hashOf(t, routerStableRev)
				setUnitRecord(isvc, router, v1beta1.RolloutPhasePaused, tc.router)
			}
			cs := tc.engine
			cs.CanaryRevisionHash, cs.StableRevisionHash = hashOf(t, engineNewRev), hashOf(t, engineStableRev)
			setUnitRecord(isvc, engine, v1beta1.RolloutPhasePaused, &cs)
			isvc.Spec.Rollout.Groups[0].Canary, isvc.Spec.Rollout.Groups[1].Canary = tc.editRouter, tc.editEngine
			annotate(isvc, constants.RolloutRepinAnnotation, "now")
			if out := pass(t, isvc, tc.routerIR, engineIR); !out.StateChanged {
				t.Fatalf("the plan verb must apply: %+v", out)
			}
			got := rollout.CanaryStatusFor(&isvc.Status, engine)
			if got.CurrentStep != tc.wantEngine.step || got.PreStepHold != tc.wantEngine.hold {
				t.Errorf("engine step %d hold %v, want step %d hold %v", got.CurrentStep, got.PreStepHold, tc.wantEngine.step, tc.wantEngine.hold)
			}
			if tc.wantRouter != nil {
				got := rollout.CanaryStatusFor(&isvc.Status, router)
				if got.CurrentStep != tc.wantRouter.step || got.PreStepHold != tc.wantRouter.hold {
					t.Errorf("router step %d hold %v, want step %d hold %v", got.CurrentStep, got.PreStepHold, tc.wantRouter.step, tc.wantRouter.hold)
				}
			}
		})
	}
}
