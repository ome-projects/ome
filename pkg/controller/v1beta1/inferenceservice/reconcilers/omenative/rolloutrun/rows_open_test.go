package rolloutrun

import (
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Tests for a pinned run, fresh (Open) or adopted around a ladder that
// was already in flight (Adopted). From the pass after the one that pinned
// it an adopted run is judged exactly as a fresh one, so most cases run the
// same story from both starts.

// pinnedStart opens a run and returns the service with its diverged replica.
type pinnedStart func(t *testing.T) (*v1beta1.InferenceService, *v1beta1.InferenceReplica)

// openInlineCanary pins a fresh run over an inline canary group.
func openInlineCanary(t *testing.T) (*v1beta1.InferenceService, *v1beta1.InferenceReplica) {
	t.Helper()
	isvc, ir := isvcFixture(inlineCanary()), divergedIR(oldRev, newRev)
	if out := pass(t, isvc, ir); !out.Opened || out.Adopted {
		t.Fatalf("fixture: %+v, want a fresh open", out)
	}
	return isvc, ir
}

// adoptInlineCanary pins a run around a unit found mid-ladder with no pin.
func adoptInlineCanary(t *testing.T) (*v1beta1.InferenceService, *v1beta1.InferenceReplica) {
	t.Helper()
	isvc, ir := isvcFixture(inlineCanary()), divergedIR(oldRev, newRev)
	setEngineUnit(isvc, v1beta1.RolloutPhaseCanarying, midLadderRecord(t))
	if out := pass(t, isvc, ir); !out.Opened || !out.Adopted {
		t.Fatalf("fixture: %+v, want an adopted open", out)
	}
	return isvc, ir
}

var pinnedStarts = map[string]pinnedStart{"Open": openInlineCanary, "Adopted": adoptInlineCanary}

// openRefCanary pins a run over a referenced canary group, fresh or adopted.
func openRefCanary(t *testing.T, adopted bool) (*v1beta1.InferenceService, *v1beta1.InferenceReplica) {
	t.Helper()
	isvc, ir := isvcFixture(refCanary(policyName)), divergedIR(oldRev, newRev)
	if adopted {
		setEngineUnit(isvc, v1beta1.RolloutPhaseCanarying, midLadderRecord(t))
	}
	if out := pass(t, isvc, ir, canaryPolicy(policyName)); !out.Opened || out.Adopted != adopted {
		t.Fatalf("fixture: %+v, want an open with adopted=%v", out, adopted)
	}
	return isvc, ir
}

func requireSameRun(t *testing.T, isvc *v1beta1.InferenceService, id string) *v1beta1.RolloutRun {
	t.Helper()
	run := requirePinned(t, isvc)
	if run.RunID != id {
		t.Fatalf("the run was re-minted: %s -> %s", id, run.RunID)
	}
	return run
}

func requireDrift(t *testing.T, isvc *v1beta1.InferenceService, status corev1.ConditionStatus, reason string) {
	t.Helper()
	if d := driftCondition(isvc); d == nil || d.Status != status || d.Reason != reason {
		t.Fatalf("RolloutPlanDrift = %+v, want %s/%s", d, status, reason)
	}
}

func hasEvent(events []string, reason string) bool {
	for _, e := range events {
		if strings.Contains(e, reason) {
			return true
		}
	}
	return false
}

// A pinned run continues on its pin through everything that does not move
// its targets or converge them: a reshaped or removed group list (drift,
// and a warning when the canary group left), a replica that disappeared, a
// ladder short of its end, a unit parked Failed, a unit re-armed by the
// resume verb, an Idle coordination group, the rollback verb (the
// executor's), and a pass with nothing changed.
func TestAPinnedRunContinuesOnItsPin(t *testing.T) {
	type story struct {
		name string
		// apply changes the service or the objects the next pass reads.
		apply func(t *testing.T, isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica) []runtime.Object
		check func(t *testing.T, isvc *v1beta1.InferenceService, out Outcome, events []string)
	}
	stories := []story{
		{
			name: "spec.planEdit[inlineBody]",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica) []runtime.Object {
				isvc.Spec.Rollout.Groups[0].Canary = canaryBody(50, 100)
				return []runtime.Object{ir}
			},
			check: func(t *testing.T, isvc *v1beta1.InferenceService, out Outcome, events []string) {
				requireDrift(t, isvc, corev1.ConditionTrue, v1beta1.RolloutPlanDriftReasonSpecNewerThanRun)
				if isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Canary.Steps[0].Traffic != 10 {
					t.Fatal("the pinned body moved with the edit")
				}
			},
		},
		{
			name: "spec.planEdit[groupShape]",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica) []runtime.Object {
				isvc.Spec.Rollout.Groups = []v1beta1.RolloutGroup{
					inlineBlueGreen(),
					{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
				}
				return []runtime.Object{ir}
			},
			check: func(t *testing.T, isvc *v1beta1.InferenceService, out Outcome, events []string) {
				requireDrift(t, isvc, corev1.ConditionTrue, v1beta1.RolloutPlanDriftReasonSpecNewerThanRun)
				if !hasEvent(events, EventGroupRemoved) {
					t.Fatalf("events = %v, want %s for the canary group that left the spec", events, EventGroupRemoved)
				}
				if len(isvc.Status.Rollout.ActiveRun.Plan.Groups) != 1 || isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Canary == nil {
					t.Fatal("the pinned plan followed the reshaped spec")
				}
			},
		},
		{
			name: "spec.planEdit[allGroupsRemoved]",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica) []runtime.Object {
				isvc.Spec.Rollout = nil
				return []runtime.Object{settledIR(newRev)}
			},
			check: func(t *testing.T, isvc *v1beta1.InferenceService, out Outcome, events []string) {
				requireDrift(t, isvc, corev1.ConditionTrue, v1beta1.RolloutPlanDriftReasonSpecNewerThanRun)
				if out.RequeueAfter != shortRequeue || !hasEvent(events, EventGroupRemoved) {
					t.Fatalf("outcome = %+v events = %v, want the ten-second requeue and the removed-group warning", out, events)
				}
				if isvc.Status.Rollout.LastRun != nil {
					t.Fatal("a converged replica closed the run although no spec group is left to judge it by")
				}
			},
		},
		{
			name: "ir.missing",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica) []runtime.Object {
				return nil
			},
			check: func(t *testing.T, isvc *v1beta1.InferenceService, out Outcome, events []string) {
				if !emptyOutcome(out) || isvc.Status.Rollout.LastRun != nil {
					t.Fatalf("outcome = %+v lastRun = %+v, want the pin kept with no decision", out, isvc.Status.Rollout.LastRun)
				}
			},
		},
		{
			name: "canary.progress",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica) []runtime.Object {
				cs := midLadderRecord(t)
				cs.CurrentStep, cs.ObservedTrafficWeight = 1, 10
				setEngineUnit(isvc, v1beta1.RolloutPhasePaused, cs)
				return []runtime.Object{settledIR(newRev)}
			},
		},
		{
			name: "canary.failed",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica) []runtime.Object {
				cs := midLadderRecord(t)
				cs.Failed = &v1beta1.CanaryFailure{Reason: v1beta1.CanaryFailureCapacityTimeout}
				setEngineUnit(isvc, v1beta1.RolloutPhaseFailed, cs)
				return []runtime.Object{ir}
			},
		},
		{
			name: "canary.parked",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica) []runtime.Object {
				cs := rejectedRecord(t)
				cs.Failed = &v1beta1.CanaryFailure{Reason: v1beta1.CanaryFailureStableRevisionMissing}
				setEngineUnit(isvc, v1beta1.RolloutPhaseFailed, cs)
				return []runtime.Object{ir}
			},
		},
		{
			name: "canary.resumed",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica) []runtime.Object {
				setEngineUnit(isvc, v1beta1.RolloutPhaseRollingBack, rejectedRecord(t))
				id := isvc.Status.Rollout.ActiveRun.RunID
				pass(t, isvc, ir)
				requireSameRun(t, isvc, id)
				resumed := midLadderRecord(t)
				setEngineUnit(isvc, v1beta1.RolloutPhasePending, resumed)
				return []runtime.Object{ir}
			},
		},
		{
			name: "coord.settled",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica) []runtime.Object {
				setCoordinationPhase(isvc, v1beta1.CoordinationPhaseIdle)
				return []runtime.Object{ir}
			},
		},
		{
			name: "verb.rollback",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica) []runtime.Object {
				annotate(isvc, constants.RolloutRollbackAnnotation, "true")
				return []runtime.Object{ir}
			},
			check: func(t *testing.T, isvc *v1beta1.InferenceService, out Outcome, events []string) {
				if isvc.Annotations[constants.RolloutRollbackAnnotation] != "true" {
					t.Fatal("the run layer consumed the rollback verb")
				}
			},
		},
		{
			name: "ctrl.resync",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica) []runtime.Object {
				return []runtime.Object{ir}
			},
			check: func(t *testing.T, isvc *v1beta1.InferenceService, out Outcome, events []string) {
				if !emptyOutcome(out) || len(events) != 0 {
					t.Fatalf("outcome = %+v events = %v, want nothing", out, events)
				}
			},
		},
	}
	for startName, start := range pinnedStarts {
		for _, s := range stories {
			t.Run(startName+"/"+s.name, func(t *testing.T) {
				isvc, ir := start(t)
				pass(t, isvc, ir) // the pass after the open; an adopted run is Open from here on
				id := isvc.Status.Rollout.ActiveRun.RunID
				objects := s.apply(t, isvc, ir)
				before := isvc.Status.DeepCopy()
				out, events := passWith(t, isvc, func(in *Inputs) { in.Now = time.Unix(5000, 0) }, objects...)
				if out.Opened || out.Parked || out.StateChanged {
					t.Fatalf("outcome = %+v, want the run left on its pin", out)
				}
				requireSameRun(t, isvc, id)
				if s.check != nil {
					s.check(t, isvc, out, events)
				} else if !reflect.DeepEqual(before.Rollout.ActiveRun, isvc.Status.Rollout.ActiveRun) || isvc.Status.Rollout.LastRun != nil {
					t.Fatalf("the pin moved:\nbefore %+v\nafter  %+v", before.Rollout, isvc.Status.Rollout)
				}
			})
		}
	}
}

// A referenced body that changes, stops resolving or resolves again while
// its run is pinned never parks the run: the pin keeps executing and
// RolloutPlanDrift reports the live render against the pinned one.
func TestAPinnedRunReportsPolicyDriftAndKeepsExecuting(t *testing.T) {
	for _, adopted := range []bool{false, true} {
		name := "Open"
		if adopted {
			name = "Adopted"
		}
		t.Run(name, func(t *testing.T) {
			isvc, ir := openRefCanary(t, adopted)
			id := isvc.Status.Rollout.ActiveRun.RunID
			pinned := isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest

			out := pass(t, isvc, ir, canaryPolicy(policyName, 50, 100))
			if !emptyOutcome(out) {
				t.Fatalf("policy.changed: outcome = %+v, want nothing", out)
			}
			requireSameRun(t, isvc, id)
			requireDrift(t, isvc, corev1.ConditionTrue, v1beta1.RolloutPlanDriftReasonPolicyNewerThanRun)

			out = pass(t, isvc, ir)
			if !emptyOutcome(out) {
				t.Fatalf("policy.unresolved: outcome = %+v, want the pin kept rather than a park", out)
			}
			requireSameRun(t, isvc, id)
			requireDrift(t, isvc, corev1.ConditionTrue, v1beta1.RolloutPlanDriftReasonPolicyNewerThanRun)
			if !strings.Contains(driftCondition(isvc).Message, "(unresolvable)") {
				t.Fatalf("drift message = %q, want the live render reported unresolvable", driftCondition(isvc).Message)
			}
			if c := planReady(isvc); c == nil || c.Status != corev1.ConditionTrue || c.Reason != v1beta1.RolloutPlanReasonPinned {
				t.Fatalf("RolloutPlanReady = %+v, want Pinned", c)
			}

			pass(t, isvc, ir, canaryPolicy(policyName))
			run := requireSameRun(t, isvc, id)
			requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)
			if run.Plan.Groups[0].PortableDigest != pinned {
				t.Fatal("the pin changed across the policy's edits")
			}
		})
	}
}

// A member whose observed target lands on the revision its own unit rolled
// back is a hold, not a retarget: the retarget check skips it and the run
// continues on its pin.
func TestATargetOnItsUnitsRejectedRevisionIsAHoldUnderAPinnedRun(t *testing.T) {
	for _, adopted := range []bool{false, true} {
		name := "Open"
		if adopted {
			name = "Adopted"
		}
		t.Run(name, func(t *testing.T) {
			isvc := concurrentCanaryISVC(t)
			if adopted {
				// Adoption reads the first canary group's unit only, so the
				// engine group goes first for its ladder to be adopted.
				groups := isvc.Spec.Rollout.Groups
				groups[0], groups[1] = groups[1], groups[0]
				setEngineUnit(isvc, v1beta1.RolloutPhaseCanarying, &v1beta1.CanaryStatus{
					CanaryRevisionHash: hashOf(t, engineNewRev), StableRevisionHash: hashOf(t, engineStableRev), CurrentStep: 0,
				})
			}
			router := namedIR("llm-a-router", routerStableRev, routerStableRev)
			engine := namedIR("llm-a-engine", engineStableRev, engineNewRev)
			out := pass(t, isvc, router, engine)
			if !out.Opened || out.Adopted != adopted {
				t.Fatalf("open = %+v, want adopted=%v", out, adopted)
			}
			id := isvc.Status.Rollout.ActiveRun.RunID

			router.Status.UpdateRevision = routerRejectedRev
			out = pass(t, isvc, router, engine)
			if out.Opened || out.StateChanged || out.Parked {
				t.Fatalf("outcome = %+v, want the run left on its pin", out)
			}
			run := requireSameRun(t, isvc, id)
			if got := pinnedRevision(run.TargetRevisions, v1beta1.RouterComponent); got != hashOf(t, routerStableRev) {
				t.Fatalf("router pinned at %s, want its stable %s", got, hashOf(t, routerStableRev))
			}
		})
	}
}

// A lifecycle partition deliberately holds mixed revisions: a run whose
// unconverged members all belong to a coordination group resting Staged
// closes Completed. The staged member still diverges, so the pass after the
// close opens a fresh run over it and the pass after that closes it again.
func TestAStagedGroupClosesAPinnedRun(t *testing.T) {
	t.Run("Open", func(t *testing.T) {
		isvc := isvcFixture(inlineBlueGreen())
		staged := stragglingIR(oldRev)
		staged.Status.UpdateRevision = newRev
		if out := pass(t, isvc, staged); !out.Opened {
			t.Fatalf("open = %+v", out)
		}
		setCoordinationPhase(isvc, v1beta1.CoordinationPhaseStaged)
		out := pass(t, isvc, staged)
		if !out.StateChanged || v1beta1.RolloutRunActive(isvc) {
			t.Fatalf("outcome = %+v active=%v, want the staged rest to close the run", out, v1beta1.RolloutRunActive(isvc))
		}
		if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunCompleted {
			t.Fatalf("lastRun = %+v, want Completed", last)
		}
		// The window after the close: the member still diverges, so the
		// next pass opens again over it, and the staged rest closes that run
		// in turn.
		if out := pass(t, isvc, staged); !out.Opened {
			t.Fatalf("pass after the close = %+v; want the staged member to reopen a run", out)
		}
		if out := pass(t, isvc, staged); !out.StateChanged || v1beta1.RolloutRunActive(isvc) {
			t.Fatalf("second close = %+v active=%v", out, v1beta1.RolloutRunActive(isvc))
		}
	})
	t.Run("Adopted", func(t *testing.T) {
		isvc := isvcFixture(inlineCanary())
		isvc.Spec.Rollout.Groups = append(isvc.Spec.Rollout.Groups, v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{},
		})
		setEngineUnit(isvc, v1beta1.RolloutPhaseCanarying, midLadderRecord(t))
		decoder := namedIR("llm-a-decoder", "llm-a-decoder-dddddddd", "llm-a-decoder-eeeeeeee")
		decoder.Status.Replicas, decoder.Status.UpdatedReplicas = 2, 1
		if out := pass(t, isvc, divergedIR(oldRev, newRev), decoder); !out.Adopted {
			t.Fatalf("open = %+v, want adopted", out)
		}
		done := midLadderRecord(t)
		done.CurrentStep, done.ObservedTrafficWeight = 2, 100
		setEngineUnit(isvc, v1beta1.RolloutPhaseStable, done)
		isvc.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{Groups: []v1beta1.RolloutCoordinationGroupStatus{
			{Name: "1", Phase: v1beta1.CoordinationPhaseStaged, CompositePhase: string(v1beta1.CoordinationPhaseStaged)},
		}}
		out := pass(t, isvc, settledIR(newRev), decoder)
		if !out.StateChanged || v1beta1.RolloutRunActive(isvc) {
			t.Fatalf("outcome = %+v active=%v, want the staged rest to close the adopted run", out, v1beta1.RolloutRunActive(isvc))
		}
		if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunCompleted {
			t.Fatalf("lastRun = %+v, want Completed", last)
		}
		if out := pass(t, isvc, settledIR(newRev), decoder); !out.Opened {
			t.Fatalf("pass after the close = %+v; want the staged member to reopen a run", out)
		}
	})
}

// A repin applied to an adopted run keeps the run's identity and progress:
// the plan is replaced under the same RunID with PinnedAt advanced and the
// unit's step clamped into the new ladder. The consuming pass reads the
// annotation's latest value, so a rewrite before it runs is what applies.
func TestARepinOnAnAdoptedRunKeepsItsIdentity(t *testing.T) {
	isvc, ir := adoptInlineCanary(t)
	run := requirePinned(t, isvc)
	id, pinnedAt := run.RunID, run.PinnedAt
	isvc.Spec.Rollout.Groups[0].Canary = canaryBody(25, 100)
	annotate(isvc, constants.RolloutRepinAnnotation, "rp1:stale")
	annotate(isvc, constants.RolloutRepinAnnotation, "now")

	out := passAt(t, isvc, time.Unix(2000, 0), ir)
	if !out.StateChanged || out.Opened {
		t.Fatalf("outcome = %+v, want the repin applied as a boundary", out)
	}
	if _, still := isvc.Annotations[constants.RolloutRepinAnnotation]; still {
		t.Fatal("the applied repin left its annotation")
	}
	run = requireSameRun(t, isvc, id)
	if !run.PinnedAt.After(pinnedAt.Time) || run.Plan.Groups[0].Group.Canary.Steps[0].Traffic != 25 {
		t.Fatalf("run = %+v, want the new ladder pinned with PinnedAt advanced", run)
	}
	if cs := isvc.Status.Components[v1beta1.EngineComponent].Canary; cs.CurrentStep != 0 || cs.CanaryRevisionHash != hashOf(t, newRev) {
		t.Fatalf("record = %+v, want the adopted progress kept", cs)
	}
	requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)
}

// A pinned run, fresh or adopted, retargets the same way: a target that
// moves off the pin closes it Superseded and opens a fresh run (never an
// adoption) with the unit's ladder restarted by the bind; a revert that
// leaves Instances trailing opens the fresh run toward the restored
// revision. The pass after finds the fresh pin as it is.
func TestAPinnedRunRetargetsAndTheNextPassFindsTheFreshPin(t *testing.T) {
	moves := map[string]func(t *testing.T) (*v1beta1.InferenceReplica, string){
		"new": func(t *testing.T) (*v1beta1.InferenceReplica, string) {
			return divergedIR(oldRev, thirdRev), hashOf(t, thirdRev)
		},
		"revert": func(t *testing.T) (*v1beta1.InferenceReplica, string) {
			return irPublished(t, oldRev, oldRev, []workloadtypes.InstanceStatus{rowOn(0, oldRev), rowOn(1, newRev)}), hashOf(t, oldRev)
		},
	}
	for startName, start := range pinnedStarts {
		for moveName, move := range moves {
			t.Run(startName+"/"+moveName, func(t *testing.T) {
				isvc, _ := start(t)
				id := isvc.Status.Rollout.ActiveRun.RunID
				ir, want := move(t)
				out := pass(t, isvc, ir)
				if !out.Opened || out.Adopted {
					t.Fatalf("outcome = %+v, want a fresh open", out)
				}
				run := requirePinned(t, isvc)
				if run.RunID == id || pinnedRevision(run.TargetRevisions, v1beta1.EngineComponent) != want {
					t.Fatalf("run = %+v, want a new run at %s", run, want)
				}
				if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunSuperseded {
					t.Fatalf("lastRun = %+v, want Superseded", last)
				}
				fresh := run.RunID
				if out := pass(t, isvc, ir); out.Opened || out.StateChanged {
					t.Fatalf("the pass after the retarget = %+v, want the fresh pin found", out)
				}
				requireSameRun(t, isvc, fresh)
			})
		}
	}
}

// An adopted canary run closes Completed only once its unit is done and
// reads Stable with nothing trailing: a converged replica under a ladder
// still short of its end keeps the run pinned, trailing Instances keep it
// pinned past the done sentinel, and a replica whose status trails its spec
// defers the judgement for ten seconds. After the close a pass finds the
// record and opens nothing.
func TestAnAdoptedRunClosesOnceItsUnitIsDone(t *testing.T) {
	isvc, _ := adoptInlineCanary(t)
	id := isvc.Status.Rollout.ActiveRun.RunID

	if out := pass(t, isvc, settledIR(newRev)); out.StateChanged {
		t.Fatalf("ir.converged under a ladder mid-flight closed the run: %+v", out)
	}
	requireSameRun(t, isvc, id)

	done := midLadderRecord(t)
	done.CurrentStep, done.ObservedTrafficWeight = 2, 100
	setEngineUnit(isvc, v1beta1.RolloutPhaseStable, done)
	if out := pass(t, isvc, stragglingIR(newRev)); out.StateChanged {
		t.Fatalf("ir.straggling past the done sentinel closed the run: %+v", out)
	}
	requireSameRun(t, isvc, id)

	stale := settledIR(newRev)
	stale.Status.ObservedGeneration = 0
	before := isvc.Status.DeepCopy()
	if out := passAt(t, isvc, time.Unix(5000, 0), stale); out.RequeueAfter != shortRequeue || out.StateChanged || !reflect.DeepEqual(before, &isvc.Status) {
		t.Fatalf("ir.stale: outcome = %+v, want a ten-second wait with nothing written", out)
	}

	out := pass(t, isvc, settledIR(newRev))
	if !out.StateChanged || v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("outcome = %+v active=%v, want the Completed close", out, v1beta1.RolloutRunActive(isvc))
	}
	last := isvc.Status.Rollout.LastRun
	if last == nil || last.Outcome != v1beta1.RolloutRunCompleted {
		t.Fatalf("lastRun = %+v, want Completed", last)
	}
	if out := pass(t, isvc, settledIR(newRev)); out.Opened || !reflect.DeepEqual(last, isvc.Status.Rollout.LastRun) {
		t.Fatalf("the pass after the close reopened or rewrote the record: %+v %+v", out, isvc.Status.Rollout.LastRun)
	}
	requireNoRun(t, isvc, v1beta1.RolloutPlanReasonNoRun)
}

// A closed run stays closed: the pass after a Completed close (by
// convergence or by the unit's completion) or a RolledBack close finds the
// record, opens nothing and leaves it untouched.
func TestAClosedRunStaysClosedOnTheNextPass(t *testing.T) {
	cases := map[string]func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object){
		"ir.converged": func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
			isvc := isvcFixture(inlineBlueGreen())
			pass(t, isvc, divergedIR(oldRev, newRev))
			return isvc, []runtime.Object{settledIR(newRev)}
		},
		"canary.completed": func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
			isvc, _ := openInlineCanary(t)
			done := midLadderRecord(t)
			done.CurrentStep, done.ObservedTrafficWeight = 2, 100
			setEngineUnit(isvc, v1beta1.RolloutPhaseStable, done)
			return isvc, []runtime.Object{settledIR(newRev)}
		},
		"canary.rolledBack[settled]": func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
			isvc, _ := openInlineCanary(t)
			setEngineUnit(isvc, v1beta1.RolloutPhaseRolledBack, rejectedRecord(t))
			return isvc, []runtime.Object{divergedIR(oldRev, newRev)}
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			isvc, objects := setup(t)
			if out := pass(t, isvc, objects...); !out.StateChanged || v1beta1.RolloutRunActive(isvc) {
				t.Fatalf("close = %+v active=%v", out, v1beta1.RolloutRunActive(isvc))
			}
			last := isvc.Status.Rollout.LastRun.DeepCopy()
			for i := 0; i < 2; i++ {
				if out := pass(t, isvc, objects...); out.Opened || out.StateChanged || !reflect.DeepEqual(last, isvc.Status.Rollout.LastRun) {
					t.Fatalf("pass %d after the close = %+v, lastRun %+v", i, out, isvc.Status.Rollout.LastRun)
				}
			}
			requireNoRun(t, isvc, v1beta1.RolloutPlanReasonNoRun)
		})
	}
}
