package rolloutrun

import (
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/rollout"
)

// Tests for a pinned run, fresh (Open) or adopted around a ladder that
// was already in flight (Adopted). From the pass after the one that pinned
// it an adopted run is judged exactly as a fresh one, so most cases run the
// same story from both starts.

// pinnedStart opens a run and returns the service with its diverged replica.
type pinnedStart func(t *testing.T) (*v1beta1.InferenceService, *v1beta1.InferenceReplica)

// openInlineCanary opens a fresh run over an inline canary group.
func openInlineCanary(t *testing.T) (*v1beta1.InferenceService, *v1beta1.InferenceReplica) {
	t.Helper()
	isvc, ir := isvcFixture(inlineCanary()), divergedIR(oldRev, newRev)
	if out := pass(t, isvc, ir); !out.Opened || out.Adopted {
		t.Fatalf("fixture: %+v, want a fresh open", out)
	}
	return isvc, ir
}

// adoptInlineCanary opens a run around a unit found short of its last step with no run on record.
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
				return []runtime.Object{ir}
			},
			check: func(t *testing.T, isvc *v1beta1.InferenceService, out Outcome, events []string) {
				requireDrift(t, isvc, corev1.ConditionTrue, v1beta1.RolloutPlanDriftReasonSpecNewerThanRun)
				if out.RequeueAfter != shortRequeue || !hasEvent(events, EventGroupRemoved) {
					t.Fatalf("outcome = %+v events = %v, want the ten-second requeue and the removed-group warning", out, events)
				}
				if isvc.Status.Rollout.LastRun != nil {
					t.Fatal("a roll still pending closed the run")
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
					t.Fatalf("outcome = %+v lastRun = %+v, want the run kept with no decision", out, isvc.Status.Rollout.LastRun)
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

// A run whose groups left the spec is judged by the members it recorded, so
// it still reaches its end: Completed once its unit finishes, RolledBack
// once its unit's revert settles, and Superseded when a recorded member
// retargets. No run opens after the close, since nothing is declared.
func TestARunWhoseGroupsLeftTheSpecStillCloses(t *testing.T) {
	type story struct {
		name    string
		apply   func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object
		outcome v1beta1.RolloutRunOutcome
	}
	stories := []story{
		{
			name: "canary.completed",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object {
				cs := midLadderRecord(t)
				cs.CurrentStep, cs.ObservedTrafficWeight, cs.StableRevisionHash = 2, 100, ""
				setEngineUnit(isvc, v1beta1.RolloutPhaseStable, cs)
				return []runtime.Object{settledIR(newRev)}
			},
			outcome: v1beta1.RolloutRunCompleted,
		},
		{
			name: "canary.rolledBack[settled]",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object {
				// Every Instance is back on the stable revision while the
				// replica's spec target still names the rejected one.
				setEngineUnit(isvc, v1beta1.RolloutPhaseRolledBack, rejectedRecord(t))
				return []runtime.Object{divergedIR(oldRev, newRev)}
			},
			outcome: v1beta1.RolloutRunRolledBack,
		},
		{
			name: "spec.revision[new]",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) []runtime.Object {
				return []runtime.Object{divergedIR(oldRev, thirdRev)}
			},
			outcome: v1beta1.RolloutRunSuperseded,
		},
	}
	for startName, start := range pinnedStarts {
		for _, s := range stories {
			t.Run(startName+"/"+s.name, func(t *testing.T) {
				isvc, ir := start(t)
				pass(t, isvc, ir) // the pass after the open; an adopted run is Open from here on
				id := isvc.Status.Rollout.ActiveRun.RunID
				isvc.Spec.Rollout = nil
				pass(t, isvc, ir) // the removal is read: drift, and the roll still pending keeps the run open
				requireSameRun(t, isvc, id)
				requireDrift(t, isvc, corev1.ConditionTrue, v1beta1.RolloutPlanDriftReasonSpecNewerThanRun)
				objects := s.apply(t, isvc)
				out, events := passWith(t, isvc, nil, objects...)
				if !out.StateChanged || out.Opened || v1beta1.RolloutRunActive(isvc) {
					t.Fatalf("outcome = %+v active=%v, want the run closed and nothing opened", out, v1beta1.RolloutRunActive(isvc))
				}
				if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != s.outcome {
					t.Fatalf("lastRun = %+v, want the %s close", last, s.outcome)
				}
				if !hasEvent(events, EventRunClosed) {
					t.Fatalf("events = %v, want %s", events, EventRunClosed)
				}
				requireNoRun(t, isvc, v1beta1.RolloutPlanReasonNoRun)
				requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)
				if out := pass(t, isvc, objects...); !emptyOutcome(out) || v1beta1.RolloutRunActive(isvc) {
					t.Fatalf("the pass after the close = %+v active=%v, want nothing: no group is declared", out, v1beta1.RolloutRunActive(isvc))
				}
			})
		}
	}
}

// A run whose groups left the spec before its unit ever armed closes
// Completed once every recorded member rests on the recorded revision.
func TestARunWhoseGroupsLeftTheSpecClosesOnceItsRecordedMembersConverge(t *testing.T) {
	isvc, ir := openInlineCanary(t)
	pass(t, isvc, ir)
	isvc.Spec.Rollout = nil
	pass(t, isvc, ir)
	requireDrift(t, isvc, corev1.ConditionTrue, v1beta1.RolloutPlanDriftReasonSpecNewerThanRun)
	out := pass(t, isvc, settledIR(newRev))
	if !out.StateChanged || v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("outcome = %+v active=%v, want the Completed close", out, v1beta1.RolloutRunActive(isvc))
	}
	if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunCompleted {
		t.Fatalf("lastRun = %+v, want Completed", last)
	}
	requireNoRun(t, isvc, v1beta1.RolloutPlanReasonNoRun)
	requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)
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
// closes Completed. The staged member stays off its target under the
// partition, and the passes after the close read a member whose rows rest
// at the shape its partition asks for as no pending work, so no run opens
// again and the record of the close stands until the spec changes.
func TestAStagedGroupClosesAPinnedRun(t *testing.T) {
	t.Run("Open", func(t *testing.T) {
		isvc := isvcFixture(inlineBlueGreen())
		if out := pass(t, isvc, divergedIR(oldRev, newRev)); !out.Opened {
			t.Fatalf("open = %+v", out)
		}
		staged := stagedIR()
		setCoordinationPhase(isvc, v1beta1.CoordinationPhaseStaged)
		out := pass(t, isvc, staged)
		if !out.StateChanged || v1beta1.RolloutRunActive(isvc) {
			t.Fatalf("outcome = %+v active=%v, want the staged rest to close the run", out, v1beta1.RolloutRunActive(isvc))
		}
		if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunCompleted {
			t.Fatalf("lastRun = %+v, want Completed", last)
		}
		requireClosedRunStands(t, isvc, staged)
	})
	t.Run("Adopted", func(t *testing.T) {
		isvc := isvcFixture(inlineCanary())
		isvc.Spec.Rollout.Groups = append(isvc.Spec.Rollout.Groups, v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{},
		})
		setEngineUnit(isvc, v1beta1.RolloutPhaseCanarying, midLadderRecord(t))
		decoder := namedIR("llm-a-decoder", "llm-a-decoder-dddddddd", "llm-a-decoder-eeeeeeee")
		decoder.Status.Replicas, decoder.Status.UpdatedReplicas = 2, 1
		decoder.Spec.Lifecycle = stagedIR().Spec.Lifecycle
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
		requireClosedRunStands(t, isvc, settledIR(newRev), decoder)
	})
}

// requireClosedRunStands runs two passes over a service whose last run
// closed on a group resting Staged and holds the run closed: nothing opens,
// no boundary moves, and the record of the close is the same afterwards.
func requireClosedRunStands(t *testing.T, isvc *v1beta1.InferenceService, objects ...runtime.Object) {
	t.Helper()
	closed := isvc.Status.Rollout.LastRun.DeepCopy()
	for i := 1; i <= 2; i++ {
		if out := pass(t, isvc, objects...); out.Opened || out.StateChanged || out.RequeueAfter != 0 {
			t.Fatalf("pass %d after the close = %+v; want the staged member to open nothing", i, out)
		}
		requireNoRun(t, isvc, v1beta1.RolloutPlanReasonNoRun)
	}
	if !reflect.DeepEqual(closed, isvc.Status.Rollout.LastRun) {
		t.Fatalf("the record of the close moved: %+v -> %+v", closed, isvc.Status.Rollout.LastRun)
	}
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
		t.Fatalf("ir.converged under a canary mid-flight closed the run: %+v", out)
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

// A run that records no stable revision for a member, a decoder created in
// the same change that put it in the canary group, closes RolledBack when the
// unit's revert settles like any other, and the close writes nothing on the
// member: it leaves the open run with its record as the dispatch left it.
// The dispatch records such a member on the revision its replica settled on
// while the unit rests on the rejection, in the pass the revert completes,
// so a member whose replica settled by then leaves the run recorded; one
// whose replica has not settled leaves it with no rolled-out revision and
// is recorded once it does. Either way the closed run's record carries the
// member with no stable revision.
func TestARolledBackCloseKeepsAStableLessMembersRecordAsTheDispatchLeftIt(t *testing.T) {
	const decoderRev = "llm-a-decoder-dddddddd"
	const decoderRecord = "llm-a-decoder-rev-dddddddd"
	for name, recorded := range map[string]bool{"recorded before the close": true, "not settled by the close": false} {
		t.Run(name, func(t *testing.T) {
			isvc := isvcFixture(v1beta1.RolloutGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Canary:     canaryBody(10, 100),
			})
			one := 1
			isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &one}}
			setEngineUnit(isvc, v1beta1.RolloutPhaseRolledBack, &v1beta1.CanaryStatus{
				CanaryRevisionHash:     hashOf(t, newRev),
				StableRevisionHash:     hashOf(t, oldRev),
				RolledBackRevisionHash: hashOf(t, newRev),
			})
			want := v1beta1.ComponentStatusSpec{}
			if recorded {
				want = v1beta1.ComponentStatusSpec{LatestRolledoutRevision: decoderRecord, LatestReadyRevision: decoderRecord}
			}
			isvc.Status.Components[v1beta1.DecoderComponent] = want
			isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
				RunID: "llm-a-run",
				TargetRevisions: []v1beta1.RolloutRunTarget{
					{Component: v1beta1.EngineComponent, Revision: hashOf(t, newRev), StableRevision: hashOf(t, oldRev)},
					{Component: v1beta1.DecoderComponent, Revision: hashOf(t, decoderRev)},
				},
				Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{Group: isvc.Spec.Rollout.Groups[0]}}},
			}}
			engine := namedIR("llm-a-engine", oldRev, newRev)
			decoder := namedIR("llm-a-decoder", decoderRev, decoderRev)
			decoder.Status.Replicas, decoder.Status.UpdatedReplicas = 1, 1

			out := pass(t, isvc, engine, decoder)
			if !out.StateChanged || v1beta1.RolloutRunActive(isvc) {
				t.Fatalf("the settled revert did not close the run: %+v", out)
			}
			last := isvc.Status.Rollout.LastRun
			if last == nil || last.Outcome != v1beta1.RolloutRunRolledBack {
				t.Fatalf("lastRun = %+v, want RolledBack", last)
			}
			if got := pinnedRevision(last.TargetRevisions, v1beta1.DecoderComponent); got != hashOf(t, decoderRev) {
				t.Fatalf("the record names the decoder at %q, want its only revision %s", got, hashOf(t, decoderRev))
			}
			for _, target := range last.TargetRevisions {
				if target.Component == v1beta1.DecoderComponent && target.StableRevision != "" {
					t.Fatalf("the record gave the decoder a stable revision it never had: %+v", target)
				}
			}
			if rec := isvc.Status.Components[v1beta1.DecoderComponent]; !reflect.DeepEqual(rec, want) {
				t.Fatalf("the close moved the stable-less member's record: %+v, want %+v", rec, want)
			}
			// The pass after the close finds the hold and the record as they are.
			if next := pass(t, isvc, engine, decoder); next.Opened || next.StateChanged || v1beta1.RolloutRunActive(isvc) {
				t.Fatalf("the pass after the close reopened: %+v", next)
			}
			if rec := isvc.Status.Components[v1beta1.DecoderComponent]; !reflect.DeepEqual(rec, want) {
				t.Fatalf("the pass after the close moved the member's record: %+v, want %+v", rec, want)
			}
		})
	}
}

// modesWith is the deployment-mode map the controller resolves for a pass:
// the engine and the decoder in the OMENative mode, except the Components
// named as left, which run as RawDeployment.
func modesWith(left ...v1beta1.ComponentType) map[v1beta1.ComponentType]constants.DeploymentModeType {
	modes := map[v1beta1.ComponentType]constants.DeploymentModeType{
		v1beta1.EngineComponent:  constants.OMENative,
		v1beta1.DecoderComponent: constants.OMENative,
	}
	for _, c := range left {
		modes[c] = constants.RawDeployment
	}
	return modes
}

// passUnderModes is a pass that reads the given deployment modes.
func passUnderModes(t *testing.T, isvc *v1beta1.InferenceService, modes map[v1beta1.ComponentType]constants.DeploymentModeType, objects ...runtime.Object) (Outcome, []string) {
	t.Helper()
	return passWith(t, isvc, func(in *Inputs) { in.ComponentDeploymentModes = modes }, objects...)
}

// withDecoder declares a decoder beside the fixture's engine.
func withDecoder(isvc *v1beta1.InferenceService) *v1beta1.InferenceService {
	one := 1
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &one}}
	return isvc
}

const decoderCurrentRev, decoderTargetRev = "llm-a-decoder-dddddddd", "llm-a-decoder-eeeeeeee"

// pairStart opens a run over an engine and a decoder in one group, both
// diverged: fresh over a blueGreen group, or adopted around the engine's
// canary record short of its last step.
type pairStart func(t *testing.T) (*v1beta1.InferenceService, *v1beta1.InferenceReplica, *v1beta1.InferenceReplica)

var pairStarts = map[string]pairStart{
	"Open": func(t *testing.T) (*v1beta1.InferenceService, *v1beta1.InferenceReplica, *v1beta1.InferenceReplica) {
		t.Helper()
		isvc := withDecoder(isvcFixture(v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}}))
		engine, decoder := divergedIR(oldRev, newRev), divergedDecoderIR()
		if out := pass(t, isvc, engine, decoder); !out.Opened || out.Adopted {
			t.Fatalf("fixture: %+v, want a fresh open", out)
		}
		return isvc, engine, decoder
	},
	"Adopted": func(t *testing.T) (*v1beta1.InferenceService, *v1beta1.InferenceReplica, *v1beta1.InferenceReplica) {
		t.Helper()
		isvc := withDecoder(isvcFixture(v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Canary: canaryBody(10, 100)}))
		setEngineUnit(isvc, v1beta1.RolloutPhaseCanarying, midLadderRecord(t))
		engine, decoder := divergedIR(oldRev, newRev), divergedDecoderIR()
		if out := pass(t, isvc, engine, decoder); !out.Opened || !out.Adopted {
			t.Fatalf("fixture: %+v, want an adopted open", out)
		}
		return isvc, engine, decoder
	},
}

// divergedDecoderIR is the decoder's replica with its target moved off its
// current revision and nothing rolled yet.
func divergedDecoderIR() *v1beta1.InferenceReplica {
	ir := namedIR("llm-a-decoder", decoderCurrentRev, decoderTargetRev)
	ir.Status.Replicas, ir.Status.UpdatedReplicas = 2, 0
	return ir
}

// settledDecoderIR is the decoder's replica resting on one revision.
func settledDecoderIR(rev string) *v1beta1.InferenceReplica {
	ir := namedIR("llm-a-decoder", rev, rev)
	ir.Status.Replicas, ir.Status.UpdatedReplicas = 2, 2
	return ir
}

// engineConverged is the engine's replica settled on newRev; under a canary
// start the unit's record reads done and the phase Stable, as the executor
// leaves them once the cutover lands.
func engineConverged(t *testing.T, isvc *v1beta1.InferenceService) *v1beta1.InferenceReplica {
	t.Helper()
	if cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent); cs != nil {
		cs.CurrentStep = 2
		setEngineUnit(isvc, v1beta1.RolloutPhaseStable, cs)
	}
	return settledIR(newRev)
}

// requireClosed checks that no run is open and the last run closed with
// the outcome.
func requireClosed(t *testing.T, isvc *v1beta1.InferenceService, outcome v1beta1.RolloutRunOutcome) *v1beta1.RolloutRunRecord {
	t.Helper()
	if v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("a run is still open: %+v", isvc.Status.Rollout.ActiveRun)
	}
	last := isvc.Status.Rollout.LastRun
	if last == nil || last.Outcome != outcome {
		t.Fatalf("lastRun = %+v, want %s", last, outcome)
	}
	return last
}

// A run whose only recorded member leaves the OMENative mode closes. The
// engine rolls to version 2 under a run; the operator switches the engine
// to another deployment mode and, as admission requires of a member that
// is not OMENative, takes it out of the rollout group in the same write,
// putting a decoder group in its place. Nothing of the engine can retarget
// or converge again: the cleanup deletes its replica. So the run layer
// reads the engine as departed from the deployment modes resolved for the
// pass and closes the run as replaced on that pass, ahead of the cleanup
// and without reading the engine's replica. The record's shape under the
// run does not change the reading: a stable the history carries, no
// stable, and a stable the history does not carry close the same way, as
// does a run adopted around the engine's canary. The next pass finds
// nothing to roll, and the decoder's own push then opens the decoder
// group's run.
func TestARunClosesOnceItsOnlyMemberLeftTheMode(t *testing.T) {
	const unretainedRev = "llm-a-engine-0badc0de"
	firstRollout := func() *v1beta1.InferenceReplica {
		ir := irFixture("", newRev)
		ir.Status.Replicas, ir.Status.UpdatedReplicas = 2, 0
		return ir
	}
	shapes := []struct {
		name    string
		group   v1beta1.RolloutGroup
		open    func(isvc *v1beta1.InferenceService) *v1beta1.InferenceReplica
		stable  string
		adopted bool
	}{
		{
			name:   "Open/a stable the history carries",
			group:  inlineBlueGreen(),
			open:   func(*v1beta1.InferenceService) *v1beta1.InferenceReplica { return divergedIR(oldRev, newRev) },
			stable: hashOf(t, oldRev),
		},
		{
			// The engine's first rollout: no current revision and nothing on record.
			name:   "Open/no stable",
			group:  inlineBlueGreen(),
			open:   func(*v1beta1.InferenceService) *v1beta1.InferenceReplica { return firstRollout() },
			stable: "",
		},
		{
			// No current revision; the record names a hash no ControllerRevision carries.
			name:  "Open/a stable the history does not carry",
			group: inlineBlueGreen(),
			open: func(isvc *v1beta1.InferenceService) *v1beta1.InferenceReplica {
				isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
					v1beta1.EngineComponent: {LatestRolledoutRevision: unretainedRev},
				}
				return firstRollout()
			},
			stable: hashOf(t, unretainedRev),
		},
		{
			name:  "Adopted",
			group: inlineCanary(),
			open: func(isvc *v1beta1.InferenceService) *v1beta1.InferenceReplica {
				setEngineUnit(isvc, v1beta1.RolloutPhaseCanarying, midLadderRecord(t))
				return divergedIR(oldRev, newRev)
			},
			stable:  hashOf(t, oldRev),
			adopted: true,
		},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			isvc := withDecoder(isvcFixture(shape.group))
			engine := shape.open(isvc)
			if out := pass(t, isvc, engine); !out.Opened || out.Adopted != shape.adopted {
				t.Fatalf("fixture: %+v, want an open with adopted=%v", out, shape.adopted)
			}
			id := requirePinned(t, isvc).RunID
			if got := pinnedTargetFor(t, requirePinned(t, isvc), v1beta1.EngineComponent); got.Revision != hashOf(t, newRev) || got.StableRevision != shape.stable {
				t.Fatalf("fixture recorded %+v, want %s over stable %q", got, hashOf(t, newRev), shape.stable)
			}
			// The engine leaves the mode: taken out of the rollout group, a
			// decoder group in its place. The run layer runs ahead of the
			// cleanup, so the engine's replica and status are still there.
			isvc.Spec.Rollout.Groups = []v1beta1.RolloutGroup{{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}}}
			decoder := settledDecoderIR(decoderCurrentRev)
			out, events := passUnderModes(t, isvc, modesWith(v1beta1.EngineComponent), engine, decoder)
			if !out.StateChanged || out.Opened || out.Parked {
				t.Fatalf("the switch pass: outcome = %+v, want the run closed", out)
			}
			last := requireClosed(t, isvc, v1beta1.RolloutRunSuperseded)
			if got := pinnedRevision(last.TargetRevisions, v1beta1.EngineComponent); got != hashOf(t, newRev) {
				t.Fatalf("the record names the engine at %q, want %s", got, hashOf(t, newRev))
			}
			requireNoRun(t, isvc, v1beta1.RolloutPlanReasonNoRun)
			requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)
			if !hasEvent(events, EventRunClosed) || !hasEvent(events, "left the OMENative mode") {
				t.Fatalf("events = %v, want the close naming the engine that left the mode", events)
			}
			// The cleanup deletes the engine's replica and clears its status.
			if isvc.Status.Components == nil {
				isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
			}
			isvc.Status.Components[v1beta1.EngineComponent] = v1beta1.ComponentStatusSpec{}
			// The pass after the close finds nothing to roll: the decoder's
			// pair agrees and no record is left to adopt.
			if out, _ := passUnderModes(t, isvc, modesWith(v1beta1.EngineComponent), decoder); !emptyOutcome(out) || v1beta1.RolloutRunActive(isvc) {
				t.Fatalf("the pass after the close: outcome = %+v, want nothing opened", out)
			}
			if id == "" || isvc.Status.Rollout.LastRun.Outcome != v1beta1.RolloutRunSuperseded {
				t.Fatalf("the close was rewritten: %+v", isvc.Status.Rollout.LastRun)
			}
			// The decoder's push opens the decoder group's own run.
			if out, _ := passUnderModes(t, isvc, modesWith(v1beta1.EngineComponent), divergedDecoderIR()); !out.Opened {
				t.Fatalf("the decoder's push: outcome = %+v, want a run opened for the decoder", out)
			}
			run := requirePinned(t, isvc)
			if run.RunID == id || len(run.TargetRevisions) != 1 {
				t.Fatalf("the fresh run = %+v, want a new run over the decoder alone", run)
			}
			if got := pinnedTargetFor(t, run, v1beta1.DecoderComponent); got.Revision != hashOf(t, decoderTargetRev) || got.StableRevision != hashOf(t, decoderCurrentRev) {
				t.Fatalf("the fresh run records %+v, want the decoder at %s over %s", got, hashOf(t, decoderTargetRev), hashOf(t, decoderCurrentRev))
			}
		})
	}
}

// A run with two recorded members is decided on the one that stays in the
// mode. The engine and the decoder roll to version 2 in one group; the
// operator switches the decoder to another deployment mode and takes it
// out of the group. The run stays open under drift, since the group lost a
// member, and the decoder is read neither for freshness nor for the close:
// its replica, still being deleted with a projection generation behind the
// service, holds no pass. Once the engine converges the run closes
// Completed.
func TestARunIsDecidedOnTheMembersThatStayedInTheMode(t *testing.T) {
	for name, start := range pairStarts {
		t.Run(name, func(t *testing.T) {
			isvc, engine, decoder := start(t)
			id := requirePinned(t, isvc).RunID
			isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent}
			isvc.Generation = 3
			decoder.Annotations = map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: "2"}
			out, _ := passUnderModes(t, isvc, modesWith(v1beta1.DecoderComponent), engine, decoder)
			if out.StateChanged || out.Opened || out.RequeueAfter != 0 {
				t.Fatalf("the switch pass: outcome = %+v, want the run kept with no wait on the decoder", out)
			}
			requireSameRun(t, isvc, id)
			requireDrift(t, isvc, corev1.ConditionTrue, v1beta1.RolloutPlanDriftReasonSpecNewerThanRun)
			// The engine converges; the decoder is not waited on.
			out, _ = passUnderModes(t, isvc, modesWith(v1beta1.DecoderComponent), engineConverged(t, isvc), decoder)
			if !out.StateChanged || out.Opened {
				t.Fatalf("the engine's convergence: outcome = %+v, want the run closed", out)
			}
			requireClosed(t, isvc, v1beta1.RolloutRunCompleted)
		})
	}
}

// A group whose members change under an unchanged progression body is
// drift, and a plan reread takes it. The engine and the decoder roll to
// version 2 in one group; the operator takes the decoder out of the group,
// both still OMENative. The digests cover the bodies alone, so the drift
// reads the members: RolloutPlanDrift names the members before and after.
// A plan reread replaces the plan under the same run with the members the
// spec names now, and the decoder, in no recorded group any more, is no
// longer read: the run closes Completed once the engine converges, and the
// decoder's divergence, in no group, opens nothing.
func TestAMemberSwapUnderAnUnchangedBodyIsDriftAndAPlanRereadTakesIt(t *testing.T) {
	for name, start := range pairStarts {
		t.Run(name, func(t *testing.T) {
			isvc, engine, decoder := start(t)
			run := requirePinned(t, isvc)
			id, recordedAt, oldDigest := run.RunID, run.PinnedAt, run.Plan.Groups[0].PortableDigest
			isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent}
			out, _ := passUnderModes(t, isvc, modesWith(), engine, decoder)
			if out.StateChanged || out.Opened {
				t.Fatalf("the swap: outcome = %+v, want the run kept under drift", out)
			}
			requireSameRun(t, isvc, id)
			requireDrift(t, isvc, corev1.ConditionTrue, v1beta1.RolloutPlanDriftReasonSpecNewerThanRun)
			if msg := driftCondition(isvc).Message; !strings.Contains(msg, "members changed from [engine, decoder] to [engine]") {
				t.Fatalf("drift message = %q, want the members before and after", msg)
			}
			// A plan reread takes the swap: the plan is replaced under the same run.
			annotate(isvc, constants.RolloutRepinAnnotation, "now")
			out, events := passWith(t, isvc, func(in *Inputs) {
				in.Now = time.Unix(2000, 0)
				in.ComponentDeploymentModes = modesWith()
			}, engine, decoder)
			if !out.StateChanged || out.Opened {
				t.Fatalf("the plan reread: outcome = %+v, want the plan replaced", out)
			}
			if _, still := isvc.Annotations[constants.RolloutRepinAnnotation]; still {
				t.Fatal("the plan reread left its annotation")
			}
			run = requireSameRun(t, isvc, id)
			if !run.PinnedAt.After(recordedAt.Time) || run.Plan.Groups[0].PortableDigest != oldDigest || !sameMembers(run.Plan.Groups[0].Group.Components, []v1beta1.ComponentType{v1beta1.EngineComponent}) {
				t.Fatalf("run = %+v, want the same body recorded again over the engine alone", run)
			}
			if !hasEvent(events, EventPlanRepinned) || !hasEvent(events, "members are now groups[0]=[engine]") {
				t.Fatalf("events = %v, want the plan reread naming the members", events)
			}
			if out, _ := passUnderModes(t, isvc, modesWith(), engine, decoder); out.StateChanged || out.Opened {
				t.Fatalf("the pass after the plan reread: outcome = %+v, want the run kept", out)
			}
			requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)
			// The engine converges: the decoder, in no recorded group, is not waited on.
			out, _ = passUnderModes(t, isvc, modesWith(), engineConverged(t, isvc), decoder)
			if !out.StateChanged || out.Opened {
				t.Fatalf("the engine's convergence: outcome = %+v, want the run closed", out)
			}
			requireClosed(t, isvc, v1beta1.RolloutRunCompleted)
			// The decoder diverges in no group: nothing opens for it.
			if out, _ := passUnderModes(t, isvc, modesWith(), settledIR(newRev), decoder); out.Opened || v1beta1.RolloutRunActive(isvc) {
				t.Fatalf("the pass after the close: outcome = %+v, want nothing opened for an ungrouped decoder", out)
			}
		})
	}
}

// A referenced policy whose body changes while its run is open changes
// nothing about the run: the same run id, the recorded body executing, and
// the drift condition naming the newer policy until the next open composes
// it.
func TestAnOpenRunKeepsItsRecordedPlanWhenThePolicyBodyChanges(t *testing.T) {
	isvc, ir := openRefCanary(t, false)
	id := isvc.Status.Rollout.ActiveRun.RunID
	out := pass(t, isvc, ir, canaryPolicy(policyName, 50, 100))
	if !emptyOutcome(out) {
		t.Fatalf("outcome = %+v, want nothing after a policy body change", out)
	}
	requireSameRun(t, isvc, id)
	requireDrift(t, isvc, corev1.ConditionTrue, v1beta1.RolloutPlanDriftReasonPolicyNewerThanRun)
}

// A close that opens nothing is a run boundary like any other: the pass
// reports the state change, so the controller writes the close before
// anything fallible runs, and the close event says what follows it. Two
// engines in a blueGreen group: version 2 is pushed and the run opens;
// before any pod moved the template returns to version 1, so the target is
// back on the revision every Instance runs with nothing trailing. The run
// closes as replaced, the event says nothing diverges and no run opens, the
// plan conditions read NoActiveRun and InSync, and the next pass finds the
// record and decides nothing. A retarget whose plan no longer composes
// closes the same way: the outcome reports the boundary beside the hold,
// and the event says the plan for the new target does not compose.
func TestACloseWithNoFreshRunReportsItsBoundary(t *testing.T) {
	t.Run("a revert with nothing trailing", func(t *testing.T) {
		isvc := isvcFixture(inlineBlueGreen())
		if out := pass(t, isvc, divergedIR(oldRev, newRev)); !out.Opened {
			t.Fatalf("fixture: %+v, want an open", out)
		}
		id := isvc.Status.Rollout.ActiveRun.RunID
		out, events := passWith(t, isvc, nil, settledIR(oldRev))
		if !out.StateChanged || out.Opened || out.Parked || out.RequeueAfter != 0 {
			t.Fatalf("outcome = %+v, want the close reported as a state change and nothing else", out)
		}
		requireNoRun(t, isvc, v1beta1.RolloutPlanReasonNoRun)
		if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunSuperseded {
			t.Fatalf("lastRun = %+v, want Superseded", last)
		}
		if d := driftCondition(isvc); d == nil || d.Status != corev1.ConditionFalse {
			t.Fatalf("RolloutPlanDrift = %+v, want False", d)
		}
		if len(events) != 1 || !strings.Contains(events[0], EventRunClosed+" run "+id+" ") || !strings.Contains(events[0], "; nothing diverges, so no run opens") {
			t.Fatalf("events = %q, want one close event naming the run and saying nothing diverges and no run opens", events)
		}
		if again := pass(t, isvc, settledIR(oldRev)); !emptyOutcome(again) {
			t.Fatalf("the pass after the close = %+v, want nothing decided", again)
		}
	})
	t.Run("a retarget whose plan does not compose", func(t *testing.T) {
		isvc := isvcFixture(refBlueGreen(policyName))
		if out := pass(t, isvc, divergedIR(oldRev, newRev), blueGreenPolicy(policyName)); !out.Opened {
			t.Fatalf("fixture: %+v, want an open", out)
		}
		id := isvc.Status.Rollout.ActiveRun.RunID
		out, events := passWith(t, isvc, nil, divergedIR(oldRev, thirdRev))
		if !out.StateChanged || out.Opened {
			t.Fatalf("outcome = %+v, want the close reported as a state change beside the hold", out)
		}
		requireParked(t, out, isvc, v1beta1.RolloutPlanReasonPolicyNotFound)
		if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunSuperseded {
			t.Fatalf("lastRun = %+v, want Superseded", last)
		}
		if len(events) != 2 || !strings.Contains(events[0], EventRunClosed+" run "+id+" ") || !strings.Contains(events[0], "; the plan for the new target does not compose, so no run opens") || !strings.Contains(events[1], EventPlanParked) {
			t.Fatalf("events = %q, want the close naming the run and saying the plan for the new target does not compose, then the hold", events)
		}
	})
}

// A push reverted before any Instance moved ends with its run. Two engines
// under an inline canary: version 2 is pushed, the run opens and the unit's
// record is written at step 0 toward it. The template returns to version 1
// while every Instance still runs it. The run closes with nothing to roll
// and no fresh run opens: the record, written for a target no member has
// any more, is not a roll in flight, so it neither keeps the run nor opens
// another, and the executor ends it on the same pass. The same record found
// with no run on file opens nothing either. A record toward the revision
// the member still targets is in flight and is taken over.
func TestARevertBeforeAnyInstanceMovedOpensNoRunOverItsRecord(t *testing.T) {
	recordToward := func(t *testing.T, targetID string) *v1beta1.CanaryStatus {
		t.Helper()
		return &v1beta1.CanaryStatus{TargetID: targetID, CanaryRevisionHash: hashOf(t, newRev), StableRevisionHash: hashOf(t, oldRev)}
	}
	t.Run("under the run it closes", func(t *testing.T) {
		isvc, _ := openInlineCanary(t)
		recorded := isvc.Status.Rollout.ActiveRun.TargetRevisions
		id := rollout.CanaryTargetID([]v1beta1.ComponentType{v1beta1.EngineComponent}, func(c v1beta1.ComponentType) string { return pinnedRevision(recorded, c) })
		setEngineUnit(isvc, v1beta1.RolloutPhasePending, recordToward(t, id))
		out := pass(t, isvc, settledIR(oldRev))
		if !out.StateChanged || out.Opened {
			t.Fatalf("outcome = %+v, want the close with no fresh run", out)
		}
		requireNoRun(t, isvc, v1beta1.RolloutPlanReasonNoRun)
		if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunSuperseded {
			t.Fatalf("lastRun = %+v, want the closed run", last)
		}
	})
	t.Run("with no run on file", func(t *testing.T) {
		isvc := isvcFixture(inlineCanary())
		setEngineUnit(isvc, v1beta1.RolloutPhasePending, recordToward(t, ""))
		if out := pass(t, isvc, settledIR(oldRev)); !emptyOutcome(out) || v1beta1.RolloutRunActive(isvc) {
			t.Fatalf("outcome = %+v, want nothing opened over a record toward a target the member no longer has", out)
		}
		requireNoRun(t, isvc, v1beta1.RolloutPlanReasonNoRun)
	})
	t.Run("a record toward the target the member still has is taken over", func(t *testing.T) {
		isvc := isvcFixture(inlineCanary())
		setEngineUnit(isvc, v1beta1.RolloutPhasePending, recordToward(t, ""))
		if out := pass(t, isvc, divergedIR(oldRev, newRev)); !out.Opened || !out.Adopted {
			t.Fatalf("outcome = %+v, want the in-flight record taken over", out)
		}
	})
}

// sequentialChainISVC is a Sequential roll written as two single-member
// groups with no progression body, the decoder before the engine. Each
// group resolves to the default blueGreen when a run opens.
func sequentialChainISVC() *v1beta1.InferenceService {
	isvc := isvcFixture(v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}})
	isvc.Spec.Rollout.Groups = append(isvc.Spec.Rollout.Groups, v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
	})
	return isvc
}

// sequentialChainReplicas are the chain's two replicas, both pushed to a
// new revision with no Instance rolled yet.
func sequentialChainReplicas() []runtime.Object {
	return []runtime.Object{decoderIR(decoderRev, decoderNewRev, 2, 0), divergedIR(oldRev, newRev)}
}

// askPlanReread runs one pass with the plan reread asked for and checks
// that the pass consumed the request.
func askPlanReread(t *testing.T, isvc *v1beta1.InferenceService, objects ...runtime.Object) Outcome {
	t.Helper()
	key := constants.RolloutRepinAnnotation
	annotate(isvc, key, "now")
	out := passAt(t, isvc, time.Unix(2000, 0), objects...)
	if _, still := isvc.Annotations[key]; still {
		t.Fatal("the reread request was not consumed")
	}
	return out
}

// A Sequential roll written as single-member groups with no progression
// body: two decoders then two engines, version 2 pushed on both. The run
// opens on the first pass, and every pass after reads the plan InSync
// while nothing is edited: the live groups are digested as the open
// digested them, each resolved to the default blueGreen, so the two
// digests agree and the condition names no edit nobody made. A plan
// reread asked for with nothing edited is pure cleanup and leaves the
// same reading.
func TestASequentialPlanWithNoBodiesReadsInSyncWhileNothingIsEdited(t *testing.T) {
	isvc := sequentialChainISVC()
	replicas := sequentialChainReplicas()
	if out := pass(t, isvc, replicas...); !out.Opened || out.Adopted {
		t.Fatalf("outcome = %+v, want a fresh open", out)
	}
	id := isvc.Status.Rollout.ActiveRun.RunID
	requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)
	for i := 1; i <= 3; i++ {
		if out := pass(t, isvc, replicas...); !emptyOutcome(out) {
			t.Fatalf("pass %d with nothing edited: outcome = %+v, want nothing decided", i, out)
		}
		requireSameRun(t, isvc, id)
		requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)
	}
	if out := askPlanReread(t, isvc, replicas...); out.StateChanged || out.Opened {
		t.Fatalf("the reread with nothing edited: outcome = %+v, want pure cleanup", out)
	}
	requireSameRun(t, isvc, id)
	requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)
}

// Under the same run, spelling the default out is no edit and a changed
// body is one the reread takes. The engine group gains an explicit
// blueGreen body: it resolves to the body the run recorded, so the plan
// reads InSync. The decoder group then gains a rollingUpdate body with a
// surge budget of one: the next pass reads SpecNewerThanRun with the run
// still on its recorded plan, the reread replaces the plan under the same
// run id with the new body and the drift reads InSync, and the pass after
// reads nothing new.
func TestASequentialPlanWithNoBodiesReadsAnEditedBodyAsDriftUntilTheReread(t *testing.T) {
	isvc := sequentialChainISVC()
	replicas := sequentialChainReplicas()
	if out := pass(t, isvc, replicas...); !out.Opened {
		t.Fatalf("outcome = %+v, want a fresh open", out)
	}
	run := isvc.Status.Rollout.ActiveRun
	id, recorded := run.RunID, run.Plan.Groups[0].PortableDigest

	isvc.Spec.Rollout.Groups[1].BlueGreen = &v1beta1.GroupBlueGreen{}
	if out := pass(t, isvc, replicas...); !emptyOutcome(out) {
		t.Fatalf("the default spelled out: outcome = %+v, want nothing decided", out)
	}
	requireSameRun(t, isvc, id)
	requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)

	one := intstr.FromInt32(1)
	isvc.Spec.Rollout.Groups[0].RollingUpdate = &v1beta1.GroupRollingUpdate{MaxSurge: &one}
	if out := pass(t, isvc, replicas...); !emptyOutcome(out) {
		t.Fatalf("the body edit: outcome = %+v, want the run left on its recorded plan", out)
	}
	run = requireSameRun(t, isvc, id)
	requireDrift(t, isvc, corev1.ConditionTrue, v1beta1.RolloutPlanDriftReasonSpecNewerThanRun)
	if got := run.Plan.Groups[0].PortableDigest; got != recorded {
		t.Fatalf("the edit moved the recorded plan: %s -> %s", recorded, got)
	}

	if out := askPlanReread(t, isvc, replicas...); !out.StateChanged || out.Opened {
		t.Fatalf("the reread: outcome = %+v, want the plan replaced under the same run", out)
	}
	run = requireSameRun(t, isvc, id)
	requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)
	if got := run.Plan.Groups[0].Group.RollingUpdate; got == nil || got.MaxSurge == nil || got.MaxSurge.IntValue() != 1 {
		t.Fatalf("the reread recorded %+v, want the rollingUpdate body with a surge budget of one", run.Plan.Groups[0].Group)
	}
	if out := pass(t, isvc, replicas...); !emptyOutcome(out) {
		t.Fatalf("the pass after the reread: outcome = %+v, want nothing decided", out)
	}
	requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)
}

// A member still in the OMENative mode whose replica is missing keeps the
// run open: the projector recreates the replica, and only a member that
// left the mode is read as departed.
func TestAMissingReplicaOfAMemberStillInTheModeKeepsTheRunOpen(t *testing.T) {
	isvc, _ := openInlineCanary(t)
	id := requirePinned(t, isvc).RunID
	if out, _ := passUnderModes(t, isvc, modesWith()); !emptyOutcome(out) || isvc.Status.Rollout.LastRun != nil {
		t.Fatalf("outcome = %+v lastRun = %+v, want the run kept with no decision", out, isvc.Status.Rollout.LastRun)
	}
	requireSameRun(t, isvc, id)
}
