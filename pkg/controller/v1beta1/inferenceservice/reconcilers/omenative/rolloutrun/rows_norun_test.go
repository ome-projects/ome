package rolloutrun

import (
	"context"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Fixtures shared by the run machine's state tests: one run-layer pass over a
// service and the objects it reads, with the status carried forward the way
// the controller's flush carries it, and the small shapes each case starts
// from (a settled or diverged replica, an inline or referenced group, a
// canary record in one of its states).

const (
	thirdRev   = "llm-a-engine-cccccccc"
	policyName = "canary-std-v1"
)

// passWith runs one pass over isvc with a fresh client holding objects,
// after mutate adjusted the inputs, and returns the outcome with the events
// the pass emitted. isvc carries the resulting status forward.
func passWith(t *testing.T, isvc *v1beta1.InferenceService, mutate func(*Inputs), objects ...runtime.Object) (Outcome, []string) {
	t.Helper()
	isvc.ResourceVersion = ""
	in := testInputs(t, isvc, objects...)
	recorder := record.NewFakeRecorder(16)
	in.Recorder = recorder
	if mutate != nil {
		mutate(&in)
	}
	out, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	close(recorder.Events)
	var events []string
	for e := range recorder.Events {
		events = append(events, e)
	}
	return out, events
}

func pass(t *testing.T, isvc *v1beta1.InferenceService, objects ...runtime.Object) Outcome {
	t.Helper()
	out, _ := passWith(t, isvc, nil, objects...)
	return out
}

// passAt is pass with the clock set, for rows that compare timestamps.
func passAt(t *testing.T, isvc *v1beta1.InferenceService, now time.Time, objects ...runtime.Object) Outcome {
	t.Helper()
	out, _ := passWith(t, isvc, func(in *Inputs) { in.Now = now }, objects...)
	return out
}

func inlineBlueGreen() v1beta1.RolloutGroup {
	return v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}}
}

func inlineCanary(steps ...int32) v1beta1.RolloutGroup {
	if len(steps) == 0 {
		steps = []int32{10, 100}
	}
	return v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: canaryBody(steps...)}
}

func refCanary(name string) v1beta1.RolloutGroup {
	return v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		PolicyRef:  &v1beta1.RolloutPolicyRef{Name: name, Progression: v1beta1.RolloutProgressionCanary},
	}
}

func canaryPolicy(name string, steps ...int32) *v1beta1.RolloutPolicy {
	if len(steps) == 0 {
		steps = []int32{10, 100}
	}
	return &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Generation: 1},
		Spec:       v1beta1.RolloutPolicySpec{Canary: canaryBody(steps...)},
	}
}

// settledIR is a replica resting on one revision with nothing trailing.
func settledIR(rev string) *v1beta1.InferenceReplica {
	ir := irFixture(rev, rev)
	ir.Status.Replicas, ir.Status.UpdatedReplicas = 2, 2
	return ir
}

// divergedIR is a replica whose target moved off its current revision with
// no Instance rolled yet.
func divergedIR(current, target string) *v1beta1.InferenceReplica {
	ir := irFixture(current, target)
	ir.Status.Replicas, ir.Status.UpdatedReplicas = 2, 0
	return ir
}

// stragglingIR is a replica whose pair agrees while one Instance trails.
func stragglingIR(rev string) *v1beta1.InferenceReplica {
	ir := irFixture(rev, rev)
	ir.Status.Replicas, ir.Status.UpdatedReplicas = 2, 1
	return ir
}

func setEngineUnit(isvc *v1beta1.InferenceService, phase v1beta1.RolloutPhase, cs *v1beta1.CanaryStatus) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	engine := isvc.Status.Components[v1beta1.EngineComponent]
	engine.RolloutPhase = phase
	engine.Canary = cs
	isvc.Status.Components[v1beta1.EngineComponent] = engine
}

// midLadderRecord is a unit armed toward newRev and short of its ladder's end.
func midLadderRecord(t *testing.T) *v1beta1.CanaryStatus {
	t.Helper()
	return &v1beta1.CanaryStatus{CanaryRevisionHash: hashOf(t, newRev), StableRevisionHash: hashOf(t, oldRev), CurrentStep: 0}
}

// rejectedRecord is a unit whose ladder rejected newRev.
func rejectedRecord(t *testing.T) *v1beta1.CanaryStatus {
	t.Helper()
	return &v1beta1.CanaryStatus{CanaryRevisionHash: hashOf(t, newRev), StableRevisionHash: hashOf(t, oldRev), RolledBackRevisionHash: hashOf(t, newRev)}
}

func setCoordinationPhase(isvc *v1beta1.InferenceService, phase v1beta1.CoordinationPhase) {
	isvc.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{
		Groups: []v1beta1.RolloutCoordinationGroupStatus{{Name: "0", Phase: phase, CompositePhase: string(phase)}},
	}
}

func annotate(isvc *v1beta1.InferenceService, key, value string) {
	if isvc.Annotations == nil {
		isvc.Annotations = map[string]string{}
	}
	isvc.Annotations[key] = value
}

func driftCondition(isvc *v1beta1.InferenceService) *apis.Condition {
	return isvc.Status.GetCondition(apis.ConditionType(v1beta1.RolloutPlanDriftCondition))
}

// emptyOutcome reports a pass that decided nothing: no boundary, no park,
// no requeue.
func emptyOutcome(out Outcome) bool {
	return !out.Parked && !out.StateChanged && !out.Opened && !out.Adopted && out.RequeueAfter == 0
}

func requireNoRun(t *testing.T, isvc *v1beta1.InferenceService, reason string) {
	t.Helper()
	if v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("a run is pinned: %+v", isvc.Status.Rollout.ActiveRun)
	}
	if c := planReady(isvc); c == nil || c.Status != corev1.ConditionTrue || c.Reason != reason {
		t.Fatalf("RolloutPlanReady = %+v, want True with reason %s", c, reason)
	}
}

func requirePinned(t *testing.T, isvc *v1beta1.InferenceService) *v1beta1.RolloutRun {
	t.Helper()
	if !v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("no run pinned: %+v", isvc.Status.Rollout)
	}
	return isvc.Status.Rollout.ActiveRun
}

func requireParked(t *testing.T, out Outcome, isvc *v1beta1.InferenceService, reason string) {
	t.Helper()
	if !out.Parked || out.RequeueAfter != parkRequeue || v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("outcome = %+v active=%v, want parked with the one-minute requeue and no run", out, v1beta1.RolloutRunActive(isvc))
	}
	if c := planReady(isvc); c == nil || c.Status != corev1.ConditionFalse || c.Reason != reason {
		t.Fatalf("RolloutPlanReady = %+v, want False with reason %s", c, reason)
	}
}

// movingTargetReader moves a replica's target right after the pass read it:
// the window between the one live read and the flush.
type movingTargetReader struct {
	client.Reader
	writer client.Client
	name   string
	to     string
	moved  bool
}

func (r *movingTargetReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := r.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if ir, ok := obj.(*v1beta1.InferenceReplica); ok && key.Name == r.name && !r.moved {
		r.moved = true
		live := ir.DeepCopy()
		live.Status.UpdateRevision = r.to
		return r.writer.Update(ctx, live)
	}
	return nil
}

func moveTargetAfterRead(to string) func(*Inputs) {
	return func(in *Inputs) {
		in.Reader = &movingTargetReader{Reader: in.Reader, writer: in.Client, name: "llm-a-engine", to: to}
	}
}

// --- NoRun ---------------------------------------------------------------

// A service with no run pinned and nothing diverging is settled: the pass
// re-derives RolloutPlanReady True NoActiveRun and opens nothing, whatever
// else moved (a target back on the running revision, a plan edit, a
// reference resolving or not, a missing or converged replica, a coordination
// phase, a rollback verb).
func TestNoRunLeavesASettledServiceAlone(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object)
		check func(t *testing.T, isvc *v1beta1.InferenceService)
	}{
		{
			name: "spec.revision[revert]",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				return isvcFixture(inlineBlueGreen()), []runtime.Object{settledIR(oldRev)}
			},
		},
		{
			name: "spec.planEdit",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := isvcFixture(inlineCanary(10, 100))
				pass(t, isvc, settledIR(oldRev))
				isvc.Spec.Rollout.Groups[0].Canary = canaryBody(50, 100)
				return isvc, []runtime.Object{settledIR(oldRev)}
			},
			check: func(t *testing.T, isvc *v1beta1.InferenceService) {
				want, err := liveSourceDigest(&isvc.Spec.Rollout.Groups[0], Outcome{}.Policies)
				if err != nil {
					t.Fatal(err)
				}
				if got := isvc.Status.Rollout.Groups[0].ObservedDigest; got != want {
					t.Fatalf("resolution view digest = %s, want the edited body's %s", got, want)
				}
			},
		},
		{
			name: "policy.unresolved",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				return isvcFixture(refCanary(policyName)), []runtime.Object{settledIR(oldRev)}
			},
			check: func(t *testing.T, isvc *v1beta1.InferenceService) {
				if got := isvc.Status.Rollout.Groups[0].ObservedDigest; got != "" {
					t.Fatalf("resolution view digest = %q for a missing policy, want empty", got)
				}
			},
		},
		{
			name: "policy.resolved",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				return isvcFixture(refCanary(policyName)), []runtime.Object{settledIR(oldRev), canaryPolicy(policyName)}
			},
			check: func(t *testing.T, isvc *v1beta1.InferenceService) {
				if got := isvc.Status.Rollout.Groups[0].ObservedDigest; got == "" {
					t.Fatal("resolution view carries no digest for a policy that resolves")
				}
			},
		},
		{
			name: "ir.missing",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				return isvcFixture(inlineBlueGreen()), nil
			},
		},
		{
			name: "ir.converged",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				return isvcFixture(inlineBlueGreen()), []runtime.Object{settledIR(newRev)}
			},
		},
		{
			name: "coord.staged",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := isvcFixture(inlineBlueGreen())
				setCoordinationPhase(isvc, v1beta1.CoordinationPhaseStaged)
				return isvc, []runtime.Object{settledIR(oldRev)}
			},
		},
		{
			name: "coord.settled",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := isvcFixture(inlineBlueGreen())
				setCoordinationPhase(isvc, v1beta1.CoordinationPhaseIdle)
				return isvc, []runtime.Object{settledIR(oldRev)}
			},
		},
		{
			name: "verb.rollback",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := isvcFixture(inlineCanary())
				annotate(isvc, constants.RolloutRollbackAnnotation, "true")
				return isvc, []runtime.Object{settledIR(oldRev)}
			},
			check: func(t *testing.T, isvc *v1beta1.InferenceService) {
				if isvc.Annotations[constants.RolloutRollbackAnnotation] != "true" {
					t.Fatal("the run layer consumed the rollback verb, which is the canary executor's")
				}
			},
		},
	}
	for _, prefix := range []string{"", "Closed/"} {
		for _, tc := range cases {
			t.Run(prefix+tc.name, func(t *testing.T) {
				isvc, objects := tc.setup(t)
				if prefix != "" {
					// A closed record on file changes nothing about the settled reading.
					if isvc.Status.Rollout == nil {
						isvc.Status.Rollout = &v1beta1.RolloutStatus{}
					}
					isvc.Status.Rollout.LastRun = &v1beta1.RolloutRunRecord{Outcome: v1beta1.RolloutRunCompleted}
				}
				out := pass(t, isvc, objects...)
				if out.Parked || out.Opened || out.StateChanged {
					t.Fatalf("outcome = %+v, want nothing to happen", out)
				}
				requireNoRun(t, isvc, v1beta1.RolloutPlanReasonNoRun)
				if prefix != "" && (isvc.Status.Rollout.LastRun == nil || isvc.Status.Rollout.LastRun.Outcome != v1beta1.RolloutRunCompleted) {
					t.Fatalf("the settled pass touched the record: %+v", isvc.Status.Rollout.LastRun)
				}
				if d := driftCondition(isvc); d == nil || d.Status != corev1.ConditionFalse {
					t.Fatalf("RolloutPlanDrift = %+v, want False", d)
				}
				if tc.check != nil {
					tc.check(t, isvc)
				}
			})
		}
	}
}

// A replica whose status trails its generation, or whose projection trails
// the service's, describes an older workload: with no run pinned the pass
// decides nothing, writes no condition and comes back in ten seconds.
func TestNoRunWaitsOnAStaleReplica(t *testing.T) {
	stale := func(t *testing.T) *v1beta1.InferenceReplica {
		ir := divergedIR(oldRev, newRev)
		ir.Status.ObservedGeneration = 0
		return ir
	}
	trailing := func(t *testing.T) *v1beta1.InferenceReplica {
		ir := divergedIR(oldRev, newRev)
		ir.Annotations = map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: "1"}
		return ir
	}
	starts := map[string]func(t *testing.T) *v1beta1.InferenceService{
		"NoRun": func(t *testing.T) *v1beta1.InferenceService { return isvcFixture(inlineBlueGreen()) },
		"Closed": func(t *testing.T) *v1beta1.InferenceService {
			isvc := isvcFixture(inlineBlueGreen())
			isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{Outcome: v1beta1.RolloutRunCompleted}}
			return isvc
		},
		"RejectedSticky": func(t *testing.T) *v1beta1.InferenceService { return rejectedISVC(t, v1beta1.RolloutPhaseRolledBack) },
	}
	for name, build := range map[string]func(*testing.T) *v1beta1.InferenceReplica{"statusTrails": stale, "projectionTrails": trailing} {
		for startName, start := range starts {
			t.Run(startName+"/"+name, func(t *testing.T) {
				isvc := start(t)
				isvc.Generation = 2
				out := pass(t, isvc, build(t))
				if out.RequeueAfter != shortRequeue || out.Parked || out.Opened || v1beta1.RolloutRunActive(isvc) {
					t.Fatalf("outcome = %+v active=%v, want a ten-second wait with no decision", out, v1beta1.RolloutRunActive(isvc))
				}
				if planReady(isvc) != nil {
					t.Fatalf("a pass waiting on a stale replica wrote RolloutPlanReady %+v", planReady(isvc))
				}
			})
		}
	}
}

// A pass with nothing changed re-derives the same conditions and view and
// writes nothing: the status is byte-identical and the outcome empty. The
// one exception is the first pass after a close, which replaces the close's
// RolloutPlanReady message ("last run closed ...") with the settled one
// ("no rollout in progress") and so re-stamps the condition once.
func TestNoRunResyncWritesNothing(t *testing.T) {
	for _, closed := range []bool{false, true} {
		isvc := isvcFixture(inlineCanary())
		if closed {
			pass(t, isvc, divergedIR(oldRev, newRev))
			pass(t, isvc, settledIR(newRev))
			if isvc.Status.Rollout.LastRun == nil {
				t.Fatal("fixture: the run did not close")
			}
			closeMessage := planReady(isvc).Message
			pass(t, isvc, settledIR(newRev))
			if got := planReady(isvc).Message; got == closeMessage {
				t.Fatalf("the first pass after the close kept the close's message %q; the settle rewrites it", got)
			}
		} else {
			passAt(t, isvc, time.Unix(1000, 0), settledIR(newRev))
		}
		before := isvc.Status.DeepCopy()
		out := passAt(t, isvc, time.Unix(5000, 0), settledIR(newRev))
		if !emptyOutcome(out) {
			t.Fatalf("closed=%v: resync outcome = %+v, want nothing", closed, out)
		}
		if !reflect.DeepEqual(before, &isvc.Status) {
			t.Fatalf("closed=%v: a resync rewrote the status:\nbefore %+v\nafter  %+v", closed, before, isvc.Status)
		}
	}
}

// The repin verb is read only under a pinned run. Written while none is,
// the annotation stays where it is through the park or the idle passes and
// through the pass that opens the next run; the first pass under that run
// renders to exactly the pinned digests and removes it as cleanup, with no
// re-stamp of the pin.
func TestARepinWrittenWithNoRunWaitsForTheNextRun(t *testing.T) {
	cases := []struct {
		name string
		// idle builds the service and the objects of the idle passes; diverge
		// returns the objects that open the run.
		idle    func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object)
		diverge func(t *testing.T) []runtime.Object
	}{
		{
			name: "NoRun",
			idle: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				return isvcFixture(inlineCanary()), []runtime.Object{settledIR(oldRev)}
			},
			diverge: func(t *testing.T) []runtime.Object { return []runtime.Object{divergedIR(oldRev, newRev)} },
		},
		{
			name: "Parked",
			idle: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				return isvcFixture(refCanary(policyName)), []runtime.Object{divergedIR(oldRev, newRev)}
			},
			diverge: func(t *testing.T) []runtime.Object {
				return []runtime.Object{divergedIR(oldRev, newRev), canaryPolicy(policyName)}
			},
		},
		{
			name: "Closed",
			idle: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := isvcFixture(inlineCanary())
				pass(t, isvc, divergedIR(oldRev, newRev))
				pass(t, isvc, settledIR(newRev))
				return isvc, []runtime.Object{settledIR(newRev)}
			},
			diverge: func(t *testing.T) []runtime.Object { return []runtime.Object{divergedIR(newRev, thirdRev)} },
		},
		{
			name: "RejectedSticky",
			idle: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := rejectedISVC(t, v1beta1.RolloutPhaseRolledBack)
				return isvc, []runtime.Object{divergedIR(oldRev, newRev)}
			},
			diverge: func(t *testing.T) []runtime.Object { return []runtime.Object{divergedIR(oldRev, thirdRev)} },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc, objects := tc.idle(t)
			annotate(isvc, constants.RolloutRepinAnnotation, "now")
			for i := 0; i < 2; i++ {
				pass(t, isvc, objects...)
				if v1beta1.RolloutRunActive(isvc) {
					t.Fatalf("idle pass %d pinned a run", i)
				}
				if isvc.Annotations[constants.RolloutRepinAnnotation] != "now" {
					t.Fatalf("idle pass %d consumed the repin verb with no run to apply it to", i)
				}
			}
			opened := pass(t, isvc, tc.diverge(t)...)
			if !opened.Opened {
				t.Fatalf("the divergence did not open a run: %+v", opened)
			}
			run := requirePinned(t, isvc)
			if isvc.Annotations[constants.RolloutRepinAnnotation] != "now" {
				t.Fatal("the opening pass read the repin verb; it is read only under a run that was already pinned")
			}
			id, pinnedAt := run.RunID, run.PinnedAt
			next := pass(t, isvc, tc.diverge(t)...)
			if _, still := isvc.Annotations[constants.RolloutRepinAnnotation]; still {
				t.Fatal("the first pass under the run left the lingering repin verb in place")
			}
			if next.StateChanged || next.Opened {
				t.Fatalf("the cleanup claimed a state change: %+v", next)
			}
			if run = requirePinned(t, isvc); run.RunID != id || !run.PinnedAt.Equal(&pinnedAt) {
				t.Fatalf("the cleanup re-stamped the pin: %s@%v -> %s@%v", id, pinnedAt, run.RunID, run.PinnedAt)
			}
		})
	}
}

// Every opener pins once: the pass after an open, a straggler open, an
// adoption or a park that lifted finds the pin it wrote and reports no new
// boundary.
func TestASecondPassFindsThePinTheFirstOneWrote(t *testing.T) {
	closedRecord := func(isvc *v1beta1.InferenceService) *v1beta1.InferenceService {
		isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{Outcome: v1beta1.RolloutRunCompleted}}
		return isvc
	}
	cases := []struct {
		name    string
		setup   func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object)
		adopted bool
		check   func(t *testing.T, isvc *v1beta1.InferenceService)
	}{
		{
			name: "spec.revision[new]",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				return isvcFixture(inlineBlueGreen()), []runtime.Object{divergedIR(oldRev, newRev)}
			},
		},
		{
			name: "ir.straggling",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				return isvcFixture(inlineBlueGreen()), []runtime.Object{stragglingIR(oldRev)}
			},
		},
		{
			name: "ctrl.runLost",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := isvcFixture(inlineCanary())
				setEngineUnit(isvc, v1beta1.RolloutPhaseCanarying, midLadderRecord(t))
				return isvc, []runtime.Object{divergedIR(oldRev, newRev)}
			},
			adopted: true,
		},
		{
			name: "Closed/spec.revision[new]",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				return closedRecord(isvcFixture(inlineBlueGreen())), []runtime.Object{divergedIR(oldRev, newRev)}
			},
		},
		{
			name: "Closed/ir.straggling",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				return closedRecord(isvcFixture(inlineBlueGreen())), []runtime.Object{stragglingIR(oldRev)}
			},
		},
		{
			name: "Closed/ctrl.runLost",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := closedRecord(isvcFixture(inlineCanary()))
				setEngineUnit(isvc, v1beta1.RolloutPhaseCanarying, midLadderRecord(t))
				return isvc, []runtime.Object{divergedIR(oldRev, newRev)}
			},
			adopted: true,
		},
		{
			name: "Closed/canary.rolledBack[inFlight]",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := rejectedISVC(t, v1beta1.RolloutPhaseRollingBack)
				isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{Outcome: v1beta1.RolloutRunCompleted}}
				draining := irPublished(t, oldRev, newRev, []workloadtypes.InstanceStatus{rowOn(0, oldRev), rowOn(1, newRev)})
				return isvc, []runtime.Object{draining}
			},
			adopted: true,
		},
		{
			name: "Parked/policy.resolved",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := isvcFixture(refCanary(policyName))
				requireParked(t, pass(t, isvc, divergedIR(oldRev, newRev)), isvc, v1beta1.RolloutPlanReasonPolicyNotFound)
				return isvc, []runtime.Object{divergedIR(oldRev, newRev), canaryPolicy(policyName)}
			},
		},
		{
			// The router's own hold is skipped before the straggler check;
			// the engine trailing outside the rejection opens the run, with
			// the router pinned at its stable revision.
			name: "RejectedSticky/ir.straggling",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := concurrentCanaryISVC(t)
				router := namedIR("llm-a-router", routerStableRev, routerRejectedRev)
				engine := namedIR("llm-a-engine", engineStableRev, engineStableRev)
				engine.Status.Replicas, engine.Status.UpdatedReplicas = 2, 1
				return isvc, []runtime.Object{router, engine}
			},
			check: func(t *testing.T, isvc *v1beta1.InferenceService) {
				run := requirePinned(t, isvc)
				if got := pinnedRevision(run.TargetRevisions, v1beta1.RouterComponent); got != hashOf(t, routerStableRev) {
					t.Fatalf("router pinned at %s, want its stable %s", got, hashOf(t, routerStableRev))
				}
				if got := pinnedRevision(run.TargetRevisions, v1beta1.EngineComponent); got != hashOf(t, engineStableRev) {
					t.Fatalf("engine pinned at %s, want its agreeing revision %s", got, hashOf(t, engineStableRev))
				}
			},
		},
		{
			// The executor re-arms the unit at step zero as it clears the
			// rejection, so the record the open finds is mid-ladder and the
			// open reports Adopted.
			name: "RejectedSticky/canary.resumed",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := rejectedISVC(t, v1beta1.RolloutPhaseRolledBack)
				requireNoRunAfter(t, isvc, divergedIR(oldRev, newRev))
				cs := rejectedRecord(t)
				cs.RolledBackRevisionHash = ""
				setEngineUnit(isvc, v1beta1.RolloutPhasePending, cs)
				return isvc, []runtime.Object{divergedIR(oldRev, newRev)}
			},
			adopted: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc, objects := tc.setup(t)
			first := pass(t, isvc, objects...)
			if !first.Opened || first.Adopted != tc.adopted || !first.StateChanged {
				t.Fatalf("first pass = %+v, want an open with adopted=%v", first, tc.adopted)
			}
			run := requirePinned(t, isvc)
			id := run.RunID
			if tc.check != nil {
				tc.check(t, isvc)
			}
			second := pass(t, isvc, objects...)
			if second.Opened || second.Adopted || second.StateChanged || second.Parked {
				t.Fatalf("second pass = %+v, want the pin found as is", second)
			}
			if run = requirePinned(t, isvc); run.RunID != id {
				t.Fatalf("second pass re-minted the run: %s -> %s", id, run.RunID)
			}
			if c := planReady(isvc); c == nil || c.Reason != v1beta1.RolloutPlanReasonPinned {
				t.Fatalf("RolloutPlanReady = %+v, want Pinned", c)
			}
		})
	}
}

// requireNoRunAfter runs one pass and asserts it pinned nothing.
func requireNoRunAfter(t *testing.T, isvc *v1beta1.InferenceService, objects ...runtime.Object) {
	t.Helper()
	if out := pass(t, isvc, objects...); out.Opened || v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("fixture pass opened a run: %+v", out)
	}
}

// The pass composes its targets from one live read. A target that moves
// between that read and the flush is invisible to the pass, which pins what
// it read; the next pass sees the move against the pin and retargets, closing
// the run Superseded and opening a fresh one at the moved target.
func TestATargetThatMovesAfterTheReadIsTheNextPassesRetarget(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T) (*v1beta1.InferenceService, *v1beta1.InferenceReplica)
		// pinnedFirst is the engine revision the first pass pins: what it read.
		pinnedFirst string
	}{
		{
			name: "NoRun",
			setup: func(t *testing.T) (*v1beta1.InferenceService, *v1beta1.InferenceReplica) {
				return isvcFixture(inlineBlueGreen()), divergedIR(oldRev, newRev)
			},
			pinnedFirst: hashOf(t, newRev),
		},
		{
			name: "Closed",
			setup: func(t *testing.T) (*v1beta1.InferenceService, *v1beta1.InferenceReplica) {
				isvc := isvcFixture(inlineBlueGreen())
				isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{Outcome: v1beta1.RolloutRunCompleted}}
				return isvc, divergedIR(oldRev, newRev)
			},
			pinnedFirst: hashOf(t, newRev),
		},
		{
			name: "RejectedSticky",
			setup: func(t *testing.T) (*v1beta1.InferenceService, *v1beta1.InferenceReplica) {
				isvc := rejectedISVC(t, v1beta1.RolloutPhaseRolledBack)
				requireNoRunAfter(t, isvc, divergedIR(oldRev, newRev))
				return isvc, divergedIR(oldRev, "llm-a-engine-dddddddd")
			},
			pinnedFirst: "dddddddd",
		},
		{
			name: "Open",
			setup: func(t *testing.T) (*v1beta1.InferenceService, *v1beta1.InferenceReplica) {
				isvc := isvcFixture(inlineBlueGreen())
				pass(t, isvc, divergedIR(oldRev, "llm-a-engine-dddddddd"))
				requirePinned(t, isvc)
				return isvc, divergedIR(oldRev, newRev)
			},
			pinnedFirst: hashOf(t, newRev),
		},
		{
			name: "Adopted",
			setup: func(t *testing.T) (*v1beta1.InferenceService, *v1beta1.InferenceReplica) {
				isvc := isvcFixture(inlineCanary())
				setEngineUnit(isvc, v1beta1.RolloutPhaseCanarying, midLadderRecord(t))
				return isvc, divergedIR(oldRev, newRev)
			},
			pinnedFirst: hashOf(t, newRev),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc, ir := tc.setup(t)
			first, _ := passWith(t, isvc, moveTargetAfterRead(thirdRev), ir)
			if !first.Opened {
				t.Fatalf("first pass = %+v, want an open", first)
			}
			run := requirePinned(t, isvc)
			if got := pinnedRevision(run.TargetRevisions, v1beta1.EngineComponent); got != tc.pinnedFirst {
				t.Fatalf("first pass pinned %s, want the revision it read, %s", got, tc.pinnedFirst)
			}
			id := run.RunID

			ir.Status.UpdateRevision = thirdRev
			second := pass(t, isvc, ir)
			if !second.Opened || second.Adopted {
				t.Fatalf("second pass = %+v, want a fresh open after the retarget", second)
			}
			run = requirePinned(t, isvc)
			if run.RunID == id || pinnedRevision(run.TargetRevisions, v1beta1.EngineComponent) != hashOf(t, thirdRev) {
				t.Fatalf("second pass pinned %+v, want a new run at the moved target", run)
			}
			if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunSuperseded || pinnedRevision(last.TargetRevisions, v1beta1.EngineComponent) != tc.pinnedFirst {
				t.Fatalf("lastRun = %+v, want the first pin closed Superseded", last)
			}
		})
	}
}

// --- Parked --------------------------------------------------------------

// parkedOnMissingPolicy parks a referenced canary group whose policy does
// not exist, with the engine diverged toward newRev.
func parkedOnMissingPolicy(t *testing.T) *v1beta1.InferenceService {
	t.Helper()
	isvc := isvcFixture(refCanary(policyName))
	requireParked(t, pass(t, isvc, divergedIR(oldRev, newRev)), isvc, v1beta1.RolloutPlanReasonPolicyNotFound)
	return isvc
}

// While the plan does not compose the open is re-attempted on every pass
// and parks again, whatever else moved: a further target, trailing
// Instances, a coordination phase, a verb the run layer does not read. The
// park lifts, pinning the newest target, once the plan renders.
func TestParkedReattemptsTheOpenEveryPass(t *testing.T) {
	cases := []struct {
		name  string
		apply func(t *testing.T, isvc *v1beta1.InferenceService) *v1beta1.InferenceReplica
		check func(t *testing.T, isvc *v1beta1.InferenceService)
	}{
		{
			name: "spec.revision[new]",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) *v1beta1.InferenceReplica {
				return divergedIR(oldRev, thirdRev)
			},
		},
		{
			name: "ir.straggling",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) *v1beta1.InferenceReplica {
				return stragglingIR(newRev)
			},
		},
		{
			name: "coord.staged",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) *v1beta1.InferenceReplica {
				setCoordinationPhase(isvc, v1beta1.CoordinationPhaseStaged)
				return divergedIR(oldRev, newRev)
			},
		},
		{
			name: "coord.settled",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) *v1beta1.InferenceReplica {
				setCoordinationPhase(isvc, v1beta1.CoordinationPhaseIdle)
				return divergedIR(oldRev, newRev)
			},
		},
		{
			name: "verb.repin",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) *v1beta1.InferenceReplica {
				annotate(isvc, constants.RolloutRepinAnnotation, "now")
				return divergedIR(oldRev, newRev)
			},
			check: func(t *testing.T, isvc *v1beta1.InferenceService) {
				if isvc.Annotations[constants.RolloutRepinAnnotation] != "now" {
					t.Fatal("a parked pass consumed the repin verb")
				}
			},
		},
		{
			name: "verb.rollback",
			apply: func(t *testing.T, isvc *v1beta1.InferenceService) *v1beta1.InferenceReplica {
				annotate(isvc, constants.RolloutRollbackAnnotation, "true")
				return divergedIR(oldRev, newRev)
			},
			check: func(t *testing.T, isvc *v1beta1.InferenceService) {
				if isvc.Annotations[constants.RolloutRollbackAnnotation] != "true" {
					t.Fatal("a parked pass consumed the rollback verb")
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := parkedOnMissingPolicy(t)
			ir := tc.apply(t, isvc)
			requireParked(t, pass(t, isvc, ir), isvc, v1beta1.RolloutPlanReasonPolicyNotFound)
			if tc.check != nil {
				tc.check(t, isvc)
			}
			lifted := pass(t, isvc, ir, canaryPolicy(policyName))
			if !lifted.Opened || lifted.Adopted {
				t.Fatalf("with the policy present the park did not lift: %+v", lifted)
			}
			want := hashOf(t, ir.Status.UpdateRevision)
			if got := pinnedRevision(requirePinned(t, isvc).TargetRevisions, v1beta1.EngineComponent); got != want {
				t.Fatalf("the open pinned %s, want the target read at the open, %s", got, want)
			}
		})
	}
}

// A plan edit while parked is composed again on the next pass: an inline
// arm added to the unresolvable reference, or a reshaped group list that
// renders, opens the run; the edited body is what gets pinned.
func TestParkedPlanEditIsComposedOnTheNextPass(t *testing.T) {
	t.Run("inlineBody", func(t *testing.T) {
		isvc := parkedOnMissingPolicy(t)
		isvc.Spec.Rollout.Groups[0].Canary = canaryBody(25, 100)
		out := pass(t, isvc, divergedIR(oldRev, newRev))
		if !out.Opened {
			t.Fatalf("outcome = %+v, want the inline arm to open the run", out)
		}
		g := requirePinned(t, isvc).Plan.Groups[0]
		if g.Source != v1beta1.RolloutPlanSourceInline || g.Group.Canary == nil || g.Group.Canary.Steps[0].Traffic != 25 {
			t.Fatalf("pinned group = %+v, want the inline body", g)
		}
	})
	t.Run("groupShape", func(t *testing.T) {
		isvc := parkedOnMissingPolicy(t)
		isvc.Spec.Rollout.Groups = []v1beta1.RolloutGroup{
			inlineBlueGreen(),
			{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		}
		decoder := namedIR("llm-a-decoder", "llm-a-decoder-dddddddd", "llm-a-decoder-dddddddd")
		out := pass(t, isvc, divergedIR(oldRev, newRev), decoder)
		if !out.Opened {
			t.Fatalf("outcome = %+v, want the reshaped plan to open the run", out)
		}
		if run := requirePinned(t, isvc); len(run.Plan.Groups) != 2 || len(run.TargetRevisions) != 2 {
			t.Fatalf("pinned run = %+v, want both groups and both members", run)
		}
	})
}

// A referenced body that still resolves and changes while another reference
// keeps the plan parked is composed again on the next pass, which parks for
// the same reason and writes nothing.
func TestParkedIgnoresAnEditToAPolicyThatStillResolves(t *testing.T) {
	isvc := isvcFixture(refCanary(policyName))
	isvc.Spec.Rollout.Groups = append(isvc.Spec.Rollout.Groups, v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent},
		PolicyRef:  &v1beta1.RolloutPolicyRef{Name: "decoder-canary", Progression: v1beta1.RolloutProgressionCanary},
	})
	decoder := namedIR("llm-a-decoder", "llm-a-decoder-dddddddd", "llm-a-decoder-dddddddd")
	requireParked(t, passAt(t, isvc, time.Unix(1000, 0), divergedIR(oldRev, newRev), decoder, canaryPolicy("decoder-canary", 10, 100)), isvc, v1beta1.RolloutPlanReasonPolicyNotFound)
	before := isvc.Status.DeepCopy()

	out, events := passWith(t, isvc, func(in *Inputs) { in.Now = time.Unix(2000, 0) },
		divergedIR(oldRev, newRev), decoder, canaryPolicy("decoder-canary", 50, 100))
	requireParked(t, out, isvc, v1beta1.RolloutPlanReasonPolicyNotFound)
	if len(events) != 0 {
		t.Fatalf("the same park re-emitted %v", events)
	}
	if !reflect.DeepEqual(before.Conditions, isvc.Status.Conditions) || isvc.Status.Rollout.LastRun != nil {
		t.Fatalf("the same park rewrote the conditions:\nbefore %+v\nafter  %+v", before.Conditions, isvc.Status.Conditions)
	}
	// The resolution view is the one thing that follows the edit between runs.
	if before.Rollout.Groups[1].ObservedDigest == isvc.Status.Rollout.Groups[1].ObservedDigest {
		t.Fatal("the resolution view did not pick up the edited body's digest")
	}
}

// The park ends when nothing diverges any more: a target back on the
// running revision with nothing trailing, a replica that disappeared, a
// roll that converged on its own, a target that moved onto the revision its
// unit rolled back, or a revert that settled on its hold. The pass then
// settles RolloutPlanReady True NoActiveRun with no pin, keeping whatever
// record stood.
func TestParkSettlesWhenNothingDivergesAnyMore(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object)
	}{
		{
			name: "spec.revision[revert]",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				return parkedOnMissingPolicy(t), []runtime.Object{settledIR(oldRev)}
			},
		},
		{
			name: "ir.missing",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				return parkedOnMissingPolicy(t), nil
			},
		},
		{
			name: "ir.converged",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				return parkedOnMissingPolicy(t), []runtime.Object{settledIR(newRev)}
			},
		},
		{
			name: "spec.revision[rejected]",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				// The unit rejected newRev under an earlier run; a third target
				// parked on the missing policy, and now moves back onto the
				// rejected revision.
				isvc := isvcFixture(refCanary(policyName))
				setEngineUnit(isvc, v1beta1.RolloutPhaseRolledBack, rejectedRecord(t))
				isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{
					Outcome:         v1beta1.RolloutRunRolledBack,
					TargetRevisions: []v1beta1.RolloutRunTarget{{Component: v1beta1.EngineComponent, Revision: hashOf(t, newRev), StableRevision: hashOf(t, oldRev)}},
				}}
				requireParked(t, pass(t, isvc, divergedIR(oldRev, thirdRev)), isvc, v1beta1.RolloutPlanReasonPolicyNotFound)
				return isvc, []runtime.Object{divergedIR(oldRev, newRev)}
			},
		},
		{
			name: "canary.rolledBack[settled]",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := isvcFixture(refCanary(policyName))
				setEngineUnit(isvc, v1beta1.RolloutPhaseRollingBack, rejectedRecord(t))
				draining := irPublished(t, oldRev, newRev, []workloadtypes.InstanceStatus{rowOn(0, oldRev), rowOn(1, newRev)})
				requireParked(t, pass(t, isvc, draining), isvc, v1beta1.RolloutPlanReasonPolicyNotFound)
				setEngineUnit(isvc, v1beta1.RolloutPhaseRolledBack, rejectedRecord(t))
				settled := irPublished(t, oldRev, newRev, []workloadtypes.InstanceStatus{rowOn(0, oldRev), rowOn(1, oldRev)})
				return isvc, []runtime.Object{settled}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc, objects := tc.setup(t)
			last := isvc.Status.Rollout.LastRun
			out := passAt(t, isvc, time.Unix(2000, 0), objects...)
			if out.Parked || out.Opened || out.RequeueAfter != 0 {
				t.Fatalf("outcome = %+v, want the park to settle", out)
			}
			requireNoRun(t, isvc, v1beta1.RolloutPlanReasonNoRun)
			if !reflect.DeepEqual(last, isvc.Status.Rollout.LastRun) {
				t.Fatalf("the settle touched the record: %+v -> %+v", last, isvc.Status.Rollout.LastRun)
			}
			before := isvc.Status.DeepCopy()
			if again := passAt(t, isvc, time.Unix(3000, 0), objects...); again.Parked || again.Opened || !reflect.DeepEqual(before, &isvc.Status) {
				t.Fatalf("the settle wrote more than once: %+v\nbefore %+v\nafter  %+v", again, before, isvc.Status)
			}
		})
	}
}

// With no spec groups and no pin the pass returns before any condition is
// touched: the park condition keeps reading False with its reason although
// nothing is parked any more, and the plan gate holds no one since no member
// is grouped.
func TestRemovingEveryGroupLeavesTheParkConditionStanding(t *testing.T) {
	isvc := parkedOnMissingPolicy(t)
	isvc.Spec.Rollout = nil
	out := passAt(t, isvc, time.Unix(2000, 0), divergedIR(oldRev, newRev))
	if !emptyOutcome(out) {
		t.Fatalf("outcome = %+v, want nothing", out)
	}
	if c := planReady(isvc); c == nil || c.Status != corev1.ConditionFalse || c.Reason != v1beta1.RolloutPlanReasonPolicyNotFound {
		t.Fatalf("RolloutPlanReady = %+v; want the park condition left standing with no spec group", c)
	}
	if isvc.Status.Rollout.Groups != nil {
		t.Fatalf("resolution view = %+v, want cleared with the groups", isvc.Status.Rollout.Groups)
	}
}

// A parked plan makes no open decision on a replica whose status trails its
// spec: the pass requeues in ten seconds and leaves the park condition as it
// was.
func TestParkedWaitsOnAStaleReplica(t *testing.T) {
	isvc := parkedOnMissingPolicy(t)
	before := isvc.Status.DeepCopy()
	stale := divergedIR(oldRev, newRev)
	stale.Status.ObservedGeneration = 0
	out := passAt(t, isvc, time.Unix(2000, 0), stale)
	if out.RequeueAfter != shortRequeue || out.Parked || out.Opened || v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("outcome = %+v, want a ten-second wait with no decision", out)
	}
	if !reflect.DeepEqual(before, &isvc.Status) {
		t.Fatalf("the wait rewrote the status:\nbefore %+v\nafter  %+v", before, isvc.Status)
	}
}

// A revert still draining under a reference that does not resolve counts as
// in flight: the adoption is attempted and parks again; so does a ladder
// found short of its end, which is adopted once the plan composes. A record
// whose rejection was cleared reads as a divergence again and parks the same
// way.
func TestParkedAdoptionIsReattemptedUntilThePlanComposes(t *testing.T) {
	t.Run("canary.rolledBack[inFlight]", func(t *testing.T) {
		isvc := isvcFixture(refCanary(policyName))
		setEngineUnit(isvc, v1beta1.RolloutPhaseRollingBack, rejectedRecord(t))
		draining := irPublished(t, oldRev, newRev, []workloadtypes.InstanceStatus{rowOn(0, oldRev), rowOn(1, newRev)})
		for i := 0; i < 2; i++ {
			requireParked(t, pass(t, isvc, draining), isvc, v1beta1.RolloutPlanReasonPolicyNotFound)
		}
		out := pass(t, isvc, draining, canaryPolicy(policyName))
		if !out.Opened || !out.Adopted {
			t.Fatalf("outcome = %+v, want the revert adopted once the plan composes", out)
		}
	})
	t.Run("ctrl.runLost", func(t *testing.T) {
		isvc := isvcFixture(refCanary(policyName))
		setEngineUnit(isvc, v1beta1.RolloutPhaseCanarying, midLadderRecord(t))
		for i := 0; i < 2; i++ {
			requireParked(t, pass(t, isvc, divergedIR(oldRev, newRev)), isvc, v1beta1.RolloutPlanReasonPolicyNotFound)
		}
		out := pass(t, isvc, divergedIR(oldRev, newRev), canaryPolicy(policyName))
		if !out.Opened || !out.Adopted {
			t.Fatalf("outcome = %+v, want the ladder adopted once the plan composes", out)
		}
		if cs := isvc.Status.Components[v1beta1.EngineComponent].Canary; cs.CurrentStep != 0 || cs.CanaryRevisionHash != hashOf(t, newRev) {
			t.Fatalf("adoption touched the record: %+v", cs)
		}
	})
	t.Run("canary.resumed", func(t *testing.T) {
		isvc := isvcFixture(refCanary(policyName))
		setEngineUnit(isvc, v1beta1.RolloutPhaseRolledBack, rejectedRecord(t))
		isvc.Spec.Rollout.Groups = append(isvc.Spec.Rollout.Groups, v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{},
		})
		held := divergedIR(oldRev, newRev)
		decoder := namedIR("llm-a-decoder", "llm-a-decoder-dddddddd", "llm-a-decoder-eeeeeeee")
		requireParked(t, pass(t, isvc, held, decoder), isvc, v1beta1.RolloutPlanReasonPolicyNotFound)

		resumed := rejectedRecord(t)
		resumed.RolledBackRevisionHash = ""
		setEngineUnit(isvc, v1beta1.RolloutPhasePending, resumed)
		requireParked(t, pass(t, isvc, held, decoder), isvc, v1beta1.RolloutPlanReasonPolicyNotFound)
		// The re-armed record is mid-ladder, so the open that lifts the
		// park reports Adopted and leaves the record as the executor set it.
		out := pass(t, isvc, held, decoder, canaryPolicy(policyName))
		if !out.Opened || !out.Adopted {
			t.Fatalf("outcome = %+v, want an open toward the resumed target", out)
		}
		if got := pinnedRevision(requirePinned(t, isvc).TargetRevisions, v1beta1.EngineComponent); got != hashOf(t, newRev) {
			t.Fatalf("pinned %s, want the formerly rejected revision %s", got, hashOf(t, newRev))
		}
	})
}

// A parked pass with nothing changed composes the plan again, parks for the
// same reason, writes nothing and emits nothing; the one-minute requeue is
// re-armed.
func TestParkedResyncRewritesNothing(t *testing.T) {
	isvc := isvcFixture(refCanary(policyName))
	first, events := passWith(t, isvc, func(in *Inputs) { in.Now = time.Unix(1000, 0) }, divergedIR(oldRev, newRev))
	requireParked(t, first, isvc, v1beta1.RolloutPlanReasonPolicyNotFound)
	if len(events) != 1 {
		t.Fatalf("the park emitted %v, want one RolloutPlanParked event", events)
	}
	before := isvc.Status.DeepCopy()
	again, events := passWith(t, isvc, func(in *Inputs) { in.Now = time.Unix(2000, 0) }, divergedIR(oldRev, newRev))
	requireParked(t, again, isvc, v1beta1.RolloutPlanReasonPolicyNotFound)
	if len(events) != 0 {
		t.Fatalf("the same park re-emitted %v", events)
	}
	if !reflect.DeepEqual(before, &isvc.Status) {
		t.Fatalf("the same park rewrote the status:\nbefore %+v\nafter  %+v", before, isvc.Status)
	}
}

// A conflict rebase of a parked pass onto a live object that pinned a run
// in the meantime keeps the live run: a copy with no run state has the
// zero clock, so any pin is newer.
func TestPreserveNewerRunKeepsALivePinOverAParkedCopy(t *testing.T) {
	parked := &v1beta1.InferenceServiceStatus{Rollout: &v1beta1.RolloutStatus{Groups: []v1beta1.RolloutGroupResolution{{Index: 0}}}}
	opened := metav1.NewTime(time.Unix(2000, 0))
	live := &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{RunID: "run-1", OpenedAt: opened, PinnedAt: opened}}
	PreserveNewerRun(parked, live, false)
	if parked.Rollout.ActiveRun == nil || parked.Rollout.ActiveRun.RunID != "run-1" {
		t.Fatalf("rebased status = %+v, want the live pin kept", parked.Rollout)
	}
	if len(parked.Rollout.Groups) != 1 {
		t.Fatal("the rebase dropped the pass's own resolution view")
	}
}
