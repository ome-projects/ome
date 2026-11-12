package canary

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// stoppedFailed builds a single-engine canary stopped in the Failed terminal at
// step 0 on revision "new", with the resume verb set to value.
func stoppedFailed(value string) (*v1beta1.InferenceService, ReconcileInputs) {
	return stoppedFailedOn(canaryISVC(twoStep(), nil), value)
}

// stoppedFailedOn stops the engine unit of isvc in the Failed terminal at step
// 0 on revision "new", with the resume verb set to value.
func stoppedFailedOn(isvc *v1beta1.InferenceService, value string) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc = pinActiveRun(isvc)
	isvc.Namespace = "ns"
	isvc.Annotations = map[string]string{constants.RolloutResumeAnnotation: value}
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:   "new",
		StableRevisionHash:   "old",
		CurrentStep:          0,
		AnalysisFailedChecks: 3,
		StepEnteredTime:      &metav1.Time{Time: time.Unix(500, 0)},
	}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseFailed)
	return isvc, baseInputs(isvc, map[string]int32{"new": 0, "old": 4})
}

// recordedStop gives a stopped unit its recorded reason, so a pass over it
// raises no event of its own and only the verb under test can speak.
func recordedStop(isvc *v1beta1.InferenceService) {
	isvc.Status.Canary.Failed = &v1beta1.CanaryFailure{Reason: v1beta1.CanaryFailureCapacityTimeout, Time: &metav1.Time{Time: time.Unix(600, 0)}}
}

func eventsFrom(rec *record.FakeRecorder) []string {
	out := []string{}
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
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

// withRouterUnit adds a second canary unit, the router, so a verb can be
// addressed to a unit other than the engine.
func withRouterUnit(isvc *v1beta1.InferenceService) *v1beta1.InferenceService {
	isvc.Spec.Rollout.Groups = append(isvc.Spec.Rollout.Groups, v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent},
		Canary:     &v1beta1.GroupCanary{Steps: twoStep()},
	})
	return isvc
}

func resumeCount(isvc *v1beta1.InferenceService, outcome string) float64 {
	return testutil.ToFloat64(canaryResumeTotal.WithLabelValues(isvc.Namespace, isvc.Name, string(v1beta1.EngineComponent), outcome))
}

// TestResume_ClearsFailedHold is the verb's reason to exist: a stopped Failed
// canary with a fresh target and no way out re-enters the steps at step 0
// against the revision already there, without minting a new one.
func TestResume_ClearsFailedHold(t *testing.T) {
	isvc, in := stoppedFailed("new")
	rec := record.NewFakeRecorder(8)
	in.Recorder = rec

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !res.Active || res.RequeueAfter != reconcileRequeue {
		t.Fatalf("resume should stay active on the standard requeue, got %+v", res)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("resume must clear the terminal phase, got %q", phaseOf(isvc))
	}
	cs := isvc.Status.Canary
	if cs.CurrentStep != 0 || cs.AnalysisFailedChecks != 0 || cs.CanaryRevisionHash != "new" {
		t.Fatalf("resume must start again at step 0 on the stopped revision with a fresh budget, got %+v", cs)
	}
	if cs.StableRevisionHash != "old" {
		t.Fatalf("resume must preserve the pre-canary stable identity, got %q", cs.StableRevisionHash)
	}
	if !cs.StepEnteredTime.Time.Equal(in.Now) {
		t.Fatalf("resume must re-anchor the step clock, got %v", cs.StepEnteredTime.Time)
	}
	if _, still := isvc.Annotations[constants.RolloutResumeAnnotation]; still {
		t.Fatal("resume must consume its annotation")
	}
	if !hasEvent(eventsFrom(rec), EventReasonRolloutResumed) {
		t.Fatal("an applied resume must be visible as an Event")
	}

	// The canary actually runs on the next pass rather than staying stopped.
	in.Now = in.Now.Add(reconcileRequeue)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile after resume: %v", err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("resumed canary should wait on capacity at step 0, got %q", phaseOf(isvc))
	}
}

// TestResume_ScopedToComponent checks the "<component>=<hash>" form: the scope
// names which canary unit the verb addresses.
func TestResume_ScopedToComponent(t *testing.T) {
	isvc, in := stoppedFailed("engine=new")
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("a scope naming this Component must resume it, got %q", phaseOf(isvc))
	}
}

// TestResume_ScopedElsewhereIsUntouched checks the non-cannibalization rule: a
// verb addressed to another canary unit is neither acted on nor consumed, so
// that unit's own dispatch still sees it.
func TestResume_ScopedElsewhereIsUntouched(t *testing.T) {
	isvc, in := stoppedFailedOn(withRouterUnit(canaryISVC(twoStep(), nil)), "router=new")
	recordedStop(isvc)
	rec := record.NewFakeRecorder(8)
	in.Recorder = rec

	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("a verb scoped to another Component must not resume this one, got %q", phaseOf(isvc))
	}
	if isvc.Annotations[constants.RolloutResumeAnnotation] != "router=new" {
		t.Fatal("a verb scoped to another Component must survive this Component's pass")
	}
	if len(eventsFrom(rec)) != 0 {
		t.Fatal("a verb addressed elsewhere must stay silent here")
	}
}

// TestResume_BareHashAddressesOnlyItsOwnCanary is the same rule for the bare
// form: an unscoped hash belongs to whichever unit's canary carries it.
func TestResume_BareHashAddressesOnlyItsOwnCanary(t *testing.T) {
	isvc, in := stoppedFailedOn(withRouterUnit(canaryISVC(twoStep(), nil)), "somebodyelse")
	recordedStop(isvc)
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, isvc.Status.Canary)
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.RouterComponent, &v1beta1.CanaryStatus{
		CanaryRevisionHash: "somebodyelse",
		StableRevisionHash: "router-old",
	})
	rec := record.NewFakeRecorder(8)
	in.Recorder = rec

	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("a hash naming another unit's canary must not resume this one, got %q", phaseOf(isvc))
	}
	if isvc.Annotations[constants.RolloutResumeAnnotation] != "somebodyelse" {
		t.Fatal("a hash naming another unit's canary must be left for the unit that owns it")
	}
	if len(eventsFrom(rec)) != 0 {
		t.Fatal("a verb addressed elsewhere must stay silent here")
	}
}

// TestResume_UnaddressedOnIdleServiceIsConsumed checks the Idle cell of the
// grid: a request naming the stable revision of a service with no canary
// addresses nothing, so it is consumed and ignored, said out loud and
// counted. Left in place it would fire on a later canary minted with the
// same hash, a stop it was never meant for.
func TestResume_UnaddressedOnIdleServiceIsConsumed(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = "ns"
	isvc.Annotations = map[string]string{constants.RolloutResumeAnnotation: "old"}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithObjects(isvc).Build()
	in := baseInputs(isvc, map[string]int32{"old": 4})
	in.Client = c
	in.RunActive = false
	in.CanaryRevisionHash, in.StableRevisionHash = "old", ""
	rec := record.NewFakeRecorder(8)
	in.Recorder = rec
	before := resumeCount(isvc, resumeIgnored)

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.Active || rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent) != nil {
		t.Fatalf("an unaddressed resume must not start anything, got %+v", res)
	}
	if _, still := isvc.Annotations[constants.RolloutResumeAnnotation]; still {
		t.Fatal("a resume that addresses no canary must be consumed")
	}
	if len(res.Consume) != 1 || res.Consume[0] != constants.RolloutResumeAnnotation {
		t.Fatalf("the pass must hand the resume back for removal after the flush, got %v", res.Consume)
	}
	if _, present := storedAnnotations(t, c, isvc)[constants.RolloutResumeAnnotation]; !present {
		t.Fatal("the pass must not remove the annotation before the status flush")
	}
	events := eventsFrom(rec)
	if !hasEvent(events, EventReasonRolloutResumeRejected) || !hasEvent(events, "addresses no canary") {
		t.Fatalf("an unaddressed resume must be reported as addressing no canary, got %v", events)
	}
	if got := resumeCount(isvc, resumeIgnored) - before; got != 1 {
		t.Fatalf("an unaddressed resume must be counted once as ignored, got %v", got)
	}
}

// TestResume_UnaddressedWhileCanaryLiveIsConsumed is the same rule with a
// canary in flight: a request naming a revision no canary carries is
// consumed without touching the live step.
func TestResume_UnaddressedWhileCanaryLiveIsConsumed(t *testing.T) {
	isvc, in := stoppedFailed("old")
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseCanarying)
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
	rec := record.NewFakeRecorder(8)
	in.Recorder = rec

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	cs := isvc.Status.Canary
	if cs.CanaryRevisionHash != "new" || cs.CurrentStep != 0 || cs.AnalysisFailedChecks != 3 {
		t.Fatalf("an unaddressed resume must not touch the live canary, got %+v", cs)
	}
	if !res.Active || phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("the live step must run on unchanged, got %+v phase %q", res, phaseOf(isvc))
	}
	if len(res.Consume) != 1 || res.Consume[0] != constants.RolloutResumeAnnotation {
		t.Fatalf("a resume that addresses no canary must be consumed, got %v", res.Consume)
	}
	if !hasEvent(eventsFrom(rec), EventReasonRolloutResumeRejected) {
		t.Fatal("an unaddressed resume must be reported, not silently dropped")
	}
}

// TestResume_ScopedToNoUnitIsConsumed: a scope naming a Component that is
// no canary unit of the service addresses nothing either.
func TestResume_ScopedToNoUnitIsConsumed(t *testing.T) {
	isvc, in := stoppedFailed("router=new")
	rec := record.NewFakeRecorder(8)
	in.Recorder = rec

	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("a scope naming no unit must not resume this one, got %q", phaseOf(isvc))
	}
	if _, still := isvc.Annotations[constants.RolloutResumeAnnotation]; still {
		t.Fatal("a scope naming no unit must be consumed")
	}
	if !hasEvent(eventsFrom(rec), EventReasonRolloutResumeRejected) {
		t.Fatal("a scope naming no unit must be reported")
	}
}

// TestResume_ScopedToTheUnitsOtherMember: the decoder shares the engine's
// unit, so a scope naming it addresses that unit's stopped canary.
func TestResume_ScopedToTheUnitsOtherMember(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	isvc, in := stoppedFailedOn(isvc, "decoder=new")
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("a scope naming the unit's other member must resume it, got %q", phaseOf(isvc))
	}
}

// TestResume_StaleHashRejected checks the CAS guard under an explicit scope: the
// scope addresses this Component, so the mismatch is a real rejection — said
// out loud, and consumed so it cannot retry forever.
func TestResume_StaleHashRejected(t *testing.T) {
	isvc, in := stoppedFailed("engine=stalehash")
	rec := record.NewFakeRecorder(8)
	in.Recorder = rec

	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("a stale hash must leave the hold in place, got %q", phaseOf(isvc))
	}
	if isvc.Status.Canary.AnalysisFailedChecks != 3 {
		t.Fatal("a rejected resume must not reset canary state")
	}
	if _, still := isvc.Annotations[constants.RolloutResumeAnnotation]; still {
		t.Fatal("a rejected resume must still be consumed")
	}
	if !hasEvent(eventsFrom(rec), EventReasonRolloutResumeRejected) {
		t.Fatal("a rejected resume must say so rather than fail silently")
	}
}

// TestResume_RejectedWithoutPinnedRun checks the run license on a stop that
// holds no rejection: such a stop keeps its run open by construction, so one
// without a run has no recorded canary to re-enter and no rejection whose
// clearing would make the run layer open one. That holds whatever stopped
// the unit: the capacity wait running out, or a gate-decided rollback that
// found a member's stable revision running no Instance and stopped where it
// stood without recording a rejection.
func TestResume_RejectedWithoutPinnedRun(t *testing.T) {
	cases := []struct {
		name string
		stop func(isvc *v1beta1.InferenceService, in ReconcileInputs)
	}{
		{"capacity wait ran out", func(*v1beta1.InferenceService, ReconcileInputs) {}},
		{"stable revision missing, no rejection recorded", func(isvc *v1beta1.InferenceService, in ReconcileInputs) {
			isvc.Status.Canary.Failed = &v1beta1.CanaryFailure{Reason: v1beta1.CanaryFailureStableRevisionMissing, Time: &metav1.Time{Time: in.Now}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc, in := stoppedFailed("new")
			tc.stop(isvc, in)
			in.RunActive = false
			rec := record.NewFakeRecorder(8)
			in.Recorder = rec

			if _, err := Reconcile(context.Background(), in); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
				t.Fatalf("no recorded run must leave the hold in place, got %q", phaseOf(isvc))
			}
			if isvc.Status.Canary.AnalysisFailedChecks != 3 {
				t.Fatal("a rejected resume must not reset canary state")
			}
			if _, still := isvc.Annotations[constants.RolloutResumeAnnotation]; still {
				t.Fatal("a rejected resume must still be consumed")
			}
			if !hasEvent(eventsFrom(rec), EventReasonRolloutResumeRejected) {
				t.Fatal("no recorded run must be reported, not silently ignored")
			}
		})
	}
}

// stoppedForAMissingStableRevision builds the stop a settled rollback reaches
// once its stable revision is gone: the operator rolled version 2 back, the
// revert settled on version 1 and the run closed RolledBack; the stable
// revision's ControllerRevision was then lost, so the pass that re-resolves
// it stopped the unit Failed with the stable revision missing, the rejection
// still on the record and no run open. The resume verb is set to value.
func stoppedForAMissingStableRevision(value string) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = "ns"
	isvc.Annotations = map[string]string{constants.RolloutResumeAnnotation: value}
	now := time.Unix(1000, 0)
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:     "new",
		StableRevisionHash:     "old",
		RolledBackRevisionHash: "new",
		Failed:                 &v1beta1.CanaryFailure{Reason: v1beta1.CanaryFailureStableRevisionMissing, Time: &metav1.Time{Time: now}},
		StepEnteredTime:        &metav1.Time{Time: time.Unix(500, 0)},
	}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseFailed)
	isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{
		Outcome: v1beta1.RolloutRunRolledBack,
		TargetRevisions: []v1beta1.RolloutRunTarget{
			{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
		},
	}}
	in := baseInputs(isvc, map[string]int32{"old": 4})
	in.RunActive = false
	return isvc, in
}

// TestResume_TakenOnAMissingStableStopWithNoRun: a stop that holds a
// rejection needs no run open to resume. Four engines rolled version 2
// back, the run closed, and the stable revision's ControllerRevision was
// lost afterwards, so the unit stopped Failed with the stable revision
// missing and no run open. The operator's resume naming version 2 is taken
// like a resume of the settled rollback: the rejection and the stop are
// cleared, the record starts again at step 0 toward version 2 with version
// 1 kept as its stable, nothing is asked to roll back, and the run layer
// reads the cleared target as pending work on its next pass. The same stop
// reached under an open run already took the resume.
func TestResume_TakenOnAMissingStableStopWithNoRun(t *testing.T) {
	isvc, in := stoppedForAMissingStableRevision("new")
	rec := record.NewFakeRecorder(8)
	in.Recorder = rec

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !res.Active || res.RolledBack || res.RequeueAfter != reconcileRequeue {
		t.Fatalf("the resume must leave the unit live on the standard requeue with no revert signalled, got %+v", res)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("the resume must clear the stop, got phase %q", phaseOf(isvc))
	}
	cs := isvc.Status.Canary
	if cs.RolledBackRevisionHash != "" || cs.Failed != nil {
		t.Fatalf("the resume must clear the rejection and the stop from the record, got %+v", cs)
	}
	if cs.CurrentStep != 0 || cs.CanaryRevisionHash != "new" || cs.StableRevisionHash != "old" {
		t.Fatalf("the resume must start again at step 0 toward the stopped revision over its stable, got %+v", cs)
	}
	if _, still := isvc.Annotations[constants.RolloutResumeAnnotation]; still {
		t.Fatal("the resume must consume its annotation")
	}
	events := eventsFrom(rec)
	if !hasEvent(events, EventReasonRolloutResumed) || hasEvent(events, EventReasonRolloutResumeRejected) {
		t.Fatalf("the resume must be reported as applied, got %v", events)
	}

	// The run layer takes the record over on its next pass; under it the
	// steps run again from the record, waiting on step 0's capacity.
	in.RunActive = true
	in.Now = in.Now.Add(reconcileRequeue)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile after resume: %v", err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePending || isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("the resumed canary should wait on capacity at step 0, got %q step %d", phaseOf(isvc), isvc.Status.Canary.CurrentStep)
	}
}

// TestResume_NoTerminalPhaseIsNoop checks the no-op branch: an addressed verb
// with nothing stopped changes no state, and is consumed rather than left to
// fire at some later hold.
func TestResume_NoTerminalPhaseIsNoop(t *testing.T) {
	isvc, in := stoppedFailed("new")
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseCanarying)
	isvc.Status.Canary.AnalysisFailedChecks = 3

	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if isvc.Status.Canary.AnalysisFailedChecks != 3 {
		t.Fatal("a no-op resume must not reset a live canary")
	}
	if _, still := isvc.Annotations[constants.RolloutResumeAnnotation]; still {
		t.Fatal("an addressed no-op resume must be consumed")
	}
}

// TestResume_PausedRolloutIgnoresTheVerb checks pause depth: a paused rollout
// observes only — no state change and no annotation consumption — so the
// resume is still there to apply when the pause clears.
func TestResume_PausedRolloutIgnoresTheVerb(t *testing.T) {
	isvc, in := stoppedFailed("new")
	isvc.Annotations[constants.PausedRolloutAnnotation] = "true"

	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("a paused rollout must not resume, got %q", phaseOf(isvc))
	}
	if isvc.Annotations[constants.RolloutResumeAnnotation] != "new" {
		t.Fatal("a paused rollout must not consume the verb")
	}
}

// TestResume_ConsumptionFollowsTheFlush checks the verb's removal contract: the
// pass applies the resume in status and hands the annotation back, the
// controller removes it after the status write has landed, and a copy that
// outlives that write (a stale cache, a failed removal) finds nothing stopped
// and is consumed without starting anything again.
func TestResume_ConsumptionFollowsTheFlush(t *testing.T) {
	isvc, in := stoppedFailed("new")
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithObjects(isvc).Build()
	in.Client = c

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(res.Consume) != 1 || res.Consume[0] != constants.RolloutResumeAnnotation {
		t.Fatalf("the pass must hand the resume back for removal after the flush, got %v", res.Consume)
	}
	if _, present := storedAnnotations(t, c, isvc)[constants.RolloutResumeAnnotation]; !present {
		t.Fatal("the pass must not remove the annotation before its status write is durable")
	}
	stepEnteredAt := isvc.Status.Canary.StepEnteredTime.Time

	// The controller flushed and removed it; a stale cache still shows it.
	flushed(t, c, isvc, res)
	if _, still := storedAnnotations(t, c, isvc)[constants.RolloutResumeAnnotation]; still {
		t.Fatal("resume consumption must be persisted to the apiserver after the flush")
	}
	isvc.Annotations = map[string]string{constants.RolloutResumeAnnotation: "new"}
	in.Now = in.Now.Add(reconcileRequeue)
	res, err = Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("Reconcile with a lingering resume: %v", err)
	}
	if !isvc.Status.Canary.StepEnteredTime.Time.Equal(stepEnteredAt) || isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("a lingering resume on a canary started again must not start it once more, got %+v", isvc.Status.Canary)
	}
	if len(res.Consume) != 1 || res.Consume[0] != constants.RolloutResumeAnnotation {
		t.Fatalf("a lingering resume with nothing stopped is consumed, got %v", res.Consume)
	}
}

// TestResume_ClearsRolledBackHoldAndTakesTheRollbackVerb covers the second
// terminal. A rollback that was requested by annotation leaves the verb live
// (it is consumed only on a fresh start), so a resume that did not take it with the
// phase would start the hold again on the very next pass.
func TestResume_ClearsRolledBackHoldAndTakesTheRollbackVerb(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(twoStep(), nil))
	isvc.Namespace = "ns"
	isvc.Annotations = map[string]string{
		constants.RolloutRollbackAnnotation: "true",
		constants.RolloutResumeAnnotation:   "new",
	}
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:     "new",
		StableRevisionHash:     "old",
		RolledBackRevisionHash: "new",
	}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseRolledBack)

	in := baseInputs(isvc, map[string]int32{"old": 4})
	// A completed rollback moved every Instance back to the stable revision,
	// but the replica keeps reporting the spec's revision as its target, so
	// the observed target still names the stopped canary. The rollback closed
	// the run, which is why this terminal needs no recorded plan.
	in.CanaryRevisionHash = "new"
	in.RunActive = false

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RolledBack {
		t.Fatal("a resumed rollback must stop signalling the revert, or the IR stays set to stable")
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("resume must clear the RolledBack hold, got %q", phaseOf(isvc))
	}
	cs := isvc.Status.Canary
	if cs.RolledBackRevisionHash != "" {
		t.Fatalf("resume must drop the rejection, got %q", cs.RolledBackRevisionHash)
	}
	if cs.CanaryRevisionHash != "new" {
		t.Fatalf("resume must start again on the STOPPED revision, not the observed stable one, got %q", cs.CanaryRevisionHash)
	}
	for _, key := range []string{constants.RolloutResumeAnnotation, constants.RolloutRollbackAnnotation} {
		if _, still := isvc.Annotations[key]; still {
			t.Fatalf("resume must consume %s, or the hold starts again next pass", key)
		}
	}
}

// TestResume_RolledBackDoesNotRunTheStepsOnTheResumePass is why a resume
// returns instead of falling through: the pass that clears the rejection
// writes the record of the fresh start and drops the replica's rollback target, and
// the steps run from the record the next pass reads back. No traffic moves
// on the resume pass.
func TestResume_RolledBackDoesNotRunTheStepsOnTheResumePass(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(twoStep(), nil))
	isvc.Annotations = map[string]string{constants.RolloutResumeAnnotation: "new"}
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:     "new",
		StableRevisionHash:     "old",
		RolledBackRevisionHash: "new",
	}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseRolledBack)

	in := baseInputs(isvc, map[string]int32{"old": 4})
	in.RunActive = false
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := isvc.Status.Components[v1beta1.EngineComponent].Traffic; len(got) != 0 {
		t.Fatalf("no traffic may be programmed on the resume pass, got %+v", got)
	}
}

// TestResume_RefusedWhenNoMemberTargetsTheStoppedRevision: a resume that
// arrives after the operator set the spec back to the stable revision has
// nothing to re-enter. Four engine Instances, version 2 rolled back, the spec
// back on version 1, then a resume naming version 2: the verb is refused with
// a warning that names the value and each member's target, the hold keeps
// its revert signal, the annotation is removed, and the next pass changes
// nothing. Without the refusal the record restarted the steps toward version 2 and the
// run layer then recorded a canary of version 1 over itself.
func TestResume_RefusedWhenNoMemberTargetsTheStoppedRevision(t *testing.T) {
	cases := []struct {
		name        string
		members     []v1beta1.ComponentType
		secondaries map[v1beta1.ComponentType]MemberRevisions
		wantTargets string
	}{
		{
			name:        "an engine alone",
			members:     []v1beta1.ComponentType{v1beta1.EngineComponent},
			wantTargets: "engine targets old",
		},
		{
			name:    "an engine with a decoder",
			members: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			secondaries: map[v1beta1.ComponentType]MemberRevisions{
				v1beta1.DecoderComponent: {CanaryRevisionHash: "dec-old", StableRevisionHash: "dec-old"},
			},
			wantTargets: "engine targets old, decoder targets dec-old",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := canaryISVC(twoStep(), nil)
			isvc.Namespace = "ns"
			isvc.Spec.Rollout.Groups[0].Components = tc.members
			isvc.Annotations = map[string]string{constants.RolloutResumeAnnotation: "new"}
			isvc.Status.Canary = &v1beta1.CanaryStatus{
				CanaryRevisionHash:     "new",
				StableRevisionHash:     "old",
				RolledBackRevisionHash: "new",
			}
			setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseRolledBack)

			in := baseInputs(isvc, map[string]int32{"old": 4})
			// The spec went back to the stable revision: every member reports
			// it as its target, and the run that presented version 2 closed.
			in.CanaryRevisionHash = "old"
			in.RunActive = false
			in.Secondaries = tc.secondaries
			rec := record.NewFakeRecorder(8)
			in.Recorder = rec
			rejected := resumeCount(isvc, resumeRejected)

			res, err := Reconcile(context.Background(), in)
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if phaseOf(isvc) != v1beta1.RolloutPhaseRolledBack || !res.RolledBack {
				t.Fatalf("a refused resume must leave the hold and its revert signal in place, got phase %q rolledBack=%v", phaseOf(isvc), res.RolledBack)
			}
			cs := isvc.Status.Canary
			if cs.RolledBackRevisionHash != "new" || cs.CanaryRevisionHash != "new" || cs.CurrentStep != 0 {
				t.Fatalf("a refused resume must not touch the record, got %+v", cs)
			}
			if _, still := isvc.Annotations[constants.RolloutResumeAnnotation]; still {
				t.Fatal("a refused resume must be removed, or it is read again on every pass")
			}
			if len(res.Consume) != 1 || res.Consume[0] != constants.RolloutResumeAnnotation {
				t.Fatalf("the refusal must hand the verb back for removal after the flush, got %v", res.Consume)
			}
			events := eventsFrom(rec)
			named := false
			for _, e := range events {
				if strings.Contains(e, EventReasonRolloutResumeRejected) && strings.Contains(e, `"new"`) && strings.Contains(e, tc.wantTargets) {
					named = true
				}
			}
			if !named {
				t.Fatalf("the warning must name the value and every member's target, got %v", events)
			}
			if got := resumeCount(isvc, resumeRejected); got != rejected+1 {
				t.Fatalf("the refusal must count as a rejected resume, got %v, want %v", got, rejected+1)
			}

			// The next pass finds the same hold: nothing re-entered the steps.
			in.Now = in.Now.Add(reconcileRequeue)
			res, err = Reconcile(context.Background(), in)
			if err != nil {
				t.Fatalf("Reconcile after the refusal: %v", err)
			}
			if phaseOf(isvc) != v1beta1.RolloutPhaseRolledBack || !res.RolledBack || res.Stepped {
				t.Fatalf("the hold must stand on the pass after a refused resume, got phase %q %+v", phaseOf(isvc), res)
			}
		})
	}
}

// TestDispatch_ResumeClearsTheIRRollbackTarget closes the loop on the
// RolledBack terminal: clearing the hold is only half the escape, because the
// IR is still overridden onto the stable ControllerRevision and would keep
// every Instance there. The same pass that resumes must drop that override so
// the Component can roll forward again.
func TestDispatch_ResumeClearsTheIRRollbackTarget(t *testing.T) {
	ns := "default"
	n4 := 4
	// No recorded run: the completed rollback closed it.
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = ns
	isvc.Name = "resumed"
	isvc.Annotations = map[string]string{constants.RolloutResumeAnnotation: "target"}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:     "target",
		StableRevisionHash:     "stable",
		RolledBackRevisionHash: "target",
	}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseRolledBack)

	stableCR := "resumed-engine-stable"
	engineIR := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "resumed-engine", Generation: 3}}
	engineIR.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	engineIR.Spec.Pacing = &v1beta1.InferenceReplicaPacing{RollbackToRevision: &stableCR}
	// The revert completed: every Instance runs the stable revision, so the
	// replica's current revision is stable, while its update revision keeps
	// reporting the spec's revision, the one the operator resumes.
	engineIR.Status.CurrentRevision = stableCR
	engineIR.Status.UpdateRevision = "resumed-engine-target"
	engineIR.Status.ObservedGeneration = 3

	c := fake.NewClientBuilder().
		WithScheme(canaryScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithRuntimeObjects(
			isvc, engineIR,
			canaryPod(ns, isvc.Name, "engine", "stable", "stable-0"),
			canaryPod(ns, isvc.Name, "engine", "stable", "stable-1"),
			canaryControllerRevision(ns, isvc.Name, "engine", "stable", 1),
			canaryControllerRevision(ns, isvc.Name, "engine", "target", 2),
		).
		Build()
	ctx := context.Background()
	syncInstanceRows(t, c, isvc, v1beta1.EngineComponent)
	if _, err := Dispatch(ctx, DispatchDeps{
		Client: c, Reader: c, ISVC: isvc,
		ComponentRunnerPorts: canaryRunnerPorts(),
		Group:                rollout.CanaryGroup(isvc, rollout.Policies{}),
	}); err != nil {
		t.Fatalf("Dispatch resume: %v", err)
	}

	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("resume must clear the RolledBack hold, got %q", phaseOf(isvc))
	}
	if isvc.Status.Canary.CanaryRevisionHash != "target" || isvc.Status.Canary.RolledBackRevisionHash != "" {
		t.Fatalf("resume must start again on the stopped revision, got %+v", isvc.Status.Canary)
	}
	live := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: engineIR.Name}, live); err != nil {
		t.Fatal(err)
	}
	if live.Spec.Pacing != nil && live.Spec.Pacing.RollbackToRevision != nil {
		t.Fatalf("resume must drop the IR rollback target, still %q", *live.Spec.Pacing.RollbackToRevision)
	}
}

// TestResetCanaryStatus_ClearsTerminalPhase is the regression for the one-way
// trap. The phase lives on the Component status, not in the canary status, so
// a fresh start that reset one and not the other published a canary open on the
// current target while still labelled with the phase it escaped — and the
// hold's own fresh-start test (observed target != cs.CanaryRevisionHash) then
// compares the target against itself and is false forever.
func TestResetCanaryStatus_ClearsTerminalPhase(t *testing.T) {
	for _, stopped := range []v1beta1.RolloutPhase{v1beta1.RolloutPhaseFailed, v1beta1.RolloutPhaseRolledBack} {
		isvc := canaryISVC(twoStep(), nil)
		setPhase(isvc, v1beta1.EngineComponent, stopped)
		cs := &v1beta1.CanaryStatus{CanaryRevisionHash: "old", StableRevisionHash: "old"}
		rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, cs)

		resetCanaryStatus(isvc, v1beta1.EngineComponent, cs, "t1", "new", time.Unix(1000, 0))

		if got := phaseOf(isvc); got == v1beta1.RolloutPhaseFailed || got == v1beta1.RolloutPhaseRolledBack {
			t.Fatalf("a fresh start out of %s must not leave a terminal phase behind, got %q", stopped, got)
		}
	}
}

// A fresh start projects Pending whatever phase it found: the restarted step has
// staged nothing and shifted nothing, and a serving phase left behind would
// make it read as a split that dipped rather than one staging from scratch.
// The step clocks are re-anchored by the reset itself, so nothing mid-bake
// survives it to be disturbed.
func TestResetCanaryStatus_ProjectsStagingPhase(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseCanarying)
	cs := &v1beta1.CanaryStatus{CanaryRevisionHash: "old"}
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, cs)

	resetCanaryStatus(isvc, v1beta1.EngineComponent, cs, "t1", "new", time.Unix(1000, 0))

	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("a fresh start must project the honest phase at step 0, got %q", phaseOf(isvc))
	}
}

func TestParseResume(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  resumeRequest
		ok    bool
	}{
		{value: "abc123", want: resumeRequest{hash: "abc123"}, ok: true},
		{value: "engine=abc123", want: resumeRequest{component: v1beta1.EngineComponent, hash: "abc123"}, ok: true},
		{value: ""},
		{value: "="},
		{value: "engine="},
		{value: "=abc123"},
	} {
		got, ok := parseResume(tc.value)
		if ok != tc.ok || got != tc.want {
			t.Fatalf("parseResume(%q) = (%+v, %v), want (%+v, %v)", tc.value, got, ok, tc.want, tc.ok)
		}
	}
}
