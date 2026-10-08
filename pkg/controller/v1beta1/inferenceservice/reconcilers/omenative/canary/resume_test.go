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

// parkedFailed builds a single-engine canary parked in the Failed terminal at
// step 0 on revision "new", with the resume verb set to value.
func parkedFailed(value string) (*v1beta1.InferenceService, ReconcileInputs) {
	return parkedFailedOn(canaryISVC(twoStep(), nil), value)
}

// parkedFailedOn parks the engine unit of isvc in the Failed terminal at step
// 0 on revision "new", with the resume verb set to value.
func parkedFailedOn(isvc *v1beta1.InferenceService, value string) (*v1beta1.InferenceService, ReconcileInputs) {
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

// TestResume_ClearsFailedHold is the verb's reason to exist: a parked Failed
// canary with a fresh target and no way out re-enters the ladder at step 0
// against the revision already there, without minting a new one.
func TestResume_ClearsFailedHold(t *testing.T) {
	isvc, in := parkedFailed("new")
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
		t.Fatalf("resume must re-arm at step 0 on the parked revision with a fresh budget, got %+v", cs)
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

	// The ladder actually runs on the next pass rather than staying parked.
	in.Now = in.Now.Add(reconcileRequeue)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile after resume: %v", err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("resumed canary should wait on capacity at step 0, got %q", phaseOf(isvc))
	}
}

// TestResume_ScopedToComponent pins the "<component>=<hash>" form: the scope
// names which canary unit the verb addresses.
func TestResume_ScopedToComponent(t *testing.T) {
	isvc, in := parkedFailed("engine=new")
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("a scope naming this Component must resume it, got %q", phaseOf(isvc))
	}
}

// TestResume_ScopedElsewhereIsUntouched pins the non-cannibalization rule: a
// verb addressed to another canary unit is neither acted on nor consumed, so
// that unit's own dispatch still sees it.
func TestResume_ScopedElsewhereIsUntouched(t *testing.T) {
	isvc, in := parkedFailedOn(withRouterUnit(canaryISVC(twoStep(), nil)), "router=new")
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
	isvc, in := parkedFailedOn(withRouterUnit(canaryISVC(twoStep(), nil)), "somebodyelse")
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

// TestResume_UnaddressedOnIdleServiceIsConsumed pins the Idle cell of the
// grid: a request naming the stable revision of a service with no canary
// addresses nothing, so it is consumed and ignored, said out loud and
// counted. Left in place it would fire on a later canary minted with the
// same hash, a park it was never meant for.
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
		t.Fatalf("an unaddressed resume must not arm anything, got %+v", res)
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
	isvc, in := parkedFailed("old")
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
	isvc, in := parkedFailed("router=new")
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
// unit, so a scope naming it addresses that unit's parked canary.
func TestResume_ScopedToTheUnitsOtherMember(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	isvc, in := parkedFailedOn(isvc, "decoder=new")
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("a scope naming the unit's other member must resume it, got %q", phaseOf(isvc))
	}
}

// TestResume_StaleHashRejected pins the CAS guard under an explicit scope: the
// scope addresses this Component, so the mismatch is a real rejection — said
// out loud, and consumed so it cannot retry forever.
func TestResume_StaleHashRejected(t *testing.T) {
	isvc, in := parkedFailed("engine=stalehash")
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

// TestResume_RejectedWithoutPinnedRun pins the run license on the Failed
// terminal: a Failed hold keeps its run open by construction, so one without a
// run has no pinned ladder to re-enter and no rollback signal to drop either.
func TestResume_RejectedWithoutPinnedRun(t *testing.T) {
	isvc, in := parkedFailed("new")
	in.RunActive = false
	rec := record.NewFakeRecorder(8)
	in.Recorder = rec

	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("no pinned run must leave the hold in place, got %q", phaseOf(isvc))
	}
	if _, still := isvc.Annotations[constants.RolloutResumeAnnotation]; still {
		t.Fatal("a rejected resume must still be consumed")
	}
	if !hasEvent(eventsFrom(rec), EventReasonRolloutResumeRejected) {
		t.Fatal("no pinned run must be reported, not silently ignored")
	}
}

// TestResume_NoTerminalPhaseIsNoop pins the no-op branch: an addressed verb
// with nothing parked changes no state, and is consumed rather than left to
// fire at some later hold.
func TestResume_NoTerminalPhaseIsNoop(t *testing.T) {
	isvc, in := parkedFailed("new")
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

// TestResume_PausedRolloutIgnoresTheVerb pins pause depth: a paused rollout
// observes only — no state change and no annotation consumption — so the
// resume is still there to apply when the pause clears.
func TestResume_PausedRolloutIgnoresTheVerb(t *testing.T) {
	isvc, in := parkedFailed("new")
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

// TestResume_ConsumptionFollowsTheFlush pins the verb's removal contract: the
// pass applies the resume in status and hands the annotation back, the
// controller removes it after the status write has landed, and a copy that
// outlives that write (a stale cache, a failed removal) finds nothing parked
// and is consumed without re-arming anything.
func TestResume_ConsumptionFollowsTheFlush(t *testing.T) {
	isvc, in := parkedFailed("new")
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
	armedAt := isvc.Status.Canary.StepEnteredTime.Time

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
	if !isvc.Status.Canary.StepEnteredTime.Time.Equal(armedAt) || isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("a lingering resume on a re-armed canary must not re-arm it again, got %+v", isvc.Status.Canary)
	}
	if len(res.Consume) != 1 || res.Consume[0] != constants.RolloutResumeAnnotation {
		t.Fatalf("a lingering resume with nothing parked is consumed, got %v", res.Consume)
	}
}

// TestResume_ClearsRolledBackHoldAndTakesTheRollbackVerb covers the second
// terminal. A rollback that was requested by annotation leaves the verb live
// (it is consumed only on a re-arm), so a resume that did not take it with the
// phase would re-arm the hold on the very next pass.
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
	// A completed rollback has already pointed the IR back at stable, so the
	// observed target names the STABLE revision, not the parked canary — and
	// it closed the run, which is why this terminal needs no pinned plan.
	in.CanaryRevisionHash = "old"
	in.RunActive = false

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RolledBack {
		t.Fatal("a resumed rollback must stop signalling the revert, or the IR stays pinned to stable")
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("resume must clear the RolledBack hold, got %q", phaseOf(isvc))
	}
	cs := isvc.Status.Canary
	if cs.RolledBackRevisionHash != "" {
		t.Fatalf("resume must drop the rejection, got %q", cs.RolledBackRevisionHash)
	}
	if cs.CanaryRevisionHash != "new" {
		t.Fatalf("resume must re-arm on the PARKED revision, not the observed stable one, got %q", cs.CanaryRevisionHash)
	}
	for _, key := range []string{constants.RolloutResumeAnnotation, constants.RolloutRollbackAnnotation} {
		if _, still := isvc.Annotations[key]; still {
			t.Fatalf("resume must consume %s, or the hold re-arms next pass", key)
		}
	}
}

// TestResume_RolledBackDoesNotRunTheLadderOnTheInvertedTarget is why a resume
// returns instead of falling through. Until the IR's rollback target clears,
// the observed target still names the stable revision; running the step
// machine against it would program stable as its own canary.
func TestResume_RolledBackDoesNotRunTheLadderOnTheInvertedTarget(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(twoStep(), nil))
	isvc.Annotations = map[string]string{constants.RolloutResumeAnnotation: "new"}
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:     "new",
		StableRevisionHash:     "old",
		RolledBackRevisionHash: "new",
	}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseRolledBack)

	in := baseInputs(isvc, map[string]int32{"old": 4})
	in.CanaryRevisionHash = "old"
	in.RunActive = false
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := isvc.Status.Components[v1beta1.EngineComponent].Traffic; len(got) != 0 {
		t.Fatalf("no traffic may be programmed on the resume pass, got %+v", got)
	}
}

// TestDispatch_ResumeUnpinsTheIRRollbackTarget closes the loop on the
// RolledBack terminal: clearing the hold is only half the escape, because the
// IR is still overridden onto the stable ControllerRevision and would keep
// every Instance there. The same pass that resumes must drop that override so
// the Component can roll forward again.
func TestDispatch_ResumeUnpinsTheIRRollbackTarget(t *testing.T) {
	ns := "default"
	n4 := 4
	// No pinned run: the completed rollback closed it.
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
	// The revert completed: the IR names stable as both its current and its
	// update revision, so the observed target reports stable.
	engineIR.Status.CurrentRevision = stableCR
	engineIR.Status.UpdateRevision = stableCR
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
		t.Fatalf("resume must re-arm on the parked revision, got %+v", isvc.Status.Canary)
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
// a re-arm that reset one and not the other published a canary armed on the
// current target while still labelled with the phase it escaped — and the
// hold's own re-arm test (observed target != cs.CanaryRevisionHash) then
// compares the target against itself and is false forever.
func TestResetCanaryStatus_ClearsTerminalPhase(t *testing.T) {
	for _, parked := range []v1beta1.RolloutPhase{v1beta1.RolloutPhaseFailed, v1beta1.RolloutPhaseRolledBack} {
		isvc := canaryISVC(twoStep(), nil)
		setPhase(isvc, v1beta1.EngineComponent, parked)
		cs := &v1beta1.CanaryStatus{CanaryRevisionHash: "old", StableRevisionHash: "old"}
		rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, cs)

		resetCanaryStatus(isvc, v1beta1.EngineComponent, cs, "t1", "new", time.Unix(1000, 0))

		if got := phaseOf(isvc); terminalPhase(got) {
			t.Fatalf("a re-arm out of %s must not leave a terminal phase behind, got %q", parked, got)
		}
	}
}

// A re-arm projects Pending whatever phase it found: the re-armed step has
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
		t.Fatalf("a re-arm must project the honest phase at step 0, got %q", phaseOf(isvc))
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
