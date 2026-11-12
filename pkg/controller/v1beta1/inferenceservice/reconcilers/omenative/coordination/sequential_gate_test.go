package coordination

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// fakeClientForSeqISVC creates a fake client seeded with InferenceReplica objects
// built from the ISVC's Status.Components, for sequential gate tests.
//
// Each Component's Lifecycle.ObservedGeneration is modeled as the parent
// ISVC generation the projector last applied to that IR: it lands in the
// parent-generation annotation the gate's projection-lag signal reads. A
// value that does not match isvc.Generation therefore models "this IR's
// projection was not produced from the ISVC generation being reconciled".
// The IR's own metadata.generation is set to match its
// status.observedGeneration so the same-object status-lag signal stays
// silent — these fakes isolate the projection-lag signal.
func fakeClientForSeqISVC(isvc *v1beta1.InferenceService) client.Client {
	if isvc == nil || isvc.Status.Components == nil {
		return fake.NewClientBuilder().WithScheme(testScheme()).Build()
	}
	var irs []client.Object
	for c, cs := range isvc.Status.Components {
		if cs.Lifecycle == nil {
			continue
		}
		ir := &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{
				Name:        isvc.Name + "-" + string(c),
				Namespace:   isvc.Namespace,
				Generation:  cs.Lifecycle.ObservedGeneration,
				Annotations: parentGenAnnotation(cs.Lifecycle.ObservedGeneration),
			},
			Status: v1beta1.InferenceReplicaStatus{
				ObservedGeneration: cs.Lifecycle.ObservedGeneration,
				CurrentRevision:    cs.Lifecycle.CurrentRevision,
				UpdateRevision:     cs.Lifecycle.UpdateRevision,
				Conditions:         cs.Lifecycle.Conditions,
			},
		}
		irs = append(irs, ir)
	}
	return fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(irs...).Build()
}

// parentGenAnnotation builds the projector's parent-generation stamp for a
// fake IR.
func parentGenAnnotation(gen int64) map[string]string {
	return map[string]string{
		constants.InferenceReplicaParentGenerationAnnotationKey: strconv.FormatInt(gen, 10),
	}
}

// --- activeSequentialComponent (pure helper shared with state machine) ---

func TestActiveSequentialComponent_PicksFirstInFlight(t *testing.T) {
	order := []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent}
	components := map[v1beta1.ComponentType]ComponentObservation{
		v1beta1.DecoderComponent: {Component: v1beta1.DecoderComponent, RolloutInFlight: true},
		v1beta1.EngineComponent:  {Component: v1beta1.EngineComponent, RolloutInFlight: false},
	}
	got, ok := activeSequentialComponent(order, components)
	if !ok {
		t.Fatal("expected ok=true when first in Order is rolling")
	}
	if got != v1beta1.DecoderComponent {
		t.Errorf("got %q want decoder", got)
	}
}

func TestActiveSequentialComponent_SkipsCompletedToFindRolling(t *testing.T) {
	order := []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent}
	components := map[v1beta1.ComponentType]ComponentObservation{
		v1beta1.DecoderComponent: {Component: v1beta1.DecoderComponent, RolloutInFlight: false},
		v1beta1.EngineComponent:  {Component: v1beta1.EngineComponent, RolloutInFlight: true},
	}
	got, ok := activeSequentialComponent(order, components)
	if !ok {
		t.Fatal("expected ok=true when later-in-Order is rolling")
	}
	if got != v1beta1.EngineComponent {
		t.Errorf("got %q want engine", got)
	}
}

func TestActiveSequentialComponent_NoneInFlightReturnsFalse(t *testing.T) {
	order := []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent}
	components := map[v1beta1.ComponentType]ComponentObservation{
		v1beta1.DecoderComponent: {Component: v1beta1.DecoderComponent, RolloutInFlight: false},
		v1beta1.EngineComponent:  {Component: v1beta1.EngineComponent, RolloutInFlight: false},
	}
	if _, ok := activeSequentialComponent(order, components); ok {
		t.Errorf("expected ok=false when no Component is rolling")
	}
}

func TestActiveSequentialComponent_RespectsOrderNotMapIteration(t *testing.T) {
	// Both Components rolling — should pick decoder because it's first
	// in Order, regardless of map iteration order.
	order := []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent}
	for i := 0; i < 100; i++ {
		components := map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {Component: v1beta1.DecoderComponent, RolloutInFlight: true},
			v1beta1.EngineComponent:  {Component: v1beta1.EngineComponent, RolloutInFlight: true},
		}
		got, ok := activeSequentialComponent(order, components)
		if !ok || got != v1beta1.DecoderComponent {
			t.Fatalf("iteration %d: got=%q ok=%v want decoder/true", i, got, ok)
		}
	}
}

// --- CheckSequentialGate ---

// mkSequentialFixture returns an ISVC declared with one Sequential
// coordination group [decoder, engine]. Caller can then mutate
// Status.Components and Status.RolloutCoordination to model the
// scenario under test.
func mkSequentialFixture(name string, soak time.Duration) *v1beta1.InferenceService {
	// v2: Sequential-across-Components is a RUN of single-Component
	// blueGreen groups, one per Component, in list order. ResolveGroups
	// collapses that run back into a single Sequential ResolvedGroup whose
	// Order is the Components in list order (and whose Soak is the max of
	// the per-group soaks).
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{
					{
						Components: []v1beta1.ComponentType{v1beta1.DecoderComponent},
						BlueGreen:  &v1beta1.GroupBlueGreen{},
					},
					{
						Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
						BlueGreen:  &v1beta1.GroupBlueGreen{},
					},
				},
			},
		},
	}
	if soak > 0 {
		// Inter-Component soak rides on the group(s); collapseSequential
		// takes the max across the run.
		isvc.Spec.Rollout.Groups[0].Soak = &metav1.Duration{Duration: soak}
	}
	isvc.Name = name
	return pinActiveRun(isvc)
}

// sequentialChainISVC is the decoder-then-engine chain of
// mkSequentialFixture with the given wait after the decoder, as the
// coordination pass reads it: the namespace and uid of the pass fixtures,
// both Components OMENative, and an open run holding the plan, so the
// replica-side gate answers too.
func sequentialChainISVC(wait time.Duration) *v1beta1.InferenceService {
	base := testOMENativeISVC()
	isvc := mkSequentialFixture(base.Name, wait)
	isvc.Namespace, isvc.UID = base.Namespace, base.UID
	isvc.Spec.Engine = base.Spec.Engine
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		},
	}
	return isvc
}

// setComponentRevisions stamps Status.Components[c].Lifecycle with
// ControllerRevision names that produce the desired RolloutInFlight
// answer (target!=current ⇒ in flight). Revision-name shape is
// <isvcName>-<component>-<hash>, which query.RevisionFromName splits
// to extract the trailing hash.
func setComponentRevisions(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, currentHash, targetHash string) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[c]
	if cs.Lifecycle == nil {
		cs.Lifecycle = &v1beta1.LifecycleStatus{}
	}
	cs.Lifecycle.CurrentRevision = isvc.Name + "-" + string(c) + "-" + currentHash
	cs.Lifecycle.UpdateRevision = isvc.Name + "-" + string(c) + "-" + targetHash
	isvc.Status.Components[c] = cs
}

// setComponentObservedGeneration stamps
// Status.Components[c].Lifecycle.ObservedGeneration with `gen`, which
// fakeClientForSeqISVC also mirrors into the fake IR's parent-generation
// annotation. Simulating a moment-of-bump therefore means setting it to
// N while leaving isvc.Generation at N+1 — the gate's projection-lag
// signal sees the stamp trailing the live generation.
func setComponentObservedGeneration(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, gen int64) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[c]
	if cs.Lifecycle == nil {
		cs.Lifecycle = &v1beta1.LifecycleStatus{}
	}
	cs.Lifecycle.ObservedGeneration = gen
	isvc.Status.Components[c] = cs
}

// setComponentReadyAt stamps Status.Components[c].Lifecycle with a
// Ready=True condition whose LastTransitionTime is `at` — the
// timestamp shape the Sequential soak gate consumes. Mirrors what the
// IR status writer stamps via apimeta.SetStatusCondition when the
// rollout converges (Unknown→True).
func setComponentReadyAt(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, at time.Time) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[c]
	if cs.Lifecycle == nil {
		cs.Lifecycle = &v1beta1.LifecycleStatus{}
	}
	cs.Lifecycle.Conditions = append(cs.Lifecycle.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             "AllInstancesReady",
		LastTransitionTime: metav1.NewTime(at),
	})
	isvc.Status.Components[c] = cs
}

func TestCheckSequentialGate_NilISVC(t *testing.T) {
	allowed, _ := CheckSequentialGate(context.Background(), nil, nil, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("nil ISVC should bypass the gate")
	}
}

func TestCheckSequentialGate_NoCoordBlock(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	client := fakeClientForSeqISVC(isvc)
	allowed, _ := CheckSequentialGate(context.Background(), client, isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("no coord block should bypass the gate")
	}
}

func TestCheckSequentialGate_ComponentNotInAnyGroup(t *testing.T) {
	isvc := mkSequentialFixture("seq-isvc", 0)
	// Router isn't in the [decoder, engine] group.
	client := fakeClientForSeqISVC(isvc)
	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.RouterComponent)
	if !allowed {
		t.Errorf("component outside any coord group must bypass: %s", reason)
	}
}

func TestCheckSequentialGate_NonSequentialGroupBypasses(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
					BlueGreen:  &v1beta1.GroupBlueGreen{},
				}},
			},
		},
	}
	pinActiveRun(isvc)
	client := fakeClientForSeqISVC(isvc)
	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("BlueGreen group must bypass Sequential gate: %s", reason)
	}
	if !strings.Contains(reason, "Sequential") {
		t.Errorf("reason should mention Sequential when bypassing: %s", reason)
	}
}

func TestCheckSequentialGate_IdleGroupBypasses(t *testing.T) {
	// Spec declares Sequential, but no Component has a target revision
	// — no rollout in flight, so the gate must bypass (the dispatcher
	// won't even call it if there's no work, but if it does, this is
	// the safe answer).
	isvc := mkSequentialFixture("seq-isvc", 0)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev1", "rev1")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev1")
	client := fakeClientForSeqISVC(isvc)
	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("idle group must bypass: %s", reason)
	}
}

func TestCheckSequentialGate_ActiveComponentAllowed(t *testing.T) {
	// Decoder is rolling (target!=current); engine is idle.
	// Decoder IS the active Component — gate must allow.
	isvc := mkSequentialFixture("seq-isvc", 0)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev1", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev1")
	client := fakeClientForSeqISVC(isvc)
	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.DecoderComponent)
	if !allowed {
		t.Errorf("decoder is active; gate must allow: %s", reason)
	}
}

func TestCheckSequentialGate_NonActiveComponentDenied(t *testing.T) {
	// Decoder is rolling; engine is also bumped but Sequential says
	// engine must wait. The gate must deny for engine and name the
	// active Component in the reason.
	isvc := mkSequentialFixture("seq-isvc", 0)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev1", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
	client := fakeClientForSeqISVC(isvc)
	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.EngineComponent)
	if allowed {
		t.Errorf("engine must wait while decoder rolls: %s", reason)
	}
	if !strings.Contains(reason, "decoder") {
		t.Errorf("denial reason should name the active Component: %s", reason)
	}
	if !strings.Contains(reason, "Sequential waiting") {
		t.Errorf("denial reason should say 'Sequential waiting': %s", reason)
	}
}

func TestCheckSequentialGate_LaterComponentActiveDeniesEarlier(t *testing.T) {
	// Decoder is already on rev2 (completed). Engine is rolling
	// (rev1→rev2). Engine is the active Component now. If the
	// dispatcher asks about decoder again (e.g., scale-up adds another
	// decoder Instance), the gate must... wait. Decoder isn't rolling
	// per the revision-hash check, so the active selector returns
	// engine. Asked about decoder, the gate denies (engine has the
	// turn). This mirrors the state machine: once Sequential walks
	// past decoder, decoder cannot disrupt engine's rollout window.
	isvc := mkSequentialFixture("seq-isvc", 0)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev2", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
	client := fakeClientForSeqISVC(isvc)
	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.DecoderComponent)
	if allowed {
		t.Errorf("decoder must defer to active engine: %s", reason)
	}
	if !strings.Contains(reason, "engine") {
		t.Errorf("denial reason should name engine: %s", reason)
	}
}

func TestCheckSequentialGate_SoakHoldsActiveComponent(t *testing.T) {
	// Decoder completed at T=now-1s (recorded in its Ready
	// condition's LastTransitionTime); engine is now active (rolling).
	// Soak is 15s → gate must deny engine for the remaining ~14s.
	const soak = 15 * time.Second
	isvc := mkSequentialFixture("seq-isvc", soak)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev2", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
	setComponentReadyAt(isvc, v1beta1.DecoderComponent, time.Now().Add(-1*time.Second))
	client := fakeClientForSeqISVC(isvc)
	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.EngineComponent)
	if allowed {
		t.Errorf("soak window must hold engine: %s", reason)
	}
	if !strings.Contains(reason, "Sequential.Soak") {
		t.Errorf("denial reason should mention Sequential.Soak: %s", reason)
	}
}

// TestCheckSequentialGate_SoakReasonStableAcrossElapsedTime: the soak
// denial message carries only stable facts (component + configured
// duration), not a live elapsed/remaining countdown. Two consults against
// the same shape, one with the decoder finished 1s ago and one with it
// finished 6s ago, must return byte-identical reason strings. The gate
// reads time.Since(decoderReady), so moving the finish 5s back is the
// clock moving 5s forward between the consults. A caller persisting this
// string into status.rolloutHold.reason relies on that stability to avoid
// rewriting status (and resetting rolloutHold.since) on every reconcile for
// the entire soak window.
func TestCheckSequentialGate_SoakReasonStableAcrossElapsedTime(t *testing.T) {
	const soak = 15 * time.Second
	consult := func(finishedAgo time.Duration) (bool, string) {
		isvc := mkSequentialFixture("seq-isvc", soak)
		setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev2", "rev2")
		setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
		setComponentReadyAt(isvc, v1beta1.DecoderComponent, time.Now().Add(-finishedAgo))
		return CheckSequentialGate(context.Background(), fakeClientForSeqISVC(isvc), isvc, v1beta1.EngineComponent)
	}

	allowed1, reason1 := consult(1 * time.Second)
	allowed2, reason2 := consult(6 * time.Second)

	if allowed1 || allowed2 {
		t.Fatalf("both consults must stay denied within the soak window: allowed1=%v allowed2=%v", allowed1, allowed2)
	}
	if reason1 != reason2 {
		t.Errorf("soak denial reason must be stable across elapsed time (no live countdown): first=%q second=%q", reason1, reason2)
	}
}

func TestCheckSequentialGate_SoakReleasesAfterDuration(t *testing.T) {
	// Same shape as the hold case but decoder's Ready transition
	// is well past the soak window — gate allows.
	const soak = 5 * time.Second
	isvc := mkSequentialFixture("seq-isvc", soak)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev2", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
	setComponentReadyAt(isvc, v1beta1.DecoderComponent, time.Now().Add(-soak-1*time.Second))
	client := fakeClientForSeqISVC(isvc)
	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("soak elapsed; gate must allow: %s", reason)
	}
}

func TestCheckSequentialGate_SoakNotArmedForFirstComponent(t *testing.T) {
	// Decoder (first in Order) is the active Component. No prior
	// Component completed → no soak to honor. Gate must allow even
	// with a configured soak.
	const soak = 5 * time.Second
	isvc := mkSequentialFixture("seq-isvc", soak)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev1", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev1")
	client := fakeClientForSeqISVC(isvc)
	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.DecoderComponent)
	if !allowed {
		t.Errorf("first Component must not be soak-held: %s", reason)
	}
}

func TestCheckSequentialGate_SoakWithoutPriorReadyBypasses(t *testing.T) {
	// active==component AND a previous Component is recorded as
	// completed, BUT no Ready condition has been written yet —
	// soak gate has no clock, must default to allow.
	const soak = 5 * time.Second
	isvc := mkSequentialFixture("seq-isvc", soak)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev2", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
	// No Ready condition on decoder.
	client := fakeClientForSeqISVC(isvc)
	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("missing prior Ready must bypass soak: %s", reason)
	}
}

func TestCheckSequentialGate_SoakWithPriorReadyFalseBypasses(t *testing.T) {
	// Decoder's Ready condition is present but False (no rollout
	// has finished yet on the previous Component). The gate must NOT
	// fall back to time.Now() — return zero time so the soak doesn't
	// fire on a bogus clock.
	const soak = 5 * time.Second
	isvc := mkSequentialFixture("seq-isvc", soak)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev2", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
	// Ready=False on decoder.
	cs := isvc.Status.Components[v1beta1.DecoderComponent]
	cs.Lifecycle.Conditions = []metav1.Condition{{
		Type:               conditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             "ReplicaCountMismatch",
		LastTransitionTime: metav1.Now(),
	}}
	isvc.Status.Components[v1beta1.DecoderComponent] = cs
	client := fakeClientForSeqISVC(isvc)
	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("Ready=False must not arm soak clock: %s", reason)
	}
}

func TestCheckSequentialGate_PostDecoderConvergence_AllowsEngine(t *testing.T) {
	// End-to-end post-convergence reproduction of a sequential-rollout
	// stall:
	//
	//   - decoder rolled first and completed (revisions converged, the
	//     OMENative status reconciler promoted CurrentRevision and stamped
	//     Ready=True). decoder is the previously-active Sequential
	//     Component and has been past its Ready transition for well
	//     longer than the configured soak.
	//   - engine's per-reconcile AggregateAndWriteStatus deferred write
	//     advanced UpdateRevision to NEW, AND ObservedGeneration to
	//     isvc.Generation. CurrentRevision is still OLD because no engine
	//     pod has flipped onto the new revision yet (the gate has been
	//     denying it the whole time decoder was rolling).
	//
	// The gate's revision-skew signal pins engine as RolloutInFlight=true
	// (UpdateRev != CurrentRev). decoder shows RolloutInFlight=false
	// (revisions converged + ObservedGeneration caught up). active selector
	// returns engine; soak window has elapsed; the gate MUST allow engine
	// to fire its first per-Instance Update. If it doesn't, the dispatcher
	// returns RequeueAfter on the gate denial and the rollout stalls
	// indefinitely until an unrelated watch event happens to wake the
	// reconciler.
	const soak = 10 * time.Second
	isvc := mkSequentialFixture("seq-isvc", soak)
	isvc.Generation = 2 // operator bumped both Components one generation ago

	// Decoder: fully converged. Revisions match, ObservedGeneration
	// caught up, Ready=True well past the soak window.
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "NEW", "NEW")
	setComponentObservedGeneration(isvc, v1beta1.DecoderComponent, 2)
	setComponentReadyAt(isvc, v1beta1.DecoderComponent, time.Now().Add(-(soak + 5*time.Second)))

	// Engine: CurrentRevision still OLD (no pod yet on the new
	// revision), UpdateRevision already advanced to NEW by the deferred
	// status write, ObservedGeneration caught up.
	setComponentRevisions(isvc, v1beta1.EngineComponent, "OLD", "NEW")
	setComponentObservedGeneration(isvc, v1beta1.EngineComponent, 2)

	client := fakeClientForSeqISVC(isvc)
	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("post-decoder-convergence + soak elapsed: engine MUST be allowed, got denied: %s", reason)
	}
	if !strings.Contains(reason, "Sequential active Component") {
		t.Errorf("post-decoder allowance reason should signal active-Component path: %s", reason)
	}
}

func TestCheckSequentialGate_SoakWindowRespected(t *testing.T) {
	// Companion to PostDecoderConvergence_AllowsEngine: SAME ISVC shape
	// (decoder converged, engine pending), but the previous Component's
	// Ready transition is WITHIN the soak window. The gate must deny
	// engine until soak elapses; the dispatcher's RequeueAfter cycles the
	// reconcile every few seconds and the gate flips to allow as soon as
	// time.Since(decoderReady) >= group.Soak.
	const soak = 30 * time.Second
	isvc := mkSequentialFixture("seq-isvc", soak)
	isvc.Generation = 2

	setComponentRevisions(isvc, v1beta1.DecoderComponent, "NEW", "NEW")
	setComponentObservedGeneration(isvc, v1beta1.DecoderComponent, 2)
	// Decoder just became Ready 2s ago — well inside the 30s soak.
	setComponentReadyAt(isvc, v1beta1.DecoderComponent, time.Now().Add(-2*time.Second))

	setComponentRevisions(isvc, v1beta1.EngineComponent, "OLD", "NEW")
	setComponentObservedGeneration(isvc, v1beta1.EngineComponent, 2)

	client := fakeClientForSeqISVC(isvc)
	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.EngineComponent)
	if allowed {
		t.Errorf("within-soak-window: engine MUST be denied, got allowed: %s", reason)
	}
	if !strings.Contains(reason, "Sequential.Soak") {
		t.Errorf("within-soak denial reason should mention 'Sequential.Soak': %s", reason)
	}
}

func TestCheckSequentialGate_MomentOfBump(t *testing.T) {
	// Reproduces a same-instant bump (gap=0s between decoder/engine
	// creationTimestamps when both spec
	// annotations are bumped in the same operator action):
	//
	// At the moment of bump, isvc.Generation has incremented (N → N+1)
	// but no Component's deferred AggregateAndWriteStatus has run yet,
	// so:
	//   - UpdateRevision still names the OLD ControllerRevision for
	//     BOTH Components (the new target.Name lives only in the
	//     reconciler's local `target` variable, not in status).
	//   - ObservedGeneration is still N on every Component's status
	//     block.
	//
	// Revision-hash skew is silent here (UpdateRevision ==
	// CurrentRevision == OLD) for both Components, so without a lag
	// signal the gate would fall through to "Sequential group idle" and
	// both Components would dispatch concurrently, blowing the
	// Sequential invariant. The projection-lag signal (parent-generation
	// stamp N ≠ isvc.Generation N+1) flips RolloutInFlight=true on every
	// Component the projector hasn't re-applied yet;
	// activeSequentialComponent picks decoder (first in Order) and
	// engine is correctly denied.
	isvc := mkSequentialFixture("seq-isvc", 0)
	isvc.Generation = 2 // operator's spec bump just landed
	// Both Components carry stale stamps: UpdateRevision points at the
	// pre-bump CR, the projected parent generation is one behind
	// isvc.Generation.
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev1", "rev1")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev1")
	setComponentObservedGeneration(isvc, v1beta1.DecoderComponent, 1)
	setComponentObservedGeneration(isvc, v1beta1.EngineComponent, 1)

	// Engine must wait — decoder is first in Order and the
	// projection-lag signal pins decoder as the active Sequential
	// Component.
	client := fakeClientForSeqISVC(isvc)
	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.EngineComponent)
	if allowed {
		t.Errorf("engine must be gated at moment-of-bump: %s", reason)
	}
	if !strings.Contains(reason, "decoder") {
		t.Errorf("denial reason should name the active Component (decoder): %s", reason)
	}
	if !strings.Contains(reason, "Sequential waiting") {
		t.Errorf("denial reason should say 'Sequential waiting': %s", reason)
	}

	// Decoder is the active Sequential Component — gate must allow.
	client = fakeClientForSeqISVC(isvc)
	if allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.DecoderComponent); !allowed {
		t.Errorf("decoder is the active Component at moment-of-bump: %s", reason)
	}
}

// withIRSpecPartition stamps the projected
// spec.lifecycle.updateStrategy.rollingUpdate.partition on a fake IR —
// the effective-partition field the gate reads. The projector fills it
// from the merged ISVC↔runtime lifecycle, so an IR carrying it with no
// matching ISVC-side lifecycle models a runtime-inherited partition.
func withIRSpecPartition(ir *v1beta1.InferenceReplica, partition int32) *v1beta1.InferenceReplica {
	ir.Spec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			RollingUpdate: &v1beta1.RollingUpdate{Partition: &partition},
		},
	}
	return ir
}

// seqReadyIR builds an InferenceReplica whose status models `replicas`
// instances all Ready on runningHash, with the given current/update
// revision hashes and ObservedGeneration. Unlike fakeClientForSeqISVC —
// which copies only the Lifecycle fields off the ISVC — this seeds
// Replicas + InstanceStatuses so ReachedDesiredShape (and thus
// AtDesiredShape) is actually exercised. The absence of these fields is
// exactly why TestCheckSequentialGate_MomentOfBump missed the
// moment-of-bump overlap regression below.
func seqReadyIR(isvcName string, c v1beta1.ComponentType, currentHash, updateHash, runningHash string, replicas int32, observedGen int64) *v1beta1.InferenceReplica {
	rev := func(h string) string { return isvcName + "-" + string(c) + "-" + h }
	insts := make([]v1beta1.OMENativeInstanceStatus, replicas)
	for i := range insts {
		insts[i] = v1beta1.OMENativeInstanceStatus{
			Index:           int32(i),
			Phase:           v1beta1.OMENativeInstanceReady,
			RunningRevision: rev(runningHash),
		}
	}
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:        isvcName + "-" + string(c),
			Generation:  observedGen,
			Annotations: parentGenAnnotation(observedGen),
		},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration: observedGen,
			CurrentRevision:    rev(currentHash),
			UpdateRevision:     rev(updateHash),
			Replicas:           replicas,
			InstanceStatuses:   insts,
		},
	}
}

// TestCheckSequentialGate_MomentOfBump_SteadyStateInstances is the
// regression for a Sequential overlap where the trailing Component
// started rolling concurrently with the leader. At the moment of bump
// every instance is still Ready on the OLD revision — the realistic
// steady state — and the lag window leaves UpdateRevision naming
// the OLD CR.
//
// Bug: observeSequentialComponentsForGate computed AtDesiredShape from
// that stale UpdateRevision. With all instances Ready on OLD,
// ReachedDesiredShape(insts-on-old, target=old, partition=0, replicas)
// returns true → AtDesiredShape=true on BOTH Components at once, while
// RolloutInFlight is also true (lag). activeSequentialComponent
// skips on AtDesiredShape, finds nothing active, and the gate falls
// through to "Sequential group idle" — so engine dispatches concurrently
// with decoder.
//
// TestCheckSequentialGate_MomentOfBump did not catch this because its
// fake IRs carried no Replicas/InstanceStatuses, so ReachedDesiredShape
// returned false and AtDesiredShape was accidentally false.
func TestCheckSequentialGate_MomentOfBump_SteadyStateInstances(t *testing.T) {
	isvc := mkSequentialFixture("seq-isvc", 0)
	isvc.Generation = 2 // operator's spec bump just landed

	cl := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(
		// Steady state, one reconcile behind the bump: current==update==OLD
		// (revisionSkew silent), ObservedGeneration one behind, all
		// instances Ready on OLD.
		seqReadyIR("seq-isvc", v1beta1.DecoderComponent, "rev1", "rev1", "rev1", 4, 1),
		seqReadyIR("seq-isvc", v1beta1.EngineComponent, "rev1", "rev1", "rev1", 4, 1),
	).Build()

	allowed, reason := CheckSequentialGate(context.Background(), cl, isvc, v1beta1.EngineComponent)
	if allowed {
		t.Errorf("engine must be gated at moment-of-bump with steady-state instances (overlap): %s", reason)
	}
	if !strings.Contains(reason, "Sequential waiting") || !strings.Contains(reason, "decoder") {
		t.Errorf("denial should name the active Component (decoder): %s", reason)
	}
	if allowed, reason := CheckSequentialGate(context.Background(), cl, isvc, v1beta1.DecoderComponent); !allowed {
		t.Errorf("decoder must be allowed as the active Component: %s", reason)
	}
}

// TestCheckSequentialGate_StagedPartitionReleasesNext guards the other
// direction: the revisionSkew guard must NOT suppress a
// legitimately-known target. A partitioned leading Component holds
// old pods by design and keeps RolloutInFlight=true forever, but once its
// status records the NEW target (genuine revisionSkew) AND it has
// converged to its staged shape, AtDesiredShape must be computed and the
// gate must release the next Component.
func TestCheckSequentialGate_StagedPartitionReleasesNext(t *testing.T) {
	// The partition lives only on the projected IR spec — the raw ISVC
	// carries no lifecycle at all, modeling a partition inherited from
	// the ServingRuntime. The gate must still recognize the staged shape.
	isvc := mkSequentialFixture("seq-isvc", 0)
	isvc.Generation = 3

	cl := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(
		// Decoder: genuine revisionSkew (target rev2 recorded), converged
		// to the staged shape — 3 instances on rev2, 1 held on rev1.
		// Effective partition=1: 3 of 4 roll to NEW, 1 held on OLD.
		func() *v1beta1.InferenceReplica {
			ir := seqReadyIR("seq-isvc", v1beta1.DecoderComponent, "rev1", "rev2", "rev2", 4, 3)
			ir.Status.InstanceStatuses[3].RunningRevision = "seq-isvc-decoder-rev1" // held
			return withIRSpecPartition(ir, 1)
		}(),
		// Engine: also bumped to rev2, nothing rolled yet.
		seqReadyIR("seq-isvc", v1beta1.EngineComponent, "rev1", "rev2", "rev1", 4, 3),
	).Build()

	allowed, reason := CheckSequentialGate(context.Background(), cl, isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("engine must be released once decoder reached its staged partition shape: %s", reason)
	}
}

func TestCheckSequentialGate_ProjectionLagOnlyOneComponent(t *testing.T) {
	// Defensive: only decoder's projection is stale (engine already
	// re-projected for the live generation). Decoder's projection lag
	// alone is enough to pin it as the active Sequential Component;
	// engine must defer until the projector's pass reaches decoder.
	isvc := mkSequentialFixture("seq-isvc", 0)
	isvc.Generation = 5
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev1", "rev1")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev1")
	setComponentObservedGeneration(isvc, v1beta1.DecoderComponent, 4) // stale
	setComponentObservedGeneration(isvc, v1beta1.EngineComponent, 5)  // caught up

	client := fakeClientForSeqISVC(isvc)
	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.EngineComponent)
	if allowed {
		t.Errorf("engine must wait while decoder's projection lags: %s", reason)
	}
	if !strings.Contains(reason, "decoder") {
		t.Errorf("denial reason should name decoder: %s", reason)
	}
}

func TestCheckSequentialGate_LaterOnlyBumpReleasesLaterComponent(t *testing.T) {
	// Partial bump: only the engine — later in Order — was changed.
	// After the projector's pass, the untouched decoder carries a
	// current parent-generation stamp with converged revisions and a
	// status that matches its own (unmoved) IR generation; the engine
	// carries genuine revision skew. The gate must route active to the
	// engine and release it: an idle earlier-in-Order Component must
	// never hold a later Component's rollout hostage.
	isvc := mkSequentialFixture("seq-isvc", 0)
	isvc.Generation = 2 // engine-only bump landed and was projected

	cl := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(
		// Decoder: nothing to roll. Projection stamp current (2), all
		// instances Ready on its only revision.
		seqReadyIR("seq-isvc", v1beta1.DecoderComponent, "rev1", "rev1", "rev1", 2, 2),
		// Engine: bumped — status already records the new target.
		seqReadyIR("seq-isvc", v1beta1.EngineComponent, "rev1", "rev2", "rev1", 2, 2),
	).Build()

	allowed, reason := CheckSequentialGate(context.Background(), cl, isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("engine-only bump: engine must be released without a decoder handoff, got denied: %s", reason)
	}
	if !strings.Contains(reason, "Sequential active Component") {
		t.Errorf("allowance reason should signal the active-Component path: %s", reason)
	}
}

func TestCheckSequentialGate_StampAheadOfISVCReadHolds(t *testing.T) {
	// Strict-equality guard on the projection signal: a
	// parent-generation stamp AHEAD of the ISVC in hand means the gate
	// is deciding against a stale ISVC read — the projector has
	// already applied a newer spec. The pair is not one consistent
	// snapshot, so the Component counts as in-flight and its
	// Sequential peers stay held until a clean read lands.
	isvc := mkSequentialFixture("seq-isvc", 0)
	isvc.Generation = 2

	decoder := seqReadyIR("seq-isvc", v1beta1.DecoderComponent, "rev1", "rev1", "rev1", 2, 2)
	decoder.Annotations = parentGenAnnotation(3) // projected for a newer ISVC generation
	engine := seqReadyIR("seq-isvc", v1beta1.EngineComponent, "rev1", "rev1", "rev1", 2, 2)
	cl := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(decoder, engine).Build()

	allowed, reason := CheckSequentialGate(context.Background(), cl, isvc, v1beta1.EngineComponent)
	if allowed {
		t.Errorf("engine must be held while decoder's stamp is ahead of the ISVC read: %s", reason)
	}
	if !strings.Contains(reason, "decoder") {
		t.Errorf("denial reason should name decoder: %s", reason)
	}
}

func TestCheckSequentialGate_StatusAheadOfIRGenerationHolds(t *testing.T) {
	// Strict-equality guard on the same-object signal: a status block
	// recording a generation the IR object in hand does not carry
	// means the read is torn (a stale IR paired with a newer status
	// write). The Component counts as in-flight until a clean
	// observation lands, holding its Sequential peers.
	isvc := mkSequentialFixture("seq-isvc", 0)
	isvc.Generation = 2

	decoder := seqReadyIR("seq-isvc", v1beta1.DecoderComponent, "rev1", "rev1", "rev1", 2, 2)
	decoder.Generation = 1 // status (2) was written for a newer object than this read
	engine := seqReadyIR("seq-isvc", v1beta1.EngineComponent, "rev1", "rev1", "rev1", 2, 2)
	cl := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(decoder, engine).Build()

	allowed, reason := CheckSequentialGate(context.Background(), cl, isvc, v1beta1.EngineComponent)
	if allowed {
		t.Errorf("engine must be held while decoder's status is ahead of its IR generation: %s", reason)
	}
	if !strings.Contains(reason, "decoder") {
		t.Errorf("denial reason should name decoder: %s", reason)
	}
}

// A replica whose parent-generation annotation is missing or unreadable
// counts as rolling: the projector has not written this pass's projection
// to it, so its settled-looking status says nothing about the service in
// hand, and the next member in the Order waits on it. After a controller
// upgrade every replica reads this way until the projector's first pass.
func TestCheckSequentialGate_AReplicaWithoutAParentGenerationAnnotationReadsInFlight(t *testing.T) {
	cases := map[string]map[string]string{
		"missing":    nil,
		"unreadable": {constants.InferenceReplicaParentGenerationAnnotationKey: "x"},
	}
	for name, annotations := range cases {
		t.Run(name, func(t *testing.T) {
			isvc := mkSequentialFixture("seq-isvc", 0)
			isvc.Generation = 3
			decoder := seqReadyIR(isvc.Name, v1beta1.DecoderComponent, "rev1", "rev1", "rev1", 2, 3)
			decoder.Annotations = annotations
			engine := seqReadyIR(isvc.Name, v1beta1.EngineComponent, "rev1", "rev1", "rev1", 2, 3)
			reads := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(decoder, engine).Build()
			allowed, reason := CheckSequentialGate(context.Background(), reads, isvc, v1beta1.EngineComponent)
			if allowed {
				t.Fatalf("the engine must wait on a decoder whose projection the gate cannot place: %s", reason)
			}
			if !strings.Contains(reason, "waiting on decoder") {
				t.Fatalf("reason = %q, want the gate waiting on the decoder", reason)
			}
		})
	}
}

func TestCheckSequentialGate_GenerationCaughtUpAndIdleBypasses(t *testing.T) {
	// Once every Component's ObservedGeneration matches isvc.Generation
	// AND revisions are converged, the gate must bypass (no rollout in
	// flight anywhere). This is the steady-state read; without it the
	// fix would wedge the gate on a converged ISVC.
	isvc := mkSequentialFixture("seq-isvc", 0)
	isvc.Generation = 7
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev2", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev2", "rev2")
	setComponentObservedGeneration(isvc, v1beta1.DecoderComponent, 7)
	setComponentObservedGeneration(isvc, v1beta1.EngineComponent, 7)

	client := fakeClientForSeqISVC(isvc)
	if allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.EngineComponent); !allowed {
		t.Errorf("converged group must bypass gate: %s", reason)
	}
	client = fakeClientForSeqISVC(isvc)
	if allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.DecoderComponent); !allowed {
		t.Errorf("converged group must bypass gate: %s", reason)
	}
}

func TestCheckSequentialGate_NoOrderBypasses(t *testing.T) {
	// Defensive: a 2-Component group with no Order set resolves to a
	// (non-Sequential) blueGreen group — the spec cannot express
	// "Sequential without Order" (Sequential is a run of single-Component
	// groups the controller collapses with a derived Order). The gate must
	// still bypass cleanly without panic / wedge.
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
					BlueGreen:  &v1beta1.GroupBlueGreen{},
					// Order intentionally empty.
				}},
			},
		},
	}
	client := fakeClientForSeqISVC(isvc)
	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("order-less group must not wedge dispatcher: %s", reason)
	}
}

// --- Sequential handoff on desired-shape convergence ---

// mkSequentialFixtureEngineFirst returns an ISVC with a Sequential run
// [engine, decoder] (engine first in Order). The engine's partition is
// stamped on its fake IR spec (withIRSpecPartition) — the effective
// value the gate reads — so the ISVC itself carries no lifecycle.
func mkSequentialFixtureEngineFirst(name string) *v1beta1.InferenceService {
	isvc := mkSequentialFixtureInOrder(name, v1beta1.EngineComponent, v1beta1.DecoderComponent)
	isvc.Spec.Engine, isvc.Spec.Decoder = &v1beta1.EngineSpec{}, &v1beta1.DecoderSpec{}
	return isvc
}

// seqIR builds an InferenceReplica carrying the instance-level status
// the desired-shape convergence check reads (Replicas + InstanceStatuses)
// on top of the revision names. Namespace is empty to match the
// namespace-less fixtures: DecodedComponentIRFor keys the read by the
// service's namespace, "" here.
// The recorded parent generation is the fixture service's, as the
// projector writes it, so the replica reads as projected for the service.
func seqIR(isvcName string, c v1beta1.ComponentType, currentHash, updateHash string, replicas int32, insts []v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:        isvcName + "-" + string(c),
			Annotations: parentGenAnnotation(0),
		},
		Status: v1beta1.InferenceReplicaStatus{
			CurrentRevision:  isvcName + "-" + string(c) + "-" + currentHash,
			UpdateRevision:   isvcName + "-" + string(c) + "-" + updateHash,
			Replicas:         replicas,
			InstanceStatuses: insts,
		},
	}
}

// revName is the <isvcName>-<component>-<hash> ControllerRevision-name
// shape query.RevisionFromName splits to compare hashes.
func revName(isvcName string, c v1beta1.ComponentType, hash string) string {
	return isvcName + "-" + string(c) + "-" + hash
}

func TestCheckSequentialGate_StagedLeaderHandsOffToNext(t *testing.T) {
	// Engine (first in Order) reached its desired staged shape: partition=1
	// holds one instance on OLD, one instance rolled to NEW, both Ready. Its
	// revisions still skew (CurrentRevision=OLD != UpdateRevision=NEW) so
	// RolloutInFlight stays true — but AtDesiredShape releases it for
	// handoff. Decoder is genuinely rolling. The gate must ALLOW decoder
	// (without the AtDesiredShape release the gate would return
	// "Sequential waiting on engine" forever).
	const name = "seq-isvc"
	isvc := mkSequentialFixtureEngineFirst(name)
	client := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(
		withIRSpecPartition(seqIR(name, v1beta1.EngineComponent, "OLD", "NEW", 2, []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: revName(name, v1beta1.EngineComponent, "NEW")},
			{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: revName(name, v1beta1.EngineComponent, "OLD")},
		}), 1),
		seqIR(name, v1beta1.DecoderComponent, "OLD", "NEW", 2, []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: revName(name, v1beta1.DecoderComponent, "OLD")},
			{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: revName(name, v1beta1.DecoderComponent, "OLD")},
		}),
	).Build()

	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.DecoderComponent)
	if !allowed {
		t.Errorf("staged engine must hand off; decoder MUST be allowed, got denied: %s", reason)
	}
	if !strings.Contains(reason, "Sequential active Component") {
		t.Errorf("allowance reason should signal active-Component path: %s", reason)
	}
}

func TestCheckSequentialGate_UnconvergedLeaderStillBlocks(t *testing.T) {
	// Regression: engine is in flight and NOT at its desired shape (an
	// instance is still Updating on OLD, none on NEW). It must STILL be the
	// active/blocking Component; decoder's gate stays denied.
	const name = "seq-isvc"
	isvc := mkSequentialFixtureEngineFirst(name)
	client := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(
		withIRSpecPartition(seqIR(name, v1beta1.EngineComponent, "OLD", "NEW", 2, []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: revName(name, v1beta1.EngineComponent, "OLD")},
			{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: revName(name, v1beta1.EngineComponent, "OLD")},
		}), 1),
		seqIR(name, v1beta1.DecoderComponent, "OLD", "NEW", 2, []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: revName(name, v1beta1.DecoderComponent, "OLD")},
			{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: revName(name, v1beta1.DecoderComponent, "OLD")},
		}),
	).Build()

	allowed, reason := CheckSequentialGate(context.Background(), client, isvc, v1beta1.DecoderComponent)
	if allowed {
		t.Errorf("unconverged engine must still block decoder, got allowed: %s", reason)
	}
	if !strings.Contains(reason, "engine") || !strings.Contains(reason, "Sequential waiting") {
		t.Errorf("denial reason should name the active Component (engine) and say 'Sequential waiting': %s", reason)
	}
}

// emitSequentialDeferredEvent: when an operator bumps a later-in-Order
// Component's spec while an earlier Component is still rolling,
// Sequential semantics defer the later rollout and the controller
// must emit SequentialNextSpecBumpDeferred so the operator understands
// why the spec change isn't visible yet.

func TestEmitSequentialDeferred_NotSequentialPolicy(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	isvc := &v1beta1.InferenceService{}
	g := ResolvedGroup{
		Policy: v1beta1.CoordinationPolicyBlueGreen,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
	}
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {RolloutInFlight: true, TargetRevisionHash: "rev2"},
			v1beta1.EngineComponent:  {RolloutInFlight: true, TargetRevisionHash: "rev2"},
		},
	}
	emitSequentialDeferredEvent(rec, isvc, g, obs, "")
	if got := drainEvents(rec); len(got) != 0 {
		t.Errorf("BlueGreen policy must not emit Sequential events; got %v", got)
	}
}

func TestEmitSequentialDeferred_NoComponentRolling(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	isvc := &v1beta1.InferenceService{}
	g := ResolvedGroup{
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
	}
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {RolloutInFlight: false},
			v1beta1.EngineComponent:  {RolloutInFlight: false},
		},
	}
	emitSequentialDeferredEvent(rec, isvc, g, obs, "")
	if got := drainEvents(rec); len(got) != 0 {
		t.Errorf("no active component → no deferred event; got %v", got)
	}
}

func TestEmitSequentialDeferred_ActiveButNoLaterBump(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	isvc := &v1beta1.InferenceService{}
	g := ResolvedGroup{
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
	}
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {RolloutInFlight: true, TargetRevisionHash: "rev2"},
			v1beta1.EngineComponent:  {RolloutInFlight: false, CurrentRevisionHash: "rev1"},
		},
	}
	emitSequentialDeferredEvent(rec, isvc, g, obs, "")
	if got := drainEvents(rec); len(got) != 0 {
		t.Errorf("active decoder + idle engine → no deferred event; got %v", got)
	}
}

// TestEmitSequentialDeferred_AStagedEarlierComponentDefersNothing records the
// warning's reading of a Component resting at its staged shape under a
// partition: its roll stays in flight by design, but it is done for the
// next Component, which rolls rather than waits, so no deferral is
// reported. The same Component short of its staged shape still defers the
// later one.
func TestEmitSequentialDeferred_AStagedEarlierComponentDefersNothing(t *testing.T) {
	g := ResolvedGroup{
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
	}
	staged := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.EngineComponent:  {RolloutInFlight: true, AtDesiredShape: true, Partition: 3, TargetRevisionHash: "rev2"},
			v1beta1.DecoderComponent: {RolloutInFlight: true, TargetRevisionHash: "rev2"},
		},
	}
	rec := record.NewFakeRecorder(10)
	emitSequentialDeferredEvent(rec, &v1beta1.InferenceService{}, g, staged, "")
	if got := drainEvents(rec); len(got) != 0 {
		t.Errorf("a staged earlier Component defers nothing; got %v", got)
	}

	rolling := staged
	rolling.Components = map[v1beta1.ComponentType]ComponentObservation{
		v1beta1.EngineComponent:  {RolloutInFlight: true, AtDesiredShape: false, Partition: 3, TargetRevisionHash: "rev2"},
		v1beta1.DecoderComponent: {RolloutInFlight: true, TargetRevisionHash: "rev2"},
	}
	rec = record.NewFakeRecorder(10)
	emitSequentialDeferredEvent(rec, &v1beta1.InferenceService{}, g, rolling, "")
	got := drainEvents(rec)
	if len(got) != 1 || !strings.Contains(got[0], "SequentialNextSpecBumpDeferred") || !strings.Contains(got[0], "decoder") {
		t.Errorf("an earlier Component short of its staged shape defers the later one; got %v", got)
	}
}

func TestEmitSequentialDeferred_FiresWhenLaterComponentAlsoBumped(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	isvc := &v1beta1.InferenceService{}
	g := ResolvedGroup{
		Name:   "0",
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
	}
	// Decoder is active (RolloutInFlight). Engine was ALSO bumped
	// while decoder is rolling → Sequential defers engine. Expect one
	// deferred event for engine.
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {RolloutInFlight: true, TargetRevisionHash: "rev2"},
			v1beta1.EngineComponent:  {RolloutInFlight: true, TargetRevisionHash: "rev2"},
		},
	}
	emitSequentialDeferredEvent(rec, isvc, g, obs, "")
	got := drainEvents(rec)
	if len(got) != 1 {
		t.Fatalf("want exactly 1 deferred event; got %d: %v", len(got), got)
	}
	if !contains(got[0], EventReasonSequentialNextSpecBumpDeferred) {
		t.Errorf("event reason mismatch: %s", got[0])
	}
	if !contains(got[0], "engine") {
		t.Errorf("event should name later Component (engine): %s", got[0])
	}
	if !contains(got[0], "decoder") {
		t.Errorf("event should name active Component (decoder): %s", got[0])
	}
}

func TestEmitSequentialDeferred_MultipleLaterComponentsFireMultipleEvents(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	isvc := &v1beta1.InferenceService{}
	g := ResolvedGroup{
		Name:   "0",
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent, v1beta1.RouterComponent},
	}
	// Decoder active, both engine + router also bumped → 2 deferred events.
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {RolloutInFlight: true, TargetRevisionHash: "rev2"},
			v1beta1.EngineComponent:  {RolloutInFlight: true, TargetRevisionHash: "rev2"},
			v1beta1.RouterComponent:  {RolloutInFlight: true, TargetRevisionHash: "rev2"},
		},
	}
	emitSequentialDeferredEvent(rec, isvc, g, obs, "")
	got := drainEvents(rec)
	if len(got) != 2 {
		t.Fatalf("want 2 deferred events (engine + router); got %d: %v", len(got), got)
	}
}

func TestEmitSequentialDeferred_NilRecorder(t *testing.T) {
	// Must not panic on nil recorder.
	g := ResolvedGroup{
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
	}
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {RolloutInFlight: true},
			v1beta1.EngineComponent:  {RolloutInFlight: true},
		},
	}
	emitSequentialDeferredEvent(nil, &v1beta1.InferenceService{}, g, obs, "")
}

func TestEmitSequentialDeferred_EmptyOrder(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	isvc := &v1beta1.InferenceService{}
	g := ResolvedGroup{
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  nil,
	}
	emitSequentialDeferredEvent(rec, isvc, g, GroupObservation{Group: g}, "")
	if got := drainEvents(rec); len(got) != 0 {
		t.Errorf("empty Order → no events; got %v", got)
	}
}

// =============================================================================
// Test helpers
// =============================================================================

func drainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	n := len(sub)
	for i := 0; i+n <= len(s); i++ {
		if s[i:i+n] == sub {
			return i
		}
	}
	return -1
}

func TestEmitSequentialDeferred_DedupWhenActiveUnchanged(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	isvc := &v1beta1.InferenceService{}
	g := ResolvedGroup{
		Name:   "0",
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
	}
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {RolloutInFlight: true, TargetRevisionHash: "rev2"},
			v1beta1.EngineComponent:  {RolloutInFlight: true, TargetRevisionHash: "rev2"},
		},
	}
	// prevActive=decoder matches current activeComponent → dedup suppresses.
	emitSequentialDeferredEvent(rec, isvc, g, obs, v1beta1.DecoderComponent)
	if got := drainEvents(rec); len(got) != 0 {
		t.Errorf("dedup: same active Component must suppress re-emit; got %v", got)
	}
}

func TestEmitSequentialDeferred_FiresWhenActiveAdvances(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	isvc := &v1beta1.InferenceService{}
	g := ResolvedGroup{
		Name:   "0",
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent, v1beta1.RouterComponent},
	}
	// Decoder done; engine now active; router deferred.
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {RolloutInFlight: false, CurrentRevisionHash: "rev2"},
			v1beta1.EngineComponent:  {RolloutInFlight: true, TargetRevisionHash: "rev2"},
			v1beta1.RouterComponent:  {RolloutInFlight: true, TargetRevisionHash: "rev2"},
		},
	}
	// prevActive=decoder; now activeComponent=engine → dedup clears, emit.
	emitSequentialDeferredEvent(rec, isvc, g, obs, v1beta1.DecoderComponent)
	got := drainEvents(rec)
	if len(got) != 1 {
		t.Fatalf("active advanced (decoder→engine) → expect re-emit for router; got %d: %v", len(got), got)
	}
	if !contains(got[0], "router") {
		t.Errorf("event should name router as deferred: %s", got[0])
	}
}

// Force corev1 import (used indirectly via record).
var _ = corev1.EventTypeWarning

// ResolveGateContext is the once-per-tick prelude helper backing all
// Check* gates. These tests pin the four short-circuit paths and the
// resolved-group success path; the per-gate behavior is already covered
// by the existing ratio_test.go / sequential_gate_test.go suites that
// continue to exercise the public CheckRatioGate / CheckSurgeGate /
// CheckUnavailabilityGate / CheckSequentialGate wrappers.

func TestResolveGateContext_NilISVC(t *testing.T) {
	ctx := ResolveGateContext(context.Background(), nil, nil, v1beta1.EngineComponent)
	if !ctx.ShortCircuit {
		t.Fatalf("nil ISVC: want ShortCircuit=true, got %+v", ctx)
	}
	if ctx.ShortReason != "nil ISVC" {
		t.Errorf("nil ISVC: want ShortReason=nil ISVC, got %q", ctx.ShortReason)
	}
}

func TestResolveGateContext_NoCoordBlock(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	ctx := ResolveGateContext(context.Background(), nil, isvc, v1beta1.EngineComponent)
	if !ctx.ShortCircuit {
		t.Fatalf("no coord block: want ShortCircuit=true, got %+v", ctx)
	}
	if ctx.ShortReason != "no coordination groups" {
		t.Errorf("no coord block: want ShortReason=no coordination groups, got %q", ctx.ShortReason)
	}
}

func TestResolveGateContext_ComponentNotInAnyGroup(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{
					{
						Components: []v1beta1.ComponentType{v1beta1.DecoderComponent},
						BlueGreen:  &v1beta1.GroupBlueGreen{},
					},
				},
			},
		},
	}
	ctx := ResolveGateContext(context.Background(), nil, isvc, v1beta1.EngineComponent)
	if !ctx.ShortCircuit {
		t.Fatalf("not in any group: want ShortCircuit=true, got %+v", ctx)
	}
	if ctx.ShortReason != "component not in any coord group" {
		t.Errorf("not in any group: want ShortReason=component not in any coord group, got %q", ctx.ShortReason)
	}
}

func TestResolveGateContext_ResolvesGroup(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{
					{
						Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
						BlueGreen:  &v1beta1.GroupBlueGreen{},
					},
				},
			},
		},
	}
	pinActiveRun(isvc)
	ctx := ResolveGateContext(context.Background(), nil, isvc, v1beta1.EngineComponent)
	if ctx.ShortCircuit {
		t.Fatalf("resolved group: want ShortCircuit=false, got %+v", ctx)
	}
	if ctx.Group.Policy != v1beta1.CoordinationPolicyBlueGreen {
		t.Errorf("resolved group: want Policy=BlueGreen, got %q", ctx.Group.Policy)
	}
	if len(ctx.Group.Components) != 2 {
		t.Errorf("resolved group: want 2 Components, got %d", len(ctx.Group.Components))
	}
	if ctx.ISVC != isvc {
		t.Errorf("resolved group: ISVC pointer not threaded through")
	}
	if ctx.Component != v1beta1.EngineComponent {
		t.Errorf("resolved group: Component not threaded through, got %q", ctx.Component)
	}
}

// TestGateContextCheckRatio_ShortCircuitsAllowed pins the
// short-circuit path on the GateContext-aware variant — the dispatcher
// reads the (allowed=true, reason) tuple even when the ISVC has no
// coord block. Behavior parity with CheckRatioGate's "nil ISVC" /
// "no coordination block" / "component not in any coord group"
// branches is what makes the wrapper safe to keep as a 1-liner.
func TestGateContextCheckRatio_ShortCircuitsAllowed(t *testing.T) {
	ctx := GateContext{
		ShortCircuit: true,
		ShortReason:  "no coordination block",
	}
	if allowed, reason := ctx.CheckRatio(0, 0, -1); !allowed || reason != "no coordination block" {
		t.Errorf("short-circuit: got (allowed=%v, reason=%q), want (true, no coordination block)", allowed, reason)
	}
	if allowed, reason := ctx.CheckSurge(0); !allowed || reason != "no coordination block" {
		t.Errorf("short-circuit surge: got (allowed=%v, reason=%q)", allowed, reason)
	}
	if allowed, reason := ctx.CheckUnavailability(0); !allowed || reason != "no coordination block" {
		t.Errorf("short-circuit unavail: got (allowed=%v, reason=%q)", allowed, reason)
	}
	if allowed, reason := ctx.CheckSequential(); !allowed || reason != "no coordination block" {
		t.Errorf("short-circuit sequential: got (allowed=%v, reason=%q)", allowed, reason)
	}
}

// TestCheckSequentialGate_ReadErrorFailsClosed pins the fail-closed
// contract: a transient IR read error on a group member must DENY, not
// degrade to a zero-valued observation — a fabricated
// RolloutInFlight=false on the rolling peer would admit a second
// Component and break the at-most-one-rolling invariant.
func TestCheckSequentialGate_ReadErrorFailsClosed(t *testing.T) {
	isvc := mkSequentialFixture("seq-isvc", 0)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev1", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
	reader := &failingIRClient{Client: fakeClientForSeqISVC(isvc)}
	allowed, reason := CheckSequentialGate(context.Background(), reader, isvc, v1beta1.EngineComponent)
	if allowed {
		t.Fatalf("IR read error must fail closed: %s", reason)
	}
	if !strings.Contains(reason, "failing closed") {
		t.Errorf("denial reason should mark the degraded read: %s", reason)
	}
}

// TestCheckSequentialGate_SoakIgnoresLegacyAvailableCondition pins the
// soak anchor to the IR Ready condition. The old "Available" condition
// type has had no producer since its stamper was deleted; a stray one
// left in status must not arm (or satisfy) the soak clock — absent a
// Ready=True transition there is no completion timestamp, so the gate
// allows.
func TestCheckSequentialGate_SoakIgnoresLegacyAvailableCondition(t *testing.T) {
	const soak = time.Hour
	isvc := mkSequentialFixture("seq-isvc", soak)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev2", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
	cs := isvc.Status.Components[v1beta1.DecoderComponent]
	cs.Lifecycle.Conditions = []metav1.Condition{{
		Type:               "Available",
		Status:             metav1.ConditionTrue,
		Reason:             "Ready",
		LastTransitionTime: metav1.Now(),
	}}
	isvc.Status.Components[v1beta1.DecoderComponent] = cs
	cl := fakeClientForSeqISVC(isvc)
	allowed, reason := CheckSequentialGate(context.Background(), cl, isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("legacy Available condition must not arm the soak clock: %s", reason)
	}
}

// stagedIR is a Component's replica resting at its staged shape: replicas
// Instances, the lowest held of them Ready on currentHash and the rest
// Ready on updateHash, the partition on the replica's spec, the status
// written for the replica's generation and the projection for the
// service's, the counters saying every Instance serves, and the Ready
// condition True with reason Staged since readyAt.
func stagedIR(isvcName string, c v1beta1.ComponentType, currentHash, updateHash string, replicas, held int32, gen int64, readyAt time.Time) *v1beta1.InferenceReplica {
	ir := seqReadyIR(isvcName, c, currentHash, updateHash, updateHash, replicas, gen)
	for i := int32(0); i < held; i++ {
		ir.Status.InstanceStatuses[i].RunningRevision = revName(isvcName, c, currentHash)
	}
	ir.Status.ReadyReplicas, ir.Status.ServingReplicas = replicas, replicas
	ir.Status.Conditions = []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionTrue, Reason: "Staged", LastTransitionTime: metav1.NewTime(readyAt)}}
	return withIRSpecPartition(ir, held)
}

// TestCheckSequentialGate_TheWaitAfterReadinessIsMeasuredAfterAStagedComponent
// 12 engines with 2 kept on version 1 by a partition, then 8 decoders, with
// a ten-minute wait between the two. The ten movable engines serve version 2
// and the engine rests at its staged shape, Ready. The decoder's first start
// waits out the ten minutes from the moment the engine turned Ready, as it
// does after an engine with no partition, and is admitted once they have
// passed.
func TestCheckSequentialGate_TheWaitAfterReadinessIsMeasuredAfterAStagedComponent(t *testing.T) {
	const name = "seq-isvc"
	isvc := mkSequentialFixtureEngineFirst(name)
	isvc.Spec.Rollout.Groups[0].Soak = &metav1.Duration{Duration: 10 * time.Minute}
	isvc = pinActiveRun(isvc)
	isvc.Generation = 3
	for _, tc := range []struct {
		name       string
		readyFor   time.Duration
		wantAllow  bool
		wantReason string
	}{
		{name: "one minute after the engine staged the decoder waits", readyFor: time.Minute, wantReason: "Sequential.Soak: decoder waiting out the 10m0s soak after engine"},
		{name: "eleven minutes after the engine staged the decoder starts", readyFor: 11 * time.Minute, wantAllow: true, wantReason: "Sequential active Component"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decoder := seqReadyIR(name, v1beta1.DecoderComponent, "v1", "v2", "v1", 8, 3)
			decoder.Status.ReadyReplicas, decoder.Status.ServingReplicas = 8, 8
			cl := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(
				stagedIR(name, v1beta1.EngineComponent, "v1", "v2", 12, 2, 3, time.Now().Add(-tc.readyFor)),
				decoder,
			).Build()
			allowed, reason := CheckSequentialGate(context.Background(), cl, isvc, v1beta1.DecoderComponent)
			if allowed != tc.wantAllow || reason != tc.wantReason {
				t.Fatalf("decoder: allowed=%v reason=%q, want allowed=%v reason=%q", allowed, reason, tc.wantAllow, tc.wantReason)
			}
			allowed, gate, reason := EvaluateUpdateGate(context.Background(), cl, isvc, v1beta1.DecoderComponent, revName(name, v1beta1.DecoderComponent, "v1"),
				pairingCR("", name, v1beta1.DecoderComponent, "v2", ""), nil, GroupDefaults{}, workloadtypes.UpdateStrategyRecreatePod, 0, 0)
			// The whole gate stack answers the same; an admitted start carries no reason.
			wantGate, wantReason := v1beta1.RolloutHoldGate(""), ""
			if !tc.wantAllow {
				wantGate, wantReason = v1beta1.RolloutHoldGateSequential, tc.wantReason
			}
			if allowed != tc.wantAllow || gate != wantGate || reason != wantReason {
				t.Fatalf("the gate stack answered allowed=%v gate=%q reason=%q, want allowed=%v gate=%q reason=%q", allowed, gate, reason, tc.wantAllow, wantGate, wantReason)
			}
		})
	}
}

// seqReplicaWithRows is a fake InferenceReplica of one Sequential member
// as the replica controller publishes it: its count and user partition on
// the spec, the revision pair, and one Ready row per Instance on the
// revision given, in index order. The parent-generation annotation and
// the observed generation match the ISVC's, so only the revision pair and the
// rows decide what the gate reads.
func seqReplicaWithRows(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, partition int32, current, update string, running ...string) *v1beta1.InferenceReplica {
	rows := make([]v1beta1.OMENativeInstanceStatus, 0, len(running))
	for i, rev := range running {
		rows = append(rows, v1beta1.OMENativeInstanceStatus{Index: int32(i), Phase: v1beta1.OMENativeInstanceReady, RunningRevision: rev})
	}
	replicas := int32(len(running))
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:        isvc.Name + "-" + string(c),
			Namespace:   isvc.Namespace,
			Generation:  isvc.Generation,
			Annotations: parentGenAnnotation(isvc.Generation),
		},
		Spec: v1beta1.InferenceReplicaSpec{Replicas: &replicas},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration: isvc.Generation,
			Replicas:           replicas,
			ReadyReplicas:      replicas,
			CurrentRevision:    current,
			UpdateRevision:     update,
			InstanceStatuses:   rows,
		},
	}
	if partition > 0 {
		ir.Spec.Lifecycle = &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{
			RollingUpdate: &v1beta1.RollingUpdate{Partition: &partition}}}
	}
	return ir
}

// TestCheckSequentialGate_APartitionAboveTheOldCountReleasesTheNextComponent
// records the turn passing under a partition the first Component cannot fill.
// The order is decoder then engine. The decoder has 12 Instances, 8 on the new revision
// and 4 on the old, all Ready and nothing in flight, under a partition of
// 8 raised after 8 moved. A partition holds the Instances still on the old
// revision, so it holds those 4 and the decoder rests at that shape: the
// engine's start is admitted. Under a partition of 2 the decoder still has
// two Instances to move and the engine waits on it. A decoder scaled to 4
// under a partition of 6, every Instance on the old revision, rests the
// same way, and one scaled to 4 under a partition of 2 still rolls.
func TestCheckSequentialGate_APartitionAboveTheOldCountReleasesTheNextComponent(t *testing.T) {
	dec, eng := v1beta1.DecoderComponent, v1beta1.EngineComponent
	revision := func(c v1beta1.ComponentType, hash string) string { return "seq-isvc-" + string(c) + "-" + hash }
	fleet := func(c v1beta1.ComponentType, onNew, onOld int) []string {
		var out []string
		for i := 0; i < onNew; i++ {
			out = append(out, revision(c, "rev2"))
		}
		for i := 0; i < onOld; i++ {
			out = append(out, revision(c, "rev1"))
		}
		return out
	}
	for _, tc := range []struct {
		name      string
		partition int32
		onNew     int
		onOld     int
		admitted  bool
	}{
		{name: "partition 8 raised after 8 of 12 moved releases the engine", partition: 8, onNew: 8, onOld: 4, admitted: true},
		{name: "partition 4 met exactly releases the engine", partition: 4, onNew: 8, onOld: 4, admitted: true},
		{name: "partition 2 with 4 still old keeps the turn", partition: 2, onNew: 8, onOld: 4, admitted: false},
		{name: "a count scaled to 4 under a partition of 6 releases the engine", partition: 6, onNew: 0, onOld: 4, admitted: true},
		{name: "a count scaled to 4 under a partition of 2 keeps the turn", partition: 2, onNew: 0, onOld: 4, admitted: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isvc := mkSequentialFixture("seq-isvc", 0)
			decoder := seqReplicaWithRows(isvc, dec, tc.partition, revision(dec, "rev1"), revision(dec, "rev2"), fleet(dec, tc.onNew, tc.onOld)...)
			engine := seqReplicaWithRows(isvc, eng, 0, revision(eng, "rev1"), revision(eng, "rev2"), fleet(eng, 0, 2)...)
			reads := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(decoder, engine).Build()
			allowed, reason := CheckSequentialGate(context.Background(), reads, isvc, eng)
			if allowed != tc.admitted {
				t.Fatalf("engine start admitted=%v (%s), want %v", allowed, reason, tc.admitted)
			}
			if !tc.admitted && reason != "Sequential waiting on decoder" {
				t.Fatalf("engine hold reads %q, want \"Sequential waiting on decoder\"", reason)
			}
		})
	}
}

// The replica-side gate measures the wait after a finished member from the
// instant the group recorded that member done, the same clock the group's
// phase reads, so the member losing readiness during the wait neither admits
// the next member nor delays it past the window. Decoder then engine with a
// five-minute wait: every decoder pod is out of rotation at t+2m45s and the
// engine stays held; the pods are Ready again at t+3m and the engine is
// admitted at t+5m, not t+8m.
func TestCheckSequentialGate_TheWaitRunsFromTheRecordedFinishThroughAReadinessLoss(t *testing.T) {
	ctx := context.Background()
	const wait = 5 * time.Minute
	// picture is the chain as of now, with the decoder finished finishedAgo
	// ago and the group's pass of that instant recorded.
	picture := func(t *testing.T, finishedAgo time.Duration) (*v1beta1.InferenceService, client.Client, *v1beta1.InferenceReplica) {
		t.Helper()
		t0 := time.Now().Add(-finishedAgo)
		isvc := sequentialChainISVC(wait)
		decoder := chainMemberIR(isvc, v1beta1.DecoderComponent, "bbbb", "bbbb", "bbbb", metav1.ConditionTrue, t0)
		engine := chainMemberIR(isvc, v1beta1.EngineComponent, "aaaa", "bbbb", "aaaa", metav1.ConditionUnknown, t0)
		c := testClient(append(chainPods(isvc, "bbbb", "aaaa"), decoder, engine)...)
		sequentialChainPass(ctx, t, isvc, c, t0, 0, v1beta1.CompositePhaseSequentialAwaiting)
		return isvc, c, decoder
	}
	const notReady = "Sequential.Soak: engine held while decoder is not Ready; it starts once decoder is Ready and the 5m0s soak after decoder has elapsed"
	t.Run("every decoder pod out of rotation at t+2m45s keeps the engine held, naming the decoder's readiness", func(t *testing.T) {
		isvc, c, decoder := picture(t, 2*time.Minute+45*time.Second)
		setChainMemberReadiness(ctx, t, c, decoder, "bbbb", metav1.ConditionFalse, time.Now())
		allowed, reason := CheckSequentialGate(ctx, c, isvc, v1beta1.EngineComponent)
		if allowed || reason != notReady {
			t.Fatalf("the decoder is not Ready; got allowed=%v reason=%q, want held with %q", allowed, reason, notReady)
		}
	})
	t.Run("the decoder Ready again at t+3m does not start the wait over", func(t *testing.T) {
		isvc, c, decoder := picture(t, wait+time.Second)
		setChainMemberReadiness(ctx, t, c, decoder, "bbbb", metav1.ConditionTrue, time.Now().Add(-2*time.Minute-time.Second))
		allowed, reason := CheckSequentialGate(ctx, c, isvc, v1beta1.EngineComponent)
		if !allowed {
			t.Fatalf("the wait elapsed at t+5m; the gate still holds: %s", reason)
		}
	})
	t.Run("a decoder still not Ready after the wait elapsed holds the engine, and its recovery admits it at once", func(t *testing.T) {
		isvc, c, decoder := picture(t, 6*time.Minute)
		setChainMemberReadiness(ctx, t, c, decoder, "bbbb", metav1.ConditionFalse, time.Now().Add(-3*time.Minute-15*time.Second))
		allowed, reason := CheckSequentialGate(ctx, c, isvc, v1beta1.EngineComponent)
		if allowed || reason != notReady {
			t.Fatalf("the decoder is not Ready although the wait elapsed; got allowed=%v reason=%q, want held with %q", allowed, reason, notReady)
		}
		setChainMemberReadiness(ctx, t, c, decoder, "bbbb", metav1.ConditionTrue, time.Now())
		allowed, reason = CheckSequentialGate(ctx, c, isvc, v1beta1.EngineComponent)
		if !allowed {
			t.Fatalf("the decoder is Ready again and the wait elapsed during the loss; the gate still holds: %s", reason)
		}
	})
}

// mkSequentialFixtureInOrder returns an ISVC whose rollout is a run of
// single-Component blueGreen groups in the given order, recorded as the
// active run, which ResolveGroups folds into one Sequential group.
func mkSequentialFixtureInOrder(name string, order ...v1beta1.ComponentType) *v1beta1.InferenceService {
	groups := make([]v1beta1.RolloutGroup, 0, len(order))
	for _, c := range order {
		groups = append(groups, v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{c}, BlueGreen: &v1beta1.GroupBlueGreen{}})
	}
	isvc := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Groups: groups}}}
	isvc.Name = name
	return pinActiveRun(isvc)
}

// TestCheckSequentialGate_ALaterComponentWaitsUntilTheStagedOneRecordsThePush
// 12 engines, then 8 decoders with 2 kept on version 1, then 2 routers.
// Version 2 rolled and the decoder rests at its staged shape: 6 Instances
// on version 2, 2 held, its current revision still version 1. The operator
// pushes version 3 for the decoder and the router. While the decoder's
// status was not written for the spec it holds, or its projection not for
// the service in hand, the router waits on the decoder; once the status
// records version 3 the router waits on the 6 decoders still to move. A
// decoder the push did not touch rests done at its staged shape and the
// router starts.
func TestCheckSequentialGate_ALaterComponentWaitsUntilTheStagedOneRecordsThePush(t *testing.T) {
	const name = "seq-isvc"
	eng, dec, rtr := v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent
	isvc := mkSequentialFixtureInOrder(name, eng, dec, rtr)
	isvc.Generation = 4
	staged := func() *v1beta1.InferenceReplica {
		return stagedIR(name, dec, "v1", "v2", 8, 2, 4, time.Now().Add(-time.Hour))
	}
	for _, tc := range []struct {
		name       string
		decoder    *v1beta1.InferenceReplica
		wantAllow  bool
		wantReason string
	}{
		{
			name: "the decoder's status still records the previous push",
			decoder: func() *v1beta1.InferenceReplica {
				ir := staged()
				ir.Status.ObservedGeneration = 3 // the spec moved to generation 4; the status was written for 3
				return ir
			}(),
			wantReason: "Sequential waiting on decoder",
		},
		{
			name: "the decoder's projection trails the service",
			decoder: func() *v1beta1.InferenceReplica {
				ir := staged()
				ir.Generation, ir.Status.ObservedGeneration = 3, 3
				ir.Annotations = parentGenAnnotation(3) // the service is at generation 4
				return ir
			}(),
			wantReason: "Sequential waiting on decoder",
		},
		{
			name: "the decoder's status records the push with nothing moved yet",
			decoder: func() *v1beta1.InferenceReplica {
				ir := staged()
				ir.Status.UpdateRevision = revName(name, dec, "v3")
				return ir
			}(),
			wantReason: "Sequential waiting on decoder",
		},
		{
			name:       "the push did not touch the decoder",
			decoder:    staged(),
			wantAllow:  true,
			wantReason: "Sequential active Component",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := seqReadyIR(name, eng, "v2", "v2", "v2", 12, 4)
			router := seqReadyIR(name, rtr, "v2", "v3", "v2", 2, 4)
			router.Status.ReadyReplicas, router.Status.ServingReplicas = 2, 2
			cl := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(engine, tc.decoder, router).Build()
			allowed, reason := CheckSequentialGate(context.Background(), cl, isvc, rtr)
			if allowed != tc.wantAllow || reason != tc.wantReason {
				t.Fatalf("router: allowed=%v reason=%q, want allowed=%v reason=%q", allowed, reason, tc.wantAllow, tc.wantReason)
			}
		})
	}
}
