package canary

import (
	"context"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
)

func i32(v int32) *int32 { return &v }

// canaryISVC builds an ISVC whose rollout canaries the single engine Component
// (v2: one RolloutGroup with a canary progression over [engine]). Each step's own
// Pause/Analysis decides its gate (manual = bare pause, timed = pause+duration,
// metric-gated = analysis).
func canaryISVC(steps []v1beta1.RolloutGroupStep, scaleDownDelay *int32) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc"},
		Spec: v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			Canary: &v1beta1.GroupCanary{
				Steps:                 steps,
				ScaleDownDelaySeconds: scaleDownDelay,
			},
		}}}},
	}
}

func twoStep() []v1beta1.RolloutGroupStep {
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
}

// pinActiveRun pins the ISVC's current spec.rollout as its active rollout run
// (the state the run layer produces at run open), so Dispatch derives
// RunActive=true and rollout.CanaryGroups reads the pinned plan. A pinned plan
// is inert to later spec edits — re-pin after mutating spec.rollout when the
// edit should take effect.
func pinActiveRun(isvc *v1beta1.InferenceService) *v1beta1.InferenceService {
	if isvc == nil || isvc.Spec.Rollout == nil {
		return isvc
	}
	groups := make([]v1beta1.RolloutRunGroup, 0, len(isvc.Spec.Rollout.Groups))
	for i := range isvc.Spec.Rollout.Groups {
		groups = append(groups, v1beta1.RolloutRunGroup{
			Source: v1beta1.RolloutPlanSourceInline,
			Group:  *isvc.Spec.Rollout.Groups[i].DeepCopy(),
		})
	}
	isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
		RunID:    "test",
		OpenedAt: metav1.Now(),
		Plan:     v1beta1.RolloutRunPlan{Groups: groups},
	}}
	return isvc
}

func phaseOf(isvc *v1beta1.InferenceService) v1beta1.RolloutPhase {
	return isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase
}

func baseInputs(isvc *v1beta1.InferenceService, pods map[string]int32) ReconcileInputs {
	return ReconcileInputs{
		ISVC:                   isvc,
		Component:              v1beta1.EngineComponent,
		CanaryRevisionHash:     "new",
		StableRevisionHash:     "old",
		DesiredReplicas:        4,
		PerRevisionPods:        pods,
		SecondaryCapacityReady: true, // single-component tests have no secondaries
		Now:                    time.Unix(1000, 0),
		// A pinned run licenses initialization / re-arm; these tests exercise
		// the step machine, not the run layer.
		RunActive: true,
		// The capacity-gate escalation bound is operator-configured; tests pick
		// the value the assertions are written against.
		DefaultReadyTimeout: 15 * time.Minute,
	}
}

func TestReconcile_NoCanaryIsNoop(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	res, err := Reconcile(context.Background(), ReconcileInputs{ISVC: isvc})
	if err != nil || res == nil || res.Active {
		t.Fatalf("no canary must be inactive noop: res=%+v err=%v", res, err)
	}
	if isvc.Status.Canary != nil {
		t.Fatal("no canary status should be written")
	}
}

func TestReconcile_CapacityGatePending(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	res, _ := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 0}))
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("expected Pending while canary pods not ready, got %q", phaseOf(isvc))
	}
	if res.Partition != 2 { // desired 4 - newCount 2
		t.Fatalf("expected Partition 2, got %d", res.Partition)
	}
	if len(isvc.Status.Components[v1beta1.EngineComponent].Traffic) != 0 {
		t.Fatal("no traffic should shift while gated")
	}
}

func TestReconcile_CapacityUsesReadyInstanceCount(t *testing.T) {
	for _, tc := range []struct {
		name           string
		step           int32
		readyPods      int32
		readyInstances int32
	}{
		{name: "half-capacity-instance-shortfall", step: 0, readyPods: 16, readyInstances: 3},
		{name: "full-capacity-instance-shortfall", step: 1, readyPods: 16, readyInstances: 7},
		{name: "pod-readiness-shortfall", step: 0, readyPods: 3, readyInstances: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isvc := canaryISVC(twoStep(), nil)
			isvc.Status.Canary = &v1beta1.CanaryStatus{
				CanaryRevisionHash: "new",
				StableRevisionHash: "old",
				CurrentStep:        tc.step,
			}
			in := baseInputs(isvc, map[string]int32{"new": tc.readyPods, "old": 8})
			in.DesiredReplicas = 8
			in.ReadyCanaryInstances = i32(tc.readyInstances)

			if _, err := Reconcile(context.Background(), in); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if phaseOf(isvc) != v1beta1.RolloutPhasePending {
				t.Fatalf("pods=%d instances=%d must not satisfy step %d, got %q", tc.readyPods, tc.readyInstances, tc.step, phaseOf(isvc))
			}
		})
	}
}

func TestReconcile_InitialConvergenceUsesReadyInstanceCount(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	in := baseInputs(isvc, map[string]int32{"new": 16, "old": 8})
	in.DesiredReplicas = 8
	in.ReadyCanaryInstances = i32(3)

	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if isvc.Status.Canary == nil {
		t.Fatal("three ready Instances must not look fully converged despite 16 ready pods")
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("initial step must wait for four ready Instances, got %q", phaseOf(isvc))
	}
}

func TestReconcile_CanaryingPaused(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 2}))
	if phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("expected Paused at a manual paused step, got %q", phaseOf(isvc))
	}
	tr := isvc.Status.Components[v1beta1.EngineComponent].Traffic
	if len(tr) != 2 {
		t.Fatalf("expected 50/50 split, got %+v", tr)
	}
	if isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("should not advance without promote, step=%d", isvc.Status.Canary.CurrentStep)
	}
}

func TestReconcile_StepZeroResyncsReadyLiveTarget(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash: "old",
		CurrentStep:        0,
		StepEnteredTime:    &metav1.Time{Time: time.Unix(900, 0)},
	}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {RolloutPhase: v1beta1.RolloutPhasePending},
	}

	in := baseInputs(isvc, map[string]int32{"old": 2, "new": 2})
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if isvc.Status.Canary.CanaryRevisionHash != "new" {
		t.Fatalf("step 0 must identify the live target before traffic shifts, got %q", isvc.Status.Canary.CanaryRevisionHash)
	}
	if isvc.Status.Canary.StableRevisionHash != "old" {
		t.Fatalf("stable identity must remain old, got %q", isvc.Status.Canary.StableRevisionHash)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("ready target should enter the manual pause, got %q", phaseOf(isvc))
	}

	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("promoting Reconcile: %v", err)
	}
	if isvc.Status.Canary.CurrentStep != 1 {
		t.Fatalf("promotion of the published target must advance, got step %d", isvc.Status.Canary.CurrentStep)
	}
}

func TestReconcile_StepZeroResyncsReadyTargetBeforeRollback(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash: "old",
		StableRevisionHash: "old",
		CurrentStep:        0,
		StepEnteredTime:    &metav1.Time{Time: time.Unix(900, 0)},
	}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {RolloutPhase: v1beta1.RolloutPhasePending},
	}
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}

	in := baseInputs(isvc, map[string]int32{"old": 2, "new": 2})
	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !res.RolledBack {
		t.Fatalf("rollback request must enter the rollback path, got %+v", res)
	}
	if got := isvc.Status.Canary.CanaryRevisionHash; got != "new" {
		t.Fatalf("rollback must bind to the live target, got canary hash %q", got)
	}
	if got := isvc.Status.Canary.RolledBackRevisionHash; got != "new" {
		t.Fatalf("rollback must reject the live target, got %q", got)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("ready target pods should be draining, got phase %q", phaseOf(isvc))
	}

	in.PerRevisionPods = map[string]int32{"old": 4}
	res, err = Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("held rollback Reconcile: %v", err)
	}
	if !res.RolledBack || phaseOf(isvc) != v1beta1.RolloutPhaseRolledBack {
		t.Fatalf("rollback must stay held after the target drains, phase=%q res=%+v", phaseOf(isvc), res)
	}
	if got := isvc.Status.Canary.RolledBackRevisionHash; got != "new" {
		t.Fatalf("held rollback must keep rejecting the live target, got %q", got)
	}
}

func TestReconcile_ManualPromoteAdvances(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 2}))
	if isvc.Status.Canary.CurrentStep != 1 {
		t.Fatalf("promote should advance to step 1, got %d", isvc.Status.Canary.CurrentStep)
	}
	if isvc.Status.Canary.PromotedThrough != "new" {
		t.Fatalf("the advance must record the applied promote value in status, got %q", isvc.Status.Canary.PromotedThrough)
	}
}

func TestReconcile_StalePromoteDoesNotAdvance(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "someoldhash"}
	Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 2}))
	if isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("stale promote (wrong hash) must not advance, step=%d", isvc.Status.Canary.CurrentStep)
	}
}

func TestReconcile_AutoAdvance(t *testing.T) {
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: 30 * time.Minute}}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	isvc := canaryISVC(steps, nil)
	// The pause is anchored to when the split FIRST serves, not to step entry. Seed
	// StepEnteredTime 40 min in the past (> the 30 min pause) to mimic a slow capacity
	// wait: the step was "entered" long ago, but traffic only shifts now — the soak
	// must still run from now, so the canary must NOT advance immediately.
	t0 := time.Unix(100000, 0)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, StepEnteredTime: &metav1.Time{Time: t0.Add(-40 * time.Minute)}}

	in := baseInputs(isvc, map[string]int32{"new": 2})
	in.Now = t0 // capacity met → split serves → pause re-anchored to t0, phase Paused
	Reconcile(context.Background(), in)
	if isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("pause must anchor to the split serving, not the 40-min-old step entry; advanced too early, step=%d", isvc.Status.Canary.CurrentStep)
	}
	in.Now = t0.Add(10 * time.Minute) // before the pause elapses (from t0)
	Reconcile(context.Background(), in)
	if isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("auto must not advance before the pause elapses, step=%d", isvc.Status.Canary.CurrentStep)
	}
	in.Now = t0.Add(31 * time.Minute) // after the pause elapses (from t0)
	Reconcile(context.Background(), in)
	if isvc.Status.Canary.CurrentStep != 1 {
		t.Fatalf("auto must advance after the pause elapses, step=%d", isvc.Status.Canary.CurrentStep)
	}
}

func TestReconcile_FinalStepCompletes(t *testing.T) {
	isvc := canaryISVC([]v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}, nil)
	// Mid-rollout at the final step (canary already initialized): pods fully on
	// the target, no drain delay → completes. Starting fresh with pods already
	// converged is the "already done" case, not a completion (see
	// TestReconcile_AlreadyConvergedNoRestart).
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, StepEnteredTime: &metav1.Time{Time: time.Unix(1000, 0)}}
	res, _ := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 4}))
	if phaseOf(isvc) != v1beta1.RolloutPhaseStable {
		t.Fatalf("expected Stable on completion, got %q", phaseOf(isvc))
	}
	if !res.Complete || res.Partition != 0 {
		t.Fatalf("expected complete + partition 0, got %+v", res)
	}
	// Status is KEPT at the done sentinel (CurrentStep == len(steps)), not cleared:
	// EffectivePartition maps that to partition 0 so the old revision drains. A
	// cleared (nil) status would re-default EffectivePartition to step 0 and hold
	// instances back on the old revision after completion.
	if isvc.Status.Canary == nil || int(isvc.Status.Canary.CurrentStep) != 1 {
		t.Fatalf("canary status should be marked done (CurrentStep=len(steps)=1), got %+v", isvc.Status.Canary)
	}
	// A subsequent reconcile of the finished canary is a no-op — no re-completion.
	res2, _ := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 4}))
	if res2.Active || res2.Complete {
		t.Fatalf("finished canary must be a no-op on re-reconcile, got %+v", res2)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseStable {
		t.Fatalf("finished canary should stay Stable, got %q", phaseOf(isvc))
	}
}

// TestReconcile_AlreadyConvergedNoRestart guards completion idempotency: once a
// canary finishes it clears Status.Canary, but the canary spec stays on the
// ISVC. A reconcile that then sees a nil status alongside a component already
// fully on the target revision must NOT re-initialize a fresh canary — otherwise
// the rollout loops forever (verified on KIND). A canary only starts when there
// is an actual revision change still to roll out.
func TestReconcile_AlreadyConvergedNoRestart(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	res, _ := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 4}))
	if res.Active {
		t.Fatalf("converged component must not (re)start a canary, got %+v", res)
	}
	if isvc.Status.Canary != nil {
		t.Fatal("no canary status should be initialized when already converged")
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseStable {
		t.Fatalf("converged component should report Stable, got %q", phaseOf(isvc))
	}
}

func TestReconcile_NewRunRearmsUnchangedPrimary(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		TargetID:           "target-1",
		CanaryRevisionHash: "router",
		CurrentStep:        2,
	}
	in := baseInputs(isvc, map[string]int32{"router": 4})
	in.CanaryRevisionHash = "router"
	in.StableRevisionHash = "router"
	in.TargetID = "target-2"
	in.SecondaryCapacityReady = false

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Active || isvc.Status.Canary.CurrentStep != 0 || isvc.Status.Canary.TargetID != "target-2" {
		t.Fatalf("new group run must re-arm unchanged primary: status=%+v result=%+v", isvc.Status.Canary, res)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("secondary capacity gate must hold the re-armed run, got %q", phaseOf(isvc))
	}
}

func TestReconcile_LegacyStatusAdoptsRunWithoutRestart(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "router", CurrentStep: 2}
	in := baseInputs(isvc, map[string]int32{"router": 4})
	in.CanaryRevisionHash = "router"
	in.TargetID = "target-1"

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if res.Active || isvc.Status.Canary.CurrentStep != 2 || isvc.Status.Canary.TargetID != "target-1" {
		t.Fatalf("legacy status must adopt the run without restarting: status=%+v result=%+v", isvc.Status.Canary, res)
	}
}

// TestReconcile_UnknownTargetNoStart guards the IR-not-ready window: with no
// known target hash yet (empty), the canary must wait rather than initialize a
// state machine keyed on an empty revision.
func TestReconcile_UnknownTargetNoStart(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	in := baseInputs(isvc, map[string]int32{"old": 4})
	in.CanaryRevisionHash = ""
	res, _ := Reconcile(context.Background(), in)
	if res.Active {
		t.Fatalf("unknown target must not start a canary, got %+v", res)
	}
	if isvc.Status.Canary != nil {
		t.Fatal("no canary status should be initialized before the target is known")
	}
}

func TestReconcile_FinalStepDrainWait(t *testing.T) {
	isvc := canaryISVC([]v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}, i32(60))
	// StepEnteredTime here is the step-entry time; the drain window is anchored to
	// the moment traffic shifts (the first capacity-met reconcile), NOT to this.
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, StepEnteredTime: &metav1.Time{Time: time.Unix(1000, 0)}}

	in := baseInputs(isvc, map[string]int32{"new": 4})
	// Capacity met here → traffic shifts → drain window starts NOW (t=2000), even
	// though the step was "entered" at t=1000.
	in.Now = time.Unix(2000, 0)
	res, _ := Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePromoting || res.Complete {
		t.Fatalf("expected Promoting (draining), got phase=%q complete=%v", phaseOf(isvc), res.Complete)
	}
	in.Now = time.Unix(2000, 0).Add(30 * time.Second) // within drain window (from t=2000)
	res, _ = Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePromoting || res.Complete {
		t.Fatalf("expected still Promoting within the drain window, got phase=%q complete=%v", phaseOf(isvc), res.Complete)
	}
	in.Now = time.Unix(2000, 0).Add(61 * time.Second) // drain elapsed (from the traffic-shift moment)
	res, _ = Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseStable || !res.Complete {
		t.Fatalf("expected Stable + complete after drain, got phase=%q complete=%v", phaseOf(isvc), res.Complete)
	}
}

// TestReconcile_Rollback pins the rollback state machine: record the rejected
// revision, shift traffic to stable, RollingBack while the rejected pods drain →
// RolledBack (held), and re-arm only when a different target appears.
func TestReconcile_Rollback(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, ObservedTrafficWeight: 50}
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}

	// Mid-canary rollback: rejected ("new") pods still present → RollingBack,
	// 100% traffic to stable, the rejected hash recorded.
	res, _ := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 2, "old": 2}))
	if !res.RolledBack || phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("expected RollingBack + RolledBack result, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	if isvc.Status.Canary.RolledBackRevisionHash != "new" {
		t.Fatalf("rejected revision should be recorded, got %+v", isvc.Status.Canary)
	}
	tr := isvc.Status.Components[v1beta1.EngineComponent].Traffic
	if len(tr) != 1 || tr[0].Percent != 100 {
		t.Fatalf("rollback must route 100%% to stable, got %+v", tr)
	}

	// Rejected pods drained → RolledBack (held on stable).
	res, _ = Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"old": 4}))
	if !res.RolledBack || phaseOf(isvc) != v1beta1.RolloutPhaseRolledBack {
		t.Fatalf("expected RolledBack (held) after drain, got phase=%q res=%+v", phaseOf(isvc), res)
	}

	// Clearing the annotation does NOT retry the rejected revision (still held).
	delete(isvc.Annotations, constants.RolloutRollbackAnnotation)
	in := baseInputs(isvc, map[string]int32{"old": 4})
	res, _ = Reconcile(context.Background(), in)
	if !res.RolledBack || isvc.Status.Canary.RolledBackRevisionHash != "new" {
		t.Fatalf("rejected revision must stay held until a new target appears, got %+v res=%+v", isvc.Status.Canary, res)
	}

	// A NEW target re-arms: fresh canary toward it (rejected hash cleared).
	in = baseInputs(isvc, map[string]int32{"old": 4})
	in.CanaryRevisionHash = "v3"
	Reconcile(context.Background(), in)
	if isvc.Status.Canary.RolledBackRevisionHash != "" || isvc.Status.Canary.CanaryRevisionHash != "v3" || isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("new target must re-arm a fresh canary toward v3, got %+v", isvc.Status.Canary)
	}
}

func TestReconcile_NewRunRearmsRollbackWithUnchangedPrimary(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		TargetID:               "target-1",
		CanaryRevisionHash:     "router",
		StableRevisionHash:     "router",
		RolledBackRevisionHash: "router",
	}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseRolledBack)
	in := baseInputs(isvc, map[string]int32{"router": 4})
	in.CanaryRevisionHash = "router"
	in.StableRevisionHash = "router"
	in.TargetID = "target-2"
	in.SecondaryCapacityReady = false

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Active || isvc.Status.Canary.TargetID != "target-2" || isvc.Status.Canary.RolledBackRevisionHash != "" {
		t.Fatalf("new group run must clear the old rollback hold: status=%+v result=%+v", isvc.Status.Canary, res)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("secondary capacity gate must hold the re-armed run, got %q", phaseOf(isvc))
	}
}

func TestReconcile_UnchangedPrimaryRollbackCompletes(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	// The primary under test is the router, so its unit must be the one the
	// group drives — a plan is resolved per unit.
	isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.RouterComponent}
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		TargetID:               "target-1",
		CanaryRevisionHash:     "router",
		StableRevisionHash:     "router",
		RolledBackRevisionHash: "router",
	}
	in := baseInputs(isvc, map[string]int32{"router": 4})
	in.Component = v1beta1.RouterComponent
	in.CanaryRevisionHash = "router"
	in.StableRevisionHash = "router"
	in.TargetID = "target-1"

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	phase := isvc.Status.Components[v1beta1.RouterComponent].RolloutPhase
	if !res.RolledBack || phase != v1beta1.RolloutPhaseRolledBack {
		t.Fatalf("unchanged primary is already at its stable revision: phase=%q result=%+v", phase, res)
	}
}

// TestReconcile_SecondaryCapacityGate pins the PD gate: even when the primary's
// own canary pods are up, no traffic shifts until every secondary component's
// canary capacity is ready.
func TestReconcile_SecondaryCapacityGate(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	in := baseInputs(isvc, map[string]int32{"new": 2}) // primary capacity met
	in.SecondaryCapacityReady = false                  // a secondary's canary pods are not up
	Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("primary ready but a secondary not → Pending, got %q", phaseOf(isvc))
	}
	if len(isvc.Status.Components[v1beta1.EngineComponent].Traffic) != 0 {
		t.Fatal("no traffic should shift until every component's canary capacity is up")
	}
}

// TestReconcile_CapacityGateFailsAfterTimeout pins the Failed escalation: a step
// whose canary pods never become Ready parks in Failed (stable still serving)
// after the ready-timeout instead of polling Pending forever.
func TestReconcile_CapacityGateFailsAfterTimeout(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, StepEnteredTime: &metav1.Time{Time: time.Unix(1000, 0)}}
	in := baseInputs(isvc, map[string]int32{"new": 0}) // canary pods never come up

	in.Now = time.Unix(1000, 0) // enter the capacity wait (anchors the timeout)
	Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("entering the capacity wait → Pending, got %q", phaseOf(isvc))
	}

	in.Now = time.Unix(1000, 0).Add(5 * time.Minute) // before the 15m default
	Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("before ready-timeout → Pending, got %q", phaseOf(isvc))
	}

	in.Now = time.Unix(1000, 0).Add(16 * time.Minute) // past the timeout
	res, _ := Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("after ready-timeout → Failed, got %q", phaseOf(isvc))
	}
	if res.RequeueAfter != failedRequeue {
		t.Fatalf("Failed should slow-requeue at %v, got %v", failedRequeue, res.RequeueAfter)
	}
	if len(isvc.Status.Components[v1beta1.EngineComponent].Traffic) != 0 {
		t.Fatal("no traffic should have shifted (stable keeps serving while Failed)")
	}
}

// TestReconcile_DoneSentinelRestartsOnNewTarget pins the other half of the
// done-sentinel branch: a finished canary that sees a NEW target hash restarts
// the plan from step 0 against the new revision (a subsequent spec bump),
// carrying NONE of the finished canary's analysis state.
func TestReconcile_DoneSentinelRestartsOnNewTarget(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	// Finished canary on the previous target (done sentinel: CurrentStep == 2).
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:    "old-canary",
		CurrentStep:           2,
		ObservedTrafficWeight: 100,
		AnalysisFailedChecks:  2,
		LastEvaluationTime:    &metav1.Time{Time: time.Unix(900, 0)},
		MetricResults:         []v1beta1.AnalysisMetricResult{{Name: "err", Passed: false}},
	}
	in := baseInputs(isvc, map[string]int32{"v3": 0}) // new target v3, its pods not up yet
	in.CanaryRevisionHash = "v3"
	res, _ := Reconcile(context.Background(), in)
	if !res.Active {
		t.Fatalf("a new target after completion must restart the canary (Active), got %+v", res)
	}
	cs := isvc.Status.Canary
	if cs.CurrentStep != 0 || cs.CanaryRevisionHash != "v3" {
		t.Fatalf("restart should reset to step 0 + the new hash, got %+v", cs)
	}
	if cs.AnalysisFailedChecks != 0 || cs.LastEvaluationTime != nil || cs.MetricResults != nil {
		t.Fatalf("restart must not carry the prior canary's analysis state, got %+v", cs)
	}
}

// TestReconcile_RetargetResetsAnalysisState pins the mid-rollout retarget: a new
// target revision restarts the plan with a FULL status reset. Carrying the prior
// canary's failure budget / metric results over would let a stale
// AnalysisFailedChecks roll back the new (healthy) revision on its first breach.
func TestReconcile_RetargetResetsAnalysisState(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	t0 := time.Unix(100000, 0)
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:           "new",
		CurrentStep:                  0,
		ObservedTrafficWeight:        50, // traffic shifted → retarget (not the step-0 re-sync)
		StepEnteredTime:              &metav1.Time{Time: t0.Add(-time.Hour)},
		AnalysisFailedChecks:         2,
		LastEvaluationTime:           &metav1.Time{Time: t0.Add(-time.Minute)},
		LastConclusiveEvaluationTime: &metav1.Time{Time: t0.Add(-time.Minute)},
		MetricResults:                []v1beta1.AnalysisMetricResult{{Name: "err", Passed: false}},
	}
	in := baseInputs(isvc, map[string]int32{"v3": 0, "new": 2, "old": 2})
	in.CanaryRevisionHash = "v3"
	in.Now = t0
	Reconcile(context.Background(), in)
	cs := isvc.Status.Canary
	if cs.CanaryRevisionHash != "v3" || cs.CurrentStep != 0 || cs.ObservedTrafficWeight != 0 {
		t.Fatalf("retarget must restart at step 0 toward v3, got %+v", cs)
	}
	if cs.AnalysisFailedChecks != 0 || cs.LastEvaluationTime != nil || cs.LastConclusiveEvaluationTime != nil || cs.MetricResults != nil {
		t.Fatalf("retarget must not carry prior-canary analysis state, got %+v", cs)
	}
}

func TestReconcile_ZeroTrafficStepRetargetResetsState(t *testing.T) {
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 0, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	isvc := canaryISVC(steps, nil)
	t0 := time.Unix(100000, 0)
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:           "first",
		StableRevisionHash:           "old",
		CurrentStep:                  0,
		ObservedTrafficWeight:        0,
		PromotedThrough:              "first",
		StepEnteredTime:              &metav1.Time{Time: t0.Add(-time.Hour)},
		AnalysisFailedChecks:         2,
		LastEvaluationTime:           &metav1.Time{Time: t0.Add(-time.Minute)},
		LastConclusiveEvaluationTime: &metav1.Time{Time: t0.Add(-time.Minute)},
		MetricResults:                []v1beta1.AnalysisMetricResult{{Name: "err", Passed: false}},
	}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhasePaused)

	in := baseInputs(isvc, map[string]int32{"second": 2, "first": 2, "old": 2})
	in.CanaryRevisionHash = "second"
	in.Now = t0
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	cs := isvc.Status.Canary
	if cs.CanaryRevisionHash != "second" || cs.CurrentStep != 0 || cs.ObservedTrafficWeight != 0 {
		t.Fatalf("zero-traffic retarget must restart step 0 toward second, got %+v", cs)
	}
	if cs.StableRevisionHash != "old" {
		t.Fatalf("zero-traffic retarget must preserve stable identity, got %q", cs.StableRevisionHash)
	}
	if cs.PromotedThrough != "" || cs.AnalysisFailedChecks != 0 || cs.LastEvaluationTime != nil ||
		cs.LastConclusiveEvaluationTime != nil || cs.MetricResults != nil {
		t.Fatalf("zero-traffic retarget must clear prior-target state, got %+v", cs)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("ready retarget should hold at the manual step, got %q", phaseOf(isvc))
	}

	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "second"}
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("promoting Reconcile: %v", err)
	}
	if cs.CurrentStep != 1 {
		t.Fatalf("promotion of the retargeted revision must advance, got step %d", cs.CurrentStep)
	}
}

// threeStep is a plan with two manual (bare-pause) steps before the final 100%
// step, so a second manual promotion is required mid-rollout.
func threeStep() []v1beta1.RolloutGroupStep {
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 25, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
}

// TestReconcile_PromoteConsumptionPersisted pins the durable promote sequence:
// the advancing pass records the promote in status and writes NO metadata (the
// annotation must outlive the pass — removing it before the status flush is
// durable would lose the promote if that flush fails); the next pass removes
// the annotation on the apiserver while the recorded value keeps it from
// advancing a second step; once the annotation is observed gone the record
// clears, re-arming manual promotion.
func TestReconcile_PromoteConsumptionPersisted(t *testing.T) {
	isvc := canaryISVC(threeStep(), nil)
	isvc.Namespace = "ns"
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithObjects(isvc).Build()

	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.Client = c
	// Pass 1: the promote advances one step in-memory; the apiserver metadata is
	// untouched — the durable record of the promotion is status, not the
	// annotation removal.
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if isvc.Status.Canary.CurrentStep != 1 {
		t.Fatalf("promote should advance one step, got %d", isvc.Status.Canary.CurrentStep)
	}
	if isvc.Status.Canary.PromotedThrough != "new" {
		t.Fatalf("the advance must record the applied promote, got %q", isvc.Status.Canary.PromotedThrough)
	}
	stored := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "svc"}, stored); err != nil {
		t.Fatal(err)
	}
	if _, present := stored.Annotations[constants.RolloutPromoteAnnotation]; !present {
		t.Fatal("the advancing pass must not remove the annotation before the advance is durable")
	}

	// Pass 2: the advance is durable (status carried over); the lingering
	// annotation is inert and is now removed on the apiserver.
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if isvc.Status.Canary.CurrentStep != 1 {
		t.Fatalf("one promote must advance one step only, got %d", isvc.Status.Canary.CurrentStep)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "svc"}, stored); err != nil {
		t.Fatal(err)
	}
	if _, still := stored.Annotations[constants.RolloutPromoteAnnotation]; still {
		t.Fatal("promote consumption must be persisted to the apiserver, not just in-memory")
	}

	// Pass 3: the annotation is observed gone → the record clears (rides the
	// status flush), and the step still holds.
	isvc.Annotations = stored.Annotations
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if isvc.Status.Canary.CurrentStep != 1 {
		t.Fatalf("clearing the record must not advance, got %d", isvc.Status.Canary.CurrentStep)
	}
	if isvc.Status.Canary.PromotedThrough != "" {
		t.Fatalf("the record must clear once the annotation is observed gone, got %q", isvc.Status.Canary.PromotedThrough)
	}
}

// TestReconcile_PromoteSurvivesLostStatusWrite pins the lose-forever fix: the
// step advance rides the controller's deferred status write, which can be lost
// (conflict-retry exhaustion, or the process dying before the flush). Because
// the advancing pass leaves the annotation on the apiserver, replaying the
// reconcile from the pre-advance persisted state re-applies the promote
// instead of holding Paused forever.
func TestReconcile_PromoteSurvivesLostStatusWrite(t *testing.T) {
	isvc := canaryISVC(threeStep(), nil)
	isvc.Namespace = "ns"
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithObjects(isvc).Build()

	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.Client = c
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if isvc.Status.Canary.CurrentStep != 1 {
		t.Fatalf("promote should advance in-memory, got %d", isvc.Status.Canary.CurrentStep)
	}

	// The status flush never lands: rebuild the reconcile state from what IS
	// persisted — annotations from the apiserver, status from before the pass.
	stored := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "svc"}, stored); err != nil {
		t.Fatal(err)
	}
	replay := canaryISVC(threeStep(), nil)
	replay.Namespace = "ns"
	replay.Annotations = stored.Annotations
	runIn := baseInputs(replay, map[string]int32{"new": 2, "old": 2})
	runIn.Client = c
	if _, err := Reconcile(context.Background(), runIn); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if replay.Status.Canary == nil || replay.Status.Canary.CurrentStep != 1 {
		t.Fatalf("the promote must survive a lost status write and re-advance, got %+v", replay.Status.Canary)
	}
	if replay.Status.Canary.PromotedThrough != "new" {
		t.Fatalf("the replayed advance must record the promote, got %q", replay.Status.Canary.PromotedThrough)
	}
}

// TestReconcile_LingeringPromoteDoesNotDoubleAdvance pins the other half of the
// crash window: the advance persisted (step + PromotedThrough) but the process
// died before the annotation was removed. The next reconcile must treat the
// lingering annotation as already applied — no second advance — and complete
// the removal on the apiserver.
func TestReconcile_LingeringPromoteDoesNotDoubleAdvance(t *testing.T) {
	isvc := canaryISVC(threeStep(), nil)
	isvc.Namespace = "ns"
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash: "new",
		CurrentStep:        1,
		PromotedThrough:    "new",
		StepEnteredTime:    &metav1.Time{Time: time.Unix(1000, 0)},
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithObjects(isvc).Build()

	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.Client = c
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if isvc.Status.Canary.CurrentStep != 1 {
		t.Fatalf("a lingering applied promote must not advance again, got step %d", isvc.Status.Canary.CurrentStep)
	}
	stored := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "svc"}, stored); err != nil {
		t.Fatal(err)
	}
	if _, still := stored.Annotations[constants.RolloutPromoteAnnotation]; still {
		t.Fatal("the lingering annotation must be removed on the apiserver")
	}
}

// TestReconcile_RepromoteAdvancesNextManualStep pins re-arming: once a
// promotion fully settles (annotation removed, record cleared), a fresh
// promote — necessarily carrying the SAME canary hash — must advance the next
// manual step rather than being mistaken for the previous promotion's leftover.
func TestReconcile_RepromoteAdvancesNextManualStep(t *testing.T) {
	isvc := canaryISVC(threeStep(), nil)
	isvc.Namespace = "ns"
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithObjects(isvc).Build()

	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.Client = c
	// Promote settles over three passes: advance, remove annotation, clear record.
	for i := 0; i < 3; i++ {
		if _, err := Reconcile(context.Background(), in); err != nil {
			t.Fatalf("Reconcile pass %d: %v", i, err)
		}
	}
	if isvc.Status.Canary.CurrentStep != 1 || isvc.Status.Canary.PromotedThrough != "" {
		t.Fatalf("promotion should be settled at step 1 with a cleared record, got %+v", isvc.Status.Canary)
	}

	// The operator promotes the next step with the same hash value.
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if isvc.Status.Canary.CurrentStep != 2 {
		t.Fatalf("a fresh promote after settling must advance the next step, got %d", isvc.Status.Canary.CurrentStep)
	}
}

// TestReconcile_ConsumeFailureDoesNotAbort pins that annotation removal is
// best-effort: a failing metadata patch must not error the reconcile or
// disturb the already-durable advance; the removal is simply retried on a
// later pass, and the lingering annotation stays inert meanwhile.
func TestReconcile_ConsumeFailureDoesNotAbort(t *testing.T) {
	isvc := canaryISVC(threeStep(), nil)
	isvc.Namespace = "ns"
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash: "new",
		CurrentStep:        1,
		PromotedThrough:    "new",
		StepEnteredTime:    &metav1.Time{Time: time.Unix(1000, 0)},
	}
	base := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithObjects(isvc).Build()
	failing := interceptor.NewClient(base, interceptor.Funcs{
		Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
			return errors.New("injected patch failure")
		},
	})

	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.Client = failing
	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("a failed annotation removal must not abort the reconcile: %v", err)
	}
	if !res.Active || isvc.Status.Canary.CurrentStep != 1 {
		t.Fatalf("the durable advance must be undisturbed, got step %d res %+v", isvc.Status.Canary.CurrentStep, res)
	}
	if isvc.Status.Canary.PromotedThrough != "new" {
		t.Fatalf("the record must survive a failed removal (it keeps the annotation inert), got %q", isvc.Status.Canary.PromotedThrough)
	}
	if _, present := isvc.Annotations[constants.RolloutPromoteAnnotation]; !present {
		t.Fatal("a failed removal must keep the annotation (retried next pass)")
	}

	// A later pass with a healthy client converges: annotation removed durably.
	in.Client = base
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	stored := &v1beta1.InferenceService{}
	if err := base.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "svc"}, stored); err != nil {
		t.Fatal(err)
	}
	if _, still := stored.Annotations[constants.RolloutPromoteAnnotation]; still {
		t.Fatal("the retried removal must land on the apiserver")
	}
	if isvc.Status.Canary.CurrentStep != 1 {
		t.Fatalf("one promote must advance one step only, got %d", isvc.Status.Canary.CurrentStep)
	}
}

// TestReconcile_CompletionClearsPromotedThrough pins the completion end of the
// durable-promote lifecycle: a promote that advances into the final step can be
// consumed on the same pass the final capacity converges, so completion lands
// while the record still awaits observed annotation absence. Completion must
// clear the record — the done sentinel returns before the main-path sync, so
// residue would otherwise persist until a new canary re-arms.
func TestReconcile_CompletionClearsPromotedThrough(t *testing.T) {
	isvc := canaryISVC(threeStep(), nil)
	isvc.Namespace = "ns"
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash: "new",
		CurrentStep:        1,
		StepEnteredTime:    &metav1.Time{Time: time.Unix(1000, 0)},
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithObjects(isvc).Build()

	in := baseInputs(isvc, map[string]int32{"new": 4})
	in.Client = c
	// Pass 1: the promote advances into the final step, recording the durable
	// promote; the annotation stays until the advance persists.
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if isvc.Status.Canary.CurrentStep != 2 || isvc.Status.Canary.PromotedThrough != "new" {
		t.Fatalf("promote should advance into the final step recording the promote, got %+v", isvc.Status.Canary)
	}

	// Pass 2: the annotation is consumed AND the final step completes (capacity
	// already converged, no drain window) on the same pass.
	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !res.Complete || isvc.Status.Canary.CurrentStep != 3 {
		t.Fatalf("expected completion, got res=%+v status=%+v", res, isvc.Status.Canary)
	}
	if isvc.Status.Canary.PromotedThrough != "" {
		t.Fatalf("completion must clear the durable promote record, got %q", isvc.Status.Canary.PromotedThrough)
	}
	stored := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "svc"}, stored); err != nil {
		t.Fatal(err)
	}
	if _, still := stored.Annotations[constants.RolloutPromoteAnnotation]; still {
		t.Fatal("the applied promote must be removed on the apiserver by completion")
	}

	// Pass 3: the done sentinel holds — inactive, no residue reappears.
	res, err = Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.Active || isvc.Status.Canary.PromotedThrough != "" {
		t.Fatalf("done canary must stay settled, got res=%+v record=%q", res, isvc.Status.Canary.PromotedThrough)
	}
}

// TestReconcile_DoneSentinelSettlesPromoteResidue pins residue convergence for
// an already-completed canary whose durable promote record survived completion
// (e.g. a status recorded before completion cleared it): the done sentinel must
// still retry the annotation removal while it lingers and clear the record once
// the annotation is observed gone — without re-activating the canary.
func TestReconcile_DoneSentinelSettlesPromoteResidue(t *testing.T) {
	isvc := canaryISVC(threeStep(), nil)
	isvc.Namespace = "ns"
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:    "new",
		CurrentStep:           3, // done sentinel
		ObservedTrafficWeight: 100,
		PromotedThrough:       "new",
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithObjects(isvc).Build()

	in := baseInputs(isvc, map[string]int32{"new": 4})
	in.Client = c
	// Pass 1: the lingering annotation matches the record → removed on the
	// apiserver; the record waits for observed absence.
	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.Active || isvc.Status.Canary.CurrentStep != 3 {
		t.Fatalf("done canary must stay inactive at the sentinel, got res=%+v status=%+v", res, isvc.Status.Canary)
	}
	stored := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "svc"}, stored); err != nil {
		t.Fatal(err)
	}
	if _, still := stored.Annotations[constants.RolloutPromoteAnnotation]; still {
		t.Fatal("the sentinel must retry the lingering annotation removal on the apiserver")
	}

	// Pass 2: the annotation is observed gone → the record clears.
	isvc.Annotations = stored.Annotations
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if isvc.Status.Canary.PromotedThrough != "" {
		t.Fatalf("the record must clear once the annotation is observed gone, got %q", isvc.Status.Canary.PromotedThrough)
	}
	if isvc.Status.Canary.CurrentStep != 3 {
		t.Fatalf("settling residue must not re-activate the canary, got step %d", isvc.Status.Canary.CurrentStep)
	}
}

// TestReconcile_RollbackConsumptionPersistedOnRearm pins the rollback-annotation
// half of durable consumption: a re-arm toward a new target must remove
// ome.io/rollout-rollback on the apiserver — left in place, the reloaded
// annotation marks the fresh canary rolled back on its first pass.
func TestReconcile_RollbackConsumptionPersistedOnRearm(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = "ns"
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", RolledBackRevisionHash: "new", AnalysisFailedChecks: 3}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithObjects(isvc).Build()

	in := baseInputs(isvc, map[string]int32{"old": 4})
	in.Client = c
	in.CanaryRevisionHash = "v3"
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	cs := isvc.Status.Canary
	if cs.CanaryRevisionHash != "v3" || cs.RolledBackRevisionHash != "" || cs.AnalysisFailedChecks != 0 {
		t.Fatalf("re-arm must fully reset toward v3, got %+v", cs)
	}
	stored := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "svc"}, stored); err != nil {
		t.Fatal(err)
	}
	if _, still := stored.Annotations[constants.RolloutRollbackAnnotation]; still {
		t.Fatal("rollback consumption must be persisted to the apiserver, not just in-memory")
	}
}

// TestReconcile_CapacityDipAfterLongBakeIsNotFailed pins the capacity-gate
// anchor: the ready-timeout measures the CURRENT capacity wait, not time since
// step entry. A step that has been serving (baking) longer than the timeout
// must re-enter Pending on a capacity dip — not park a healthy canary Failed
// on the first pass.
func TestReconcile_CapacityDipAfterLongBakeIsNotFailed(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	t0 := time.Unix(100000, 0)
	// Step 0 has been serving (Paused) for 20 min — past the 15 min default timeout.
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:    "new",
		CurrentStep:           0,
		ObservedTrafficWeight: 50,
		StepEnteredTime:       &metav1.Time{Time: t0.Add(-20 * time.Minute)},
	}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhasePaused)

	in := baseInputs(isvc, map[string]int32{"new": 0, "old": 2}) // canary pods just dipped
	in.Now = t0
	Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("capacity dip after a long bake → Pending (fresh wait), got %q", phaseOf(isvc))
	}
	// Still down a full ready-timeout after the dip → Failed.
	in.Now = t0.Add(16 * time.Minute)
	Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("capacity still down a full timeout after the dip → Failed, got %q", phaseOf(isvc))
	}
}

// TestReconcile_NegativeStepClamped guards the unvalidated status subresource:
// an externally-written negative CurrentStep must clamp to 0, not panic the
// reconcile indexing plan.Steps.
func TestReconcile_NegativeStepClamped(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: -3}
	res, err := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 0}))
	if err != nil || res == nil {
		t.Fatalf("negative step must not panic: res=%+v err=%v", res, err)
	}
	if isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("negative step should clamp to 0, got %d", isvc.Status.Canary.CurrentStep)
	}
}

// TestReconcile_ReadyTimeoutAnnotationOverride pins that an operator-shortened
// ready-timeout actually changes when Failed is entered.
func TestReconcile_ReadyTimeoutAnnotationOverride(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Annotations = map[string]string{constants.RolloutReadyTimeoutAnnotation: "2m"}
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, StepEnteredTime: &metav1.Time{Time: time.Unix(1000, 0)}}
	in := baseInputs(isvc, map[string]int32{"new": 0}) // canary pods never come up

	in.Now = time.Unix(1000, 0) // enter the capacity wait (anchors the timeout)
	Reconcile(context.Background(), in)
	in.Now = time.Unix(1000, 0).Add(90 * time.Second) // < 2m
	Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("before the 2m override → Pending, got %q", phaseOf(isvc))
	}
	in.Now = time.Unix(1000, 0).Add(121 * time.Second) // > 2m (well before the 15m default)
	Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("after the 2m override → Failed, got %q", phaseOf(isvc))
	}
}

// TestReconcile_AnalysisFailedIsSticky pins the parked-terminal contract for the
// analysis path: once a step escalates to Failed (analysis stalled inconclusive
// past the ready-timeout), it stays Failed across reconciles — it does NOT
// oscillate back to Paused. The hold must not re-stamp StepEnteredTime (that
// would reset the stall clock the next pass reads as "not stalled," flipping to
// Paused, then re-failing a timeout later) nor re-sample. It re-arms only when a
// genuinely new target revision appears.
func TestReconcile_AnalysisFailedIsSticky(t *testing.T) {
	analysisStep := v1beta1.RolloutGroupStep{
		Capacity: intstr.FromString("50%"), Traffic: 50,
		Analysis: &v1beta1.RolloutAnalysis{
			Interval:     metav1.Duration{Duration: time.Minute},
			FailureLimit: 3,
			Metrics:      []v1beta1.AnalysisMetric{{Name: "err", Query: "q", Operator: v1beta1.ComparisonLTE, Threshold: "0.05"}},
		},
	}
	isvc := canaryISVC([]v1beta1.RolloutGroupStep{analysisStep, {Capacity: intstr.FromString("100%"), Traffic: 100}}, nil)

	t0 := time.Unix(100000, 0)
	// Seed the step already Canarying with both anchors 20 min in the past (> the
	// 15 min default ready-timeout) so the next inconclusive sample is past the
	// stall and escalates to Failed on this reconcile. Phase Canarying makes the
	// intermediate-step anchor skip its re-stamp, so the past anchors survive.
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:           "new",
		CurrentStep:                  0,
		StepEnteredTime:              &metav1.Time{Time: t0.Add(-20 * time.Minute)},
		LastConclusiveEvaluationTime: &metav1.Time{Time: t0.Add(-20 * time.Minute)},
	}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseCanarying)

	// First reconcile: inconclusive sample past the stall → Failed.
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.Now = t0
	inconclusive := &countingSample{outcome: analysis.Inconclusive, at: t0}
	in.Sampler = inconclusive
	res, _ := Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("analysis stalled past the ready-timeout must escalate to Failed, got %q", phaseOf(isvc))
	}
	if res.RequeueAfter != failedRequeue {
		t.Fatalf("Failed should slow-requeue at %v, got %v", failedRequeue, res.RequeueAfter)
	}
	enteredAtFailure := isvc.Status.Canary.StepEnteredTime.Time

	// Second reconcile: same (unchanged) target, a later clock, and a sampler that
	// would PASS — under the oscillation bug the re-stamp + re-evaluate would flip
	// this to Paused (or advance). Sticky Failed must hold without re-sampling and
	// without re-stamping StepEnteredTime.
	in.Now = t0.Add(13 * time.Minute) // ready-timeout + stall would fire again on their cadence
	passing := &countingSample{outcome: analysis.Pass, at: in.Now}
	in.Sampler = passing
	res, _ = Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("Failed must be sticky, not oscillate to %q", phaseOf(isvc))
	}
	if res.RequeueAfter != failedRequeue {
		t.Fatalf("held Failed should keep the slow heartbeat %v, got %v", failedRequeue, res.RequeueAfter)
	}
	if !isvc.Status.Canary.StepEnteredTime.Time.Equal(enteredAtFailure) {
		t.Fatalf("StepEnteredTime must not be re-stamped while held Failed: was %v, now %v", enteredAtFailure, isvc.Status.Canary.StepEnteredTime.Time)
	}
	if passing.calls != 0 {
		t.Fatalf("held Failed must not re-sample analysis, got %d calls", passing.calls)
	}
	if isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("held Failed must not advance the step, got %d", isvc.Status.Canary.CurrentStep)
	}

	// A NEW target re-arms a fresh canary (step 0, new hash) and leaves the parked
	// Failed phase behind.
	in.Now = t0.Add(20 * time.Minute)
	in.CanaryRevisionHash = "v3"
	in.PerRevisionPods = map[string]int32{"old": 4} // new target's pods not up yet
	in.Sampler = &countingSample{outcome: analysis.Pass, at: in.Now}
	Reconcile(context.Background(), in)
	if isvc.Status.Canary.CanaryRevisionHash != "v3" || isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("a new target must re-arm a fresh canary toward v3, got %+v", isvc.Status.Canary)
	}
	if phaseOf(isvc) == v1beta1.RolloutPhaseFailed {
		t.Fatal("re-armed canary must leave the parked Failed phase, still Failed")
	}
}

// TestReconcile_InitCapturesStableIdentity pins that first sight of a new
// canary persists which revision was stable at that moment — the rollback
// target must not depend on later, mutable observations.
func TestReconcile_InitCapturesStableIdentity(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 0, "old": 4}))
	if isvc.Status.Canary == nil || isvc.Status.Canary.StableRevisionHash != "old" {
		t.Fatalf("init must capture the stable revision identity, got %+v", isvc.Status.Canary)
	}
}

// TestReconcile_RetargetRollbackReturnsToOriginalStable pins the retarget
// contract: stable A → partial canary B → retarget C → rollback returns to A.
// The stable identity is the persisted status value, never re-inferred from
// live pods (B's pods outnumber A's here, so an inference would name B).
func TestReconcile_RetargetRollbackReturnsToOriginalStable(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)

	// Canary toward B initializes while A serves: stable identity A captured.
	inB := baseInputs(isvc, map[string]int32{"revB": 0, "revA": 4})
	inB.CanaryRevisionHash = "revB"
	inB.StableRevisionHash = "revA"
	Reconcile(context.Background(), inB)
	if got := isvc.Status.Canary.StableRevisionHash; got != "revA" {
		t.Fatalf("init must persist stable identity A, got %q", got)
	}

	// B partially rolls; the 50% split serves (traffic shifted).
	inB = baseInputs(isvc, map[string]int32{"revB": 2, "revA": 2})
	inB.CanaryRevisionHash = "revB"
	inB.StableRevisionHash = "revA"
	Reconcile(context.Background(), inB)
	if isvc.Status.Canary.ObservedTrafficWeight != 50 {
		t.Fatalf("setup: traffic should have shifted to the B split, got %+v", isvc.Status.Canary)
	}

	// Retarget to C mid-canary. The observed stable input names B (the
	// most-populated live non-canary revision) — the persisted A must win.
	inC := baseInputs(isvc, map[string]int32{"revC": 0, "revB": 3, "revA": 1})
	inC.CanaryRevisionHash = "revC"
	inC.StableRevisionHash = "revB"
	Reconcile(context.Background(), inC)
	cs := isvc.Status.Canary
	if cs.CanaryRevisionHash != "revC" || cs.CurrentStep != 0 {
		t.Fatalf("retarget must restart toward C, got %+v", cs)
	}
	if cs.StableRevisionHash != "revA" {
		t.Fatalf("retarget must preserve the original stable identity A, got %q", cs.StableRevisionHash)
	}

	// Rollback of C: traffic must return 100% to A, not to B.
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	inRB := baseInputs(isvc, map[string]int32{"revC": 2, "revB": 3, "revA": 1})
	inRB.CanaryRevisionHash = "revC"
	inRB.StableRevisionHash = "revB"
	res, _ := Reconcile(context.Background(), inRB)
	if !res.RolledBack || isvc.Status.Canary.RolledBackRevisionHash != "revC" {
		t.Fatalf("rollback must reject C, got res=%+v cs=%+v", res, isvc.Status.Canary)
	}
	tr := isvc.Status.Components[v1beta1.EngineComponent].Traffic
	wantStable := coordination.PerRevisionServiceName("svc", v1beta1.EngineComponent, "revA")
	if len(tr) != 1 || tr[0].Percent != 100 || tr[0].RevisionName != wantStable {
		t.Fatalf("rollback must route 100%% to the original stable %s, got %+v", wantStable, tr)
	}
}

// TestReconcile_RollbackTargetSurvivesRestart pins the durability contract:
// the stable identity lives in the status subresource, so a rollback decided
// by a freshly restarted controller — holding only the persisted status and
// live observations, no in-memory history — still returns to the original
// stable revision.
func TestReconcile_RollbackTargetSurvivesRestart(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	// The status as re-read from the apiserver after a restart: mid-canary
	// toward C following an earlier A→B→C retarget.
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:    "revC",
		StableRevisionHash:    "revA",
		CurrentStep:           0,
		ObservedTrafficWeight: 50,
	}
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	in := baseInputs(isvc, map[string]int32{"revC": 2, "revB": 3, "revA": 1})
	in.CanaryRevisionHash = "revC"
	in.StableRevisionHash = "revB" // the live-pod inference names B; persisted A must win
	res, _ := Reconcile(context.Background(), in)
	if !res.RolledBack {
		t.Fatalf("expected rollback, got %+v", res)
	}
	tr := isvc.Status.Components[v1beta1.EngineComponent].Traffic
	wantStable := coordination.PerRevisionServiceName("svc", v1beta1.EngineComponent, "revA")
	if len(tr) != 1 || tr[0].Percent != 100 || tr[0].RevisionName != wantStable {
		t.Fatalf("a restart must not lose the rollback target: want 100%% to %s, got %+v", wantStable, tr)
	}
}

// TestReconcile_CompleteClearsStableIdentity pins the identity lifecycle end:
// completion clears the persisted stable hash (the canary revision IS stable
// now), and a later canary re-armed from the done sentinel captures its own
// stable from what is serving at that point.
func TestReconcile_CompleteClearsStableIdentity(t *testing.T) {
	isvc := canaryISVC([]v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}, nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash: "revB",
		StableRevisionHash: "revA",
		CurrentStep:        0,
		StepEnteredTime:    &metav1.Time{Time: time.Unix(1000, 0)},
	}
	in := baseInputs(isvc, map[string]int32{"revB": 4})
	in.CanaryRevisionHash = "revB"
	in.StableRevisionHash = "" // A fully drained; nothing left to observe
	res, _ := Reconcile(context.Background(), in)
	if !res.Complete {
		t.Fatalf("expected completion, got %+v", res)
	}
	if isvc.Status.Canary.StableRevisionHash != "" {
		t.Fatalf("completion must clear the stable identity, got %q", isvc.Status.Canary.StableRevisionHash)
	}

	// A new canary toward C re-arms from the done sentinel: its stable is B
	// (what serves now), captured from the observed stable revision.
	in2 := baseInputs(isvc, map[string]int32{"revC": 0, "revB": 4})
	in2.CanaryRevisionHash = "revC"
	in2.StableRevisionHash = "revB"
	Reconcile(context.Background(), in2)
	cs := isvc.Status.Canary
	if cs.CanaryRevisionHash != "revC" || cs.StableRevisionHash != "revB" {
		t.Fatalf("re-armed canary must capture B as its stable, got %+v", cs)
	}
}

// TestReconcile_GlobalPauseFreezesStepMachine pins the observe-only contract of
// ome.io/rollout-paused=true: an elapsed timed pause must not advance the step,
// no traffic intent is written, the phase is untouched, and a pending promote
// annotation is neither honored nor consumed while paused.
func TestReconcile_GlobalPauseFreezesStepMachine(t *testing.T) {
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: 30 * time.Minute}}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	isvc := canaryISVC(steps, nil)
	t0 := time.Unix(100000, 0)
	entered := t0.Add(-40 * time.Minute) // timed pause elapsed long ago
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:    "new",
		CurrentStep:           0,
		ObservedTrafficWeight: 50,
		StepEnteredTime:       &metav1.Time{Time: entered},
	}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhasePaused)
	isvc.Annotations = map[string]string{
		constants.PausedRolloutAnnotation:  "true",
		constants.RolloutPromoteAnnotation: "new",
	}

	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.Now = t0
	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !res.Active {
		t.Fatalf("paused mid-flight canary must report Active, got %+v", res)
	}
	if isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("paused canary must not advance (timed pause elapsed, promote pending), step=%d", isvc.Status.Canary.CurrentStep)
	}
	if !isvc.Status.Canary.StepEnteredTime.Time.Equal(entered) {
		t.Fatalf("paused canary must not touch StepEnteredTime: was %v, now %v", entered, isvc.Status.Canary.StepEnteredTime.Time)
	}
	if len(isvc.Status.Components[v1beta1.EngineComponent].Traffic) != 0 {
		t.Fatal("paused canary must not write traffic intent")
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("paused canary must not change the phase, got %q", phaseOf(isvc))
	}
	if _, still := isvc.Annotations[constants.RolloutPromoteAnnotation]; !still {
		t.Fatal("paused canary must not consume the promote annotation")
	}
	if res.Partition != 2 { // step 0: 50% of 4 → 2 new, partition 2 (unchanged)
		t.Fatalf("paused canary must echo the current step's partition 2, got %d", res.Partition)
	}

	// Clearing the pause resumes the SAME step with its clocks intact: the timed
	// pause elapsed relative to the untouched StepEnteredTime, so the step
	// advances on the first unpaused pass.
	delete(isvc.Annotations, constants.PausedRolloutAnnotation)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if isvc.Status.Canary.CurrentStep != 1 {
		t.Fatalf("resume must continue from the frozen step with timers intact, step=%d", isvc.Status.Canary.CurrentStep)
	}
}

// TestReconcile_GlobalPauseDoesNotArmRollback pins that a rollback command
// arriving while globally paused is deferred, not executed: no rollback state
// is armed and the annotation survives to be honored after the pause clears.
func TestReconcile_GlobalPauseDoesNotArmRollback(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, ObservedTrafficWeight: 50}
	isvc.Annotations = map[string]string{
		constants.PausedRolloutAnnotation:   "true",
		constants.RolloutRollbackAnnotation: "true",
	}
	res, err := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 2, "old": 2}))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RolledBack || isvc.Status.Canary.RolledBackRevisionHash != "" {
		t.Fatalf("paused canary must not arm a rollback, got res=%+v status=%+v", res, isvc.Status.Canary)
	}
	if _, still := isvc.Annotations[constants.RolloutRollbackAnnotation]; !still {
		t.Fatal("paused canary must not consume the rollback annotation")
	}
}

// TestReconcile_GlobalPausePreservesRollbackHold pins the other rollback
// direction: a rollback already in progress is neither cleared nor re-armed
// while paused — the result keeps echoing RolledBack so the controller leaves
// the IR's RollbackToRevision signal in place, and a new target revision
// appearing mid-pause does not start a fresh canary.
func TestReconcile_GlobalPausePreservesRollbackHold(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", RolledBackRevisionHash: "new", CurrentStep: 0}
	isvc.Annotations = map[string]string{
		constants.PausedRolloutAnnotation:   "true",
		constants.RolloutRollbackAnnotation: "true",
	}
	in := baseInputs(isvc, map[string]int32{"old": 4})
	in.CanaryRevisionHash = "v3" // a new target that would normally re-arm
	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !res.RolledBack {
		t.Fatalf("paused rollback hold must keep echoing RolledBack, got %+v", res)
	}
	if isvc.Status.Canary.RolledBackRevisionHash != "new" || isvc.Status.Canary.CanaryRevisionHash != "new" {
		t.Fatalf("paused rollback hold must not re-arm toward the new target, got %+v", isvc.Status.Canary)
	}
	if _, still := isvc.Annotations[constants.RolloutRollbackAnnotation]; !still {
		t.Fatal("paused rollback hold must not consume the rollback annotation")
	}
	if len(isvc.Status.Components[v1beta1.EngineComponent].Traffic) != 0 {
		t.Fatal("paused rollback hold must not write traffic intent")
	}
}

// TestReconcile_GlobalPauseDoesNotStartCanary pins pause-before-start: a new
// target appearing while paused must not initialize the state machine (that
// would stamp step timers mid-pause); the canary starts once the pause clears.
func TestReconcile_GlobalPauseDoesNotStartCanary(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Annotations = map[string]string{constants.PausedRolloutAnnotation: "true"}
	res, err := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 0, "old": 4}))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.Active {
		t.Fatalf("paused not-started canary must stay inactive, got %+v", res)
	}
	if isvc.Status.Canary != nil {
		t.Fatal("paused canary must not initialize status")
	}
}

// TestReconcile_RollbackFalseOrGarbageIgnored pins the boolean contract of
// ome.io/rollout-rollback: only "true" requests a rollback. "false" is
// documented as identical to absence, and any other value (reachable via
// direct writes that bypass admission) must not roll back production.
func TestReconcile_RollbackFalseOrGarbageIgnored(t *testing.T) {
	for _, value := range []string{"false", "yes", ""} {
		t.Run("value="+value, func(t *testing.T) {
			isvc := canaryISVC(twoStep(), nil)
			isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, ObservedTrafficWeight: 50}
			isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: value}
			res, err := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 2, "old": 2}))
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if res.RolledBack || isvc.Status.Canary.RolledBackRevisionHash != "" {
				t.Fatalf("rollback=%q must be treated as absent, got res=%+v status=%+v", value, res, isvc.Status.Canary)
			}
			if phaseOf(isvc) != v1beta1.RolloutPhasePaused {
				t.Fatalf("canary should keep progressing normally (hold at the manual step), got %q", phaseOf(isvc))
			}
		})
	}
}

// TestReconcile_FinalStepManualPauseHoldsUntilPromote pins the final-step gate:
// a trailing bare Pause holds completion at 100% traffic until the operator
// promotes the exact canary hash, and the promote is consumed at completion.
func TestReconcile_FinalStepManualPauseHoldsUntilPromote(t *testing.T) {
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: &v1beta1.RolloutPause{}},
	}
	isvc := canaryISVC(steps, nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 1, StepEnteredTime: &metav1.Time{Time: time.Unix(1000, 0)}}

	in := baseInputs(isvc, map[string]int32{"new": 4})
	res, _ := Reconcile(context.Background(), in)
	if res.Complete || phaseOf(isvc) != v1beta1.RolloutPhasePromoting {
		t.Fatalf("manual final step must hold (Promoting) without a promote, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	if int(isvc.Status.Canary.CurrentStep) != 1 {
		t.Fatalf("held final step must not move, step=%d", isvc.Status.Canary.CurrentStep)
	}

	// A stale promote (wrong hash) must not complete the final step either.
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "someoldhash"}
	res, _ = Reconcile(context.Background(), in)
	if res.Complete {
		t.Fatalf("a stale promote must not complete the final step, got %+v", res)
	}

	// The matching promote completes, and is consumed at the completion edge.
	isvc.Annotations[constants.RolloutPromoteAnnotation] = "new"
	res, _ = Reconcile(context.Background(), in)
	if !res.Complete || phaseOf(isvc) != v1beta1.RolloutPhaseStable {
		t.Fatalf("matching promote must complete the final step, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	if int(isvc.Status.Canary.CurrentStep) != len(steps) {
		t.Fatalf("completion must mark the done sentinel, step=%d", isvc.Status.Canary.CurrentStep)
	}
	if _, still := isvc.Annotations[constants.RolloutPromoteAnnotation]; still {
		t.Fatal("the promote that opened the final gate must be consumed on completion")
	}
}

// TestReconcile_FinalStepTimedPauseWaits pins the timed variant of the
// final-step gate: a trailing Pause with a Duration holds completion for that
// duration, anchored to the moment 100% traffic shifts (Promoting entry).
func TestReconcile_FinalStepTimedPauseWaits(t *testing.T) {
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: 10 * time.Minute}}},
	}
	isvc := canaryISVC(steps, nil)
	// Entered the final step long ago; the timed gate anchors to the traffic
	// shift (the first capacity-met pass), not to step entry.
	t0 := time.Unix(100000, 0)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 1, StepEnteredTime: &metav1.Time{Time: t0.Add(-time.Hour)}}

	in := baseInputs(isvc, map[string]int32{"new": 4})
	in.Now = t0 // traffic shifts here → the 10m window starts now
	res, _ := Reconcile(context.Background(), in)
	if res.Complete || phaseOf(isvc) != v1beta1.RolloutPhasePromoting {
		t.Fatalf("timed final step must hold when the window starts, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	in.Now = t0.Add(5 * time.Minute)
	res, _ = Reconcile(context.Background(), in)
	if res.Complete {
		t.Fatalf("timed final step must hold within its window, got %+v", res)
	}
	in.Now = t0.Add(11 * time.Minute)
	res, _ = Reconcile(context.Background(), in)
	if !res.Complete || phaseOf(isvc) != v1beta1.RolloutPhaseStable {
		t.Fatalf("timed final step must complete after its window, got phase=%q res=%+v", phaseOf(isvc), res)
	}
}

// TestReconcile_CapacityGateFailedIsSticky pins the same parked-terminal contract
// for the capacity-gate Failed path: a step whose canary pods never become Ready
// escalates to Failed after the ready-timeout and then STAYS Failed. While held
// Failed the hold returns before the Pending anchor, so StepEnteredTime is never
// re-stamped — this guards the sticky hold (same slow heartbeat, no re-stamp, no
// re-arm) rather than regressing it.
func TestReconcile_CapacityGateFailedIsSticky(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, StepEnteredTime: &metav1.Time{Time: time.Unix(1000, 0)}}
	in := baseInputs(isvc, map[string]int32{"new": 0}) // canary pods never come up

	in.Now = time.Unix(1000, 0) // enter the capacity wait (anchors the timeout)
	Reconcile(context.Background(), in)
	in.Now = time.Unix(1000, 0).Add(16 * time.Minute) // past the 15m default
	Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("capacity gate past the ready-timeout → Failed, got %q", phaseOf(isvc))
	}
	enteredAtFailure := isvc.Status.Canary.StepEnteredTime.Time

	// Later reconcile, same target, pods still down: must stay Failed, not flip to
	// Pending, and must not re-stamp the entry time.
	in.Now = time.Unix(1000, 0).Add(40 * time.Minute)
	res, _ := Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("capacity-gate Failed must be sticky, not oscillate to %q", phaseOf(isvc))
	}
	if res.RequeueAfter != failedRequeue {
		t.Fatalf("held Failed should keep the slow heartbeat %v, got %v", failedRequeue, res.RequeueAfter)
	}
	if !isvc.Status.Canary.StepEnteredTime.Time.Equal(enteredAtFailure) {
		t.Fatalf("StepEnteredTime must not be re-stamped while held Failed: was %v, now %v", enteredAtFailure, isvc.Status.Canary.StepEnteredTime.Time)
	}
}

// TestReconcile_NoRunNoStart pins the run license: a diverged target with no
// pinned rollout run must not initialize the step machine — opening a run is
// the run layer's job, and starting here would execute an unpinned plan.
func TestReconcile_NoRunNoStart(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2}) // target diverged, not converged
	in.RunActive = false
	res, err := Reconcile(context.Background(), in)
	if err != nil || res == nil || res.Active {
		t.Fatalf("no active run must hold inactive: res=%+v err=%v", res, err)
	}
	if isvc.Status.Canary != nil {
		t.Fatalf("no active run must not initialize CanaryStatus, got %+v", isvc.Status.Canary)
	}
}

// TestReconcile_RetargetRequiresRun pins the re-arm license: a new target hash
// resets the state machine only under a pinned run (the run layer closes and
// reopens the run on a retarget in the same pass); without one the persisted
// state holds as-is.
func TestReconcile_RetargetRequiresRun(t *testing.T) {
	mk := func() *v1beta1.InferenceService {
		isvc := canaryISVC(twoStep(), nil)
		isvc.Status.Canary = &v1beta1.CanaryStatus{
			CanaryRevisionHash: "new",
			StableRevisionHash: "old",
			CurrentStep:        1,
			StepEnteredTime:    &metav1.Time{Time: time.Unix(900, 0)},
		}
		return isvc
	}

	held := mk()
	in := baseInputs(held, map[string]int32{"v3": 0, "new": 2, "old": 2})
	in.CanaryRevisionHash = "v3"
	in.RunActive = false
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if cs := held.Status.Canary; cs.CanaryRevisionHash != "new" || cs.CurrentStep != 1 {
		t.Fatalf("retarget without a run must not reset the canary, got %+v", cs)
	}

	reset := mk()
	in = baseInputs(reset, map[string]int32{"v3": 0, "new": 2, "old": 2})
	in.CanaryRevisionHash = "v3"
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if cs := reset.Status.Canary; cs.CanaryRevisionHash != "v3" || cs.CurrentStep != 0 {
		t.Fatalf("retarget under a run must re-arm at step 0 toward v3, got %+v", cs)
	}
}

// TestReconcile_PreStepHoldBlocksTrafficUntilPromote pins the pre-step hold: a
// repin-clamped step keeps capacity converging but does not raise traffic until
// an explicit promote (value = the canary revision hash) releases it.
func TestReconcile_PreStepHoldBlocksTrafficUntilPromote(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash: "new",
		StableRevisionHash: "old",
		CurrentStep:        0,
		PreStepHold:        true,
		StepEnteredTime:    &metav1.Time{Time: time.Unix(1000, 0)},
	}
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2}) // step-0 capacity satisfied

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("pre-step hold must pause, got %q", phaseOf(isvc))
	}
	if tr := isvc.Status.Components[v1beta1.EngineComponent].Traffic; len(tr) != 0 {
		t.Fatalf("pre-step hold must not apply traffic, got %+v", tr)
	}
	if !res.Active || !isvc.Status.Canary.PreStepHold {
		t.Fatalf("hold must stay armed until promoted, got res=%+v cs=%+v", res, isvc.Status.Canary)
	}

	// A promote naming a different revision does not release the hold.
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "someotherhash"}
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !isvc.Status.Canary.PreStepHold || len(isvc.Status.Components[v1beta1.EngineComponent].Traffic) != 0 {
		t.Fatalf("mismatched promote must keep the hold, got %+v", isvc.Status.Canary)
	}

	// Promote with the canary hash: hold clears, the promote is consumed by the
	// release (so it cannot also advance the step), and traffic applies.
	isvc.Annotations[constants.RolloutPromoteAnnotation] = "new"
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("promoting Reconcile: %v", err)
	}
	if isvc.Status.Canary.PreStepHold {
		t.Fatal("promote with the canary hash must clear the hold")
	}
	if _, ok := isvc.Annotations[constants.RolloutPromoteAnnotation]; ok {
		t.Fatal("the releasing promote must be consumed")
	}
	if tr := isvc.Status.Components[v1beta1.EngineComponent].Traffic; len(tr) != 2 {
		t.Fatalf("released hold must apply the step's split, got %+v", tr)
	}
	if isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("the consumed promote must not double as a step advance, got step %d", isvc.Status.Canary.CurrentStep)
	}
}

// TestReconcile_PinnedPlanInertToSpecEdits pins the run-model inertness
// contract: while a run is open the engine indexes the PINNED steps, so a
// mid-run spec edit changes nothing until the next run.
func TestReconcile_PinnedPlanInertToSpecEdits(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	pinActiveRun(isvc)
	// Mid-run spec edit: shrink step 0 to 25% capacity / 10% traffic.
	isvc.Spec.Rollout.Groups[0].Canary.Steps = []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("25%"), Traffic: 10, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}

	res, err := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 2, "old": 2}))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// Pinned step 0 is 50% of 4 → newCount 2, partition 2. The edited spec
	// (25% → newCount 1) would report partition 3.
	if res.Partition != 2 {
		t.Fatalf("engine must index the pinned steps (partition 2), got %d", res.Partition)
	}
	var canaryPercent int32 = -1
	for _, tr := range isvc.Status.Components[v1beta1.EngineComponent].Traffic {
		if tr.RevisionName == coordination.PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "new") {
			canaryPercent = tr.Percent
		}
	}
	if canaryPercent != 50 {
		t.Fatalf("traffic must follow the pinned step (50), got %d", canaryPercent)
	}
	if p, ok := EffectivePartition(isvc, v1beta1.EngineComponent, 4); !ok || p == nil || *p != 2 {
		t.Fatalf("EffectivePartition must read the pinned plan, got %v ok=%v", p, ok)
	}
}

// ---------------------------------------------------------------------------
// Idle units. A run is opened for the whole ISVC, so a unit that did NOT
// retarget is still handed a TargetID. It must not arm on that alone.
// ---------------------------------------------------------------------------

// runWithTargets pins the spec as an active run (so the executor resolves a
// group at all) and records exactly the TargetRevisions given.
func runWithTargets(isvc *v1beta1.InferenceService, targets ...v1beta1.RolloutRunTarget) {
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.RunID = "run-1"
	isvc.Status.Rollout.ActiveRun.TargetRevisions = targets
}

// The bug: a router-only bounce opens a run, the engine unit is handed that
// run's TargetID, and it armed a full no-op ladder on a revision it already
// serves -- burning its analysis budget and able to reach a rollback for a
// rollout that never happened.
func TestReconcile_IdleUnitDoesNotArm(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	runWithTargets(isvc,
		v1beta1.RolloutRunTarget{Component: v1beta1.RouterComponent, Revision: "rnew", StableRevision: "rold"},
		v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "same", StableRevision: "same"},
	)
	in := baseInputs(isvc, map[string]int32{"same": 4})
	in.CanaryRevisionHash = "same"
	in.StableRevisionHash = "same"
	in.TargetID = "target-from-the-router-run"

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if res.Active {
		t.Fatal("the engine unit did not retarget; it must sit the run out")
	}
	if isvc.Status.Canary != nil {
		t.Fatalf("no canary status should be written for an idle unit, got %+v", isvc.Status.Canary)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseStable {
		t.Fatalf("an idle converged unit should read Stable, got %q", phaseOf(isvc))
	}
}

// The unit that DID retarget must still arm, in the same run.
func TestReconcile_RetargetedUnitStillArms(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	runWithTargets(isvc,
		v1beta1.RolloutRunTarget{Component: v1beta1.RouterComponent, Revision: "rsame", StableRevision: "rsame"},
		v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
	)
	in := baseInputs(isvc, map[string]int32{"new": 0, "old": 4})
	in.TargetID = "engine-target"

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Active {
		t.Fatal("the engine retargeted; its canary must arm")
	}
	if isvc.Status.Canary == nil {
		t.Fatal("expected canary status for the retargeted unit")
	}
}

// Per-UNIT, not per-Component: a group whose primary is unchanged but whose
// secondary retargeted has real work. Guarding on the primary's hashes alone
// would skip it (regression guard for TestDispatch_SecondaryOnlyTargetStartsCanary).
func TestReconcile_SecondaryOnlyRetargetArms(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	runWithTargets(isvc,
		v1beta1.RolloutRunTarget{Component: v1beta1.RouterComponent, Revision: "same", StableRevision: "same"},
		v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "e-new", StableRevision: "e-old"},
	)
	in := baseInputs(isvc, map[string]int32{"same": 4})
	in.Component = v1beta1.RouterComponent
	in.CanaryRevisionHash = "same"
	in.StableRevisionHash = "same"
	in.TargetID = "pair-target"
	in.Group = &v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent},
	}

	if !unitRetargeted(in) {
		t.Fatal("a unit whose secondary retargeted must count as retargeted")
	}
}

// Missing information must never suppress a rollout.
func TestUnitRetargeted_ConservativeWhenUnknown(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	in := baseInputs(isvc, nil)

	if !unitRetargeted(in) {
		t.Error("no active run: must not suppress")
	}
	runWithTargets(isvc)
	if !unitRetargeted(in) {
		t.Error("run with no recorded targets: must not suppress")
	}
	runWithTargets(isvc, v1beta1.RolloutRunTarget{Component: v1beta1.DecoderComponent, Revision: "a", StableRevision: "a"})
	if !unitRetargeted(in) {
		t.Error("run recording no member of this unit: must not suppress")
	}
}
