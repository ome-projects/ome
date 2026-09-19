package inferencereplica

import (
	"context"
	"strconv"
	"testing"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
)

// pinRun pins the ISVC's current spec.rollout as its active run: the update
// gates fail closed for grouped Components without one, so every gate fixture
// models the production state where the run layer has already pinned.
func pinRun(isvc *v1beta1.InferenceService) *v1beta1.InferenceService {
	if isvc.Spec.Rollout == nil {
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

// mkSequentialParent returns an ISVC named "llama" (the parent every
// baselineIR points at) declaring a Sequential rollout over [decoder,
// engine] in that order. In the v2 rollout API Sequential is spelled as a run
// of single-Component blueGreen groups, in list order; the controller's
// collapseSequential folds them back into one Sequential group whose Order
// is the list order (decoder→engine). Generation is 2; callers stamp
// each IR's parent-generation annotation and status to model in-flight
// vs converged (stamp ≠ 2 ⇒ projection lag, the moment-of-bump signal
// observeSequentialComponentsForGate keys on).
func mkSequentialParent() *v1beta1.InferenceService {
	return pinRun(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "default", Generation: 2},
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
	})
}

// mkIR creates an InferenceReplica for the given component of parent "llama".
// Callers stamp status fields to set up test scenarios.
func mkIR(c v1beta1.ComponentType, replicas int32) *v1beta1.InferenceReplica {
	return baselineIR("llama-"+string(c), "default", replicas)
}

// setParentGenStamp models the projector's parent-generation annotation
// on a fake IR — the stamp the Sequential gate's projection-lag signal
// reads.
func setParentGenStamp(ir *v1beta1.InferenceReplica, gen int64) {
	if ir.Annotations == nil {
		ir.Annotations = map[string]string{}
	}
	ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = strconv.FormatInt(gen, 10)
}

func setObservedGen(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, gen int64) {
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

// TestBuildReconcileInput_WiresSequentialGate pins that the IR-managed
// path wires ReconcileInput.UpdateGate: with the gate nil the dispatcher
// skips CheckSequential entirely and both Components of a Sequential
// group roll concurrently (the engine starts before the decoder
// finishes). The gate must be wired AND deny the engine while the
// decoder (first in Order) is in flight.
func TestBuildReconcileInput_WiresSequentialGate(t *testing.T) {
	g := gomega.NewWithT(t)
	// Moment-of-bump: both Components' parent-generation stamps lag
	// isvc.Generation=2 (the projector hasn't re-applied them yet) ⇒
	// both in flight. The active selector picks decoder (first in
	// Order); the engine must wait.
	decoderIR := mkIR(v1beta1.DecoderComponent, 1)
	decoderIR.Generation = 1
	decoderIR.Status.ObservedGeneration = 1
	setParentGenStamp(decoderIR, 1)
	engineIR := mkIR(v1beta1.EngineComponent, 1)
	engineIR.Generation = 1
	engineIR.Status.ObservedGeneration = 1
	setParentGenStamp(engineIR, 1)
	r, _ := newReconciler(t, decoderIR, engineIR)

	parent := mkSequentialParent()

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil, nil, 0, 0, coordination.GroupDefaults{})
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil(),
		"UpdateGate must be wired on the IR path when the parent declares coordination")

	allowed, gate, reason := input.UpdateGate(workload.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeFalse(),
		"engine must be denied while decoder (first in Sequential Order) is in flight")
	g.Expect(reason).To(gomega.ContainSubstring("Sequential waiting on decoder"))
	g.Expect(gate).To(gomega.Equal(workload.RolloutHoldGateSequential),
		"a Sequential denial must report gate=Sequential so the RolloutHold surface names the right layer")
}

// TestBuildReconcileInput_SequentialReleasesActiveComponent proves the
// gate is not a blanket block: once the decoder has converged, the engine
// becomes the active Sequential Component and is allowed to roll.
func TestBuildReconcileInput_SequentialReleasesActiveComponent(t *testing.T) {
	g := gomega.NewWithT(t)
	// Decoder converged: its parent-generation stamp matches parent
	// Generation=2 and its status has caught up to its own IR generation
	// ⇒ not in flight. Engine's projected spec just changed (IR
	// generation 2, status still at 1) ⇒ it is the active Sequential
	// Component. Seed BOTH IRs so the gate reads their fresh status
	// (the group is not idle — engine is genuinely in flight — so this exercises
	// the active-component release path, not the "Sequential group idle" bypass).
	decoderIR := mkIR(v1beta1.DecoderComponent, 1)
	decoderIR.Generation = 1
	decoderIR.Status.ObservedGeneration = 1
	setParentGenStamp(decoderIR, 2)
	engineIR := mkIR(v1beta1.EngineComponent, 1)
	engineIR.Generation = 2
	engineIR.Status.ObservedGeneration = 1
	setParentGenStamp(engineIR, 2)
	r, _ := newReconciler(t, decoderIR, engineIR)

	parent := mkSequentialParent()

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil, nil, 0, 0, coordination.GroupDefaults{})
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, _, _ := input.UpdateGate(workload.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeTrue(),
		"engine is the active Sequential Component once decoder converged; it must be allowed")
}

// TestBuildReconcileInput_NilParentLeavesGateNil pins that without a
// resolvable parent there is no RolloutCoordination block to enforce, so
// the gate stays nil and the dispatcher's "always allowed" fallback
// applies (matching the documented EventTarget fallback behavior).
func TestBuildReconcileInput_NilParentLeavesGateNil(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	engineIR := baselineIR("llama-engine", "default", 1)

	input := r.buildReconcileInput(context.Background(), engineIR, nil, nil, nil, 0, 0, coordination.GroupDefaults{})
	g.Expect(input.UpdateGate).To(gomega.BeNil(),
		"no parent ⇒ no coordination to enforce ⇒ gate stays nil")
}

// TestBuildReconcileInput_WiresMigrationWhenParentSet pins Layer 2: the
// IR-managed path always wires MutateMigration (the executor's
// status.migrations write-back seam) and mirrors the persisted records
// onto ObservedState.Migrations (the dispatcher's work source); with a
// resolvable parent it points the migration audit ledger at the parent
// ISVC via LedgerOwner, while the IR still owns the pods.
func TestBuildReconcileInput_WiresMigrationWhenParentSet(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	engineIR := baselineIR("llama-engine", "default", 1)
	engineIR.Status.Migrations = []v1beta1.MigrationStatus{{
		RequestUUID: "u-1", Trigger: v1beta1.MigrationTriggerManual,
		Phase: v1beta1.MigrationPhaseAccepted, SourceInstance: 0,
	}}

	parent := mkSequentialParent() // any non-nil ISVC named "llama"
	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil, nil, 0, 0, coordination.GroupDefaults{})

	g.Expect(input.MutateMigration).NotTo(gomega.BeNil(),
		"MutateMigration must be wired on the IR path")
	g.Expect(input.AppendMigration).NotTo(gomega.BeNil(),
		"AppendMigration must be wired on the IR path (the disposition's Auto mirror)")
	g.Expect(input.ObservedState.Migrations).To(gomega.HaveLen(1),
		"status.migrations must mirror onto ObservedState.Migrations")
	g.Expect(input.ObservedState.Migrations[0].RequestUUID).To(gomega.Equal("u-1"))
	g.Expect(input.LedgerOwner).NotTo(gomega.BeNil(),
		"the migration ledger must be owned by the parent ISVC, not the IR")
	g.Expect(input.LedgerOwner.GetName()).To(gomega.Equal(parent.Name))
	g.Expect(input.LedgerOwnerGVK).To(gomega.Equal(isvcGVK))

	// Nil parent ⇒ ledger falls back to the IR (workload-side owner
	// resolution); the migration seam stays wired.
	nilInput := r.buildReconcileInput(context.Background(), engineIR, nil, nil, nil, 0, 0, coordination.GroupDefaults{})
	g.Expect(nilInput.MutateMigration).NotTo(gomega.BeNil())
	g.Expect(nilInput.AppendMigration).NotTo(gomega.BeNil())
	g.Expect(nilInput.LedgerOwner).To(gomega.BeNil())
}

// mkRatioParent returns an ISVC named "llama" declaring a RatioBalanced
// (MaintainRatio) rollingUpdate group over [engine, decoder] anchored at a
// symmetric 4:4 original ratio. The per-Component projected Status reflects
// whatever the caller stamps — modeling the lagged irprojector rollup the
// gate reads for PEER Components.
func mkRatioParent(tol int32, engServing, decServing int32) *v1beta1.InferenceService {
	return pinRun(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "default", Generation: 1},
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: &tol},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyRollingUpdate,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  4,
							v1beta1.DecoderComponent: 4,
						},
					},
				}},
			},
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent:  {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: engServing}},
				v1beta1.DecoderComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: decServing}},
			},
		},
	})
}

// peerIR returns a sibling IR (e.g. the decoder) with the given fresh
// serving count stamped on its status — the authoritative count the
// gate should read for a PEER Component, not the parent's lagged
// projection.
func peerIR(component v1beta1.ComponentType, replicas, serving int32) *v1beta1.InferenceReplica {
	engineIR := baselineIR("llama-"+string(component), "default", replicas)
	engineIR.Spec.Component = component
	engineIR.Status.Replicas = replicas
	engineIR.Status.ServingReplicas = serving
	return engineIR
}

// TestBuildReconcileInput_GateReadsFreshPeerStatus is the regression guard
// for the cross-Component coordination stale-status race. The gate the IR
// path wires reads the GATED Component's counts from the IR's own fresh
// status, but it must ALSO read every PEER Component's counts from that
// peer's fresh IR status — not the parent ISVC's lagged projection.
//
// Scenario (RatioBalanced 4:4, tol 25% → band [0.75, 1.25]): the engine is
// at full serving (4/4) and asks to surge one new-revision pod. The decoder
// is actually mid-roll with one pod already out of rotation (fresh decoder
// IR: serving 3/4), but the parent ISVC's projected status still reports the
// decoder fully serving (4/4) because the irprojector rollup lags.
//
//   - Reading the STALE projected decoder (4/4): engine surge projects
//     5/4 = 1.25, inside the strict band → engine ALLOWED to run ahead.
//   - Reading the FRESH decoder (3/4): engine surge projects 5/3 = 1.667,
//     past the band; the surge tiebreaker also refuses (the baseline 4/3 is
//     already out of band) → engine correctly DENIED until the decoder
//     catches up.
//
// A gate reading the stale projection would let the engine outrun the
// decoder past the RatioBalanced tolerance; the gate therefore overlays
// each peer's fresh IR status onto its view, so the engine is held.
func TestBuildReconcileInput_GateReadsFreshPeerStatus(t *testing.T) {
	g := gomega.NewWithT(t)

	// Decoder is genuinely behind: fresh IR serving 3/4 (one pod out).
	decoder := peerIR(v1beta1.DecoderComponent, 4, 3)

	// Engine IR being reconciled: at full serving 4/4, about to surge. Seed it
	// into the client too, so the gate reads the engine's OWN fresh status (not
	// nil) and the denial is genuinely driven by the decoder's fresh 3/4.
	engineIR := baselineIR("llama-engine", "default", 4)
	engineIR.Status.Replicas = 4
	engineIR.Status.ServingReplicas = 4
	r, _ := newReconciler(t, decoder, engineIR)

	// Parent projection is STALE: it still reports the decoder fully
	// serving (4/4), which is the lagged irprojector rollup.
	parent := mkRatioParent(25, 4, 4)

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil, nil, 0, 0, coordination.GroupDefaults{})
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, gate, reason := input.UpdateGate(workload.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeFalse(),
		"engine must be held while the decoder is genuinely behind (fresh decoder serving 3/4 → "+
			"engine surge 5/3 = 1.667 out of band); got allowed — the gate read the stale parent "+
			"projection (decoder 4/4) instead of the decoder's fresh IR status: "+reason)
	g.Expect(gate).To(gomega.Equal(workload.RolloutHoldGateRatio),
		"a RatioBalanced denial must report gate=Ratio so the RolloutHold surface names the right layer")
}

// TestBuildReconcileInput_GateFreshPeerReleasesWhenBalanced is the GREEN
// companion: when the decoder's fresh IR status shows it back in balance
// (4/4), the same engine surge projects 5/4 = 1.25 (in band) and must be
// ALLOWED. This pins that the peer-freshness overlay does not turn into a
// blanket block — it tracks the peer's true position both ways.
func TestBuildReconcileInput_GateFreshPeerReleasesWhenBalanced(t *testing.T) {
	g := gomega.NewWithT(t)

	// Decoder is caught up: fresh IR serving 4/4.
	decoder := peerIR(v1beta1.DecoderComponent, 4, 4)
	engineIR := baselineIR("llama-engine", "default", 4)
	engineIR.Status.Replicas = 4
	engineIR.Status.ServingReplicas = 4
	r, _ := newReconciler(t, decoder, engineIR)

	// Parent projection is irrelevant here (stamp it stale-low to prove the
	// gate prefers the fresh peer IR): decoder projected at 3/4.
	parent := mkRatioParent(25, 4, 3)

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil, nil, 0, 0, coordination.GroupDefaults{})
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, _, reason := input.UpdateGate(workload.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeTrue(),
		"engine surge must be allowed once the decoder's FRESH status is balanced (4/4 → engine 5/4 = "+
			"1.25 in band), even though the parent projection lags at 3/4: "+reason)
}

// TestBuildReconcileInput_RatioRecoveryStartsFromAuthoritativeZero verifies
// that authoritative positive desired state at zero serving admits one
// recovery surge and serializes a second same-Component start in the wake-up.
func TestBuildReconcileInput_RatioRecoveryStartsFromAuthoritativeZero(t *testing.T) {
	g := gomega.NewWithT(t)
	engineIR := peerIR(v1beta1.EngineComponent, 4, 0)
	decoderIR := peerIR(v1beta1.DecoderComponent, 4, 0)
	for i := int32(0); i < 4; i++ {
		engineIR.Status.InstanceStatuses = append(engineIR.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
			Index: i,
			Phase: v1beta1.OMENativeInstanceFailed,
		})
		decoderIR.Status.InstanceStatuses = append(decoderIR.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
			Index: i,
			Phase: v1beta1.OMENativeInstanceFailed,
		})
	}
	r, _ := newReconciler(t, engineIR, decoderIR)

	parent := mkRatioParent(25, 4, 1)
	maxSurge := intstr.FromInt32(4)
	parent.Spec.Rollout.Groups[0].RollingUpdate.MaxSurge = &maxSurge
	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil, nil, 0, 0, coordination.GroupDefaults{})
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, _, reason := input.UpdateGate(workload.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeTrue(),
		"positive authoritative shape at 0:0 serving must admit one recovery bootstrap: "+reason)

	allowed, _, reason = input.UpdateGate(workload.UpdateStrategySurgeThenDrain, 1, 0)
	g.Expect(allowed).To(gomega.BeFalse(),
		"same-wakeup recovery must serialize per Component even when MaxSurge permits more: "+reason)
}

// TestBuildReconcileInput_SequentialGateReadsFreshPeerStatus is the
// Sequential analogue of the RatioBalanced peer-freshness guard. The
// decoder is first in Order; the engine must not start until the decoder
// finishes. Here the parent ISVC's projected status reports the decoder
// fully CONVERGED (UpdateRevision == CurrentRevision, ObservedGeneration
// caught up) because the irprojector rollup lags, but the decoder's OWN
// fresh IR status shows it still mid-rollout (UpdateRevision !=
// CurrentRevision).
//
//   - Reading the STALE projection: the decoder looks done, no Component
//     is in flight, the gate reports "Sequential group idle" → the engine
//     starts EARLY (before the decoder finishes).
//   - Reading the decoder's FRESH IR status: the decoder is in flight and
//     is the active Sequential Component → the engine is correctly DENIED.
func TestBuildReconcileInput_SequentialGateReadsFreshPeerStatus(t *testing.T) {
	g := gomega.NewWithT(t)

	// Fresh decoder IR: revision skew (v2 target, v1 current) ⇒ still
	// rolling, even though the parent projection will say it's done.
	decoder := baselineIR("llama-decoder", "default", 1)
	decoder.Spec.Component = v1beta1.DecoderComponent
	decoder.Status.ObservedGeneration = 2
	decoder.Status.CurrentRevision = "llama-decoder-v1"
	decoder.Status.UpdateRevision = "llama-decoder-v2"
	r, _ := newReconciler(t, decoder)

	engineIR := baselineIR("llama-engine", "default", 1) // Component=engine

	parent := mkSequentialParent() // Generation=2, Order=[decoder, engine]
	// STALE projection: both Components look converged (decoder "done").
	setObservedGen(parent, v1beta1.DecoderComponent, 2)
	setObservedGen(parent, v1beta1.EngineComponent, 2)

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil, nil, 0, 0, coordination.GroupDefaults{})
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, _, reason := input.UpdateGate(workload.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeFalse(),
		"engine must wait while the decoder is genuinely still rolling (fresh decoder IR has "+
			"UpdateRevision != CurrentRevision); got allowed — the gate read the stale parent "+
			"projection (decoder converged) instead of the decoder's fresh IR status: "+reason)
	g.Expect(reason).To(gomega.ContainSubstring("Sequential waiting on decoder"))
}

// TestBuildReconcileInput_ThreadsGangSchedulingAvailable pins that
// buildReconcileInput threads the controller's GangSchedulingAvailable flag
// into DesiredSpec, because EnsurePodGroups gates per-Instance PodGroup
// creation on it. Without it, IR-managed multi-node pods render the gang
// reference but no PodGroup object is ever created, so the gang stays
// Pending forever ("PodGroup not found").
func TestBuildReconcileInput_ThreadsGangSchedulingAvailable(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	engineIR := baselineIR("llama-engine", "default", 1)

	r.GangSchedulingAvailable = true
	g.Expect(r.buildReconcileInput(context.Background(), engineIR, nil, nil, nil, 0, 0, coordination.GroupDefaults{}).DesiredSpec.GangSchedulingAvailable).To(gomega.BeTrue(),
		"DesiredSpec.GangSchedulingAvailable must follow the controller flag (true) so EnsurePodGroups runs")

	r.GangSchedulingAvailable = false
	g.Expect(r.buildReconcileInput(context.Background(), engineIR, nil, nil, nil, 0, 0, coordination.GroupDefaults{}).DesiredSpec.GangSchedulingAvailable).To(gomega.BeFalse(),
		"flag false ⇒ DesiredSpec false ⇒ EnsurePodGroups skips (CRD absent / degradation surface)")
}

func TestBuildReconcileInput_ParentPauseAnnotationIsAuthoritative(t *testing.T) {
	tests := []struct {
		name        string
		irPaused    bool
		irPauseMode v1beta1.PauseMode
		parent      *v1beta1.InferenceService
		wantPaused  bool
		wantFreeze  bool
	}{
		{
			name:       "readable parent pause overrides stale false IR projection",
			parent:     &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.PausedRolloutAnnotation: "true"}}},
			wantPaused: true,
		},
		{
			name:       "readable parent removal overrides stale true IR projection",
			irPaused:   true,
			parent:     &v1beta1.InferenceService{},
			wantPaused: false,
		},
		{
			name:       "unreadable parent falls back to projected IR value",
			irPaused:   true,
			wantPaused: true,
		},
		{
			name:       "readable parent freeze value sets both pause depths",
			parent:     &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.PausedRolloutAnnotation: constants.PausedRolloutFreezeValue}}},
			wantPaused: true,
			wantFreeze: true,
		},
		{
			name:        "readable parent plain pause overrides stale freeze IR projection",
			irPaused:    true,
			irPauseMode: v1beta1.PauseModeFreeze,
			parent:      &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.PausedRolloutAnnotation: "true"}}},
			wantPaused:  true,
			wantFreeze:  false,
		},
		{
			name:        "unreadable parent falls back to projected freeze",
			irPaused:    true,
			irPauseMode: v1beta1.PauseModeFreeze,
			wantPaused:  true,
			wantFreeze:  true,
		},
		{
			name:       "unknown parent annotation value is not paused",
			irPaused:   true,
			parent:     &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.PausedRolloutAnnotation: "True"}}},
			wantPaused: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := newReconciler(t)
			engineIR := baselineIR("llama-engine", "default", 1)
			engineIR.Spec.Paused = tc.irPaused
			engineIR.Spec.PauseMode = tc.irPauseMode
			desired := r.buildReconcileInput(context.Background(), engineIR, tc.parent, nil, nil, 0, 0, coordination.GroupDefaults{}).DesiredSpec
			if desired.Paused != tc.wantPaused {
				t.Fatalf("DesiredSpec.Paused: got %v want %v", desired.Paused, tc.wantPaused)
			}
			if desired.PauseFreeze != tc.wantFreeze {
				t.Fatalf("DesiredSpec.PauseFreeze: got %v want %v", desired.PauseFreeze, tc.wantFreeze)
			}
		})
	}
}

// TestBuildReconcileInput_GateUsesFreshIRStatus pins the IR-path
// gate-staleness guard: the gate must read the GATED Component's
// counts from the IR's OWN fresh status, not the parent ISVC's lagged
// projection. Here the IR's fresh status shows the engine already one pod
// down (serving 3/4); a RatioBalanced drain (RecreatePod → -1) must be
// DENIED — the tiebreaker bounds in-flight to one pod — even though the
// parent's projected status still reports a full 4/4 (which, if the gate
// read it, would let the tiebreaker fire again and over-drain).
func TestBuildReconcileInput_GateUsesFreshIRStatus(t *testing.T) {
	g := gomega.NewWithT(t)
	engineIR := baselineIR("llama-engine", "default", 4) // engine, parent llama
	engineIR.Status.Replicas = 4
	engineIR.Status.ServingReplicas = 3 // FRESH: one engine pod already out of rotation
	// Peer decoder is balanced at 4/4 (fresh); only the engine is down. Seed
	// both so the gate reads the engine's OWN fresh 3/4 (not nil) — the drain
	// denial must be driven by that, not by the engine being absent.
	decoderIR := peerIR(v1beta1.DecoderComponent, 4, 4)
	r, _ := newReconciler(t, engineIR, decoderIR)

	tol := int32(25)
	parent := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "default", Generation: 1},
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					// RatioBalanced pacing is spelled as MaintainRatio in v2;
					// tolerance 25 preserves the original RatioTolerancePercent.
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: &tol},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyRollingUpdate,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  4,
							v1beta1.DecoderComponent: 4,
						},
					},
				}},
			},
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				// STALE projection: still reports engine fully serving (4/4).
				v1beta1.EngineComponent:  {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 4}},
				v1beta1.DecoderComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 4}},
			},
		},
	}
	pinRun(parent)

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil, nil, 0, 0, coordination.GroupDefaults{})
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, _, reason := input.UpdateGate(workload.UpdateStrategyRecreatePod, 0, 0)
	g.Expect(allowed).To(gomega.BeFalse(),
		"with the IR's fresh status (engine serving 3/4 = one pod already out), the "+
			"RatioBalanced tiebreaker must refuse a second drain; got allowed — the gate "+
			"read the stale parent projection (4/4) instead of the IR's fresh status: "+reason)
}
