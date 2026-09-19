package coordination

import (
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// helper: build an idle observation set (no rollout in flight).
func idleObs(g ResolvedGroup) GroupObservation {
	obs := GroupObservation{
		Group:      g,
		Components: map[v1beta1.ComponentType]ComponentObservation{},
	}
	for _, c := range g.Components {
		obs.Components[c] = ComponentObservation{
			Component:            c,
			DesiredReplicas:      2,
			TotalPods:            2,
			ReadyPods:            2,
			NewRevisionPods:      2,
			NewRevisionReadyPods: 2,
			TargetRevisionHash:   "rev1",
			CurrentRevisionHash:  "rev1",
			RolloutInFlight:      false,
		}
	}
	return obs
}

func TestComputeTransition_PausedGlobal(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
	obs := idleObs(g)
	obs.PausedGlobal = true
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhasePaused {
		t.Errorf("global pause: phase got %q want Paused", tr.Phase)
	}
}

func TestComputeTransition_IndependentIdle(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyIndependent}
	tr := ComputeTransition(idleObs(g))
	if tr.Phase != v1beta1.CoordinationPhaseIdle {
		t.Errorf("idle: got %q want Idle", tr.Phase)
	}
}

func TestComputeTransition_IndependentInFlight(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyIndependent}
	obs := idleObs(g)
	c := obs.Components[v1beta1.EngineComponent]
	c.RolloutInFlight = true
	obs.Components[v1beta1.EngineComponent] = c
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseShifting {
		t.Errorf("Independent in-flight: got %q want Shifting", tr.Phase)
	}
}

func TestComputeTransition_IndependentFailed(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyIndependent}
	obs := idleObs(g)
	c := obs.Components[v1beta1.EngineComponent]
	c.Failed = true
	obs.Components[v1beta1.EngineComponent] = c
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseFailed {
		t.Errorf("Independent failed: got %q want Failed", tr.Phase)
	}
}

func TestComputeTransition_BlueGreenSurging(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
	obs := idleObs(g)
	// One Component is rolling but has zero new pods (not surged yet).
	engine := obs.Components[v1beta1.EngineComponent]
	engine.RolloutInFlight = true
	engine.NewRevisionPods = 0
	engine.NewRevisionReadyPods = 0
	engine.TargetRevisionHash = "rev2"
	obs.Components[v1beta1.EngineComponent] = engine
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseSurging {
		t.Errorf("BlueGreen surging: got %q want Surging", tr.Phase)
	}
}

func TestComputeTransition_BlueGreenWaiting(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
	obs := idleObs(g)
	// Both rolling, both have surge pods, but engine's new pods not Ready.
	for _, c := range g.Components {
		comp := obs.Components[c]
		comp.RolloutInFlight = true
		comp.NewRevisionPods = 2
		comp.NewRevisionReadyPods = 2
		comp.TotalPods = 4 // 2 new + 2 old still up
		comp.TargetRevisionHash = "rev2"
		obs.Components[c] = comp
	}
	engine := obs.Components[v1beta1.EngineComponent]
	engine.NewRevisionReadyPods = 1
	obs.Components[v1beta1.EngineComponent] = engine
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseWaiting {
		t.Errorf("BlueGreen Waiting: got %q want Waiting", tr.Phase)
	}
}

func TestComputeTransition_BlueGreenShifting(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
	obs := idleObs(g)
	// Both rolling, all new pods Ready, but old pods still up.
	for _, c := range g.Components {
		comp := obs.Components[c]
		comp.RolloutInFlight = true
		comp.NewRevisionPods = 2
		comp.NewRevisionReadyPods = 2
		comp.TotalPods = 4 // 2 new + 2 old still up
		comp.TargetRevisionHash = "rev2"
		obs.Components[c] = comp
	}
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseShifting {
		t.Errorf("BlueGreen Shifting: got %q want Shifting", tr.Phase)
	}
}

func TestComputeTransition_BlueGreenScalingDown(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
	obs := idleObs(g)
	// All rolling, new pods Ready, old pods gone (TotalPods == NewRevisionPods).
	for _, c := range g.Components {
		comp := obs.Components[c]
		comp.RolloutInFlight = true
		comp.NewRevisionPods = 2
		comp.NewRevisionReadyPods = 2
		comp.TotalPods = 2
		comp.TargetRevisionHash = "rev2"
		obs.Components[c] = comp
	}
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseScalingDown {
		t.Errorf("BlueGreen ScalingDown: got %q want ScalingDown", tr.Phase)
	}
}

func TestComputeTransition_BlueGreenFailed(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
	obs := idleObs(g)
	engine := obs.Components[v1beta1.EngineComponent]
	engine.Failed = true
	obs.Components[v1beta1.EngineComponent] = engine
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseFailed {
		t.Errorf("BlueGreen any-failure: got %q want Failed", tr.Phase)
	}
}

// TestComputeTransition_BlueGreenPaused asserts BlueGreen honors the
// ISVC-wide PausedGlobal signal (the only pause path coordination
// supports today). Per-Component pause was never wired in production;
// it was removed in favor of PausedGlobal exclusively.
func TestComputeTransition_BlueGreenPaused(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
	obs := idleObs(g)
	obs.PausedGlobal = true
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhasePaused {
		t.Errorf("BlueGreen with global pause: got %q want Paused", tr.Phase)
	}
}

// TestComputeTransition_BlueGreenStaged verifies a BlueGreen group that
// has converged to a static partition rests at Staged instead of
// churning Shifting forever. The component reached its desired
// staged shape (AtDesiredShape) with a partition intentionally holding
// old-revision pods (TotalPods > NewRevisionPods by design).
func TestComputeTransition_BlueGreenStaged(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
	obs := idleObs(g)
	engine := obs.Components[v1beta1.EngineComponent]
	engine.RolloutInFlight = true
	engine.AtDesiredShape = true
	engine.Partition = 2
	engine.DesiredReplicas = 8
	engine.NewRevisionPods = 6
	engine.NewRevisionReadyPods = 6
	engine.TotalPods = 8 // 6 new + 2 held old by partition
	engine.TargetRevisionHash = "rev2"
	obs.Components[v1beta1.EngineComponent] = engine
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseStaged {
		t.Errorf("BlueGreen partitioned converged: got %q want Staged", tr.Phase)
	}
}

// TestComputeTransition_RollingUpdateStaged is the RollingUpdate
// companion: a rolling component converged to a static partition rests
// at Staged instead of ScalingDown forever.
func TestComputeTransition_RollingUpdateStaged(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyRollingUpdate}
	obs := idleObs(g)
	engine := obs.Components[v1beta1.EngineComponent]
	engine.RolloutInFlight = true
	engine.AtDesiredShape = true
	engine.Partition = 2
	engine.DesiredReplicas = 8
	engine.NewRevisionPods = 6
	engine.NewRevisionReadyPods = 6
	engine.TotalPods = 8 // 6 new + 2 held old by partition
	engine.TargetRevisionHash = "rev2"
	obs.Components[v1beta1.EngineComponent] = engine
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseStaged {
		t.Errorf("RollingUpdate partitioned converged: got %q want Staged", tr.Phase)
	}
}

// TestComputeTransition_StagedNotWhenMidRoll is a regression guard: a
// component that has NOT reached its desired shape (new pods not all
// Ready) must NOT be reported Staged — it walks the normal
// Surging/Waiting path.
func TestComputeTransition_StagedNotWhenMidRoll(t *testing.T) {
	for _, policy := range []v1beta1.CoordinationPolicy{
		v1beta1.CoordinationPolicyBlueGreen,
		v1beta1.CoordinationPolicyRollingUpdate,
	} {
		g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: policy}
		obs := idleObs(g)
		engine := obs.Components[v1beta1.EngineComponent]
		engine.RolloutInFlight = true
		engine.AtDesiredShape = false // mid-roll
		engine.Partition = 2
		engine.DesiredReplicas = 8
		engine.NewRevisionPods = 6
		engine.NewRevisionReadyPods = 4 // not all Ready
		engine.TotalPods = 8
		engine.TargetRevisionHash = "rev2"
		obs.Components[v1beta1.EngineComponent] = engine
		tr := ComputeTransition(obs)
		if tr.Phase == v1beta1.CoordinationPhaseStaged {
			t.Errorf("%s mid-roll must NOT be Staged; got %q", policy, tr.Phase)
		}
		if tr.Phase != v1beta1.CoordinationPhaseWaiting {
			t.Errorf("%s mid-roll: got %q want Waiting", policy, tr.Phase)
		}
	}
}

// TestComputeTransition_StagedNotWhenPartitionZero is a regression
// guard: a full rollout (Partition=0) that converged must NOT hijack
// the Staged path — it reaches ScalingDown/Idle via the existing flow.
func TestComputeTransition_StagedNotWhenPartitionZero(t *testing.T) {
	for _, policy := range []v1beta1.CoordinationPolicy{
		v1beta1.CoordinationPolicyBlueGreen,
		v1beta1.CoordinationPolicyRollingUpdate,
	} {
		g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: policy}
		obs := idleObs(g)
		engine := obs.Components[v1beta1.EngineComponent]
		engine.RolloutInFlight = true
		engine.AtDesiredShape = true
		engine.Partition = 0 // full rollout
		engine.DesiredReplicas = 8
		engine.NewRevisionPods = 6
		engine.NewRevisionReadyPods = 6
		engine.TotalPods = 8 // old pods still draining
		engine.TargetRevisionHash = "rev2"
		obs.Components[v1beta1.EngineComponent] = engine
		tr := ComputeTransition(obs)
		if tr.Phase == v1beta1.CoordinationPhaseStaged {
			t.Errorf("%s full rollout (Partition=0) must NOT be Staged; got %q", policy, tr.Phase)
		}
	}
}

func TestComputeTransition_RollingUpdateSurging(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyRollingUpdate}
	obs := idleObs(g)
	engine := obs.Components[v1beta1.EngineComponent]
	engine.RolloutInFlight = true
	engine.NewRevisionPods = 0
	obs.Components[v1beta1.EngineComponent] = engine
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseSurging {
		t.Errorf("RollingUpdate any-surging: got %q want Surging", tr.Phase)
	}
}

func TestComputeTransition_RollingUpdateIdle(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyRollingUpdate}
	tr := ComputeTransition(idleObs(g))
	if tr.Phase != v1beta1.CoordinationPhaseIdle {
		t.Errorf("RollingUpdate idle: got %q want Idle", tr.Phase)
	}
}

func TestComputeTransition_RollingUpdateFailed(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyRollingUpdate}
	obs := idleObs(g)
	engine := obs.Components[v1beta1.EngineComponent]
	engine.Failed = true
	obs.Components[v1beta1.EngineComponent] = engine
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseFailed {
		t.Errorf("RollingUpdate failure: got %q want Failed", tr.Phase)
	}
}

// TestComputeTransition_FailedExitsOnRecovery pins the terminal-state
// exit: the group Phase is a pure rollup
// of per-Component state, so once a corrective edit reconciles the
// failed Component back to health (Failed cleared, nothing rolling),
// the very next transition leaves Failed — no group-level latch, no
// operator action. Asserted per policy over the same recovered
// observation.
func TestComputeTransition_FailedExitsOnRecovery(t *testing.T) {
	for _, policy := range []v1beta1.CoordinationPolicy{
		v1beta1.CoordinationPolicyIndependent,
		v1beta1.CoordinationPolicyBlueGreen,
		v1beta1.CoordinationPolicyRollingUpdate,
		v1beta1.CoordinationPolicySequential,
	} {
		t.Run(string(policy), func(t *testing.T) {
			g := ResolvedGroup{
				Name:       "0",
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Order:      []v1beta1.ComponentType{v1beta1.EngineComponent},
				Policy:     policy,
			}
			obs := idleObs(g)
			engine := obs.Components[v1beta1.EngineComponent]
			engine.Failed = true
			obs.Components[v1beta1.EngineComponent] = engine
			if tr := ComputeTransition(obs); tr.Phase != v1beta1.CoordinationPhaseFailed {
				t.Fatalf("failed component: got %q want Failed", tr.Phase)
			}

			// The corrective edit reconciled: Failed cleared, no rollout
			// in flight. The rollup must settle at Idle immediately.
			engine.Failed = false
			obs.Components[v1beta1.EngineComponent] = engine
			if tr := ComputeTransition(obs); tr.Phase != v1beta1.CoordinationPhaseIdle {
				t.Errorf("recovered component: got %q want Idle (Failed must exit on rollup)", tr.Phase)
			}
		})
	}
}

func TestComputeTransition_SequentialActiveComponent(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
	}
	obs := idleObs(g)
	// Decoder is rolling; engine is idle.
	decoder := obs.Components[v1beta1.DecoderComponent]
	decoder.RolloutInFlight = true
	decoder.NewRevisionPods = 0
	decoder.TargetRevisionHash = "rev2"
	obs.Components[v1beta1.DecoderComponent] = decoder
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseSurging {
		t.Errorf("Sequential active: phase got %q want Surging", tr.Phase)
	}
	if tr.CompositePhase != "decoder.Surging" {
		t.Errorf("composite phase: got %q want decoder.Surging", tr.CompositePhase)
	}
	if tr.CurrentComponent != v1beta1.DecoderComponent {
		t.Errorf("currentComponent: got %q want decoder", tr.CurrentComponent)
	}
}

func TestComputeTransition_SequentialAwaitingNext(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
	}
	// All Components idle (neither has rolled yet) — group is Idle
	// at the base layer; composite is plain Idle since no Component
	// has advanced.
	obs := idleObs(g)
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseIdle {
		t.Errorf("all-idle: phase got %q want Idle", tr.Phase)
	}
	// Now decoder is on a new revision but engine isn't rolling.
	decoder := obs.Components[v1beta1.DecoderComponent]
	decoder.CurrentRevisionHash = "rev2"
	decoder.TargetRevisionHash = "rev2"
	decoder.RolloutInFlight = false
	obs.Components[v1beta1.DecoderComponent] = decoder
	// Engine still on rev1, no rollout in flight.
	tr = ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseIdle {
		t.Errorf("Sequential awaiting: phase got %q want Idle", tr.Phase)
	}
}

func TestComputeTransition_SequentialFailedBlocks(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
	}
	obs := idleObs(g)
	decoder := obs.Components[v1beta1.DecoderComponent]
	decoder.Failed = true
	decoder.FailureMessage = "ready timeout"
	obs.Components[v1beta1.DecoderComponent] = decoder
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseFailed {
		t.Errorf("Sequential failure: got %q want Failed", tr.Phase)
	}
	if tr.CompositePhase != v1beta1.CompositePhaseSequentialFailed {
		t.Errorf("composite Sequential.Failed: got %q", tr.CompositePhase)
	}
	if tr.CurrentComponent != v1beta1.DecoderComponent {
		t.Errorf("CurrentComponent: got %q want decoder", tr.CurrentComponent)
	}
}

func TestComputeTransition_SequentialNextStartsAfterPrevious(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
	}
	obs := idleObs(g)
	// Decoder done (Idle on new rev), engine now starts rolling.
	decoder := obs.Components[v1beta1.DecoderComponent]
	decoder.RolloutInFlight = false
	obs.Components[v1beta1.DecoderComponent] = decoder
	engine := obs.Components[v1beta1.EngineComponent]
	engine.RolloutInFlight = true
	engine.NewRevisionPods = 0
	engine.TargetRevisionHash = "rev2"
	obs.Components[v1beta1.EngineComponent] = engine
	tr := ComputeTransition(obs)
	if tr.CurrentComponent != v1beta1.EngineComponent {
		t.Errorf("CurrentComponent: got %q want engine", tr.CurrentComponent)
	}
	if tr.PreviousComponent != v1beta1.DecoderComponent {
		t.Errorf("PreviousComponent: got %q want decoder", tr.PreviousComponent)
	}
	if tr.Phase != v1beta1.CoordinationPhaseSurging {
		t.Errorf("phase: got %q want Surging", tr.Phase)
	}
}

// TestComputeTransition_SequentialSoakHolds verifies the soak gate
// blocks the next Component from starting until the configured Soak
// duration has elapsed since the previous Component completed (as
// recorded in PreviousPhaseEnteredAt).
func TestComputeTransition_SequentialSoakHolds(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Soak:       5 * time.Second,
	}
	now := time.Now()
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {
				Component:           v1beta1.DecoderComponent,
				DesiredReplicas:     1,
				TargetRevisionHash:  "rev2",
				CurrentRevisionHash: "rev2",
				RolloutInFlight:     false,
			},
			v1beta1.EngineComponent: {
				Component:           v1beta1.EngineComponent,
				DesiredReplicas:     1,
				TargetRevisionHash:  "rev2",
				CurrentRevisionHash: "rev1",
				RolloutInFlight:     true,
			},
		},
		Now:                    now,
		PreviousPhaseEnteredAt: now.Add(-2 * time.Second), // 2s into a 5s soak
	}
	tr := ComputeTransition(obs)
	if tr.CompositePhase != v1beta1.CompositePhaseSequentialAwaiting {
		t.Errorf("soak should hold engine at Sequential.Awaiting; got composite=%q phase=%q msg=%q",
			tr.CompositePhase, tr.Phase, tr.Message)
	}
	if tr.CurrentComponent != v1beta1.EngineComponent {
		t.Errorf("CurrentComponent during soak: got %q want engine", tr.CurrentComponent)
	}
	if tr.PreviousComponent != v1beta1.DecoderComponent {
		t.Errorf("PreviousComponent during soak: got %q want decoder", tr.PreviousComponent)
	}
}

// TestComputeTransition_SequentialSoakReleases verifies the soak gate
// releases once the configured Soak duration has elapsed, letting the
// next Component begin its rollout.
func TestComputeTransition_SequentialSoakReleases(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Soak:       5 * time.Second,
	}
	now := time.Now()
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {
				Component:           v1beta1.DecoderComponent,
				DesiredReplicas:     1,
				TargetRevisionHash:  "rev2",
				CurrentRevisionHash: "rev2",
				RolloutInFlight:     false,
			},
			v1beta1.EngineComponent: {
				Component:           v1beta1.EngineComponent,
				DesiredReplicas:     1,
				TargetRevisionHash:  "rev2",
				CurrentRevisionHash: "rev1",
				RolloutInFlight:     true,
			},
		},
		Now:                    now,
		PreviousPhaseEnteredAt: now.Add(-10 * time.Second), // soak elapsed (10s > 5s)
	}
	tr := ComputeTransition(obs)
	if tr.CompositePhase == v1beta1.CompositePhaseSequentialAwaiting {
		t.Errorf("soak elapsed: must NOT be Awaiting; got composite=%q", tr.CompositePhase)
	}
	if tr.CurrentComponent != v1beta1.EngineComponent {
		t.Errorf("CurrentComponent post-soak: got %q want engine", tr.CurrentComponent)
	}
}

// TestComputeTransition_SequentialNoSoakRunsImmediately verifies the
// default behavior (Soak=0): the next Component starts the moment
// the previous Component finishes, no time-based hold.
func TestComputeTransition_SequentialNoSoakRunsImmediately(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Soak:       0,
	}
	now := time.Now()
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {
				Component:           v1beta1.DecoderComponent,
				DesiredReplicas:     1,
				TargetRevisionHash:  "rev2",
				CurrentRevisionHash: "rev2",
				RolloutInFlight:     false,
			},
			v1beta1.EngineComponent: {
				Component:           v1beta1.EngineComponent,
				DesiredReplicas:     1,
				TargetRevisionHash:  "rev2",
				CurrentRevisionHash: "rev1",
				RolloutInFlight:     true,
			},
		},
		Now:                    now,
		PreviousPhaseEnteredAt: now.Add(-1 * time.Millisecond), // immediately after completion
	}
	tr := ComputeTransition(obs)
	if tr.CompositePhase == v1beta1.CompositePhaseSequentialAwaiting {
		t.Errorf("Soak=0 must NOT engage Awaiting; got composite=%q", tr.CompositePhase)
	}
}

// TestComputeSurgeBudgetWithRatio_UsesServingPodsBaseline pins the
// fix for the spurious RatioSkewRejected event spam at rollout start.
// The function MUST project against live serving capacity (ServingPods),
// NOT against NewRevisionPods which is zero before the first surge lands.
//
// Without populating state.Serving the EvaluateSurge call falls back
// to NewPods={Engine:0, Decoder:0} at rollout start, and pairwise
// respectsBand sees a zero peer → `pb <= 0` → reject. Every surge
// attempt then zeros the budget and trips the RatioSkewRejected
// metric/event.
//
// This test sets ServingPods correctly and asserts the LARGER side's
// surge survives (engine 4→5, ratio 5/2=2.5 at upper band → allowed).
// The smaller-side decoder ALWAYS gets rejected here because its +1
// surge against the larger-side's flat 4 would drop the ratio to 4/3
// = 1.33 → below lower band 1.5; that's the correct ratio-preserving
// behavior, not a regression. The point of this test is that the
// LARGER side's surge gets through — that's what unblocks rollouts.
func TestComputeSurgeBudgetWithRatio_UsesServingPodsBaseline(t *testing.T) {
	tol := int32(25)
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
		Pacing: v1beta1.CoordinationPacing{
			Type:                  v1beta1.CoordinationPacingRatioBalanced,
			RatioTolerancePercent: &tol,
		},
	}
	// Live state: 4 engine + 2 decoder pods, all serving, NO surge
	// pods yet (NewRevisionPods=0). With Original={4, 2} and tol=25%
	// the band is [1.5, 2.5]. A +1 engine surge against ServingPods
	// projects to 5/2=2.5 — at the boundary, in band → ALLOW.
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.EngineComponent: {
				Component:       v1beta1.EngineComponent,
				DesiredReplicas: 4,
				TotalPods:       4,
				ReadyPods:       4,
				ServingPods:     4,
				NewRevisionPods: 0,
			},
			v1beta1.DecoderComponent: {
				Component:       v1beta1.DecoderComponent,
				DesiredReplicas: 2,
				TotalPods:       2,
				ReadyPods:       2,
				ServingPods:     2,
				NewRevisionPods: 0,
			},
		},
		OriginalReplicas: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  4,
			v1beta1.DecoderComponent: 2,
		},
	}
	budget, _ := computeSurgeBudgetWithRatio(obs)
	// MaxSurge defaults to 25% — for 4 engine replicas, budget = 1.
	// EvaluateSurge with +1 against Serving={4,2} projects to 5/2=2.5
	// → at upper band → ALLOW → budget stays 1. This is the
	// rollout-unblocking path: with the broken NewPods baseline
	// (peer at 0), engine would have been rejected and the rollout
	// would deadlock.
	if budget[v1beta1.EngineComponent] != 1 {
		t.Errorf("engine surge budget should be 1 (default 25%% of 4, projection 5/2=2.5 at upper band): got %d", budget[v1beta1.EngineComponent])
	}
}

// TestComputeSurgeBudgetWithRatio_NewPodsBaselineWouldDeadlock is the
// negative-side companion: documents that if ServingPods were absent
// and the function still fell back to NewPods, the budget would be
// zeroed even though the live cluster ratio sits in band. This test
// exists as a tripwire — re-introducing the NewPods fallback (e.g.,
// by reverting the ServingPods population in buildComponentObservation)
// would break this test alongside the BlueGreen+RatioBalanced KIND specs.
func TestComputeSurgeBudgetWithRatio_NewPodsBaselineWouldDeadlock(t *testing.T) {
	tol := int32(25)
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
		Pacing: v1beta1.CoordinationPacing{
			Type:                  v1beta1.CoordinationPacingRatioBalanced,
			RatioTolerancePercent: &tol,
		},
	}
	// Same fleet, but ServingPods left at zero — the {0, 0} shape a
	// NewPods-based baseline would see at rollout start.
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.EngineComponent: {
				Component:       v1beta1.EngineComponent,
				DesiredReplicas: 4,
				TotalPods:       4,
				ReadyPods:       4,
				ServingPods:     0, // <-- the zero-serving shape
				NewRevisionPods: 0,
			},
			v1beta1.DecoderComponent: {
				Component:       v1beta1.DecoderComponent,
				DesiredReplicas: 2,
				TotalPods:       2,
				ReadyPods:       2,
				ServingPods:     0, // <-- the zero-serving shape
				NewRevisionPods: 0,
			},
		},
		OriginalReplicas: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  4,
			v1beta1.DecoderComponent: 2,
		},
	}
	_, skew := computeSurgeBudgetWithRatio(obs)
	if !skew {
		t.Errorf("with ServingPods=0 (a zero-serving baseline) the gate MUST trip skew — this test pins the deadlock shape so a regression is visible at unit level, not just in KIND")
	}
}

// --- Sequential handoff on desired-shape convergence ---

// TestActiveSequentialComponent_SkipsAtDesiredShape verifies a leading
// Component that reached its desired staged shape is treated as done for
// handoff (skipped) even though RolloutInFlight is still true, so the
// active selector advances to the next genuinely-unfinished Component.
func TestActiveSequentialComponent_SkipsAtDesiredShape(t *testing.T) {
	order := []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	components := map[v1beta1.ComponentType]ComponentObservation{
		v1beta1.EngineComponent:  {Component: v1beta1.EngineComponent, RolloutInFlight: true, AtDesiredShape: true},
		v1beta1.DecoderComponent: {Component: v1beta1.DecoderComponent, RolloutInFlight: true, AtDesiredShape: false},
	}
	got, ok := activeSequentialComponent(order, components)
	if !ok || got != v1beta1.DecoderComponent {
		t.Errorf("active: got (%q,%v) want (decoder,true) — a staged engine must not block", got, ok)
	}
}

// TestActiveSequentialComponent_StillBlocksWhenNotStaged is the
// regression guard: a leading Component that is in flight and NOT at its
// desired shape must still be the active/blocking Component.
func TestActiveSequentialComponent_StillBlocksWhenNotStaged(t *testing.T) {
	order := []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	components := map[v1beta1.ComponentType]ComponentObservation{
		v1beta1.EngineComponent:  {Component: v1beta1.EngineComponent, RolloutInFlight: true, AtDesiredShape: false},
		v1beta1.DecoderComponent: {Component: v1beta1.DecoderComponent, RolloutInFlight: true, AtDesiredShape: false},
	}
	got, ok := activeSequentialComponent(order, components)
	if !ok || got != v1beta1.EngineComponent {
		t.Errorf("active: got (%q,%v) want (engine,true) — an unconverged engine must still block", got, ok)
	}
}

// TestCompletedSequentialComponents_CountsAtDesiredShape verifies a
// staged leading Component counts as completed so the count advances past
// it to the next Component.
func TestCompletedSequentialComponents_CountsAtDesiredShape(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Order:      []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
	}
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.EngineComponent:  {Component: v1beta1.EngineComponent, RolloutInFlight: true, AtDesiredShape: true},
			v1beta1.DecoderComponent: {Component: v1beta1.DecoderComponent, RolloutInFlight: true, AtDesiredShape: false},
		},
	}
	if got := completedSequentialComponents(obs); got != 1 {
		t.Errorf("completed: got %d want 1 — a staged engine must count as completed", got)
	}
}

// TestComputeTransition_SequentialHandsOffOnStaged is the end-to-end
// selector+counter check: with engine staged (RolloutInFlight, staged)
// and decoder genuinely rolling, the group drives decoder — not engine.
func TestComputeTransition_SequentialHandsOffOnStaged(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Order:      []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
	}
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.EngineComponent: {
				Component: v1beta1.EngineComponent, DesiredReplicas: 2,
				RolloutInFlight: true, AtDesiredShape: true, Partition: 1,
			},
			v1beta1.DecoderComponent: {
				Component: v1beta1.DecoderComponent, DesiredReplicas: 2,
				RolloutInFlight: true, AtDesiredShape: false,
				NewRevisionPods: 0, TargetRevisionHash: "rev2",
			},
		},
	}
	tr := ComputeTransition(obs)
	if tr.CurrentComponent != v1beta1.DecoderComponent {
		t.Errorf("CurrentComponent: got %q want decoder — staged engine must hand off", tr.CurrentComponent)
	}
	if tr.PreviousComponent != v1beta1.EngineComponent {
		t.Errorf("PreviousComponent: got %q want engine", tr.PreviousComponent)
	}
	if tr.Phase != v1beta1.CoordinationPhaseSurging {
		t.Errorf("phase: got %q want Surging", tr.Phase)
	}
}
