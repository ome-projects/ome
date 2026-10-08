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

// --- ComputeTransition from every group state, under every event ---

// settledOn is one member at rest on rev: two Ready, serving pods on its
// only revision, which is both its current and its update revision, so it
// stands at its desired shape.
func settledOn(c v1beta1.ComponentType, rev string) ComponentObservation {
	return ComponentObservation{
		Component: c, DesiredReplicas: 2, TotalPods: 2, ReadyPods: 2, ServingPods: 2,
		NewRevisionPods: 2, NewRevisionReadyPods: 2,
		TargetRevisionHash: rev, CurrentRevisionHash: rev, AtDesiredShape: true,
	}
}

// rollingTo is one member mid-roll from rev1 to rev2 with the named pod
// counts: pods of any revision, pods on rev2, and Ready pods on rev2.
func rollingTo(c v1beta1.ComponentType, total, onTarget, readyOnTarget int32) ComponentObservation {
	serving := total - onTarget + readyOnTarget
	return ComponentObservation{
		Component: c, DesiredReplicas: 2, TotalPods: total, ReadyPods: serving, ServingPods: serving,
		NewRevisionPods: onTarget, NewRevisionReadyPods: readyOnTarget,
		TargetRevisionHash: "rev2", CurrentRevisionHash: "rev1", RolloutInFlight: true,
	}
}

// stagedMember is one member resting at its staged shape under a partition
// of one: one Instance Ready on rev2, the held one Ready on rev1.
func stagedMember(c v1beta1.ComponentType) ComponentObservation {
	m := rollingTo(c, 2, 1, 1)
	m.Partition = 1
	m.AtDesiredShape = true
	return m
}

// darkened takes every pod of a member out of readiness and rotation, as
// rows demoted to Pending read, without a Failed row.
func darkened(m ComponentObservation) ComponentObservation {
	m.ReadyPods, m.ServingPods, m.NewRevisionReadyPods, m.AtDesiredShape = 0, 0, 0, false
	return m
}

// edited applies one edit to a member.
func edited(m ComponentObservation, edit func(*ComponentObservation)) ComponentObservation {
	edit(&m)
	return m
}

// groupOf builds a group observation over the members, in order; a
// Sequential group's Order is the member order.
func groupOf(policy v1beta1.CoordinationPolicy, members ...ComponentObservation) GroupObservation {
	obs := GroupObservation{
		Group:      ResolvedGroup{Name: "0", Policy: policy},
		Components: map[v1beta1.ComponentType]ComponentObservation{},
	}
	for _, m := range members {
		obs.Group.Components = append(obs.Group.Components, m.Component)
		obs.Components[m.Component] = m
	}
	if policy == v1beta1.CoordinationPolicySequential {
		obs.Group.Order = append([]v1beta1.ComponentType(nil), obs.Group.Components...)
	}
	return obs
}

// soaking is a Sequential group in its soak window: the decoder completed,
// the engine bumped and rolling, and the soak not yet elapsed since the
// group's last base-phase change.
func soaking(engine ComponentObservation, elapsed time.Duration) GroupObservation {
	obs := groupOf(v1beta1.CoordinationPolicySequential, settledOn(v1beta1.DecoderComponent, "rev2"), engine)
	obs.Group.Soak = 5 * time.Minute
	obs.Now = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	obs.PreviousPhaseEnteredAt = obs.Now.Add(-elapsed)
	return obs
}

// ratioBalanced puts the group under RatioBalanced pacing with a 25%
// tolerance and the named anchor.
func ratioBalanced(obs GroupObservation, engine, decoder int32) GroupObservation {
	tol := int32(25)
	obs.Group.Pacing = v1beta1.CoordinationPacing{Type: v1beta1.CoordinationPacingRatioBalanced, RatioTolerancePercent: &tol}
	obs.OriginalReplicas = map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: engine, v1beta1.DecoderComponent: decoder}
	return obs
}

// TestComputeTransition_PhaseFollowsTheMembersFromEveryState walks the
// group machine one case at a time: the observation a state and an
// event leave behind, and the phase the transition function computes from
// it. The phase is a pure function of the members, so an event that leaves
// the facts the state already reads keeps the phase, and one that changes
// them moves it, whatever phase the previous pass persisted. BlueGreen is
// the dominant policy; a case whose outcome differs under another policy
// carries that policy's variant beside it.
func TestComputeTransition_PhaseFollowsTheMembersFromEveryState(t *testing.T) {
	const (
		idle    = v1beta1.CoordinationPhaseIdle
		surging = v1beta1.CoordinationPhaseSurging
		waiting = v1beta1.CoordinationPhaseWaiting
		shift   = v1beta1.CoordinationPhaseShifting
		scaling = v1beta1.CoordinationPhaseScalingDown
		stagedP = v1beta1.CoordinationPhaseStaged
		failed  = v1beta1.CoordinationPhaseFailed
		paused  = v1beta1.CoordinationPhasePaused
	)
	bg, ru, ind, seq := v1beta1.CoordinationPolicyBlueGreen, v1beta1.CoordinationPolicyRollingUpdate, v1beta1.CoordinationPolicyIndependent, v1beta1.CoordinationPolicySequential
	engine, decoder := v1beta1.EngineComponent, v1beta1.DecoderComponent
	settled := func(c v1beta1.ComponentType) ComponentObservation { return settledOn(c, "rev1") }
	missing := func(c v1beta1.ComponentType) ComponentObservation { return buildComponentObservation(nil, c, nil, 0) }
	failedMember := func(c v1beta1.ComponentType) ComponentObservation {
		return edited(rollingTo(c, 3, 1, 0), func(m *ComponentObservation) { m.Failed = true })
	}
	retargeted := func(m ComponentObservation) ComponentObservation {
		return edited(m, func(m *ComponentObservation) {
			m.TargetRevisionHash, m.NewRevisionPods, m.NewRevisionReadyPods, m.AtDesiredShape = "rev3", 0, 0, false
		})
	}
	withPartition := func(m ComponentObservation, p int32) ComponentObservation {
		return edited(m, func(m *ComponentObservation) { m.Partition = p })
	}
	pausedGroup := func(obs GroupObservation) GroupObservation { obs.PausedGlobal = true; return obs }

	cases := []struct {
		name      string
		obs       GroupObservation
		want      v1beta1.CoordinationPhase
		composite string
		skew      *bool
	}{
		// Idle: the bump and later facts observed in one pass enter at the
		// phase the facts compute; facts that do not open a roll keep Idle.
		{name: "idle-surge-pod-observed-with-the-bump", obs: groupOf(bg, rollingTo(engine, 3, 1, 0), settled(decoder)), want: waiting},
		{name: "idle-ready-target-observed-with-the-bump", obs: groupOf(bg, rollingTo(engine, 4, 2, 2), settled(decoder)), want: shift},
		{name: "idle-ready-target-observed-with-no-old-pod", obs: groupOf(bg, rollingTo(engine, 2, 2, 2), settled(decoder)), want: scaling},
		{name: "idle-capacity-lost-keeps-idle", obs: groupOf(bg, darkened(settled(engine)), settled(decoder)), want: idle},
		{name: "idle-dark-member-keeps-idle", obs: groupOf(bg, darkened(settled(engine)), darkened(settled(decoder))), want: idle},
		{name: "idle-converged-members-keep-idle", obs: groupOf(bg, settledOn(engine, "rev2"), settledOn(decoder, "rev2")), want: idle},
		{name: "idle-partition-set-keeps-idle", obs: groupOf(bg, edited(withPartition(settled(engine), 1), func(m *ComponentObservation) { m.AtDesiredShape = true }), settled(decoder)), want: idle},
		{name: "idle-partition-cleared-keeps-idle", obs: groupOf(bg, withPartition(settled(engine), 0), settled(decoder)), want: idle},
		{name: "idle-counters-trailing-keep-idle", obs: groupOf(bg, edited(settled(engine), func(m *ComponentObservation) { m.ReadyPods, m.ServingPods = 1, 1 }), settled(decoder)), want: idle},
		{name: "idle-under-rollingupdate", obs: groupOf(ru, settled(engine), settled(decoder)), want: idle},
		{name: "idle-under-independent", obs: groupOf(ind, settled(engine), settled(decoder)), want: idle},
		{name: "idle-under-sequential", obs: groupOf(seq, settled(decoder), settled(engine)), want: idle, composite: "Idle"},

		// Sequential, awaiting the next member: the soak hold is judged before the active member's
		// pods, so nothing the member does under the window moves the group.
		{name: "awaiting-surge-pod-under-the-soak-keeps-awaiting", obs: soaking(rollingTo(engine, 3, 1, 0), time.Minute), want: idle, composite: v1beta1.CompositePhaseSequentialAwaiting},
		{name: "awaiting-ready-target-under-the-soak-keeps-awaiting", obs: soaking(rollingTo(engine, 4, 2, 2), time.Minute), want: idle, composite: v1beta1.CompositePhaseSequentialAwaiting},
		{name: "awaiting-soak-elapsed-enters-the-member-phase", obs: soaking(rollingTo(engine, 3, 1, 0), 6*time.Minute), want: waiting, composite: "engine.Waiting"},
		{name: "awaiting-soak-long-elapsed-reads-the-member-phase", obs: soaking(rollingTo(engine, 2, 0, 0), time.Hour), want: surging, composite: "engine.Surging"},
		{name: "awaiting-completed-member-capacity-lost-keeps-awaiting", obs: edited2(soaking(rollingTo(engine, 2, 0, 0), time.Minute), decoder, darkened), want: idle, composite: v1beta1.CompositePhaseSequentialAwaiting},
		{name: "awaiting-partition-on-the-next-member-keeps-awaiting", obs: soaking(withPartition(rollingTo(engine, 2, 0, 0), 1), time.Minute), want: idle, composite: v1beta1.CompositePhaseSequentialAwaiting},
		{name: "awaiting-partition-cleared-keeps-awaiting", obs: soaking(withPartition(rollingTo(engine, 2, 0, 0), 0), time.Minute), want: idle, composite: v1beta1.CompositePhaseSequentialAwaiting},
		{name: "awaiting-counters-trailing-keep-awaiting", obs: edited2(soaking(rollingTo(engine, 2, 0, 0), time.Minute), decoder, func(m ComponentObservation) ComponentObservation { m.ReadyPods = 1; return m }), want: idle, composite: v1beta1.CompositePhaseSequentialAwaiting},
		{name: "awaiting-resync-keeps-awaiting", obs: soaking(rollingTo(engine, 2, 0, 0), 2*time.Minute), want: idle, composite: v1beta1.CompositePhaseSequentialAwaiting},
		{name: "awaiting-next-member-missing-reads-complete", obs: soaking(missing(engine), time.Minute), want: idle, composite: "Idle"},
		{name: "awaiting-collapsed-to-bluegreen-reads-the-rolling-member", obs: groupOf(bg, settledOn(decoder, "rev2"), rollingTo(engine, 2, 0, 0)), want: surging},

		// Surging: a member with no new pod is what Surging means, so facts on
		// other pods keep it; a new pod on every member moves it.
		{name: "surging-rebump-keeps-surging", obs: groupOf(bg, retargeted(rollingTo(engine, 2, 0, 0)), settled(decoder)), want: surging},
		{name: "surging-new-pod-unready-elsewhere-keeps-surging", obs: groupOf(bg, rollingTo(engine, 2, 0, 0), rollingTo(decoder, 3, 1, 0)), want: surging},
		{name: "surging-old-capacity-lost-keeps-surging", obs: groupOf(bg, darkened(rollingTo(engine, 2, 0, 0)), settled(decoder)), want: surging},
		{name: "surging-old-pods-gone-first-keeps-surging", obs: groupOf(bg, rollingTo(engine, 0, 0, 0), settled(decoder)), want: surging},
		{name: "surging-dark-settled-member-keeps-surging", obs: groupOf(bg, rollingTo(engine, 2, 0, 0), darkened(settled(decoder))), want: surging},
		{name: "surging-ratio-admits-the-step-keeps-surging", obs: ratioBalanced(groupOf(bg, fourWide(rollingTo(engine, 4, 0, 0)), fourWide(rollingTo(decoder, 4, 0, 0))), 4, 4), want: surging, skew: ptrBool(false)},
		{name: "surging-under-rollingupdate", obs: groupOf(ru, rollingTo(engine, 2, 0, 0), settled(decoder)), want: surging},
		{name: "surging-under-independent-reads-shifting", obs: groupOf(ind, rollingTo(engine, 2, 0, 0), settled(decoder)), want: shift},
		{name: "surging-partition-set-keeps-surging", obs: groupOf(bg, withPartition(rollingTo(engine, 2, 0, 0), 1), settled(decoder)), want: surging},
		{name: "surging-partition-cleared-keeps-surging", obs: groupOf(bg, withPartition(rollingTo(engine, 2, 0, 0), 0), settled(decoder)), want: surging},
		{name: "surging-only-rolling-member-missing-reads-idle", obs: groupOf(bg, missing(engine), settled(decoder)), want: idle},
		{name: "surging-resync-keeps-surging", obs: groupOf(bg, rollingTo(engine, 2, 0, 0), settled(decoder)), want: surging},

		// Waiting: every rolling member has a new pod and one is not Ready.
		{name: "waiting-rebump-reads-surging", obs: groupOf(bg, retargeted(rollingTo(engine, 3, 1, 0)), settled(decoder)), want: surging},
		{name: "waiting-member-joining-the-roll-reads-surging", obs: groupOf(bg, rollingTo(engine, 3, 1, 0), rollingTo(decoder, 2, 0, 0)), want: surging},
		{name: "waiting-further-new-pod-keeps-waiting", obs: groupOf(bg, rollingTo(engine, 4, 2, 0), settled(decoder)), want: waiting},
		{name: "waiting-new-pods-all-gone-reads-surging", obs: groupOf(bg, rollingTo(engine, 2, 0, 0), settled(decoder)), want: surging},
		{name: "waiting-old-capacity-lost-keeps-waiting", obs: groupOf(bg, edited(rollingTo(engine, 3, 1, 0), func(m *ComponentObservation) { m.ReadyPods, m.ServingPods = 0, 0 }), settled(decoder)), want: waiting},
		{name: "waiting-old-pods-gone-keeps-waiting", obs: groupOf(bg, rollingTo(engine, 1, 1, 0), settled(decoder)), want: waiting},
		{name: "waiting-dark-settled-member-keeps-waiting", obs: groupOf(bg, rollingTo(engine, 3, 1, 0), darkened(settled(decoder))), want: waiting},
		{name: "waiting-ratio-admits-the-step-keeps-waiting", obs: ratioBalanced(groupOf(bg, fourWide(rollingTo(engine, 5, 1, 0)), fourWide(rollingTo(decoder, 5, 1, 0))), 4, 4), want: waiting, skew: ptrBool(false)},
		{name: "waiting-under-rollingupdate", obs: groupOf(ru, rollingTo(engine, 3, 1, 0), settled(decoder)), want: waiting},
		{name: "waiting-partition-set-keeps-waiting", obs: groupOf(bg, withPartition(rollingTo(engine, 3, 1, 0), 1), settled(decoder)), want: waiting},
		{name: "waiting-partition-cleared-keeps-waiting", obs: groupOf(bg, withPartition(rollingTo(engine, 3, 1, 0), 0), settled(decoder)), want: waiting},
		{name: "waiting-only-rolling-member-missing-reads-idle", obs: groupOf(bg, missing(engine), settled(decoder)), want: idle},
		{name: "waiting-resync-keeps-waiting", obs: groupOf(bg, rollingTo(engine, 3, 1, 0), settled(decoder)), want: waiting},

		// Shifting: every new pod Ready, old pods remain.
		{name: "shifting-rebump-reads-surging", obs: groupOf(bg, retargeted(rollingTo(engine, 4, 2, 2)), settled(decoder)), want: surging},
		{name: "shifting-further-new-pod-reads-waiting", obs: groupOf(bg, rollingTo(engine, 5, 3, 2), settled(decoder)), want: waiting},
		{name: "shifting-another-ready-pod-keeps-shifting", obs: groupOf(bg, rollingTo(engine, 4, 2, 2), settled(decoder)), want: shift},
		{name: "shifting-new-pod-unready-reads-waiting", obs: groupOf(bg, rollingTo(engine, 4, 2, 1), settled(decoder)), want: waiting},
		{name: "shifting-new-pods-all-gone-reads-surging", obs: groupOf(bg, rollingTo(engine, 2, 0, 0), settled(decoder)), want: surging},
		{name: "shifting-old-capacity-lost-keeps-shifting", obs: groupOf(bg, edited(rollingTo(engine, 4, 2, 2), func(m *ComponentObservation) { m.ReadyPods, m.ServingPods = 2, 2 }), settled(decoder)), want: shift},
		{name: "shifting-dark-settled-member-reads-waiting", obs: groupOf(bg, rollingTo(engine, 4, 2, 2), darkened(settled(decoder))), want: waiting},
		{name: "shifting-dark-settled-member-under-rollingupdate-keeps-shifting", obs: groupOf(ru, rollingTo(engine, 2, 2, 2), darkened(settled(decoder))), want: shift},
		{name: "shifting-under-rollingupdate-reads-scalingdown", obs: groupOf(ru, rollingTo(engine, 4, 2, 2), settled(decoder)), want: scaling},
		{name: "shifting-under-independent", obs: groupOf(ind, rollingTo(engine, 4, 2, 2), settled(decoder)), want: shift},
		{name: "shifting-partition-set-keeps-shifting", obs: groupOf(bg, withPartition(rollingTo(engine, 4, 2, 2), 1), settled(decoder)), want: shift},
		{name: "shifting-partition-cleared-keeps-shifting", obs: groupOf(bg, withPartition(rollingTo(engine, 4, 2, 2), 0), settled(decoder)), want: shift},
		{name: "shifting-only-rolling-member-missing-reads-idle", obs: groupOf(bg, missing(engine), settled(decoder)), want: idle},
		{name: "shifting-resync-keeps-shifting", obs: groupOf(bg, rollingTo(engine, 4, 2, 2), settled(decoder)), want: shift},

		// ScalingDown: every new pod Ready, no old pod left, not promoted.
		{name: "scalingdown-rebump-reads-surging", obs: groupOf(bg, retargeted(rollingTo(engine, 2, 2, 2)), settled(decoder)), want: surging},
		{name: "scalingdown-further-new-pod-reads-waiting", obs: groupOf(bg, rollingTo(engine, 3, 3, 2), settled(decoder)), want: waiting},
		{name: "scalingdown-another-ready-pod-keeps-scalingdown", obs: groupOf(bg, rollingTo(engine, 2, 2, 2), settled(decoder)), want: scaling},
		{name: "scalingdown-new-pod-unready-reads-waiting", obs: groupOf(bg, rollingTo(engine, 2, 2, 1), settled(decoder)), want: waiting},
		{name: "scalingdown-new-pods-all-gone-reads-surging", obs: groupOf(bg, rollingTo(engine, 0, 0, 0), settled(decoder)), want: surging},
		{name: "scalingdown-dark-settled-member-reads-waiting", obs: groupOf(bg, rollingTo(engine, 2, 2, 2), darkened(settled(decoder))), want: waiting},
		{name: "scalingdown-under-rollingupdate-reads-shifting", obs: groupOf(ru, rollingTo(engine, 2, 2, 2), settled(decoder)), want: shift},
		{name: "scalingdown-under-rollingupdate-old-capacity-lost-keeps-scalingdown", obs: groupOf(ru, edited(rollingTo(engine, 4, 2, 2), func(m *ComponentObservation) { m.ReadyPods, m.ServingPods = 2, 2 }), settled(decoder)), want: scaling},
		{name: "scalingdown-partition-set-keeps-scalingdown", obs: groupOf(bg, withPartition(rollingTo(engine, 2, 2, 2), 1), settled(decoder)), want: scaling},
		{name: "scalingdown-partition-cleared-keeps-scalingdown", obs: groupOf(bg, withPartition(rollingTo(engine, 2, 2, 2), 0), settled(decoder)), want: scaling},
		{name: "scalingdown-only-rolling-member-missing-reads-idle", obs: groupOf(bg, missing(engine), settled(decoder)), want: idle},
		{name: "scalingdown-resync-keeps-scalingdown", obs: groupOf(bg, rollingTo(engine, 2, 2, 2), settled(decoder)), want: scaling},

		// Staged: the staged shape is re-judged against every fact.
		{name: "staged-rebump-reads-surging", obs: groupOf(bg, retargeted(stagedMember(engine)), settled(decoder)), want: surging},
		{name: "staged-released-instance-surges-reads-waiting", obs: groupOf(bg, edited(stagedMember(engine), func(m *ComponentObservation) { m.NewRevisionPods, m.TotalPods, m.AtDesiredShape = 2, 3, false }), settled(decoder)), want: waiting},
		{name: "staged-another-ready-pod-keeps-staged", obs: groupOf(bg, stagedMember(engine), settled(decoder)), want: stagedP},
		{name: "staged-new-pod-unready-reads-waiting", obs: groupOf(bg, edited(stagedMember(engine), func(m *ComponentObservation) { m.NewRevisionReadyPods, m.AtDesiredShape = 0, false }), settled(decoder)), want: waiting},
		{name: "staged-held-pod-unready-reads-shifting", obs: groupOf(bg, edited(stagedMember(engine), func(m *ComponentObservation) { m.ReadyPods, m.ServingPods, m.AtDesiredShape = 1, 1, false }), settled(decoder)), want: shift},
		{name: "staged-held-pods-gone-reads-scalingdown", obs: groupOf(bg, edited(stagedMember(engine), func(m *ComponentObservation) {
			m.TotalPods, m.ReadyPods, m.ServingPods, m.AtDesiredShape = 1, 1, 1, false
		}), settled(decoder)), want: scaling},
		{name: "staged-rollback-reads-idle", obs: groupOf(bg, withPartition(settled(engine), 1), settled(decoder)), want: idle},
		{name: "staged-dark-member-reads-waiting", obs: groupOf(bg, darkened(stagedMember(engine)), settled(decoder)), want: waiting},
		{name: "staged-under-rollingupdate", obs: groupOf(ru, stagedMember(engine), settled(decoder)), want: stagedP},
		{name: "staged-under-independent-reads-shifting", obs: groupOf(ind, stagedMember(engine), settled(decoder)), want: shift},
		{name: "staged-scale-up-reads-waiting", obs: groupOf(bg, edited(stagedMember(engine), func(m *ComponentObservation) {
			m.DesiredReplicas, m.NewRevisionPods, m.AtDesiredShape = 3, 2, false
		}), settled(decoder)), want: waiting},
		{name: "staged-scale-down-drops-the-target-rows-reads-surging", obs: groupOf(bg, edited(stagedMember(engine), func(m *ComponentObservation) {
			m.DesiredReplicas, m.TotalPods, m.NewRevisionPods, m.NewRevisionReadyPods, m.AtDesiredShape = 1, 1, 0, 0, false
		}), settled(decoder)), want: surging},
		{name: "staged-partition-moved-reads-shifting", obs: groupOf(bg, edited(stagedMember(engine), func(m *ComponentObservation) { m.Partition, m.AtDesiredShape = 2, false }), settled(decoder)), want: shift},
		{name: "staged-only-rolling-member-missing-reads-idle", obs: groupOf(bg, missing(engine), settled(decoder)), want: idle},
		{name: "staged-resync-keeps-staged", obs: groupOf(bg, stagedMember(engine), settled(decoder)), want: stagedP},

		// Failed: the rollup is judged first, so no other fact moves the
		// group until no row reads Failed.
		{name: "failed-surge-pod-elsewhere-keeps-failed", obs: groupOf(bg, failedMember(engine), rollingTo(decoder, 3, 1, 0)), want: failed},
		{name: "failed-ready-target-elsewhere-keeps-failed", obs: groupOf(bg, failedMember(engine), rollingTo(decoder, 4, 2, 2)), want: failed},
		{name: "failed-old-pods-gone-elsewhere-keeps-failed", obs: groupOf(bg, failedMember(engine), rollingTo(decoder, 2, 2, 2)), want: failed},
		{name: "failed-converged-elsewhere-keeps-failed", obs: groupOf(bg, failedMember(engine), settledOn(decoder, "rev2")), want: failed},
		{name: "failed-staged-elsewhere-keeps-failed", obs: groupOf(bg, failedMember(engine), stagedMember(decoder)), want: failed},
		{name: "failed-rollback-keeps-failed", obs: groupOf(bg, edited(settled(engine), func(m *ComponentObservation) { m.Failed = true }), settled(decoder)), want: failed},
		{name: "failed-capacity-lost-keeps-failed", obs: groupOf(bg, failedMember(engine), darkened(rollingTo(decoder, 3, 1, 0))), want: failed},
		{name: "failed-dark-member-keeps-failed", obs: groupOf(bg, darkened(failedMember(engine)), darkened(settled(decoder))), want: failed},
		{name: "failed-member-removed-recomputes-the-rest", obs: groupOf(bg, settled(decoder)), want: idle},
		{name: "failed-under-rollingupdate", obs: groupOf(ru, failedMember(engine), settled(decoder)), want: failed},
		{name: "failed-under-independent", obs: groupOf(ind, failedMember(engine), settled(decoder)), want: failed},
		{name: "failed-under-sequential", obs: groupOf(seq, settled(decoder), failedMember(engine)), want: failed, composite: v1beta1.CompositePhaseSequentialFailed},
		{name: "failed-partition-set-keeps-failed", obs: groupOf(bg, withPartition(failedMember(engine), 1), settled(decoder)), want: failed},
		{name: "failed-partition-cleared-keeps-failed", obs: groupOf(bg, withPartition(failedMember(engine), 0), settled(decoder)), want: failed},
		{name: "failed-member-missing-reads-the-rest", obs: groupOf(bg, missing(engine), settled(decoder)), want: idle},
		{name: "failed-resync-keeps-failed", obs: groupOf(bg, failedMember(engine), settled(decoder)), want: failed},

		// Paused: the pause is judged before any member fact.
		{name: "paused-soak-elapsed-keeps-paused", obs: pausedGroup(soaking(rollingTo(engine, 2, 0, 0), time.Hour)), want: paused},
		{name: "paused-value-change-keeps-paused", obs: pausedGroup(groupOf(bg, rollingTo(engine, 3, 1, 0), settled(decoder))), want: paused},
		{name: "paused-under-rollingupdate", obs: pausedGroup(groupOf(ru, rollingTo(engine, 3, 1, 0), settled(decoder))), want: paused},
		{name: "paused-partition-set-keeps-paused", obs: pausedGroup(groupOf(bg, withPartition(rollingTo(engine, 3, 1, 0), 1), settled(decoder))), want: paused},
		{name: "paused-partition-cleared-keeps-paused", obs: pausedGroup(groupOf(bg, withPartition(rollingTo(engine, 3, 1, 0), 0), settled(decoder))), want: paused},
		{name: "paused-member-missing-keeps-paused", obs: pausedGroup(groupOf(bg, missing(engine), settled(decoder))), want: paused},
		{name: "paused-counters-trailing-keep-paused", obs: pausedGroup(groupOf(bg, edited(rollingTo(engine, 3, 1, 0), func(m *ComponentObservation) { m.ReadyPods = 0 }), settled(decoder))), want: paused},
		{name: "paused-resync-keeps-paused", obs: pausedGroup(groupOf(bg, rollingTo(engine, 3, 1, 0), settled(decoder))), want: paused},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := ComputeTransition(tc.obs)
			if tr.Phase != tc.want {
				t.Fatalf("phase: got %q want %q (composite %q, message %q)", tr.Phase, tc.want, tr.CompositePhase, tr.Message)
			}
			if tc.composite != "" && tr.CompositePhase != tc.composite {
				t.Fatalf("composite: got %q want %q", tr.CompositePhase, tc.composite)
			}
			if tc.skew != nil && tr.RatioSkewRejected != *tc.skew {
				t.Fatalf("ratio skew: got %v want %v", tr.RatioSkewRejected, *tc.skew)
			}
		})
	}
}

// edited2 applies one edit to the named member of a group.
func edited2(obs GroupObservation, c v1beta1.ComponentType, edit func(ComponentObservation) ComponentObservation) GroupObservation {
	obs.Components[c] = edit(obs.Components[c])
	return obs
}

// fourWide widens a member to four desired, serving pods, the shape the
// ratio band is measured against.
func fourWide(m ComponentObservation) ComponentObservation {
	m.DesiredReplicas = 4
	m.ServingPods = 4
	m.ReadyPods = 4
	return m
}

func ptrBool(b bool) *bool { return &b }
