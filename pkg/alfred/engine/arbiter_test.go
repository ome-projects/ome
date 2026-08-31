package engine

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/policy/defrag"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/alfred/testutil"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

var testNow = testutil.ReferenceTime

// scenario: prod/a (2 GPUs) on node1, prod/b (2 GPUs) on node3, node2 empty.
func scenario() *testutil.SnapshotBuilder {
	return testutil.NewSnapshot().
		WithNode("node1", "h100", 8).
		WithNode("node2", "h100", 8).
		WithNode("node3", "h100", 8).
		WithInstance("prod/a", v1beta1.EngineComponent, constants.RawDeployment, "node1", 2).
		WithInstance("prod/b", v1beta1.EngineComponent, constants.RawDeployment, "node3", 2)
}

// cand is an executable surge-shaped defrag candidate for a scenario
// workload ("namespace/name"). Hints default to node2.
func cand(workload, from string, hints ...string) policy.Candidate {
	if len(hints) == 0 {
		hints = []string{"node2"}
	}
	parts := strings.SplitN(workload, "/", 2)
	if len(parts) != 2 {
		panic("workload must be namespace/name: " + workload)
	}
	nn := types.NamespacedName{Namespace: parts[0], Name: parts[1]}
	return policy.Candidate{
		Policy:          "defragmentation",
		Workload:        nn,
		Component:       v1beta1.EngineComponent,
		Instance:        0,
		Mode:            constants.RawDeployment,
		Reason:          policy.ReasonFragmentation,
		FromNode:        from,
		HintTargetNodes: hints,
		Executable:      true,
		SurgeShaped:     true,
		FootprintGPUs:   2,
		Score:           0.2,
	}
}

func health(workload, from string, hints ...string) policy.Candidate {
	c := cand(workload, from, hints...)
	c.Policy = "nodehealth"
	c.Reason = policy.ReasonNodeUnhealthy
	c.Score = 0.1 // deliberately below the defrag scores in tests
	return c
}

func admit(t *testing.T, a *Arbiter, snap *snapshot.ClusterSnapshot, cfg *config.Config, cands ...policy.Candidate) []Decision {
	t.Helper()
	if a.Ledger == nil {
		a.Ledger = NewLedger()
	}
	return a.Admit(snap, cands, cfg, testNow)
}

func decisionFor(t *testing.T, decisions []Decision, workload string) Decision {
	t.Helper()
	for _, d := range decisions {
		if d.Candidate.Workload.String() == workload {
			return d
		}
	}
	t.Fatalf("no decision for %s in %+v", workload, decisions)
	return Decision{}
}

func TestAdmitHappyPathAndAdvisoryBypass(t *testing.T) {
	advisory := cand("prod/b", "node3")
	advisory.Executable = false
	advisory.AdvisoryReason = "RawDeploymentMigrationUnsupported"
	advisory.FootprintGPUs = 8 // advisory metadata must never reserve this

	decisions := admit(t, &Arbiter{}, scenario().Build(), config.Default(),
		advisory, cand("prod/a", "node1"))

	if len(decisions) != 1 {
		t.Fatalf("advisories must not be arbitrated: %+v", decisions)
	}
	d := decisions[0]
	if !d.Admitted || d.Reason != "" || d.Target != "node2" || d.CooldownOverridden {
		t.Fatalf("Raw advisory must not enter arbitration or claim node2: %+v", d)
	}
}

func TestFourTargetOMENativeSurgePreservesPlacementProof(t *testing.T) {
	b := testutil.NewSnapshot()
	for i := 1; i <= 4; i++ {
		b.WithNode(fmt.Sprintf("source%d", i), "h100", 8)
		b.WithNode(fmt.Sprintf("target%d", i), "h100", 8)
		b.WithNode(fmt.Sprintf("blocker-source%d", i), "h100", 8)
		b.WithOtherOccupant(fmt.Sprintf("target%d", i), 7)
		b.WithInstance(fmt.Sprintf("prod/blocker-%d", i), v1beta1.EngineComponent,
			constants.RawDeployment, fmt.Sprintf("blocker-source%d", i), 1)
	}
	b.WithMultiPodInstance("prod/wide", v1beta1.EngineComponent, constants.OMENative, 1,
		"source1", "source2", "source3", "source4")
	snap := b.Build()
	cfg := config.Default()
	*cfg.Policies.Defragmentation.FragmentationThreshold = 0.01
	cfg.Policies.Defragmentation.Aggressiveness = config.AggressivenessAggressive

	var p defrag.Policy
	var wide policy.Candidate
	for _, c := range p.Evaluate(snap, cfg) {
		if c.Executable && c.Workload.String() == "prod/wide" {
			wide = c
			break
		}
	}
	if wide.Workload.Name == "" {
		t.Fatal("defrag policy did not emit the wide OMENative candidate")
	}
	wantHints := []string{"target1", "target2", "target3"}
	if len(wide.HintTargetNodes) != len(wantHints) {
		t.Fatalf("operator hints = %v, want %v", wide.HintTargetNodes, wantHints)
	}
	for i := range wantHints {
		if wide.HintTargetNodes[i] != wantHints[i] {
			t.Fatalf("operator hints = %v, want %v", wide.HintTargetNodes, wantHints)
		}
	}
	wantPlacementTargets := []string{"target1", "target2", "target3", "target4"}
	if len(wide.PlacementTargetNodes) < len(wantPlacementTargets) {
		t.Fatalf("placement targets = %v, want at least %v", wide.PlacementTargetNodes, wantPlacementTargets)
	}
	for i := range wantPlacementTargets {
		if wide.PlacementTargetNodes[i] != wantPlacementTargets[i] {
			t.Fatalf("placement targets = %v, want prefix %v", wide.PlacementTargetNodes, wantPlacementTargets)
		}
	}
	for _, target := range append(append([]string(nil), wide.HintTargetNodes...), wide.PlacementTargetNodes...) {
		if strings.HasPrefix(target, "source") {
			t.Fatalf("source node leaked into target lists: %+v", wide)
		}
	}

	wide.Score = 10 // keep the proof-producing candidate first in arbitration
	candidates := []policy.Candidate{wide}
	for i := 1; i <= 4; i++ {
		blocker := cand(fmt.Sprintf("prod/blocker-%d", i), fmt.Sprintf("blocker-source%d", i), fmt.Sprintf("target%d", i))
		blocker.Score = 0.01
		candidates = append(candidates, blocker)
	}
	decisions := admit(t, &Arbiter{}, snap, cfg, candidates...)
	wideDecision := decisionFor(t, decisions, "prod/wide")
	if !wideDecision.Admitted || wideDecision.Target != "target1" {
		t.Fatalf("four-target placement proof must be admitted: %+v", wideDecision)
	}
	for i := 1; i <= 4; i++ {
		d := decisionFor(t, decisions, fmt.Sprintf("prod/blocker-%d", i))
		if d.Admitted || d.Reason != RejectTargetNodeBusy {
			t.Fatalf("target%d must be claimed by the wide surge: %+v", i, d)
		}
	}
}

func TestDefragReroutesUsingPlacementAlternatives(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("source", "h100", 8).
		WithNode("target1", "h100", 8).
		WithNode("target2", "h100", 8).
		WithNode("blocker-source", "h100", 8).
		WithInstance("prod/mover", v1beta1.EngineComponent, constants.OMENative, "source", 1).
		WithInstance("prod/blocker", v1beta1.EngineComponent, constants.RawDeployment, "blocker-source", 1)
	b.WithOtherOccupant("target1", 7)
	b.WithOtherOccupant("target2", 7)
	snap := b.Build()
	cfg := config.Default()
	*cfg.Policies.Defragmentation.FragmentationThreshold = 0.01
	cfg.Policies.Defragmentation.Aggressiveness = config.AggressivenessAggressive

	var p defrag.Policy
	var mover policy.Candidate
	for _, c := range p.Evaluate(snap, cfg) {
		if c.Executable && c.Workload.String() == "prod/mover" {
			mover = c
			break
		}
	}
	if mover.Workload.Name == "" {
		t.Fatal("defrag policy did not emit the mover candidate")
	}
	if len(mover.PlacementTargetNodes) < 2 || mover.PlacementTargetNodes[0] != "target1" || mover.PlacementTargetNodes[1] != "target2" {
		t.Fatalf("placement alternatives = %v, want target1 then target2", mover.PlacementTargetNodes)
	}

	blocker := cand("prod/blocker", "blocker-source", "target1")
	blocker.Score = mover.Score + 1
	decisions := admit(t, &Arbiter{}, snap, cfg, mover, blocker)
	if d := decisionFor(t, decisions, "prod/blocker"); !d.Admitted || d.Target != "target1" {
		t.Fatalf("higher-priority move must consume target1: %+v", d)
	}
	if d := decisionFor(t, decisions, "prod/mover"); !d.Admitted || d.Target != "target2" {
		t.Fatalf("defrag must reroute to its second placement alternative: %+v", d)
	}
}

func TestHealthPreemptsDefragOnSameWorkload(t *testing.T) {
	defrag := cand("prod/a", "node1")
	defrag.Score = 5.0 // even a much better defrag score must lose

	decisions := admit(t, &Arbiter{}, scenario().Build(), config.Default(),
		defrag, health("prod/a", "node1"))

	if decisions[0].Candidate.Reason != policy.ReasonNodeUnhealthy || !decisions[0].Admitted {
		t.Fatalf("health must be arbitrated first and admitted: %+v", decisions)
	}
	if decisions[1].Admitted || decisions[1].Reason != RejectWorkloadBusy {
		t.Fatalf("defrag on the same workload must lose: %+v", decisions[1])
	}
}

func TestDefragNeverLandsOnEvacuatingNode(t *testing.T) {
	// prod/b is being evacuated off node3; defrag wants to move prod/a
	// onto node3.
	snap := scenario().Build()
	cfg := config.Default()

	blocked := admit(t, &Arbiter{}, snap, cfg,
		health("prod/b", "node3"), cand("prod/a", "node1", "node3"))
	d := decisionFor(t, blocked, "prod/a")
	if d.Admitted || d.Reason != RejectTargetUnderEvac {
		t.Fatalf("move toward an evacuating node must be rejected: %+v", d)
	}

	// With an alternative hint the move proceeds — around both the
	// evacuation and the node the evacuation itself lands on (node2).
	four := scenario().WithNode("node4", "h100", 8).Build()
	rerouted := admit(t, &Arbiter{}, four, cfg,
		health("prod/b", "node3"), cand("prod/a", "node1", "node3", "node2", "node4"))
	if d := decisionFor(t, rerouted, "prod/b"); !d.Admitted || d.Target != "node2" {
		t.Fatalf("evacuation must land on node2: %+v", d)
	}
	d = decisionFor(t, rerouted, "prod/a")
	if !d.Admitted || d.Target != "node4" {
		t.Fatalf("defrag must reroute around the evacuation and its landing: %+v", d)
	}
}

func TestWorkloadMutualExclusion(t *testing.T) {
	b := scenario().
		WithInstance("prod/a", v1beta1.EngineComponent, constants.RawDeployment, "node1", 2)
	second := cand("prod/a", "node1")
	second.Instance = 1
	second.Score = 0.1

	decisions := admit(t, &Arbiter{}, b.Build(), config.Default(),
		cand("prod/a", "node1"), second)
	if !decisions[0].Admitted {
		t.Fatalf("first candidate must be admitted: %+v", decisions[0])
	}
	if decisions[1].Admitted || decisions[1].Reason != RejectWorkloadBusy {
		t.Fatalf("second candidate on one workload must lose the cycle: %+v", decisions[1])
	}

	inFlight := scenario().ConfigureWorkload("prod/a", func(w *snapshot.Workload) {
		w.ActiveMigrations = []snapshot.InFlight{{UUID: "u1", Component: v1beta1.EngineComponent, RequestedAt: testNow.Add(-2 * time.Minute)}}
	})
	d := admit(t, &Arbiter{}, inFlight.Build(), config.Default(), cand("prod/a", "node1"))[0]
	if d.Admitted || d.Reason != RejectWorkloadBusy {
		t.Fatalf("an in-flight migration must exclude the workload: %+v", d)
	}
}

func TestTargetNodeExclusionPerCycle(t *testing.T) {
	// node2 has room for both 2-GPU replacements; the per-target rule
	// still allows only one landing per cycle.
	decisions := admit(t, &Arbiter{}, scenario().Build(), config.Default(),
		cand("prod/a", "node1"), cand("prod/b", "node3"))
	if !decisions[0].Admitted || decisions[0].Target != "node2" {
		t.Fatalf("first landing on node2 must be admitted: %+v", decisions[0])
	}
	if decisions[1].Admitted || decisions[1].Reason != RejectTargetNodeBusy {
		t.Fatalf("second landing on node2 in one cycle must be rejected: %+v", decisions[1])
	}
}

func TestTerminatingInstanceExcluded(t *testing.T) {
	b := scenario().ConfigureWorkload("prod/a", func(w *snapshot.Workload) {
		w.Components[v1beta1.EngineComponent].Instances[0].Pods[0].Terminating = true
	})
	d := admit(t, &Arbiter{}, b.Build(), config.Default(), cand("prod/a", "node1"))[0]
	if d.Admitted || d.Reason != RejectTerminating {
		t.Fatalf("terminating instance must never be acted on: %+v", d)
	}
}

func TestInstanceGone(t *testing.T) {
	missing := cand("prod/a", "node1")
	missing.Instance = 7
	d := admit(t, &Arbiter{}, scenario().Build(), config.Default(), missing)[0]
	if d.Admitted || d.Reason != RejectInstanceGone {
		t.Fatalf("candidate for a vanished instance: %+v", d)
	}
}

func TestClassAwareWorkloadCooldown(t *testing.T) {
	cfg := config.Default()
	migrated := func(age time.Duration) *snapshot.ClusterSnapshot {
		at := testNow.Add(-age)
		return scenario().ConfigureWorkload("prod/a", func(w *snapshot.Workload) {
			w.LastMigration = &at
		}).Build()
	}

	d := admit(t, &Arbiter{}, migrated(10*time.Minute), cfg, cand("prod/a", "node1"))[0]
	if d.Admitted || d.Reason != RejectCooldown {
		t.Fatalf("defrag inside the 30m window must wait: %+v", d)
	}

	d = admit(t, &Arbiter{}, migrated(10*time.Minute), cfg, health("prod/a", "node1"))[0]
	if !d.Admitted || !d.CooldownOverridden {
		t.Fatalf("health past the 5m floor must be admitted with the override audited: %+v", d)
	}

	d = admit(t, &Arbiter{}, migrated(3*time.Minute), cfg, health("prod/a", "node1"))[0]
	if d.Admitted || d.Reason != RejectCooldown {
		t.Fatalf("inside the floor even health waits: %+v", d)
	}

	d = admit(t, &Arbiter{}, migrated(31*time.Minute), cfg, cand("prod/a", "node1"))[0]
	if !d.Admitted || d.CooldownOverridden {
		t.Fatalf("defrag past the standard window is clean: %+v", d)
	}
}

func TestPlacementCooldownAuthorshipBlind(t *testing.T) {
	cfg := config.Default()
	placed := func(age time.Duration) *snapshot.ClusterSnapshot {
		at := testNow.Add(-age)
		return scenario().ConfigureWorkload("prod/a", func(w *snapshot.Workload) {
			w.Components[v1beta1.EngineComponent].Instances[0].Pods[0].StartTime = &at
		}).Build()
	}

	d := admit(t, &Arbiter{}, placed(5*time.Minute), cfg, cand("prod/a", "node1"))[0]
	if d.Admitted || d.Reason != RejectPlacementCooldown {
		t.Fatalf("a freshly placed instance must settle first: %+v", d)
	}
	d = admit(t, &Arbiter{}, placed(6*time.Minute), cfg, health("prod/a", "node1"))[0]
	if !d.Admitted {
		t.Fatalf("health waits only the floor on placement: %+v", d)
	}
	d = admit(t, &Arbiter{}, placed(2*time.Minute), cfg, health("prod/a", "node1"))[0]
	if d.Admitted || d.Reason != RejectPlacementCooldown {
		t.Fatalf("inside the floor even health waits on placement: %+v", d)
	}
}

func TestPerNodeCooldown(t *testing.T) {
	cfg := config.Default()
	snap := scenario().Build()

	// node1 was a landing target 5 minutes ago.
	ledger := NewLedger()
	ledger.RecordDispatch(DispatchRecord{
		Workload: types.NamespacedName{Namespace: "prod", Name: "x"},
		FromNode: "node9", Target: "node1", GPUs: 2, At: testNow.Add(-5 * time.Minute),
	})

	// Source side: a routine move may not pull from the cooling node.
	d := admit(t, &Arbiter{Ledger: ledger}, snap, cfg, cand("prod/a", "node1"))[0]
	if d.Admitted || d.Reason != RejectNodeCooldown {
		t.Fatalf("defrag from a cooling node must wait: %+v", d)
	}
	// Health evacuation is exempt as a source — draining is the point.
	d = admit(t, &Arbiter{Ledger: ledger}, snap, cfg, health("prod/a", "node1"))[0]
	if !d.Admitted {
		t.Fatalf("health evacuation must be exempt from source cooling: %+v", d)
	}

	// Target side: nothing lands on the cooling node.
	d = admit(t, &Arbiter{Ledger: ledger}, snap, cfg, cand("prod/b", "node3", "node1"))[0]
	if d.Admitted || d.Reason != RejectNodeCooldown {
		t.Fatalf("landing on a cooling node must be rejected: %+v", d)
	}
}

func TestGlobalCaps(t *testing.T) {
	cfg := config.Default()

	// In-flight cap: three running migrations saturate the default cap.
	b := scenario().WithNode("node4", "h100", 8)
	for i, w := range []string{"prod/m1", "prod/m2", "prod/m3"} {
		b.WithInstance(w, v1beta1.EngineComponent, constants.RawDeployment, "node4", 1).
			ConfigureWorkload(w, func(w *snapshot.Workload) {
				w.ActiveMigrations = []snapshot.InFlight{{UUID: fmt.Sprintf("u%d", i), RequestedAt: testNow.Add(-5 * time.Minute)}}
			})
	}
	d := admit(t, &Arbiter{}, b.Build(), cfg, cand("prod/a", "node1"))[0]
	if d.Admitted || d.Reason != RejectInFlightCap {
		t.Fatalf("in-flight cap must bind: %+v", d)
	}

	// Hourly cap: ten ledger dispatches inside the hour saturate it; the
	// same ten aged past the hour do not.
	saturated, expired := NewLedger(), NewLedger()
	for i := 0; i < 10; i++ {
		rec := DispatchRecord{
			Workload: types.NamespacedName{Namespace: "prod", Name: fmt.Sprintf("w%d", i)},
			Target:   "node9", GPUs: 1,
		}
		rec.At = testNow.Add(-30 * time.Minute)
		saturated.RecordDispatch(rec)
		rec.At = testNow.Add(-61 * time.Minute)
		expired.RecordDispatch(rec)
	}
	d = admit(t, &Arbiter{Ledger: saturated}, scenario().Build(), cfg, cand("prod/a", "node1"))[0]
	if d.Admitted || d.Reason != RejectHourlyCap {
		t.Fatalf("hourly cap must bind: %+v", d)
	}
	d = admit(t, &Arbiter{Ledger: expired}, scenario().Build(), cfg, cand("prod/a", "node1"))[0]
	if !d.Admitted {
		t.Fatalf("dispatches older than an hour must not count: %+v", d)
	}
}

func TestCircuitBreaker(t *testing.T) {
	ledger := NewLedger()
	for _, ok := range []bool{true, false, false, false} {
		ledger.RecordOutcome(ok, testNow.Add(-time.Minute))
	}
	if !ledger.BreakerOpen(testNow) {
		t.Fatal("3 failures in 4 outcomes must trip the breaker")
	}
	d := admit(t, &Arbiter{Ledger: ledger}, scenario().Build(), config.Default(), cand("prod/a", "node1"))[0]
	if d.Admitted || d.Reason != RejectCircuitBreakerOpen {
		t.Fatalf("open breaker must pause all execution: %+v", d)
	}
	if ledger.BreakerOpen(testNow.Add(61 * time.Minute)) {
		t.Fatal("breaker must release after the pause")
	}

	// The trip cleared the window: the first post-resume outcome must be
	// judged fresh, not against the stale failures that tripped it.
	resumed := testNow.Add(61 * time.Minute)
	ledger.RecordOutcome(true, resumed)
	if ledger.BreakerOpen(resumed.Add(time.Minute)) {
		t.Fatal("stale failures must not re-trip the breaker after the pause")
	}

	fresh := NewLedger()
	fresh.RecordOutcome(false, testNow)
	fresh.RecordOutcome(false, testNow)
	if fresh.BreakerOpen(testNow) {
		t.Fatal("two early failures must not freeze execution for an hour")
	}
}

func TestAutoscalerDefer(t *testing.T) {
	snap := scenario().Build()
	cfg := config.Default()
	for _, intent := range []AutoscalerIntent{IntentScaling, IntentUnknown} {
		a := &Arbiter{AutoscalerIntent: func(*snapshot.ClusterSnapshot, string) AutoscalerIntent {
			return intent
		}}
		d := admit(t, a, snap, cfg, cand("prod/a", "node1"))[0]
		if d.Admitted || d.Reason != RejectAutoscalerActive {
			t.Fatalf("intent %v must defer the workload: %+v", intent, d)
		}
	}
	idle := &Arbiter{AutoscalerIntent: func(*snapshot.ClusterSnapshot, string) AutoscalerIntent {
		return IntentIdle
	}}
	if d := admit(t, idle, snap, cfg, cand("prod/a", "node1"))[0]; !d.Admitted {
		t.Fatalf("idle autoscaler must not defer: %+v", d)
	}
}

func TestCapacityNetOfInFlightClaims(t *testing.T) {
	// prod/b's still-running migration claims 7 of node2's 8 free GPUs;
	// the record is old enough to be outside the node cooldown but well
	// inside the claim window, and the workload's migration is still
	// visible in the snapshot so the claim is not absorbed.
	b := scenario().ConfigureWorkload("prod/b", func(w *snapshot.Workload) {
		w.ActiveMigrations = []snapshot.InFlight{{UUID: "u1", RequestedAt: testNow.Add(-30 * time.Minute)}}
	})
	ledger := NewLedger()
	ledger.RecordDispatch(DispatchRecord{
		Workload: types.NamespacedName{Namespace: "prod", Name: "b"},
		Target:   "node2", GPUs: 7, At: testNow.Add(-30 * time.Minute),
	})
	d := admit(t, &Arbiter{Ledger: ledger}, b.Build(), config.Default(), cand("prod/a", "node1"))[0]
	if d.Admitted || d.Reason != RejectNoCapacity {
		t.Fatalf("claimed capacity must count as allocated: %+v", d)
	}
}

func TestArbiterEnforcesOptOutAndMalformedRequests(t *testing.T) {
	cfg := config.Default()

	optOut := scenario().ConfigureWorkload("prod/a", func(w *snapshot.Workload) {
		w.Movable = false
	})
	d := admit(t, &Arbiter{}, optOut.Build(), cfg, cand("prod/a", "node1"))[0]
	if d.Admitted || d.Reason != RejectNotMovable {
		t.Fatalf("an opted-out workload must be rejected regardless of policy: %+v", d)
	}

	malformed := scenario().ConfigureWorkload("prod/a", func(w *snapshot.Workload) {
		w.MalformedRequests = map[string]string{"bad-1": "not json"}
	})
	d = admit(t, &Arbiter{}, malformed.Build(), cfg, cand("prod/a", "node1"))[0]
	if d.Admitted || d.Reason != RejectMalformedRequest {
		t.Fatalf("a pending malformed request must hold the workload: %+v", d)
	}
}

// TestSurgeWithoutPodDetailStillClaims: a pluggable policy may declare a
// footprint without per-pod GPU detail; the capacity gate must still run and
// the admission must still claim its block.
func TestSurgeWithoutPodDetailStillClaims(t *testing.T) {
	b := scenario().
		WithInstance("prod/z", v1beta1.EngineComponent, constants.RawDeployment, "node1", 0)
	ghost := cand("prod/z", "node1")
	ghost.FootprintGPUs = 2

	decisions := admit(t, &Arbiter{}, b.Build(), config.Default(), ghost, cand("prod/b", "node3"))
	if !decisions[0].Admitted || decisions[0].Target != "node2" {
		t.Fatalf("declared footprint must pass the capacity gate and claim a target: %+v", decisions[0])
	}
	if decisions[1].Admitted || decisions[1].Reason != RejectTargetNodeBusy {
		t.Fatalf("the claim must be visible to the next candidate: %+v", decisions[1])
	}
}

func TestEvictionNeedsNoHeadroom(t *testing.T) {
	b := scenario()
	b.WithOtherOccupant("node2", 8) // every hint is full
	evict := cand("prod/a", "node1")
	evict.SurgeShaped = false
	d := admit(t, &Arbiter{}, b.Build(), config.Default(), evict)[0]
	if !d.Admitted || d.Target != "" {
		t.Fatalf("free-then-place cannot deadlock and needs no claim: %+v", d)
	}
}

func TestLedgerAbsorbAndClaims(t *testing.T) {
	ledger := NewLedger()
	ledger.RecordDispatch(DispatchRecord{
		Workload: types.NamespacedName{Namespace: "prod", Name: "a"},
		Target:   "node2", GPUs: 4, At: testNow.Add(-20 * time.Minute),
	})
	if got := ledger.ActiveClaims()["node2"]; got != 4 {
		t.Fatalf("claim = %d, want 4", got)
	}

	// The workload's migration completed after the dispatch: the claim is
	// absorbed into real occupancy, but the hourly ledger still counts it.
	done := testNow.Add(-10 * time.Minute)
	snap := scenario().ConfigureWorkload("prod/a", func(w *snapshot.Workload) {
		w.LastMigration = &done
	}).Build()
	ledger.AbsorbSnapshot(snap, testNow)
	if got := ledger.ActiveClaims()["node2"]; got != 0 {
		t.Fatalf("absorbed claim must release: %d", got)
	}
	if got := ledger.DispatchesWithinHour(testNow); got != 1 {
		t.Fatalf("absorbed dispatch must still count hourly: %d", got)
	}

	// Past retention the record disappears entirely.
	ledger.AbsorbSnapshot(snap, testNow.Add(2*time.Hour))
	if got := ledger.DispatchesWithinHour(testNow.Add(2 * time.Hour)); got != 0 {
		t.Fatalf("expired record must drop: %d", got)
	}
}

// TestLedgerReleasesClaimsOfDeletedWorkloads: a dispatch record whose
// workload vanished from the cluster stops claiming capacity (the migration
// is moot) but still counts toward the hourly ledger.
func TestLedgerReleasesClaimsOfDeletedWorkloads(t *testing.T) {
	ledger := NewLedger()
	ledger.RecordDispatch(DispatchRecord{
		Workload: types.NamespacedName{Namespace: "prod", Name: "ghost"},
		Target:   "node2", GPUs: 4, At: testNow.Add(-20 * time.Minute),
	})
	ledger.AbsorbSnapshot(scenario().Build(), testNow)
	if got := ledger.ActiveClaims()["node2"]; got != 0 {
		t.Fatalf("deleted workload's claim must release: %d", got)
	}
	if got := ledger.DispatchesWithinHour(testNow); got != 1 {
		t.Fatalf("the dispatch must still count hourly: %d", got)
	}
}

// TestNodeCoolingIgnoresEmptyName: eviction records carry an empty Target;
// querying an empty node name must not match them.
func TestNodeCoolingIgnoresEmptyName(t *testing.T) {
	ledger := NewLedger()
	ledger.RecordDispatch(DispatchRecord{
		Workload: types.NamespacedName{Namespace: "prod", Name: "a"},
		FromNode: "node1", Target: "", At: testNow.Add(-time.Minute),
	})
	if ledger.NodeCooling("", 10*time.Minute, testNow) {
		t.Fatal("an empty node name must never read as cooling")
	}
	if !ledger.NodeCooling("node1", 10*time.Minute, testNow) {
		t.Fatal("the defrag source must still cool")
	}
}
