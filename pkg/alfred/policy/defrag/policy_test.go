package defrag

import (
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/alfred/testutil"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

const (
	rawMigrationUnsupportedReason     = "RawDeploymentMigrationUnsupported"
	omenativeObservationInvalidReason = "OMENativeObservationInvalid"
	omenativeStateIneligibleReason    = "OMENativeStateIneligible"
	nonExecutableFragmentationReason  = "NonExecutableObservedFragmentation"
)

// makeOMENativeSteady fills the exact controller-owned state an OMENative
// component must expose before Alfred may ask its executor to migrate one
// Instance. Tests mutate one dimension from this literal steady baseline.
func makeOMENativeSteady(t *testing.T, snap *snapshot.ClusterSnapshot, workload string) *snapshot.ClusterSnapshot {
	t.Helper()
	parts := types.NamespacedName{Namespace: "prod", Name: workload}
	w := snap.Workloads[parts]
	if w == nil {
		t.Fatalf("workload %s missing", parts.String())
	}
	comp := w.Components[v1beta1.EngineComponent]
	if comp == nil || comp.DeploymentMode != constants.OMENative {
		t.Fatalf("OMENative engine component missing for %s", parts.String())
	}
	replicas := int32(len(comp.Instances))
	const revision = "revision-1"
	comp.IR = &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Generation: 1},
		Spec: v1beta1.InferenceReplicaSpec{
			Replicas: &replicas,
		},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration:   1,
			Replicas:             replicas,
			ReadyReplicas:        replicas,
			ServingReplicas:      replicas,
			AvailableReplicas:    replicas,
			UpdatedReplicas:      replicas,
			UpdatedReadyReplicas: replicas,
			CurrentRevision:      revision,
			UpdateRevision:       revision,
		},
	}
	comp.StatusFresh = true
	comp.ObservationValid = true
	for _, inst := range comp.Instances {
		inst.Phase = v1beta1.OMENativeInstanceReady
		inst.RunningRevision = revision
		inst.TargetRevision = revision
		inst.Admitted = true
		inst.DesiredPods = int32(len(inst.Pods))
		inst.StatusPods = int32(len(inst.Pods))
		inst.ObservedPods = int32(len(inst.Pods))
		inst.ReadyPods = int32(len(inst.Pods))
		inst.ServingPods = int32(len(inst.Pods))
		inst.AvailablePods = int32(len(inst.Pods))
		inst.ObservationValid = true
		inst.Operation = nil
		for i := range inst.Pods {
			inst.Pods[i].Ready = true
			inst.Pods[i].Terminating = false
		}
	}
	w.Movable = true
	w.MigrationStateValid = true
	w.MalformedRequests = nil
	w.ActiveMigrations = nil
	w.LastMigration = nil
	return snap
}

// consolidationBuilder is the canonical single-move scenario: prod/mover's
// 1-GPU pod on node1 (7 free) can fill node2's hole (1 free), freeing a full
// 8-GPU node. Observed F = 0.2175, best F = 0 (the pinScenario numbers).
func consolidationBuilder() *testutil.SnapshotBuilder {
	b := testutil.NewSnapshot().
		WithNode("node1", "h100", 8).
		WithNode("node2", "h100", 8).
		WithInstance("prod/mover", v1beta1.EngineComponent, constants.OMENative, "node1", 1)
	b.WithOtherOccupant("node2", 7)
	return b
}

// lowGate returns a config whose gate the consolidation scenario passes
// (0.2175 sits below the default 0.25 threshold by design — the default gate
// is deliberately conservative).
func lowGate() *config.Config {
	cfg := config.Default()
	*cfg.Policies.Defragmentation.FragmentationThreshold = 0.1
	return cfg
}

func evaluate(t *testing.T, snap *snapshot.ClusterSnapshot, cfg *config.Config) []policy.Candidate {
	t.Helper()
	var p Policy
	if p.Name() != PolicyName {
		t.Fatalf("policy name = %q", p.Name())
	}
	return p.Evaluate(snap, cfg)
}

func executables(cands []policy.Candidate) []policy.Candidate {
	var out []policy.Candidate
	for _, c := range cands {
		if c.Executable {
			out = append(out, c)
		}
	}
	return out
}

func advisories(cands []policy.Candidate, reason string) []policy.Candidate {
	var out []policy.Candidate
	for _, c := range cands {
		if !c.Executable && c.AdvisoryReason == reason {
			out = append(out, c)
		}
	}
	return out
}

// TestGate: below the threshold (or disabled) the policy returns nothing —
// Alfred stays quiescent on a cluster it cannot improve.
func TestGate(t *testing.T) {
	snap := consolidationBuilder().Build()

	if got := evaluate(t, snap, config.Default()); got != nil {
		// 0.2175 < default 0.25: the reclaimable shape alone must not
		// wake the policy at default settings.
		t.Fatalf("below-threshold Evaluate = %v, want nil", got)
	}

	cfg := lowGate()
	*cfg.Policies.Defragmentation.Enabled = false
	if got := evaluate(t, snap, cfg); got != nil {
		t.Fatalf("disabled Evaluate = %v, want nil", got)
	}
}

// TestConsolidationCandidate pins the whole pipeline on the canonical move:
// OMENative surge-shaped, benefit = the full reclaimable 0.2175, cost =
// surge, hints = the hole it should fill.
func TestConsolidationCandidate(t *testing.T) {
	cands := evaluate(t, consolidationBuilder().Build(), lowGate())
	execs := executables(cands)
	if len(execs) != 1 {
		t.Fatalf("want exactly one executable candidate, got %+v", cands)
	}
	c := execs[0]
	if c.Workload.String() != "prod/mover" || c.Component != v1beta1.EngineComponent || c.Instance != 0 {
		t.Fatalf("candidate identity: %+v", c)
	}
	if c.Mode != constants.OMENative || !c.SurgeShaped {
		t.Fatalf("eligible OMENative must simulate place-then-free surge: %+v", c)
	}
	if c.FromNode != "node1" {
		t.Fatalf("FromNode = %q, want node1", c.FromNode)
	}
	if len(c.HintTargetNodes) != 1 || c.HintTargetNodes[0] != "node2" {
		t.Fatalf("hints = %v, want [node2] (fill the hole)", c.HintTargetNodes)
	}
	almost(t, "Benefit", c.Benefit, 0.2175)
	almost(t, "Cost", c.Cost, costOMENativeSurge)
	almost(t, "Score", c.Score, 0.2175-costOMENativeSurge)
	if c.Emergency {
		t.Fatal("no pending pod, must not be an emergency")
	}
	if c.FootprintGPUs != 1 {
		t.Fatalf("FootprintGPUs = %d, want 1", c.FootprintGPUs)
	}
}

// TestNoSurgeHeadroomAdvisory: an 8-GPU OMENative instance with no 8-free
// target must not be dispatched — it degrades to advisory instead of
// stalling in SurgePending until timeout.
func TestNoSurgeHeadroomAdvisory(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("node1", "h100", 8).
		WithNode("node2", "h100", 8).
		WithNode("node3", "h100", 8).
		WithInstance("prod/big", v1beta1.EngineComponent, constants.OMENative, "node1", 8).
		WithInstance("prod/mover", v1beta1.EngineComponent, constants.OMENative, "node2", 1)
	b.WithOtherOccupant("node3", 7)
	cfg := lowGate()
	*cfg.Policies.Defragmentation.FragmentationThreshold = 0.05

	cands := evaluate(t, b.Build(), cfg)
	blocked := advisories(cands, policy.AdvisoryNoSurgeHeadroom)
	if len(blocked) != 1 {
		t.Fatalf("want one NoSurgeHeadroom advisory, got %+v", cands)
	}
	if blocked[0].Workload.String() != "prod/big" || blocked[0].FromNode != "node1" ||
		blocked[0].FootprintGPUs != 8 || !blocked[0].SurgeShaped {
		t.Fatalf("advisory shape: %+v", blocked[0])
	}
	execs := executables(cands)
	if len(execs) != 1 || execs[0].Workload.String() != "prod/mover" {
		t.Fatalf("the small consolidation move must still be executable: %+v", cands)
	}
}

func TestMultiPodOMENativeCandidateIsSurgeShaped(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("source1", "h100", 8).
		WithNode("source2", "h100", 8).
		WithNode("target1", "h100", 8).
		WithNode("target2", "h100", 8).
		WithMultiPodInstance("prod/wide", v1beta1.EngineComponent, constants.OMENative, 1, "source1", "source2")
	b.WithOtherOccupant("target1", 7)
	b.WithOtherOccupant("target2", 7)
	cfg := lowGate()
	cfg.Policies.Defragmentation.Aggressiveness = config.AggressivenessAggressive
	*cfg.Policies.Defragmentation.FragmentationThreshold = 0.01

	execs := executables(evaluate(t, b.Build(), cfg))
	if len(execs) != 1 {
		t.Fatalf("want one executable multi-pod Instance, got %+v", execs)
	}
	c := execs[0]
	if !c.SurgeShaped || c.FootprintGPUs != 2 || c.Score <= 0 {
		t.Fatalf("multi-pod OMENative must be a positive place-then-free surge: %+v", c)
	}
	for _, target := range c.HintTargetNodes {
		if target == "source1" || target == "source2" {
			t.Fatalf("every current source node must be excluded from hints: %+v", c)
		}
	}
}

// TestEnumerationFilters: cooldown (with override), in-flight migrations,
// and Movable=false all keep a workload out of the candidate set.
func TestEnumerationFilters(t *testing.T) {
	cfg := lowGate()

	recent := testutil.ReferenceTime.Add(-5 * time.Minute)
	b := consolidationBuilder().ConfigureWorkload("prod/mover", func(w *snapshot.Workload) {
		w.LastMigration = &recent
	})
	if got := executables(evaluate(t, b.Build(), cfg)); len(got) != 0 {
		t.Fatalf("workload inside the 30m cooldown must not produce candidates: %+v", got)
	}

	short := time.Minute
	b = consolidationBuilder().ConfigureWorkload("prod/mover", func(w *snapshot.Workload) {
		w.LastMigration = &recent
		w.CooldownOverride = &short
	})
	if got := executables(evaluate(t, b.Build(), cfg)); len(got) != 1 {
		t.Fatalf("cooldown override must re-admit the workload: %+v", got)
	}

	b = consolidationBuilder().ConfigureWorkload("prod/mover", func(w *snapshot.Workload) {
		w.ActiveMigrations = []snapshot.InFlight{{UUID: "u1", Component: v1beta1.EngineComponent}}
	})
	if got := executables(evaluate(t, b.Build(), cfg)); len(got) != 0 {
		t.Fatalf("in-flight migration must exclude the workload: %+v", got)
	}

	b = consolidationBuilder().ConfigureWorkload("prod/mover", func(w *snapshot.Workload) {
		w.Movable = false
	})
	if got := executables(evaluate(t, b.Build(), cfg)); len(got) != 0 {
		t.Fatalf("Movable=false must exclude the workload: %+v", got)
	}
}

// withBystander adds a third node carrying an extra workload without
// disturbing the mover's consolidation shape (node1 must still reach 8 free).
func withBystander(workload string, mode constants.DeploymentModeType) *testutil.SnapshotBuilder {
	return consolidationBuilder().
		WithNode("node3", "h100", 8).
		WithInstance(workload, v1beta1.EngineComponent, mode, "node3", 1)
}

// bystanderGate: the third node dilutes the pool, so the reclaimable share
// shrinks below the test gate of 0.1; open the gate further.
func bystanderGate() *config.Config {
	cfg := lowGate()
	*cfg.Policies.Defragmentation.FragmentationThreshold = 0.05
	cfg.Policies.Defragmentation.Aggressiveness = config.AggressivenessAggressive
	return cfg
}

// TestLWSAdvisory: LWS-backed components surface as advisory-only, and the
// lwsRecommendationsEnabled switch silences them.
func TestLWSAdvisory(t *testing.T) {
	build := func() *snapshot.ClusterSnapshot {
		return withBystander("prod/lws", constants.MultiNode).Build()
	}
	cfg := bystanderGate()

	cands := evaluate(t, build(), cfg)
	lws := advisories(cands, policy.AdvisoryLWSMigrationUnsupported)
	if len(lws) != 1 {
		t.Fatalf("want one LWS advisory, got %+v", cands)
	}
	if lws[0].Instance != policy.ComponentWideInstance || lws[0].Mode != constants.MultiNode {
		t.Fatalf("LWS advisory shape: %+v", lws[0])
	}
	if len(executables(cands)) != 1 {
		t.Fatalf("mover must stay executable alongside the advisory: %+v", cands)
	}

	*cfg.LWSRecommendationsEnabled = false
	if got := advisories(evaluate(t, build(), cfg), policy.AdvisoryLWSMigrationUnsupported); len(got) != 0 {
		t.Fatalf("disabled LWS recommendations must not surface: %+v", got)
	}
}

// TestVolumePinnedAdvisory: an RWO-backed workload cannot move by any
// mechanism; it surfaces as advisory while other moves stay executable.
func TestVolumePinnedAdvisory(t *testing.T) {
	b := withBystander("prod/pinned", constants.OMENative)
	b.WithModel("prod/pinned", &snapshot.ModelAvailability{
		Key:            snapshot.ModelKey{Kind: snapshot.ModelKindBaseModel, Namespace: "prod", Name: "rwo"},
		Backend:        snapshot.BackendPVC,
		PVCAccessModes: []string{"ReadWriteOnce"},
		VolumePinned:   true,
	})
	cands := evaluate(t, b.Build(), bystanderGate())

	pinned := advisories(cands, policy.AdvisoryVolumePinned)
	if len(pinned) != 1 || pinned[0].Workload.String() != "prod/pinned" {
		t.Fatalf("want one VolumePinned advisory for prod/pinned, got %+v", cands)
	}
	execs := executables(cands)
	if len(execs) != 1 || execs[0].Workload.String() != "prod/mover" {
		t.Fatalf("mover must stay executable: %+v", execs)
	}
}

// TestOMENativeUnavailableAdvisory: without an executor the migration verb
// has no consumer; OMENative candidates degrade to advisory.
func TestOMENativeUnavailableAdvisory(t *testing.T) {
	b := withBystander("prod/omen", constants.OMENative).WithOMENative(false)
	cands := evaluate(t, b.Build(), bystanderGate())
	degraded := advisories(cands, policy.AdvisoryOMENativeUnavailable)
	if len(degraded) != 2 || len(executables(cands)) != 0 {
		t.Fatalf("every OMENative workload must degrade when the executor is unavailable, got %+v", cands)
	}
}

// TestEmergencyBoost: a move that unblocks an over-age pending pod in the
// workload's own namespace doubles its score; the same pending in a foreign
// namespace (no shared tenant group) must not boost it.
func TestEmergencyBoost(t *testing.T) {
	cfg := lowGate()
	oneExecutable := func(b *testutil.SnapshotBuilder) policy.Candidate {
		t.Helper()
		execs := executables(evaluate(t, b.Build(), cfg))
		if len(execs) != 1 {
			t.Fatalf("want exactly one executable candidate, got %+v", execs)
		}
		return execs[0]
	}

	// The pending demand shifts the blend weights, so the boost baseline
	// is the identically-shaped cross-namespace run: same weights, same
	// benefit, no tenant-compatible beneficiary.
	unboosted := oneExecutable(consolidationBuilder().WithPendingPodIn("other", 8, 30*time.Minute, "h100"))
	if unboosted.Emergency {
		t.Fatalf("cross-namespace pending without a shared tenant group must not boost: %+v", unboosted)
	}

	boosted := oneExecutable(consolidationBuilder().WithPendingPodIn("prod", 8, 30*time.Minute, "h100"))
	if !boosted.Emergency {
		t.Fatalf("unblocking an over-age same-namespace pending must be an emergency: %+v", boosted)
	}
	almost(t, "boosted score", boosted.Score, unboosted.Score*emergencyBoostFactor)

	if c := oneExecutable(consolidationBuilder().WithPendingPodIn("prod", 8, 5*time.Minute, "h100")); c.Emergency {
		t.Fatalf("a pending younger than emergencyPendingAgeMinutes must not boost: %+v", c)
	}
}

// TestInstanceFootprintsDeterministicOrder: the tie-break must travel with
// the sorted elements. The mixed-size shape below is the regression: the
// size comparison swaps the 8s past the 1 first, and a tie-break read
// through a stale parallel index would then leave "y" ahead of "x".
func TestInstanceFootprintsDeterministicOrder(t *testing.T) {
	inst := &snapshot.Instance{Pods: []snapshot.PodInfo{
		{Namespace: "p", Name: "a", Node: "n1", GPUs: 1},
		{Namespace: "p", Name: "y", Node: "n2", GPUs: 8},
		{Namespace: "p", Name: "x", Node: "n3", GPUs: 8},
	}}
	got := instanceFootprints(inst)
	wantNodes := []string{"n3", "n2", "n1"} // 8@p/x, 8@p/y, 1@p/a
	wantGPUs := []int64{8, 8, 1}
	if len(got) != len(wantNodes) {
		t.Fatalf("footprints = %+v", got)
	}
	for i := range wantNodes {
		if got[i].node != wantNodes[i] || got[i].gpus != wantGPUs[i] {
			t.Fatalf("footprint[%d] = %+v, want %d GPUs from %s (largest first, name tie-break)",
				i, got[i], wantGPUs[i], wantNodes[i])
		}
	}
}

// TestMaintenanceWindowGatesDispatch: outside every configured window,
// executable candidates are dropped; advisories survive (they dispatch
// nothing). ReferenceTime is a Thursday, 12:00 UTC.
func TestMaintenanceWindowGatesDispatch(t *testing.T) {
	build := func() *snapshot.ClusterSnapshot {
		return withBystander("prod/lws", constants.MultiNode).Build()
	}

	cfg := bystanderGate()
	cfg.MaintenanceWindows = []config.MaintenanceWindow{{Days: []string{"Mon"}, Start: "09:00", End: "17:00"}}
	closed := evaluate(t, build(), cfg)
	if len(executables(closed)) != 0 {
		t.Fatalf("outside the window executables must be dropped: %+v", closed)
	}
	if len(advisories(closed, policy.AdvisoryLWSMigrationUnsupported)) != 1 {
		t.Fatalf("advisories must survive a closed window: %+v", closed)
	}

	cfg.MaintenanceWindows = []config.MaintenanceWindow{{Days: []string{"Thu"}, Start: "09:00", End: "17:00"}}
	if open := evaluate(t, build(), cfg); len(executables(open)) != 1 {
		t.Fatalf("inside the window the move must dispatch: %+v", open)
	}

	// An emergency — a pod starved past emergencyPendingAgeMinutes —
	// overrides a closed window: a steady-state optimization can wait, a
	// starving workload cannot. The age threshold is pinned explicitly so
	// the assertion does not lean on the default.
	cfg.MaintenanceWindows = []config.MaintenanceWindow{{Days: []string{"Mon"}, Start: "09:00", End: "17:00"}}
	cfg.EmergencyPendingAgeMinutes = 15
	starving := consolidationBuilder().WithPendingPodIn("prod", 8, 30*time.Minute, "h100")
	emergency := executables(evaluate(t, starving.Build(), cfg))
	if len(emergency) != 1 || !emergency[0].Emergency {
		t.Fatalf("an emergency must override the closed window: %+v", emergency)
	}
}

// TestModelReadinessFiltersTargets: per-node models restrict hints to nodes
// whose readiness label is set — migrating onto a node that must first pull
// a multi-hundred-GB model defeats the purpose.
func TestModelReadinessFiltersTargets(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("node1", "h100", 8).
		WithNode("node2", "h100", 8).
		WithNode("node3", "h100", 8).
		WithInstance("prod/mover", v1beta1.EngineComponent, constants.OMENative, "node1", 1)
	b.WithOtherOccupant("node2", 7)
	b.WithOtherOccupant("node3", 7)
	b.WithModel("prod/mover", &snapshot.ModelAvailability{
		Key:        snapshot.ModelKey{Kind: snapshot.ModelKindBaseModel, Namespace: "prod", Name: "llm"},
		Backend:    snapshot.BackendPerNode,
		NodesReady: []string{"node1", "node3"},
	})
	cfg := lowGate()
	*cfg.Policies.Defragmentation.FragmentationThreshold = 0.05

	execs := executables(evaluate(t, b.Build(), cfg))
	if len(execs) != 1 {
		t.Fatalf("want the mover candidate, got %+v", execs)
	}
	if len(execs[0].HintTargetNodes) != 1 || execs[0].HintTargetNodes[0] != "node3" {
		t.Fatalf("hints = %v, want [node3] (node2 lacks the model)", execs[0].HintTargetNodes)
	}
}

// TestRankingPrefersScoreThenSmallerFootprint: candidates order by score
// descending; equal scores break toward the smaller footprint.
func TestRankingPrefersScoreThenSmallerFootprint(t *testing.T) {
	cands := []policy.Candidate{
		{Workload: types.NamespacedName{Namespace: "prod", Name: "b"}, Score: 0.1, FootprintGPUs: 8},
		{Workload: types.NamespacedName{Namespace: "prod", Name: "a"}, Score: 0.1, FootprintGPUs: 2},
		{Workload: types.NamespacedName{Namespace: "prod", Name: "c"}, Score: 0.3, FootprintGPUs: 8},
	}
	rankCandidates(cands)
	if cands[0].Workload.Name != "c" || cands[1].FootprintGPUs != 2 || cands[2].FootprintGPUs != 8 {
		t.Fatalf("ranking order wrong: %+v", cands)
	}
}

func TestRawDeploymentIsAlwaysPerInstanceAdvisory(t *testing.T) {
	build := func() *snapshot.ClusterSnapshot {
		b := testutil.NewSnapshot().
			WithNode("node1", "h100", 8).
			WithNode("node2", "h100", 8).
			WithNode("node3", "h100", 8).
			WithInstance("prod/raw", v1beta1.EngineComponent, constants.RawDeployment, "node1", 1).
			WithInstance("prod/raw", v1beta1.EngineComponent, constants.RawDeployment, "node3", 1)
		b.WithOtherOccupant("node2", 7)
		return b.Build()
	}

	for _, mode := range []string{config.ModeRecommendOnly, config.ModeExecute} {
		for _, enabled := range []bool{false, true} {
			t.Run(mode+"/enabled="+fmt.Sprint(enabled), func(t *testing.T) {
				cfg := lowGate()
				cfg.Mode = mode
				*cfg.RawDeploymentMigrationEnabled = enabled
				got := evaluate(t, build(), cfg)
				if len(executables(got)) != 0 {
					t.Fatalf("RawDeployment must never be executable: %+v", got)
				}
				raw := advisories(got, rawMigrationUnsupportedReason)
				if len(raw) != 2 {
					t.Fatalf("want one unsupported advisory per Raw instance, got %+v", got)
				}
				for i, c := range raw {
					if c.Instance != int32(i) || c.FromNode == "" || len(c.HintTargetNodes) != 0 || c.FootprintGPUs != 0 {
						t.Fatalf("Raw advisory must identify only its source, without capacity claims: %+v", c)
					}
				}
			})
		}
	}
}

func TestOMENativeEligibilityFailsClosed(t *testing.T) {
	steady := func() *snapshot.ClusterSnapshot {
		b := testutil.NewSnapshot().
			WithNode("node1", "h100", 8).
			WithNode("node2", "h100", 8).
			WithInstance("prod/mover", v1beta1.EngineComponent, constants.OMENative, "node1", 1)
		b.WithOtherOccupant("node2", 7)
		return makeOMENativeSteady(t, b.Build(), "mover")
	}

	if got := executables(evaluate(t, steady(), lowGate())); len(got) != 1 || !got[0].SurgeShaped || got[0].Score <= 0 {
		t.Fatalf("fully steady OMENative state must be executable and surge-shaped: %+v", got)
	}

	tests := []struct {
		name   string
		reason string
		mutate func(*snapshot.ClusterSnapshot)
	}{
		{name: "structured executor unavailable despite legacy true", reason: policy.AdvisoryOMENativeUnavailable, mutate: func(s *snapshot.ClusterSnapshot) {
			s.OMENativeAvailable = true
			s.OMENativeExecutor.Available = false
		}},
		{name: "missing IR", reason: omenativeObservationInvalidReason, mutate: func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].Components[v1beta1.EngineComponent].IR = nil
		}},
		{name: "invalid component observation", reason: omenativeObservationInvalidReason, mutate: func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].Components[v1beta1.EngineComponent].ObservationValid = false
		}},
		{name: "invalid instance join", reason: omenativeObservationInvalidReason, mutate: func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].Components[v1beta1.EngineComponent].Instances[0].ObservationValid = false
		}},
		{name: "stale status generation", reason: omenativeObservationInvalidReason, mutate: func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].Components[v1beta1.EngineComponent].StatusFresh = false
		}},
		{name: "paused IR", reason: omenativeStateIneligibleReason, mutate: func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].Components[v1beta1.EngineComponent].IR.Spec.Paused = true
		}},
		{name: "migration policy Never", reason: omenativeStateIneligibleReason, mutate: func(s *snapshot.ClusterSnapshot) {
			comp := s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].Components[v1beta1.EngineComponent]
			comp.IR.Spec.Lifecycle = &v1beta1.LifecycleSpec{MigrationPolicy: &v1beta1.MigrationPolicy{Mode: v1beta1.MigrationPolicyModeNever}}
		}},
		{name: "scale count mismatch", reason: omenativeStateIneligibleReason, mutate: func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].Components[v1beta1.EngineComponent].IR.Status.ReadyReplicas = 0
		}},
		{name: "rollout revision mismatch", reason: omenativeStateIneligibleReason, mutate: func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].Components[v1beta1.EngineComponent].IR.Status.UpdateRevision = "revision-2"
		}},
		{name: "instance not Ready", reason: omenativeStateIneligibleReason, mutate: func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].Components[v1beta1.EngineComponent].Instances[0].Phase = v1beta1.OMENativeInstanceUpdating
		}},
		{name: "instance not admitted", reason: omenativeStateIneligibleReason, mutate: func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].Components[v1beta1.EngineComponent].Instances[0].Admitted = false
		}},
		{name: "instance readiness mismatch", reason: omenativeStateIneligibleReason, mutate: func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].Components[v1beta1.EngineComponent].Instances[0].ReadyPods = 0
		}},
		{name: "instance revision mismatch", reason: omenativeStateIneligibleReason, mutate: func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].Components[v1beta1.EngineComponent].Instances[0].TargetRevision = "revision-2"
		}},
		{name: "active instance operation", reason: omenativeStateIneligibleReason, mutate: func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].Components[v1beta1.EngineComponent].Instances[0].Operation = &v1beta1.InstanceOperation{ID: "busy"}
		}},
		{name: "terminating pod", reason: omenativeStateIneligibleReason, mutate: func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].Components[v1beta1.EngineComponent].Instances[0].Pods[0].Terminating = true
		}},
		{name: "active migration", reason: omenativeStateIneligibleReason, mutate: func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].ActiveMigrations = []snapshot.InFlight{{UUID: "active"}}
		}},
		{name: "malformed migration", reason: omenativeStateIneligibleReason, mutate: func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].MalformedRequests = map[string]string{"bad": "bounded"}
		}},
		{name: "invalid migration observation", reason: omenativeStateIneligibleReason, mutate: func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].MigrationStateValid = false
		}},
		{name: "workload immovable", reason: omenativeStateIneligibleReason, mutate: func(s *snapshot.ClusterSnapshot) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].Movable = false
		}},
		{name: "workload cooldown", reason: omenativeStateIneligibleReason, mutate: func(s *snapshot.ClusterSnapshot) {
			recent := testutil.ReferenceTime.Add(-time.Minute)
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "mover"}].LastMigration = &recent
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap := steady()
			tt.mutate(snap)
			got := evaluate(t, snap, lowGate())
			if len(executables(got)) != 0 {
				t.Fatalf("ineligible OMENative state must fail closed: %+v", got)
			}
			advisory := advisories(got, tt.reason)
			if len(advisory) != 1 || advisory[0].Instance != 0 || len(advisory[0].HintTargetNodes) != 0 || advisory[0].FootprintGPUs != 0 {
				t.Fatalf("want one bounded source-only advisory reason %q, got %+v", tt.reason, got)
			}
		})
	}
}

func TestObservedOnlyGateNeverCreatesCapacityClaim(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("node1", "h100", 8).
		WithNode("node2", "h100", 8).
		WithInstance("prod/raw", v1beta1.EngineComponent, constants.RawDeployment, "node1", 1)
	b.WithOtherOccupant("node2", 7)
	snap := b.Build()

	scores := ComputeScores(snap, config.Default())
	cs := scores.PerPool["h100"]
	if cs.FObserved <= 0 {
		t.Fatal("Raw occupancy must remain visible in observed fragmentation")
	}
	if cs.FReclaimable != 0 || cs.PendingPressure != 0 || cs.Score != 0 || scores.FragmentationScore != 0 {
		t.Fatalf("advisory-only occupancy leaked into execution score: %+v", cs)
	}

	got := evaluate(t, snap, lowGate())
	if len(got) != 1 || got[0].Executable || got[0].AdvisoryReason != rawMigrationUnsupportedReason || len(got[0].HintTargetNodes) != 0 || got[0].FootprintGPUs != 0 {
		t.Fatalf("observed-only gate must emit exactly one claim-free advisory: %+v", got)
	}

	quiet := lowGate()
	*quiet.Policies.Defragmentation.FragmentationThreshold = cs.FObserved + 0.01
	if got := evaluate(t, snap, quiet); len(got) != 0 {
		t.Fatalf("below-threshold advisory gap must stay silent: %+v", got)
	}
}

func TestNonpositiveOMENativeScoreIsAdvisory(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("node1", "h100", 8).
		WithNode("node2", "h100", 8).
		WithInstance("prod/mover", v1beta1.EngineComponent, constants.OMENative, "node1", 1).
		WithInstance("prod/weak", v1beta1.EngineComponent, constants.OMENative, "node2", 1)
	b.WithOtherOccupant("node2", 6)
	snap := makeOMENativeSteady(t, b.Build(), "mover")
	snap = makeOMENativeSteady(t, snap, "weak")

	got := evaluate(t, snap, lowGate())
	if len(executables(got)) != 1 || executables(got)[0].Workload.String() != "prod/mover" || !executables(got)[0].SurgeShaped {
		t.Fatalf("positive OMENative consolidation must remain the sole executable surge: %+v", got)
	}
	weak := advisories(got, nonExecutableFragmentationReason)
	if len(weak) != 1 || weak[0].Workload.String() != "prod/weak" || weak[0].Score > 0 || len(weak[0].HintTargetNodes) != 0 || weak[0].FootprintGPUs != 0 {
		t.Fatalf("nonpositive move must degrade to a claim-free observed-fragmentation advisory: %+v", got)
	}
}
