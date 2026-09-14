package coordination

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestBuildTrafficTargets_FillsPerRevisionServiceName(t *testing.T) {
	got := BuildTrafficTargets("llama", v1beta1.EngineComponent, []RevisionWeight{
		{RevisionHash: "newhash", Percent: 25, Tag: "latest", LatestRevision: true},
		{RevisionHash: "oldhash", Percent: 75, Tag: "prev"},
	})
	want := []v1beta1.ComponentTrafficTarget{
		{RevisionName: "llama-engine-rev-newhash", Percent: 25, Tag: "latest", LatestRevision: true},
		{RevisionName: "llama-engine-rev-oldhash", Percent: 75, Tag: "prev"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("BuildTrafficTargets diff (-want +got):\n%s", diff)
	}
}

func TestBuildTrafficTargets_SkipsZeroOrEmpty(t *testing.T) {
	got := BuildTrafficTargets("llama", v1beta1.EngineComponent, []RevisionWeight{
		{RevisionHash: "", Percent: 25},
		{RevisionHash: "hash", Percent: 0},
		{RevisionHash: "hash2", Percent: 100},
	})
	if len(got) != 1 {
		t.Fatalf("filter: got %d entries want 1: %+v", len(got), got)
	}
	if got[0].RevisionName != "llama-engine-rev-hash2" {
		t.Errorf("kept entry: got %q", got[0].RevisionName)
	}
}

func TestSortedWeights_DescendingByPercent(t *testing.T) {
	in := []RevisionWeight{
		{RevisionHash: "a", Percent: 25},
		{RevisionHash: "b", Percent: 75},
		{RevisionHash: "c", Percent: 75},
	}
	out := SortedWeights(in)
	if out[0].Percent != 75 || out[1].Percent != 75 || out[2].Percent != 25 {
		t.Errorf("descending: got %+v", out)
	}
	if out[0].RevisionHash != "b" || out[1].RevisionHash != "c" {
		t.Errorf("tie-break by hash: got %+v", out)
	}
}

func TestNormalizeWeights_AbsorbsResidualOnMax(t *testing.T) {
	in := []RevisionWeight{
		{RevisionHash: "big", Percent: 70},
		{RevisionHash: "small", Percent: 29},
	}
	out := NormalizeWeights(in)
	if out[0].Percent != 71 {
		t.Errorf("biggest absorbs +1: got %d want 71", out[0].Percent)
	}
	if out[1].Percent != 29 {
		t.Errorf("smaller unchanged: got %d want 29", out[1].Percent)
	}
}

func TestNormalizeWeights_NoOpWhenSumIs100(t *testing.T) {
	in := []RevisionWeight{
		{RevisionHash: "a", Percent: 60},
		{RevisionHash: "b", Percent: 40},
	}
	out := NormalizeWeights(in)
	if diff := cmp.Diff(in, out); diff != "" {
		t.Errorf("100-sum should be unchanged (-want +got):\n%s", diff)
	}
}

func TestNormalizeWeights_ZeroSumLeavesAsIs(t *testing.T) {
	in := []RevisionWeight{
		{RevisionHash: "a", Percent: 0},
		{RevisionHash: "b", Percent: 0},
	}
	out := NormalizeWeights(in)
	if diff := cmp.Diff(in, out); diff != "" {
		t.Errorf("zero-sum: got diff (-want +got):\n%s", diff)
	}
}

func TestComputeWeightsFromPods_SingleRevisionIs100(t *testing.T) {
	got := ComputeWeightsFromPods(map[string]int32{"hash1": 3}, 3, "hash1")
	if len(got) != 1 {
		t.Fatalf("got %d entries want 1", len(got))
	}
	if got[0].Percent != 100 {
		t.Errorf("Percent: got %d want 100", got[0].Percent)
	}
	if !got[0].LatestRevision {
		t.Errorf("LatestRevision: should be true when hash matches latestHash")
	}
}

func TestComputeWeightsFromPods_MultiRevisionApportions(t *testing.T) {
	got := ComputeWeightsFromPods(map[string]int32{
		"oldhash": 3,
		"newhash": 1,
	}, 4, "newhash")
	// 3/4 = 75%, 1/4 = 25%. Sum = 100, no normalization needed.
	weights := map[string]int32{}
	latest := map[string]bool{}
	for _, w := range got {
		weights[w.RevisionHash] = w.Percent
		latest[w.RevisionHash] = w.LatestRevision
	}
	if weights["oldhash"] != 75 || weights["newhash"] != 25 {
		t.Errorf("apportion: got %+v want oldhash=75 newhash=25", weights)
	}
	if !latest["newhash"] || latest["oldhash"] {
		t.Errorf("LatestRevision: got %+v", latest)
	}
}

func TestComputeWeightsFromPods_TagsLatestAndPrev(t *testing.T) {
	got := ComputeWeightsFromPods(map[string]int32{
		"oldhash": 3,
		"newhash": 1,
	}, 4, "newhash")
	tags := map[string]string{}
	for _, w := range got {
		tags[w.RevisionHash] = w.Tag
	}
	if tags["newhash"] != TrafficTagLatest {
		t.Errorf("newhash Tag: got %q want %q", tags["newhash"], TrafficTagLatest)
	}
	if tags["oldhash"] != TrafficTagPrev {
		t.Errorf("oldhash Tag: got %q want %q", tags["oldhash"], TrafficTagPrev)
	}
}

// With no latest hash to compare against, every revision would read "prev",
// which misleads more than an absent tag.
func TestComputeWeightsFromPods_NoLatestHashLeavesTagEmpty(t *testing.T) {
	got := ComputeWeightsFromPods(map[string]int32{"a": 1, "b": 1}, 2, "")
	for _, w := range got {
		if w.Tag != "" {
			t.Errorf("%s Tag: got %q want empty when latestHash is unset", w.RevisionHash, w.Tag)
		}
	}
}

func TestComputeWeightsFromPods_ZeroTotalReturnsNil(t *testing.T) {
	got := ComputeWeightsFromPods(map[string]int32{}, 0, "")
	if got != nil {
		t.Errorf("zero total: got %+v want nil", got)
	}
}

func TestComputeWeightsFromPods_NormalizesRoundoff(t *testing.T) {
	// 3 revisions at 1 pod each, total 3: 1/3 = 33% each → sum 99.
	// NormalizeWeights bumps the largest entry by 1 to land on 100.
	got := ComputeWeightsFromPods(map[string]int32{
		"a": 1,
		"b": 1,
		"c": 1,
	}, 3, "")
	sum := int32(0)
	for _, w := range got {
		sum += w.Percent
	}
	if sum != 100 {
		t.Errorf("normalized sum: got %d want 100; got entries=%+v", sum, got)
	}
}

func TestTrafficDiffersMeaningfully_LenMismatch(t *testing.T) {
	desired := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100}}
	if !TrafficDiffersMeaningfully(desired, nil) {
		t.Errorf("different length: should report diff")
	}
}

func TestTrafficDiffersMeaningfully_PercentMismatch(t *testing.T) {
	desired := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100}}
	observed := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 99}}
	if !TrafficDiffersMeaningfully(desired, observed) {
		t.Errorf("percent drift: should report diff")
	}
}

func TestTrafficDiffersMeaningfully_SetEqualityIgnoresOrder(t *testing.T) {
	desired := []v1beta1.ComponentTrafficTarget{
		{RevisionName: "a", Percent: 25},
		{RevisionName: "b", Percent: 75},
	}
	observed := []v1beta1.ComponentTrafficTarget{
		{RevisionName: "b", Percent: 75},
		{RevisionName: "a", Percent: 25},
	}
	if TrafficDiffersMeaningfully(desired, observed) {
		t.Errorf("same content reordered: should not report diff")
	}
}

func TestTrafficDiffersMeaningfully_TagDriftIgnored(t *testing.T) {
	desired := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100, Tag: "latest"}}
	observed := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100, Tag: ""}}
	if TrafficDiffersMeaningfully(desired, observed) {
		t.Errorf("tag-only drift: should not report diff")
	}
}

// TestTrafficDiffersMeaningfully_LatestRevisionFlip pins the regression: when the engine's traffic[0] was
// initially written with LatestRevision=false (because the IR Status had
// not yet populated LifecycleStatus.UpdateRevision on the first
// reconcile that observed pods), subsequent reconciles computed the
// correct LatestRevision=true value but the no-op short-circuit treated
// the flag as cosmetic and suppressed the write — leaving the engine
// entry permanently stuck without the flag while decoder/router (which
// won the IR-Status race) had it set correctly.
//
// The fix: a LatestRevision flip is meaningful drift (operator-visible
// and consumed by downstream HTTPRoute reconcilers) and MUST trigger a
// write.
func TestTrafficDiffersMeaningfully_LatestRevisionFlipReportsDiff(t *testing.T) {
	desired := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100, LatestRevision: true}}
	observed := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100, LatestRevision: false}}
	if !TrafficDiffersMeaningfully(desired, observed) {
		t.Errorf("LatestRevision flip false->true: should report diff (otherwise the no-op short-circuit strands the stale flag)")
	}
	// Symmetric case: flipping true->false also matters (e.g., when the
	// rollout advances and the prior LatestRolledoutRevision is demoted).
	if !TrafficDiffersMeaningfully(observed, desired) {
		t.Errorf("LatestRevision flip true->false: should report diff")
	}
}

// TestTrafficDiffersMeaningfully_LatestRevisionSameNoDiff pins the
// no-op fast path: when LatestRevision matches across desired and
// observed (most reconciles), the no-op short-circuit still kicks in
// and the writer skips the apiserver round-trip.
func TestTrafficDiffersMeaningfully_LatestRevisionSameNoDiff(t *testing.T) {
	desired := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100, LatestRevision: true}}
	observed := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100, LatestRevision: true}}
	if TrafficDiffersMeaningfully(desired, observed) {
		t.Errorf("identical LatestRevision: should not report diff")
	}
}

// TestTrafficWithinDeadband_BlipSuppressed pins the core hysteresis fix:
// weights are derived from RAW live pod counts, so a single pod momentarily
// Pending between reconciles shifts a revision's percent by a few points. With
// a non-zero deadband, that sub-band jitter is treated as noise and the status
// write (and the HTTPRoute re-enqueue it would trigger) is suppressed.
//
// Scenario: a large Component already written at 80/20; one pod blips and the
// recomputed weights become 78/22 — a 2-point swing on each revision. With a
// deadband of 5, the write is suppressed (no churn).
func TestTrafficWithinDeadband_BlipSuppressed(t *testing.T) {
	observed := []v1beta1.ComponentTrafficTarget{
		{RevisionName: "a", Percent: 80, LatestRevision: true},
		{RevisionName: "b", Percent: 20},
	}
	desired := []v1beta1.ComponentTrafficTarget{
		{RevisionName: "a", Percent: 78, LatestRevision: true},
		{RevisionName: "b", Percent: 22},
	}
	// The raw diff IS meaningful (percent changed), so absent hysteresis this
	// would write every reconcile.
	if !TrafficDiffersMeaningfully(desired, observed) {
		t.Fatalf("precondition: a 2-point percent change should be a raw diff")
	}
	if !TrafficWithinDeadband(desired, observed, 5) {
		t.Errorf("blip within deadband: should suppress the write (got churn)")
	}
}

// TestTrafficWithinDeadband_SustainedChangeConverges proves eventual
// correctness: a real ratio shift that moves a revision by >= deadband is NOT
// suppressed, so weights converge to the true distribution.
func TestTrafficWithinDeadband_SustainedChangeConverges(t *testing.T) {
	observed := []v1beta1.ComponentTrafficTarget{
		{RevisionName: "a", Percent: 80, LatestRevision: true},
		{RevisionName: "b", Percent: 20},
	}
	desired := []v1beta1.ComponentTrafficTarget{
		{RevisionName: "a", Percent: 60, LatestRevision: true},
		{RevisionName: "b", Percent: 40},
	}
	if TrafficWithinDeadband(desired, observed, 5) {
		t.Errorf("sustained 20-point shift exceeds deadband: must NOT be suppressed")
	}
}

// TestTrafficWithinDeadband_ZeroDisablesBand pins the graceful-degradation
// contract: deadband <= 0 (absent config) restores the exact prior
// write-on-any-diff behavior — TrafficWithinDeadband never suppresses.
func TestTrafficWithinDeadband_ZeroDisablesBand(t *testing.T) {
	observed := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 80}, {RevisionName: "b", Percent: 20}}
	desired := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 79}, {RevisionName: "b", Percent: 21}}
	if TrafficWithinDeadband(desired, observed, 0) {
		t.Errorf("deadband 0: band disabled, must never suppress")
	}
	if TrafficWithinDeadband(desired, observed, -3) {
		t.Errorf("negative deadband: band disabled, must never suppress")
	}
}

// TestTrafficWithinDeadband_RevisionSetChangeNotSuppressed: a revision
// appearing or disappearing is a real topology change, never jitter.
func TestTrafficWithinDeadband_RevisionSetChangeNotSuppressed(t *testing.T) {
	observed := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100, LatestRevision: true}}
	// New revision "b" appears; lengths differ.
	desiredAppear := []v1beta1.ComponentTrafficTarget{
		{RevisionName: "a", Percent: 98, LatestRevision: true},
		{RevisionName: "b", Percent: 2},
	}
	if TrafficWithinDeadband(desiredAppear, observed, 5) {
		t.Errorf("new revision appearing: must not be suppressed")
	}
	// Same length but the revision name swapped — not jitter.
	desiredSwap := []v1beta1.ComponentTrafficTarget{{RevisionName: "c", Percent: 100, LatestRevision: true}}
	if TrafficWithinDeadband(desiredSwap, observed, 5) {
		t.Errorf("revision name swap: must not be suppressed")
	}
}

// TestTrafficWithinDeadband_TerminalConvergenceNotSuppressed pins the
// boundary-write fix: when an operator sets a deadband LARGER than the final
// canary step, the terminal write to a clean single-revision split must still
// happen. Last-written 95/5, target 100/0, deadband 10: both revisions move
// only 5 (< band), so a naive band would suppress the write and stick traffic
// at 95/5 forever. A revision arriving at 100 (or leaving to 0) is terminal
// convergence, never jitter, so the write must proceed.
func TestTrafficWithinDeadband_TerminalConvergenceNotSuppressed(t *testing.T) {
	observed := []v1beta1.ComponentTrafficTarget{
		{RevisionName: "a", Percent: 95, LatestRevision: true},
		{RevisionName: "b", Percent: 5},
	}
	desired := []v1beta1.ComponentTrafficTarget{
		{RevisionName: "a", Percent: 100, LatestRevision: true},
		{RevisionName: "b", Percent: 0},
	}
	// Sanity: both per-revision moves (5) are strictly under the band (10), so
	// only the boundary carve-out keeps this from being suppressed.
	if TrafficWithinDeadband(desired, observed, 10) {
		t.Errorf("terminal convergence 95/5 -> 100/0 with deadband 10: must NOT be suppressed (traffic would stick at 95/5)")
	}
}

// TestTrafficWithinDeadband_LatestRevisionFlipNotSuppressed: a LatestRevision
// flag flip is operator-visible and consumed by HTTPRoute reconcilers, so it
// must write even when the percents are within the band.
func TestTrafficWithinDeadband_LatestRevisionFlipNotSuppressed(t *testing.T) {
	observed := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100, LatestRevision: false}}
	desired := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100, LatestRevision: true}}
	if TrafficWithinDeadband(desired, observed, 5) {
		t.Errorf("LatestRevision flip: must not be suppressed even with identical percent")
	}
}

// A canary step whose canary and stable revision are the same -- what a revert
// of a failed rollout produces, since the hash is content-addressed over the
// pod template -- must emit ONE target, not two. Status.Components.<c>.Traffic
// is +listMapKey=revisionName, so a duplicate makes the apiserver reject every
// subsequent status write for the ISVC.
func TestBuildTrafficTargets_MergesSameRevision(t *testing.T) {
	got := BuildTrafficTargets("llama", v1beta1.RouterComponent, []RevisionWeight{
		{RevisionHash: "samehash", Percent: 10, Tag: "canary", LatestRevision: true, PairingProtocol: "p1"},
		{RevisionHash: "samehash", Percent: 90, Tag: "stable"},
	})
	want := []v1beta1.ComponentTrafficTarget{
		{RevisionName: "llama-router-rev-samehash", Percent: 100, Tag: "canary", LatestRevision: true, PairingProtocol: "p1"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("BuildTrafficTargets diff (-want +got):\n%s", diff)
	}
}

// The latest-revision weight owns the merged target's tag regardless of the
// order the weights arrive in (SortedWeights puts the larger percent first, so
// the stable entry leads whenever the canary weight is under 50).
func TestBuildTrafficTargets_MergeKeepsLatestIdentityWhenStableLeads(t *testing.T) {
	got := BuildTrafficTargets("llama", v1beta1.RouterComponent, []RevisionWeight{
		{RevisionHash: "samehash", Percent: 90, Tag: "stable"},
		{RevisionHash: "samehash", Percent: 10, Tag: "canary", LatestRevision: true, PairingProtocol: "p1"},
	})
	if len(got) != 1 {
		t.Fatalf("merge: got %d entries want 1: %+v", len(got), got)
	}
	if got[0].Percent != 100 || got[0].Tag != "canary" || !got[0].LatestRevision || got[0].PairingProtocol != "p1" {
		t.Errorf("merged target: %+v", got[0])
	}
}

// Distinct revisions must still produce distinct targets.
func TestBuildTrafficTargets_DoesNotMergeDistinctRevisions(t *testing.T) {
	got := BuildTrafficTargets("llama", v1beta1.RouterComponent, []RevisionWeight{
		{RevisionHash: "a", Percent: 10, Tag: "canary", LatestRevision: true},
		{RevisionHash: "b", Percent: 90, Tag: "stable"},
	})
	if len(got) != 2 {
		t.Fatalf("distinct revisions: got %d entries want 2: %+v", len(got), got)
	}
}
