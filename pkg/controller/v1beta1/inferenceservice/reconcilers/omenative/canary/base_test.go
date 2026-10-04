package canary

import (
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func statusWithRecord(cs *v1beta1.CanaryStatus, phase v1beta1.RolloutPhase) *v1beta1.InferenceServiceStatus {
	return &v1beta1.InferenceServiceStatus{
		Canary: cs.DeepCopy(),
		Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.EngineComponent: {Canary: cs.DeepCopy(), RolloutPhase: phase},
		},
	}
}

func engineRecord(s *v1beta1.InferenceServiceStatus) *v1beta1.CanaryStatus {
	return s.Components[v1beta1.EngineComponent].Canary
}

// isvcWith wraps a status in an object with no rollout spec: only the
// Components carrying a record are executor-owned.
func isvcWith(s *v1beta1.InferenceServiceStatus) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{Status: *s}
}

// A pass that read the current record writes its decision.
func TestBaseKeepsADecisionMadeFromTheCurrentRecord(t *testing.T) {
	read := statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "new"}, v1beta1.RolloutPhasePaused)
	base := NewBase(isvcWith(read))
	desired := read.DeepCopy()
	engineRecord(desired).RolledBackRevisionHash = "new"
	live := read.DeepCopy()

	if base.PreserveFresh(desired, live) || base.Stale() {
		t.Fatal("a decision made from the current record was treated as stale")
	}
	if engineRecord(desired).RolledBackRevisionHash != "new" {
		t.Fatalf("the pass's decision was dropped: %+v", engineRecord(desired))
	}
}

// A pass that read a stale copy keeps the live record, phase and traffic,
// and reports it so the pass decides again from the current record.
func TestBaseKeepsTheLiveRecordOverAStalePass(t *testing.T) {
	stale := statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "new"}, v1beta1.RolloutPhasePaused)
	base := NewBase(isvcWith(stale))
	live := statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "new", RolledBackRevisionHash: "new"}, v1beta1.RolloutPhaseRollingBack)
	entry := live.Components[v1beta1.EngineComponent]
	entry.Traffic = []v1beta1.ComponentTrafficTarget{{RevisionName: "stable", Percent: 100}}
	live.Components[v1beta1.EngineComponent] = entry
	desired := stale.DeepCopy()
	// The stale pass stepped the canary it believed was still live.
	engineRecord(desired).CurrentStep = 1
	desired.Canary.CurrentStep = 1
	entry = desired.Components[v1beta1.EngineComponent]
	entry.RolloutPhase = v1beta1.RolloutPhasePending
	entry.Lifecycle = &v1beta1.LifecycleStatus{ReadyReplicas: 3}
	desired.Components[v1beta1.EngineComponent] = entry

	if !base.PreserveFresh(desired, live) || !base.Stale() {
		t.Fatal("a decision made from a stale record was not detected")
	}
	got := desired.Components[v1beta1.EngineComponent]
	if got.Canary.RolledBackRevisionHash != "new" || got.Canary.CurrentStep != 0 || got.RolloutPhase != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the live record was not kept: %+v phase=%q", got.Canary, got.RolloutPhase)
	}
	if len(got.Traffic) != 1 || got.Traffic[0].Percent != 100 {
		t.Fatalf("the live traffic was not kept: %+v", got.Traffic)
	}
	if desired.Canary.RolledBackRevisionHash != "new" || desired.Canary.CurrentStep != 0 {
		t.Fatalf("the live alias was not kept: %+v", desired.Canary)
	}
	if got.Lifecycle == nil || got.Lifecycle.ReadyReplicas != 3 {
		t.Fatalf("state the executor does not own was touched: %+v", got.Lifecycle)
	}
}

// A live record that appeared after the pass read no record is newer too.
func TestBaseKeepsARecordThePassNeverRead(t *testing.T) {
	base := NewBase(&v1beta1.InferenceService{})
	live := statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "new"}, v1beta1.RolloutPhasePending)
	desired := &v1beta1.InferenceServiceStatus{}

	if !base.PreserveFresh(desired, live) {
		t.Fatal("a record the pass never read was overwritten")
	}
	if engineRecord(desired) == nil || engineRecord(desired).CanaryRevisionHash != "new" || desired.Canary == nil {
		t.Fatalf("the live record was not kept: %+v", desired)
	}
}

// The state a write persisted is the base of the pass's later decisions.
func TestBaseAdvancesToTheWrittenState(t *testing.T) {
	read := statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "old", RolledBackRevisionHash: "old"}, v1beta1.RolloutPhaseRolledBack)
	base := NewBase(isvcWith(read))
	written := statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "new"}, v1beta1.RolloutPhasePending)
	base.Advance(written)
	desired := written.DeepCopy()
	engineRecord(desired).CurrentStep = 1

	if base.PreserveFresh(desired, written.DeepCopy()) || base.Stale() {
		t.Fatal("a decision made from the state this pass wrote was treated as stale")
	}
	if engineRecord(desired).CurrentStep != 1 {
		t.Fatalf("the pass's decision was dropped: %+v", engineRecord(desired))
	}
}

// Without a base nothing is guarded.
func TestNilBaseGuardsNothing(t *testing.T) {
	var base *Base
	desired := statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "new"}, v1beta1.RolloutPhasePaused)
	live := statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "new", RolledBackRevisionHash: "new"}, v1beta1.RolloutPhaseRollingBack)
	if base.PreserveFresh(desired, live) || base.Stale() {
		t.Fatal("a nil base must not guard")
	}
	if engineRecord(desired).RolledBackRevisionHash != "" {
		t.Fatal("a nil base must not touch the status")
	}
}

// A secondary of a canary group carries no record of its own, yet its traffic
// is the executor's: a live write to it is kept over a stale pass exactly as
// the primary's is. Outside a canary group the same Component is not guarded.
func TestBaseKeepsALiveSecondaryTrafficOverAStalePass(t *testing.T) {
	read := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Canary:     &v1beta1.GroupCanary{Steps: twoStep()},
	}}}}}
	read.Status = *statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "new"}, v1beta1.RolloutPhasePaused)
	split := []v1beta1.ComponentTrafficTarget{{RevisionName: "d-new", Percent: 50}, {RevisionName: "d-stable", Percent: 50}}
	whole := []v1beta1.ComponentTrafficTarget{{RevisionName: "d-stable", Percent: 100}}

	base := NewBase(read)
	live := read.Status.DeepCopy()
	live.Components[v1beta1.DecoderComponent] = v1beta1.ComponentStatusSpec{Traffic: whole}
	desired := read.Status.DeepCopy()
	desired.Components[v1beta1.DecoderComponent] = v1beta1.ComponentStatusSpec{Traffic: split}
	if !base.PreserveFresh(desired, live) || !base.Stale() {
		t.Fatal("a newer live split on a secondary was not detected")
	}
	if got := desired.Components[v1beta1.DecoderComponent].Traffic; len(got) != 1 || got[0].RevisionName != "d-stable" {
		t.Fatalf("the live secondary traffic was not kept: %+v", got)
	}

	unowned := NewBase(&v1beta1.InferenceService{Status: *read.Status.DeepCopy()})
	desired = read.Status.DeepCopy()
	desired.Components[v1beta1.DecoderComponent] = v1beta1.ComponentStatusSpec{Traffic: split}
	if unowned.PreserveFresh(desired, live) || unowned.Stale() {
		t.Fatal("a Component outside every canary group is not the executor's to guard")
	}
	if got := desired.Components[v1beta1.DecoderComponent].Traffic; len(got) != 2 {
		t.Fatalf("the pass's own write to an unowned Component was dropped: %+v", got)
	}
}

// The rolled-out revision fields of an owned Component are the executor's:
// a live record of a completed promotion is kept over a stale pass, and a
// pass whose read differs from the live object in those fields alone read a
// stale copy.
func TestBaseKeepsTheLiveRolledOutRevisionsOverAStalePass(t *testing.T) {
	read := statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "new", StableRevisionHash: "old", CurrentStep: 1}, v1beta1.RolloutPhasePromoting)
	base := NewBase(isvcWith(read))
	live := read.DeepCopy()
	entry := live.Components[v1beta1.EngineComponent]
	entry.LatestReadyRevision, entry.LatestRolledoutRevision, entry.PreviousRolledoutRevision = "e-new", "e-new", "e-old"
	live.Components[v1beta1.EngineComponent] = entry
	desired := read.DeepCopy()
	entry = desired.Components[v1beta1.EngineComponent]
	entry.LatestReadyRevision = "e-stale"
	desired.Components[v1beta1.EngineComponent] = entry

	if !base.PreserveFresh(desired, live) || !base.Stale() {
		t.Fatal("a live record that differs in the rolled-out revisions alone was not detected")
	}
	got := desired.Components[v1beta1.EngineComponent]
	if got.LatestReadyRevision != "e-new" || got.LatestRolledoutRevision != "e-new" || got.PreviousRolledoutRevision != "e-old" {
		t.Fatalf("the live rolled-out revisions were not kept: ready %q latest %q previous %q",
			got.LatestReadyRevision, got.LatestRolledoutRevision, got.PreviousRolledoutRevision)
	}
}
