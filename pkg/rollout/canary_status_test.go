package rollout

import (
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestCanaryStatusForDoesNotLeakAcrossUnits pins the property that makes
// per-unit canary runs safe: once any unit has recorded a run, a unit that has
// not bound one reads nil rather than whichever run reached the legacy alias
// first. Units advance on their own ladders, so inheriting another unit's
// revision pair would step one unit's canary and roll it back to the other's
// stable.
func TestCanaryStatusForDoesNotLeakAcrossUnits(t *testing.T) {
	s := &v1beta1.InferenceServiceStatus{Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.RouterComponent: {},
		v1beta1.EngineComponent: {},
	}}

	SetCanaryStatusFor(s, v1beta1.EngineComponent, &v1beta1.CanaryStatus{
		TargetID:           "engine-run",
		CanaryRevisionHash: "engine-canary",
		StableRevisionHash: "engine-stable",
	})

	if got := CanaryStatusFor(s, v1beta1.RouterComponent); got != nil {
		t.Fatalf("router with no run read the engine's: canary=%q stable=%q",
			got.CanaryRevisionHash, got.StableRevisionHash)
	}
	if got := CanaryStatusFor(s, v1beta1.EngineComponent); got == nil || got.CanaryRevisionHash != "engine-canary" {
		t.Fatalf("engine lost its own run: %+v", got)
	}

	// The decoder is not its own unit; it reads the engine's run.
	if got := CanaryStatusFor(s, v1beta1.DecoderComponent); got == nil || got.CanaryRevisionHash != "engine-canary" {
		t.Fatalf("decoder must read the engine unit's run, got %+v", got)
	}

	// Each unit keeps its own revisions once both are bound.
	SetCanaryStatusFor(s, v1beta1.RouterComponent, &v1beta1.CanaryStatus{
		TargetID:           "router-run",
		CanaryRevisionHash: "router-canary",
		StableRevisionHash: "router-stable",
	})
	if got := CanaryStatusFor(s, v1beta1.RouterComponent); got.StableRevisionHash != "router-stable" {
		t.Fatalf("router stable = %q, want router-stable", got.StableRevisionHash)
	}
	if got := CanaryStatusFor(s, v1beta1.EngineComponent); got.StableRevisionHash != "engine-stable" {
		t.Fatalf("engine stable = %q, want engine-stable", got.StableRevisionHash)
	}
}

// TestCanaryStatusForLegacyAlias pins the migration path the alias exists for: a
// status persisted before per-unit state carries the run only in the legacy
// field, and the unit must still find it there rather than restarting the run
// from step 0.
func TestCanaryStatusForLegacyAlias(t *testing.T) {
	s := &v1beta1.InferenceServiceStatus{
		Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{v1beta1.EngineComponent: {}},
		Canary:     &v1beta1.CanaryStatus{TargetID: "legacy-run", CurrentStep: 3},
	}

	adopted := CanaryStatusFor(s, v1beta1.EngineComponent)
	if adopted == nil || adopted.CurrentStep != 3 {
		t.Fatalf("un-migrated run must be adopted, got %+v", adopted)
	}

	// Once adopted, the run is recorded per unit and the alias is re-derived
	// from it, so legacy readers keep following the run it continues.
	adopted.CurrentStep = 4
	SetCanaryStatusFor(s, v1beta1.EngineComponent, adopted)
	SyncLegacyCanaryAlias(s)
	if s.Canary == nil || s.Canary.CurrentStep != 4 {
		t.Fatalf("adopting unit must keep the alias current, got %+v", s.Canary)
	}
}

// TestSyncLegacyCanaryAliasFollowsEntrypoint pins that the legacy alias is
// derived from per-unit state on every sync, so it tracks the entrypoint unit's
// run as it steps instead of freezing at the state it was first published with.
// The router is the entrypoint when it has a run.
func TestSyncLegacyCanaryAliasFollowsEntrypoint(t *testing.T) {
	s := &v1beta1.InferenceServiceStatus{Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {},
	}}
	SetCanaryStatusFor(s, v1beta1.EngineComponent, &v1beta1.CanaryStatus{TargetID: "run", CurrentStep: 0})
	SyncLegacyCanaryAlias(s)
	if s.Canary == nil || s.Canary.CurrentStep != 0 {
		t.Fatalf("alias must publish the engine unit's run, got %+v", s.Canary)
	}

	// The run advances through a status round-trip, which leaves the alias a
	// separate object from the run the executor mutates.
	s.Canary = s.Canary.DeepCopy()
	CanaryStatusFor(s, v1beta1.EngineComponent).CurrentStep = 2
	SyncLegacyCanaryAlias(s)
	if s.Canary.CurrentStep != 2 {
		t.Fatalf("alias froze at step %d while the run advanced to 2", s.Canary.CurrentStep)
	}

	// A router run takes over the alias: it is the entrypoint.
	SetCanaryStatusFor(s, v1beta1.RouterComponent, &v1beta1.CanaryStatus{TargetID: "router-run", CurrentStep: 1})
	SyncLegacyCanaryAlias(s)
	if s.Canary.TargetID != "router-run" {
		t.Fatalf("alias must follow the router entrypoint, got %q", s.Canary.TargetID)
	}
	// ...without disturbing the engine unit's own run.
	if got := CanaryStatusFor(s, v1beta1.EngineComponent); got.CurrentStep != 2 {
		t.Fatalf("engine unit lost its step: %+v", got)
	}
}

// TestSyncLegacyCanaryAliasKeepsUnmigratedRun pins that a status with no
// per-unit state is left alone: the alias may be the only copy of a run
// persisted before per-unit state existed, and clearing it would restart that
// run from step 0.
func TestSyncLegacyCanaryAliasKeepsUnmigratedRun(t *testing.T) {
	s := &v1beta1.InferenceServiceStatus{
		Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{v1beta1.EngineComponent: {}},
		Canary:     &v1beta1.CanaryStatus{TargetID: "legacy-run", CurrentStep: 3},
	}
	SyncLegacyCanaryAlias(s)
	if s.Canary == nil || s.Canary.CurrentStep != 3 {
		t.Fatalf("un-migrated run must survive a sync, got %+v", s.Canary)
	}
}
