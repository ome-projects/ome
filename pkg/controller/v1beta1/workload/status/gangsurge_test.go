package status

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func TestRestoreGangSurgeTargetMarker_StrongOwnershipGuard(t *testing.T) {
	source := terminalStatusFixture(3)
	surgeIndex := *source.Operation.SurgeIndex
	targetRevision := source.Operation.TargetRevision
	tests := []struct {
		name           string
		prepare        func(*terminalMutationStore)
		wantResolution GangSurgeTargetMarker
		wantWrites     int
	}{
		{
			name:           "exact source restores marker",
			wantResolution: GangSurgeTargetMarkerRestored,
			wantWrites:     1,
		},
		{
			name: "wire-normalized source restores marker",
			prepare: func(store *terminalMutationStore) {
				current := store.statuses[source.Index]
				current.Operation.StartedAt = metav1.NewTime(current.Operation.StartedAt.Truncate(time.Second))
				current.Operation.LastProgressAt = metav1.NewTime(current.Operation.LastProgressAt.Truncate(time.Second))
				store.statuses[source.Index] = current
			},
			wantResolution: GangSurgeTargetMarkerRestored,
			wantWrites:     1,
		},
		{
			name: "existing matching marker is confirmed",
			prepare: func(store *terminalMutationStore) {
				store.statuses[surgeIndex] = types.InstanceStatus{
					Index:          surgeIndex,
					Incarnation:    1,
					Phase:          types.InstancePhaseCreating,
					TargetRevision: targetRevision,
					Operation: &types.InstanceOperation{
						ID:             "existing-target",
						Type:           types.InstanceOperationUpdate,
						Step:           types.UpdateStepGangSurgeTarget,
						TargetRevision: targetRevision,
					},
				}
			},
			wantResolution: GangSurgeTargetMarkerActive,
		},
		{
			name: "existing cleanup marker is reported",
			prepare: func(store *terminalMutationStore) {
				marker := types.InstanceStatus{
					Index:          surgeIndex,
					Incarnation:    1,
					Phase:          types.InstancePhaseCreating,
					TargetRevision: targetRevision,
					Operation: &types.InstanceOperation{
						ID:             "existing-target",
						Type:           types.InstanceOperationUpdate,
						Step:           types.UpdateStepGangSurgeTargetCleanup,
						TargetRevision: targetRevision,
					},
				}
				store.statuses[surgeIndex] = marker
			},
			wantResolution: GangSurgeTargetMarkerCleanup,
		},
		{
			name: "source ownership drift rejects marker",
			prepare: func(store *terminalMutationStore) {
				current := store.statuses[source.Index]
				current.Operation.ID = "replacement-source"
				store.statuses[source.Index] = current
			},
		},
		{
			name: "owner recreation rejects marker",
			prepare: func(store *terminalMutationStore) {
				store.ownerUID = "owner-b"
			},
		},
		{
			name: "occupied target rejects marker",
			prepare: func(store *terminalMutationStore) {
				store.statuses[surgeIndex] = types.InstanceStatus{
					Index: surgeIndex, Incarnation: 4, Phase: types.InstancePhaseReady,
					RunningRevision: "unrelated",
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &terminalMutationStore{
				ownerUID: "owner-a",
				statuses: map[int32]types.InstanceStatus{source.Index: cloneTerminalStatus(source)},
			}
			if test.prepare != nil {
				test.prepare(store)
			}
			input := types.ReconcileInput{
				OwnerObject:                          &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "owner-a"}},
				FinalizeInstanceResources:            func(context.Context, int32) (bool, error) { return true, nil },
				ApplyInstanceMutationsWithRetryBlock: store.apply,
			}

			resolution, err := RestoreGangSurgeTarget(
				context.Background(), input, &source, surgeIndex, targetRevision, time.Minute,
			)
			if err != nil || resolution != test.wantResolution {
				t.Fatalf("result: resolution=%v err=%v", resolution, err)
			}
			if store.writes != test.wantWrites {
				t.Fatalf("row writes=%d want %d", store.writes, test.wantWrites)
			}
			if test.wantResolution == GangSurgeTargetMarkerRestored || test.wantResolution == GangSurgeTargetMarkerActive {
				marker := store.statuses[surgeIndex]
				if !gangSurgeTargetMatches(&marker, targetRevision) {
					t.Fatalf("target marker=%+v", marker)
				}
			}
		})
	}
}

func TestClaimGangSurgeDrain_RejectsAuthoritativePairDrift(t *testing.T) {
	const targetRevision = "pair-guard-engine-newrev"
	surgeIndex := int32(2)
	source := gangSurgeRecoverySource(surgeIndex, targetRevision)
	target := gangSurgeActiveTarget(surgeIndex, targetRevision)
	tests := []struct {
		name   string
		mutate func(map[int32]types.InstanceStatus)
	}{
		{
			name: "source lifecycle",
			mutate: func(statuses map[int32]types.InstanceStatus) {
				changed := statuses[source.Index]
				changed.Operation.ID = "replacement-source"
				statuses[source.Index] = changed
			},
		},
		{
			name: "target lifecycle",
			mutate: func(statuses map[int32]types.InstanceStatus) {
				changed := statuses[target.Index]
				changed.Operation.ID = "replacement-target"
				statuses[target.Index] = changed
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statuses := map[int32]types.InstanceStatus{
				source.Index: cloneTerminalStatus(source),
				target.Index: cloneTerminalStatus(target),
			}
			test.mutate(statuses)
			store := &terminalMutationStore{ownerUID: "owner-a", statuses: statuses}
			input := gangSurgeRecoveryInput("owner-a", "pair-guard", "test-ns", store,
				cloneTerminalStatus(source), cloneTerminalStatus(target))

			claimed, err := StampGangSurgeDrainStep(context.Background(), input, &source, &target)
			if err != nil || claimed || store.writes != 0 {
				t.Fatalf("claim with drift: claimed=%v writes=%d err=%v", claimed, store.writes, err)
			}
		})
	}
}

func TestPromoteGangSurgeTarget_RejectsAuthoritativePairDrift(t *testing.T) {
	const targetRevision = "promotion-guard-engine-newrev"
	surgeIndex := int32(2)
	source := gangSurgeRecoverySource(surgeIndex, targetRevision)
	source.Operation.Step = types.UpdateStepSurgeDrain
	target := gangSurgeActiveTarget(surgeIndex, targetRevision)
	tests := []struct {
		name   string
		mutate func(map[int32]types.InstanceStatus)
	}{
		{
			name: "source lifecycle",
			mutate: func(statuses map[int32]types.InstanceStatus) {
				changed := statuses[source.Index]
				changed.Operation.ID = "replacement-source"
				statuses[source.Index] = changed
			},
		},
		{
			name: "target lifecycle",
			mutate: func(statuses map[int32]types.InstanceStatus) {
				changed := statuses[target.Index]
				changed.Operation.ID = "replacement-target"
				statuses[target.Index] = changed
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statuses := map[int32]types.InstanceStatus{
				source.Index: cloneTerminalStatus(source),
				target.Index: cloneTerminalStatus(target),
			}
			test.mutate(statuses)
			store := &terminalMutationStore{ownerUID: "owner-a", statuses: statuses}
			input := gangSurgeRecoveryInput("owner-a", "promotion-guard", "test-ns", store,
				cloneTerminalStatus(source), cloneTerminalStatus(target))

			promoted, err := StampGangSurgeTargetReady(context.Background(), input, &source, &target, targetRevision)
			if err != nil || promoted || store.writes != 0 {
				t.Fatalf("promotion with drift: promoted=%v writes=%d err=%v", promoted, store.writes, err)
			}
			if current := store.statuses[target.Index]; current.Operation == nil || current.Operation.ID != "replacement-target" && test.name == "target lifecycle" {
				t.Fatalf("promotion changed authoritative target: %+v", current)
			}
		})
	}
}

func TestGangSurge_TargetConflictRollbackGuardsAuthoritativePair(t *testing.T) {
	surgeIndex := int32(2)
	const targetRevision = "gang-pair-guard-engine-newrev"
	source := gangSurgeRecoverySource(surgeIndex, targetRevision)
	occupied := types.InstanceStatus{
		Index:           surgeIndex,
		Incarnation:     8,
		Phase:           types.InstancePhaseReady,
		RunningRevision: "unrelated-revision",
	}
	tests := []struct {
		name       string
		ownerUID   k8stypes.UID
		storeUID   k8stypes.UID
		mutateLive func(map[int32]types.InstanceStatus)
	}{
		{
			name:     "owner changed",
			ownerUID: "owner-a",
			storeUID: "owner-b",
		},
		{
			name:     "source lifecycle changed",
			ownerUID: "owner-a",
			storeUID: "owner-a",
			mutateLive: func(statuses map[int32]types.InstanceStatus) {
				changed := statuses[source.Index]
				changed.Operation.Step = types.UpdateStepSurgeDrain
				statuses[source.Index] = changed
			},
		},
		{
			name:     "target lifecycle changed",
			ownerUID: "owner-a",
			storeUID: "owner-a",
			mutateLive: func(statuses map[int32]types.InstanceStatus) {
				changed := statuses[occupied.Index]
				changed.Incarnation++
				statuses[occupied.Index] = changed
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statuses := map[int32]types.InstanceStatus{
				source.Index:   cloneTerminalStatus(source),
				occupied.Index: cloneTerminalStatus(occupied),
			}
			if test.mutateLive != nil {
				test.mutateLive(statuses)
			}
			store := &terminalMutationStore{ownerUID: test.storeUID, statuses: statuses}
			input := gangSurgeRecoveryInput(test.ownerUID, "gang-pair-guard", "test-ns", store,
				cloneTerminalStatus(source), cloneTerminalStatus(occupied))

			reset, err := ResetGangSurgeSource(context.Background(), input, &source, &occupied)
			if err != nil || reset {
				t.Fatalf("guarded rollback: reset=%v err=%v", reset, err)
			}
			if store.writes != 0 {
				t.Fatalf("guarded rollback status writes=%d want 0", store.writes)
			}
		})
	}
}

// The cleanup step lands on the marker only while both rows are still the
// pair the pass read; a marker re-pinned since is left alone, and one
// already on the cleanup step is confirmed without a write.
func TestStampGangSurgeTargetCleanupStep_RejectsPairDriftAndConfirmsItsOwnWrite(t *testing.T) {
	const targetRevision = "cleanup-guard-engine-newrev"
	surgeIndex := int32(2)
	source := gangSurgeRecoverySource(surgeIndex, targetRevision)
	marker := gangSurgeActiveTarget(surgeIndex, targetRevision)

	drifted := &terminalMutationStore{ownerUID: "owner-a", statuses: map[int32]types.InstanceStatus{
		source.Index: cloneTerminalStatus(source), marker.Index: cloneTerminalStatus(marker),
	}}
	changed := drifted.statuses[marker.Index]
	changed.Operation.ID = "replacement-target"
	drifted.statuses[marker.Index] = changed
	input := gangSurgeRecoveryInput("owner-a", "cleanup-guard", "test-ns", drifted, cloneTerminalStatus(source), cloneTerminalStatus(marker))
	stepped, err := StampGangSurgeTargetCleanupStep(context.Background(), input, &source, &marker)
	if err != nil || stepped || drifted.writes != 0 {
		t.Fatalf("cleanup step with drift: stepped=%v writes=%d err=%v", stepped, drifted.writes, err)
	}

	store := &terminalMutationStore{ownerUID: "owner-a", statuses: map[int32]types.InstanceStatus{
		source.Index: cloneTerminalStatus(source), marker.Index: cloneTerminalStatus(marker),
	}}
	input = gangSurgeRecoveryInput("owner-a", "cleanup-guard", "test-ns", store, cloneTerminalStatus(source), cloneTerminalStatus(marker))
	stepped, err = StampGangSurgeTargetCleanupStep(context.Background(), input, &source, &marker)
	if err != nil || !stepped || store.writes != 1 {
		t.Fatalf("cleanup step: stepped=%v writes=%d err=%v", stepped, store.writes, err)
	}
	if !GangSurgeCleanupTargetClaimMatches(ptr(store.statuses[marker.Index]), targetRevision) {
		t.Fatalf("marker = %+v, want the cleanup step", store.statuses[marker.Index])
	}
	stepped, err = StampGangSurgeTargetCleanupStep(context.Background(), input, &source, &marker)
	if err != nil || !stepped || store.writes != 1 {
		t.Fatalf("repeated cleanup step: stepped=%v writes=%d err=%v, want confirmed without a write", stepped, store.writes, err)
	}
}

// The cleanup marker is restored only against the source the pass read:
// an absent slot gets it, a live marker is moved onto it, and a source
// whose lifecycle drifted writes nothing.
func TestRestoreGangSurgeTargetCleanup_FencesOnTheSource(t *testing.T) {
	const targetRevision = "restore-cleanup-engine-newrev"
	surgeIndex := int32(2)
	source := gangSurgeRecoverySource(surgeIndex, targetRevision)

	drifted := &terminalMutationStore{ownerUID: "owner-a", statuses: map[int32]types.InstanceStatus{source.Index: cloneTerminalStatus(source)}}
	changed := drifted.statuses[source.Index]
	changed.Operation.ID = "replacement-source"
	drifted.statuses[source.Index] = changed
	input := gangSurgeRecoveryInput("owner-a", "restore-cleanup", "test-ns", drifted, cloneTerminalStatus(source))
	restored, err := RestoreGangSurgeTargetCleanup(context.Background(), input, &source, surgeIndex, targetRevision, time.Minute)
	if err != nil || restored || drifted.writes != 0 {
		t.Fatalf("restore with drift: restored=%v writes=%d err=%v", restored, drifted.writes, err)
	}

	absent := &terminalMutationStore{ownerUID: "owner-a", statuses: map[int32]types.InstanceStatus{source.Index: cloneTerminalStatus(source)}}
	input = gangSurgeRecoveryInput("owner-a", "restore-cleanup", "test-ns", absent, cloneTerminalStatus(source))
	restored, err = RestoreGangSurgeTargetCleanup(context.Background(), input, &source, surgeIndex, targetRevision, time.Minute)
	if err != nil || !restored || absent.writes != 1 || !GangSurgeCleanupTargetClaimMatches(ptr(absent.statuses[surgeIndex]), targetRevision) {
		t.Fatalf("restore into an absent slot: restored=%v writes=%d err=%v marker=%+v", restored, absent.writes, err, absent.statuses[surgeIndex])
	}

	live := &terminalMutationStore{ownerUID: "owner-a", statuses: map[int32]types.InstanceStatus{
		source.Index: cloneTerminalStatus(source), surgeIndex: gangSurgeActiveTarget(surgeIndex, targetRevision),
	}}
	input = gangSurgeRecoveryInput("owner-a", "restore-cleanup", "test-ns", live, cloneTerminalStatus(source))
	restored, err = RestoreGangSurgeTargetCleanup(context.Background(), input, &source, surgeIndex, targetRevision, time.Minute)
	if err != nil || !restored || live.writes != 1 || !GangSurgeCleanupTargetClaimMatches(ptr(live.statuses[surgeIndex]), targetRevision) {
		t.Fatalf("restore over a live marker: restored=%v writes=%d err=%v marker=%+v", restored, live.writes, err, live.statuses[surgeIndex])
	}
}

// The reset-and-remove commits the source reset and the marker removal
// together, only while both rows are the pair the pass read; a marker
// that drifted since writes nothing and finalizes nothing.
func TestResetGangSurgeSourceAndRemoveMarker_CommitsBothOrNeither(t *testing.T) {
	const targetRevision = "abandon-engine-newrev"
	surgeIndex := int32(2)
	source := gangSurgeRecoverySource(surgeIndex, targetRevision)
	marker := gangSurgeActiveTarget(surgeIndex, targetRevision)
	marker.Operation.Step = types.UpdateStepGangSurgeTargetCleanup
	deps := types.Deps{Expectations: types.NewExpectations()}

	drifted := &terminalMutationStore{ownerUID: "owner-a", statuses: map[int32]types.InstanceStatus{
		source.Index: cloneTerminalStatus(source), marker.Index: cloneTerminalStatus(marker),
	}}
	changed := drifted.statuses[marker.Index]
	changed.Incarnation++
	drifted.statuses[marker.Index] = changed
	finalizes := 0
	input := gangSurgeRecoveryInput("owner-a", "abandon", "test-ns", drifted, cloneTerminalStatus(source), cloneTerminalStatus(marker))
	input.FinalizeInstanceResources = func(context.Context, int32) (bool, error) { finalizes++; return true, nil }
	reset, err := ResetGangSurgeSourceAndRemoveMarker(context.Background(), deps, input, &source, &marker, surgeIndex, source.RunningRevision, targetRevision, "Stuck", true)
	if err != nil || reset || drifted.writes != 0 || finalizes != 0 {
		t.Fatalf("reset with drift: reset=%v writes=%d finalizes=%d err=%v", reset, drifted.writes, finalizes, err)
	}

	store := &terminalMutationStore{ownerUID: "owner-a", statuses: map[int32]types.InstanceStatus{
		source.Index: cloneTerminalStatus(source), marker.Index: cloneTerminalStatus(marker),
	}}
	input = gangSurgeRecoveryInput("owner-a", "abandon", "test-ns", store, cloneTerminalStatus(source), cloneTerminalStatus(marker))
	input.FinalizeInstanceResources = func(context.Context, int32) (bool, error) { finalizes++; return true, nil }
	reset, err = ResetGangSurgeSourceAndRemoveMarker(context.Background(), deps, input, &source, &marker, surgeIndex, source.RunningRevision, targetRevision, "Stuck", true)
	if err != nil || !reset || store.writes != 1 || finalizes != 1 {
		t.Fatalf("reset: reset=%v writes=%d finalizes=%d err=%v", reset, store.writes, finalizes, err)
	}
	if _, present := store.statuses[marker.Index]; present {
		t.Fatal("the marker must be removed in the same write")
	}
	if got := store.statuses[source.Index]; got.Phase != types.InstancePhaseReady || got.Operation != nil || got.RunningRevision != source.RunningRevision {
		t.Fatalf("source = %+v, want Ready on its running revision", got)
	}
}
