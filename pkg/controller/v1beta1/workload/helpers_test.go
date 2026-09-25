package workload

import (
	"context"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Test-only seams for the external test package: the hold pass is
// engine-internal (invoked only by Reconcile), and the snapshot's pod
// buckets are otherwise materialized from a client.

// HoldPassForTest invokes the hold pass alone — what a paused Component
// runs without the repair that follows it.
func HoldPassForTest(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, snapshot *ObservedSnapshot) error {
	_, err := runHoldPass(ctx, deps, input, plan, snapshot, nil)
	return err
}

// SnapshotWithPodsForTest returns a snapshot whose pod buckets are
// pre-materialized for both read sources, so tests supply the bucketed
// pods without a client.
func SnapshotWithPodsForTest(input types.ReconcileInput, byIdx map[int32][]*corev1.Pod) *ObservedSnapshot {
	return SnapshotWithDistinctPodsForTest(input, byIdx, byIdx)
}

// SnapshotWithDistinctPodsForTest keeps the API-reader and cache views
// separate so tests can model informer lag.
func SnapshotWithDistinctPodsForTest(input types.ReconcileInput, liveByIdx, cachedByIdx map[int32][]*corev1.Pod) *ObservedSnapshot {
	live, err := NewDecisionObservation(input.ObservedState.InstanceStatuses,
		NewAPIReaderSelectorPodObservation(nil, liveByIdx))
	if err != nil {
		panic(err)
	}
	cached, err := NewDecisionObservation(input.ObservedState.InstanceStatuses,
		NewCachedSelectorPodObservation(nil, cachedByIdx))
	if err != nil {
		panic(err)
	}
	return &ObservedSnapshot{
		input:  input,
		insts:  input.ObservedState.InstanceStatuses,
		live:   memoObservation{done: true, observation: live},
		cached: memoObservation{done: true, observation: cached},
	}
}
