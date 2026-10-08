package escalation_test

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestPerComponentMaxSurgeBudget_Integer covers the int form across
// the documented edge cases. nil → BudgetNoLimit so the dispatcher
// defers to the coordination-group layer; 0 → 0 (RecreatePod-like
// behavior for this layer); positive integers pass through verbatim.
func TestPerComponentMaxSurgeBudget_Integer(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    *types.RollingUpdate
		replicas int32
		want     int32
	}{
		{
			name:     "nil RollingUpdate returns BudgetNoLimit",
			input:    nil,
			replicas: 4,
			want:     escalation.BudgetNoLimit,
		},
		{
			name:     "nil MaxSurge returns BudgetNoLimit",
			input:    &types.RollingUpdate{},
			replicas: 4,
			want:     escalation.BudgetNoLimit,
		},
		{
			name:     "MaxSurge=1 returns 1 regardless of replicas",
			input:    &types.RollingUpdate{MaxSurge: intOrStringInt(1)},
			replicas: 5,
			want:     1,
		},
		{
			name:     "MaxSurge=0 returns 0 (no surge allowed by this layer)",
			input:    &types.RollingUpdate{MaxSurge: intOrStringInt(0)},
			replicas: 5,
			want:     0,
		},
		{
			name:     "negative MaxSurge clamps to 0 (defensive)",
			input:    &types.RollingUpdate{MaxSurge: intOrStringInt(-3)},
			replicas: 5,
			want:     0,
		},
		{
			name:     "MaxSurge=N permitted larger than replicas",
			input:    &types.RollingUpdate{MaxSurge: intOrStringInt(10)},
			replicas: 3,
			want:     10,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := escalation.PerComponentMaxSurgeBudget(tc.input, tc.replicas)
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// TestPerComponentMaxSurgeBudget_Percent covers the percent form with
// ceil rounding so a 25% expression on 4 replicas yields 1 (not 0). The
// task spec calls out this specific case as the expected behavior.
func TestPerComponentMaxSurgeBudget_Percent(t *testing.T) {
	for _, tc := range []struct {
		name     string
		pct      string
		replicas int32
		want     int32
	}{
		{name: "25% on 4 replicas → 1 (ceil)", pct: "25%", replicas: 4, want: 1},
		{name: "25% on 8 replicas → 2", pct: "25%", replicas: 8, want: 2},
		{name: "50% on 10 replicas → 5", pct: "50%", replicas: 10, want: 5},
		{name: "100% on 4 replicas → 4 (capped at total)", pct: "100%", replicas: 4, want: 4},
		{name: "0% on N replicas → 0", pct: "0%", replicas: 10, want: 0},
		{name: "33% on 6 replicas → 2 (ceil of 1.98)", pct: "33%", replicas: 6, want: 2},
		{name: "1% on 1 replica → 1 (ceil of 0.01)", pct: "1%", replicas: 1, want: 1},
		{name: "25% on 0 replicas → 0 (zero base)", pct: "25%", replicas: 0, want: 0},
		{name: "150% clamps to 100% → equals replicas", pct: "150%", replicas: 5, want: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ru := &types.RollingUpdate{MaxSurge: intOrStringStr(tc.pct)}
			got := escalation.PerComponentMaxSurgeBudget(ru, tc.replicas)
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// TestPerComponentMaxUnavailableBudget_Integer mirrors the surge
// integer case for the unavailability budget.
func TestPerComponentMaxUnavailableBudget_Integer(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    *types.RollingUpdate
		replicas int32
		want     int32
	}{
		{
			name:     "nil RollingUpdate returns BudgetNoLimit",
			input:    nil,
			replicas: 4,
			want:     escalation.BudgetNoLimit,
		},
		{
			name:     "nil MaxUnavailable returns BudgetNoLimit",
			input:    &types.RollingUpdate{MaxSurge: intOrStringInt(1)},
			replicas: 4,
			want:     escalation.BudgetNoLimit,
		},
		{
			name:     "MaxUnavailable=2 returns 2",
			input:    &types.RollingUpdate{MaxUnavailable: intOrStringInt(2)},
			replicas: 5,
			want:     2,
		},
		{
			name:     "MaxUnavailable=0 returns 0",
			input:    &types.RollingUpdate{MaxUnavailable: intOrStringInt(0)},
			replicas: 5,
			want:     0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := escalation.PerComponentMaxUnavailableBudget(tc.input, tc.replicas)
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// TestPerComponentMaxUnavailableBudget_Percent mirrors the surge
// percent case for the unavailability budget. The non-exact rows pin
// the ceil rounding — a floor regression would return 1 for 33% of 6
// and 0 for 1% of 1.
func TestPerComponentMaxUnavailableBudget_Percent(t *testing.T) {
	for _, tc := range []struct {
		name     string
		pct      string
		replicas int32
		want     int32
	}{
		{name: "25% on 4 replicas → 1 (exact)", pct: "25%", replicas: 4, want: 1},
		{name: "33% on 6 replicas → 2 (ceil of 1.98)", pct: "33%", replicas: 6, want: 2},
		{name: "1% on 1 replica → 1 (ceil of 0.01)", pct: "1%", replicas: 1, want: 1},
		{name: "0% on N replicas → 0", pct: "0%", replicas: 10, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ru := &types.RollingUpdate{MaxUnavailable: intOrStringStr(tc.pct)}
			got := escalation.PerComponentMaxUnavailableBudget(ru, tc.replicas)
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// TestCurrentSurgeInFlight verifies the per-Instance step-set counter
// the dispatcher feeds into the budget projection. Only surge lifecycle
// steps contribute; other ops (Drain, InPlace) don't add
// pods.
func TestCurrentSurgeInFlight(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []types.InstanceStatus
		want int32
	}{
		{name: "empty", in: nil, want: 0},
		{name: "no operation", in: []types.InstanceStatus{
			{Index: 0, Phase: types.InstancePhaseReady},
		}, want: 0},
		{name: "one Surge step", in: []types.InstanceStatus{
			{Index: 0, Operation: &types.InstanceOperation{Step: "Surge"}},
		}, want: 1},
		{name: "one SurgeDrain step", in: []types.InstanceStatus{
			{Index: 0, Operation: &types.InstanceOperation{Step: "SurgeDrain"}},
		}, want: 1},
		{name: "one SurgeDrainSettle step", in: []types.InstanceStatus{
			{Index: 0, Operation: &types.InstanceOperation{Step: "SurgeDrainSettle"}},
		}, want: 1},
		{name: "two surge variants and one in-place skipped", in: []types.InstanceStatus{
			{Index: 0, Operation: &types.InstanceOperation{Step: "Surge"}},
			{Index: 1, Operation: &types.InstanceOperation{Step: "SurgeDrain"}},
			{Index: 2, Operation: &types.InstanceOperation{Step: "InPlace"}}, // skipped
			{Index: 3, Operation: &types.InstanceOperation{Step: "Drain"}},   // skipped
		}, want: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := escalation.CurrentSurgeInFlight(tc.in)
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// TestCurrentUnavailableInFlight verifies the dual counter — Updating
// minus the surge lifecycle steps (which preserve steady capacity).
func TestCurrentUnavailableInFlight(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []types.InstanceStatus
		want int32
	}{
		{name: "empty", in: nil, want: 0},
		{name: "Ready not counted", in: []types.InstanceStatus{
			{Index: 0, Phase: types.InstancePhaseReady},
		}, want: 0},
		{name: "Updating + InPlace counted", in: []types.InstanceStatus{
			{Index: 0, Phase: types.InstancePhaseUpdating, Operation: &types.InstanceOperation{Step: "InPlace"}},
		}, want: 1},
		{name: "Updating + Drain counted", in: []types.InstanceStatus{
			{Index: 0, Phase: types.InstancePhaseUpdating, Operation: &types.InstanceOperation{Step: "Drain"}},
		}, want: 1},
		{name: "Updating + Surge skipped (no offline pod)", in: []types.InstanceStatus{
			{Index: 0, Phase: types.InstancePhaseUpdating, Operation: &types.InstanceOperation{Step: "Surge"}},
		}, want: 0},
		{name: "Updating + SurgeDrain skipped", in: []types.InstanceStatus{
			{Index: 0, Phase: types.InstancePhaseUpdating, Operation: &types.InstanceOperation{Step: "SurgeDrain"}},
		}, want: 0},
		{name: "Updating + SurgeDrainSettle skipped", in: []types.InstanceStatus{
			{Index: 0, Phase: types.InstancePhaseUpdating, Operation: &types.InstanceOperation{Step: "SurgeDrainSettle"}},
		}, want: 0},
		{name: "Updating without Operation counted (defensive)", in: []types.InstanceStatus{
			{Index: 0, Phase: types.InstancePhaseUpdating},
		}, want: 1},
		{name: "Failed update + Drain remains counted while retrying", in: []types.InstanceStatus{
			{Index: 0, Phase: types.InstancePhaseFailed, Operation: &types.InstanceOperation{
				Type: types.InstanceOperationUpdate, Step: "Drain",
			}},
		}, want: 1},
		{name: "Failed update + InPlace remains counted while retrying", in: []types.InstanceStatus{
			{Index: 0, Phase: types.InstancePhaseFailed, Operation: &types.InstanceOperation{
				Type: types.InstanceOperationUpdate, Step: "InPlace",
			}},
		}, want: 1},
		{name: "Failed update + Surge skipped because source remains online", in: []types.InstanceStatus{
			{Index: 0, Phase: types.InstancePhaseFailed, Operation: &types.InstanceOperation{
				Type: types.InstanceOperationUpdate, Step: "Surge",
			}},
		}, want: 0},
		{name: "Failed restart is outside the update budget", in: []types.InstanceStatus{
			{Index: 0, Phase: types.InstancePhaseFailed, Operation: &types.InstanceOperation{
				Type: types.InstanceOperationRestart, Step: "Drain",
			}},
		}, want: 0},
		{name: "Failed without an operation is outside the in-flight budget", in: []types.InstanceStatus{
			{Index: 0, Phase: types.InstancePhaseFailed},
		}, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := escalation.CurrentUnavailableInFlight(tc.in)
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// Helpers for IntOrString construction in tests.

func intOrStringInt(v int) *intstr.IntOrString {
	x := intstr.FromInt(v)
	return &x
}

func intOrStringStr(s string) *intstr.IntOrString {
	x := intstr.FromString(s)
	return &x
}

// Per-Component RollingUpdate.MaxSurge / MaxUnavailable dispatcher
// integration. The budget calculations themselves are covered in
// budget_test.go; these tests pin the dispatcher behavior when the
// per-Component layer denies, allows, and composes with the group-
// level UpdateGate. The end-to-end pod surge behavior at scale is
// covered in the KIND suite (tests/integration/.../omenative_coordination_kind/).

// TestReconcile_PerComponentMaxSurge_DeniesAtBudget pins the
// per-Component gate refusing to start a fresh surge when the layer's
// budget is already exhausted by a prior-pass in-flight surge.
// Setup: 3 plan instances, all needing update; budget=1; one Instance
// already in Phase=Updating + Step=Surge from a prior wake-up. The
// dispatcher MUST NOT start a fresh surge — it should requeue at the
// gate interval (Requeue / RequeueAfter > 0) and leave the in-flight
// Instance alone.
func TestReconcile_PerComponentMaxSurge_DeniesAtBudget(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := types.Deps{Client: c}

	in := minimalInput(t)
	in.DesiredSpec.Replicas = 3
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		// Index 0: already mid-surge from a prior pass (charged to
		// priorSurgeInFlight=1). The dispatcher's Update call on this
		// one is allowed (startingFresh=false).
		{
			Index: 0, Incarnation: 1,
			Phase: types.InstancePhaseUpdating, RunningRevision: "prior-rev",
			Operation: &types.InstanceOperation{
				Type: types.InstanceOperationUpdate, Step: "Surge",
			},
		},
		// Indices 1 and 2: Ready on prior-rev. Should be gated because
		// the per-Component MaxSurge=1 is already consumed by index 0.
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
	}
	// Track which Instance indices the dispatcher attempted to start.
	// MutateInstance is the cheapest signal — any Update call mutates
	// status. Index 0 is allowed (already in progress); indices 1 / 2
	// must NOT mutate when the gate denies.
	mutatedIndices := map[int32]int{}
	in.MutateInstance = func(_ context.Context, idx int32, fn func(*types.InstanceStatus) bool) error {
		mutatedIndices[idx]++
		for i := range in.ObservedState.InstanceStatuses {
			if in.ObservedState.InstanceStatuses[i].Index == idx {
				_ = fn(&in.ObservedState.InstanceStatuses[i])
				break
			}
		}
		return nil
	}

	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  3,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: types.UpdateStrategy{
			Type: types.UpdateStrategySurgeThenDrain,
			RollingUpdate: &types.RollingUpdate{
				MaxSurge: intOrStringInt(1),
			},
		},
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	result, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		// Per-op machines can error on the empty fake client (no live
		// pods to drain). The contract under test is the gate behavior
		// — not the per-op success.
		t.Logf("Reconcile op error (expected against empty fake client): %v", err)
	}

	// Load-bearing assertions:
	// 1) Indices 1 and 2 must NOT have been started — the per-Component
	//    MaxSurge=1 is fully consumed by the prior-pass index 0.
	if mutatedIndices[1] > 0 {
		t.Errorf("index 1 must be gated by per-Component MaxSurge=1; got %d mutations", mutatedIndices[1])
	}
	if mutatedIndices[2] > 0 {
		t.Errorf("index 2 must be gated by per-Component MaxSurge=1; got %d mutations", mutatedIndices[2])
	}
	// 2) The dispatcher must requeue — at least one Instance was gated.
	if !result.Requeue && result.RequeueAfter == 0 {
		t.Errorf("dispatcher must Requeue when any Instance was gated; got %+v", result)
	}
}

// TestReconcile_PerComponentMaxSurge_AllowsWithinBudget is the dual:
// when the per-Component MaxSurge has headroom, the dispatcher allows
// at least one fresh start this wake-up. Setup: 3 plan instances all
// Ready on prior-rev (no prior-pass in-flight), budget=1, no group-
// level UpdateGate. Exactly one Instance should be started this pass.
func TestReconcile_PerComponentMaxSurge_AllowsWithinBudget(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := types.Deps{Client: c}

	in := minimalInput(t)
	in.DesiredSpec.Replicas = 3
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
	}
	mutatedIndices := map[int32]int{}
	in.MutateInstance = func(_ context.Context, idx int32, fn func(*types.InstanceStatus) bool) error {
		mutatedIndices[idx]++
		for i := range in.ObservedState.InstanceStatuses {
			if in.ObservedState.InstanceStatuses[i].Index == idx {
				_ = fn(&in.ObservedState.InstanceStatuses[i])
				break
			}
		}
		return nil
	}

	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  3,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: types.UpdateStrategy{
			Type: types.UpdateStrategySurgeThenDrain,
			RollingUpdate: &types.RollingUpdate{
				MaxSurge: intOrStringInt(1),
			},
		},
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	_, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Logf("Reconcile op error (expected against empty fake client): %v", err)
	}

	// Load-bearing: exactly one Instance was started (other two gated).
	// Count distinct indices that mutated at least once.
	started := 0
	for _, count := range mutatedIndices {
		if count > 0 {
			started++
		}
	}
	if started != 1 {
		t.Errorf("with MaxSurge=1 and 3 Ready instances, expected exactly 1 fresh start; got %d (mutations=%v)", started, mutatedIndices)
	}
}

// TestReconcile_PerComponentMaxSurge_NilDefersToGroupGate verifies the
// composition rule when the per-Component layer is unset: the
// dispatcher MUST defer to whatever the group-level UpdateGate
// decides (the cited "BudgetNoLimit means no cap" behavior). We wire
// an UpdateGate that always denies and assert nothing starts.
func TestReconcile_PerComponentMaxSurge_NilDefersToGroupGate(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := types.Deps{Client: c}

	in := minimalInput(t)
	in.DesiredSpec.Replicas = 2
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
	}
	// Per-Component RollingUpdate left nil (BudgetNoLimit). The group
	// gate should be the only constraint.
	in.UpdateGate = func(_ types.UpdateStrategyType, _, _ int32) (bool, types.RolloutHoldGate, string) {
		return false, types.RolloutHoldGateBudget, "group gate denies"
	}
	mutatedIndices := map[int32]int{}
	in.MutateInstance = func(_ context.Context, idx int32, fn func(*types.InstanceStatus) bool) error {
		mutatedIndices[idx]++
		for i := range in.ObservedState.InstanceStatuses {
			if in.ObservedState.InstanceStatuses[i].Index == idx {
				_ = fn(&in.ObservedState.InstanceStatuses[i])
				break
			}
		}
		return nil
	}

	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  2,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: types.UpdateStrategy{Type: types.UpdateStrategySurgeThenDrain},
		// RollingUpdate nil → per-Component layer doesn't cap.
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	result, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Logf("Reconcile op error (expected against empty fake client): %v", err)
	}

	// Load-bearing: NO fresh starts (group gate said deny, per-Component
	// didn't cap so deferred to the group gate).
	for idx, count := range mutatedIndices {
		if count > 0 {
			t.Errorf("group gate denied yet index %d still mutated %d times", idx, count)
		}
	}
	// And the dispatcher must requeue.
	if !result.Requeue && result.RequeueAfter == 0 {
		t.Errorf("dispatcher must Requeue when all Instances are gated; got %+v", result)
	}
}

// TestReconcile_FailedUpdateHoldsMaxUnavailableBudget is the two-replica
// outage regression. Once the first RecreatePod update times out, its
// preserved Update/Drain operation must keep MaxUnavailable=1 exhausted while
// that Instance retries. The other healthy Instance must remain gated.
func TestReconcile_FailedUpdateHoldsMaxUnavailableBudget(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := types.Deps{Client: c}

	in := minimalInput(t)
	in.DesiredSpec.Replicas = 2
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		// Index 0: replacement timed out after the source was drained.
		{
			Index: 0, Incarnation: 2,
			Phase: types.InstancePhaseFailed, RunningRevision: "prior-rev",
			Operation: &types.InstanceOperation{
				Type: types.InstanceOperationUpdate, Step: "Drain",
			},
		},
		// Index 1: Ready, needs update.
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
	}
	mutatedIndices := map[int32]int{}
	in.MutateInstance = func(_ context.Context, idx int32, fn func(*types.InstanceStatus) bool) error {
		mutatedIndices[idx]++
		for i := range in.ObservedState.InstanceStatuses {
			if in.ObservedState.InstanceStatuses[i].Index == idx {
				_ = fn(&in.ObservedState.InstanceStatuses[i])
				break
			}
		}
		return nil
	}

	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  2,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: types.UpdateStrategy{
			Type: types.UpdateStrategyRecreatePod,
			RollingUpdate: &types.RollingUpdate{
				MaxUnavailable: intOrStringInt(1),
			},
		},
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	result, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Logf("Reconcile op error (expected against empty fake client): %v", err)
	}

	// Index 1 must be gated (budget=1 consumed by index 0).
	if mutatedIndices[1] > 0 {
		t.Errorf("index 1 must be gated by per-Component MaxUnavailable=1; got %d mutations", mutatedIndices[1])
	}
	if !result.Requeue && result.RequeueAfter == 0 {
		t.Errorf("dispatcher must Requeue when Instance is gated; got %+v", result)
	}
}

// TestReconcile_FailedRestart_RecreateBypassesCoordGate is the
// coordination-gate starvation regression. A Failed Instance with a
// preserved Restart operation and zero serving pods (restart-deadline
// expiry after node loss) starts fresh, and the coordination gate's
// unavailability accounting is serving-based — the dead Instance is
// already inside currentUnavailable. Consulting the gate for its own
// recreate projects current+1 over a MaxUnavailable=1 group budget on
// every pass: the recreate is denied forever at the gate requeue
// interval and nothing else can raise ServingReplicas, because the
// denied recreate IS the recovery. The recreate must skip the gate
// consult (its outage is already charged) while the per-Component
// budget still applies, and the gate delta seen by later consults in
// the same pass must not be inflated by the exempt start.
func TestReconcile_FailedRestart_RecreateBypassesCoordGate(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := types.Deps{Client: c}

	in := minimalInput(t)
	in.DesiredSpec.Replicas = 2
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		// Index 0: restart escalated to Failed with the Restart
		// operation preserved; its pod is gone (zero serving).
		{
			Index: 0, Incarnation: 2,
			Phase: types.InstancePhaseFailed, RunningRevision: "prior-rev",
			Operation: &types.InstanceOperation{
				Type: types.InstanceOperationRestart, Step: "Drain",
			},
		},
		// Index 1: Ready and serving on the old revision.
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev",
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1},
	}
	mutatedIndices := map[int32]int{}
	in.MutateInstance = func(_ context.Context, idx int32, fn func(*types.InstanceStatus) bool) error {
		mutatedIndices[idx]++
		for i := range in.ObservedState.InstanceStatuses {
			if in.ObservedState.InstanceStatuses[i].Index == idx {
				_ = fn(&in.ObservedState.InstanceStatuses[i])
				break
			}
		}
		return nil
	}
	// Mirror coordination.CheckUnavailability's serving-based
	// accounting for a MaxUnavailable=1 group: desired=2, serving=1
	// (index 0 is dead), so ANY consulted start projects
	// 1 + delta + 1 = 2 > 1 and is denied — the double count under
	// test.
	var gateDeltas []int32
	in.UpdateGate = func(_ types.UpdateStrategyType, _, inFlightUnavail int32) (bool, types.RolloutHoldGate, string) {
		gateDeltas = append(gateDeltas, inFlightUnavail)
		const desired, serving, budget = 2, 1, 1
		if desired-serving+inFlightUnavail+1 > budget {
			return false, types.RolloutHoldGateBudget, "unavailable budget exhausted"
		}
		return true, "", "within unavailable budget"
	}

	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  2,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 2, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: types.UpdateStrategy{
			Type: types.UpdateStrategyRecreatePod,
			RollingUpdate: &types.RollingUpdate{
				// Per-Component layer stays live for the fresh start:
				// index 0 is not in the prior in-flight anchor
				// (Failed+Restart is not budget-charged), so
				// projected 0+1 = 1 fits and the recreate proceeds.
				MaxUnavailable: intOrStringInt(1),
			},
		},
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	_, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Logf("Reconcile op error (expected against empty fake client): %v", err)
	}

	// Load-bearing: the dead Instance's recreate was admitted — the
	// always-over-budget gate must not have been consulted for it.
	if mutatedIndices[0] == 0 {
		t.Errorf("Failed+Restart recreate must bypass the coordination gate consult; index 0 never started (mutations=%v, gate deltas=%v)", mutatedIndices, gateDeltas)
	}
	// Any consult that did happen (index 1) must see delta 0: the
	// exempt start takes no serving pod offline, so charging it would
	// re-introduce the double count against healthy peers.
	for _, d := range gateDeltas {
		if d != 0 {
			t.Errorf("gate consult saw in-flight unavail delta %d, want 0 (exempt start must not be charged); deltas=%v", d, gateDeltas)
		}
	}
}

// TestReconcile_FailedRestart_RecreateAnswersThePlanGate pins the other
// half of the exemption: a zero-serving Instance's recreate skips the
// capacity consult, whose count already carries its outage, but not the
// plan. With the plan seam holding (no run pinned for the group) neither
// the dead Instance's recreate nor the serving peer's start opens, the
// capacity gate is never asked, the pass records the Plan hold and
// requeues at the gate cadence.
func TestReconcile_FailedRestart_RecreateAnswersThePlanGate(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := types.Deps{Client: c}

	in := minimalInput(t)
	in.DesiredSpec.Replicas = 2
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{
			Index: 0, Incarnation: 2,
			Phase: types.InstancePhaseFailed, RunningRevision: "prior-rev",
			Operation: &types.InstanceOperation{
				Type: types.InstanceOperationRestart, Step: "Drain",
			},
		},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev",
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1},
	}
	mutatedIndices := map[int32]int{}
	in.MutateInstance = func(_ context.Context, idx int32, fn func(*types.InstanceStatus) bool) error {
		mutatedIndices[idx]++
		for i := range in.ObservedState.InstanceStatuses {
			if in.ObservedState.InstanceStatuses[i].Index == idx {
				_ = fn(&in.ObservedState.InstanceStatuses[i])
				break
			}
		}
		return nil
	}
	gateConsults, planConsults := 0, 0
	in.UpdateGate = func(types.UpdateStrategyType, int32, int32) (bool, types.RolloutHoldGate, string) {
		gateConsults++
		return true, "", ""
	}
	in.PlanGate = func() (bool, types.RolloutHoldGate, string) {
		planConsults++
		return false, types.RolloutHoldGate(v1beta1.RolloutHoldGatePlan), "rollout plan not pinned: no active run for this rollout group"
	}
	var hold *types.RolloutHold
	in.RecordRolloutHold = func(h *types.RolloutHold) { hold = h }

	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  2,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 2, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: types.UpdateStrategy{
			Type:          types.UpdateStrategyRecreatePod,
			RollingUpdate: &types.RollingUpdate{MaxUnavailable: intOrStringInt(1)},
		},
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	res, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(mutatedIndices) != 0 {
		t.Errorf("rows written while the plan was unpinned: %v; the zero-serving Instance's recreate answers the plan gate like every fresh start", mutatedIndices)
	}
	if planConsults == 0 {
		t.Errorf("the plan seam was never asked")
	}
	if gateConsults != 0 {
		t.Errorf("capacity gate consults = %d, want 0: a held plan is decided before any capacity question", gateConsults)
	}
	if hold == nil || hold.Gate != types.RolloutHoldGate(v1beta1.RolloutHoldGatePlan) {
		t.Errorf("recorded hold = %+v, want the plan gate's hold", hold)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("result = %+v, want a requeue at the gate cadence", res)
	}
}

// TestReconcile_PerComponentBudget_PercentForm exercises the percent
// resolver through the dispatcher: 25% on 4 replicas → budget=1, so
// only one fresh start is allowed. This is the load-bearing
// integration assertion for percent expressions matching the
// budget_test.go ceil-rounding case.
func TestReconcile_PerComponentBudget_PercentForm(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := types.Deps{Client: c}

	in := minimalInput(t)
	in.DesiredSpec.Replicas = 4
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 3, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
	}
	mutatedIndices := map[int32]int{}
	in.MutateInstance = func(_ context.Context, idx int32, fn func(*types.InstanceStatus) bool) error {
		mutatedIndices[idx]++
		for i := range in.ObservedState.InstanceStatuses {
			if in.ObservedState.InstanceStatuses[i].Index == idx {
				_ = fn(&in.ObservedState.InstanceStatuses[i])
				break
			}
		}
		return nil
	}

	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  4,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 3, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: types.UpdateStrategy{
			Type: types.UpdateStrategySurgeThenDrain,
			RollingUpdate: &types.RollingUpdate{
				MaxSurge: intOrStringStr("25%"), // 25% of 4 = ceil(1.0) = 1
			},
		},
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	_, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Logf("Reconcile op error (expected against empty fake client): %v", err)
	}

	started := 0
	for _, count := range mutatedIndices {
		if count > 0 {
			started++
		}
	}
	if started != 1 {
		t.Errorf("MaxSurge=25%% on 4 replicas yields budget=1; expected exactly 1 fresh start, got %d (mutations=%v)", started, mutatedIndices)
	}
}

// TestReconcile_ConcurrentGangSurge_DistinctIndices pins the
// distinct-surge-index contract at the reconcile level. When several
// multi-pod (gang) Instances start a SurgeThenDrain rollout in the
// SAME wake-up, each gangSurgeUpdate must allocate a DISTINCT surge
// index. Two colliding gangs sharing one surge index collapse
// capacity: when the single shared surge becomes Ready, the drain
// logic releases BOTH sources at once.
//
// Setup: one multi-pod Component (leader:1 + worker:1 = a gang per
// Instance), Replicas=3, MaxSurge=2, all three Ready on the prior
// revision, then a spec bump (target != RunningRevision). The
// per-Component MaxSurge=2 lets two gangs start this pass; each stamps
// its source Instance's Operation.SurgeIndex via MutateInstance. We
// capture every stamped (index, SurgeIndex) and assert:
//   - no two gangs stamped the SAME SurgeIndex, and
//   - no stamped SurgeIndex collides with a steady index 0/1/2.
//
// Regression direction: if both gangs picked the lowest-free index
// (3), the captured set would contain a duplicate and this test would
// fail. AllocateSurgeIndex excludes in-flight Operation.SurgeIndex and
// gangSurgeUpdate reserves the claim on the source snapshot, so the
// two gangs pick distinct indices (3 and 4).
func TestReconcile_ConcurrentGangSurge_DistinctIndices(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := types.Deps{Client: c}

	const replicas = 3
	const steadyIndices = replicas // steady indices are 0..replicas-1

	in := minimalInput(t)
	in.DesiredSpec.Replicas = replicas
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
	}

	// Capture every SurgeIndex the pass stamps onto a source Instance,
	// keyed by the source index. The gang-surge source stamp
	// (patchInstanceStatusGangSurging) sets Operation.SurgeIndex; the
	// GangSurgeTarget stamp on the surge index itself does not, so only
	// source stamps land here. Apply the mutation against the observed
	// snapshot so gangSurgeUpdate's in-memory reservation and the next
	// sibling's AllocateSurgeIndex see the just-claimed slot.
	stampedSurge := map[int32]int32{}
	in.MutateInstance = func(_ context.Context, idx int32, fn func(*types.InstanceStatus) bool) error {
		s := findObserved(&in, idx)
		if s == nil {
			// Surge/target index absent from the snapshot — apply against a
			// scratch status and append so later reads observe it.
			scratch := types.InstanceStatus{Index: idx}
			if fn(&scratch) {
				in.ObservedState.InstanceStatuses = append(in.ObservedState.InstanceStatuses, scratch)
				s = findObserved(&in, idx)
			}
		} else {
			fn(s)
		}
		if s != nil && s.Operation != nil && s.Operation.SurgeIndex != nil {
			stampedSurge[idx] = *s.Operation.SurgeIndex
		}
		return nil
	}

	gang := func(idx int32) types.InstancePlan {
		return types.InstancePlan{
			Index: idx, Incarnation: 1,
			Runners: []types.RunnerPlan{
				{Name: "leader", Size: 1},
				{Name: "worker", Size: 1},
			},
		}
	}
	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  replicas,
		Instances: []types.InstancePlan{gang(0), gang(1), gang(2)},
		UpdateStrategy: types.UpdateStrategy{
			Type: types.UpdateStrategySurgeThenDrain,
			RollingUpdate: &types.RollingUpdate{
				MaxSurge: intOrStringInt(2),
			},
		},
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	_, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Logf("Reconcile op error (expected against empty fake client): %v", err)
	}

	// At least two gangs must have surged this pass (MaxSurge=2) for the
	// distinctness check to be load-bearing. Fewer means the budget/gate
	// short-circuited before the collision could ever occur — the test
	// would silently pass without exercising the contract.
	if len(stampedSurge) < 2 {
		t.Fatalf("expected >=2 concurrent gang surges under MaxSurge=2; got %d stamped (surge=%v)", len(stampedSurge), stampedSurge)
	}

	seen := map[int32]int32{} // surgeIndex -> sourceIndex that claimed it
	for srcIdx, surgeIdx := range stampedSurge {
		// No stamped surge index may equal a steady index 0..replicas-1 —
		// a surge must land on a fresh slot, never atop a serving gang.
		if surgeIdx < steadyIndices {
			t.Errorf("source %d stamped surge index %d which collides with steady index range [0,%d)", srcIdx, surgeIdx, steadyIndices)
		}
		// No two sources may share a surge index — a shared surge going
		// Ready would release every sharing source at once.
		if prev, dup := seen[surgeIdx]; dup {
			t.Errorf("surge index %d claimed by BOTH source %d and source %d (shared-surge collision)", surgeIdx, prev, srcIdx)
		}
		seen[surgeIdx] = srcIdx
	}
}

// findObserved returns the observed InstanceStatus pointer for idx, or
// nil. Local to the concurrent-surge test's mutate hook so it can write
// back through the snapshot the dispatcher reads.
func findObserved(in *types.ReconcileInput, idx int32) *types.InstanceStatus {
	for i := range in.ObservedState.InstanceStatuses {
		if in.ObservedState.InstanceStatuses[i].Index == idx {
			return &in.ObservedState.InstanceStatuses[i]
		}
	}
	return nil
}

// terminatingSurgePod is one pod of Instance idx at ordinal, Terminating
// until deadline when deadline is non-zero and live otherwise.
func terminatingSurgePod(idx, ordinal int32, deadline time.Time) *corev1.Pod {
	pod := enginePod("llama-70b", "prod", idx)
	pod.Name = fmt.Sprintf("%s-%d", pod.Name, ordinal)
	pod.Labels[query.LabelPodOrdinal] = fmt.Sprintf("%d", ordinal)
	if !deadline.IsZero() {
		stamp := metav1.NewTime(deadline)
		pod.DeletionTimestamp = &stamp
	}
	return pod
}

// liveSurgePod is one live pod of Instance idx at ordinal carrying the
// revision hash rev: the shape a replacement a disposed attempt left
// behind has.
func liveSurgePod(idx, ordinal int32, rev string) *corev1.Pod {
	pod := terminatingSurgePod(idx, ordinal, time.Time{})
	pod.Labels[query.LabelRevisionHash] = rev
	return pod
}

// gangPods returns n pods of Instance idx, Terminating until deadline when
// deadline is non-zero.
func gangPods(idx, n int32, deadline time.Time) []*corev1.Pod {
	pods := make([]*corev1.Pod, 0, n)
	for i := int32(0); i < n; i++ {
		pods = append(pods, terminatingSurgePod(idx, i, deadline))
	}
	return pods
}

// TestTerminatingSurgeInFlight pins what a Component's extra pods hold
// against its surge budget: a pod the API still lists beyond its
// Instance's pod count holds the slot its surge took — a Terminating pod
// inside its own deletion grace, and a live pod a disposed attempt left
// behind for as long as it exists. The deadline is the pod's
// DeletionTimestamp, which the API stamps at the request time plus the
// pod's grace, so a pod past it stops counting however long its kubelet
// takes to reap it. A live extra pod is attributed to its Instance, whose
// next start replaces it.
func TestTerminatingSurgeInFlight(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	within := now.Add(30 * time.Second)
	elapsed := now.Add(-time.Second)
	singlePod := []types.RunnerPlan{{Name: "default", Size: 1}}
	gang := []types.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 7}}
	planOf := func(runners []types.RunnerPlan, indices ...int32) types.ComponentPlan {
		plan := types.ComponentPlan{Component: types.ComponentEngine, Replicas: int32(len(indices))}
		for _, idx := range indices {
			plan.Instances = append(plan.Instances, types.InstancePlan{Index: idx, Incarnation: 1, Runners: runners})
		}
		return plan
	}
	ready := func(idx int32) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"}
	}
	surgeStep := func(idx int32, step string, surgeIndex *int32) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Incarnation: 1, Phase: types.InstancePhaseUpdating, RunningRevision: "prior-rev",
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: step, SurgeIndex: surgeIndex}}
	}
	// A disposed row names its revision in full, as the adapter records
	// it, so its pods' revision hashes read against it.
	failed := func(idx int32) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: "llama-70b-engine-priorrev"}
	}
	cases := []struct {
		name       string
		plan       types.ComponentPlan
		statuses   []types.InstanceStatus
		pods       map[int32][]*corev1.Pod
		rolled     []int32
		wantSlots  int32
		wantPods   []string
		wantLive   []string
		wantByInst map[int32]int32
		wantWakeAt time.Time
	}{
		{
			name:     "an abandoned replacement draining beside its serving source holds one slot",
			plan:     planOf(singlePod, 0, 1),
			statuses: []types.InstanceStatus{ready(0), {Index: 1, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: "prior-rev"}},
			pods: map[int32][]*corev1.Pod{
				0: {terminatingSurgePod(0, 0, time.Time{})},
				1: {terminatingSurgePod(1, 0, time.Time{}), terminatingSurgePod(1, 1, within)},
			},
			wantSlots:  1,
			wantPods:   []string{"llama-70b-engine-1-default-0-1"},
			wantWakeAt: within,
		},
		{
			name:     "a replacement past its deletion deadline stops counting",
			plan:     planOf(singlePod, 0, 1),
			statuses: []types.InstanceStatus{ready(0), ready(1)},
			pods: map[int32][]*corev1.Pod{
				0: {terminatingSurgePod(0, 0, time.Time{})},
				1: {terminatingSurgePod(1, 0, time.Time{}), terminatingSurgePod(1, 1, elapsed)},
			},
		},
		{
			name:     "a replacement a disposed attempt left alive beside its source holds one slot, attributed to its Instance",
			plan:     planOf(singlePod, 0, 1),
			statuses: []types.InstanceStatus{ready(0), failed(1)},
			pods: map[int32][]*corev1.Pod{
				0: {terminatingSurgePod(0, 0, time.Time{})},
				1: {terminatingSurgePod(1, 0, time.Time{}), liveSurgePod(1, 1, "crash-rev")},
			},
			wantSlots:  1,
			wantLive:   []string{"llama-70b-engine-1-default-0-1"},
			wantByInst: map[int32]int32{1: 1},
		},
		{
			name:     "a live extra pod beside a Terminating one counts with it, and only the Terminating one sets the wake",
			plan:     planOf(singlePod, 0, 1),
			statuses: []types.InstanceStatus{ready(0), failed(1)},
			pods: map[int32][]*corev1.Pod{
				0: {terminatingSurgePod(0, 0, time.Time{})},
				1: {terminatingSurgePod(1, 0, time.Time{}), terminatingSurgePod(1, 1, within), liveSurgePod(1, 2, "crash-rev")},
			},
			wantSlots:  2,
			wantPods:   []string{"llama-70b-engine-1-default-0-1"},
			wantLive:   []string{"llama-70b-engine-1-default-0-2"},
			wantByInst: map[int32]int32{1: 1},
			wantWakeAt: within,
		},
		{
			name:     "the live pods of an index the plan does not own are the scale-down's, not the budget's",
			plan:     planOf(singlePod, 0, 1),
			statuses: []types.InstanceStatus{ready(0), ready(1)},
			pods: map[int32][]*corev1.Pod{
				0: {terminatingSurgePod(0, 0, time.Time{})},
				1: {terminatingSurgePod(1, 0, time.Time{})},
				5: {terminatingSurgePod(5, 0, time.Time{})},
			},
		},
		{
			name:     "a gang's live extra pods count in whole Instances",
			plan:     planOf(gang, 0, 1),
			statuses: []types.InstanceStatus{ready(0), failed(1)},
			pods: map[int32][]*corev1.Pod{
				0: gangPods(0, 8, time.Time{}),
				1: append(gangPods(1, 8, time.Time{}), liveSurgePod(1, 8, "crash-rev"), liveSurgePod(1, 9, "crash-rev")),
			},
			wantSlots:  1,
			wantLive:   []string{"llama-70b-engine-1-default-0-8", "llama-70b-engine-1-default-0-9"},
			wantByInst: map[int32]int32{1: 1},
		},
		{
			name:     "a source draining under its own surge step is charged by the step, not again here",
			plan:     planOf(singlePod, 0, 1),
			statuses: []types.InstanceStatus{ready(0), surgeStep(1, types.UpdateStepSurgeDrain, nil)},
			pods: map[int32][]*corev1.Pod{
				0: {terminatingSurgePod(0, 0, time.Time{})},
				1: {terminatingSurgePod(1, 0, within), terminatingSurgePod(1, 1, time.Time{})},
			},
		},
		{
			name:     "a gang source's pinned replacement index is charged by the source's step",
			plan:     planOf(gang, 0, 1),
			statuses: []types.InstanceStatus{surgeStep(0, types.UpdateStepSurge, func() *int32 { i := int32(2); return &i }()), ready(1)},
			pods: map[int32][]*corev1.Pod{
				0: gangPods(0, 8, time.Time{}),
				1: gangPods(1, 8, time.Time{}),
				2: gangPods(2, 8, within),
			},
		},
		{
			name:     "an Instance already charged as rolled but not serving is not charged again",
			plan:     planOf(singlePod, 0, 1),
			statuses: []types.InstanceStatus{ready(0), {Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "target-rev"}},
			pods: map[int32][]*corev1.Pod{
				0: {terminatingSurgePod(0, 0, time.Time{})},
				1: {terminatingSurgePod(1, 0, time.Time{}), terminatingSurgePod(1, 1, within)},
			},
			rolled: []int32{1},
		},
		{
			name:     "a pod set torn down in place holds nothing",
			plan:     planOf(singlePod, 0, 1),
			statuses: []types.InstanceStatus{ready(0), {Index: 1, Incarnation: 1, Phase: types.InstancePhaseRestarting, RunningRevision: "prior-rev"}},
			pods: map[int32][]*corev1.Pod{
				0: {terminatingSurgePod(0, 0, time.Time{})},
				1: {terminatingSurgePod(1, 0, within)},
			},
		},
		{
			name:     "every pod of an index the plan does not own is extra while it drains",
			plan:     planOf(singlePod, 0, 1),
			statuses: []types.InstanceStatus{ready(0), ready(1)},
			pods: map[int32][]*corev1.Pod{
				0: {terminatingSurgePod(0, 0, time.Time{})},
				1: {terminatingSurgePod(1, 0, time.Time{})},
				5: {terminatingSurgePod(5, 0, within)},
			},
			wantSlots:  1,
			wantPods:   []string{"llama-70b-engine-5-default-0-0"},
			wantWakeAt: within,
		},
		{
			name:     "a gang's extra pods count in whole Instances, and the first deadline is the wake",
			plan:     planOf(gang, 0, 1),
			statuses: []types.InstanceStatus{ready(0), ready(1)},
			pods: map[int32][]*corev1.Pod{
				0: append(gangPods(0, 8, time.Time{}), gangPods(0, 8, within.Add(time.Minute))[7]),
				1: gangPods(1, 8, time.Time{}),
				3: gangPods(3, 3, within),
			},
			wantSlots: 2,
			wantPods: []string{"llama-70b-engine-0-default-0-7",
				"llama-70b-engine-3-default-0-0", "llama-70b-engine-3-default-0-1", "llama-70b-engine-3-default-0-2"},
			wantWakeAt: within,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := escalation.ExtraPodSurgeInFlight(tc.plan, tc.statuses, tc.pods, tc.rolled, now)
			if got.Slots != tc.wantSlots {
				t.Errorf("slots = %d, want %d", got.Slots, tc.wantSlots)
			}
			if !slices.Equal(got.Terminating, tc.wantPods) {
				t.Errorf("terminating pods = %v, want %v", got.Terminating, tc.wantPods)
			}
			if !slices.Equal(got.Live, tc.wantLive) {
				t.Errorf("live extra pods = %v, want %v", got.Live, tc.wantLive)
			}
			wantByInst := tc.wantByInst
			if wantByInst == nil {
				wantByInst = map[int32]int32{}
			}
			if !maps.Equal(got.LiveSlotsByInstance, wantByInst) {
				t.Errorf("live slots by Instance = %v, want %v", got.LiveSlotsByInstance, wantByInst)
			}
			if !got.NextRelease.Equal(tc.wantWakeAt) {
				t.Errorf("next release = %v, want %v", got.NextRelease, tc.wantWakeAt)
			}
		})
	}
}

// An attempt parked after its disposition holds no unavailability slot: the
// next attempt on its own Instance is a fresh start the budget must be
// free to admit, in whichever phase the parked row's pods give it.
func TestCurrentUnavailableInFlight_ParkedAttemptHoldsNoSlot(t *testing.T) {
	parked := func(phase types.InstancePhase, waiting string) types.InstanceStatus {
		return types.InstanceStatus{Index: 0, Phase: phase, Operation: &types.InstanceOperation{
			Type: types.InstanceOperationUpdate, Step: types.UpdateStepParked, TargetRevision: "rev-b", Waiting: waiting,
		}}
	}
	inFlight := types.InstanceStatus{Index: 0, Phase: types.InstancePhaseUpdating, Operation: &types.InstanceOperation{
		Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, TargetRevision: "rev-b",
	}}
	for _, tc := range []struct {
		name string
		row  types.InstanceStatus
		want int32
	}{
		{name: "parked on the ladder while its set serves", row: parked(types.InstancePhaseUpdating, string(types.RolloutHoldGateRetryBlock)), want: 0},
		{name: "parked on the ladder while its set is down", row: parked(types.InstancePhaseFailed, string(types.RolloutHoldGateRetryBlock)), want: 0},
		{name: "parked under a held ladder", row: parked(types.InstancePhaseFailed, string(types.RolloutHoldGateHeld)), want: 0},
		{name: "parked behind the budget", row: parked(types.InstancePhaseUpdating, string(types.RolloutHoldGateBudget)), want: 0},
		{name: "an attempt in flight still holds its slot", row: inFlight, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := escalation.CurrentUnavailableInFlight([]types.InstanceStatus{tc.row}); got != tc.want {
				t.Fatalf("CurrentUnavailableInFlight = %d, want %d", got, tc.want)
			}
		})
	}
}
