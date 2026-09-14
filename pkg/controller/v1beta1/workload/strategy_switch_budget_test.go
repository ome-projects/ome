package workload_test

import (
	"context"
	"slices"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
)

// unavailableModeStep reports whether an Operation step belongs to a mode
// that takes its Instance's pod out of rotation (in-place patch, or the
// recreate drain). Surge steps do not: the replacement rotates in before
// the source rotates out.
func unavailableModeStep(step string) bool {
	switch step {
	case workload.UpdateStepSurge, workload.UpdateStepSurgeDrain, "SurgeDrainSettle",
		workload.UpdateStepGangSurgeTarget, workload.UpdateStepGangSurgeTargetCleanup:
		return false
	case "":
		return false
	default:
		return true
	}
}

// TestReconcile_StrategyFlipMidSurge_RespectsUnavailabilityBudget pins the
// budget contract across a mid-rollout strategy change.
//
// The two per-Component budgets are accounted from InstanceOperation.Step,
// but the dispatcher chooses WHICH budget a fresh start is charged to from
// the live strategy. An Instance admitted under maxSurge and still carrying
// a surge step therefore contributes nothing to the unavailability count —
// even on the pass where the flipped strategy is about to drive it as an
// unavailable update. The dispatcher reads that as unused headroom and
// admits a fresh start on top.
//
// The invariant asserted here is mode-agnostic and holds under any fix: at
// the end of one pass, no more Instances may be driven as unavailable-mode
// updates than maxUnavailable allows, and no more as surge-mode updates
// than maxSurge allows. It does not presume whether the in-flight Instance
// keeps surging or is handed to the new mode — only that whichever happens,
// it is counted.
//
// The Instances here carry no recorded running-revision baseline, so
// InPlaceIfPossible resolves to recreate exactly as it does in production
// against an unrecorded baseline. Both flipped arms therefore drive the
// recreate drain; both are unavailable modes, which is the property the
// budget is denominated in. The genuine image-patch path is covered by the
// per-Instance tests in workload/ops.
func TestReconcile_StrategyFlipMidSurge_RespectsUnavailabilityBudget(t *testing.T) {
	const (
		maxSurge       = 1
		maxUnavailable = 1
	)
	for _, tc := range []struct {
		name     string
		strategy workload.UpdateStrategyType
	}{
		{"SurgeThenDrain retained", workload.UpdateStrategySurgeThenDrain},
		{"flipped to InPlaceIfPossible", workload.UpdateStrategyInPlaceIfPossible},
		{"flipped to RecreatePod", workload.UpdateStrategyRecreatePod},
		// InPlaceOnly is deliberately absent. These Instances carry no recorded
		// running-revision baseline, so InPlaceOnly cannot prove the diff is
		// image-only and the mode chooser errors out before the pass reaches any
		// budget decision — the row would assert nothing. Its dispatch is covered
		// by the per-Instance tests in workload/ops, which do seed a baseline.
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := makeScheme(t)
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			deps := workload.Deps{Client: c}

			in := minimalInput(t)
			in.DesiredSpec.Replicas = 4
			in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
				// Index 0 was admitted under maxSurge on a prior wake-up and
				// still carries its surge step.
				{
					Index: 0, Incarnation: 1,
					Phase: workload.InstancePhaseUpdating, RunningRevision: "prior-rev",
					Operation: &workload.InstanceOperation{
						Type: workload.InstanceOperationUpdate, Step: workload.UpdateStepSurge,
						TargetRevision: "llama-70b-engine-newtarget",
					},
				},
				{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
				{Index: 2, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
				{Index: 3, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: "prior-rev"},
			}
			// Record the mode each Instance was actually driven as, at the
			// moment its Update operation is stamped. Reading Operation after
			// the pass would miss it: with no pods in the fake client the ops
			// escalate and clear the operation before the pass returns.
			drivenAs := map[int32]string{}
			in.MutateInstance = func(_ context.Context, idx int32, fn func(*workload.InstanceStatus) bool) error {
				for i := range in.ObservedState.InstanceStatuses {
					if in.ObservedState.InstanceStatuses[i].Index != idx {
						continue
					}
					s := &in.ObservedState.InstanceStatuses[i]
					_ = fn(s)
					if _, seen := drivenAs[idx]; !seen &&
						s.Operation != nil && s.Operation.Type == workload.InstanceOperationUpdate {
						drivenAs[idx] = s.Operation.Step
					}
					break
				}
				return nil
			}

			instances := make([]workload.InstancePlan, 4)
			for i := range instances {
				instances[i] = workload.InstancePlan{
					Index: int32(i), Incarnation: 1,
					Runners: []workload.RunnerPlan{{Name: "default", Size: 1}},
				}
			}
			plan := workload.ComponentPlan{
				Component: workload.ComponentEngine,
				Replicas:  4,
				Instances: instances,
				UpdateStrategy: workload.UpdateStrategy{
					Type: tc.strategy,
					RollingUpdate: &workload.RollingUpdate{
						MaxSurge:       intOrStringInt(maxSurge),
						MaxUnavailable: intOrStringInt(maxUnavailable),
					},
				},
			}
			target := &appsv1.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
			}

			if _, err := workload.Reconcile(context.Background(), deps, in, plan, target); err != nil {
				// Per-op state machines can error against an empty fake client
				// (no live pods to drain). The contract under test is the
				// budget accounting, not the per-op outcome.
				t.Logf("Reconcile op error (expected against empty fake client): %v", err)
			}

			var unavailable, surging []int32
			for idx, step := range drivenAs {
				if unavailableModeStep(step) {
					unavailable = append(unavailable, idx)
					continue
				}
				surging = append(surging, idx)
			}
			slices.Sort(unavailable)
			slices.Sort(surging)

			if len(unavailable) > maxUnavailable {
				t.Errorf("%d Instances driven as unavailable-mode updates (indices %v, steps %v) against maxUnavailable=%d",
					len(unavailable), unavailable, drivenAs, maxUnavailable)
			}
			if len(surging) > maxSurge {
				t.Errorf("%d Instances driven as surge-mode updates (indices %v, steps %v) against maxSurge=%d",
					len(surging), surging, drivenAs, maxSurge)
			}
		})
	}
}
