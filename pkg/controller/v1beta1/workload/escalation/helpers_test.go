package escalation

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/holds"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Test-only seam for the external test package: the pass is engine-
// internal, and its inputs come from the reconcile's memoized pod read,
// so tests drive it over pre-bucketed pods instead of a client.

// EscalateFromEvidenceForTest invokes the hold pass and the
// terminal-failure escalation pass in the order a reconcile runs them,
// over the given pod buckets. target is the reconcile's target
// ControllerRevision (nil is valid — the Create-op fallback stays empty).
func EscalateFromEvidenceForTest(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, byIdx map[int32][]*corev1.Pod) error {
	pods := func(context.Context) (map[int32][]*corev1.Pod, error) { return byIdx, nil }
	held, err := holds.Run(ctx, holds.PassInput{
		Deps:  deps,
		Input: input,
		Plan:  plan,
		Rows:  holds.RowsForPlan(plan, input.ObservedState.InstanceStatuses, nil),
		Pods:  pods,
	})
	if err != nil {
		return err
	}
	return Run(ctx, PassInput{Deps: deps, Input: input, Plan: plan, Target: target, Pods: pods, Held: held})
}
