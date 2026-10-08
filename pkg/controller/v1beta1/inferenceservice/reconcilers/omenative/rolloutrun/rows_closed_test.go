package rolloutrun

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// Tests for a rejected revision that stays rejected: no run is pinned and a
// canary unit's record holds a rejected revision a member still targets.
// The divergence toward that revision is a hold, so the passes below open
// nothing and park nothing whatever else moved around the hold.

// A standing hold is left alone by everything that is not a different
// target, a cleared hold or a straggler outside the rejection: a target
// back on the stable revision ends the hold with nothing to roll, a plan or
// policy edit, a reference that stops or starts resolving, a missing or
// converged replica, a coordination phase and the rollback verb all leave
// the service with no run and RolloutPlanReady True NoActiveRun.
func TestAStandingHoldOpensNothing(t *testing.T) {
	held := func() *v1beta1.InferenceReplica { return divergedIR(oldRev, newRev) }
	refHeld := func(t *testing.T) *v1beta1.InferenceService {
		isvc := isvcFixture(refCanary(policyName))
		setEngineUnit(isvc, v1beta1.RolloutPhaseRolledBack, rejectedRecord(t))
		return isvc
	}
	cases := []struct {
		name  string
		setup func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object)
		check func(t *testing.T, isvc *v1beta1.InferenceService)
	}{
		{
			name: "spec.revision[revert]",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				return rejectedISVC(t, v1beta1.RolloutPhaseRolledBack), []runtime.Object{settledIR(oldRev)}
			},
		},
		{
			name: "spec.planEdit",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := rejectedISVC(t, v1beta1.RolloutPhaseRolledBack)
				requireNoRunAfter(t, isvc, held())
				isvc.Spec.Rollout.Groups[0].Canary = canaryBody(50, 100)
				return isvc, []runtime.Object{held()}
			},
		},
		{
			name: "policy.changed",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := refHeld(t)
				requireNoRunAfter(t, isvc, held(), canaryPolicy(policyName))
				return isvc, []runtime.Object{held(), canaryPolicy(policyName, 50, 100)}
			},
		},
		{
			name: "policy.unresolved",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := refHeld(t)
				requireNoRunAfter(t, isvc, held(), canaryPolicy(policyName))
				return isvc, []runtime.Object{held()}
			},
		},
		{
			name: "policy.resolved",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := refHeld(t)
				requireNoRunAfter(t, isvc, held())
				return isvc, []runtime.Object{held(), canaryPolicy(policyName)}
			},
		},
		{
			name: "ir.missing",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				return rejectedISVC(t, v1beta1.RolloutPhaseRolledBack), nil
			},
		},
		{
			// The engine converged on its own revision while the router's hold stands.
			name: "ir.converged",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				engine := namedIR("llm-a-engine", engineStableRev, engineStableRev)
				engine.Status.Replicas, engine.Status.UpdatedReplicas = 2, 2
				return concurrentCanaryISVC(t), []runtime.Object{namedIR("llm-a-router", routerStableRev, routerRejectedRev), engine}
			},
		},
		{
			name: "coord.staged",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := rejectedISVC(t, v1beta1.RolloutPhaseRolledBack)
				setCoordinationPhase(isvc, v1beta1.CoordinationPhaseStaged)
				return isvc, []runtime.Object{held()}
			},
		},
		{
			name: "coord.settled",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := rejectedISVC(t, v1beta1.RolloutPhaseRolledBack)
				setCoordinationPhase(isvc, v1beta1.CoordinationPhaseIdle)
				return isvc, []runtime.Object{held()}
			},
		},
		{
			name: "verb.rollback",
			setup: func(t *testing.T) (*v1beta1.InferenceService, []runtime.Object) {
				isvc := rejectedISVC(t, v1beta1.RolloutPhaseRolledBack)
				annotate(isvc, constants.RolloutRollbackAnnotation, "true")
				return isvc, []runtime.Object{held()}
			},
			check: func(t *testing.T, isvc *v1beta1.InferenceService) {
				if isvc.Annotations[constants.RolloutRollbackAnnotation] != "true" {
					t.Fatal("the run layer consumed the rollback verb")
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc, objects := tc.setup(t)
			for i := 0; i < 2; i++ {
				out := pass(t, isvc, objects...)
				if out.Parked || out.Opened || out.StateChanged {
					t.Fatalf("pass %d: outcome = %+v, want the hold left alone", i, out)
				}
				requireNoRun(t, isvc, v1beta1.RolloutPlanReasonNoRun)
			}
			if tc.check != nil {
				tc.check(t, isvc)
			}
		})
	}
}
