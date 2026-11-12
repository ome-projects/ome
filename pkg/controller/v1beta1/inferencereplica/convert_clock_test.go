package inferencereplica

import (
	"context"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestCurrentRevisionWithdrawnConditionTakesThePassClock: the write that
// empties the current revision while an Instance is on another revision
// raises the CurrentRevisionWithdrawn condition at the pass's clock, never
// at the wall clock, from the promotion at the end of a pass and from the
// record of a rollback target ahead of it.
func TestCurrentRevisionWithdrawnConditionTakesThePassClock(t *testing.T) {
	readyOnPrior := v1beta1.OMENativeInstanceStatus{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: promotePriorRev}

	t.Run("the promotion that empties the current revision", func(t *testing.T) {
		ir := baselineIR("llama-engine", "prod", 2)
		ir.Status.CurrentRevision, ir.Status.UpdateRevision = promotePriorRev, promotePriorRev
		ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: promotePriorRev, Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepInPlace, TargetRevision: promoteTargetRev}},
			readyOnPrior,
		}
		r, _ := clockedReconciler(t, ir)
		if err := buildPromoteCurrentRevision(r.statusWriter(), r.Client, r.Client, ir)(context.Background(), promotePriorRev); err != nil {
			t.Fatalf("promote the current revision: %v", err)
		}
		fresh := getFreshIR(t, r, ir)
		if fresh.Status.CurrentRevision != "" {
			t.Fatalf("current revision %q, want it empty while an Instance is held on another revision", fresh.Status.CurrentRevision)
		}
		expectTransitionAtThePassClock(t, fresh.Status.Conditions, InferenceReplicaConditionCurrentRevisionWithdrawn)
	})

	t.Run("the record of a rollback target that empties the current revision", func(t *testing.T) {
		ir := baselineIR("llama-engine", "prod", 2)
		ir.Status.CurrentRevision, ir.Status.UpdateRevision = promotePriorRev, promoteTargetRev
		ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: promotePriorRev, Operation: &v1beta1.InstanceOperation{
				ID: "surge-0", Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepSurge, TargetRevision: promoteTargetRev}},
			readyOnPrior,
		}
		r, _ := clockedReconciler(t, ir)
		if err := buildRecordUpdateRevision(r.statusWriter(), r.Client, r.Client, ir, workloadtypes.ComponentPlan{}, nil)(context.Background(), promotePriorRev); err != nil {
			t.Fatalf("record the rollback target: %v", err)
		}
		fresh := getFreshIR(t, r, ir)
		if fresh.Status.CurrentRevision != "" || fresh.Status.UpdateRevision != promotePriorRev {
			t.Fatalf("revision pair current %q update %q, want an empty current revision under the rollback target", fresh.Status.CurrentRevision, fresh.Status.UpdateRevision)
		}
		expectTransitionAtThePassClock(t, fresh.Status.Conditions, InferenceReplicaConditionCurrentRevisionWithdrawn)
	})
}

// TestAggregateConditionWithNoTransitionTimeTakesThePassClock: a condition
// handed to the Component condition writer with no transition time
// transitions at the pass's clock, never at the wall clock; a time the
// caller names is kept.
func TestAggregateConditionWithNoTransitionTimeTakesThePassClock(t *testing.T) {
	condition := func() metav1.Condition {
		return metav1.Condition{
			Type:    string(workloadtypes.ConditionGangSchedulingUnavailable),
			Status:  metav1.ConditionTrue,
			Reason:  string(workloadtypes.ReasonPodGroupCRDNotInstalled),
			Message: "the PodGroup CRD is not installed",
		}
	}

	t.Run("a condition with no transition time", func(t *testing.T) {
		ir := baselineIR("llama-engine", "prod", 1)
		r, _ := clockedReconciler(t, ir)
		if err := buildWriteAggregateCondition(r.statusWriter(), r.Client, ir)(context.Background(), condition()); err != nil {
			t.Fatalf("write the condition: %v", err)
		}
		expectTransitionAtThePassClock(t, getFreshIR(t, r, ir).Status.Conditions, string(workloadtypes.ConditionGangSchedulingUnavailable))
	})

	t.Run("a condition whose caller names a time", func(t *testing.T) {
		ir := baselineIR("llama-engine", "prod", 1)
		r, _ := clockedReconciler(t, ir)
		named := condition()
		named.LastTransitionTime = metav1.NewTime(publicationClock.Add(-time.Hour))
		if err := buildWriteAggregateCondition(r.statusWriter(), r.Client, ir)(context.Background(), named); err != nil {
			t.Fatalf("write the condition: %v", err)
		}
		conditions := getFreshIR(t, r, ir).Status.Conditions
		cond := apimeta.FindStatusCondition(conditions, named.Type)
		if cond == nil {
			t.Fatalf("no %s condition was written: %+v", named.Type, conditions)
		}
		if !cond.LastTransitionTime.Time.Equal(named.LastTransitionTime.Time) {
			t.Errorf("condition %s transitioned at %s, want the caller's time %s", cond.Type, cond.LastTransitionTime.Time, named.LastTransitionTime.Time)
		}
	})
}
