package v1beta1testing

import (
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Status setters for the wrappers whose CRD status carries a
// []metav1.Condition (BaseModel / ClusterBaseModel via ModelStatusSpec,
// ServingRuntime / ClusterServingRuntime via ServingRuntimeStatus).
//
// These build an object whose status is pre-populated so a test can
// seed a "ready" (or any) status via the status subresource without
// hand-assembling Status structs. They use apimeta.SetStatusCondition
// so LastTransitionTime is filled and a condition of the same type is
// upserted (not duplicated), matching how the controllers mutate
// status.
//
// Seeding status still requires a status-subresource write by the
// caller (Create only persists spec). Note Create overwrites the local
// object with the server's copy (status cleared), so re-apply the
// seeded status onto the created object before the status write:
//
//	desired := MakeBaseModel("m", ns).StorageURI(uri).
//	    StatusState(v1beta1.LifeCycleStateReady).Obj()
//	bm := desired.DeepCopy()
//	Expect(c.Create(ctx, bm)).To(Succeed())
//	bm.Status = desired.Status
//	Expect(c.Status().Update(ctx, bm)).To(Succeed())
//
// InferenceService is intentionally excluded — its status uses knative
// duckv1 conditions, not []metav1.Condition.

// readyCondition is a tiny constructor for the common
// status==True/False condition the shortcuts below seed. Reason is
// required by the apiserver for a condition, so callers must supply it.
func readyCondition(conditionType string, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{
		Type:    conditionType,
		Status:  status,
		Reason:  reason,
		Message: message,
	}
}

// --- BaseModel ---

// StatusState sets status.State (the model lifecycle state, e.g.
// v1beta1.LifeCycleStateReady).
func (w *BaseModelWrapper) StatusState(state v1beta1.LifeCycleState) *BaseModelWrapper {
	w.Status.State = state
	return w
}

// StatusCondition upserts a condition into status.Conditions.
func (w *BaseModelWrapper) StatusCondition(cond metav1.Condition) *BaseModelWrapper {
	apimeta.SetStatusCondition(&w.Status.Conditions, cond)
	return w
}

// StatusConditionTrue upserts a True condition of the given type/reason.
func (w *BaseModelWrapper) StatusConditionTrue(conditionType, reason string) *BaseModelWrapper {
	return w.StatusCondition(readyCondition(conditionType, metav1.ConditionTrue, reason, ""))
}

// --- ClusterBaseModel ---

// StatusState sets status.State.
func (w *ClusterBaseModelWrapper) StatusState(state v1beta1.LifeCycleState) *ClusterBaseModelWrapper {
	w.Status.State = state
	return w
}

// StatusCondition upserts a condition into status.Conditions.
func (w *ClusterBaseModelWrapper) StatusCondition(cond metav1.Condition) *ClusterBaseModelWrapper {
	apimeta.SetStatusCondition(&w.Status.Conditions, cond)
	return w
}

// StatusConditionTrue upserts a True condition of the given type/reason.
func (w *ClusterBaseModelWrapper) StatusConditionTrue(conditionType, reason string) *ClusterBaseModelWrapper {
	return w.StatusCondition(readyCondition(conditionType, metav1.ConditionTrue, reason, ""))
}

// --- ServingRuntime ---

// StatusCondition upserts a condition into status.Conditions.
func (w *ServingRuntimeWrapper) StatusCondition(cond metav1.Condition) *ServingRuntimeWrapper {
	apimeta.SetStatusCondition(&w.Status.Conditions, cond)
	return w
}

// StatusConditionTrue upserts a True condition of the given type/reason.
func (w *ServingRuntimeWrapper) StatusConditionTrue(conditionType, reason string) *ServingRuntimeWrapper {
	return w.StatusCondition(readyCondition(conditionType, metav1.ConditionTrue, reason, ""))
}

// --- ClusterServingRuntime ---

// StatusCondition upserts a condition into status.Conditions.
func (w *ClusterServingRuntimeWrapper) StatusCondition(cond metav1.Condition) *ClusterServingRuntimeWrapper {
	apimeta.SetStatusCondition(&w.Status.Conditions, cond)
	return w
}

// StatusConditionTrue upserts a True condition of the given type/reason.
func (w *ClusterServingRuntimeWrapper) StatusConditionTrue(conditionType, reason string) *ClusterServingRuntimeWrapper {
	return w.StatusCondition(readyCondition(conditionType, metav1.ConditionTrue, reason, ""))
}
