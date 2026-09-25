package irprojector

import (
	"testing"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// TestIRStatusToComponentStatus_FieldContract pins the 1:1 projection of
// every IR.Status summary field onto the ISVC-side LifecycleStatus. The
// ISVC subtree is the only status surface kubectl/dashboard consumers
// read for an OMENative Component, so a field silently dropped here
// disappears from the user-visible API even though the IR carries it.
// ObservedGeneration is the one exception to the 1:1 copy: it follows
// lifecycleObservedGeneration (the parent-generation stamp here).
func TestIRStatusToComponentStatus_FieldContract(t *testing.T) {
	g := gomega.NewWithT(t)
	cc := int32(3)
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Generation: 4,
			Annotations: map[string]string{
				constants.InferenceReplicaParentGenerationAnnotationKey: "9",
			},
		},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration:   4,
			Replicas:             5,
			ReadyReplicas:        4,
			ServingReplicas:      3,
			AvailableReplicas:    2,
			UpdatedReplicas:      1,
			UpdatedReadyReplicas: 1,
			CurrentRevision:      "engine-aaaa1111",
			UpdateRevision:       "engine-bbbb2222",
			CollisionCount:       &cc,
			LabelSelector:        "app=example,component=engine",
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionUnknown, Reason: "RolloutInProgress"},
				{Type: "RolloutStalled", Status: metav1.ConditionFalse, Reason: "Progressing"},
			},
		},
	}

	out := IRStatusToComponentStatus(ir, nil)

	g.Expect(out.ObservedGeneration).To(gomega.Equal(int64(9)),
		"ObservedGeneration must be the parent ISVC generation stamp, not the IR's own generation")
	g.Expect(out.Replicas).To(gomega.Equal(int32(5)))
	g.Expect(out.ReadyReplicas).To(gomega.Equal(int32(4)))
	g.Expect(out.ServingReplicas).To(gomega.Equal(int32(3)))
	g.Expect(out.AvailableReplicas).To(gomega.Equal(int32(2)))
	g.Expect(out.UpdatedReplicas).To(gomega.Equal(int32(1)))
	g.Expect(out.UpdatedReadyReplicas).To(gomega.Equal(int32(1)))
	g.Expect(out.CurrentRevision).To(gomega.Equal("engine-aaaa1111"))
	g.Expect(out.UpdateRevision).To(gomega.Equal("engine-bbbb2222"))
	g.Expect(out.LabelSelector).To(gomega.Equal("app=example,component=engine"))
	g.Expect(out.CollisionCount).NotTo(gomega.BeNil())
	g.Expect(*out.CollisionCount).To(gomega.Equal(int32(3)))
	g.Expect(out.Conditions).To(gomega.HaveLen(2))
	g.Expect(out.Conditions[0].Type).To(gomega.Equal("Ready"))
	g.Expect(out.Conditions[1].Type).To(gomega.Equal("RolloutStalled"))
}

// TestIRStatusToComponentStatus_ObservedGenerationRule pins
// lifecycleObservedGeneration case by case: the stamp when the IR is
// reconciled, the carried live value otherwise, never the IR generation.
func TestIRStatusToComponentStatus_ObservedGenerationRule(t *testing.T) {
	const irGeneration = int64(4)
	mkIR := func(stamp *string, observed int64) *v1beta1.InferenceReplica {
		ir := &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{Generation: irGeneration},
			Status:     v1beta1.InferenceReplicaStatus{ObservedGeneration: observed},
		}
		if stamp != nil {
			ir.Annotations = map[string]string{
				constants.InferenceReplicaParentGenerationAnnotationKey: *stamp,
			}
		}
		return ir
	}
	stamp := func(s string) *string { return &s }
	carried := &v1beta1.LifecycleStatus{ObservedGeneration: 2}

	tests := []struct {
		name string
		ir   *v1beta1.InferenceReplica
		live *v1beta1.LifecycleStatus
		want int64
	}{
		{name: "fresh IR adopts the stamp", ir: mkIR(stamp("9"), irGeneration), live: nil, want: 9},
		{name: "fresh IR adopts the stamp over a stale live value", ir: mkIR(stamp("9"), irGeneration), live: carried, want: 9},
		{name: "unfresh IR carries the live value", ir: mkIR(stamp("9"), 3), live: carried, want: 2},
		{name: "unfresh IR with no live block reports zero", ir: mkIR(stamp("9"), 3), live: nil, want: 0},
		{name: "never-reconciled IR carries the live value", ir: mkIR(stamp("9"), 0), live: carried, want: 2},
		{name: "missing stamp carries the live value", ir: mkIR(nil, irGeneration), live: carried, want: 2},
		{name: "missing stamp with no live block reports zero", ir: mkIR(nil, irGeneration), live: nil, want: 0},
		{name: "unparsable stamp carries the live value", ir: mkIR(stamp("not-a-number"), irGeneration), live: carried, want: 2},
		{name: "unparsable stamp with no live block reports zero", ir: mkIR(stamp(""), irGeneration), live: nil, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			out := IRStatusToComponentStatus(tt.ir, tt.live)
			g.Expect(out.ObservedGeneration).To(gomega.Equal(tt.want))
			g.Expect(out.ObservedGeneration).NotTo(gomega.Equal(irGeneration),
				"the IR's own generation must never surface as the Lifecycle ObservedGeneration")
		})
	}
}

// TestIRStatusToComponentStatus_ConditionsDoNotAlias pins the deep-copy
// contract on the projected Conditions slice: the ISVC-side copy must not
// share a backing array with the IR's status, or a later in-place
// condition update on the IR (SetStatusCondition mutates entries) would
// silently rewrite the already-projected ISVC subtree.
func TestIRStatusToComponentStatus_ConditionsDoNotAlias(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := &v1beta1.InferenceReplica{
		Status: v1beta1.InferenceReplicaStatus{
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllInstancesReady"},
			},
		},
	}

	out := IRStatusToComponentStatus(ir, nil)
	ir.Status.Conditions[0].Status = metav1.ConditionFalse
	ir.Status.Conditions[0].Reason = "InstanceFailed"

	g.Expect(out.Conditions[0].Status).To(gomega.Equal(metav1.ConditionTrue),
		"the projected condition must not alias the IR's backing array")
	g.Expect(out.Conditions[0].Reason).To(gomega.Equal("AllInstancesReady"))
}

// TestIRStatusToComponentStatus_EmptyConditionsProjectNil pins the
// nil-vs-empty contract: an IR with no conditions projects a nil slice
// (omitted in serialization), not an empty non-nil one, and the returned
// LifecycleStatus pointer itself is always non-nil — its presence marks
// the Component as IR-managed for downstream preservation logic.
func TestIRStatusToComponentStatus_EmptyConditionsProjectNil(t *testing.T) {
	g := gomega.NewWithT(t)
	out := IRStatusToComponentStatus(&v1beta1.InferenceReplica{}, nil)
	g.Expect(out).NotTo(gomega.BeNil())
	g.Expect(out.Conditions).To(gomega.BeNil())
	g.Expect(out.CollisionCount).To(gomega.BeNil())
}
