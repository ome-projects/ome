package placement

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestPlacementPolicyConditionTransitions(t *testing.T) {
	for _, status := range []corev1.ConditionStatus{corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionUnknown} {
		t.Run(string(status), func(t *testing.T) {
			for _, tt := range []struct {
				name string
				edit func(*policyCondition)
			}{
				{name: "unchanged"},
				{name: "reason changed", edit: func(c *policyCondition) { c.cond.Reason = "InputsChanged" }},
				{name: "message changed", edit: func(c *policyCondition) { c.cond.Message = "Member inputs changed" }},
				{name: "status changed", edit: func(c *policyCondition) {
					c.cond.Status = corev1.ConditionFalse
					if status == corev1.ConditionFalse {
						c.cond.Status = corev1.ConditionTrue
					}
				}},
				{name: "condition cleared", edit: func(c *policyCondition) { c.clear = true }},
			} {
				t.Run(tt.name, func(t *testing.T) {
					stableAt := apis.VolatileTime{Inner: metav1.NewTime(time.Unix(10, 0))}
					st := &v1beta1.InferenceServiceStatus{}
					condition := backendCondition(status, "InputsVerified", "Member inputs verified")
					applyPolicyConditions(st, []policyCondition{condition})
					for i := range st.Conditions {
						st.Conditions[i].LastTransitionTime = stableAt
					}
					unrelated := apis.Condition{Type: "ExternalHealth", Status: corev1.ConditionTrue, LastTransitionTime: stableAt}
					st.Conditions = append(st.Conditions, unrelated)
					if tt.edit != nil {
						tt.edit(&condition)
					}
					applyPolicyConditions(st, []policyCondition{condition})
					got := st.GetCondition(condition.condType)
					var want *apis.Condition
					if !condition.clear {
						want = &apis.Condition{Type: condition.condType, Status: condition.cond.Status, Severity: apis.ConditionSeverityInfo, Reason: condition.cond.Reason, Message: condition.cond.Message, LastTransitionTime: stableAt}
						if tt.edit != nil && got != nil {
							if !got.LastTransitionTime.Inner.After(stableAt.Inner.Time) {
								t.Error("changed condition must advance its transition time")
							}
							want.LastTransitionTime = got.LastTransitionTime
						}
					}
					if diff := cmp.Diff(want, got); diff != "" {
						t.Errorf("condition (-want +got):\n%s", diff)
					}
					if diff := cmp.Diff(&unrelated, st.GetCondition(unrelated.Type)); diff != "" {
						t.Errorf("unrelated condition (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}
