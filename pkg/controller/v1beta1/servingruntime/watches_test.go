package servingruntime

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// Each case mutates a deep copy of one base runtime, so the transition under
// test is the only thing that differs between old and new.
func TestInheritanceTriggerPredicate_Update(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1beta1.ClusterServingRuntime)
		want   bool
	}{
		{
			// inherit-from is an annotation and both runtime CRDs have a
			// status subresource, so repointing a parent leaves generation
			// untouched — the edit that most needs re-resolution is exactly
			// the one a generation-only filter drops.
			name: "repointing the parent passes",
			mutate: func(rt *v1beta1.ClusterServingRuntime) {
				rt.Annotations[constants.RuntimeInheritFromAnnotationKey] = "profile-b"
			},
			want: true,
		},
		{
			name:   "a spec change passes",
			mutate: func(rt *v1beta1.ClusterServingRuntime) { rt.Generation++ },
			want:   true,
		},
		{
			// Annotations are compared whole rather than inherit-from alone,
			// so an unrelated edit costs one re-resolve that finds no status
			// diff and writes nothing.
			name: "an unrelated annotation passes",
			mutate: func(rt *v1beta1.ClusterServingRuntime) {
				rt.Annotations["ome.io/unrelated"] = "x"
			},
			want: true,
		},
		{
			name: "a status-only write is dropped",
			mutate: func(rt *v1beta1.ClusterServingRuntime) {
				rt.Status = v1beta1.ServingRuntimeStatus{
					InheritanceChain: []string{"profile", "rt"},
					Conditions: []metav1.Condition{{
						Type:   constants.InheritanceReadyConditionType,
						Status: metav1.ConditionTrue,
						Reason: ReasonResolved,
					}},
				}
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			old := mkCSR("rt", "profile", v1beta1.ServingRuntimeSpec{})
			updated := old.DeepCopy()
			tc.mutate(updated)

			got := inheritanceTriggerPredicate().Update(event.UpdateEvent{ObjectOld: old, ObjectNew: updated})

			if got != tc.want {
				t.Errorf("Update() = %v, want %v", got, tc.want)
			}
		})
	}
}

// Only status-only updates are dropped; every other event kind changes the
// graph or the object's lifecycle and must reach the reconciler.
func TestInheritanceTriggerPredicate_NonUpdateEventsPass(t *testing.T) {
	tests := []struct {
		name string
		pass func(predicate.Predicate, client.Object) bool
	}{
		{
			name: "create",
			pass: func(p predicate.Predicate, o client.Object) bool {
				return p.Create(event.CreateEvent{Object: o})
			},
		},
		{
			name: "delete",
			pass: func(p predicate.Predicate, o client.Object) bool {
				return p.Delete(event.DeleteEvent{Object: o})
			},
		},
		{
			name: "generic",
			pass: func(p predicate.Predicate, o client.Object) bool {
				return p.Generic(event.GenericEvent{Object: o})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rt := mkCSR("rt", "profile", v1beta1.ServingRuntimeSpec{})

			if !tc.pass(inheritanceTriggerPredicate(), rt) {
				t.Errorf("%s event was dropped, want it to reach the reconciler", tc.name)
			}
		})
	}
}
