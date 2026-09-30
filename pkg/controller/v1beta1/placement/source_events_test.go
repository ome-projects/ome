package placement

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestPlacementSourceEventTriggers(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*v1beta1.InferenceService)
		want bool
	}{
		{name: "unchanged", edit: func(*v1beta1.InferenceService) {}},
		{name: "status output", edit: func(s *v1beta1.InferenceService) {
			s.Status.Placement = &v1beta1.PlacementStatus{Phase: v1beta1.PlacementPhaseAdmitting}
		}},
		{name: "resource version only", edit: func(s *v1beta1.InferenceService) { s.ResourceVersion = "2" }},
		{name: "finalizer added", edit: func(s *v1beta1.InferenceService) { s.Finalizers = append(s.Finalizers, "example.com/cleanup") }},
		{name: "finalizer removed", edit: func(s *v1beta1.InferenceService) { s.Finalizers = nil }, want: true},
		{name: "source replaced", edit: func(s *v1beta1.InferenceService) { s.UID = "replacement" }, want: true},
		{name: "spec updated", edit: func(s *v1beta1.InferenceService) { s.Generation++ }, want: true},
		{name: "label updated", edit: func(s *v1beta1.InferenceService) { s.Labels = map[string]string{"team": "team-b"} }, want: true},
		{name: "annotation updated", edit: func(s *v1beta1.InferenceService) { s.Annotations = map[string]string{"example.com/config": "changed"} }, want: true},
		{name: "deletion requested", edit: func(s *v1beta1.InferenceService) { now := metav1.Now(); s.DeletionTimestamp = &now }, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{UID: "source", Generation: 1, Finalizers: []string{PlacementFinalizer}}}
			after := before.DeepCopy()
			tt.edit(after)
			if diff := cmp.Diff(tt.want, placementRelevantSourceChange.Update(event.UpdateEvent{ObjectOld: before, ObjectNew: after})); diff != "" {
				t.Errorf("update trigger (-want +got):\n%s", diff)
			}
		})
	}
	for _, tt := range []struct {
		name string
		got  bool
	}{
		{name: "create", got: placementRelevantSourceChange.Create(event.CreateEvent{Object: &v1beta1.InferenceService{}})},
		{name: "delete", got: placementRelevantSourceChange.Delete(event.DeleteEvent{Object: &v1beta1.InferenceService{}})},
		{name: "generic", got: placementRelevantSourceChange.Generic(event.GenericEvent{Object: &v1beta1.InferenceService{}})},
		{name: "missing objects", got: placementRelevantSourceChange.Update(event.UpdateEvent{})},
		{name: "unexpected kind", got: placementRelevantSourceChange.Update(event.UpdateEvent{ObjectOld: &corev1.Pod{}, ObjectNew: &corev1.Pod{}})},
		{name: "typed nil", got: placementRelevantSourceChange.Update(event.UpdateEvent{ObjectOld: (*v1beta1.InferenceService)(nil), ObjectNew: &v1beta1.InferenceService{}})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(true, tt.got); diff != "" {
				t.Errorf("event trigger (-want +got):\n%s", diff)
			}
		})
	}
	t.Run("placement finalizer addition is an output", func(t *testing.T) {
		before := &v1beta1.InferenceService{}
		after := before.DeepCopy()
		after.Finalizers = []string{PlacementFinalizer}
		if diff := cmp.Diff(false, placementRelevantSourceChange.Update(event.UpdateEvent{ObjectOld: before, ObjectNew: after})); diff != "" {
			t.Errorf("finalizer addition trigger (-want +got):\n%s", diff)
		}
	})
}
