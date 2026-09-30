package placement

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestAdmissionEventResolvesOwnedSource(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*metav1.PartialObjectMetadata)
		want bool
	}{
		{name: "owned component", want: true},
		{name: "ordinary local component", edit: func(m *metav1.PartialObjectMetadata) { delete(m.Labels, PlacementOriginLabel) }},
		{name: "other control plane", edit: func(m *metav1.PartialObjectMetadata) { m.Labels[PlacementControlPlaneLabel] = "other-control" }},
		{name: "unowned component", edit: func(m *metav1.PartialObjectMetadata) { m.OwnerReferences = nil }},
		{name: "wrong owner kind", edit: func(m *metav1.PartialObjectMetadata) { m.OwnerReferences[0].Kind = "Pod" }},
		{name: "missing owner identity", edit: func(m *metav1.PartialObjectMetadata) { m.OwnerReferences[0].UID = "" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			owner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service-a", UID: "derived-a"}}
			meta := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: "service-a-engine", Namespace: "team-a", Labels: map[string]string{PlacementOriginLabel: "source-a", PlacementControlPlaneLabel: "control-a"}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(owner, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))}}}
			if tt.edit != nil {
				tt.edit(meta)
			}
			cfg := AdmissionFunnelConfigFor("control-a")
			got, ok := cfg.Resolve(meta)
			if diff := cmp.Diff(tt.want, ok); diff != "" {
				t.Error(diff)
			}
			if ok {
				if diff := cmp.Diff(types.NamespacedName{Namespace: "team-a", Name: "service-a"}, got); diff != "" {
					t.Error(diff)
				}
			}
			if tt.want && !cfg.WatchSelector.Matches(labels.Set(meta.Labels)) {
				t.Error("owned component filtered from watch")
			}
		})
	}
}

func TestStatusEventChannelsAccumulate(t *testing.T) {
	first, second := make(chan event.GenericEvent), make(chan event.GenericEvent)
	got := resolveConvergeConfig(WithStatusEvents(first), WithStatusEvents(nil), WithStatusEvents(second))
	if diff := cmp.Diff([]<-chan event.GenericEvent{first, second}, got.statusEvents); diff != "" {
		t.Errorf("event sources (-want +got):\n%s", diff)
	}
}
