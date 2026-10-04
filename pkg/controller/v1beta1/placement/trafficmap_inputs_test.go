package placement

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestTrafficMapInputsExcludePublicationOutputs(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(*v1beta1.TrafficMap)
		changed bool
	}{
		{name: "resource version", edit: func(tm *v1beta1.TrafficMap) { tm.ResourceVersion = "2" }},
		{name: "publication acknowledgement", edit: func(tm *v1beta1.TrafficMap) {
			tm.Status.Published = true
			tm.Status.ObservedTrafficMapGeneration = tm.Generation
			tm.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{PublisherName: "example", ClaimedTargets: []string{"target"}}
			tm.Status.GatewayRef = &v1beta1.TrafficMapGatewayRef{Kind: "HTTPRoute", Name: "route"}
			tm.Status.Conditions = []metav1.Condition{{Type: v1beta1.TrafficMapPublished, Status: metav1.ConditionTrue}}
		}},
		{name: "spec drift", changed: true, edit: func(tm *v1beta1.TrafficMap) { tm.Spec.Service = "changed" }},
		{name: "source provenance", changed: true, edit: func(tm *v1beta1.TrafficMap) { tm.Status.SourceUID = "source" }},
		{name: "orphan", changed: true, edit: func(tm *v1beta1.TrafficMap) { tm.OwnerReferences = nil }},
		{name: "cleanup finalizer", changed: true, edit: func(tm *v1beta1.TrafficMap) { tm.Finalizers = []string{TrafficMapPublisherFinalizer} }},
		{name: "deletion", changed: true, edit: func(tm *v1beta1.TrafficMap) { now := metav1.Now(); tm.DeletionTimestamp = &now }},
		{name: "annotation", changed: true, edit: func(tm *v1beta1.TrafficMap) { tm.Annotations = map[string]string{"test": "value"} }},
		{name: "label", changed: true, edit: func(tm *v1beta1.TrafficMap) { tm.Labels = map[string]string{"test": "value"} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			old := &v1beta1.TrafficMap{ObjectMeta: metav1.ObjectMeta{Generation: 1, OwnerReferences: []metav1.OwnerReference{{UID: "owner"}}}}
			current := old.DeepCopy()
			tt.edit(current)
			require.Equal(t, tt.changed, TrafficMapInputsChanged(old, current))
		})
	}
}
