package protocol

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestAffinityMemberBoundary(t *testing.T) {
	for _, tt := range []struct {
		name                        string
		origin, selected, execution bool
		want                        bool
	}{
		{name: "local"},
		{name: "local with similarly named metadata", selected: true, execution: true},
		{name: "legacy member", origin: true},
		{name: "opt-in member before allocation", origin: true, selected: true, want: true},
		{name: "planned member", origin: true, execution: true, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}}}
			if tt.origin {
				service.Annotations[constants.PlacementOriginUID] = "source-uid"
			}
			if tt.selected {
				service.Annotations[constants.PlacementPolicy] = "ClusterAffinity"
			}
			if tt.execution {
				service.Annotations[constants.PlacementExecution] = "{}"
			}
			if diff := cmp.Diff(tt.want, IsAffinityMember(service)); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
