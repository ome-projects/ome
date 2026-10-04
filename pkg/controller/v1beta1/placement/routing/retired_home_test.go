package routing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestRoutingRetiredHomeWithStaleAdmission(t *testing.T) {
	for _, mode := range []v1beta1.PlacementMode{v1beta1.PlacementModeAll, v1beta1.PlacementModeSplit} {
		t.Run(string(mode), func(t *testing.T) {
			source := splitISVC([]string{"member-a", "member-b"}, []int32{1, 1}, []int32{1, 1})
			source.Spec.Placement.Mode = mode
			source.Status.Placement.Plan = &v1beta1.PlacementPlanStatus{Mode: mode}
			a := &source.Status.Placement.Candidates[0]
			a.Allocation = &v1beta1.CandidateAllocationStatus{CurrentReplicas: 0, DesiredReplicas: 0, DrainRequested: true}
			source.Status.Placement.Candidates[1].Allocation = &v1beta1.CandidateAllocationStatus{CurrentReplicas: 1, DesiredReplicas: 1}

			// Completion clears drain intent before the member observation is refreshed.
			a.Allocation.DrainRequested = false
			spec, _, _ := buildSpec(source, ResolvedProbePolicy{}, ResolvedCapacityPolicy{}, nil, nil)
			require.Len(t, spec.Entries, 1)
			require.Equal(t, "member-b", spec.Entries[0].Cluster)
			require.Positive(t, spec.Entries[0].Weight)

			// A retained home can autoscale above a zero floor and must stay addressable.
			a.Allocation.CurrentHome = &v1beta1.PlacementHomePolicy{ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent}}}
			spec, _, _ = buildSpec(source, ResolvedProbePolicy{}, ResolvedCapacityPolicy{}, nil, nil)
			require.Len(t, spec.Entries, 2)
			require.Positive(t, spec.Entries[0].Weight)
		})
	}
}
