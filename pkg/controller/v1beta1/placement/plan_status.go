package placement

import "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"

// mergeCandidateObservations keeps allocation authority when refreshing member
// observations. An omitted planned home remains present with unknown observation.
func mergeCandidateObservations(stored, observed []v1beta1.CandidatePlacement) []v1beta1.CandidatePlacement {
	planned := map[string]v1beta1.CandidatePlacement{}
	for _, candidate := range stored {
		if candidate.Allocation != nil {
			planned[candidate.Cluster] = candidate
		}
	}
	if len(planned) == 0 {
		return observed
	}
	out := make([]v1beta1.CandidatePlacement, 0, len(observed)+len(planned))
	for _, observation := range observed {
		candidate := *observation.DeepCopy()
		if previous, ok := planned[candidate.Cluster]; ok {
			candidate.Allocation = previous.Allocation.DeepCopy()
			// Unidentified observations cannot acknowledge a stored allocation.
			if observation.Allocation == nil || observation.Allocation.ClusterUID != previous.Allocation.ClusterUID {
				candidate.ObservationKnown = false
				candidate.AppliedPlanID = ""
			}
			delete(planned, candidate.Cluster)
		}
		out = append(out, candidate)
	}
	for _, previous := range stored {
		if _, missing := planned[previous.Cluster]; missing {
			candidate := *previous.DeepCopy()
			candidate.ObservationKnown = false
			out = append(out, candidate)
		}
	}
	return out
}
