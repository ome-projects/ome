package coordination

import (
	"sort"

	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// KeepLiveEntries copies the live object's group entries and their
// readiness condition onto dst, the status a pass writes before its
// coordination step has run. The entries such a pass carries are its cached
// copy's, and the cache trails the previous pass's write, never leads it, so
// writing them could only move a group's phase back. dst is the pass's
// working copy, so its coordination step then compares the phase it
// computes with the live entry. An entry or a condition the live object
// lacks is dropped from dst for the same reason.
func KeepLiveEntries(dst, live *v1beta1.InferenceServiceStatus) {
	if dst == nil || live == nil {
		return
	}
	dst.RolloutCoordination = live.RolloutCoordination.DeepCopy()
	ready := apis.ConditionType(v1beta1.RolloutCoordinationReady)
	kept := dst.Conditions[:0:0]
	for _, cond := range dst.Conditions {
		if cond.Type != ready {
			kept = append(kept, cond)
		}
	}
	// The condition is copied as it stands, its transition time included;
	// the condition manager would move the time of a value this pass did
	// not compute to now. The list stays sorted by type, as the manager
	// keeps it, so the copy differs from the live object only where the
	// pass computes something.
	if cond := live.GetCondition(ready); cond != nil {
		kept = append(kept, *cond)
		sort.Slice(kept, func(i, j int) bool { return kept[i].Type < kept[j].Type })
	}
	dst.Conditions = kept
}
