package canary

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/rollout"
)

// releaseOrder is the order the Components are read in, so a pass releases
// them the same way every time.
var releaseOrder = v1beta1.RolesByPrimaryRank()

// ReleaseUndeclaredUnits drops the status a canary projected onto a
// Component that no group of the effective plan names any more: the unit's
// record and the rollout phase are the executor's state, and a Component
// without an executor keeps none of it, so it reads like a Component no
// rollout ever governed and follows its own spec. The traffic split is left
// to the coordination pass, which writes it from the pods for every
// Component, grouped or not. A Component a canary governs (an effective
// group, a declared canary-kind group whose plan does not resolve, or a
// run's recorded plan) and a Component a coordination group owns keep their
// status. The legacy alias follows the records left. It reports the
// Components it released.
func ReleaseUndeclaredUnits(isvc *v1beta1.InferenceService, policies rollout.Policies) []v1beta1.ComponentType {
	if isvc == nil || len(isvc.Status.Components) == 0 {
		return nil
	}
	owned := coordination.ResolveGroups(rollout.Effective(isvc, policies), coordination.GroupDefaults{})
	var released []v1beta1.ComponentType
	for _, c := range releaseOrder {
		cs, ok := isvc.Status.Components[c]
		if !ok || (cs.Canary == nil && cs.RolloutPhase == "") {
			continue
		}
		if Governs(isvc, policies, c) {
			continue
		}
		if _, member := coordination.MembershipFor(owned, c); member {
			continue
		}
		cs.Canary, cs.RolloutPhase = nil, ""
		isvc.Status.Components[c] = cs
		released = append(released, c)
	}
	if len(released) == 0 {
		return nil
	}
	rollout.SyncLegacyCanaryAlias(&isvc.Status)
	if !anyUnitRecord(isvc) {
		isvc.Status.Canary = nil
	}
	return released
}

// anyUnitRecord reports whether any Component still carries a canary record.
func anyUnitRecord(isvc *v1beta1.InferenceService) bool {
	for _, cs := range isvc.Status.Components {
		if cs.Canary != nil {
			return true
		}
	}
	return false
}
