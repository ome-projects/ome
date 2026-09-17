package canary

import (
	"sort"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

const canaryTargetIDPrefix = "ct1:"

// BindRun initializes the canary step state for a freshly opened run, or binds
// legacy in-flight state to an adopted run without restarting it. The controller
// calls this before its immediate run-boundary status write so the pinned plan
// and the corresponding step state become visible atomically.
func BindRun(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup, adopting bool) {
	if isvc == nil || isvc.Status.Rollout == nil || isvc.Status.Rollout.ActiveRun == nil {
		return
	}
	targetID := activeCanaryTargetID(isvc, g)
	if targetID == "" {
		return
	}
	primary := primaryComponentOf(g)
	if adopting {
		if cs := rollout.CanaryStatusFor(&isvc.Status, primary); cs != nil && cs.TargetID == "" {
			cs.TargetID = targetID
		}
		return
	}

	target, ok := activeRunTarget(isvc, primary)
	if !ok || target.Revision == "" {
		return
	}
	cs := rollout.CanaryStatusFor(&isvc.Status, primary)
	if cs == nil {
		cs = &v1beta1.CanaryStatus{}
	}
	rollout.SetCanaryStatusFor(&isvc.Status, primary, cs)
	resetCanaryStatus(cs, targetID, target.Revision, isvc.Status.Rollout.ActiveRun.OpenedAt.Time)
	if target.StableRevision != "" {
		cs.StableRevisionHash = target.StableRevision
	}
}

// activeCanaryTargetID fingerprints only the canary group's Component targets.
// The whole-run ID also changes when another group retargets; using it here
// would violate the per-group reset contract.
func activeCanaryTargetID(isvc *v1beta1.InferenceService, group *v1beta1.RolloutGroup) string {
	if isvc == nil || isvc.Status.Rollout == nil || isvc.Status.Rollout.ActiveRun == nil {
		return ""
	}
	if group == nil {
		return ""
	}
	parts := make([]string, 0, len(group.Components))
	seen := make(map[v1beta1.ComponentType]struct{}, len(group.Components))
	for _, comp := range group.Components {
		if _, duplicate := seen[comp]; duplicate {
			continue
		}
		seen[comp] = struct{}{}
		target, ok := activeRunTarget(isvc, comp)
		if !ok || target.Revision == "" {
			return ""
		}
		parts = append(parts, string(comp)+"="+target.Revision)
	}
	if len(parts) == 0 {
		return ""
	}
	sort.Strings(parts)
	return canaryTargetIDPrefix + rolloutpolicy.ShortHash([]byte(strings.Join(parts, ";")))
}

func activeRunTarget(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType) (v1beta1.RolloutRunTarget, bool) {
	if isvc == nil || isvc.Status.Rollout == nil || isvc.Status.Rollout.ActiveRun == nil {
		return v1beta1.RolloutRunTarget{}, false
	}
	for _, target := range isvc.Status.Rollout.ActiveRun.TargetRevisions {
		if target.Component == comp {
			return target, true
		}
	}
	return v1beta1.RolloutRunTarget{}, false
}
