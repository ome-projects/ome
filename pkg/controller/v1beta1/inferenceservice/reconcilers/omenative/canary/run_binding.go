package canary

import (
	"context"
	"sort"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

const canaryTargetIDPrefix = "ct1:"

// BindRun initializes the canary step state for a freshly opened run, or binds
// legacy in-flight state to an adopted run without restarting it. The controller
// calls this before its immediate run-boundary status write so the pinned plan
// and the corresponding step state become visible atomically.
//
// A rollback request applies to the target it was recorded against. Re-arming
// a rolled-back unit therefore removes the request with the rejected hash,
// durably and before the write, like the executor's own re-arm: the re-arm
// does not depend on it, and a copy left behind would roll the new canary
// back. A nil client removes it in-memory only.
func BindRun(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup, adopting bool) error {
	if isvc == nil || isvc.Status.Rollout == nil || isvc.Status.Rollout.ActiveRun == nil {
		return nil
	}
	targetID := activeCanaryTargetID(isvc, g)
	if targetID == "" {
		return nil
	}
	primary := primaryComponentOf(g)
	if adopting {
		if cs := rollout.CanaryStatusFor(&isvc.Status, primary); cs != nil && cs.TargetID == "" {
			cs.TargetID = targetID
		}
		return nil
	}

	target, ok := activeRunTarget(isvc, primary)
	if !ok || target.Revision == "" {
		return nil
	}
	// No stable revision means nothing to shift traffic from: the unit's
	// first rollout is not a canary. The step machine arms later only if a
	// stable revision becomes known.
	if target.StableRevision == "" {
		return nil
	}
	// A run is opened for the whole InferenceService, so a group whose unit
	// did not retarget is still handed a target. State is what arms the step
	// machine; an idle unit must sit the run out with none, or it walks its
	// ladder, spends its analysis budget, and can reach a rollback over a
	// rollout that never happened.
	if !groupRetargeted(isvc, g, primary) {
		return nil
	}
	cs := rollout.CanaryStatusFor(&isvc.Status, primary)
	if cs == nil {
		cs = &v1beta1.CanaryStatus{}
	}
	if cs.RolledBackRevisionHash != "" {
		if err := consumeAnnotation(ctx, c, isvc, constants.RolloutRollbackAnnotation); err != nil {
			return err
		}
	}
	rollout.SetCanaryStatusFor(&isvc.Status, primary, cs)
	resetCanaryStatus(isvc, primary, cs, targetID, target.Revision, isvc.Status.Rollout.ActiveRun.OpenedAt.Time)
	if target.StableRevision != "" {
		cs.StableRevisionHash = target.StableRevision
	}
	return nil
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
