package canary

import (
	"context"

	"k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
)

// RunOpen is the run layer's report of the run a pass opened: whether it
// adopted a record in flight, and the run the pass closed ahead of the open,
// whose plan a kept record's step indexes. Closed is nil when the pass opened
// over no run, or over a run closed on an earlier pass.
type RunOpen struct {
	Adopted bool
	Closed  *v1beta1.RolloutRun
}

// BindRun initializes the canary step state for a freshly opened run, or binds
// legacy in-flight state to an adopted run without restarting it. The controller
// calls this before its immediate run-boundary status write so the pinned plan
// and the corresponding step state become visible atomically.
//
// A rollback request applies to the target it was recorded against. Re-arming
// a rolled-back unit therefore removes the request with the rejected hash,
// durably and before the write, like the executor's own re-arm: the re-arm
// does not depend on it, and a copy left behind would roll the new canary
// back. A nil client removes it in-memory only, and leaves every member's
// rolled-out record as it is, since the revision history cannot be read.
func BindRun(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup, open RunOpen) error {
	if isvc == nil || isvc.Status.Rollout == nil || isvc.Status.Rollout.ActiveRun == nil {
		return nil
	}
	targetID := activeCanaryTargetID(isvc, g)
	if targetID == "" {
		return nil
	}
	primary := primaryComponentOf(g)
	if open.Adopted {
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
	// Another unit's push reopens the run with this unit recorded at the
	// target it already holds. Its record stands with its step, its split
	// and its holds: the reset is for a unit whose own target changed.
	if cs != nil && cs.TargetID == targetID && cs.CanaryRevisionHash == target.Revision {
		keepRecordUnderPlan(isvc, g, primary, cs, open.Closed)
		return nil
	}
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
	return recordStableRevisions(ctx, c, isvc, g)
}

// keepRecordUnderPlan reads a kept record against the step list the fresh
// run records. A list equal to the one the unit ran under leaves the record
// as it is. A changed list is met the way a plan verb's clamp meets it: the
// step index stays inside the new list, a finished unit stays finished, and
// the pre-step hold is set when the kept step would raise traffic, so the
// raise waits for a promote. With no closed run in hand the list the unit
// ran under is unknown and the record stands.
func keepRecordUnderPlan(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup, primary v1beta1.ComponentType, cs *v1beta1.CanaryStatus, closed *v1beta1.RolloutRun) {
	ranUnder, known := stepsRanUnder(closed, primary)
	if !known || g.Canary == nil || equality.Semantic.DeepEqual(ranUnder, g.Canary.Steps) {
		return
	}
	rollout.ClampStep(cs, isvc.Status.Components[primary].RolloutPhase, ranUnder, g.Canary.Steps)
}

// stepsRanUnder is the step list the closed run recorded for the unit with
// the given primary, and whether it recorded one.
func stepsRanUnder(closed *v1beta1.RolloutRun, primary v1beta1.ComponentType) ([]v1beta1.RolloutGroupStep, bool) {
	if closed == nil {
		return nil, false
	}
	for i := range closed.Plan.Groups {
		g := &closed.Plan.Groups[i].Group
		if g.Canary != nil && primaryComponentOf(g) == primary {
			return g.Canary.Steps, true
		}
	}
	return nil, false
}

// recordStableRevisions writes the run's stable revision as a member's
// LatestRolledoutRevision when the record names a revision outside the run's
// stable and target whose ControllerRevision the sweep has collected, and
// keeps the stale name as previous: such a revision runs on no Instance, and
// the executor writes the record next at the cutover or the revert. A
// revision the history still carries may be the one the Instances run, since
// the stable comes from the replica's current revision, which trails the pods
// for a pass or two after a cutover; that record stands, as does one naming
// the stable or the target, an empty one, and every record with no reader.
func recordStableRevisions(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup) error {
	if reads == nil {
		return nil
	}
	for _, comp := range configuredComponents(g) {
		target, ok := activeRunTarget(isvc, comp)
		if !ok || target.StableRevision == "" {
			continue
		}
		recorded := query.RevisionFromName(isvc.Status.Components[comp].LatestRolledoutRevision).Hash()
		if recorded == "" || recorded == target.StableRevision || recorded == target.Revision {
			continue
		}
		retained, err := revisionRetained(ctx, reads, isvc, comp, recorded)
		if err != nil {
			return err
		}
		if retained {
			continue
		}
		recordRolledOut(isvc, comp, target.StableRevision, "")
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
	return rollout.CanaryTargetID(group.Components, func(comp v1beta1.ComponentType) string {
		target, ok := activeRunTarget(isvc, comp)
		if !ok {
			return ""
		}
		return target.Revision
	})
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
