package canary

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rollout"
)

// EffectivePartition returns the StatefulSet-style partition the controller
// should apply to a component while a canary is in progress, reading the
// current step from status. Instances with index < Partition are held on the
// stable revision; (desired - Partition) roll to the canary revision.
//
// It is a pure read used by the controller to project the partition onto the
// InferenceReplica's rollout-control spec.pacing.partition before the
// component reconcilers run; the engine's partition hold reads it ahead of
// an operator-set lifecycle partition. Returns (nil, false) when no canary
// plan is set for the InferenceService.
//
//   - The current step's Capacity resolved against desired.
//
// Rollback (ome.io/rollout-rollback) is special-cased: when the status records
// a rolled-back revision, this returns Partition 0 (hold nothing). Partition
// alone cannot reverse a roll — the workload skips held instances rather than
// draining their canary pods — so the revert is driven by the IR's
// RollbackToRevision target (it overrides the desired pod template with the
// stable revision and rolls every instance back onto it). A non-zero partition
// here would fight that revert, so it is forced to 0 for the duration.
func EffectivePartition(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, desiredReplicas int32) (*int32, bool) {
	g := rollout.CanaryGroupFor(isvc, component)
	if g == nil || g.Canary == nil || len(g.Canary.Steps) == 0 {
		return nil, false
	}
	// The canary partition applies only to the canary group's own Component(s).
	if !groupHasComponent(g, component) {
		return nil, false
	}
	plan := g.Canary
	// Rolled back: hold NOTHING via partition (0). The component is driven back
	// onto the stable revision by the IR's RollbackToRevision target, not by the
	// partition; a non-zero partition here would fight that revert.
	// The group's run state is keyed by its primary; a member of the group's
	// other unit must read the same state to stay in step with it.
	cs := rollout.GroupCanaryStatusFor(isvc, component)
	if cs != nil && cs.RolledBackRevisionHash != "" {
		zero := int32(0)
		return &zero, true
	}
	idx := int32(0)
	if cs != nil {
		idx = cs.CurrentStep
	}
	// Status is an unvalidated subresource: clamp a negative step (an external
	// write) before it can index plan.Steps.
	if idx < 0 {
		idx = 0
	}
	// Done sentinel (CurrentStep past the last step): the canary finished — hold
	// NOTHING, the component runs entirely on the canary revision (partition 0).
	// Don't clamp to the final step and recompute: if that step's Capacity were
	// < 100% (admission allows it — only final Traffic==100 is enforced),
	// the recompute would strand (desired-newCount) instances on the stable
	// revision permanently after completion.
	if int(idx) >= len(plan.Steps) {
		zero := int32(0)
		return &zero, true
	}
	newCount := resolveStepNewCount(plan.Steps[idx], desiredReplicas)
	p := partitionForNewCount(desiredReplicas, newCount)
	return &p, true
}

// StepPartition returns the current canary step's partition for a merged
// Component, the value the projector writes to the InferenceReplica's
// rollout-control spec.pacing.partition so the engine's partition hold
// stages the split. The merged Component's lifecycle is left untouched: it
// is the user's update strategy. nil when no canary is active for the
// Component. desiredReplicas is read from the Component's MinReplicas
// (fallback MaxReplicas).
func StepPartition(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, ext *v1beta1.ComponentExtensionSpec) *int32 {
	if ext == nil {
		return nil
	}
	desired := int32(ext.MaxReplicas)
	if ext.MinReplicas != nil {
		desired = int32(*ext.MinReplicas)
	}
	p, ok := EffectivePartition(isvc, component, desired)
	if !ok {
		return nil
	}
	return p
}

// groupHasComponent reports whether c is a member of the group.
func groupHasComponent(g *v1beta1.RolloutGroup, c v1beta1.ComponentType) bool {
	for _, gc := range g.Components {
		if gc == c {
			return true
		}
	}
	return false
}

// PlanGateHoldPartition returns a full-hold partition (every instance held
// on its current revision) for a Component that belongs to a canary-KIND
// spec group — an inline canary or a policyRef declaring canary — while NO
// effective canary plan is resolvable (run not yet open, or parked on an
// unresolvable ref). It is the projection-side twin of the update gates'
// plan hold: the projected spec stays DETERMINISTIC across the pre-open and
// parked states, so a transiently lost pin cannot flap the projected IR
// between a step partition and no partition (which churns the IR generation
// and starves every fresh-snapshot gate downstream). nil when a canary plan
// IS effective (StepPartition owns that) or the Component is not in a
// canary-kind group.
func PlanGateHoldPartition(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, ext *v1beta1.ComponentExtensionSpec) *int32 {
	if ext == nil || isvc.Spec.Rollout == nil || rollout.CanaryGroupFor(isvc, component) != nil {
		return nil
	}
	member := false
	for i := range isvc.Spec.Rollout.Groups {
		g := &isvc.Spec.Rollout.Groups[i]
		if g.DeclaredProgression() == v1beta1.RolloutProgressionCanary && groupHasComponent(g, component) {
			member = true
			break
		}
	}
	if !member {
		return nil
	}
	desired := int32(ext.MaxReplicas)
	if ext.MinReplicas != nil {
		desired = int32(*ext.MinReplicas)
	}
	return &desired
}
