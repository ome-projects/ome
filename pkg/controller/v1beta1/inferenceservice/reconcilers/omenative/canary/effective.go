package canary

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rollout"
)

// EffectivePartition returns the StatefulSet-style RollingUpdate.Partition the
// controller should apply to a component while a canary is in progress, reading
// the current step from status. Instances with index < Partition are held on the
// stable revision; (desired - Partition) roll to the canary revision.
//
// It is a pure read used by the controller to stamp the partition onto the merged
// component lifecycle before the component reconcilers run — the same
// spec→IR→plan→HeldByPartition path an operator-set partition takes. Returns
// (nil, false) when no canary plan is set for the InferenceService.
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
	if cs := rollout.CanaryStatusFor(&isvc.Status, component); cs != nil && cs.RolledBackRevisionHash != "" {
		zero := int32(0)
		return &zero, true
	}
	idx := int32(0)
	if cs := rollout.CanaryStatusFor(&isvc.Status, component); cs != nil {
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

// StampStepPartition stamps the current canary step's RollingUpdate.Partition
// onto a merged Component's lifecycle (creating the lifecycle / updateStrategy /
// rollingUpdate chain as needed), so the standard spec→IR→plan→HeldByPartition
// path holds the staged split. No-op when no canary is active for the ISVC.
// desiredReplicas is read from the Component's MinReplicas (fallback MaxReplicas).
func StampStepPartition(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, ext *v1beta1.ComponentExtensionSpec) {
	if ext == nil {
		return
	}
	desired := int32(ext.MaxReplicas)
	if ext.MinReplicas != nil {
		desired = int32(*ext.MinReplicas)
	}
	p, ok := EffectivePartition(isvc, component, desired)
	if !ok {
		return
	}
	if ext.Lifecycle == nil {
		ext.Lifecycle = &v1beta1.LifecycleSpec{}
	}
	if ext.Lifecycle.UpdateStrategy == nil {
		ext.Lifecycle.UpdateStrategy = &v1beta1.UpdateStrategy{}
	}
	if ext.Lifecycle.UpdateStrategy.RollingUpdate == nil {
		ext.Lifecycle.UpdateStrategy.RollingUpdate = &v1beta1.RollingUpdate{}
	}
	ext.Lifecycle.UpdateStrategy.RollingUpdate.Partition = p
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

// StampPlanGateHold stamps a full-hold partition (every instance held on its
// current revision) onto a Component that belongs to a canary-KIND spec group
// — an inline canary or a policyRef declaring canary — while NO effective
// canary plan is resolvable (run not yet open, or parked on an unresolvable
// ref). It is the projection-side twin of the update gates' plan hold: the
// stamped spec stays DETERMINISTIC across the pre-open and parked states, so
// a transiently lost pin cannot flap the projected IR between a step
// partition and no partition (which churns the IR generation and starves
// every fresh-snapshot gate downstream). No-op when a canary plan IS
// effective (StampStepPartition owns that) or the Component is not in a
// canary-kind group.
func StampPlanGateHold(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, ext *v1beta1.ComponentExtensionSpec) {
	if ext == nil || isvc.Spec.Rollout == nil || rollout.CanaryGroupFor(isvc, component) != nil {
		return
	}
	member := false
	for i := range isvc.Spec.Rollout.Groups {
		g := &isvc.Spec.Rollout.Groups[i]
		kind := g.Canary != nil ||
			(g.BlueGreen == nil && g.RollingUpdate == nil && g.PolicyRef != nil && g.PolicyRef.Progression == v1beta1.RolloutProgressionCanary)
		if kind && groupHasComponent(g, component) {
			member = true
			break
		}
	}
	if !member {
		return
	}
	desired := int32(ext.MaxReplicas)
	if ext.MinReplicas != nil {
		desired = int32(*ext.MinReplicas)
	}
	if ext.Lifecycle == nil {
		ext.Lifecycle = &v1beta1.LifecycleSpec{}
	}
	if ext.Lifecycle.UpdateStrategy == nil {
		ext.Lifecycle.UpdateStrategy = &v1beta1.UpdateStrategy{}
	}
	if ext.Lifecycle.UpdateStrategy.RollingUpdate == nil {
		ext.Lifecycle.UpdateStrategy.RollingUpdate = &v1beta1.RollingUpdate{}
	}
	ext.Lifecycle.UpdateStrategy.RollingUpdate.Partition = &desired
}
