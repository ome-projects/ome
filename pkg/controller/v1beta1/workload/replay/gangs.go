package replay

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	workloadgang "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/gang"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	types "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// podGroupFinalizer holds a deleted PodGroup as a Terminating object, the
// way the pod finalizer holds a deleted pod: the gang's Terminating state
// is a real reading the engine acts on, and a group that vanished on the
// delete call could never produce it.
const podGroupFinalizer = "replay.workload.ome.io/podgroup-lifecycle"

// reconcileGangs is the PodGroup pass that runs ahead of the dispatcher:
// it observes the owner's PodGroups once, announces the group of every
// multi-pod Instance in the plan before any member is created, records
// what each group says into input.Gangs, and wires the inline surge
// prerequisite the gang-surge op reaches back through. A single-pod
// Component has no gangs and the whole pass is inert.
func (d *driver) reconcileGangs(ctx context.Context, deps *types.Deps, input types.ReconcileInput, plan types.ComponentPlan) error {
	if !d.spec.GangScheduling {
		return nil
	}
	inventory, err := workloadgang.ObservePodGroups(ctx, d.cli, d.owner)
	if err != nil {
		return fmt.Errorf("replay: observe podgroups: %w", err)
	}
	state := workloadgang.PodGroupReconcileState{Inventory: inventory}
	deps.EnsureGangPodGroup = workloadgang.EnsureSurgePodGroupWithState(*deps, state)
	if _, err := workloadgang.EnsurePodGroupsWithState(ctx, *deps, input, plan, state); err != nil {
		return fmt.Errorf("replay: ensure podgroups: %w", err)
	}
	return nil
}

// podGroupName resolves the deterministic PodGroup name of one Instance,
// exactly as the engine composes it.
func (d *driver) podGroupName(index int32) string {
	return query.PodGroupName(d.opts.OwnerName, d.opts.Component, index)
}

// applyPodGroup is the gang.podGroup family: what an Instance's PodGroup
// says about its gang, written onto the object so the classification the
// engine reads is the one it derives itself.
func applyPodGroup(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Args.Instance == nil {
		return "", fmt.Errorf("replay: gang.podGroup needs the Instance index its group belongs to")
	}
	name := d.podGroupName(*ev.Args.Instance)
	key := client.ObjectKey{Namespace: d.opts.Namespace, Name: name}
	group := &schedulingv1alpha1.PodGroup{}
	if err := d.cli.Get(ctx, key, group); err != nil {
		return "", fmt.Errorf("replay: get podgroup %s: %w", name, err)
	}
	detail := "podGroup=" + name
	switch ev.Variant {
	case "Terminating":
		// A finalizer is what leaves the object behind with a deletion
		// stamp on it, which is the state the engine waits out.
		if err := d.staging(func() error {
			group.Finalizers = append(group.Finalizers, podGroupFinalizer)
			if err := d.cli.Update(ctx, group); err != nil {
				return err
			}
			return d.cli.Delete(ctx, group)
		}); err != nil {
			return "", fmt.Errorf("replay: terminate podgroup %s: %w", name, err)
		}
	case "Deleted":
		if err := d.staging(func() error { return d.cli.Delete(ctx, group) }); err != nil {
			return "", fmt.Errorf("replay: delete podgroup %s: %w", name, err)
		}
	case "OwnershipConflict":
		if err := d.staging(func() error {
			group.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "StatefulSet",
				Name:       "other-owner",
				UID:        "other-owner-uid",
				Controller: boolPtr(true),
			}}
			return d.cli.Update(ctx, group)
		}); err != nil {
			return "", fmt.Errorf("replay: reassign podgroup %s: %w", name, err)
		}
	case "PhaseFailed":
		if err := d.staging(func() error {
			group.Status.Phase = schedulingv1alpha1.PodGroupFailed
			group.Status.Failed = group.Spec.MinMember
			return d.cli.Status().Update(ctx, group)
		}); err != nil {
			return "", fmt.Errorf("replay: fail podgroup %s: %w", name, err)
		}
	default:
		return "", fmt.Errorf("replay: gang.podGroup[%s]: the driver writes only the group states the engine classifies "+
			"(Terminating, Deleted, OwnershipConflict, PhaseFailed); a placement verdict reaches the Instance on its member pods", ev.Variant)
	}
	return detail + " state=" + ev.Variant, nil
}
