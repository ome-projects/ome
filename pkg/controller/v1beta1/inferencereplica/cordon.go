package inferencereplica

import (
	"context"

	"github.com/go-logr/logr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// cordonedInstances returns the indices of the replica's Instances with a
// pod on a cordoned node, read through the cache, for the scale-down to
// remove first. The preference decides nothing unless the replica holds
// more Instances than it wants, so any other pass reads nothing. A read
// error returns nil and costs only the preference for this pass.
func (r *Reconciler) cordonedInstances(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica, input workloadtypes.ReconcileInput) map[int32]struct{} {
	if r.Client == nil || int32(len(input.ObservedState.InstanceStatuses)) <= input.DesiredSpec.Replicas {
		return nil
	}
	nodes, err := query.CordonedNodes(ctx, r.Client)
	if err != nil {
		log.V(1).Info("cordoned nodes unreadable; scale-down ranks without them this pass", "error", err.Error())
		return nil
	}
	if len(nodes) == 0 {
		return nil
	}
	pods, err := query.ListOMENativePodsByName(ctx, r.Client, ir.Namespace, ir.NamePrefix(),
		v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), true)
	if err != nil {
		log.V(1).Info("component pods unreadable; scale-down ranks without cordoned nodes this pass", "error", err.Error())
		return nil
	}
	return query.CordonedInstances(query.BucketPodsByInstanceIdx(podsControlledBy(pods, ir.UID)), nodes)
}
