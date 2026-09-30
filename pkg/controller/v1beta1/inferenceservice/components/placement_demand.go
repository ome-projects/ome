package components

import (
	"context"
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/capacity"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// checkPlacementDemand compares the actual member rendering before workloads
// are projected. An absent contract preserves ordinary component reconciliation.
func checkPlacementDemand(ctx context.Context, base *BaseComponentFields, service *v1beta1.InferenceService, component v1beta1.ComponentType, leader, worker bool, templates ReplicaTemplates) error {
	policy, err := protocol.FromDerived(service)
	if err != nil {
		return err
	}
	if policy == nil || policy.Demand == nil {
		return nil
	}
	if base.APIReader == nil {
		return fmt.Errorf("placement demand verification requires a direct member reader")
	}
	sets, err := templates.PodSets(base.DeploymentMode, leader, worker)
	if err != nil {
		return err
	}
	actual, err := capacity.ComponentFingerprint(ctx, base.APIReader, component, base.DeploymentMode, sets)
	if err != nil {
		return err
	}
	for _, expected := range policy.Demand.Components {
		if expected.Component == component {
			if actual != expected.RenderingHash {
				return fmt.Errorf("placement demand for %s differs from the member rendering", component)
			}
			return nil
		}
	}
	return fmt.Errorf("placement demand has no %s rendering", component)
}
