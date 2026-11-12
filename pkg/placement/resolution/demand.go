package resolution

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/capacity"
	quotacapacity "sigs.k8s.io/ome/pkg/quota/capacity"
)

// ResolvedDemand retains verified member rendering, hardware mapping, and root
// identity. Consumers match Report to fresh fleet provenance before planning,
// then verify that members apply the rendering before advancing a transition.
type ResolvedDemand struct {
	Unit     *ResolvedUnit
	Demand   capacity.Demand
	Report   metav1.ObjectMeta
	Hardware []v1beta1.AcceleratorCapacityStatus
	Contract *v1beta1.PlacementDemandContract
}

func (d *ResolvedDemand) Check(ctx context.Context) error {
	if d == nil {
		return fmt.Errorf("accelerator demand has no verified inputs")
	}
	return d.Unit.Check(ctx)
}

// ResolveDemand attributes a rendered unit using the identified member's live
// flavor catalog and local root report. The root name is explicit; quota amounts
// do not select hardware pools.
func (r Resolver) ResolveDemand(ctx context.Context, desired, standing *v1beta1.InferenceService, rootName string) (*ResolvedDemand, error) {
	if rootName == "" {
		return nil, fmt.Errorf("demand resolution requires the member capacity root name")
	}
	unit, err := r.ResolveUnit(ctx, desired, standing)
	if err != nil {
		return nil, err
	}
	reads := unit.Runtime.reads
	root := &v1beta1.AcceleratorQuota{}
	if err := reads.Get(ctx, client.ObjectKey{Name: rootName}, root); err != nil {
		return nil, err
	}
	if !isLocalRoot(root) {
		return nil, fmt.Errorf("capacity report is not a local quota root")
	}
	var catalog kueuev1beta2.ResourceFlavorList
	if err := reads.List(ctx, &catalog); err != nil {
		return nil, err
	}
	demand, err := capacity.AttributeUnit(unit.Demand, flavorCatalog(catalog.Items), root.Status.Capacity)
	if err != nil {
		return nil, err
	}
	out := &ResolvedDemand{Unit: unit, Demand: demand, Report: *root.ObjectMeta.DeepCopy()}
	out.Hardware = root.DeepCopy().Status.Capacity
	out.Contract = demandContract(unit, demand)
	if err := out.Check(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// isLocalRoot reports whether the quota is the member's own capacity root: a
// cohort without a parent that no other cluster projected.
func isLocalRoot(root *v1beta1.AcceleratorQuota) bool {
	_, projected := root.Labels[v1beta1.AcceleratorQuotaOriginLabel]
	return !projected && root.Spec.ParentRef == nil && root.Spec.Role == v1beta1.AcceleratorQuotaRoleCohort
}

// flavorCatalog is the member's ResourceFlavors as the attribution reads them.
func flavorCatalog(items []kueuev1beta2.ResourceFlavor) []quotacapacity.Flavor {
	flavors := make([]quotacapacity.Flavor, 0, len(items))
	for _, flavor := range items {
		flavors = append(flavors, quotacapacity.Flavor{Name: flavor.Name, UID: flavor.UID, NodeLabels: flavor.Spec.NodeLabels})
	}
	return flavors
}

// demandContract records the attributed demand's fingerprint and the rendering
// hash of each placed component in role order, the engine before the decoder.
func demandContract(unit *ResolvedUnit, demand capacity.Demand) *v1beta1.PlacementDemandContract {
	contract := &v1beta1.PlacementDemandContract{Fingerprint: demand.Fingerprint}
	for _, role := range v1beta1.Roles {
		if hash, exists := unit.Rendering[role.Type]; exists {
			contract.Components = append(contract.Components, v1beta1.PlacementComponentDemand{Component: role.Type, RenderingHash: hash})
		}
	}
	return contract
}
