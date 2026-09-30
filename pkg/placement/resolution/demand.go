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
	_, projected := root.Labels[v1beta1.AcceleratorQuotaOriginLabel]
	if projected || root.Spec.ParentRef != nil || root.Spec.Role != v1beta1.AcceleratorQuotaRoleCohort {
		return nil, fmt.Errorf("capacity report is not a local quota root")
	}
	var catalog kueuev1beta2.ResourceFlavorList
	if err := reads.List(ctx, &catalog); err != nil {
		return nil, err
	}
	flavors := make([]quotacapacity.Flavor, 0, len(catalog.Items))
	for _, flavor := range catalog.Items {
		flavors = append(flavors, quotacapacity.Flavor{Name: flavor.Name, UID: flavor.UID, NodeLabels: flavor.Spec.NodeLabels})
	}
	demand, err := capacity.AttributeUnit(unit.Demand, flavors, root.Status.Capacity)
	if err != nil {
		return nil, err
	}
	out := &ResolvedDemand{Unit: unit, Demand: demand, Report: *root.ObjectMeta.DeepCopy()}
	out.Hardware = root.DeepCopy().Status.Capacity
	out.Contract = &v1beta1.PlacementDemandContract{Fingerprint: demand.Fingerprint}
	for _, component := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		if hash, exists := unit.Rendering[component]; exists {
			out.Contract.Components = append(out.Contract.Components, v1beta1.PlacementComponentDemand{Component: component, RenderingHash: hash})
		}
	}
	if err := out.Check(ctx); err != nil {
		return nil, err
	}
	return out, nil
}
