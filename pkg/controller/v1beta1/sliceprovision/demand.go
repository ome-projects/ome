package sliceprovision

import (
	"context"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/tpuslice"
)

// ErrInvalidDemand reports pods that select a provision-only pool but cannot
// fill a provisionable slice exactly. Retrying does not help until the pods
// or the configuration change.
var ErrInvalidDemand = errors.New("pods cannot be placed on a provisioned slice")

// Demand is the slice a slot's pods need.
type Demand struct {
	Shape     tpuslice.Shape
	SliceType string
}

// Resolve returns the slice the pods of one Instance need; pods holds one
// spec per pod.
//
// needed is false, with a nil error, when the pods select no topology or no
// provision-only node carries their accelerator: a static pool's slices are
// never provisioned. Otherwise no pod may select a slice itself, the
// accelerator must be configured, the topology provisionable, and the pods'
// chips must fill it exactly; a failure of any of these wraps
// ErrInvalidDemand.
func Resolve(ctx context.Context, nodes client.Reader, cfg *controllerconfig.TPUSliceProvisioningConfig, pods []*corev1.PodSpec) (d Demand, needed bool, err error) {
	if cfg == nil {
		return Demand{}, false, nil
	}
	shape, found, err := commonShape(cfg.ShapeKeys(), pods)
	if err != nil || !found {
		return Demand{}, false, err
	}
	list := &corev1.NodeList{}
	if err := nodes.List(ctx, list, client.MatchingLabels{
		cfg.NodeLabels.Accelerator: shape.Accelerator,
		cfg.ProvisionOnly.Key:      cfg.ProvisionOnly.Value,
	}); err != nil {
		return Demand{}, false, fmt.Errorf("list provision-only nodes: %w", err)
	}
	if len(list.Items) == 0 {
		return Demand{}, false, nil
	}
	for i, spec := range pods {
		if name, ok := spec.NodeSelector[cfg.NodeLabels.Slice]; ok {
			return Demand{}, false, fmt.Errorf("%w: pod %d selects slice %q; a provision-only pool's slice is provisioned for the pods",
				ErrInvalidDemand, i, name)
		}
	}
	acc, ok := cfg.Accelerators[shape.Accelerator]
	if !ok {
		return Demand{}, false, fmt.Errorf("%w: accelerator %q has provision-only nodes but no provisioning configuration",
			ErrInvalidDemand, shape.Accelerator)
	}
	if !acc.Allows(shape.Topology) {
		return Demand{}, false, fmt.Errorf("%w: topology %s is not provisionable for accelerator %q; provisionable: %s",
			ErrInvalidDemand, shape.Topology, shape.Accelerator, strings.Join(acc.Topologies, ", "))
	}
	chips := make([]int64, len(pods))
	for i, spec := range pods {
		chips[i] = tpuslice.ContainerChips(spec.Containers, cfg.ChipResource)
	}
	if err := tpuslice.Tile(shape.Topology, acc.ChipsPerHost, chips); err != nil {
		return Demand{}, false, fmt.Errorf("%w: %w", ErrInvalidDemand, err)
	}
	return Demand{Shape: shape, SliceType: acc.SliceType}, true, nil
}

// commonShape is the shape the pods select. The pods of one Instance share a
// slice, so all of them select the same shape or none does.
func commonShape(keys tpuslice.Keys, pods []*corev1.PodSpec) (tpuslice.Shape, bool, error) {
	var shape tpuslice.Shape
	selecting := 0
	for i, spec := range pods {
		if spec == nil {
			return tpuslice.Shape{}, false, fmt.Errorf("pod %d has no spec", i)
		}
		s, found, err := tpuslice.FromNodeSelector(spec.NodeSelector, keys)
		if err != nil {
			return tpuslice.Shape{}, false, fmt.Errorf("%w: pod %d: %w", ErrInvalidDemand, i, err)
		}
		if !found {
			continue
		}
		if selecting > 0 && s != shape {
			return tpuslice.Shape{}, false, fmt.Errorf("%w: pods select %s %s and %s %s; the pods of one instance share one slice",
				ErrInvalidDemand, shape.Accelerator, shape.Topology, s.Accelerator, s.Topology)
		}
		shape = s
		selecting++
	}
	if selecting == 0 {
		return tpuslice.Shape{}, false, nil
	}
	if selecting != len(pods) {
		return tpuslice.Shape{}, false, fmt.Errorf("%w: %d of %d pods select topology %s; the pods of one instance share one slice",
			ErrInvalidDemand, selecting, len(pods), shape.Topology)
	}
	return shape, true, nil
}
