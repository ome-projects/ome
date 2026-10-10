package policy

import (
	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

// TPUSlicePlan describes a move of a TPU instance onto a new OME-provisioned
// slice. FreePartitions counts partitions of Topology that looked free when
// the plan was made; it is a capacity estimate, not a reservation.
type TPUSlicePlan struct {
	Topology       string `json:"topology"`
	FreePartitions int    `json:"freePartitions"`
}

// PlanTPUSliceReplacement decides whether a TPU instance can move to a new
// slice. It returns the plan, or an advisory reason when the instance must
// stay advisory: the path is off, a member pod is not slice-provisioned or
// the members disagree on topology, or no free partition of that topology
// exists. A partition is free when every node carrying its id label is an
// acceptable destination, reports the healthy partition state, and holds no
// accelerator pod.
func PlanTPUSliceReplacement(snap *snapshot.ClusterSnapshot, cfg *config.Config, inst *snapshot.Instance) (*TPUSlicePlan, string) {
	if snap == nil || cfg == nil || inst == nil {
		return nil, AdvisoryAcceleratorPlacementUnmodeled
	}
	tc := &cfg.TPUSlicePartitions
	if cfg.TPUSliceMigrationEnabled == nil || !*cfg.TPUSliceMigrationEnabled || inst.TotalGPUs > 0 || len(inst.Pods) == 0 {
		return nil, AdvisoryAcceleratorPlacementUnmodeled
	}
	topology := ""
	for i := range inst.Pods {
		pod := &inst.Pods[i]
		value := pod.NodeSelector[tc.TopologyLabel]
		if !pod.TPUSliceProvisioned || value == "" || (topology != "" && value != topology) {
			return nil, AdvisoryAcceleratorPlacementUnmodeled
		}
		topology = value
	}

	idLabel := tc.Label(tc.IDLabel, topology)
	stateLabel := tc.Label(tc.StateLabel, topology)
	free := map[string]bool{}
	for _, node := range snap.Nodes {
		if node == nil {
			continue
		}
		id := node.Labels[idLabel]
		if id == "" {
			continue
		}
		usable, seen := free[id]
		if seen && !usable {
			continue
		}
		free[id] = !node.UnavailableAsTarget() && node.Labels[stateLabel] == tc.HealthyState &&
			!holdsAcceleratorPod(node)
	}
	count := 0
	for _, usable := range free {
		if usable {
			count++
		}
	}
	if count == 0 {
		return nil, AdvisoryNoTPUSliceCapacity
	}
	return &TPUSlicePlan{Topology: topology, FreePartitions: count}, ""
}

func holdsAcceleratorPod(node *snapshot.Node) bool {
	for i := range node.OMEPods {
		if node.OMEPods[i].HoldsAccelerators() {
			return true
		}
	}
	for i := range node.OtherOccupants {
		if node.OtherOccupants[i].HoldsAccelerators() {
			return true
		}
	}
	return false
}
