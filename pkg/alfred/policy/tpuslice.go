package policy

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"

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
// exists. A partition is free when it has at least one host per member pod
// and every node carrying its id label is an acceptable destination, reports
// the healthy partition state, belongs to no slice, holds no accelerator pod,
// and fits the member pods' node selectors and required node affinity. A
// slice claims its hosts before its pods start, so an empty host can still be
// taken.
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

	fits, ok := replacementHostFilter(inst.Pods, tc)
	if !ok {
		return nil, AdvisoryAcceleratorPlacementUnmodeled
	}
	idLabel := tc.Label(tc.IDLabel, topology)
	stateLabel := tc.Label(tc.StateLabel, topology)
	// The replacement needs one host per member pod, so a partition with fewer
	// visible hosts, for example one whose node was deleted during repair,
	// cannot hold it even if every remaining host is usable.
	hosts := map[string]int{}
	usable := map[string]bool{}
	for _, node := range snap.Nodes {
		if node == nil {
			continue
		}
		id := node.Labels[idLabel]
		if id == "" {
			continue
		}
		ok, seen := usable[id]
		hosts[id]++
		if seen && !ok {
			continue
		}
		usable[id] = !node.UnavailableAsTarget() && node.Labels[stateLabel] == tc.HealthyState &&
			node.Labels[tc.SliceLabel] == "" && !holdsAcceleratorPod(node) && fits(node)
	}
	count := 0
	for id, ok := range usable {
		if ok && hosts[id] >= len(inst.Pods) {
			count++
		}
	}
	if count == 0 {
		return nil, AdvisoryNoTPUSliceCapacity
	}
	return &TPUSlicePlan{Topology: topology, FreePartitions: count}, ""
}

// holdsAcceleratorPod reports whether any pod on the node requests GPUs or TPU
// chips, OME-managed or not.
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

// TPUReplacementHostFilter returns the test PlanTPUSliceReplacement applies to
// each host of a candidate partition, or false when a pod's required node
// affinity cannot be parsed.
func TPUReplacementHostFilter(pods []snapshot.PodInfo, tc *config.TPUSlicePartitions) (func(*snapshot.Node) bool, bool) {
	return replacementHostFilter(pods, tc)
}

// replacementHostFilter returns a test of whether a node can run every member
// of the replacement. The replacement keeps the source pods' node selectors and
// required node affinity, except the slice and topology selectors the slice
// provisioner writes onto nodes when it forms the new slice. It returns false
// when a pod's required node affinity cannot be parsed.
func replacementHostFilter(pods []snapshot.PodInfo, tc *config.TPUSlicePartitions) (func(*snapshot.Node) bool, bool) {
	selectors := map[string]string{}
	conflict := false
	var affinities []*nodeaffinity.NodeSelector
	for i := range pods {
		for k, v := range pods[i].NodeSelector {
			if k == tc.SliceLabel || k == tc.TopologyLabel {
				continue
			}
			if existing, seen := selectors[k]; seen && existing != v {
				conflict = true
			}
			selectors[k] = v
		}
		if pods[i].RequiredNodeAffinity != nil {
			selector, err := nodeaffinity.NewNodeSelector(pods[i].RequiredNodeAffinity)
			if err != nil {
				return nil, false
			}
			affinities = append(affinities, selector)
		}
	}
	return func(node *snapshot.Node) bool {
		if conflict {
			return false
		}
		for k, v := range selectors {
			if node.Labels[k] != v {
				return false
			}
		}
		if len(affinities) == 0 {
			return true
		}
		candidate := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: node.Name, Labels: node.Labels}}
		for _, selector := range affinities {
			if !selector.Match(candidate) {
				return false
			}
		}
		return true
	}, true
}
