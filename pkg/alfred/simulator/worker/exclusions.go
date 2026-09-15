package worker

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kube-scheduler/framework"
	"sigs.k8s.io/ome/scheduler/pkg/plugins/gangpack"
)

const exclusionPlugin = "AlfredSnapshotExclusions"

type exclusions struct {
	nodes        map[string]bool
	replacements map[types.UID]bool
}

func (e exclusions) Name() string { return exclusionPlugin }
func (e exclusions) Filter(_ context.Context, _ framework.CycleState, pod *v1.Pod, node framework.NodeInfo) *framework.Status {
	if e.replacements[pod.UID] && e.nodes[node.Node().Name] {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, "node excluded by simulation request")
	}
	return nil
}

// excludedGangPack limits only OME's replacement domain planning. All other
// hooks are delegated unchanged. Mutating Pod affinity or globally cordoning
// nodes would change topology-spread domains or competitors' scheduling rules.
type excludedGangPack struct {
	*gangpack.GangPack
	excluded exclusions
}

func (g *excludedGangPack) PreFilter(ctx context.Context, state framework.CycleState, pod *v1.Pod, nodes []framework.NodeInfo) (*framework.PreFilterResult, *framework.Status) {
	if !g.excluded.replacements[pod.UID] {
		return g.GangPack.PreFilter(ctx, state, pod, nodes)
	}
	allowed := make([]framework.NodeInfo, 0, len(nodes))
	for _, node := range nodes {
		if !g.excluded.nodes[node.Node().Name] {
			allowed = append(allowed, node)
		}
	}
	return g.GangPack.PreFilter(ctx, state, pod, allowed)
}
