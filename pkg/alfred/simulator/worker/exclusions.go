package worker

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kube-scheduler/framework"
)

const exclusionPlugin = "AlfredSnapshotExclusions"

type exclusions map[string]bool

func (e exclusions) Name() string { return exclusionPlugin }
func (e exclusions) Filter(_ context.Context, _ framework.CycleState, _ *v1.Pod, node framework.NodeInfo) *framework.Status {
	if e[node.Node().Name] {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, "node excluded by simulation request")
	}
	return nil
}
