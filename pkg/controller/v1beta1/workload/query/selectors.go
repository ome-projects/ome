package query

import workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"

// LeaderRoutingSelector narrows base to the one pod of a multi-pod Instance
// that serves traffic: the leader runner, pinned to ordinal 0 when ordinal is
// set. Ordinals are numbered per runner, so the runner key is what keeps the
// rank-0 worker out; the leader runner has size one, so ordinal 0 is its only
// pod. Every Service OME renders to route traffic to a leader/worker
// Component narrows its selector here, so no producer can admit a worker.
// base is copied, never modified.
func LeaderRoutingSelector(base map[string]string, ordinal bool) map[string]string {
	out := make(map[string]string, len(base)+2)
	for k, v := range base {
		out[k] = v
	}
	out[LabelRunner] = workload.RunnerLeader
	if ordinal {
		out[LabelPodOrdinal] = "0"
	}
	return out
}
