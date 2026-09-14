package irprojector

import (
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils"
)

// applyQuotaGovernance returns p with the Kueue queue-name label cleared from
// the two metadata sources the projector stamps, for a Component whose pods
// hold no accelerator capacity.
//
// The label is the whole of Kueue's pod integration: a pod carrying it holds a
// scheduling gate until a ClusterQueue admits it. Where no chips are requested
// there is no accelerator budget to adjudicate, so the gate yields nothing and
// costs a Workload object, an admission round trip, and a dependency on that
// queue covering every ordinary resource the pod requests. A router running on
// cpu and memory alone is the concrete case.
//
// The filtering is done on copies. p.ObjectMeta.Labels is the map the dispatch
// site also passes to the per-Component Service and PodMonitor, and
// p.ComponentExt points into the merged ComponentExtensionSpec that the ISVC
// reconciler reads for autoscaling and status, so clearing a key in place
// would reach well beyond the pod template. Params is taken by value, which
// keeps a swapped-in map local to this projection.
func applyQuotaGovernance(p Params) Params {
	if quotaGoverned(p) {
		return p
	}
	p.ObjectMeta.Labels = withoutQueueLabel(p.ObjectMeta.Labels)
	if p.ComponentExt != nil {
		ext := *p.ComponentExt
		ext.Labels = withoutQueueLabel(ext.Labels)
		p.ComponentExt = &ext
	}
	return p
}

// quotaGoverned reports whether this Component belongs under Kueue admission
// control: its pods request accelerator capacity, so they charge a budget.
//
// The decision is per Component, not per Runner — a multi-pod Component is one
// gang, and a leader that only coordinates is as much a part of it as the
// workers holding the chips. Gating half a gang would release the leader while
// its workers still waited on quota.
//
// An empty QuotaAcceleratorResources governs everything, because the two errors
// are not symmetric: gating a pod that need not be gated delays it, while
// ungating one that holds silicon runs it against no budget at all, invisibly.
// A cluster that names no accelerator resources has told the manager nothing it
// can act on, so every Component stays governed.
func quotaGoverned(p Params) bool {
	if len(p.QuotaAcceleratorResources) == 0 {
		return true
	}
	return utils.PodRequestsAccelerator(p.PodSpec, p.QuotaAcceleratorResources) ||
		utils.PodRequestsAccelerator(p.WorkerPodSpec, p.QuotaAcceleratorResources)
}

// withoutQueueLabel returns labels minus the Kueue queue-name key, or labels
// itself when the key is absent — the common path allocates nothing and a nil
// map stays nil, so an exempt Component projects the same metadata shape as
// one that never carried the label.
func withoutQueueLabel(labels map[string]string) map[string]string {
	if _, ok := labels[constants.KueueQueueLabelKey]; !ok {
		return labels
	}
	out := make(map[string]string, len(labels)-1)
	for k, v := range labels {
		if k == constants.KueueQueueLabelKey {
			continue
		}
		out[k] = v
	}
	return out
}
