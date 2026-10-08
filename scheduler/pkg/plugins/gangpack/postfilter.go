package gangpack

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/kube-scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

// PostFilter unwinds a gang when every candidate failed before Reserve. Domain
// planning intentionally evaluates only resources and hard node constraints;
// other framework filters (for example volumes, ports, and inter-pod affinity)
// may still reject the domain. Without this hook no Reserve/Unreserve callback
// runs and the pin's exclusive reservation could persist indefinitely.
//
// A member vetoed in a domain new to its gang is activated here while another
// domain is untried: the vetoing filters register no event for a retry that only
// the plugin's failed-domain memory changes, and parked siblings cannot plan for it.
func (g *GangPack) PostFilter(ctx context.Context, state framework.CycleState, pod *v1.Pod, _ framework.NodeToStatusReader) (*framework.PostFilterResult, *framework.Status) {
	pin := readPin(state)
	if pin == nil {
		return nil, framework.NewStatus(framework.Unschedulable)
	}
	if _, newDomain := g.releaseAttempt(pin, pod, true); newDomain && !pin.soleFit && g.handle != nil {
		gangActivationTotal.WithLabelValues(activationTriggerDomainFailed).Inc()
		g.handle.Activate(klog.Background(), map[string]*v1.Pod{pod.Namespace + "/" + pod.Name: pod})
	}
	return nil, framework.NewStatus(framework.Unschedulable, "gang reservation released after all candidate nodes were filtered")
}

func failedDomainKey(gang gangInfo) string {
	return gang.key + "\x00" + gang.uid
}

// markFailedDomain records a domain whose candidates all failed another Filter
// and reports whether the gang had not failed it before.
func (g *GangPack) markFailedDomain(gang gangInfo, domain placement.Domain) bool {
	g.failedMu.Lock()
	defer g.failedMu.Unlock()
	if g.failedDomains == nil {
		g.failedDomains = make(map[string]sets.Set[placement.Domain])
	}
	key := failedDomainKey(gang)
	if g.failedDomains[key] == nil {
		g.failedDomains[key] = sets.New[placement.Domain]()
	}
	if g.failedDomains[key].Has(domain) {
		return false
	}
	g.failedDomains[key].Insert(domain)
	return true
}

func (g *GangPack) clearFailedDomains(gang gangInfo) {
	g.failedMu.Lock()
	delete(g.failedDomains, failedDomainKey(gang))
	g.failedMu.Unlock()
}

// withoutFailedDomains returns a copy with failed domains removed. The caller
// retries the unfiltered map after this set is exhausted, producing a stable
// best-fit order rather than selecting the same failing domain forever.
func (g *GangPack) withoutFailedDomains(gang gangInfo, free topology.FreeByDomain) (topology.FreeByDomain, bool) {
	g.failedMu.Lock()
	failed := g.failedDomains[failedDomainKey(gang)].Clone()
	g.failedMu.Unlock()
	if len(failed) == 0 {
		return free, false
	}
	out := make(topology.FreeByDomain, len(free))
	for name, capacity := range free {
		if !failed.Has(placement.Domain{TopologyKey: gang.topologyKey, Name: name}) {
			out[name] = capacity
		}
	}
	return out, true
}
