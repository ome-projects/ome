package gangpack

import (
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
)

// podGroupLabel is the scheduler-plugins standard pod label naming a pod's
// PodGroup. It is an ecosystem standard (sigs.k8s.io/scheduler-plugins), not an
// OME convention, so it is used as-is — not made configurable.
const podGroupLabel = "scheduling.x-k8s.io/pod-group"

// gangInfo is everything the plugin needs to place and gate a gang, all declared
// by the workload (never global config): the gang's key, how many members must
// land together, which node label denotes its domain, and how long a member may
// wait at the gate for its siblings.
type gangInfo struct {
	key         string // namespace/podgroup-name
	uid         string // immutable PodGroup identity; a reused name inherits no assignment
	minMember   int
	topologyKey string
	timeout     time.Duration
}

// placementGang is the gang as the assignment store names it.
func (g gangInfo) placementGang() placement.Gang {
	return placement.Gang{Group: g.key, Owner: g.uid, TopologyKey: g.topologyKey}
}

// gangFacts is what a PodGroup declares about its gang: how many members must
// land together, the node label that denotes its domain (empty when the
// PodGroup declares none), how long a member may wait at the gate, the
// PodGroup's immutable identity, and whether it carries the configured
// placement-group label, which names a partner gang whatever its value.
type gangFacts struct {
	minMember        int
	topologyKey      string
	timeout          time.Duration
	uid              string
	inPlacementGroup bool
}

// podGroupReader resolves a PodGroup into its gang facts. It is an interface so
// the plugin logic is unit-testable without a live PodGroup informer.
type podGroupReader interface {
	get(namespace, name string) (gangFacts, bool)
}

// gang builds the gangInfo for the PodGroup key from its facts. The configured
// fallback stands in for a topology key the PodGroup does not declare; ok is
// false when neither names one, so there is no domain to assign.
func (f gangFacts) gang(key, fallbackTopologyKey string) (gangInfo, bool) {
	topologyKey := f.topologyKey
	if topologyKey == "" {
		topologyKey = fallbackTopologyKey
	}
	if topologyKey == "" {
		return gangInfo{}, false
	}
	return gangInfo{key: key, uid: f.uid, minMember: f.minMember, topologyKey: topologyKey, timeout: f.timeout}, true
}

// podGroupNameOf returns the (namespace, name) of a pod's PodGroup — namespace
// is the pod's own, name is the standard pod-group label value — and false when
// the pod carries no pod-group label (not a gang member).
func podGroupNameOf(pod *v1.Pod) (namespace, name string, ok bool) {
	if pod == nil {
		return "", "", false
	}
	name = pod.Labels[podGroupLabel]
	if name == "" {
		return "", "", false
	}
	return pod.Namespace, name, true
}

// splitGangKey splits a gang key ("namespace/podgroup-name") back into its parts.
// ok is false for a malformed key (no separator, or an empty half).
func splitGangKey(key string) (namespace, name string, ok bool) {
	i := strings.IndexByte(key, '/')
	if i <= 0 || i == len(key)-1 {
		return "", "", false
	}
	return key[:i], key[i+1:], true
}

// resolveGang assembles a pod's gangInfo from its PodGroup. It returns false —
// meaning "not a domain-assigned gang, leave placement unconstrained" — when the
// pod is not a gang member, its PodGroup is absent, its minMember is
// non-positive, or neither the PodGroup nor the fallback names a topology key.
func resolveGang(pod *v1.Pod, r podGroupReader, fallbackTopologyKey string) (gangInfo, bool) {
	ns, name, ok := podGroupNameOf(pod)
	if !ok {
		return gangInfo{}, false
	}
	facts, found := r.get(ns, name)
	if !found || facts.minMember <= 0 {
		return gangInfo{}, false
	}
	return facts.gang(ns+"/"+name, fallbackTopologyKey)
}
