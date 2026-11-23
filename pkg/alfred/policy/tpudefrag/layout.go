package tpudefrag

import (
	"sort"
	"strings"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

// shape is a TPU partition shape present on the cluster's nodes.
type shape struct {
	name  string
	chips int
	// hosts is the host count of a complete partition of this shape: the most
	// hosts any partition of the shape has.
	hosts int
}

// host is one TPU node as the layout sees it.
type host struct {
	node *snapshot.Node
	// pod is the TPU pod the host belongs to, from tpuSlicePartitions.podLabel.
	pod string
	// ids and healthy hold, per shape, the host's partition id and whether
	// that partition reports the healthy state.
	ids     map[string]string
	healthy map[string]bool
	// busy: the host carries a slice or holds an accelerator pod.
	busy bool
	// fence is this policy's fence on the host, nil when it has none.
	fence *Fence
	// blocked: the host cannot take a new slice for a reason other than this
	// policy's fence (unhealthy, under maintenance, cordoned by someone else,
	// marked for scale-down, or carrying an unreadable fence).
	blocked bool
}

// partition is one TPU partition of one shape.
type partition struct {
	shape string
	id    string
	pod   string
	hosts []string
}

// layout is the cluster's TPU partition tree, derived from node labels.
type layout struct {
	cfg *config.Config
	// shapes is ascending by chip count.
	shapes []shape
	byName map[string]shape
	hosts  map[string]*host
	parts  map[string]map[string]*partition
}

// buildLayout reads every node that carries a partition id label or this
// policy's fence. A fenced node that lost its partition labels is kept so its
// drain still releases it. Shapes come from the label keys that match
// tpuSlicePartitions.idLabel.
func buildLayout(snap *snapshot.ClusterSnapshot, cfg *config.Config) *layout {
	tc := &cfg.TPUSlicePartitions
	prefix, suffix, _ := strings.Cut(tc.IDLabel, "%s")
	l := &layout{cfg: cfg, byName: map[string]shape{}, hosts: map[string]*host{}, parts: map[string]map[string]*partition{}}
	chips := map[string]int{}
	for name, node := range snap.Nodes {
		if node == nil {
			continue
		}
		h := &host{node: node, pod: node.Labels[tc.PodLabel], ids: map[string]string{}, healthy: map[string]bool{}}
		for key, id := range node.Labels {
			if id == "" || !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, suffix) || len(key) <= len(prefix)+len(suffix) {
				continue
			}
			name := key[len(prefix) : len(key)-len(suffix)]
			c, ok := config.ShapeChips(name)
			if !ok {
				continue
			}
			chips[name] = c
			h.ids[name] = id
			h.healthy[name] = node.Labels[tc.Label(tc.StateLabel, name)] == tc.HealthyState
		}
		if len(h.ids) == 0 && node.DefragmentationFence == "" {
			continue
		}
		h.busy = node.Labels[tc.SliceLabel] != "" || holdsAccelerators(node)
		if node.DefragmentationFence != "" {
			if f, ok := DecodeFence(node.DefragmentationFence); ok {
				h.fence = &f
			}
		}
		h.blocked = node.Health.Quarantined() || node.Maintenance.Requested || node.ScaleDownMarked ||
			(node.Cordoned && h.fence == nil) || (node.DefragmentationFence != "" && h.fence == nil)
		l.hosts[name] = h
		for s, id := range h.ids {
			if l.parts[s] == nil {
				l.parts[s] = map[string]*partition{}
			}
			p := l.parts[s][id]
			if p == nil {
				p = &partition{shape: s, id: id, pod: h.pod}
				l.parts[s][id] = p
			}
			p.hosts = append(p.hosts, name)
		}
	}
	for name, c := range chips {
		s := shape{name: name, chips: c}
		for _, p := range l.parts[name] {
			sort.Strings(p.hosts)
			if len(p.hosts) > s.hosts {
				s.hosts = len(p.hosts)
			}
		}
		l.shapes = append(l.shapes, s)
		l.byName[name] = s
	}
	sort.Slice(l.shapes, func(i, j int) bool {
		if l.shapes[i].chips != l.shapes[j].chips {
			return l.shapes[i].chips < l.shapes[j].chips
		}
		return l.shapes[i].name < l.shapes[j].name
	})
	return l
}

func holdsAccelerators(node *snapshot.Node) bool {
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

// complete reports whether the partition has every host its shape implies.
// A partition that lost a node cannot take a slice of its shape.
func (l *layout) complete(p *partition) bool {
	return len(p.hosts) == l.byName[p.shape].hosts
}

// freeHost reports whether a host could take part in a new slice once any
// fence of this policy is removed: no slice, no accelerator pod, not blocked
// for another reason, and healthy in the smallest shape it carries. Node
// pools may carry different shapes, so that shape is the host's own.
func (l *layout) freeHost(h *host) bool {
	if h.busy || h.blocked {
		return false
	}
	for _, s := range l.shapes {
		if _, ok := h.ids[s.name]; ok {
			return h.healthy[s.name]
		}
	}
	return false
}

// wholeFree reports whether a slice of the partition's shape could be placed
// on it now. taken and fenced are hosts a prediction has filled or fenced.
func (l *layout) wholeFree(p *partition, taken, fenced map[string]bool) bool {
	if !l.complete(p) {
		return false
	}
	for _, name := range p.hosts {
		h := l.hosts[name]
		if h.busy || h.blocked || h.fence != nil || !h.healthy[p.shape] || taken[name] || fenced[name] {
			return false
		}
	}
	return true
}

// podStats counts, per TPU pod, the free hosts and the whole free partitions
// of each shape. Hosts without the pod label are left out: they may belong to
// different TPU pods, so no gap, drain or gauge is computed for them.
type podStats struct {
	freeHosts int
	wholeFree map[string]int
}

func (l *layout) stats() map[string]*podStats {
	out := map[string]*podStats{}
	get := func(pod string) *podStats {
		if out[pod] == nil {
			out[pod] = &podStats{wholeFree: map[string]int{}}
		}
		return out[pod]
	}
	for _, h := range l.hosts {
		if h.pod == "" {
			continue
		}
		st := get(h.pod)
		if l.freeHost(h) {
			st.freeHosts++
		}
	}
	for s, parts := range l.parts {
		for _, p := range parts {
			if p.pod != "" && l.wholeFree(p, nil, nil) {
				get(p.pod).wholeFree[s]++
			}
		}
	}
	return out
}

// packingBound is how many whole free partitions of the shape the pod would
// hold if its free hosts were packed perfectly.
func (l *layout) packingBound(st *podStats, s shape) int {
	if s.hosts == 0 {
		return 0
	}
	return st.freeHosts / s.hosts
}

// ancestorChips returns the chip count of the largest wholly free partition
// that contains p, p itself included. slice-scheduler prefers the partition
// whose value is smallest. Every larger shape is checked: a host may carry no
// partition of an intermediate shape, and each shape reports its own health.
func (l *layout) ancestorChips(p *partition, taken, fenced map[string]bool) int {
	best := l.byName[p.shape].chips
	first := l.hosts[p.hosts[0]]
	for _, s := range l.shapes {
		if s.chips <= l.byName[p.shape].chips {
			continue
		}
		ancestor := l.parts[s.name][first.ids[s.name]]
		if ancestor == nil || !l.wholeFree(ancestor, taken, fenced) {
			continue
		}
		best = s.chips
	}
	return best
}
