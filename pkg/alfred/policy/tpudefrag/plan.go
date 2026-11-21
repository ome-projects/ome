package tpudefrag

import (
	"fmt"
	"math"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// Drain outcomes; also the outcome label of alfred_tpu_drains_total.
const (
	OutcomeCompleted       = "completed"
	OutcomeDeadline        = "deadline"
	OutcomeWindowClosed    = "window_closed"
	OutcomeFenceNotHonored = "fence_not_honored"
	OutcomeCordonFailed    = "cordon_failed"
	OutcomeAborted         = "aborted"
)

// InstanceKey names one OMENative Instance.
type InstanceKey struct {
	Workload  types.NamespacedName
	Component v1beta1.ComponentType
	Instance  int32
}

func (k InstanceKey) String() string {
	return fmt.Sprintf("%s/%s/%d", k.Workload, k.Component, k.Instance)
}

// Move is one Instance a drain must move, and the fenced host it moves from.
type Move struct {
	Key      InstanceKey
	FromNode string
	// Partition is the drained partition the Instance occupies.
	Partition string
}

// Drain is a set of partitions of the unit shape in one TPU pod, plus every
// Instance on them. A drain is proposed only when every one of those
// Instances is predicted to fit elsewhere without breaking a whole free
// partition of the unit shape.
type Drain struct {
	// ID is empty on a proposed drain; the fencer names it when it fences.
	ID         string
	Pod        string
	Unit       string
	Partitions []string
	// Hosts are every host of Partitions; Busy the ones holding a slice now.
	Hosts    []string
	Busy     []string
	Moves    []Move
	Deadline time.Time
}

// OpenDrain is a drain whose fence is on at least one host.
type OpenDrain struct {
	Drain
	// Fenced hosts carry this drain's fence; Missing hosts belong to its
	// partitions but do not.
	Fenced  []string
	Missing []string
	// Settled: every host is fenced and cordoned, and the last cordon has
	// stood for fenceSettleSeconds.
	Settled bool
	// Empty lists partitions that hold no slice any more. The fencer removes
	// their fence at once.
	Empty []string
	// End is the outcome that ends the drain now; empty while it continues.
	End string
}

// Plan is the policy's view of one snapshot.
type Plan struct {
	// Unit is the drain shape, empty when no target shape exists on the
	// cluster.
	Unit    string
	Open    []*OpenDrain
	Intents []*Drain
}

// Enabled reports whether the policy is switched on.
func Enabled(cfg *config.Config) bool {
	e := cfg.Policies.TPUDefragmentation.Enabled
	return e != nil && *e
}

// Build plans drains from one snapshot. It is pure. backoff reports TPU pods
// in which no new drain may open; nil means none.
func Build(snap *snapshot.ClusterSnapshot, cfg *config.Config, now time.Time, backoff func(pod string) bool) *Plan {
	plan := &Plan{}
	if snap == nil || cfg == nil {
		return plan
	}
	l := buildLayout(snap, cfg)
	plan.Open = openDrains(snap, cfg, l, now)
	unit, ok := unitShape(l, cfg)
	if !ok {
		return plan
	}
	plan.Unit = unit.name
	if !Enabled(cfg) {
		return plan
	}
	// One open drain per cluster. A drain's replacements may be predicted to
	// land in another TPU pod, so a second drain could fence the hosts the
	// first counts on. Dispatch is serial, so a second drain would add no
	// moves, only fenced capacity.
	if len(plan.Open) > 0 {
		return plan
	}
	stats := l.stats()
	td := &cfg.Policies.TPUDefragmentation
	gap := map[string]int{}
	pods := make([]string, 0, len(stats))
	for pod, st := range stats {
		gap[pod] = l.packingBound(st, unit) - st.wholeFree[unit.name]
		if gap[pod] >= td.MinGap && (backoff == nil || !backoff(pod)) {
			pods = append(pods, pod)
		}
	}
	sort.Slice(pods, func(i, j int) bool {
		if gap[pods[i]] != gap[pods[j]] {
			return gap[pods[i]] > gap[pods[j]]
		}
		return pods[i] < pods[j]
	})
	view := planningView(snap)
	for _, pod := range pods {
		if d := selectDrain(view, cfg, l, unit, pod); d != nil {
			d.Deadline = now.Add(td.DrainDeadline())
			plan.Intents = append(plan.Intents, d)
			break
		}
	}
	return plan
}

// planningView is the snapshot drains are planned against. Without a
// configured migration API every Instance is reported unmovable; planning
// still judges movability as if execution were enabled, so recommend-only
// mode shows the drains Alfred would run. The emitted advisories keep the
// real reason.
func planningView(snap *snapshot.ClusterSnapshot) *snapshot.ClusterSnapshot {
	if snap.OMENativeExecutor.Available {
		return snap
	}
	view := *snap
	view.OMENativeExecutor.Available = true
	return &view
}

// unitShape is the largest target shape present on the cluster.
func unitShape(l *layout, cfg *config.Config) (shape, bool) {
	var unit shape
	found := false
	for _, name := range cfg.Policies.TPUDefragmentation.TargetShapes {
		s, ok := l.byName[name]
		if ok && s.hosts > 0 && (!found || s.chips > unit.chips) {
			unit, found = s, true
		}
	}
	return unit, found
}

// openDrains reads drains back from fence annotations and decides, for each,
// whether it ends now and which partitions have emptied.
func openDrains(snap *snapshot.ClusterSnapshot, cfg *config.Config, l *layout, now time.Time) []*OpenDrain {
	byID := map[string]*OpenDrain{}
	partitions := map[string]map[string]bool{}
	var latest = map[string]time.Time{}
	names := sortedHostNames(l)
	for _, name := range names {
		h := l.hosts[name]
		if h.fence == nil {
			continue
		}
		f := h.fence
		d := byID[f.Drain]
		if d == nil {
			d = &OpenDrain{Drain: Drain{ID: f.Drain, Pod: h.pod, Unit: f.Unit, Deadline: f.Deadline}}
			byID[f.Drain] = d
			partitions[f.Drain] = map[string]bool{}
		}
		if f.Deadline.Before(d.Deadline) {
			d.Deadline = f.Deadline
		}
		if f.FencedAt.After(latest[f.Drain]) {
			latest[f.Drain] = f.FencedAt
		}
		partitions[f.Drain][f.Partition] = true
		d.Fenced = append(d.Fenced, name)
	}
	td := &cfg.Policies.TPUDefragmentation
	out := make([]*OpenDrain, 0, len(byID))
	for id, d := range byID {
		fenced := map[string]bool{}
		for _, name := range d.Fenced {
			fenced[name] = true
		}
		hosts := map[string]bool{}
		for pid := range partitions[id] {
			d.Partitions = append(d.Partitions, pid)
			members := []string(nil)
			if p := l.parts[d.Unit][pid]; p != nil {
				members = p.hosts
			}
			empty := true
			for _, name := range members {
				hosts[name] = true
				if l.hosts[name].busy {
					empty = false
				}
			}
			for _, name := range d.Fenced {
				if l.hosts[name].fence.Partition == pid {
					hosts[name] = true
					if l.hosts[name].busy {
						empty = false
					}
				}
			}
			if empty {
				d.Empty = append(d.Empty, pid)
			}
		}
		sort.Strings(d.Partitions)
		sort.Strings(d.Empty)
		for name := range hosts {
			d.Hosts = append(d.Hosts, name)
			if !fenced[name] {
				d.Missing = append(d.Missing, name)
			}
		}
		sort.Strings(d.Hosts)
		sort.Strings(d.Missing)
		d.Settled = len(d.Missing) == 0 && now.Sub(latest[id]) >= td.FenceSettle()
		for _, name := range d.Fenced {
			if !l.hosts[name].node.Cordoned {
				d.Settled = false
			}
		}
		d.End = drainEnd(cfg, l, d, now)
		emptied := map[string]bool{}
		for _, pid := range d.Empty {
			emptied[pid] = true
		}
		d.Busy, d.Moves = occupants(snap, l, d.Hosts, func(name string) string {
			if f := l.hosts[name].fence; f != nil && f.Drain == id {
				return f.Partition
			}
			return l.hosts[name].ids[d.Unit]
		}, emptied)
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func drainEnd(cfg *config.Config, l *layout, d *OpenDrain, now time.Time) string {
	switch {
	case !Enabled(cfg) || cfg.Mode != config.ModeExecute:
		return OutcomeAborted
	case !now.Before(d.Deadline):
		return OutcomeDeadline
	case !cfg.MaintenanceWindowOpen(now):
		return OutcomeWindowClosed
	}
	for _, name := range d.Fenced {
		h := l.hosts[name]
		if h.fence.FreeAtFence && h.busy {
			return OutcomeFenceNotHonored
		}
	}
	return ""
}

// occupants returns the busy hosts among hosts and one Move per Instance on
// them, skipping hosts whose partition (as partitionOf names it) is in skip.
func occupants(snap *snapshot.ClusterSnapshot, l *layout, hosts []string, partitionOf func(string) string, skip map[string]bool) ([]string, []Move) {
	var busy []string
	moves := map[InstanceKey]Move{}
	for _, name := range hosts {
		h := l.hosts[name]
		if h == nil || !h.busy {
			continue
		}
		pid := partitionOf(name)
		if skip[pid] {
			continue
		}
		busy = append(busy, name)
		for _, pod := range h.node.OMEPods {
			key, ok := podInstance(snap, pod)
			if !ok {
				continue
			}
			if m, seen := moves[key]; !seen || name < m.FromNode {
				moves[key] = Move{Key: key, FromNode: name, Partition: pid}
			}
		}
	}
	out := make([]Move, 0, len(moves))
	for _, m := range moves {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key.String() < out[j].Key.String() })
	return busy, out
}

// podInstance resolves the Instance an accelerator pod belongs to.
func podInstance(snap *snapshot.ClusterSnapshot, pod snapshot.PodInfo) (InstanceKey, bool) {
	if !pod.HoldsAccelerators() || !pod.InstanceIndexValid || pod.ISVC.Name == "" {
		return InstanceKey{}, false
	}
	key := InstanceKey{Workload: pod.ISVC, Component: pod.Component, Instance: pod.InstanceIndex}
	_, _, inst := lookup(snap, key)
	return key, inst != nil
}

func lookup(snap *snapshot.ClusterSnapshot, key InstanceKey) (*snapshot.Workload, *snapshot.Component, *snapshot.Instance) {
	w := snap.Workloads[key.Workload]
	if w == nil {
		return nil, nil, nil
	}
	comp := w.Components[key.Component]
	if comp == nil {
		return w, nil, nil
	}
	for _, inst := range comp.Instances {
		if inst != nil && inst.Index == key.Instance {
			return w, comp, inst
		}
	}
	return w, comp, nil
}

// movableReason returns "" when the Instance may move onto a new TPU slice
// now, or the advisory reason it may not.
func movableReason(snap *snapshot.ClusterSnapshot, cfg *config.Config, key InstanceKey) (string, *policy.TPUSlicePlan) {
	w, comp, inst := lookup(snap, key)
	if inst == nil {
		return policy.AdvisoryOMENativeObservationInvalid, nil
	}
	switch comp.DeploymentMode {
	case constants.OMENative:
	case constants.RawDeployment:
		return policy.AdvisoryRawDeploymentMigrationUnsupported, nil
	case constants.MultiNode:
		return policy.AdvisoryLWSMigrationUnsupported, nil
	default:
		return policy.AdvisoryOMENativeObservationInvalid, nil
	}
	if reason := policy.ModelAdvisoryReason(snap, w); reason != "" {
		return reason, nil
	}
	if cfg.OMENativeMigrationEnabled == nil || !*cfg.OMENativeMigrationEnabled {
		return policy.AdvisoryMigrationSurfaceDisabled, nil
	}
	if reason := policy.OMENativeEligibility(snap, w, comp, inst); reason != "" {
		return reason, nil
	}
	plan, reason := policy.PlanTPUSliceReplacement(snap, cfg, inst)
	return reason, plan
}

// candidate is a partition of the unit shape that a drain could empty.
type candidate struct {
	p     *partition
	busy  []string
	moves []Move
}

// mover is one Instance a prediction places.
type mover struct {
	key      InstanceKey
	topology string
	chips    int
	pods     []snapshot.PodInfo
}

// selectDrain picks the partitions of one TPU pod to drain: candidates in
// order of fewest moves, then fewest busy hosts, then id, each kept only if
// the drain stays feasible with it.
func selectDrain(snap *snapshot.ClusterSnapshot, cfg *config.Config, l *layout, unit shape, pod string) *Drain {
	td := &cfg.Policies.TPUDefragmentation
	var cands []candidate
	ids := make([]string, 0, len(l.parts[unit.name]))
	for id := range l.parts[unit.name] {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		p := l.parts[unit.name][id]
		if p.pod != pod {
			continue
		}
		if c, ok := drainable(snap, cfg, l, unit, p); ok {
			cands = append(cands, c)
		}
	}
	sort.SliceStable(cands, func(i, j int) bool {
		if len(cands[i].moves) != len(cands[j].moves) {
			return len(cands[i].moves) < len(cands[j].moves)
		}
		if len(cands[i].busy) != len(cands[j].busy) {
			return len(cands[i].busy) < len(cands[j].busy)
		}
		return cands[i].p.id < cands[j].p.id
	})
	var chosen []candidate
	for _, c := range cands {
		if len(chosen) == td.MaxDrainPartitions {
			break
		}
		if feasible(snap, cfg, l, unit, append(append([]candidate(nil), chosen...), c)) {
			chosen = append(chosen, c)
		}
	}
	if len(chosen) == 0 {
		return nil
	}
	d := &Drain{Pod: pod, Unit: unit.name}
	for _, c := range chosen {
		d.Partitions = append(d.Partitions, c.p.id)
		d.Hosts = append(d.Hosts, c.p.hosts...)
		d.Busy = append(d.Busy, c.busy...)
		d.Moves = append(d.Moves, c.moves...)
	}
	sort.Strings(d.Partitions)
	sort.Strings(d.Hosts)
	sort.Strings(d.Busy)
	sort.Slice(d.Moves, func(i, j int) bool { return d.Moves[i].Key.String() < d.Moves[j].Key.String() })
	return d
}

// drainable reports whether every Instance on the partition can move: the
// partition is complete, healthy, not fenced or blocked, holds between one
// and maxBusyHostsPerPartition busy hosts, and each of those holds only
// movable OMENative Instances whose pods all sit inside the partition.
func drainable(snap *snapshot.ClusterSnapshot, cfg *config.Config, l *layout, unit shape, p *partition) (candidate, bool) {
	if !l.complete(p) {
		return candidate{}, false
	}
	inside := map[string]bool{}
	for _, name := range p.hosts {
		h := l.hosts[name]
		if h.blocked || h.fence != nil || !h.healthy[unit.name] {
			return candidate{}, false
		}
		inside[name] = true
	}
	busy, moves := occupants(snap, l, p.hosts, func(string) string { return p.id }, nil)
	if len(busy) == 0 || len(busy) > cfg.Policies.TPUDefragmentation.MaxBusyHostsPerPartition {
		return candidate{}, false
	}
	owned := map[string]bool{}
	for _, m := range moves {
		if reason, _ := movableReason(snap, cfg, m.Key); reason != "" {
			return candidate{}, false
		}
		_, _, inst := lookup(snap, m.Key)
		for _, pod := range inst.Pods {
			if !inside[pod.Node] {
				return candidate{}, false
			}
			owned[pod.Node] = true
		}
	}
	// A busy host none of the movable Instances accounts for holds a slice or
	// accelerator pod this policy cannot move.
	for _, name := range busy {
		if !owned[name] || len(l.hosts[name].node.OtherOccupants) > 0 {
			return candidate{}, false
		}
	}
	return candidate{p: p, busy: busy, moves: moves}, true
}

// feasible predicts where slice-scheduler places each replacement and reports
// whether all of them fit outside the drain without breaking a whole free
// partition of the unit shape anywhere in the cluster, and without moving
// more than maxMovesPerWorkloadPerDrain Instances of one InferenceService.
func feasible(snap *snapshot.ClusterSnapshot, cfg *config.Config, l *layout, unit shape, drain []candidate) bool {
	fenced := map[string]bool{}
	perWorkload := map[types.NamespacedName]int{}
	var movers []mover
	for _, c := range drain {
		for _, name := range c.p.hosts {
			fenced[name] = true
		}
		for _, m := range c.moves {
			perWorkload[m.Key.Workload]++
			if perWorkload[m.Key.Workload] > cfg.Policies.TPUDefragmentation.MaxMovesPerWorkloadPerDrain {
				return false
			}
			_, _, inst := lookup(snap, m.Key)
			topology := ""
			if len(inst.Pods) > 0 {
				topology = inst.Pods[0].NodeSelector[cfg.TPUSlicePartitions.TopologyLabel]
			}
			s, ok := l.byName[topology]
			if !ok {
				return false
			}
			movers = append(movers, mover{key: m.Key, topology: topology, chips: s.chips, pods: inst.Pods})
		}
	}
	sort.Slice(movers, func(i, j int) bool {
		if movers[i].chips != movers[j].chips {
			return movers[i].chips > movers[j].chips
		}
		return movers[i].key.String() < movers[j].key.String()
	})
	taken := map[string]bool{}
	for _, m := range movers {
		p, chips := bestFit(l, cfg, m, taken, fenced)
		if p == nil || chips >= unit.chips {
			return false
		}
		for _, name := range p.hosts {
			taken[name] = true
		}
	}
	return true
}

// bestFit copies slice-scheduler's rule: among whole free partitions of the
// Instance's shape that its pods can run on, the one whose largest whole
// free ancestor is smallest, ties to the lowest id. It returns that
// partition and the ancestor's chip count.
func bestFit(l *layout, cfg *config.Config, m mover, taken, fenced map[string]bool) (*partition, int) {
	fits, ok := policy.TPUReplacementHostFilter(m.pods, &cfg.TPUSlicePartitions)
	if !ok {
		return nil, 0
	}
	var best *partition
	bestChips := math.MaxInt
	for _, p := range l.parts[m.topology] {
		if len(p.hosts) < len(m.pods) || !l.wholeFree(p, taken, fenced) {
			continue
		}
		usable := true
		for _, name := range p.hosts {
			if !fits(l.hosts[name].node) {
				usable = false
				break
			}
		}
		if !usable {
			continue
		}
		chips := l.ancestorChips(p, taken, fenced)
		if chips < bestChips || (chips == bestChips && p.id < best.id) {
			best, bestChips = p, chips
		}
	}
	return best, bestChips
}

func sortedHostNames(l *layout) []string {
	names := make([]string, 0, len(l.hosts))
	for name := range l.hosts {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
