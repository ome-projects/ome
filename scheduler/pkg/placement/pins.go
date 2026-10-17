// Package placement holds ome-scheduler's stateful placement decisions layered
// on the pure accounting in package topology. Its first piece is the pin store:
// which domain each placement group's gangs are committed to, and how much whole-
// node capacity each has reserved there but not yet filled.
package placement

import (
	"sync"

	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

// Pins records the domain each gang is pinned to. The first pod chooses a domain
// via best-fit and every later member follows the pin. Partner-gang placement
// groups are not represented until their reservation semantics are defined.
//
// A pin also carries a capacity reservation: the whole nodes the gang has
// committed to its domain but not yet filled. A gang's members arrive one
// scheduling cycle at a time, so between pinning a domain and completing there is
// a window where the gang's future members' capacity is unclaimed; without a
// reservation a second gang can best-fit into it and strand the first (which then
// times out and re-plans). The reservation closes that window; it drains to zero
// via Place as members become real occupancy, so the live snapshot's own
// accounting is never double-counted.
//
// The scheduler runs scheduling cycles single-threaded, but plugin state is also
// touched from event handlers (failover rebuild, release on gang teardown), so
// the store guards itself with a mutex.
type Pins struct {
	mu      sync.Mutex
	byGroup map[string]*commitment
	nextID  uint64
	// reserved is the running per-domain sum of outstanding reservations. Choose
	// uses it as an exclusive-domain barrier for heterogeneous forming gangs.
	reserved map[Domain]int
	// reservedNodes is the physical-node projection of every outstanding domain
	// reservation. Topology keys are only alternate ways to partition the same
	// nodes, so reservations must conflict by node identity as well as by
	// (topologyKey, domain) metadata.
	reservedNodes map[string]int
}

// Domain identifies one topology domain without assuming that domain values are
// globally unique. Two workloads may use different topology keys with the same
// value, and their reservations must not interfere.
type Domain struct {
	TopologyKey string
	Name        string
}

// Reservation is an outstanding whole-node claim in a topology domain.
type Reservation struct {
	Group     string
	Domain    Domain
	Remaining int
	Nodes     []string
}

// commitment is a group's placement decision: the pinned domain plus the count
// of whole nodes still reserved there for members not yet placed. remaining==0
// means the gang is fully committed — every reserved slot has been placed (or the
// pin was rebuilt from already-bound members on failover) — which is the signal
// the gate uses to know it must also count bound siblings, not just waiting ones.
type commitment struct {
	domain    Domain
	owner     string
	remaining int
	id        uint64
	nodes     map[string]struct{}
}

// New returns an empty pin store.
func New() *Pins {
	return &Pins{
		byGroup:       make(map[string]*commitment),
		reserved:      make(map[Domain]int),
		reservedNodes: make(map[string]int),
	}
}

func (p *Pins) nextCommitmentID() uint64 {
	p.nextID++
	return p.nextID
}

func nodeSet(nodes []string) map[string]struct{} {
	out := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if node != "" {
			out[node] = struct{}{}
		}
	}
	return out
}

// addReservation adjusts the per-domain aggregate by delta, dropping the key when
// it reaches zero so the map stays bounded by live domains. Caller holds the lock.
func (p *Pins) addReservation(domain Domain, delta int) {
	p.reserved[domain] += delta
	if p.reserved[domain] <= 0 {
		delete(p.reserved, domain)
	}
}

// setRemaining updates both logical-domain and physical-node reservation
// indexes. Caller holds p.mu.
func (p *Pins) setRemaining(c *commitment, remaining int) {
	if remaining < 0 {
		remaining = 0
	}
	wasReserved := c.remaining > 0
	willReserve := remaining > 0
	p.addReservation(c.domain, remaining-c.remaining)
	if wasReserved != willReserve {
		delta := -1
		if willReserve {
			delta = 1
		}
		for node := range c.nodes {
			p.reservedNodes[node] += delta
			if p.reservedNodes[node] <= 0 {
				delete(p.reservedNodes, node)
			}
		}
	}
	c.remaining = remaining
}

// Choose returns the domain the group is committed to. If the group is already
// pinned, that domain is returned as-is — regardless of current free counts,
// because members are already reserved there; if the gang can no longer complete
// there the caller unwinds via Release and calls Choose again. If the group is
// not yet pinned, Choose best-fits over domains without an outstanding
// reservation, so it never treats heterogeneous claims as fungible slots. It
// then pins the winner and
// reserves gangSize whole nodes there. ok is false only when the group is
// unpinned and no domain currently fits the gang (the gang waits, still
// unpinned).
func (p *Pins) Choose(group string, rawFree topology.FreeByDomain, gangSize int) (domain string, ok bool) {
	return p.ChooseInTopology(group, "", rawFree, gangSize)
}

// ChooseInTopology is Choose with an explicit topology label namespace.
func (p *Pins) ChooseInTopology(group, topologyKey string, rawFree topology.FreeByDomain, gangSize int) (domain string, ok bool) {
	domain, _, ok = p.ChooseInTopologyOnNodes(group, topologyKey, rawFree, nil, gangSize)
	return domain, ok
}

// ChooseInTopologyOnNodes is ChooseInTopology with the physical membership of
// each candidate domain. Physical membership prevents two different topology
// keys from reserving overlapping nodes. The returned id owns this commitment;
// lifecycle callbacks must present it before mutating or releasing the pin.
func (p *Pins) ChooseInTopologyOnNodes(group, topologyKey string, rawFree topology.FreeByDomain, nodesByDomain map[string][]string, gangSize int) (domain string, id uint64, ok bool) {
	return p.ChooseForOwnerInTopologyOnNodes(group, "", topologyKey, rawFree, nodesByDomain, gangSize)
}

// ChooseForOwnerInTopologyOnNodes binds the commitment to an external owner
// identity. GangPack uses the PodGroup UID so a delete/recreate with the same
// namespace/name cannot inherit the old object's placement decision.
func (p *Pins) ChooseForOwnerInTopologyOnNodes(group, owner, topologyKey string, rawFree topology.FreeByDomain, nodesByDomain map[string][]string, gangSize int) (domain string, id uint64, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if c, pinned := p.byGroup[group]; pinned {
		if c.owner != owner {
			return "", 0, false
		}
		return c.domain.Name, c.id, true
	}

	eff := p.withoutReservedDomainsLocked(topologyKey, rawFree, nodesByDomain)

	d, fit := topology.BestFit(eff, gangSize)
	if !fit {
		return "", 0, false
	}
	dom := Domain{TopologyKey: topologyKey, Name: d}
	c := &commitment{domain: dom, owner: owner, id: p.nextCommitmentID(), nodes: nodeSet(nodesByDomain[d])}
	p.byGroup[group] = c
	p.setRemaining(c, gangSize)
	return d, c.id, true
}

// WithoutReservedDomains returns a snapshot of rawFree with domains claimed by
// another forming gang set to zero. GangPack uses it while deferring a hard
// topology-spread gang's final domain choice until after the framework filters
// run. ChooseForOwnerInTopologyOnNodes repeats the same check atomically when
// Reserve commits the selected domain.
func (p *Pins) WithoutReservedDomains(topologyKey string, rawFree topology.FreeByDomain, nodesByDomain map[string][]string) topology.FreeByDomain {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.withoutReservedDomainsLocked(topologyKey, rawFree, nodesByDomain)
}

func (p *Pins) withoutReservedDomainsLocked(topologyKey string, rawFree topology.FreeByDomain, nodesByDomain map[string][]string) topology.FreeByDomain {
	// A forming gang exclusively owns its domain. Integer slot subtraction is not
	// sound for heterogeneous pods: a reservation for a large or selector-bound
	// member cannot be replaced by an arbitrary smaller slot. Hold the domain until
	// the reservation drains into real snapshot occupancy.
	eff := make(topology.FreeByDomain, len(rawFree))
	for d, f := range rawFree {
		blocked := p.reserved[Domain{TopologyKey: topologyKey, Name: d}] > 0
		for _, node := range nodesByDomain[d] {
			if p.reservedNodes[node] > 0 {
				blocked = true
				break
			}
		}
		if blocked {
			eff[d] = 0
		} else {
			eff[d] = f
		}
	}
	return eff
}

// Place records that one of a group's members has landed on a real node in its
// pinned domain, shrinking the reservation by one whole node — the member is now
// counted by the live snapshot instead, so draining the reservation in step keeps
// the two from double-counting the same node. No-op for an unpinned group or a
// reservation already at zero.
func (p *Pins) Place(group string) {
	p.PlaceIf(group, 0)
}

// PlaceIf drains one reservation only when id owns the current commitment.
// id==0 is retained for the package's compatibility helpers and tests. drained
// reports that the last outstanding claim became real snapshot occupancy.
func (p *Pins) PlaceIf(group string, id uint64) (placed, drained bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.byGroup[group]; ok && (id == 0 || c.id == id) && c.remaining > 0 {
		p.setRemaining(c, c.remaining-1)
		return true, c.remaining == 0
	}
	return false, false
}

// Len returns the number of groups currently pinned (with an outstanding
// commitment) — used to surface a gauge of active placements.
func (p *Pins) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.byGroup)
}

// Domains returns a snapshot of every pinned group's domain. It lets a caller
// compute, against the live node snapshot, how much each group should still have
// reserved — the input to Reconcile — without holding the store's lock across that
// (potentially O(nodes)) scan.
func (p *Pins) Domains() map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]string, len(p.byGroup))
	for group, c := range p.byGroup {
		out[group] = c.domain.Name
	}
	return out
}

// PinnedDomains returns domain names together with their topology keys.
func (p *Pins) PinnedDomains() map[string]Domain {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]Domain, len(p.byGroup))
	for group, c := range p.byGroup {
		out[group] = c.domain
	}
	return out
}

// Get returns the pinned domain for a group, or ("", false) if unpinned.
func (p *Pins) Get(group string) (domain string, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.byGroup[group]; ok {
		return c.domain.Name, true
	}
	return "", false
}

// GetOwned returns the current commitment metadata needed by a scheduling
// cycle. The id prevents callbacks from an older cycle from mutating a newer
// retry's commitment.
func (p *Pins) GetOwned(group string) (domain Domain, id uint64, remaining int, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.byGroup[group]
	if !ok {
		return Domain{}, 0, 0, false
	}
	return c.domain, c.id, c.remaining, true
}

// GetOwnedBy returns a commitment and whether it belongs to owner. The two
// booleans distinguish an absent pin from a stale same-name owner's pin.
func (p *Pins) GetOwnedBy(group, owner string) (domain Domain, id uint64, remaining int, found, ownerMatch bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, found := p.byGroup[group]
	if !found {
		return Domain{}, 0, 0, false, false
	}
	return c.domain, c.id, c.remaining, true, c.owner == owner
}

// Committed reports whether a group is pinned with no outstanding reservation
// (remaining==0): every reserved slot has been placed, or the pin was rebuilt
// from already-bound members on failover. Either way its members are (being)
// bound, so the gate must also count bound siblings, not just waiting ones. A
// still-forming gang has remaining>0, so this stays false and the gate never
// pays the bound-sibling scan while a gang is filling normally.
func (p *Pins) Committed(group string) bool {
	return p.CommittedIf(group, 0)
}

// CommittedIf reports whether the owned attempt has converted every reserved
// slot into an assumed or bound pod. Permit uses this instead of combining the
// scheduler snapshot with waiting pods, because waiting pods are already assumed
// in that snapshot and would otherwise be counted twice.
func (p *Pins) CommittedIf(group string, id uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.byGroup[group]
	return ok && (id == 0 || c.id == id) && c.remaining == 0
}

// Set pins a group to a domain without best-fit. Used on leader failover to
// rebuild the store from where bound pods already sit — the domain is simply
// where those pods are. Those pods are real occupancy in the snapshot, so the
// rebuilt commitment holds no outstanding reservation.
func (p *Pins) Set(group, domain string) {
	p.SetReservation(group, "", domain, 0)
}

// SetReservation rebuilds a pin and its still-outstanding reservation.
func (p *Pins) SetReservation(group, topologyKey, domain string, remaining int) {
	p.SetReservationOnNodes(group, topologyKey, domain, remaining, nil)
}

// SetReservationOnNodes rebuilds a commitment from bound members and returns
// its ownership id. Physical domain membership is recorded for cross-topology
// exclusion while the remaining reservation is non-zero.
func (p *Pins) SetReservationOnNodes(group, topologyKey, domain string, remaining int, nodes []string) uint64 {
	return p.SetReservationForOwnerOnNodes(group, "", topologyKey, domain, remaining, nodes)
}

// SetReservationForOwnerOnNodes rebuilds an owner-bound commitment from members
// already present in the snapshot.
func (p *Pins) SetReservationForOwnerOnNodes(group, owner, topologyKey, domain string, remaining int, nodes []string) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if remaining < 0 {
		remaining = 0
	}
	// Drop any prior commitment's reservation before overwriting.
	if old, ok := p.byGroup[group]; ok {
		p.setRemaining(old, 0)
	}
	dom := Domain{TopologyKey: topologyKey, Name: domain}
	c := &commitment{domain: dom, owner: owner, id: p.nextCommitmentID(), nodes: nodeSet(nodes)}
	p.byGroup[group] = c
	p.setRemaining(c, remaining)
	return c.id
}

// Owners returns the owner identity recorded for every live commitment.
func (p *Pins) Owners() map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]string, len(p.byGroup))
	for group, c := range p.byGroup {
		out[group] = c.owner
	}
	return out
}

// ReleaseIfOwner drops a commitment only when owner still identifies it.
func (p *Pins) ReleaseIfOwner(group, owner string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.byGroup[group]
	if !ok || c.owner != owner {
		return false
	}
	delete(p.byGroup, group)
	p.setRemaining(c, 0)
	return true
}

// Reservations returns a stable snapshot of outstanding claims. Callers use it
// to keep ordinary pods out of a domain while a gang is still forming.
func (p *Pins) Reservations() []Reservation {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Reservation, 0, len(p.byGroup))
	for group, c := range p.byGroup {
		if c.remaining > 0 {
			nodes := make([]string, 0, len(c.nodes))
			for node := range c.nodes {
				nodes = append(nodes, node)
			}
			out = append(out, Reservation{Group: group, Domain: c.domain, Remaining: c.remaining, Nodes: nodes})
		}
	}
	return out
}

// BlocksNode reports whether a forming gang has an outstanding claim on this
// physical node. Commitments created through compatibility helpers may not have
// physical membership; labels provide the conservative metadata fallback.
func (p *Pins) BlocksNode(name string, nodeLabels map[string]string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reservedNodes[name] > 0 {
		return true
	}
	for _, c := range p.byGroup {
		if c.remaining > 0 && len(c.nodes) == 0 && nodeLabels[c.domain.TopologyKey] == c.domain.Name {
			return true
		}
	}
	return false
}

// RetainOnly releases every pinned group NOT present in live, returning how many
// were released. It is the garbage collector's hook: a gang whose pods are all
// gone (completed and torn down, deleted, or abandoned before Reserve) leaves a
// pin no failure path reclaims, so byGroup would grow unbounded and a reused
// PodGroup name could inherit a stale domain. Reconciling against the set of
// gangs that still have live pods bounds byGroup to live gangs.
func (p *Pins) RetainOnly(live map[string]bool) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	released := 0
	for group, c := range p.byGroup {
		if !live[group] {
			delete(p.byGroup, group)
			p.setRemaining(c, 0)
			released++
		}
	}
	return released
}

// Reconcile resets each present group's reservation to the size it can still
// justify against ground truth, closing the reservation leak. A reservation is
// drained per-Place only when a member is assumed, but a group can pin a domain
// (reserving gangSize) and then fail its scheduling cycle before any member is
// assumed — Filter rejects every candidate, so no Reserve or Unreserve runs and
// the reservation is never returned. Such a reservation subtracts capacity forever,
// starving other groups.
//
// want maps a group to how many whole nodes it still legitimately needs reserved
// — gangSize minus the members already placed in its domain, computed from the
// live snapshot. Set on the periodic GC sweep, this bounds a stale reservation's
// lifetime to one interval instead of forever. A group absent from want is left
// untouched (its facts weren't resolvable this pass — reconciling on a partial
// view could wrongly reclaim a live reservation). Negative values clamp to zero.
func (p *Pins) Reconcile(want map[string]int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for group, c := range p.byGroup {
		r, ok := want[group]
		if !ok {
			continue
		}
		if r < 0 {
			r = 0
		}
		if r != c.remaining {
			p.setRemaining(c, r)
		}
	}
}

// Release drops a group's pin and its reservation — on gang completion, on
// gate-timeout unwind, or on teardown — so the next Choose re-runs best-fit and
// may pick a different domain. It reports whether a pin was actually present, so
// callers can act once per gang even though the unwind re-enters per member.
func (p *Pins) Release(group string) bool {
	return p.ReleaseIf(group, 0, nil)
}

// ReleaseIf drops a commitment only when id still owns it. The callback runs
// while the store is locked, closing the window in which a new retry could pin
// before old waiting siblings have been rejected. id==0 is the compatibility
// form used by package-level callers that do not carry lifecycle ownership.
func (p *Pins) ReleaseIf(group string, id uint64, beforeUnlock func()) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.byGroup[group]
	if !ok || (id != 0 && c.id != id) {
		return false
	}
	delete(p.byGroup, group)
	p.setRemaining(c, 0)
	if beforeUnlock != nil {
		beforeUnlock()
	}
	return true
}
