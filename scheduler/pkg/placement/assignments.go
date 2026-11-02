// Package placement holds ome-scheduler's stateful placement decisions layered
// on the pure accounting in package topology. Its first piece is the domain
// assignment store: the domain each placement group's gang is committed to, and
// how much whole-node capacity each has reserved there but not yet filled.
package placement

import (
	"sync"

	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

// DomainAssignments records the domain each gang is committed to. The first pod
// chooses a domain via best-fit and every later member follows the assignment.
// Partner-gang placement groups are not represented until their reservation
// semantics are defined.
//
// An assignment also carries a capacity reservation: the whole nodes the gang
// has committed to its domain but not yet filled. A gang's members arrive one
// scheduling cycle at a time, so between choosing a domain and completing there
// is a window where the gang's future members' capacity is unclaimed; without a
// reservation a second gang can best-fit into it and strand the first (which
// then times out and re-plans). The reservation closes that window; it drains
// to zero via Place as members become real occupancy, so the live snapshot's
// own accounting is never double-counted.
//
// Every write after Choose or Set names the commitment it acts on by the id
// those two returned, so a callback from an older scheduling attempt cannot
// drain or release a newer attempt's commitment.
//
// The scheduler runs scheduling cycles single-threaded, but plugin state is also
// touched from event handlers (failover rebuild, release on gang teardown), so
// the store guards itself with a mutex.
type DomainAssignments struct {
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

// Gang names the placement group a commitment belongs to: the group key, the
// owner identity behind it (GangPack passes the PodGroup UID, so a delete and
// recreate under the same name inherits no decision), and the topology key its
// domains are read under.
type Gang struct {
	Group       string
	Owner       string
	TopologyKey string
}

// commitment is a group's placement decision: the domain it is committed to plus
// the count of whole nodes still reserved there for members not yet placed.
// remaining==0 means the gang is fully committed — every reserved slot has been
// placed (or the assignment was rebuilt from already-bound members on failover)
// — which is the signal the gate uses to know it must also count bound
// siblings, not just waiting ones.
type commitment struct {
	domain    Domain
	owner     string
	remaining int
	id        uint64
	nodes     map[string]struct{}
	// soleFit records that the domain was the only choice when the commitment
	// was made: no other domain free of reservations had room for the gang, or
	// bound members imposed it. A member vetoed there has nowhere else to go.
	soleFit bool
}

// New returns an empty assignment store.
func New() *DomainAssignments {
	return &DomainAssignments{
		byGroup:       make(map[string]*commitment),
		reserved:      make(map[Domain]int),
		reservedNodes: make(map[string]int),
	}
}

func (p *DomainAssignments) nextCommitmentID() uint64 {
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
func (p *DomainAssignments) addReservation(domain Domain, delta int) {
	p.reserved[domain] += delta
	if p.reserved[domain] <= 0 {
		delete(p.reserved, domain)
	}
}

// setRemaining updates both logical-domain and physical-node reservation
// indexes. Caller holds p.mu.
func (p *DomainAssignments) setRemaining(c *commitment, remaining int) {
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

// Choose returns the domain the gang is committed to and the id that owns the
// commitment. A gang that already holds a commitment under the same owner gets
// its domain back as-is, regardless of the current free counts, because members
// are already reserved there; a gang that can no longer complete there is
// released by the caller and chosen again. A commitment held under another
// owner answers nothing. A gang without a commitment is best-fitted over the
// domains without an outstanding reservation, by reservation metadata and by
// the physical nodes nodesByDomain lists, so heterogeneous claims are never
// treated as fungible slots and two topology keys cannot reserve overlapping
// nodes; the chosen domain becomes the commitment and size whole nodes are
// reserved there. ok is false only when the gang holds no commitment and no
// domain currently fits it (the gang waits, unassigned).
func (p *DomainAssignments) Choose(gang Gang, free topology.FreeByDomain, nodesByDomain map[string][]string, size int) (domain string, id uint64, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if c, assigned := p.byGroup[gang.Group]; assigned {
		if c.owner != gang.Owner {
			return "", 0, false
		}
		return c.domain.Name, c.id, true
	}

	// A forming gang exclusively owns its domain. Integer slot subtraction is not
	// sound for heterogeneous pods: a reservation for a large or selector-bound
	// member cannot be replaced by an arbitrary smaller slot. Hold the domain until
	// the reservation drains into real snapshot occupancy.
	eff := make(topology.FreeByDomain, len(free))
	for d, f := range free {
		blocked := p.reserved[Domain{TopologyKey: gang.TopologyKey, Name: d}] > 0
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

	d, fit := topology.BestFit(eff, size)
	if !fit {
		return "", 0, false
	}
	dom := Domain{TopologyKey: gang.TopologyKey, Name: d}
	c := &commitment{domain: dom, owner: gang.Owner, id: p.nextCommitmentID(), nodes: nodeSet(nodesByDomain[d]),
		soleFit: fittingDomains(eff, size) < 2}
	p.byGroup[gang.Group] = c
	p.setRemaining(c, size)
	return d, c.id, true
}

// fittingDomains counts the domains with room for the whole gang.
func fittingDomains(free topology.FreeByDomain, gangSize int) int {
	n := 0
	for _, room := range free {
		if room >= gangSize {
			n++
		}
	}
	return n
}

// Set rebuilds a commitment from members already present in the snapshot and
// returns the id that owns it. Used on leader failover: domain is where the
// gang's bound pods sit, remaining is the whole nodes still owed to members not
// yet placed, and nodes is the domain's physical membership, recorded so another
// topology key cannot reserve the same nodes while remaining is above zero. A
// commitment the group held before gives up its reservation first. A rebuilt
// commitment reports no alternative domain.
func (p *DomainAssignments) Set(gang Gang, domain string, remaining int, nodes []string) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if remaining < 0 {
		remaining = 0
	}
	if old, ok := p.byGroup[gang.Group]; ok {
		p.setRemaining(old, 0)
	}
	dom := Domain{TopologyKey: gang.TopologyKey, Name: domain}
	c := &commitment{domain: dom, owner: gang.Owner, id: p.nextCommitmentID(), nodes: nodeSet(nodes), soleFit: true}
	p.byGroup[gang.Group] = c
	p.setRemaining(c, remaining)
	return c.id
}

// Place records that one member of the commitment id owns has landed on a real
// node in its domain and shrinks the reservation by one whole node: the member
// is now counted by the live snapshot, so draining in step keeps the two from
// double-counting the same node. placed is false when id no longer owns the
// group's commitment or the reservation is already at zero; drained reports
// that the last outstanding claim became real snapshot occupancy.
func (p *DomainAssignments) Place(group string, id uint64) (placed, drained bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.byGroup[group]; ok && c.id == id && c.remaining > 0 {
		p.setRemaining(c, c.remaining-1)
		return true, c.remaining == 0
	}
	return false, false
}

// Release drops the commitment id owns together with its reservation, on gang
// completion, on gate-timeout unwind or on teardown, so the next Choose runs
// best-fit again and may pick a different domain. It reports whether the
// commitment was present, so callers act once per gang even though the unwind
// re-enters per member. beforeUnlock runs while the store is locked, which
// closes the window in which a new retry could take a commitment before the
// old attempt's waiting siblings have been rejected.
func (p *DomainAssignments) Release(group string, id uint64, beforeUnlock func()) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.byGroup[group]
	if !ok || c.id != id {
		return false
	}
	delete(p.byGroup, group)
	p.setRemaining(c, 0)
	if beforeUnlock != nil {
		beforeUnlock()
	}
	return true
}

// Committed reports whether the commitment id owns has converted every reserved
// slot into an assumed or bound pod: every slot was placed, or the commitment
// was rebuilt from already-bound members on failover. Either way its members
// are being bound, so the gate must also count bound siblings, not just waiting
// ones. Permit reads this instead of combining the scheduler snapshot with the
// waiting pods, which are already assumed in that snapshot and would be counted
// twice. A still-forming gang has a reservation left, so this stays false and
// the gate never pays the bound-sibling scan while a gang is filling normally.
func (p *DomainAssignments) Committed(group string, id uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.byGroup[group]
	return ok && c.id == id && c.remaining == 0
}

// SoleFit reports whether the commitment id owns had no alternative when it
// was made: no other domain free of reservations had room for the gang, or
// bound members imposed the domain. Every member following the assignment
// shares the answer, so a veto in the domain is retried at once only when
// another domain could take the gang.
func (p *DomainAssignments) SoleFit(group string, id uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.byGroup[group]
	return ok && c.id == id && c.soleFit
}

// GetOwned returns the current commitment metadata needed by a scheduling
// cycle. The id prevents callbacks from an older cycle from mutating a newer
// retry's commitment.
func (p *DomainAssignments) GetOwned(group string) (domain Domain, id uint64, remaining int, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.byGroup[group]
	if !ok {
		return Domain{}, 0, 0, false
	}
	return c.domain, c.id, c.remaining, true
}

// GetOwnedBy returns a commitment and whether it belongs to owner. The two
// booleans distinguish an absent assignment from a stale same-name owner's
// assignment.
func (p *DomainAssignments) GetOwnedBy(group, owner string) (domain Domain, id uint64, remaining int, found, ownerMatch bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, found := p.byGroup[group]
	if !found {
		return Domain{}, 0, 0, false, false
	}
	return c.domain, c.id, c.remaining, true, c.owner == owner
}

// Owners returns the owner identity recorded for every live commitment.
func (p *DomainAssignments) Owners() map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]string, len(p.byGroup))
	for group, c := range p.byGroup {
		out[group] = c.owner
	}
	return out
}

// Len returns the number of groups that currently hold an assignment; the
// plugin exports it as a gauge of active placements.
func (p *DomainAssignments) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.byGroup)
}

// BlocksNode reports whether a forming gang has an outstanding claim on this
// physical node. A commitment recorded without its node set is matched through
// the node's label for the commitment's topology key instead.
func (p *DomainAssignments) BlocksNode(name string, nodeLabels map[string]string) bool {
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
