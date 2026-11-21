// Package preemption chooses which lower-priority work leaves one topology
// domain so that a waiting gang can start there. It is free of the scheduler
// framework: the caller lists each domain's candidate units and answers whether
// the gang fits once a given set of them is gone. The same inputs always give
// the same answer, so every eviction can be explained afterwards.
package preemption

import (
	"math"
	"sort"
)

// Unit is work that leaves as a whole: every pod of one gang, or one pod that
// belongs to no gang. A gang is never split.
type Unit struct {
	// Key names the unit and breaks every tie, so the order never depends on
	// the order the caller listed units in.
	Key string
	// Priority is the highest priority among the unit's pods.
	Priority int32
	// Pods is how many pods leave with the unit.
	Pods int
	// Accelerators is the sum of the extended resources the unit's pods
	// request, such as GPUs.
	Accelerators int64
}

// Domain is one topology domain and the units the gang may displace from it.
type Domain struct {
	Name  string
	Units []Unit
}

// Choice is the domain to free and the units that must leave it, most
// important first. A choice that evicts nothing means the domain fits once the
// pods already shutting down there are gone.
type Choice struct {
	Domain string
	Evict  []Unit
}

// FitsFunc reports whether the gang fits the domain once the listed units are
// gone, together with any pods already shutting down there.
type FitsFunc func(domain string, gone []Unit) bool

// Choose picks one domain to free and the fewest units that must leave it.
//
// In each domain it first checks that the gang fits with every candidate unit
// gone; a domain where it does not fit is skipped. It then adds units back, the
// most important first, and keeps each one the gang still fits beside. The
// units left over are the ones the domain evicts.
//
// Across domains it prefers the domain whose most important evicted unit ranks
// lowest. Ties go to fewer evicted units, then fewer accelerators, then fewer
// pods, then the domain name. ok is false when no domain can be freed.
func Choose(domains []Domain, fits FitsFunc) (Choice, bool) {
	var best Choice
	var bestCost cost
	found := false
	for _, domain := range domains {
		evict, ok := unitsToEvict(domain, fits)
		if !ok {
			continue
		}
		c := costOf(domain.Name, evict)
		if !found || c.less(bestCost) {
			best, bestCost, found = Choice{Domain: domain.Name, Evict: evict}, c, true
		}
	}
	return best, found
}

// unitsToEvict returns the units that must leave the domain for the gang to fit,
// most important first, and false when the gang does not fit even with every
// candidate gone.
func unitsToEvict(domain Domain, fits FitsFunc) ([]Unit, bool) {
	gone := append([]Unit(nil), domain.Units...)
	sort.Slice(gone, func(i, j int) bool { return moreImportant(gone[i], gone[j]) })
	if !fits(domain.Name, gone) {
		return nil, false
	}
	for i := 0; i < len(gone); {
		without := make([]Unit, 0, len(gone)-1)
		without = append(without, gone[:i]...)
		without = append(without, gone[i+1:]...)
		if fits(domain.Name, without) {
			gone = without
			continue
		}
		i++
	}
	return gone, true
}

// moreImportant orders the units a domain keeps first: higher priority, then
// more accelerators, so the larger of two equal units stays and fewer
// accelerators are displaced, then the key.
func moreImportant(a, b Unit) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if a.Accelerators != b.Accelerators {
		return a.Accelerators > b.Accelerators
	}
	return a.Key < b.Key
}

// cost is what freeing one domain displaces, compared field by field in
// order.
type cost struct {
	topPriority  int64
	units        int
	accelerators int64
	pods         int
	domain       string
}

func costOf(domain string, evict []Unit) cost {
	c := cost{topPriority: math.MinInt64, units: len(evict), domain: domain}
	for _, v := range evict {
		c.topPriority = max(c.topPriority, int64(v.Priority))
		c.accelerators += v.Accelerators
		c.pods += v.Pods
	}
	return c
}

func (c cost) less(o cost) bool {
	switch {
	case c.topPriority != o.topPriority:
		return c.topPriority < o.topPriority
	case c.units != o.units:
		return c.units < o.units
	case c.accelerators != o.accelerators:
		return c.accelerators < o.accelerators
	case c.pods != o.pods:
		return c.pods < o.pods
	default:
		return c.domain < o.domain
	}
}
