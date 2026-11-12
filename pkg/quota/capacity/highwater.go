package capacity

import (
	"sort"

	"gopkg.in/inf.v0"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Track folds a fresh observation into a running high-water mark.
//
// Budget checks compare against the mark rather than the instantaneous value,
// because capacity dips for reasons that say nothing about how much hardware a
// tenant is entitled to: a drain, a rolling reboot, a device plugin restarting.
// Following those down would mark budgets Degraded — and, once materialization
// exists, freeze them — every time a rack was patched.
//
// Growth is believed immediately: new hardware is usable the moment it reports.
// A drop is believed only once it exceeds bandPercent of the mark, which is
// what distinguishes a decommission from a reboot. It is deliberately a
// magnitude band and not a duration: a fleet that genuinely shrank should stop
// claiming capacity it no longer has, and waiting a timeout to say so only
// delays the same conclusion.
//
// bandPercent at or below zero disables damping, so the mark follows the
// observation exactly — absent config disables rather than assuming a band. At
// 100 or above it never lowers the mark, which is what that number reads as.
//
// `observed` is installed capacity when called from TrackAll; see there for why
// it is not the schedulable figure.
func Track(mark, observed resource.Quantity, bandPercent int32) resource.Quantity {
	if observed.Cmp(mark) >= 0 {
		return observed
	}
	if bandPercent <= 0 {
		return observed
	}
	if bandPercent >= 100 {
		// The reading an operator typing 100 intends: never lower the mark.
		// Treating it as "no damping" would invert the knob at its extreme,
		// which is the one place a mistake is least likely to be noticed.
		return mark
	}
	// Compare observed*100 against mark*(100-band) so the threshold needs no
	// division and no rounding decision.
	scaled := new(inf.Dec).Mul(observed.AsDec(), inf.NewDec(100, 0))
	threshold := new(inf.Dec).Mul(mark.AsDec(), inf.NewDec(int64(100-bandPercent), 0))
	if scaled.Cmp(threshold) < 0 {
		return observed
	}
	return mark
}

// TrackAll folds a fresh capacity reading into the marks carried in status.
//
// The mark tracks **installed** capacity — allocatable plus what is parked on
// cordoned or NotReady nodes — not the schedulable figure. Tracking allocatable
// would defeat the point: draining a quarter of a fleet is the archetypal dip
// the mark exists to absorb, and it moves allocatable far past any sane band, so
// the mark would follow it down and budgets would Degrade during exactly the
// maintenance the damping was for. A cordon leaves installed capacity untouched,
// which is the honest reading — the hardware is still in the rack.
//
// Entries are keyed by (resource, flavor), the granularity Kueue keys quota by.
// A pair that has disappeared from the observation keeps its mark and reports
// zero allocatable: hardware that stops reporting is the other case the mark
// exists for, and dropping the entry would lose the only record that the pair
// was ever larger.
func TrackAll(previous []Mark, observed []Capacity, bandPercent int32) []Mark {
	carried := carriedMarks(previous)
	out := make([]Mark, 0, len(carried.order)+len(observed))
	seen := map[resourceFlavor]struct{}{}
	for _, c := range observed {
		out = append(out, Mark{
			ResourceName:   c.ResourceName,
			ResourceFlavor: c.ResourceFlavor,
			Allocatable:    c.Allocatable,
			HighWaterMark:  Track(carried.marks[c.key()], installed(c), bandPercent),
		})
		seen[c.key()] = struct{}{}
	}
	out = append(out, carried.unobserved(seen)...)
	sort.Slice(out, func(i, j int) bool { return out[i].key().less(out[j].key()) })
	return out
}

// marksByPair is the marks carried in status, one per (resource, flavor)
// pair, in the order status listed them.
type marksByPair struct {
	marks map[resourceFlavor]resource.Quantity
	order []resourceFlavor
}

// carriedMarks reads the marks in status; a pair listed twice keeps its
// first mark.
func carriedMarks(previous []Mark) marksByPair {
	carried := marksByPair{marks: make(map[resourceFlavor]resource.Quantity, len(previous))}
	for _, p := range previous {
		if _, dup := carried.marks[p.key()]; dup {
			continue
		}
		carried.marks[p.key()] = p.HighWaterMark
		carried.order = append(carried.order, p.key())
	}
	return carried
}

// unobserved carries forward the marks of the pairs the observation did not
// report, so a pair that stops reporting still shows what it once held. A
// mark that has reached zero is dropped, or a renamed flavor would leave a
// dead entry in status for the life of the cluster.
func (m marksByPair) unobserved(seen map[resourceFlavor]struct{}) []Mark {
	var out []Mark
	for _, k := range m.order {
		mark := m.marks[k]
		if _, ok := seen[k]; ok || mark.IsZero() {
			continue
		}
		out = append(out, Mark{ResourceName: k.resource, ResourceFlavor: k.flavor, HighWaterMark: mark})
	}
	return out
}

// installed is what the pair physically has: schedulable now plus parked on a
// cordoned or NotReady node. Summing without mutating either input, because both
// belong to the caller's Capacity.
func installed(c Capacity) resource.Quantity {
	total := c.Allocatable.DeepCopy()
	total.Add(c.Unavailable)
	return total
}

// Mark is one (resource, flavor) pair's current and peak capacity.
type Mark struct {
	ResourceName   string
	ResourceFlavor string
	Allocatable    resource.Quantity
	HighWaterMark  resource.Quantity
}

// key is the pair the mark is kept for.
func (m Mark) key() resourceFlavor {
	return resourceFlavor{resource: m.ResourceName, flavor: m.ResourceFlavor}
}
