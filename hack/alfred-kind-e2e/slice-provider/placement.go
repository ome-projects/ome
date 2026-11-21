package main

import (
	"sort"
	"strconv"
	"strings"
)

// host is one TPU node as the stand-in provider sees it.
type host struct {
	name string
	// ids holds the node's partition id per shape, for example
	// {"2x2x1": "p0", "2x2x4": "q0"}.
	ids map[string]string
	// healthy holds, per shape, whether the node's partition of that shape
	// reports the healthy state.
	healthy map[string]bool
	// free: no slice holds the node and it is not cordoned. GKE copies a
	// node's cordon taint onto every partition that contains it, and a slice
	// scheduler that filters on taints never places a slice there.
	free bool
}

// chips returns the chip count of a shape such as "2x2x4", 0 when malformed.
func chips(shape string) int {
	dims := strings.Split(shape, "x")
	if len(dims) != 3 {
		return 0
	}
	n := 1
	for _, d := range dims {
		v, err := strconv.Atoi(d)
		if err != nil || v <= 0 {
			return 0
		}
		n *= v
	}
	return n
}

// choose returns the partition of shape a new slice is placed on, and its
// hosts, using the slice scheduler's best fit: among wholly free partitions of
// the shape, the one whose largest wholly free containing partition is
// smallest, ties to the lowest id. ok is false when no partition of the shape
// is wholly free.
func choose(hosts []host, shape string) (id string, members []string, ok bool) {
	parts := map[string]map[string][]string{}
	shapes := map[string]bool{}
	for _, h := range hosts {
		for s, pid := range h.ids {
			if parts[s] == nil {
				parts[s] = map[string][]string{}
			}
			parts[s][pid] = append(parts[s][pid], h.name)
			shapes[s] = true
		}
	}
	size := map[string]int{}
	for s, byID := range parts {
		for _, m := range byID {
			if len(m) > size[s] {
				size[s] = len(m)
			}
		}
	}
	byName := map[string]host{}
	for _, h := range hosts {
		byName[h.name] = h
	}
	wholeFree := func(s, pid string) bool {
		m := parts[s][pid]
		if len(m) == 0 || len(m) != size[s] {
			return false
		}
		for _, n := range m {
			if h := byName[n]; !h.free || !h.healthy[s] {
				return false
			}
		}
		return true
	}
	var larger []string
	for s := range shapes {
		if chips(s) > chips(shape) {
			larger = append(larger, s)
		}
	}
	sort.Slice(larger, func(i, j int) bool { return chips(larger[i]) < chips(larger[j]) })

	best, bestScore := "", 0
	ids := make([]string, 0, len(parts[shape]))
	for pid := range parts[shape] {
		ids = append(ids, pid)
	}
	sort.Strings(ids)
	for _, pid := range ids {
		if !wholeFree(shape, pid) {
			continue
		}
		score := chips(shape)
		first := byName[parts[shape][pid][0]]
		for _, s := range larger {
			if !wholeFree(s, first.ids[s]) {
				break
			}
			score = chips(s)
		}
		if best == "" || score < bestScore {
			best, bestScore = pid, score
		}
	}
	if best == "" {
		return "", nil, false
	}
	members = append([]string(nil), parts[shape][best]...)
	sort.Strings(members)
	return best, members, true
}
