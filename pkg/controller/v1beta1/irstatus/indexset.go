package irstatus

import (
	"cmp"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
)

type indexInterval struct {
	first int32
	last  int32
}

// indexSet stores sorted, disjoint, nonadjacent inclusive intervals.
type indexSet struct {
	intervals   []indexInterval
	cardinality uint64
}

func parseIndexSet(raw string, maxCardinality uint64) (indexSet, error) {
	cardinality, termCount, err := scanIndexSet(raw, maxCardinality, nil)
	if err != nil {
		return indexSet{}, err
	}

	set := indexSet{
		intervals:   make([]indexInterval, 0, termCount),
		cardinality: cardinality,
	}
	_, _, err = scanIndexSet(raw, maxCardinality, func(interval indexInterval) {
		set.intervals = append(set.intervals, interval)
	})
	if err != nil {
		return indexSet{}, err
	}
	return set, nil
}

// scanIndexSet walks the terms of raw, "0-7,9,12-14", checks that they are
// canonical and bounded by maxCardinality, and reports the cardinality and
// the term count. visit, when set, receives every interval in term order.
func scanIndexSet(raw string, maxCardinality uint64, visit func(indexInterval)) (uint64, int, error) {
	if raw == "" {
		return 0, 0, newCodecError(ErrorReasonRangeSyntax)
	}
	if maxCardinality == 0 {
		return 0, 0, newCodecError(ErrorReasonCardinalityLimit)
	}

	var scan indexSetScan
	for rest, last := raw, false; !last; {
		var term string
		var err error
		if term, rest, last, err = nextTerm(rest); err != nil {
			return 0, 0, err
		}
		interval, err := parseIndexInterval(term)
		if err != nil {
			return 0, 0, err
		}
		if err := scan.accept(interval, maxCardinality); err != nil {
			return 0, 0, err
		}
		if visit != nil {
			visit(interval)
		}
	}
	return scan.cardinality, scan.terms, nil
}

// nextTerm splits the first comma-separated term off rest and reports
// whether it was the last one. An empty term, which a leading, doubled or
// trailing comma leaves, is a syntax error.
func nextTerm(rest string) (term, remainder string, last bool, err error) {
	comma := strings.IndexByte(rest, ',')
	if comma < 0 {
		term, last = rest, true
	} else {
		term, remainder = rest[:comma], rest[comma+1:]
	}
	if term == "" {
		return "", "", false, newCodecError(ErrorReasonRangeSyntax)
	}
	return term, remainder, last, nil
}

// indexSetScan is the running state of one scan: the terms accepted so far,
// their cardinality, and the last interval the next one must follow.
type indexSetScan struct {
	cardinality uint64
	terms       int
	previous    indexInterval
	hasPrevious bool
}

// accept checks interval against the previous one and the bound, then
// counts it. The interval must start after the previous one ends and not
// right after it, since adjacent intervals are one term in canonical form,
// and the running cardinality may not pass maxCardinality.
func (s *indexSetScan) accept(interval indexInterval, maxCardinality uint64) error {
	if s.hasPrevious {
		switch {
		case interval.first <= s.previous.last:
			return newCodecError(ErrorReasonRangeOrder)
		case s.previous.last != math.MaxInt32 && interval.first == s.previous.last+1:
			return newCodecError(ErrorReasonCanonicalOrder)
		}
	}
	width := uint64(interval.last) - uint64(interval.first) + 1
	if s.cardinality+width > maxCardinality {
		return newCodecError(ErrorReasonCardinalityLimit)
	}
	s.cardinality += width
	s.terms++
	s.previous, s.hasPrevious = interval, true
	return nil
}

func parseIndexInterval(term string) (indexInterval, error) {
	dash := strings.IndexByte(term, '-')
	if dash < 0 {
		value, err := parseIndex(term)
		if err != nil {
			return indexInterval{}, err
		}
		return indexInterval{first: value, last: value}, nil
	}
	if dash == 0 || dash == len(term)-1 || strings.IndexByte(term[dash+1:], '-') >= 0 {
		return indexInterval{}, newCodecError(ErrorReasonRangeSyntax)
	}

	first, err := parseIndex(term[:dash])
	if err != nil {
		return indexInterval{}, err
	}
	last, err := parseIndex(term[dash+1:])
	if err != nil {
		return indexInterval{}, err
	}
	if first >= last {
		return indexInterval{}, newCodecError(ErrorReasonRangeOrder)
	}
	return indexInterval{first: first, last: last}, nil
}

func parseIndex(raw string) (int32, error) {
	if raw == "" || (len(raw) > 1 && raw[0] == '0') {
		return 0, newCodecError(ErrorReasonRangeSyntax)
	}

	var value uint64
	for i := 0; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return 0, newCodecError(ErrorReasonRangeSyntax)
		}
		digit := uint64(raw[i] - '0')
		if value > (uint64(math.MaxInt32)-digit)/10 {
			return 0, newCodecError(ErrorReasonRangeOverflow)
		}
		value = value*10 + digit
	}
	return int32(value), nil
}

func indexSetFromIndices(indices []int32, maxCardinality uint64) (indexSet, error) {
	if len(indices) == 0 {
		return indexSet{}, newCodecError(ErrorReasonValueDomain)
	}
	if maxCardinality == 0 || uint64(len(indices)) > maxCardinality {
		return indexSet{}, newCodecError(ErrorReasonCardinalityLimit)
	}

	for _, index := range indices {
		if index < 0 {
			return indexSet{}, newCodecError(ErrorReasonValueDomain)
		}
	}
	ordered := append([]int32(nil), indices...)
	slices.Sort(ordered)

	set := indexSet{cardinality: uint64(len(ordered))}
	for _, index := range ordered {
		if len(set.intervals) == 0 {
			set.intervals = append(set.intervals, indexInterval{first: index, last: index})
			continue
		}
		last := &set.intervals[len(set.intervals)-1]
		switch {
		case index == last.last:
			return indexSet{}, newCodecError(ErrorReasonValueDomain)
		case last.last != math.MaxInt32 && index == last.last+1:
			last.last = index
		default:
			set.intervals = append(set.intervals, indexInterval{first: index, last: index})
		}
	}
	return set, nil
}

func encodeIndexSet(indices []int32, maxCardinality uint64) (string, error) {
	set, err := indexSetFromIndices(indices, maxCardinality)
	if err != nil {
		return "", err
	}
	return set.String(), nil
}

func (s indexSet) Cardinality() uint64 {
	return s.cardinality
}

func (s indexSet) Contains(index int32) bool {
	if index < 0 {
		return false
	}
	position := sort.Search(len(s.intervals), func(i int) bool {
		return s.intervals[i].last >= index
	})
	return position < len(s.intervals) && s.intervals[position].first <= index
}

func (s indexSet) IsSubsetOf(other indexSet) bool {
	if s.cardinality > other.cardinality {
		return false
	}
	otherPosition := 0
	for _, interval := range s.intervals {
		for otherPosition < len(other.intervals) && other.intervals[otherPosition].last < interval.first {
			otherPosition++
		}
		if otherPosition == len(other.intervals) ||
			other.intervals[otherPosition].first > interval.first ||
			other.intervals[otherPosition].last < interval.last {
			return false
		}
	}
	return true
}

// prefixOffsets returns, per interval, how many indices precede it in the
// ascending enumeration of s; rankOf uses it to map an index to a position.
func (s indexSet) prefixOffsets() []uint64 {
	offsets := make([]uint64, len(s.intervals))
	var total uint64
	for i, interval := range s.intervals {
		offsets[i] = total
		total += uint64(interval.last) - uint64(interval.first) + 1
	}
	return offsets
}

// rankOf returns the zero-based position of index in the ascending
// enumeration of s, or false when index is not a member.
func (s indexSet) rankOf(index int32, offsets []uint64) (uint64, bool) {
	if index < 0 {
		return 0, false
	}
	position := sort.Search(len(s.intervals), func(i int) bool {
		return s.intervals[i].last >= index
	})
	if position == len(s.intervals) || s.intervals[position].first > index {
		return 0, false
	}
	return offsets[position] + uint64(index-s.intervals[position].first), true
}

// disjointIndexSets reports whether no index belongs to two of the sets. It
// sorts the pooled intervals once instead of comparing sets pairwise.
func disjointIndexSets(sets []indexSet) bool {
	if len(sets) < 2 {
		return true
	}
	total := 0
	for _, set := range sets {
		total += len(set.intervals)
	}
	pooled := make([]indexInterval, 0, total)
	for _, set := range sets {
		pooled = append(pooled, set.intervals...)
	}
	slices.SortFunc(pooled, func(a, b indexInterval) int { return cmp.Compare(a.first, b.first) })
	for i := 1; i < len(pooled); i++ {
		if pooled[i].first <= pooled[i-1].last {
			return false
		}
	}
	return true
}

func (s indexSet) forEach(visit func(int32)) {
	for _, interval := range s.intervals {
		for value := interval.first; ; value++ {
			visit(value)
			if value == interval.last {
				break
			}
		}
	}
}

// String spells the set as the terms the wire carries, "0-7,9,12-14". Each
// number is written into the builder's own buffer, so a set of many short
// terms costs a few allocations rather than one per number.
func (s indexSet) String() string {
	var result strings.Builder
	result.Grow(12 * len(s.intervals))
	var digits [11]byte
	for i, interval := range s.intervals {
		if i > 0 {
			result.WriteByte(',')
		}
		result.Write(strconv.AppendInt(digits[:0], int64(interval.first), 10))
		if interval.first != interval.last {
			result.WriteByte('-')
			result.Write(strconv.AppendInt(digits[:0], int64(interval.last), 10))
		}
	}
	return result.String()
}
