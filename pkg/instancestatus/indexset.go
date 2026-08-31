package instancestatus

import (
	"math"
	"strconv"
	"strings"
)

type indexSet struct {
	ordered []int32
	lookup  map[int32]struct{}
}

func parseIndexSet(raw string, maxCardinality int) (indexSet, error) {
	if maxCardinality <= 0 || maxCardinality > DefaultMaxRows {
		return indexSet{}, newDecodeError(ErrorReasonCardinality)
	}
	if raw == "" {
		return indexSet{}, newDecodeError(ErrorReasonIndexSyntax)
	}
	// A canonical set needs at most ten digits plus one separator per member.
	// Reject a larger raw field before token scanning so adversarial comma-heavy
	// input cannot allocate in proportion to untrusted bytes.
	if len(raw) > maxCardinality*11 {
		return indexSet{}, newDecodeError(ErrorReasonCardinality)
	}

	cardinality, err := scanIndexSet(raw, maxCardinality, nil)
	if err != nil {
		return indexSet{}, err
	}

	set := indexSet{
		ordered: make([]int32, 0, int(cardinality)),
		lookup:  make(map[int32]struct{}, int(cardinality)),
	}
	_, err = scanIndexSet(raw, maxCardinality, func(first, last int32) {
		for value := first; ; value++ {
			set.ordered = append(set.ordered, value)
			set.lookup[value] = struct{}{}
			if value == last {
				break
			}
		}
	})
	if err != nil {
		return indexSet{}, err
	}
	return set, nil
}

func scanIndexSet(raw string, maxCardinality int, visit func(first, last int32)) (uint64, error) {
	var cardinality uint64
	previousLast := int32(-2)
	for termStart := 0; termStart < len(raw); {
		relativeEnd := strings.IndexByte(raw[termStart:], ',')
		termEnd := len(raw)
		if relativeEnd >= 0 {
			termEnd = termStart + relativeEnd
		}
		if termEnd == termStart {
			return 0, newDecodeError(ErrorReasonIndexSyntax)
		}
		first, last, err := parseInterval(raw[termStart:termEnd])
		if err != nil {
			return 0, err
		}
		if first <= previousLast ||
			(previousLast != math.MaxInt32 && first == previousLast+1) {
			return 0, newDecodeError(ErrorReasonIndexCanonical)
		}
		cardinality += uint64(last) - uint64(first) + 1
		if cardinality > uint64(maxCardinality) {
			return 0, newDecodeError(ErrorReasonCardinality)
		}
		if visit != nil {
			visit(first, last)
		}
		previousLast = last
		if termEnd == len(raw) {
			break
		}
		termStart = termEnd + 1
		if termStart == len(raw) {
			return 0, newDecodeError(ErrorReasonIndexSyntax)
		}
	}
	return cardinality, nil
}

func parseInterval(term string) (int32, int32, error) {
	if term == "" {
		return 0, 0, newDecodeError(ErrorReasonIndexSyntax)
	}
	dash := strings.IndexByte(term, '-')
	if dash < 0 {
		value, err := parseIndex(term)
		return value, value, err
	}
	if dash == 0 || dash == len(term)-1 || strings.IndexByte(term[dash+1:], '-') >= 0 {
		return 0, 0, newDecodeError(ErrorReasonIndexSyntax)
	}
	first, err := parseIndex(term[:dash])
	if err != nil {
		return 0, 0, err
	}
	last, err := parseIndex(term[dash+1:])
	if err != nil {
		return 0, 0, err
	}
	if first >= last {
		return 0, 0, newDecodeError(ErrorReasonIndexCanonical)
	}
	return first, last, nil
}

func parseIndex(raw string) (int32, error) {
	if raw == "" || (len(raw) > 1 && raw[0] == '0') {
		return 0, newDecodeError(ErrorReasonIndexSyntax)
	}
	for _, char := range raw {
		if char < '0' || char > '9' {
			return 0, newDecodeError(ErrorReasonIndexSyntax)
		}
	}
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || value < 0 {
		return 0, newDecodeError(ErrorReasonIndexSyntax)
	}
	return int32(value), nil
}

func subsetOf(candidate, members indexSet) bool {
	for _, index := range candidate.ordered {
		if _, ok := members.lookup[index]; !ok {
			return false
		}
	}
	return true
}
