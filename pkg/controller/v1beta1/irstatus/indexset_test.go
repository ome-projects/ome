package irstatus

import (
	"math"
	"math/bits"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestParseIndexSetCanonicalInputs(t *testing.T) {
	t.Parallel()

	maxDomainCardinality := uint64(math.MaxInt32) + 1
	tests := []struct {
		raw             string
		limit           uint64
		wantCardinality uint64
		wantValues      []int32
	}{
		{raw: "0", limit: 1, wantCardinality: 1, wantValues: []int32{0}},
		{raw: "2147483647", limit: 1, wantCardinality: 1, wantValues: []int32{math.MaxInt32}},
		{raw: "0-1", limit: 2, wantCardinality: 2, wantValues: []int32{0, 1}},
		{raw: "0,2,4-6,2147483647", limit: 6, wantCardinality: 6, wantValues: []int32{0, 2, 4, 5, 6, math.MaxInt32}},
		{raw: "0-2147483647", limit: maxDomainCardinality, wantCardinality: maxDomainCardinality},
	}

	for _, test := range tests {
		t.Run(test.raw, func(t *testing.T) {
			set, err := parseIndexSet(test.raw, test.limit)
			if err != nil {
				t.Fatalf("parseIndexSet() error = %v", err)
			}
			if got := set.Cardinality(); got != test.wantCardinality {
				t.Fatalf("Cardinality() = %d, want %d", got, test.wantCardinality)
			}
			if got := set.String(); got != test.raw {
				t.Fatalf("String() = %q, want canonical input %q", got, test.raw)
			}
			if test.raw == "0-2147483647" && len(set.intervals) != 1 {
				t.Fatalf("full-domain range expanded into %d intervals", len(set.intervals))
			}
			for _, value := range test.wantValues {
				if !set.Contains(value) {
					t.Errorf("Contains(%d) = false", value)
				}
			}
			if len(test.wantValues) > 0 && test.wantCardinality == uint64(len(test.wantValues)) {
				if values := valuesOf(set); !reflect.DeepEqual(values, test.wantValues) {
					t.Fatalf("members = %v, want %v", values, test.wantValues)
				}
			}
		})
	}
}

func TestParseIndexSetRejectsNonCanonicalOrUnboundedInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		raw        string
		limit      uint64
		wantReason ErrorReason
	}{
		{name: "empty", raw: "", limit: 1, wantReason: ErrorReasonRangeSyntax},
		{name: "zero limit", raw: "0", limit: 0, wantReason: ErrorReasonCardinalityLimit},
		{name: "leading comma", raw: ",0", limit: 2, wantReason: ErrorReasonRangeSyntax},
		{name: "trailing comma", raw: "0,", limit: 2, wantReason: ErrorReasonRangeSyntax},
		{name: "empty term", raw: "0,,2", limit: 3, wantReason: ErrorReasonRangeSyntax},
		{name: "leading zero", raw: "01", limit: 1, wantReason: ErrorReasonRangeSyntax},
		{name: "range leading zero", raw: "0-01", limit: 2, wantReason: ErrorReasonRangeSyntax},
		{name: "plus sign", raw: "+1", limit: 1, wantReason: ErrorReasonRangeSyntax},
		{name: "negative sign", raw: "-1", limit: 1, wantReason: ErrorReasonRangeSyntax},
		{name: "whitespace", raw: "0, 2", limit: 2, wantReason: ErrorReasonRangeSyntax},
		{name: "tab", raw: "0\t", limit: 1, wantReason: ErrorReasonRangeSyntax},
		{name: "unicode digit", raw: "١", limit: 1, wantReason: ErrorReasonRangeSyntax},
		{name: "missing range start", raw: "-2", limit: 2, wantReason: ErrorReasonRangeSyntax},
		{name: "missing range end", raw: "1-", limit: 2, wantReason: ErrorReasonRangeSyntax},
		{name: "multiple dashes", raw: "1--2", limit: 2, wantReason: ErrorReasonRangeSyntax},
		{name: "equal range", raw: "1-1", limit: 1, wantReason: ErrorReasonRangeOrder},
		{name: "descending range", raw: "2-1", limit: 2, wantReason: ErrorReasonRangeOrder},
		{name: "duplicate singleton", raw: "1,1", limit: 2, wantReason: ErrorReasonRangeOrder},
		{name: "overlap", raw: "1-3,3-5", limit: 6, wantReason: ErrorReasonRangeOrder},
		{name: "descending terms", raw: "2,0", limit: 2, wantReason: ErrorReasonRangeOrder},
		{name: "adjacent singletons", raw: "0,1", limit: 2, wantReason: ErrorReasonCanonicalOrder},
		{name: "adjacent range and singleton", raw: "0-2,3", limit: 4, wantReason: ErrorReasonCanonicalOrder},
		{name: "index overflow", raw: "2147483648", limit: 1, wantReason: ErrorReasonRangeOverflow},
		{name: "range overflow", raw: "0-999999999999999999999999999", limit: 1, wantReason: ErrorReasonRangeOverflow},
		{name: "very long overflow", raw: strings.Repeat("9", 4096), limit: 1, wantReason: ErrorReasonRangeOverflow},
		{name: "cardinality above limit", raw: "0-2", limit: 2, wantReason: ErrorReasonCardinalityLimit},
		{name: "cumulative cardinality above limit", raw: "0,2-3", limit: 2, wantReason: ErrorReasonCardinalityLimit},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			set, err := parseIndexSet(test.raw, test.limit)
			if len(set.intervals) != 0 || set.Cardinality() != 0 {
				t.Fatalf("failed parse returned partial set: %+v", set)
			}
			assertCodecReason(t, err, test.wantReason)
		})
	}
}

func TestEncodeIndexSetCanonicalizesOrderWithoutMutatingInput(t *testing.T) {
	t.Parallel()

	indices := []int32{9, 1, 3, 2, 7, 8, 0}
	original := append([]int32(nil), indices...)
	got, err := encodeIndexSet(indices, uint64(len(indices)))
	if err != nil {
		t.Fatalf("encodeIndexSet() error = %v", err)
	}
	if got != "0-3,7-9" {
		t.Fatalf("encodeIndexSet() = %q, want %q", got, "0-3,7-9")
	}
	if !reflect.DeepEqual(indices, original) {
		t.Fatalf("encodeIndexSet() mutated input: got %v, want %v", indices, original)
	}
}

func TestEncodeIndexSetRejectsInvalidLogicalSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		indices    []int32
		limit      uint64
		wantReason ErrorReason
	}{
		{name: "empty", limit: 1, wantReason: ErrorReasonValueDomain},
		{name: "negative", indices: []int32{-1}, limit: 1, wantReason: ErrorReasonValueDomain},
		{name: "duplicate", indices: []int32{2, 1, 2}, limit: 3, wantReason: ErrorReasonValueDomain},
		{name: "zero limit", indices: []int32{0}, limit: 0, wantReason: ErrorReasonCardinalityLimit},
		{name: "above limit", indices: []int32{0, 1}, limit: 1, wantReason: ErrorReasonCardinalityLimit},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := encodeIndexSet(test.indices, test.limit)
			if encoded != "" {
				t.Fatalf("failed encode returned %q", encoded)
			}
			assertCodecReason(t, err, test.wantReason)
		})
	}
}

func TestIndexSetExhaustiveSmallDomainRoundTrip(t *testing.T) {
	t.Parallel()

	const domain = 12
	for mask := 1; mask < 1<<domain; mask++ {
		indices := make([]int32, 0, domain)
		for index := domain - 1; index >= 0; index-- {
			if mask&(1<<index) != 0 {
				indices = append(indices, int32(index))
			}
		}

		encoded, err := encodeIndexSet(indices, uint64(len(indices)))
		if err != nil {
			t.Fatalf("mask %012b encode error = %v", mask, err)
		}
		parsed, err := parseIndexSet(encoded, uint64(len(indices)))
		if err != nil {
			t.Fatalf("mask %012b parse %q error = %v", mask, encoded, err)
		}
		values := valuesOf(parsed)
		sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
		if !reflect.DeepEqual(values, indices) {
			t.Fatalf("mask %012b values = %v, want %v", mask, values, indices)
		}
		if parsed.String() != encoded {
			t.Fatalf("mask %012b second encoding = %q, want %q", mask, parsed.String(), encoded)
		}
	}
}

func TestIndexSetSubset(t *testing.T) {
	t.Parallel()

	members := mustParseIndexSet(t, "0-3,7-9", 7)
	subset := mustParseIndexSet(t, "1-2,8", 3)
	empty := indexSet{}

	if !subset.IsSubsetOf(members) || members.IsSubsetOf(subset) {
		t.Fatal("subset relation is incorrect")
	}
	if !empty.IsSubsetOf(members) || !empty.IsSubsetOf(empty) {
		t.Fatal("empty set must be a subset")
	}
}

func TestIndexSetSubsetExhaustiveSmallDomain(t *testing.T) {
	t.Parallel()

	const domain = 8
	sets := make([]indexSet, 1<<domain)
	for mask := 1; mask < len(sets); mask++ {
		indices := make([]int32, 0, bits.OnesCount(uint(mask)))
		for index := 0; index < domain; index++ {
			if mask&(1<<index) != 0 {
				indices = append(indices, int32(index))
			}
		}
		set, err := indexSetFromIndices(indices, uint64(len(indices)))
		if err != nil {
			t.Fatalf("mask %08b: indexSetFromIndices() error = %v", mask, err)
		}
		sets[mask] = set
	}

	for leftMask, left := range sets {
		for rightMask, right := range sets {
			wantSubset := leftMask&^rightMask == 0
			if got := left.IsSubsetOf(right); got != wantSubset {
				t.Fatalf("%08b subset of %08b = %t, want %t", leftMask, rightMask, got, wantSubset)
			}
		}
	}
}

func TestIndexSetContainsBoundaries(t *testing.T) {
	t.Parallel()

	set := mustParseIndexSet(t, "0,2-4,2147483647", 5)
	for _, value := range []int32{0, 2, 3, 4, math.MaxInt32} {
		if !set.Contains(value) {
			t.Errorf("Contains(%d) = false", value)
		}
	}
	for _, value := range []int32{-1, 1, 5, math.MaxInt32 - 1} {
		if set.Contains(value) {
			t.Errorf("Contains(%d) = true", value)
		}
	}
}

func mustParseIndexSet(t *testing.T, raw string, limit uint64) indexSet {
	t.Helper()
	set, err := parseIndexSet(raw, limit)
	if err != nil {
		t.Fatalf("parseIndexSet(%q, %d) error = %v", raw, limit, err)
	}
	return set
}

// valuesOf lists the set's members in ascending order.
func valuesOf(set indexSet) []int32 {
	values := make([]int32, 0, set.Cardinality())
	set.forEach(func(value int32) { values = append(values, value) })
	return values
}
