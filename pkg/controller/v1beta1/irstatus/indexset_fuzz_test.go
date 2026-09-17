package irstatus

import (
	"encoding/binary"
	"slices"
	"strings"
	"testing"
)

func FuzzParseIndexSet(f *testing.F) {
	for _, seed := range []struct {
		raw   string
		limit uint16
	}{
		{raw: "0", limit: 1},
		{raw: "0-499", limit: 500},
		{raw: "0-8,10-114,116-499", limit: 498},
		{raw: "2147483647", limit: 1},
		{raw: "0,1", limit: 2},
		{raw: "0-2147483647", limit: 1},
		{raw: strings.Repeat("9", 4096), limit: 1},
		{raw: "", limit: 1},
		{raw: "01", limit: 1},
		{raw: "0-1999", limit: 2000},
		{raw: "0-4999", limit: 5000},
		{raw: "6,43,175", limit: 3},
		{raw: "9,115", limit: 2},
		{raw: "0-3,7", limit: 5},
		{raw: "1-3,3-5", limit: 6},
		{raw: "0-2,3", limit: 4},
		{raw: "2147483646-2147483647", limit: 2},
		{raw: "0-2147483646", limit: 65535},
		{raw: "4294967295", limit: 1},
		{raw: "1-1", limit: 1},
		{raw: "3-1", limit: 3},
		{raw: "0,", limit: 1},
		{raw: ",0", limit: 1},
		{raw: "0,,1", limit: 2},
		{raw: "0-", limit: 1},
		{raw: "-", limit: 1},
		{raw: "--1", limit: 1},
		{raw: "1e3", limit: 1},
		{raw: "0x10", limit: 1},
		{raw: " 0", limit: 1},
		{raw: "0 ", limit: 1},
		{raw: "0\n", limit: 1},
		{raw: "٠", limit: 1},
	} {
		f.Add(seed.raw, seed.limit)
	}

	f.Fuzz(func(t *testing.T, raw string, rawLimit uint16) {
		limit := uint64(rawLimit)
		if limit == 0 {
			limit = 1
		}
		set, err := parseIndexSet(raw, limit)
		if err != nil {
			if _, ok := ErrorReasonOf(err); !ok {
				t.Fatalf("parseIndexSet() returned uncatalogued error: %v", err)
			}
			return
		}
		if set.String() != raw {
			t.Fatalf("accepted noncanonical input %q as %q", raw, set.String())
		}
		if set.Cardinality() == 0 || set.Cardinality() > limit {
			t.Fatalf("accepted cardinality %d with limit %d", set.Cardinality(), limit)
		}
		values, err := set.Values(limit)
		if err != nil {
			t.Fatalf("Values() error = %v", err)
		}
		rebuilt, err := indexSetFromIndices(values, limit)
		if err != nil {
			t.Fatalf("indexSetFromIndices() error = %v", err)
		}
		if !rebuilt.Equal(set) || rebuilt.String() != raw {
			t.Fatalf("round trip changed %q to %q", raw, rebuilt.String())
		}
	})
}

func FuzzEncodeIndexSet(f *testing.F) {
	for _, seed := range [][]byte{
		{0, 0, 0, 0},
		{3, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0},
		{0xff, 0xff, 0xff, 0x7f},
		{0, 0, 0, 0, 0xfe, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff, 0x7f},
		{0, 0, 0, 0x80},
		{1, 0, 0, 0, 1, 0, 0, 0},
		nil,
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		const maxIndices = 256
		count := min(len(raw)/4, maxIndices)
		indices := make([]int32, count)
		for i := range indices {
			indices[i] = int32(binary.LittleEndian.Uint32(raw[i*4 : (i+1)*4]))
		}
		limit := uint64(len(indices))
		if limit == 0 {
			limit = 1
		}
		encoded, err := encodeIndexSet(indices, limit)
		if err != nil {
			if _, ok := ErrorReasonOf(err); !ok {
				t.Fatalf("encodeIndexSet() returned uncatalogued error: %v", err)
			}
			return
		}
		parsed, err := parseIndexSet(encoded, limit)
		if err != nil {
			t.Fatalf("parse encoded %q: %v", encoded, err)
		}
		values, err := parsed.Values(limit)
		if err != nil {
			t.Fatalf("Values() error = %v", err)
		}
		if len(values) != len(indices) {
			t.Fatalf("round trip length = %d, want %d", len(values), len(indices))
		}
		want := append([]int32(nil), indices...)
		slices.Sort(want)
		if !slices.Equal(values, want) {
			t.Fatalf("round trip values = %v, want %v", values, want)
		}
		encodedAgain, err := encodeIndexSet(values, limit)
		if err != nil {
			t.Fatalf("second encode error = %v", err)
		}
		if encodedAgain != encoded {
			t.Fatalf("second encoding = %q, want %q", encodedAgain, encoded)
		}
	})
}
