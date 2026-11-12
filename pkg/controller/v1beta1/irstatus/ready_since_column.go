package irstatus

import (
	"cmp"
	"math"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// The readySince columns store the time each Instance became Ready once per
// Instance as a small integer instead of a time: readySinceBase is the
// earliest such time, readySinceIndexes names the Instances that have one,
// and readySinceOffsets lists, in their ascending index order, the whole
// seconds from the base to each. The columns carry the times exactly or not
// at all: when a time has a sub-second part, or the latest is more than
// math.MaxInt32 seconds after the earliest, every time stays in the entries.

// ReadySinceForm is where a status write stores the time each Instance
// became Ready. The served schema decides it: a schema that lacks any of
// the three columns would drop them from every write without an error, so
// under such a schema every time stays in the entries, the form every
// schema stores. The decoder reads both forms whatever the writer's form.
type ReadySinceForm int

const (
	// ReadySinceInColumns stores the times in readySinceBase,
	// readySinceIndexes and readySinceOffsets, the form of the schema this
	// release ships. It is the zero value.
	ReadySinceInColumns ReadySinceForm = iota
	// ReadySinceInEntries stores each time in its Instance's entry, the
	// form for a served schema without the three columns.
	ReadySinceInEntries
)

// String names the form for a log line.
func (f ReadySinceForm) String() string {
	if f == ReadySinceInEntries {
		return "entries"
	}
	return "columns"
}

// readySinceAt is one row's readySince as the collector keeps it.
type readySinceAt struct {
	index   int32
	seconds int64
}

// readySinceCollector gathers the readySince of every row of one encode and
// decides whether the three columns carry them.
type readySinceCollector struct {
	times []readySinceAt
	exact bool
	form  ReadySinceForm
}

// collectReadySince reads every row's readySince for an encode that stores
// the times in form.
func collectReadySince(rows []v1beta1.OMENativeInstanceStatus, form ReadySinceForm) readySinceCollector {
	collected := readySinceCollector{exact: true, form: form}
	for i := range rows {
		row := &rows[i]
		if row.ReadySince == nil {
			continue
		}
		if row.ReadySince.Nanosecond() != 0 {
			collected.exact = false
		}
		collected.times = append(collected.times, readySinceAt{index: row.Index, seconds: row.ReadySince.Unix()})
	}
	return collected
}

// inColumns reports whether the columns carry the times: the encode stores
// them in the columns, there is at least one, every one is a whole second,
// and the span fits an int32 of seconds.
func (c *readySinceCollector) inColumns() bool {
	if c.form != ReadySinceInColumns || len(c.times) == 0 || !c.exact {
		return false
	}
	earliest, latest := c.times[0].seconds, c.times[0].seconds
	for _, at := range c.times[1:] {
		earliest = min(earliest, at.seconds)
		latest = max(latest, at.seconds)
	}
	return latest-earliest <= math.MaxInt32
}

// store writes the three columns when the columns carry the times; the
// index set is bounded by n, the row count.
func (c *readySinceCollector) store(columns *v1beta1.InstanceStatusColumns, n uint64) error {
	if !c.inColumns() {
		return nil
	}
	slices.SortFunc(c.times, func(a, b readySinceAt) int { return cmp.Compare(a.index, b.index) })
	base := c.times[0].seconds
	indices := make([]int32, len(c.times))
	offsets := make([]int32, len(c.times))
	for i, at := range c.times {
		base = min(base, at.seconds)
		indices[i] = at.index
	}
	for i, at := range c.times {
		offsets[i] = int32(at.seconds - base)
	}
	indexes, err := encodeIndexSet(indices, n)
	if err != nil {
		return err
	}
	baseTime := metav1.NewTime(time.Unix(base, 0).UTC())
	columns.ReadySinceBase = &baseTime
	columns.ReadySinceIndexes = &indexes
	columns.ReadySinceOffsets = offsets
	return nil
}

// readySincePlan is a validated readySince column set, ready to expand.
type readySincePlan struct {
	indexes indexSet
	offsets []int32
	base    metav1.Time
}

// validateReadySinceColumns checks the three columns together: all present
// or all absent, the index set canonical and inside members, one offset per
// named Instance, no offset negative, and the earliest offset zero, since the
// base is the earliest time. An absent set returns a nil plan.
func validateReadySinceColumns(columns *v1beta1.InstanceStatusColumns, members indexSet) (*readySincePlan, error) {
	base, raw, offsets := columns.ReadySinceBase, columns.ReadySinceIndexes, columns.ReadySinceOffsets
	if base == nil && raw == nil && offsets == nil {
		return nil, nil
	}
	if base == nil || raw == nil || offsets == nil {
		return nil, newCodecError(ErrorReasonValueDomain)
	}
	indexes, err := validateOptionalIndexSet(raw, members)
	if err != nil {
		return nil, err
	}
	if len(offsets) == 0 {
		return nil, newCodecError(ErrorReasonValueDomain)
	}
	if uint64(len(offsets)) != indexes.Cardinality() {
		return nil, newCodecError(ErrorReasonCoverage)
	}
	earliestIsZero := false
	for _, offset := range offsets {
		if offset < 0 {
			return nil, newCodecError(ErrorReasonValueDomain)
		}
		earliestIsZero = earliestIsZero || offset == 0
	}
	if !earliestIsZero {
		return nil, newCodecError(ErrorReasonCanonicalOrder)
	}
	return &readySincePlan{indexes: *indexes, offsets: offsets, base: *base}, nil
}

// expand gives each named row its time, the base plus its offset. The times
// live in one slice of their own, so a row shares nothing with the payload
// and a decode allocates once for every time, not once per time.
func (p *readySincePlan) expand(rowAt func(int32) *v1beta1.OMENativeInstanceStatus) {
	if p == nil {
		return
	}
	times := make([]metav1.Time, len(p.offsets))
	position := 0
	p.indexes.forEach(func(index int32) {
		times[position] = metav1.NewTime(p.base.Add(time.Duration(p.offsets[position]) * time.Second))
		rowAt(index).ReadySince = &times[position]
		position++
	})
}
