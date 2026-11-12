package irstatus

import (
	"cmp"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// EncodeColumns builds the canonical ColumnarV2 payload for rows, the dense
// list in its stored order, with the readySince of each row in the three
// readySince columns. Rows are not eligible when there are none, when
// there are more than maxInstances, or when a row carries a negative or
// duplicate index, a negative count, an unsupported phase, or an
// activeOrdinal outside 0 and 1; the typed reason lets the caller keep
// DenseV1. readyPodCount, scheduledPodCount, and nodesOccupied have no
// column and are never represented.
func EncodeColumns(rows []v1beta1.OMENativeInstanceStatus, maxInstances uint64) (*v1beta1.InstanceStatusColumns, error) {
	return EncodeColumnsStoring(rows, maxInstances, ReadySinceInColumns)
}

// EncodeColumnsStoring is EncodeColumns with the readySince of each row
// stored in form: the three columns, or each row's entry when the served
// schema lacks the columns.
func EncodeColumnsStoring(rows []v1beta1.OMENativeInstanceStatus, maxInstances uint64, form ReadySinceForm) (*v1beta1.InstanceStatusColumns, error) {
	if len(rows) == 0 {
		return nil, newCodecError(ErrorReasonValueDomain)
	}
	if maxInstances == 0 || uint64(len(rows)) > maxInstances {
		return nil, newCodecError(ErrorReasonCardinalityLimit)
	}
	order, err := rowOrderOf(rows, maxInstances)
	if err != nil {
		return nil, err
	}
	columns := &v1beta1.InstanceStatusColumns{Members: order.members.String(), RowOrder: order.explicit}
	var collected rowCollection
	collected.gather(rows, form)
	if err := collected.store(columns, uint64(len(rows))); err != nil {
		return nil, err
	}
	return columns, nil
}

// rowOrder is the member set of one encode and, when the rows are not in
// ascending index order, the order the payload spells out. Ascending order
// is spelled by absence.
type rowOrder struct {
	members  indexSet
	explicit []int32
}

// rowOrderOf checks every row against the column domains, in row order, and
// builds the member set from the row indices. The first row outside the
// domains fails the encode; a negative or duplicate index fails it when the
// set is built.
func rowOrderOf(rows []v1beta1.OMENativeInstanceStatus, maxInstances uint64) (rowOrder, error) {
	indices := make([]int32, len(rows))
	ascending := true
	for i := range rows {
		row := &rows[i]
		if !rowInDomain(row) {
			return rowOrder{}, newCodecError(ErrorReasonValueDomain)
		}
		indices[i] = row.Index
		if i > 0 && row.Index <= rows[i-1].Index {
			ascending = false
		}
	}
	members, err := indexSetFromIndices(indices, maxInstances)
	if err != nil {
		return rowOrder{}, err
	}
	order := rowOrder{members: members}
	if !ascending {
		order.explicit = indices
	}
	return order, nil
}

// rowInDomain reports whether every grouped value of row is one the column
// schema can store: a supported phase, counts that are not negative, and an
// activeOrdinal of 0 or 1.
func rowInDomain(row *v1beta1.OMENativeInstanceStatus) bool {
	return supportedPhase(row.Phase) && row.PodCount >= 0 && row.ServingPodCount >= 0 && row.AvailablePodCount >= 0 &&
		(row.ActiveOrdinal == 0 || row.ActiveOrdinal == 1)
}

// rowCollection gathers, in one pass over the rows, the grouped columns and
// the columns no group carries: the admitted set, the activeOrdinal one set,
// the readySince columns, and the entries of the rows with exceptional
// records.
type rowCollection struct {
	grouped          [len(groupedColumns)]columnCollector
	admitted         []int32
	activeOrdinalOne []int32
	readySince       readySinceCollector
	entries          []v1beta1.InstanceStatusEntry
}

func (c *rowCollection) gather(rows []v1beta1.OMENativeInstanceStatus, form ReadySinceForm) {
	for i, column := range groupedColumns {
		c.grouped[i] = column.newCollector()
	}
	c.readySince = collectReadySince(rows, form)
	readySinceInColumns := c.readySince.inColumns()
	for i := range rows {
		row := &rows[i]
		for _, collector := range c.grouped {
			collector.add(row)
		}
		if row.Admitted {
			c.admitted = append(c.admitted, row.Index)
		}
		if row.ActiveOrdinal == 1 {
			c.activeOrdinalOne = append(c.activeOrdinalOne, row.Index)
		}
		if entry, ok := entryOf(row, readySinceInColumns); ok {
			c.entries = append(c.entries, entry)
		}
	}
}

// store writes the collection onto columns: every grouped column, the two
// optional index sets, the readySince columns, and the entries in ascending
// index order. Every index set is bounded by n, the row count.
func (c *rowCollection) store(columns *v1beta1.InstanceStatusColumns, n uint64) error {
	for _, collector := range c.grouped {
		if err := collector.store(columns, n); err != nil {
			return err
		}
	}
	var err error
	if columns.Admitted, err = encodeOptionalIndexSet(c.admitted, n); err != nil {
		return err
	}
	if columns.ActiveOrdinalOne, err = encodeOptionalIndexSet(c.activeOrdinalOne, n); err != nil {
		return err
	}
	if err = c.readySince.store(columns, n); err != nil {
		return err
	}
	if len(c.entries) > 0 {
		slices.SortFunc(c.entries, func(a, b v1beta1.InstanceStatusEntry) int { return cmp.Compare(a.Index, b.Index) })
		columns.Entries = c.entries
	}
	return nil
}

func encodeOptionalIndexSet(indices []int32, n uint64) (*string, error) {
	if len(indices) == 0 {
		return nil, nil
	}
	encoded, err := encodeIndexSet(indices, n)
	if err != nil {
		return nil, err
	}
	return &encoded, nil
}

// entryOf copies the exceptional records of row into an entry, reporting
// false when the row has none. An empty conditions list counts as absent,
// and so does the readySince when the readySince columns carry it.
func entryOf(row *v1beta1.OMENativeInstanceStatus, readySinceInColumns bool) (v1beta1.InstanceStatusEntry, bool) {
	readySince := row.ReadySince
	if readySinceInColumns {
		readySince = nil
	}
	if len(row.Conditions) == 0 && readySince == nil && row.Operation == nil && row.LastFailure == nil &&
		len(row.Announced) == 0 {
		return v1beta1.InstanceStatusEntry{}, false
	}
	entry := v1beta1.InstanceStatusEntry{
		Index:       row.Index,
		ReadySince:  readySince.DeepCopy(),
		Operation:   row.Operation.DeepCopy(),
		LastFailure: row.LastFailure.DeepCopy(),
	}
	if len(row.Announced) > 0 {
		entry.Announced = append([]string(nil), row.Announced...)
	}
	if len(row.Conditions) > 0 {
		entry.Conditions = cloneConditions(row.Conditions)
	}
	return entry, true
}

func cloneConditions(conditions []metav1.Condition) []metav1.Condition {
	out := make([]metav1.Condition, len(conditions))
	for i := range conditions {
		conditions[i].DeepCopyInto(&out[i])
	}
	return out
}
