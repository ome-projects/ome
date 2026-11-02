package irstatus

import (
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// EncodeColumns builds the canonical ColumnarV2 payload for rows, the dense
// list in its stored order. Rows are not eligible when there are none, when
// there are more than maxInstances, or when a row carries a negative or
// duplicate index, a negative count, an unsupported phase, or an
// activeOrdinal outside 0 and 1; the typed reason lets the caller keep
// DenseV1. readyPodCount, scheduledPodCount, and nodesOccupied have no
// column and are never represented.
func EncodeColumns(rows []v1beta1.OMENativeInstanceStatus, maxInstances uint64) (*v1beta1.InstanceStatusColumns, error) {
	if len(rows) == 0 {
		return nil, newCodecError(ErrorReasonValueDomain)
	}
	if maxInstances == 0 || uint64(len(rows)) > maxInstances {
		return nil, newCodecError(ErrorReasonCardinalityLimit)
	}

	indices := make([]int32, len(rows))
	ascending := true
	for i := range rows {
		row := &rows[i]
		if !supportedPhase(row.Phase) || row.PodCount < 0 || row.ServingPodCount < 0 || row.AvailablePodCount < 0 ||
			(row.ActiveOrdinal != 0 && row.ActiveOrdinal != 1) {
			return nil, newCodecError(ErrorReasonValueDomain)
		}
		indices[i] = row.Index
		if i > 0 && row.Index <= rows[i-1].Index {
			ascending = false
		}
	}
	members, err := indexSetFromIndices(indices, maxInstances)
	if err != nil {
		return nil, err
	}

	columns := &v1beta1.InstanceStatusColumns{Members: members.String()}
	if !ascending {
		columns.RowOrder = indices
	}

	var collectors [len(groupedColumns)]columnCollector
	for i, column := range groupedColumns {
		collectors[i] = column.newCollector()
	}
	var (
		admitted         []int32
		activeOrdinalOne []int32
		entries          []v1beta1.InstanceStatusEntry
	)
	for i := range rows {
		row := &rows[i]
		for _, collector := range collectors {
			collector.add(row)
		}
		if row.Admitted {
			admitted = append(admitted, row.Index)
		}
		if row.ActiveOrdinal == 1 {
			activeOrdinalOne = append(activeOrdinalOne, row.Index)
		}
		if entry, ok := entryOf(row); ok {
			entries = append(entries, entry)
		}
	}

	n := uint64(len(rows))
	for _, collector := range collectors {
		if err := collector.store(columns, n); err != nil {
			return nil, err
		}
	}
	if columns.Admitted, err = encodeOptionalIndexSet(admitted, n); err != nil {
		return nil, err
	}
	if columns.ActiveOrdinalOne, err = encodeOptionalIndexSet(activeOrdinalOne, n); err != nil {
		return nil, err
	}
	if len(entries) > 0 {
		sort.Slice(entries, func(i, j int) bool { return entries[i].Index < entries[j].Index })
		columns.Entries = entries
	}
	return columns, nil
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
// false when the row has none. An empty conditions list counts as absent.
func entryOf(row *v1beta1.OMENativeInstanceStatus) (v1beta1.InstanceStatusEntry, bool) {
	if len(row.Conditions) == 0 && row.ReadySince == nil && row.Operation == nil && row.LastFailure == nil &&
		len(row.Announced) == 0 {
		return v1beta1.InstanceStatusEntry{}, false
	}
	entry := v1beta1.InstanceStatusEntry{
		Index:       row.Index,
		ReadySince:  row.ReadySince.DeepCopy(),
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
