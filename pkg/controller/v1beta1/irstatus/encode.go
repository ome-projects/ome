package irstatus

import (
	"cmp"
	"slices"
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

	var (
		phases             valueGroups[v1beta1.OMENativeInstancePhase]
		runningRevisions   valueGroups[string]
		targetRevisions    valueGroups[string]
		incarnations       valueGroups[int64]
		podCounts          valueGroups[int32]
		servingPodCounts   valueGroups[int32]
		availablePodCounts valueGroups[int32]
		admitted           []int32
		activeOrdinalOne   []int32
		entries            []v1beta1.InstanceStatusEntry
	)
	for i := range rows {
		row := &rows[i]
		phases.add(row.Phase, row.Index)
		if row.RunningRevision != "" {
			runningRevisions.add(row.RunningRevision, row.Index)
		}
		if row.TargetRevision != "" {
			targetRevisions.add(row.TargetRevision, row.Index)
		}
		if row.Incarnation != 0 {
			incarnations.add(row.Incarnation, row.Index)
		}
		if row.PodCount != 0 {
			podCounts.add(row.PodCount, row.Index)
		}
		if row.ServingPodCount != 0 {
			servingPodCounts.add(row.ServingPodCount, row.Index)
		}
		if row.AvailablePodCount != 0 {
			availablePodCounts.add(row.AvailablePodCount, row.Index)
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
	if columns.Phases, err = buildGroups(&phases, n, func(value v1beta1.OMENativeInstancePhase, indexes string) v1beta1.InstanceStatusPhaseGroup {
		return v1beta1.InstanceStatusPhaseGroup{Value: value, Indexes: indexes}
	}); err != nil {
		return nil, err
	}
	if columns.RunningRevisions, err = buildGroups(&runningRevisions, n, newRevisionGroup); err != nil {
		return nil, err
	}
	if columns.TargetRevisions, err = buildGroups(&targetRevisions, n, newRevisionGroup); err != nil {
		return nil, err
	}
	if columns.Incarnations, err = buildGroups(&incarnations, n, func(value int64, indexes string) v1beta1.InstanceStatusIncarnationGroup {
		return v1beta1.InstanceStatusIncarnationGroup{Value: value, Indexes: indexes}
	}); err != nil {
		return nil, err
	}
	if columns.PodCounts, err = buildGroups(&podCounts, n, newCountGroup); err != nil {
		return nil, err
	}
	if columns.ServingPodCounts, err = buildGroups(&servingPodCounts, n, newCountGroup); err != nil {
		return nil, err
	}
	if columns.AvailablePodCounts, err = buildGroups(&availablePodCounts, n, newCountGroup); err != nil {
		return nil, err
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

// valueGroups collects, per distinct column value, the indices of the rows
// that carry it, in row order.
type valueGroups[V cmp.Ordered] struct {
	values  []V
	indices map[V][]int32
}

func (g *valueGroups[V]) add(value V, index int32) {
	if g.indices == nil {
		g.indices = make(map[V][]int32)
	}
	list, seen := g.indices[value]
	if !seen {
		g.values = append(g.values, value)
	}
	g.indices[value] = append(list, index)
}

// buildGroups emits one group per distinct value in ascending value order,
// which is bytewise for strings and signed numeric for integers, each with
// the canonical index set of its rows. Absent values produce a nil slice.
func buildGroups[V cmp.Ordered, G any](groups *valueGroups[V], n uint64, build func(V, string) G) ([]G, error) {
	if len(groups.values) == 0 {
		return nil, nil
	}
	slices.Sort(groups.values)
	out := make([]G, 0, len(groups.values))
	for _, value := range groups.values {
		indexes, err := encodeIndexSet(groups.indices[value], n)
		if err != nil {
			return nil, err
		}
		out = append(out, build(value, indexes))
	}
	return out, nil
}

func newRevisionGroup(value string, indexes string) v1beta1.InstanceStatusRevisionGroup {
	return v1beta1.InstanceStatusRevisionGroup{Value: value, Indexes: indexes}
}

func newCountGroup(value int32, indexes string) v1beta1.InstanceStatusCountGroup {
	return v1beta1.InstanceStatusCountGroup{Value: value, Indexes: indexes}
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
	if len(row.Conditions) == 0 && row.ReadySince == nil && row.Operation == nil && row.LastFailure == nil {
		return v1beta1.InstanceStatusEntry{}, false
	}
	entry := v1beta1.InstanceStatusEntry{
		Index:       row.Index,
		ReadySince:  row.ReadySince.DeepCopy(),
		Operation:   row.Operation.DeepCopy(),
		LastFailure: row.LastFailure.DeepCopy(),
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
