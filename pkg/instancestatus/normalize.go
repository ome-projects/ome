// Package instancestatus normalizes InferenceReplica per-instance status
// encodings into checked, bounded rows for shared read consumers.
package instancestatus

import "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"

// DefaultMaxRows bounds expansion of a compact ColumnarV2 payload. The limit
// is deliberately much larger than a normal serving fleet while preventing a
// malformed range from causing unbounded CLI memory use.
const DefaultMaxRows = 10_000

// Encoding identifies the source representation used for normalized rows.
type Encoding string

const (
	EncodingDenseV1    Encoding = "DenseV1"
	EncodingColumnarV2 Encoding = "ColumnarV2"
)

// Result contains independent, normalized rows and their source encoding.
// Rows preserve DenseV1 order or ColumnarV2 RowOrder; ColumnarV2 without an
// explicit RowOrder is ascending by instance index.
type Result struct {
	Encoding Encoding
	Rows     []v1beta1.OMENativeInstanceStatus
}

// Normalize decodes status with DefaultMaxRows as the expansion bound.
func Normalize(status *v1beta1.InferenceReplicaStatus) (Result, error) {
	return normalizeWithLimit(status, DefaultMaxRows)
}

func normalizeWithLimit(status *v1beta1.InferenceReplicaStatus, maxRows int) (Result, error) {
	if maxRows <= 0 || maxRows > DefaultMaxRows {
		return Result{}, newDecodeError(ErrorReasonCardinality)
	}
	if status == nil {
		return Result{}, newDecodeError(ErrorReasonRepresentation)
	}
	if status.InstanceStatusEncoding == nil {
		if status.InstanceStatusColumns != nil {
			return Result{}, newDecodeError(ErrorReasonRepresentation)
		}
		return normalizeDense(status.InstanceStatuses, maxRows)
	}
	if *status.InstanceStatusEncoding != v1beta1.InstanceStatusEncodingColumnarV2 {
		return Result{}, newDecodeError(ErrorReasonUnknownEncoding)
	}
	if status.InstanceStatusColumns == nil || len(status.InstanceStatuses) != 0 {
		return Result{}, newDecodeError(ErrorReasonRepresentation)
	}
	return normalizeColumnar(status.InstanceStatusColumns, maxRows)
}

func normalizeDense(source []v1beta1.OMENativeInstanceStatus, maxRows int) (Result, error) {
	if len(source) > maxRows {
		return Result{}, newDecodeError(ErrorReasonCardinality)
	}
	rows := make([]v1beta1.OMENativeInstanceStatus, len(source))
	seen := make(map[int32]struct{}, len(source))
	for i := range source {
		row := &source[i]
		if row.Index < 0 {
			return Result{}, newDecodeError(ErrorReasonValueDomain)
		}
		if _, duplicate := seen[row.Index]; duplicate {
			return Result{}, newDecodeError(ErrorReasonCoverage)
		}
		seen[row.Index] = struct{}{}
		if !validPhase(row.Phase) || row.Incarnation < 0 ||
			row.PodCount < 0 || row.ServingPodCount < 0 ||
			row.AvailablePodCount < 0 ||
			(row.ActiveOrdinal != 0 && row.ActiveOrdinal != 1) {
			return Result{}, newDecodeError(ErrorReasonValueDomain)
		}
		rows[i] = *row.DeepCopy()
	}
	return Result{Encoding: EncodingDenseV1, Rows: rows}, nil
}

func normalizeColumnar(columns *v1beta1.InstanceStatusColumns, maxRows int) (Result, error) {
	members, err := parseIndexSet(columns.Members, maxRows)
	if err != nil {
		return Result{}, err
	}
	rowsByIndex := make(map[int32]*v1beta1.OMENativeInstanceStatus, len(members.ordered))
	for _, index := range members.ordered {
		rowsByIndex[index] = &v1beta1.OMENativeInstanceStatus{Index: index}
	}

	phaseSeen := make(map[int32]struct{}, len(members.ordered))
	for _, group := range columns.Phases {
		if !validPhase(group.Value) {
			return Result{}, newDecodeError(ErrorReasonValueDomain)
		}
		set, err := checkedSubset(group.Indexes, members, maxRows)
		if err != nil {
			return Result{}, err
		}
		for _, index := range set.ordered {
			if _, duplicate := phaseSeen[index]; duplicate {
				return Result{}, newDecodeError(ErrorReasonCoverage)
			}
			phaseSeen[index] = struct{}{}
			rowsByIndex[index].Phase = group.Value
		}
	}
	if len(phaseSeen) != len(members.ordered) {
		return Result{}, newDecodeError(ErrorReasonCoverage)
	}

	if err := applyStringGroups(columns.RunningRevisions, members, maxRows, rowsByIndex, func(row *v1beta1.OMENativeInstanceStatus, value string) {
		row.RunningRevision = value
	}); err != nil {
		return Result{}, err
	}
	if err := applyStringGroups(columns.TargetRevisions, members, maxRows, rowsByIndex, func(row *v1beta1.OMENativeInstanceStatus, value string) {
		row.TargetRevision = value
	}); err != nil {
		return Result{}, err
	}
	if err := applyInt64Groups(columns.Incarnations, members, maxRows, rowsByIndex, func(row *v1beta1.OMENativeInstanceStatus, value int64) {
		row.Incarnation = value
	}); err != nil {
		return Result{}, err
	}
	if err := applyCountGroups(columns.PodCounts, members, maxRows, rowsByIndex, func(row *v1beta1.OMENativeInstanceStatus, value int32) {
		row.PodCount = value
	}); err != nil {
		return Result{}, err
	}
	if err := applyCountGroups(columns.ServingPodCounts, members, maxRows, rowsByIndex, func(row *v1beta1.OMENativeInstanceStatus, value int32) {
		row.ServingPodCount = value
	}); err != nil {
		return Result{}, err
	}
	if err := applyCountGroups(columns.AvailablePodCounts, members, maxRows, rowsByIndex, func(row *v1beta1.OMENativeInstanceStatus, value int32) {
		row.AvailablePodCount = value
	}); err != nil {
		return Result{}, err
	}

	if err := applyBooleanSet(columns.Admitted, members, maxRows, rowsByIndex, func(row *v1beta1.OMENativeInstanceStatus) {
		row.Admitted = true
	}); err != nil {
		return Result{}, err
	}
	if err := applyBooleanSet(columns.ActiveOrdinalOne, members, maxRows, rowsByIndex, func(row *v1beta1.OMENativeInstanceStatus) {
		row.ActiveOrdinal = 1
	}); err != nil {
		return Result{}, err
	}

	entrySeen := make(map[int32]struct{}, len(columns.Entries))
	for i := range columns.Entries {
		entry := &columns.Entries[i]
		row, member := rowsByIndex[entry.Index]
		if !member {
			return Result{}, newDecodeError(ErrorReasonCoverage)
		}
		if _, duplicate := entrySeen[entry.Index]; duplicate {
			return Result{}, newDecodeError(ErrorReasonCoverage)
		}
		entrySeen[entry.Index] = struct{}{}
		if len(entry.Conditions) == 0 && entry.Operation == nil && entry.LastFailure == nil {
			return Result{}, newDecodeError(ErrorReasonValueDomain)
		}
		copy := entry.DeepCopy()
		row.Conditions = copy.Conditions
		row.Operation = copy.Operation
		row.LastFailure = copy.LastFailure
	}

	order, err := checkedRowOrder(columns.RowOrder, members)
	if err != nil {
		return Result{}, err
	}
	rows := make([]v1beta1.OMENativeInstanceStatus, len(order))
	for i, index := range order {
		rows[i] = *rowsByIndex[index]
	}
	return Result{Encoding: EncodingColumnarV2, Rows: rows}, nil
}

func checkedSubset(raw string, members indexSet, maxRows int) (indexSet, error) {
	set, err := parseIndexSet(raw, maxRows)
	if err != nil {
		return indexSet{}, err
	}
	if !subsetOf(set, members) {
		return indexSet{}, newDecodeError(ErrorReasonCoverage)
	}
	return set, nil
}

func applyStringGroups(
	groups []v1beta1.InstanceStatusStringGroup,
	members indexSet,
	maxRows int,
	rows map[int32]*v1beta1.OMENativeInstanceStatus,
	assign func(*v1beta1.OMENativeInstanceStatus, string),
) error {
	seen := make(map[int32]struct{})
	for _, group := range groups {
		if group.Value == "" {
			return newDecodeError(ErrorReasonValueDomain)
		}
		set, err := checkedSubset(group.Indexes, members, maxRows)
		if err != nil {
			return err
		}
		for _, index := range set.ordered {
			if _, duplicate := seen[index]; duplicate {
				return newDecodeError(ErrorReasonCoverage)
			}
			seen[index] = struct{}{}
			assign(rows[index], group.Value)
		}
	}
	return nil
}

func applyInt64Groups(
	groups []v1beta1.InstanceStatusInt64Group,
	members indexSet,
	maxRows int,
	rows map[int32]*v1beta1.OMENativeInstanceStatus,
	assign func(*v1beta1.OMENativeInstanceStatus, int64),
) error {
	return applyGroups(members, maxRows, rows, len(groups), func(i int) (string, bool, func(*v1beta1.OMENativeInstanceStatus)) {
		group := groups[i]
		return group.Indexes, group.Value > 0, func(row *v1beta1.OMENativeInstanceStatus) { assign(row, group.Value) }
	})
}

func applyCountGroups(
	groups []v1beta1.InstanceStatusCountGroup,
	members indexSet,
	maxRows int,
	rows map[int32]*v1beta1.OMENativeInstanceStatus,
	assign func(*v1beta1.OMENativeInstanceStatus, int32),
) error {
	return applyGroups(members, maxRows, rows, len(groups), func(i int) (string, bool, func(*v1beta1.OMENativeInstanceStatus)) {
		group := groups[i]
		return group.Indexes, group.Value > 0, func(row *v1beta1.OMENativeInstanceStatus) { assign(row, group.Value) }
	})
}

func applyGroups(
	members indexSet,
	maxRows int,
	rows map[int32]*v1beta1.OMENativeInstanceStatus,
	count int,
	group func(int) (string, bool, func(*v1beta1.OMENativeInstanceStatus)),
) error {
	seen := make(map[int32]struct{})
	for i := 0; i < count; i++ {
		indexes, valid, assign := group(i)
		if !valid {
			return newDecodeError(ErrorReasonValueDomain)
		}
		set, err := checkedSubset(indexes, members, maxRows)
		if err != nil {
			return err
		}
		for _, index := range set.ordered {
			if _, duplicate := seen[index]; duplicate {
				return newDecodeError(ErrorReasonCoverage)
			}
			seen[index] = struct{}{}
			assign(rows[index])
		}
	}
	return nil
}

func applyBooleanSet(
	raw *string,
	members indexSet,
	maxRows int,
	rows map[int32]*v1beta1.OMENativeInstanceStatus,
	assign func(*v1beta1.OMENativeInstanceStatus),
) error {
	if raw == nil {
		return nil
	}
	set, err := checkedSubset(*raw, members, maxRows)
	if err != nil {
		return err
	}
	for _, index := range set.ordered {
		assign(rows[index])
	}
	return nil
}

func checkedRowOrder(rowOrder []int32, members indexSet) ([]int32, error) {
	if len(rowOrder) == 0 {
		return append([]int32(nil), members.ordered...), nil
	}
	if len(rowOrder) != len(members.ordered) {
		return nil, newDecodeError(ErrorReasonRowOrder)
	}
	seen := make(map[int32]struct{}, len(rowOrder))
	for _, index := range rowOrder {
		if _, member := members.lookup[index]; !member {
			return nil, newDecodeError(ErrorReasonRowOrder)
		}
		if _, duplicate := seen[index]; duplicate {
			return nil, newDecodeError(ErrorReasonRowOrder)
		}
		seen[index] = struct{}{}
	}
	return append([]int32(nil), rowOrder...), nil
}

func validPhase(phase v1beta1.OMENativeInstancePhase) bool {
	switch phase {
	case v1beta1.OMENativeInstancePending,
		v1beta1.OMENativeInstanceCreating,
		v1beta1.OMENativeInstanceReady,
		v1beta1.OMENativeInstanceUpdating,
		v1beta1.OMENativeInstanceRestarting,
		v1beta1.OMENativeInstanceMigrating,
		v1beta1.OMENativeInstanceFailed,
		v1beta1.OMENativeInstanceDeleting:
		return true
	default:
		return false
	}
}
