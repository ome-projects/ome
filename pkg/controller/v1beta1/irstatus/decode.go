package irstatus

import (
	"math"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// DecodeColumns validates the complete ColumnarV2 payload and expands it
// into dense rows in the stored row order. Every check that can fail runs
// before the row slice is allocated: index sets are scanned with
// overflow-checked arithmetic, members is bounded by maxInstances, every
// other set and list is bounded by the member count, and coverage, that
// the sets are disjoint, value domains, and canonical order are checked on
// ranges.
// On error no rows are returned. maxInstances has no default; zero fails.
func DecodeColumns(columns *v1beta1.InstanceStatusColumns, maxInstances uint64) ([]v1beta1.OMENativeInstanceStatus, error) {
	if columns == nil {
		return nil, newCodecError(ErrorReasonRepresentationUnion)
	}
	plan, err := validateColumns(columns, maxInstances)
	if err != nil {
		return nil, err
	}
	return plan.expand(columns), nil
}

// decodePlan holds the parsed index sets of a validated payload so that
// expansion neither reparses nor revalidates anything.
type decodePlan struct {
	members       indexSet
	memberOffsets []uint64
	rowCount      int
	rowOrder      []int32

	phases             []indexSet
	runningRevisions   []indexSet
	targetRevisions    []indexSet
	incarnations       []indexSet
	podCounts          []indexSet
	servingPodCounts   []indexSet
	availablePodCounts []indexSet
	admitted           *indexSet
	activeOrdinalOne   *indexSet
}

func validateColumns(columns *v1beta1.InstanceStatusColumns, maxInstances uint64) (*decodePlan, error) {
	if maxInstances == 0 {
		return nil, newCodecError(ErrorReasonCardinalityLimit)
	}
	members, err := parseIndexSet(columns.Members, maxInstances)
	if err != nil {
		return nil, err
	}
	if members.Cardinality() > uint64(math.MaxInt) {
		return nil, newCodecError(ErrorReasonRangeOverflow)
	}
	plan := &decodePlan{members: members, rowCount: int(members.Cardinality())}

	if plan.phases, err = validateGroupColumn(columns.Phases, members, true, phaseGroupAccess); err != nil {
		return nil, err
	}
	if plan.runningRevisions, err = validateGroupColumn(columns.RunningRevisions, members, false, revisionGroupAccess); err != nil {
		return nil, err
	}
	if plan.targetRevisions, err = validateGroupColumn(columns.TargetRevisions, members, false, revisionGroupAccess); err != nil {
		return nil, err
	}
	if plan.incarnations, err = validateGroupColumn(columns.Incarnations, members, false, incarnationGroupAccess); err != nil {
		return nil, err
	}
	if plan.podCounts, err = validateGroupColumn(columns.PodCounts, members, false, countGroupAccess); err != nil {
		return nil, err
	}
	if plan.servingPodCounts, err = validateGroupColumn(columns.ServingPodCounts, members, false, countGroupAccess); err != nil {
		return nil, err
	}
	if plan.availablePodCounts, err = validateGroupColumn(columns.AvailablePodCounts, members, false, countGroupAccess); err != nil {
		return nil, err
	}
	if plan.admitted, err = validateOptionalIndexSet(columns.Admitted, members); err != nil {
		return nil, err
	}
	if plan.activeOrdinalOne, err = validateOptionalIndexSet(columns.ActiveOrdinalOne, members); err != nil {
		return nil, err
	}
	if err = validateEntries(columns.Entries, members); err != nil {
		return nil, err
	}

	// The permutation check needs one flag per member; it runs last, after
	// every bound above has confirmed that the member count is acceptable.
	plan.memberOffsets = members.prefixOffsets()
	if err = validateRowOrder(columns.RowOrder, members, plan.memberOffsets); err != nil {
		return nil, err
	}
	plan.rowOrder = columns.RowOrder
	return plan, nil
}

// groupAccess adapts one typed group slice to the shared column validator.
type groupAccess[G any] struct {
	indexes    func(*G) string
	valueValid func(*G) bool
	ascending  func(previous, current *G) bool
}

var (
	phaseGroupAccess = groupAccess[v1beta1.InstanceStatusPhaseGroup]{
		indexes:    func(g *v1beta1.InstanceStatusPhaseGroup) string { return g.Indexes },
		valueValid: func(g *v1beta1.InstanceStatusPhaseGroup) bool { return supportedPhase(g.Value) },
		ascending:  func(p, c *v1beta1.InstanceStatusPhaseGroup) bool { return p.Value < c.Value },
	}
	revisionGroupAccess = groupAccess[v1beta1.InstanceStatusRevisionGroup]{
		indexes:    func(g *v1beta1.InstanceStatusRevisionGroup) string { return g.Indexes },
		valueValid: func(g *v1beta1.InstanceStatusRevisionGroup) bool { return g.Value != "" },
		ascending:  func(p, c *v1beta1.InstanceStatusRevisionGroup) bool { return p.Value < c.Value },
	}
	incarnationGroupAccess = groupAccess[v1beta1.InstanceStatusIncarnationGroup]{
		indexes:    func(g *v1beta1.InstanceStatusIncarnationGroup) string { return g.Indexes },
		valueValid: func(g *v1beta1.InstanceStatusIncarnationGroup) bool { return g.Value != 0 },
		ascending:  func(p, c *v1beta1.InstanceStatusIncarnationGroup) bool { return p.Value < c.Value },
	}
	countGroupAccess = groupAccess[v1beta1.InstanceStatusCountGroup]{
		indexes:    func(g *v1beta1.InstanceStatusCountGroup) string { return g.Indexes },
		valueValid: func(g *v1beta1.InstanceStatusCountGroup) bool { return g.Value > 0 },
		ascending:  func(p, c *v1beta1.InstanceStatusCountGroup) bool { return p.Value < c.Value },
	}
)

// validateGroupColumn checks one grouped column: a bounded group count,
// values inside their domain in strictly ascending canonical order, each
// index set canonical, bounded by and contained in members, the groups
// pairwise disjoint, and, for a required column, exact coverage of members.
// An absent optional column is valid; an explicitly empty one is not.
func validateGroupColumn[G any](groups []G, members indexSet, required bool, access groupAccess[G]) ([]indexSet, error) {
	n := members.Cardinality()
	if len(groups) == 0 {
		switch {
		case required:
			return nil, newCodecError(ErrorReasonCoverage)
		case groups != nil:
			return nil, newCodecError(ErrorReasonValueDomain)
		default:
			return nil, nil
		}
	}
	if uint64(len(groups)) > n {
		return nil, newCodecError(ErrorReasonCardinalityLimit)
	}

	sets := make([]indexSet, len(groups))
	var covered uint64
	for i := range groups {
		group := &groups[i]
		if !access.valueValid(group) {
			return nil, newCodecError(ErrorReasonValueDomain)
		}
		if i > 0 && !access.ascending(&groups[i-1], group) {
			return nil, newCodecError(ErrorReasonCanonicalOrder)
		}
		set, err := parseIndexSet(access.indexes(group), n)
		if err != nil {
			return nil, err
		}
		if !set.IsSubsetOf(members) {
			return nil, newCodecError(ErrorReasonCoverage)
		}
		covered += set.Cardinality()
		if covered > n {
			return nil, newCodecError(ErrorReasonCoverage)
		}
		sets[i] = set
	}
	if required && covered != n {
		return nil, newCodecError(ErrorReasonCoverage)
	}
	if !disjointIndexSets(sets) {
		return nil, newCodecError(ErrorReasonCoverage)
	}
	return sets, nil
}

func validateOptionalIndexSet(raw *string, members indexSet) (*indexSet, error) {
	if raw == nil {
		return nil, nil
	}
	set, err := parseIndexSet(*raw, members.Cardinality())
	if err != nil {
		return nil, err
	}
	if !set.IsSubsetOf(members) {
		return nil, newCodecError(ErrorReasonCoverage)
	}
	return &set, nil
}

// validateEntries checks that entries are in strictly ascending index order,
// at most one per member, each referencing a member and carrying at least
// one exceptional record.
func validateEntries(entries []v1beta1.InstanceStatusEntry, members indexSet) error {
	if entries == nil {
		return nil
	}
	if len(entries) == 0 {
		return newCodecError(ErrorReasonValueDomain)
	}
	if uint64(len(entries)) > members.Cardinality() {
		return newCodecError(ErrorReasonCardinalityLimit)
	}
	for i := range entries {
		entry := &entries[i]
		if i > 0 {
			switch {
			case entry.Index == entries[i-1].Index:
				return newCodecError(ErrorReasonCoverage)
			case entry.Index < entries[i-1].Index:
				return newCodecError(ErrorReasonCanonicalOrder)
			}
		}
		if !members.Contains(entry.Index) {
			return newCodecError(ErrorReasonCoverage)
		}
		if !entryHasContent(entry) {
			return newCodecError(ErrorReasonCoverage)
		}
	}
	return nil
}

// validateRowOrder checks that a present row order is a permutation of
// members other than the ascending one, which must be spelled by absence.
func validateRowOrder(order []int32, members indexSet, offsets []uint64) error {
	if order == nil {
		return nil
	}
	if len(order) == 0 {
		return newCodecError(ErrorReasonValueDomain)
	}
	if uint64(len(order)) != members.Cardinality() {
		return newCodecError(ErrorReasonCoverage)
	}
	seen := make([]bool, len(order))
	ascending := true
	for i, index := range order {
		rank, ok := members.rankOf(index, offsets)
		if !ok || seen[rank] {
			return newCodecError(ErrorReasonCoverage)
		}
		seen[rank] = true
		if i > 0 && index <= order[i-1] {
			ascending = false
		}
	}
	if ascending {
		return newCodecError(ErrorReasonCanonicalOrder)
	}
	return nil
}

// expand materializes the rows of a validated payload. Nested records are
// copied so the rows share nothing with the payload.
func (p *decodePlan) expand(columns *v1beta1.InstanceStatusColumns) []v1beta1.OMENativeInstanceStatus {
	rows := make([]v1beta1.OMENativeInstanceStatus, p.rowCount)
	var positionByRank []int32
	if p.rowOrder == nil {
		rank := 0
		p.members.forEach(func(index int32) {
			rows[rank].Index = index
			rank++
		})
	} else {
		positionByRank = make([]int32, p.rowCount)
		for position, index := range p.rowOrder {
			rank, _ := p.members.rankOf(index, p.memberOffsets)
			rows[position].Index = index
			positionByRank[rank] = int32(position)
		}
	}
	rowAt := func(index int32) *v1beta1.OMENativeInstanceStatus {
		rank, _ := p.members.rankOf(index, p.memberOffsets)
		if positionByRank != nil {
			return &rows[positionByRank[rank]]
		}
		return &rows[rank]
	}

	for i, set := range p.phases {
		value := columns.Phases[i].Value
		set.forEach(func(index int32) { rowAt(index).Phase = value })
	}
	for i, set := range p.runningRevisions {
		value := columns.RunningRevisions[i].Value
		set.forEach(func(index int32) { rowAt(index).RunningRevision = value })
	}
	for i, set := range p.targetRevisions {
		value := columns.TargetRevisions[i].Value
		set.forEach(func(index int32) { rowAt(index).TargetRevision = value })
	}
	for i, set := range p.incarnations {
		value := columns.Incarnations[i].Value
		set.forEach(func(index int32) { rowAt(index).Incarnation = value })
	}
	for i, set := range p.podCounts {
		value := columns.PodCounts[i].Value
		set.forEach(func(index int32) { rowAt(index).PodCount = value })
	}
	for i, set := range p.servingPodCounts {
		value := columns.ServingPodCounts[i].Value
		set.forEach(func(index int32) { rowAt(index).ServingPodCount = value })
	}
	for i, set := range p.availablePodCounts {
		value := columns.AvailablePodCounts[i].Value
		set.forEach(func(index int32) { rowAt(index).AvailablePodCount = value })
	}
	if p.admitted != nil {
		p.admitted.forEach(func(index int32) { rowAt(index).Admitted = true })
	}
	if p.activeOrdinalOne != nil {
		p.activeOrdinalOne.forEach(func(index int32) { rowAt(index).ActiveOrdinal = 1 })
	}
	for i := range columns.Entries {
		entry := &columns.Entries[i]
		row := rowAt(entry.Index)
		if len(entry.Conditions) > 0 {
			row.Conditions = cloneConditions(entry.Conditions)
		}
		row.ReadySince = entry.ReadySince.DeepCopy()
		row.Operation = entry.Operation.DeepCopy()
		row.LastFailure = entry.LastFailure.DeepCopy()
	}
	return rows
}
