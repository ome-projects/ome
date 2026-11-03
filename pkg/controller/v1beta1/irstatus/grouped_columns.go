package irstatus

import (
	"cmp"
	"slices"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// groupedColumns lists the grouped columns of the ColumnarV2 payload in
// payload order, which is the order the codec builds, checks and expands
// them. Each entry names the row field a column carries, the payload field
// that stores it and the group type it is stored as.
var groupedColumns = [...]groupedColumn{
	column[v1beta1.OMENativeInstancePhase, v1beta1.InstanceStatusPhaseGroup]{
		required: true,
		field:    func(row *v1beta1.OMENativeInstanceStatus) *v1beta1.OMENativeInstancePhase { return &row.Phase },
		groups: func(columns *v1beta1.InstanceStatusColumns) *[]v1beta1.InstanceStatusPhaseGroup {
			return &columns.Phases
		},
		kind: phaseGroups,
	},
	column[string, v1beta1.InstanceStatusRevisionGroup]{
		field: func(row *v1beta1.OMENativeInstanceStatus) *string { return &row.RunningRevision },
		groups: func(columns *v1beta1.InstanceStatusColumns) *[]v1beta1.InstanceStatusRevisionGroup {
			return &columns.RunningRevisions
		},
		kind: revisionGroups,
	},
	column[string, v1beta1.InstanceStatusRevisionGroup]{
		field: func(row *v1beta1.OMENativeInstanceStatus) *string { return &row.TargetRevision },
		groups: func(columns *v1beta1.InstanceStatusColumns) *[]v1beta1.InstanceStatusRevisionGroup {
			return &columns.TargetRevisions
		},
		kind: revisionGroups,
	},
	column[int64, v1beta1.InstanceStatusIncarnationGroup]{
		field: func(row *v1beta1.OMENativeInstanceStatus) *int64 { return &row.Incarnation },
		groups: func(columns *v1beta1.InstanceStatusColumns) *[]v1beta1.InstanceStatusIncarnationGroup {
			return &columns.Incarnations
		},
		kind: incarnationGroups,
	},
	column[int32, v1beta1.InstanceStatusCountGroup]{
		field: func(row *v1beta1.OMENativeInstanceStatus) *int32 { return &row.PodCount },
		groups: func(columns *v1beta1.InstanceStatusColumns) *[]v1beta1.InstanceStatusCountGroup {
			return &columns.PodCounts
		},
		kind: countGroups,
	},
	column[int32, v1beta1.InstanceStatusCountGroup]{
		field: func(row *v1beta1.OMENativeInstanceStatus) *int32 { return &row.ServingPodCount },
		groups: func(columns *v1beta1.InstanceStatusColumns) *[]v1beta1.InstanceStatusCountGroup {
			return &columns.ServingPodCounts
		},
		kind: countGroups,
	},
	column[int32, v1beta1.InstanceStatusCountGroup]{
		field: func(row *v1beta1.OMENativeInstanceStatus) *int32 { return &row.AvailablePodCount },
		groups: func(columns *v1beta1.InstanceStatusColumns) *[]v1beta1.InstanceStatusCountGroup {
			return &columns.AvailablePodCounts
		},
		kind: countGroups,
	},
}

// groupedColumn is one grouped column as the codec drives it: an encode
// collects it from the rows, a decode validates it and expands it into rows.
type groupedColumn interface {
	newCollector() columnCollector
	validate(columns *v1beta1.InstanceStatusColumns, members indexSet) ([]indexSet, error)
	expand(columns *v1beta1.InstanceStatusColumns, sets []indexSet, rowAt func(int32) *v1beta1.OMENativeInstanceStatus)
}

// columnCollector gathers one column's groups from the rows of one encode
// and stores them on the payload.
type columnCollector interface {
	add(row *v1beta1.OMENativeInstanceStatus)
	store(columns *v1beta1.InstanceStatusColumns, n uint64) error
}

// column is a grouped column whose values are V, stored as groups of type G.
// field points at the row field the column carries and groups at the payload
// field that stores it. A required column covers every member row.
type column[V cmp.Ordered, G any] struct {
	required bool
	field    func(*v1beta1.OMENativeInstanceStatus) *V
	groups   func(*v1beta1.InstanceStatusColumns) *[]G
	kind     groupKind[V, G]
}

// groupKind is what the columns stored as one group type share: the value
// domain, how a group is built, and how its value and index set are read. A
// row contributes to a column only when its value is in the domain, so the
// zero value, which no domain contains, means absent.
type groupKind[V cmp.Ordered, G any] struct {
	inDomain func(V) bool
	newGroup func(V, string) G
	value    func(*G) V
	indexes  func(*G) string
}

var (
	phaseGroups = groupKind[v1beta1.OMENativeInstancePhase, v1beta1.InstanceStatusPhaseGroup]{
		inDomain: supportedPhase,
		newGroup: func(value v1beta1.OMENativeInstancePhase, indexes string) v1beta1.InstanceStatusPhaseGroup {
			return v1beta1.InstanceStatusPhaseGroup{Value: value, Indexes: indexes}
		},
		value:   func(g *v1beta1.InstanceStatusPhaseGroup) v1beta1.OMENativeInstancePhase { return g.Value },
		indexes: func(g *v1beta1.InstanceStatusPhaseGroup) string { return g.Indexes },
	}
	revisionGroups = groupKind[string, v1beta1.InstanceStatusRevisionGroup]{
		inDomain: func(value string) bool { return value != "" },
		newGroup: func(value string, indexes string) v1beta1.InstanceStatusRevisionGroup {
			return v1beta1.InstanceStatusRevisionGroup{Value: value, Indexes: indexes}
		},
		value:   func(g *v1beta1.InstanceStatusRevisionGroup) string { return g.Value },
		indexes: func(g *v1beta1.InstanceStatusRevisionGroup) string { return g.Indexes },
	}
	incarnationGroups = groupKind[int64, v1beta1.InstanceStatusIncarnationGroup]{
		inDomain: func(value int64) bool { return value != 0 },
		newGroup: func(value int64, indexes string) v1beta1.InstanceStatusIncarnationGroup {
			return v1beta1.InstanceStatusIncarnationGroup{Value: value, Indexes: indexes}
		},
		value:   func(g *v1beta1.InstanceStatusIncarnationGroup) int64 { return g.Value },
		indexes: func(g *v1beta1.InstanceStatusIncarnationGroup) string { return g.Indexes },
	}
	countGroups = groupKind[int32, v1beta1.InstanceStatusCountGroup]{
		inDomain: func(value int32) bool { return value > 0 },
		newGroup: func(value int32, indexes string) v1beta1.InstanceStatusCountGroup {
			return v1beta1.InstanceStatusCountGroup{Value: value, Indexes: indexes}
		},
		value:   func(g *v1beta1.InstanceStatusCountGroup) int32 { return g.Value },
		indexes: func(g *v1beta1.InstanceStatusCountGroup) string { return g.Indexes },
	}
)

func (c column[V, G]) newCollector() columnCollector {
	return &columnGroups[V, G]{column: c}
}

func (c column[V, G]) validate(columns *v1beta1.InstanceStatusColumns, members indexSet) ([]indexSet, error) {
	return validateGroupColumn(*c.groups(columns), members, c.required, c.kind)
}

// expand writes each group's value into the rows its index set names.
func (c column[V, G]) expand(columns *v1beta1.InstanceStatusColumns, sets []indexSet, rowAt func(int32) *v1beta1.OMENativeInstanceStatus) {
	groups := *c.groups(columns)
	for i, set := range sets {
		value := c.kind.value(&groups[i])
		set.forEach(func(index int32) { *c.field(rowAt(index)) = value })
	}
}

// columnGroups collects one column's groups during an encode.
type columnGroups[V cmp.Ordered, G any] struct {
	column    column[V, G]
	collected valueGroups[V]
}

func (c *columnGroups[V, G]) add(row *v1beta1.OMENativeInstanceStatus) {
	if value := *c.column.field(row); c.column.kind.inDomain(value) {
		c.collected.add(value, row.Index)
	}
}

func (c *columnGroups[V, G]) store(columns *v1beta1.InstanceStatusColumns, n uint64) error {
	groups, err := buildGroups(&c.collected, n, c.column.kind.newGroup)
	if err != nil {
		return err
	}
	*c.column.groups(columns) = groups
	return nil
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

// validateGroupColumn checks one grouped column: a bounded group count,
// values inside their domain in strictly ascending canonical order, each
// index set canonical, bounded by and contained in members, the groups
// pairwise disjoint, and, for a required column, exact coverage of members.
// An absent optional column is valid; an explicitly empty one is not.
func validateGroupColumn[V cmp.Ordered, G any](groups []G, members indexSet, required bool, kind groupKind[V, G]) ([]indexSet, error) {
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
		value := kind.value(group)
		if !kind.inDomain(value) {
			return nil, newCodecError(ErrorReasonValueDomain)
		}
		if i > 0 && kind.value(&groups[i-1]) >= value {
			return nil, newCodecError(ErrorReasonCanonicalOrder)
		}
		set, err := parseIndexSet(kind.indexes(group), n)
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
