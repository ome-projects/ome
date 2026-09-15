package v1alpha1

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

const QuotaTreeReportKind = "QuotaTreeReport"

// QuotaTreeSnapshot describes the cluster-scoped acquisition, not controller state.
type QuotaTreeSnapshot struct {
	Scope         string        `json:"scope"`
	Completeness  string        `json:"completeness"`
	ObservedPages int           `json:"observedPages"`
	ObservedItems int           `json:"observedItems"`
	Evidence      EvidenceLevel `json:"evidence"`
}

// QuotaTreeContent is an advisory description of declared topology and budgets.
type QuotaTreeContent struct {
	Target    string             `json:"target,omitempty"`
	Snapshot  QuotaTreeSnapshot  `json:"snapshot"`
	Structure string             `json:"structure"`
	Nodes     []QuotaTreeNode    `json:"nodes"`
	Problems  []QuotaTreeProblem `json:"problems"`
}

// QuotaTreeNode has no arbitrary metadata or controller messages. ComputedPath
// is computed from declared parent references; it is never an API status path.
type QuotaTreeNode struct {
	Name             string            `json:"name"`
	Role             string            `json:"role"`
	Tenant           string            `json:"tenant,omitempty"`
	ParentName       string            `json:"parentName,omitempty"`
	ComputedPath     string            `json:"computedPath,omitempty"`
	Depth            int               `json:"depth"`
	Selected         bool              `json:"selected"`
	Position         string            `json:"position"`
	TopologyEvidence EvidenceLevel     `json:"topologyEvidence"`
	PositionEvidence EvidenceLevel     `json:"positionEvidence"`
	PriorityTier     string            `json:"priorityTier,omitempty"`
	Distribution     string            `json:"distribution"`
	Budgets          []QuotaTreeBudget `json:"budgets"`
}

// QuotaTreeBudget retains only declared budget fields and their provenance.
type QuotaTreeBudget struct {
	ResourceName   string                  `json:"resourceName"`
	ResourceFlavor string                  `json:"resourceFlavor"`
	Nominal        string                  `json:"nominal"`
	BorrowingLimit string                  `json:"borrowingLimit,omitempty"`
	LendingLimit   string                  `json:"lendingLimit,omitempty"`
	Policy         string                  `json:"policy"`
	PolicySource   string                  `json:"policySource"`
	PerCluster     []QuotaTreeClusterShare `json:"perCluster"`
	Evidence       EvidenceLevel           `json:"evidence"`
}

type QuotaTreeClusterShare struct {
	Cluster string `json:"cluster"`
	Nominal string `json:"nominal"`
}

// QuotaTreeProblem has a fixed reason vocabulary. No upstream error or
// condition messages enter the report; problems describe the entire snapshot.
type QuotaTreeProblem struct {
	Node     string        `json:"node"`
	Reason   string        `json:"reason"`
	Evidence EvidenceLevel `json:"evidence"`
}

// Canonical deeply copies and totally orders even unknown caller values.
func (c QuotaTreeContent) Canonical() QuotaTreeContent {
	c.Nodes = append([]QuotaTreeNode{}, c.Nodes...)
	for i := range c.Nodes {
		n := &c.Nodes[i]
		n.Budgets = append([]QuotaTreeBudget{}, n.Budgets...)
		for j := range n.Budgets {
			b := &n.Budgets[j]
			b.PerCluster = append([]QuotaTreeClusterShare{}, b.PerCluster...)
			quotaSort(b.PerCluster)
		}
		quotaSort(n.Budgets)
	}
	sort.Slice(c.Nodes, func(i, j int) bool {
		a, b := c.Nodes[i], c.Nodes[j]
		if (a.Depth >= 0) != (b.Depth >= 0) {
			return a.Depth >= 0
		}
		if a.ComputedPath != b.ComputedPath {
			return a.ComputedPath < b.ComputedPath
		}
		return quotaKey(a) < quotaKey(b)
	})
	c.Problems = append([]QuotaTreeProblem{}, c.Problems...)
	quotaSort(c.Problems)
	return c
}

func quotaKey[T any](v T) string { data, _ := json.Marshal(v); return string(data) }
func quotaSort[T any](values []T) {
	sort.Slice(values, func(i, j int) bool { return quotaKey(values[i]) < quotaKey(values[j]) })
}

// Table provides a physically bounded, single-column tree. Each clipped line
// includes a digest so distinct long identities remain distinguishable.
func (c QuotaTreeContent) Table() report.Table {
	c = c.Canonical()
	rows := [][]string{{"Declared budgets; computed ancestry; advisory only."},
		{"No admission, enforcement, controller, Kueue, or capacity claim."},
		{fmt.Sprintf("Snapshot: %s; %d items / %d pages; %s", c.Snapshot.Completeness, c.Snapshot.ObservedItems, c.Snapshot.ObservedPages, c.Structure)}}
	if c.Target != "" {
		rows = append(rows, []string{"Selected ancestry and descendants: " + c.Target})
	}
	if len(c.Nodes) == 0 {
		rows = append(rows, []string{"No AcceleratorQuotas observed."})
	}
	for _, n := range c.Nodes {
		depth := n.Depth
		if depth < 0 {
			depth = 0
		}
		if depth > 8 {
			depth = 8
		}
		prefix := strings.Repeat("  ", depth)
		identity := n.Name + " [" + n.Role + "]"
		if n.Depth > 8 {
			identity = fmt.Sprintf("depth=%d %s", n.Depth, identity)
		}
		if n.Tenant != "" {
			identity += " tenant=" + n.Tenant
		}
		if n.Position == "Unresolved" {
			identity += " (unresolved)"
		}
		rows = append(rows, []string{prefix + identity})
		for _, b := range n.Budgets {
			rows = append(rows, []string{prefix + "  budget " + b.ResourceName + "/" + b.ResourceFlavor + "=" + b.Nominal + " (Declared)"})
		}
	}
	for _, p := range c.Problems {
		rows = append(rows, []string{"! " + p.Node + ": " + p.Reason + " (Computed; whole snapshot)"})
	}
	for i := range rows {
		rows[i][0] = quotaClip(rows[i][0])
	}
	return report.Table{Headers: []string{"QUOTA TREE (advisory)"}, Rows: rows}
}

func quotaClip(s string) string {
	s = printers.BoundedCell(s, len(s)*4+1)
	if printers.CellDisplayWidth(s) <= 80 {
		return s
	}
	digest := sha256.Sum256([]byte(s))
	suffix := fmt.Sprintf("#%x", digest[:4])
	runes := []rune(s)
	end := 0
	for end < len(runes) && printers.CellDisplayWidth(string(runes[:end+1])) <= 71 {
		end++
	}
	return string(runes[:end]) + suffix
}

// QuotaTreeWideTable exposes every safe report field, source and warning.
func QuotaTreeWideTable(e Envelope[QuotaTreeContent]) report.Table {
	e = e.Canonical()
	c := e.Content
	rows := [][]string{{"report", e.Kind}, {"collectedAt", e.CollectedAt.Format("2006-01-02T15:04:05.999999999Z07:00")}, {"target", c.Target}, {"snapshot", quotaKey(c.Snapshot)}, {"structure", c.Structure}}
	for _, n := range c.Nodes {
		rows = append(rows, []string{"node", quotaKey(n)})
	}
	for _, p := range c.Problems {
		rows = append(rows, []string{"problem", quotaKey(p)})
	}
	for _, s := range e.Sources {
		rows = append(rows, []string{"source", quotaKey(s)})
	}
	for _, w := range e.Warnings {
		rows = append(rows, []string{"warning", quotaKey(w)})
	}
	return report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}
}
