// Package quotatreeprojection projects a bounded quota snapshot without cluster
// reads. The shared pure quota/tree package owns cross-object invariants.
package quotatreeprojection

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
	api "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/quotacollection"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/quota/tree"
)

var (
	ErrIncompleteSnapshot = errors.New("AcceleratorQuota snapshot is incomplete; topology is unknown")
	ErrInvalidSnapshot    = errors.New("AcceleratorQuota snapshot is invalid; topology is unknown")
	ErrTargetNotFound     = errors.New("AcceleratorQuota target was not found in the complete snapshot")
)

// Input contains the full cluster-scoped snapshot even for a selected target.
type Input struct {
	Quotas       []api.AcceleratorQuota
	Completeness quotacollection.Completeness
	Target       string
}

// Project requires complete acquisition before asserting any structural fact.
// No controller configuration, current capacity or admission state is inferred.
func Project(in Input, clock v.Clock) (v.Envelope[v.QuotaTreeContent], error) {
	empty := v.Envelope[v.QuotaTreeContent]{}
	if in.Completeness.Truncated {
		return empty, ErrIncompleteSnapshot
	}
	if in.Completeness.ObservedPages < 1 || in.Completeness.ObservedPages > 20 || in.Completeness.ObservedItems != len(in.Quotas) || len(in.Quotas) > 1000 || in.Target != "" && !validName(in.Target, 63) {
		return empty, ErrInvalidSnapshot
	}
	quotas := make([]api.AcceleratorQuota, len(in.Quotas))
	for i := range in.Quotas {
		q := in.Quotas[i].DeepCopy()
		if !validQuota(q) {
			return empty, ErrInvalidSnapshot
		}
		// Canonical duplicate selection depends only on relevant declared fields.
		q.Annotations = nil
		q.Labels = nil
		q.Status = api.AcceleratorQuotaStatus{}
		for j := range q.Spec.Budgets {
			shares := q.Spec.Budgets[j].PerCluster
			sort.Slice(shares, func(a, b int) bool { return key(shares[a]) < key(shares[b]) })
		}
		sort.Slice(q.Spec.Budgets, func(i, j int) bool { return key(q.Spec.Budgets[i]) < key(q.Spec.Budgets[j]) })
		quotas[i] = *q
	}
	sort.Slice(quotas, func(i, j int) bool {
		a, b := quotas[i], quotas[j]
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		if key(a.Spec) != key(b.Spec) {
			return key(a.Spec) < key(b.Spec)
		}
		return a.Generation < b.Generation
	})
	assembled, violations, err := tree.Build(quotas, tree.Options{RootName: api.AcceleratorQuotaRootName})
	if err != nil {
		return empty, ErrInvalidSnapshot
	}
	content := v.QuotaTreeContent{Target: in.Target, Structure: "NoProblemsDetected", Snapshot: v.QuotaTreeSnapshot{Scope: "Cluster", Completeness: "Complete", ObservedPages: in.Completeness.ObservedPages, ObservedItems: len(quotas), Evidence: v.EvidenceObserved}}
	add := func(name, reason string) {
		content.Problems = append(content.Problems, v.QuotaTreeProblem{Node: name, Reason: reason, Evidence: v.EvidenceComputed})
	}
	for _, problem := range violations {
		add(problem.Node, problem.Reason)
	}
	root, exists := assembled.Node(api.AcceleratorQuotaRootName)
	if !exists {
		add("root", "RootMissing")
	} else {
		if root.Quota.Spec.ParentRef != nil {
			add("root", "RootHasParent")
		}
		if root.Role() != api.AcceleratorQuotaRoleCohort {
			add("root", "RootRoleInvalid")
		}
	}
	selected := map[string]bool{}
	if in.Target != "" {
		target, ok := assembled.Node(in.Target)
		if !ok {
			return empty, ErrTargetNotFound
		}
		for _, n := range target.Ancestors() {
			selected[n.Name()] = true
		}
		for _, n := range assembled.Subtree(in.Target) {
			selected[n.Name()] = true
		}
	}
	// Schema-level malformed/duplicate budget facts apply to the full snapshot,
	// including every duplicate input, rather than only the selected tree view.
	for i := range quotas {
		for _, reason := range budgetProblems(&quotas[i]) {
			add(quotas[i].Name, reason)
		}
	}
	for _, n := range assembled.Nodes() {
		if in.Target != "" && !selected[n.Name()] {
			continue
		}
		q := n.Quota
		item := v.QuotaTreeNode{Name: q.Name, Role: safeRole(q.Spec.Role), Depth: n.Depth, Selected: q.Name == in.Target, Position: "Unresolved", TopologyEvidence: v.EvidenceDeclared, PositionEvidence: v.EvidenceComputed, PriorityTier: q.Spec.PriorityTier, Distribution: safePolicy(distribution(q))}
		if q.Spec.ParentRef != nil {
			item.ParentName = q.Spec.ParentRef.Name
		}
		if n.IsLeaf() {
			item.Tenant = q.Name
		}
		if n.Reachable() {
			item.Position = "Resolved"
			names := []string{}
			for _, a := range n.Ancestors() {
				names = append(names, a.Name())
			}
			names = append(names, n.Name())
			item.ComputedPath = "/" + strings.Join(names, "/")
		}
		for _, b := range q.Spec.Budgets {
			policy, source := distribution(q), "Node"
			if b.Policy != "" {
				policy = b.Policy
				source = "Budget"
			}
			if policy == "" {
				source = "Unavailable/NotConfigured"
			}
			budget := v.QuotaTreeBudget{ResourceName: b.ResourceName, ResourceFlavor: b.ResourceFlavor, Nominal: quantity(b.Nominal), Policy: safePolicy(policy), PolicySource: source, Evidence: v.EvidenceDeclared}
			if b.BorrowingLimit != nil {
				budget.BorrowingLimit = quantity(*b.BorrowingLimit)
			}
			if b.LendingLimit != nil {
				budget.LendingLimit = quantity(*b.LendingLimit)
			}
			for _, s := range b.PerCluster {
				budget.PerCluster = append(budget.PerCluster, v.QuotaTreeClusterShare{Cluster: s.Cluster, Nominal: quantity(s.Nominal)})
			}
			item.Budgets = append(item.Budgets, budget)
		}
		content.Nodes = append(content.Nodes, item)
	}
	if len(content.Problems) > 0 {
		content.Structure = "ProblemsDetected"
	}
	name := "root"
	if in.Target != "" {
		name = in.Target
	}
	out := v.NewEnvelope(v.QuotaTreeReportKind, v.Metadata{Name: name}, content, clock)
	for _, q := range quotas {
		out.Sources = append(out.Sources, v.SourceReference{Kind: "AcceleratorQuota", Name: q.Name, Generation: q.Generation, Evidence: v.EvidenceDeclared, CollectedAt: out.CollectedAt})
	}
	out.Warnings = []v.Warning{{Code: "AdvisoryOnly", Message: "Declared budgets and computed ancestry do not prove admission, enforcement, controller installation, Kueue integration, materialization, or available capacity."}}
	return out.Canonical(), nil
}

func key[T any](value T) string { data, _ := json.Marshal(value); return string(data) }
func validName(value string, max int) bool {
	return len(value) <= max && len(validation.IsDNS1123Subdomain(value)) == 0
}
func validQuota(q *api.AcceleratorQuota) bool {
	if !validName(q.Name, api.AcceleratorQuotaMaxNameLength) || q.Namespace != "" || q.Generation < 0 || len(q.Spec.Budgets) > 16 {
		return false
	}
	if q.Spec.ParentRef != nil && !validName(q.Spec.ParentRef.Name, 253) {
		return false
	}
	if q.Spec.PriorityTier != "" && !validName(q.Spec.PriorityTier, 63) {
		return false
	}
	for _, b := range q.Spec.Budgets {
		if len(b.ResourceName) > 253 || len(validation.IsQualifiedName(b.ResourceName)) > 0 || !validName(b.ResourceFlavor, 253) || len(b.PerCluster) > 1000 {
			return false
		}
		for _, s := range b.PerCluster {
			if !validName(s.Cluster, 253) {
				return false
			}
		}
	}
	return true
}
func safeRole(role api.AcceleratorQuotaRole) string {
	if role == api.AcceleratorQuotaRoleCohort || role == api.AcceleratorQuotaRoleClusterQueue {
		return string(role)
	}
	return "Unknown"
}
func distribution(q *api.AcceleratorQuota) api.AcceleratorQuotaDistributionPolicy {
	if q.Spec.Distribution != nil {
		return q.Spec.Distribution.Policy
	}
	return ""
}
func safePolicy(policy api.AcceleratorQuotaDistributionPolicy) string {
	switch policy {
	case api.AcceleratorQuotaDistributionExplicit, api.AcceleratorQuotaDistributionProportional:
		return string(policy)
	case "":
		return "Unavailable/NotConfigured"
	default:
		return "Unknown"
	}
}
func quantity(q resource.Quantity) string {
	value := q.String()
	if len(value) > 64 {
		return "Invalid"
	}
	if _, err := resource.ParseQuantity(value); err != nil {
		return "Invalid"
	}
	return value
}

// budgetProblems checks local wire shape. Cross-object role, parent and
// containment rules belong exclusively to quota/tree.Build above.
func budgetProblems(q *api.AcceleratorQuota) []string {
	reasons := map[string]bool{}
	seen := map[string]bool{}
	if safePolicy(distribution(q)) == "Unknown" {
		reasons["DistributionInvalid"] = true
	}
	for _, b := range q.Spec.Budgets {
		id := b.ResourceName + "/" + b.ResourceFlavor
		if seen[id] {
			reasons["DuplicateBudget"] = true
		}
		seen[id] = true
		if safePolicy(b.Policy) == "Unknown" {
			reasons["DistributionInvalid"] = true
		}
		if b.Nominal.Sign() < 0 || quantity(b.Nominal) == "Invalid" {
			reasons["BudgetInvalid"] = true
		}
		for _, limit := range []*resource.Quantity{b.BorrowingLimit, b.LendingLimit} {
			if limit != nil && (limit.Sign() < 0 || quantity(*limit) == "Invalid") {
				reasons["BudgetInvalid"] = true
			}
		}
		clusters := map[string]bool{}
		for _, s := range b.PerCluster {
			if clusters[s.Cluster] {
				reasons["DuplicateClusterShare"] = true
			}
			clusters[s.Cluster] = true
			if s.Nominal.Sign() < 0 || quantity(s.Nominal) == "Invalid" {
				reasons["BudgetInvalid"] = true
			}
		}
	}
	result := []string{}
	for reason := range reasons {
		result = append(result, reason)
	}
	sort.Strings(result)
	return result
}
