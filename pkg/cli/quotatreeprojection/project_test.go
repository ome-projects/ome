package quotatreeprojection

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	api "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/quotacollection"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

var fixed = v.ClockFunc(func() time.Time { return time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC) })

func node(name, parent, role, nominal string) api.AcceleratorQuota {
	q := api.AcceleratorQuota{ObjectMeta: metav1.ObjectMeta{Name: name, Generation: 1}, Spec: api.AcceleratorQuotaSpec{Role: api.AcceleratorQuotaRole(role)}}
	if parent != "" {
		q.Spec.ParentRef = &api.AcceleratorQuotaParentRef{Name: parent}
	}
	if nominal != "" {
		q.Spec.Budgets = []api.AcceleratorBudget{{ResourceName: "nvidia.com/gpu", ResourceFlavor: "a100", Nominal: resource.MustParse(nominal)}}
	}
	return q
}
func input(q ...api.AcceleratorQuota) Input {
	return Input{Quotas: q, Completeness: quotacollection.Completeness{ObservedPages: 1, ObservedItems: len(q)}}
}

// A leaf's tenant must be metadata.name, and grouping nodes are never tenants.
func TestCurrentAPITreeAndSelection(t *testing.T) {
	in := input(node("team", "org", "ClusterQueue", "8"), node("root", "", "Cohort", "10"), node("org", "root", "Cohort", "8"), node("other", "root", "ClusterQueue", "2"))
	in.Target = "org"
	got, err := Project(in, fixed)
	require.NoError(t, err)
	require.Len(t, got.Content.Nodes, 3)
	assert.Equal(t, []string{"root", "org", "team"}, []string{got.Content.Nodes[0].Name, got.Content.Nodes[1].Name, got.Content.Nodes[2].Name})
	assert.Equal(t, "/root/org/team", got.Content.Nodes[2].ComputedPath)
	assert.Equal(t, "team", got.Content.Nodes[2].Tenant)
	assert.Empty(t, got.Content.Nodes[0].Tenant)
	assert.Empty(t, got.Content.Nodes[1].Tenant)
	assert.Equal(t, "Computed", string(got.Content.Nodes[2].PositionEvidence))
	assert.Equal(t, "Declared", string(got.Content.Nodes[2].Budgets[0].Evidence))
	assert.Len(t, got.Sources, 4)
	assert.Empty(t, got.Content.Problems)
	assert.Equal(t, "Complete", got.Content.Snapshot.Completeness)
	assert.Equal(t, "NoProblemsDetected", got.Content.Structure)
}

// Each reason is an independently expected consequence of the fixture.
func TestDefectsUseSharedTreeSemantics(t *testing.T) {
	tests := []struct {
		name    string
		quotas  []api.AcceleratorQuota
		reasons []string
	}{
		{"empty", nil, []string{"RootMissing"}},
		{"missing", []api.AcceleratorQuota{node("orphan", "missing", "ClusterQueue", "1")}, []string{"ParentMissing", "RootMissing"}},
		{"cycle", []api.AcceleratorQuota{node("a", "b", "Cohort", ""), node("b", "a", "Cohort", "")}, []string{"ParentCycle", "RootMissing"}},
		{"self", []api.AcceleratorQuota{node("root", "root", "Cohort", "")}, []string{"ParentCycle", "RootHasParent"}},
		{"multi-root", []api.AcceleratorQuota{node("root", "", "Cohort", ""), node("other", "", "Cohort", ""), node("child", "other", "ClusterQueue", "1")}, []string{"ParentMissing", "Unreachable"}},
		{"root-role", []api.AcceleratorQuota{node("root", "", "ClusterQueue", "1")}, []string{"RootRoleInvalid"}},
		{"leaf-parent", []api.AcceleratorQuota{node("root", "", "Cohort", ""), node("team", "root", "ClusterQueue", "1"), node("child", "team", "ClusterQueue", "1")}, []string{"NodeKindInvalid"}},
		{"containment", []api.AcceleratorQuota{node("root", "", "Cohort", "1"), node("team", "root", "ClusterQueue", "2")}, []string{"ContainmentViolated"}},
		{"unknown-role", []api.AcceleratorQuota{node("root", "", "TOKEN-secret", "")}, []string{"NodeKindInvalid", "RootRoleInvalid"}},
		{"negative-budget", []api.AcceleratorQuota{node("root", "", "Cohort", "-1")}, []string{"BudgetInvalid"}},
		{"duplicate", []api.AcceleratorQuota{node("root", "", "Cohort", "2"), node("root", "", "Cohort", "1")}, []string{"DuplicateNode"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Project(input(tt.quotas...), fixed)
			require.NoError(t, err)
			reasons := []string{}
			for _, p := range got.Content.Problems {
				reasons = append(reasons, p.Reason)
				assert.Equal(t, v.EvidenceComputed, p.Evidence)
			}
			for _, reason := range tt.reasons {
				assert.Contains(t, reasons, reason)
			}
			assert.Equal(t, "ProblemsDetected", got.Content.Structure)
			data, _ := json.Marshal(got)
			assert.NotContains(t, string(data), "TOKEN-secret")
		})
	}
}

func TestProjectionRefusesUnprovenOrUnsafeSnapshot(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Input)
		want   error
	}{
		{"truncated", func(i *Input) { i.Completeness.Truncated = true }, ErrIncompleteSnapshot},
		{"wrong count", func(i *Input) { i.Completeness.ObservedItems++ }, ErrInvalidSnapshot},
		{"no pages", func(i *Input) { i.Completeness.ObservedPages = 0 }, ErrInvalidSnapshot},
		{"unsafe name", func(i *Input) { i.Quotas[0].Name = "secret\nTOKEN" }, ErrInvalidSnapshot},
		{"long name", func(i *Input) { i.Quotas[0].Name = strings.Repeat("a", 64) }, ErrInvalidSnapshot},
		{"namespace", func(i *Input) { i.Quotas[0].Namespace = "unexpected" }, ErrInvalidSnapshot},
		{"parent", func(i *Input) { i.Quotas[0].Spec.ParentRef = &api.AcceleratorQuotaParentRef{Name: "bad\nparent"} }, ErrInvalidSnapshot},
		{"generation", func(i *Input) { i.Quotas[0].Generation = -1 }, ErrInvalidSnapshot},
		{"unknown target", func(i *Input) { i.Target = "missing" }, ErrTargetNotFound},
		{"unsafe target", func(i *Input) { i.Target = "bad\ntarget" }, ErrInvalidSnapshot},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := input(node("root", "", "Cohort", "1"))
			tt.mutate(&in)
			_, err := Project(in, fixed)
			assert.ErrorIs(t, err, tt.want)
		})
	}
}

func TestWholeSnapshotAuthorityPrivacyAndShuffle(t *testing.T) {
	root := node("root", "", "Cohort", "1")
	root.Annotations = map[string]string{"secret": "annotation-secret"}
	root.Labels = map[string]string{"secret": "label-secret"}
	root.UID = "uid-secret"
	root.ResourceVersion = "rv-secret"
	root.Status.Conditions = []metav1.Condition{{Message: "status-secret", Reason: "condition-secret"}}
	in := input(root, node("a", "root", "ClusterQueue", "1"), node("b", "root", "ClusterQueue", "2"), node("b", "root", "ClusterQueue", "3"))
	in.Target = "a"
	before, _ := json.Marshal(in)
	expected, err := Project(in, fixed)
	require.NoError(t, err)
	want, _ := json.Marshal(expected)
	require.Len(t, expected.Content.Nodes, 2)
	assert.Contains(t, string(want), "ContainmentViolated")
	assert.Contains(t, string(want), "DuplicateNode")
	for _, token := range []string{"annotation-secret", "label-secret", "uid-secret", "rv-secret", "status-secret", "condition-secret"} {
		assert.NotContains(t, string(want), token)
	}
	after, _ := json.Marshal(in)
	assert.Equal(t, string(before), string(after))
	rng := rand.New(rand.NewSource(17))
	for n := 0; n < 40; n++ {
		rng.Shuffle(len(in.Quotas), func(i, j int) { in.Quotas[i], in.Quotas[j] = in.Quotas[j], in.Quotas[i] })
		got, err := Project(in, fixed)
		require.NoError(t, err)
		data, _ := json.Marshal(got)
		assert.Equal(t, string(want), string(data))
	}
}

// Policies, optional limits and shares remain declared facts, with no guessed
// operator default. Invalid local shapes must remain explicit diagnostics.
func TestDeclaredBudgetDetailsAndDefects(t *testing.T) {
	q := node("team", "root", "ClusterQueue", "8")
	q.Spec.PriorityTier = "urgent"
	q.Spec.Distribution = &api.AcceleratorQuotaDistribution{Policy: api.AcceleratorQuotaDistributionProportional}
	q.Spec.Budgets[0].Policy = api.AcceleratorQuotaDistributionExplicit
	borrow, lend := resource.MustParse("2"), resource.MustParse("1")
	q.Spec.Budgets[0].BorrowingLimit = &borrow
	q.Spec.Budgets[0].LendingLimit = &lend
	q.Spec.Budgets[0].PerCluster = []api.AcceleratorClusterShare{{Cluster: "z", Nominal: resource.MustParse("3")}, {Cluster: "a", Nominal: resource.MustParse("5")}}
	q.Spec.Budgets = append(q.Spec.Budgets, api.AcceleratorBudget{ResourceName: "amd.com/gpu", ResourceFlavor: "mi300x", Nominal: resource.MustParse("2")})
	in := input(node("root", "", "Cohort", ""), q)
	got, err := Project(in, fixed)
	require.NoError(t, err)
	assert.Empty(t, got.Content.Problems)
	n := got.Content.Nodes[1]
	assert.Equal(t, "urgent", n.PriorityTier)
	assert.Equal(t, "Proportional", n.Distribution)
	assert.Equal(t, []v.QuotaTreeBudget{
		{ResourceName: "amd.com/gpu", ResourceFlavor: "mi300x", Nominal: "2", Policy: "Proportional", PolicySource: "Node", PerCluster: []v.QuotaTreeClusterShare{}, Evidence: v.EvidenceDeclared},
		{ResourceName: "nvidia.com/gpu", ResourceFlavor: "a100", Nominal: "8", BorrowingLimit: "2", LendingLimit: "1", Policy: "Explicit", PolicySource: "Budget", PerCluster: []v.QuotaTreeClusterShare{{Cluster: "a", Nominal: "5"}, {Cluster: "z", Nominal: "3"}}, Evidence: v.EvidenceDeclared},
	}, n.Budgets)
	// Reversing budgets and share order cannot change representative selection.
	duplicate := *q.DeepCopy()
	duplicate.Spec.Budgets[0], duplicate.Spec.Budgets[1] = duplicate.Spec.Budgets[1], duplicate.Spec.Budgets[0]
	duplicate.Spec.Budgets[1].PerCluster[0], duplicate.Spec.Budgets[1].PerCluster[1] = duplicate.Spec.Budgets[1].PerCluster[1], duplicate.Spec.Budgets[1].PerCluster[0]
	in.Quotas[1] = duplicate
	other, err := Project(in, fixed)
	require.NoError(t, err)
	assert.Equal(t, got, other)
	q.Spec.Distribution.Policy = "secret-policy"
	q.Spec.Budgets[0].Policy = "secret-policy"
	q.Spec.Budgets[0].PerCluster = append(q.Spec.Budgets[0].PerCluster, api.AcceleratorClusterShare{Cluster: "a", Nominal: resource.MustParse("-2")})
	neg := resource.MustParse("-1")
	q.Spec.Budgets[0].LendingLimit = &neg
	q.Spec.Budgets = append(q.Spec.Budgets, q.Spec.Budgets[0])
	bad, err := Project(input(node("root", "", "Cohort", ""), q), fixed)
	require.NoError(t, err)
	assert.Equal(t, []v.QuotaTreeProblem{{Node: "team", Reason: "BudgetInvalid", Evidence: v.EvidenceComputed}, {Node: "team", Reason: "DistributionInvalid", Evidence: v.EvidenceComputed}, {Node: "team", Reason: "DuplicateBudget", Evidence: v.EvidenceComputed}, {Node: "team", Reason: "DuplicateClusterShare", Evidence: v.EvidenceComputed}}, bad.Content.Problems)
	data, _ := json.Marshal(bad)
	assert.NotContains(t, string(data), "secret-policy")
}

func TestInvalidBoundedBudgetIdentities(t *testing.T) {
	for _, mutate := range []func(*api.AcceleratorQuota){
		func(q *api.AcceleratorQuota) { q.Spec.PriorityTier = "Bad_Priority" },
		func(q *api.AcceleratorQuota) { q.Spec.Budgets[0].ResourceName = "secret\nresource" },
		func(q *api.AcceleratorQuota) { q.Spec.Budgets[0].ResourceFlavor = "Bad_Flavor" },
		func(q *api.AcceleratorQuota) {
			q.Spec.Budgets[0].PerCluster = []api.AcceleratorClusterShare{{Cluster: "Bad_Cluster"}}
		},
		func(q *api.AcceleratorQuota) {
			q.Spec.Budgets[0].PerCluster = make([]api.AcceleratorClusterShare, 1001)
		},
		func(q *api.AcceleratorQuota) { q.Spec.Budgets = make([]api.AcceleratorBudget, 17) },
	} {
		q := node("root", "", "Cohort", "1")
		mutate(&q)
		_, err := Project(input(q), fixed)
		assert.ErrorIs(t, err, ErrInvalidSnapshot)
	}
	q := node("root", "", "Cohort", strings.Repeat("9", 70))
	got, err := Project(input(q), fixed)
	require.NoError(t, err)
	assert.Equal(t, "Invalid", got.Content.Nodes[0].Budgets[0].Nominal)
	assert.Equal(t, "BudgetInvalid", got.Content.Problems[0].Reason)
}

func TestSnapshotMaximumAndDeepAncestry(t *testing.T) {
	quotas := []api.AcceleratorQuota{node("root", "", "Cohort", "")}
	parent := "root"
	for i := 1; i < 1000; i++ {
		name := fmt.Sprintf("node-%04d", i)
		quotas = append(quotas, node(name, parent, "Cohort", ""))
		parent = name
	}
	in := input(quotas...)
	in.Target = parent
	got, err := Project(in, fixed)
	require.NoError(t, err)
	require.Len(t, got.Content.Nodes, 1000)
	assert.Equal(t, 999, got.Content.Nodes[999].Depth)
	assert.Empty(t, got.Content.Problems)
	assert.True(t, strings.HasPrefix(got.Content.Nodes[999].ComputedPath, "/root/node-0001/"))
	assert.True(t, strings.HasSuffix(got.Content.Nodes[999].ComputedPath, "/node-0999"))
	in.Quotas = append(in.Quotas, node("extra", parent, "Cohort", ""))
	in.Completeness.ObservedItems++
	_, err = Project(in, fixed)
	assert.ErrorIs(t, err, ErrInvalidSnapshot)
}

// Duplicate-name representatives must not depend on the original ordering of
// nested shares, even when repeated budget identities tie all earlier fields.
func TestDuplicateRepresentativeCanonicalizesNestedSharesBeforeBudgetOrder(t *testing.T) {
	a := node("team", "root", "ClusterQueue", "1")
	share := func(names ...string) []api.AcceleratorClusterShare {
		out := []api.AcceleratorClusterShare{}
		for _, name := range names {
			out = append(out, api.AcceleratorClusterShare{Cluster: name, Nominal: resource.MustParse("1")})
		}
		return out
	}
	a.Spec.Budgets[0].PerCluster = share("z", "a")
	second := a.Spec.Budgets[0]
	second.PerCluster = share("b", "y")
	a.Spec.Budgets = append(a.Spec.Budgets, second)
	b := node("team", "root", "ClusterQueue", "1")
	b.Spec.Budgets[0].PerCluster = share("aa", "bb")
	in := input(node("root", "", "Cohort", ""), a, b)
	first, err := Project(in, fixed)
	require.NoError(t, err)
	in.Quotas[1].Spec.Budgets[0].PerCluster = share("a", "z")
	secondReport, err := Project(in, fixed)
	require.NoError(t, err)
	assert.Equal(t, first, secondReport)
}
