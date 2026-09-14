package projection

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/tree"
)

const (
	origin  = "hub-1"
	cluster = "member-a"
	tpu     = "google.com/tpu"
	flavor  = "tpu7x"
)

var cmpQuantity = cmp.Comparer(func(a, b resource.Quantity) bool { return a.Cmp(b) == 0 })

func qty(s string) resource.Quantity { return resource.MustParse(s) }

func qtyPtr(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}

// budget builds a source budget, optionally with the pass-through limits and the
// split inputs a projection must drop.
func budget(nominal string, opts ...func(*v1beta1.AcceleratorBudget)) v1beta1.AcceleratorBudget {
	b := v1beta1.AcceleratorBudget{ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty(nominal)}
	for _, o := range opts {
		o(&b)
	}
	return b
}

func withLimits(borrow, lend string) func(*v1beta1.AcceleratorBudget) {
	return func(b *v1beta1.AcceleratorBudget) {
		b.BorrowingLimit = qtyPtr(borrow)
		b.LendingLimit = qtyPtr(lend)
	}
}

func withSplit(b *v1beta1.AcceleratorBudget) {
	b.Policy = v1beta1.AcceleratorQuotaDistributionExplicit
	b.PerCluster = []v1beta1.AcceleratorClusterShare{
		{Cluster: "member-a", Nominal: qty("60")},
		{Cluster: "member-b", Nominal: qty("40")},
	}
}

func node(name, parent string, role v1beta1.AcceleratorQuotaRole,
	budgets ...v1beta1.AcceleratorBudget,
) v1beta1.AcceleratorQuota {
	q := v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			UID:        types.UID("uid-" + name),
			Generation: 7,
		},
		Spec: v1beta1.AcceleratorQuotaSpec{
			Role:    role,
			Budgets: budgets,
		},
	}
	if parent != "" {
		q.Spec.ParentRef = &v1beta1.AcceleratorQuotaParentRef{Name: parent}
	}
	return q
}

// fleet is root -> org -> {team-a, team-b}, the smallest tree with a grouping
// tier to project and a sibling to leave out.
func fleet(t *testing.T, leafBudgets ...v1beta1.AcceleratorBudget) *tree.Tree {
	t.Helper()
	if len(leafBudgets) == 0 {
		leafBudgets = []v1beta1.AcceleratorBudget{budget("100")}
	}
	nodes := []v1beta1.AcceleratorQuota{
		node("root", "", v1beta1.AcceleratorQuotaRoleCohort),
		node("org", "root", v1beta1.AcceleratorQuotaRoleCohort, budget("200")),
		node("team-a", "org", v1beta1.AcceleratorQuotaRoleClusterQueue, leafBudgets...),
		node("team-b", "org", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("50")),
	}
	built, _, err := tree.Build(nodes, tree.Options{RootName: "root", MaxDepth: 5})
	if err != nil {
		t.Fatalf("tree.Build() error = %v", err)
	}
	return built
}

func names(objs []*v1beta1.AcceleratorQuota) []string {
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		out = append(out, o.Name)
	}
	return out
}

func find(objs []*v1beta1.AcceleratorQuota, name string) *v1beta1.AcceleratorQuota {
	for _, o := range objs {
		if o.Name == name {
			return o
		}
	}
	return nil
}

// Which nodes travel is decided by the arithmetic, never by a selector. Getting
// the set wrong is silent in both directions: a missing ancestor leaves the
// leaf's parentRef dangling so the member's tree never assembles, and an extra
// leaf creates a queue for a tenant that cluster cannot serve.
func TestForSelectsTheMatchedSet(t *testing.T) {
	tests := []struct {
		name       string
		allowances []Allowance
		want       []string
	}{
		{
			name:       "a matched leaf brings its ancestors but not the root",
			allowances: []Allowance{{Node: "team-a", ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("60")}},
			want:       []string{"org", "team-a"},
		},
		{
			name: "two matched leaves share one ancestor",
			allowances: []Allowance{
				{Node: "team-a", ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("60")},
				{Node: "team-b", ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("40")},
			},
			want: []string{"org", "team-a", "team-b"},
		},
		{
			// Under Proportional a cluster with none of the flavor is apportioned
			// exactly zero. Projecting it would create a queue that admits
			// nothing; the arithmetic is what excludes it.
			name:       "a leaf apportioned nothing is not projected",
			allowances: []Allowance{{Node: "team-a", ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("0")}},
			want:       []string{},
		},
		{
			// And its ancestors go with it: an org tier alone would be topology
			// for a tenant that is not there.
			name: "a cluster with nothing matched receives nothing at all",
			allowances: []Allowance{
				{Node: "team-a", ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("0")},
				{Node: "team-b", ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("0")},
			},
			want: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := For(fleet(t), cluster, tc.allowances, Options{Origin: origin})
			if err != nil {
				t.Fatalf("For() error = %v", err)
			}
			if diff := cmp.Diff(tc.want, names(got)); diff != "" {
				t.Errorf("projected set mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// A projection is a resolved number wearing the source's shape. Everything that
// describes how the number was reached is dropped, because a member that could
// see the split could re-derive a different one.
func TestForRendersALeaf(t *testing.T) {
	src := fleet(t, budget("100", withLimits("20", "10"), withSplit))
	got, err := For(src, cluster,
		[]Allowance{{Node: "team-a", ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("60")}},
		Options{Origin: origin})
	if err != nil {
		t.Fatalf("For() error = %v", err)
	}

	leaf := find(got, "team-a")
	if leaf == nil {
		t.Fatal("team-a was not projected")
	}

	want := v1beta1.AcceleratorQuotaSpec{
		Role:      v1beta1.AcceleratorQuotaRoleClusterQueue,
		ParentRef: &v1beta1.AcceleratorQuotaParentRef{Name: "org"},
		Budgets: []v1beta1.AcceleratorBudget{{
			ResourceName:   tpu,
			ResourceFlavor: flavor,
			// The share, not the fleet total.
			Nominal: qty("60"),
			// Cluster-local limits Kueue enforces on the member, so they pass
			// through untouched rather than being split.
			BorrowingLimit: qtyPtr("20"),
			LendingLimit:   qtyPtr("10"),
			// Policy and PerCluster deliberately absent.
		}},
	}
	if diff := cmp.Diff(want, leaf.Spec, cmpQuantity); diff != "" {
		t.Errorf("projected leaf spec mismatch (-want +got):\n%s", diff)
	}

	wantMeta := map[string]string{
		v1beta1.AcceleratorQuotaOriginUIDAnnotation:        "uid-team-a",
		v1beta1.AcceleratorQuotaSourceGenerationAnnotation: "7",
		v1beta1.AcceleratorQuotaClusterAnnotation:          cluster,
	}
	if diff := cmp.Diff(wantMeta, leaf.Annotations); diff != "" {
		t.Errorf("projection marks mismatch (-want +got):\n%s", diff)
	}
	if got := leaf.Labels[v1beta1.AcceleratorQuotaOriginLabel]; got != origin {
		t.Errorf("origin label = %q, want %q", got, origin)
	}
}

// An ancestor travels for one reason: so the leaf's parentRef resolves. It
// carries no allowance of its own, because a fleet total means nothing on one
// cluster and a budget-less node is unconstrained for containment there.
func TestForRendersAnAncestorAsTopologyOnly(t *testing.T) {
	got, err := For(fleet(t), cluster,
		[]Allowance{{Node: "team-a", ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("60")}},
		Options{Origin: origin})
	if err != nil {
		t.Fatalf("For() error = %v", err)
	}

	org := find(got, "org")
	if org == nil {
		t.Fatal("org was not projected")
	}
	want := v1beta1.AcceleratorQuotaSpec{
		Role:      v1beta1.AcceleratorQuotaRoleCohort,
		ParentRef: &v1beta1.AcceleratorQuotaParentRef{Name: "root"},
	}
	if diff := cmp.Diff(want, org.Spec, cmpQuantity); diff != "" {
		t.Errorf("projected ancestor spec mismatch (-want +got):\n%s", diff)
	}
	if org.Labels[v1beta1.AcceleratorQuotaOriginLabel] != origin {
		t.Error("an ancestor was projected without the origin mark, so a sweep could not reap it")
	}
}

// The member's own controller creates the root and derives that cluster's
// capacity onto its status. A projected root would put two writers on one
// object, which is exactly what the origin marking exists to prevent.
func TestForNeverProjectsTheRoot(t *testing.T) {
	got, err := For(fleet(t), cluster,
		[]Allowance{
			{Node: "team-a", ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("60")},
			{Node: "team-b", ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("40")},
		},
		Options{Origin: origin})
	if err != nil {
		t.Fatalf("For() error = %v", err)
	}
	if find(got, "root") != nil {
		t.Error("the root was projected; the member creates its own")
	}
}

// What cannot be rendered must say so rather than emit a partial tree: a
// projection is applied with server-side apply, so a wrong object is not a
// failed write but a wrong budget a member will happily enforce.
func TestForRejects(t *testing.T) {
	tests := []struct {
		name       string
		cluster    string
		origin     string
		allowances []Allowance
		wantErr    string
	}{
		{
			name: "no origin to mark the copy with", cluster: cluster, origin: "",
			allowances: []Allowance{{Node: "team-a", ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("1")}},
			wantErr:    "no origin configured",
		},
		{
			name: "no cluster named", cluster: "", origin: origin,
			allowances: []Allowance{{Node: "team-a", ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("1")}},
			wantErr:    "no cluster named",
		},
		{
			name: "an allowance for a node that is not in the tree", cluster: cluster, origin: origin,
			allowances: []Allowance{{Node: "ghost", ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("1")}},
			wantErr:    "not in the tree",
		},
		{
			// A grouping tier materializes no queue, so a share there has
			// nowhere to land.
			name: "an allowance on a grouping tier", cluster: cluster, origin: origin,
			allowances: []Allowance{{Node: "org", ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("1")}},
			wantErr:    "not a leaf",
		},
		{
			// Resolving a budget the source never wrote would invent an
			// allowance nobody authorized.
			name: "an allowance for a budget the node does not have", cluster: cluster, origin: origin,
			allowances: []Allowance{{Node: "team-a", ResourceName: "nvidia.com/gpu", ResourceFlavor: flavor, Nominal: qty("1")}},
			wantErr:    "no budget for",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := For(fleet(t), tc.cluster, tc.allowances, Options{Origin: tc.origin})
			if err == nil {
				t.Fatalf("For() = %v, want an error naming %q", names(got), tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("For() = %q, want it to name %q", err, tc.wantErr)
			}
		})
	}
}

// Two passes over unchanged inputs must render the same objects, or every pass
// would rewrite every projection in the fleet and re-materialize every queue.
func TestForIsDeterministicAndDoesNotMutate(t *testing.T) {
	src := fleet(t, budget("100", withLimits("20", "10"), withSplit))
	allowances := []Allowance{
		{Node: "team-b", ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("40")},
		{Node: "team-a", ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("60")},
	}

	first, err := For(src, cluster, allowances, Options{Origin: origin})
	if err != nil {
		t.Fatalf("For() error = %v", err)
	}
	second, err := For(src, cluster, allowances, Options{Origin: origin})
	if err != nil {
		t.Fatalf("second For() error = %v", err)
	}
	if diff := cmp.Diff(first, second, cmpQuantity); diff != "" {
		t.Errorf("rendering is not deterministic (-first +second):\n%s", diff)
	}

	// The source keeps its split: rendering reads the tree, and a tree shared
	// with every other cluster's pass cannot be edited by one of them.
	team, _ := src.Node("team-a")
	if len(team.Quota.Spec.Budgets[0].PerCluster) != 2 {
		t.Error("rendering stripped the split from the source rather than from the copy")
	}
	if team.Quota.Spec.Budgets[0].Nominal.Cmp(qty("100")) != 0 {
		t.Error("rendering overwrote the source's fleet total with a share")
	}
}

// The copies are applied to a member one at a time, and the member's webhook
// rejects a node whose parentRef resolves to nothing yet — so the order For
// returns them in is part of the contract, not a presentation detail.
//
// Sorting by name alone satisfies this only when every parent happens to sort
// before its children; a parent whose name sorts after its child's breaks it.
func TestForOrdersParentsBeforeChildren(t *testing.T) {
	tests := []struct {
		name  string
		nodes []v1beta1.AcceleratorQuota
		leaf  string
		want  []string
	}{
		{
			name: "a leaf whose name sorts before its parent",
			nodes: []v1beta1.AcceleratorQuota{
				node("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				node("sim-tenants", "root", v1beta1.AcceleratorQuotaRoleCohort),
				node("sim-serving", "sim-tenants", v1beta1.AcceleratorQuotaRoleClusterQueue,
					budget("100")),
			},
			leaf: "sim-serving",
			want: []string{"sim-tenants", "sim-serving"},
		},
		{
			name: "a leaf whose name sorts after its parent",
			nodes: []v1beta1.AcceleratorQuota{
				node("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				node("org", "root", v1beta1.AcceleratorQuotaRoleCohort),
				node("team-a", "org", v1beta1.AcceleratorQuotaRoleClusterQueue,
					budget("100")),
			},
			leaf: "team-a",
			want: []string{"org", "team-a"},
		},
		{
			// Every tier reversed against the alphabet, so a sort that is not
			// depth-first gets all of them wrong rather than one.
			name: "every tier of a deep tree sorts against the alphabet",
			nodes: []v1beta1.AcceleratorQuota{
				node("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				node("ccc", "root", v1beta1.AcceleratorQuotaRoleCohort),
				node("bbb", "ccc", v1beta1.AcceleratorQuotaRoleCohort),
				node("aaa", "bbb", v1beta1.AcceleratorQuotaRoleClusterQueue,
					budget("100")),
			},
			leaf: "aaa",
			want: []string{"ccc", "bbb", "aaa"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			built, _, err := tree.Build(tc.nodes, tree.Options{RootName: "root", MaxDepth: 5})
			if err != nil {
				t.Fatalf("tree.Build() error = %v", err)
			}
			got, err := For(built, cluster,
				[]Allowance{{Node: tc.leaf, ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("60")}},
				Options{Origin: origin})
			if err != nil {
				t.Fatalf("For() error = %v", err)
			}
			if diff := cmp.Diff(tc.want, names(got)); diff != "" {
				t.Errorf("apply order mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
