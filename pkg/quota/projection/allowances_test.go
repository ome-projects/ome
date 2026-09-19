package projection

import (
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/tree"
)

// whole builds a Fleet in which every member that appears in the readings is
// also registered and reporting -- the healthy case the tests below are about.
// The incomplete-basis cases construct their Fleet explicitly.
func whole(capacity []Capacity) Fleet {
	seen := map[string]struct{}{}
	var names []string
	for _, c := range capacity {
		if _, ok := seen[c.Cluster]; ok {
			continue
		}
		seen[c.Cluster] = struct{}{}
		names = append(names, c.Cluster)
	}
	sort.Strings(names)
	return Fleet{Registered: names, Reported: names, Capacity: capacity}
}

func cap3(cluster, nominal string) Capacity {
	return Capacity{Cluster: cluster, ResourceName: tpu, ResourceFlavor: flavor, Allocatable: qty(nominal)}
}

func allow(node, nominal string) Allowance {
	return Allowance{Node: node, ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty(nominal)}
}

// oneLeaf is root -> team, the smallest tree that resolves a split.
func oneLeaf(t *testing.T, budgets ...v1beta1.AcceleratorBudget) *tree.Tree {
	t.Helper()
	nodes := []v1beta1.AcceleratorQuota{
		node("root", "", v1beta1.AcceleratorQuotaRoleCohort),
		node("team", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budgets...),
	}
	built, _, err := tree.Build(nodes, tree.Options{RootName: "root", MaxDepth: 5})
	if err != nil {
		t.Fatalf("tree.Build() error = %v", err)
	}
	return built
}

func withPolicy(p v1beta1.AcceleratorQuotaDistributionPolicy) func(*v1beta1.AcceleratorBudget) {
	return func(b *v1beta1.AcceleratorBudget) { b.Policy = p }
}

func withPerCluster(pairs ...[2]string) func(*v1beta1.AcceleratorBudget) {
	return func(b *v1beta1.AcceleratorBudget) {
		for _, p := range pairs {
			b.PerCluster = append(b.PerCluster, v1beta1.AcceleratorClusterShare{
				Cluster: p[0], Nominal: qty(p[1]),
			})
		}
	}
}

// How a fleet total becomes per-cluster numbers, under each policy and each way
// of arriving at one.
func TestResolve(t *testing.T) {
	tests := []struct {
		name     string
		budgets  []v1beta1.AcceleratorBudget
		capacity []Capacity
		opts     ResolveOptions
		want     map[string][]Allowance
	}{
		{
			name: "an explicit split is taken verbatim",
			budgets: []v1beta1.AcceleratorBudget{budget("100",
				withPolicy(v1beta1.AcceleratorQuotaDistributionExplicit),
				withPerCluster([2]string{"member-a", "60"}, [2]string{"member-b", "40"}))},
			want: map[string][]Allowance{
				"member-a": {allow("team", "60")},
				"member-b": {allow("team", "40")},
			},
		},
		{
			name: "a proportional split follows reported capacity",
			budgets: []v1beta1.AcceleratorBudget{budget("120",
				withPolicy(v1beta1.AcceleratorQuotaDistributionProportional))},
			capacity: []Capacity{cap3("member-a", "3"), cap3("member-b", "1")},
			want: map[string][]Allowance{
				"member-a": {allow("team", "90")},
				"member-b": {allow("team", "30")},
			},
		},
		{
			// A cluster reporting none of the flavor takes zero, which is what
			// keeps the projector from creating a queue that admits nothing.
			name: "a cluster without the flavor is apportioned zero",
			budgets: []v1beta1.AcceleratorBudget{budget("100",
				withPolicy(v1beta1.AcceleratorQuotaDistributionProportional))},
			capacity: []Capacity{cap3("member-a", "8"), cap3("member-b", "0")},
			want: map[string][]Allowance{
				"member-a": {allow("team", "100")},
				"member-b": {allow("team", "0")},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Resolve(oneLeaf(t, tc.budgets...), whole(tc.capacity), tc.opts)
			if len(got.Unresolved) != 0 {
				t.Fatalf("Resolve() left nodes unresolved: %v", got.Unresolved)
			}
			if diff := cmp.Diff(tc.want, got.ByCluster, cmpQuantity); diff != "" {
				t.Errorf("Resolve() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// Where the policy comes from, in precedence order. Each layer is a deliberate
// override of the one behind it, and a budget silently taking the wrong one
// would split a tenant's quota by a rule nobody chose.
func TestResolvePolicyPrecedence(t *testing.T) {
	capacity := []Capacity{cap3("member-a", "3"), cap3("member-b", "1")}
	explicitSplit := withPerCluster([2]string{"member-a", "10"}, [2]string{"member-b", "90"})

	tests := []struct {
		name    string
		budget  v1beta1.AcceleratorBudget
		nodePol v1beta1.AcceleratorQuotaDistributionPolicy
		opts    ResolveOptions
		want    map[string]string // cluster -> nominal
	}{
		{
			// Explicit on the budget beats Proportional on the node: 10/90, not
			// the 75/25 the capacities would give.
			name:    "the budget's own policy wins",
			budget:  budget("100", withPolicy(v1beta1.AcceleratorQuotaDistributionExplicit), explicitSplit),
			nodePol: v1beta1.AcceleratorQuotaDistributionProportional,
			want:    map[string]string{"member-a": "10", "member-b": "90"},
		},
		{
			name:    "the node's policy applies when the budget declares none",
			budget:  budget("100"),
			nodePol: v1beta1.AcceleratorQuotaDistributionProportional,
			want:    map[string]string{"member-a": "75", "member-b": "25"},
		},
		{
			name:   "the fleet default applies when neither does",
			budget: budget("100"),
			opts:   ResolveOptions{DefaultPolicy: v1beta1.AcceleratorQuotaDistributionProportional},
			want:   map[string]string{"member-a": "75", "member-b": "25"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodes := []v1beta1.AcceleratorQuota{
				node("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				node("team", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, tc.budget),
			}
			if tc.nodePol != "" {
				nodes[1].Spec.Distribution = &v1beta1.AcceleratorQuotaDistribution{Policy: tc.nodePol}
			}
			built, _, err := tree.Build(nodes, tree.Options{RootName: "root", MaxDepth: 5})
			if err != nil {
				t.Fatalf("tree.Build() error = %v", err)
			}

			got := Resolve(built, whole(capacity), tc.opts)
			if len(got.Unresolved) != 0 {
				t.Fatalf("Resolve() left nodes unresolved: %v", got.Unresolved)
			}
			for cluster, nominal := range tc.want {
				a := got.ByCluster[cluster]
				if len(a) != 1 {
					t.Fatalf("cluster %s got %d allowances, want 1", cluster, len(a))
				}
				if a[0].Nominal.Cmp(qty(nominal)) != 0 {
					t.Errorf("cluster %s got %s, want %s", cluster, a[0].Nominal.String(), nominal)
				}
			}
		})
	}
}

// A leaf that cannot be split is skipped and explained, never half-projected.
// Putting a real budget on some clusters and silently omitting it on others
// reads as a working tenant that is quietly missing capacity.
func TestResolveUnresolved(t *testing.T) {
	tests := []struct {
		name     string
		budgets  []v1beta1.AcceleratorBudget
		capacity []Capacity
		opts     ResolveOptions
		wantWhy  string
	}{
		{
			name:    "no policy anywhere",
			budgets: []v1beta1.AcceleratorBudget{budget("100")},
			wantWhy: "no distribution policy",
		},
		{
			name: "explicit with no shares named",
			budgets: []v1beta1.AcceleratorBudget{budget("100",
				withPolicy(v1beta1.AcceleratorQuotaDistributionExplicit))},
			wantWhy: "names no per-cluster shares",
		},
		{
			// Re-checked on every pass rather than trusted from admission: a
			// hub re-splits continuously and the source can be edited between
			// passes. Short strands capacity; over hands out more than the
			// admin authorized.
			name: "explicit shares that do not add up",
			budgets: []v1beta1.AcceleratorBudget{budget("100",
				withPolicy(v1beta1.AcceleratorQuotaDistributionExplicit),
				withPerCluster([2]string{"member-a", "60"}, [2]string{"member-b", "30"}))},
			wantWhy: "sum to 90",
		},
		{
			name: "explicit naming one cluster twice",
			budgets: []v1beta1.AcceleratorBudget{budget("100",
				withPolicy(v1beta1.AcceleratorQuotaDistributionExplicit),
				withPerCluster([2]string{"member-a", "60"}, [2]string{"member-a", "40"}))},
			wantWhy: "twice",
		},
		{
			// Not a misconfiguration: members report capacity on their own
			// roots, so before they are up there is nothing to divide by. The
			// message says so, because this one clears itself.
			name: "proportional before any member has reported",
			budgets: []v1beta1.AcceleratorBudget{budget("100",
				withPolicy(v1beta1.AcceleratorQuotaDistributionProportional))},
			wantWhy: "has reported capacity",
		},
		{
			name: "proportional where every reported capacity is zero",
			budgets: []v1beta1.AcceleratorBudget{budget("100",
				withPolicy(v1beta1.AcceleratorQuotaDistributionProportional))},
			capacity: []Capacity{cap3("member-a", "0"), cap3("member-b", "0")},
			wantWhy:  "nothing to be proportional to",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Resolve(oneLeaf(t, tc.budgets...), whole(tc.capacity), tc.opts)
			why, ok := got.Unresolved["team"]
			if !ok {
				t.Fatalf("Resolve() resolved a leaf it should not have: %v", got.ByCluster)
			}
			if !strings.Contains(why, tc.wantWhy) {
				t.Errorf("reason = %q, want it to name %q", why, tc.wantWhy)
			}
			if len(got.ByCluster) != 0 {
				t.Errorf("an unresolved leaf was still projected somewhere: %v", got.ByCluster)
			}
		})
	}
}

// One misauthored tenant must not cost every other tenant its quota, so a pass
// carries the good leaves and reports the bad one.
func TestResolveIsolatesAFailedLeaf(t *testing.T) {
	nodes := []v1beta1.AcceleratorQuota{
		node("root", "", v1beta1.AcceleratorQuotaRoleCohort),
		node("good", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("100", withPolicy(v1beta1.AcceleratorQuotaDistributionProportional))),
		// Explicit, and the shares do not add up.
		node("bad", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("100", withPolicy(v1beta1.AcceleratorQuotaDistributionExplicit),
			withPerCluster([2]string{"member-a", "1"}))),
	}
	built, _, err := tree.Build(nodes, tree.Options{RootName: "root", MaxDepth: 5})
	if err != nil {
		t.Fatalf("tree.Build() error = %v", err)
	}

	got := Resolve(built, whole([]Capacity{cap3("member-a", "1")}), ResolveOptions{})

	if _, bad := got.Unresolved["bad"]; !bad {
		t.Error("the misauthored leaf was not reported")
	}
	if _, stillProjected := got.Unresolved["good"]; stillProjected {
		t.Error("a sound leaf was dropped because a sibling was misauthored")
	}
	a := got.ByCluster["member-a"]
	if len(a) != 1 || a[0].Node != "good" {
		t.Errorf("member-a got %v, want only the sound leaf's allowance", a)
	}
}

// A proportional split divides the fleet total by the fleet. If the basis is
// missing a registered member, the total is divided among whoever answered and
// every survivor's share grows to cover the absence -- while the absent member
// still holds the projection it last received. The fleet has then granted more
// than the admin authorized, and the excess surfaces only when the member
// returns.
//
// Worked example: a 96-chip tenant split 64/32 becomes 96 on the survivor
// while the absent member keeps its 32.
func TestResolveHoldsAnIncompleteBasis(t *testing.T) {
	proportional := ResolveOptions{DefaultPolicy: v1beta1.AcceleratorQuotaDistributionProportional}

	tests := []struct {
		name  string
		fleet Fleet
		opts  ResolveOptions
		want  map[string][]Allowance
		held  bool
	}{
		{
			name: "every registered member reported",
			fleet: Fleet{
				Registered: []string{"member-a", "member-b"},
				Reported:   []string{"member-a", "member-b"},
				Capacity:   []Capacity{cap3("member-a", "2"), cap3("member-b", "1")},
			},
			opts: proportional,
			want: map[string][]Allowance{
				"member-a": {allow("team", "80")},
				"member-b": {allow("team", "40")},
			},
		},
		{
			// The whole bug. Without the hold, member-a takes all 120.
			name: "a registered member that did not report holds the split",
			fleet: Fleet{
				Registered: []string{"member-a", "member-b"},
				Reported:   []string{"member-a"},
				Capacity:   []Capacity{cap3("member-a", "2")},
			},
			opts: proportional,
			held: true,
		},
		{
			// Answering with none of the budgeted flavor is a reading, not a
			// silence: that member is apportioned nothing and the rest of the
			// fleet splits the total between them.
			name: "a member that reported no capacity for the flavor is not silent",
			fleet: Fleet{
				Registered: []string{"member-a", "member-b"},
				Reported:   []string{"member-a", "member-b"},
				Capacity:   []Capacity{cap3("member-a", "2")},
			},
			opts: proportional,
			want: map[string][]Allowance{
				"member-a": {allow("team", "120")},
			},
		},
		{
			// Explicit is the admin's own arithmetic and needs no reading, so a
			// silent member cannot make it wrong.
			name: "an explicit split is unaffected by a silent member",
			fleet: Fleet{
				Registered: []string{"member-a", "member-b"},
				Reported:   []string{"member-a"},
				Capacity:   []Capacity{cap3("member-a", "2")},
			},
			opts: ResolveOptions{DefaultPolicy: v1beta1.AcceleratorQuotaDistributionExplicit},
			want: map[string][]Allowance{
				"member-a": {allow("team", "70")},
				"member-b": {allow("team", "50")},
			},
		},
		{
			// Nobody has come up yet. Still held, and for the same reason, but
			// this one clears itself.
			name: "no member reported at all",
			fleet: Fleet{
				Registered: []string{"member-a", "member-b"},
			},
			opts: proportional,
			held: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			budget := v1beta1.AcceleratorBudget{
				ResourceName:   tpu,
				ResourceFlavor: flavor,
				Nominal:        qty("120"),
			}
			if tc.opts.DefaultPolicy == v1beta1.AcceleratorQuotaDistributionExplicit {
				budget.PerCluster = []v1beta1.AcceleratorClusterShare{
					{Cluster: "member-a", Nominal: qty("70")},
					{Cluster: "member-b", Nominal: qty("50")},
				}
			}

			got := Resolve(oneLeaf(t, budget), tc.fleet, tc.opts)

			if tc.held {
				if _, ok := got.Unresolved["team"]; !ok {
					t.Fatalf("the split was taken against an incomplete basis: %v", got.ByCluster)
				}
				if len(got.ByCluster) != 0 {
					t.Errorf("a held leaf was still projected: %v", got.ByCluster)
				}
				return
			}
			if len(got.Unresolved) != 0 {
				t.Fatalf("Resolve() left nodes unresolved: %v", got.Unresolved)
			}
			if diff := cmp.Diff(tc.want, got.ByCluster, cmpQuantity); diff != "" {
				t.Errorf("Resolve() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// Allocatable moves whenever a node is cordoned or replaced. Splitting against
// it would move every tenant's share on the whole fleet whenever one node
// anywhere was drained, which is what the damped mark exists to prevent -- and
// what shares.Weight documents its snapshot contract for.
func TestResolveSplitsOnTheHighWaterMark(t *testing.T) {
	mark := func(cluster, allocatable, hwm string) Capacity {
		c := cap3(cluster, allocatable)
		c.HighWaterMark = qty(hwm)
		return c
	}

	tests := []struct {
		name     string
		capacity []Capacity
		want     map[string][]Allowance
	}{
		{
			// member-a is half drained. Its mark, and so its share, holds.
			name:     "a drained member keeps the share its mark earned",
			capacity: []Capacity{mark("member-a", "1", "2"), mark("member-b", "1", "1")},
			want: map[string][]Allowance{
				"member-a": {allow("team", "80")},
				"member-b": {allow("team", "40")},
			},
		},
		{
			// No mark reported: allocatable is all there is to go on.
			name:     "allocatable stands in where no mark is reported",
			capacity: []Capacity{cap3("member-a", "2"), cap3("member-b", "1")},
			want: map[string][]Allowance{
				"member-a": {allow("team", "80")},
				"member-b": {allow("team", "40")},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Resolve(oneLeaf(t, v1beta1.AcceleratorBudget{
				ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty("120"),
			}), whole(tc.capacity), ResolveOptions{
				DefaultPolicy: v1beta1.AcceleratorQuotaDistributionProportional,
			})
			if len(got.Unresolved) != 0 {
				t.Fatalf("Resolve() left nodes unresolved: %v", got.Unresolved)
			}
			if diff := cmp.Diff(tc.want, got.ByCluster, cmpQuantity); diff != "" {
				t.Errorf("Resolve() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
