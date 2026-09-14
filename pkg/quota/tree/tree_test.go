package tree

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const rootName = "root"

func opts() Options { return Options{RootName: rootName, MaxDepth: 5} }

func qty(s string) resource.Quantity { return resource.MustParse(s) }

func budget(nominal string) v1beta1.AcceleratorBudget {
	return v1beta1.AcceleratorBudget{
		ResourceName:   "google.com/tpu",
		ResourceFlavor: "tpu7x",
		Nominal:        qty(nominal),
	}
}

func gpuBudget(nominal string) v1beta1.AcceleratorBudget {
	return v1beta1.AcceleratorBudget{
		ResourceName:   "nvidia.com/gpu",
		ResourceFlavor: "gb300",
		Nominal:        qty(nominal),
	}
}

func cohort(name, parent string, budgets ...v1beta1.AcceleratorBudget) v1beta1.AcceleratorQuota {
	q := v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1beta1.AcceleratorQuotaSpec{
			Role:    v1beta1.AcceleratorQuotaRoleCohort,
			Budgets: budgets,
		},
	}
	if parent != "" {
		q.Spec.ParentRef = &v1beta1.AcceleratorQuotaParentRef{Name: parent}
	}
	return q
}

func leaf(name, parent string, budgets ...v1beta1.AcceleratorBudget) v1beta1.AcceleratorQuota {
	return v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1beta1.AcceleratorQuotaSpec{
			Role:      v1beta1.AcceleratorQuotaRoleClusterQueue,
			ParentRef: &v1beta1.AcceleratorQuotaParentRef{Name: parent},
			Budgets:   budgets,
		},
	}
}

func mustBuild(t *testing.T, quotas []v1beta1.AcceleratorQuota, o Options) (*Tree, Violations) {
	t.Helper()
	tr, vs, err := Build(quotas, o)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return tr, vs
}

// Cases below compare a whole Violations slice rather than probing it for a
// substring, which is what makes a table exhaustive: a check that fires
// somewhere unexpected shows up as an extra element instead of going unnoticed.
// Messages are compared in full because they are user-facing — they reach an
// operator through a rejected apply and a Degraded condition — so a reworded
// message has to be reviewed rather than silently passing.

// shape is a Node reduced to its computed position.
type shape struct {
	Depth     int
	Leaf      bool
	Reachable bool
}

func shapes(tr *Tree) map[string]shape {
	out := map[string]shape{}
	for _, n := range tr.Nodes() {
		out[n.Name()] = shape{
			Depth:     n.Depth,
			Leaf:      n.IsLeaf(),
			Reachable: n.Reachable(),
		}
	}
	return out
}

// deepChain is a root plus `levels` nested Cohorts and a leaf at the bottom.
func deepChain() []v1beta1.AcceleratorQuota {
	quotas := []v1beta1.AcceleratorQuota{cohort(rootName, "")}
	parent := rootName
	for _, n := range []string{"l1", "l2", "l3"} {
		quotas = append(quotas, cohort(n, parent))
		parent = n
	}
	return append(quotas, leaf("deep", parent, budget("1")))
}

func TestBuild(t *testing.T) {
	base := func(extra ...v1beta1.AcceleratorQuota) []v1beta1.AcceleratorQuota {
		return append([]v1beta1.AcceleratorQuota{cohort(rootName, "", budget("128"))}, extra...)
	}

	cohortWith := func(mutate func(*v1beta1.AcceleratorQuota)) v1beta1.AcceleratorQuota {
		c := cohort("org", rootName)
		mutate(&c)
		return c
	}

	tests := []struct {
		name   string
		quotas []v1beta1.AcceleratorQuota
		opts   *Options // nil uses opts()
		want   Violations
	}{
		{
			name: "a well-formed tree violates nothing",
			quotas: base(
				cohort("org", rootName, budget("100")),
				leaf("team-a", "org", budget("60")),
				leaf("team-b", "org", budget("40")),
			),
		},
		{
			name:   "empty input is not a violation",
			quotas: nil,
		},
		{
			name:   "a dangling parentRef is reported on the child",
			quotas: base(leaf("orphan", "ghost", budget("1"))),
			want: Violations{{
				Node: "orphan", Reason: v1beta1.AcceleratorQuotaReasonParentMissing, Subject: "ghost",
				Message: `parentRef "ghost" does not resolve to an existing AcceleratorQuota`,
			}},
		},
		{
			name:   "a second parent-less node is not a second root",
			quotas: base(cohort("other", "", budget("1"))),
			want: Violations{{
				Node: "other", Reason: v1beta1.AcceleratorQuotaReasonParentMissing, Subject: rootName,
				Message: `only the reserved root "root" may omit parentRef; every other node must name its parent`,
			}},
		},
		{
			name:   "a node parented to itself",
			quotas: base(cohort("loop", "loop")),
			want: Violations{{
				Node: "loop", Reason: v1beta1.AcceleratorQuotaReasonParentCycle, Subject: "loop",
				Message: "parentRef points at the node itself",
			}},
		},
		{
			name:   "a leaf with no budget admits nothing",
			quotas: base(leaf("empty", rootName)),
			want: Violations{{
				Node: "empty", Reason: v1beta1.AcceleratorQuotaReasonNodeKindInvalid,
				Message: "a ClusterQueue node must carry at least one budget",
			}},
		},
		{
			name: "a Cohort may not set a distribution policy",
			quotas: base(cohortWith(func(c *v1beta1.AcceleratorQuota) {
				c.Spec.Distribution = &v1beta1.AcceleratorQuotaDistribution{
					Policy: v1beta1.AcceleratorQuotaDistributionProportional,
				}
			})),
			want: Violations{{
				Node: "org", Reason: v1beta1.AcceleratorQuotaReasonNodeKindInvalid, Subject: "distribution",
				Message: "a Cohort node must not set distribution; no workload binds to a grouping",
			}},
		},
		{
			name: "a Cohort budget may not carry leaf-only fields",
			quotas: base(cohortWith(func(c *v1beta1.AcceleratorQuota) {
				b := budget("10")
				lim := qty("1")
				b.BorrowingLimit = &lim
				c.Spec.Budgets = []v1beta1.AcceleratorBudget{b}
			})),
			want: Violations{{
				Node: "org", Reason: v1beta1.AcceleratorQuotaReasonNodeKindInvalid,
				Subject: "google.com/tpu on tpu7x",
				Message: "budget google.com/tpu on tpu7x carries leaf-only fields; a Cohort budget is an " +
					"authoring guardrail that never materializes, so it holds only resourceName, " +
					"resourceFlavor, and nominal",
			}},
		},
		{
			name: "an unrecognised role",
			quotas: base(v1beta1.AcceleratorQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "weird"},
				Spec: v1beta1.AcceleratorQuotaSpec{
					Role: "Nope", ParentRef: &v1beta1.AcceleratorQuotaParentRef{Name: rootName},
				},
			}),
			want: Violations{{
				Node: "weird", Reason: v1beta1.AcceleratorQuotaReasonNodeKindInvalid,
				Message: `unknown role "Nope"`,
			}},
		},
		{
			name:   "a name claimed twice",
			quotas: base(leaf("dupe", rootName, budget("1")), leaf("dupe", rootName, budget("1"))),
			want: Violations{{
				Node: "dupe", Reason: v1beta1.AcceleratorQuotaReasonDuplicateNode,
				Message: "node name appears more than once in the assembled set",
			}},
		},
		{
			// Otherwise a third party could freeze someone else's live tenant
			// just by creating a node under it. The leaf must also not be
			// charged containment for a child it cannot have.
			name:   "a child under a leaf blames the child, not the leaf",
			quotas: base(leaf("team-a", rootName, budget("10")), leaf("sub", "team-a", budget("500"))),
			want: Violations{{
				Node: "sub", Reason: v1beta1.AcceleratorQuotaReasonNodeKindInvalid, Subject: "team-a",
				Message: `parent "team-a" is a ClusterQueue node, which takes no children; ` +
					"attach to a Cohort, or add a ClusterQueue leaf beside it instead",
			}},
		},
		{
			name: "children busting the parent",
			quotas: base(
				cohort("org", rootName, budget("50")),
				leaf("big", "org", budget("60")),
			),
			want: Violations{{
				Node: "org", Reason: v1beta1.AcceleratorQuotaReasonContainmentViolated,
				Subject: "google.com/tpu on tpu7x",
				Message: "budget google.com/tpu on tpu7x is 50 but its children total 60",
			}},
		},
		{
			name: "siblings are summed against the parent",
			quotas: base(
				cohort("org", rootName, budget("100")),
				leaf("team-a", "org", budget("60")),
				leaf("team-b", "org", budget("60")),
			),
			want: Violations{{
				Node: "org", Reason: v1beta1.AcceleratorQuotaReasonContainmentViolated,
				Subject: "google.com/tpu on tpu7x",
				Message: "budget google.com/tpu on tpu7x is 100 but its children total 120",
			}},
		},
		{
			name: "a budget-less grouping is unconstrained",
			quotas: base(
				cohort("org", rootName),
				leaf("team-a", "org", budget("60")),
				leaf("team-b", "org", budget("40")),
			),
		},
		{
			// A ResourceFlavor names hardware; it says nothing about which
			// resource a count applies to. Quota is keyed by the pair.
			name: "a (resource, flavor) pair the parent does not budget is unconstrained",
			quotas: base(
				cohort("org", rootName, budget("100")),
				leaf("team-a", "org", budget("60"), gpuBudget("8")),
			),
		},
		{
			name: "containment fires only on the matched (resource, flavor) pair",
			quotas: base(
				cohort("org", rootName, budget("100"), gpuBudget("4")),
				leaf("team-a", "org", budget("60"), gpuBudget("8")),
			),
			want: Violations{{
				Node: "org", Reason: v1beta1.AcceleratorQuotaReasonContainmentViolated,
				Subject: "nvidia.com/gpu on gb300",
				Message: "budget nvidia.com/gpu on gb300 is 4 but its children total 8",
			}},
		},
		{
			// Every node beneath a break reports, so the freeze set is complete
			// — otherwise a descendant would materialize under a parent that
			// was never created.
			name: "every node beneath a break reports",
			quotas: base(
				cohort("x1", "ghost"),
				cohort("x2", "x1"),
				leaf("x3", "x2", budget("1")),
			),
			want: Violations{
				{
					Node: "x1", Reason: v1beta1.AcceleratorQuotaReasonParentMissing, Subject: "ghost",
					Message: `parentRef "ghost" does not resolve to an existing AcceleratorQuota`,
				},
				{
					Node: "x2", Reason: v1beta1.AcceleratorQuotaReasonUnreachable, Subject: "x1",
					Message: `node has no position in the tree because ancestor "x1" is unresolved`,
				},
				{
					Node: "x3", Reason: v1beta1.AcceleratorQuotaReasonUnreachable, Subject: "x1",
					Message: `node has no position in the tree because ancestor "x1" is unresolved`,
				},
			},
		},
		{
			name:   "nothing claims the reserved root name",
			quotas: []v1beta1.AcceleratorQuota{leaf("team-a", "org", budget("1")), cohort("org", "")},
			want: Violations{
				{
					Node: "org", Reason: v1beta1.AcceleratorQuotaReasonParentMissing, Subject: rootName,
					Message: `only the reserved root "root" may omit parentRef; every other node must name its parent`,
				},
				{
					Node: "team-a", Reason: v1beta1.AcceleratorQuotaReasonUnreachable, Subject: "org",
					Message: `node has no position in the tree because ancestor "org" is unresolved`,
				},
			},
		},
		{
			name:   "a chain within the depth limit",
			quotas: deepChain(),
			opts:   &Options{RootName: rootName, MaxDepth: 4},
		},
		{
			name:   "a chain past the depth limit",
			quotas: deepChain(),
			opts:   &Options{RootName: rootName, MaxDepth: 3},
			want: Violations{{
				Node: "deep", Reason: v1beta1.AcceleratorQuotaReasonDepthExceeded,
				Message: "node is 4 levels below the root, exceeding the configured maximum of 3",
			}},
		},
		{
			name:   "MaxDepth zero disables the check rather than bounding at zero",
			quotas: deepChain(),
			opts:   &Options{RootName: rootName},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := opts()
			if tc.opts != nil {
				o = *tc.opts
			}
			_, vs := mustBuild(t, tc.quotas, o)
			if diff := cmp.Diff(tc.want, vs); diff != "" {
				t.Errorf("violations mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBuildTreeShape(t *testing.T) {
	tests := []struct {
		name     string
		quotas   []v1beta1.AcceleratorQuota
		want     map[string]shape
		wantRoot string // "" when no node claims the reserved name
	}{
		{
			name: "a well-formed tree",
			quotas: []v1beta1.AcceleratorQuota{
				cohort(rootName, "", budget("128")),
				cohort("org", rootName, budget("100")),
				leaf("team-a", "org", budget("60")),
				leaf("team-b", "org", budget("40")),
			},
			wantRoot: rootName,
			want: map[string]shape{
				rootName: {Depth: 0, Reachable: true},
				"org":    {Depth: 1, Reachable: true},
				"team-a": {Depth: 2, Leaf: true, Reachable: true},
				"team-b": {Depth: 2, Leaf: true, Reachable: true},
			},
		},
		{
			// Nodes hanging off a break keep the -1 depth they were built with,
			// which is what Reachable reports on. An empty path is the signal a
			// materializer must not act.
			name: "nodes beneath a break have no position",
			quotas: []v1beta1.AcceleratorQuota{
				cohort(rootName, "", budget("128")),
				cohort("x1", "ghost"),
				leaf("x2", "x1", budget("1")),
			},
			wantRoot: rootName,
			want: map[string]shape{
				rootName: {Depth: 0, Reachable: true},
				"x1":     {Depth: -1},
				"x2":     {Depth: -1, Leaf: true},
			},
		},
		{
			name:     "no node claims the reserved name",
			quotas:   []v1beta1.AcceleratorQuota{cohort("org", ""), leaf("team-a", "org", budget("1"))},
			wantRoot: "",
			want: map[string]shape{
				"org":    {Depth: -1},
				"team-a": {Depth: -1, Leaf: true},
			},
		},
		{
			name:   "an empty set builds an empty tree",
			quotas: nil,
			want:   map[string]shape{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr, _ := mustBuild(t, tc.quotas, opts())
			if diff := cmp.Diff(tc.want, shapes(tr)); diff != "" {
				t.Errorf("tree shape mismatch (-want +got):\n%s", diff)
			}
			var gotRoot string
			if tr.Root != nil {
				gotRoot = tr.Root.Name()
			}
			if gotRoot != tc.wantRoot {
				t.Errorf("Root = %q, want %q", gotRoot, tc.wantRoot)
			}
			if got := tr.Len(); got != len(tc.want) {
				t.Errorf("Len = %d, want %d", got, len(tc.want))
			}
		})
	}
}

// Frozen is the set a materializer must leave at its last-good state. It is the
// violating nodes joined with everything beneath them, because a descendant of a
// broken node cannot be materialized correctly either.
func TestFrozen(t *testing.T) {
	tests := []struct {
		name   string
		quotas []v1beta1.AcceleratorQuota
		// want maps a frozen node to the reason it inherits.
		want map[string]string
	}{
		{
			name: "a clean tree freezes nothing",
			quotas: []v1beta1.AcceleratorQuota{
				cohort(rootName, "", budget("128")),
				leaf("team-a", rootName, budget("60")),
			},
			want: map[string]string{},
		},
		{
			name: "a violating node and its descendants, but not its ancestors",
			quotas: []v1beta1.AcceleratorQuota{
				cohort(rootName, "", budget("128")),
				cohort("org", rootName, budget("50")),
				leaf("big", "org", budget("60")),
			},
			want: map[string]string{
				"org": v1beta1.AcceleratorQuotaReasonContainmentViolated,
				"big": v1beta1.AcceleratorQuotaReasonContainmentViolated,
			},
		},
		{
			name: "an unreachable chain freezes whole",
			quotas: []v1beta1.AcceleratorQuota{
				cohort(rootName, "", budget("128")),
				cohort("x1", "ghost"),
				cohort("x2", "x1"),
				leaf("x3", "x2", budget("1")),
			},
			want: map[string]string{
				"x1": v1beta1.AcceleratorQuotaReasonParentMissing,
				"x2": v1beta1.AcceleratorQuotaReasonUnreachable,
				"x3": v1beta1.AcceleratorQuotaReasonUnreachable,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr, vs := mustBuild(t, tc.quotas, opts())
			got := map[string]string{}
			for node, v := range tr.Frozen(vs) {
				got[node] = v.Reason
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("frozen set mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// The webhook contract: a write is judged by what it *adds*, not by the state it
// inherits, or an admin could never repair an already-broken tree.
func TestDiff(t *testing.T) {
	live := []v1beta1.AcceleratorQuota{
		cohort(rootName, "", budget("128")),
		cohort("org", rootName, budget("10")),
		leaf("bad", "org", budget("99")),
	}

	tests := []struct {
		name string
		// after is the world the write would produce.
		after []v1beta1.AcceleratorQuota
		want  Violations
	}{
		{
			name:  "shrinking an existing overrun is admitted",
			after: Splice(live, leaf("bad", "org", budget("50"))),
		},
		{
			// Same node, same reason, different budget — a keyed diff must
			// still catch this even though the tree was already violating.
			name: "a new overrun on a different budget is caught",
			after: Splice(
				Splice(live, leaf("bad", "org", budget("99"), gpuBudget("40"))),
				cohort("org", rootName, budget("10"), gpuBudget("4")),
			),
			want: Violations{{
				Node: "org", Reason: v1beta1.AcceleratorQuotaReasonContainmentViolated,
				Subject: "nvidia.com/gpu on gb300",
				Message: "budget nvidia.com/gpu on gb300 is 4 but its children total 40",
			}},
		},
		{
			// Recorded rather than discovered: Diff keys on
			// (Node, Reason, Subject), so worsening an overrun that already
			// existed on that key is admitted. The controller's re-check is
			// what reports it.
			name:  "worsening an existing overrun is admitted, by design",
			after: Splice(live, leaf("bad", "org", budget("500"))),
		},
		{
			name:  "a delete that orphans a subtree is caught",
			after: Without(live, "org"),
			want: Violations{
				{
					Node: "bad", Reason: v1beta1.AcceleratorQuotaReasonParentMissing, Subject: "org",
					Message: `parentRef "org" does not resolve to an existing AcceleratorQuota`,
				},
			},
		},
	}

	_, before := mustBuild(t, live, opts())
	if len(before) == 0 {
		t.Fatal("fixture should start already violating, or Diff is not under test")
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, after := mustBuild(t, tc.after, opts())
			if diff := cmp.Diff(tc.want, after.Diff(before)); diff != "" {
				t.Errorf("newly-added violations mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// Splice and Without build the hypothetical world an admission check runs
// against, so neither may disturb the caller's slice.
func TestSpliceAndWithout(t *testing.T) {
	existing := []v1beta1.AcceleratorQuota{
		cohort(rootName, "", budget("128")),
		leaf("team-a", rootName, budget("10")),
	}
	untouched := append([]v1beta1.AcceleratorQuota(nil), existing...)

	tests := []struct {
		name      string
		apply     func() []v1beta1.AcceleratorQuota
		wantNames []string
		// wantNominal is checked on the last entry when set.
		wantNominal string
	}{
		{
			name:        "Splice replaces a node of the same name in place",
			apply:       func() []v1beta1.AcceleratorQuota { return Splice(existing, leaf("team-a", rootName, budget("20"))) },
			wantNames:   []string{rootName, "team-a"},
			wantNominal: "20",
		},
		{
			name:        "Splice appends an unseen name",
			apply:       func() []v1beta1.AcceleratorQuota { return Splice(existing, leaf("team-b", rootName, budget("5"))) },
			wantNames:   []string{rootName, "team-a", "team-b"},
			wantNominal: "5",
		},
		{
			name:      "Without removes a node",
			apply:     func() []v1beta1.AcceleratorQuota { return Without(existing, "team-a") },
			wantNames: []string{rootName},
		},
		{
			name:      "Without an unknown name is a no-op",
			apply:     func() []v1beta1.AcceleratorQuota { return Without(existing, "absent") },
			wantNames: []string{rootName, "team-a"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := tc.apply()
			var gotNames []string
			for _, q := range out {
				gotNames = append(gotNames, q.Name)
			}
			if diff := cmp.Diff(tc.wantNames, gotNames); diff != "" {
				t.Errorf("names mismatch (-want +got):\n%s", diff)
			}
			if tc.wantNominal != "" {
				if got := out[len(out)-1].Spec.Budgets[0].Nominal.String(); got != tc.wantNominal {
					t.Errorf("nominal = %s, want %s", got, tc.wantNominal)
				}
			}
			// The whole input, not a spot check: a shallow guard on one entry
			// would miss a reorder, or an edit landing on the root instead of
			// the intended node.
			if diff := cmp.Diff(untouched, existing); diff != "" {
				t.Errorf("the caller's slice was mutated (-before +after):\n%s", diff)
			}
		})
	}
}

func TestTreeAccessors(t *testing.T) {
	// Pre-order, so a materializer can create a parent before the objects that
	// name it. Name-sorted order would put "org" before "root".
	t.Run("Subtree is pre-order", func(t *testing.T) {
		tr, _ := mustBuild(t, []v1beta1.AcceleratorQuota{
			cohort(rootName, "", budget("128")),
			cohort("org", rootName, budget("100")),
			cohort("sub", "org", budget("100")),
			leaf("team-a", "sub", budget("60")),
		}, opts())
		var got []string
		for _, n := range tr.Subtree(rootName) {
			got = append(got, n.Name())
		}
		if diff := cmp.Diff([]string{rootName, "org", "sub", "team-a"}, got); diff != "" {
			t.Errorf("Subtree order mismatch (-want +got):\n%s", diff)
		}
		if tr.Subtree("nope") != nil {
			t.Errorf("Subtree of an unknown node should be nil")
		}
	})

	t.Run("Ancestors is root-first", func(t *testing.T) {
		tr, _ := mustBuild(t, []v1beta1.AcceleratorQuota{
			cohort(rootName, "", budget("128")),
			cohort("org", rootName, budget("100")),
			leaf("team-a", "org", budget("60")),
		}, opts())
		teamA, _ := tr.Node("team-a")
		var got []string
		for _, a := range teamA.Ancestors() {
			got = append(got, a.Name())
		}
		if diff := cmp.Diff([]string{rootName, "org"}, got); diff != "" {
			t.Errorf("Ancestors mismatch (-want +got):\n%s", diff)
		}
		if root, _ := tr.Node(rootName); len(root.Ancestors()) != 0 {
			t.Errorf("the root has no ancestors")
		}
		if _, ok := tr.Node("nope"); ok {
			t.Errorf("Node of an unknown name should report missing")
		}
	})

	// A bare parent walk would spin here.
	t.Run("Ancestors terminates on a cycle", func(t *testing.T) {
		tr, _ := mustBuild(t, []v1beta1.AcceleratorQuota{
			cohort(rootName, ""),
			cohort("a", "c"), cohort("b", "a"), cohort("c", "b"),
			leaf("hanging", "a", budget("1")),
		}, opts())
		h, _ := tr.Node("hanging")
		done := make(chan int, 1)
		go func() { done <- len(h.Ancestors()) }()
		select {
		case n := <-done:
			if n == 0 {
				t.Errorf("Ancestors over a cycle should still report the chain it walked")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Ancestors did not terminate on a cycle")
		}
	})

	t.Run("accessors are safe on an empty tree", func(t *testing.T) {
		tr, vs := mustBuild(t, nil, opts())
		if tr.Leaves() != nil {
			t.Errorf("Leaves of an empty tree should be nil")
		}
		if tr.Nodes() == nil {
			t.Errorf("Nodes must return an empty slice, not nil")
		}
		if len(tr.Frozen(vs)) != 0 {
			t.Errorf("nothing to freeze in an empty tree")
		}
	})

	// Membership follows the declared role, never the presence of budgets or
	// children: a grouping authored before its first child is still a grouping,
	// and a leaf whose budget is missing is still a leaf — it has to be, or the
	// violation saying so would have nowhere to attach. Hence the budget-less
	// "unfinished" leaf in the fixture, which Leaves must still return.
	t.Run("Leaves reports every node whose declared role is a leaf", func(t *testing.T) {
		tr, _ := mustBuild(t, []v1beta1.AcceleratorQuota{
			cohort(rootName, "", budget("128")),
			cohort("org", rootName, budget("100")),
			cohort("childless", rootName),
			leaf("team-a", "org", budget("60")),
			leaf("team-b", "org", budget("40")),
			leaf("unfinished", rootName),
		}, opts())
		var got []string
		for _, n := range tr.Leaves() {
			got = append(got, n.Name())
		}
		if diff := cmp.Diff([]string{"team-a", "team-b", "unfinished"}, got); diff != "" {
			t.Errorf("Leaves mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestBuildCycle(t *testing.T) {
	quotas := []v1beta1.AcceleratorQuota{
		cohort(rootName, "", budget("128")),
		cohort("a", "c"), cohort("b", "a"), cohort("c", "b"),
	}
	tr, vs := mustBuild(t, quotas, opts())

	t.Run("every member reports and none is reachable", func(t *testing.T) {
		for _, n := range []string{"a", "b", "c"} {
			if len(vs.For(n)) == 0 {
				t.Errorf("node %q in a cycle should report", n)
			}
			node, _ := tr.Node(n)
			if node.Reachable() {
				t.Errorf("node %q in a cycle must not be reachable (depth %d)", n, node.Depth)
			}
		}
	})

	// Spelled out once; the rest reference it, so a long cycle does not repeat
	// the whole path per member.
	t.Run("the path is rendered exactly once", func(t *testing.T) {
		var full int
		for _, v := range vs {
			if strings.Contains(v.Message, "forms a cycle") {
				full++
			}
		}
		if full != 1 {
			t.Errorf("the cycle path should be rendered once, got %d times", full)
		}
	})

	// One Subject across the cycle keeps Diff stable regardless of which node
	// the walk happened to start from.
	t.Run("members share one Subject", func(t *testing.T) {
		subj := vs.For("a")[0].Subject
		for _, n := range []string{"b", "c"} {
			if got := vs.For(n)[0].Subject; got != subj {
				t.Errorf("cycle Subject differs: %q vs %q", got, subj)
			}
		}
	})

	t.Run("the root is unaffected and traversal terminates", func(t *testing.T) {
		if tr.Root == nil || tr.Root.Depth != 0 {
			t.Errorf("root should still be resolved at depth 0")
		}
		if got := tr.Subtree("a"); len(got) != 3 {
			t.Errorf("Subtree over a cycle = %d nodes, want 3 (must terminate)", len(got))
		}
	})

	// A long cycle must not produce a message that overruns an admission
	// response or the apiserver's condition-message limit.
	t.Run("the message stays bounded on a long cycle", func(t *testing.T) {
		long := []v1beta1.AcceleratorQuota{cohort(rootName, "")}
		const n = 300
		for i := 0; i < n; i++ {
			prev := i - 1
			if prev < 0 {
				prev = n - 1
			}
			long = append(long, cohort(nodeName(i), nodeName(prev)))
		}
		_, got := mustBuild(t, long, opts())
		for _, v := range got {
			if len(v.Message) > 2000 {
				t.Fatalf("cycle message is %d bytes; must stay bounded", len(v.Message))
			}
		}
		if len(got.Error()) > 200000 {
			t.Fatalf("rendered violations are %d bytes; must stay bounded", len(got.Error()))
		}
	})
}

func nodeName(i int) string { return "n" + strings.Repeat("0", 3-len(itoa(i))) + itoa(i) }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func TestBuildRequiresRootName(t *testing.T) {
	tr, vs, err := Build([]v1beta1.AcceleratorQuota{cohort(rootName, "")}, Options{})
	if !errors.Is(err, ErrRootNameUnset) {
		t.Fatalf("want ErrRootNameUnset, got tree=%v vs=%v err=%v", tr, vs, err)
	}
}

// Summing must not disturb the caller's CRs, and must handle the scaled forms a
// Quantity round-trips through.
func TestContainmentDoesNotMutateInput(t *testing.T) {
	cpu := func(nominal string) v1beta1.AcceleratorBudget {
		return v1beta1.AcceleratorBudget{
			ResourceName: "cpu", ResourceFlavor: "generic", Nominal: qty(nominal),
		}
	}
	// Decimal-milli and binary suffixes in one sum, so the total can only come
	// out right if the addition goes through Quantity rather than through
	// whichever scale it was parsed in.
	quotas := []v1beta1.AcceleratorQuota{
		cohort(rootName, ""),
		cohort("org", rootName, cpu("1")),
		leaf("team-a", "org", cpu("1500m")),
		leaf("team-b", "org", cpu("2Gi")),
	}
	wantTotal := qty("1500m")
	sum := qty("2Gi")
	wantTotal.Add(sum)

	before := quotas[2].Spec.Budgets[0].Nominal.String()
	_, vs := mustBuild(t, quotas, opts())
	if after := quotas[2].Spec.Budgets[0].Nominal.String(); after != before {
		t.Fatalf("Build mutated a caller budget: %s -> %s", before, after)
	}
	// The total is asserted exactly, not just the fact that something fired.
	// Either child alone already busts a parent of 1, so a summation bug that
	// dropped one would still produce a violation — only the rendered total
	// distinguishes summing from noticing.
	want := Violations{{
		Node: "org", Reason: v1beta1.AcceleratorQuotaReasonContainmentViolated,
		Subject: "cpu on generic",
		Message: "budget cpu on generic is 1 but its children total " + wantTotal.String(),
	}}
	if diff := cmp.Diff(want, vs); diff != "" {
		t.Errorf("containment on mixed-scale quantities (-want +got):\n%s", diff)
	}
}

func TestViolations(t *testing.T) {
	t.Run("rendering", func(t *testing.T) {
		tests := []struct {
			name string
			vs   Violations
			want string
		}{
			{name: "empty renders empty", vs: nil, want: ""},
			{
				name: "one is node-prefixed",
				vs:   Violations{{Node: "a", Reason: "R", Message: "boom"}},
				want: "a: boom",
			},
			{
				name: "several are counted and joined",
				vs:   Violations{{Node: "b", Message: "second"}, {Node: "a", Message: "first"}},
				want: "2 invariants violated: b: second; a: first",
			},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if diff := cmp.Diff(tc.want, tc.vs.Error()); diff != "" {
					t.Errorf("Error() mismatch (-want +got):\n%s", diff)
				}
			})
		}
	})

	t.Run("OrNil converts only a non-empty set", func(t *testing.T) {
		var empty Violations
		if empty.OrNil() != nil {
			t.Errorf("empty Violations must convert to a nil error")
		}
		if (Violations{{Node: "a", Message: "boom"}}).OrNil() == nil {
			t.Errorf("non-empty Violations must convert to a non-nil error")
		}
	})

	t.Run("Nodes is sorted, deduped, and skips node-less entries", func(t *testing.T) {
		tests := []struct {
			name string
			vs   Violations
			want []string
		}{
			{
				name: "sorted and deduped",
				vs: Violations{
					{Node: "b", Message: "x"}, {Node: "a", Message: "y"}, {Node: "b", Message: "z"},
				},
				want: []string{"a", "b"},
			},
			{
				// Empty rather than nil: callers range over it.
				name: "a node-less violation contributes no node",
				vs:   Violations{{Message: "config problem"}},
				want: []string{},
			},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if diff := cmp.Diff(tc.want, tc.vs.Nodes()); diff != "" {
					t.Errorf("Nodes() mismatch (-want +got):\n%s", diff)
				}
			})
		}
	})

	t.Run("For filters by node", func(t *testing.T) {
		vs := Violations{{Node: "b", Message: "second"}, {Node: "a", Message: "first"}}
		if diff := cmp.Diff(Violations{{Node: "a", Message: "first"}}, vs.For("a")); diff != "" {
			t.Errorf("For() mismatch (-want +got):\n%s", diff)
		}
		if got := vs.For("absent"); len(got) != 0 {
			t.Errorf("For an unmatched node should be empty, got %v", got)
		}
	})

	t.Run("a node-less violation renders without a separator", func(t *testing.T) {
		if got := (Violation{Message: "config problem"}).String(); got != "config problem" {
			t.Errorf("node-less violation = %q", got)
		}
	})
}

// A node broken several ways reports the cause an operator must fix first, not
// whichever reason happens to sort earliest.
func TestPrimary(t *testing.T) {
	t.Run("the structural cause wins over the node's own shape", func(t *testing.T) {
		// A leaf that is both parented on a node that does not exist and carrying
		// no budget. Fixing its budget while it still hangs off nothing changes
		// nothing, so the missing parent is what the node must report.
		quotas := []v1beta1.AcceleratorQuota{
			cohort(rootName, "", budget("128")),
			leaf("team-a", rootName, budget("10")),
			leaf("team-b", "ghost"),
		}
		_, vs := mustBuild(t, quotas, opts())
		got := vs.For("team-b")
		if len(got) < 2 {
			t.Fatalf("team-b should report both a missing parent and a missing budget, got: %v", got)
		}
		if r := got.Primary().Reason; r != v1beta1.AcceleratorQuotaReasonParentMissing {
			t.Errorf("Primary = %q, want ParentMissing (fix the structure before the shape)", r)
		}
	})

	t.Run("Primary of an empty set is the zero value", func(t *testing.T) {
		if (Violations{}).Primary() != (Violation{}) {
			t.Errorf("Primary of an empty set should be the zero Violation")
		}
	})
}

// Two builds of the same input must render identically, or a status message
// would flap and a Diff would report phantom changes.
func TestViolationOrderIsStable(t *testing.T) {
	quotas := []v1beta1.AcceleratorQuota{
		cohort(rootName, "", budget("1")),
		leaf("zeta", "ghost", budget("1")),
		leaf("alpha", "ghost", budget("1")),
	}
	_, a := mustBuild(t, quotas, opts())
	_, b := mustBuild(t, quotas, opts())
	if diff := cmp.Diff(a, b); diff != "" {
		t.Errorf("violation order is unstable (-first +second):\n%s", diff)
	}
	if len(a) < 2 || a[0].Node != "alpha" {
		t.Errorf("violations should be node-sorted, got %v", a.Nodes())
	}
}
