package kueue

import (
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/backend"
	"sigs.k8s.io/ome/pkg/quota/tree"
)

// The rules pinned here are Kueue's, and every one of them fails silently when
// broken: a queue with no namespaceSelector admits nothing, a flavor missing a
// covered resource is never assigned, and a budget naming an absent flavor
// makes the whole queue inactive. None surfaces as a rejected write, so the
// mapping is the only place they can be caught.

func aq(name, parent string, role v1beta1.AcceleratorQuotaRole, budgets ...v1beta1.AcceleratorBudget) v1beta1.AcceleratorQuota {
	q := v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
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

func budget(resourceName, flavor, nominal string) v1beta1.AcceleratorBudget {
	return v1beta1.AcceleratorBudget{
		ResourceName:   resourceName,
		ResourceFlavor: flavor,
		Nominal:        resource.MustParse(nominal),
	}
}

func nodeFor(t *testing.T, quotas []v1beta1.AcceleratorQuota, name string) *tree.Node {
	t.Helper()
	built, _, err := tree.Build(quotas, tree.Options{RootName: "root"})
	if err != nil {
		t.Fatalf("tree.Build() error = %v", err)
	}
	n, ok := built.Node(name)
	if !ok {
		t.Fatalf("node %q absent from the built tree", name)
	}
	return n
}

func testOptions() Options { return testOptionsIn("ns-a") }

// testOptionsIn varies the enrolled namespace set, which is deploy config now
// rather than anything a node declares: every leaf gets a LocalQueue in each of
// these, and every ClusterQueue selects exactly this set.
func testOptionsIn(namespaces ...string) Options {
	return Options{
		FieldManager: "ome-quota",
		CoverResources: map[corev1.ResourceName]resource.Quantity{
			corev1.ResourceCPU:    resource.MustParse("1k"),
			corev1.ResourceMemory: resource.MustParse("1Ti"),
		},
		EnrolledNamespaces: namespaces,
	}
}

// flavorSet spells the "flavors could not be read" case as nil, distinct from
// "no flavor exists", which is an empty non-nil set the renderer never sees
// because it treats both as unreadable.
func flavorSet(names ...string) map[string]struct{} {
	if names == nil {
		return nil
	}
	out := map[string]struct{}{}
	for _, n := range names {
		out[n] = struct{}{}
	}
	return out
}

// renderedCQ flattens a ClusterQueue apply configuration into the facts the
// tables assert on. Comparing the apply configuration itself would compare
// pointer-laden generated structs and read as a dump rather than a diff.
type renderedCQ struct {
	Name       string
	CohortName string
	Namespaces []string
	Covered    []string
	Flavors    []renderedFlavor
	Labels     map[string]string
}

type renderedFlavor struct {
	Name      string
	Resources []renderedResource
}

type renderedResource struct {
	Name           string
	NominalQuota   string
	BorrowingLimit string
	LendingLimit   string
}

func flattenCQ(t *testing.T, objs Objects) []renderedCQ {
	t.Helper()
	out := make([]renderedCQ, 0, len(objs.ClusterQueues))
	for _, cq := range objs.ClusterQueues {
		r := renderedCQ{Name: *cq.Name, Labels: cq.Labels}
		if cq.Spec == nil {
			out = append(out, r)
			continue
		}
		if cq.Spec.CohortName != nil {
			r.CohortName = string(*cq.Spec.CohortName)
		}
		if sel := cq.Spec.NamespaceSelector; sel != nil {
			for _, expr := range sel.MatchExpressions {
				r.Namespaces = append(r.Namespaces, expr.Values...)
			}
		}
		for _, g := range cq.Spec.ResourceGroups {
			for _, c := range g.CoveredResources {
				r.Covered = append(r.Covered, string(c))
			}
			for _, f := range g.Flavors {
				rf := renderedFlavor{Name: string(*f.Name)}
				for _, res := range f.Resources {
					rr := renderedResource{
						Name:         string(*res.Name),
						NominalQuota: res.NominalQuota.String(),
					}
					if res.BorrowingLimit != nil {
						rr.BorrowingLimit = res.BorrowingLimit.String()
					}
					if res.LendingLimit != nil {
						rr.LendingLimit = res.LendingLimit.String()
					}
					rf.Resources = append(rf.Resources, rr)
				}
				r.Flavors = append(r.Flavors, rf)
			}
		}
		out = append(out, r)
	}
	return out
}

func TestRenderClusterQueue(t *testing.T) {
	tests := []struct {
		name    string
		quotas  []v1beta1.AcceleratorQuota
		leaf    string
		flavors map[string]struct{}
		// enrolled overrides the default single-namespace install, which is what
		// the ClusterQueue's namespaceSelector now reads.
		enrolled []string
		want     []renderedCQ
	}{
		{
			// The cover is in the same group as the accelerator, and the
			// accelerator's own flavor funds cpu/memory too -- otherwise a pod
			// asking for cpu and a GPU can never be assigned this flavor.
			name: "a leaf funds its accelerator alongside the cover",
			quotas: []v1beta1.AcceleratorQuota{
				aq("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				aq("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("nvidia.com/gpu", "a100", "8")),
			},
			leaf:    "team-a",
			flavors: flavorSet("a100"),
			want: []renderedCQ{{
				Name:       "team-a",
				CohortName: "root",
				Namespaces: []string{"ns-a"},
				Covered:    []string{"cpu", "memory", "nvidia.com/gpu"},
				Flavors: []renderedFlavor{{
					Name: "a100",
					Resources: []renderedResource{
						{Name: "cpu", NominalQuota: "1k"},
						{Name: "memory", NominalQuota: "1Ti"},
						{Name: "nvidia.com/gpu", NominalQuota: "8", BorrowingLimit: "0"},
					},
				}},
				Labels: map[string]string{
					"ome.io/quota-managed-by":  "ome-quota",
					"ome.io/accelerator-quota": "team-a",
				},
			}},
		},
		{
			// Two flavors, each funding a different accelerator. Both must
			// enumerate BOTH accelerators; the one it does not fund is carried
			// at zero rather than omitted, which Kueue requires.
			name: "a flavor carries a resource it does not fund at zero",
			quotas: []v1beta1.AcceleratorQuota{
				aq("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				aq("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("nvidia.com/gpu", "a100", "8"),
					budget("google.com/tpu", "tpu7x", "16")),
			},
			leaf:    "team-a",
			flavors: flavorSet("a100", "tpu7x"),
			want: []renderedCQ{{
				Name:       "team-a",
				CohortName: "root",
				Namespaces: []string{"ns-a"},
				Covered:    []string{"cpu", "google.com/tpu", "memory", "nvidia.com/gpu"},
				Flavors: []renderedFlavor{
					{
						Name: "a100",
						Resources: []renderedResource{
							{Name: "cpu", NominalQuota: "1k"},
							{Name: "google.com/tpu", NominalQuota: "0", BorrowingLimit: "0"},
							{Name: "memory", NominalQuota: "1Ti"},
							{Name: "nvidia.com/gpu", NominalQuota: "8", BorrowingLimit: "0"},
						},
					},
					{
						Name: "tpu7x",
						Resources: []renderedResource{
							{Name: "cpu", NominalQuota: "1k"},
							{Name: "google.com/tpu", NominalQuota: "16", BorrowingLimit: "0"},
							{Name: "memory", NominalQuota: "1Ti"},
							{Name: "nvidia.com/gpu", NominalQuota: "0", BorrowingLimit: "0"},
						},
					},
				},
				Labels: map[string]string{
					"ome.io/quota-managed-by":  "ome-quota",
					"ome.io/accelerator-quota": "team-a",
				},
			}},
		},
		{
			// A budget naming an absent flavor is dropped rather than rendered:
			// rendering it makes the entire queue inactive, so one typo would
			// stop a tenant admitting anything at all.
			name: "a budget naming an absent flavor is dropped, not rendered",
			quotas: []v1beta1.AcceleratorQuota{
				aq("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				aq("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("nvidia.com/gpu", "a100", "8"),
					budget("google.com/tpu", "typo", "16")),
			},
			leaf:    "team-a",
			flavors: flavorSet("a100"),
			want: []renderedCQ{{
				Name:       "team-a",
				CohortName: "root",
				Namespaces: []string{"ns-a"},
				Covered:    []string{"cpu", "memory", "nvidia.com/gpu"},
				Flavors: []renderedFlavor{{
					Name: "a100",
					Resources: []renderedResource{
						{Name: "cpu", NominalQuota: "1k"},
						{Name: "memory", NominalQuota: "1Ti"},
						{Name: "nvidia.com/gpu", NominalQuota: "8", BorrowingLimit: "0"},
					},
				}},
				Labels: map[string]string{
					"ome.io/quota-managed-by":  "ome-quota",
					"ome.io/accelerator-quota": "team-a",
				},
			}},
		},
		{
			// Unreadable flavors must not be read as "every flavor is missing",
			// which would zero every tenant on a transient API failure.
			name: "unreadable flavors drop nothing",
			quotas: []v1beta1.AcceleratorQuota{
				aq("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				aq("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("nvidia.com/gpu", "a100", "8")),
			},
			leaf:    "team-a",
			flavors: nil,
			want: []renderedCQ{{
				Name:       "team-a",
				CohortName: "root",
				Namespaces: []string{"ns-a"},
				Covered:    []string{"cpu", "memory", "nvidia.com/gpu"},
				Flavors: []renderedFlavor{{
					Name: "a100",
					Resources: []renderedResource{
						{Name: "cpu", NominalQuota: "1k"},
						{Name: "memory", NominalQuota: "1Ti"},
						{Name: "nvidia.com/gpu", NominalQuota: "8", BorrowingLimit: "0"},
					},
				}},
				Labels: map[string]string{
					"ome.io/quota-managed-by":  "ome-quota",
					"ome.io/accelerator-quota": "team-a",
				},
			}},
		},
		{
			// Every enrolled namespace lands in one selector, sorted, so the
			// apply is a no-op when nothing changed. Not a tenant boundary --
			// every leaf's queue selects the same set -- but it is what stops a
			// workload outside those namespaces charging any budget at all.
			name: "the enrolled namespaces are selected by name, sorted",
			quotas: []v1beta1.AcceleratorQuota{
				aq("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				aq("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("nvidia.com/gpu", "a100", "8")),
			},
			leaf:     "team-a",
			flavors:  flavorSet("a100"),
			enrolled: []string{"ns-z", "ns-a"},
			want: []renderedCQ{{
				Name:       "team-a",
				CohortName: "root",
				Namespaces: []string{"ns-a", "ns-z"},
				Covered:    []string{"cpu", "memory", "nvidia.com/gpu"},
				Flavors: []renderedFlavor{{
					Name: "a100",
					Resources: []renderedResource{
						{Name: "cpu", NominalQuota: "1k"},
						{Name: "memory", NominalQuota: "1Ti"},
						{Name: "nvidia.com/gpu", NominalQuota: "8", BorrowingLimit: "0"},
					},
				}},
				Labels: map[string]string{
					"ome.io/quota-managed-by":  "ome-quota",
					"ome.io/accelerator-quota": "team-a",
				},
			}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := nodeFor(t, tc.quotas, tc.leaf)
			opts := testOptions()
			if tc.enrolled != nil {
				opts = testOptionsIn(tc.enrolled...)
			}
			got := Render(backend.Plan{Write: []*tree.Node{node}}, tc.flavors, opts)

			if diff := cmp.Diff(tc.want, flattenCQ(t, got)); diff != "" {
				t.Errorf("Render() ClusterQueue mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// A leaf always emits exactly one resource group. Kueue scopes its
// resource-uniqueness and flavor-uniqueness checks across all groups, and the
// cover sits in whichever group it is written to, so any split leaves flavors
// that cannot satisfy a pod requesting cpu.
func TestRenderEmitsOneResourceGroup(t *testing.T) {
	tests := []struct {
		name    string
		budgets []v1beta1.AcceleratorBudget
		flavors map[string]struct{}
		want    int
	}{
		{
			name:    "one resource on one flavor",
			budgets: []v1beta1.AcceleratorBudget{budget("nvidia.com/gpu", "a100", "8")},
			flavors: flavorSet("a100"),
			want:    1,
		},
		{
			name: "two resources sharing one flavor",
			budgets: []v1beta1.AcceleratorBudget{
				budget("nvidia.com/gpu", "a100", "8"),
				budget("example.com/fpga", "a100", "4"),
			},
			flavors: flavorSet("a100"),
			want:    1,
		},
		{
			name: "two resources on disjoint flavors, which a naive split would separate",
			budgets: []v1beta1.AcceleratorBudget{
				budget("nvidia.com/gpu", "a100", "8"),
				budget("google.com/tpu", "tpu7x", "16"),
			},
			flavors: flavorSet("a100", "tpu7x"),
			want:    1,
		},
		{
			name:    "no budgets emits no group at all",
			budgets: nil,
			flavors: flavorSet("a100"),
			want:    0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			quotas := []v1beta1.AcceleratorQuota{
				aq("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				aq("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, tc.budgets...),
			}
			node := nodeFor(t, quotas, "team-a")
			got := Render(backend.Plan{Write: []*tree.Node{node}}, tc.flavors, testOptions())

			groups := 0
			if cq := got.ClusterQueues[0]; cq.Spec != nil {
				groups = len(cq.Spec.ResourceGroups)
			}
			if groups != tc.want {
				t.Errorf("Render() emitted %d resource groups, want %d", groups, tc.want)
			}
		})
	}
}

func TestRenderCohort(t *testing.T) {
	tests := []struct {
		name       string
		quotas     []v1beta1.AcceleratorQuota
		node       string
		wantParent string
		wantGroups int
	}{
		{
			// The reserved root has no parent; the field must be absent rather
			// than empty, since Kueue rejects a borrowing limit on a parentless
			// cohort and treats an empty parent as "no parent".
			name: "the reserved root emits no parent",
			quotas: []v1beta1.AcceleratorQuota{
				aq("root", "", v1beta1.AcceleratorQuotaRoleCohort),
			},
			node:       "root",
			wantParent: "",
			wantGroups: 0,
		},
		{
			name: "an internal node names its parent",
			quotas: []v1beta1.AcceleratorQuota{
				aq("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				aq("org", "root", v1beta1.AcceleratorQuotaRoleCohort),
			},
			node:       "org",
			wantParent: "root",
			wantGroups: 0,
		},
		{
			// An internal node's budget is an authoring guardrail. Kueue's
			// cohort quota is additive, so materializing it would hand out
			// quota on top of what the children already contribute.
			name: "an internal node's budget does not materialize",
			quotas: []v1beta1.AcceleratorQuota{
				aq("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				aq("org", "root", v1beta1.AcceleratorQuotaRoleCohort, budget("nvidia.com/gpu", "a100", "100")),
			},
			node:       "org",
			wantParent: "root",
			wantGroups: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := nodeFor(t, tc.quotas, tc.node)
			got := Render(backend.Plan{Write: []*tree.Node{node}}, flavorSet("a100"), testOptions())

			if len(got.Cohorts) != 1 {
				t.Fatalf("Render() emitted %d cohorts, want 1", len(got.Cohorts))
			}
			c := got.Cohorts[0]
			parent := ""
			if c.Spec != nil && c.Spec.ParentName != nil {
				parent = string(*c.Spec.ParentName)
			}
			if parent != tc.wantParent {
				t.Errorf("Render() cohort parent = %q, want %q", parent, tc.wantParent)
			}
			groups := 0
			if c.Spec != nil {
				groups = len(c.Spec.ResourceGroups)
			}
			if groups != tc.wantGroups {
				t.Errorf("Render() cohort resource groups = %d, want %d", groups, tc.wantGroups)
			}
			if len(got.ClusterQueues) != 0 || len(got.LocalQueues) != 0 {
				t.Errorf("Render() emitted queues for an internal node: %d CQ, %d LQ",
					len(got.ClusterQueues), len(got.LocalQueues))
			}
		})
	}
}

// Every leaf gets a LocalQueue in every enrolled namespace, named after the
// leaf. The name is the tenant: Kueue offers no way to aim a workload at a
// ClusterQueue directly, so the LocalQueue name in the queue-name label is the
// only thing that picks one budget out of the many sharing a namespace.
func TestRenderLocalQueues(t *testing.T) {
	tests := []struct {
		name     string
		enrolled []string
		leaves   []string
		want     []string
	}{
		{
			name:     "one enrolled namespace",
			enrolled: []string{"ns-a"},
			leaves:   []string{"team-a"},
			want:     []string{"ns-a/team-a"},
		},
		{
			name:     "one leaf reaches every enrolled namespace, sorted",
			enrolled: []string{"ns-z", "ns-a", "ns-m"},
			leaves:   []string{"team-a"},
			want:     []string{"ns-a/team-a", "ns-m/team-a", "ns-z/team-a"},
		},
		{
			// The shape the design exists for: tenancy is not namespace-scoped,
			// so several leaves coexist in one namespace, each with its own
			// pointer at its own budget.
			name:     "many leaves share one namespace, each with its own queue",
			enrolled: []string{"ns-a"},
			leaves:   []string{"team-a", "team-b", "team-c"},
			want:     []string{"ns-a/team-a", "ns-a/team-b", "ns-a/team-c"},
		},
		{
			name:     "no enrolled namespace leaves nowhere to point",
			enrolled: nil,
			leaves:   []string{"team-a"},
			want:     []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			quotas := []v1beta1.AcceleratorQuota{
				aq("root", "", v1beta1.AcceleratorQuotaRoleCohort),
			}
			for _, name := range tc.leaves {
				quotas = append(quotas, aq(name, "root",
					v1beta1.AcceleratorQuotaRoleClusterQueue, budget("nvidia.com/gpu", "a100", "8")))
			}
			var write []*tree.Node
			for _, name := range tc.leaves {
				write = append(write, nodeFor(t, quotas, name))
			}
			got := Render(backend.Plan{Write: write}, flavorSet("a100"), testOptionsIn(tc.enrolled...))

			names := []string{}
			for _, lq := range got.LocalQueues {
				names = append(names, *lq.Namespace+"/"+*lq.Name)
				// Named for the leaf and pointing at the leaf. That identity is
				// what lets one queue name reach one budget.
				if got := string(*lq.Spec.ClusterQueue); got != *lq.Name {
					t.Errorf("LocalQueue %q points at %q, want its own name", *lq.Name, got)
				}
			}
			sort.Strings(names)
			if diff := cmp.Diff(tc.want, names); diff != "" {
				t.Errorf("Render() LocalQueues mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// A budget naming a flavor this cluster does not have is reported so the caller
// can raise FlavorMissing. Reporting is the whole point: dropping it silently
// would leave a tenant under-budgeted with nothing to read.
func TestRenderReportsMissingFlavors(t *testing.T) {
	tests := []struct {
		name    string
		budgets []v1beta1.AcceleratorBudget
		flavors map[string]struct{}
		want    map[string][]string
	}{
		{
			name:    "every flavor exists",
			budgets: []v1beta1.AcceleratorBudget{budget("nvidia.com/gpu", "a100", "8")},
			flavors: flavorSet("a100"),
			want:    map[string][]string{},
		},
		{
			name: "one flavor is absent",
			budgets: []v1beta1.AcceleratorBudget{
				budget("nvidia.com/gpu", "a100", "8"),
				budget("google.com/tpu", "typo", "16"),
			},
			flavors: flavorSet("a100"),
			want:    map[string][]string{"team-a": {"typo"}},
		},
		{
			name: "several absent flavors are reported sorted",
			budgets: []v1beta1.AcceleratorBudget{
				budget("google.com/tpu", "zeta", "16"),
				budget("nvidia.com/gpu", "alpha", "8"),
			},
			flavors: flavorSet("a100"),
			want:    map[string][]string{"team-a": {"alpha", "zeta"}},
		},
		{
			name:    "unreadable flavors report nothing missing",
			budgets: []v1beta1.AcceleratorBudget{budget("nvidia.com/gpu", "a100", "8")},
			flavors: nil,
			want:    map[string][]string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			quotas := []v1beta1.AcceleratorQuota{
				aq("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				aq("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, tc.budgets...),
			}
			node := nodeFor(t, quotas, "team-a")
			got := Render(backend.Plan{Write: []*tree.Node{node}}, tc.flavors, testOptions())

			if diff := cmp.Diff(tc.want, got.Skipped); diff != "" {
				t.Errorf("Render() Skipped mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// Rendering the same plan twice must produce the same objects, or every
// reconcile writes and the apply is never a no-op.
func TestRenderIsDeterministic(t *testing.T) {
	tests := []struct {
		name    string
		budgets []v1beta1.AcceleratorBudget
		flavors map[string]struct{}
	}{
		{
			name: "several flavors and resources",
			budgets: []v1beta1.AcceleratorBudget{
				budget("google.com/tpu", "tpu7x", "16"),
				budget("nvidia.com/gpu", "a100", "8"),
				budget("example.com/fpga", "a100", "4"),
			},
			flavors: flavorSet("a100", "tpu7x"),
		},
		{
			name:    "no budgets",
			budgets: nil,
			flavors: flavorSet("a100"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			quotas := []v1beta1.AcceleratorQuota{
				aq("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				aq("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, tc.budgets...),
			}
			node := nodeFor(t, quotas, "team-a")
			plan := backend.Plan{Write: []*tree.Node{node}, Retain: sets.New[string]()}

			first := flattenCQ(t, Render(plan, tc.flavors, testOptions()))
			second := flattenCQ(t, Render(plan, tc.flavors, testOptions()))

			if diff := cmp.Diff(first, second); diff != "" {
				t.Errorf("Render() is not deterministic (-first +second):\n%s", diff)
			}
		})
	}
}

// Render must not write through to the caller's AcceleratorQuota objects; the
// controller reuses the listed items for status and would otherwise persist
// whatever the renderer sorted in place.
func TestRenderDoesNotMutateInput(t *testing.T) {
	tests := []struct {
		name       string
		namespaces []string
		budgets    []v1beta1.AcceleratorBudget
	}{
		{
			name:       "namespaces are sorted for the selector",
			namespaces: []string{"ns-z", "ns-a"},
			budgets:    []v1beta1.AcceleratorBudget{budget("nvidia.com/gpu", "a100", "8")},
		},
		{
			name:       "budgets are filtered for missing flavors",
			namespaces: []string{"ns-a"},
			budgets: []v1beta1.AcceleratorBudget{
				budget("nvidia.com/gpu", "typo", "8"),
				budget("google.com/tpu", "tpu7x", "16"),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			quotas := []v1beta1.AcceleratorQuota{
				aq("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				aq("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, tc.budgets...),
			}
			node := nodeFor(t, quotas, "team-a")

			before := node.Quota.DeepCopy()
			Render(backend.Plan{Write: []*tree.Node{node}}, flavorSet("a100", "tpu7x"), testOptions())

			if diff := cmp.Diff(before, node.Quota, cmp.Comparer(quantityEqual)); diff != "" {
				t.Errorf("Render() mutated the caller's quota (-before +after):\n%s", diff)
			}
		})
	}
}

// Quantity carries unexported cached state, so compare on value.
func quantityEqual(a, b resource.Quantity) bool { return a.Cmp(b) == 0 }

// Nominal is a ceiling unless a budget says otherwise, and the rendered queue
// has to say so explicitly: Kueue reads an absent borrowingLimit as unbounded,
// so leaving the field off translates OME's "this leaf's share" into "at least
// this much, plus whatever the cohort is not using".
//
// It is worse than an over-large grant, because this plane sets no preemption
// and Kueue defaults reclaimWithinCohort to Never: an unbounded borrower cannot
// be made to give a share back, so the number would be neither a ceiling for
// the borrower nor a floor for the lender.
func TestRenderBorrowingIsOffUnlessAuthored(t *testing.T) {
	limit := func(q *resource.Quantity) *v1beta1.AcceleratorBudget {
		return &v1beta1.AcceleratorBudget{
			ResourceName:   "nvidia.com/gpu",
			ResourceFlavor: "a100",
			Nominal:        resource.MustParse("8"),
			BorrowingLimit: q,
		}
	}
	four := resource.MustParse("4")
	zero := resource.MustParse("0")

	tests := []struct {
		name   string
		budget *v1beta1.AcceleratorBudget
		want   string
	}{
		{
			name:   "an unauthored limit is rendered as no borrowing",
			budget: limit(nil),
			want:   "0",
		},
		{
			name:   "an authored limit is passed through",
			budget: limit(&four),
			want:   "4",
		},
		{
			// Explicitly zero and absent must render the same, or an admin who
			// writes the safe value gets a different queue from one who omits it.
			name:   "an explicit zero is the same as none",
			budget: limit(&zero),
			want:   "0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			funded := map[string]v1beta1.AcceleratorBudget{"nvidia.com/gpu": *tc.budget}
			got := resourceQuota("nvidia.com/gpu", funded, Options{})
			if got.BorrowingLimit == nil {
				t.Fatal("no borrowing limit rendered; Kueue reads that as unbounded")
			}
			if s := got.BorrowingLimit.String(); s != tc.want {
				t.Errorf("borrowingLimit = %s, want %s", s, tc.want)
			}
		})
	}
}
