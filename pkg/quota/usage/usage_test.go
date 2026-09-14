package usage

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/tree"
)

const gpu = "nvidia.com/gpu"

func key(flavor string) Key { return Key{ResourceName: gpu, ResourceFlavor: flavor} }

func budget(flavor, nominal string) v1beta1.AcceleratorBudget {
	return v1beta1.AcceleratorBudget{
		ResourceName:   gpu,
		ResourceFlavor: flavor,
		Nominal:        resource.MustParse(nominal),
	}
}

func node(name, parent string, role v1beta1.AcceleratorQuotaRole, budgets ...v1beta1.AcceleratorBudget) v1beta1.AcceleratorQuota {
	q := v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1beta1.AcceleratorQuotaSpec{Role: role, Budgets: budgets},
	}
	if parent != "" {
		q.Spec.ParentRef = &v1beta1.AcceleratorQuotaParentRef{Name: parent}
	}
	return q
}

func observed(admitted, reserved, borrowed string) Observed {
	return Observed{
		Admitted: resource.MustParse(admitted),
		Reserved: resource.MustParse(reserved),
		Borrowed: resource.MustParse(borrowed),
	}
}

func buildTree(t *testing.T, quotas ...v1beta1.AcceleratorQuota) *tree.Tree {
	t.Helper()
	built, _, err := tree.Build(quotas, tree.Options{RootName: "root"})
	if err != nil {
		t.Fatalf("tree.Build() error = %v", err)
	}
	return built
}

// rendered flattens a roll into comparable strings. Comparing quantities
// directly would compare their unexported cached state, and comparing whole
// structs would report a diff as an unreadable dump.
type rendered struct{ Admitted, Reserved, Borrowed string }

func flatten(t *testing.T, got map[string]map[Key]Total) map[string]map[string]rendered {
	t.Helper()
	out := map[string]map[string]rendered{}
	for node, totals := range got {
		out[node] = map[string]rendered{}
		for _, k := range SortedKeys(totals) {
			v := totals[k]
			out[node][k.ResourceName+" on "+k.ResourceFlavor] = rendered{
				Admitted: v.Admitted.String(),
				Reserved: v.Reserved.String(),
				Borrowed: v.Borrowed.String(),
			}
		}
	}
	return out
}

// Every row here is a way the rollup could report a plausible number that is
// wrong. A tenant's consumption is what an operator reads before deciding
// whether a fleet is full, so a quietly wrong total is worse than none.
func TestRoll(t *testing.T) {
	tests := []struct {
		name   string
		quotas []v1beta1.AcceleratorQuota
		leaves map[string]map[Key]Observed
		want   map[string]map[string]rendered
	}{
		{
			name: "a leaf reports the backend's own figures unchanged",
			quotas: []v1beta1.AcceleratorQuota{
				node("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				node("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("a100", "8")),
			},
			leaves: map[string]map[Key]Observed{
				"team-a": {key("a100"): observed("6", "7", "2")},
			},
			want: map[string]map[string]rendered{
				"team-a": {"nvidia.com/gpu on a100": {"6", "7", "2"}},
				// The root has no budget, so it borrows nothing by definition.
				"root": {"nvidia.com/gpu on a100": {"6", "7", "0"}},
			},
		},
		{
			name: "siblings sum onto their parent",
			quotas: []v1beta1.AcceleratorQuota{
				node("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				node("org", "root", v1beta1.AcceleratorQuotaRoleCohort, budget("a100", "16")),
				node("team-a", "org", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("a100", "8")),
				node("team-b", "org", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("a100", "8")),
			},
			leaves: map[string]map[Key]Observed{
				"team-a": {key("a100"): observed("6", "6", "0")},
				"team-b": {key("a100"): observed("4", "5", "0")},
			},
			want: map[string]map[string]rendered{
				"team-a": {"nvidia.com/gpu on a100": {"6", "6", "0"}},
				"team-b": {"nvidia.com/gpu on a100": {"4", "5", "0"}},
				"org":    {"nvidia.com/gpu on a100": {"10", "11", "0"}},
				"root":   {"nvidia.com/gpu on a100": {"10", "11", "0"}},
			},
		},
		{
			// The reason Borrowed is not summed. team-a is over its own
			// nominal by 4 and reports borrowing; org holds 10 against its own
			// 16, so from outside the subtree org has borrowed NOTHING -- the
			// loan came from its own sibling. Summing would claim org borrowed
			// 4 chips it never took from anyone.
			name: "a loan between siblings is not borrowing by their parent",
			quotas: []v1beta1.AcceleratorQuota{
				node("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				node("org", "root", v1beta1.AcceleratorQuotaRoleCohort, budget("a100", "16")),
				node("team-a", "org", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("a100", "4")),
				node("team-b", "org", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("a100", "12")),
			},
			leaves: map[string]map[Key]Observed{
				"team-a": {key("a100"): observed("8", "8", "4")},
				"team-b": {key("a100"): observed("2", "2", "0")},
			},
			want: map[string]map[string]rendered{
				"team-a": {"nvidia.com/gpu on a100": {"8", "8", "4"}},
				"team-b": {"nvidia.com/gpu on a100": {"2", "2", "0"}},
				"org":    {"nvidia.com/gpu on a100": {"10", "10", "0"}},
				"root":   {"nvidia.com/gpu on a100": {"10", "10", "0"}},
			},
		},
		{
			// A parent genuinely over its own allowance does report borrowing.
			name: "a parent above its own nominal reports the overage",
			quotas: []v1beta1.AcceleratorQuota{
				node("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				node("org", "root", v1beta1.AcceleratorQuotaRoleCohort, budget("a100", "8")),
				node("team-a", "org", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("a100", "8")),
				node("team-b", "org", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("a100", "8")),
			},
			leaves: map[string]map[Key]Observed{
				"team-a": {key("a100"): observed("8", "8", "0")},
				"team-b": {key("a100"): observed("4", "4", "0")},
			},
			want: map[string]map[string]rendered{
				"team-a": {"nvidia.com/gpu on a100": {"8", "8", "0"}},
				"team-b": {"nvidia.com/gpu on a100": {"4", "4", "0"}},
				"org":    {"nvidia.com/gpu on a100": {"12", "12", "4"}},
				"root":   {"nvidia.com/gpu on a100": {"12", "12", "0"}},
			},
		},
		{
			// Reserved above Admitted is the wedged tenant: it owns the chips
			// and has not started on them. Dropping Reserved would make this
			// read as an idle tenant with spare capacity.
			name: "reserved above admitted is carried, not collapsed",
			quotas: []v1beta1.AcceleratorQuota{
				node("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				node("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("a100", "8")),
			},
			leaves: map[string]map[Key]Observed{
				"team-a": {key("a100"): observed("0", "8", "0")},
			},
			want: map[string]map[string]rendered{
				"team-a": {"nvidia.com/gpu on a100": {"0", "8", "0"}},
				"root":   {"nvidia.com/gpu on a100": {"0", "8", "0"}},
			},
		},
		{
			// Distinct pairs never merge: two flavors of the same resource are
			// separate quota, and adding them would report a fleet as fuller
			// than it is.
			name: "distinct flavors stay separate",
			quotas: []v1beta1.AcceleratorQuota{
				node("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				node("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue,
					budget("a100", "8"), budget("h100", "4")),
			},
			leaves: map[string]map[Key]Observed{
				"team-a": {
					key("a100"): observed("6", "6", "0"),
					key("h100"): observed("1", "1", "0"),
				},
			},
			want: map[string]map[string]rendered{
				"team-a": {
					"nvidia.com/gpu on a100": {"6", "6", "0"},
					"nvidia.com/gpu on h100": {"1", "1", "0"},
				},
				"root": {
					"nvidia.com/gpu on a100": {"6", "6", "0"},
					"nvidia.com/gpu on h100": {"1", "1", "0"},
				},
			},
		},
		{
			// No queue materialized yet is not the same claim as a queue
			// holding nothing, and the status should not invent the latter.
			name: "a node with no reading reports nothing at all",
			quotas: []v1beta1.AcceleratorQuota{
				node("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				node("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("a100", "8")),
				node("team-b", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("a100", "8")),
			},
			leaves: map[string]map[Key]Observed{
				"team-a": {key("a100"): observed("2", "2", "0")},
			},
			want: map[string]map[string]rendered{
				"team-a": {"nvidia.com/gpu on a100": {"2", "2", "0"}},
				"root":   {"nvidia.com/gpu on a100": {"2", "2", "0"}},
			},
		},
		{
			name:   "nothing observed rolls to nothing",
			quotas: []v1beta1.AcceleratorQuota{node("root", "", v1beta1.AcceleratorQuotaRoleCohort)},
			leaves: nil,
			want:   map[string]map[string]rendered{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Roll(buildTree(t, tc.quotas...), tc.leaves)
			if diff := cmp.Diff(tc.want, flatten(t, got)); diff != "" {
				t.Errorf("Roll() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// The caller's readings are reused for status, so a roll that accumulated into
// them in place would persist whatever it summed.
func TestRollDoesNotMutateInput(t *testing.T) {
	tests := []struct {
		name   string
		leaves map[string]map[Key]Observed
	}{
		{
			name: "two leaves under one parent",
			leaves: map[string]map[Key]Observed{
				"team-a": {key("a100"): observed("6", "6", "1")},
				"team-b": {key("a100"): observed("4", "4", "0")},
			},
		},
		{
			name: "one leaf, several pairs",
			leaves: map[string]map[Key]Observed{
				"team-a": {key("a100"): observed("6", "7", "1"), key("h100"): observed("2", "2", "0")},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := map[string]map[string]rendered{}
			for n, m := range tc.leaves {
				before[n] = map[string]rendered{}
				for k, o := range m {
					before[n][k.ResourceFlavor] = rendered{o.Admitted.String(), o.Reserved.String(), o.Borrowed.String()}
				}
			}

			built := buildTree(t,
				node("root", "", v1beta1.AcceleratorQuotaRoleCohort),
				node("team-a", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("a100", "4"), budget("h100", "4")),
				node("team-b", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, budget("a100", "4")),
			)
			Roll(built, tc.leaves)

			after := map[string]map[string]rendered{}
			for n, m := range tc.leaves {
				after[n] = map[string]rendered{}
				for k, o := range m {
					after[n][k.ResourceFlavor] = rendered{o.Admitted.String(), o.Reserved.String(), o.Borrowed.String()}
				}
			}
			if diff := cmp.Diff(before, after); diff != "" {
				t.Errorf("Roll() mutated the caller's readings (-before +after):\n%s", diff)
			}
		})
	}
}

// Status is rewritten only when it changes, so the key order has to be stable
// across rolls or an idle fleet spins its own watch.
func TestSortedKeysIsStable(t *testing.T) {
	tests := []struct {
		name   string
		totals map[Key]Total
		want   []string
	}{
		{
			name:   "empty",
			totals: map[Key]Total{},
			want:   []string{},
		},
		{
			name: "ordered by resource then flavor",
			totals: map[Key]Total{
				{ResourceName: "z.com/tpu", ResourceFlavor: "a"}: {},
				{ResourceName: gpu, ResourceFlavor: "h100"}:      {},
				{ResourceName: gpu, ResourceFlavor: "a100"}:      {},
			},
			want: []string{"nvidia.com/gpu|a100", "nvidia.com/gpu|h100", "z.com/tpu|a"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := []string{}
			for _, k := range SortedKeys(tc.totals) {
				got = append(got, k.ResourceName+"|"+k.ResourceFlavor)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("SortedKeys() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
