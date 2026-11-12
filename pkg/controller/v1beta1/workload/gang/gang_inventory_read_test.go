package gang

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateGangInventoryReadGoldens = flag.Bool("update-gang-inventory-read", false, "re-record testdata/gang_inventory_read_allocations.json from this run")

// storedGroups answers a PodGroup list with the groups it holds, uncopied,
// so a count over a read through it is the inventory's own work and none
// of it the copy the informer makes of every group. It serves nothing
// else.
type storedGroups struct {
	groups []schedulingv1alpha1.PodGroup
	err    error
	lists  int
}

func (s *storedGroups) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("stored groups: a read of one object")
}

func (s *storedGroups) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	typed, ok := list.(*schedulingv1alpha1.PodGroupList)
	if !ok {
		return fmt.Errorf("stored groups: a list of %T", list)
	}
	s.lists++
	if s.err != nil {
		return s.err
	}
	typed.Items = s.groups
	return nil
}

// noPodGroupKind is the apiserver's answer to a list of a kind it does
// not serve.
func noPodGroupKind() error {
	return apierrors.NewNotFound(schema.GroupResource{Group: "scheduling.x-k8s.io", Resource: "podgroups"}, "")
}

// readOwnerGroups is the read every pass of a gang Component makes of the
// owner's PodGroups through the informer cache.
func readOwnerGroups(tb testing.TB, reader client.Reader, owner client.Object) *PodGroupInventory {
	tb.Helper()
	inv, err := ObserveCachedPodGroups(context.Background(), reader, owner)
	if err != nil {
		tb.Fatalf("ObserveCachedPodGroups: %v", err)
	}
	return inv
}

// requireInventoryAtRest holds what the golden cannot express as a count:
// the inventory is the cache's picture, every planned gang's group is in
// it under the gang's name and owned, and its entries are every gang in
// name order, each with its index read.
func (f *podGroupPassFixture) requireInventoryAtRest(tb testing.TB, inv *PodGroupInventory) {
	tb.Helper()
	if !inv.Available() || !inv.Cached() {
		tb.Fatalf("the inventory reads available=%v cached=%v; want the cache's picture of a cluster with the API", inv.Available(), inv.Cached())
	}
	for _, inst := range f.plan.Instances {
		name := f.groupNameOf(inst.Index)
		if group, found := inv.OwnedByName(name); !found || group.Name != name {
			tb.Fatalf("gang %d: its group %s is not owned in the inventory", inst.Index, name)
		}
	}
	entries := inv.OwnedEntries()
	if len(entries) != len(f.plan.Instances) {
		tb.Fatalf("the inventory holds %d owned entries; want one per gang, %d", len(entries), len(f.plan.Instances))
	}
	for i, entry := range entries {
		index, ok := query.PodGroupIndex(entry.Name, f.input.Key.OwnerName, f.plan.Component)
		if !ok || !entry.IndexKnown || entry.Index != index {
			tb.Fatalf("entry %s reads index %d (known %v); want %d", entry.Name, entry.Index, entry.IndexKnown, index)
		}
		if i > 0 && entries[i-1].Name >= entry.Name {
			tb.Fatalf("entries %s and %s are not in name order", entries[i-1].Name, entry.Name)
		}
	}
}

// TestGangInventoryReadAtRestStaysWithinBudget holds the read of the
// owner's PodGroups to its golden: per shape, the allocations and the
// bytes of reading every gang's group from the cache into the inventory,
// the groups served uncopied so the count is the read's own; beside the
// golden, one list per read, every gang's group owned under its name, and
// the entries in name order with their indexes read.
func TestGangInventoryReadAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "gang_inventory_read_allocations.json"), *updateGangInventoryReadGoldens)
	defer golden.Close()
	for _, shape := range podGroupPassShapes() {
		f := newPodGroupPassFixture(t, shape, true)
		stored := &storedGroups{groups: f.cached.groups}
		read := func() { readOwnerGroups(t, stored, f.input.OwnerObject) }
		golden.Check(shape.name+"/inventory-read/allocations", read)
		golden.CheckBytes(shape.name+"/inventory-read/bytes", read)
		before := stored.lists
		f.requireInventoryAtRest(t, readOwnerGroups(t, stored, f.input.OwnerObject))
		if lists := stored.lists - before; lists != 1 {
			t.Fatalf("%s: one read listed the groups %d times; want once", shape.name, lists)
		}
	}
}

// BenchmarkGangInventoryRead is the wall time and the allocations of
// reading the owner's PodGroups from the cache, the groups served
// uncopied.
func BenchmarkGangInventoryRead(b *testing.B) {
	for _, shape := range podGroupPassShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newPodGroupPassFixture(b, shape, true)
			stored := &storedGroups{groups: f.cached.groups}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				readOwnerGroups(b, stored, f.input.OwnerObject)
			}
		})
	}
}

// inventoryReadCase is one picture of the groups a reader lists for an
// owner and what the inventory holds after reading it.
type inventoryReadCase struct {
	name string
	// groups are the objects the reader serves, in the order it serves
	// them; listErr is the error it answers instead.
	groups  func(t *testing.T, owner, other client.Object) []schedulingv1alpha1.PodGroup
	listErr error
	// wantOwned are the owned entries' names in the order the inventory
	// keeps them; wantIndexUnknown those of them whose index is not read.
	wantOwned        []string
	wantIndexUnknown []string
	// wantByName are the names found by name, owned or not.
	wantByName      []string
	wantUnavailable bool
}

// ownedGroups builds the owner's group of each index with the labels the
// pass writes.
func ownedGroups(t *testing.T, owner client.Object, indexes ...int32) []schedulingv1alpha1.PodGroup {
	t.Helper()
	groups := make([]schedulingv1alpha1.PodGroup, 0, len(indexes))
	for _, index := range indexes {
		groups = append(groups, *inventoryPodGroup(t, owner, owner.GetName(), index))
	}
	return groups
}

// groupNames are the owner's group names of the indexes, in that order.
func groupNames(owner client.Object, indexes ...int32) []string {
	names := make([]string, 0, len(indexes))
	for _, index := range indexes {
		names = append(names, query.PodGroupName(owner.GetName(), "engine", index))
	}
	return names
}

// inventoryOrderCases are the pictures of the list's order and length: the
// entries are kept in name order whatever order the list had, and a list
// with nothing, or no list at all, leaves an inventory holding nothing.
func inventoryOrderCases(owner client.Object) []inventoryReadCase {
	return []inventoryReadCase{
		{name: "no group listed",
			groups: func(*testing.T, client.Object, client.Object) []schedulingv1alpha1.PodGroup { return nil }},
		{name: "the API gone",
			groups:          func(*testing.T, client.Object, client.Object) []schedulingv1alpha1.PodGroup { return nil },
			listErr:         noPodGroupKind(),
			wantUnavailable: true},
		{name: "groups listed in index order are kept in name order",
			groups: func(t *testing.T, owner, _ client.Object) []schedulingv1alpha1.PodGroup {
				return ownedGroups(t, owner, 1, 2, 10)
			},
			wantOwned:  groupNames(owner, 1, 10, 2),
			wantByName: groupNames(owner, 1, 10, 2)},
		{name: "groups listed in reverse name order",
			groups: func(t *testing.T, owner, _ client.Object) []schedulingv1alpha1.PodGroup {
				return ownedGroups(t, owner, 2, 10, 1)
			},
			wantOwned:  groupNames(owner, 1, 10, 2),
			wantByName: groupNames(owner, 1, 10, 2)},
	}
}

// inventoryOwnerCases are the pictures of who owns a listed group: another
// owner's group at one of this owner's names and a group with no
// controller are found by name and owned by nobody.
func inventoryOwnerCases(owner client.Object) []inventoryReadCase {
	return []inventoryReadCase{
		{name: "another owner's group at one of our names",
			groups: func(t *testing.T, owner, other client.Object) []schedulingv1alpha1.PodGroup {
				return append(ownedGroups(t, owner, 7), *inventoryPodGroup(t, other, owner.GetName(), 8))
			},
			wantOwned:  groupNames(owner, 7),
			wantByName: groupNames(owner, 7, 8)},
		{name: "a group with no controller reference",
			groups: func(t *testing.T, owner, _ client.Object) []schedulingv1alpha1.PodGroup {
				groups := ownedGroups(t, owner, 7, 8)
				groups[1].OwnerReferences = nil
				return groups
			},
			wantOwned:  groupNames(owner, 7),
			wantByName: groupNames(owner, 7, 8)},
	}
}

// inventoryIndexCases are the pictures of the index label: a group with no
// label, a label that is not a number and a negative one are owned and
// kept in name order with their index unread.
func inventoryIndexCases(owner client.Object) []inventoryReadCase {
	relabelled := func(label string, set bool) func(t *testing.T, owner, _ client.Object) []schedulingv1alpha1.PodGroup {
		return func(t *testing.T, owner, _ client.Object) []schedulingv1alpha1.PodGroup {
			groups := ownedGroups(t, owner, 3, 4, 5)
			if set {
				groups[1].Labels[query.LabelInstanceIdx] = label
			} else {
				delete(groups[1].Labels, query.LabelInstanceIdx)
			}
			return groups
		}
	}
	return []inventoryReadCase{
		{name: "a group with no index label", groups: relabelled("", false),
			wantOwned: groupNames(owner, 3, 4, 5), wantIndexUnknown: groupNames(owner, 4), wantByName: groupNames(owner, 3, 4, 5)},
		{name: "a group whose index label is not a number", groups: relabelled("x", true),
			wantOwned: groupNames(owner, 3, 4, 5), wantIndexUnknown: groupNames(owner, 4), wantByName: groupNames(owner, 3, 4, 5)},
		{name: "a group whose index label is negative", groups: relabelled("-1", true),
			wantOwned: groupNames(owner, 3, 4, 5), wantIndexUnknown: groupNames(owner, 4), wantByName: groupNames(owner, 3, 4, 5)},
	}
}

// requireInventory holds the inventory to a picture's expectations.
func requireInventory(t *testing.T, inv *PodGroupInventory, tc inventoryReadCase) {
	t.Helper()
	if inv.Available() == tc.wantUnavailable {
		t.Fatalf("available=%v, want %v", inv.Available(), !tc.wantUnavailable)
	}
	var owned, unknown []string
	for _, entry := range inv.OwnedEntries() {
		owned = append(owned, entry.Name)
		if !entry.IndexKnown {
			unknown = append(unknown, entry.Name)
		}
		if index, ok := query.PodGroupIndex(entry.Name, "llama", "engine"); entry.IndexKnown && (!ok || index != entry.Index) {
			t.Fatalf("entry %s reads index %d; want the index its name spells", entry.Name, entry.Index)
		}
	}
	if !slices.Equal(owned, tc.wantOwned) || !slices.Equal(unknown, tc.wantIndexUnknown) {
		t.Fatalf("owned entries %v (index unread on %v), want %v (%v)", owned, unknown, tc.wantOwned, tc.wantIndexUnknown)
	}
	for _, name := range tc.wantByName {
		if group, found := inv.ByName(name); !found || group.Name != name {
			t.Fatalf("%s is not found by name", name)
		}
		if _, found := inv.OwnedByName(name); found != slices.Contains(tc.wantOwned, name) {
			t.Fatalf("%s owned=%v, want %v", name, found, !found)
		}
	}
	if _, found := inv.ByName(query.PodGroupName("llama", "engine", 999)); found {
		t.Fatalf("a name nobody listed is found")
	}
}

// TestPodGroupInventoryRead_OnEveryPicture holds, for each picture of the
// groups a reader lists, what the inventory holds: which names are found,
// which are owned, the owned entries in name order with the index each
// reads, and whether the API answered; and that a group written after a
// read is found by name whatever the read answered.
func TestPodGroupInventoryRead_OnEveryPicture(t *testing.T) {
	owner, other := newOwner("prod", "llama"), newOwner("prod", "other")
	cases := slices.Concat(inventoryOrderCases(owner), inventoryOwnerCases(owner), inventoryIndexCases(owner))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stored := &storedGroups{groups: tc.groups(t, owner, other), err: tc.listErr}
			inv := readOwnerGroups(t, stored, owner)
			requireInventory(t, inv, tc)
			written := inventoryPodGroup(t, owner, owner.GetName(), 20)
			inv.recordReconciled(written)
			if _, found := inv.OwnedByName(written.Name); !found {
				t.Fatalf("a group written after the read is not found under its name")
			}
			if len(inv.OwnedEntries()) != len(tc.wantOwned) {
				t.Fatalf("a group written after the read joined the owned entries")
			}
		})
	}
}

// TestPodGroupInventoryRead_ASecondReadReplacesTheFirst: an inventory read
// from the cache and then from the apiserver holds the apiserver's groups
// alone, in name order, and is the apiserver's picture.
// secondReadCase is the apiserver's answer to a live read after the cache
// answered, and what the inventory holds once it is read.
type secondReadCase struct {
	name          string
	first, second *storedGroups
	wantNames     []string
	wantAvailable bool
	wantErr       bool
}

func secondReadCases(t *testing.T, owner client.Object) []secondReadCase {
	cached := func() *storedGroups { return &storedGroups{groups: ownedGroups(t, owner, 1, 2)} }
	return []secondReadCase{
		{name: "the apiserver lists one group the cache had and one it had not", first: cached(),
			second: &storedGroups{groups: ownedGroups(t, owner, 3, 2)}, wantNames: groupNames(owner, 2, 3), wantAvailable: true},
		{name: "the apiserver lists no group", first: cached(), second: &storedGroups{}, wantAvailable: true},
		{name: "the apiserver no longer serves the kind", first: cached(), second: &storedGroups{err: noPodGroupKind()}},
		{name: "the list fails", first: cached(), second: &storedGroups{err: errors.New("the list failed")},
			wantAvailable: true, wantErr: true},
		{name: "the cache had no kind and the apiserver lists two groups", first: &storedGroups{err: noPodGroupKind()},
			second: &storedGroups{groups: ownedGroups(t, owner, 3, 2)}, wantNames: groupNames(owner, 2, 3), wantAvailable: true},
	}
}

func TestPodGroupInventoryRead_ASecondReadReplacesTheFirst(t *testing.T) {
	owner := newOwner("prod", "llama")
	for _, tc := range secondReadCases(t, owner) {
		t.Run(tc.name, func(t *testing.T) {
			inv := readOwnerGroups(t, tc.first, owner)
			err := inv.ReadLive(context.Background(), tc.second)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ReadLive: %v, want an error %v", err, tc.wantErr)
			}
			var names []string
			for _, entry := range inv.OwnedEntries() {
				names = append(names, entry.Name)
			}
			if !slices.Equal(names, tc.wantNames) || inv.Available() != tc.wantAvailable {
				t.Fatalf("after the second read the inventory holds %v (available %v); want %v (available %v)",
					names, inv.Available(), tc.wantNames, tc.wantAvailable)
			}
			if inv.Cached() != tc.wantErr {
				t.Fatalf("cached %v after the second read; want the apiserver's picture unless the read failed", inv.Cached())
			}
			if _, found := inv.ByName(groupNames(owner, 1)[0]); found {
				t.Fatalf("a group the apiserver no longer lists is still found")
			}
			if !slices.IsSorted(names) {
				t.Fatalf("the entries %v are not in name order", names)
			}
		})
	}
}
