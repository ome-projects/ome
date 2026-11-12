package gang

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"sort"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podgroup"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// grownInventory is the inventory's picture read the way a read that grows
// its maps and its entries one group at a time builds it, kept as the
// reference the sized read is held to: every listed object by name, the
// owner's by name again, and the owner's entries sorted by name.
type grownInventory struct {
	byName, ownedName map[string]*schedulingv1alpha1.PodGroup
	owned             []PodGroupInventoryEntry
}

func readGrowing(items []schedulingv1alpha1.PodGroup, ownerUID types.UID) grownInventory {
	g := grownInventory{byName: map[string]*schedulingv1alpha1.PodGroup{}, ownedName: map[string]*schedulingv1alpha1.PodGroup{}}
	for idx := range items {
		pg := &items[idx]
		g.byName[pg.Name] = pg
		if !podgroup.ControlledByUID(pg, ownerUID) {
			continue
		}
		g.ownedName[pg.Name] = pg
		entry := PodGroupInventoryEntry{Name: pg.Name, PodGroup: pg}
		if index, ok := podGroupIndex(pg); ok {
			entry.Index = index
			entry.IndexKnown = true
		}
		g.owned = append(g.owned, entry)
	}
	sort.Slice(g.owned, func(a, b int) bool { return g.owned[a].Name < g.owned[b].Name })
	return g
}

// inventoryDraw is one random list of groups for one owner: up to twelve
// groups with distinct names, each the owner's, another owner's or
// nobody's, its index label right, missing, a word, negative or with a
// leading zero, in random order; one draw in ten the API is gone and one
// in twenty the list fails.
type inventoryDraw struct {
	items                []schedulingv1alpha1.PodGroup
	err                  error
	owned, others, noRef int
	noLabel, badLabel    int
	notFound, failed     bool
}

var errListFailed = errors.New("the list failed")

func drawInventory(t *testing.T, r *rand.Rand, owner, other *inventoryOwners) inventoryDraw {
	t.Helper()
	var d inventoryDraw
	switch r.IntN(20) {
	case 0:
		d.notFound, d.err = true, noPodGroupKind()
	case 1:
		d.failed, d.err = true, errListFailed
	}
	indexes := r.Perm(40)[:r.IntN(13)]
	for _, index := range indexes {
		group := *owner.group(t, int32(index))
		switch r.IntN(6) {
		case 0:
			group.OwnerReferences = other.group(t, int32(index)).OwnerReferences
			d.others++
		case 1:
			group.OwnerReferences = nil
			d.noRef++
		default:
			d.owned++
		}
		switch r.IntN(8) {
		case 0:
			delete(group.Labels, query.LabelInstanceIdx)
			d.noLabel++
		case 1:
			group.Labels[query.LabelInstanceIdx] = []string{"x", "-1", "01", "2147483648"}[r.IntN(4)]
			d.badLabel++
		}
		d.items = append(d.items, group)
	}
	return d
}

// inventoryOwners builds the groups of one owner at any index, each once.
type inventoryOwners struct {
	groups map[int32]*schedulingv1alpha1.PodGroup
	build  func(t *testing.T, index int32) *schedulingv1alpha1.PodGroup
}

func (o *inventoryOwners) group(t *testing.T, index int32) *schedulingv1alpha1.PodGroup {
	t.Helper()
	if group, ok := o.groups[index]; ok {
		return group
	}
	group := o.build(t, index)
	o.groups[index] = group
	return group
}

// requireSameInventory holds the inventory to the grown reference: the same
// names found, the same objects at them, the same owned entries in the same
// order with the same index readings.
func requireSameInventory(t *testing.T, draw int, inv *PodGroupInventory, want grownInventory) {
	t.Helper()
	if len(inv.byName) != len(want.byName) || len(inv.ownedName) != len(want.ownedName) {
		t.Fatalf("draw %d: %d names and %d owned, want %d and %d", draw, len(inv.byName), len(inv.ownedName), len(want.byName), len(want.ownedName))
	}
	for name, group := range want.byName {
		if got, found := inv.ByName(name); !found || got != group {
			t.Fatalf("draw %d: %s found=%v as %p, want %p", draw, name, found, got, group)
		}
	}
	for name, group := range want.ownedName {
		if got, found := inv.OwnedByName(name); !found || got != group {
			t.Fatalf("draw %d: owned %s found=%v as %p, want %p", draw, name, found, got, group)
		}
	}
	if !slices.Equal(inv.OwnedEntries(), want.owned) {
		t.Fatalf("draw %d: owned entries %+v, want %+v", draw, inv.OwnedEntries(), want.owned)
	}
}

// TestPodGroupInventoryRead_AgreesWithTheGrowingReadOnRandomLists draws
// 200,000 lists of groups and holds the inventory the sized read builds
// equal to the one a read that grows its maps and entries one group at a
// time builds: the same names, the same objects at them, the same owned
// entries in the same order, and the same answer when the API is gone or
// the list fails.
func TestPodGroupInventoryRead_AgreesWithTheGrowingReadOnRandomLists(t *testing.T) {
	r := rand.New(rand.NewPCG(28, 1725))
	ownerObj, otherObj := newOwner("prod", "llama"), newOwner("prod", "other")
	owner := &inventoryOwners{groups: map[int32]*schedulingv1alpha1.PodGroup{},
		build: func(t *testing.T, index int32) *schedulingv1alpha1.PodGroup {
			return inventoryPodGroup(t, ownerObj, ownerObj.GetName(), index)
		}}
	other := &inventoryOwners{groups: map[int32]*schedulingv1alpha1.PodGroup{},
		build: func(t *testing.T, index int32) *schedulingv1alpha1.PodGroup {
			return inventoryPodGroup(t, otherObj, ownerObj.GetName(), index)
		}}
	const draws = 200_000
	var tally inventoryDraw
	var notFound, failed int
	for draw := 0; draw < draws; draw++ {
		d := drawInventory(t, r, owner, other)
		stored := &storedGroups{groups: d.items, err: d.err}
		inv, err := ObserveCachedPodGroups(context.Background(), stored, ownerObj)
		switch {
		case d.failed:
			if !errors.Is(err, errListFailed) || inv != nil {
				t.Fatalf("draw %d: a failed list answered %v, %v", draw, inv, err)
			}
			failed++
			continue
		case err != nil:
			t.Fatalf("draw %d: ObserveCachedPodGroups: %v", draw, err)
		case d.notFound:
			if inv.Available() || len(inv.OwnedEntries()) != 0 || len(inv.byName) != 0 {
				t.Fatalf("draw %d: the API gone left %+v", draw, inv)
			}
			notFound++
			continue
		}
		requireSameInventory(t, draw, inv, readGrowing(d.items, ownerObj.GetUID()))
		tally.owned += d.owned
		tally.others += d.others
		tally.noRef += d.noRef
		tally.noLabel += d.noLabel
		tally.badLabel += d.badLabel
	}
	t.Logf("%d draws: %d owned groups, %d of another owner, %d with no controller, %d with no index label, %d with a label that reads no index, %d lists with the API gone, %d failed",
		draws, tally.owned, tally.others, tally.noRef, tally.noLabel, tally.badLabel, notFound, failed)
	if tally.others == 0 || tally.noRef == 0 || tally.noLabel == 0 || tally.badLabel == 0 || notFound == 0 || failed == 0 {
		t.Fatalf("the draws left a picture out")
	}
}
