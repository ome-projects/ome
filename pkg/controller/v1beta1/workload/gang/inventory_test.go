package gang

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podgroup"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestObservePodGroups_OneListIndexesOwnedAndRetainsForeignNames(t *testing.T) {
	owner := newOwner("prod", "llama")
	foreignOwner := newOwner("prod", "other")
	owned := inventoryPodGroup(t, owner, "llama", 7)
	foreign := inventoryPodGroup(t, foreignOwner, "llama", 8)
	base := newGangClient(t, owner, foreignOwner, owned, foreign)
	lists := fakeclient.CountingVerb(fakeclient.List).OnKind(&schedulingv1alpha1.PodGroupList{})
	gets := fakeclient.CountingVerb(fakeclient.Get).OnKind(&schedulingv1alpha1.PodGroup{})
	counting := fakeclient.Wrap(t, base, lists, gets)

	inv, err := ObservePodGroups(context.Background(), counting, owner)
	if err != nil {
		t.Fatalf("ObservePodGroups: %v", err)
	}
	if lists.Count() != 1 || gets.Count() != 0 {
		t.Fatalf("inventory reads: lists=%d gets=%d, want exactly one LIST and zero GETs", lists.Count(), gets.Count())
	}
	if !inv.Available() || inv.OwnerUID() != owner.GetUID() {
		t.Fatalf("inventory identity: available=%v ownerUID=%q", inv.Available(), inv.OwnerUID())
	}
	if got, found := inv.ByName(owned.Name); !found || got.Name != owned.Name {
		t.Fatalf("owned collision lookup: found=%v got=%v", found, got)
	}
	if got, found := inv.OwnedByName(owned.Name); !found || got.Name != owned.Name {
		t.Fatalf("owned name lookup: found=%v got=%v", found, got)
	}
	if _, found := inv.OwnedByName(foreign.Name); found {
		t.Fatalf("foreign PodGroup appeared in owned lookup")
	}
	if got, found := inv.ByName(foreign.Name); !found || got.Name != foreign.Name {
		t.Fatalf("foreign name collision was not retained: found=%v got=%v", found, got)
	}
	entries := inv.OwnedEntries()
	if len(entries) != 1 || !entries[0].IndexKnown || entries[0].Index != 7 || entries[0].Name != owned.Name {
		t.Fatalf("owned entries: %+v", entries)
	}
}

// TestObserveCachedPodGroups_ReadsTheOwnersGroupsThroughTheIndex: the
// cache's inventory holds the groups this owner controls, found through
// the controller-UID index whatever their labels say, and none of another
// owner's, even one labelled as this owner's; it is one list and no get,
// and it says it is the cache's picture.
func TestObserveCachedPodGroups_ReadsTheOwnersGroupsThroughTheIndex(t *testing.T) {
	owner := newOwner("prod", "llama")
	otherOwner := newOwner("prod", "other")
	owned := inventoryPodGroup(t, owner, "llama", 7)
	owned.Labels = nil
	labelledAsOurs := inventoryPodGroup(t, otherOwner, "llama", 8)
	labelledAsOurs.Labels = map[string]string{
		constants.InferenceServicePodLabelKey: owner.GetName(),
		constants.OMEComponentLabel:           string(types.ComponentEngine),
		query.LabelManagedBy:                  query.ManagedByOMENative,
		query.LabelInstanceIdx:                "8",
	}
	base := newGangClient(t, owner, otherOwner, owned, labelledAsOurs)
	lists := fakeclient.CountingVerb(fakeclient.List).OnKind(&schedulingv1alpha1.PodGroupList{})
	gets := fakeclient.CountingVerb(fakeclient.Get).OnKind(&schedulingv1alpha1.PodGroup{})
	counting := fakeclient.Wrap(t, base, lists, gets)

	inv, err := ObserveCachedPodGroups(context.Background(), counting, owner)
	if err != nil {
		t.Fatalf("ObserveCachedPodGroups: %v", err)
	}
	if lists.Count() != 1 || gets.Count() != 0 {
		t.Fatalf("inventory reads: lists=%d gets=%d, want exactly one LIST and zero GETs", lists.Count(), gets.Count())
	}
	if !inv.Available() || !inv.Cached() || inv.OwnerUID() != owner.GetUID() {
		t.Fatalf("inventory identity: available=%v cached=%v ownerUID=%q", inv.Available(), inv.Cached(), inv.OwnerUID())
	}
	if _, found := inv.OwnedByName(owned.Name); !found {
		t.Fatalf("the owned group with no labels is missing from the cache's inventory")
	}
	if _, found := inv.ByName(labelledAsOurs.Name); found {
		t.Fatalf("another owner's group labelled as ours is in the cache's inventory; the index reads the controller UID")
	}
	if entries := inv.OwnedEntries(); len(entries) != 1 || entries[0].Name != owned.Name {
		t.Fatalf("owned entries: %+v", entries)
	}
}

// TestPodGroupInventory_ReadLiveTakesTheApiserversPicture: a cached
// inventory that is behind the apiserver, a group created since and
// another owner's object at one of this owner's names, reads both on
// ReadLive, once; a second ReadLive lists nothing, and an inventory the
// apiserver served lists nothing either.
func TestPodGroupInventory_ReadLiveTakesTheApiserversPicture(t *testing.T) {
	owner := newOwner("prod", "llama")
	otherOwner := newOwner("prod", "other")
	created := inventoryPodGroup(t, owner, "llama", 1)
	occupied := inventoryPodGroup(t, otherOwner, "llama", 2)
	cache := newGangClient(t, owner)
	liveLists := fakeclient.CountingVerb(fakeclient.List).OnKind(&schedulingv1alpha1.PodGroupList{})
	live := fakeclient.Wrap(t, newGangClient(t, owner, otherOwner, created, occupied), liveLists)

	inv, err := ObserveCachedPodGroups(context.Background(), cache, owner)
	if err != nil {
		t.Fatalf("ObserveCachedPodGroups: %v", err)
	}
	if _, found := inv.ByName(created.Name); found {
		t.Fatalf("premise: the cache must not show the group yet")
	}
	for round := 1; round <= 2; round++ {
		if err := inv.ReadLive(context.Background(), live); err != nil {
			t.Fatalf("ReadLive %d: %v", round, err)
		}
	}
	if liveLists.Count() != 1 {
		t.Fatalf("ReadLive listed the apiserver %d time(s), want once", liveLists.Count())
	}
	if inv.Cached() || !inv.Available() {
		t.Fatalf("after ReadLive: cached=%v available=%v, want the apiserver's available picture", inv.Cached(), inv.Available())
	}
	if _, found := inv.OwnedByName(created.Name); !found {
		t.Fatalf("the group created since the cache's read is missing after ReadLive")
	}
	if pg, found := inv.ByName(occupied.Name); !found || podgroup.ControlledByUID(pg, owner.GetUID()) {
		t.Fatalf("another owner's object at our name is not retained by name after ReadLive: found=%v", found)
	}
	fromLive, err := ObservePodGroups(context.Background(), live, owner)
	if err != nil {
		t.Fatalf("ObservePodGroups: %v", err)
	}
	before := liveLists.Count()
	if err := fromLive.ReadLive(context.Background(), live); err != nil || liveLists.Count() != before {
		t.Fatalf("ReadLive on the apiserver's own picture: err=%v lists=%d, want no list", err, liveLists.Count()-before)
	}
}

// TestPodGroupInventory_DeletesRefuseTheCachesPicture: a reset and a
// finalization through a cached inventory refuse, delete nothing and name
// the reason; after ReadLive the same calls delete the group.
func TestPodGroupInventory_DeletesRefuseTheCachesPicture(t *testing.T) {
	owner := newOwner("prod", "llama")
	pg := inventoryPodGroup(t, owner, "llama", 4)
	base := newGangClient(t, owner, pg)
	deletes := fakeclient.CountingVerb(fakeclient.Delete).OnKind(&schedulingv1alpha1.PodGroup{})
	counting := fakeclient.Wrap(t, base, deletes)

	inv, err := ObserveCachedPodGroups(context.Background(), base, owner)
	if err != nil {
		t.Fatalf("ObserveCachedPodGroups: %v", err)
	}
	if err := inv.DeleteOwnedName(context.Background(), counting, pg.Name); !errors.Is(err, ErrInventoryCached) {
		t.Fatalf("DeleteOwnedName on the cache's picture: got %v, want %v", err, ErrInventoryCached)
	}
	if _, err := inv.FinalizeOwnedName(context.Background(), counting, pg.Name); !errors.Is(err, ErrInventoryCached) {
		t.Fatalf("FinalizeOwnedName on the cache's picture: got %v, want %v", err, ErrInventoryCached)
	}
	if deletes.Count() != 0 {
		t.Fatalf("the cache's picture deleted %d group(s), want none", deletes.Count())
	}
	if err := inv.ReadLive(context.Background(), base); err != nil {
		t.Fatalf("ReadLive: %v", err)
	}
	if err := inv.DeleteOwnedName(context.Background(), counting, pg.Name); err != nil {
		t.Fatalf("DeleteOwnedName on the apiserver's picture: %v", err)
	}
	if deletes.Count() != 1 || !inv.DeleteAccepted(pg.Name) {
		t.Fatalf("after ReadLive: deletes=%d accepted=%v, want one accepted delete", deletes.Count(), inv.DeleteAccepted(pg.Name))
	}
}

// TestBuildFinalizeInstanceResources_ReadsACachedInventoryLiveFirst: the
// finalizer built over a cached inventory lists the apiserver once before
// its first delete, and the deletes that follow in the pass read nothing
// more.
func TestBuildFinalizeInstanceResources_ReadsACachedInventoryLiveFirst(t *testing.T) {
	owner := newOwner("prod", "llama")
	first := inventoryPodGroup(t, owner, "llama", 5)
	second := inventoryPodGroup(t, owner, "llama", 6)
	base := newGangClient(t, owner, first, second)
	liveLists := fakeclient.CountingVerb(fakeclient.List).OnKind(&schedulingv1alpha1.PodGroupList{})
	deletes := fakeclient.CountingVerb(fakeclient.Delete).OnKind(&schedulingv1alpha1.PodGroup{})
	counting := fakeclient.Wrap(t, base, liveLists, deletes)

	inv, err := ObserveCachedPodGroups(context.Background(), base, owner)
	if err != nil {
		t.Fatalf("ObserveCachedPodGroups: %v", err)
	}
	finalize := BuildFinalizeInstanceResources(counting, counting, inv, owner, "llama", types.ComponentEngine)
	for _, idx := range []int32{5, 6} {
		complete, err := finalize(context.Background(), idx)
		if err != nil {
			t.Fatalf("finalize %d: %v", idx, err)
		}
		if complete {
			t.Fatalf("finalize %d reported complete on the pass that requested the delete", idx)
		}
	}
	if liveLists.Count() != 1 || deletes.Count() != 2 {
		t.Fatalf("finalizer over a cached inventory: live lists=%d deletes=%d, want one list and two deletes", liveLists.Count(), deletes.Count())
	}
}

func TestPodGroupControllerUIDIndexExtractorSkipsNonControllers(t *testing.T) {
	if got := PodGroupControllerUIDIndexExtractor(&corev1.Pod{}); got != nil {
		t.Fatalf("non-PodGroup index values = %v, want nil", got)
	}
	if got := PodGroupControllerUIDIndexExtractor(&schedulingv1alpha1.PodGroup{}); got != nil {
		t.Fatalf("ownerless PodGroup index values = %v, want nil", got)
	}
}

func TestPodGroupIndexRejectsInvalidValues(t *testing.T) {
	for _, raw := range []string{"", "malformed", "-1", "2147483648"} {
		pg := &schedulingv1alpha1.PodGroup{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{query.LabelInstanceIdx: raw},
		}}
		if index, ok := podGroupIndex(pg); ok {
			t.Errorf("podGroupIndex(%q) = %d, true; want invalid", raw, index)
		}
	}
	pg := &schedulingv1alpha1.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Labels: map[string]string{query.LabelInstanceIdx: "2147483647"},
	}}
	if index, ok := podGroupIndex(pg); !ok || index != 2147483647 {
		t.Fatalf("podGroupIndex(max int32) = %d, %t", index, ok)
	}
}

func TestObserveCachedPodGroups_RuntimeAPIRemovalIsSoftFail(t *testing.T) {
	owner := newOwner("prod", "llama")
	base := newGangClient(t, owner)
	reader := fakeclient.Wrap(t, base, fakeclient.FailingVerb(fakeclient.List, fakeclient.EveryCall,
		&apimeta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "scheduling.x-k8s.io", Kind: "PodGroup"}}))
	inv, err := ObserveCachedPodGroups(context.Background(), reader, owner)
	if err != nil {
		t.Fatalf("runtime API removal must soft-fail: %v", err)
	}
	if inv.Available() || !inv.Cached() {
		t.Fatalf("NoMatch through the cache: available=%v cached=%v, want an unavailable cached inventory", inv.Available(), inv.Cached())
	}
}

func TestObservePodGroups_RuntimeAPIRemovalIsSoftFail(t *testing.T) {
	owner := newOwner("prod", "llama")
	base := newGangClient(t, owner)
	reader := fakeclient.Wrap(t, base, fakeclient.FailingVerb(fakeclient.List, fakeclient.EveryCall,
		&apimeta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "scheduling.x-k8s.io", Kind: "PodGroup"}}))
	inv, err := ObservePodGroups(context.Background(), reader, owner)
	if err != nil {
		t.Fatalf("runtime API removal must soft-fail: %v", err)
	}
	if inv.Available() {
		t.Fatal("NoMatch inventory must report unavailable")
	}
}

func TestObservePodGroups_UnregisteredOptionalTypeIsSoftFail(t *testing.T) {
	owner := newOwner("prod", "llama")
	reader := fakeclient.New(t, fakeclient.WithScheme(runtime.NewScheme()))
	inv, err := ObservePodGroups(context.Background(), reader, owner)
	if err != nil {
		t.Fatalf("unregistered optional type must soft-fail: %v", err)
	}
	if inv.Available() {
		t.Fatal("unregistered optional type must report unavailable")
	}
}

func TestEnsurePodGroupsWithState_UsesInventoryWithoutPodGroupReads(t *testing.T) {
	owner := newOwner("prod", "llama")
	pg := inventoryPodGroup(t, owner, "llama", 0)
	base := newGangClient(t, owner, pg)
	inv, err := ObservePodGroups(context.Background(), base, owner)
	if err != nil {
		t.Fatalf("ObservePodGroups: %v", err)
	}
	pgLists := fakeclient.CountingVerb(fakeclient.List).OnKind(&schedulingv1alpha1.PodGroupList{})
	pgGets := fakeclient.CountingVerb(fakeclient.Get).OnKind(&schedulingv1alpha1.PodGroup{})
	counting := fakeclient.Wrap(t, base, pgLists, pgGets)
	input, _ := inputWithConditionStore(owner, true, 1, true, "custom-scheduler")
	input.AuthoritativePods = &types.ComponentPodSnapshot{ByInstance: map[int32][]*corev1.Pod{}}
	plan := planFor(types.ComponentEngine, []int32{0}, true, 1, 5*time.Minute)

	if _, err := EnsurePodGroupsWithState(context.Background(), types.Deps{Client: counting}, input, plan,
		PodGroupReconcileState{Inventory: inv}); err != nil {
		t.Fatalf("EnsurePodGroupsWithState: %v", err)
	}
	if pgReads := countCalls(pgLists, pgGets); pgReads != 0 {
		t.Fatalf("shared inventory path issued %d hidden PodGroup read(s)", pgReads)
	}
}

func TestEnsureSurgePodGroupWithState_SeesTopLevelCreateWithoutReads(t *testing.T) {
	owner := newOwner("prod", "llama")
	base := newGangClient(t, owner)
	inv, err := ObservePodGroups(context.Background(), base, owner)
	if err != nil {
		t.Fatalf("ObservePodGroups: %v", err)
	}
	podLists := fakeclient.CountingVerb(fakeclient.List).OnKind(&corev1.PodList{})
	podGets := fakeclient.CountingVerb(fakeclient.Get).OnKind(&corev1.Pod{})
	pgLists := fakeclient.CountingVerb(fakeclient.List).OnKind(&schedulingv1alpha1.PodGroupList{})
	pgGets := fakeclient.CountingVerb(fakeclient.Get).OnKind(&schedulingv1alpha1.PodGroup{})
	creates := fakeclient.CountingVerb(fakeclient.Create).OnKind(&schedulingv1alpha1.PodGroup{})
	deletes := fakeclient.CountingVerb(fakeclient.Delete).OnKind(&schedulingv1alpha1.PodGroup{})
	counting := fakeclient.Wrap(t, base, podLists, podGets, pgLists, pgGets, creates, deletes)
	input, _ := inputWithConditionStore(owner, true, 1, true, "custom-scheduler")
	input.AuthoritativePods = &types.ComponentPodSnapshot{
		Pods:       []*corev1.Pod{},
		ByInstance: map[int32][]*corev1.Pod{},
	}
	plan := planFor(types.ComponentEngine, []int32{6}, true, 1, 5*time.Minute)
	state := PodGroupReconcileState{Inventory: inv}

	if _, err := EnsurePodGroupsWithState(context.Background(), types.Deps{Client: counting}, input, plan, state); err != nil {
		t.Fatalf("EnsurePodGroupsWithState: %v", err)
	}
	ensure := EnsureSurgePodGroupWithState(types.Deps{Client: counting}, state)
	if _, err := ensure(context.Background(), input, plan, plan.Instances[0]); err != nil {
		t.Fatalf("EnsureSurgePodGroupWithState: %v", err)
	}
	if reads := countCalls(podLists, podGets, pgLists, pgGets); reads != 0 {
		t.Fatalf("shared surge prerequisite issued %d hidden Pod/PodGroup read(s)", reads)
	}
	if creates.Count() != 1 {
		t.Fatalf("top-level plus inline ensure created %d PodGroups, want exactly 1", creates.Count())
	}
	finalize := BuildFinalizeInstanceResources(counting, counting, inv, owner, "llama", types.ComponentEngine)
	complete, err := finalize(context.Background(), 6)
	if err != nil {
		t.Fatalf("finalize same-pass PodGroup: %v", err)
	}
	if complete || deletes.Count() != 1 || !inv.DeleteAccepted(query.PodGroupName("llama", types.ComponentEngine, 6)) {
		t.Fatalf("same-pass finalization: complete=%v deletes=%d accepted=%v", complete, deletes.Count(),
			inv.DeleteAccepted(query.PodGroupName("llama", types.ComponentEngine, 6)))
	}
}

func TestEnsureSurgePodGroupWithState_SeesTopLevelUpdate(t *testing.T) {
	owner := newOwner("prod", "llama")
	pg := inventoryPodGroup(t, owner, "llama", 6)
	pg.Spec.MinMember = 99
	base := newGangClient(t, owner, pg)
	inv, err := ObservePodGroups(context.Background(), base, owner)
	if err != nil {
		t.Fatalf("ObservePodGroups: %v", err)
	}
	updates := fakeclient.CountingVerb(fakeclient.Update).OnKind(&schedulingv1alpha1.PodGroup{})
	counting := fakeclient.Wrap(t, base, updates)
	input, _ := inputWithConditionStore(owner, true, 1, true, "custom-scheduler")
	input.AuthoritativePods = &types.ComponentPodSnapshot{ByInstance: map[int32][]*corev1.Pod{}}
	plan := planFor(types.ComponentEngine, []int32{6}, true, 1, 5*time.Minute)
	state := PodGroupReconcileState{Inventory: inv}

	if _, err := EnsurePodGroupsWithState(context.Background(), types.Deps{Client: counting}, input, plan, state); err != nil {
		t.Fatalf("EnsurePodGroupsWithState: %v", err)
	}
	ensure := EnsureSurgePodGroupWithState(types.Deps{Client: counting}, state)
	if _, err := ensure(context.Background(), input, plan, plan.Instances[0]); err != nil {
		t.Fatalf("EnsureSurgePodGroupWithState: %v", err)
	}
	if updates.Count() != 1 {
		t.Fatalf("top-level plus inline ensure updated %d PodGroups, want exactly 1", updates.Count())
	}
}

func TestEnsurePodGroupsWithState_TerminalOwnedSkipsEnsure(t *testing.T) {
	owner := newOwner("prod", "llama")
	pg := inventoryPodGroup(t, owner, "llama", 3)
	pg.Spec.MinMember = 99
	base := newGangClient(t, owner, pg)
	inv, err := ObservePodGroups(context.Background(), base, owner)
	if err != nil {
		t.Fatalf("ObservePodGroups: %v", err)
	}
	creates := fakeclient.CountingVerb(fakeclient.Create).OnKind(&schedulingv1alpha1.PodGroup{})
	updates := fakeclient.CountingVerb(fakeclient.Update).OnKind(&schedulingv1alpha1.PodGroup{})
	counting := fakeclient.Wrap(t, base, creates, updates)
	input, _ := inputWithConditionStore(owner, true, 1, true, "custom-scheduler")
	input.AuthoritativePods = &types.ComponentPodSnapshot{ByInstance: map[int32][]*corev1.Pod{}}
	plan := planFor(types.ComponentEngine, []int32{3}, true, 1, 5*time.Minute)

	if _, err := EnsurePodGroupsWithState(context.Background(), types.Deps{Client: counting}, input, plan,
		PodGroupReconcileState{Inventory: inv, TerminalOwned: map[int32]struct{}{3: {}}}); err != nil {
		t.Fatalf("EnsurePodGroupsWithState: %v", err)
	}
	if creates.Count() != 0 {
		t.Fatalf("terminal-owned index recreated %d PodGroup(s)", creates.Count())
	}
	if updates.Count() != 0 {
		t.Fatalf("terminal-owned index updated %d PodGroup(s)", updates.Count())
	}
	got := &schedulingv1alpha1.PodGroup{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(pg), got); err != nil {
		t.Fatalf("get terminal-owned PodGroup: %v", err)
	}
	if got.Spec.MinMember != 99 {
		t.Fatalf("terminal-owned PodGroup drift was reconciled: got MinMember=%d want 99", got.Spec.MinMember)
	}
}

func TestEnsurePodGroupsWithState_DeleteOwnedReboundSkipsEnsure(t *testing.T) {
	owner := newOwner("prod", "llama")
	base := newGangClient(t, owner)
	inv, err := ObservePodGroups(context.Background(), base, owner)
	if err != nil {
		t.Fatalf("ObservePodGroups: %v", err)
	}
	input, _ := inputWithConditionStore(owner, true, 1, true, "custom-scheduler")
	input.AuthoritativePods = &types.ComponentPodSnapshot{ByInstance: map[int32][]*corev1.Pod{}}
	input.ObservedState.InstanceStatuses = []types.InstanceStatus{{
		Index: 4,
		Phase: types.InstancePhaseDeleting,
		Operation: &types.InstanceOperation{
			Type: types.InstanceOperationDelete,
			Step: "Drain",
		},
	}}
	plan := planFor(types.ComponentEngine, []int32{4}, true, 1, 5*time.Minute)

	if _, err := EnsurePodGroupsWithState(context.Background(), types.Deps{Client: base}, input, plan,
		PodGroupReconcileState{Inventory: inv}); err != nil {
		t.Fatalf("EnsurePodGroupsWithState: %v", err)
	}
	pg := &schedulingv1alpha1.PodGroup{}
	err = base.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: query.PodGroupName("llama", types.ComponentEngine, 4)}, pg)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("DeleteOwned rebound recreated PodGroup: %v", err)
	}
}

// TestEnsurePodGroupsWithState_ForeignCollisionAndTerminatingGate: a
// deterministic name this owner cannot write — held by another
// controller, or by an object being collected — is classified onto the
// Instance that wanted it and costs the rest of the Component nothing.
// The pass keeps going, ensures every other planned gang, and leaves the
// blocked row to the escalation pass.
func TestEnsurePodGroupsWithState_ForeignCollisionAndTerminatingGate(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mutate    func(*schedulingv1alpha1.PodGroup)
		wantState types.GangState
	}{
		{
			name: "foreign",
			mutate: func(pg *schedulingv1alpha1.PodGroup) {
				foreign := newOwner("prod", "foreign")
				pg.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(foreign, testOwnerGVK)}
			},
			wantState: types.GangStateOwnershipConflict,
		},
		{
			name: "terminating owned",
			mutate: func(pg *schedulingv1alpha1.PodGroup) {
				now := metav1.Now()
				pg.DeletionTimestamp = &now
			},
			wantState: types.GangStateTerminating,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := newOwner("prod", "llama")
			pg := inventoryPodGroup(t, owner, "llama", 0)
			tc.mutate(pg)
			inv := newPodGroupInventory(owner.GetUID(), true)
			inv.byName[pg.Name] = pg
			if podgroup.ControlledByUID(pg, owner.GetUID()) {
				inv.ownedName[pg.Name] = pg
			}
			base := newGangClient(t, owner)
			input, _ := inputWithConditionStore(owner, true, 1, true, "custom-scheduler")
			input.AuthoritativePods = &types.ComponentPodSnapshot{ByInstance: map[int32][]*corev1.Pod{}}
			input.Gangs = types.NewGangObservations()
			plan := planFor(types.ComponentEngine, []int32{0, 1}, true, 1, 5*time.Minute)
			_, err := EnsurePodGroupsWithState(context.Background(), types.Deps{Client: base}, input, plan,
				PodGroupReconcileState{Inventory: inv})
			if err != nil {
				t.Fatalf("EnsurePodGroupsWithState: got %v want the blocked name classified, not an error", err)
			}
			if got := input.Gangs.For(0); got.State != tc.wantState {
				t.Errorf("instance 0 gang state: got %q want %q", got.State, tc.wantState)
			}
			if !input.Gangs.BlocksPods(0) {
				t.Errorf("instance 0 must not accept pods while its PodGroup name is unusable")
			}
			sibling := &schedulingv1alpha1.PodGroup{}
			if err := base.Get(context.Background(), client.ObjectKey{
				Namespace: "prod", Name: query.PodGroupName("llama", types.ComponentEngine, 1),
			}, sibling); err != nil {
				t.Errorf("sibling gang: got %v want it ensured despite the blocked name", err)
			}
			if input.Gangs.BlocksPods(1) {
				t.Errorf("instance 1 must keep reconciling: %+v", input.Gangs.For(1))
			}
		})
	}
}

// TestGang_FailedPodGroupIsResetNotEscalated: the gang scheduler's
// Failed verdict is absorbing — its controller stops reconciling the
// object and this pass only reconciles labels, ownership and size — so
// the group is deleted and rebuilt under the same deterministic name.
// The row itself is untouched and members are withheld until the
// replacement exists; an operator learns of it from one event.
func TestGang_FailedPodGroupIsResetNotEscalated(t *testing.T) {
	owner := newOwner("prod", "llama")
	pg := inventoryPodGroup(t, owner, "llama", 0)
	pg.Status.Phase = schedulingv1alpha1.PodGroupFailed
	inv := newPodGroupInventory(owner.GetUID(), true)
	inv.byName[pg.Name] = pg
	inv.ownedName[pg.Name] = pg
	base := newGangClient(t, owner, pg)
	recorder := record.NewFakeRecorder(8)
	input, _ := inputWithConditionStore(owner, true, 1, true, "custom-scheduler")
	input.AuthoritativePods = &types.ComponentPodSnapshot{ByInstance: map[int32][]*corev1.Pod{}}
	input.Gangs = types.NewGangObservations()
	plan := planFor(types.ComponentEngine, []int32{0}, true, 1, 5*time.Minute)

	if _, err := EnsurePodGroupsWithState(context.Background(), types.Deps{Client: base, Recorder: recorder},
		input, plan, PodGroupReconcileState{Inventory: inv}); err != nil {
		t.Fatalf("EnsurePodGroupsWithState: %v", err)
	}
	if got := input.Gangs.For(0); got.State != types.GangStateFailed {
		t.Errorf("gang state: got %q want %q", got.State, types.GangStateFailed)
	}
	if !input.Gangs.BlocksPods(0) {
		t.Errorf("members must be withheld until the replacement group exists")
	}
	gone := &schedulingv1alpha1.PodGroup{}
	err := base.Get(context.Background(), client.ObjectKeyFromObject(pg), gone)
	if !apierrors.IsNotFound(err) && (err != nil || gone.DeletionTimestamp == nil) {
		t.Fatalf("failed PodGroup: got err=%v obj=%+v want deleted", err, gone)
	}
	select {
	case ev := <-recorder.Events:
		if !strings.Contains(ev, string(types.EventReasonPodGroupReset)) || !strings.Contains(ev, "Failed") {
			t.Errorf("event: got %q want a PodGroupReset naming the failure", ev)
		}
	default:
		t.Error("no event: the reset is the only trace an operator gets")
	}

	// Next pass: the group is gone, so the ensure builds a fresh one and
	// nothing withholds members any more.
	fresh, err := ObservePodGroups(context.Background(), base, owner)
	if err != nil {
		t.Fatalf("ObservePodGroups: %v", err)
	}
	input.Gangs = types.NewGangObservations()
	if _, err := EnsurePodGroupsWithState(context.Background(), types.Deps{Client: base, Recorder: recorder},
		input, plan, PodGroupReconcileState{Inventory: fresh}); err != nil {
		t.Fatalf("EnsurePodGroupsWithState (rebuild): %v", err)
	}
	if input.Gangs.BlocksPods(0) {
		t.Errorf("rebuilt gang: got blocked want its members admitted, %+v", input.Gangs.For(0))
	}
	rebuilt := &schedulingv1alpha1.PodGroup{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(pg), rebuilt); err != nil {
		t.Fatalf("rebuilt PodGroup: %v", err)
	}
	if rebuilt.Status.Phase == schedulingv1alpha1.PodGroupFailed {
		t.Errorf("rebuilt PodGroup still carries the absorbing verdict")
	}
}

// TestGang_FailedPodGroupResetWaitsForTerminalMemberRecycle: the gang
// controller derives a group's phase from its members, so rebuilding
// around a dead one would hand the replacement the same verdict. The
// create pass recycles the pod first; the reset lands on the pass after.
func TestGang_FailedPodGroupResetWaitsForTerminalMemberRecycle(t *testing.T) {
	owner := newOwner("prod", "llama")
	pg := inventoryPodGroup(t, owner, "llama", 0)
	pg.Status.Phase = schedulingv1alpha1.PodGroupFailed
	dead := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "llama-engine-0-leader-0",
		Namespace: "prod",
		Labels:    map[string]string{query.LabelPodGroup: pg.Name},
	}, Status: corev1.PodStatus{Phase: corev1.PodFailed}}

	newPass := func(pods ...*corev1.Pod) (*PodGroupInventory, types.ReconcileInput, *record.FakeRecorder, client.Client) {
		inv := newPodGroupInventory(owner.GetUID(), true)
		inv.byName[pg.Name] = pg
		inv.ownedName[pg.Name] = pg
		base := newGangClient(t, owner, pg)
		input, _ := inputWithConditionStore(owner, true, 1, true, "custom-scheduler")
		input.AuthoritativePods = &types.ComponentPodSnapshot{Pods: pods}
		input.Gangs = types.NewGangObservations()
		return inv, input, record.NewFakeRecorder(8), base
	}
	plan := planFor(types.ComponentEngine, []int32{0}, true, 1, 5*time.Minute)

	inv, input, recorder, base := newPass(dead)
	if _, err := EnsurePodGroupsWithState(context.Background(), types.Deps{Client: base, Recorder: recorder},
		input, plan, PodGroupReconcileState{Inventory: inv}); err != nil {
		t.Fatalf("EnsurePodGroupsWithState: %v", err)
	}
	if inv.DeleteAccepted(pg.Name) {
		t.Errorf("group deleted while a dead member still occupies its name")
	}
	select {
	case ev := <-recorder.Events:
		t.Errorf("event: got %q want none until the member is recycled", ev)
	default:
	}
	if !input.Gangs.BlocksPods(0) {
		t.Errorf("members must stay withheld while the group is failed")
	}

	inv, input, recorder, base = newPass()
	if _, err := EnsurePodGroupsWithState(context.Background(), types.Deps{Client: base, Recorder: recorder},
		input, plan, PodGroupReconcileState{Inventory: inv}); err != nil {
		t.Fatalf("EnsurePodGroupsWithState (recycled): %v", err)
	}
	if !inv.DeleteAccepted(pg.Name) {
		t.Errorf("group must be reset once its dead member is gone")
	}
	select {
	case ev := <-recorder.Events:
		if !strings.Contains(ev, string(types.EventReasonPodGroupReset)) {
			t.Errorf("event: got %q want a PodGroupReset", ev)
		}
	default:
		t.Error("no event: the reset is the only trace an operator gets")
	}
}

func TestBuildFinalizeInstanceResources_DeletesOwnedOnceAndIgnoresForeign(t *testing.T) {
	t.Run("without inventory", func(t *testing.T) {
		owner := newOwner("prod", "llama")
		pg := inventoryPodGroup(t, owner, "llama", 5)
		base := newGangClient(t, owner, pg)
		finalize := BuildFinalizeInstanceResources(base, base, nil, owner, "llama", types.ComponentEngine)

		complete, err := finalize(context.Background(), 5)
		if err != nil || complete {
			t.Fatalf("delete request: complete=%v err=%v", complete, err)
		}
		complete, err = finalize(context.Background(), 5)
		if err != nil || !complete {
			t.Fatalf("authoritative absence: complete=%v err=%v", complete, err)
		}
	})

	t.Run("owned", func(t *testing.T) {
		owner := newOwner("prod", "llama")
		pg := inventoryPodGroup(t, owner, "llama", 5)
		pg.UID = "pg-uid"
		base := newGangClient(t, owner, pg)
		inv, err := ObservePodGroups(context.Background(), base, owner)
		if err != nil {
			t.Fatalf("ObservePodGroups: %v", err)
		}
		deletes := fakeclient.CountingVerb(fakeclient.Delete)
		counting := fakeclient.Wrap(t, base, deletes)
		finalize := BuildFinalizeInstanceResources(counting, counting, inv, owner, "llama", types.ComponentEngine)
		complete, err := finalize(context.Background(), 5)
		if err != nil || complete {
			t.Fatalf("finalize: %v", err)
		}
		complete, err = finalize(context.Background(), 5)
		if err != nil || complete {
			t.Fatalf("idempotent finalize: %v", err)
		}
		if deletes.Count() != 1 {
			t.Fatalf("delete calls: got %d want 1", deletes.Count())
		}
		if !inv.DeleteAccepted(pg.Name) {
			t.Fatal("successful delete was not recorded as accepted")
		}
		got := &schedulingv1alpha1.PodGroup{}
		if err := base.Get(context.Background(), client.ObjectKeyFromObject(pg), got); !apierrors.IsNotFound(err) {
			t.Fatalf("owned PodGroup still exists: %v", err)
		}
		fresh, err := ObservePodGroups(context.Background(), base, owner)
		if err != nil {
			t.Fatalf("refresh inventory: %v", err)
		}
		complete, err = BuildFinalizeInstanceResources(base, base, fresh, owner, "llama", types.ComponentEngine)(context.Background(), 5)
		if err != nil || !complete {
			t.Fatalf("absence did not complete finalization: complete=%v err=%v", complete, err)
		}
	})

	t.Run("foreign", func(t *testing.T) {
		owner := newOwner("prod", "llama")
		foreignOwner := newOwner("prod", "foreign")
		pg := inventoryPodGroup(t, foreignOwner, "llama", 5)
		base := newGangClient(t, owner, foreignOwner, pg)
		inv, err := ObservePodGroups(context.Background(), base, owner)
		if err != nil {
			t.Fatalf("ObservePodGroups: %v", err)
		}
		finalize := BuildFinalizeInstanceResources(base, base, inv, owner, "llama", types.ComponentEngine)
		complete, err := finalize(context.Background(), 5)
		if err != nil || !complete {
			t.Fatalf("foreign object is absent from this owner's finalization set: %v", err)
		}
		if inv.DeleteAccepted(pg.Name) {
			t.Fatal("foreign object was recorded as an accepted owned delete")
		}
		got := &schedulingv1alpha1.PodGroup{}
		if err := base.Get(context.Background(), client.ObjectKeyFromObject(pg), got); err != nil {
			t.Fatalf("foreign PodGroup was deleted: %v", err)
		}
	})

	t.Run("delete error remains retryable", func(t *testing.T) {
		owner := newOwner("prod", "llama")
		pg := inventoryPodGroup(t, owner, "llama", 5)
		base := newGangClient(t, owner, pg)
		inv, err := ObservePodGroups(context.Background(), base, owner)
		if err != nil {
			t.Fatalf("ObservePodGroups: %v", err)
		}
		attempts := fakeclient.CountingVerb(fakeclient.Delete)
		flaky := fakeclient.Wrap(t, base, attempts,
			fakeclient.FailingVerb(fakeclient.Delete, 1, errors.New("transient delete failure")))
		finalize := BuildFinalizeInstanceResources(flaky, flaky, inv, owner, "llama", types.ComponentEngine)
		if _, err := finalize(context.Background(), 5); err == nil {
			t.Fatal("transient delete failure was swallowed")
		}
		if inv.DeleteAccepted(pg.Name) {
			t.Fatal("failed delete was recorded as accepted")
		}
		complete, err := finalize(context.Background(), 5)
		if err != nil || complete {
			t.Fatalf("retry finalize: %v", err)
		}
		if attempts.Count() != 2 || !inv.DeleteAccepted(pg.Name) {
			t.Fatalf("retry state: attempts=%d accepted=%v", attempts.Count(), inv.DeleteAccepted(pg.Name))
		}
	})
}

// countCalls sums the calls the counters saw.
func countCalls(counters ...*fakeclient.Calls) int {
	total := 0
	for _, c := range counters {
		total += c.Count()
	}
	return total
}

func inventoryPodGroup(t *testing.T, owner client.Object, ownerName string, index int32) *schedulingv1alpha1.PodGroup {
	t.Helper()
	plan := planFor(types.ComponentEngine, []int32{index}, true, 1, 5*time.Minute)
	pg, err := podgroup.BuildPodGroup(owner, testOwnerGVK, ownerName, plan, 0, plan.Instances[0])
	if err != nil {
		t.Fatalf("BuildPodGroup: %v", err)
	}
	pg.Labels = map[string]string{
		constants.InferenceServicePodLabelKey: ownerName,
		constants.OMEComponentLabel:           string(types.ComponentEngine),
		query.LabelManagedBy:                  query.ManagedByOMENative,
		query.LabelInstanceIdx:                stringIndex(index),
	}
	return pg
}

func stringIndex(index int32) string {
	return fmt.Sprintf("%d", index)
}

// A retired gang's PodGroup that is already Terminating holds the index:
// the finalization issues no second delete and reports the index incomplete
// until the object is gone, which is what keeps a cleanup marker in place
// for one more pass.
func TestFinalizeOwnedName_TerminatingGroupHoldsTheIndex(t *testing.T) {
	owner := newOwner("prod", "llama")
	pg := inventoryPodGroup(t, owner, "llama", 1)
	pg.UID = "pg-uid"
	pg.Finalizers = []string{"example.com/keep"}
	base := newGangClient(t, owner, pg)
	if err := base.Delete(context.Background(), pg); err != nil {
		t.Fatalf("stamp deletion: %v", err)
	}
	held := &schedulingv1alpha1.PodGroup{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(pg), held); err != nil {
		t.Fatalf("get terminating group: %v", err)
	}
	if held.DeletionTimestamp == nil {
		t.Fatal("the finalizer must leave the group Terminating")
	}

	inv, err := ObservePodGroups(context.Background(), base, owner)
	if err != nil {
		t.Fatalf("ObservePodGroups: %v", err)
	}
	deletes := fakeclient.CountingVerb(fakeclient.Delete)
	counting := fakeclient.Wrap(t, base, deletes)
	finalize := BuildFinalizeInstanceResources(counting, counting, inv, owner, "llama", types.ComponentEngine)
	complete, err := finalize(context.Background(), 1)
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if complete {
		t.Fatal("a Terminating group must hold the index incomplete")
	}
	if deletes.Count() != 0 {
		t.Fatalf("delete calls on a Terminating group: got %d want 0", deletes.Count())
	}

	held.Finalizers = nil
	if err := base.Update(context.Background(), held); err != nil {
		t.Fatalf("release finalizer: %v", err)
	}
	fresh, err := ObservePodGroups(context.Background(), base, owner)
	if err != nil {
		t.Fatalf("refresh inventory: %v", err)
	}
	complete, err = BuildFinalizeInstanceResources(base, base, fresh, owner, "llama", types.ComponentEngine)(context.Background(), 1)
	if err != nil || !complete {
		t.Fatalf("absence did not complete finalization: complete=%v err=%v", complete, err)
	}
}
