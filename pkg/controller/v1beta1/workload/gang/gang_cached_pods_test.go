package gang

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podgroup"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The PodGroup pass reads the owner's PodGroups and the Component's pods
// from the cache and asks the apiserver only when the cached pods cannot
// settle a gang or a group would be written or reset. These tests hold
// where each reader is asked.

const (
	cachedPodsOwner = "fleet"
	cachedPodsKey   = "topology.example.com/rack"
)

// twoReaders is one Component seen through two readers, the cache, which
// may lag and carries the manager's pod and PodGroup indexes, and the
// apiserver, each counting the pod lists and the PodGroup lists it served,
// with the writes made through the cached client.
type twoReaders struct {
	owner            client.Object
	deps             workloadtypes.Deps
	cached, live     client.WithWatch
	cachedLists      *fakeclient.Calls
	liveLists        *fakeclient.Calls
	cachedGroupLists *fakeclient.Calls
	liveGroupLists   *fakeclient.Calls
	writes           *fakeclient.Calls
}

func newTwoReaders(t *testing.T, cachedObjects, liveObjects []client.Object) *twoReaders {
	t.Helper()
	return newTwoReadersWith(t, cachedObjects, liveObjects, nil, nil)
}

// newTwoReadersWith is newTwoReaders with each reader's own options, such
// as a verb it refuses, layered on its store.
func newTwoReadersWith(t *testing.T, cachedObjects, liveObjects []client.Object, cachedOpts, liveOpts []fakeclient.Option) *twoReaders {
	t.Helper()
	owner := newOwner("prod", cachedPodsOwner)
	cachedLists := fakeclient.CountingVerb(fakeclient.List).OnKind(&corev1.PodList{})
	liveLists := fakeclient.CountingVerb(fakeclient.List).OnKind(&corev1.PodList{})
	cachedGroupLists := fakeclient.CountingVerb(fakeclient.List).OnKind(&schedulingv1alpha1.PodGroupList{})
	liveGroupLists := fakeclient.CountingVerb(fakeclient.List).OnKind(&schedulingv1alpha1.PodGroupList{})
	writes := fakeclient.CountingWrites()
	cached := newGangClientWith(t, append([]fakeclient.Option{cachedLists, cachedGroupLists, writes,
		fakeclient.WithIndex(&corev1.Pod{}, query.OMENativePodIndexField, query.OMENativePodIndexExtractor)}, cachedOpts...),
		append([]client.Object{owner}, cachedObjects...)...)
	live := newGangClientWith(t, append([]fakeclient.Option{liveLists, liveGroupLists}, liveOpts...),
		append([]client.Object{owner}, liveObjects...)...)
	return &twoReaders{
		owner:            owner,
		deps:             workloadtypes.Deps{Client: cached, APIReader: live},
		cached:           cached,
		live:             live,
		cachedLists:      cachedLists,
		liveLists:        liveLists,
		cachedGroupLists: cachedGroupLists,
		liveGroupLists:   liveGroupLists,
		writes:           writes,
	}
}

// planOnKey is one gang of a leader and workers workers, planned on key,
// with the input the pass records on.
func (r *twoReaders) planOnKey(available bool, workers int32, key string) (workloadtypes.ReconcileInput, workloadtypes.ComponentPlan) {
	input, plan, _ := r.planOnKeyRecordingConditions(available, workers, key)
	return input, plan
}

// planOnKeyRecordingConditions is planOnKey with the store the pass's
// condition writes land in.
func (r *twoReaders) planOnKeyRecordingConditions(available bool, workers int32, key string) (workloadtypes.ReconcileInput, workloadtypes.ComponentPlan, *conditionStore) {
	input, conditions := inputWithConditionStore(r.owner, true, workers, available, "gang-scheduler")
	input.Gangs = workloadtypes.NewGangObservations()
	plan := planFor(workloadtypes.ComponentEngine, []int32{0}, true, workers, 5*time.Minute)
	plan.TopologyKey = key
	return input, plan, conditions
}

// inventory is the owner's PodGroups as the adapter brings them to a pass
// at rest: the cache's picture.
func (r *twoReaders) inventory(t *testing.T, available bool) PodGroupReconcileState {
	t.Helper()
	var state PodGroupReconcileState
	if !available {
		return state
	}
	inventory, err := ObserveCachedPodGroups(context.Background(), r.cached, r.owner)
	if err != nil {
		t.Fatalf("ObserveCachedPodGroups: %v", err)
	}
	state.Inventory = inventory
	return state
}

// pass runs the PodGroup pass over one gang of a leader and workers
// workers, planned on key, with the inventory read from the cache as the
// adapter reads it, and returns the gang's effective topology key.
func (r *twoReaders) pass(t *testing.T, available bool, workers int32, key string) string {
	t.Helper()
	input, plan := r.planOnKey(available, workers, key)
	topology, err := EnsurePodGroupsWithState(context.Background(), r.deps, input, plan, r.inventory(t, available))
	if err != nil {
		t.Fatalf("EnsurePodGroupsWithState: %v", err)
	}
	return topology[0]
}

// requireReads holds which reader served the pods and whether anything
// was written.
func (r *twoReaders) requireReads(t *testing.T, cachedLists, liveLists, writes int) {
	t.Helper()
	if got := r.cachedLists.Count(); got != cachedLists {
		t.Errorf("pod lists served by the cache: got %d, want %d", got, cachedLists)
	}
	if got := r.liveLists.Count(); got != liveLists {
		t.Errorf("pod lists served by the apiserver: got %d, want %d", got, liveLists)
	}
	if got := r.writes.Count(); got != writes {
		t.Errorf("writes: got %d, want %d", got, writes)
	}
}

// requireGroupReads holds which reader served the owner's PodGroups.
func (r *twoReaders) requireGroupReads(t *testing.T, cachedLists, liveLists int) {
	t.Helper()
	if got := r.cachedGroupLists.Count(); got != cachedLists {
		t.Errorf("PodGroup lists served by the cache: got %d, want %d", got, cachedLists)
	}
	if got := r.liveGroupLists.Count(); got != liveLists {
		t.Errorf("PodGroup lists served by the apiserver: got %d, want %d", got, liveLists)
	}
}

// gangGroup is the PodGroup the pass itself writes for one gang of a leader
// and workers workers planned on key.
func gangGroup(t *testing.T, owner client.Object, workers int32, key string) *schedulingv1alpha1.PodGroup {
	t.Helper()
	plan := planFor(workloadtypes.ComponentEngine, []int32{0}, true, workers, 5*time.Minute)
	plan.TopologyKey = key
	group, err := podgroup.BuildPodGroup(owner, testOwnerGVK, owner.GetName(), plan, 0, plan.Instances[0])
	if err != nil {
		t.Fatalf("BuildPodGroup: %v", err)
	}
	return group
}

func gangPodName() string {
	return query.PodGroupName(cachedPodsOwner, workloadtypes.ComponentEngine, 0)
}

func leaderPod() *corev1.Pod {
	return topologyLeaderPod("prod", cachedPodsOwner, workloadtypes.ComponentEngine, 0, gangPodName())
}

func workerPod(key string) *corev1.Pod {
	return topologyWorkerPod("prod", cachedPodsOwner, workloadtypes.ComponentEngine, 0, gangPodName(), key)
}

// TestEnsurePodGroups_AtRestDecidesOnTheCachedPods: a gang whose pods are
// all in the cache, and whose group matches the plan when the API is
// present, is settled from the cache: the apiserver serves no pod list and
// nothing is written.
func TestEnsurePodGroups_AtRestDecidesOnTheCachedPods(t *testing.T) {
	for _, available := range []bool{true, false} {
		owner := newOwner("prod", cachedPodsOwner)
		objects := []client.Object{leaderPod(), workerPod(cachedPodsKey)}
		if available {
			objects = append(objects, gangGroup(t, owner, 1, cachedPodsKey))
		}
		r := newTwoReaders(t, objects, objects)
		if key := r.pass(t, available, 1, cachedPodsKey); key != cachedPodsKey {
			t.Errorf("available=%v: topology key %q, want %q", available, key, cachedPodsKey)
		}
		r.requireReads(t, 1, 0, 0)
		if available {
			r.requireGroupReads(t, 1, 0)
		}
	}
}

// TestEnsurePodGroups_AGroupThatDriftsIsWrittenFromTheLivePods: a group
// whose member count differs from the plan's is written, and the write is
// decided on the apiserver's pods, as every write is.
func TestEnsurePodGroups_AGroupThatDriftsIsWrittenFromTheLivePods(t *testing.T) {
	owner := newOwner("prod", cachedPodsOwner)
	objects := []client.Object{leaderPod(), workerPod(cachedPodsKey), gangGroup(t, owner, 1, cachedPodsKey)}
	r := newTwoReaders(t, objects, objects)
	if key := r.pass(t, true, 2, cachedPodsKey); key != cachedPodsKey {
		t.Errorf("topology key %q, want %q", key, cachedPodsKey)
	}
	r.requireReads(t, 1, 1, 1)
	r.requireGroupReads(t, 1, 1)
	got := &schedulingv1alpha1.PodGroup{}
	if err := r.deps.Client.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: gangPodName()}, got); err != nil {
		t.Fatalf("get the group: %v", err)
	}
	if got.Spec.MinMember != 3 {
		t.Errorf("minMember after the write: got %d, want 3", got.Spec.MinMember)
	}
}

// TestEnsurePodGroups_AGroupRecordingItsKeySettlesAGangTheCacheShowsInPart:
// the cache has not shown the worker yet, but the group records the key the
// gang was created under, so the gang keeps that key with no apiserver read.
func TestEnsurePodGroups_AGroupRecordingItsKeySettlesAGangTheCacheShowsInPart(t *testing.T) {
	owner := newOwner("prod", cachedPodsOwner)
	group := gangGroup(t, owner, 1, cachedPodsKey)
	r := newTwoReaders(t,
		[]client.Object{leaderPod(), group},
		[]client.Object{leaderPod(), workerPod(cachedPodsKey), group.DeepCopy()})
	if key := r.pass(t, true, 1, cachedPodsKey); key != cachedPodsKey {
		t.Errorf("topology key %q, want %q", key, cachedPodsKey)
	}
	r.requireReads(t, 1, 0, 0)
	r.requireGroupReads(t, 1, 0)
}

// TestEnsurePodGroups_AGroupWithoutAKeySendsAGangTheCacheShowsInPartLive: a
// group that records no key leans on the pods alone, so a gang the cache
// shows in part is decided on the apiserver's pods, where the worker proves
// the key the group then records.
func TestEnsurePodGroups_AGroupWithoutAKeySendsAGangTheCacheShowsInPartLive(t *testing.T) {
	owner := newOwner("prod", cachedPodsOwner)
	group := gangGroup(t, owner, 1, "")
	r := newTwoReaders(t,
		[]client.Object{leaderPod(), group},
		[]client.Object{leaderPod(), workerPod(cachedPodsKey), group.DeepCopy()})
	if key := r.pass(t, true, 1, ""); key != cachedPodsKey {
		t.Errorf("topology key %q, want the pods' %q", key, cachedPodsKey)
	}
	r.requireReads(t, 1, 1, 1)
	r.requireGroupReads(t, 1, 1)
}

// TestEnsurePodGroups_AFailedGroupIsResetOnTheLivePods: a group the gang
// scheduler failed is reset only when no member the kubelet finished with
// remains, and that reading is taken on the apiserver's pods: the cache
// shows the leader running, the apiserver shows it failed, and the reset
// holds.
func TestEnsurePodGroups_AFailedGroupIsResetOnTheLivePods(t *testing.T) {
	owner := newOwner("prod", cachedPodsOwner)
	group := gangGroup(t, owner, 1, cachedPodsKey)
	group.Status.Phase = schedulingv1alpha1.PodGroupFailed
	failedLeader := leaderPod()
	failedLeader.Status.Phase = corev1.PodFailed
	r := newTwoReaders(t,
		[]client.Object{leaderPod(), workerPod(cachedPodsKey), group},
		[]client.Object{failedLeader, workerPod(cachedPodsKey), group.DeepCopy()})
	input, plan := r.planOnKey(true, 1, cachedPodsKey)
	state := r.inventory(t, true)
	if _, err := EnsurePodGroupsWithState(context.Background(), r.deps, input, plan, state); err != nil {
		t.Fatalf("EnsurePodGroupsWithState: %v", err)
	}
	r.requireReads(t, 1, 1, 0)
	r.requireGroupReads(t, 1, 1)
	if state.Inventory.DeleteAccepted(gangPodName()) {
		t.Errorf("the group was reset while its failed member still occupies its name")
	}
	if !input.Gangs.BlocksPods(0) {
		t.Errorf("members must stay withheld while the group is failed")
	}
}

// TestEnsurePodGroups_WithoutTheAPIEveryPodCachedSettlesAGangWithNoKey:
// without the PodGroup API a gang with no topology configured, every pod of
// it in the cache and none proving a key, keeps no key, with no apiserver
// read: the apiserver's list could prove no other.
func TestEnsurePodGroups_WithoutTheAPIEveryPodCachedSettlesAGangWithNoKey(t *testing.T) {
	objects := []client.Object{leaderPod(), workerPod("")}
	r := newTwoReaders(t, objects, objects)
	if key := r.pass(t, false, 1, ""); key != "" {
		t.Errorf("topology key %q, want none", key)
	}
	r.requireReads(t, 1, 0, 0)
}

// TestEnsurePodGroups_WithoutTheAPIAGangShortInTheCacheIsReadLive: without
// the PodGroup API nothing but the pods records a gang's key, so a gang the
// cache shows in part is read from the apiserver, whose worker proves it.
func TestEnsurePodGroups_WithoutTheAPIAGangShortInTheCacheIsReadLive(t *testing.T) {
	r := newTwoReaders(t,
		[]client.Object{leaderPod()},
		[]client.Object{leaderPod(), workerPod(cachedPodsKey)})
	if key := r.pass(t, false, 1, ""); key != cachedPodsKey {
		t.Errorf("topology key %q, want the pods' %q", key, cachedPodsKey)
	}
	r.requireReads(t, 1, 1, 0)
}

const cachedPodsNewKey = "topology.example.com/row"

// groupOnTheCachedClient reads the gang's group back through the cached
// client, the one the pass writes through.
func (r *twoReaders) groupOnTheCachedClient(t *testing.T) *schedulingv1alpha1.PodGroup {
	t.Helper()
	group := &schedulingv1alpha1.PodGroup{}
	if err := r.deps.Client.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: gangPodName()}, group); err != nil {
		t.Fatalf("get the group: %v", err)
	}
	return group
}

// TestEnsurePodGroups_EmptyGangUnderAKeyChangeAdvancesOnTheLivePods: the
// operator moved the topology key, which rolls every gang. The gang's old
// pods are gone from the apiserver; the cache still shows them. The gang is
// in a key change, so it is decided on the apiserver's pods: the group
// advances to the new key now, and the engine renders the rebuilt gang on
// it, never on the key the plan left.
func TestEnsurePodGroups_EmptyGangUnderAKeyChangeAdvancesOnTheLivePods(t *testing.T) {
	for _, available := range []bool{true, false} {
		owner := newOwner("prod", cachedPodsOwner)
		stale := []client.Object{leaderPod(), workerPod(cachedPodsKey)}
		var current []client.Object
		if available {
			group := gangGroup(t, owner, 1, cachedPodsKey)
			stale = append(stale, group)
			current = append(current, group.DeepCopy())
		}
		r := newTwoReaders(t, stale, current)
		if key := r.pass(t, available, 1, cachedPodsNewKey); key != cachedPodsNewKey {
			t.Errorf("available=%v: the gang is empty on the apiserver, the pass published %q, want the plan's %q", available, key, cachedPodsNewKey)
		}
		if !available {
			r.requireReads(t, 1, 1, 0)
			continue
		}
		r.requireReads(t, 1, 1, 1)
		r.requireGroupReads(t, 1, 1)
		if key := r.groupOnTheCachedClient(t).Annotations[query.AnnotationTopologyKey]; key != cachedPodsNewKey {
			t.Errorf("the group records %q, want %q once the gang is empty", key, cachedPodsNewKey)
		}
	}
}

// TestEnsurePodGroups_AGangUnderAKeyChangeWithItsPodsStillUpKeepsTheOldKey:
// the key moved but the gang's old pods are still up on both readers, so
// the group must keep recording the key those pods carry; the decision is
// taken on the apiserver's pods all the same, since the key is changing.
func TestEnsurePodGroups_AGangUnderAKeyChangeWithItsPodsStillUpKeepsTheOldKey(t *testing.T) {
	owner := newOwner("prod", cachedPodsOwner)
	objects := []client.Object{leaderPod(), workerPod(cachedPodsKey), gangGroup(t, owner, 1, cachedPodsKey)}
	r := newTwoReaders(t, objects, objects)
	if key := r.pass(t, true, 1, cachedPodsNewKey); key != cachedPodsKey {
		t.Errorf("topology key %q, want the pods' %q while they are up", key, cachedPodsKey)
	}
	r.requireReads(t, 1, 1, 0)
	r.requireGroupReads(t, 1, 1)
}

// TestEnsurePodGroups_AGroupDeletedByHandIsRecreatedFromTheLivePods:
// someone deleted the gang's PodGroup and the cache has shown the
// deletion. The name reads free, so the pass reads the owner's groups and
// the pods from the apiserver and creates the group on the key those pods
// prove.
func TestEnsurePodGroups_AGroupDeletedByHandIsRecreatedFromTheLivePods(t *testing.T) {
	objects := []client.Object{leaderPod(), workerPod(cachedPodsKey)}
	r := newTwoReaders(t, objects, objects)
	if key := r.pass(t, true, 1, cachedPodsKey); key != cachedPodsKey {
		t.Errorf("topology key %q, want %q", key, cachedPodsKey)
	}
	r.requireReads(t, 1, 1, 1)
	r.requireGroupReads(t, 1, 1)
}

// TestEnsurePodGroups_AGroupDeletedByHandWhileTheCacheShowsItIsRecreatedOnceTheCacheShowsTheDeletion:
// someone deleted the gang's PodGroup, and the cache still shows it. The
// pass settles on the cache, writes nothing and reads nothing from the
// apiserver; the pass after the cache has caught up reads the apiserver
// and creates the group again.
func TestEnsurePodGroups_AGroupDeletedByHandWhileTheCacheShowsItIsRecreatedOnceTheCacheShowsTheDeletion(t *testing.T) {
	owner := newOwner("prod", cachedPodsOwner)
	group := gangGroup(t, owner, 1, cachedPodsKey)
	pods := []client.Object{leaderPod(), workerPod(cachedPodsKey)}
	r := newTwoReaders(t, append([]client.Object{group}, pods...), pods)
	if key := r.pass(t, true, 1, cachedPodsKey); key != cachedPodsKey {
		t.Errorf("topology key %q, want %q", key, cachedPodsKey)
	}
	r.requireReads(t, 1, 0, 0)
	r.requireGroupReads(t, 1, 0)

	if err := r.cached.Delete(context.Background(), group); err != nil {
		t.Fatalf("the cache catches up with the deletion: %v", err)
	}
	if key := r.pass(t, true, 1, cachedPodsKey); key != cachedPodsKey {
		t.Errorf("topology key %q, want %q", key, cachedPodsKey)
	}
	r.requireReads(t, 2, 1, 2)
	r.requireGroupReads(t, 2, 1)
	if err := r.live.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: gangPodName()}, &schedulingv1alpha1.PodGroup{}); err == nil {
		t.Errorf("premise: the create goes through the cached client, so the apiserver stand-in must still lack the group")
	}
	r.groupOnTheCachedClient(t)
}

// TestEnsurePodGroups_AFailedPhaseTheCacheHasNotShownIsResetOnThePassThatShowsIt:
// the gang scheduler failed the group. The cache still shows the group
// scheduling, so the pass settles on it and reads nothing live; the pass
// after the cache shows the failure reads the apiserver and resets the
// group there.
func TestEnsurePodGroups_AFailedPhaseTheCacheHasNotShownIsResetOnThePassThatShowsIt(t *testing.T) {
	owner := newOwner("prod", cachedPodsOwner)
	group := gangGroup(t, owner, 1, cachedPodsKey)
	failed := group.DeepCopy()
	failed.Status.Phase = schedulingv1alpha1.PodGroupFailed
	r := newTwoReaders(t,
		[]client.Object{leaderPod(), workerPod(cachedPodsKey), group},
		[]client.Object{leaderPod(), workerPod(cachedPodsKey), failed})
	r.pass(t, true, 1, cachedPodsKey)
	r.requireReads(t, 1, 0, 0)
	r.requireGroupReads(t, 1, 0)

	shown := r.groupOnTheCachedClient(t)
	shown.Status.Phase = schedulingv1alpha1.PodGroupFailed
	if err := r.cached.Update(context.Background(), shown); err != nil {
		t.Fatalf("the cache catches up with the failure: %v", err)
	}
	input, plan := r.planOnKey(true, 1, cachedPodsKey)
	state := r.inventory(t, true)
	if _, err := EnsurePodGroupsWithState(context.Background(), r.deps, input, plan, state); err != nil {
		t.Fatalf("EnsurePodGroupsWithState: %v", err)
	}
	// Two writes: the test's own update of the cached group, and the reset.
	r.requireReads(t, 2, 1, 2)
	r.requireGroupReads(t, 2, 1)
	if !state.Inventory.DeleteAccepted(gangPodName()) {
		t.Errorf("the failed group was not reset on the pass whose cache shows the failure")
	}
}

// TestEnsurePodGroups_ACacheBehindThePassesOwnWriteWritesNothingTwice: the
// cache still shows the group as it was before the last pass wrote it. The
// group reads drifted, so the pass reads the apiserver, finds the write
// there and writes nothing.
func TestEnsurePodGroups_ACacheBehindThePassesOwnWriteWritesNothingTwice(t *testing.T) {
	owner := newOwner("prod", cachedPodsOwner)
	pods := []client.Object{leaderPod(), workerPod(cachedPodsKey)}
	r := newTwoReaders(t,
		append([]client.Object{gangGroup(t, owner, 1, cachedPodsKey)}, pods...),
		append([]client.Object{gangGroup(t, owner, 2, cachedPodsKey)}, pods...))
	if key := r.pass(t, true, 2, cachedPodsKey); key != cachedPodsKey {
		t.Errorf("topology key %q, want %q", key, cachedPodsKey)
	}
	r.requireReads(t, 1, 1, 0)
	r.requireGroupReads(t, 1, 1)
}

// TestEnsurePodGroups_AnotherOwnersGroupAtTheNameIsFoundOnTheLiveRead: the
// cache's inventory holds this owner's groups alone, so a name another
// controller's object occupies reads free; the pass reads the apiserver,
// records the collision on the row and writes nothing.
func TestEnsurePodGroups_AnotherOwnersGroupAtTheNameIsFoundOnTheLiveRead(t *testing.T) {
	owner := newOwner("prod", cachedPodsOwner)
	otherOwner := newOwner("prod", "other")
	occupied := gangGroup(t, owner, 1, cachedPodsKey)
	occupied.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(otherOwner, testOwnerGVK)}
	pods := []client.Object{leaderPod(), workerPod(cachedPodsKey)}
	r := newTwoReaders(t, append([]client.Object{otherOwner, occupied}, pods...), append([]client.Object{otherOwner, occupied.DeepCopy()}, pods...))
	input, plan := r.planOnKey(true, 1, cachedPodsKey)
	if _, err := EnsurePodGroupsWithState(context.Background(), r.deps, input, plan, r.inventory(t, true)); err != nil {
		t.Fatalf("EnsurePodGroupsWithState: %v", err)
	}
	r.requireReads(t, 1, 1, 0)
	r.requireGroupReads(t, 1, 1)
	if got := input.Gangs.For(0).State; got != workloadtypes.GangStateOwnershipConflict {
		t.Errorf("gang state %q, want %q", got, workloadtypes.GangStateOwnershipConflict)
	}
}

// TestEnsureSurgePodGroup_ACachedInventoryIsReadLiveBeforeTheWrite: the
// surge callback writes, so an inventory the cache served is read from
// the apiserver first: a surge group the cache has not shown yet is found
// there and not created twice.
func TestEnsureSurgePodGroup_ACachedInventoryIsReadLiveBeforeTheWrite(t *testing.T) {
	owner := newOwner("prod", cachedPodsOwner)
	group := gangGroup(t, owner, 1, "")
	r := newTwoReaders(t, nil, []client.Object{group})
	input, plan := r.planOnKey(true, 1, "")
	input.AuthoritativePods = &workloadtypes.ComponentPodSnapshot{ByInstance: map[int32][]*corev1.Pod{}}
	state := r.inventory(t, true)
	if _, err := EnsureSurgePodGroupWithState(r.deps, state)(context.Background(), input, plan, plan.Instances[0]); err != nil {
		t.Fatalf("EnsureSurgePodGroupWithState: %v", err)
	}
	r.requireReads(t, 0, 0, 0)
	r.requireGroupReads(t, 1, 1)
	if state.Inventory.Cached() {
		t.Errorf("the surge callback left the inventory on the cache's picture")
	}
}

// TestGangWrite_RefusesAWriteOnTheCachesPicture: a write asked to decide on
// the cache's inventory refuses and names the reason; nothing is created.
func TestGangWrite_RefusesAWriteOnTheCachesPicture(t *testing.T) {
	r := newTwoReaders(t, nil, nil)
	input, plan := r.planOnKey(true, 1, "")
	state := r.inventory(t, true)
	shared := &gangPassShared{deps: r.deps, input: &input, plan: plan, inventory: state.Inventory}
	write := &gangWrite{gangPassShared: shared, inst: &plan.Instances[0], name: gangPodName()}
	if err := write.readName(context.Background()); err != nil {
		t.Fatalf("readName: %v", err)
	}
	if _, err := write.write(context.Background()); !errors.Is(err, ErrInventoryCached) {
		t.Fatalf("write on the cache's picture: got %v, want %v", err, ErrInventoryCached)
	}
	r.requireReads(t, 0, 0, 0)
}

// TestEnsurePodGroups_ASplitGangInTheCacheIsReadLive: the cache shows two
// workers on different keys, which no group can describe; the apiserver
// shows one gang on one key. The pass does not settle on the cache.
func TestEnsurePodGroups_ASplitGangInTheCacheIsReadLive(t *testing.T) {
	owner := newOwner("prod", cachedPodsOwner)
	group := gangGroup(t, owner, 2, cachedPodsKey)
	second := func(key string) *corev1.Pod {
		pod := workerPod(key)
		pod.Name = query.PodName(cachedPodsOwner, workloadtypes.ComponentEngine, 0, workloadtypes.RunnerWorker, 1)
		return pod
	}
	r := newTwoReaders(t,
		[]client.Object{leaderPod(), workerPod(cachedPodsKey), second(cachedPodsNewKey), group},
		[]client.Object{leaderPod(), workerPod(cachedPodsKey), second(cachedPodsKey), group.DeepCopy()})
	if key := r.pass(t, true, 2, cachedPodsKey); key != cachedPodsKey {
		t.Errorf("topology key %q, want %q", key, cachedPodsKey)
	}
	r.requireReads(t, 1, 1, 0)
	r.requireGroupReads(t, 1, 1)
}

// TestEnsurePodGroups_APodRebuiltUnderItsNameOnTheGangsKeyNeedsNoRead: a
// member was deleted and rebuilt under its name on the key the group
// records; the cache still shows the old pod. Both readers prove the same
// key, so the pass has nothing to write and reads nothing live.
func TestEnsurePodGroups_APodRebuiltUnderItsNameOnTheGangsKeyNeedsNoRead(t *testing.T) {
	owner := newOwner("prod", cachedPodsOwner)
	group := gangGroup(t, owner, 1, cachedPodsKey)
	rebuilt := workerPod(cachedPodsKey)
	rebuilt.UID = "rebuilt-uid"
	r := newTwoReaders(t,
		[]client.Object{leaderPod(), workerPod(cachedPodsKey), group},
		[]client.Object{leaderPod(), rebuilt, group.DeepCopy()})
	if key := r.pass(t, true, 1, cachedPodsKey); key != cachedPodsKey {
		t.Errorf("topology key %q, want %q", key, cachedPodsKey)
	}
	r.requireReads(t, 1, 0, 0)
	r.requireGroupReads(t, 1, 0)
}
