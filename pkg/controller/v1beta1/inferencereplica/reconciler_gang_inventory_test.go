package inferencereplica

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	workloadgang "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/gang"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The PodGroup pass reads the PodGroups this owner controls before it
// writes any gang's group. These tests drive the whole pass over a
// Component of gangs through two readers, the cache and the apiserver, and
// hold what a pass at rest reads and writes, and that a group someone
// deleted by hand comes back.

const gangInventoryKey = "topology.example.com/rack"

// readyGangsOnKey is gangs Ready one-worker gangs on revision rev, built as
// the engine builds them: a Ready row each, pods labelled with their
// group, each worker carrying the term that names its leader on key, each
// group recording key, and a serving endpoint per pod.
func readyGangsOnKey(ir *v1beta1.InferenceReplica, rev, key string, gangs int32) []client.Object {
	ir.Spec.Replicas = ptr.To(gangs)
	ir.Status.CurrentRevision, ir.Status.UpdateRevision = rev, rev
	ir.Status.ObservedGeneration = ir.Generation
	hash := query.RevisionFromName(rev).Hash()
	var objs []client.Object
	for idx := int32(0); idx < gangs; idx++ {
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
			Index: idx, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			RunningRevision: rev, PodCount: 2, ServingPodCount: 2,
		})
		groupName := query.PodGroupName(ir.Spec.ParentRef.Name, workloadtypes.ComponentEngine, idx)
		for _, runner := range []string{workloadtypes.RunnerLeader, workloadtypes.RunnerWorker} {
			pod := podForIR(ir, idx, runner, 0, true, true)
			pod.Labels[query.LabelRevisionHash] = hash
			pod.Labels[query.LabelPodGroup] = groupName
			if runner == workloadtypes.RunnerWorker {
				pod.Spec.Affinity = leaderAffinityOnKey(ir, idx, key)
			}
			objs = append(objs, pod, sliceForIRPod(ir, pod, true))
		}
		group := ownedPodGroupForIR(ir, groupName, idx)
		group.Annotations = map[string]string{query.AnnotationTopologyKey: key}
		group.Spec.MinMember = 2
		objs = append(objs, group)
	}
	return objs
}

// leaderAffinityOnKey is the affinity term the renderer gives a worker: it
// follows its gang's leader on key.
func leaderAffinityOnKey(ir *v1beta1.InferenceReplica, idx int32, key string) *corev1.Affinity {
	return &corev1.Affinity{PodAffinity: &corev1.PodAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			TopologyKey: key,
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
				constants.InferenceServicePodLabelKey: ir.Spec.ParentRef.Name,
				constants.OMEComponentLabel:           string(workloadtypes.ComponentEngine),
				query.LabelInstanceIdx:                intToLabel(int64(idx)),
				query.LabelRunner:                     workloadtypes.RunnerLeader,
			}},
		}},
	}}
}

// gangReaders is a Component of gangs seen through two readers over one
// store: the cache, which may serve a PodGroup list it took before the
// store's last change, and the apiserver, which serves the store; each
// counts the PodGroup lists it served, and the cache counts the PodGroup
// and pod writes made through it.
type gangReaders struct {
	t     *testing.T
	r     *Reconciler
	base  client.Client
	ir    *v1beta1.InferenceReplica
	req   ctrl.Request
	stale struct {
		groups []schedulingv1alpha1.PodGroup
		left   int
	}
	cachedGroupLists, liveGroupLists int
	groupWrites, podWrites           *fakeclient.Calls
	// apiGone is set while the PodGroup API is removed from the cluster:
	// the apiserver answers every PodGroup list and write with NoKindMatch.
	apiGone bool
	// liveListFailure, when set, is what the apiserver answers a PodGroup
	// list with instead of the list.
	liveListFailure error
}

// newGangReaders seeds gangs Ready gangs on gangInventoryKey on a cluster
// with the PodGroup API, runs the passes that bring the groups to what the
// plan asks for, and installs the two counting readers.
func newGangReaders(t *testing.T, gangs int32) *gangReaders {
	t.Helper()
	return seedGangReaders(t, gangs, true)
}

// newGangReadersWithoutThePodGroupAPI seeds gangs Ready gangs on a cluster
// that never had the PodGroup API: no group exists, and the manager's
// discovery at its start found none.
func newGangReadersWithoutThePodGroupAPI(t *testing.T, gangs int32) *gangReaders {
	t.Helper()
	return seedGangReaders(t, gangs, false)
}

func seedGangReaders(t *testing.T, gangs int32, podGroupAPI bool) *gangReaders {
	t.Helper()
	ir := gangWidthIR("llama-engine", "prod", 1, v1beta1.UpdateStrategyRecreatePod)
	ir.Spec.TopologyKey = ptr.To(gangInventoryKey)
	var objs []client.Object
	for _, obj := range readyGangsOnKey(ir, targetRevisionNameFor(t, ir), gangInventoryKey, gangs) {
		if _, group := obj.(*schedulingv1alpha1.PodGroup); group && !podGroupAPI {
			continue
		}
		objs = append(objs, obj)
	}
	r, base := newReconciler(t, append([]client.Object{ir}, objs...)...)
	r.GangSchedulingAvailable = podGroupAPI
	g := &gangReaders{t: t, r: r, base: base, ir: ir, req: ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}}
	for pass := 0; pass < 3; pass++ {
		g.pass()
	}
	g.groupWrites = fakeclient.CountingWrites().OnKind(&schedulingv1alpha1.PodGroup{})
	g.podWrites = fakeclient.CountingWrites().OnKind(&corev1.Pod{})
	counted := fakeclient.Wrap(t, base.(client.WithWatch), g.groupWrites, g.podWrites)
	r.Client = interceptor.NewClient(counted, interceptor.Funcs{
		List: g.cacheList, Create: g.cacheCreate, Update: g.cacheUpdate, Delete: g.cacheDelete})
	r.APIReader = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{List: g.liveList})
	return g
}

// noPodGroupAPIAnswer is the apiserver's answer to a PodGroup list or
// write once the API is removed.
func noPodGroupAPIAnswer() error {
	return &apimeta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "scheduling.x-k8s.io", Kind: "PodGroup"}}
}

// podGroupWriteRefused is the apiserver's answer to a write of obj: the
// no-API answer for a PodGroup while the API is removed, nil otherwise.
func (g *gangReaders) podGroupWriteRefused(obj client.Object) error {
	if _, group := obj.(*schedulingv1alpha1.PodGroup); group && g.apiGone {
		return noPodGroupAPIAnswer()
	}
	return nil
}

func (g *gangReaders) cacheCreate(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
	if err := g.podGroupWriteRefused(obj); err != nil {
		return err
	}
	return c.Create(ctx, obj, opts...)
}

func (g *gangReaders) cacheUpdate(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
	if err := g.podGroupWriteRefused(obj); err != nil {
		return err
	}
	return c.Update(ctx, obj, opts...)
}

func (g *gangReaders) cacheDelete(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
	if err := g.podGroupWriteRefused(obj); err != nil {
		return err
	}
	return c.Delete(ctx, obj, opts...)
}

// cacheList serves the cache's lists: a PodGroup list is counted and, while
// a list taken before the store's last change is left to serve, answered
// from it, in the namespace asked for and under the controller UID the
// list's field selector names, as the manager's cache index answers.
func (g *gangReaders) cacheList(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
	groups, ok := list.(*schedulingv1alpha1.PodGroupList)
	if !ok {
		return c.List(ctx, list, opts...)
	}
	g.cachedGroupLists++
	if g.stale.left == 0 {
		return c.List(ctx, list, opts...)
	}
	g.stale.left--
	options := client.ListOptions{}
	options.ApplyOptions(opts)
	groups.Items = nil
	for i := range g.stale.groups {
		if staleGroupSelected(&g.stale.groups[i], &options) {
			groups.Items = append(groups.Items, *g.stale.groups[i].DeepCopy())
		}
	}
	return nil
}

func staleGroupSelected(group *schedulingv1alpha1.PodGroup, options *client.ListOptions) bool {
	if options.Namespace != "" && group.Namespace != options.Namespace {
		return false
	}
	if options.FieldSelector == nil {
		return true
	}
	for _, req := range options.FieldSelector.Requirements() {
		if req.Field != workloadgang.PodGroupControllerUIDIndexField {
			return false
		}
		values := workloadgang.PodGroupControllerUIDIndexExtractor(group)
		if len(values) != 1 || values[0] != req.Value {
			return false
		}
	}
	return true
}

// liveList serves the apiserver's lists and counts the PodGroup lists; a
// PodGroup list is refused while the API is removed, or with the failure
// the test set.
func (g *gangReaders) liveList(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*schedulingv1alpha1.PodGroupList); ok {
		g.liveGroupLists++
		if g.apiGone {
			return noPodGroupAPIAnswer()
		}
		if g.liveListFailure != nil {
			return g.liveListFailure
		}
	}
	return c.List(ctx, list, opts...)
}

// passMayFail is one pass of the reconciler that hands its error back.
func (g *gangReaders) passMayFail() error {
	g.r.Expectations = workloadtypes.NewExpectations()
	_, err := g.r.Reconcile(context.Background(), g.req)
	return err
}

// removeThePodGroupAPI takes the PodGroup API out of the cluster: the
// apiserver deletes every group, as its own cleanup does when the kind
// goes, and from then on answers every PodGroup list and write with
// NoKindMatch. The manager keeps the discovery it made at its start.
func (g *gangReaders) removeThePodGroupAPI() {
	g.t.Helper()
	groups := g.groups()
	for i := range groups {
		if err := g.base.Delete(context.Background(), &groups[i]); err != nil {
			g.t.Fatalf("delete %s with the API: %v", groups[i].Name, err)
		}
	}
	g.apiGone = true
}

// reinstallThePodGroupAPI brings the PodGroup API back, with no group in it.
func (g *gangReaders) reinstallThePodGroupAPI() {
	g.apiGone = false
}

// requireCondition holds the GangSchedulingUnavailable condition the
// replica publishes, and returns it.
func (g *gangReaders) requireCondition(status metav1.ConditionStatus, reason workloadtypes.ConditionReason) metav1.Condition {
	g.t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if err := g.base.Get(context.Background(), g.req.NamespacedName, ir); err != nil {
		g.t.Fatalf("read the replica: %v", err)
	}
	cond := apimeta.FindStatusCondition(ir.Status.Conditions, string(workloadtypes.ConditionGangSchedulingUnavailable))
	if cond == nil {
		g.t.Fatalf("the replica publishes no %s condition", workloadtypes.ConditionGangSchedulingUnavailable)
	}
	if cond.Status != status || cond.Reason != string(reason) {
		g.t.Fatalf("%s is %s/%s, want %s/%s", cond.Type, cond.Status, cond.Reason, status, reason)
	}
	return *cond
}

// requireGroupWrites holds how many PodGroup writes the passes made.
func (g *gangReaders) requireGroupWrites(want int) {
	g.t.Helper()
	if n := g.groupWrites.Count(); n != want {
		g.t.Fatalf("the passes made %d PodGroup write(s), want %d", n, want)
	}
}

// pass is one pass of the reconciler, with the watch taken to have shown
// the last pass's writes.
func (g *gangReaders) pass() {
	g.t.Helper()
	g.r.Expectations = workloadtypes.NewExpectations()
	if _, err := g.r.Reconcile(context.Background(), g.req); err != nil {
		g.t.Fatalf("pass: %v", err)
	}
}

// groups lists the store's PodGroups.
func (g *gangReaders) groups() []schedulingv1alpha1.PodGroup {
	g.t.Helper()
	list := &schedulingv1alpha1.PodGroupList{}
	if err := g.base.List(context.Background(), list, client.InNamespace(g.ir.Namespace)); err != nil {
		g.t.Fatalf("list the groups: %v", err)
	}
	return list.Items
}

// cacheServesLastListOnce makes the cache answer its next PodGroup list
// with the groups as the store holds them now, whatever the store holds
// by then.
func (g *gangReaders) cacheServesLastListOnce() {
	g.stale.groups, g.stale.left = g.groups(), 1
}

// deleteGroupByHand removes gang idx's PodGroup from the store, as an
// operator's delete does.
func (g *gangReaders) deleteGroupByHand(idx int32) {
	g.t.Helper()
	group := &schedulingv1alpha1.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Namespace: g.ir.Namespace, Name: query.PodGroupName(g.ir.Spec.ParentRef.Name, workloadtypes.ComponentEngine, idx),
	}}
	if err := g.base.Delete(context.Background(), group); err != nil {
		g.t.Fatalf("delete the group by hand: %v", err)
	}
}

// requireGroupOfGang holds that gang idx has its PodGroup: controlled by
// the replica, recording the key its workers carry, sized to the gang.
func (g *gangReaders) requireGroupOfGang(idx int32) {
	g.t.Helper()
	group := &schedulingv1alpha1.PodGroup{}
	name := query.PodGroupName(g.ir.Spec.ParentRef.Name, workloadtypes.ComponentEngine, idx)
	if err := g.base.Get(context.Background(), client.ObjectKey{Namespace: g.ir.Namespace, Name: name}, group); err != nil {
		g.t.Fatalf("gang %d has no PodGroup: %v", idx, err)
	}
	if controller := metav1.GetControllerOf(group); controller == nil || controller.UID != g.ir.UID {
		g.t.Fatalf("gang %d's PodGroup is controlled by %v, want the replica", idx, controller)
	}
	if key := group.Annotations[query.AnnotationTopologyKey]; key != gangInventoryKey {
		g.t.Fatalf("gang %d's PodGroup records %q, want its workers' %q", idx, key, gangInventoryKey)
	}
	if group.Spec.MinMember != 2 {
		g.t.Fatalf("gang %d's PodGroup asks for %d members, want 2", idx, group.Spec.MinMember)
	}
}

// requireNoPodWrite holds that no pass created, changed or deleted a pod.
func (g *gangReaders) requireNoPodWrite() {
	g.t.Helper()
	if n := g.podWrites.Count(); n != 0 {
		g.t.Fatalf("the passes wrote %d pod(s); a Component of Ready gangs must write none", n)
	}
}

// TestReconcile_GangsAtRestWriteNoPodGroup: two Ready gangs whose groups
// match the plan. A pass writes no PodGroup and no pod; the groups stand
// as they were.
func TestReconcile_GangsAtRestWriteNoPodGroup(t *testing.T) {
	g := newGangReaders(t, 2)
	before := g.groups()
	g.pass()
	if n := g.groupWrites.Count(); n != 0 {
		t.Fatalf("a pass over Ready gangs wrote %d PodGroup(s), want none", n)
	}
	g.requireNoPodWrite()
	after := g.groups()
	if len(after) != len(before) {
		t.Fatalf("the pass left %d group(s), want the %d it found", len(after), len(before))
	}
	for i := range before {
		if before[i].ResourceVersion != after[i].ResourceVersion {
			t.Fatalf("the pass changed %s (%s -> %s); a group that matches the plan is left alone", before[i].Name, before[i].ResourceVersion, after[i].ResourceVersion)
		}
	}
}

// TestReconcile_GangGroupDeletedByHandIsRecreated: someone deletes a Ready
// gang's PodGroup, and the cache has shown the deletion by the time the
// pass runs, as it has on the pass the delete event brings. The pass
// creates the group again, controlled by the replica and on the key the
// gang's workers carry, and touches no pod.
func TestReconcile_GangGroupDeletedByHandIsRecreated(t *testing.T) {
	g := newGangReaders(t, 2)
	g.deleteGroupByHand(1)
	g.pass()
	g.requireGroupOfGang(1)
	g.requireGroupOfGang(0)
	g.requireNoPodWrite()
	if n := g.groupWrites.Count(); n != 1 {
		t.Fatalf("the pass made %d PodGroup write(s), want the one create", n)
	}
}

// TestReconcile_GangGroupDeletedByHandIsRecreatedOnceTheCacheShowsTheDeletion:
// someone deletes a Ready gang's PodGroup, and a pass runs while the cache
// still lists the group. That pass touches no pod, and the group is back
// by the end of the pass after it, when the cache has caught up.
func TestReconcile_GangGroupDeletedByHandIsRecreatedOnceTheCacheShowsTheDeletion(t *testing.T) {
	g := newGangReaders(t, 2)
	g.cacheServesLastListOnce()
	g.deleteGroupByHand(0)
	g.pass()
	g.requireNoPodWrite()
	g.pass()
	g.requireGroupOfGang(0)
	g.requireGroupOfGang(1)
	g.requireNoPodWrite()
}

// requireGroupLists holds which reader served the owner's PodGroups over
// the passes so far.
func (g *gangReaders) requireGroupLists(cached, live int) {
	g.t.Helper()
	if g.cachedGroupLists != cached {
		g.t.Errorf("PodGroup lists served by the cache: got %d, want %d", g.cachedGroupLists, cached)
	}
	if g.liveGroupLists != live {
		g.t.Errorf("PodGroup lists served by the apiserver: got %d, want %d", g.liveGroupLists, live)
	}
}

// TestReconcile_GangsAtRestReadNoPodGroupLive: two Ready gangs whose groups
// match the plan. A pass reads the owner's PodGroups from the cache once
// and asks the apiserver for none.
func TestReconcile_GangsAtRestReadNoPodGroupLive(t *testing.T) {
	g := newGangReaders(t, 2)
	g.pass()
	g.requireGroupLists(1, 0)
	if n := g.groupWrites.Count(); n != 0 {
		t.Fatalf("a pass over Ready gangs wrote %d PodGroup(s), want none", n)
	}
}

// TestReconcile_GangGroupDeletedWhileTheCacheShowsItIsCreatedOnTheApiserversPicture:
// someone deletes a Ready gang's PodGroup. The pass whose cache still lists
// the group settles on the cache and asks the apiserver for nothing; the
// pass after, whose cache shows the name free, reads the owner's groups
// from the apiserver before it creates the group again.
func TestReconcile_GangGroupDeletedWhileTheCacheShowsItIsCreatedOnTheApiserversPicture(t *testing.T) {
	g := newGangReaders(t, 2)
	g.cacheServesLastListOnce()
	g.deleteGroupByHand(0)
	g.pass()
	g.requireGroupLists(1, 0)
	if n := g.groupWrites.Count(); n != 0 {
		t.Fatalf("the pass behind the deletion wrote %d PodGroup(s), want none", n)
	}
	g.pass()
	g.requireGroupLists(2, 1)
	g.requireGroupOfGang(0)
	if n := g.groupWrites.Count(); n != 1 {
		t.Fatalf("the passes made %d PodGroup write(s), want the one create", n)
	}
	g.requireNoPodWrite()
}

// TestReconcile_StaleSinglePodGroupIsDeletedOnTheApiserversInventory: a
// group left at an Instance the plan has made single-pod is found in the
// cache's inventory, and its deletion is decided on the apiserver's, read
// once before it.
func TestReconcile_StaleSinglePodGroupIsDeletedOnTheApiserversInventory(t *testing.T) {
	ir := baselineIR("llama-engine", "podgroup-stale-single", 1)
	pgName := query.PodGroupName(ir.Spec.ParentRef.Name, workloadtypes.ComponentEngine, 0)
	pg := ownedPodGroupForIR(ir, pgName, 0)
	pg.Finalizers = []string{"test.ome.io/hold"}
	r, c := newReconciler(t, ir, pg)
	reader := &podGroupListCountingReader{Reader: c}
	r.APIReader = reader
	r.GangSchedulingAvailable = true

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if reader.lists != 1 {
		t.Fatalf("the deletion read the apiserver's PodGroups %d time(s), want once", reader.lists)
	}
	got := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pg), got); err != nil {
		t.Fatalf("get the stale group: %v", err)
	}
	if got.DeletionTimestamp == nil {
		t.Fatalf("the stale group was not deleted")
	}
}

// TestReconcile_WithoutThePodGroupAPIAtStartGangsRunWithoutGroups: the
// manager started on a cluster without the PodGroup API. A pass over two
// Ready gangs asks neither reader for a PodGroup, writes none, and
// publishes GangSchedulingUnavailable=True; the pods keep running.
func TestReconcile_WithoutThePodGroupAPIAtStartGangsRunWithoutGroups(t *testing.T) {
	g := newGangReadersWithoutThePodGroupAPI(t, 2)
	g.pass()
	g.requireCondition(metav1.ConditionTrue, workloadtypes.ReasonPodGroupCRDNotInstalled)
	g.requireGroupLists(0, 0)
	g.requireGroupWrites(0)
	g.requireNoPodWrite()
}

// TestReconcile_PodGroupAPIRemovedUnderReadyGangsPublishesTheConditionAndGoesOn:
// the PodGroup API is removed while two Ready gangs run. The pass finds
// the names free in the cache, reads the apiserver before it would write,
// is told there is no PodGroup API, and goes on as on a cluster without
// it: no group written, no pod touched, GangSchedulingUnavailable=True.
// The pass after reads the apiserver first and writes no status at all,
// so the condition does not move on every pass.
func TestReconcile_PodGroupAPIRemovedUnderReadyGangsPublishesTheConditionAndGoesOn(t *testing.T) {
	g := newGangReaders(t, 2)
	g.removeThePodGroupAPI()
	g.pass()
	g.requireCondition(metav1.ConditionTrue, workloadtypes.ReasonPodGroupCRDNotInstalled)
	g.requireGroupLists(1, 1)
	g.requireGroupWrites(0)
	g.requireNoPodWrite()

	before := g.replicaResourceVersion()
	g.pass()
	g.requireCondition(metav1.ConditionTrue, workloadtypes.ReasonPodGroupCRDNotInstalled)
	if after := g.replicaResourceVersion(); after != before {
		t.Fatalf("the pass after the API's removal wrote the replica's status (%s -> %s); nothing changed", before, after)
	}
	g.requireGroupLists(1, 2)
	g.requireGroupWrites(0)
	g.requireNoPodWrite()
}

// TestReconcile_PodGroupAPIReinstalledIsNoticedOnTheNextPass: the PodGroup
// API was removed under two Ready gangs and is installed again, empty. The
// next pass reads the apiserver first, finds the API back, writes both
// groups again and publishes GangSchedulingUnavailable=False; the pass
// after that reads the cache again.
func TestReconcile_PodGroupAPIReinstalledIsNoticedOnTheNextPass(t *testing.T) {
	g := newGangReaders(t, 2)
	g.removeThePodGroupAPI()
	g.pass()
	g.pass()
	g.reinstallThePodGroupAPI()
	g.pass()
	g.requireCondition(metav1.ConditionFalse, workloadtypes.ReasonGangSchedulingAvailable)
	g.requireGroupOfGang(0)
	g.requireGroupOfGang(1)
	g.requireGroupWrites(2)
	g.requireGroupLists(1, 3)
	g.pass()
	g.requireGroupLists(2, 3)
	g.requireGroupWrites(2)
	g.requireNoPodWrite()
}

// TestReconcile_ATransientPodGroupListErrorEndsThePassAndKeepsTheCondition:
// a Ready gang's group was deleted by hand, so the pass reads the apiserver
// before it writes the group again, and the apiserver answers that list
// with a transient failure. The pass ends with that error and writes
// nothing, the condition keeps saying gang scheduling is available, and
// the pass after the failure writes the group.
func TestReconcile_ATransientPodGroupListErrorEndsThePassAndKeepsTheCondition(t *testing.T) {
	g := newGangReaders(t, 2)
	g.deleteGroupByHand(0)
	g.liveListFailure = apierrors.NewServiceUnavailable("the apiserver is busy")
	if err := g.passMayFail(); err == nil || !apierrors.IsServiceUnavailable(err) {
		t.Fatalf("pass under a transient PodGroup list failure: error %v, want the failure", err)
	}
	g.requireCondition(metav1.ConditionFalse, workloadtypes.ReasonGangSchedulingAvailable)
	g.requireGroupWrites(0)
	g.liveListFailure = nil
	g.pass()
	g.requireGroupOfGang(0)
	g.requireGroupWrites(1)
	g.requireNoPodWrite()
}

// replicaResourceVersion is the stored replica's resource version, which
// moves on every status write.
func (g *gangReaders) replicaResourceVersion() string {
	g.t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if err := g.base.Get(context.Background(), g.req.NamespacedName, ir); err != nil {
		g.t.Fatalf("read the replica: %v", err)
	}
	return ir.ResourceVersion
}
