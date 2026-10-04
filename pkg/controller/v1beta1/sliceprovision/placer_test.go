package sliceprovision

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// placerFixture is the slice store, the node store, the owner's pods, and
// the event sink a reconcile pass's Placer works against.
type placerFixture struct {
	slices    client.WithWatch
	nodes     client.WithWatch
	p         *Provisioner
	recorder  *record.FakeRecorder
	nodeLists *int
	// owned are the owner's pods; podReads counts their reads.
	owned    *[]*corev1.Pod
	podReads *int
}

// newPlacerFixture serves nodes from a store holding only the given nodes,
// counting each list. The owner has no pods.
func newPlacerFixture(t *testing.T, nodes ...client.Object) placerFixture {
	t.Helper()
	slices := newFakeClient()
	var lists, reads int
	return placerFixture{
		slices: slices,
		nodes: fake.NewClientBuilder().WithObjects(nodes...).WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				lists++
				return c.List(ctx, list, opts...)
			},
		}).Build(),
		p:         newProvisioner(t, slices, testOwner),
		recorder:  record.NewFakeRecorder(16),
		nodeLists: &lists,
		owned:     &[]*corev1.Pod{},
		podReads:  &reads,
	}
}

// provisioning is a fixture whose accelerator has a provision-only pool.
func provisioning(t *testing.T) placerFixture {
	t.Helper()
	return newPlacerFixture(t, provisionOnlyNode("node-a", "tpu-a"))
}

func (f placerFixture) placer() *Placer { return NewPlacer(f.p, f.nodes, f.pods, f.recorder) }

func (f placerFixture) pods(context.Context) ([]*corev1.Pod, error) {
	*f.podReads++
	return *f.owned, nil
}

// addNodes adds the nodes to the node store.
func (f placerFixture) addNodes(t *testing.T, nodes ...*corev1.Node) {
	t.Helper()
	for _, n := range nodes {
		if err := f.nodes.Create(context.Background(), n); err != nil {
			t.Fatalf("add node %s: %v", n.Name, err)
		}
	}
}

// own gives the owner the pods.
func (f placerFixture) own(pods ...*corev1.Pod) { *f.owned = pods }

// onSlice is a running pod confined to slice name.
func onSlice(podName, name string) *corev1.Pod {
	return pod(podName, map[string]string{keySlice: name}, corev1.PodRunning)
}

// events drains the warnings recorded so far.
func (f placerFixture) events() []string {
	var out []string
	for {
		select {
		case e := <-f.recorder.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// update rewrites the stored slice named name.
func (f placerFixture) update(t *testing.T, name string, mutate ...func(*unstructured.Unstructured)) {
	t.Helper()
	u, found := getSlice(t, f.slices, name)
	if !found {
		t.Fatalf("slice %s not found", name)
	}
	for _, m := range mutate {
		m(u)
	}
	if err := f.slices.Update(context.Background(), u); err != nil {
		t.Fatalf("update slice %s: %v", name, err)
	}
}

func (f placerFixture) seed(t *testing.T, objs ...*unstructured.Unstructured) {
	t.Helper()
	for _, u := range objs {
		if err := f.slices.Create(context.Background(), u); err != nil {
			t.Fatalf("seed %s: %v", u.GetName(), err)
		}
	}
}

func singlePod(idx int32) workload.InstancePlan {
	return workload.InstancePlan{Index: idx, Runners: []workload.RunnerPlan{{Name: workload.RunnerDefault, Size: 1}}}
}

func leaderWorkers(idx, workers int32) workload.InstancePlan {
	return workload.InstancePlan{Index: idx, Runners: []workload.RunnerPlan{
		{Name: workload.RunnerLeader, Size: 1},
		{Name: workload.RunnerWorker, Size: workers},
	}}
}

func placerInput(pod, worker *corev1.PodSpec, rows ...workload.InstanceStatus) workload.ReconcileInput {
	return workload.ReconcileInput{
		EventTarget:   &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "ns"}},
		Key:           workload.Key{Namespace: "ns", Component: workload.ComponentEngine, OwnerName: "svc"},
		DesiredSpec:   workload.WorkloadDesiredSpec{PodSpec: pod, WorkerPodSpec: worker},
		ObservedState: workload.WorkloadObservedState{InstanceStatuses: rows},
	}
}

func place(t *testing.T, pl *Placer, input workload.ReconcileInput, inst workload.InstancePlan, runner string, ordinal int32) (map[string]string, bool) {
	t.Helper()
	selector, placed, err := pl.Place(context.Background(), input, workload.ComponentPlan{}, inst, workload.RunnerPlan{Name: runner}, ordinal)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	return selector, placed
}

func pending(t *testing.T, pl *Placer, input workload.ReconcileInput, inst workload.InstancePlan, ordinal int32) (string, bool) {
	t.Helper()
	reason, held, err := pl.Pending(context.Background(), input, workload.ComponentPlan{}, inst, workload.RunnerPlan{Name: workload.RunnerDefault}, ordinal)
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	return reason, held
}

func TestPlaceLeavesStaticPoolsUnconfined(t *testing.T) {
	f := newPlacerFixture(t, node("node-a", map[string]string{keyAccelerator: "tpu-a"}))
	input := placerInput(podSpec("tpu-a", "2x2x1", 4), nil)
	selector, placed := place(t, f.placer(), input, singlePod(0), workload.RunnerDefault, 0)
	if !placed || selector != nil {
		t.Fatalf("Place = %v, %v; want the pod placed unconfined", selector, placed)
	}
	if reason, held := pending(t, f.placer(), input, singlePod(0), 0); held {
		t.Fatalf("Pending = %q, want nothing withheld", reason)
	}
	if held, err := f.p.Holds(context.Background()); err != nil || held {
		t.Fatalf("Holds = %v, %v; want no slice created", held, err)
	}
}

func TestPlaceConfinesThePodOnceItsSliceIsReady(t *testing.T) {
	f := provisioning(t)
	input := placerInput(podSpec("tpu-a", "2x2x1", 4), nil)
	inst := singlePod(3)
	name := f.p.Name(Slot{Instance: 3, Ordinal: 1})

	if selector, placed := place(t, f.placer(), input, inst, workload.RunnerDefault, 1); placed || selector != nil {
		t.Fatalf("Place = %v, %v; want the pod withheld until its slice is ready", selector, placed)
	}
	if _, found := getSlice(t, f.slices, name); !found {
		t.Fatalf("Place did not create slice %s", name)
	}
	if reason, held := pending(t, f.placer(), input, inst, 1); !held || !strings.Contains(reason, name) {
		t.Fatalf("Pending = %q, %v; want the slice named", reason, held)
	}

	f.addNodes(t, sliceNode("node-b", name))
	f.update(t, name, withState("ACTIVE", ""))
	selector, placed := place(t, f.placer(), input, inst, workload.RunnerDefault, 1)
	if !placed {
		t.Fatal("Place withheld a pod whose slice is ready")
	}
	if diff := cmp.Diff(map[string]string{keySlice: name}, selector); diff != "" {
		t.Fatalf("Place node selector mismatch (-want +got):\n%s", diff)
	}
	if reason, held := pending(t, f.placer(), input, inst, 1); held {
		t.Fatalf("Pending = %q, want nothing withheld", reason)
	}
	if got := f.events(); len(got) != 0 {
		t.Fatalf("events = %v, want none", got)
	}
	if *f.podReads != 0 {
		t.Fatalf("read pods %d times, want none for a slice whose hosts can take pods", *f.podReads)
	}
}

// Every member of a multi-pod Instance shares one slice, and a pass sees one
// placement for it: the members start together or not at all.
func TestPlaceGivesAGangOnePlacementPerPass(t *testing.T) {
	var creates int
	f := provisioning(t)
	f.slices = fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			creates++
			return c.Create(ctx, obj, opts...)
		},
	}).Build()
	f.p = newProvisioner(t, f.slices, testOwner)
	spec := podSpec("tpu-a", "2x2x2", 4)
	input := placerInput(spec, spec.DeepCopy())
	inst := leaderWorkers(0, 1)

	pl := f.placer()
	place(t, pl, input, inst, workload.RunnerLeader, 0)
	f.addNodes(t, sliceNode("host-a", f.p.Name(Slot{})), sliceNode("host-b", f.p.Name(Slot{})))
	f.update(t, f.p.Name(Slot{}), withState("ACTIVE", ""))
	if _, placed := place(t, pl, input, inst, workload.RunnerWorker, 0); placed {
		t.Fatal("a member was placed on a slice its peer was withheld from in the same pass")
	}
	if creates != 1 {
		t.Fatalf("created %d slices, want one for the gang", creates)
	}

	pl = f.placer()
	leader, placedLeader := place(t, pl, input, inst, workload.RunnerLeader, 0)
	worker, placedWorker := place(t, pl, input, inst, workload.RunnerWorker, 0)
	if !placedLeader || !placedWorker || !cmp.Equal(leader, worker) {
		t.Fatalf("members placed %v %v on %v and %v, want both on the one slice", placedLeader, placedWorker, leader, worker)
	}
	if *f.nodeLists != 3 {
		t.Fatalf("listed nodes %d times over two passes, want the demand resolved once per pass and the ready slice's hosts vetted once", *f.nodeLists)
	}
}

func TestPlaceWithholdsAnInvalidDemand(t *testing.T) {
	f := provisioning(t)
	input := placerInput(podSpec("tpu-a", "2x2x2", 4), nil)
	pl := f.placer()
	for range 2 {
		if selector, placed := place(t, pl, input, singlePod(0), workload.RunnerDefault, 0); placed || selector != nil {
			t.Fatalf("Place = %v, %v; want a pod that cannot fill a slice withheld", selector, placed)
		}
	}
	got := f.events()
	if len(got) != 1 || !strings.Contains(got[0], string(EventReasonSliceDemandInvalid)) || !strings.Contains(got[0], "instance=0") {
		t.Fatalf("events = %v, want one %s warning naming the Instance", got, EventReasonSliceDemandInvalid)
	}
	if reason, held := pending(t, pl, input, singlePod(0), 0); !held || !strings.Contains(reason, "2x2x2") {
		t.Fatalf("Pending = %q, %v; want the demand's error", reason, held)
	}
	if held, err := f.p.Holds(context.Background()); err != nil || held {
		t.Fatalf("Holds = %v, %v; want no slice created", held, err)
	}
}

func TestPlaceWithholdsFromASliceItDoesNotHold(t *testing.T) {
	f := provisioning(t)
	d := demand(t, "2x2x1")
	f.seed(t, stored(t, f.p, d, Slot{}, withState("ACTIVE", ""), withLabel(LabelOwnerUID, "uid-b")))
	input := placerInput(podSpec("tpu-a", "2x2x1", 4), nil)
	pl := f.placer()
	for range 2 {
		if selector, placed := place(t, pl, input, singlePod(0), workload.RunnerDefault, 0); placed || selector != nil {
			t.Fatalf("Place = %v, %v; want the pod withheld from a foreign slice", selector, placed)
		}
	}
	got := f.events()
	if len(got) != 1 || !strings.Contains(got[0], string(EventReasonSliceOwnershipConflict)) {
		t.Fatalf("events = %v, want one %s warning", got, EventReasonSliceOwnershipConflict)
	}
	if reason, held := pending(t, pl, input, singlePod(0), 0); !held || !strings.Contains(reason, f.p.Name(Slot{})) {
		t.Fatalf("Pending = %q, %v; want the foreign slice named", reason, held)
	}
}

func TestPlaceRetriesFailedReads(t *testing.T) {
	boom := errors.New("boom")
	failNodes := true
	f := provisioning(t)
	nodes := f.nodes
	f.nodes = fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		List: func(ctx context.Context, _ client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if failNodes {
				return boom
			}
			return nodes.List(ctx, list, opts...)
		},
	}).Build()
	input := placerInput(podSpec("tpu-a", "2x2x1", 4), nil)
	pl := f.placer()
	ctx := context.Background()
	if _, _, err := pl.Place(ctx, input, workload.ComponentPlan{}, singlePod(0), workload.RunnerPlan{}, 0); !errors.Is(err, boom) {
		t.Fatalf("Place error = %v, want the node read's", err)
	}
	if _, _, err := pl.Pending(ctx, input, workload.ComponentPlan{}, singlePod(0), workload.RunnerPlan{}, 0); !errors.Is(err, boom) {
		t.Fatalf("Pending error = %v, want the node read's", err)
	}
	failNodes = false
	if _, placed := place(t, pl, input, singlePod(0), workload.RunnerDefault, 0); placed {
		t.Fatal("Place placed a pod on a slice it just created")
	}
	if _, found := getSlice(t, f.slices, f.p.Name(Slot{})); !found {
		t.Fatal("Place did not retry the failed read within the pass")
	}
}

func TestPlaceReturnsSliceWriteErrors(t *testing.T) {
	boom := errors.New("boom")
	f := provisioning(t)
	f.p = newProvisioner(t, fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error { return boom },
	}).Build(), testOwner)
	input := placerInput(podSpec("tpu-a", "2x2x1", 4), nil)
	if _, _, err := f.placer().Place(context.Background(), input, workload.ComponentPlan{}, singlePod(0), workload.RunnerPlan{}, 0); !errors.Is(err, boom) {
		t.Fatalf("Place error = %v, want the create error", err)
	}
}

// Pending only reads: a slot without a slice waits for Place to create it.
func TestPendingNeverCreates(t *testing.T) {
	f := provisioning(t)
	input := placerInput(podSpec("tpu-a", "2x2x1", 4), nil)
	reason, held := pending(t, f.placer(), input, singlePod(0), 0)
	if !held || !strings.Contains(reason, "not created yet") {
		t.Fatalf("Pending = %q, %v; want the slot held for its slice", reason, held)
	}
	if held, err := f.p.Holds(context.Background()); err != nil || held {
		t.Fatalf("Holds = %v, %v; want no slice created", held, err)
	}
}

// A migration surge renders its pods from the source's revision, so a pass
// resolves each template set it places pods from.
func TestPlaceResolvesEachTemplateSet(t *testing.T) {
	f := provisioning(t)
	input := placerInput(podSpec("tpu-a", "2x2x1", 4), nil)
	surge := input
	surge.DesiredSpec.PodSpec = podSpec("tpu-a", "2x2x2", 4)
	pl := f.placer()
	place(t, pl, input, singlePod(0), workload.RunnerDefault, 0)
	pending(t, pl, input, singlePod(0), 0)
	if *f.nodeLists != 1 {
		t.Fatalf("listed nodes %d times, want the Instance's demand resolved once", *f.nodeLists)
	}
	place(t, pl, surge, singlePod(1), workload.RunnerDefault, 0)
	if *f.nodeLists != 2 {
		t.Fatalf("listed nodes %d times, want the surge's templates resolved on their own", *f.nodeLists)
	}
	if got := f.events(); len(got) != 1 || !strings.Contains(got[0], "instance=1") {
		t.Fatalf("events = %v, want the surge's untileable demand reported", got)
	}
}

func TestPlacerSweep(t *testing.T) {
	f := provisioning(t)
	d := demand(t, "2x2x1")
	wide := demand(t, "2x2x2")
	surgeTarget := int32(8)
	rows := []workload.InstanceStatus{
		{Index: 0, ActiveOrdinal: 1},
		{Index: 1, Operation: &workload.InstanceOperation{Type: workload.InstanceOperationUpdate, Step: workload.UpdateStepSurge}},
		{Index: 4, Operation: &workload.InstanceOperation{Type: workload.InstanceOperationMigrate, SurgeIndex: &surgeTarget}},
		{Index: 6, Operation: &workload.InstanceOperation{Type: workload.InstanceOperationMigrate, SurgeIndex: &surgeTarget}},
	}
	input := placerInput(podSpec("tpu-a", "2x2x1", 4), nil, rows...)
	plan := workload.ComponentPlan{Instances: []workload.InstancePlan{singlePod(0), singlePod(1), singlePod(2), singlePod(3), singlePod(4)}}

	seed := map[string]*unstructured.Unstructured{
		"inactive ordinal":        stored(t, f.p, d, Slot{Instance: 0}),
		"active ordinal":          stored(t, f.p, d, Slot{Instance: 0, Ordinal: 1}),
		"surging, ordinal 0":      stored(t, f.p, d, Slot{Instance: 1}),
		"surging, ordinal 1":      stored(t, f.p, d, Slot{Instance: 1, Ordinal: 1}),
		"no row yet":              stored(t, f.p, d, Slot{Instance: 2}),
		"no row, ordinal 1":       stored(t, f.p, d, Slot{Instance: 2, Ordinal: 1}),
		"other shape":             stored(t, f.p, wide, Slot{Instance: 3}),
		"surge target":            stored(t, f.p, d, Slot{Instance: surgeTarget}),
		"placed this pass":        stored(t, f.p, d, Slot{Instance: 9}, withState("ACTIVE", "")),
		"scaled away":             stored(t, f.p, d, Slot{Instance: 5}),
		"scaled away, but pinned": stored(t, f.p, d, Slot{Instance: 7}),
	}
	for _, u := range seed {
		f.seed(t, u)
	}
	f.addNodes(t, sliceNode("host-9", seed["placed this pass"].GetName()))

	pl := f.placer()
	if _, placed := place(t, pl, input, singlePod(9), workload.RunnerDefault, 0); !placed {
		t.Fatal("Place withheld a pod whose slice is ready")
	}
	f.own(onSlice("pinning", seed["scaled away, but pinned"].GetName()))
	if err := pl.Sweep(context.Background(), input, plan); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	for key, want := range map[string]bool{
		"inactive ordinal":        false,
		"active ordinal":          true,
		"surging, ordinal 0":      true,
		"surging, ordinal 1":      true,
		"no row yet":              true,
		"no row, ordinal 1":       false,
		"other shape":             false,
		"surge target":            true,
		"placed this pass":        true,
		"scaled away":             false,
		"scaled away, but pinned": true,
	} {
		if _, found := getSlice(t, f.slices, seed[key].GetName()); found != want {
			t.Errorf("slice %q (%s) present = %v, want %v", key, seed[key].GetName(), found, want)
		}
	}
}

func TestPlacerSweepKeepsNothingForAnInvalidDemand(t *testing.T) {
	f := provisioning(t)
	f.seed(t, stored(t, f.p, demand(t, "2x2x1"), Slot{}))
	input := placerInput(podSpec("tpu-a", "2x2x2", 4), nil)
	if err := f.placer().Sweep(context.Background(), input, workload.ComponentPlan{Instances: []workload.InstancePlan{singlePod(0)}}); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if held, err := f.p.Holds(context.Background()); err != nil || held {
		t.Fatalf("Holds = %v, %v; want the slice no pod can use released", held, err)
	}
}

func TestPlacerSweepReleasesNothingWhenADemandCannotBeRead(t *testing.T) {
	boom := errors.New("boom")
	f := provisioning(t)
	f.nodes = fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error { return boom },
	}).Build()
	stale := stored(t, f.p, demand(t, "2x2x1"), Slot{Instance: 5})
	f.seed(t, stale)
	input := placerInput(podSpec("tpu-a", "2x2x1", 4), nil)
	err := f.placer().Sweep(context.Background(), input, workload.ComponentPlan{Instances: []workload.InstancePlan{singlePod(0)}})
	if !errors.Is(err, boom) {
		t.Fatalf("Sweep error = %v, want the node read's", err)
	}
	if _, found := getSlice(t, f.slices, stale.GetName()); !found {
		t.Fatal("Sweep released a slice without knowing what the plan needs")
	}
}

func TestPodSpecs(t *testing.T) {
	leader := podSpec("tpu-a", "2x2x2", 4)
	worker := podSpec("tpu-a", "2x2x2", 4)
	inst := leaderWorkers(0, 2)
	got := podSpecs(workload.WorkloadDesiredSpec{PodSpec: leader, WorkerPodSpec: worker}, inst)
	if len(got) != 3 || got[0] != leader || got[1] != worker || got[2] != worker {
		t.Fatalf("podSpecs = %v, want the leader's template then a worker template per worker", got)
	}
	got = podSpecs(workload.WorkloadDesiredSpec{PodSpec: leader}, inst)
	if len(got) != 3 || got[0] != leader || got[1] != leader || got[2] != leader {
		t.Fatalf("podSpecs = %v, want the pod template for every pod without a worker template", got)
	}
}

func TestLiveInstances(t *testing.T) {
	target, orphanTarget, plannedTarget := int32(5), int32(6), int32(1)
	input := placerInput(nil, nil,
		workload.InstanceStatus{Index: 0, Operation: &workload.InstanceOperation{SurgeIndex: &target}},
		workload.InstanceStatus{Index: 3, Operation: &workload.InstanceOperation{SurgeIndex: &orphanTarget}},
		workload.InstanceStatus{Index: 2, Operation: &workload.InstanceOperation{SurgeIndex: &plannedTarget}},
	)
	plan := workload.ComponentPlan{Instances: []workload.InstancePlan{leaderWorkers(0, 2), singlePod(1), singlePod(2)}}
	want := []workload.InstancePlan{leaderWorkers(0, 2), singlePod(1), singlePod(2), leaderWorkers(5, 2)}
	if diff := cmp.Diff(want, liveInstances(input, plan)); diff != "" {
		t.Fatalf("liveInstances mismatch (-want +got):\n%s", diff)
	}
}

func TestLiveOrdinals(t *testing.T) {
	input := placerInput(nil, nil,
		workload.InstanceStatus{Index: 0, ActiveOrdinal: 1},
		workload.InstanceStatus{Index: 1, Operation: &workload.InstanceOperation{Type: workload.InstanceOperationUpdate}},
		workload.InstanceStatus{Index: 2, ActiveOrdinal: 1, Operation: &workload.InstanceOperation{Type: workload.InstanceOperationUpdate}},
	)
	for _, tt := range []struct {
		name string
		inst workload.InstancePlan
		want []int32
	}{
		{name: "settled", inst: singlePod(0), want: []int32{1}},
		{name: "operation in flight", inst: singlePod(1), want: []int32{0, 1}},
		{name: "a gang holds one slot", inst: leaderWorkers(2, 1), want: []int32{0}},
		{name: "no row yet", inst: singlePod(3), want: []int32{0}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, liveOrdinals(input, tt.inst)); diff != "" {
				t.Fatalf("liveOrdinals mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPlacerSweepKeepsSlicesAcrossSliceStates(t *testing.T) {
	f := provisioning(t)
	d := demand(t, "2x2x1")
	f.seed(t,
		stored(t, f.p, d, Slot{}, withState("PROVISIONING", "")),
		stored(t, f.p, d, Slot{Instance: 1}, withState("FAILED", "")),
	)
	input := placerInput(podSpec("tpu-a", "2x2x1", 4), nil)
	plan := workload.ComponentPlan{Instances: []workload.InstancePlan{singlePod(0), singlePod(1)}}
	if err := f.placer().Sweep(context.Background(), input, plan); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if *f.podReads != 0 {
		t.Fatalf("Sweep read pods %d times, want none: every slice is wanted", *f.podReads)
	}
	for _, slot := range []Slot{{}, {Instance: 1}} {
		if _, found := getSlice(t, f.slices, f.p.Name(slot)); !found {
			t.Errorf("slice for %+v released while its slot is planned", slot)
		}
	}
}

// A ready slice that no pod holds cannot start its slot on a host no pod can
// be scheduled on: its pods are withheld and reported, Sweep releases it, and
// the next pass provisions the slot again.
func TestPlaceReplacesASliceWithAnUnavailableHost(t *testing.T) {
	f := provisioning(t)
	name := f.p.Name(Slot{})
	f.seed(t, stored(t, f.p, demand(t, "2x2x1"), Slot{}, withState("ACTIVE", "")))
	f.addNodes(t, sliceNode("node-b", name, cordoned))
	old, _ := getSlice(t, f.slices, name)
	input := placerInput(podSpec("tpu-a", "2x2x1", 4), nil)
	plan := workload.ComponentPlan{Instances: []workload.InstancePlan{singlePod(0)}}

	pl := f.placer()
	for range 2 {
		if selector, placed := place(t, pl, input, singlePod(0), workload.RunnerDefault, 0); placed || selector != nil {
			t.Fatalf("Place = %v, %v; want the pod withheld from a slice with a cordoned host", selector, placed)
		}
	}
	got := f.events()
	if len(got) != 1 || !strings.Contains(got[0], string(EventReasonSliceHostUnavailable)) ||
		!strings.Contains(got[0], "instance=0") || !strings.Contains(got[0], "node node-b is cordoned") {
		t.Fatalf("events = %v, want one %s warning naming the Instance and the host", got, EventReasonSliceHostUnavailable)
	}
	for _, p := range []*Placer{pl, f.placer()} {
		if reason, held := pending(t, p, input, singlePod(0), 0); !held || !strings.Contains(reason, name) || !strings.Contains(reason, "node node-b is cordoned") {
			t.Fatalf("Pending = %q, %v; want the slice and its host named", reason, held)
		}
	}
	if err := pl.Sweep(context.Background(), input, plan); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if _, found := getSlice(t, f.slices, name); found {
		t.Fatal("Sweep kept a slice its slot cannot start on")
	}

	if _, placed := place(t, f.placer(), input, singlePod(0), workload.RunnerDefault, 0); placed {
		t.Fatal("Place placed a pod on a slice it just created")
	}
	fresh, found := getSlice(t, f.slices, name)
	if !found || fresh.GetUID() == old.GetUID() {
		t.Fatalf("slice %s found = %v; want the slot provisioned again", name, found)
	}
}

// A slice a pod holds is left to the scheduler: the pods its slot still needs
// may fit on the hosts that can take them.
func TestPlaceLeavesAHeldSliceToTheScheduler(t *testing.T) {
	f := provisioning(t)
	name := f.p.Name(Slot{})
	f.seed(t, stored(t, f.p, demand(t, "2x2x2"), Slot{}, withState("ACTIVE", "")))
	f.addNodes(t, sliceNode("node-b", name, cordoned), sliceNode("node-c", name))
	f.own(onSlice("leader", name))
	spec := podSpec("tpu-a", "2x2x2", 4)
	input := placerInput(spec, spec.DeepCopy())
	inst := leaderWorkers(0, 1)

	pl := f.placer()
	selector, placed := place(t, pl, input, inst, workload.RunnerWorker, 0)
	if !placed {
		t.Fatal("Place withheld a pod from a slice a pod holds")
	}
	if diff := cmp.Diff(map[string]string{keySlice: name}, selector); diff != "" {
		t.Fatalf("Place node selector mismatch (-want +got):\n%s", diff)
	}
	if reason, held := pending(t, pl, input, inst, 0); held {
		t.Fatalf("Pending = %q, want nothing withheld", reason)
	}
	if err := pl.Sweep(context.Background(), input, workload.ComponentPlan{Instances: []workload.InstancePlan{inst}}); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if _, found := getSlice(t, f.slices, name); !found {
		t.Fatal("Sweep released a slice a pod holds")
	}
	if got := f.events(); len(got) != 0 {
		t.Fatalf("events = %v, want none", got)
	}
}

// A pod being deleted pins its slice but no longer holds it: the slot's next
// pod is withheld, and the slice is released once the pod is gone.
func TestPlaceReplacesASliceOnceItsLastPodIsGone(t *testing.T) {
	f := provisioning(t)
	name := f.p.Name(Slot{})
	f.seed(t, stored(t, f.p, demand(t, "2x2x1"), Slot{}, withState("ACTIVE", "")))
	f.addNodes(t, sliceNode("node-b", name, readiness(corev1.ConditionFalse)))
	leaving := onSlice("leaving", name)
	leaving.DeletionTimestamp = &metav1.Time{}
	f.own(leaving)
	input := placerInput(podSpec("tpu-a", "2x2x1", 4), nil)
	plan := workload.ComponentPlan{Instances: []workload.InstancePlan{singlePod(0)}}

	for _, tt := range []struct {
		pods  []*corev1.Pod
		found bool
	}{
		{pods: []*corev1.Pod{leaving}, found: true},
		{pods: nil, found: false},
	} {
		f.own(tt.pods...)
		pl := f.placer()
		if _, placed := place(t, pl, input, singlePod(0), workload.RunnerDefault, 0); placed {
			t.Fatalf("Place placed a pod on a slice whose host is not ready, with pods %v", tt.pods)
		}
		if err := pl.Sweep(context.Background(), input, plan); err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if _, found := getSlice(t, f.slices, name); found != tt.found {
			t.Fatalf("slice present = %v with pods %v, want %v", found, tt.pods, tt.found)
		}
	}
}

func TestPlacerSweepNeedsAPodSource(t *testing.T) {
	f := provisioning(t)
	f.seed(t, stored(t, f.p, demand(t, "2x2x1"), Slot{Instance: 5}))
	input := placerInput(podSpec("tpu-a", "2x2x1", 4), nil)
	if err := NewPlacer(f.p, f.nodes, nil, f.recorder).Sweep(context.Background(), input, workload.ComponentPlan{}); err == nil {
		t.Fatal("Sweep released slices without reading which pods pin them")
	}
	if held, err := f.p.Holds(context.Background()); err != nil || !held {
		t.Fatalf("Holds = %v, %v; want the slice kept", held, err)
	}
}

func TestPlaceRetriesFailedPodReads(t *testing.T) {
	boom := errors.New("boom")
	f := provisioning(t)
	name := f.p.Name(Slot{})
	f.seed(t, stored(t, f.p, demand(t, "2x2x1"), Slot{}, withState("ACTIVE", "")))
	f.addNodes(t, sliceNode("node-b", name, cordoned))
	fail := true
	pl := NewPlacer(f.p, f.nodes, func(ctx context.Context) ([]*corev1.Pod, error) {
		if fail {
			return nil, boom
		}
		return f.pods(ctx)
	}, f.recorder)
	input := placerInput(podSpec("tpu-a", "2x2x1", 4), nil)
	ctx := context.Background()
	if _, _, err := pl.Place(ctx, input, workload.ComponentPlan{}, singlePod(0), workload.RunnerPlan{}, 0); !errors.Is(err, boom) {
		t.Fatalf("Place error = %v, want the pod read's", err)
	}
	if _, _, err := pl.Pending(ctx, input, workload.ComponentPlan{}, singlePod(0), workload.RunnerPlan{}, 0); !errors.Is(err, boom) {
		t.Fatalf("Pending error = %v, want the pod read's", err)
	}
	fail = false
	if _, placed := place(t, pl, input, singlePod(0), workload.RunnerDefault, 0); placed {
		t.Fatal("Place placed a pod on a slice with a cordoned host")
	}
	if got := f.events(); len(got) != 1 || !strings.Contains(got[0], string(EventReasonSliceHostUnavailable)) {
		t.Fatalf("events = %v, want the retried vet reported once", got)
	}
}

// withReadiness sets the Ready condition's reason, status and last transition.
func withReadiness(reason, status string, since time.Time) func(*unstructured.Unstructured) {
	return func(u *unstructured.Unstructured) {
		u.Object["status"] = map[string]interface{}{"conditions": []interface{}{map[string]interface{}{
			"type": "Ready", "status": status, "reason": reason, "lastTransitionTime": since.UTC().Format(time.RFC3339),
		}}}
	}
}

// A ready slice's hosts are found by a label the provider adds as it
// activates. The slot's pods wait, unwarned and with the slice kept, until
// every host the slice needs is visible.
func TestPlaceWaitsForEveryHost(t *testing.T) {
	f := provisioning(t)
	name := f.p.Name(Slot{})
	f.seed(t, stored(t, f.p, demand(t, "2x2x2"), Slot{}, withState("ACTIVE", "")))
	f.addNodes(t, sliceNode("host-a", name))
	spec := podSpec("tpu-a", "2x2x2", 4)
	input := placerInput(spec, spec.DeepCopy())
	inst := leaderWorkers(0, 1)

	pl := f.placer()
	if selector, placed := place(t, pl, input, inst, workload.RunnerLeader, 0); placed || selector != nil {
		t.Fatalf("Place = %v, %v; want the gang held until both hosts are visible", selector, placed)
	}
	if reason, held := pending(t, pl, input, inst, 0); !held || !strings.Contains(reason, "only 1 of its 2 hosts are visible") {
		t.Fatalf("Pending = %q, %v; want the missing host reported", reason, held)
	}
	if err := pl.Sweep(context.Background(), input, workload.ComponentPlan{Instances: []workload.InstancePlan{inst}}); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if _, found := getSlice(t, f.slices, name); !found {
		t.Fatal("Sweep released a slice whose hosts are still being labeled")
	}
	if got := f.events(); len(got) != 0 {
		t.Fatalf("events = %v, want none while hosts are labeled", got)
	}

	f.addNodes(t, sliceNode("host-b", name))
	if _, placed := place(t, f.placer(), input, inst, workload.RunnerLeader, 0); !placed {
		t.Fatal("Place withheld the gang once both hosts were visible")
	}
}

// A provider that reports a ready state with unknown readiness is not trusted
// with pods.
func TestPlaceWithholdsFromUnknownReadiness(t *testing.T) {
	f := provisioning(t)
	name := f.p.Name(Slot{})
	f.seed(t, stored(t, f.p, demand(t, "2x2x1"), Slot{}, withReadiness("ACTIVE", "Unknown", time.Now())))
	f.addNodes(t, sliceNode("host-a", name))
	input := placerInput(podSpec("tpu-a", "2x2x1", 4), nil)
	if _, placed := place(t, f.placer(), input, singlePod(0), workload.RunnerDefault, 0); placed {
		t.Fatal("Place placed a pod on a slice whose readiness is unknown")
	}
	if reason, held := pending(t, f.placer(), input, singlePod(0), 0); !held || !strings.Contains(reason, "readiness is unknown") {
		t.Fatalf("Pending = %q, %v; want the unknown readiness reported", reason, held)
	}
}

// A slice that has partitions but stays out of a ready state past the ready
// timeout is released, so the slot gets a new partition, unless a pod holds
// it. A slice waiting for a partition is waiting for capacity and is kept.
func TestPlaceReprovisionsAStuckSlice(t *testing.T) {
	now := time.Now()
	for _, tt := range []struct {
		name     string
		timeout  string
		since    time.Duration
		parts    bool
		holder   bool
		released bool
	}{
		{name: "stuck past the timeout", timeout: "10m", since: 15 * time.Minute, parts: true, released: true},
		{name: "within the timeout", timeout: "10m", since: 5 * time.Minute, parts: true},
		{name: "waiting for a partition", timeout: "10m", since: 15 * time.Minute},
		{name: "a pod holds it", timeout: "10m", since: 15 * time.Minute, parts: true, holder: true},
		{name: "no ready timeout", since: 15 * time.Minute, parts: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := provisioning(t)
			cfg := testConfig()
			cfg.Slice.ReadyTimeout = tt.timeout
			p, err := New(cfg, f.slices, f.slices, f.slices, testOwner)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			f.p = p
			name := f.p.Name(Slot{})
			mutate := []func(*unstructured.Unstructured){withReadiness("ACTIVATING", "False", now.Add(-tt.since))}
			if tt.parts {
				mutate = append(mutate, withPartitions("p-1"))
			}
			f.seed(t, stored(t, f.p, demand(t, "2x2x1"), Slot{}, mutate...))
			if tt.holder {
				f.own(onSlice("holder", name))
			}
			input := placerInput(podSpec("tpu-a", "2x2x1", 4), nil)
			pl := f.placer()
			if _, placed := place(t, pl, input, singlePod(0), workload.RunnerDefault, 0); placed {
				t.Fatal("Place placed a pod on a slice that is not ready")
			}
			if err := pl.Sweep(context.Background(), input, workload.ComponentPlan{Instances: []workload.InstancePlan{singlePod(0)}}); err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if _, found := getSlice(t, f.slices, name); found == tt.released {
				t.Fatalf("slice present = %v, want released %v", found, tt.released)
			}
			got := f.events()
			warned := len(got) == 1 && strings.Contains(got[0], string(EventReasonSliceReadyTimeout)) && strings.Contains(got[0], "ready timeout")
			if warned != tt.released || (!tt.released && len(got) != 0) {
				t.Fatalf("events = %v, want a %s warning only when the slice is released", got, EventReasonSliceReadyTimeout)
			}
		})
	}
}
