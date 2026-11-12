package inferencereplica

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var updatePublicationGoldens = flag.Bool("update-publication", false, "re-record testdata/publication_allocations.json from this run")

// publicationShape is one replica the publication runs over: Instances of
// one pod each, at rest, every row carrying its own readySince as a
// reference replica stores them.
type publicationShape struct {
	name      string
	instances int
}

// publicationShapes are the budget's shapes: the largest reference
// replica at 4,000 Instances and a quarter of it.
func publicationShapes() []publicationShape {
	return []publicationShape{
		{name: "readySince/1000", instances: 1000},
		{name: "readySince/4000", instances: 4000},
	}
}

// publicationClock is the moment every publication of the fixtures runs
// at.
var publicationClock = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

const (
	// publicationNodes is how many nodes the pods spread over.
	publicationNodes = 64
	// publicationEndpointsPerSlice is the endpoint controller's slice size.
	publicationEndpointsPerSlice = 100
	// publicationColumnBound is the row bound the operator configures for
	// the column form of the status.
	publicationColumnBound = 20000
)

// publicationFixture is one replica at rest behind the fake apiserver, with
// the pass that publishes its status and the counts of the pod lists and
// the writes the publication makes.
type publicationFixture struct {
	pass     *replicaPass
	podLists *fakeclient.Calls
	writes   *fakeclient.Calls
}

// newPublicationFixture stores the replica in the column form production
// writes, its pods bound over the nodes with every pod serving and a Ready
// endpoint, and the pass's copy decoded from the cache as Reconcile reads
// it. The publication is run until one writes nothing, so the measured one
// is the steady one.
func newPublicationFixture(tb testing.TB, shape publicationShape) *publicationFixture {
	tb.Helper()
	ir := publicationReplica(shape)
	f := buildPublicationFixture(tb, shape.name, ir, publicationPods(ir, shape.instances))
	f.settle(tb)
	return f
}

// publicationReplica is the replica of the shape as the pass's copy holds
// it: every row Ready on the revision with its own readySince, the counters
// at the Instance count.
func publicationReplica(shape publicationShape) *v1beta1.InferenceReplica {
	ir := baselineIR("llama-engine", "default", int32(shape.instances))
	ir.Status = *irstatustest.LogicalStatus(irstatustest.ReadySinceRows(shape.instances))
	count := int32(shape.instances)
	ir.Status.ObservedGeneration = ir.Generation
	ir.Status.ReadyReplicas, ir.Status.ServingReplicas, ir.Status.AvailableReplicas = count, count, count
	ir.Status.UpdatedReplicas, ir.Status.UpdatedReadyReplicas = count, count
	ir.Status.UpdateRevision = irstatustest.Revision
	return ir
}

// publicationPods are the replica's pods: one serving pod per Instance on
// the replica's revision, Running on one of the nodes with an address.
func publicationPods(ir *v1beta1.InferenceReplica, instances int) []*corev1.Pod {
	hash := query.RevisionFromName(irstatustest.Revision).Hash()
	pods := make([]*corev1.Pod, 0, instances)
	for i := 0; i < instances; i++ {
		pod := podForIR(ir, int32(i), string(v1beta1.RunnerNameDefault), 0, true, true)
		pod.Labels[query.LabelRevisionHash] = hash
		pod.Spec.NodeName = fmt.Sprintf("node-%d", i%publicationNodes)
		pod.Status.Phase = corev1.PodRunning
		pod.Status.PodIP = fmt.Sprintf("10.%d.%d.%d", i/65536%256, i/256%256, i%256)
		pods = append(pods, pod)
	}
	return pods
}

// buildPublicationFixture stores the replica in the column form production
// writes beside its pods, their endpoint slices and the nodes, and builds
// the pass over the copy decoded from the cache as Reconcile reads it. The
// reconciler's clock stands still, so two fixtures over the same pods
// publish the same times.
func buildPublicationFixture(tb testing.TB, name string, ir *v1beta1.InferenceReplica, pods []*corev1.Pod) *publicationFixture {
	tb.Helper()
	selection, err := irstatus.SelectCandidate(ir.Status.DeepCopy(), publicationColumnBound)
	if err != nil || selection.Selected != irstatus.EncodingColumnarV2 {
		tb.Fatalf("%s: SelectCandidate() = %+v, %v; the column form must be selected", name, selection, err)
	}
	stored := ir.DeepCopy()
	stored.Status = *selection.SelectedCandidate().Status

	component := v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component)
	objects := []client.Object{stored}
	for _, pod := range pods {
		objects = append(objects, pod)
	}
	objects = append(objects, publicationEndpointSlices(ir, component, pods)...)
	for i := 0; i < publicationNodes; i++ {
		objects = append(objects, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%d", i)}})
	}

	podLists := fakeclient.CountingVerb(fakeclient.List).OnKind(&corev1.PodList{})
	writes := fakeclient.CountingWrites()
	cache := fakeclient.New(tb,
		fakeclient.WithObjects(objects...),
		fakeclient.WithStatusSubresource(&v1beta1.InferenceReplica{}),
		fakeclient.WithIndex(&corev1.Pod{}, query.OMENativePodIndexField, query.OMENativePodIndexExtractor),
		podLists, writes)

	decoder := irstatus.NewDecoder(publicationColumnBound)
	reconciler := &Reconciler{
		Client:                cache,
		APIReader:             cache,
		Log:                   logf.Log.WithName("publication"),
		Recorder:              record.NewFakeRecorder(16),
		Expectations:          workloadtypes.NewExpectations(),
		Clock:                 clocktesting.NewFakeClock(publicationClock),
		InstanceStatusDecoder: decoder,
		InstanceStatusTarget:  irstatus.EncodingColumnarV2,
	}
	passCopy := &v1beta1.InferenceReplica{}
	if _, err := irstatus.GetDecoded(context.Background(), irstatus.NewReader(cache, decoder), client.ObjectKeyFromObject(stored), passCopy); err != nil {
		tb.Fatalf("%s: decode the pass's copy: %v", name, err)
	}
	return &publicationFixture{
		pass: &replicaPass{
			reconciler:           reconciler,
			ir:                   passCopy,
			log:                  reconciler.Log,
			priorCurrentRevision: passCopy.Status.CurrentRevision,
			specTarget:           &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: irstatustest.Revision, Namespace: ir.Namespace}},
			plan:                 publicationPlan(component, len(pods)),
			instancesAtStart:     instanceIndices(passCopy.Status.InstanceStatuses),
		},
		podLists: podLists,
		writes:   writes,
	}
}

// takeThePassList takes the Component's pods from the cache as the
// engine takes them in a pass, so the publication that follows is one of a
// pass that listed the pods and wrote none since.
func (f *publicationFixture) takeThePassList(tb testing.TB) {
	tb.Helper()
	f.pass.cachedPods = workloadtypes.NewCachedPodList(f.pass.reconciler.Client)
	if _, err := f.pass.cachedPods.Pods(context.Background(), componentPodLister(f.pass.reconciler.Client, f.pass.ir)); err != nil {
		tb.Fatalf("the engine's list: %v", err)
	}
}

// storedStatus reads the replica's status as the apiserver stores it.
func (f *publicationFixture) storedStatus(tb testing.TB) v1beta1.InferenceReplicaStatus {
	tb.Helper()
	stored := &v1beta1.InferenceReplica{}
	if err := f.pass.reconciler.Client.Get(context.Background(), client.ObjectKeyFromObject(f.pass.ir), stored); err != nil {
		tb.Fatalf("read the stored replica: %v", err)
	}
	return stored.Status
}

// publicationPlan is the plan of a Component of single-pod Instances, every
// Instance on its first attempt.
func publicationPlan(component workloadtypes.ComponentType, instances int) workloadtypes.ComponentPlan {
	plan := workloadtypes.ComponentPlan{
		Component: component,
		Replicas:  int32(instances),
		Instances: make([]workloadtypes.InstancePlan, instances),
	}
	for i := range plan.Instances {
		plan.Instances[i] = workloadtypes.InstancePlan{
			Index: int32(i), Incarnation: 1,
			Runners: []workloadtypes.RunnerPlan{{Name: string(v1beta1.RunnerNameDefault), Size: 1}},
		}
	}
	return plan
}

// publicationEndpointSlices are the headless Service's slices over pods,
// every pod a Ready endpoint, at the endpoint controller's slice size.
func publicationEndpointSlices(ir *v1beta1.InferenceReplica, component workloadtypes.ComponentType, pods []*corev1.Pod) []client.Object {
	serviceName := query.HeadlessServiceName(ir.NamePrefix(), component)
	var slices []client.Object
	for start := 0; start < len(pods); start += publicationEndpointsPerSlice {
		slice := &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-%d", serviceName, start/publicationEndpointsPerSlice),
				Namespace: ir.Namespace,
				Labels:    map[string]string{discoveryv1.LabelServiceName: serviceName},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
		}
		for _, pod := range pods[start:min(start+publicationEndpointsPerSlice, len(pods))] {
			slice.Endpoints = append(slice.Endpoints, discoveryv1.Endpoint{
				Addresses:  []string{pod.Status.PodIP},
				Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
				TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name},
			})
		}
		slices = append(slices, slice)
	}
	return slices
}

// publish runs one publication of the pass: the promotion and the status
// write, over one list of the Component's pods.
func (f *publicationFixture) publish(tb testing.TB) {
	tb.Helper()
	if _, err := f.pass.publishStatus(context.Background(), ctrl.Result{}, nil); err != nil {
		tb.Fatalf("publishStatus: %v", err)
	}
}

// publishCounting runs one publication and returns the pod lists and the
// writes it made.
func (f *publicationFixture) publishCounting(tb testing.TB) (lists, writes int) {
	tb.Helper()
	listsBefore, writesBefore := f.podLists.Count(), f.writes.Count()
	f.publish(tb)
	return f.podLists.Count() - listsBefore, f.writes.Count() - writesBefore
}

// settle runs publications until one writes nothing: the first ones write
// what the fixture leaves to the controller.
func (f *publicationFixture) settle(tb testing.TB) {
	tb.Helper()
	for i := 0; i < 8; i++ {
		if _, writes := f.publishCounting(tb); writes == 0 {
			return
		}
	}
	tb.Fatalf("the replica did not come to rest in 8 publications")
}

// BenchmarkPublication is the cost of the publication at the end of one
// pass over a replica at rest at fleet size, with the pod lists it makes
// beside the time.
func BenchmarkPublication(b *testing.B) {
	for _, shape := range publicationShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newPublicationFixture(b, shape)
			b.ReportAllocs()
			start := f.podLists.Count()
			b.ResetTimer()
			for b.Loop() {
				f.publish(b)
			}
			b.StopTimer()
			b.ReportMetric(float64(f.podLists.Count()-start)/float64(b.N), "pod-lists/op")
		})
	}
}

// TestPublicationStaysWithinBudget holds the allocations and the bytes of
// the publication at the end of one pass over a replica at rest, at 1,000
// and 4,000 Instances, to the recorded golden, and its pod lists: one when
// the pass listed no pods before it, since the promotion and the status
// write decide from the same list, and none when the pass took the pods
// and wrote none since, since the publication reads the pass's list. A
// publication over a replica at rest writes nothing.
func TestPublicationStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "publication_allocations.json"), *updatePublicationGoldens)
	defer golden.Close()
	for _, shape := range publicationShapes() {
		f := newPublicationFixture(t, shape)
		golden.Check(shape.name+"/allocations", func() { f.publish(t) })
		golden.CheckBytes(shape.name+"/bytes", func() { f.publish(t) })
		lists, writes := f.publishCounting(t)
		golden.CheckCount(shape.name+"/pod-lists", int64(lists))
		if lists != 1 {
			t.Errorf("%s: the publication listed the pods %d times, want 1: the promotion and the status write read one list", shape.name, lists)
		}
		if writes != 0 {
			t.Errorf("%s: the publication wrote %d times; a replica at rest is not written", shape.name, writes)
		}

		f.takeThePassList(t)
		golden.Check(shape.name+"/pods-listed/allocations", func() { f.publish(t) })
		golden.CheckBytes(shape.name+"/pods-listed/bytes", func() { f.publish(t) })
		lists, writes = f.publishCounting(t)
		golden.CheckCount(shape.name+"/pods-listed/pod-lists", int64(lists))
		if lists != 0 {
			t.Errorf("%s: the publication listed the pods %d times after the pass took them and wrote none; want 0", shape.name, lists)
		}
		if writes != 0 {
			t.Errorf("%s: the publication from the pass's list wrote %d times; a replica at rest is not written", shape.name, writes)
		}
	}
}

// TestPublicationFromThePassListWritesTheSameStatus: the first
// publication over a replica whose counters the fixture left to the
// controller writes the same status whether it lists the pods itself or
// reads the list the pass took: the same bytes, from the same pods.
func TestPublicationFromThePassListWritesTheSameStatus(t *testing.T) {
	shape := publicationShape{name: "readySince/8", instances: 8}
	ir := publicationReplica(shape)
	pods := publicationPods(ir, shape.instances)
	fresh := buildPublicationFixture(t, "fresh list", ir.DeepCopy(), copyPods(pods))
	taken := buildPublicationFixture(t, "the pass's list", ir.DeepCopy(), copyPods(pods))
	taken.takeThePassList(t)

	freshLists, freshWrites := fresh.publishCounting(t)
	takenLists, takenWrites := taken.publishCounting(t)
	if freshWrites != 1 || takenWrites != 1 {
		t.Fatalf("the first publication writes once: %d writes from a fresh list, %d from the pass's", freshWrites, takenWrites)
	}
	if freshLists != 1 || takenLists != 0 {
		t.Fatalf("pod lists: %d from a fresh list (want 1), %d from the pass's (want 0)", freshLists, takenLists)
	}
	if freshStatus, takenStatus := fresh.storedStatus(t), taken.storedStatus(t); !equality.Semantic.DeepEqual(freshStatus, takenStatus) {
		t.Fatalf("the two publications stored different statuses:\nfresh list: %+v\nthe pass's list: %+v", freshStatus, takenStatus)
	}
}

// TestPublicationConditionsTakeThePassClock: the conditions the first
// publication writes transition at the pass's clock, never at the wall
// clock, so two publications of one reading agree to the second whenever
// they run.
func TestPublicationConditionsTakeThePassClock(t *testing.T) {
	shape := publicationShape{name: "readySince/8", instances: 8}
	ir := publicationReplica(shape)
	f := buildPublicationFixture(t, "the first publication", ir, publicationPods(ir, shape.instances))
	f.publish(t)
	conditions := f.storedStatus(t).Conditions
	if apimeta.FindStatusCondition(conditions, InferenceReplicaConditionReady) == nil {
		t.Fatalf("the first publication wrote no Ready condition: %+v", conditions)
	}
	for _, cond := range conditions {
		if !cond.LastTransitionTime.Time.Equal(publicationClock) {
			t.Errorf("condition %s transitioned at %s, want the pass's clock %s", cond.Type, cond.LastTransitionTime.Time, publicationClock)
		}
	}
}

// TestPublicationListsAgainAfterAPodWrite: a pod written through the
// pass's client after the pass took the pods makes the publication list
// again, so it publishes the write: a pod deleted after the list leaves the
// counters one short. The list it takes is its own: the next publication
// lists again.
func TestPublicationListsAgainAfterAPodWrite(t *testing.T) {
	ctx := context.Background()
	f := newPublicationFixture(t, publicationShape{name: "readySince/8", instances: 8})
	f.takeThePassList(t)
	gone := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: f.pass.ir.Namespace, Name: query.PodName(f.pass.ir.NamePrefix(), workloadtypes.ComponentEngine, 0, string(v1beta1.RunnerNameDefault), 0)}}
	if err := f.pass.cachedPods.Client().Delete(ctx, gone); err != nil {
		t.Fatalf("delete the pod: %v", err)
	}
	lists, writes := f.publishCounting(t)
	if lists != 1 || writes != 1 {
		t.Fatalf("after a pod write: %d pod lists and %d writes, want 1 and 1", lists, writes)
	}
	if status := f.storedStatus(t); status.ServingReplicas != 7 || status.ReadyReplicas != 7 {
		t.Fatalf("the publication did not see the deleted pod: serving %d, ready %d, want 7 and 7", status.ServingReplicas, status.ReadyReplicas)
	}
	if lists, writes := f.publishCounting(t); lists != 1 || writes != 0 {
		t.Fatalf("the next publication: %d pod lists and %d writes, want 1 and 0: every reader after a pod write lists again", lists, writes)
	}
}

// copyPods deep-copies the pods, so two fixtures hold the same pods and
// neither sees the other's writes.
func copyPods(pods []*corev1.Pod) []*corev1.Pod {
	copies := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		copies = append(copies, pod.DeepCopy())
	}
	return copies
}

// laggingPodLists is an informer one read behind the store: a List of pods
// serves the pods as the List before it saw them, so a pod write lands in
// the cache between two reads and is never shown by the first read after
// it. The first List, and a List of any other kind, reads the store.
type laggingPodLists struct {
	previous *corev1.PodList
}

func (l *laggingPodLists) hooks() interceptor.Funcs {
	return interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			pods, ok := list.(*corev1.PodList)
			if !ok {
				return c.List(ctx, list, opts...)
			}
			live := &corev1.PodList{}
			if err := c.List(ctx, live, opts...); err != nil {
				return err
			}
			served := live
			if l.previous != nil {
				served = l.previous
			}
			l.previous = live
			served.DeepCopyInto(pods)
			return nil
		},
	}
}

// decodedRows reads the replica's rows as the apiserver stores them, decoded
// from the column form.
func (f *publicationFixture) decodedRows(tb testing.TB) []v1beta1.OMENativeInstanceStatus {
	tb.Helper()
	stored := &v1beta1.InferenceReplica{}
	reads := irstatus.NewReader(f.pass.reconciler.APIReader, f.pass.reconciler.InstanceStatusDecoder)
	if _, err := irstatus.GetDecoded(context.Background(), reads, client.ObjectKeyFromObject(f.pass.ir), stored); err != nil {
		tb.Fatalf("decode the stored replica: %v", err)
	}
	return stored.Status.InstanceStatuses
}

// TestPublicationReadsThePodsAsTheyStandAfterThePassDeletedOne is the pass
// that cleans a failed replacement away from a serving Instance whose row
// is Ready: the update pass deletes the failed replacement, the create pass lists
// right after, before the cache shows the delete, and the publication at
// the end of the pass must read the pods as they stand by then, not that
// list. A Ready row read against a live pod of its set that serves nothing
// publishes Restarting, which the operator sees as a restart that never
// happened. The cache is one read behind the store, as an informer is for
// the first read after a write.
func TestPublicationReadsThePodsAsTheyStandAfterThePassDeletedOne(t *testing.T) {
	ctx := context.Background()
	const instances = 8
	ir := publicationReplica(publicationShape{name: "serving/8", instances: instances})
	f := buildPublicationFixture(t, "serving/8", ir, publicationPods(ir, instances))
	f.settle(t)
	for _, row := range f.decodedRows(t) {
		if row.Phase != v1beta1.OMENativeInstanceReady {
			t.Fatalf("precondition: every serving Instance is published Ready, got %+v", row)
		}
	}
	store, ok := f.pass.reconciler.Client.(client.WithWatch)
	if !ok {
		t.Fatalf("the fixture's client is not a WithWatch")
	}
	lag := &laggingPodLists{}
	f.pass.reconciler.Client = fakeclient.Wrap(t, store, fakeclient.WithInterceptors(lag.hooks()))

	// The failed replacement beside the serving pod: live, on another
	// revision, serving nothing.
	replacement := podForIR(ir, 0, string(v1beta1.RunnerNameDefault), 1, false, false)
	replacement.Labels[query.LabelRevisionHash] = query.RevisionFromName(irstatustest.OtherRevision).Hash()
	replacement.Spec.NodeName = "node-0"
	replacement.Status.Phase = corev1.PodPending
	if err := store.Create(ctx, replacement); err != nil {
		t.Fatalf("create the failed replacement: %v", err)
	}

	f.takeThePassList(t)
	if err := f.pass.cachedPods.Client().Delete(ctx, replacement); err != nil {
		t.Fatalf("the update pass deletes the failed replacement: %v", err)
	}
	// The create pass lists right after the delete; the cache does not show
	// the delete yet.
	if _, err := f.pass.cachedPods.Pods(ctx, componentPodLister(f.pass.reconciler.Client, f.pass.ir)); err != nil {
		t.Fatalf("the create pass's list: %v", err)
	}

	f.publish(t)
	rows := f.decodedRows(t)
	if len(rows) != instances {
		t.Fatalf("the publication wrote %d rows, want %d", len(rows), instances)
	}
	if rows[0].Phase != v1beta1.OMENativeInstanceReady {
		t.Fatalf("the publication after the pass deleted the failed replacement published Instance 0 %s with %d pod(s), %d serving; want Ready: the pass's own delete is read as a pod that serves nothing", rows[0].Phase, rows[0].PodCount, rows[0].ServingPodCount)
	}
}
