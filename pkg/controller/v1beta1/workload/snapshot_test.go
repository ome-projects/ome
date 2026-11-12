package workload_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// countingReader wraps a client.Reader and counts List calls, so tests can
// assert the snapshot Lists each source at most once (memoization).
type countingReader struct {
	client.Client
	lists   int
	listErr error
}

func (c *countingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.lists++
	if c.listErr != nil {
		return c.listErr
	}
	return c.Client.List(ctx, list, opts...)
}

func snapshotScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	return s
}

// TestObservedSnapshot_LivePodsMemoized: the live source is Listed at most
// once per snapshot no matter how many passes call LivePods — the property
// that keeps the read count identical to the per-pass helpers (and off the
// apiserver hot path on large clusters).
func TestObservedSnapshot_LivePodsMemoized(t *testing.T) {
	fc := fake.NewClientBuilder().WithScheme(snapshotScheme(t)).Build()
	cr := &countingReader{Client: fc}
	deps := types.Deps{Client: fc, APIReader: cr}
	input := types.ReconcileInput{Key: types.Key{Namespace: "ns", Component: types.ComponentEngine, OwnerName: "own"}}

	snap := workload.NewObservedSnapshot(deps, input, types.ComponentEngine, nil)
	for i := 0; i < 3; i++ {
		if _, err := snap.LivePods(context.Background()); err != nil {
			t.Fatalf("LivePods: %v", err)
		}
	}
	if cr.lists != 1 {
		t.Errorf("live source Listed %d times across 3 LivePods calls, want 1 (memoized)", cr.lists)
	}

	// A fresh snapshot re-reads (memoization is per-reconcile, not global).
	snap2 := workload.NewObservedSnapshot(deps, input, types.ComponentEngine, nil)
	if _, err := snap2.LivePods(context.Background()); err != nil {
		t.Fatalf("LivePods (snap2): %v", err)
	}
	if cr.lists != 2 {
		t.Errorf("fresh snapshot must re-List: got %d want 2", cr.lists)
	}
}

func TestObservedSnapshotKeepsCachedAndLiveObservationsSeparate(t *testing.T) {
	scheme := snapshotScheme(t)
	cachedPod := snapshotPod("cached-pod", "0")
	livePod := snapshotPod("live-pod", "1")
	cachedClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cachedPod).
		WithIndex(&corev1.Pod{}, query.OMENativePodIndexField, query.OMENativePodIndexExtractor).
		Build()
	liveClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(livePod).Build()
	cached := &countingReader{Client: cachedClient}
	live := &countingReader{Client: liveClient}
	input := types.ReconcileInput{
		Key: types.Key{Namespace: "ns", Component: types.ComponentEngine, OwnerName: "own"},
		ObservedState: types.WorkloadObservedState{InstanceStatuses: []types.InstanceStatus{
			{Index: 0},
			{Index: 1},
		}},
	}
	snapshot := workload.NewObservedSnapshot(
		types.Deps{Client: cached, APIReader: live},
		input,
		types.ComponentEngine,
		input.ObservedState.InstanceStatuses,
	)

	for i := 0; i < 3; i++ {
		cachedObservation, err := snapshot.CachedObservation(context.Background())
		if err != nil {
			t.Fatalf("CachedObservation: %v", err)
		}
		if cachedObservation.PodSource() != workload.PodObservationSourceCache ||
			cachedObservation.PodScope() != workload.PodObservationScopeSelector {
			t.Fatalf("cached provenance = source %v scope %v", cachedObservation.PodSource(), cachedObservation.PodScope())
		}
		cachedZero, _ := cachedObservation.CurrentCounters(0)
		cachedOne, _ := cachedObservation.CurrentCounters(1)
		if cachedZero.PodCount != 1 || cachedOne.PodCount != 0 {
			t.Fatalf("cached observation used the wrong Pod set")
		}

		liveObservation, err := snapshot.LiveObservation(context.Background())
		if err != nil {
			t.Fatalf("LiveObservation: %v", err)
		}
		if liveObservation.PodSource() != workload.PodObservationSourceAPIReader ||
			liveObservation.PodScope() != workload.PodObservationScopeSelector {
			t.Fatalf("live provenance = source %v scope %v", liveObservation.PodSource(), liveObservation.PodScope())
		}
		liveZero, _ := liveObservation.CurrentCounters(0)
		liveOne, _ := liveObservation.CurrentCounters(1)
		if liveZero.PodCount != 0 || liveOne.PodCount != 1 {
			t.Fatalf("live observation used the wrong Pod set")
		}
	}
	if cached.lists != 1 || live.lists != 1 {
		t.Fatalf("Pod Lists = cached %d live %d, want 1 each", cached.lists, live.lists)
	}
}

func TestObservedSnapshotAuthoritativeEmptyObservationDoesNotList(t *testing.T) {
	fc := fake.NewClientBuilder().WithScheme(snapshotScheme(t)).WithObjects(snapshotPod("live-pod", "0")).Build()
	live := &countingReader{Client: fc}
	owner := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "owner-uid"}}
	input := types.ReconcileInput{
		OwnerObject:   owner,
		Key:           types.Key{Namespace: "ns", Component: types.ComponentEngine, OwnerName: "own"},
		ObservedState: types.WorkloadObservedState{InstanceStatuses: []types.InstanceStatus{{Index: 0}}},
		AuthoritativePods: &types.ComponentPodSnapshot{
			OwnerUID:   owner.UID,
			Pods:       []*corev1.Pod{},
			ByInstance: map[int32][]*corev1.Pod{},
		},
	}
	snapshot := workload.NewObservedSnapshot(
		types.Deps{Client: fc, APIReader: live},
		input,
		types.ComponentEngine,
		input.ObservedState.InstanceStatuses,
	)

	observation, err := snapshot.LiveObservation(context.Background())
	if err != nil {
		t.Fatalf("LiveObservation: %v", err)
	}
	if live.lists != 0 {
		t.Fatalf("authoritative empty observation performed %d Pod Lists", live.lists)
	}
	current, _ := observation.CurrentCounters(0)
	if observation.PodScope() != workload.PodObservationScopeOwnerUID || current.PodCount != 0 {
		t.Fatalf("authoritative empty observation = scope %v current %+v", observation.PodScope(), current)
	}
}

func TestObservedSnapshotReportsUnprovenAuthoritativeScopeAsUnknown(t *testing.T) {
	fc := fake.NewClientBuilder().WithScheme(snapshotScheme(t)).Build()
	owner := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "owner-uid"}}
	tests := []struct {
		name     string
		ownerUID k8stypes.UID
	}{
		{name: "missing owner UID"},
		{name: "different owner UID", ownerUID: "other-owner"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := types.ReconcileInput{
				OwnerObject: owner,
				Key:         types.Key{Namespace: "ns", Component: types.ComponentEngine, OwnerName: "own"},
				AuthoritativePods: &types.ComponentPodSnapshot{
					OwnerUID: test.ownerUID,
					ByInstance: map[int32][]*corev1.Pod{
						0: {snapshotPod("preloaded", "0")},
					},
				},
			}
			snapshot := workload.NewObservedSnapshot(types.Deps{Client: fc}, input, types.ComponentEngine, nil)
			observation, err := snapshot.LiveObservation(context.Background())
			if err != nil {
				t.Fatalf("LiveObservation: %v", err)
			}
			current, _ := observation.CurrentCounters(0)
			if observation.PodScope() != workload.PodObservationScopeUnknown || current.PodCount != 1 {
				t.Fatalf("scope=%v current=%+v", observation.PodScope(), current)
			}
		})
	}
}

func TestObservedSnapshotTagsLiveFallbackAsCache(t *testing.T) {
	fc := fake.NewClientBuilder().WithScheme(snapshotScheme(t)).WithObjects(snapshotPod("cached-pod", "0")).Build()
	cached := &countingReader{Client: fc}
	input := types.ReconcileInput{
		Key: types.Key{Namespace: "ns", Component: types.ComponentEngine, OwnerName: "own"},
	}
	snapshot := workload.NewObservedSnapshot(types.Deps{Client: cached}, input, types.ComponentEngine, nil)

	observation, err := snapshot.LiveObservation(context.Background())
	if err != nil {
		t.Fatalf("LiveObservation: %v", err)
	}
	if observation.PodSource() != workload.PodObservationSourceCache ||
		observation.PodScope() != workload.PodObservationScopeSelector {
		t.Fatalf("fallback provenance = source %v scope %v", observation.PodSource(), observation.PodScope())
	}
	if cached.lists != 1 {
		t.Fatalf("fallback Lists = %d, want 1", cached.lists)
	}
}

func TestObservedSnapshotMemoizesReadErrors(t *testing.T) {
	wantErr := errors.New("pod list failed")
	fc := fake.NewClientBuilder().WithScheme(snapshotScheme(t)).Build()
	cached := &countingReader{Client: fc, listErr: wantErr}
	live := &countingReader{Client: fc, listErr: wantErr}
	input := types.ReconcileInput{Key: types.Key{Namespace: "ns", Component: types.ComponentEngine, OwnerName: "own"}}
	snapshot := workload.NewObservedSnapshot(types.Deps{Client: cached, APIReader: live}, input, types.ComponentEngine, nil)

	for i := 0; i < 3; i++ {
		if _, err := snapshot.CachedObservation(context.Background()); !errors.Is(err, wantErr) {
			t.Fatalf("CachedObservation error = %v, want %v", err, wantErr)
		}
		if _, err := snapshot.LiveObservation(context.Background()); !errors.Is(err, wantErr) {
			t.Fatalf("LiveObservation error = %v, want %v", err, wantErr)
		}
	}
	if cached.lists != 1 || live.lists != 1 {
		t.Fatalf("failed Pod Lists = cached %d live %d, want 1 each", cached.lists, live.lists)
	}
}

func snapshotPod(name, index string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: "ns",
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: "own",
			constants.OMEComponentLabel:           string(types.ComponentEngine),
			query.LabelManagedBy:                  query.ManagedByOMENative,
			query.LabelInstanceIdx:                index,
		},
	}}
}

// revisionGetCounter counts the ControllerRevision Gets a reader serves,
// so a test can assert how many times the snapshot read a revision.
type revisionGetCounter struct {
	client.Client
	gets int
}

func (c *revisionGetCounter) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*appsv1.ControllerRevision); ok {
		c.gets++
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// TestObservedSnapshot_RunningRevisionPodSpecMemoizedByRevision: the
// running revision an Instance names is read once per revision name,
// however many Instances share it and however many times Plan asks,
// through the revision-bookkeeping role (the live reader); a row that
// names no revision or a revision that is gone is no baseline and no
// error.
func TestObservedSnapshot_RunningRevisionPodSpecMemoizedByRevision(t *testing.T) {
	scheme := snapshotScheme(t)
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	spec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}}
	raw, err := json.Marshal(revision.DataPayload{PodSpec: spec})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	shared := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "svc-engine-shared", Namespace: "ns"}}
	shared.Data.Raw = raw
	// The revision lives only behind the live reader: the cached client
	// must not be the one answering.
	live := &revisionGetCounter{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(shared).Build()}
	cached := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := types.Deps{Client: cached, APIReader: live}
	input := types.ReconcileInput{
		Key: types.Key{Namespace: "ns", Component: types.ComponentEngine, OwnerName: "svc"},
		ObservedState: types.WorkloadObservedState{InstanceStatuses: []types.InstanceStatus{
			{Index: 0, RunningRevision: "svc-engine-shared"},
			{Index: 1, RunningRevision: "svc-engine-shared"},
			{Index: 2, RunningRevision: "svc-engine-gone"},
			{Index: 3},
		}},
	}
	snap := workload.NewObservedSnapshot(deps, input, types.ComponentEngine, input.ObservedState.InstanceStatuses)

	for _, idx := range []int32{0, 1, 0, 1} {
		got, err := snap.RunningRevisionPodSpec(context.Background(), idx)
		if err != nil {
			t.Fatalf("RunningRevisionPodSpec(%d): %v", idx, err)
		}
		if got == nil || got.Containers[0].Image != "test:v1" {
			t.Errorf("RunningRevisionPodSpec(%d) = %+v, want the recorded spec", idx, got)
		}
	}
	if live.gets != 1 {
		t.Errorf("shared revision read %d times across two Instances and repeated calls, want 1 (memoized)", live.gets)
	}
	for _, idx := range []int32{2, 3} {
		got, err := snap.RunningRevisionPodSpec(context.Background(), idx)
		if err != nil || got != nil {
			t.Errorf("RunningRevisionPodSpec(%d) = (%+v, %v), want no baseline and no error", idx, got, err)
		}
	}
	if live.gets != 2 {
		t.Errorf("a gone revision costs one read and a row without a revision none: got %d reads, want 2", live.gets)
	}
}

// snapshotBoundPod is a Running pod of Instance index bound to node.
func snapshotBoundPod(name, index, node string) *corev1.Pod {
	pod := snapshotPod(name, index)
	pod.UID = k8stypes.UID("uid-" + name)
	pod.Spec.NodeName = node
	pod.Status.Phase = corev1.PodRunning
	return pod
}

// TestObservedSnapshot_PodBoundToAMissingNodeIsLeftOutOfBothViews: a pod
// whose Node object no longer exists runs nowhere, so neither the cached
// nor the live view counts it; a pod on a Node the cluster holds, and an
// unbound pod, stay.
func TestObservedSnapshot_PodBoundToAMissingNodeIsLeftOutOfBothViews(t *testing.T) {
	scheme := snapshotScheme(t)
	onHeld := snapshotBoundPod("on-held", "0", "node-a")
	onMissing := snapshotBoundPod("on-missing", "1", "node-b")
	unbound := snapshotBoundPod("unbound", "2", "")
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(onHeld, onMissing, unbound, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}).
		WithIndex(&corev1.Pod{}, query.OMENativePodIndexField, query.OMENativePodIndexExtractor).
		Build()
	input := types.ReconcileInput{Key: types.Key{Namespace: "ns", Component: types.ComponentEngine, OwnerName: "own"}}

	snapshot := workload.NewObservedSnapshot(types.Deps{Client: c, APIReader: c}, input, types.ComponentEngine, nil)
	for name, view := range map[string]func(context.Context) (map[int32][]*corev1.Pod, error){
		"cached": snapshot.CachedPods, "live": snapshot.LivePods,
	} {
		pods, err := view(context.Background())
		if err != nil {
			t.Fatalf("%s view: %v", name, err)
		}
		if len(pods[0]) != 1 || len(pods[2]) != 1 {
			t.Fatalf("%s view lost a pod it must keep: instance 0 has %d, instance 2 has %d", name, len(pods[0]), len(pods[2]))
		}
		if len(pods[1]) != 0 {
			t.Fatalf("%s view still counts pod %s, bound to a Node the cluster does not hold", name, pods[1][0].Name)
		}
	}
}

// TestObservedSnapshot_PodsLeftOutForAMissingNodeAreKeptAsideInNameOrder:
// the pods a view left out are what the pass removes, named once each
// across both views and in a fixed order.
func TestObservedSnapshot_PodsLeftOutForAMissingNodeAreKeptAsideInNameOrder(t *testing.T) {
	scheme := snapshotScheme(t)
	second := snapshotBoundPod("b-on-missing", "1", "node-b")
	first := snapshotBoundPod("a-on-missing", "2", "node-c")
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(snapshotBoundPod("on-held", "0", "node-a"), second, first, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}).
		WithIndex(&corev1.Pod{}, query.OMENativePodIndexField, query.OMENativePodIndexExtractor).
		Build()
	input := types.ReconcileInput{Key: types.Key{Namespace: "ns", Component: types.ComponentEngine, OwnerName: "own"}}

	snapshot := workload.NewObservedSnapshot(types.Deps{Client: c, APIReader: c}, input, types.ComponentEngine, nil)
	if got := snapshot.GonePods(); len(got) != 0 {
		t.Fatalf("before any view is taken nothing is kept aside, got %d", len(got))
	}
	if _, err := snapshot.CachedPods(context.Background()); err != nil {
		t.Fatalf("cached view: %v", err)
	}
	if _, err := snapshot.LivePods(context.Background()); err != nil {
		t.Fatalf("live view: %v", err)
	}
	gone := snapshot.GonePods()
	if len(gone) != 2 || gone[0].Name != "a-on-missing" || gone[1].Name != "b-on-missing" {
		names := make([]string, 0, len(gone))
		for _, pod := range gone {
			names = append(names, pod.Name)
		}
		t.Fatalf("kept aside = %v; want the two pods on missing nodes once each, in name order", names)
	}
}

// TestObservedSnapshot_PreloadedPodsAreReadTheSameWay: the live view over
// the pods the adapter preloaded applies the same Node reading and keeps
// the adapter's buckets when nothing is gone.
func TestObservedSnapshot_PreloadedPodsAreReadTheSameWay(t *testing.T) {
	scheme := snapshotScheme(t)
	onHeld := snapshotBoundPod("on-held", "0", "node-a")
	onMissing := snapshotBoundPod("on-missing", "1", "node-b")
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}).Build()
	input := types.ReconcileInput{
		Key: types.Key{Namespace: "ns", Component: types.ComponentEngine, OwnerName: "own"},
		AuthoritativePods: &types.ComponentPodSnapshot{
			Pods:       []*corev1.Pod{onHeld, onMissing},
			ByInstance: query.BucketPodsByInstanceIdx([]*corev1.Pod{onHeld, onMissing}),
		},
	}
	snapshot := workload.NewObservedSnapshot(types.Deps{Client: c, APIReader: c}, input, types.ComponentEngine, nil)
	pods, err := snapshot.LivePods(context.Background())
	if err != nil {
		t.Fatalf("live view: %v", err)
	}
	if len(pods[0]) != 1 || len(pods[1]) != 0 {
		t.Fatalf("preloaded view: instance 0 has %d pods, instance 1 has %d; want 1 and 0", len(pods[0]), len(pods[1]))
	}
	if gone := snapshot.GonePods(); len(gone) != 1 || gone[0] != onMissing {
		t.Fatalf("kept aside %d pods, want the preloaded pod on the missing node", len(gone))
	}
}

// TestObservedSnapshot_RevisionNumberMemoizedByName: a revision's number
// is read once per name through the revision-bookkeeping role (the live
// reader), however many times Plan asks; a revision that is gone reads as
// not found with no error, at one read; a snapshot built without a reader
// knows no numbers and reads nothing.
func TestObservedSnapshot_RevisionNumberMemoizedByName(t *testing.T) {
	scheme := snapshotScheme(t)
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	second := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "svc-engine-second", Namespace: "ns"}, Revision: 2}
	live := &revisionGetCounter{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(second).Build()}
	cached := fake.NewClientBuilder().WithScheme(scheme).Build()
	input := types.ReconcileInput{Key: types.Key{Namespace: "ns", Component: types.ComponentEngine, OwnerName: "svc"}}
	snap := workload.NewObservedSnapshot(types.Deps{Client: cached, APIReader: live}, input, types.ComponentEngine, nil)

	for range 3 {
		number, found, err := snap.RevisionNumber(context.Background(), "svc-engine-second")
		if err != nil || !found || number != 2 {
			t.Fatalf("RevisionNumber(second) = (%d, %v, %v), want (2, true, nil)", number, found, err)
		}
	}
	if live.gets != 1 {
		t.Errorf("one revision read %d times across repeated calls, want 1 (memoized)", live.gets)
	}
	for range 2 {
		number, found, err := snap.RevisionNumber(context.Background(), "svc-engine-gone")
		if err != nil || found || number != 0 {
			t.Fatalf("RevisionNumber(gone) = (%d, %v, %v), want (0, false, nil)", number, found, err)
		}
	}
	if live.gets != 2 {
		t.Errorf("a gone revision costs one read: got %d reads, want 2", live.gets)
	}

	readerless := workload.NewObservedSnapshot(types.Deps{}, input, types.ComponentEngine, nil)
	if number, found, err := readerless.RevisionNumber(context.Background(), "svc-engine-second"); err != nil || found || number != 0 {
		t.Errorf("RevisionNumber without a reader = (%d, %v, %v), want (0, false, nil)", number, found, err)
	}
}
