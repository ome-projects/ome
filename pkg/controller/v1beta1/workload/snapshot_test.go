package workload_test

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
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
