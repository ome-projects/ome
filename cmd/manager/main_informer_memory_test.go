package main

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/watch"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulerpluginsv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferencereplica"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/managercache"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement"
	workloadgang "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/gang"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeapiserver"
)

var (
	updateInformerMemoryGoldens = flag.Bool("update", false, "re-record testdata/informer_memory_bytes.json and testdata/informer_memory_allocations.json from this run")
	informerMemoryFull          = flag.Bool("informer-memory-full", false, "measure the informer caches at every fleet size up to 20,000 pods and print the readings")
)

const (
	informerMemoryBytesGolden       = "testdata/informer_memory_bytes.json"
	informerMemoryAllocationsGolden = "testdata/informer_memory_allocations.json"
	// cacheSyncWait bounds a cache start; a sync that takes longer is a
	// failure, not a slow machine.
	cacheSyncWait = 2 * time.Minute
	// lockPods and lockReplicas are the counts the lock measures at: the
	// per-object cost is the slope between the smaller and the larger run,
	// so the cache's own fixed cost cancels. Every replica carries
	// lockReplicaRows rows, the largest reference replica, and the
	// Instance cost is the slope over replicas of that one size: the decoded
	// rows of a replica grow in steps with its row count, so a slope between
	// two row counts of one replica moves by up to a quarter with the counts
	// chosen. The fixed cost differs by tens of kilobytes from one sample to
	// the next; eight replicas of 4,000 rows cost megabytes, which keeps that
	// margin under one percent of the slope.
	lockPods          = 1000
	lockPodsLarge     = 2000
	lockReplicaRows   = 4000
	lockReplicas      = 2
	lockReplicasLarge = 10
	// lockPodsFleet is the most pods one replica puts on a cluster in a
	// large install. Beside the slope, the lock records what the manager's
	// cache holds per pod over lockPods and over lockPodsFleet pods beyond
	// the empty cache: the whole cost at a fleet size an operator can point
	// at, not only the cost of one more pod.
	lockPodsFleet = 4000
)

// fleetServer serves the kinds the measured cache opens: pods, replicas
// and services, and the Node and PodGroup kinds the field indexes open.
func fleetServer(tb testing.TB, initialEvents bool) *fakeapiserver.Server {
	tb.Helper()
	server := fakeapiserver.New(tb, scheme)
	server.InitialEvents = initialEvents
	server.Serve(&corev1.Pod{}, meta.RESTScopeNameNamespace)
	server.Serve(&v1beta1.InferenceReplica{}, meta.RESTScopeNameNamespace)
	server.Serve(&v1beta1.InferenceService{}, meta.RESTScopeNameNamespace)
	server.Serve(&corev1.Node{}, meta.RESTScopeNameRoot)
	server.Serve(&schedulerpluginsv1alpha1.PodGroup{}, meta.RESTScopeNameNamespace)
	return server
}

// podCacheVariant says what the measured cache does to a pod before it
// stores it.
type podCacheVariant int

const (
	// cachedAsTheManagerDoes is the manager's own option set.
	cachedAsTheManagerDoes podCacheVariant = iota
	// cachedWithManagedFieldsStrippedOnly removes managedFields and nothing
	// else: the transform every other kind gets.
	cachedWithManagedFieldsStrippedOnly
	// cachedAsServed stores the pod as the apiserver sends it.
	cachedAsServed
)

func (v podCacheVariant) String() string {
	switch v {
	case cachedWithManagedFieldsStrippedOnly:
		return "managed-fields-stripped-only"
	case cachedAsServed:
		return "as-served"
	default:
		return "as-the-manager-caches-it"
	}
}

// managerCache builds the cache the way cmd/manager builds it: the
// manager's cache options (managercache.Options: the pod selector and the pod
// transform) and the field indexes the controllers register at setup. The scheme and the
// mapper are what manager.New fills in from its own options and from
// discovery. A variant other than the manager's replaces the pod transform
// to measure what it saves.
func managerCache(tb testing.TB, server *fakeapiserver.Server, variant podCacheVariant) cache.Cache {
	tb.Helper()
	opts, err := managercache.Options()
	if err != nil {
		tb.Fatal(err)
	}
	opts.Scheme, opts.Mapper = scheme, server.Mapper()
	switch variant {
	case cachedWithManagedFieldsStrippedOnly:
		setPodTransform(&opts, cache.TransformStripManagedFields())
	case cachedAsServed:
		setPodTransform(&opts, func(obj any) (any, error) { return obj, nil })
	}
	c, err := cache.New(server.Config(), opts)
	if err != nil {
		tb.Fatal(err)
	}
	ctx := context.Background()
	for _, register := range []func(context.Context, client.FieldIndexer) error{
		query.RegisterOMENativePodIndex,
		workloadgang.RegisterPodGroupControllerUIDIndex,
		inferencereplica.RegisterReferenceIndexes,
		query.RegisterNodeUnschedulableIndex,
		inferenceservice.RegisterRuntimeIndexes,
		inferenceservice.RegisterAutoscalerPolicyIndex,
		inferenceservice.RegisterRolloutPolicyIndex,
		placement.RegisterPlacementEligibleIndex,
	} {
		if err := register(ctx, c); err != nil {
			tb.Fatal(err)
		}
	}
	return c
}

// setPodTransform replaces the transform of the pod entry of opts.
func setPodTransform(opts *cache.Options, transform toolscache.TransformFunc) {
	for key, byObject := range opts.ByObject {
		if _, isPod := key.(*corev1.Pod); isPod {
			byObject.Transform = transform
			opts.ByObject[key] = byObject
		}
	}
}

// runningCache is a started and synced cache with the informers the
// controllers watch open, and the pod informer's store for the test to
// count.
type runningCache struct {
	cache cache.Cache
	pods  toolscache.Indexer
	stop  func()
}

func startManagerCache(tb testing.TB, c cache.Cache) *runningCache {
	tb.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Start(ctx)
	}()
	stop := func() {
		cancel()
		<-done
	}
	var pods toolscache.Indexer
	for _, obj := range []client.Object{&corev1.Pod{}, &v1beta1.InferenceReplica{}, &v1beta1.InferenceService{}} {
		informer, err := c.GetInformer(ctx, obj)
		if err != nil {
			stop()
			tb.Fatal(err)
		}
		if _, isPod := obj.(*corev1.Pod); isPod {
			shared, ok := informer.(toolscache.SharedIndexInformer)
			if !ok {
				stop()
				tb.Fatalf("the pod informer is a %T, not a SharedIndexInformer", informer)
			}
			pods = shared.GetIndexer()
		}
	}
	syncCtx, syncCancel := context.WithTimeout(ctx, cacheSyncWait)
	defer syncCancel()
	if !c.WaitForCacheSync(syncCtx) {
		stop()
		tb.Fatalf("the cache did not sync within %s", cacheSyncWait)
	}
	return &runningCache{cache: c, pods: pods, stop: stop}
}

// waitForPods waits until the pod store holds count pods.
func (r *runningCache) waitForPods(tb testing.TB, count int) {
	tb.Helper()
	deadline := time.Now().Add(cacheSyncWait)
	for len(r.pods.ListKeys()) < count {
		if time.Now().After(deadline) {
			tb.Fatalf("the pod store holds %d pods after %s, want %d", len(r.pods.ListKeys()), cacheSyncWait, count)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// waitForPodVersion waits until the named pod is stored at resourceVersion.
func (r *runningCache) waitForPodVersion(tb testing.TB, key, resourceVersion string) {
	tb.Helper()
	deadline := time.Now().Add(cacheSyncWait)
	for {
		obj, exists, err := r.pods.GetByKey(key)
		if err == nil && exists && obj.(*corev1.Pod).ResourceVersion == resourceVersion {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("pod %s did not reach resourceVersion %s within %s", key, resourceVersion, cacheSyncWait)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// liveHeap is the heap after a full collection: the bytes of live objects
// (HeapAlloc) and the bytes of the spans holding them (HeapInuse). The
// smallest of liveHeapReadings readings is taken, since what another
// goroutine allocates between the collection and the reading can only add
// bytes to it.
type liveHeap struct {
	alloc, inuse uint64
}

// liveHeapReadings is how many collections one point reading takes.
const liveHeapReadings = 3

func readLiveHeap() liveHeap {
	var best liveHeap
	for i := 0; i < liveHeapReadings; i++ {
		runtime.GC()
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		if i == 0 || stats.HeapAlloc < best.alloc {
			best = liveHeap{alloc: stats.HeapAlloc, inuse: stats.HeapInuse}
		}
	}
	return best
}

// heapSettled is the margin within which two readings heapSettlePoll apart
// count as the same heap: the connections of the run before are then closed
// and their buffers collected. A heap still moving after heapSettlePolls
// readings is read as it stands.
const (
	heapSettled     = 1024
	heapSettlePoll  = 5 * time.Millisecond
	heapSettlePolls = 20
)

// readSettledLiveHeap reads the live heap once it stops moving, so a
// reading never includes what an earlier run is still releasing.
func readSettledLiveHeap() liveHeap {
	last := readLiveHeap()
	for i := 0; i < heapSettlePolls; i++ {
		time.Sleep(heapSettlePoll)
		next := readLiveHeap()
		if next.alloc+heapSettled > last.alloc && last.alloc+heapSettled > next.alloc {
			return next
		}
		last = next
	}
	return last
}

// A sample's first reading waits for the goroutines of what ran before (the
// cache and server of the sample before, the test before this one) to
// finish, so nothing they hold is released into the sample: the goroutine
// count must hold still for goroutineSettlePolls polls goroutineSettlePoll
// apart, and a count that keeps moving is waited on for goroutineSettleWait
// at most.
const (
	goroutineSettlePoll  = 10 * time.Millisecond
	goroutineSettlePolls = 5
	goroutineSettleWait  = 10 * time.Second
)

// waitForGoroutinesToSettle returns once the goroutine count has held still
// for goroutineSettlePolls polls, or after goroutineSettleWait.
func waitForGoroutinesToSettle() {
	deadline := time.Now().Add(goroutineSettleWait)
	last, still := runtime.NumGoroutine(), 0
	for still < goroutineSettlePolls && time.Now().Before(deadline) {
		time.Sleep(goroutineSettlePoll)
		next := runtime.NumGoroutine()
		if next == last {
			still++
		} else {
			still = 0
		}
		last = next
	}
}

func mallocs() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.Mallocs
}

// cacheReading is what one cache cost: the live heap it holds beyond the
// process before it started, the allocations from its start to its sync,
// and how long the sync took.
type cacheReading struct {
	objects     int
	heapAlloc   int64
	heapInuse   int64
	allocations int64
	sync        time.Duration
	// heapInuseTotal is the whole process's heap in use at the reading, the
	// nearest thing to the resident memory a manager holding the objects
	// would show.
	heapInuseTotal int64
	// heapSamples is the live heap of every sample behind heapAlloc, for the
	// log of a reading that fails.
	heapSamples []int64
}

// cacheSamples is how many times one shape is measured. The median live
// heap is the reading: a goroutine outside the measured cache can add bytes
// to a sample while it holds a buffer and take bytes from it when it lets
// one go, and the median stands while fewer than half the samples carry
// either. The smallest allocation count is the reading: an allocation
// elsewhere can only add to a sample.
const cacheSamples = 5

// warmUpOnce runs one small cache before the first measured one: the first
// cache in a process pays for the scheme's conversions and the codecs'
// tables, which no later cache pays again.
var warmUpOnce sync.Once

// measureCache loads objects into a fleet server, starts the manager's
// cache over them and reads what the cache holds once the server has
// dropped its own copy of the bytes, cacheSamples times. forget names the
// kinds the server drops before the reading.
func measureCache(tb testing.TB, variant podCacheVariant, initialEvents bool, build func() []client.Object, forget ...client.Object) cacheReading {
	tb.Helper()
	warmUpOnce.Do(func() {
		measureCacheOnce(tb, cachedAsTheManagerDoes, false, func() []client.Object { return podObjects(100, fleetPod) }, &corev1.Pod{})
	})
	samples := make([]cacheReading, cacheSamples)
	for i := range samples {
		samples[i] = measureCacheOnce(tb, variant, initialEvents, build, forget...)
	}
	return combineSamples(samples)
}

// combineSamples is the reading of one shape from its samples: the median
// of each heap figure and the smallest allocation count and sync time.
func combineSamples(samples []cacheReading) cacheReading {
	column := func(of func(cacheReading) int64) []int64 {
		values := make([]int64, len(samples))
		for i, sample := range samples {
			values[i] = of(sample)
		}
		return values
	}
	heaps := column(func(sample cacheReading) int64 { return sample.heapAlloc })
	syncs := make([]time.Duration, len(samples))
	for i, sample := range samples {
		syncs[i] = sample.sync
	}
	return cacheReading{
		objects:        samples[0].objects,
		heapAlloc:      median(heaps),
		heapInuse:      median(column(func(sample cacheReading) int64 { return sample.heapInuse })),
		heapInuseTotal: median(column(func(sample cacheReading) int64 { return sample.heapInuseTotal })),
		allocations:    slices.Min(column(func(sample cacheReading) int64 { return sample.allocations })),
		sync:           slices.Min(syncs),
		heapSamples:    heaps,
	}
}

// median is the middle value of values; for an even count, the mean of the
// two middle values.
func median(values []int64) int64 {
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	count := len(sorted)
	if count%2 == 1 {
		return sorted[count/2]
	}
	return (sorted[count/2-1] + sorted[count/2]) / 2
}

func measureCacheOnce(tb testing.TB, variant podCacheVariant, initialEvents bool, build func() []client.Object, forget ...client.Object) cacheReading {
	tb.Helper()
	server := fleetServer(tb, initialEvents)
	c := managerCache(tb, server, variant)
	waitForGoroutinesToSettle()
	before := readSettledLiveHeap()
	count := loadBuilt(server, build)
	start := time.Now()
	mallocsBefore := mallocs()
	running := startManagerCache(tb, c)
	mallocsAfter := mallocs()
	elapsed := time.Since(start)
	for _, kind := range forget {
		server.Forget(kind)
	}
	after := readSettledLiveHeap()
	running.stop()
	server.Close()
	return cacheReading{
		objects:     count,
		heapAlloc:   int64(after.alloc) - int64(before.alloc),
		heapInuse:   int64(after.inuse) - int64(before.inuse),
		allocations: int64(mallocsAfter - mallocsBefore),
		sync:        elapsed,

		heapInuseTotal: int64(after.inuse),
	}
}

// loadBuilt builds the objects, loads them into the server and returns how
// many there were. The objects go out of scope here, so only the server's
// encoded bytes outlive the call.
func loadBuilt(server *fakeapiserver.Server, build func() []client.Object) int {
	objects := build()
	server.Load(objects...)
	return len(objects)
}

func podObjects(count int, build func(int) *corev1.Pod) []client.Object {
	objects := make([]client.Object, count)
	for i := range objects {
		objects[i] = build(i)
	}
	return objects
}

// measurePods measures the cache over count pods listed at start.
func measurePods(tb testing.TB, count int, variant podCacheVariant, initialEvents bool, build func(int) *corev1.Pod) cacheReading {
	tb.Helper()
	return measureCache(tb, variant, initialEvents, func() []client.Object { return podObjects(count, build) }, &corev1.Pod{})
}

// measureReplicas measures the cache over count replicas, each with the
// given rows stored in the given form, beside their parent service.
func measureReplicas(tb testing.TB, count int, rows []v1beta1.OMENativeInstanceStatus, form irstatus.Encoding) cacheReading {
	tb.Helper()
	build := func() []client.Object {
		objects := make([]client.Object, 0, count+1)
		for index := 0; index < count; index++ {
			objects = append(objects, fleetReplicaAt(index, rows, form))
		}
		return append(objects, fleetInferenceService(len(rows)))
	}
	return measureCache(tb, cachedAsTheManagerDoes, false, build, &v1beta1.InferenceReplica{}, &v1beta1.InferenceService{})
}

// watchReading is the allocations one pod costs when it arrives as a watch
// event: added as a new pod, and modified in place.
type watchReading struct {
	added, modified int64
}

// measureWatchEvents starts the cache over count pods, then streams count
// more as ADDED events and the same count again as MODIFIED events,
// cacheSamples times; the smallest counts are the readings.
func measureWatchEvents(tb testing.TB, count int) watchReading {
	tb.Helper()
	var best watchReading
	for i := 0; i < cacheSamples; i++ {
		sample := measureWatchEventsOnce(tb, count)
		if i == 0 {
			best = sample
			continue
		}
		best.added = min(best.added, sample.added)
		best.modified = min(best.modified, sample.modified)
	}
	return best
}

func measureWatchEventsOnce(tb testing.TB, count int) watchReading {
	tb.Helper()
	server := fleetServer(tb, false)
	c := managerCache(tb, server, cachedAsTheManagerDoes)
	server.Load(podObjects(count, fleetPod)...)
	running := startManagerCache(tb, c)
	defer server.Close()
	defer running.stop()

	frames, _, _ := encodePodEvents(server, watch.Added, count, func(i int) *corev1.Pod { return fleetPod(count + i) })
	mallocsBefore := mallocs()
	server.Stream(&corev1.Pod{}, frames)
	running.waitForPods(tb, 2*count)
	added := int64(mallocs()-mallocsBefore) / int64(count)

	frames, lastKey, lastVersion := encodePodEvents(server, watch.Modified, count, func(i int) *corev1.Pod {
		pod := fleetPod(count + i)
		pod.Status.ContainerStatuses[0].RestartCount++
		return pod
	})
	mallocsBefore = mallocs()
	server.Stream(&corev1.Pod{}, frames)
	running.waitForPodVersion(tb, lastKey, lastVersion)
	modified := int64(mallocs()-mallocsBefore) / int64(count)
	// A server lives until the test's cleanup closes it, so the pods it was
	// loaded with are dropped here rather than carried in every later heap.
	server.Forget(&corev1.Pod{})
	return watchReading{added: added, modified: modified}
}

// encodePodEvents builds count pods and encodes them as events of the given
// type. It returns the frames and the store key and resourceVersion of the
// last pod, the one the store reaches last. The pods go out of scope here,
// so the measurement holds only the frames.
func encodePodEvents(server *fakeapiserver.Server, eventType watch.EventType, count int, build func(int) *corev1.Pod) (frames [][]byte, lastKey, lastVersion string) {
	pods := podObjects(count, build)
	frames = server.Encode(eventType, pods...)
	last := pods[count-1]
	return frames, last.GetNamespace() + "/" + last.GetName(), last.GetResourceVersion()
}

// perObject is the slope between two readings: what one more object costs.
func perObject(large, small, objects int64) int64 {
	return (large - small) / objects
}

// perThousand is the slope per 1,000 objects, for a cost of a few units
// per object that a whole number per object would round away.
func perThousand(large, small, objects int64) int64 {
	return (large - small) * 1000 / objects
}

// replicaShapes are the stored forms a replica at rest takes: the column
// form with every row equal, the column form with every row carrying its
// own readySince (the form the manager stores), and the plain list. A column
// form allocates nothing per Instance (one index set and one offset list
// serve every row), a reading too close to zero for a percent tolerance:
// the uniform form is not measured, and the form with times locks its
// bytes, which do grow per Instance, and reports its allocations.
var replicaShapes = []struct {
	name            string
	rows            func(int) []v1beta1.OMENativeInstanceStatus
	form            irstatus.Encoding
	lockBytes       bool
	lockAllocations bool
}{
	{name: "columns-uniform", rows: irstatustest.UniformRows, form: irstatus.EncodingColumnarV2},
	{name: "columns-ready-since", rows: irstatustest.ReadySinceRows, form: irstatus.EncodingColumnarV2, lockBytes: true},
	{name: "plain-list", rows: irstatustest.UniformRows, form: irstatus.EncodingDenseV1, lockBytes: true, lockAllocations: true},
}

// TestInformerMemoryStaysWithinBudget holds the bytes one pod and one
// Instance cost in the manager's informer caches, and the allocations
// admitting one costs, to the recorded goldens. Both are the same on every
// machine with the same Go release: the per-object cost is the slope
// between two fleet sizes, so the cache's fixed cost cancels. For the pod
// as the manager caches it the lock also holds the cost per pod over
// lockPods and over lockPodsFleet pods, each beyond the empty cache. Each
// reading is the median of cacheSamples samples, so heap another goroutine
// holds or lets go during one sample moves no reading.
func TestInformerMemoryStaysWithinBudget(t *testing.T) {
	bytesGolden := allocbudget.Open(t, informerMemoryBytesGolden, *updateInformerMemoryGoldens)
	allocationsGolden := allocbudget.Open(t, informerMemoryAllocationsGolden, *updateInformerMemoryGoldens)
	defer bytesGolden.Close()
	defer allocationsGolden.Close()

	empty := measureCache(t, cachedAsTheManagerDoes, false, func() []client.Object { return nil })
	for _, variant := range []podCacheVariant{cachedAsTheManagerDoes, cachedWithManagedFieldsStrippedOnly, cachedAsServed} {
		small := measurePods(t, lockPods, variant, false, fleetPod)
		large := measurePods(t, lockPodsLarge, variant, false, fleetPod)
		pods := int64(lockPodsLarge - lockPods)
		t.Logf("%s: live heap samples %v B at %d pods, %v B at %d pods", variant, small.heapSamples, lockPods, large.heapSamples, lockPodsLarge)
		bytesGolden.CheckCount("pod/live-heap-bytes-per-pod/"+variant.String(), perObject(large.heapAlloc, small.heapAlloc, pods))
		if variant == cachedAsTheManagerDoes {
			allocationsGolden.CheckCount("pod/allocations-per-pod/list", perObject(large.allocations, small.allocations, pods))
			lockCachedPodAtFleetSizes(t, bytesGolden, empty, small)
		}
	}
	events := measureWatchEvents(t, lockPods)
	allocationsGolden.CheckCount("pod/allocations-per-pod/watch-added", events.added)
	allocationsGolden.CheckCount("pod/allocations-per-pod/watch-modified", events.modified)

	for _, shape := range replicaShapes {
		if !shape.lockBytes && !shape.lockAllocations {
			continue
		}
		rows := shape.rows(lockReplicaRows)
		small := measureReplicas(t, lockReplicas, rows, shape.form)
		large := measureReplicas(t, lockReplicasLarge, rows, shape.form)
		instances := int64((lockReplicasLarge - lockReplicas) * lockReplicaRows)
		t.Logf("%s: live heap samples %v B with %d replicas, %v B with %d replicas of %d rows", shape.name, small.heapSamples, lockReplicas, large.heapSamples, lockReplicasLarge, lockReplicaRows)
		if shape.lockBytes {
			bytesGolden.CheckCount("instance/live-heap-bytes-per-1000-instances/"+shape.name, perThousand(large.heapAlloc, small.heapAlloc, instances))
		}
		allocations := perThousand(large.allocations, small.allocations, instances)
		if shape.lockAllocations {
			allocationsGolden.CheckCount("instance/allocations-per-1000-instances/"+shape.name, allocations)
		} else {
			t.Logf("%s: %d allocations per 1000 Instances, reported and not locked", shape.name, allocations)
		}
	}
}

// lockCachedPodAtFleetSizes holds what the manager's cache holds per pod
// over lockPods and over lockPodsFleet pods: the live heap of the cache
// over that many pods beyond the empty cache, divided by the count. The
// slope readings are the cost of one more pod; these are the whole cost at
// a reference fleet size. small is the reading over lockPods the
// slope already took.
func lockCachedPodAtFleetSizes(t *testing.T, golden *allocbudget.File, empty, small cacheReading) {
	t.Helper()
	fleet := measurePods(t, lockPodsFleet, cachedAsTheManagerDoes, false, fleetPod)
	mb := func(bytes int64) float64 { return float64(bytes) / 1e6 }
	t.Logf("%s: the empty cache holds %.2f MB (samples %v B); the cache holds %.2f MB more over %d pods and %.2f MB more over %d pods (samples %v B)",
		cachedAsTheManagerDoes, mb(empty.heapAlloc), empty.heapSamples, mb(small.heapAlloc-empty.heapAlloc), lockPods, mb(fleet.heapAlloc-empty.heapAlloc), lockPodsFleet, fleet.heapSamples)
	for _, size := range []struct {
		pods    int
		reading cacheReading
	}{{lockPods, small}, {lockPodsFleet, fleet}} {
		golden.CheckCount(fmt.Sprintf("pod/live-heap-bytes-per-pod/%s/over-%d-pods", cachedAsTheManagerDoes, size.pods), perObject(size.reading.heapAlloc, empty.heapAlloc, int64(size.pods)))
	}
}

// TestInformerMemoryReadings prints the readings behind the budget at every
// fleet size, with the parts of a pod that carry the bytes. It runs with
// -informer-memory-full.
func TestInformerMemoryReadings(t *testing.T) {
	if !*informerMemoryFull {
		t.Skip("pass -informer-memory-full to measure every fleet size")
	}
	mb := func(bytes int64) string { return fmt.Sprintf("%.2f MB", float64(bytes)/1e6) }
	perThousandPods := func(r cacheReading, field int64) string {
		return mb(field * 1000 / int64(r.objects))
	}
	empty := measureCache(t, cachedAsTheManagerDoes, false, func() []client.Object { return nil })
	t.Logf("empty cache, five informers: live heap %s (in use %s), %d allocations, sync %s", mb(empty.heapAlloc), mb(empty.heapInuse), empty.allocations, empty.sync)

	t.Logf("pods | cached | protocol | live heap per 1,000 pods | heap in use, whole process | allocations per pod | sync")
	for _, run := range []struct {
		count         int
		variant       podCacheVariant
		initialEvents bool
	}{
		{1000, cachedAsTheManagerDoes, false}, {2000, cachedAsTheManagerDoes, false}, {5000, cachedAsTheManagerDoes, false}, {10000, cachedAsTheManagerDoes, false}, {20000, cachedAsTheManagerDoes, false},
		{10000, cachedWithManagedFieldsStrippedOnly, false}, {20000, cachedWithManagedFieldsStrippedOnly, false},
		{10000, cachedAsServed, false}, {20000, cachedAsServed, false},
		{10000, cachedAsTheManagerDoes, true}, {20000, cachedAsTheManagerDoes, true},
	} {
		r := measurePods(t, run.count, run.variant, run.initialEvents, fleetPod)
		protocol := "list"
		if run.initialEvents {
			protocol = "watch-list"
		}
		t.Logf("%6d | %s | %s | %s | %s | %d | %s", run.count, run.variant, protocol, perThousandPods(r, r.heapAlloc-empty.heapAlloc), mb(r.heapInuseTotal), r.allocations/int64(run.count), r.sync)
	}

	t.Logf("what carries a cached pod's bytes (10,000 pods, as the manager caches them): live heap per 1,000 pods without each part")
	whole := measurePods(t, 10000, cachedAsTheManagerDoes, false, fleetPod)
	t.Logf("whole pod: %s", perThousandPods(whole, whole.heapAlloc-empty.heapAlloc))
	for _, part := range []struct {
		name string
		trim func(*corev1.Pod)
	}{
		{"annotations", func(p *corev1.Pod) { p.Annotations = nil }},
		{"container env", func(p *corev1.Pod) { p.Spec.Containers[0].Env = nil }},
		{"container args", func(p *corev1.Pod) { p.Spec.Containers[0].Args = nil }},
		{"probes", func(p *corev1.Pod) {
			p.Spec.Containers[0].StartupProbe, p.Spec.Containers[0].ReadinessProbe, p.Spec.Containers[0].LivenessProbe = nil, nil, nil
		}},
		{"resources (spec and status)", func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources = corev1.ResourceRequirements{}
			p.Status.ContainerStatuses[0].AllocatedResources, p.Status.ContainerStatuses[0].Resources = nil, nil
		}},
		{"volumes and mounts", func(p *corev1.Pod) {
			p.Spec.Volumes, p.Spec.Containers[0].VolumeMounts, p.Status.ContainerStatuses[0].VolumeMounts = nil, nil, nil
		}},
		{"tolerations", func(p *corev1.Pod) { p.Spec.Tolerations = nil }},
		{"status conditions", func(p *corev1.Pod) { p.Status.Conditions = nil }},
		{"container statuses", func(p *corev1.Pod) { p.Status.ContainerStatuses = nil }},
	} {
		trimmed := measurePods(t, 10000, cachedAsTheManagerDoes, false, func(i int) *corev1.Pod {
			pod := fleetPod(i)
			part.trim(pod)
			return pod
		})
		t.Logf("without %s: %s (the part costs %s per 1,000 pods)", part.name, perThousandPods(trimmed, trimmed.heapAlloc-empty.heapAlloc), perThousandPods(trimmed, whole.heapAlloc-trimmed.heapAlloc))
	}

	events := measureWatchEvents(t, 10000)
	t.Logf("watch events at 10,000 pods: %d allocations per added pod, %d per modified pod", events.added, events.modified)

	t.Logf("replica form | live heap per 1,000 Instances (slope over %d to %d replicas of %d rows) | allocations per 1,000 Instances | live heap of one replica", lockReplicas, lockReplicasLarge, lockReplicaRows)
	for _, shape := range replicaShapes {
		rows := shape.rows(lockReplicaRows)
		small := measureReplicas(t, lockReplicas, rows, shape.form)
		large := measureReplicas(t, lockReplicasLarge, rows, shape.form)
		instances := int64((lockReplicasLarge - lockReplicas) * lockReplicaRows)
		t.Logf("%s | %s | %d | %s", shape.name, mb(perThousand(large.heapAlloc, small.heapAlloc, instances)), perThousand(large.allocations, small.allocations, instances), mb((large.heapAlloc-small.heapAlloc)/int64(lockReplicasLarge-lockReplicas)))
	}
	service := measureCache(t, cachedAsTheManagerDoes, false, func() []client.Object { return []client.Object{fleetInferenceService(1000)} }, &v1beta1.InferenceService{})
	t.Logf("one InferenceService: live heap %s beyond the empty cache, %d allocations", mb(service.heapAlloc-empty.heapAlloc), service.allocations-empty.allocations)
}
