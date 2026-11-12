package coordination

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

// updateEndpointReadingAllocations re-records the endpoint reading's
// golden; named apart from -update, which the gate stack's golden in this
// package registers.
var updateEndpointReadingAllocations = flag.Bool("update-endpoint-reading", false, "re-record testdata/endpoint_reading_allocations.json from this run")

// endpointReadingFixture is a service at rest: n single-pod engine
// Instances serving on one revision, every pod a Ready endpoint of the
// revision's routed Service, and a rotation that reads that Service's
// slices once, as a pass's does.
type endpointReadingFixture struct {
	isvc       *v1beta1.InferenceService
	rotation   *drain.Batcher
	components []v1beta1.ComponentType
	pods       map[v1beta1.ComponentType][]*corev1.Pod
}

func newEndpointReadingFixture(n int) *endpointReadingFixture {
	isvc := testOMENativeISVC()
	engine := v1beta1.EngineComponent
	pods := make([]*corev1.Pod, 0, n)
	for idx := 0; idx < n; idx++ {
		pods = append(pods, instancePod(isvc, engine, "v1", idx, 0))
	}
	objs := []runtime.Object{listedEndpoints(isvc, engine, "v1", pods...)}
	for _, pod := range pods {
		objs = append(objs, pod)
	}
	return &endpointReadingFixture{
		isvc:       isvc,
		rotation:   drain.NewBatcher(testClient(objs...), isvc.Namespace),
		components: []v1beta1.ComponentType{engine},
		pods:       map[v1beta1.ComponentType][]*corev1.Pod{engine: pods},
	}
}

// read runs the endpoint reading and holds its answer: every pod serves
// and is listed, so the revision carries them all and nothing waits.
func (f *endpointReadingFixture) read(t testing.TB) {
	t.Helper()
	listed, waits, err := servingPodsListedByRevision(context.Background(), f.rotation, f.isvc, f.components, f.pods)
	if err != nil {
		t.Fatal(err)
	}
	engine := v1beta1.EngineComponent
	if want := int32(len(f.pods[engine])); listed[engine]["v1"] != want || len(waits) != 0 {
		t.Fatalf("the reading counts %d of %d serving pods listed with %d waits; want all listed and no wait", listed[engine]["v1"], want, len(waits))
	}
}

// TestEndpointReadingStaysWithinBudget holds the allocations of the
// endpoint reading over a service at rest to the golden, at 1,000 and
// 4,000 Instances serving on one revision with the revision's slices
// already read. Beside the golden, the reading allocates less than once
// per pod at either size: what it builds per revision, the routed
// Service's name among it, it builds once.
func TestEndpointReadingStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "endpoint_reading_allocations.json"), *updateEndpointReadingAllocations)
	defer golden.Close()
	for _, n := range []int{1000, 4000} {
		f := newEndpointReadingFixture(n)
		count := allocbudget.Measure(func() { f.read(t) })
		golden.CheckCount(fmt.Sprintf("%dx1/one-revision", n), count)
		if count >= int64(n) {
			t.Errorf("the endpoint reading over %d serving pods allocates %d times, at least once per pod", n, count)
		}
	}
}

// BenchmarkEndpointReading prints the wall time of the reading the test
// above locks by allocations.
func BenchmarkEndpointReading(b *testing.B) {
	for _, n := range []int{1000, 4000} {
		b.Run(fmt.Sprintf("%dx1/one-revision", n), func(b *testing.B) {
			f := newEndpointReadingFixture(n)
			f.read(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				f.read(b)
			}
		})
	}
}
