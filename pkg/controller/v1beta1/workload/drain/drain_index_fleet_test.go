package drain

import (
	"context"
	"flag"
	"path/filepath"
	"strconv"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// updateEndpointIndexAllocations re-records the endpoint index's golden.
var updateEndpointIndexAllocations = flag.Bool("update-endpoint-index", false, "re-record testdata/endpoint_index_allocations.json from this run")

// indexFleetEndpointsPerSlice is the endpoints controller's slice size in
// the fixture: a Service of 4,000 pods spans 40 slices.
const indexFleetEndpointsPerSlice = 100

// indexFleetNamespace is the one namespace of the fixture.
const indexFleetNamespace = "fleet"

// indexedService is one routed Service's EndpointSlices as the endpoints
// controller keeps them, with the pods a pass reads against them. Every
// routed pod of the Component (a gang's leader, a single pod itself) is one
// endpoint. At rest every endpoint is Ready, Serving and not Terminating.
// Under a roll the first terminating endpoints are Ready and Terminating,
// the pods leaving; the notReady after them are not Ready, the pods coming
// up.
type indexedService struct {
	// name is <shape>/<component>, or <shape>/<component>/rolling.
	name    string
	service string
	slices  []discoveryv1.EndpointSlice
	pods    []*corev1.Pod
	// terminating and notReady count the endpoints in those states, from
	// the front of the list.
	terminating, notReady int
	// built is the index of the last build, kept so the build is not
	// optimized away.
	built endpointIndex
}

// indexedServices is every routed Service of the coordination pass
// fixture's reference shapes, the fleet of 1,725 gangs of two, and the
// quarter shape under a roll at a budget of ten percent: a tenth of its
// pods leaving, a tenth coming up.
func indexedServices() []*indexedService {
	shapes := []struct {
		name                  string
		endpoints             int
		terminating, notReady int
	}{
		{name: "engine/1000x1", endpoints: 1000},
		{name: "engine/4000x1", endpoints: 4000},
		{name: "engine/160x8", endpoints: 160},
		{name: "engine/1725x2", endpoints: 1725},
		{name: "prefill-decode/12:8/engine", endpoints: 12},
		{name: "prefill-decode/12:8/decoder", endpoints: 8},
		{name: "prefill-decode/12:8/router", endpoints: 4},
		{name: "prefill-decode/30:10/engine", endpoints: 30},
		{name: "prefill-decode/30:10/decoder", endpoints: 10},
		{name: "prefill-decode/30:10/router", endpoints: 4},
		{name: "engine/1000x1/rolling", endpoints: 1000, terminating: 100, notReady: 100},
	}
	out := make([]*indexedService, 0, len(shapes))
	for _, shape := range shapes {
		out = append(out, newIndexedService(shape.name, shape.endpoints, shape.terminating, shape.notReady))
	}
	return out
}

// newIndexedService builds the Service's slices and pods for the shape.
func newIndexedService(name string, endpoints, terminating, notReady int) *indexedService {
	s := &indexedService{name: name, service: "svc-" + strconv.Itoa(endpoints), terminating: terminating, notReady: notReady}
	for idx := 0; idx < endpoints; idx++ {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: indexFleetNamespace,
			Name:      s.service + "-engine-" + strconv.Itoa(idx) + "-0",
			UID:       types.UID("uid-" + strconv.Itoa(idx)),
		}}
		s.pods = append(s.pods, pod)
		if idx%indexFleetEndpointsPerSlice == 0 {
			s.slices = append(s.slices, discoveryv1.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: indexFleetNamespace,
					Name:      s.service + "-" + strconv.Itoa(idx/indexFleetEndpointsPerSlice),
					Labels:    map[string]string{discoveryv1.LabelServiceName: s.service},
				},
				AddressType: discoveryv1.AddressTypeIPv4,
				Ports:       []discoveryv1.EndpointPort{{Name: ptr.To("serving"), Port: ptr.To(int32(8000)), Protocol: ptr.To(corev1.ProtocolTCP)}},
			})
		}
		ready, leaving := idx >= terminating+notReady || idx < terminating, idx < terminating
		slice := &s.slices[len(s.slices)-1]
		slice.Endpoints = append(slice.Endpoints, discoveryv1.Endpoint{
			Addresses:  []string{"10." + strconv.Itoa(idx/65536%256) + "." + strconv.Itoa(idx/256%256) + "." + strconv.Itoa(idx%256)},
			Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(ready), Serving: ptr.To(ready), Terminating: ptr.To(leaving)},
			NodeName:   ptr.To("node-" + strconv.Itoa(idx)),
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
		})
	}
	return s
}

// index builds the Service's endpoint index, as a Batcher does the first
// time a pass reads the Service.
func (s *indexedService) index() {
	s.built = indexEndpoints(s.slices)
}

// batcherReadings reads every pod of the Service through a Batcher over a
// cache stand-in that hands out the held slices, and counts the answers.
type batcherReadings struct {
	drained, routed, inRotation int
}

// read answers the three readings for every pod off one read of the
// Service and counts them; the first reading lists and indexes, the rest
// look up.
func (s *indexedService) read(b *Batcher) (batcherReadings, error) {
	ctx := context.Background()
	var n batcherReadings
	for _, pod := range s.pods {
		drained, err := b.IsPodDrained(ctx, s.service, pod)
		if err != nil {
			return n, err
		}
		routed, err := b.IsPodRouted(ctx, s.service, pod)
		if err != nil {
			return n, err
		}
		inRotation, err := b.IsPodInRotation(ctx, s.service, pod)
		if err != nil {
			return n, err
		}
		if drained {
			n.drained++
		}
		if routed {
			n.routed++
		}
		if inRotation {
			n.inRotation++
		}
	}
	return n, nil
}

// checkReadings holds the Batcher's answers over the Service to the shape:
// a pod is routed unless its endpoint is not Ready, in rotation unless it
// is also Terminating, drained when not routed; the Service is listed once
// and no slice is copied; and once the Service is read, the readings of
// every pod allocate nothing.
func (s *indexedService) checkReadings(t *testing.T) {
	t.Helper()
	held := make([]*discoveryv1.EndpointSlice, len(s.slices))
	for i := range s.slices {
		held[i] = &s.slices[i]
	}
	reader := fakeclient.ReadFromMemory(slicesAsObjects(held)...)
	b := NewBatcher(reader, indexFleetNamespace)
	n, err := s.read(b)
	if err != nil {
		t.Fatalf("%s: %v", s.name, err)
	}
	want := batcherReadings{drained: s.notReady, routed: len(s.pods) - s.notReady, inRotation: len(s.pods) - s.notReady - s.terminating}
	if n != want {
		t.Errorf("%s: the readings count %+v over %d pods, want %+v", s.name, n, len(s.pods), want)
	}
	if reader.Lists != 1 || reader.Copies != 0 {
		t.Errorf("%s: the Batcher listed %d times and the cache copied %d slices, want one list and no copy", s.name, reader.Lists, reader.Copies)
	}
	if allocations := allocbudget.Measure(func() { _, _ = s.read(b) }); allocations != 0 {
		t.Errorf("%s: the readings of %d pods after the read allocate %d times, want none", s.name, len(s.pods), allocations)
	}
}

// TestEndpointIndexAtRestStaysWithinBudget holds what the index of one
// routed Service's EndpointSlices costs, per reference shape. A Batcher
// builds it the first time a pass reads the Service: the coordination pass
// once per routed Service on every pass, the drain and rotation gates for
// the pods of an attempt in flight.
func TestEndpointIndexAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "endpoint_index_allocations.json"), *updateEndpointIndexAllocations)
	defer golden.Close()
	for _, s := range indexedServices() {
		allocations, bytes := allocbudget.Smallest(s.index)
		golden.CheckCount(s.name+"/allocations", allocations)
		golden.CheckCount(s.name+"/bytes", bytes)
		s.checkReadings(t)
	}
}

// BenchmarkEndpointIndex prints the time of one index build per shape,
// with its endpoints beside it.
func BenchmarkEndpointIndex(b *testing.B) {
	for _, s := range indexedServices() {
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				s.index()
			}
			b.ReportMetric(float64(len(s.pods)), "endpoints/op")
		})
	}
}
