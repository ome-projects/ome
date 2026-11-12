package status

import (
	"context"
	"flag"
	"fmt"
	"maps"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	k8sptr "k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var updateAvailablePodSetGolden = flag.Bool("update-available-pod-set", false, "re-record testdata/available_pod_set_allocations.json from this run")

const (
	availabilityNamespace = "default"
	availabilityOwner     = "example-service"
	// availabilityEndpointsPerSlice is the endpointslice controller's
	// default slice size.
	availabilityEndpointsPerSlice = 100
)

// availablePodSetShape is a Component at rest: every Instance Ready, its
// routed pod a Ready endpoint of the headless Service. A Service publishes
// the leader of a gang and never a worker, so a gang is one endpoint.
type availablePodSetShape struct {
	name            string
	instances       int
	podsPerInstance int
}

func availablePodSetShapes() []availablePodSetShape {
	return []availablePodSetShape{
		{name: "1000x1", instances: 1000, podsPerInstance: 1},
		{name: "4000x1", instances: 4000, podsPerInstance: 1},
		{name: "160x8", instances: 160, podsPerInstance: 8},
		{name: "1725x2", instances: 1725, podsPerInstance: 2},
	}
}

// routedPodName is the name of the pod the Service publishes for an
// Instance: its single pod, or the leader of its gang.
func routedPodName(shape availablePodSetShape, index int) string {
	runner := types.RunnerDefault
	if shape.podsPerInstance > 1 {
		runner = types.RunnerLeader
	}
	return query.PodName(availabilityOwner, types.ComponentEngine, int32(index), runner, 0)
}

// availabilityEndpoint is an endpoint as the endpointslice controller
// writes one for a Ready pod: the address, the three conditions, the pod
// reference with its UID, the node and the zone.
func availabilityEndpoint(podName string, index int) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses: []string{fmt.Sprintf("10.%d.%d.%d", index/65536%256, index/256%256, index%256)},
		Conditions: discoveryv1.EndpointConditions{
			Ready: k8sptr.To(true), Serving: k8sptr.To(true), Terminating: k8sptr.To(false),
		},
		TargetRef: &corev1.ObjectReference{
			Kind: "Pod", Namespace: availabilityNamespace, Name: podName,
			UID: k8stypes.UID(podName + "-uid"),
		},
		NodeName: k8sptr.To(fmt.Sprintf("node-%d", index%64)),
		Zone:     k8sptr.To(fmt.Sprintf("zone-%d", index%3)),
	}
}

// availabilitySlices builds the headless Service's EndpointSlices for the
// shape, filled to the controller's slice size, with the names the
// availability reading must find.
func availabilitySlices(shape availablePodSetShape) ([]client.Object, map[string]struct{}) {
	serviceName := query.HeadlessServiceName(availabilityOwner, types.ComponentEngine)
	want := make(map[string]struct{}, shape.instances)
	var slices []client.Object
	var slice *discoveryv1.EndpointSlice
	for index := 0; index < shape.instances; index++ {
		if index%availabilityEndpointsPerSlice == 0 {
			slice = &discoveryv1.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("%s-%d", serviceName, len(slices)), Namespace: availabilityNamespace,
					Labels: map[string]string{
						discoveryv1.LabelServiceName: serviceName,
						discoveryv1.LabelManagedBy:   "endpointslice-controller.k8s.io",
					},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "v1", Kind: "Service", Name: serviceName,
						UID: k8stypes.UID(serviceName + "-uid"), Controller: k8sptr.To(true), BlockOwnerDeletion: k8sptr.To(true),
					}},
				},
				AddressType: discoveryv1.AddressTypeIPv4,
				Ports:       []discoveryv1.EndpointPort{{Name: k8sptr.To("http"), Port: k8sptr.To(int32(8080)), Protocol: k8sptr.To(corev1.ProtocolTCP)}},
			}
			slices = append(slices, slice)
		}
		name := routedPodName(shape, index)
		want[name] = struct{}{}
		slice.Endpoints = append(slice.Endpoints, availabilityEndpoint(name, index))
	}
	return slices, want
}

// availablePodSetFixture is the headless Service's slices held as the
// informer cache holds them, with a copy to prove them unchanged by a read.
type availablePodSetFixture struct {
	reader      *fakeclient.MemoryReader
	serviceName string
	held        []client.Object
	before      []client.Object
	want        map[string]struct{}
	last        map[string]struct{}
}

func newAvailablePodSetFixture(shape availablePodSetShape) *availablePodSetFixture {
	held, want := availabilitySlices(shape)
	before := make([]client.Object, len(held))
	for i, obj := range held {
		before[i] = obj.DeepCopyObject().(client.Object)
	}
	return &availablePodSetFixture{
		reader:      fakeclient.ReadFromMemory(held...),
		serviceName: query.HeadlessServiceName(availabilityOwner, types.ComponentEngine),
		held:        held,
		before:      before,
		want:        want,
	}
}

// read is one availability reading over the fixture.
func (f *availablePodSetFixture) read(tb testing.TB) {
	set, err := AvailablePodSet(context.Background(), f.reader, availabilityNamespace, f.serviceName)
	if err != nil {
		tb.Fatalf("AvailablePodSet: %v", err)
	}
	f.last = set
}

// checkReading holds what every reading must leave behind: the set of the
// Ready, non-terminating pod endpoints' names, and the held slices as they
// were.
func (f *availablePodSetFixture) checkReading(tb testing.TB, shape string) {
	tb.Helper()
	if !maps.Equal(f.last, f.want) {
		tb.Errorf("%s: the set holds %d names, want the %d Ready endpoints' pod names", shape, len(f.last), len(f.want))
	}
	for i := range f.held {
		if !equality.Semantic.DeepEqual(f.held[i], f.before[i]) {
			tb.Errorf("%s: the reading changed the held slice %s", shape, f.held[i].GetName())
		}
	}
}

// BenchmarkAvailablePodSetAtRest is the cost of the availability reading
// over a Component at rest at fleet size, with the slices the cache copied
// for it beside the time.
func BenchmarkAvailablePodSetAtRest(b *testing.B) {
	for _, shape := range availablePodSetShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newAvailablePodSetFixture(shape)
			b.ReportAllocs()
			copies := f.reader.Copies
			b.ResetTimer()
			for b.Loop() {
				f.read(b)
			}
			b.StopTimer()
			b.ReportMetric(float64(f.reader.Copies-copies)/float64(b.N), "slice-copies/op")
			f.checkReading(b, shape.name)
		})
	}
}

// TestAvailablePodSetAtRestStaysWithinBudget holds the availability reading
// of one pass over a Component at rest, at 1,000 and 4,000 single-pod
// Instances, 160 gangs of eight and 1,725 gangs of two, to the recorded
// golden: the slice lists it makes and the slices the cache copies for it
// exactly, and its allocations and bytes within the tolerance. Beside the
// golden, the set is the Ready, non-terminating pod endpoints' names and
// the held slices are as they were.
func TestAvailablePodSetAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "available_pod_set_allocations.json"), *updateAvailablePodSetGolden)
	defer golden.Close()
	for _, shape := range availablePodSetShapes() {
		f := newAvailablePodSetFixture(shape)
		lists, copies := f.reader.Lists, f.reader.Copies
		f.read(t)
		golden.CheckCount(shape.name+"/slice-lists", int64(f.reader.Lists-lists))
		golden.CheckCount(shape.name+"/slice-copies", int64(f.reader.Copies-copies))
		golden.Check(shape.name+"/allocations", func() { f.read(t) })
		golden.CheckBytes(shape.name+"/bytes", func() { f.read(t) })
		f.checkReading(t, shape.name)
	}
}
