package drain

import (
	"context"
	"testing"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// TestEndpointSlicesForService_AsksTheCacheForItsOwnSlices holds the list
// to the request the cache answers without a copy: the namespace, the
// Service's label, and the deep copy switched off. A plain fake client
// answers the request as the apiserver would, the switch unread.
func TestEndpointSlicesForService_AsksTheCacheForItsOwnSlices(t *testing.T) {
	lists := fakeclient.CountingVerb(fakeclient.List).OnKind(&discoveryv1.EndpointSliceList{})
	reader := fakeclient.New(t, lists,
		fakeclient.WithObjects(sliceForService("ns", "svc-1", "svc", endpointSpec{podName: "routed", address: "10.0.0.1", ready: ptr.To(true)})))
	slices, err := EndpointSlicesForService(context.Background(), reader, "ns", "svc")
	if err != nil {
		t.Fatalf("EndpointSlicesForService: %v", err)
	}
	if len(slices) != 1 || slices[0].Name != "svc-1" {
		t.Fatalf("listed %d slices, want svc-1", len(slices))
	}
	options := fakeclient.OptionsOf[client.ListOptions](lists)
	if len(options) != 1 {
		t.Fatalf("recorded %d EndpointSlice lists, want 1", len(options))
	}
	asked := options[0]
	if asked.Namespace != "ns" {
		t.Errorf("the list asked namespace %q, want ns", asked.Namespace)
	}
	if asked.LabelSelector == nil || asked.LabelSelector.String() != discoveryv1.LabelServiceName+"=svc" {
		t.Errorf("the list asked labels %v, want %s=svc", asked.LabelSelector, discoveryv1.LabelServiceName)
	}
	if asked.UnsafeDisableDeepCopy == nil || !*asked.UnsafeDisableDeepCopy {
		t.Errorf("the list asked for copies; want the cache's own slices (UnsafeDisableDeepCopy)")
	}
}

// TestEndpointSlicesForService_ReadsTheHeldSlicesWithoutACopy holds the
// list over a cache stand-in to no copy at all: the slices handed back are
// the held ones.
func TestEndpointSlicesForService_ReadsTheHeldSlicesWithoutACopy(t *testing.T) {
	held, before := heldSlices()
	reader := fakeclient.ReadFromMemory(held...)
	slices, err := EndpointSlicesForService(context.Background(), reader, "ns", "svc")
	if err != nil {
		t.Fatalf("EndpointSlicesForService: %v", err)
	}
	if reader.Copies != 0 {
		t.Errorf("the cache copied %d slices for the list, want none", reader.Copies)
	}
	if want := held[0].(*discoveryv1.EndpointSlice); len(slices) != 2 || slices[0].Endpoints[0].TargetRef != want.Endpoints[0].TargetRef {
		t.Errorf("the list handed back a copy of %s; want the held slice's own endpoints", want.Name)
	}
	checkHeldUnchanged(t, held, before)
}
