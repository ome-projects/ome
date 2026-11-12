package drain

import (
	"context"
	"testing"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// heldSlices is a Service's EndpointSlices held as the informer cache holds
// them: one pod routed, one draining, one of another Service, one of
// another namespace; with a copy of each to prove them unchanged.
func heldSlices() (held []client.Object, before []client.Object) {
	held = []client.Object{
		sliceForService("ns", "svc-1", "svc", endpointSpec{podName: "routed", address: "10.0.0.1", ready: ptr.To(true)}),
		sliceForService("ns", "svc-2", "svc", endpointSpec{podName: "draining", address: "10.0.0.2", ready: ptr.To(false)}),
		sliceForService("ns", "other-1", "other", endpointSpec{podName: "elsewhere", address: "10.0.0.3", ready: ptr.To(true)}),
		sliceForService("far", "svc-1", "svc", endpointSpec{podName: "far", address: "10.0.0.4", ready: ptr.To(true)}),
	}
	for _, obj := range held {
		before = append(before, obj.DeepCopyObject().(client.Object))
	}
	return held, before
}

func checkHeldUnchanged(t *testing.T, held, before []client.Object) {
	t.Helper()
	for i := range held {
		if !equality.Semantic.DeepEqual(held[i], before[i]) {
			t.Errorf("the reading changed the held slice %s/%s", held[i].GetNamespace(), held[i].GetName())
		}
	}
}

// TestEndpointSlicesForService_ListsTheServicesSlicesOfTheNamespace holds the
// list to the Service's own slices in the namespace, in one list.
func TestEndpointSlicesForService_ListsTheServicesSlicesOfTheNamespace(t *testing.T) {
	held, before := heldSlices()
	reader := fakeclient.ReadFromMemory(held...)
	slices, err := EndpointSlicesForService(context.Background(), reader, "ns", "svc")
	if err != nil {
		t.Fatalf("EndpointSlicesForService: %v", err)
	}
	if len(slices) != 2 || slices[0].Name != "svc-1" || slices[1].Name != "svc-2" {
		t.Fatalf("listed %d slices, want svc-1 and svc-2 of namespace ns", len(slices))
	}
	if reader.Lists != 1 {
		t.Fatalf("listed %d times, want once", reader.Lists)
	}
	checkHeldUnchanged(t, held, before)
}

// TestBatcher_LeavesTheHeldSlicesAsTheyWere holds the drain and rotation
// readings to reading alone: the slices the cache holds are the same
// objects after every reading of the Batcher.
func TestBatcher_LeavesTheHeldSlicesAsTheyWere(t *testing.T) {
	held, before := heldSlices()
	reader := fakeclient.ReadFromMemory(held...)
	b := NewBatcher(reader, "ns")
	ctx := context.Background()
	readings := map[string]bool{}
	for _, name := range []string{"routed", "draining", "absent"} {
		pod := testPod("ns", name)
		drained, err := b.IsPodDrained(ctx, "svc", pod)
		if err != nil {
			t.Fatalf("IsPodDrained(%s): %v", name, err)
		}
		inRotation, err := b.IsPodInRotation(ctx, "svc", pod)
		if err != nil {
			t.Fatalf("IsPodInRotation(%s): %v", name, err)
		}
		readings[name+" drained"], readings[name+" in rotation"] = drained, inRotation
	}
	want := map[string]bool{
		"routed drained": false, "routed in rotation": true,
		"draining drained": true, "draining in rotation": false,
		"absent drained": true, "absent in rotation": false,
	}
	for name, reading := range want {
		if readings[name] != reading {
			t.Errorf("%s = %v, want %v", name, readings[name], reading)
		}
	}
	if reader.Lists != 1 {
		t.Errorf("the Batcher listed %d times, want once for the Service", reader.Lists)
	}
	checkHeldUnchanged(t, held, before)
	if list := (&discoveryv1.EndpointSliceList{}); reader.List(ctx, list, client.InNamespace("ns")) != nil || len(list.Items) != 3 {
		t.Errorf("the namespace holds %d slices after the readings, want 3", len(list.Items))
	}
}
