package status

import (
	"context"
	"errors"
	"maps"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sptr "k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

const (
	pictureNamespace = "ns"
	pictureService   = "svc-headless"
)

// pictureSlice is one EndpointSlice of serviceName in namespace holding
// the endpoints given.
func pictureSlice(namespace, name, serviceName string, endpoints ...discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: namespace,
			Labels: map[string]string{discoveryv1.LabelServiceName: serviceName},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   endpoints,
	}
}

// podEndpoint is an endpoint naming podName with the conditions given; a
// nil condition is one the slice leaves unset.
func podEndpoint(podName string, ready, serving, terminating *bool) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses:  []string{"10.0.0.1"},
		Conditions: discoveryv1.EndpointConditions{Ready: ready, Serving: serving, Terminating: terminating},
		TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: pictureNamespace, Name: podName},
	}
}

// readAvailable is the availability reading of the Service over reader.
func readAvailable(t *testing.T, reader client.Reader) map[string]struct{} {
	t.Helper()
	set, err := AvailablePodSet(context.Background(), reader, pictureNamespace, pictureService)
	if err != nil {
		t.Fatalf("AvailablePodSet: %v", err)
	}
	return set
}

// TestAvailablePodSet_OnEveryPicture reads one endpoint in every picture of
// its conditions and its target and says whether its pod is available for
// new traffic: Ready (or Ready unset, which routes) and not Terminating,
// named by a target of kind Pod or of no kind; Serving is not asked, and a
// target of another namespace is kept by its name.
func TestAvailablePodSet_OnEveryPicture(t *testing.T) {
	yes, no := k8sptr.To(true), k8sptr.To(false)
	pod := func(ready, serving, terminating *bool) discoveryv1.Endpoint {
		return podEndpoint("pod-a", ready, serving, terminating)
	}
	withTarget := func(target *corev1.ObjectReference) discoveryv1.Endpoint {
		ep := pod(yes, yes, no)
		ep.TargetRef = target
		return ep
	}
	pictures := []struct {
		name      string
		endpoint  discoveryv1.Endpoint
		available bool
	}{
		{"ready, serving, not terminating", pod(yes, yes, no), true},
		{"ready with no other condition set", pod(yes, nil, nil), true},
		{"no condition set", pod(nil, nil, nil), true},
		{"ready, not serving", pod(yes, no, no), true},
		{"ready, terminating false", pod(yes, nil, no), true},
		{"not ready", pod(no, no, no), false},
		{"not ready, serving", pod(no, yes, no), false},
		{"ready, terminating", pod(yes, yes, yes), false},
		{"ready unset, terminating", pod(nil, nil, yes), false},
		{"target of kind Pod", withTarget(&corev1.ObjectReference{Kind: "Pod", Namespace: pictureNamespace, Name: "pod-a"}), true},
		{"target of no kind", withTarget(&corev1.ObjectReference{Namespace: pictureNamespace, Name: "pod-a"}), true},
		{"target of another namespace", withTarget(&corev1.ObjectReference{Kind: "Pod", Namespace: "elsewhere", Name: "pod-a"}), true},
		{"target of no namespace", withTarget(&corev1.ObjectReference{Kind: "Pod", Name: "pod-a"}), true},
		{"target of kind Node", withTarget(&corev1.ObjectReference{Kind: "Node", Name: "pod-a"}), false},
		{"no target", withTarget(nil), false},
	}
	for _, picture := range pictures {
		t.Run(picture.name, func(t *testing.T) {
			reader := fakeclient.ReadFromMemory(pictureSlice(pictureNamespace, "svc-1", pictureService, picture.endpoint))
			set := readAvailable(t, reader)
			_, available := set["pod-a"]
			if available != picture.available {
				t.Fatalf("available = %v, want %v; the set is %v", available, picture.available, set)
			}
			if len(set) > 1 {
				t.Fatalf("the set names %d pods, want at most the one endpoint's", len(set))
			}
		})
	}
}

// TestAvailablePodSet_ReadsTheServicesSlicesOfTheNamespaceOnce holds the
// reading to one list of the Service's own slices: a slice of another
// Service or of another namespace is not read, and a pod two slices name
// is one name.
func TestAvailablePodSet_ReadsTheServicesSlicesOfTheNamespaceOnce(t *testing.T) {
	yes := k8sptr.To(true)
	reader := fakeclient.ReadFromMemory(
		pictureSlice(pictureNamespace, "svc-1", pictureService, podEndpoint("pod-a", yes, yes, nil), podEndpoint("pod-b", yes, yes, nil)),
		pictureSlice(pictureNamespace, "svc-2", pictureService, podEndpoint("pod-b", yes, yes, nil), podEndpoint("pod-c", yes, yes, nil)),
		pictureSlice(pictureNamespace, "other-1", "other-headless", podEndpoint("pod-d", yes, yes, nil)),
		pictureSlice("elsewhere", "svc-1", pictureService, podEndpoint("pod-e", yes, yes, nil)),
	)
	set := readAvailable(t, reader)
	want := map[string]struct{}{"pod-a": {}, "pod-b": {}, "pod-c": {}}
	if !maps.Equal(set, want) {
		t.Fatalf("the set is %v, want %v", set, want)
	}
	if reader.Lists != 1 {
		t.Fatalf("the reading listed %d times, want once", reader.Lists)
	}
}

func TestAvailablePodSet_NoSliceIsAnEmptySet(t *testing.T) {
	set := readAvailable(t, fakeclient.ReadFromMemory())
	if len(set) != 0 {
		t.Fatalf("the set is %v, want empty", set)
	}
}

func TestAvailablePodSet_AListErrorIsTheReadingsError(t *testing.T) {
	refused := errors.New("the cache is not synced")
	reader := fakeclient.New(t, fakeclient.FailingVerb(fakeclient.List, fakeclient.EveryCall, refused))
	set, err := AvailablePodSet(context.Background(), reader, pictureNamespace, pictureService)
	if !errors.Is(err, refused) {
		t.Fatalf("err = %v, want the list's error", err)
	}
	if set != nil {
		t.Fatalf("the set is %v on a failed list, want none", set)
	}
}

// TestAvailablePodSet_LeavesTheHeldSlicesAsTheyWere holds the reading to
// reading alone: the slices the cache holds are the same objects after it,
// endpoint by endpoint.
func TestAvailablePodSet_LeavesTheHeldSlicesAsTheyWere(t *testing.T) {
	f := newAvailablePodSetFixture(availablePodSetShape{name: "picture", instances: 250, podsPerInstance: 1})
	for i := 0; i < 3; i++ {
		f.read(t)
		f.checkReading(t, "picture")
	}
}
