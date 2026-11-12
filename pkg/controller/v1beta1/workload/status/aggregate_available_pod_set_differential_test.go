package status

import (
	"fmt"
	"maps"
	"math/rand"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// randomEndpoint draws one endpoint: each condition set true, set false or
// unset; the target a Pod, of no kind, of another kind, or absent; the
// name one of a few so slices overlap.
func randomEndpoint(r *rand.Rand) discoveryv1.Endpoint {
	draw := func() *bool {
		switch r.Intn(3) {
		case 0:
			return nil
		case 1:
			v := false
			return &v
		default:
			v := true
			return &v
		}
	}
	ep := discoveryv1.Endpoint{
		Addresses:  []string{fmt.Sprintf("10.0.0.%d", r.Intn(250))},
		Conditions: discoveryv1.EndpointConditions{Ready: draw(), Serving: draw(), Terminating: draw()},
	}
	name := fmt.Sprintf("pod-%d", r.Intn(6))
	switch r.Intn(5) {
	case 0:
		ep.TargetRef = nil
	case 1:
		ep.TargetRef = &corev1.ObjectReference{Name: name}
	case 2:
		ep.TargetRef = &corev1.ObjectReference{Kind: "Node", Name: name}
	default:
		ep.TargetRef = &corev1.ObjectReference{Kind: "Pod", Namespace: pictureNamespace, Name: name}
	}
	return ep
}

// randomSlices draws the slices a namespace holds: the Service's own and
// a few of another Service or another namespace.
func randomSlices(r *rand.Rand) []client.Object {
	var slices []client.Object
	for i, n := 0, 1+r.Intn(4); i < n; i++ {
		namespace, service := pictureNamespace, pictureService
		switch r.Intn(6) {
		case 0:
			service = "other-headless"
		case 1:
			namespace = "elsewhere"
		}
		endpoints := make([]discoveryv1.Endpoint, r.Intn(5))
		for j := range endpoints {
			endpoints[j] = randomEndpoint(r)
		}
		slices = append(slices, pictureSlice(namespace, fmt.Sprintf("slice-%d", i), service, endpoints...))
	}
	return slices
}

// TestAvailablePodSet_AgreesOnTheHeldSlicesAndTheirCopies holds the reading
// over the cache's own slices to the reading over copies of them, the
// answer the apiserver's client gives, on random pictures of a namespace.
func TestAvailablePodSet_AgreesOnTheHeldSlicesAndTheirCopies(t *testing.T) {
	r := rand.New(rand.NewSource(32))
	for i := 0; i < 500; i++ {
		slices := randomSlices(r)
		uncopied := readAvailable(t, fakeclient.ReadFromMemory(slices...))
		copied := readAvailable(t, fakeclient.New(t, fakeclient.WithObjects(slices...)))
		if !maps.Equal(uncopied, copied) {
			t.Fatalf("picture %d: over the held slices the set is %v, over their copies %v", i, uncopied, copied)
		}
	}
}
