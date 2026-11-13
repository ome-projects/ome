package drain

import (
	"context"
	"math/rand"
	"strconv"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// slicesAsObjects is the slices as a cache stand-in holds them.
func slicesAsObjects(slices []*discoveryv1.EndpointSlice) []client.Object {
	out := make([]client.Object, 0, len(slices))
	for _, slice := range slices {
		out = append(out, slice)
	}
	return out
}

// endpointNamesPod is the naming rule of the index, written out: a
// TargetRef of kind Pod or of no kind, with the pod's name, in the pod's
// namespace or in no namespace. An endpoint with no TargetRef names no pod.
func endpointNamesPod(ep *discoveryv1.Endpoint, pod *corev1.Pod) bool {
	ref := ep.TargetRef
	if ref == nil || (ref.Kind != "" && ref.Kind != "Pod") || ref.Name != pod.Name {
		return false
	}
	return ref.Namespace == "" || ref.Namespace == pod.Namespace
}

// readingsByTheRule is the rule the index answers, written out as a walk
// over every endpoint of the Service's slices: the pod is routed when some
// endpoint naming it is Ready (a missing Ready reads as Ready), and in
// rotation when some endpoint naming it is Ready and not Terminating (a
// missing Terminating reads as not terminating).
func readingsByTheRule(slices []discoveryv1.EndpointSlice, pod *corev1.Pod) (routed, inRotation bool) {
	for i := range slices {
		for j := range slices[i].Endpoints {
			ep := &slices[i].Endpoints[j]
			if !endpointNamesPod(ep, pod) {
				continue
			}
			ready := ep.Conditions.Ready == nil || *ep.Conditions.Ready
			terminating := ep.Conditions.Terminating != nil && *ep.Conditions.Terminating
			routed = routed || ready
			inRotation = inRotation || (ready && !terminating)
		}
	}
	return routed, inRotation
}

// podReadings is the Batcher's three answers for one pod.
type podReadings struct {
	drained, routed, inRotation bool
}

// batcherReadingsOf asks the Batcher's three readings for pod against service.
func batcherReadingsOf(t *testing.T, b *Batcher, service string, pod *corev1.Pod) podReadings {
	t.Helper()
	ctx := context.Background()
	drained, err := b.IsPodDrained(ctx, service, pod)
	if err != nil {
		t.Fatalf("IsPodDrained(%s/%s): %v", pod.Namespace, pod.Name, err)
	}
	routed, err := b.IsPodRouted(ctx, service, pod)
	if err != nil {
		t.Fatalf("IsPodRouted(%s/%s): %v", pod.Namespace, pod.Name, err)
	}
	inRotation, err := b.IsPodInRotation(ctx, service, pod)
	if err != nil {
		t.Fatalf("IsPodInRotation(%s/%s): %v", pod.Namespace, pod.Name, err)
	}
	return podReadings{drained: drained, routed: routed, inRotation: inRotation}
}

// ruleEndpoint builds one endpoint whose TargetRef names pod, with the Ready
// and Terminating conditions given; a nil pointer leaves the condition out.
func ruleEndpoint(pod *corev1.Pod, ready, terminating *bool) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses:  []string{"10.0.0.1"},
		Conditions: discoveryv1.EndpointConditions{Ready: ready, Terminating: terminating},
		TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name},
	}
}

// ruleSlice builds one slice of service in namespace with the endpoints.
func ruleSlice(namespace, name, service string, endpoints ...discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: namespace, Name: name, Labels: map[string]string{discoveryv1.LabelServiceName: service}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   endpoints,
	}
}

// withRef rewrites the endpoint's TargetRef kind and namespace.
func withRef(ep discoveryv1.Endpoint, kind, namespace string) discoveryv1.Endpoint {
	ep.TargetRef = &corev1.ObjectReference{Kind: kind, Namespace: namespace, Name: ep.TargetRef.Name}
	return ep
}

// withoutRef drops the endpoint's TargetRef.
func withoutRef(ep discoveryv1.Endpoint) discoveryv1.Endpoint {
	ep.TargetRef = nil
	return ep
}

// TestEndpointIndex_ReadsEveryEndpointOfThePod holds the Batcher's three
// readings, picture by picture, to the rule: routed when some endpoint
// naming the pod is Ready, in rotation when one of those is not
// Terminating, drained when not routed. Every list holds a slice of the
// Service, so no reading falls back to the Service's existence.
func TestEndpointIndex_ReadsEveryEndpointOfThePod(t *testing.T) {
	yes, no := ptr.To(true), ptr.To(false)
	p := testPod("ns", "p1")
	far := testPod("far", "p1")
	other := testPod("ns", "p2")
	one := func(endpoints ...discoveryv1.Endpoint) []*discoveryv1.EndpointSlice {
		return []*discoveryv1.EndpointSlice{ruleSlice("ns", "svc-1", "svc", endpoints...)}
	}
	two := func(first, second discoveryv1.Endpoint) []*discoveryv1.EndpointSlice {
		return []*discoveryv1.EndpointSlice{ruleSlice("ns", "svc-1", "svc", first), ruleSlice("ns", "svc-2", "svc", second)}
	}
	rows := []struct {
		name               string
		slices             []*discoveryv1.EndpointSlice
		pod                *corev1.Pod
		routed, inRotation bool
	}{
		{"a Ready endpoint routes the pod and puts it in rotation", one(ruleEndpoint(p, yes, nil)), p, true, true},
		{"a Ready endpoint that is not Terminating does the same", one(ruleEndpoint(p, yes, no)), p, true, true},
		{"an endpoint with no Ready condition reads as Ready", one(ruleEndpoint(p, nil, nil)), p, true, true},
		{"a not-Ready endpoint routes nothing", one(ruleEndpoint(p, no, nil)), p, false, false},
		{"a Ready, Terminating endpoint routes the pod but keeps it out of rotation", one(ruleEndpoint(p, yes, yes)), p, true, false},
		{"a Terminating endpoint with no Ready condition is routed, out of rotation", one(ruleEndpoint(p, nil, yes)), p, true, false},
		{"a not-Ready, Terminating endpoint routes nothing", one(ruleEndpoint(p, no, yes)), p, false, false},
		{"two endpoints of the pod, not Ready then Ready", one(ruleEndpoint(p, no, nil), ruleEndpoint(p, yes, nil)), p, true, true},
		{"two endpoints of the pod, Ready then not Ready", one(ruleEndpoint(p, yes, nil), ruleEndpoint(p, no, nil)), p, true, true},
		{"two endpoints of the pod, Terminating then not Terminating", one(ruleEndpoint(p, yes, yes), ruleEndpoint(p, yes, no)), p, true, true},
		{"two endpoints of the pod, not Terminating then Terminating", one(ruleEndpoint(p, yes, no), ruleEndpoint(p, yes, yes)), p, true, true},
		{"the Terminating endpoint in the first slice, the other in the second", two(ruleEndpoint(p, yes, yes), ruleEndpoint(p, yes, no)), p, true, true},
		{"the other endpoint in the first slice, the Terminating one in the second", two(ruleEndpoint(p, yes, no), ruleEndpoint(p, yes, yes)), p, true, true},
		{"the not-Ready endpoint in the first slice, the Ready one in the second", two(ruleEndpoint(p, no, nil), ruleEndpoint(p, yes, nil)), p, true, true},
		{"the Ready endpoint in the first slice, the not-Ready one in the second", two(ruleEndpoint(p, yes, nil), ruleEndpoint(p, no, nil)), p, true, true},
		{"two Terminating endpoints of the pod keep it out of rotation", one(ruleEndpoint(p, yes, yes), ruleEndpoint(p, yes, yes)), p, true, false},
		{"two not-Ready endpoints route nothing", one(ruleEndpoint(p, no, nil), ruleEndpoint(p, no, nil)), p, false, false},
		{"the same endpoint listed twice counts once", one(ruleEndpoint(p, yes, nil), ruleEndpoint(p, yes, nil)), p, true, true},
		{"an endpoint naming another pod says nothing about this one", one(ruleEndpoint(other, yes, nil)), p, false, false},
		{"a TargetRef with no namespace names the pod in any namespace", one(withRef(ruleEndpoint(p, yes, nil), "Pod", "")), far, true, true},
		{"a TargetRef of another namespace names no pod of this one", one(ruleEndpoint(p, yes, nil)), far, false, false},
		{"a TargetRef of kind Node names no pod", one(withRef(ruleEndpoint(p, yes, nil), "Node", "ns")), p, false, false},
		{"a TargetRef of no kind names the pod", one(withRef(ruleEndpoint(p, yes, nil), "", "ns")), p, true, true},
		{"an endpoint with no TargetRef names no pod", one(withoutRef(ruleEndpoint(p, yes, nil))), p, false, false},
		{"a Serving endpoint that is not Ready routes nothing", one(func() discoveryv1.Endpoint {
			ep := ruleEndpoint(p, no, no)
			ep.Conditions.Serving = yes
			return ep
		}()), p, false, false},
		{"an endpoint of another Service's slice is not read", []*discoveryv1.EndpointSlice{ruleSlice("ns", "svc-1", "svc", ruleEndpoint(other, yes, nil)), ruleSlice("ns", "other-1", "other", ruleEndpoint(p, yes, nil))}, p, false, false},
		{"a slice of the Service in another namespace is not read", []*discoveryv1.EndpointSlice{ruleSlice("ns", "svc-1", "svc", ruleEndpoint(other, yes, nil)), ruleSlice("far", "svc-1", "svc", ruleEndpoint(p, yes, nil))}, p, false, false},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			reader := fakeclient.ReadFromMemory(slicesAsObjects(row.slices)...)
			got := batcherReadingsOf(t, NewBatcher(reader, "ns"), "svc", row.pod)
			want := podReadings{drained: !row.routed, routed: row.routed, inRotation: row.inRotation}
			if got != want {
				t.Errorf("the Batcher reads %+v, want %+v", got, want)
			}
			ofService := slicesOfService(row.slices, "ns", "svc")
			if routed, inRotation := readingsByTheRule(ofService, row.pod); routed != row.routed || inRotation != row.inRotation {
				t.Errorf("the rule reads routed=%v inRotation=%v, want %v %v: the row disagrees with the rule", routed, inRotation, row.routed, row.inRotation)
			}
			if reader.Lists != 1 || reader.Copies != 0 {
				t.Errorf("the Batcher listed %d times and the cache copied %d slices, want one list and no copy", reader.Lists, reader.Copies)
			}
		})
	}
}

// slicesOfService is the slices the Batcher's list hands back: the ones of
// service in namespace, in the order held.
func slicesOfService(slices []*discoveryv1.EndpointSlice, namespace, service string) []discoveryv1.EndpointSlice {
	var out []discoveryv1.EndpointSlice
	for _, slice := range slices {
		if slice.Namespace == namespace && slice.Labels[discoveryv1.LabelServiceName] == service {
			out = append(out, *slice)
		}
	}
	return out
}

// TestEndpointIndex_AgreesWithTheRuleOnRandomSlices reads 3,000 random
// slice lists through the Batcher and holds its three readings to the
// rule written out, for eight pods each: one to four slices, of the
// Service or another, in the namespace or another, with up to six
// endpoints whose TargetRef is missing or names one of three pods by any
// kind and namespace, under every Ready and Terminating value; the
// Service itself present or not. With no slice of the Service the Batcher
// answers from the Service's existence: drained when it is absent, routed
// either way, never in rotation.
func TestEndpointIndex_AgreesWithTheRuleOnRandomSlices(t *testing.T) {
	rng := rand.New(rand.NewSource(45))
	pick := func(options ...string) string { return options[rng.Intn(len(options))] }
	condition := func() *bool {
		switch rng.Intn(3) {
		case 0:
			return nil
		case 1:
			return ptr.To(true)
		default:
			return ptr.To(false)
		}
	}
	var pods []*corev1.Pod
	for _, namespace := range []string{"ns", "far"} {
		for _, name := range []string{"p0", "p1", "p2", "p3"} {
			pods = append(pods, testPod(namespace, name))
		}
	}
	for list := 0; list < 3000; list++ {
		var held []client.Object
		var slices []*discoveryv1.EndpointSlice
		for i := 1 + rng.Intn(4); i > 0; i-- {
			slice := ruleSlice(pick("ns", "ns", "ns", "far"), "s"+strconv.Itoa(i), pick("svc", "svc", "svc", "other"))
			for j := rng.Intn(7); j > 0; j-- {
				ep := discoveryv1.Endpoint{
					Addresses:  []string{"10.0." + strconv.Itoa(i) + "." + strconv.Itoa(j)},
					Conditions: discoveryv1.EndpointConditions{Ready: condition(), Serving: condition(), Terminating: condition()},
				}
				if rng.Intn(12) != 0 {
					ep.TargetRef = &corev1.ObjectReference{Kind: pick("Pod", "Pod", "Pod", "", "Node"), Namespace: pick("ns", "ns", "far", ""), Name: pick("p0", "p1", "p2")}
				}
				slice.Endpoints = append(slice.Endpoints, ep)
			}
			slices = append(slices, slice)
			held = append(held, slice)
		}
		serviceExists := rng.Intn(2) == 0
		if serviceExists {
			held = append(held, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "svc"}})
		}
		b := NewBatcher(fakeclient.ReadFromMemory(held...), "ns")
		ofService := slicesOfService(slices, "ns", "svc")
		for _, pod := range pods {
			got := batcherReadingsOf(t, b, "svc", pod)
			want := podReadings{drained: !serviceExists, routed: true}
			if len(ofService) > 0 {
				routed, inRotation := readingsByTheRule(ofService, pod)
				want = podReadings{drained: !routed, routed: routed, inRotation: inRotation}
			}
			if got != want {
				t.Fatalf("list %d, pod %s/%s: the Batcher reads %+v, the rule %+v over %d slices of the Service (service exists: %v)", list, pod.Namespace, pod.Name, got, want, len(ofService), serviceExists)
			}
		}
	}
}
