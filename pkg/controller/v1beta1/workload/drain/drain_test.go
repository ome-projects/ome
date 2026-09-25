package drain

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// newDrainTestClient builds a fake controller-runtime client with the
// scheme drain.go reads from (corev1 + discoveryv1).
func newDrainTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add discoveryv1: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()
}

func testPod(namespace, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(name + "-uid"),
		},
	}
}

// sliceForService builds an EndpointSlice labeled for serviceName with one
// endpoint per (podName, ready) tuple supplied. Each endpoint's TargetRef
// is set to {Kind: Pod, Namespace: namespace, Name: podName}.
func sliceForService(namespace, sliceName, serviceName string, endpoints ...endpointSpec) *discoveryv1.EndpointSlice {
	es := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sliceName,
			Namespace: namespace,
			Labels: map[string]string{
				discoveryv1.LabelServiceName: serviceName,
			},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
	}
	for _, e := range endpoints {
		ep := discoveryv1.Endpoint{
			Addresses: []string{e.address},
			Conditions: discoveryv1.EndpointConditions{
				Ready: e.ready,
			},
		}
		if e.podName != "" {
			ep.TargetRef = &corev1.ObjectReference{
				Kind:      "Pod",
				Namespace: namespace,
				Name:      e.podName,
			}
		}
		es.Endpoints = append(es.Endpoints, ep)
	}
	return es
}

type endpointSpec struct {
	podName string
	address string
	ready   *bool
}

func TestIsPodDrained_NilPodRejected(t *testing.T) {
	c := newDrainTestClient(t)
	if _, err := IsPodDrained(context.Background(), c, "ns", "svc", nil); err == nil {
		t.Fatal("expected error for nil pod")
	}
}

func TestIsPodDrained_EmptyServiceNameRejected(t *testing.T) {
	c := newDrainTestClient(t)
	pod := testPod("ns", "p1")
	if _, err := IsPodDrained(context.Background(), c, "ns", "", pod); err == nil {
		t.Fatal("expected error for empty service name")
	}
}

func TestIsPodDrained_NoSlicesAtAll_ReturnsTrue(t *testing.T) {
	// No EndpointSlices AND no Service in the cluster — drained by
	// definition (kube-proxy has nothing to route to). PR B's
	// fail-loud branch only fires when the Service exists; absence
	// of both is the trivial-drain case.
	c := newDrainTestClient(t)
	pod := testPod("ns", "p1")
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !drained {
		t.Fatalf("expected drained=true with no slices and no service")
	}
}

func TestIsPodDrained_ServiceExistsNoSlices_ReturnsNotDrained(t *testing.T) {
	// Conservative: an empty slice list against an existing Service is
	// transient (informer cold-start, no matching pods yet, KCM lag) and
	// must NOT report drained.
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "ns"},
	}
	pod := testPod("ns", "p1")
	c := newDrainTestClient(t, svc)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if drained {
		t.Fatalf("expected drained=false when Service exists but no slices")
	}
}

func TestIsPodDrained_SlicesForDifferentService_ReturnsTrue(t *testing.T) {
	// A slice exists, but it's labeled for a different Service. Drain is
	// scoped to the named Service, so this is still drained.
	pod := testPod("ns", "p1")
	other := sliceForService("ns", "other-1", "other-svc", endpointSpec{
		podName: "p1", address: "10.0.0.1", ready: ptr.To(true),
	})
	c := newDrainTestClient(t, other)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !drained {
		t.Fatalf("expected drained=true when slice targets a different service")
	}
}

func TestIsPodDrained_PodAbsentFromSlices_ReturnsTrue(t *testing.T) {
	// Slice exists for svc but lists a different pod. p1 is not in
	// rotation through svc.
	pod := testPod("ns", "p1")
	slice := sliceForService("ns", "svc-1", "svc", endpointSpec{
		podName: "p-other", address: "10.0.0.2", ready: ptr.To(true),
	})
	c := newDrainTestClient(t, slice)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !drained {
		t.Fatalf("expected drained=true when pod is absent from slice")
	}
}

func TestIsPodDrained_PodPresentAndReady_ReturnsFalse(t *testing.T) {
	pod := testPod("ns", "p1")
	slice := sliceForService("ns", "svc-1", "svc", endpointSpec{
		podName: "p1", address: "10.0.0.1", ready: ptr.To(true),
	})
	c := newDrainTestClient(t, slice)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if drained {
		t.Fatalf("expected drained=false when pod is in slice with Ready=true")
	}
}

func TestIsPodDrained_PodPresentButNotReady_ReturnsTrue(t *testing.T) {
	pod := testPod("ns", "p1")
	slice := sliceForService("ns", "svc-1", "svc", endpointSpec{
		podName: "p1", address: "10.0.0.1", ready: ptr.To(false),
	})
	c := newDrainTestClient(t, slice)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !drained {
		t.Fatalf("expected drained=true when pod endpoint has Ready=false")
	}
}

func TestIsPodDrained_PodPresentReadyNil_TreatedAsReady(t *testing.T) {
	// discovery/v1 says Conditions.Ready SHOULD be set; if nil, presume
	// ready (matches kube-proxy). So drained=false.
	pod := testPod("ns", "p1")
	slice := sliceForService("ns", "svc-1", "svc", endpointSpec{
		podName: "p1", address: "10.0.0.1", ready: nil,
	})
	c := newDrainTestClient(t, slice)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if drained {
		t.Fatalf("expected drained=false when Ready is nil (presumed ready)")
	}
}

func TestIsPodDrained_AcrossMultipleSlices_FindsRoutableEndpoint(t *testing.T) {
	// Slice A has p1 with Ready=false; Slice B has p1 with Ready=true.
	// Any routable endpoint means not drained.
	pod := testPod("ns", "p1")
	a := sliceForService("ns", "svc-1", "svc", endpointSpec{
		podName: "p1", address: "10.0.0.1", ready: ptr.To(false),
	})
	b := sliceForService("ns", "svc-2", "svc", endpointSpec{
		podName: "p1", address: "10.0.0.1", ready: ptr.To(true),
	})
	c := newDrainTestClient(t, a, b)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if drained {
		t.Fatalf("expected drained=false when any slice still publishes pod as Ready")
	}
}

func TestIsPodDrained_AcrossMultipleSlices_AllNotReady(t *testing.T) {
	pod := testPod("ns", "p1")
	a := sliceForService("ns", "svc-1", "svc", endpointSpec{
		podName: "p1", address: "10.0.0.1", ready: ptr.To(false),
	})
	b := sliceForService("ns", "svc-2", "svc", endpointSpec{
		podName: "p-other", address: "10.0.0.2", ready: ptr.To(true),
	})
	c := newDrainTestClient(t, a, b)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !drained {
		t.Fatalf("expected drained=true: every endpoint targeting p1 is NotReady, other endpoints don't count")
	}
}

func TestIsPodDrained_TargetRefNamespaceMismatchIgnored(t *testing.T) {
	// EndpointSlice targets a pod named p1 in a different namespace —
	// must not match our pod.
	pod := testPod("ns", "p1")
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-1",
			Namespace: "ns",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "svc"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: ptr.To(true),
				},
				TargetRef: &corev1.ObjectReference{
					Kind: "Pod", Namespace: "other-ns", Name: "p1",
				},
			},
		},
	}
	c := newDrainTestClient(t, slice)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !drained {
		t.Fatalf("expected drained=true when TargetRef.Namespace differs from pod namespace")
	}
}

func TestIsPodDrained_TargetRefNilIgnored(t *testing.T) {
	// An endpoint with no TargetRef (e.g., custom publisher) can't be
	// matched to a pod and is ignored.
	pod := testPod("ns", "p1")
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-1",
			Namespace: "ns",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "svc"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: ptr.To(true),
				},
				TargetRef: nil,
			},
		},
	}
	c := newDrainTestClient(t, slice)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !drained {
		t.Fatalf("expected drained=true when no endpoint has TargetRef matching the pod")
	}
}

func TestIsPodDrained_TargetRefKindEmpty_TreatedAsPod(t *testing.T) {
	// A publisher that omits TargetRef.Kind but supplies Namespace+Name
	// should still match. Empty Kind is accepted; only an explicitly
	// non-"Pod" kind is rejected.
	pod := testPod("ns", "p1")
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-1",
			Namespace: "ns",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "svc"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: ptr.To(true),
				},
				TargetRef: &corev1.ObjectReference{
					Kind: "", Namespace: "ns", Name: "p1",
				},
			},
		},
	}
	c := newDrainTestClient(t, slice)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if drained {
		t.Fatalf("expected drained=false when TargetRef.Kind is empty but Name+Namespace match a ready endpoint")
	}
}

func TestIsPodDrained_TargetRefKindNotPod_Rejected(t *testing.T) {
	// A TargetRef pointing at a non-Pod resource (e.g., Node) must not
	// match the pod, even when the name happens to coincide.
	pod := testPod("ns", "p1")
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-1",
			Namespace: "ns",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "svc"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: ptr.To(true),
				},
				TargetRef: &corev1.ObjectReference{
					Kind: "Node", Namespace: "ns", Name: "p1",
				},
			},
		},
	}
	c := newDrainTestClient(t, slice)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !drained {
		t.Fatalf("expected drained=true when TargetRef.Kind is not 'Pod' (must not match)")
	}
}

// --- Batcher: single LIST per serviceName, identical semantics ---

// countingReader records the reads that build a Service drain observation and
// can inject failures at either boundary.
type countingReader struct {
	client.Reader
	sliceLists  int
	serviceGets int
	listErr     error
	getErr      error
}

func (r *countingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*discoveryv1.EndpointSliceList); ok {
		r.sliceLists++
		if r.listErr != nil {
			return r.listErr
		}
	}
	return r.Reader.List(ctx, list, opts...)
}

func (r *countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Service); ok {
		r.serviceGets++
		if r.getErr != nil {
			return r.getErr
		}
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestBatcher_OneListPerServiceNameAcrossPods(t *testing.T) {
	// Two pods of the same gang routed through the same per-revision
	// Service — the Batcher must LIST that Service's slices exactly once.
	p1 := testPod("ns", "p1")
	p2 := testPod("ns", "p2")
	slice := sliceForService("ns", "svc-1", "svc",
		endpointSpec{podName: "p1", address: "10.0.0.1", ready: ptr.To(false)},
		endpointSpec{podName: "p2", address: "10.0.0.2", ready: ptr.To(false)},
	)
	cr := &countingReader{Reader: newDrainTestClient(t, slice)}
	b := NewBatcher(cr, "ns")

	for _, pod := range []*corev1.Pod{p1, p2} {
		drained, err := b.IsPodDrained(context.Background(), "svc", pod)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !drained {
			t.Fatalf("expected drained=true for %s (endpoint Ready=false)", pod.Name)
		}
	}
	if cr.sliceLists != 1 {
		t.Fatalf("expected exactly 1 EndpointSlice LIST across 2 pods, got %d", cr.sliceLists)
	}
	if cr.serviceGets != 0 {
		t.Fatalf("non-empty EndpointSlices must not GET the Service, got %d GETs", cr.serviceGets)
	}
}

func TestBatcher_ListsPerDistinctServiceName(t *testing.T) {
	// Pods on different per-revision Services each trigger their own LIST,
	// but only once per distinct serviceName.
	pA := testPod("ns", "pA")
	pB := testPod("ns", "pB")
	sA := sliceForService("ns", "svcA-1", "svcA", endpointSpec{podName: "pA", address: "10.0.0.1", ready: ptr.To(false)})
	sB := sliceForService("ns", "svcB-1", "svcB", endpointSpec{podName: "pB", address: "10.0.0.2", ready: ptr.To(false)})
	cr := &countingReader{Reader: newDrainTestClient(t, sA, sB)}
	b := NewBatcher(cr, "ns")

	if _, err := b.IsPodDrained(context.Background(), "svcA", pA); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := b.IsPodDrained(context.Background(), "svcB", pB); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Re-check pA — must reuse the memoized svcA list, no new LIST.
	if _, err := b.IsPodDrained(context.Background(), "svcA", pA); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cr.sliceLists != 2 {
		t.Fatalf("expected 2 LISTs (one per distinct service), got %d", cr.sliceLists)
	}
}

func TestBatcher_MatchesIsPodDrainedSemantics(t *testing.T) {
	// The Batcher path must produce the same answer as the package-level
	// IsPodDrained across the key cases: ready (not drained), not-ready
	// (drained), Service-exists-no-slices (not drained), and no-service
	// (drained).
	cases := []struct {
		name string
		objs []client.Object
		want bool
	}{
		{
			name: "ready endpoint -> not drained",
			objs: []client.Object{sliceForService("ns", "s", "svc", endpointSpec{podName: "p1", address: "10.0.0.1", ready: ptr.To(true)})},
			want: false,
		},
		{
			name: "not-ready endpoint -> drained",
			objs: []client.Object{sliceForService("ns", "s", "svc", endpointSpec{podName: "p1", address: "10.0.0.1", ready: ptr.To(false)})},
			want: true,
		},
		{
			name: "service exists, no slices -> not drained",
			objs: []client.Object{&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "ns"}}},
			want: false,
		},
		{
			name: "no service, no slices -> drained",
			objs: nil,
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := testPod("ns", "p1")
			c := newDrainTestClient(t, tc.objs...)
			direct, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
			if err != nil {
				t.Fatalf("IsPodDrained error: %v", err)
			}
			batched, err := NewBatcher(c, "ns").IsPodDrained(context.Background(), "svc", pod)
			if err != nil {
				t.Fatalf("Batcher.IsPodDrained error: %v", err)
			}
			if direct != tc.want || batched != tc.want {
				t.Fatalf("want %v; IsPodDrained=%v Batcher=%v", tc.want, direct, batched)
			}
		})
	}
}

func TestBatcher_NilPodAndEmptyServiceRejected(t *testing.T) {
	cr := &countingReader{Reader: newDrainTestClient(t)}
	b := NewBatcher(cr, "ns")
	if _, err := b.IsPodDrained(context.Background(), "svc", nil); err == nil {
		t.Fatal("expected error for nil pod")
	}
	if _, err := b.IsPodDrained(context.Background(), "", testPod("ns", "p1")); err == nil {
		t.Fatal("expected error for empty service name")
	}
	if cr.sliceLists != 0 || cr.serviceGets != 0 {
		t.Fatalf("invalid inputs must perform no reads, got LIST=%d GET=%d", cr.sliceLists, cr.serviceGets)
	}
}

func TestBatcher_EmptySlicesCachesServiceLookup(t *testing.T) {
	tests := []struct {
		name string
		objs []client.Object
		want bool
	}{
		{
			name: "existing service is not drained",
			objs: []client.Object{&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "ns"}}},
			want: false,
		},
		{
			name: "absent service is drained",
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cr := &countingReader{Reader: newDrainTestClient(t, tt.objs...)}
			batcher := NewBatcher(cr, "ns")
			for _, pod := range []*corev1.Pod{testPod("ns", "p1"), testPod("ns", "p2"), testPod("ns", "p1")} {
				drained, err := batcher.IsPodDrained(context.Background(), "svc", pod)
				if err != nil {
					t.Fatalf("IsPodDrained(%s): %v", pod.Name, err)
				}
				if drained != tt.want {
					t.Fatalf("IsPodDrained(%s)=%v, want %v", pod.Name, drained, tt.want)
				}
			}
			if cr.sliceLists != 1 || cr.serviceGets != 1 {
				t.Fatalf("empty-slice observation reads: LIST=%d GET=%d, want 1/1", cr.sliceLists, cr.serviceGets)
			}
		})
	}
}

func TestBatcher_CachesObservationErrors(t *testing.T) {
	tests := []struct {
		name     string
		listErr  error
		getErr   error
		wantList int
		wantGet  int
	}{
		{
			name:     "EndpointSlice list failure",
			listErr:  errors.New("slice list failed"),
			wantList: 1,
		},
		{
			name:     "empty-slice Service lookup failure",
			getErr:   errors.New("service lookup failed"),
			wantList: 1,
			wantGet:  1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cr := &countingReader{
				Reader:  newDrainTestClient(t),
				listErr: tt.listErr,
				getErr:  tt.getErr,
			}
			batcher := NewBatcher(cr, "ns")
			for _, pod := range []*corev1.Pod{testPod("ns", "p1"), testPod("ns", "p2")} {
				if _, err := batcher.IsPodDrained(context.Background(), "svc", pod); err == nil {
					t.Fatalf("IsPodDrained(%s) returned no error", pod.Name)
				}
			}
			if cr.sliceLists != tt.wantList || cr.serviceGets != tt.wantGet {
				t.Fatalf("cached failure reads: LIST=%d GET=%d, want %d/%d", cr.sliceLists, cr.serviceGets, tt.wantList, tt.wantGet)
			}
		})
	}
}

func TestBatcher_IndexesRoutableTargetsWithDuplicateEndpointSemantics(t *testing.T) {
	tests := []struct {
		name      string
		endpoints []endpointSpec
		want      bool
	}{
		{
			name: "duplicate false and nil Ready remains routable",
			endpoints: []endpointSpec{
				{podName: "p1", address: "10.0.0.1", ready: ptr.To(false)},
				{podName: "p1", address: "10.0.0.2", ready: nil},
			},
			want: false,
		},
		{
			name: "duplicate false endpoints are drained",
			endpoints: []endpointSpec{
				{podName: "p1", address: "10.0.0.1", ready: ptr.To(false)},
				{podName: "p1", address: "10.0.0.2", ready: ptr.To(false)},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slice := sliceForService("ns", "svc-1", "svc", tt.endpoints...)
			cr := &countingReader{Reader: newDrainTestClient(t, slice)}
			drained, err := NewBatcher(cr, "ns").IsPodDrained(context.Background(), "svc", testPod("ns", "p1"))
			if err != nil {
				t.Fatalf("IsPodDrained: %v", err)
			}
			if drained != tt.want {
				t.Fatalf("IsPodDrained=%v, want %v", drained, tt.want)
			}
			if cr.sliceLists != 1 || cr.serviceGets != 0 {
				t.Fatalf("reads: LIST=%d GET=%d, want 1/0", cr.sliceLists, cr.serviceGets)
			}
		})
	}
}

func TestBatcher_TargetRefNamespaceIndexPreservesMatchingRules(t *testing.T) {
	pod := testPod("other-ns", "p1")
	tests := []struct {
		name         string
		refNamespace string
		refKind      string
		want         bool
	}{
		{name: "empty namespace matches", refKind: "Pod", want: false},
		{name: "exact namespace matches", refNamespace: "other-ns", refKind: "Pod", want: false},
		{name: "different namespace is ignored", refNamespace: "ns", refKind: "Pod", want: true},
		{name: "empty kind matches", refNamespace: "other-ns", want: false},
		{name: "non-Pod kind is ignored", refNamespace: "other-ns", refKind: "Node", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slice := sliceForService("ns", "svc-1", "svc")
			slice.Endpoints = []discoveryv1.Endpoint{{
				Addresses:  []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
				TargetRef: &corev1.ObjectReference{
					Kind:      tt.refKind,
					Namespace: tt.refNamespace,
					Name:      pod.Name,
				},
			}}
			drained, err := NewBatcher(newDrainTestClient(t, slice), "ns").IsPodDrained(context.Background(), "svc", pod)
			if err != nil {
				t.Fatalf("IsPodDrained: %v", err)
			}
			if drained != tt.want {
				t.Fatalf("IsPodDrained=%v, want %v", drained, tt.want)
			}
		})
	}
}

func TestBatcher_HoldsOneObservationForItsLifetime(t *testing.T) {
	pod := testPod("ns", "p1")
	slice := sliceForService("ns", "svc-1", "svc", endpointSpec{
		podName: pod.Name, address: "10.0.0.1", ready: ptr.To(true),
	})
	base := newDrainTestClient(t, slice)
	cr := &countingReader{Reader: base}
	batcher := NewBatcher(cr, "ns")

	drained, err := batcher.IsPodDrained(context.Background(), "svc", pod)
	if err != nil || drained {
		t.Fatalf("initial observation: drained=%v err=%v, want false/nil", drained, err)
	}
	fresh := slice.DeepCopy()
	fresh.Endpoints[0].Conditions.Ready = ptr.To(false)
	if err := base.Update(context.Background(), fresh); err != nil {
		t.Fatalf("update EndpointSlice: %v", err)
	}
	drained, err = batcher.IsPodDrained(context.Background(), "svc", pod)
	if err != nil || drained {
		t.Fatalf("memoized observation: drained=%v err=%v, want false/nil", drained, err)
	}
	if cr.sliceLists != 1 {
		t.Fatalf("EndpointSlice LISTs=%d, want 1", cr.sliceLists)
	}

	newBatch := NewBatcher(cr, "ns")
	drained, err = newBatch.IsPodDrained(context.Background(), "svc", pod)
	if err != nil || !drained {
		t.Fatalf("new observation: drained=%v err=%v, want true/nil", drained, err)
	}
	if cr.sliceLists != 2 {
		t.Fatalf("EndpointSlice LISTs=%d, want 2 after a new Batcher", cr.sliceLists)
	}
}

// --- EndpointAvailable tri-state ---

func TestEndpointAvailable_ReadyTrueTerminatingFalseIsAvailable(t *testing.T) {
	ep := discoveryv1.Endpoint{
		Conditions: discoveryv1.EndpointConditions{
			Ready:       ptr.To(true),
			Terminating: ptr.To(false),
		},
	}
	if !EndpointAvailable(ep) {
		t.Errorf("Ready=true, Terminating=false should be available")
	}
}

func TestEndpointAvailable_ReadyTrueTerminatingNilIsAvailable(t *testing.T) {
	// nil Terminating is the steady-state shape (no termination underway).
	ep := discoveryv1.Endpoint{
		Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
	}
	if !EndpointAvailable(ep) {
		t.Errorf("Ready=true, Terminating=nil should be available")
	}
}

func TestEndpointAvailable_ReadyTrueTerminatingTrueIsNotAvailable(t *testing.T) {
	// kube-proxy keeps a terminating-but-still-Ready endpoint in rotation
	// for in-flight requests; it shouldn't count as Available for status
	// or surge-rotation checks.
	ep := discoveryv1.Endpoint{
		Conditions: discoveryv1.EndpointConditions{
			Ready:       ptr.To(true),
			Terminating: ptr.To(true),
		},
	}
	if EndpointAvailable(ep) {
		t.Errorf("Ready=true, Terminating=true must NOT be available")
	}
}

func TestEndpointAvailable_ReadyFalseIsNotAvailable(t *testing.T) {
	ep := discoveryv1.Endpoint{
		Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(false)},
	}
	if EndpointAvailable(ep) {
		t.Errorf("Ready=false should never be available")
	}
}

// --- IsPodInRotation excludes terminating endpoints ---

func TestIsPodInRotation_TerminatingEndpointReportedNotInRotation(t *testing.T) {
	// Even with Ready=true, a terminating endpoint must not be
	// considered "in rotation" — Migrate would otherwise swap onto a
	// pod that's about to disappear.
	pod := testPod("ns", "p1")
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s",
			Namespace: "ns",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "svc"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"10.0.0.1"},
			Conditions: discoveryv1.EndpointConditions{
				Ready:       ptr.To(true),
				Terminating: ptr.To(true),
			},
			TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "ns", Name: "p1"},
		}},
	}
	c := newDrainTestClient(t, slice)
	in, err := IsPodInRotation(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if in {
		t.Errorf("terminating endpoint must not be reported as in-rotation")
	}
}

func TestIsPodInRotation_ReadyTrueNotTerminatingReportedInRotation(t *testing.T) {
	pod := testPod("ns", "p1")
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s",
			Namespace: "ns",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "svc"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"10.0.0.1"},
			Conditions: discoveryv1.EndpointConditions{
				Ready:       ptr.To(true),
				Terminating: ptr.To(false),
			},
			TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "ns", Name: "p1"},
		}},
	}
	c := newDrainTestClient(t, slice)
	in, err := IsPodInRotation(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !in {
		t.Errorf("non-terminating Ready endpoint must be reported as in-rotation")
	}
}

// TestBatcher_IsPodRoutedAbstainsWithoutSlices: the drain gate needs an
// answer either way and resolves an empty slice list against the
// Service's existence. An observation of the pod must not: a missing or
// not-yet-propagated Service is a fact about the Service, not evidence
// that the pod stopped serving, so the reading abstains and the pod
// stays routed — and the Service lookup the gate needs is never made.
func TestBatcher_IsPodRoutedAbstainsWithoutSlices(t *testing.T) {
	tests := []struct {
		name string
		objs []client.Object
	}{
		{name: "absent service"},
		{
			name: "service without slices",
			objs: []client.Object{&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "ns"}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cr := &countingReader{Reader: newDrainTestClient(t, tt.objs...)}
			routed, err := NewBatcher(cr, "ns").IsPodRouted(context.Background(), "svc", testPod("ns", "p1"))
			if err != nil {
				t.Fatalf("IsPodRouted: %v", err)
			}
			if !routed {
				t.Errorf("IsPodRouted: got false want true; an empty slice list is not a reading")
			}
			if cr.serviceGets != 0 {
				t.Errorf("Service GETs: got %d want 0; the abstain needs no Service lookup", cr.serviceGets)
			}
		})
	}
}

// TestBatcher_IsPodRoutedReadsTheSameSlicesAsTheGate pins the two views
// against one read: with slices present they are exact opposites, and
// the LIST behind them runs once.
func TestBatcher_IsPodRoutedReadsTheSameSlicesAsTheGate(t *testing.T) {
	pod := testPod("ns", "p1")
	tests := []struct {
		name       string
		endpoints  []endpointSpec
		wantRouted bool
	}{
		{
			name:       "ready endpoint",
			endpoints:  []endpointSpec{{podName: "p1", address: "10.0.0.1", ready: ptr.To(true)}},
			wantRouted: true,
		},
		{
			name:      "endpoint for another pod",
			endpoints: []endpointSpec{{podName: "p-other", address: "10.0.0.2", ready: ptr.To(true)}},
		},
		{
			name:      "endpoint not ready",
			endpoints: []endpointSpec{{podName: "p1", address: "10.0.0.1", ready: ptr.To(false)}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cr := &countingReader{Reader: newDrainTestClient(t, sliceForService("ns", "svc-1", "svc", tt.endpoints...))}
			b := NewBatcher(cr, "ns")
			routed, err := b.IsPodRouted(context.Background(), "svc", pod)
			if err != nil {
				t.Fatalf("IsPodRouted: %v", err)
			}
			drained, err := b.IsPodDrained(context.Background(), "svc", pod)
			if err != nil {
				t.Fatalf("IsPodDrained: %v", err)
			}
			if routed != tt.wantRouted || drained == routed {
				t.Errorf("routed=%v drained=%v, want routed=%v and the two opposed", routed, drained, tt.wantRouted)
			}
			if cr.sliceLists != 1 {
				t.Errorf("EndpointSlice LISTs: got %d want 1 for both views", cr.sliceLists)
			}
		})
	}
}

func TestBatcher_IsPodRoutedRejectsInvalidInput(t *testing.T) {
	cr := &countingReader{Reader: newDrainTestClient(t)}
	b := NewBatcher(cr, "ns")
	if _, err := b.IsPodRouted(context.Background(), "svc", nil); err == nil {
		t.Error("expected error for nil pod")
	}
	if _, err := b.IsPodRouted(context.Background(), "", testPod("ns", "p1")); err == nil {
		t.Error("expected error for empty service name")
	}
	if cr.sliceLists != 0 || cr.serviceGets != 0 {
		t.Errorf("invalid inputs must perform no reads, got LIST=%d GET=%d", cr.sliceLists, cr.serviceGets)
	}
}
