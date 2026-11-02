package istio

import (
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
)

func newISVC(name, ns string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
}

func translate(t *testing.T, isvc *v1beta1.InferenceService, intent *traffic.ResolvedIntent) (*unstructured.Unstructured, []string) {
	t.Helper()
	obj, passes, err := New().Translate(isvc, nil, intent)
	if err != nil {
		t.Fatalf("Translate err = %v", err)
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		t.Fatalf("Translate returned %T, want *unstructured.Unstructured", obj)
	}
	return u, passes
}

func nestedString(t *testing.T, u *unstructured.Unstructured, path ...string) string {
	t.Helper()
	v, found, err := unstructured.NestedString(u.Object, path...)
	if err != nil || !found {
		t.Fatalf("NestedString %v: found=%v err=%v", path, found, err)
	}
	return v
}

func nestedInt64(t *testing.T, u *unstructured.Unstructured, path ...string) int64 {
	t.Helper()
	v, found, err := unstructured.NestedInt64(u.Object, path...)
	if err != nil || !found {
		t.Fatalf("NestedInt64 %v: found=%v err=%v", path, found, err)
	}
	return v
}

func TestName_AndWatches(t *testing.T) {
	tr := New()
	if tr.Name() != Name {
		t.Fatalf("Name = %q, want %q", tr.Name(), Name)
	}
	w := tr.Watches()
	u, ok := w.(*unstructured.Unstructured)
	if !ok {
		t.Fatalf("Watches returned %T", w)
	}
	if got := u.GroupVersionKind(); got != PolicyGVK {
		t.Fatalf("Watches GVK = %v, want %v", got, PolicyGVK)
	}
}

func TestSupportedAnnotations_SubsetOfEG(t *testing.T) {
	// The Istio DR API doesn't model retries (those live on
	// VirtualService) or per-endpoint cb limits or max-connection
	// duration. SupportedAnnotations must therefore be a STRICT subset
	// of the EG translator's matrix.
	got := New().SupportedAnnotations()
	for _, k := range []string{
		constants.CircuitBreakerMaxConnectionsAnnotation,
		constants.CircuitBreakerMaxParallelRequestsAnnotation,
		constants.CircuitBreakerMaxPendingRequestsAnnotation,
		constants.TimeoutIdleAnnotation,
		constants.TimeoutTCPConnectAnnotation,
	} {
		if !got.Has(k) {
			t.Errorf("Istio translator must support %q", k)
		}
	}
	// Explicitly unsupported (these surface as UnsupportedField when
	// Istio is active).
	for _, k := range []string{
		constants.RetryAttemptsAnnotation,
		constants.RetryOnAnnotation,
		constants.RetryPerTryTimeoutAnnotation,
		constants.CircuitBreakerMaxParallelRetriesAnnotation,
		constants.CircuitBreakerPerEndpointMaxConnectionsAnnotation,
		constants.TimeoutMaxConnectionDurationAnnotation,
	} {
		if got.Has(k) {
			t.Errorf("Istio translator must NOT advertise support for %q", k)
		}
	}
}

func TestSupportedPassthroughPrefixes(t *testing.T) {
	got := New().SupportedPassthroughPrefixes()
	if len(got) != 1 || got[0] != constants.PassthroughIstioPrefix {
		t.Fatalf("got %v, want [%q]", got, constants.PassthroughIstioPrefix)
	}
}

func TestTranslate_HostAndMetadata(t *testing.T) {
	u, _ := translate(t, newISVC("isvc", "ns"), &traffic.ResolvedIntent{})
	if u.GetName() != "isvc" || u.GetNamespace() != "ns" {
		t.Fatalf("name/ns = %q/%q", u.GetName(), u.GetNamespace())
	}
	if u.GroupVersionKind() != PolicyGVK {
		t.Fatalf("GVK = %v", u.GroupVersionKind())
	}
	if got := nestedString(t, u, "spec", "host"); got != "isvc.ns.svc.cluster.local" {
		t.Fatalf("host = %q", got)
	}
}

func TestTranslate_LoadBalancer_AlgorithmMapping(t *testing.T) {
	cases := []struct {
		in   v1beta1.LoadBalancingType
		want string
	}{
		{v1beta1.LoadBalancingTypeRoundRobin, "ROUND_ROBIN"},
		{v1beta1.LoadBalancingTypeLeastRequest, "LEAST_REQUEST"},
		{v1beta1.LoadBalancingTypeRandom, "RANDOM"},
	}
	for _, tc := range cases {
		t.Run(string(tc.in), func(t *testing.T) {
			alg := tc.in
			u, _ := translate(t, newISVC("isvc", "ns"),
				&traffic.ResolvedIntent{Traffic: &v1beta1.TrafficSpec{Algorithm: &alg}})
			if got := nestedString(t, u, "spec", "trafficPolicy", "loadBalancer", "simple"); got != tc.want {
				t.Fatalf("simple = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTranslate_LoadBalancer_ConsistentHash_Header(t *testing.T) {
	alg := v1beta1.LoadBalancingTypeConsistentHash
	u, _ := translate(t, newISVC("isvc", "ns"), &traffic.ResolvedIntent{
		Traffic: &v1beta1.TrafficSpec{
			Algorithm: &alg,
			ConsistentHash: &v1beta1.ConsistentHashSpec{
				Type:    v1beta1.HashTypeHeader,
				Headers: []v1beta1.HashHeader{{Name: "X-Session-ID"}, {Name: "X-User-ID"}},
			},
		},
	})
	// Istio uses httpHeaderName (single string) — translator pins to first.
	if got := nestedString(t, u, "spec", "trafficPolicy", "loadBalancer", "consistentHash", "httpHeaderName"); got != "X-Session-ID" {
		t.Fatalf("httpHeaderName = %q", got)
	}
	// simple must NOT be set when ConsistentHash is in play.
	if _, found, _ := unstructured.NestedString(u.Object, "spec", "trafficPolicy", "loadBalancer", "simple"); found {
		t.Fatalf("simple must not be set alongside consistentHash")
	}
}

func TestTranslate_LoadBalancer_ConsistentHash_Cookie(t *testing.T) {
	alg := v1beta1.LoadBalancingTypeConsistentHash
	ttl := int64(60)
	u, _ := translate(t, newISVC("isvc", "ns"), &traffic.ResolvedIntent{
		Traffic: &v1beta1.TrafficSpec{
			Algorithm: &alg,
			ConsistentHash: &v1beta1.ConsistentHashSpec{
				Type:   v1beta1.HashTypeCookie,
				Cookie: &v1beta1.HashCookie{Name: "sticky", TTLSeconds: &ttl},
			},
		},
	})
	if got := nestedString(t, u, "spec", "trafficPolicy", "loadBalancer", "consistentHash", "httpCookie", "name"); got != "sticky" {
		t.Fatalf("cookie.name = %q", got)
	}
	if got := nestedString(t, u, "spec", "trafficPolicy", "loadBalancer", "consistentHash", "httpCookie", "ttl"); got != "60s" {
		t.Fatalf("cookie.ttl = %q", got)
	}
}

func TestTranslate_LoadBalancer_ConsistentHash_SourceIP(t *testing.T) {
	alg := v1beta1.LoadBalancingTypeConsistentHash
	u, _ := translate(t, newISVC("isvc", "ns"), &traffic.ResolvedIntent{
		Traffic: &v1beta1.TrafficSpec{
			Algorithm:      &alg,
			ConsistentHash: &v1beta1.ConsistentHashSpec{Type: v1beta1.HashTypeSourceIP},
		},
	})
	v, found, err := unstructured.NestedBool(u.Object, "spec", "trafficPolicy", "loadBalancer", "consistentHash", "useSourceIp")
	if err != nil || !found || !v {
		t.Fatalf("useSourceIp = %v, found=%v err=%v; want true", v, found, err)
	}
}

func TestTranslate_CircuitBreaker_SupportedFields(t *testing.T) {
	cb := &traffic.CircuitBreakerIntent{
		MaxConnections:      i32p(1024),
		MaxParallelRequests: i32p(2048),
		MaxPendingRequests:  i32p(512),
		// MaxParallelRetries + PerEndpointMaxConnections intentionally
		// set — they MUST NOT appear on the emitted DR (no Istio analogue).
		MaxParallelRetries:        i32p(8),
		PerEndpointMaxConnections: i32p(1),
	}
	u, _ := translate(t, newISVC("isvc", "ns"), &traffic.ResolvedIntent{CircuitBreaker: cb})

	if got := nestedInt64(t, u, "spec", "trafficPolicy", "connectionPool", "tcp", "maxConnections"); got != 1024 {
		t.Fatalf("maxConnections = %d", got)
	}
	if got := nestedInt64(t, u, "spec", "trafficPolicy", "connectionPool", "http", "http2MaxRequests"); got != 2048 {
		t.Fatalf("http2MaxRequests = %d", got)
	}
	if got := nestedInt64(t, u, "spec", "trafficPolicy", "connectionPool", "http", "http1MaxPendingRequests"); got != 512 {
		t.Fatalf("http1MaxPendingRequests = %d", got)
	}
	// Unsupported fields must NOT appear anywhere on the DR.
	tp, found, _ := unstructured.NestedMap(u.Object, "spec", "trafficPolicy")
	if !found {
		t.Fatalf("trafficPolicy missing")
	}
	if _, hasMPR := tp["maxParallelRetries"]; hasMPR {
		t.Errorf("maxParallelRetries must not appear (no Istio analogue)")
	}
	// perEndpoint is an Envoy concept; ensure it's absent.
	cp, _, _ := unstructured.NestedMap(u.Object, "spec", "trafficPolicy", "connectionPool")
	if _, hasPE := cp["perEndpoint"]; hasPE {
		t.Errorf("perEndpoint must not appear")
	}
}

func TestTranslate_Timeout_SupportedFields(t *testing.T) {
	idle := 30 * time.Second
	mcd := 5 * time.Minute
	tcp := 2 * time.Second
	to := &traffic.TimeoutIntent{
		Idle:                  &idle,
		MaxConnectionDuration: &mcd, // unsupported — must NOT appear
		TCPConnect:            &tcp,
	}
	u, _ := translate(t, newISVC("isvc", "ns"), &traffic.ResolvedIntent{Timeout: to})

	if got := nestedString(t, u, "spec", "trafficPolicy", "connectionPool", "http", "idleTimeout"); got != "30s" {
		t.Fatalf("idleTimeout = %q", got)
	}
	if got := nestedString(t, u, "spec", "trafficPolicy", "connectionPool", "tcp", "connectTimeout"); got != "2s" {
		t.Fatalf("tcpConnect = %q", got)
	}
	// MaxConnectionDuration must NOT appear (no Istio DR analogue).
	cp, _, _ := unstructured.NestedMap(u.Object, "spec", "trafficPolicy", "connectionPool")
	for _, m := range []string{"maxConnectionDuration", "maxStreamDuration"} {
		if _, has := cp[m]; has {
			t.Errorf("%s must not appear on Istio DR", m)
		}
	}
}

func TestTranslate_Retry_IsSilentlyIgnored(t *testing.T) {
	// Retry intent must not appear on the DR — Istio retries live on
	// VirtualService. The reconciler surfaces dropped retry-* annotations
	// as UnsupportedField; the translator just doesn't emit them.
	r := &traffic.RetryIntent{Attempts: i32p(3), RetryOn: []string{"5xx"}}
	u, _ := translate(t, newISVC("isvc", "ns"), &traffic.ResolvedIntent{Retry: r})
	if _, found, _ := unstructured.NestedMap(u.Object, "spec", "trafficPolicy", "retry"); found {
		t.Fatalf("retry must not appear on Istio DR")
	}
}

func TestTranslate_Passthrough_StitchedUnderTrafficPolicy(t *testing.T) {
	intent := &traffic.ResolvedIntent{
		PassthroughIstio: map[string]string{
			"connectionPool.tcp.tcpKeepalive.time":  "30s",
			"outlierDetection.consecutive5xxErrors": "5",
		},
	}
	u, passes := translate(t, newISVC("isvc", "ns"), intent)
	if got := nestedString(t, u, "spec", "trafficPolicy", "connectionPool", "tcp", "tcpKeepalive", "time"); got != "30s" {
		t.Fatalf("tcpKeepalive.time = %q", got)
	}
	if got := nestedInt64(t, u, "spec", "trafficPolicy", "outlierDetection", "consecutive5xxErrors"); got != 5 {
		t.Fatalf("consecutive5xxErrors = %d", got)
	}
	wantPasses := []string{
		"connectionPool.tcp.tcpKeepalive.time",
		"outlierDetection.consecutive5xxErrors",
	}
	if !reflect.DeepEqual(passes, wantPasses) {
		t.Fatalf("passes = %v, want sorted %v", passes, wantPasses)
	}
}

func TestTranslate_Determinism(t *testing.T) {
	alg := v1beta1.LoadBalancingTypeConsistentHash
	intent := &traffic.ResolvedIntent{
		Traffic: &v1beta1.TrafficSpec{
			Algorithm: &alg,
			ConsistentHash: &v1beta1.ConsistentHashSpec{
				Type:    v1beta1.HashTypeHeader,
				Headers: []v1beta1.HashHeader{{Name: "X-User"}},
			},
		},
		CircuitBreaker: &traffic.CircuitBreakerIntent{MaxConnections: i32p(100)},
		PassthroughIstio: map[string]string{
			"a.b": "1", "x.y": "2",
		},
	}
	a, p1 := translate(t, newISVC("isvc", "ns"), intent)
	b, p2 := translate(t, newISVC("isvc", "ns"), intent)
	if !reflect.DeepEqual(a.Object, b.Object) {
		t.Fatalf("non-deterministic output")
	}
	if !reflect.DeepEqual(p1, p2) {
		t.Fatalf("non-deterministic passes")
	}
}

// --- ObserveAcceptance tests ---

func drWithConditions(conds []map[string]interface{}) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(PolicyGVK)
	raw := make([]interface{}, 0, len(conds))
	for _, c := range conds {
		raw = append(raw, c)
	}
	_ = unstructured.SetNestedSlice(u.Object, raw, "status", "conditions")
	return u
}

func TestObserveAcceptance_Reconciled_True(t *testing.T) {
	u := drWithConditions([]map[string]interface{}{
		{"type": "Reconciled", "status": "True", "reason": "Reconciled", "message": "policy accepted"},
	})
	got := New().ObserveAcceptance(u)
	if got.State != traffic.AcceptanceAccepted {
		t.Fatalf("state = %d", got.State)
	}
	if got.Message != "policy accepted" {
		t.Fatalf("message = %q", got.Message)
	}
}

func TestObserveAcceptance_Reconciled_False(t *testing.T) {
	u := drWithConditions([]map[string]interface{}{
		{"type": "Reconciled", "status": "False", "reason": "Invalid", "message": "bad host"},
	})
	got := New().ObserveAcceptance(u)
	if got.State != traffic.AcceptanceRejected {
		t.Fatalf("state = %d", got.State)
	}
}

func TestObserveAcceptance_NoStatus_IsPending(t *testing.T) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(PolicyGVK)
	got := New().ObserveAcceptance(u)
	if got.State != traffic.AcceptancePending {
		t.Fatalf("state = %d", got.State)
	}
}

func TestObserveAcceptance_OtherConditionTypes_Ignored(t *testing.T) {
	u := drWithConditions([]map[string]interface{}{
		{"type": "SomethingElse", "status": "True"},
	})
	got := New().ObserveAcceptance(u)
	if got.State != traffic.AcceptancePending {
		t.Fatalf("state = %d", got.State)
	}
}

func TestSupportedTrafficFields_DeclaresDestinationRuleCoverage(t *testing.T) {
	got := New().SupportedTrafficFields()
	want := []string{
		constants.TrafficCapabilityAlgorithm,
		constants.TrafficCapabilityHashHeader,
		constants.TrafficCapabilityHashCookie,
		constants.TrafficCapabilityHashSourceIP,
	}
	for _, capability := range want {
		if !got.Has(capability) {
			t.Errorf("SupportedTrafficFields() missing %q", capability)
		}
	}
	// httpHeaderName is a single string, so multi-header concatenation
	// must NOT be declared; endpoint override has no DR analogue.
	for _, capability := range []string{
		constants.TrafficCapabilityHashMultipleHeaders,
		constants.TrafficCapabilityEndpointOverrideHeader,
		constants.TrafficCapabilityEndpointOverrideMetadata,
	} {
		if got.Has(capability) {
			t.Errorf("SupportedTrafficFields() must not declare %q", capability)
		}
	}
	if got.Len() != len(want) {
		t.Errorf("SupportedTrafficFields() has %d tokens, want %d: %v", got.Len(), len(want), got.UnsortedList())
	}
}

func i32p(v int32) *int32 { return &v }
