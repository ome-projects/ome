package envoygateway

import (
	"encoding/json"
	"reflect"
	"sort"
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

func translate(t *testing.T, isvc *v1beta1.InferenceService, routes []string, intent *traffic.ResolvedIntent) (*unstructured.Unstructured, []string) {
	t.Helper()
	obj, passes, err := New().Translate(isvc, routes, intent)
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
	if err != nil {
		t.Fatalf("NestedString %v: %v", path, err)
	}
	if !found {
		t.Fatalf("path %v not found in %s", path, mustJSON(t, u))
	}
	return v
}

func nestedInt64(t *testing.T, u *unstructured.Unstructured, path ...string) int64 {
	t.Helper()
	v, found, err := unstructured.NestedInt64(u.Object, path...)
	if err != nil {
		t.Fatalf("NestedInt64 %v: %v", path, err)
	}
	if !found {
		t.Fatalf("path %v not found in %s", path, mustJSON(t, u))
	}
	return v
}

func mustJSON(t *testing.T, u *unstructured.Unstructured) string {
	t.Helper()
	b, err := json.MarshalIndent(u.Object, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

func TestName_AndWatches(t *testing.T) {
	tr := New()
	if tr.Name() != Name {
		t.Fatalf("Name = %q, want %q", tr.Name(), Name)
	}
	w := tr.Watches()
	if w == nil {
		t.Fatalf("Watches must return a non-nil object")
	}
	u, ok := w.(*unstructured.Unstructured)
	if !ok {
		t.Fatalf("Watches returned %T, want *unstructured.Unstructured", w)
	}
	if got := u.GroupVersionKind(); got != PolicyGVK {
		t.Fatalf("Watches GVK = %v, want %v", got, PolicyGVK)
	}
}

func TestSupportedAnnotations(t *testing.T) {
	got := New().SupportedAnnotations()
	// Spot-check a few known keys; full coverage would just mirror the
	// constants list back to itself.
	for _, k := range []string{
		constants.CircuitBreakerMaxConnectionsAnnotation,
		constants.RetryAttemptsAnnotation,
		constants.TimeoutIdleAnnotation,
	} {
		if !got.Has(k) {
			t.Errorf("SupportedAnnotations missing %q", k)
		}
	}
	// Pass-through is per-prefix and must NOT be in the per-key set.
	if got.Has(constants.PassthroughEnvoyGatewayPrefix) {
		t.Errorf("SupportedAnnotations must not include pass-through prefix")
	}
}

func TestTranslate_TargetRefsAndMetadata(t *testing.T) {
	isvc := newISVC("isvc", "ns")
	alg := v1beta1.LoadBalancingTypeRoundRobin
	intent := &traffic.ResolvedIntent{Traffic: &v1beta1.TrafficSpec{Algorithm: &alg}}
	routes := []string{"isvc", "isvc-engine", "isvc-decoder"}

	u, _ := translate(t, isvc, routes, intent)

	if u.GetName() != "isvc" || u.GetNamespace() != "ns" {
		t.Fatalf("name/ns = %q/%q", u.GetName(), u.GetNamespace())
	}
	if u.GroupVersionKind() != PolicyGVK {
		t.Fatalf("GVK = %v, want %v", u.GroupVersionKind(), PolicyGVK)
	}
	refs, found, err := unstructured.NestedSlice(u.Object, "spec", "targetRefs")
	if err != nil || !found {
		t.Fatalf("targetRefs missing: %v", err)
	}
	if len(refs) != len(routes) {
		t.Fatalf("targetRefs len = %d, want %d", len(refs), len(routes))
	}
	for i, r := range refs {
		m, ok := r.(map[string]interface{})
		if !ok {
			t.Fatalf("ref %d not a map: %T", i, r)
		}
		if m["group"] != groupGatewayAPI || m["kind"] != kindHTTPRoute || m["name"] != routes[i] {
			t.Fatalf("ref %d = %+v, want HTTPRoute %q", i, m, routes[i])
		}
	}
}

func TestTranslate_LoadBalancer_AlgorithmOnly(t *testing.T) {
	alg := v1beta1.LoadBalancingTypeLeastRequest
	u, _ := translate(t, newISVC("isvc", "ns"), []string{"isvc"},
		&traffic.ResolvedIntent{Traffic: &v1beta1.TrafficSpec{Algorithm: &alg}})

	if got := nestedString(t, u, "spec", "loadBalancer", "type"); got != "LeastRequest" {
		t.Fatalf("loadBalancer.type = %q", got)
	}
}

func TestTranslate_ConsistentHash_Header(t *testing.T) {
	alg := v1beta1.LoadBalancingTypeConsistentHash
	intent := &traffic.ResolvedIntent{
		Traffic: &v1beta1.TrafficSpec{
			Algorithm: &alg,
			ConsistentHash: &v1beta1.ConsistentHashSpec{
				Type:    v1beta1.HashTypeHeader,
				Headers: []v1beta1.HashHeader{{Name: "X-Session-ID"}},
			},
		},
	}
	u, _ := translate(t, newISVC("isvc", "ns"), []string{"isvc"}, intent)

	if got := nestedString(t, u, "spec", "loadBalancer", "consistentHash", "type"); got != "Header" {
		t.Fatalf("consistentHash.type = %q", got)
	}
	if got := nestedString(t, u, "spec", "loadBalancer", "consistentHash", "header", "name"); got != "X-Session-ID" {
		t.Fatalf("consistentHash.header.name = %q", got)
	}
}

func TestTranslate_ConsistentHash_MultiHeader_PluralForm(t *testing.T) {
	// EG v1.7+ adds the "Headers" (plural) consistent-hash type and a
	// headers[] list — the singular "Header"/header is marked Deprecated
	// in v1.7 but kept for back-compat. The OME API exposes only the
	// singular "Header" enum value; the translator promotes the EG-side
	// type to "Headers" when more than one header is declared.
	alg := v1beta1.LoadBalancingTypeConsistentHash
	intent := &traffic.ResolvedIntent{
		Traffic: &v1beta1.TrafficSpec{
			Algorithm: &alg,
			ConsistentHash: &v1beta1.ConsistentHashSpec{
				Type:    v1beta1.HashTypeHeader,
				Headers: []v1beta1.HashHeader{{Name: "X-Session-ID"}, {Name: "X-User-ID"}},
			},
		},
	}
	u, _ := translate(t, newISVC("isvc", "ns"), []string{"isvc"}, intent)

	if got := nestedString(t, u, "spec", "loadBalancer", "consistentHash", "type"); got != "Headers" {
		t.Fatalf("multi-header must promote type to Headers (plural), got %q", got)
	}
	// Singular header field must NOT be set (CEL validation in EG
	// requires header XOR headers per type).
	if _, found, _ := unstructured.NestedMap(u.Object, "spec", "loadBalancer", "consistentHash", "header"); found {
		t.Fatalf("type=Headers must not set the singular header field; got header present")
	}
	headers, found, err := unstructured.NestedSlice(u.Object, "spec", "loadBalancer", "consistentHash", "headers")
	if err != nil || !found {
		t.Fatalf("consistentHash.headers missing: err=%v", err)
	}
	if len(headers) != 2 {
		t.Fatalf("len(headers)=%d, want 2", len(headers))
	}
	for i, want := range []string{"X-Session-ID", "X-User-ID"} {
		got := headers[i].(map[string]interface{})["name"]
		if got != want {
			t.Fatalf("headers[%d].name = %v, want %q", i, got, want)
		}
	}
}

func TestTranslate_ConsistentHash_Cookie_WithTTL(t *testing.T) {
	alg := v1beta1.LoadBalancingTypeConsistentHash
	ttl := int64(60)
	intent := &traffic.ResolvedIntent{
		Traffic: &v1beta1.TrafficSpec{
			Algorithm: &alg,
			ConsistentHash: &v1beta1.ConsistentHashSpec{
				Type:   v1beta1.HashTypeCookie,
				Cookie: &v1beta1.HashCookie{Name: "sticky", TTLSeconds: &ttl},
			},
		},
	}
	u, _ := translate(t, newISVC("isvc", "ns"), []string{"isvc"}, intent)
	if got := nestedString(t, u, "spec", "loadBalancer", "consistentHash", "cookie", "name"); got != "sticky" {
		t.Fatalf("cookie.name = %q", got)
	}
	if got := nestedString(t, u, "spec", "loadBalancer", "consistentHash", "cookie", "ttl"); got != "60s" {
		t.Fatalf("cookie.ttl = %q, want 60s", got)
	}
}

func TestTranslate_ConsistentHash_Cookie_WithoutTTL_OmitsField(t *testing.T) {
	alg := v1beta1.LoadBalancingTypeConsistentHash
	intent := &traffic.ResolvedIntent{
		Traffic: &v1beta1.TrafficSpec{
			Algorithm: &alg,
			ConsistentHash: &v1beta1.ConsistentHashSpec{
				Type:   v1beta1.HashTypeCookie,
				Cookie: &v1beta1.HashCookie{Name: "sticky"},
			},
		},
	}
	u, _ := translate(t, newISVC("isvc", "ns"), []string{"isvc"}, intent)
	_, found, _ := unstructured.NestedString(u.Object, "spec", "loadBalancer", "consistentHash", "cookie", "ttl")
	if found {
		t.Fatalf("ttl must be omitted when TTLSeconds nil")
	}
}

func TestTranslate_ConsistentHash_SourceIP_TypeOnly(t *testing.T) {
	alg := v1beta1.LoadBalancingTypeConsistentHash
	intent := &traffic.ResolvedIntent{
		Traffic: &v1beta1.TrafficSpec{
			Algorithm:      &alg,
			ConsistentHash: &v1beta1.ConsistentHashSpec{Type: v1beta1.HashTypeSourceIP},
		},
	}
	u, _ := translate(t, newISVC("isvc", "ns"), []string{"isvc"}, intent)
	if got := nestedString(t, u, "spec", "loadBalancer", "consistentHash", "type"); got != "SourceIP" {
		t.Fatalf("type = %q", got)
	}
	// No header or cookie subfields.
	if _, found, _ := unstructured.NestedMap(u.Object, "spec", "loadBalancer", "consistentHash", "header"); found {
		t.Fatalf("source-IP hash must not set header")
	}
	if _, found, _ := unstructured.NestedMap(u.Object, "spec", "loadBalancer", "consistentHash", "cookie"); found {
		t.Fatalf("source-IP hash must not set cookie")
	}
}

func TestTranslate_EndpointOverride_Header(t *testing.T) {
	intent := &traffic.ResolvedIntent{
		Traffic: &v1beta1.TrafficSpec{
			EndpointOverride: &v1beta1.EndpointOverrideSpec{
				Type:    v1beta1.EndpointOverrideTypeHeader,
				Headers: []v1beta1.HashHeader{{Name: "X-Endpoint-HostPort"}, {Name: "X-Endpoint-IP"}},
			},
		},
	}
	u, _ := translate(t, newISVC("isvc", "ns"), []string{"isvc"}, intent)
	extract, found, err := unstructured.NestedSlice(u.Object, "spec", "loadBalancer", "endpointOverride", "extractFrom")
	if err != nil || !found {
		t.Fatalf("extractFrom missing: %v", err)
	}
	if len(extract) != 2 {
		t.Fatalf("len(extractFrom) = %d, want 2", len(extract))
	}
	for i, want := range []string{"X-Endpoint-HostPort", "X-Endpoint-IP"} {
		got := extract[i].(map[string]interface{})["header"]
		if got != want {
			t.Fatalf("extract[%d].header = %v, want %q", i, got, want)
		}
	}
}

func TestTranslate_CircuitBreaker_AllFields(t *testing.T) {
	cb := &traffic.CircuitBreakerIntent{
		MaxConnections:            i32p(1024),
		MaxParallelRequests:       i32p(2048),
		MaxPendingRequests:        i32p(512),
		MaxParallelRetries:        i32p(8),
		PerEndpointMaxConnections: i32p(1),
	}
	u, _ := translate(t, newISVC("isvc", "ns"), []string{"isvc"},
		&traffic.ResolvedIntent{CircuitBreaker: cb})

	if got := nestedInt64(t, u, "spec", "circuitBreaker", "maxConnections"); got != 1024 {
		t.Fatalf("maxConnections = %d", got)
	}
	if got := nestedInt64(t, u, "spec", "circuitBreaker", "maxParallelRequests"); got != 2048 {
		t.Fatalf("maxParallelRequests = %d", got)
	}
	if got := nestedInt64(t, u, "spec", "circuitBreaker", "maxPendingRequests"); got != 512 {
		t.Fatalf("maxPendingRequests = %d", got)
	}
	if got := nestedInt64(t, u, "spec", "circuitBreaker", "maxParallelRetries"); got != 8 {
		t.Fatalf("maxParallelRetries = %d", got)
	}
	if got := nestedInt64(t, u, "spec", "circuitBreaker", "perEndpoint", "maxConnections"); got != 1 {
		t.Fatalf("perEndpoint.maxConnections = %d", got)
	}
}

func TestTranslate_CircuitBreaker_PartialSet_OmitsRest(t *testing.T) {
	cb := &traffic.CircuitBreakerIntent{MaxConnections: i32p(100)}
	u, _ := translate(t, newISVC("isvc", "ns"), []string{"isvc"},
		&traffic.ResolvedIntent{CircuitBreaker: cb})

	if got := nestedInt64(t, u, "spec", "circuitBreaker", "maxConnections"); got != 100 {
		t.Fatalf("maxConnections = %d", got)
	}
	for _, p := range [][]string{
		{"spec", "circuitBreaker", "maxParallelRequests"},
		{"spec", "circuitBreaker", "maxPendingRequests"},
		{"spec", "circuitBreaker", "maxParallelRetries"},
		{"spec", "circuitBreaker", "perEndpoint"},
	} {
		if _, found, _ := unstructured.NestedFieldNoCopy(u.Object, p...); found {
			t.Errorf("path %v must be omitted when intent is nil", p)
		}
	}
}

func TestTranslate_Retry_SplitsNumericIntoHttpStatusCodes(t *testing.T) {
	per := 5 * time.Second
	r := &traffic.RetryIntent{
		Attempts:      i32p(3),
		RetryOn:       []string{"5xx", "reset", "503", "gateway-error", "504"},
		PerTryTimeout: &per,
	}
	u, _ := translate(t, newISVC("isvc", "ns"), []string{"isvc"},
		&traffic.ResolvedIntent{Retry: r})

	if got := nestedInt64(t, u, "spec", "retry", "numRetries"); got != 3 {
		t.Fatalf("numRetries = %d", got)
	}
	if got := nestedString(t, u, "spec", "retry", "perRetry", "timeout"); got != "5s" {
		t.Fatalf("perRetry.timeout = %q", got)
	}
	triggers, found, err := unstructured.NestedSlice(u.Object, "spec", "retry", "retryOn", "triggers")
	if err != nil || !found {
		t.Fatalf("triggers missing: %v", err)
	}
	wantTriggers := []string{"5xx", "reset", "gateway-error"}
	if len(triggers) != len(wantTriggers) {
		t.Fatalf("triggers = %v, want %v", triggers, wantTriggers)
	}
	for i, w := range wantTriggers {
		if triggers[i] != w {
			t.Fatalf("trigger[%d] = %v, want %q", i, triggers[i], w)
		}
	}
	codes, found, _ := unstructured.NestedSlice(u.Object, "spec", "retry", "retryOn", "httpStatusCodes")
	if !found {
		t.Fatalf("httpStatusCodes missing")
	}
	wantCodes := []int64{503, 504}
	if len(codes) != len(wantCodes) {
		t.Fatalf("codes = %v, want %v", codes, wantCodes)
	}
	for i, w := range wantCodes {
		if codes[i] != w {
			t.Fatalf("code[%d] = %v, want %d", i, codes[i], w)
		}
	}
}

func TestTranslate_Retry_OnlyTriggers_OmitsCodesField(t *testing.T) {
	r := &traffic.RetryIntent{Attempts: i32p(1), RetryOn: []string{"reset"}}
	u, _ := translate(t, newISVC("isvc", "ns"), []string{"isvc"},
		&traffic.ResolvedIntent{Retry: r})

	if _, found, _ := unstructured.NestedSlice(u.Object, "spec", "retry", "retryOn", "httpStatusCodes"); found {
		t.Fatalf("httpStatusCodes must be omitted when no numeric tokens")
	}
}

func TestTranslate_Timeout_AllFields(t *testing.T) {
	idle := 30 * time.Second
	mcd := 5 * time.Minute
	tcp := 2 * time.Second
	to := &traffic.TimeoutIntent{Idle: &idle, MaxConnectionDuration: &mcd, TCPConnect: &tcp}
	u, _ := translate(t, newISVC("isvc", "ns"), []string{"isvc"},
		&traffic.ResolvedIntent{Timeout: to})

	if got := nestedString(t, u, "spec", "timeout", "http", "connectionIdleTimeout"); got != "30s" {
		t.Fatalf("idle = %q", got)
	}
	if got := nestedString(t, u, "spec", "timeout", "http", "maxConnectionDuration"); got != "5m0s" {
		t.Fatalf("maxConnectionDuration = %q", got)
	}
	if got := nestedString(t, u, "spec", "timeout", "tcp", "connectTimeout"); got != "2s" {
		t.Fatalf("tcpConnect = %q", got)
	}
}

func TestTranslate_Passthrough_StitchesAndOverrides(t *testing.T) {
	alg := v1beta1.LoadBalancingTypeRoundRobin
	intent := &traffic.ResolvedIntent{
		Traffic: &v1beta1.TrafficSpec{Algorithm: &alg},
		// Pass-through wins: this should overwrite loadBalancer.type set above.
		PassthroughEnvoyGateway: map[string]string{
			"loadBalancer.type":             "LeastRequest",
			"loadBalancer.slowStart.window": "30s",
			"loadBalancer.slowStart.aggro":  "1.5",
			"retry.numRetries":              "7",
			"someToggle":                    "true",
		},
	}
	u, passes := translate(t, newISVC("isvc", "ns"), []string{"isvc"}, intent)

	// Pass-through must win.
	if got := nestedString(t, u, "spec", "loadBalancer", "type"); got != "LeastRequest" {
		t.Fatalf("pass-through did not override loadBalancer.type: got %q", got)
	}
	// Nested string survives.
	if got := nestedString(t, u, "spec", "loadBalancer", "slowStart", "window"); got != "30s" {
		t.Fatalf("slowStart.window = %q", got)
	}
	// Numeric parses as int64.
	if got := nestedInt64(t, u, "spec", "retry", "numRetries"); got != 7 {
		t.Fatalf("retry.numRetries = %d", got)
	}
	// Float parses as float (verify via raw access).
	v, _, _ := unstructured.NestedFieldNoCopy(u.Object, "spec", "loadBalancer", "slowStart", "aggro")
	if _, ok := v.(float64); !ok {
		t.Fatalf("aggro = %v (%T), want float64", v, v)
	}
	// Bool parses as bool.
	v, _, _ = unstructured.NestedFieldNoCopy(u.Object, "spec", "someToggle")
	if got, ok := v.(bool); !ok || !got {
		t.Fatalf("someToggle = %v (%T), want true", v, v)
	}
	// Returned passthrough list is sorted for deterministic status output.
	wantPasses := []string{
		"loadBalancer.slowStart.aggro",
		"loadBalancer.slowStart.window",
		"loadBalancer.type",
		"retry.numRetries",
		"someToggle",
	}
	if !reflect.DeepEqual(passes, wantPasses) {
		t.Fatalf("passes = %v, want sorted %v", passes, wantPasses)
	}
}

func TestTranslate_Determinism_ByteIdenticalOutput(t *testing.T) {
	// Same input twice must produce byte-identical JSON. The reconciler's
	// coarse Update relies on this to avoid noisy API server writes.
	alg := v1beta1.LoadBalancingTypeConsistentHash
	intent := &traffic.ResolvedIntent{
		Traffic: &v1beta1.TrafficSpec{
			Algorithm: &alg,
			ConsistentHash: &v1beta1.ConsistentHashSpec{
				Type:    v1beta1.HashTypeHeader,
				Headers: []v1beta1.HashHeader{{Name: "X-Session-ID"}},
			},
		},
		CircuitBreaker: &traffic.CircuitBreakerIntent{MaxConnections: i32p(100)},
		Retry: &traffic.RetryIntent{
			Attempts: i32p(2),
			RetryOn:  []string{"5xx", "503"},
		},
		PassthroughEnvoyGateway: map[string]string{
			"a.b": "1",
			"x.y": "2",
		},
	}
	isvc := newISVC("isvc", "ns")
	routes := []string{"isvc", "isvc-engine"}

	u1, p1 := translate(t, isvc, routes, intent)
	u2, p2 := translate(t, isvc, routes, intent)

	b1, _ := json.Marshal(u1.Object)
	b2, _ := json.Marshal(u2.Object)
	if string(b1) != string(b2) {
		t.Fatalf("Translate is not deterministic:\n a=%s\n b=%s", b1, b2)
	}
	if !reflect.DeepEqual(p1, p2) {
		t.Fatalf("passthroughs not deterministic: %v vs %v", p1, p2)
	}
	// Sanity check the sorted property explicitly.
	sortedPasses := append([]string{}, p1...)
	sort.Strings(sortedPasses)
	if !reflect.DeepEqual(p1, sortedPasses) {
		t.Fatalf("passthroughs not sorted: %v", p1)
	}
}

func TestTranslate_EmptyIntent_ProducesShellOnly(t *testing.T) {
	// Defensive: even with an "empty" intent (no fields set besides
	// empty maps), Translate produces a valid BTP shell with just
	// targetRefs. This is technically a wasted reconcile (HasIntent
	// short-circuits at the reconciler) but we don't want a panic if
	// it ever happens.
	u, passes := translate(t, newISVC("isvc", "ns"), []string{"isvc"},
		&traffic.ResolvedIntent{PassthroughEnvoyGateway: map[string]string{}})

	if u.GetName() != "isvc" {
		t.Fatalf("name = %q", u.GetName())
	}
	if _, found, _ := unstructured.NestedSlice(u.Object, "spec", "targetRefs"); !found {
		t.Fatalf("targetRefs missing")
	}
	if _, found, _ := unstructured.NestedMap(u.Object, "spec", "loadBalancer"); found {
		t.Fatalf("loadBalancer must be absent when no traffic spec")
	}
	if len(passes) != 0 {
		t.Fatalf("passes = %v, want empty", passes)
	}
}

func TestSplitRetryOnTokens_HandlesEmptyAndWhitespace(t *testing.T) {
	triggers, codes := splitRetryOnTokens([]string{"5xx", "", "503"})
	if len(triggers) != 1 || triggers[0] != "5xx" {
		t.Fatalf("triggers = %v", triggers)
	}
	if len(codes) != 1 || codes[0] != 503 {
		t.Fatalf("codes = %v", codes)
	}
}

func TestParseScalar(t *testing.T) {
	cases := []struct {
		in   string
		want interface{}
	}{
		{"42", int64(42)},
		{"-7", int64(-7)},
		{"true", true},
		{"false", false},
		{"3.14", 3.14},
		{"hello", "hello"},
		{"", ""},
		{"30s", "30s"}, // duration strings stay string
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := parseScalar(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseScalar(%q) = %v (%T), want %v (%T)", tc.in, got, got, tc.want, tc.want)
			}
		})
	}
}

func i32p(v int32) *int32 { return &v }

// btpWithStatusConditions builds an unstructured BTP carrying the
// given Gateway-API-shaped status.conditions. Used to drive
// ObserveAcceptance tests without spinning up an Envoy Gateway control
// plane.
func btpWithStatusConditions(conds []map[string]interface{}) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(PolicyGVK)
	raw := make([]interface{}, 0, len(conds))
	for _, c := range conds {
		raw = append(raw, c)
	}
	_ = unstructured.SetNestedSlice(u.Object, raw, "status", "conditions")
	return u
}

func TestObserveAcceptance_AcceptedTrue(t *testing.T) {
	u := btpWithStatusConditions([]map[string]interface{}{
		{
			"type":    "Accepted",
			"status":  "True",
			"reason":  "Accepted",
			"message": "Policy has been accepted",
		},
	})
	got := New().ObserveAcceptance(u)
	if got.State != traffic.AcceptanceAccepted {
		t.Fatalf("state = %d, want Accepted", got.State)
	}
	if got.Reason != "Accepted" {
		t.Fatalf("reason = %q", got.Reason)
	}
	if got.Message != "Policy has been accepted" {
		t.Fatalf("message = %q", got.Message)
	}
}

func TestObserveAcceptance_AcceptedFalse(t *testing.T) {
	u := btpWithStatusConditions([]map[string]interface{}{
		{
			"type":    "Accepted",
			"status":  "False",
			"reason":  "Invalid",
			"message": "loadBalancer.type: unsupported value foo",
		},
	})
	got := New().ObserveAcceptance(u)
	if got.State != traffic.AcceptanceRejected {
		t.Fatalf("state = %d, want Rejected", got.State)
	}
	if got.Reason != "Invalid" || got.Message != "loadBalancer.type: unsupported value foo" {
		t.Fatalf("reason/message = %q / %q", got.Reason, got.Message)
	}
}

func TestObserveAcceptance_AcceptedUnknown_IsPending(t *testing.T) {
	u := btpWithStatusConditions([]map[string]interface{}{
		{"type": "Accepted", "status": "Unknown"},
	})
	got := New().ObserveAcceptance(u)
	if got.State != traffic.AcceptancePending {
		t.Fatalf("status=Unknown must map to Pending, got %d", got.State)
	}
}

func TestObserveAcceptance_NoConditions_IsPending(t *testing.T) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(PolicyGVK)
	got := New().ObserveAcceptance(u)
	if got.State != traffic.AcceptancePending {
		t.Fatalf("no status -> Pending, got %d", got.State)
	}
}

func TestObserveAcceptance_OtherConditionTypes_Skipped(t *testing.T) {
	// EG writes both "Accepted" and "Programmed" (and more). We only
	// look at "Accepted"; other types must be ignored.
	u := btpWithStatusConditions([]map[string]interface{}{
		{"type": "Programmed", "status": "True"},
		{"type": "ResolvedRefs", "status": "True"},
	})
	got := New().ObserveAcceptance(u)
	if got.State != traffic.AcceptancePending {
		t.Fatalf("non-Accepted conditions must be ignored, got %d", got.State)
	}
}

func TestObserveAcceptance_NonUnstructuredInput_IsPending(t *testing.T) {
	// Defensive: a translator misuse (passing a typed object) must
	// not panic.
	got := New().ObserveAcceptance(nil)
	if got.State != traffic.AcceptancePending {
		t.Fatalf("nil input -> Pending, got %d", got.State)
	}
}

func TestSupportedTrafficFields_DeclaresBTPCoverage(t *testing.T) {
	got := New().SupportedTrafficFields()
	want := []string{
		constants.TrafficCapabilityAlgorithm,
		constants.TrafficCapabilityHashHeader,
		constants.TrafficCapabilityHashMultipleHeaders,
		constants.TrafficCapabilityHashCookie,
		constants.TrafficCapabilityHashSourceIP,
		constants.TrafficCapabilityEndpointOverrideHeader,
	}
	for _, capability := range want {
		if !got.Has(capability) {
			t.Errorf("SupportedTrafficFields() missing %q", capability)
		}
	}
	// The reserved Metadata endpoint override emits nothing and must
	// stay undeclared so admission keeps rejecting it.
	if got.Has(constants.TrafficCapabilityEndpointOverrideMetadata) {
		t.Errorf("SupportedTrafficFields() must not declare %q", constants.TrafficCapabilityEndpointOverrideMetadata)
	}
	if got.Len() != len(want) {
		t.Errorf("SupportedTrafficFields() has %d tokens, want %d: %v", got.Len(), len(want), got.UnsortedList())
	}
}
