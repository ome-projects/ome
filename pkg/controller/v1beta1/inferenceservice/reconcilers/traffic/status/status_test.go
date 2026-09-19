package status

import (
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// fixedNow is a stable transition time used everywhere so test
// failures don't depend on wall clock.
var fixedNow = metav1.NewTime(time.Date(2026, time.May, 17, 12, 0, 0, 0, time.UTC))

// fakeEmittedPolicy is a minimal client.Object stand-in. The real
// Envoy Gateway BackendTrafficPolicy lives behind a go.mod dep that
// is not yet vendored; using a fake decouples the writer test from
// that dep.
type fakeEmittedPolicy struct {
	client.Object
	name string
	gvk  schema.GroupVersionKind
}

func (f *fakeEmittedPolicy) GetName() string { return f.name }
func (f *fakeEmittedPolicy) GetObjectKind() schema.ObjectKind {
	return &fakeObjectKind{gvk: f.gvk}
}

type fakeObjectKind struct {
	gvk schema.GroupVersionKind
}

func (f *fakeObjectKind) GroupVersionKind() schema.GroupVersionKind {
	return f.gvk
}
func (f *fakeObjectKind) SetGroupVersionKind(_ schema.GroupVersionKind) {}

// Statically verify fake satisfies client.Object via embedding (the
// fakeclient package is imported solely to surface compile errors if
// the embedded interface shape ever drifts).
var _ = fakeclient.NewClientBuilder

func newEmittedPolicy(name string) client.Object {
	return &fakeEmittedPolicy{
		name: name,
		gvk: schema.GroupVersionKind{
			Group:   "gateway.envoyproxy.io",
			Version: "v1alpha1",
			Kind:    "BackendTrafficPolicy",
		},
	}
}

func TestBuild_HasIntentFalse_ReturnsNil(t *testing.T) {
	// When the operator declares nothing, the writer must NOT emit a
	// TrafficStatus so older clients keep seeing the field as nil.
	got := Build(BuildArgs{HasIntent: false, Now: fixedNow})
	if got != nil {
		t.Fatalf("Build with HasIntent=false must return nil, got %+v", got)
	}
}

func TestBuild_NoopTranslator_NoTranslatorAvailable(t *testing.T) {
	got := Build(BuildArgs{
		TranslatorName:     NoopTranslatorName,
		HasIntent:          true,
		Algorithm:          string(v1beta1.LoadBalancingTypeRoundRobin),
		ObservedGeneration: 7,
		Now:                fixedNow,
	})
	if got == nil {
		t.Fatalf("Build must emit a TrafficStatus when intent is set, even for noop")
	}
	if got.Algorithm != string(v1beta1.LoadBalancingTypeRoundRobin) {
		t.Fatalf("LoadBalancingType = %q, want RoundRobin", got.Algorithm)
	}
	if got.BackendPolicyResource != nil {
		t.Fatalf("noop emits no policy resource; got %+v", got.BackendPolicyResource)
	}
	if len(got.TargetedHTTPRoutes) != 0 {
		t.Fatalf("noop targets no routes; got %v", got.TargetedHTTPRoutes)
	}
	if len(got.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d: %+v", len(got.Conditions), got.Conditions)
	}
	c := got.Conditions[0]
	if c.Type != v1beta1.TrafficConditionBackendPolicyReady {
		t.Fatalf("condition Type = %q", c.Type)
	}
	if c.Status != metav1.ConditionFalse {
		t.Fatalf("condition Status = %q, want False", c.Status)
	}
	if c.Reason != v1beta1.TrafficReasonNoTranslatorAvailable {
		t.Fatalf("condition Reason = %q", c.Reason)
	}
	if c.ObservedGeneration != 7 {
		t.Fatalf("ObservedGeneration = %d, want 7", c.ObservedGeneration)
	}
	if !c.LastTransitionTime.Equal(&fixedNow) {
		t.Fatalf("LastTransitionTime = %v, want %v", c.LastTransitionTime, fixedNow)
	}
}

func TestBuild_TranslateError_TranslationFailed(t *testing.T) {
	wantMsg := "unsupported algorithm"
	got := Build(BuildArgs{
		TranslatorName: "envoy-gateway",
		HasIntent:      true,
		TranslateErr:   errors.New(wantMsg),
		Now:            fixedNow,
	})
	if got == nil {
		t.Fatalf("expected status, got nil")
	}
	c := got.Conditions[0]
	if c.Status != metav1.ConditionFalse {
		t.Fatalf("Status = %q, want False", c.Status)
	}
	if c.Reason != v1beta1.TrafficReasonTranslationFailed {
		t.Fatalf("Reason = %q, want TranslationFailed", c.Reason)
	}
	if c.Message != wantMsg {
		t.Fatalf("Message = %q, want %q", c.Message, wantMsg)
	}
	if got.Algorithm != algorithmDefault {
		t.Fatalf("LoadBalancingType = %q, want %q (empty input)", got.Algorithm, algorithmDefault)
	}
}

func TestBuild_TranslateError_BeatsNoop(t *testing.T) {
	// The error branch must beat the noop short-circuit so operators
	// see the actionable reason.
	got := Build(BuildArgs{
		TranslatorName: NoopTranslatorName,
		HasIntent:      true,
		TranslateErr:   errors.New("synthetic"),
		Now:            fixedNow,
	})
	if got.Conditions[0].Reason != v1beta1.TrafficReasonTranslationFailed {
		t.Fatalf("error must beat noop; got Reason %q", got.Conditions[0].Reason)
	}
}

func TestBuild_EmittedPolicy_Pending(t *testing.T) {
	policy := newEmittedPolicy("foo-btp")
	routes := []string{"foo", "foo-engine", "foo-decoder"}

	got := Build(BuildArgs{
		TranslatorName: "envoy-gateway",
		HasIntent:      true,
		Algorithm:      string(v1beta1.LoadBalancingTypeLeastRequest),
		EmittedPolicy:  policy,
		TargetedRoutes: routes,
		Now:            fixedNow,
	})

	if got.Algorithm != string(v1beta1.LoadBalancingTypeLeastRequest) {
		t.Fatalf("LoadBalancingType = %q", got.Algorithm)
	}
	if got.BackendPolicyResource == nil {
		t.Fatalf("BackendPolicyResource missing")
	}
	if got.BackendPolicyResource.APIVersion != "gateway.envoyproxy.io/v1alpha1" {
		t.Fatalf("APIVersion = %q", got.BackendPolicyResource.APIVersion)
	}
	if got.BackendPolicyResource.Kind != "BackendTrafficPolicy" {
		t.Fatalf("Kind = %q", got.BackendPolicyResource.Kind)
	}
	if got.BackendPolicyResource.Name != "foo-btp" {
		t.Fatalf("Name = %q", got.BackendPolicyResource.Name)
	}

	// TargetedHTTPRoutes must be a defensive copy: mutating the
	// caller's slice afterwards must not change the status.
	if len(got.TargetedHTTPRoutes) != 3 {
		t.Fatalf("expected 3 routes, got %v", got.TargetedHTTPRoutes)
	}
	routes[0] = "tampered"
	if got.TargetedHTTPRoutes[0] == "tampered" {
		t.Fatalf("TargetedHTTPRoutes must be a defensive copy")
	}

	c := got.Conditions[0]
	if c.Status != metav1.ConditionUnknown {
		t.Fatalf("Status = %q, want Unknown", c.Status)
	}
	if c.Reason != v1beta1.TrafficReasonPending {
		t.Fatalf("Reason = %q, want Pending", c.Reason)
	}
}

func TestBuild_Passthroughs_AppearInMessage(t *testing.T) {
	policy := newEmittedPolicy("foo-btp")
	got := Build(BuildArgs{
		TranslatorName: "envoy-gateway",
		HasIntent:      true,
		EmittedPolicy:  policy,
		Passthroughs:   []string{"loadBalancer.slowStart.window"},
		Now:            fixedNow,
	})
	msg := got.Conditions[0].Message
	if !strings.Contains(msg, "1 pass-through") {
		t.Fatalf("expected pass-through count in message, got %q", msg)
	}
	if !strings.Contains(msg, "loadBalancer.slowStart.window") {
		t.Fatalf("expected pass-through path in message, got %q", msg)
	}
}

func TestBuild_NoEmittedPolicyNoError_Pending(t *testing.T) {
	got := Build(BuildArgs{
		TranslatorName: "envoy-gateway",
		HasIntent:      true,
		Now:            fixedNow,
	})
	c := got.Conditions[0]
	if c.Status != metav1.ConditionUnknown {
		t.Fatalf("Status = %q, want Unknown", c.Status)
	}
	if c.Reason != v1beta1.TrafficReasonPending {
		t.Fatalf("Reason = %q, want Pending", c.Reason)
	}
}

func TestBuild_Determinism(t *testing.T) {
	// Same args must produce equivalent output across calls — the
	// reconciler relies on this to avoid noisy status patches.
	args := BuildArgs{
		TranslatorName: "envoy-gateway",
		HasIntent:      true,
		Algorithm:      string(v1beta1.LoadBalancingTypeConsistentHash),
		EmittedPolicy:  newEmittedPolicy("foo-btp"),
		TargetedRoutes: []string{"foo"},
		Passthroughs:   []string{"a.b.c"},
		Now:            fixedNow,
	}
	a := Build(args)
	b := Build(args)
	if a.Algorithm != b.Algorithm ||
		a.BackendPolicyResource.Name != b.BackendPolicyResource.Name ||
		a.Conditions[0].Message != b.Conditions[0].Message {
		t.Fatalf("Build is not deterministic:\n a=%+v\n b=%+v", a, b)
	}
}

func TestBuild_GatewayAccepted_TruePassThroughMessage(t *testing.T) {
	policy := newEmittedPolicy("foo-btp")
	got := Build(BuildArgs{
		TranslatorName:           "envoy-gateway",
		HasIntent:                true,
		EmittedPolicy:            policy,
		GatewayAcceptance:        GatewayAcceptanceAccepted,
		GatewayAcceptanceReason:  "Accepted",
		GatewayAcceptanceMessage: "Policy is valid and accepted",
		Now:                      fixedNow,
	})
	c := got.Conditions[0]
	if c.Status != metav1.ConditionTrue {
		t.Fatalf("Status = %q, want True", c.Status)
	}
	if c.Reason != v1beta1.TrafficReasonAcceptedByGateway {
		t.Fatalf("Reason = %q, want AcceptedByGateway", c.Reason)
	}
	if c.Message != "Policy is valid and accepted" {
		t.Fatalf("Message = %q, want pass-through", c.Message)
	}
}

func TestBuild_GatewayAccepted_FallbackMessage(t *testing.T) {
	// When the gateway controller writes no message, Build composes
	// a default that still tells the operator the policy was accepted.
	policy := newEmittedPolicy("foo-btp")
	got := Build(BuildArgs{
		TranslatorName:    "envoy-gateway",
		HasIntent:         true,
		EmittedPolicy:     policy,
		GatewayAcceptance: GatewayAcceptanceAccepted,
		Passthroughs:      []string{"loadBalancer.slowStart.window"},
		Now:               fixedNow,
	})
	if !strings.Contains(got.Conditions[0].Message, "accepted by gateway") {
		t.Fatalf("default accepted message missing: %q", got.Conditions[0].Message)
	}
	if !strings.Contains(got.Conditions[0].Message, "loadBalancer.slowStart.window") {
		t.Fatalf("pass-through paths should appear in default message: %q", got.Conditions[0].Message)
	}
}

func TestBuild_GatewayRejected_FalsePassThroughMessage(t *testing.T) {
	policy := newEmittedPolicy("foo-btp")
	got := Build(BuildArgs{
		TranslatorName:           "envoy-gateway",
		HasIntent:                true,
		EmittedPolicy:            policy,
		GatewayAcceptance:        GatewayAcceptanceRejected,
		GatewayAcceptanceReason:  "Invalid",
		GatewayAcceptanceMessage: "unsupported field foo.bar",
		Now:                      fixedNow,
	})
	c := got.Conditions[0]
	if c.Status != metav1.ConditionFalse {
		t.Fatalf("Status = %q, want False", c.Status)
	}
	if c.Reason != v1beta1.TrafficReasonGatewayRejected {
		t.Fatalf("Reason = %q, want GatewayRejected", c.Reason)
	}
	if c.Message != "unsupported field foo.bar" {
		t.Fatalf("Message = %q", c.Message)
	}
}

func TestBuild_GatewayRejected_ReasonOnlyFallback(t *testing.T) {
	// Reason but no message — Build composes a useful fallback.
	policy := newEmittedPolicy("foo-btp")
	got := Build(BuildArgs{
		TranslatorName:          "envoy-gateway",
		HasIntent:               true,
		EmittedPolicy:           policy,
		GatewayAcceptance:       GatewayAcceptanceRejected,
		GatewayAcceptanceReason: "Invalid",
		Now:                     fixedNow,
	})
	if !strings.Contains(got.Conditions[0].Message, "Invalid") {
		t.Fatalf("Message should mention reason: %q", got.Conditions[0].Message)
	}
}

func TestBuild_GatewayPending_KeepsPendingCondition(t *testing.T) {
	// Default (Pending) acceptance must produce the Pending/Unknown
	// condition for the first-reconcile case.
	policy := newEmittedPolicy("foo-btp")
	got := Build(BuildArgs{
		TranslatorName:    "envoy-gateway",
		HasIntent:         true,
		EmittedPolicy:     policy,
		GatewayAcceptance: GatewayAcceptancePending,
		Now:               fixedNow,
	})
	c := got.Conditions[0]
	if c.Status != metav1.ConditionUnknown {
		t.Fatalf("Status = %q, want Unknown", c.Status)
	}
	if c.Reason != v1beta1.TrafficReasonPending {
		t.Fatalf("Reason = %q, want Pending", c.Reason)
	}
}

func TestBuild_TranslateError_BeatsGatewayAcceptance(t *testing.T) {
	// The error branch trumps even an "Accepted" observation — if
	// translation failed this cycle, the policy on the API server
	// is from a prior successful cycle and its acceptance state is
	// no longer authoritative.
	got := Build(BuildArgs{
		TranslatorName:    "envoy-gateway",
		HasIntent:         true,
		TranslateErr:      errors.New("boom"),
		GatewayAcceptance: GatewayAcceptanceAccepted,
		Now:               fixedNow,
	})
	if got.Conditions[0].Reason != v1beta1.TrafficReasonTranslationFailed {
		t.Fatalf("Reason = %q, want TranslationFailed", got.Conditions[0].Reason)
	}
}

func TestBuild_UnsupportedFields_AddsSecondCondition(t *testing.T) {
	policy := newEmittedPolicy("foo-btp")
	got := Build(BuildArgs{
		TranslatorName:         "envoy-gateway",
		HasIntent:              true,
		EmittedPolicy:          policy,
		UnsupportedAnnotations: []string{"ome.io/dr.foo", "ome.io/dr.bar"},
		Now:                    fixedNow,
	})
	if len(got.Conditions) != 2 {
		t.Fatalf("expected 2 conditions, got %d: %+v", len(got.Conditions), got.Conditions)
	}
	// Find the UnsupportedFields condition.
	var uc *metav1.Condition
	for i := range got.Conditions {
		if got.Conditions[i].Type == v1beta1.TrafficConditionBackendPolicyUnsupportedFields {
			uc = &got.Conditions[i]
			break
		}
	}
	if uc == nil {
		t.Fatalf("BackendPolicyUnsupportedFields condition missing: %+v", got.Conditions)
	}
	if uc.Status != metav1.ConditionTrue {
		t.Fatalf("Status = %q, want True", uc.Status)
	}
	if uc.Reason != v1beta1.TrafficReasonUnsupportedField {
		t.Fatalf("Reason = %q, want UnsupportedField", uc.Reason)
	}
	if !strings.Contains(uc.Message, "envoy-gateway") {
		t.Fatalf("Message should mention translator: %q", uc.Message)
	}
	if !strings.Contains(uc.Message, "ome.io/dr.foo") || !strings.Contains(uc.Message, "ome.io/dr.bar") {
		t.Fatalf("Message should list dropped keys: %q", uc.Message)
	}
}

func TestBuild_UnsupportedTypedFields_AddsSecondCondition(t *testing.T) {
	policy := newEmittedPolicy("foo-dr")
	got := Build(BuildArgs{
		TranslatorName:    "istio",
		HasIntent:         true,
		EmittedPolicy:     policy,
		UnsupportedFields: []string{"spec.traffic.endpointOverride.type=Header"},
		Now:               fixedNow,
	})
	if len(got.Conditions) != 2 {
		t.Fatalf("expected 2 conditions, got %d: %+v", len(got.Conditions), got.Conditions)
	}
	var uc *metav1.Condition
	for i := range got.Conditions {
		if got.Conditions[i].Type == v1beta1.TrafficConditionBackendPolicyUnsupportedFields {
			uc = &got.Conditions[i]
			break
		}
	}
	if uc == nil {
		t.Fatalf("BackendPolicyUnsupportedFields condition missing: %+v", got.Conditions)
	}
	if uc.Status != metav1.ConditionTrue {
		t.Fatalf("Status = %q, want True", uc.Status)
	}
	if uc.Reason != v1beta1.TrafficReasonUnsupportedField {
		t.Fatalf("Reason = %q, want UnsupportedField", uc.Reason)
	}
	// The condition must name both the active translator and the
	// dropped typed field so operators know what was ignored and why.
	if !strings.Contains(uc.Message, "istio") {
		t.Fatalf("Message should name the active translator: %q", uc.Message)
	}
	if !strings.Contains(uc.Message, "spec.traffic.endpointOverride.type=Header") {
		t.Fatalf("Message should name the dropped typed field: %q", uc.Message)
	}
}

func TestBuild_UnsupportedTypedFieldsAndAnnotations_SingleMergedCondition(t *testing.T) {
	policy := newEmittedPolicy("foo-dr")
	got := Build(BuildArgs{
		TranslatorName:         "istio",
		HasIntent:              true,
		EmittedPolicy:          policy,
		UnsupportedFields:      []string{"spec.traffic.consistentHash.headers(multiple)"},
		UnsupportedAnnotations: []string{"ome.io/btp.loadBalancer.slowStart.window"},
		Now:                    fixedNow,
	})
	if len(got.Conditions) != 2 {
		t.Fatalf("both sources merge into ONE UnsupportedFields condition, got %d: %+v",
			len(got.Conditions), got.Conditions)
	}
	var uc *metav1.Condition
	for i := range got.Conditions {
		if got.Conditions[i].Type == v1beta1.TrafficConditionBackendPolicyUnsupportedFields {
			uc = &got.Conditions[i]
			break
		}
	}
	if uc == nil {
		t.Fatalf("BackendPolicyUnsupportedFields condition missing: %+v", got.Conditions)
	}
	if !strings.Contains(uc.Message, "spec.traffic.consistentHash.headers(multiple)") ||
		!strings.Contains(uc.Message, "ome.io/btp.loadBalancer.slowStart.window") {
		t.Fatalf("Message should list both the typed field and the annotation: %q", uc.Message)
	}
}

func TestBuild_UnsupportedFields_EmptyList_OmitsCondition(t *testing.T) {
	policy := newEmittedPolicy("foo-btp")
	got := Build(BuildArgs{
		TranslatorName:    "envoy-gateway",
		HasIntent:         true,
		EmittedPolicy:     policy,
		GatewayAcceptance: GatewayAcceptanceAccepted,
		Now:               fixedNow,
	})
	if len(got.Conditions) != 1 {
		t.Fatalf("clean ISVC should have only the Ready condition, got %d: %+v",
			len(got.Conditions), got.Conditions)
	}
}

func TestBuild_UnsupportedFields_NoopTranslator_OmitsCondition(t *testing.T) {
	// NoTranslatorAvailable already explains why nothing is honored;
	// listing every dropped key would be noise.
	got := Build(BuildArgs{
		TranslatorName:         NoopTranslatorName,
		HasIntent:              true,
		UnsupportedAnnotations: []string{"ome.io/circuit-breaker-max-connections"},
		Now:                    fixedNow,
	})
	if len(got.Conditions) != 1 {
		t.Fatalf("noop must not surface UnsupportedFields, got %d: %+v",
			len(got.Conditions), got.Conditions)
	}
	if got.Conditions[0].Reason != v1beta1.TrafficReasonNoTranslatorAvailable {
		t.Fatalf("primary condition reason = %q", got.Conditions[0].Reason)
	}
}

func TestBuild_UnsupportedFields_TranslateError_OmitsCondition(t *testing.T) {
	// TranslationFailed already explains the situation.
	got := Build(BuildArgs{
		TranslatorName:         "envoy-gateway",
		HasIntent:              true,
		TranslateErr:           errors.New("boom"),
		UnsupportedAnnotations: []string{"ome.io/dr.foo"},
		Now:                    fixedNow,
	})
	if len(got.Conditions) != 1 {
		t.Fatalf("translate error must suppress UnsupportedFields, got %d: %+v",
			len(got.Conditions), got.Conditions)
	}
}

func TestBuild_LoadBalancingType_DefaultsWhenEmpty(t *testing.T) {
	got := Build(BuildArgs{
		TranslatorName: "envoy-gateway",
		HasIntent:      true,
		Now:            fixedNow,
	})
	if got.Algorithm != algorithmDefault {
		t.Fatalf("LoadBalancingType = %q, want %q", got.Algorithm, algorithmDefault)
	}
}
