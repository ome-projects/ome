package render

import (
	"strings"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// standardPolicy is the standard fleet shape: a binary 0-or-max activity
// trigger with fail-to-max fallback, every derived value templated.
func standardPolicy() *v1beta1.AutoscalerPolicy {
	return &v1beta1.AutoscalerPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "request-activity-v1", Namespace: "llm-serving", UID: "uid-1", Generation: 4},
		Spec: v1beta1.AutoscalerPolicySpec{
			Class: v1beta1.AutoscalerKEDA,
			Keda: &v1beta1.KedaPolicyTemplate{
				Triggers: []v1beta1.KedaTriggerTemplate{{
					Type:                        "prometheus",
					ProviderRef:                 &v1beta1.MetricProviderRef{Name: "cluster-prometheus"},
					MetricType:                  autoscalingv2.AverageValueMetricType,
					QueryReturnsDesiredReplicas: true,
					Metadata: map[string]string{
						"threshold":        "1",
						"ignoreNullValues": "false",
						"query":            `((sum({__name__=~"sglang:num_running_reqs|vllm:num_requests_running",namespace="{{ .Namespace }}",inferenceservice="{{ .ISVCName }}",component=~"engine|decoder"}) > bool 0) * {{ .MaxReplicas }})`,
					},
				}},
				Fallback: &v1beta1.FallbackTemplate{
					FailureThreshold: 3,
					Replicas:         v1beta1.ReplicaValueSource{FromComponent: ptr.To(v1beta1.BoundsFieldMaxReplicas)},
				},
			},
		},
	}
}

func engineContext(maxReplicas int32) Context {
	return Context{
		Namespace:   "llm-serving",
		ISVCName:    "llm-a",
		Component:   "engine",
		MinReplicas: 1,
		MaxReplicas: maxReplicas,
		TargetName:  "llm-a-engine",
	}
}

var testProviders = Providers{
	"cluster-prometheus": {ServerAddress: "http://prometheus.example.invalid:9090"},
}

func TestRenderGolden(t *testing.T) {
	result, err := RenderWithCache(NewCache(), standardPolicy(), testProviders, engineContext(8))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	block := result.Autoscaler
	if block.Class != v1beta1.AutoscalerKEDA || block.Keda == nil || len(block.Keda.Triggers) != 1 {
		t.Fatalf("unexpected rendered shape: %+v", block)
	}
	trigger := block.Keda.Triggers[0]
	if trigger.MetricType != autoscalingv2.AverageValueMetricType {
		t.Errorf("metricType = %q, want AverageValue", trigger.MetricType)
	}
	if got := trigger.Metadata["serverAddress"]; got != "http://prometheus.example.invalid:9090" {
		t.Errorf("serverAddress = %q, want provider binding", got)
	}
	query := trigger.Metadata["query"]
	for _, want := range []string{`namespace="llm-serving"`, `inferenceservice="llm-a"`, `) * 8)`} {
		if !strings.Contains(query, want) {
			t.Errorf("query missing %q:\n%s", want, query)
		}
	}
	if _, err := parser.ParseExpr(query); err != nil {
		t.Errorf("rendered query is not valid PromQL: %v", err)
	}
	if block.Keda.Fallback == nil || block.Keda.Fallback.Replicas != 8 || block.Keda.Fallback.FailureThreshold != 3 {
		t.Errorf("fallback = %+v, want {3, 8}", block.Keda.Fallback)
	}
	if !strings.HasPrefix(result.PortableDigest, "pv1:") || !strings.HasPrefix(result.ResolvedDigest, "rv1:") {
		t.Errorf("digests = %q / %q", result.PortableDigest, result.ResolvedDigest)
	}
}

// A Split home renders the SAME policy against its derived (smaller) bounds:
// per-home literals, same portable digest, different resolved digest.
func TestRenderPerHomeBounds(t *testing.T) {
	cache := NewCache()
	global, err := RenderWithCache(cache, standardPolicy(), testProviders, engineContext(8))
	if err != nil {
		t.Fatalf("render global: %v", err)
	}
	home, err := RenderWithCache(cache, standardPolicy(), testProviders, engineContext(3))
	if err != nil {
		t.Fatalf("render home: %v", err)
	}
	if !strings.Contains(home.Autoscaler.Keda.Triggers[0].Metadata["query"], ") * 3)") {
		t.Errorf("home query does not use derived max:\n%s", home.Autoscaler.Keda.Triggers[0].Metadata["query"])
	}
	if home.Autoscaler.Keda.Fallback.Replicas != 3 {
		t.Errorf("home fallback = %d, want 3", home.Autoscaler.Keda.Fallback.Replicas)
	}
	if home.PortableDigest != global.PortableDigest {
		t.Errorf("portable digest must be bounds-independent: %q vs %q", home.PortableDigest, global.PortableDigest)
	}
	if home.ResolvedDigest == global.ResolvedDigest {
		t.Errorf("resolved digest must differ across bounds")
	}
}

func TestRenderRejections(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*v1beta1.AutoscalerPolicy)
		ctx     Context
		wantErr string
	}{
		{"unknown variable", func(p *v1beta1.AutoscalerPolicy) {
			p.Spec.Keda.Triggers[0].Metadata["query"] = "{{ .Nope }}"
		}, engineContext(8), "unknown template variable"},
		{"control flow", func(p *v1beta1.AutoscalerPolicy) {
			p.Spec.Keda.Triggers[0].Metadata["query"] = "{{ if .Namespace }}x{{ end }}"
		}, engineContext(8), "forbidden template construct"},
		{"function call", func(p *v1beta1.AutoscalerPolicy) {
			p.Spec.Keda.Triggers[0].Metadata["query"] = `{{ printf "%s" .Namespace }}`
		}, engineContext(8), "forbidden template construct"},
		{"pipeline", func(p *v1beta1.AutoscalerPolicy) {
			p.Spec.Keda.Triggers[0].Metadata["query"] = "{{ .Namespace | html }}"
		}, engineContext(8), "forbidden template construct"},
		{"forbidden serverAddress", func(p *v1beta1.AutoscalerPolicy) {
			p.Spec.Keda.Triggers[0].Metadata["ServerAddress"] = "http://attacker.invalid"
		}, engineContext(8), "provider-owned"},
		{"forbidden authModes", func(p *v1beta1.AutoscalerPolicy) {
			p.Spec.Keda.Triggers[0].Metadata["authModes"] = "bearer"
		}, engineContext(8), "provider-owned"},
		{"missing providerRef", func(p *v1beta1.AutoscalerPolicy) {
			p.Spec.Keda.Triggers[0].ProviderRef = nil
		}, engineContext(8), "providerRef is required"},
		{"unbound provider", func(p *v1beta1.AutoscalerPolicy) {
			p.Spec.Keda.Triggers[0].ProviderRef.Name = "nowhere"
		}, engineContext(8), "not bound on this cluster"},
		{"non-positive max", func(p *v1beta1.AutoscalerPolicy) {}, engineContext(0), "positive ceiling"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			policy := standardPolicy()
			policy.UID = ""
			tc.mutate(policy)
			_, err := RenderWithCache(NewCache(), policy, testProviders, tc.ctx)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestReplicaValueSource(t *testing.T) {
	rctx := engineContext(8)
	if got, err := resolveReplicaValue(&v1beta1.ReplicaValueSource{Value: ptr.To(int32(5))}, rctx); err != nil || got != 5 {
		t.Errorf("fixed value: got %d, %v", got, err)
	}
	if got, err := resolveReplicaValue(&v1beta1.ReplicaValueSource{FromComponent: ptr.To(v1beta1.BoundsFieldMinReplicas)}, rctx); err != nil || got != 1 {
		t.Errorf("minReplicas source: got %d, %v", got, err)
	}
	if _, err := resolveReplicaValue(&v1beta1.ReplicaValueSource{}, rctx); err == nil {
		t.Errorf("empty source must error")
	}
	if _, err := resolveReplicaValue(&v1beta1.ReplicaValueSource{
		Value: ptr.To(int32(5)), FromComponent: ptr.To(v1beta1.BoundsFieldMaxReplicas),
	}, rctx); err == nil {
		t.Errorf("double source must error")
	}
}

func TestRenderHPAClassVerbatim(t *testing.T) {
	policy := &v1beta1.AutoscalerPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "cpu-hpa", Namespace: "llm-serving"},
		Spec:       v1beta1.AutoscalerPolicySpec{Class: v1beta1.AutoscalerHPA},
	}
	result, err := RenderWithCache(NewCache(), policy, nil, engineContext(4))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if result.Autoscaler.Class != v1beta1.AutoscalerHPA || result.Autoscaler.Keda != nil {
		t.Fatalf("unexpected shape: %+v", result.Autoscaler)
	}
}

func TestPortableDigestDefaultingEquivalence(t *testing.T) {
	implicit := standardPolicy().Spec
	explicit := *standardPolicy().Spec.DeepCopy()
	explicit.Enforcement = v1beta1.PolicyEnforcementDefault

	implicitDigest, err := PortableDigest(&implicit)
	if err != nil {
		t.Fatal(err)
	}
	explicitDigest, err := PortableDigest(&explicit)
	if err != nil {
		t.Fatal(err)
	}
	// The apiserver stores the defaulted form while a raw GitOps file omits
	// it; both must hash identically or CI and cluster digests split.
	if implicitDigest != explicitDigest {
		t.Errorf("defaulting changed the digest: %q vs %q", implicitDigest, explicitDigest)
	}

	changed := *standardPolicy().Spec.DeepCopy()
	changed.Keda.Fallback.FailureThreshold = 4
	changedDigest, err := PortableDigest(&changed)
	if err != nil {
		t.Fatal(err)
	}
	if changedDigest == implicitDigest {
		t.Errorf("semantic change did not change the digest")
	}
}

func TestTemplateCacheInvalidation(t *testing.T) {
	cache := NewCache()
	policy := standardPolicy()
	if _, err := RenderWithCache(cache, policy, testProviders, engineContext(8)); err != nil {
		t.Fatal(err)
	}
	// A new generation with a different template must not serve stale output.
	updated := standardPolicy()
	updated.Generation = 5
	updated.Spec.Keda.Triggers[0].Metadata["query"] = `((sum({inferenceservice="{{ .ISVCName }}"}) > bool 0) * {{ .MaxReplicas }})`
	result, err := RenderWithCache(cache, updated, testProviders, engineContext(8))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(result.Autoscaler.Keda.Triggers[0].Metadata["query"], "namespace=") {
		t.Errorf("stale template served after generation bump")
	}
}

func TestValidateSpec(t *testing.T) {
	if issues := ValidateSpec(&standardPolicy().Spec); len(issues) != 0 {
		t.Fatalf("standard policy must validate cleanly, got %v", issues)
	}

	reserved := standardPolicy().Spec
	reserved.Enforcement = v1beta1.PolicyEnforcementRequired
	assertIssue(t, ValidateSpec(&reserved), ReasonEnforcementReserved)

	mismatch := *standardPolicy().Spec.DeepCopy()
	mismatch.HPA = &v1beta1.HPAAutoscaler{}
	assertIssue(t, ValidateSpec(&mismatch), ReasonClassTemplate)

	valueType := *standardPolicy().Spec.DeepCopy()
	valueType.Keda.Triggers[0].MetricType = autoscalingv2.ValueMetricType
	assertIssue(t, ValidateSpec(&valueType), ReasonMetricTypeForced)

	implicitNulls := *standardPolicy().Spec.DeepCopy()
	delete(implicitNulls.Keda.Triggers[0].Metadata, "ignoreNullValues")
	assertIssue(t, ValidateSpec(&implicitNulls), ReasonExplicitNullValues)

	badQuery := *standardPolicy().Spec.DeepCopy()
	badQuery.Keda.Triggers[0].Metadata["query"] = `((sum({a="b"}) > bool 0) * {{ .MaxReplicas }}`
	assertIssue(t, ValidateSpec(&badQuery), v1beta1.AutoscalerPolicyReasonPromQLInvalid)

	// Syntactically valid, semantically lethal: * binds tighter than >.
	trap := *standardPolicy().Spec.DeepCopy()
	trap.Keda.Triggers[0].Metadata["query"] = `sum({a="b"}) > bool 0 * {{ .MaxReplicas }}`
	assertIssue(t, ValidateSpec(&trap), v1beta1.AutoscalerPolicyReasonPromQLInvalid)
}

func assertIssue(t *testing.T, issues []Issue, reason string) {
	t.Helper()
	for _, issue := range issues {
		if issue.Reason == reason {
			return
		}
	}
	t.Errorf("missing issue %q in %v", reason, issues)
}
