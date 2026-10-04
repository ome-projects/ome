package canary

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestResolveBearerToken(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "prom-auth", Namespace: "ns"},
		Data:       map[string][]byte{"token": []byte("s3cr3t")},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	ref := func(name, key string) *corev1.SecretKeySelector {
		return &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: name},
			Key:                  key,
		}
	}

	t.Run("success", func(t *testing.T) {
		got, err := resolveBearerToken(context.Background(), c, "ns", ref("prom-auth", "token"))
		if err != nil || got != "s3cr3t" {
			t.Fatalf("got (%q,%v), want (s3cr3t,nil)", got, err)
		}
	})
	t.Run("secret not found", func(t *testing.T) {
		_, err := resolveBearerToken(context.Background(), c, "ns", ref("missing", "token"))
		if err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("err = %v, want a not-found error", err)
		}
	})
	t.Run("missing key", func(t *testing.T) {
		_, err := resolveBearerToken(context.Background(), c, "ns", ref("prom-auth", "nope"))
		if err == nil || !strings.Contains(err.Error(), "no key") {
			t.Fatalf("err = %v, want a missing-key error", err)
		}
	})
	t.Run("nil reader", func(t *testing.T) {
		if _, err := resolveBearerToken(context.Background(), nil, "ns", ref("prom-auth", "token")); err == nil {
			t.Fatal("want an error for a nil reader")
		}
	})
}

// TestBuildSampleRequest_PlanHash pins the cache-identity contract: the key's
// PlanHash is deterministic for identical inputs, changes when ANY part of the
// effective analysis request changes (checks, source address, headers, auth
// reference, or the token value behind it), and never embeds raw secret
// material.
func TestBuildSampleRequest_PlanHash(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	const tokenValue = "s3cr3t-token"
	newInputs := func(token string) ReconcileInputs {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "prom-auth", Namespace: "ns"},
			Data:       map[string][]byte{"token": []byte(token), "alt": []byte(token)},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
		return ReconcileInputs{
			Reader: c,
			ISVC:   &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "ns"}},
			Prometheus: &v1beta1.AnalysisPrometheus{
				ServerAddress: "http://prometheus.example.com:9090",
				Headers:       map[string]string{"X-Scope-OrgID": "tenant-a"},
				AuthRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "prom-auth"},
					Key:                  "token",
				},
			},
		}
	}
	newAnalysis := func() *v1beta1.RolloutAnalysis {
		return &v1beta1.RolloutAnalysis{
			Interval:     metav1.Duration{Duration: time.Minute},
			FailureLimit: 3,
			Metrics:      []v1beta1.AnalysisMetric{{Name: "err", Query: "q", Operator: v1beta1.ComparisonLTE, Threshold: "0.05"}},
		}
	}
	build := func(t *testing.T, in ReconcileInputs, a *v1beta1.RolloutAnalysis) SampleRequest {
		t.Helper()
		req, err := buildSampleRequest(context.Background(), in, a, 2)
		if err != nil {
			t.Fatalf("buildSampleRequest: %v", err)
		}
		return req
	}

	base := build(t, newInputs(tokenValue), newAnalysis())
	if base.Key.PlanHash == "" {
		t.Fatal("PlanHash must be set")
	}
	if strings.Contains(base.Key.PlanHash, tokenValue) {
		t.Fatal("PlanHash must not embed the raw token")
	}
	if again := build(t, newInputs(tokenValue), newAnalysis()); again.Key != base.Key {
		t.Fatalf("identical inputs must produce an identical key: %+v vs %+v", again.Key, base.Key)
	}

	edits := []struct {
		name   string
		inputs func() ReconcileInputs
		mutate func(a *v1beta1.RolloutAnalysis)
	}{
		{"query", nil, func(a *v1beta1.RolloutAnalysis) { a.Metrics[0].Query = "q2" }},
		{"threshold", nil, func(a *v1beta1.RolloutAnalysis) { a.Metrics[0].Threshold = "0.01" }},
		{"operator", nil, func(a *v1beta1.RolloutAnalysis) { a.Metrics[0].Operator = v1beta1.ComparisonGTE }},
		{"failure limit", nil, func(a *v1beta1.RolloutAnalysis) { a.FailureLimit = 1 }},
		{"server address", func() ReconcileInputs {
			in := newInputs(tokenValue)
			in.Prometheus.ServerAddress = "http://other.example.com:9090"
			return in
		}, nil},
		{"headers", func() ReconcileInputs {
			in := newInputs(tokenValue)
			in.Prometheus.Headers["X-Scope-OrgID"] = "tenant-b"
			return in
		}, nil},
		{"auth secret name", func() ReconcileInputs {
			in := newInputs(tokenValue)
			in.Prometheus.AuthRef.Name = "prom-auth-2"
			sec := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "prom-auth-2", Namespace: "ns"},
				Data:       map[string][]byte{"token": []byte(tokenValue)},
			}
			in.Reader = fake.NewClientBuilder().WithScheme(scheme).WithObjects(sec).Build()
			return in
		}, nil},
		{"auth secret key", func() ReconcileInputs {
			in := newInputs(tokenValue)
			in.Prometheus.AuthRef.Key = "alt"
			return in
		}, nil},
		{"token value", func() ReconcileInputs { return newInputs("rotated-token") }, nil},
	}
	for _, tt := range edits {
		t.Run(tt.name, func(t *testing.T) {
			in := newInputs(tokenValue)
			if tt.inputs != nil {
				in = tt.inputs()
			}
			a := newAnalysis()
			if tt.mutate != nil {
				tt.mutate(a)
			}
			got := build(t, in, a)
			if got.Key.PlanHash == base.Key.PlanHash {
				t.Fatalf("editing the %s must change PlanHash", tt.name)
			}
			// Only the fingerprint moves: the target identity stays the same key prefix.
			gotID, baseID := got.Key, base.Key
			gotID.PlanHash, baseID.PlanHash = "", ""
			if gotID != baseID {
				t.Fatalf("edit must not disturb the target identity: %+v vs %+v", gotID, baseID)
			}
		})
	}
}

// TestBuildSampleRequest_ReadsSecretViaReader pins that the auth Secret is read
// through ReconcileInputs.Reader (the live API reader), never the cached
// Client — a cached Get would start a cluster-wide Secret informer.
func TestBuildSampleRequest_ReadsSecretViaReader(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "prom-auth", Namespace: "ns"},
		Data:       map[string][]byte{"token": []byte("s3cr3t")},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	in := ReconcileInputs{
		Reader: c, // Client deliberately unset: the read must go through Reader
		ISVC:   &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "ns"}},
		Prometheus: &v1beta1.AnalysisPrometheus{
			ServerAddress: "http://prom",
			AuthRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "prom-auth"},
				Key:                  "token",
			},
		},
	}
	req, err := buildSampleRequest(context.Background(), in, &v1beta1.RolloutAnalysis{}, 0)
	if err != nil {
		t.Fatalf("buildSampleRequest: %v", err)
	}
	if req.BearerToken != "s3cr3t" {
		t.Fatalf("token should resolve via Reader, got %q", req.BearerToken)
	}
}

// The Services the analysis queries name carry the Component's replica
// prefix; the service name still identifies the InferenceService.
func TestBuildSampleRequest_ServicesCarryTheReplicaPrefix(t *testing.T) {
	in := ReconcileInputs{
		ISVC: &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "ns"},
			Spec:       v1beta1.InferenceServiceSpec{ReplicaRefs: &v1beta1.ReplicaRefs{Engine: []string{"pool-a"}}},
		},
		Component:          v1beta1.EngineComponent,
		CanaryRevisionHash: "new1",
		StableRevisionHash: "old1",
	}
	req, err := buildSampleRequest(context.Background(), in, &v1beta1.RolloutAnalysis{}, 0)
	if err != nil {
		t.Fatalf("buildSampleRequest: %v", err)
	}
	if req.TemplateContext.CanaryService != "pool-a-engine-rev-new1" || req.TemplateContext.StableService != "pool-a-engine-rev-old1" {
		t.Fatalf("services must carry the replica prefix: canary=%q stable=%q", req.TemplateContext.CanaryService, req.TemplateContext.StableService)
	}
	if req.TemplateContext.ISVCName != "svc" || req.Key.ISVCName != "svc" {
		t.Fatalf("the service name identifies the InferenceService: template=%q key=%q", req.TemplateContext.ISVCName, req.Key.ISVCName)
	}
}
