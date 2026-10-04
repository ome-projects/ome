package canary

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
)

// buildSampleRequest snapshots everything the background eval needs to query one
// step's analysis WITHOUT touching the live InferenceService — the reconcile owns
// it, so a goroutine reading it would race. It resolves the source address, bearer
// token, headers, and template context up front; the eval then only performs the
// HTTP query. The address is the canary's own ServerAddress when set, else the
// operator-configured bundled address; an empty address means "no source" and the
// eval's query reads inconclusive.
func buildSampleRequest(ctx context.Context, in ReconcileInputs, a *v1beta1.RolloutAnalysis, step int32) (SampleRequest, error) {
	addr := in.BundledPrometheusAddress
	var token string
	var headers map[string]string
	var authRef *corev1.SecretKeySelector
	if src := in.Prometheus; src != nil { // canary-level shared source
		if src.ServerAddress != "" {
			addr = src.ServerAddress
		}
		// Clone the map: src.Headers points into the live ISVC spec, and the result
		// is read by the background eval goroutine. A reference copy would hand a
		// spec-owned map to another goroutine, breaking the snapshot invariant.
		headers = maps.Clone(src.Headers)
		if src.AuthRef != nil {
			authRef = src.AuthRef
			t, err := resolveBearerToken(ctx, in.Reader, in.ISVC.Namespace, src.AuthRef)
			if err != nil {
				return SampleRequest{}, fmt.Errorf("resolve bearer token: %w", err)
			}
			token = t
		}
	}
	// The per-revision Services the queries name carry the Component's
	// replica prefix, like every per-revision object of the Component.
	prefix := irprojector.RoleReplicaPrefix(in.ISVC, in.Component)
	return SampleRequest{
		Key: SampleKey{
			Namespace: in.ISVC.Namespace,
			ISVCName:  in.ISVC.Name,
			Component: string(in.Component),
			Revision:  in.CanaryRevisionHash,
			Step:      step,
			PlanHash:  analysisFingerprint(a, addr, headers, authRef, token),
		},
		ServerAddress: addr,
		BearerToken:   token,
		Headers:       headers,
		TemplateContext: analysis.TemplateContext{
			Namespace:      in.ISVC.Namespace,
			ISVCName:       in.ISVC.Name,
			Component:      string(in.Component),
			CanaryService:  coordination.PerRevisionServiceName(prefix, in.Component, in.CanaryRevisionHash),
			StableService:  coordination.PerRevisionServiceName(prefix, in.Component, in.StableRevisionHash),
			CanaryRevision: in.CanaryRevisionHash,
			StableRevision: in.StableRevisionHash,
		},
		// DeepCopy so the background eval never reads through a pointer into the live
		// spec (the reconcile owns it).
		Analysis:     a.DeepCopy(),
		QueryTimeout: in.QueryTimeout,
	}, nil
}

// analysisFingerprint is a SHA-256 identity of one effective analysis request:
// the step's checks (the full RolloutAnalysis spec) plus the resolved source
// (server address, headers, auth Secret reference, and a digest of the resolved
// token — never the raw secret material). It keys the sample cache and its
// in-flight dedup, so any edit to the plan or its source changes the key: the
// edited plan kicks a fresh query instead of consuming a result produced under
// the previous configuration, and the superseded entry ages out unread. The
// fingerprint is an internal cache key only, never surfaced in status or logs.
func analysisFingerprint(a *v1beta1.RolloutAnalysis, addr string, headers map[string]string, authRef *corev1.SecretKeySelector, token string) string {
	h := sha256.New()
	// Length-prefix every field so adjacent values cannot collide across field
	// boundaries ("ab"+"c" vs "a"+"bc"). Hash writes never fail.
	field := func(s string) {
		fmt.Fprintf(h, "%d:", len(s))
		_, _ = io.WriteString(h, s)
	}
	spec, err := json.Marshal(a)
	if err != nil {
		// RolloutAnalysis is a plain API struct and always marshals; fall back to a
		// verbose print so the fingerprint still tracks the spec if that ever breaks.
		spec = fmt.Appendf(nil, "%#v", a)
	}
	field(string(spec))
	field(addr)
	for _, k := range slices.Sorted(maps.Keys(headers)) {
		field(k)
		field(headers[k])
	}
	if authRef != nil {
		field(authRef.Name)
		field(authRef.Key)
	}
	// A digest, not the token: a rotated credential must invalidate results, but
	// raw secret material must never feed a value that could be logged or compared.
	tokenDigest := sha256.Sum256([]byte(token))
	field(hex.EncodeToString(tokenDigest[:]))
	return hex.EncodeToString(h.Sum(nil))
}

// resolveBearerToken reads the token value from the referenced Secret key in the
// ISVC namespace, through the live API reader — a cached-client Get would start
// a cluster-wide Secret informer on first use. SecretKeySelector is
// namespace-local by design — analysis auth never crosses namespaces.
func resolveBearerToken(ctx context.Context, c client.Reader, namespace string, ref *corev1.SecretKeySelector) (string, error) {
	if c == nil {
		return "", fmt.Errorf("no reader to read secret %q", ref.Name)
	}
	sec := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, sec); err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("secret %s/%s not found", namespace, ref.Name)
		}
		return "", err
	}
	b, ok := sec.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("secret %s/%s has no key %q", namespace, ref.Name, ref.Key)
	}
	return string(b), nil
}

// inconclusiveResult builds a single-metric inconclusive Result for a setup
// failure that occurs before any metric query runs.
func inconclusiveResult(name, msg string) analysis.Result {
	return analysis.Result{
		Outcome: analysis.Inconclusive,
		Metrics: []analysis.MetricResult{{Name: name, Message: msg}},
	}
}
