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

// sampledMember is one member of the unit the analysis gate reads: the
// Component whose canary pods the queries name, and its revision pair.
type sampledMember struct {
	component  v1beta1.ComponentType
	canaryHash string
	stableHash string
}

// primaryMember is the unit's primary as the gate reads it: the Component
// that carries the record, on the revision pair the executor was handed.
func primaryMember(in ReconcileInputs) sampledMember {
	return sampledMember{component: in.Component, canaryHash: in.CanaryRevisionHash, stableHash: in.StableRevisionHash}
}

// sampledMembers lists the members the gate samples, each on its own canary
// revision: the primary first, then each secondary in name order. A member
// is read when the recorded run moved it to a new revision. A member the run
// left alone serves its stable revision on its target, so a sample of it
// would grade the revision the unit keeps, not the one it brings in; it is
// not read, and on a push that moved only a secondary the primary is not
// read either. A secondary is read only once the executor observed a target
// distinct from its stable revision, the pair its traffic is written on. A
// run that recorded no target for a member is read as having moved it, as
// the crash reading does.
func sampledMembers(in ReconcileInputs) []sampledMember {
	var out []sampledMember
	if memberRetargeted(in.ISVC, in.Component) {
		out = append(out, primaryMember(in))
	}
	for _, c := range slices.Sorted(maps.Keys(in.Secondaries)) {
		m := in.Secondaries[c]
		if !m.retargeted() || !memberRetargeted(in.ISVC, c) {
			continue
		}
		out = append(out, sampledMember{component: c, canaryHash: m.CanaryRevisionHash, stableHash: m.StableRevisionHash})
	}
	return out
}

// sampleSource is the metrics source one evaluation queries, resolved once
// and shared by every member's request: the address, the bearer token and
// headers, and the auth Secret reference the fingerprint names. The address
// is the canary's own ServerAddress when set, else the operator-configured
// bundled address; an empty address means "no source" and the eval's query
// reads inconclusive.
type sampleSource struct {
	address string
	token   string
	headers map[string]string
	authRef *corev1.SecretKeySelector
}

// resolveSampleSource resolves the source the canary group declares, reading
// the bearer token through the live API reader. Nothing it returns points
// into the live InferenceService: the headers are cloned out of the spec,
// because the background eval goroutine reads the result while the
// reconcile owns the object.
func resolveSampleSource(ctx context.Context, in ReconcileInputs) (sampleSource, error) {
	src := sampleSource{address: in.BundledPrometheusAddress}
	p := in.Prometheus // canary-level shared source
	if p == nil {
		return src, nil
	}
	if p.ServerAddress != "" {
		src.address = p.ServerAddress
	}
	src.headers = maps.Clone(p.Headers)
	if p.AuthRef != nil {
		src.authRef = p.AuthRef
		t, err := resolveBearerToken(ctx, in.Reader, in.ISVC.Namespace, p.AuthRef)
		if err != nil {
			return sampleSource{}, fmt.Errorf("resolve bearer token: %w", err)
		}
		src.token = t
	}
	return src, nil
}

// buildSampleRequest snapshots everything the background eval needs to query
// one step's analysis for one member WITHOUT touching the live
// InferenceService — the reconcile owns it, so a goroutine reading it would
// race. The source is resolved up front; the eval then only performs the
// HTTP query. The request is keyed by the member and its own canary
// revision, so each member's sample is cached and deduplicated on its own,
// and its template names the member's per-revision Services.
func buildSampleRequest(in ReconcileInputs, src sampleSource, m sampledMember, a *v1beta1.RolloutAnalysis, step int32) SampleRequest {
	// The per-revision Services the queries name carry the member's replica
	// prefix, like every per-revision object of the Component.
	prefix := irprojector.RoleReplicaPrefix(in.ISVC, m.component)
	return SampleRequest{
		Key: SampleKey{
			Namespace: in.ISVC.Namespace,
			ISVCName:  in.ISVC.Name,
			Component: string(m.component),
			Revision:  m.canaryHash,
			Step:      step,
			PlanHash:  analysisFingerprint(a, src.address, src.headers, src.authRef, src.token),
		},
		ServerAddress: src.address,
		BearerToken:   src.token,
		Headers:       src.headers,
		TemplateContext: analysis.TemplateContext{
			Namespace:      in.ISVC.Namespace,
			ISVCName:       in.ISVC.Name,
			Component:      string(m.component),
			CanaryService:  coordination.PerRevisionServiceName(prefix, m.component, m.canaryHash),
			StableService:  coordination.PerRevisionServiceName(prefix, m.component, m.stableHash),
			CanaryRevision: m.canaryHash,
			StableRevision: m.stableHash,
		},
		// DeepCopy so the background eval never reads through a pointer into the live
		// spec (the reconcile owns it).
		Analysis:     a.DeepCopy(),
		QueryTimeout: in.QueryTimeout,
	}
}

// analysisFingerprint is a SHA-256 identity of one effective analysis request:
// the step's checks (the full RolloutAnalysis spec) plus the resolved source
// (server address, headers, auth Secret reference, and a digest of the resolved
// token — never the raw secret material). It keys the sample cache and its
// in-flight dedup, so any edit to the plan or its source changes the key: the
// edited plan kicks a fresh query instead of consuming a result produced under
// the previous configuration, and the replaced entry ages out unread. The
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
