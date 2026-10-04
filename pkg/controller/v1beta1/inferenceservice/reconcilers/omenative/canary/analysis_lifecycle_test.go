package canary

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
)

// These tests drive evaluateAnalysisStep against the PRODUCTION sampling stack —
// NewPrometheusSampler → prometheusEval → a real HTTP client — with an httptest
// stub standing in for Prometheus. They pin the reconcile-facing lifecycle the
// per-layer units cannot: non-blocking kick + event wake-up, interval pacing of
// actual HTTP traffic, failure accrual to rollback, query-timeout mapping to an
// inconclusive sample, and the per-metric status surface after each consume.

// promStub serves one canned Prometheus instant-query response and records what
// each request carried on the wire (auth, headers, rendered query).
type promStub struct {
	srv *httptest.Server

	mu      sync.Mutex
	status  int
	body    string
	queries []string
	auths   []string
	orgs    []string
}

func newPromStub(t *testing.T) *promStub {
	t.Helper()
	p := &promStub{status: http.StatusOK}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		p.mu.Lock()
		p.queries = append(p.queries, r.Form.Get("query"))
		p.auths = append(p.auths, r.Header.Get("Authorization"))
		p.orgs = append(p.orgs, r.Header.Get("X-Scope-OrgID"))
		status, body := p.status, p.body
		p.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *promStub) respondVector(values ...string) {
	series := make([]string, 0, len(values))
	for i, v := range values {
		series = append(series, fmt.Sprintf(`{"metric":{"pod":"pod-%d"},"value":[1700000000,%q]}`, i, v))
	}
	body := `{"status":"success","data":{"resultType":"vector","result":[`
	for i, s := range series {
		if i > 0 {
			body += ","
		}
		body += s
	}
	body += `]}}`
	p.mu.Lock()
	p.status, p.body = http.StatusOK, body
	p.mu.Unlock()
}

func (p *promStub) hits() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queries)
}

func waitEvent(t *testing.T, events <-chan event.GenericEvent) {
	t.Helper()
	select {
	case <-events:
	case <-time.After(10 * time.Second):
		t.Fatal("no sampler completion event within 10s")
	}
}

func lifecycleAnalysis(interval time.Duration, failureLimit int32) *v1beta1.RolloutAnalysis {
	return &v1beta1.RolloutAnalysis{
		Interval:     metav1.Duration{Duration: interval},
		FailureLimit: failureLimit,
		Metrics: []v1beta1.AnalysisMetric{{
			Name:      "err",
			Query:     `err_rate{ns="{{.Namespace}}"}`,
			Operator:  v1beta1.ComparisonLTE,
			Threshold: "0.05",
		}},
	}
}

func lifecycleInputs(t *testing.T, s *Sampler) (ReconcileInputs, *v1beta1.CanaryStatus) {
	t.Helper()
	now := time.Now()
	cs := &v1beta1.CanaryStatus{
		CanaryRevisionHash: "canary-a-rev2",
		StepEnteredTime:    &metav1.Time{Time: now},
	}
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "canary-a-svc", Namespace: "canary-a"}}
	isvc.Status.Canary = cs
	return ReconcileInputs{
		ISVC:               isvc,
		Component:          v1beta1.EngineComponent,
		CanaryRevisionHash: "canary-a-rev2",
		StableRevisionHash: "canary-a-rev1",
		Now:                now,
		Sampler:            s,
	}, cs
}

// TestAnalysisLifecycle_FailureAccrualAndPacing walks the full production loop
// across reconciles: the first pass kicks a background HTTP query and holds; the
// completion event lets the next pass consume a failing sample and surface it in
// status; a reconcile inside the interval is throttled and sends NO HTTP traffic;
// after the interval a fresh query runs and the second breach reaches
// FailureLimit → rollback. Auth from the referenced Secret and the shared headers
// ride every request.
func TestAnalysisLifecycle_FailureAccrualAndPacing(t *testing.T) {
	prom := newPromStub(t)
	prom.respondVector("0.5") // breaches the 0.05 threshold

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "canary-a-prom-auth", Namespace: "canary-a"},
		Data:       map[string][]byte{"token": []byte("canary-a-token")},
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	events := make(chan event.GenericEvent, 8)
	s := NewPrometheusSampler(events, 2, time.Hour)

	in, cs := lifecycleInputs(t, s)
	// The sampler stamps samples from the clock the gate paces by, as in
	// production; the passes below move that clock across the interval.
	s.now = func() time.Time { return in.Now }
	in.Reader = reader
	in.Prometheus = &v1beta1.AnalysisPrometheus{
		ServerAddress: prom.srv.URL,
		AuthRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "canary-a-prom-auth"},
			Key:                  "token",
		},
		Headers: map[string]string{"X-Scope-OrgID": "canary-a-tenant"},
	}
	a := lifecycleAnalysis(30*time.Second, 2)
	step := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("50%"), Traffic: 50}
	t0 := in.Now

	// Pass 1: cache miss — kick the background query, hold, never block.
	if got := evaluateAnalysisStep(context.Background(), in, a, cs, step); got != decHold {
		t.Fatalf("first pass = %v, want hold while the query is in flight", got)
	}
	waitEvent(t, events)

	// Pass 2: consume the failing sample; the breach accrues and surfaces.
	if got := evaluateAnalysisStep(context.Background(), in, a, cs, step); got != decHold {
		t.Fatalf("first breach with limit 2 = %v, want hold", got)
	}
	if cs.AnalysisFailedChecks != 1 {
		t.Fatalf("AnalysisFailedChecks = %d, want 1", cs.AnalysisFailedChecks)
	}
	if cs.LastEvaluationTime == nil || cs.LastConclusiveEvaluationTime == nil {
		t.Fatal("a conclusive sample must stamp both evaluation timestamps")
	}
	if len(cs.MetricResults) != 1 {
		t.Fatalf("MetricResults = %+v, want exactly one entry", cs.MetricResults)
	}
	mr := cs.MetricResults[0]
	if mr.Name != "err" || mr.Passed || mr.Value != "0.5" || mr.Threshold != "0.05" ||
		mr.Operator != v1beta1.ComparisonLTE || mr.Time == nil {
		t.Fatalf("surfaced metric result = %+v, want the failing err sample verbatim", mr)
	}
	if !mr.Time.Time.Equal(cs.LastEvaluationTime.Time) {
		t.Fatal("metric result must be stamped with the sample's produced time")
	}

	// Pass 3, inside the interval: throttled — no sampler read, no HTTP traffic.
	hitsBefore := prom.hits()
	in.Now = t0.Add(5 * time.Second)
	if got := evaluateAnalysisStep(context.Background(), in, a, cs, step); got != decHold {
		t.Fatalf("throttled pass = %v, want hold", got)
	}
	if got := prom.hits(); got != hitsBefore {
		t.Fatalf("throttled pass issued HTTP traffic: hits %d -> %d", hitsBefore, got)
	}

	// Pass 4, past the interval: the consumed sample is no longer fresh — a new
	// query is kicked; its breach reaches the limit and rolls back.
	in.Now = t0.Add(5 * time.Minute)
	if got := evaluateAnalysisStep(context.Background(), in, a, cs, step); got != decHold {
		t.Fatalf("post-interval pass = %v, want hold while the fresh query runs", got)
	}
	waitEvent(t, events)
	if got := evaluateAnalysisStep(context.Background(), in, a, cs, step); got != decRollback {
		t.Fatalf("second breach at limit 2 = %v, want rollback", got)
	}
	if cs.AnalysisFailedChecks != 2 {
		t.Fatalf("AnalysisFailedChecks = %d, want 2", cs.AnalysisFailedChecks)
	}

	// Exactly one query per interval window, each fully authenticated and
	// carrying the rendered template.
	if got := prom.hits(); got != 2 {
		t.Fatalf("prometheus hits = %d, want exactly 2 (one per interval)", got)
	}
	prom.mu.Lock()
	defer prom.mu.Unlock()
	for i := range prom.queries {
		if prom.queries[i] != `err_rate{ns="canary-a"}` {
			t.Errorf("query %d on the wire = %q, want the rendered template", i, prom.queries[i])
		}
		if prom.auths[i] != "Bearer canary-a-token" {
			t.Errorf("request %d Authorization = %q, want the Secret-resolved bearer", i, prom.auths[i])
		}
		if prom.orgs[i] != "canary-a-tenant" {
			t.Errorf("request %d X-Scope-OrgID = %q, want the shared header", i, prom.orgs[i])
		}
	}
}

// TestAnalysisLifecycle_PassAdvances runs the happy path over the bundled
// (unauthenticated) source: a multi-series healthy vector consumes as a Pass,
// surfaces the operator's worst series in status, and advances the step.
func TestAnalysisLifecycle_PassAdvances(t *testing.T) {
	prom := newPromStub(t)
	prom.respondVector("0.01", "0.04")

	events := make(chan event.GenericEvent, 8)
	s := NewPrometheusSampler(events, 2, time.Hour)

	in, cs := lifecycleInputs(t, s)
	in.BundledPrometheusAddress = prom.srv.URL
	a := lifecycleAnalysis(30*time.Second, 2)
	step := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("50%"), Traffic: 50}

	if got := evaluateAnalysisStep(context.Background(), in, a, cs, step); got != decHold {
		t.Fatalf("first pass = %v, want hold while the query is in flight", got)
	}
	waitEvent(t, events)
	if got := evaluateAnalysisStep(context.Background(), in, a, cs, step); got != decAdvance {
		t.Fatalf("passing sample on an unpaused step = %v, want advance", got)
	}
	if cs.AnalysisFailedChecks != 0 {
		t.Fatalf("AnalysisFailedChecks = %d, want 0 on a pass", cs.AnalysisFailedChecks)
	}
	mr := cs.MetricResults[0]
	if !mr.Passed || mr.Value != "0.04" {
		t.Fatalf("surfaced result = %+v, want passed with the worst series (0.04)", mr)
	}
	prom.mu.Lock()
	defer prom.mu.Unlock()
	for i, auth := range prom.auths {
		if auth != "" {
			t.Errorf("request %d sent Authorization %q; the bundled source is unauthenticated", i, auth)
		}
	}
}

// TestAnalysisLifecycle_QueryTimeoutIsInconclusive pins the timeout path end to
// end: a hung Prometheus is cut off by QueryTimeout in the background goroutine,
// and the reconcile consumes an inconclusive sample — held, surfaced with a
// message, never counted against the failure budget.
func TestAnalysisLifecycle_QueryTimeoutIsInconclusive(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Hang until the sampler's query timeout fires; release unblocks teardown
		// (it must run before the server's Close, which waits on the handler).
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer srv.Close()
	defer close(release)

	events := make(chan event.GenericEvent, 8)
	s := NewPrometheusSampler(events, 2, time.Hour)

	in, cs := lifecycleInputs(t, s)
	in.BundledPrometheusAddress = srv.URL
	in.QueryTimeout = 100 * time.Millisecond
	a := lifecycleAnalysis(30*time.Second, 2)
	step := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("50%"), Traffic: 50}

	if got := evaluateAnalysisStep(context.Background(), in, a, cs, step); got != decHold {
		t.Fatalf("first pass = %v, want hold", got)
	}
	waitEvent(t, events) // arrives once the 100ms query timeout fires
	if got := evaluateAnalysisStep(context.Background(), in, a, cs, step); got != decHold {
		t.Fatalf("inconclusive sample = %v, want hold (not a breach)", got)
	}
	if cs.AnalysisFailedChecks != 0 {
		t.Fatalf("AnalysisFailedChecks = %d; a timeout must not burn the failure budget", cs.AnalysisFailedChecks)
	}
	if cs.LastConclusiveEvaluationTime != nil {
		t.Fatal("a timed-out sample is not conclusive")
	}
	if cs.LastEvaluationTime == nil {
		t.Fatal("the inconclusive sample must still stamp LastEvaluationTime (it paces the next query)")
	}
	mr := cs.MetricResults[0]
	if mr.Passed || mr.Value != "" || mr.Message == "" {
		t.Fatalf("surfaced result = %+v, want an unevaluated metric carrying the timeout error", mr)
	}
}

// TestAnalysisLifecycle_NoSourceIsInconclusive pins the no-source contract: with
// neither a canary-level ServerAddress nor a bundled default, the sample reads
// inconclusive — the step holds and surfaces why, rather than advancing ungated
// or counting a breach.
func TestAnalysisLifecycle_NoSourceIsInconclusive(t *testing.T) {
	events := make(chan event.GenericEvent, 8)
	s := NewPrometheusSampler(events, 2, time.Hour)

	in, cs := lifecycleInputs(t, s)
	a := lifecycleAnalysis(30*time.Second, 2)
	step := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("50%"), Traffic: 50}

	if got := evaluateAnalysisStep(context.Background(), in, a, cs, step); got != decHold {
		t.Fatalf("first pass = %v, want hold", got)
	}
	waitEvent(t, events)
	if got := evaluateAnalysisStep(context.Background(), in, a, cs, step); got != decHold {
		t.Fatalf("no-source sample = %v, want hold", got)
	}
	if cs.AnalysisFailedChecks != 0 {
		t.Fatalf("AnalysisFailedChecks = %d; a missing source is not a breach", cs.AnalysisFailedChecks)
	}
	if cs.LastConclusiveEvaluationTime != nil {
		t.Fatal("a no-source sample is not conclusive")
	}
	if len(cs.MetricResults) == 0 || cs.MetricResults[0].Message == "" {
		t.Fatalf("MetricResults = %+v, want an entry explaining the failed sample", cs.MetricResults)
	}
}

// TestConsumeSample_ReplacesStatusSurface pins that each consumed sample REPLACES
// the per-metric status surface (it is "the most recent evaluation", not a log)
// and stamps it with the sample's produced time, not the reconcile's clock.
func TestConsumeSample_ReplacesStatusSurface(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	cs := &v1beta1.CanaryStatus{CanaryRevisionHash: "canary-a-rev2", StepEnteredTime: &metav1.Time{Time: now}}
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "canary-a-svc", Namespace: "canary-a"}}
	isvc.Status.Canary = cs
	in := ReconcileInputs{ISVC: isvc, Component: v1beta1.EngineComponent, CanaryRevisionHash: "canary-a-rev2", Now: now}
	step := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("50%"), Traffic: 50}
	a := lifecycleAnalysis(30*time.Second, 3)

	producedAt1 := now.Add(-10 * time.Second)
	res1 := analysis.Result{Outcome: analysis.Fail, Metrics: []analysis.MetricResult{
		{Name: "err", Value: "0.5", Threshold: "0.05", Operator: v1beta1.ComparisonLTE},
		{Name: "lat", Value: "90", Threshold: "100", Operator: v1beta1.ComparisonLTE, Passed: true},
	}}
	if got := consumeSample(in, cs, step, a, res1, producedAt1); got != decHold {
		t.Fatalf("first breach with limit 3 = %v, want hold", got)
	}
	if len(cs.MetricResults) != 2 {
		t.Fatalf("MetricResults = %+v, want both metrics surfaced", cs.MetricResults)
	}
	if !cs.MetricResults[0].Time.Time.Equal(producedAt1) {
		t.Fatalf("result time = %v, want the produced time %v (not the reconcile clock)", cs.MetricResults[0].Time, producedAt1)
	}
	if !cs.LastEvaluationTime.Time.Equal(producedAt1) {
		t.Fatal("LastEvaluationTime must be the sample's produced time")
	}

	producedAt2 := now.Add(-time.Second)
	res2 := analysis.Result{Outcome: analysis.Pass, Metrics: []analysis.MetricResult{
		{Name: "err", Value: "0.01", Threshold: "0.05", Operator: v1beta1.ComparisonLTE, Passed: true},
	}}
	if got := consumeSample(in, cs, step, a, res2, producedAt2); got != decAdvance {
		t.Fatalf("pass on an unpaused step = %v, want advance", got)
	}
	if len(cs.MetricResults) != 1 || cs.MetricResults[0].Name != "err" || !cs.MetricResults[0].Passed {
		t.Fatalf("MetricResults = %+v, want the latest sample only (replaced, not appended)", cs.MetricResults)
	}
	if !cs.LastEvaluationTime.Time.Equal(producedAt2) {
		t.Fatal("LastEvaluationTime must advance to the new sample's produced time")
	}
}
