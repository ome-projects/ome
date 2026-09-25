package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
)

const testCapacityMaxResponseBytes int64 = 1 << 20

var (
	capacityTestMap = types.NamespacedName{Name: "model-service", Namespace: "team-a"}
	capacityTestNow = time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
)

func testCapacityConfig() CapacityConfig {
	return CapacityConfig{
		Path:    "/capacity",
		Method:  http.MethodGet,
		Format:  FormatReport,
		Period:  time.Second,
		Timeout: 500 * time.Millisecond,
		Samples: 10,
		Quorum:  2,
		MaxAge:  30 * time.Second,
	}
}

// singleSampleConfig applies each reading immediately (window of one, quorum of
// one), isolating tests that are about plumbing rather than aggregation.
func singleSampleConfig() CapacityConfig {
	c := testCapacityConfig()
	c.Samples, c.Quorum = 1, 1
	return c
}

func testCapacityPolicy(t *testing.T, config CapacityConfig) ResolvedCapacityPolicy {
	t.Helper()
	digest, err := capacityPolicyDigest(config)
	if err != nil {
		t.Fatalf("capacityPolicyDigest() error = %v", err)
	}
	return ResolvedCapacityPolicy{Enabled: true, Capacity: config, PolicyDigest: digest}
}

func newTestPoller(t *testing.T) *CapacityPoller {
	t.Helper()
	executor, _, _ := startTestObserverExecutor(t, 2, 2)
	return newTestPollerWithRuntime(
		t,
		executor,
		NewObserverClient(),
		clocktesting.NewFakeClock(capacityTestNow),
		testCapacityMaxResponseBytes,
	)
}

func newTestPollerWithRuntime(
	t *testing.T,
	executor *ObserverExecutor,
	client *http.Client,
	clk clock.Clock,
	maxResponseBytes int64,
) *CapacityPoller {
	t.Helper()
	poller, err := NewCapacityPoller(executor, client, clk, maxResponseBytes, logr.Discard())
	if err != nil {
		t.Fatalf("NewCapacityPoller() error = %v", err)
	}
	return poller
}

func testCapacityTarget(cluster, endpoint string) Target {
	return Target{
		OwnerUID: "owner-uid",
		Map:      capacityTestMap,
		Cluster:  cluster,
		URL:      endpoint,
	}
}

func testCapacityRequest(policy ResolvedCapacityPolicy, targets ...Target) CapacityReconcileRequest {
	return CapacityReconcileRequest{
		OwnerUID: "owner-uid",
		Map:      capacityTestMap,
		Policy:   policy,
		Targets:  targets,
	}
}

// capacityServer serves a fixed body, so each test states exactly the report
// shape it is exercising.
func capacityServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/capacity" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func freshReport(servable int32) string {
	b, _ := json.Marshal(CapacityReport{Servable: servable, ObservedAt: capacityTestNow})
	return string(b)
}

func TestNewCapacityPollerRequiresRuntimeDependenciesAndLimit(t *testing.T) {
	executor, err := NewObserverExecutor(1, 1)
	if err != nil {
		t.Fatalf("NewObserverExecutor() error = %v", err)
	}
	client := NewObserverClient()
	clk := clocktesting.NewFakeClock(capacityTestNow)
	tests := []struct {
		name       string
		executor   *ObserverExecutor
		client     *http.Client
		clock      clock.Clock
		maxBytes   int64
		wantErrSub string
	}{
		{"nil executor", nil, client, clk, 1, "executor is nil"},
		{"nil client", executor, nil, clk, 1, "client is nil"},
		{"nil clock", executor, client, nil, 1, "clock is nil"},
		{"zero response bound", executor, client, clk, 0, "must be positive"},
		{"negative response bound", executor, client, clk, -1, "must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewCapacityPoller(tt.executor, tt.client, tt.clock, tt.maxBytes, logr.Discard())
			if err == nil || !strings.Contains(err.Error(), tt.wantErrSub) {
				t.Fatalf("NewCapacityPoller() error = %v, want containing %q", err, tt.wantErrSub)
			}
		})
	}
}

func TestCapacityPoller_AcceptsAFreshReport(t *testing.T) {
	srv := capacityServer(t, http.StatusOK, freshReport(3))
	p := newTestPoller(t)
	config := testCapacityConfig()

	got, reason := p.pollOne(context.Background(), testCapacityTarget("cluster-a", srv.URL), config)
	if got == nil {
		t.Fatalf("pollOne returned no report: %s", reason)
	}
	if *got != 3 {
		t.Fatalf("servable = %d, want 3", *got)
	}
	if reason != "" {
		t.Fatalf("reason = %q, want empty on an accepted report", reason)
	}
}

func TestCapacityPoller_AcceptsAReportOfZero(t *testing.T) {
	// Zero is a real signal ("I can serve nothing"), not an absent report. The
	// ceiling drives the home's weight to 0, which is the point.
	srv := capacityServer(t, http.StatusOK, freshReport(0))
	p := newTestPoller(t)

	got, reason := p.pollOne(context.Background(), testCapacityTarget("cluster-a", srv.URL), testCapacityConfig())
	if got == nil {
		t.Fatalf("a report of zero was discarded: %s", reason)
	}
	if *got != 0 {
		t.Fatalf("servable = %d, want 0", *got)
	}
}

func TestCapacityPoller_FailsOpenOnEveryBadAnswer(t *testing.T) {
	// Every one of these must leave the control-plane plan standing. Fail-closed
	// would turn a control-plane-to-data-plane blip into a fleet-wide cutoff,
	// and a malformed or stale number must never be able to black-hole a home.
	stale, _ := json.Marshal(CapacityReport{Servable: 2, ObservedAt: capacityTestNow.Add(-time.Hour)})
	future, _ := json.Marshal(CapacityReport{Servable: 2, ObservedAt: capacityTestNow.Add(time.Hour)})
	negative, _ := json.Marshal(CapacityReport{Servable: -1, ObservedAt: capacityTestNow})
	noStamp, _ := json.Marshal(CapacityReport{Servable: 2})
	missingServable := fmt.Sprintf(`{"observedAt":%q}`, capacityTestNow.Format(time.RFC3339Nano))
	nullServable := fmt.Sprintf(`{"servable":null,"observedAt":%q}`, capacityTestNow.Format(time.RFC3339Nano))

	tests := []struct {
		name   string
		status int
		body   string
	}{
		{"non-200 response", http.StatusServiceUnavailable, freshReport(2)},
		{"unparsable body", http.StatusOK, "not json"},
		{"stale report beyond maxAge", http.StatusOK, string(stale)},
		{"future report timestamp", http.StatusOK, string(future)},
		{"negative servable count", http.StatusOK, string(negative)},
		{"report with no observedAt stamp", http.StatusOK, string(noStamp)},
		{"report with missing servable", http.StatusOK, missingServable},
		{"report with null servable", http.StatusOK, nullServable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := capacityServer(t, tt.status, tt.body)
			p := newTestPoller(t)
			got, reason := p.pollOne(context.Background(), testCapacityTarget("cluster-a", srv.URL), testCapacityConfig())
			if got != nil {
				t.Fatalf("report = %d, want nil (fail open to the plan)", *got)
			}
			if reason == "" {
				t.Fatal("fell open with no reason; an operator cannot tell this from agreement with the plan")
			}
		})
	}
}

type capacityBodyAfterCancel struct {
	ctx    context.Context
	body   []byte
	offset int
}

func (b *capacityBodyAfterCancel) Read(dst []byte) (int, error) {
	if b.offset == 0 {
		<-b.ctx.Done()
	}
	if b.offset == len(b.body) {
		return 0, io.EOF
	}
	n := copy(dst, b.body[b.offset:])
	b.offset += n
	return n, nil
}

func TestCapacityPoller_ResponseCompletingAfterDeadlineFallsOpen(t *testing.T) {
	client := &http.Client{Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(&capacityBodyAfterCancel{
				ctx:  request.Context(),
				body: []byte(freshReport(3)),
			}),
			Header: make(http.Header),
		}, nil
	})}
	executor, err := NewObserverExecutor(1, 1)
	if err != nil {
		t.Fatalf("NewObserverExecutor() error = %v", err)
	}
	poller := newTestPollerWithRuntime(
		t,
		executor,
		client,
		clocktesting.NewFakeClock(capacityTestNow),
		testCapacityMaxResponseBytes,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	reported, reason := poller.pollOne(ctx, testCapacityTarget("cluster-a", "https://cluster-a.example"), testCapacityConfig())
	if reported != nil {
		t.Fatalf("late response produced report %d, want nil", *reported)
	}
	if !strings.Contains(reason, context.DeadlineExceeded.Error()) {
		t.Fatalf("late response reason = %q, want deadline error", reason)
	}
}

func TestCapacityPoller_UnreachableEndpointFallsOpen(t *testing.T) {
	srv := capacityServer(t, http.StatusOK, freshReport(2))
	url := srv.URL
	srv.Close()

	p := newTestPoller(t)
	got, reason := p.pollOne(context.Background(), testCapacityTarget("cluster-a", url), testCapacityConfig())
	if got != nil {
		t.Fatalf("report = %d from an unreachable endpoint, want nil", *got)
	}
	if reason == "" {
		t.Fatal("unreachable endpoint produced no reason")
	}
}

func TestCapacityPoller_ReportedIsNilWhenPollingIsOff(t *testing.T) {
	off := newTestPoller(t)
	tgt := testCapacityTarget("cluster-a", "https://cluster-a.example")
	disabled := ResolvedCapacityPolicy{}
	if r := off.Reported(tgt, disabled); r != nil {
		t.Fatalf("Reported with polling off = %d, want nil", *r)
	}
	if reason := off.FellOpen(tgt, disabled); reason != "" {
		t.Fatalf("FellOpen with polling off = %q, want empty", reason)
	}
	var nilPoller *CapacityPoller
	if r := nilPoller.Reported(tgt, testCapacityPolicy(t, testCapacityConfig())); r != nil {
		t.Fatalf("Reported on nil poller = %d, want nil", *r)
	}
}

func TestCapacityPoller_RecordDetectsCeilingOrFallbackReasonChanges(t *testing.T) {
	p := newTestPoller(t)
	tgt := testCapacityTarget("cluster-a", "https://cluster-a.example")
	policy := testCapacityPolicy(t, singleSampleConfig())
	two, three := int32(2), int32(3)

	if !p.record(tgt, policy, &two, "") {
		t.Fatal("first report should count as a change")
	}
	if p.record(tgt, policy, &two, "") {
		t.Fatal("an unchanged report was marked as a state change")
	}
	if !p.record(tgt, policy, &three, "") {
		t.Fatal("a changed report was not marked as a state change")
	}
	// A home that stops reporting releases its ceiling, which changes the
	// projected weights.
	if !p.record(tgt, policy, nil, "capacity endpoint unreachable") {
		t.Fatal("losing a report was not marked as a state change")
	}
	if p.record(tgt, policy, nil, "capacity endpoint unreachable") {
		t.Fatal("an unchanged fallback was marked as a state change")
	}
	if !p.record(tgt, policy, nil, "capacity report is stale") {
		t.Fatal("a changed fallback reason was not marked as a state change")
	}
	if reason := p.FellOpen(tgt, policy); reason != "capacity report is stale" {
		t.Fatalf("FellOpen() = %q, want latest failure reason", reason)
	}
}

func TestCapacityPoller_FirstAcceptedSampleExplainsPendingQuorum(t *testing.T) {
	p := newTestPoller(t)
	tgt := testCapacityTarget("cluster-a", "https://cluster-a.example")
	policy := testCapacityPolicy(t, testCapacityConfig())
	two := int32(2)

	if !p.record(tgt, policy, &two, "") {
		t.Fatal("first below-quorum sample did not report its new fallback reason")
	}
	if got := p.Reported(tgt, policy); got != nil {
		t.Fatalf("Reported() = %d, want nil before quorum", *got)
	}
	if reason := p.FellOpen(tgt, policy); !strings.Contains(reason, "awaiting quorum (1/2 accepted samples)") {
		t.Fatalf("FellOpen() = %q, want a nonempty pending-quorum reason", reason)
	}
}

func TestCapacityPoller_StateIdentityIncludesOwnerMapClusterURLAndPolicy(t *testing.T) {
	p := newTestPoller(t)
	tgt := testCapacityTarget("cluster-a", "https://cluster-a.example/base")
	policy := testCapacityPolicy(t, singleSampleConfig())
	two := int32(2)
	p.record(tgt, policy, &two, "")

	tests := []struct {
		name   string
		mutate func(*Target)
	}{
		{"owner UID", func(other *Target) { other.OwnerUID = "replacement-owner" }},
		{"map namespace", func(other *Target) { other.Map.Namespace = "other-namespace" }},
		{"map name", func(other *Target) { other.Map.Name = "other-map" }},
		{"cluster", func(other *Target) { other.Cluster = "cluster-b" }},
		{"URL", func(other *Target) { other.URL = "https://other.example/base" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			other := tgt
			tt.mutate(&other)
			if got := p.Reported(other, policy); got != nil {
				t.Fatalf("Reported() for different %s = %d, want nil", tt.name, *got)
			}
		})
	}

	equivalent := tgt
	equivalent.URL = "https://cluster-a.example/base/"
	if got := p.Reported(equivalent, policy); got == nil || *got != 2 {
		t.Fatalf("Reported() for canonically equivalent URL = %v, want 2", got)
	}
	probeChanged := tgt
	probeChanged.PolicyDigest = "sha256:" + strings.Repeat("a", 64)
	if got := p.Reported(probeChanged, policy); got == nil || *got != 2 {
		t.Fatalf("Reported() after unrelated probe policy change = %v, want 2", got)
	}
	changedConfig := policy.Capacity
	changedConfig.Period *= 2
	changedPolicy := testCapacityPolicy(t, changedConfig)
	if got := p.Reported(tgt, changedPolicy); got != nil {
		t.Fatalf("Reported() for different policy = %d, want nil", *got)
	}
}

func TestCapacityPoller_LateGenerationAndForgetCannotRecreateState(t *testing.T) {
	p := newTestPoller(t)
	tgt := testCapacityTarget("cluster-a", "https://cluster-a.example")
	policy := testCapacityPolicy(t, singleSampleConfig())
	request := testCapacityRequest(policy, tgt)
	first := p.prepare(request, []Target{tgt})
	p.Forget(tgt.Map)
	second := p.prepare(request, []Target{tgt})
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("prepared work lengths = %d, %d; want 1, 1", len(first), len(second))
	}

	two, nine := int32(2), int32(9)
	if !p.recordCurrent(second[0].key, second[0].generation, policy.Capacity, &nine, capacityTestNow, "") {
		t.Fatal("current generation was not recorded")
	}
	if p.recordCurrent(first[0].key, first[0].generation, policy.Capacity, &two, capacityTestNow, "") {
		t.Fatal("late predecessor generation was accepted")
	}
	if got := p.Reported(tgt, policy); got == nil || *got != 9 {
		t.Fatalf("Reported() after late predecessor = %v, want current value 9", got)
	}
	p.Forget(tgt.Map)
	if p.recordCurrent(second[0].key, second[0].generation, policy.Capacity, &two, capacityTestNow, "") {
		t.Fatal("completion after Forget recreated state")
	}
	if got := p.Reported(tgt, policy); got != nil {
		t.Fatalf("Reported() after Forget = %d, want nil", *got)
	}
}

func TestCapacityPoller_ForgetRemovesOnlyOneTrafficMap(t *testing.T) {
	p := newTestPoller(t)
	policy := testCapacityPolicy(t, singleSampleConfig())
	first := testCapacityTarget("cluster-a", "https://cluster-a.example")
	second := testCapacityTarget("cluster-b", "https://cluster-b.example")
	second.Map = types.NamespacedName{Namespace: "other", Name: "service"}
	one := int32(1)
	two := int32(2)
	p.record(first, policy, &one, "")
	p.record(second, policy, &two, "")

	p.Forget(first.Map)

	if got := p.Reported(first, policy); got != nil {
		t.Fatalf("first Reported after Forget = %d, want nil", *got)
	}
	if got := p.Reported(second, policy); got == nil || *got != two {
		t.Fatalf("second Reported after Forget = %v, want %d", got, two)
	}
}

func TestCapacityConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*CapacityConfig)
		wantErr bool
	}{
		{"fully configured is valid", func(*CapacityConfig) {}, false},
		{"disabled is valid", func(c *CapacityConfig) { *c = CapacityConfig{} }, false},
		{"relative path rejected", func(c *CapacityConfig) { c.Path = "capacity" }, true},
		{"missing method rejected", func(c *CapacityConfig) { c.Method = "" }, true},
		{"missing format rejected", func(c *CapacityConfig) { c.Format = "" }, true},
		{"unsafe method rejected", func(c *CapacityConfig) { c.Method = "DELETE" }, true},
		{"zero samples rejected", func(c *CapacityConfig) { c.Samples = 0 }, true},
		{"zero quorum rejected", func(c *CapacityConfig) { c.Quorum = 0 }, true},
		{"zero period rejected", func(c *CapacityConfig) { c.Period = 0 }, true},
		{"zero timeout rejected", func(c *CapacityConfig) { c.Timeout = 0 }, true},
		// Overlapping requests would pile up until the effective poll rate
		// silently stopped matching the configured one.
		{"timeout >= period rejected", func(c *CapacityConfig) { c.Timeout = c.Period }, true},
		// Without a bound the staleness guard can never fire.
		{"zero maxAge rejected", func(c *CapacityConfig) { c.MaxAge = 0 }, true},
		// Every report would age out before the next poll, flapping the ceiling.
		{"maxAge < period rejected", func(c *CapacityConfig) { c.MaxAge = c.Period / 2 }, true},
		{"maxAge shorter than quorum window rejected", func(c *CapacityConfig) { c.MaxAge = c.Period }, true},
		{"maxAge equal to quorum window accepted", func(c *CapacityConfig) { c.MaxAge = time.Duration(c.quorum()) * c.Period }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testCapacityConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("Validate() = nil, want an error for %+v", cfg)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

func TestProbeConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ProbeConfig)
		wantErr bool
	}{
		{"fully configured is valid", func(*ProbeConfig) {}, false},
		{"disabled is valid", func(c *ProbeConfig) { *c = ProbeConfig{} }, false},
		{"relative path rejected", func(c *ProbeConfig) { c.Path = "health" }, true},
		{"missing method rejected", func(c *ProbeConfig) { c.Method = "" }, true},
		{"unsafe method rejected", func(c *ProbeConfig) { c.Method = "CONNECT" }, true},
		{"missing accept statuses rejected", func(c *ProbeConfig) { c.AcceptStatuses = nil }, true},
		{"missing gate statuses rejected", func(c *ProbeConfig) { c.GateStatuses = nil }, true},
		{"invalid accept status rejected", func(c *ProbeConfig) { c.AcceptStatuses = []int{99} }, true},
		{"invalid gate status rejected", func(c *ProbeConfig) { c.GateStatuses = []int{600} }, true},
		{"authentication failure cannot gate", func(c *ProbeConfig) { c.GateStatuses = []int{401} }, true},
		{"authorization failure cannot gate", func(c *ProbeConfig) { c.GateStatuses = []int{403} }, true},
		{"overload cannot gate", func(c *ProbeConfig) { c.GateStatuses = []int{429} }, true},
		// An ambiguous status has no defined verdict, so the policy would
		// depend on list order rather than on the operator's intent.
		{"status in both lists rejected", func(c *ProbeConfig) { c.GateStatuses = append(c.GateStatuses, 200) }, true},
		{"zero period rejected", func(c *ProbeConfig) { c.Period = 0 }, true},
		{"zero timeout rejected", func(c *ProbeConfig) { c.Timeout = 0 }, true},
		{"timeout >= period rejected", func(c *ProbeConfig) { c.Timeout = c.Period }, true},
		{"zero failure threshold rejected", func(c *ProbeConfig) { c.FailureThreshold = 0 }, true},
		{"zero success threshold rejected", func(c *ProbeConfig) { c.SuccessThreshold = 0 }, true},
		{"missing all-failed policy rejected", func(c *ProbeConfig) { c.AllFailedPolicy = "" }, true},
		{"unknown all-failed policy rejected", func(c *ProbeConfig) { c.AllFailedPolicy = "DropEverything" }, true},
		{"drain all-failed policy is valid", func(c *ProbeConfig) { c.AllFailedPolicy = AllFailedPolicyDrain }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testProbeConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("Validate() = nil, want an error for %+v", cfg)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

func TestConfig_ValidateHonorsInstallationGate(t *testing.T) {
	tests := []struct {
		name   string
		config Config
	}{
		{
			name: "invalid probe is inert",
			config: Config{Probe: func() ProbeConfig {
				probe := testProbeConfig()
				probe.Method = "DELETE"
				return probe
			}()},
		},
		{
			name: "unavailable capacity format is inert",
			config: Config{Capacity: func() CapacityConfig {
				capacity := testCapacityConfig()
				capacity.Format = CapacityFormat("Unavailable")
				return capacity
			}()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			disabled := tt.config
			disabled.Enabled = false
			if err := disabled.Validate(); err != nil {
				t.Fatalf("disabled Validate() = %v, want nil", err)
			}

			enabled := tt.config
			enabled.Enabled = true
			if err := enabled.Validate(); err == nil {
				t.Fatal("enabled Validate() = nil, want staged configuration error")
			}
		})
	}
}

func TestAllocationSource(t *testing.T) {
	// Endpoint is claimed only when a report actually lowered the plan.
	// Attributing a control-plane number to the home would send an operator
	// looking at the wrong side of the system.
	two, nine := int32(2), int32(9)
	tests := []struct {
		name string
		home Home
		want string
	}{
		{"no report is control plane", Home{Allocated: 7}, "ControlPlane"},
		{"report above plan is control plane", Home{Allocated: 7, Reported: &nine}, "ControlPlane"},
		{"report equal to plan is control plane", Home{Allocated: 2, Reported: &two}, "ControlPlane"},
		{"report below plan is endpoint", Home{Allocated: 7, Reported: &two}, "Endpoint"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := string(allocationSource(tt.home)); got != tt.want {
				t.Fatalf("allocationSource() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCapacityPoller_BodyIsBounded(t *testing.T) {
	// A home streaming an unbounded body must not be able to exhaust
	// control-plane memory, and truncation must not be mistaken for decoding.
	body := freshReport(2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	executor, err := NewObserverExecutor(1, 1)
	if err != nil {
		t.Fatalf("NewObserverExecutor() error = %v", err)
	}
	p, err := NewCapacityPoller(
		executor,
		NewObserverClient(),
		clocktesting.NewFakeClock(capacityTestNow),
		int64(len(body)-1),
		logr.Discard(),
	)
	if err != nil {
		t.Fatalf("NewCapacityPoller() error = %v", err)
	}
	got, reason := p.pollOne(context.Background(), testCapacityTarget("cluster-a", srv.URL), testCapacityConfig())
	if got != nil {
		t.Fatalf("oversized body produced a report %d, want nil", *got)
	}
	if !strings.Contains(reason, "exceeds configured limit") {
		t.Fatalf("oversized body reason = %q, want configured-limit error", reason)
	}
}

func TestCapacityPoller_AcceptsBodyAtConfiguredLimit(t *testing.T) {
	body := freshReport(2)
	srv := capacityServer(t, http.StatusOK, string(body))
	executor, err := NewObserverExecutor(1, 1)
	if err != nil {
		t.Fatalf("NewObserverExecutor() error = %v", err)
	}
	p, err := NewCapacityPoller(
		executor,
		NewObserverClient(),
		clocktesting.NewFakeClock(capacityTestNow),
		int64(len(body)),
		logr.Discard(),
	)
	if err != nil {
		t.Fatalf("NewCapacityPoller() error = %v", err)
	}

	got, reason := p.pollOne(context.Background(), testCapacityTarget("cluster-a", srv.URL), testCapacityConfig())
	if got == nil || *got != 2 {
		t.Fatalf("report at exact limit = %v, want 2; reason = %q", got, reason)
	}
}

func TestCapacityPollerReconcileUsesAbsoluteDeadlines(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(freshReport(3)))
	}))
	defer server.Close()

	clk := clocktesting.NewFakeClock(capacityTestNow)
	executor, _, _ := startTestObserverExecutor(t, 1, 1)
	poller := newTestPollerWithRuntime(t, executor, NewObserverClient(), clk, testCapacityMaxResponseBytes)
	policy := testCapacityPolicy(t, singleSampleConfig())
	target := testCapacityTarget("cluster-a", server.URL)
	request := testCapacityRequest(policy, target)

	delay, err := poller.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("first Reconcile() error = %v", err)
	}
	if delay != policy.Capacity.Period || requests.Load() != 1 {
		t.Fatalf("first delay/requests = %s/%d, want %s/1", delay, requests.Load(), policy.Capacity.Period)
	}

	delay, err = poller.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("early Reconcile() error = %v", err)
	}
	if delay != policy.Capacity.Period || requests.Load() != 1 {
		t.Fatalf("early delay/requests = %s/%d, want %s/1", delay, requests.Load(), policy.Capacity.Period)
	}

	clk.Step(policy.Capacity.Period - time.Millisecond)
	delay, err = poller.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("near-deadline Reconcile() error = %v", err)
	}
	if delay != time.Millisecond || requests.Load() != 1 {
		t.Fatalf("near-deadline delay/requests = %s/%d, want %s/1", delay, requests.Load(), time.Millisecond)
	}

	clk.Step(time.Millisecond)
	delay, err = poller.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("due Reconcile() error = %v", err)
	}
	if delay != policy.Capacity.Period || requests.Load() != 2 {
		t.Fatalf("due delay/requests = %s/%d, want %s/2", delay, requests.Load(), policy.Capacity.Period)
	}

	clk.Step(5 * policy.Capacity.Period / 2)
	delay, err = poller.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("overdue Reconcile() error = %v", err)
	}
	if delay != policy.Capacity.Period/2 || requests.Load() != 3 {
		t.Fatalf("overdue delay/requests = %s/%d, want %s/3", delay, requests.Load(), policy.Capacity.Period/2)
	}
}

func TestCapacityPollerReconcileExpiresAReportBeforeTheNextPoll(t *testing.T) {
	var requests atomic.Int32
	observedAt := capacityTestNow.Add(-25 * time.Second)
	body, err := json.Marshal(CapacityReport{Servable: 3, ObservedAt: observedAt})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write(body)
	}))
	defer server.Close()

	config := singleSampleConfig()
	config.Period = 10 * time.Second
	config.Timeout = time.Second
	config.MaxAge = 30 * time.Second
	policy := testCapacityPolicy(t, config)
	clk := clocktesting.NewFakeClock(capacityTestNow)
	executor, _, _ := startTestObserverExecutor(t, 1, 1)
	poller := newTestPollerWithRuntime(t, executor, NewObserverClient(), clk, testCapacityMaxResponseBytes)
	target := testCapacityTarget("cluster-a", server.URL)
	request := testCapacityRequest(policy, target)

	delay, err := poller.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("first Reconcile() error = %v", err)
	}
	if delay != 5*time.Second || requests.Load() != 1 {
		t.Fatalf("first delay/requests = %s/%d, want 5s/1", delay, requests.Load())
	}
	if got := poller.Reported(target, policy); got == nil || *got != 3 {
		t.Fatalf("Reported() before expiry = %v, want 3", got)
	}

	clk.Step(5 * time.Second)
	delay, err = poller.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("expiry Reconcile() error = %v", err)
	}
	if delay != 5*time.Second || requests.Load() != 1 {
		t.Fatalf("expiry delay/requests = %s/%d, want 5s/1", delay, requests.Load())
	}
	if got := poller.Reported(target, policy); got != nil {
		t.Fatalf("Reported() at expiry = %d, want nil", *got)
	}
	if reason := poller.FellOpen(target, policy); !strings.Contains(reason, "stale") {
		t.Fatalf("FellOpen() at expiry = %q, want stale-report reason", reason)
	}
}

func TestCapacityPollerExpiresSamplesIndividually(t *testing.T) {
	config := testCapacityConfig()
	config.Period = 10 * time.Second
	config.Timeout = time.Second
	config.Samples = 3
	config.Quorum = 2
	config.MaxAge = 30 * time.Second
	policy := testCapacityPolicy(t, config)
	poller := newTestPoller(t)
	target := testCapacityTarget("cluster-a", "https://cluster-a.example")

	one, two, nine := int32(1), int32(2), int32(9)
	poller.recordAt(target, policy, &one, capacityTestNow.Add(-29*time.Second), "")
	poller.recordAt(target, policy, &two, capacityTestNow, "")
	poller.recordAt(target, policy, &nine, capacityTestNow, "")
	if got := poller.Reported(target, policy); got == nil || *got != 2 {
		t.Fatalf("Reported() before sample expiry = %v, want 2", got)
	}

	poller.Clock.(*clocktesting.FakeClock).Step(time.Second)
	if got := poller.Reported(target, policy); got == nil || *got != 9 {
		t.Fatalf("Reported() after oldest sample expired = %v, want 9", got)
	}
	if reason := poller.FellOpen(target, policy); reason != "" {
		t.Fatalf("FellOpen() with a fresh quorum = %q, want empty", reason)
	}
}

func TestCapacityPollerReconcileKeepsServicesIndependent(t *testing.T) {
	var firstRequests, secondRequests atomic.Int32
	firstServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		firstRequests.Add(1)
		_, _ = w.Write([]byte(freshReport(8)))
	}))
	defer firstServer.Close()
	secondServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondRequests.Add(1)
		_, _ = w.Write([]byte(freshReport(1)))
	}))
	defer secondServer.Close()

	clk := clocktesting.NewFakeClock(capacityTestNow)
	executor, _, _ := startTestObserverExecutor(t, 2, 2)
	poller := newTestPollerWithRuntime(t, executor, NewObserverClient(), clk, testCapacityMaxResponseBytes)
	firstPolicy := testCapacityPolicy(t, singleSampleConfig())
	secondConfig := testCapacityConfig()
	secondConfig.Period = 3 * time.Second
	secondConfig.Samples = 2
	secondConfig.Quorum = 2
	secondPolicy := testCapacityPolicy(t, secondConfig)
	firstTarget := testCapacityTarget("cluster-a", firstServer.URL)
	secondTarget := Target{
		OwnerUID: "second-owner",
		Map:      types.NamespacedName{Namespace: "team-b", Name: "secondary-service"},
		Cluster:  "cluster-b",
		URL:      secondServer.URL,
	}
	firstRequest := testCapacityRequest(firstPolicy, firstTarget)
	secondRequest := CapacityReconcileRequest{
		OwnerUID: secondTarget.OwnerUID,
		Map:      secondTarget.Map,
		Policy:   secondPolicy,
		Targets:  []Target{secondTarget},
	}

	if _, err := poller.Reconcile(context.Background(), firstRequest); err != nil {
		t.Fatalf("first service Reconcile() error = %v", err)
	}
	if _, err := poller.Reconcile(context.Background(), secondRequest); err != nil {
		t.Fatalf("second service Reconcile() error = %v", err)
	}
	if got := poller.Reported(firstTarget, firstPolicy); got == nil || *got != 8 {
		t.Fatalf("first service Reported() = %v, want 8", got)
	}
	if got := poller.Reported(secondTarget, secondPolicy); got != nil {
		t.Fatalf("second service Reported() = %d before quorum, want nil", *got)
	}
	if reason := poller.FellOpen(secondTarget, secondPolicy); !strings.Contains(reason, "awaiting quorum (1/2") {
		t.Fatalf("second service FellOpen() = %q, want pending quorum", reason)
	}

	clk.Step(time.Second)
	if _, err := poller.Reconcile(context.Background(), firstRequest); err != nil {
		t.Fatalf("due first service Reconcile() error = %v", err)
	}
	delay, err := poller.Reconcile(context.Background(), secondRequest)
	if err != nil {
		t.Fatalf("early second service Reconcile() error = %v", err)
	}
	if firstRequests.Load() != 2 || secondRequests.Load() != 1 || delay != 2*time.Second {
		t.Fatalf("independent requests/delay = %d/%d/%s, want 2/1/%s",
			firstRequests.Load(), secondRequests.Load(), delay, 2*time.Second)
	}

	clk.Step(2 * time.Second)
	if _, err := poller.Reconcile(context.Background(), secondRequest); err != nil {
		t.Fatalf("due second service Reconcile() error = %v", err)
	}
	if got := poller.Reported(secondTarget, secondPolicy); got == nil || *got != 1 {
		t.Fatalf("second service Reported() after quorum = %v, want 1", got)
	}
	if firstRequests.Load() != 2 || secondRequests.Load() != 2 {
		t.Fatalf("independent final requests = %d/%d, want 2/2", firstRequests.Load(), secondRequests.Load())
	}
}

func TestCapacityPollerReconcilePolicyChangeResetsState(t *testing.T) {
	var servable atomic.Int32
	servable.Store(2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(freshReport(servable.Load())))
	}))
	defer server.Close()

	clk := clocktesting.NewFakeClock(capacityTestNow)
	executor, _, _ := startTestObserverExecutor(t, 1, 1)
	poller := newTestPollerWithRuntime(t, executor, NewObserverClient(), clk, testCapacityMaxResponseBytes)
	oldPolicy := testCapacityPolicy(t, testCapacityConfig())
	target := testCapacityTarget("cluster-a", server.URL)
	request := testCapacityRequest(oldPolicy, target)
	if _, err := poller.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("initial Reconcile() error = %v", err)
	}
	if got := poller.Reported(target, oldPolicy); got != nil {
		t.Fatalf("first old-policy Reported() = %d before quorum, want nil", *got)
	}
	clk.Step(oldPolicy.Capacity.Period)
	if _, err := poller.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("second old-policy Reconcile() error = %v", err)
	}
	if got := poller.Reported(target, oldPolicy); got == nil || *got != 2 {
		t.Fatalf("initial Reported() = %v, want 2", got)
	}

	changedConfig := oldPolicy.Capacity
	changedConfig.Period *= 2
	changedPolicy := testCapacityPolicy(t, changedConfig)
	if changedPolicy.PolicyDigest == oldPolicy.PolicyDigest {
		t.Fatal("capacity policy digest did not change with the effective period")
	}
	servable.Store(9)
	request.Policy = changedPolicy
	if _, err := poller.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("changed-policy Reconcile() error = %v", err)
	}
	if got := poller.Reported(target, oldPolicy); got != nil {
		t.Fatalf("old-policy Reported() = %d after reset, want nil", *got)
	}
	if got := poller.Reported(target, changedPolicy); got != nil {
		t.Fatalf("changed-policy Reported() = %d before new quorum, want nil", *got)
	}
	if reason := poller.FellOpen(target, changedPolicy); !strings.Contains(reason, "awaiting quorum (1/2") {
		t.Fatalf("changed-policy FellOpen() = %q, want pending new-policy quorum", reason)
	}
	clk.Step(changedPolicy.Capacity.Period)
	if _, err := poller.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("second changed-policy Reconcile() error = %v", err)
	}
	if got := poller.Reported(target, changedPolicy); got == nil || *got != 9 {
		t.Fatalf("changed-policy Reported() after new quorum = %v, want 9", got)
	}
}

func TestCapacityPollerReconcilePrunesOnlyRequestedMap(t *testing.T) {
	policy := testCapacityPolicy(t, singleSampleConfig())
	first := testCapacityTarget("cluster-a", "https://cluster-a.example")
	second := Target{
		OwnerUID: "second-owner",
		Map:      types.NamespacedName{Namespace: "team-b", Name: "secondary-service"},
		Cluster:  "cluster-b",
		URL:      "https://cluster-b.example",
	}

	for _, tt := range []struct {
		name   string
		policy ResolvedCapacityPolicy
	}{
		{name: "empty target set", policy: policy},
		{name: "disabled policy", policy: ResolvedCapacityPolicy{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			poller := newTestPoller(t)
			one, two := int32(1), int32(2)
			poller.record(first, policy, &one, "")
			poller.record(second, policy, &two, "")

			request := testCapacityRequest(tt.policy)
			if delay, err := poller.Reconcile(context.Background(), request); err != nil || delay != 0 {
				t.Fatalf("Reconcile() delay/error = %s/%v, want 0/nil", delay, err)
			}
			if got := poller.Reported(first, policy); got != nil {
				t.Fatalf("requested-map Reported() = %d after pruning, want nil", *got)
			}
			if got := poller.Reported(second, policy); got == nil || *got != 2 {
				t.Fatalf("independent-map Reported() = %v after pruning, want 2", got)
			}
		})
	}
}

func TestCapacityPollerReconcileRejectsAmbiguousRequests(t *testing.T) {
	policy := testCapacityPolicy(t, singleSampleConfig())
	validTarget := testCapacityTarget("cluster-a", "https://cluster-a.example")
	poller := newTestPoller(t)

	if _, err := (*CapacityPoller)(nil).Reconcile(context.Background(), testCapacityRequest(policy, validTarget)); err == nil {
		t.Fatal("nil poller Reconcile() error = nil")
	}
	//nolint:staticcheck // verifies that the public boundary rejects a nil context
	if _, err := poller.Reconcile(nil, testCapacityRequest(policy, validTarget)); err == nil {
		t.Fatal("nil-context Reconcile() error = nil")
	}

	tests := []struct {
		name   string
		mutate func(*CapacityReconcileRequest)
	}{
		{name: "empty request owner UID", mutate: func(request *CapacityReconcileRequest) { request.OwnerUID = "" }},
		{name: "empty request map name", mutate: func(request *CapacityReconcileRequest) { request.Map.Name = "" }},
		{name: "empty request map namespace", mutate: func(request *CapacityReconcileRequest) { request.Map.Namespace = "" }},
		{name: "invalid policy", mutate: func(request *CapacityReconcileRequest) {
			request.Policy.Capacity.Timeout = request.Policy.Capacity.Period
		}},
		{name: "mismatched policy digest", mutate: func(request *CapacityReconcileRequest) {
			request.Policy.PolicyDigest = "sha256:" + strings.Repeat("0", 64)
		}},
		{name: "empty target owner UID", mutate: func(request *CapacityReconcileRequest) { request.Targets[0].OwnerUID = "" }},
		{name: "mismatched target owner UID", mutate: func(request *CapacityReconcileRequest) { request.Targets[0].OwnerUID = "other" }},
		{name: "empty target map", mutate: func(request *CapacityReconcileRequest) {
			request.Targets[0].Map = types.NamespacedName{}
		}},
		{name: "mismatched target map", mutate: func(request *CapacityReconcileRequest) { request.Targets[0].Map.Name = "other" }},
		{name: "empty target cluster", mutate: func(request *CapacityReconcileRequest) { request.Targets[0].Cluster = "" }},
		{name: "duplicate target cluster", mutate: func(request *CapacityReconcileRequest) {
			request.Targets = append(request.Targets, request.Targets[0])
		}},
		{name: "invalid target URL", mutate: func(request *CapacityReconcileRequest) { request.Targets[0].URL = "/relative" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := testCapacityRequest(policy, validTarget)
			tt.mutate(&request)
			if _, err := poller.Reconcile(context.Background(), request); err == nil {
				t.Fatal("Reconcile() error = nil")
			}
		})
	}
}

func TestCapacityPollerReconcileFailedPollFallsOpen(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusOK)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(int(status.Load()))
		_, _ = w.Write([]byte(freshReport(3)))
	}))
	defer server.Close()

	config := singleSampleConfig()
	policy := testCapacityPolicy(t, config)
	clk := clocktesting.NewFakeClock(capacityTestNow)
	executor, _, _ := startTestObserverExecutor(t, 1, 1)
	poller := newTestPollerWithRuntime(t, executor, NewObserverClient(), clk, testCapacityMaxResponseBytes)
	target := testCapacityTarget("cluster-a", server.URL)
	request := testCapacityRequest(policy, target)
	if _, err := poller.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("successful Reconcile() error = %v", err)
	}
	if got := poller.Reported(target, policy); got == nil || *got != 3 {
		t.Fatalf("successful Reported() = %v, want 3", got)
	}

	status.Store(http.StatusServiceUnavailable)
	clk.Step(config.Period)
	if _, err := poller.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("failed-poll Reconcile() error = %v", err)
	}
	if got := poller.Reported(target, policy); got != nil {
		t.Fatalf("Reported() after failed poll = %d, want nil", *got)
	}
	if reason := poller.FellOpen(target, policy); !strings.Contains(reason, "HTTP 503") {
		t.Fatalf("FellOpen() = %q, want HTTP failure", reason)
	}
}

func TestCapacityPollerReconcileMalformedCapacityPathFallsOpenOnCadence(t *testing.T) {
	config := singleSampleConfig()
	config.Path = "/%zz"
	policy := testCapacityPolicy(t, config)
	poller := newTestPoller(t)
	target := testCapacityTarget("cluster-a", "https://cluster-a.example")

	delay, err := poller.Reconcile(
		context.Background(), testCapacityRequest(policy, target),
	)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if delay != config.Period {
		t.Fatalf("Reconcile() delay = %s, want %s", delay, config.Period)
	}
	if got := poller.Reported(target, policy); got != nil {
		t.Fatalf("Reported() for malformed path = %d, want nil", *got)
	}
	if reason := poller.FellOpen(target, policy); !strings.Contains(reason, "request URL is invalid") {
		t.Fatalf("FellOpen() = %q, want invalid request URL", reason)
	}
}

func TestCapacityPollerReconcileSubmitsThroughExecutor(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(freshReport(3)))
	}))
	defer server.Close()

	executor, err := NewObserverExecutor(1, 1)
	if err != nil {
		t.Fatalf("NewObserverExecutor() error = %v", err)
	}
	poller := newTestPollerWithRuntime(
		t,
		executor,
		NewObserverClient(),
		clocktesting.NewFakeClock(capacityTestNow),
		testCapacityMaxResponseBytes,
	)
	policy := testCapacityPolicy(t, singleSampleConfig())
	target := testCapacityTarget("cluster-a", server.URL)
	delay, err := poller.Reconcile(context.Background(), testCapacityRequest(policy, target))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if delay != policy.Capacity.Period {
		t.Fatalf("Reconcile() delay = %s, want %s", delay, policy.Capacity.Period)
	}
	if requests.Load() != 0 {
		t.Fatalf("HTTP requests without a running executor = %d, want 0", requests.Load())
	}
	if got := poller.Reported(target, policy); got != nil {
		t.Fatalf("Reported() without a running executor = %d, want nil", *got)
	}
	if reason := poller.FellOpen(target, policy); !strings.Contains(reason, ErrObserverExecutorNotRunning.Error()) {
		t.Fatalf("FellOpen() = %q, want executor submission failure", reason)
	}
}

func TestCapacityConfig_ValidateFormats(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*CapacityConfig)
		wantErr bool
	}{
		{"format is required", func(c *CapacityConfig) { c.Format = "" }, true},
		{"explicit Report is valid", func(c *CapacityConfig) { c.Format = FormatReport }, false},
		// A format this build does not carry is a startup error, not a silent
		// fallback: ignoring it would drop the operator's intent and leave the
		// plan in force with no explanation.
		{"unregistered format rejected", func(c *CapacityConfig) { c.Format = "Bogus" }, true},
		// Report needs no format-specific settings, so an option here is either
		// a typo or a misunderstanding; either way it must not be ignored.
		{"options rejected for Report", func(c *CapacityConfig) {
			c.Options = map[string]string{"example": "1"}
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testCapacityConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("Validate() = nil, want error for %+v", cfg)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

func TestCapacityFormatRegistry_DefaultBuildCarriesOnlyTheBuiltIn(t *testing.T) {
	// A build that registers no additional formats carries exactly the
	// built-in one. Any other name here means an out-of-tree format registered
	// itself in this test binary.
	got := RegisteredCapacityFormats()
	if len(got) != 1 || got[0] != string(FormatReport) {
		t.Fatalf("default build registered %v, want only %q", got, FormatReport)
	}
}

func TestRegisterCapacityFormat_RejectsDuplicatesAndIncompletePlugins(t *testing.T) {
	// Two packages silently competing for one name would make the active
	// decoder depend on import order.
	if err := RegisterCapacityFormat(FormatReport, CapacityFormatPlugin{Decode: decodeReport}); err == nil {
		t.Fatal("re-registering an existing format succeeded, want an error")
	}
	if err := RegisterCapacityFormat("NoDecoder", CapacityFormatPlugin{}); err == nil {
		t.Fatal("registering a plugin with no Decode succeeded, want an error")
	}
	if err := RegisterCapacityFormat("", CapacityFormatPlugin{Decode: decodeReport}); err == nil {
		t.Fatal("registering an empty name succeeded, want an error")
	}
}

// feed pushes readings through the poller and returns the applied ceiling, or
// nil when the plan still stands.
func feed(t *testing.T, cfg CapacityConfig, readings ...int32) *int32 {
	t.Helper()
	// This helper exercises aggregation directly; scheduling is covered by the
	// reconcile tests above.
	p := &CapacityPoller{
		Clock: clocktesting.NewFakeClock(capacityTestNow),
		state: map[capacityKey]*capacityState{},
	}
	tgt := testCapacityTarget("cluster-a", "https://cluster-a.example")
	policy := testCapacityPolicy(t, cfg)
	for _, r := range readings {
		v := r
		p.record(tgt, policy, &v, "")
	}
	return p.Reported(tgt, policy)
}

func TestCapacityPoller_QuorumToleratesAMisbehavingReporter(t *testing.T) {
	// The control plane polls one address per home, so consecutive readings may
	// come from different reporters behind a balancer. A strict minimum would
	// let whichever reporter is broken set the ceiling alone -- and with a
	// round-robin balancer and a window at least as long as the replica count,
	// hold it there permanently. The quorum is what makes a low reading need
	// corroboration.
	cfg := testCapacityConfig() // Samples 10, Quorum 2.
	tests := []struct {
		name     string
		readings []int32
		want     *int32
	}{
		{
			// One reporter returning zero while the rest are healthy must not
			// set the ceiling.
			name:     "a single zero among healthy readings is ignored",
			readings: []int32{0, 7, 7, 7, 7},
			want:     ptr(int32(7)),
		},
		{
			// Filtering only zeros would miss this: a partially-synced reporter
			// returns a small non-zero figure, not nothing.
			name:     "a single low non-zero reading is ignored",
			readings: []int32{2, 7, 7, 7, 7},
			want:     ptr(int32(7)),
		},
		{
			// Corroborated, so believed -- the pessimist knows something.
			name:     "two agreeing low readings are believed",
			readings: []int32{3, 3, 7, 7, 7},
			want:     ptr(int32(3)),
		},
		{
			// One broken reporter plus one genuine pessimist: drop the outlier,
			// keep the pessimist.
			name:     "a broken reporter does not mask a real pessimist",
			readings: []int32{0, 3, 7, 7, 7},
			want:     ptr(int32(3)),
		},
		{
			// A home that really can serve nothing must still be able to say so.
			name:     "a unanimous zero is believed",
			readings: []int32{0, 0, 0, 0},
			want:     ptr(int32(0)),
		},
		{
			// Believing a drop takes exactly Quorum readings -- stated, so the
			// lag is predictable rather than emergent.
			name:     "a sustained drop is applied once corroborated",
			readings: []int32{9, 9, 4, 4},
			want:     ptr(int32(4)),
		},
		{
			// Anti-any-responder: a lucky high reading must not undo a ceiling
			// that corroborated readings already lowered.
			name:     "one high reading cannot raise a corroborated ceiling",
			readings: []int32{4, 4, 9},
			want:     ptr(int32(4)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := feed(t, cfg, tt.readings...)
			if got == nil || *got != *tt.want {
				g := "nil"
				if got != nil {
					g = fmt.Sprint(*got)
				}
				t.Fatalf("readings %v -> ceiling %s, want %d", tt.readings, g, *tt.want)
			}
		})
	}
}

func TestCapacityPoller_NoCeilingUntilCorroborated(t *testing.T) {
	// Before the quorum is reached the plan stands. A ceiling derived from
	// fewer readings than the quorum is exactly what the quorum prevents: the
	// first reading after startup could be the broken reporter's.
	cfg := testCapacityConfig()
	if got := feed(t, cfg, 0); got != nil {
		t.Fatalf("a lone reading set a ceiling of %d; the plan should still stand", *got)
	}
	if got := feed(t, cfg, 3); got != nil {
		t.Fatalf("a lone reading set a ceiling of %d; the plan should still stand", *got)
	}
}

func TestCapacityPoller_WindowAgesOutAStaleLow(t *testing.T) {
	// Bounded so a home recovers. An all-time minimum would pin it at its worst
	// reading forever.
	cfg := testCapacityConfig()
	cfg.Samples, cfg.Quorum = 3, 2
	if got := feed(t, cfg, 1, 1, 9, 9, 9); got == nil || *got != 9 {
		t.Fatalf("ceiling = %v after the low readings aged out, want 9", got)
	}
}

func TestCapacityPoller_FailedPollReleasesTheWindow(t *testing.T) {
	// Fail-open means the ceiling is released, so the window cannot survive --
	// retaining readings would keep constraining a home we can no longer see.
	cfg := testCapacityConfig()
	cfg.Samples, cfg.Quorum = 4, 2
	p := newTestPoller(t)
	tgt := testCapacityTarget("cluster-a", "https://cluster-a.example")
	policy := testCapacityPolicy(t, cfg)
	for _, v := range []int32{2, 2} {
		x := v
		p.record(tgt, policy, &x, "")
	}
	if got := p.Reported(tgt, policy); got == nil || *got != 2 {
		t.Fatalf("ceiling = %v before the failure, want 2", got)
	}
	p.record(tgt, policy, nil, "capacity endpoint unreachable")
	if got := p.Reported(tgt, policy); got != nil {
		t.Fatalf("ceiling = %d after a failed poll, want nil (fail open)", *got)
	}
	// And the old low readings must not come back with the next success.
	nine := int32(9)
	p.record(tgt, policy, &nine, "")
	if got := p.Reported(tgt, policy); got != nil {
		t.Fatalf("ceiling = %d from one reading after recovery, want nil until corroborated", *got)
	}
	p.record(tgt, policy, &nine, "")
	if got := p.Reported(tgt, policy); got == nil || *got != 9 {
		t.Fatalf("ceiling = %v after recovery, want 9 (stale lows must not persist)", got)
	}
}

func TestCapacityConfig_QuorumAndSamplesValidation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*CapacityConfig)
		wantErr bool
	}{
		{"configured values are valid", func(*CapacityConfig) {}, false},
		{"explicit values are valid", func(c *CapacityConfig) { c.Samples, c.Quorum = 5, 3 }, false},
		{"negative samples rejected", func(c *CapacityConfig) { c.Samples = -1 }, true},
		{"negative quorum rejected", func(c *CapacityConfig) { c.Quorum = -1 }, true},
		// Unreachable quorum means no reading could ever lower a ceiling, so the
		// input would silently do nothing.
		{"quorum above samples rejected", func(c *CapacityConfig) { c.Samples, c.Quorum = 3, 4 }, true},
		{"quorum equal to samples is valid", func(c *CapacityConfig) { c.Samples, c.Quorum = 3, 3 }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testCapacityConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("Validate() = nil, want error for samples=%d quorum=%d", cfg.Samples, cfg.Quorum)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}
