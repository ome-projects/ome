package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
)

func testCapacityConfig() CapacityConfig {
	return CapacityConfig{
		Path:    "/capacity",
		Method:  http.MethodGet,
		Period:  time.Second,
		Timeout: 500 * time.Millisecond,
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

func newTestPoller(cfg CapacityConfig) *CapacityPoller {
	return NewCapacityPoller(cfg, NewObserverClient(), logr.Discard())
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
	b, _ := json.Marshal(CapacityReport{Servable: servable, ObservedAt: time.Now()})
	return string(b)
}

func TestCapacityPoller_AcceptsAFreshReport(t *testing.T) {
	srv := capacityServer(t, http.StatusOK, freshReport(3))
	p := newTestPoller(testCapacityConfig())

	got, reason := p.pollOne(context.Background(), Target{Map: testMap, Cluster: "cluster-a", URL: srv.URL})
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
	p := newTestPoller(testCapacityConfig())

	got, reason := p.pollOne(context.Background(), Target{Map: testMap, Cluster: "cluster-a", URL: srv.URL})
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
	stale, _ := json.Marshal(CapacityReport{Servable: 2, ObservedAt: time.Now().Add(-time.Hour)})
	negative, _ := json.Marshal(CapacityReport{Servable: -1, ObservedAt: time.Now()})
	noStamp, _ := json.Marshal(CapacityReport{Servable: 2})

	tests := []struct {
		name   string
		status int
		body   string
	}{
		{"non-200 response", http.StatusServiceUnavailable, freshReport(2)},
		{"unparsable body", http.StatusOK, "not json"},
		{"stale report beyond maxAge", http.StatusOK, string(stale)},
		{"negative servable count", http.StatusOK, string(negative)},
		{"report with no observedAt stamp", http.StatusOK, string(noStamp)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := capacityServer(t, tt.status, tt.body)
			p := newTestPoller(testCapacityConfig())
			got, reason := p.pollOne(context.Background(), Target{Map: testMap, Cluster: "cluster-a", URL: srv.URL})
			if got != nil {
				t.Fatalf("report = %d, want nil (fail open to the plan)", *got)
			}
			if reason == "" {
				t.Fatal("fell open with no reason; an operator cannot tell this from agreement with the plan")
			}
		})
	}
}

func TestCapacityPoller_UnreachableEndpointFallsOpen(t *testing.T) {
	srv := capacityServer(t, http.StatusOK, freshReport(2))
	url := srv.URL
	srv.Close()

	p := newTestPoller(testCapacityConfig())
	got, reason := p.pollOne(context.Background(), Target{Map: testMap, Cluster: "cluster-a", URL: url})
	if got != nil {
		t.Fatalf("report = %d from an unreachable endpoint, want nil", *got)
	}
	if reason == "" {
		t.Fatal("unreachable endpoint produced no reason")
	}
}

func TestCapacityPoller_ReportedIsNilWhenPollingIsOff(t *testing.T) {
	off := newTestPoller(CapacityConfig{})
	if r := off.Reported(testMap, "cluster-a"); r != nil {
		t.Fatalf("Reported with polling off = %d, want nil", *r)
	}
	if reason := off.FellOpen(testMap, "cluster-a"); reason != "" {
		t.Fatalf("FellOpen with polling off = %q, want empty", reason)
	}
	var nilPoller *CapacityPoller
	if r := nilPoller.Reported(testMap, "cluster-a"); r != nil {
		t.Fatalf("Reported on nil poller = %d, want nil", *r)
	}
}

func TestCapacityPoller_NotifiesOnlyWhenTheCeilingChanges(t *testing.T) {
	p := newTestPoller(singleSampleConfig())
	tgt := Target{Map: testMap, Cluster: "cluster-a"}
	two, three := int32(2), int32(3)

	if !p.record(tgt, &two, "") {
		t.Fatal("first report should count as a change")
	}
	if p.record(tgt, &two, "") {
		t.Fatal("an unchanged report triggered a reconcile; weights depend on the number, not the poll")
	}
	if !p.record(tgt, &three, "") {
		t.Fatal("a changed report should trigger a reconcile")
	}
	// A home that stops reporting releases its ceiling, which changes weights
	// and so must notify.
	if !p.record(tgt, nil, "capacity endpoint unreachable") {
		t.Fatal("losing a report should trigger a reconcile")
	}
	if p.record(tgt, nil, "capacity endpoint unreachable") {
		t.Fatal("still-absent report triggered a redundant reconcile")
	}
}

func TestCapacityPoller_ForgetsHomesThatAreNoLongerTargets(t *testing.T) {
	p := newTestPoller(singleSampleConfig())
	tgt := Target{Map: testMap, Cluster: "cluster-a"}
	two := int32(2)
	p.record(tgt, &two, "")
	if r := p.Reported(testMap, "cluster-a"); r == nil || *r != 2 {
		t.Fatalf("Reported = %v before forget, want 2", r)
	}
	p.forget(nil)
	if r := p.Reported(testMap, "cluster-a"); r != nil {
		t.Fatalf("Reported after forget = %d, want nil", *r)
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
		{"zero period rejected", func(c *CapacityConfig) { c.Period = 0 }, true},
		{"zero timeout rejected", func(c *CapacityConfig) { c.Timeout = 0 }, true},
		// Overlapping requests would pile up until the effective poll rate
		// silently stopped matching the configured one.
		{"timeout >= period rejected", func(c *CapacityConfig) { c.Timeout = c.Period }, true},
		// Without a bound the staleness guard can never fire.
		{"zero maxAge rejected", func(c *CapacityConfig) { c.MaxAge = 0 }, true},
		// Every report would age out before the next poll, flapping the ceiling.
		{"maxAge < period rejected", func(c *CapacityConfig) { c.MaxAge = c.Period / 2 }, true},
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
		{"missing accept statuses rejected", func(c *ProbeConfig) { c.AcceptStatuses = nil }, true},
		{"missing gate statuses rejected", func(c *ProbeConfig) { c.GateStatuses = nil }, true},
		// An ambiguous status has no defined verdict, so the policy would
		// depend on list order rather than on the operator's intent.
		{"status in both lists rejected", func(c *ProbeConfig) { c.GateStatuses = append(c.GateStatuses, 200) }, true},
		{"zero period rejected", func(c *ProbeConfig) { c.Period = 0 }, true},
		{"zero timeout rejected", func(c *ProbeConfig) { c.Timeout = 0 }, true},
		{"timeout >= period rejected", func(c *ProbeConfig) { c.Timeout = c.Period }, true},
		{"zero failure threshold rejected", func(c *ProbeConfig) { c.FailureThreshold = 0 }, true},
		{"zero success threshold rejected", func(c *ProbeConfig) { c.SuccessThreshold = 0 }, true},
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
	// control-plane memory.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		chunk := make([]byte, 4096)
		for i := range chunk {
			chunk[i] = 'a'
		}
		for range 64 {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	p := newTestPoller(testCapacityConfig())
	got, reason := p.pollOne(context.Background(), Target{Map: testMap, Cluster: "cluster-a", URL: srv.URL})
	if got != nil {
		t.Fatalf("oversized body produced a report %d, want nil", *got)
	}
	if reason == "" {
		t.Fatal("oversized body produced no reason")
	}
}

func TestCapacityPoller_PollAllRecordsPerHome(t *testing.T) {
	// Two homes with different answers must not contaminate each other.
	full := capacityServer(t, http.StatusOK, freshReport(8))
	scarce := capacityServer(t, http.StatusOK, freshReport(1))

	p := newTestPoller(singleSampleConfig())
	p.Targets = func(context.Context) ([]Target, error) {
		return []Target{
			{Map: testMap, Cluster: "cluster-a", URL: full.URL},
			{Map: testMap, Cluster: "cluster-b", URL: scarce.URL},
		}, nil
	}
	var notified []types.NamespacedName
	p.OnChange = func(nn types.NamespacedName) { notified = append(notified, nn) }

	p.pollAll(context.Background())

	if r := p.Reported(testMap, "cluster-a"); r == nil || *r != 8 {
		t.Fatalf("cluster-a reported = %v, want 8", r)
	}
	if r := p.Reported(testMap, "cluster-b"); r == nil || *r != 1 {
		t.Fatalf("cluster-b reported = %v, want 1", r)
	}
	// Both homes changed, but they share one map, so one reconcile covers them.
	if len(notified) != 1 {
		t.Fatalf("notified %d times (%v), want 1 for the shared map", len(notified), notified)
	}
}

func TestCapacityConfig_ValidateFormats(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*CapacityConfig)
		wantErr bool
	}{
		{"default format is Report", func(c *CapacityConfig) { c.Format = "" }, false},
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
func feed(cfg CapacityConfig, readings ...int32) *int32 {
	p := newTestPoller(cfg)
	tgt := Target{Map: testMap, Cluster: "cluster-a"}
	for _, r := range readings {
		v := r
		p.record(tgt, &v, "")
	}
	return p.Reported(testMap, "cluster-a")
}

func TestCapacityPoller_QuorumToleratesAMisbehavingReporter(t *testing.T) {
	// The control plane polls one address per home, so consecutive readings may
	// come from different reporters behind a balancer. A strict minimum would
	// let whichever reporter is broken set the ceiling alone -- and with a
	// round-robin balancer and a window at least as long as the replica count,
	// hold it there permanently. The quorum is what makes a low reading need
	// corroboration.
	cfg := testCapacityConfig() // Samples 10, Quorum 2 by default
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
			got := feed(cfg, tt.readings...)
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
	if got := feed(cfg, 0); got != nil {
		t.Fatalf("a lone reading set a ceiling of %d; the plan should still stand", *got)
	}
	if got := feed(cfg, 3); got != nil {
		t.Fatalf("a lone reading set a ceiling of %d; the plan should still stand", *got)
	}
}

func TestCapacityPoller_WindowAgesOutAStaleLow(t *testing.T) {
	// Bounded so a home recovers. An all-time minimum would pin it at its worst
	// reading forever.
	cfg := testCapacityConfig()
	cfg.Samples, cfg.Quorum = 3, 2
	if got := feed(cfg, 1, 1, 9, 9, 9); got == nil || *got != 9 {
		t.Fatalf("ceiling = %v after the low readings aged out, want 9", got)
	}
}

func TestCapacityPoller_FailedPollReleasesTheWindow(t *testing.T) {
	// Fail-open means the ceiling is released, so the window cannot survive --
	// retaining readings would keep constraining a home we can no longer see.
	cfg := testCapacityConfig()
	cfg.Samples, cfg.Quorum = 4, 2
	p := newTestPoller(cfg)
	tgt := Target{Map: testMap, Cluster: "cluster-a"}
	for _, v := range []int32{2, 2} {
		x := v
		p.record(tgt, &x, "")
	}
	if got := p.Reported(testMap, "cluster-a"); got == nil || *got != 2 {
		t.Fatalf("ceiling = %v before the failure, want 2", got)
	}
	p.record(tgt, nil, "capacity endpoint unreachable")
	if got := p.Reported(testMap, "cluster-a"); got != nil {
		t.Fatalf("ceiling = %d after a failed poll, want nil (fail open)", *got)
	}
	// And the old low readings must not come back with the next success.
	nine := int32(9)
	p.record(tgt, &nine, "")
	if got := p.Reported(testMap, "cluster-a"); got != nil {
		t.Fatalf("ceiling = %d from one reading after recovery, want nil until corroborated", *got)
	}
	p.record(tgt, &nine, "")
	if got := p.Reported(testMap, "cluster-a"); got == nil || *got != 9 {
		t.Fatalf("ceiling = %v after recovery, want 9 (stale lows must not persist)", got)
	}
}

func TestCapacityConfig_QuorumAndSamplesValidation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*CapacityConfig)
		wantErr bool
	}{
		{"unset uses package defaults", func(*CapacityConfig) {}, false},
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
