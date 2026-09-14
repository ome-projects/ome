package routing

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// testProbeConfig is a fully-specified probe config. Every field is required
// once Path is set, so tests state them all rather than relying on defaults
// that deliberately do not exist.
func testProbeConfig() ProbeConfig {
	return ProbeConfig{
		Path:             "/healthz/deep",
		Method:           http.MethodGet,
		AcceptStatuses:   []int{200},
		GateStatuses:     []int{500, 502, 503},
		Period:           time.Second,
		Timeout:          500 * time.Millisecond,
		FailureThreshold: 3,
		SuccessThreshold: 2,
	}
}

func newTestProber(t *testing.T, cfg ProbeConfig) *Prober {
	t.Helper()
	p := NewProber(cfg, NewObserverClient(), logr.Discard())
	return p
}

var testMap = types.NamespacedName{Name: "llama", Namespace: "team-a"}

func TestProbeConfig_ClassifyStatus(t *testing.T) {
	cfg := testProbeConfig()
	tests := []struct {
		name string
		code int
		want Verdict
	}{
		{"accepted status passes", 200, VerdictPass},
		{"listed 5xx gates", 503, VerdictFail},
		// The load-bearing cases: neither of these is the home being unhealthy.
		{"429 is alive and overloaded, never gates", 429, VerdictInconclusive},
		{"401 is the prober's credentials, never gates", 401, VerdictInconclusive},
		{"403 is the prober's credentials, never gates", 403, VerdictInconclusive},
		{"unlisted status is inconclusive", 302, VerdictInconclusive},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cfg.ClassifyStatus(tt.code); got != tt.want {
				t.Fatalf("ClassifyStatus(%d) = %v, want %v", tt.code, got, tt.want)
			}
		})
	}
}

func TestProber_GateFlipsExactlyAtTheThreshold(t *testing.T) {
	p := newTestProber(t, testProbeConfig())
	tgt := Target{Map: testMap, Cluster: "cluster-a"}

	// Below the threshold the home keeps its full share: a single dropped packet
	// must not move a large traffic share.
	for i := 1; i < 3; i++ {
		if flipped := p.record(tgt, VerdictFail, "HTTP 503"); flipped {
			t.Fatalf("gate flipped after %d failures, want flip only at 3", i)
		}
		if r := p.Reachable(testMap, "cluster-a"); r == nil || !*r {
			t.Fatalf("after %d failures Reachable = %v, want still reachable", i, r)
		}
	}

	if flipped := p.record(tgt, VerdictFail, "HTTP 503"); !flipped {
		t.Fatal("gate did not flip on the 3rd consecutive failure")
	}
	if r := p.Reachable(testMap, "cluster-a"); r == nil || *r {
		t.Fatalf("after 3 failures Reachable = %v, want gated", r)
	}

	// Recovery needs SuccessThreshold passes, not one.
	if flipped := p.record(tgt, VerdictPass, "HTTP 200"); flipped {
		t.Fatal("gate reopened after a single pass, want 2")
	}
	if flipped := p.record(tgt, VerdictPass, "HTTP 200"); !flipped {
		t.Fatal("gate did not reopen on the 2nd consecutive pass")
	}
	if r := p.Reachable(testMap, "cluster-a"); r == nil || !*r {
		t.Fatalf("after recovery Reachable = %v, want reachable", r)
	}
}

func TestProber_InterruptedFailureRunDoesNotGate(t *testing.T) {
	// Hysteresis has to count *consecutive* failures. A flapping home that never
	// fails 3 times in a row is not gated, which is the whole point of the
	// threshold.
	p := newTestProber(t, testProbeConfig())
	tgt := Target{Map: testMap, Cluster: "cluster-a"}

	for range 5 {
		p.record(tgt, VerdictFail, "HTTP 503")
		p.record(tgt, VerdictFail, "HTTP 503")
		if flipped := p.record(tgt, VerdictPass, "HTTP 200"); flipped {
			t.Fatal("gate flipped during a flapping run that never reached 3 failures")
		}
	}
	if r := p.Reachable(testMap, "cluster-a"); r == nil || !*r {
		t.Fatalf("flapping home Reachable = %v, want reachable", r)
	}
}

func TestProber_InconclusiveNeverMovesTheGate(t *testing.T) {
	// The prober is a correlated component: a misconfiguration or blocked egress
	// affects every home at once. An inconclusive verdict must therefore move
	// neither counter, or a broken prober would gate the entire fleet.
	p := newTestProber(t, testProbeConfig())
	tgt := Target{Map: testMap, Cluster: "cluster-a"}

	for range 20 {
		if flipped := p.record(tgt, VerdictInconclusive, "probe could not run"); flipped {
			t.Fatal("inconclusive verdict flipped the gate")
		}
	}
	if r := p.Reachable(testMap, "cluster-a"); r != nil {
		t.Fatalf("Reachable after only-inconclusive probes = %v, want nil (no verdict)", *r)
	}

	// It must also not erode an existing failure run toward a flip.
	p.record(tgt, VerdictFail, "HTTP 503")
	p.record(tgt, VerdictFail, "HTTP 503")
	p.record(tgt, VerdictInconclusive, "HTTP 429")
	if r := p.Reachable(testMap, "cluster-a"); r == nil || !*r {
		t.Fatalf("Reachable = %v, want still reachable at 2 failures", r)
	}
	if flipped := p.record(tgt, VerdictFail, "HTTP 503"); !flipped {
		t.Fatal("inconclusive probe reset the failure count; 3rd failure should have gated")
	}
}

func TestProber_ReachableIsNilWhenProbingIsOff(t *testing.T) {
	// The default path: no config, no verdict, no gating — and a nil *Prober is
	// safe, since the controller holds one unconditionally.
	off := newTestProber(t, ProbeConfig{})
	if r := off.Reachable(testMap, "cluster-a"); r != nil {
		t.Fatalf("Reachable with probing off = %v, want nil", *r)
	}
	if pr := off.Provenance(testMap, "cluster-a"); pr != nil {
		t.Fatalf("Provenance with probing off = %+v, want nil", pr)
	}
	var nilProber *Prober
	if r := nilProber.Reachable(testMap, "cluster-a"); r != nil {
		t.Fatalf("Reachable on nil prober = %v, want nil", *r)
	}
	if pr := nilProber.Provenance(testMap, "cluster-a"); pr != nil {
		t.Fatalf("Provenance on nil prober = %+v, want nil", pr)
	}
}

func TestProber_ProvenanceReportsWhatWasObserved(t *testing.T) {
	p := newTestProber(t, testProbeConfig())
	tgt := Target{Map: testMap, Cluster: "cluster-a"}
	p.record(tgt, VerdictFail, "HTTP 502")
	p.record(tgt, VerdictFail, "HTTP 502")

	pr := p.Provenance(testMap, "cluster-a")
	if pr == nil {
		t.Fatal("Provenance = nil, want observed state")
	}
	if pr.Result != v1beta1.ProbeResultFailing {
		t.Fatalf("Result = %q, want %q", pr.Result, v1beta1.ProbeResultFailing)
	}
	// The count is what an operator checks when asking "why is this home still
	// weighted while probes fail?", so it must be visible below the threshold.
	if pr.ConsecutiveFailures != 2 {
		t.Fatalf("ConsecutiveFailures = %d, want 2", pr.ConsecutiveFailures)
	}
	if pr.Message != "HTTP 502" {
		t.Fatalf("Message = %q, want %q", pr.Message, "HTTP 502")
	}
	if pr.LastProbeTime == nil {
		t.Fatal("LastProbeTime = nil; a stalled prober must be distinguishable from a steady pass")
	}
}

func TestProber_ProbeOneClassifiesRealResponses(t *testing.T) {
	var code atomic.Int32
	code.Store(200)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz/deep" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(int(code.Load()))
	}))
	defer srv.Close()

	p := newTestProber(t, testProbeConfig())
	tgt := Target{Map: testMap, Cluster: "cluster-a", URL: srv.URL}

	for _, tc := range []struct {
		status int
		want   Verdict
	}{
		{200, VerdictPass},
		{503, VerdictFail},
		{429, VerdictInconclusive},
	} {
		code.Store(int32(tc.status))
		if got, _ := p.probeOne(context.Background(), tgt); got != tc.want {
			t.Fatalf("probeOne against HTTP %d = %v, want %v", tc.status, got, tc.want)
		}
	}
}

func TestProber_UnreachableEndpointIsAFailureNotAnError(t *testing.T) {
	// A transport error means the path did not carry a request a client would
	// have sent — that is the home's failure, and it is exactly the
	// unreachable-but-ready case the probe exists to catch.
	p := newTestProber(t, testProbeConfig())
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // now refusing connections

	got, msg := p.probeOne(context.Background(), Target{Map: testMap, Cluster: "cluster-a", URL: url})
	if got != VerdictFail {
		t.Fatalf("probeOne against a closed endpoint = %v (%s), want VerdictFail", got, msg)
	}
}

func TestProber_MalformedURLCannotGate(t *testing.T) {
	// A request we cannot even construct is our defect, not the home's. Gating
	// on it would let one bad endpoint string — or a prober bug — take traffic
	// away from a healthy home.
	p := newTestProber(t, testProbeConfig())
	got, msg := p.probeOne(context.Background(), Target{Map: testMap, Cluster: "cluster-a", URL: "://not a url"})
	if got != VerdictInconclusive {
		t.Fatalf("probeOne against a malformed URL = %v (%s), want VerdictInconclusive", got, msg)
	}
}

func TestProber_ProbeAllNotifiesOnlyOnGateFlips(t *testing.T) {
	// Counts move on every probe, but weights only change when the gate does, so
	// waking the controller on anything less would be pure write amplification.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	cfg := testProbeConfig()
	cfg.FailureThreshold = 2
	p := newTestProber(t, cfg)
	p.Targets = func(context.Context) ([]Target, error) {
		return []Target{{Map: testMap, Cluster: "cluster-a", URL: srv.URL}}, nil
	}
	var mu sync.Mutex
	var notified []types.NamespacedName
	p.OnChange = func(nn types.NamespacedName) {
		mu.Lock()
		defer mu.Unlock()
		notified = append(notified, nn)
	}

	p.probeAll(context.Background()) // failure 1 of 2: no flip
	mu.Lock()
	if len(notified) != 0 {
		mu.Unlock()
		t.Fatalf("notified after 1 failure = %v, want none", notified)
	}
	mu.Unlock()

	p.probeAll(context.Background()) // failure 2 of 2: flip
	p.probeAll(context.Background()) // still failing, already gated: no new flip

	mu.Lock()
	defer mu.Unlock()
	if len(notified) != 1 || notified[0] != testMap {
		t.Fatalf("notified = %v, want exactly one %v", notified, testMap)
	}
}

func TestProber_ForgetsHomesThatAreNoLongerTargets(t *testing.T) {
	// A removed home must not leak state, and a home that returns must be
	// re-evaluated from scratch rather than inheriting a stale gate.
	cfg := testProbeConfig()
	cfg.FailureThreshold = 1
	p := newTestProber(t, cfg)
	tgt := Target{Map: testMap, Cluster: "cluster-a"}
	p.record(tgt, VerdictFail, "HTTP 503")
	if r := p.Reachable(testMap, "cluster-a"); r == nil || *r {
		t.Fatalf("Reachable = %v, want gated before forget", r)
	}

	p.forget(nil)
	if r := p.Reachable(testMap, "cluster-a"); r != nil {
		t.Fatalf("Reachable after forget = %v, want nil (no verdict)", *r)
	}
}

func TestTargetsFromTrafficMaps_SkipsEntriesWithoutAnEndpoint(t *testing.T) {
	mustURL := func(raw string) *apis.URL {
		u, err := apis.ParseURL(raw)
		if err != nil {
			t.Fatalf("ParseURL(%q): %v", raw, err)
		}
		return u
	}
	maps := []v1beta1.TrafficMap{{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "team-a"},
		Spec: v1beta1.TrafficMapSpec{Entries: []v1beta1.TrafficMapEntry{
			{Cluster: "cluster-b", Endpoint: mustURL("https://b.example.com")},
			{Cluster: "cluster-a", Endpoint: mustURL("https://a.example.com")},
			{Cluster: "cluster-c"}, // not addressable: nothing to probe
		}},
	}}
	got := TargetsFromTrafficMaps(maps)
	if len(got) != 2 {
		t.Fatalf("targets = %d (%v), want 2", len(got), got)
	}
	// Sorted, so probe order and logs are stable.
	if got[0].Cluster != "cluster-a" || got[1].Cluster != "cluster-b" {
		t.Fatalf("targets not sorted by cluster: %v", got)
	}
	if got[0].URL != "https://a.example.com" {
		t.Fatalf("target URL = %q, want %q", got[0].URL, "https://a.example.com")
	}
}
