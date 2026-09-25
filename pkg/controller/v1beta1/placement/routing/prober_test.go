package routing

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const testProbeResponseBytes int64 = 64

var (
	testMap      = types.NamespacedName{Name: "model-service", Namespace: "team-a"}
	testOwnerUID = types.UID("isvc-uid")
	testProbeNow = time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
)

// testProbeConfig is fully specified because enabled probes have no implicit
// behavioral defaults.
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
		AllFailedPolicy:  AllFailedPolicyPreserveTraffic,
	}
}

func testProbePolicy(t *testing.T, config ProbeConfig) ResolvedProbePolicy {
	t.Helper()
	digest, err := probePolicyDigest(config)
	if err != nil {
		t.Fatalf("probePolicyDigest() error = %v", err)
	}
	return ResolvedProbePolicy{Enabled: true, Probe: config, PolicyDigest: digest}
}

func newTestProber(
	t *testing.T,
	client *http.Client,
	clk *clocktesting.FakeClock,
	workers int,
	queueCapacity int,
	maxResponseBytes int64,
) *Prober {
	t.Helper()
	executor, _, _ := startTestObserverExecutor(t, workers, queueCapacity)
	prober, err := NewProber(executor, client, clk, maxResponseBytes, logr.Discard())
	if err != nil {
		t.Fatalf("NewProber() error = %v", err)
	}
	return prober
}

func testProbeTarget(rawURL, digest string) Target {
	return Target{
		OwnerUID:     testOwnerUID,
		Map:          testMap,
		Cluster:      "cluster-a",
		URL:          rawURL,
		PolicyDigest: digest,
	}
}

func testProbeRequest(policy ResolvedProbePolicy, target Target) ProbeReconcileRequest {
	return ProbeReconcileRequest{
		OwnerUID: testOwnerUID,
		Map:      testMap,
		Policy:   policy,
		Targets:  []Target{target},
	}
}

func TestProbeConfig_ClassifyStatus(t *testing.T) {
	config := testProbeConfig()
	tests := []struct {
		name string
		code int
		want Verdict
	}{
		{name: "accepted status passes", code: 200, want: VerdictPass},
		{name: "listed 5xx gates", code: 503, want: VerdictFail},
		{name: "overload is inconclusive", code: 429, want: VerdictInconclusive},
		{name: "unauthorized is inconclusive", code: 401, want: VerdictInconclusive},
		{name: "forbidden is inconclusive", code: 403, want: VerdictInconclusive},
		{name: "unlisted status is inconclusive", code: 302, want: VerdictInconclusive},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := config.ClassifyStatus(tt.code); got != tt.want {
				t.Fatalf("ClassifyStatus(%d) = %v, want %v", tt.code, got, tt.want)
			}
		})
	}
}

func TestProbePolicyDigestGolden(t *testing.T) {
	policy := testProbePolicy(t, testProbeConfig())
	const want = "sha256:0727917987fccf0698041a81bab6747de022a8646a172535143fc83540b7b210"
	if policy.PolicyDigest != want {
		t.Fatalf("PolicyDigest = %q, want %q", policy.PolicyDigest, want)
	}
}

func TestNewProberRequiresDependenciesAndBound(t *testing.T) {
	executor, err := NewObserverExecutor(1, 1)
	if err != nil {
		t.Fatalf("NewObserverExecutor() error = %v", err)
	}
	client := NewObserverClient()
	clk := clocktesting.NewFakeClock(testProbeNow)

	tests := []struct {
		name     string
		executor *ObserverExecutor
		client   *http.Client
		clock    clock.Clock
		bound    int64
	}{
		{name: "nil executor", client: client, clock: clk, bound: 1},
		{name: "nil client", executor: executor, clock: clk, bound: 1},
		{name: "nil clock", executor: executor, client: client, bound: 1},
		{name: "zero response bound", executor: executor, client: client, clock: clk},
		{name: "negative response bound", executor: executor, client: client, clock: clk, bound: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewProber(tt.executor, tt.client, tt.clock, tt.bound, logr.Discard()); err == nil {
				t.Fatal("NewProber() error = nil")
			}
		})
	}
}

func TestCanonicalEndpoint(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "normalizes authority and root", raw: "HTTPS://SERVICE.EXAMPLE.COM/", want: "https://service.example.com"},
		{name: "cleans path", raw: "https://service.example.com/team/../model/", want: "https://service.example.com/model"},
		{name: "retains query", raw: "https://service.example.com/model/?token=a", want: "https://service.example.com/model?token=a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CanonicalEndpoint(tt.raw)
			if err != nil {
				t.Fatalf("CanonicalEndpoint() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("CanonicalEndpoint(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
	for _, raw := range []string{"", "/relative", "://broken", "ftp://service.example.com"} {
		if _, err := CanonicalEndpoint(raw); err == nil {
			t.Fatalf("CanonicalEndpoint(%q) error = nil", raw)
		}
	}
}

func TestProberReconcileUsesAbsoluteDeadlines(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	clk := clocktesting.NewFakeClock(testProbeNow)
	prober := newTestProber(t, NewObserverClient(), clk, 1, 1, testProbeResponseBytes)
	policy := testProbePolicy(t, testProbeConfig())
	target := testProbeTarget(server.URL, policy.PolicyDigest)
	request := testProbeRequest(policy, target)

	delay, err := prober.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("first Reconcile() error = %v", err)
	}
	if delay != policy.Probe.Period {
		t.Fatalf("first delay = %s, want %s", delay, policy.Probe.Period)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests after first reconcile = %d, want 1", got)
	}

	delay, err = prober.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("early Reconcile() error = %v", err)
	}
	if delay != policy.Probe.Period {
		t.Fatalf("early delay = %s, want %s", delay, policy.Probe.Period)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests before deadline = %d, want 1", got)
	}

	clk.Step(policy.Probe.Period - time.Millisecond)
	delay, err = prober.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("near-deadline Reconcile() error = %v", err)
	}
	if delay != time.Millisecond {
		t.Fatalf("near-deadline delay = %s, want %s", delay, time.Millisecond)
	}

	clk.Step(time.Millisecond)
	delay, err = prober.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("due Reconcile() error = %v", err)
	}
	if delay != policy.Probe.Period {
		t.Fatalf("due delay = %s, want %s", delay, policy.Probe.Period)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests after deadline = %d, want 2", got)
	}

	clk.Step(5 * policy.Probe.Period / 2)
	delay, err = prober.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("overdue Reconcile() error = %v", err)
	}
	if delay != policy.Probe.Period/2 {
		t.Fatalf("overdue delay = %s, want next absolute boundary in %s", delay, policy.Probe.Period/2)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("requests after overdue reconcile = %d, want 3", got)
	}
	provenance := prober.Provenance(target)
	if provenance == nil || provenance.PolicyDigest != policy.PolicyDigest ||
		provenance.LastProbeTime == nil || !provenance.LastProbeTime.Time.Equal(clk.Now()) {
		t.Fatalf("provenance after overdue reconcile = %+v", provenance)
	}
}

func TestProberReconcileMaintainsHysteresisAcrossUnknown(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusServiceUnavailable)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(int(status.Load()))
	}))
	defer server.Close()

	config := testProbeConfig()
	config.FailureThreshold = 2
	config.SuccessThreshold = 2
	clk := clocktesting.NewFakeClock(testProbeNow)
	prober := newTestProber(t, NewObserverClient(), clk, 1, 1, testProbeResponseBytes)
	policy := testProbePolicy(t, config)
	target := testProbeTarget(server.URL, policy.PolicyDigest)
	request := testProbeRequest(policy, target)

	reconcileAtNextPeriod := func() {
		t.Helper()
		if _, err := prober.Reconcile(context.Background(), request); err != nil {
			t.Fatalf("Reconcile() error = %v", err)
		}
		clk.Step(config.Period)
	}

	reconcileAtNextPeriod()
	if reachable := prober.Reachable(target); reachable == nil || !*reachable {
		t.Fatalf("Reachable() after one failure = %v, want true", reachable)
	}
	reconcileAtNextPeriod()
	if reachable := prober.Reachable(target); reachable == nil || *reachable {
		t.Fatalf("Reachable() at failure threshold = %v, want false", reachable)
	}

	status.Store(http.StatusTooManyRequests)
	reconcileAtNextPeriod()
	provenance := prober.Provenance(target)
	if provenance == nil || provenance.Result != v1beta1.ProbeResultUnknown || !provenance.Gated || provenance.ConsecutiveFailures != 2 {
		t.Fatalf("provenance after inconclusive result = %+v", provenance)
	}
	if reachable := prober.Reachable(target); reachable == nil || *reachable {
		t.Fatalf("Reachable() after inconclusive result = %v, want preserved false gate", reachable)
	}

	status.Store(http.StatusOK)
	reconcileAtNextPeriod()
	if reachable := prober.Reachable(target); reachable == nil || *reachable {
		t.Fatalf("Reachable() after first recovery pass = %v, want false", reachable)
	}
	status.Store(http.StatusTooManyRequests)
	reconcileAtNextPeriod()
	status.Store(http.StatusOK)
	reconcileAtNextPeriod()
	if reachable := prober.Reachable(target); reachable == nil || !*reachable {
		t.Fatalf("Reachable() after second recovery pass = %v, want true", reachable)
	}

	provenance = prober.Provenance(target)
	if provenance == nil || provenance.PolicyDigest != policy.PolicyDigest || provenance.Gated || provenance.Result != v1beta1.ProbeResultPassing {
		t.Fatalf("final provenance = %+v", provenance)
	}
}

func TestProberReconcileKeepsServicesIndependent(t *testing.T) {
	const (
		pathA        = "/models/a/ready"
		changedPathA = "/models/a/live"
		pathB        = "/models/b/health"
	)
	var (
		requestsMu sync.Mutex
		requests   = map[string]int{}
	)
	client := &http.Client{Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		requestsMu.Lock()
		requests[request.URL.Path]++
		requestsMu.Unlock()
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Body:       io.NopCloser(strings.NewReader("unavailable")),
			Header:     make(http.Header),
		}, nil
	})}
	requestCount := func(path string) int {
		t.Helper()
		requestsMu.Lock()
		defer requestsMu.Unlock()
		return requests[path]
	}

	clk := clocktesting.NewFakeClock(testProbeNow)
	prober := newTestProber(t, client, clk, 1, 1, testProbeResponseBytes)

	configA := testProbeConfig()
	configA.Path = "/ready"
	configA.Period = time.Second
	configA.FailureThreshold = 1
	policyA := testProbePolicy(t, configA)
	mapA := types.NamespacedName{Namespace: "team-a", Name: "service-a"}
	targetA := Target{
		OwnerUID:     types.UID("service-a-uid"),
		Map:          mapA,
		Cluster:      "cluster-a",
		URL:          "https://gateway.example.com/models/a",
		PolicyDigest: policyA.PolicyDigest,
	}
	requestA := ProbeReconcileRequest{
		OwnerUID: targetA.OwnerUID,
		Map:      mapA,
		Policy:   policyA,
		Targets:  []Target{targetA},
	}

	configB := testProbeConfig()
	configB.Path = "/health"
	configB.Period = 4 * time.Second
	configB.FailureThreshold = 3
	policyB := testProbePolicy(t, configB)
	mapB := types.NamespacedName{Namespace: "team-b", Name: "service-b"}
	targetB := Target{
		OwnerUID:     types.UID("service-b-uid"),
		Map:          mapB,
		Cluster:      "cluster-a",
		URL:          "https://gateway.example.com/models/b",
		PolicyDigest: policyB.PolicyDigest,
	}
	requestB := ProbeReconcileRequest{
		OwnerUID: targetB.OwnerUID,
		Map:      mapB,
		Policy:   policyB,
		Targets:  []Target{targetB},
	}

	if delay, err := prober.Reconcile(context.Background(), requestA); err != nil {
		t.Fatalf("initial service A Reconcile() error = %v", err)
	} else if delay != configA.Period {
		t.Fatalf("initial service A delay = %s, want %s", delay, configA.Period)
	}
	if reachable := prober.Reachable(targetA); reachable == nil || *reachable {
		t.Fatalf("service A Reachable() at its threshold = %v, want false", reachable)
	}
	if delay, err := prober.Reconcile(context.Background(), requestB); err != nil {
		t.Fatalf("initial service B Reconcile() error = %v", err)
	} else if delay != configB.Period {
		t.Fatalf("initial service B delay = %s, want %s", delay, configB.Period)
	}
	assertB := func(wantFailures int32, wantGated bool, wantProbeTime time.Time) {
		t.Helper()
		probe := prober.Provenance(targetB)
		if probe == nil || probe.PolicyDigest != policyB.PolicyDigest ||
			probe.ConsecutiveFailures != wantFailures || probe.Gated != wantGated ||
			probe.LastProbeTime == nil || !probe.LastProbeTime.Time.Equal(wantProbeTime) {
			t.Fatalf("service B provenance = %+v, want failures=%d gated=%t time=%s",
				probe, wantFailures, wantGated, wantProbeTime)
		}
	}
	assertB(1, false, testProbeNow)
	if got := requestCount(pathA); got != 1 {
		t.Fatalf("service A requests to %q = %d, want 1", pathA, got)
	}
	if got := requestCount(pathB); got != 1 {
		t.Fatalf("service B requests to %q = %d, want 1", pathB, got)
	}

	changedConfigA := configA
	changedConfigA.Path = "/live"
	changedConfigA.Period = 2 * time.Second
	changedConfigA.FailureThreshold = 2
	changedPolicyA := testProbePolicy(t, changedConfigA)
	changedTargetA := targetA
	changedTargetA.PolicyDigest = changedPolicyA.PolicyDigest
	changedRequestA := requestA
	changedRequestA.Policy = changedPolicyA
	changedRequestA.Targets = []Target{changedTargetA}
	if _, err := prober.Reconcile(context.Background(), changedRequestA); err != nil {
		t.Fatalf("changed service A Reconcile() error = %v", err)
	}
	if provenance := prober.Provenance(targetA); provenance != nil {
		t.Fatalf("old service A provenance after policy change = %+v, want nil", provenance)
	}
	if probe := prober.Provenance(changedTargetA); probe == nil || probe.ConsecutiveFailures != 1 || probe.Gated {
		t.Fatalf("changed service A provenance = %+v, want reset failure count", probe)
	}
	assertB(1, false, testProbeNow)
	if delay, err := prober.Reconcile(context.Background(), requestB); err != nil {
		t.Fatalf("service B Reconcile() after service A change error = %v", err)
	} else if delay != configB.Period {
		t.Fatalf("service B delay after service A change = %s, want %s", delay, configB.Period)
	}
	if got := requestCount(pathB); got != 1 {
		t.Fatalf("service B requests after service A change = %d, want 1", got)
	}

	prober.Forget(mapA)
	if provenance := prober.Provenance(changedTargetA); provenance != nil {
		t.Fatalf("service A provenance after Forget() = %+v, want nil", provenance)
	}
	assertB(1, false, testProbeNow)
	if delay, err := prober.Reconcile(context.Background(), requestB); err != nil {
		t.Fatalf("service B Reconcile() after service A Forget() error = %v", err)
	} else if delay != configB.Period {
		t.Fatalf("service B delay after service A Forget() = %s, want %s", delay, configB.Period)
	}

	if _, err := prober.Reconcile(context.Background(), changedRequestA); err != nil {
		t.Fatalf("reset service A Reconcile() error = %v", err)
	}
	if probe := prober.Provenance(changedTargetA); probe == nil || probe.ConsecutiveFailures != 1 || probe.Gated {
		t.Fatalf("reset service A provenance = %+v, want one fresh failure", probe)
	}
	assertB(1, false, testProbeNow)
	if got := requestCount(changedPathA); got != 2 {
		t.Fatalf("changed service A requests to %q = %d, want 2", changedPathA, got)
	}

	clk.Step(changedConfigA.Period)
	if _, err := prober.Reconcile(context.Background(), changedRequestA); err != nil {
		t.Fatalf("second changed service A Reconcile() error = %v", err)
	}
	if reachable := prober.Reachable(changedTargetA); reachable == nil || *reachable {
		t.Fatalf("changed service A Reachable() at its threshold = %v, want false", reachable)
	}
	assertB(1, false, testProbeNow)
	if delay, err := prober.Reconcile(context.Background(), requestB); err != nil {
		t.Fatalf("early service B Reconcile() error = %v", err)
	} else if delay != configB.Period-changedConfigA.Period {
		t.Fatalf("early service B delay = %s, want %s", delay, configB.Period-changedConfigA.Period)
	}
	if got := requestCount(pathB); got != 1 {
		t.Fatalf("early service B requests = %d, want 1", got)
	}

	clk.Step(configB.Period - changedConfigA.Period)
	if _, err := prober.Reconcile(context.Background(), requestB); err != nil {
		t.Fatalf("second service B Reconcile() error = %v", err)
	}
	assertB(2, false, testProbeNow.Add(configB.Period))
	if reachable := prober.Reachable(targetB); reachable == nil || !*reachable {
		t.Fatalf("service B Reachable() below its threshold = %v, want true", reachable)
	}

	clk.Step(configB.Period)
	if _, err := prober.Reconcile(context.Background(), requestB); err != nil {
		t.Fatalf("third service B Reconcile() error = %v", err)
	}
	assertB(3, true, testProbeNow.Add(2*configB.Period))
	if reachable := prober.Reachable(targetB); reachable == nil || *reachable {
		t.Fatalf("service B Reachable() at its threshold = %v, want false", reachable)
	}
	if got := requestCount(pathB); got != configB.FailureThreshold {
		t.Fatalf("service B requests to %q = %d, want %d", pathB, got, configB.FailureThreshold)
	}
}

func TestProberHydratesOnlyExactPersistedIdentity(t *testing.T) {
	config := testProbeConfig()
	policy := testProbePolicy(t, config)
	lastProbeTime := metav1.NewTime(testProbeNow)
	controller := true
	mustURL := func(t *testing.T, raw string) *apis.URL {
		t.Helper()
		parsed, err := apis.ParseURL(raw)
		if err != nil {
			t.Fatalf("ParseURL(%q) error = %v", raw, err)
		}
		return parsed
	}
	base := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testMap.Name,
			Namespace: testMap.Namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceService",
				Name:       testMap.Name,
				UID:        testOwnerUID,
				Controller: &controller,
			}},
		},
		Spec: v1beta1.TrafficMapSpec{
			Service: testMap.Name,
			Entries: []v1beta1.TrafficMapEntry{{
				Cluster:  "cluster-a",
				Endpoint: mustURL(t, "https://service.example.com/root/"),
				Probe: &v1beta1.TrafficMapProbe{
					PolicyDigest:        policy.PolicyDigest,
					Result:              v1beta1.ProbeResultPassing,
					Gated:               true,
					LastProbeTime:       &lastProbeTime,
					ConsecutiveFailures: 0,
					Message:             "partial recovery",
				},
			}},
		},
	}
	target := testProbeTarget("https://SERVICE.example.com/root", policy.PolicyDigest)

	t.Run("exact identity resumes deadline and resets unpersisted success count", func(t *testing.T) {
		var requests atomic.Int32
		client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("ok")),
				Header:     make(http.Header),
			}, nil
		})}
		clk := clocktesting.NewFakeClock(testProbeNow.Add(config.Period / 2))
		prober := newTestProber(t, client, clk, 1, 1, testProbeResponseBytes)
		request := testProbeRequest(policy, target)
		request.Persisted = base.DeepCopy()

		delay, err := prober.Reconcile(context.Background(), request)
		if err != nil {
			t.Fatalf("hydrate Reconcile() error = %v", err)
		}
		if delay != config.Period/2 || requests.Load() != 0 {
			t.Fatalf("hydrate delay/requests = %s/%d, want %s/0", delay, requests.Load(), config.Period/2)
		}
		if provenance := prober.Provenance(target); provenance == nil || !provenance.Gated || provenance.PolicyDigest != policy.PolicyDigest {
			t.Fatalf("hydrated provenance = %+v", provenance)
		}

		clk.Step(config.Period / 2)
		if _, err := prober.Reconcile(context.Background(), request); err != nil {
			t.Fatalf("first post-hydration Reconcile() error = %v", err)
		}
		if reachable := prober.Reachable(target); reachable == nil || *reachable {
			t.Fatalf("Reachable() after one post-restart pass = %v, want false", reachable)
		}
		clk.Step(config.Period)
		if _, err := prober.Reconcile(context.Background(), request); err != nil {
			t.Fatalf("second post-hydration Reconcile() error = %v", err)
		}
		if reachable := prober.Reachable(target); reachable == nil || !*reachable {
			t.Fatalf("Reachable() after two post-restart passes = %v, want true", reachable)
		}
	})

	t.Run("future persisted timestamp probes immediately", func(t *testing.T) {
		persisted := base.DeepCopy()
		future := metav1.NewTime(testProbeNow.Add(24 * time.Hour))
		persisted.Spec.Entries[0].Probe.LastProbeTime = &future
		var requests atomic.Int32
		client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("ok")),
				Header:     make(http.Header),
			}, nil
		})}
		prober := newTestProber(t, client, clocktesting.NewFakeClock(testProbeNow), 1, 1, testProbeResponseBytes)
		request := testProbeRequest(policy, target)
		request.Persisted = persisted

		if _, err := prober.Reconcile(context.Background(), request); err != nil {
			t.Fatalf("Reconcile() error = %v", err)
		}
		if got := requests.Load(); got != 1 {
			t.Fatalf("requests = %d, want immediate probe for a future persisted timestamp", got)
		}
	})

	tests := []struct {
		name   string
		mutate func(*v1beta1.TrafficMap)
	}{
		{name: "owner UID mismatch", mutate: func(tm *v1beta1.TrafficMap) { tm.OwnerReferences[0].UID = "other-owner" }},
		{name: "not controller owned", mutate: func(tm *v1beta1.TrafficMap) { tm.OwnerReferences[0].Controller = nil }},
		{name: "map mismatch", mutate: func(tm *v1beta1.TrafficMap) { tm.Name = "other-map" }},
		{name: "cluster mismatch", mutate: func(tm *v1beta1.TrafficMap) { tm.Spec.Entries[0].Cluster = "cluster-b" }},
		{name: "endpoint mismatch", mutate: func(tm *v1beta1.TrafficMap) { tm.Spec.Entries[0].Endpoint = mustURL(t, "https://other.example.com") }},
		{name: "policy digest mismatch", mutate: func(tm *v1beta1.TrafficMap) {
			tm.Spec.Entries[0].Probe.PolicyDigest = "sha256:" + strings.Repeat("0", 64)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			persisted := base.DeepCopy()
			tt.mutate(persisted)
			var requests atomic.Int32
			client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				requests.Add(1)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("ok")),
					Header:     make(http.Header),
				}, nil
			})}
			clk := clocktesting.NewFakeClock(testProbeNow.Add(config.Period / 2))
			prober := newTestProber(t, client, clk, 1, 1, testProbeResponseBytes)
			request := testProbeRequest(policy, target)
			request.Persisted = persisted

			if _, err := prober.Reconcile(context.Background(), request); err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}
			if got := requests.Load(); got != 1 {
				t.Fatalf("requests = %d, want fresh probe", got)
			}
			provenance := prober.Provenance(target)
			if provenance == nil || provenance.Gated || provenance.Result != v1beta1.ProbeResultPassing {
				t.Fatalf("fresh provenance = %+v", provenance)
			}
		})
	}
}

func TestProberRejectsLatePredecessorResult(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var requests atomic.Int32
	client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		if requests.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(strings.NewReader("old")),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("new")),
			Header:     make(http.Header),
		}, nil
	})}

	config := testProbeConfig()
	config.FailureThreshold = 1
	clk := clocktesting.NewFakeClock(testProbeNow)
	prober := newTestProber(t, client, clk, 2, 2, testProbeResponseBytes)
	policy := testProbePolicy(t, config)
	target := testProbeTarget("https://service.example.com", policy.PolicyDigest)
	request := testProbeRequest(policy, target)

	firstDone := make(chan error, 1)
	go func() {
		_, err := prober.Reconcile(context.Background(), request)
		firstDone <- err
	}()
	select {
	case <-firstStarted:
	case <-time.After(observerTestTimeout):
		t.Fatal("first probe did not start")
	}

	prober.Forget(testMap)
	if _, err := prober.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("replacement Reconcile() error = %v", err)
	}
	if reachable := prober.Reachable(target); reachable == nil || !*reachable {
		t.Fatalf("replacement Reachable() = %v, want true", reachable)
	}

	close(releaseFirst)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("predecessor Reconcile() error = %v", err)
		}
	case <-time.After(observerTestTimeout):
		t.Fatal("predecessor Reconcile() did not return")
	}
	if reachable := prober.Reachable(target); reachable == nil || !*reachable {
		t.Fatalf("Reachable() after late failure = %v, want retained true", reachable)
	}
}

func TestProberIdentityChangeResetsAndPrunesState(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusServiceUnavailable)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(int(status.Load()))
	}))
	defer server.Close()

	config := testProbeConfig()
	config.FailureThreshold = 1
	clk := clocktesting.NewFakeClock(testProbeNow)
	prober := newTestProber(t, NewObserverClient(), clk, 1, 1, testProbeResponseBytes)
	policy := testProbePolicy(t, config)
	oldTarget := testProbeTarget(server.URL, policy.PolicyDigest)
	if _, err := prober.Reconcile(context.Background(), testProbeRequest(policy, oldTarget)); err != nil {
		t.Fatalf("initial Reconcile() error = %v", err)
	}
	if reachable := prober.Reachable(oldTarget); reachable == nil || *reachable {
		t.Fatalf("initial Reachable() = %v, want false", reachable)
	}
	identityMismatches := []struct {
		name   string
		mutate func(*Target)
	}{
		{name: "owner", mutate: func(target *Target) { target.OwnerUID = "other-owner" }},
		{name: "map", mutate: func(target *Target) { target.Map.Name = "other-map" }},
		{name: "cluster", mutate: func(target *Target) { target.Cluster = "cluster-b" }},
		{name: "endpoint", mutate: func(target *Target) { target.URL += "/other" }},
		{name: "policy", mutate: func(target *Target) { target.PolicyDigest = "sha256:" + strings.Repeat("0", 64) }},
	}
	for _, tt := range identityMismatches {
		t.Run("read rejects "+tt.name+" mismatch", func(t *testing.T) {
			mismatch := oldTarget
			tt.mutate(&mismatch)
			if reachable := prober.Reachable(mismatch); reachable != nil {
				t.Fatalf("Reachable() = %v, want nil", reachable)
			}
			if provenance := prober.Provenance(mismatch); provenance != nil {
				t.Fatalf("Provenance() = %+v, want nil", provenance)
			}
		})
	}

	newTarget := oldTarget
	newTarget.OwnerUID = "replacement-uid"
	if reachable := prober.Reachable(newTarget); reachable != nil {
		t.Fatalf("Reachable() for replacement owner = %v, want nil", reachable)
	}
	status.Store(http.StatusOK)
	request := testProbeRequest(policy, newTarget)
	request.OwnerUID = newTarget.OwnerUID
	if _, err := prober.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("replacement-owner Reconcile() error = %v", err)
	}
	if reachable := prober.Reachable(newTarget); reachable == nil || !*reachable {
		t.Fatalf("replacement-owner Reachable() = %v, want true", reachable)
	}
	if reachable := prober.Reachable(oldTarget); reachable != nil {
		t.Fatalf("Reachable() for pruned owner = %v, want nil", reachable)
	}
	changedConfig := config
	changedConfig.SuccessThreshold++
	changedPolicy := testProbePolicy(t, changedConfig)
	changedTarget := newTarget
	changedTarget.PolicyDigest = changedPolicy.PolicyDigest
	request.Policy = changedPolicy
	request.Targets = []Target{changedTarget}
	if _, err := prober.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("changed-policy Reconcile() error = %v", err)
	}
	if reachable := prober.Reachable(newTarget); reachable != nil {
		t.Fatalf("Reachable() for pruned policy = %v, want nil", reachable)
	}
	if reachable := prober.Reachable(changedTarget); reachable == nil || !*reachable {
		t.Fatalf("changed-policy Reachable() = %v, want true", reachable)
	}

	disabled := request
	disabled.Policy = ResolvedProbePolicy{}
	disabled.Targets = nil
	if _, err := prober.Reconcile(context.Background(), disabled); err != nil {
		t.Fatalf("disabled Reconcile() error = %v", err)
	}
	if provenance := prober.Provenance(changedTarget); provenance != nil {
		t.Fatalf("Provenance() after disable = %+v, want nil", provenance)
	}
}

func TestProberRejectsAmbiguousIdentity(t *testing.T) {
	policy := testProbePolicy(t, testProbeConfig())
	clk := clocktesting.NewFakeClock(testProbeNow)
	prober := newTestProber(t, NewObserverClient(), clk, 1, 1, testProbeResponseBytes)
	validTarget := testProbeTarget("https://service.example.com", policy.PolicyDigest)

	tests := []struct {
		name   string
		mutate func(*ProbeReconcileRequest)
	}{
		{name: "empty request owner UID", mutate: func(request *ProbeReconcileRequest) { request.OwnerUID = "" }},
		{name: "empty request map name", mutate: func(request *ProbeReconcileRequest) { request.Map.Name = "" }},
		{name: "empty request map namespace", mutate: func(request *ProbeReconcileRequest) { request.Map.Namespace = "" }},
		{name: "empty target owner UID", mutate: func(request *ProbeReconcileRequest) { request.Targets[0].OwnerUID = "" }},
		{name: "empty target map", mutate: func(request *ProbeReconcileRequest) { request.Targets[0].Map = types.NamespacedName{} }},
		{name: "empty target digest", mutate: func(request *ProbeReconcileRequest) { request.Targets[0].PolicyDigest = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := testProbeRequest(policy, validTarget)
			tt.mutate(&request)
			if _, err := prober.Reconcile(context.Background(), request); err == nil {
				t.Fatal("Reconcile() error = nil")
			}
		})
	}
}

func TestProberTimeoutStartsAfterExecutorDequeues(t *testing.T) {
	executor, _, _ := startTestObserverExecutor(t, 1, 1)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	first, err := executor.Submit(context.Background(), observerTestTimeout, func(ctx context.Context) error {
		close(firstStarted)
		select {
		case <-releaseFirst:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil {
		t.Fatalf("Submit(first) error = %v", err)
	}
	<-firstStarted

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	config := testProbeConfig()
	config.Timeout = 30 * time.Millisecond
	clk := clocktesting.NewFakeClock(testProbeNow)
	prober, err := NewProber(executor, NewObserverClient(), clk, testProbeResponseBytes, logr.Discard())
	if err != nil {
		t.Fatalf("NewProber() error = %v", err)
	}
	policy := testProbePolicy(t, config)
	target := testProbeTarget(server.URL, policy.PolicyDigest)

	done := make(chan error, 1)
	go func() {
		_, reconcileErr := prober.Reconcile(context.Background(), testProbeRequest(policy, target))
		done <- reconcileErr
	}()
	time.Sleep(3 * config.Timeout)
	if got := requests.Load(); got != 0 {
		t.Fatalf("probe began while queued; requests = %d", got)
	}
	close(releaseFirst)
	if err := waitFuture(t, first); err != nil {
		t.Fatalf("first future error = %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Reconcile() error = %v", err)
		}
	case <-time.After(observerTestTimeout):
		t.Fatal("Reconcile() did not finish")
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests after worker release = %d, want 1", got)
	}
}

func TestObserverClientRefusesRedirects(t *testing.T) {
	var redirectedRequests atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirectedRequests.Add(1)
	}))
	defer destination.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusFound)
	}))
	defer source.Close()

	executor, err := NewObserverExecutor(1, 1)
	if err != nil {
		t.Fatalf("NewObserverExecutor() error = %v", err)
	}
	prober, err := NewProber(executor, NewObserverClient(), clocktesting.NewFakeClock(testProbeNow), testProbeResponseBytes, logr.Discard())
	if err != nil {
		t.Fatalf("NewProber() error = %v", err)
	}
	verdict, _ := prober.probeOne(context.Background(), Target{URL: source.URL}, testProbeConfig())
	if verdict != VerdictInconclusive {
		t.Fatalf("redirect verdict = %v, want %v", verdict, VerdictInconclusive)
	}
	if got := redirectedRequests.Load(); got != 0 {
		t.Fatalf("redirect destination requests = %d, want 0", got)
	}
}

func TestProberTreatsResponseBodyTimeoutAsFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-request.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	prober := &Prober{Client: NewObserverClient(), MaxResponseBytes: testProbeResponseBytes}
	verdict, message := prober.probeOne(ctx, Target{URL: server.URL}, testProbeConfig())

	if verdict != VerdictFail {
		t.Fatalf("response body timeout verdict = %v, want %v", verdict, VerdictFail)
	}
	if !strings.Contains(message, "context deadline exceeded") {
		t.Fatalf("response body timeout message = %q, want deadline error", message)
	}
}

func TestProberJoinsPathsAndTreatsTransportFailureAsFailure(t *testing.T) {
	paths := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		paths <- request.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	executor, err := NewObserverExecutor(1, 1)
	if err != nil {
		t.Fatalf("NewObserverExecutor() error = %v", err)
	}
	prober, err := NewProber(executor, NewObserverClient(), clocktesting.NewFakeClock(testProbeNow), testProbeResponseBytes, logr.Discard())
	if err != nil {
		t.Fatalf("NewProber() error = %v", err)
	}
	verdict, message := prober.probeOne(context.Background(), Target{URL: server.URL + "/team/model/"}, testProbeConfig())
	if verdict != VerdictPass {
		t.Fatalf("probeOne() = %v (%s), want %v", verdict, message, VerdictPass)
	}
	if got := <-paths; got != "/team/model/healthz/deep" {
		t.Fatalf("request path = %q, want %q", got, "/team/model/healthz/deep")
	}
	endpoint := server.URL
	server.Close()

	verdict, message = prober.probeOne(context.Background(), Target{URL: endpoint}, testProbeConfig())
	if verdict != VerdictFail || !strings.Contains(message, "transport error") {
		t.Fatalf("closed-endpoint probe = %v (%s), want transport failure", verdict, message)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type trackingBody struct {
	reader io.Reader
	read   int
	closed bool
}

func (b *trackingBody) Read(buffer []byte) (int, error) {
	n, err := b.reader.Read(buffer)
	b.read += n
	return n, err
}

func (b *trackingBody) Close() error {
	b.closed = true
	return nil
}

func TestProberBoundsAndClosesResponseBody(t *testing.T) {
	const responseBound int64 = 7
	body := &trackingBody{reader: strings.NewReader(strings.Repeat("x", 100))}
	client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       body,
			Header:     make(http.Header),
		}, nil
	})}
	executor, err := NewObserverExecutor(1, 1)
	if err != nil {
		t.Fatalf("NewObserverExecutor() error = %v", err)
	}
	prober, err := NewProber(executor, client, clocktesting.NewFakeClock(testProbeNow), responseBound, logr.Discard())
	if err != nil {
		t.Fatalf("NewProber() error = %v", err)
	}
	verdict, _ := prober.probeOne(context.Background(), Target{URL: "https://service.example.com"}, testProbeConfig())
	if verdict != VerdictPass {
		t.Fatalf("probeOne() = %v, want %v", verdict, VerdictPass)
	}
	if body.read != int(responseBound) {
		t.Fatalf("response bytes read = %d, want %d", body.read, responseBound)
	}
	if !body.closed {
		t.Fatal("response body was not closed")
	}
}

func TestTargetsFromTrafficMapsCarriesIdentityAndSorts(t *testing.T) {
	mustURL := func(raw string) *apis.URL {
		parsed, err := apis.ParseURL(raw)
		if err != nil {
			t.Fatalf("ParseURL(%q) error = %v", raw, err)
		}
		return parsed
	}
	controller := true
	maps := []v1beta1.TrafficMap{{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testMap.Name,
			Namespace: testMap.Namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceService",
				Name:       testMap.Name,
				UID:        testOwnerUID,
				Controller: &controller,
			}},
		},
		Spec: v1beta1.TrafficMapSpec{Service: testMap.Name, Entries: []v1beta1.TrafficMapEntry{
			{
				Cluster:  "cluster-b",
				Endpoint: mustURL("https://B.example.com/"),
				Probe:    &v1beta1.TrafficMapProbe{PolicyDigest: "sha256:" + strings.Repeat("b", 64)},
			},
			{Cluster: "cluster-a", Endpoint: mustURL("https://A.example.com")},
			{Cluster: "cluster-c"},
		}},
	}}
	targets := TargetsFromTrafficMaps(maps)
	if len(targets) != 2 {
		t.Fatalf("targets = %d (%v), want 2", len(targets), targets)
	}
	if targets[0].Cluster != "cluster-a" || targets[1].Cluster != "cluster-b" {
		t.Fatalf("targets not cluster-sorted: %v", targets)
	}
	if targets[0].OwnerUID != testOwnerUID || targets[0].Map != testMap {
		t.Fatalf("target identity = %+v", targets[0])
	}
	if targets[1].URL != "https://b.example.com" || targets[1].PolicyDigest == "" {
		t.Fatalf("canonical target = %+v", targets[1])
	}
}

func TestTargetsFromTrafficMapsRequiresExactControllerOwner(t *testing.T) {
	controller := true
	valid := v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testMap.Name,
			Namespace: testMap.Namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceService",
				Name:       testMap.Name,
				UID:        testOwnerUID,
				Controller: &controller,
			}},
		},
		Spec: v1beta1.TrafficMapSpec{
			Service: testMap.Name,
			Entries: []v1beta1.TrafficMapEntry{{
				Cluster:  "cluster-a",
				Endpoint: mustAPISURL(t, "https://service.example.com"),
			}},
		},
	}
	tests := []struct {
		name   string
		mutate func(*v1beta1.TrafficMap)
	}{
		{name: "wrong API version", mutate: func(tm *v1beta1.TrafficMap) { tm.OwnerReferences[0].APIVersion = "other.example/v1" }},
		{name: "wrong kind", mutate: func(tm *v1beta1.TrafficMap) { tm.OwnerReferences[0].Kind = "Other" }},
		{name: "wrong owner name", mutate: func(tm *v1beta1.TrafficMap) { tm.OwnerReferences[0].Name = "other" }},
		{name: "service mismatch", mutate: func(tm *v1beta1.TrafficMap) { tm.Spec.Service = "other" }},
		{name: "map name mismatch", mutate: func(tm *v1beta1.TrafficMap) { tm.Name = "other" }},
		{name: "missing UID", mutate: func(tm *v1beta1.TrafficMap) { tm.OwnerReferences[0].UID = "" }},
		{name: "not controller", mutate: func(tm *v1beta1.TrafficMap) { tm.OwnerReferences[0].Controller = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trafficMap := valid.DeepCopy()
			tt.mutate(trafficMap)
			if targets := TargetsFromTrafficMaps([]v1beta1.TrafficMap{*trafficMap}); len(targets) != 0 {
				t.Fatalf("TargetsFromTrafficMaps() = %+v, want none", targets)
			}
		})
	}
}

func mustAPISURL(t *testing.T, raw string) *apis.URL {
	t.Helper()
	parsed, err := apis.ParseURL(raw)
	if err != nil {
		t.Fatalf("ParseURL(%q) error = %v", raw, err)
	}
	return parsed
}
