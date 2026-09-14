package routing

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Target is one home to probe: the entry's externally-addressable URL, keyed by
// the TrafficMap it belongs to and the cluster it serves.
type Target struct {
	Map     types.NamespacedName
	Cluster string
	URL     string
}

// key identifies a probed home across passes, so its consecutive-failure count
// survives from one probe to the next.
type key struct {
	Map     types.NamespacedName
	Cluster string
}

// homeProbeState is the prober's running verdict for one home.
type homeProbeState struct {
	// gated is the value ANDed into the health gate, flipped only when a
	// threshold is crossed rather than on every probe.
	gated bool
	// consecutiveFailures and consecutivePasses drive the hysteresis; exactly one
	// is non-zero at a time.
	consecutiveFailures int
	consecutivePasses   int
	lastResult          v1beta1.ProbeResult
	lastMessage         string
	lastProbeTime       time.Time
}

// Prober runs the active end-to-end health probe and holds each
// home's gate verdict.
//
// It probes an entry's endpoint — the URL a client uses — so the verdict covers
// the whole serving path: the cluster's ingress gateway, DNS, the certificate,
// the route object. None of that is visible to readyReplicas, which is pod
// readiness observed inside the home, so a home can report every replica ready
// while nothing can reach it.
//
// The gate flips only after a threshold of consecutive identical verdicts. A
// single dropped packet must not move a large traffic share, and the
// reprogramming churn from flapping would itself be the outage.
//
// The prober is a correlated component: one bug here, or one control-plane
// network partition, marks every home down at once. Two things bound that. A
// probe that could not run at all is inconclusive rather than a failure, so a
// broken prober does not gate anything; and when every home is gated the weight
// function falls back to equal weights rather than black-holing traffic.
type Prober struct {
	Config ProbeConfig
	Log    logr.Logger

	// Client is the HTTP client used for probes. Its timeout is set from the
	// config at construction.
	Client *http.Client

	// Targets supplies the homes to probe on each tick. It is a function rather
	// than a stored list so the prober always sees the current routing tables
	// without duplicating the controller's cache.
	Targets func(context.Context) ([]Target, error)

	// OnChange is called with the affected TrafficMap whenever a home's gate
	// flips, so the routing controller can recompute and rewrite weights
	// promptly instead of waiting for unrelated ISVC churn.
	OnChange func(types.NamespacedName)

	mu    sync.RWMutex
	state map[key]*homeProbeState
}

// NewProber builds a prober for the given config, using the supplied HTTP
// client. The caller is responsible for checking cfg.IsEnabled(); a disabled
// config yields a prober that reports Unknown for every home, which never
// gates.
//
// The client carries no timeout of its own: each request is bounded by a
// context deadline instead, so one client can be shared with the capacity
// poller even though the two have different timeouts.
func NewProber(cfg ProbeConfig, client *http.Client, log logr.Logger) *Prober {
	return &Prober{
		Config: cfg,
		Log:    log,
		Client: client,
		state:  map[key]*homeProbeState{},
	}
}

// NewObserverClient builds the HTTP client both observers share. They talk to
// the same homes with the same credentials and fail the same ways, so a single
// client keeps one transport, one connection pool and one auth path rather
// than two of each.
func NewObserverClient() *http.Client {
	// No client-level timeout: per-request context deadlines carry the probe's
	// and the poller's different bounds.
	return &http.Client{}
}

// Reachable reports the probe's contribution to a home's health gate.
//
// Nil means "no verdict, do not gate": probing is off, the home has not been
// probed yet, or its probes have been inconclusive. That is deliberately the
// same answer a broken prober gives, because the alternative — treating an
// absent verdict as failure — would zero every home at once.
func (p *Prober) Reachable(m types.NamespacedName, cluster string) *bool {
	if p == nil || !p.Config.IsEnabled() {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	st, ok := p.state[key{Map: m, Cluster: cluster}]
	if !ok || st.lastResult == v1beta1.ProbeResultUnknown {
		return nil
	}
	ok2 := !st.gated
	return &ok2
}

// Provenance renders a home's probe state for the TrafficMap entry, or nil when
// there is nothing observed to report. Healthy alone does not say whether
// readiness or reachability failed, and those are different faults with
// different owners.
func (p *Prober) Provenance(m types.NamespacedName, cluster string) *v1beta1.TrafficMapProbe {
	if p == nil || !p.Config.IsEnabled() {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	st, ok := p.state[key{Map: m, Cluster: cluster}]
	if !ok {
		return nil
	}
	pr := &v1beta1.TrafficMapProbe{
		Result:              st.lastResult,
		ConsecutiveFailures: int32(st.consecutiveFailures),
		Message:             st.lastMessage,
	}
	if !st.lastProbeTime.IsZero() {
		t := metav1.NewTime(st.lastProbeTime)
		pr.LastProbeTime = &t
	}
	return pr
}

// Start runs the probe loop until the context is cancelled, satisfying
// manager.Runnable. It returns immediately when probing is not configured, so
// the prober can be wired unconditionally and stay inert by config alone.
func (p *Prober) Start(ctx context.Context) error {
	if !p.Config.IsEnabled() {
		p.Log.Info("end-to-end health probing disabled (no probe path configured)")
		return nil
	}
	p.Log.Info("starting end-to-end health probing",
		"path", p.Config.Path, "method", p.Config.Method, "period", p.Config.Period,
		"timeout", p.Config.Timeout, "failureThreshold", p.Config.FailureThreshold,
		"successThreshold", p.Config.SuccessThreshold)

	t := time.NewTicker(p.Config.Period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			p.probeAll(ctx)
		}
	}
}

// NeedLeaderElection keeps probing on the elected leader only. Every replica
// probing would multiply load on the homes by the replica count for no extra
// signal, and only the leader writes the resulting weights.
func (p *Prober) NeedLeaderElection() bool { return true }

// probeAll probes every current target once, concurrently, and notifies the
// controller for each home whose gate flipped.
func (p *Prober) probeAll(ctx context.Context) {
	targets, err := p.Targets(ctx)
	if err != nil {
		// Inconclusive by construction: without targets nothing is observed, so
		// no gate moves and the previous verdicts stand.
		p.Log.Error(err, "listing probe targets")
		return
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		changed = map[types.NamespacedName]struct{}{}
		// Bound concurrency so a large fleet does not open one socket per home
		// simultaneously.
		sem = make(chan struct{}, probeConcurrency)
	)
	for _, tgt := range targets {
		wg.Add(1)
		go func(tgt Target) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			verdict, msg := p.probeOne(ctx, tgt)
			if p.record(tgt, verdict, msg) {
				mu.Lock()
				changed[tgt.Map] = struct{}{}
				mu.Unlock()
			}
		}(tgt)
	}
	wg.Wait()

	p.forget(targets)

	if p.OnChange == nil {
		return
	}
	for m := range changed {
		p.OnChange(m)
	}
}

// probeConcurrency bounds simultaneous in-flight probes.
const probeConcurrency = 16

// probeOne issues a single probe and classifies the outcome.
//
// It distinguishes "the probe failed" from "the probe could not run". A request
// we could not even construct is our own defect, not the home's, and is
// reported inconclusive so a misconfigured prober cannot gate the fleet.
func (p *Prober) probeOne(ctx context.Context, tgt Target) (Verdict, string) {
	url := tgt.URL + p.Config.Path

	rctx, cancel := context.WithTimeout(ctx, p.Config.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(rctx, p.Config.Method, url, nil)
	if err != nil {
		return VerdictInconclusive, fmt.Sprintf("probe could not run: %v", err)
	}
	resp, err := p.Client.Do(req)
	if err != nil {
		// A transport error or timeout is the home's failure: the path did not
		// carry a request a client would have sent.
		return VerdictFail, fmt.Sprintf("transport error: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch v := p.Config.ClassifyStatus(resp.StatusCode); v {
	case VerdictPass:
		return VerdictPass, fmt.Sprintf("HTTP %d", resp.StatusCode)
	case VerdictFail:
		return VerdictFail, fmt.Sprintf("HTTP %d", resp.StatusCode)
	default:
		return VerdictInconclusive, fmt.Sprintf("HTTP %d (not in accept or gate list)", resp.StatusCode)
	}
}

// record folds one verdict into a home's state and reports whether the gate
// flipped. Only a flip is worth waking the controller for: the counts move on
// every probe, but the weights only change when the gate does.
func (p *Prober) record(tgt Target, v Verdict, msg string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	k := key{Map: tgt.Map, Cluster: tgt.Cluster}
	st, ok := p.state[k]
	if !ok {
		st = &homeProbeState{lastResult: v1beta1.ProbeResultUnknown}
		p.state[k] = st
	}
	st.lastMessage = msg
	st.lastProbeTime = time.Now()

	switch v {
	case VerdictInconclusive:
		// Neither counter moves and lastResult is left alone: an inconclusive
		// probe is not evidence either way, so a run of them must neither drift
		// a home toward a flip nor erase the verdict that came before.
		return false
	case VerdictFail:
		st.consecutivePasses = 0
		st.consecutiveFailures++
		st.lastResult = v1beta1.ProbeResultFailing
		if !st.gated && st.consecutiveFailures >= p.Config.FailureThreshold {
			st.gated = true
			return true
		}
		return false
	default: // VerdictPass
		st.consecutiveFailures = 0
		st.consecutivePasses++
		st.lastResult = v1beta1.ProbeResultPassing
		if st.gated && st.consecutivePasses >= p.Config.SuccessThreshold {
			st.gated = false
			return true
		}
		return false
	}
}

// forget drops state for homes that are no longer targets, so a deleted ISVC or
// a removed home does not leak an entry — and so a home that comes back is
// re-evaluated from scratch rather than inheriting a stale gate.
func (p *Prober) forget(targets []Target) {
	live := make(map[key]struct{}, len(targets))
	for _, t := range targets {
		live[key{Map: t.Map, Cluster: t.Cluster}] = struct{}{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for k := range p.state {
		if _, ok := live[k]; !ok {
			delete(p.state, k)
		}
	}
}

// TargetsFromTrafficMaps builds the probe target list from the TrafficMaps a
// lister returns: every entry that carries an addressable endpoint.
//
// Sorted for a deterministic probe order, which keeps logs and tests stable.
func TargetsFromTrafficMaps(maps []v1beta1.TrafficMap) []Target {
	var targets []Target
	for i := range maps {
		tm := &maps[i]
		for _, e := range tm.Spec.Entries {
			if e.Endpoint == nil || e.Endpoint.Host == "" {
				continue
			}
			targets = append(targets, Target{
				Map:     types.NamespacedName{Name: tm.Name, Namespace: tm.Namespace},
				Cluster: e.Cluster,
				URL:     e.Endpoint.String(),
			})
		}
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Map != targets[j].Map {
			return targets[i].Map.String() < targets[j].Map.String()
		}
		return targets[i].Cluster < targets[j].Cluster
	})
	return targets
}
