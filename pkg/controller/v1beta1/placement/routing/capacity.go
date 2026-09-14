package routing

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
)

// maxCapacityBody bounds how much of a capacity response is read. A home that
// streams an unbounded body must not be able to exhaust control-plane memory.
const maxCapacityBody = 64 << 10

// CapacityReport is the contract a home's capacity endpoint answers with.
//
// Servable is an ABSOLUTE count in servable units — usable prefill/decode
// pairs, or servable replicas for a non-disaggregated home. Deliberately not a
// utilization ratio: a saturated home and an idle one report the same fraction,
// which says nothing about how much either can serve. Deliberately not a flat
// sum of per-worker request slots either, since that counts prefill slots with
// no decode partner and so OVERSTATES capacity during exactly the rollout skew
// this input exists to correct.
//
// ObservedAt is when the home computed the figure, so a frozen reporter is
// detectable. Without it a stale read is indistinguishable from a fresh one and
// a wedged home would hold its weight forever.
type CapacityReport struct {
	Servable   int32     `json:"servable"`
	ObservedAt time.Time `json:"observedAt"`
}

// capacityState is the poller's running view of one home.
type capacityState struct {
	// window holds the most recent accepted readings, oldest first. The applied
	// ceiling is their Quorum-th smallest, so no single reading -- however low
	// -- can set it alone.
	window []int32
	// reported is the applied ceiling, or nil when the home has not answered
	// usably. Nil means the control-plane plan stands.
	reported *int32
	// reason explains why reported is nil, for the fail-open condition. Empty
	// when the last poll was accepted.
	reason string
}

// observe folds one accepted reading into the window and returns the resulting
// ceiling -- the quorum-th smallest retained reading -- and whether one applies
// at all.
//
// Until the window holds quorum readings, none does: the plan stands. A ceiling
// derived from fewer readings than the quorum would be exactly the thing the
// quorum exists to prevent, since the first reading after startup or recovery
// could come from the one broken reporter and would set the ceiling with
// nothing to corroborate it.
func (st *capacityState) observe(v int32, samples, quorum int) (int32, bool) {
	st.window = append(st.window, v)
	if len(st.window) > samples {
		st.window = st.window[len(st.window)-samples:]
	}
	if len(st.window) < quorum {
		return 0, false
	}
	sorted := make([]int32, len(st.window))
	copy(sorted, st.window)
	slices.Sort(sorted)
	return sorted[quorum-1], true
}

// CapacityPoller asks each home what it can currently serve and turns the
// answer into a CEILING on that home's planned allocation.
//
// Only the home knows its realized prefill/decode pairing: the control plane
// computes MIN(ready_prefill, ready_decode), an upper bound on usable pairs
// rather than a count of them, and during a rollout the two components recover
// at different rates so the skew is widest exactly when the number matters
// most.
//
// It shares the prober's HTTP client: both outputs talk to the same homes with
// the same credentials and fail the same ways, so splitting them across two
// clients would mean two auth paths, two staleness semantics, and two ways for
// an operator to half-configure the feature. They stay independently
// enabled — neither implies the other.
//
// Every failure mode falls open to the plan. Fail-closed would turn a
// control-plane-to-data-plane network blip into a fleet-wide traffic cutoff,
// which is a far worse outcome than routing to a home that is briefly
// over-weighted.
type CapacityPoller struct {
	Config CapacityConfig
	Log    logr.Logger
	Client *http.Client

	// Targets supplies the homes to poll, shared with the prober's view of the
	// current routing tables.
	Targets func(context.Context) ([]Target, error)

	// OnChange is called with the affected TrafficMap when a home's reported
	// ceiling changes, so weights are recomputed promptly.
	OnChange func(types.NamespacedName)

	mu    sync.RWMutex
	state map[key]*capacityState
}

// NewCapacityPoller builds a poller sharing the given HTTP client.
func NewCapacityPoller(cfg CapacityConfig, client *http.Client, log logr.Logger) *CapacityPoller {
	return &CapacityPoller{
		Config: cfg,
		Log:    log,
		Client: client,
		state:  map[key]*capacityState{},
	}
}

// Reported returns the home's last accepted servable count, or nil when the
// plan should stand — polling off, no answer yet, or the last answer was
// unusable. Nil is the fail-open value.
func (c *CapacityPoller) Reported(m types.NamespacedName, cluster string) *int32 {
	if c == nil || !c.Config.IsEnabled() {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	st, ok := c.state[key{Map: m, Cluster: cluster}]
	if !ok || st.reported == nil {
		return nil
	}
	v := *st.reported
	return &v
}

// FellOpen reports why a home's report was not applied, or empty when it was
// applied or polling is off.
//
// Silent fallback is as bad as fail-closed for an operator: the weight looks
// the same whether the home agreed with the plan or the poll failed, and those
// have opposite remediations.
func (c *CapacityPoller) FellOpen(m types.NamespacedName, cluster string) string {
	if c == nil || !c.Config.IsEnabled() {
		return ""
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	st, ok := c.state[key{Map: m, Cluster: cluster}]
	if !ok {
		return "no capacity report yet"
	}
	return st.reason
}

// Start runs the poll loop until the context is cancelled, satisfying
// manager.Runnable. It returns immediately when capacity polling is not
// configured, so it can be wired unconditionally and stay inert by config.
func (c *CapacityPoller) Start(ctx context.Context) error {
	if !c.Config.IsEnabled() {
		c.Log.Info("endpoint-reported capacity disabled (no capacity path configured)")
		return nil
	}
	c.Log.Info("starting endpoint-reported capacity polling",
		"path", c.Config.Path, "period", c.Config.Period, "timeout", c.Config.Timeout,
		"maxAge", c.Config.MaxAge)

	t := time.NewTicker(c.Config.Period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			c.pollAll(ctx)
		}
	}
}

// NeedLeaderElection keeps polling on the elected leader only: extra replicas
// add load on the homes without adding signal, and only the leader writes.
func (c *CapacityPoller) NeedLeaderElection() bool { return true }

// pollAll polls every current target once and notifies for each home whose
// reported ceiling changed.
func (c *CapacityPoller) pollAll(ctx context.Context) {
	targets, err := c.Targets(ctx)
	if err != nil {
		c.Log.Error(err, "listing capacity targets")
		return
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		changed = map[types.NamespacedName]struct{}{}
		sem     = make(chan struct{}, probeConcurrency)
	)
	for _, tgt := range targets {
		wg.Add(1)
		go func(tgt Target) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			reported, reason := c.pollOne(ctx, tgt)
			if c.record(tgt, reported, reason) {
				mu.Lock()
				changed[tgt.Map] = struct{}{}
				mu.Unlock()
			}
		}(tgt)
	}
	wg.Wait()
	c.forget(targets)

	if c.OnChange == nil {
		return
	}
	for m := range changed {
		c.OnChange(m)
	}
}

// pollOne fetches and validates one home's capacity report. It returns the
// accepted count, or nil plus the reason the plan stands instead.
func (c *CapacityPoller) pollOne(ctx context.Context, tgt Target) (*int32, string) {
	rctx, cancel := context.WithTimeout(ctx, c.Config.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(rctx, c.Config.Method, tgt.URL+c.Config.Path, nil)
	if err != nil {
		return nil, fmt.Sprintf("capacity request could not be built: %v", err)
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, fmt.Sprintf("capacity endpoint unreachable: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Sprintf("capacity endpoint returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxCapacityBody))
	if err != nil {
		return nil, fmt.Sprintf("reading capacity response: %v", err)
	}
	report, reason := c.Config.decode(body)
	if reason != "" {
		return nil, reason
	}
	if report.Servable < 0 {
		// Malformed, not an instruction to serve nothing. A parse or arithmetic
		// bug on the home must not be able to black-hole it.
		return nil, fmt.Sprintf("capacity report is negative (%d)", report.Servable)
	}
	if age := time.Since(report.ObservedAt); age > c.Config.MaxAge {
		return nil, fmt.Sprintf("capacity report is stale (observed %s ago, max %s)", age.Truncate(time.Second), c.Config.MaxAge)
	}
	v := report.Servable
	return &v, ""
}

// decode turns a response body into a report in servable units by handing it to
// the configured format, or explains why it cannot.
func (c CapacityConfig) decode(body []byte) (CapacityReport, string) {
	name := c.resolvedFormat()
	plugin, ok := lookupCapacityFormat(name)
	if !ok {
		// Validate rejects this at startup, so reaching here means the registry
		// changed under us. Fall open rather than guess at the shape.
		return CapacityReport{}, fmt.Sprintf("capacity format %q is not registered in this build", name)
	}
	report, reason := plugin.Decode(body, c)
	if reason != "" {
		return CapacityReport{}, reason
	}
	if report.ObservedAt.IsZero() {
		// A format that does not stamp its own reading gets one at receipt, so
		// the staleness comparison below has something to work with. Note this
		// makes MaxAge inert for such a format: the value is always ~0 old.
		report.ObservedAt = time.Now()
	}
	return report, ""
}

// record stores a home's outcome and reports whether the applied ceiling
// changed. Only a change in the reported value is worth a reconcile: the
// weights depend on the number, not on the poll having happened.
func (c *CapacityPoller) record(tgt Target, reported *int32, reason string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	k := key{Map: tgt.Map, Cluster: tgt.Cluster}
	st, ok := c.state[k]
	if !ok {
		st = &capacityState{}
		c.state[k] = st
	}
	var applied *int32
	if reported != nil {
		if v, ok := st.observe(*reported, c.Config.samples(), c.Config.quorum()); ok {
			applied = &v
		}
	} else {
		// A failed poll falls open and releases the ceiling, so the window must
		// not survive it: retaining readings would keep constraining a home we
		// can no longer observe, which is the opposite of failing open.
		st.window = nil
	}
	changed := !sameReport(st.reported, applied)
	st.reported = applied
	st.reason = reason
	return changed
}

// sameReport compares two optional counts, treating nil as its own value so a
// home that stops reporting is a change rather than a no-op.
func sameReport(a, b *int32) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}

// forget drops state for homes that are no longer targets, so a removed home
// does not leak and a returning one is re-polled rather than inheriting a stale
// ceiling.
func (c *CapacityPoller) forget(targets []Target) {
	live := make(map[key]struct{}, len(targets))
	for _, t := range targets {
		live[key{Map: t.Map, Cluster: t.Cluster}] = struct{}{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.state {
		if _, ok := live[k]; !ok {
			delete(c.state, k)
		}
	}
}
