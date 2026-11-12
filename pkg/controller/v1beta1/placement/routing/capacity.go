package routing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// CapacityReport carries capacity in replicas: a whole count for Ceiling mode,
// or a weight that may be fractional for Reported mode. Neither value
// represents utilization. ObservedAt lets the poller reject frozen reports even
// when the endpoint still answers.
type CapacityReport struct {
	// Servable is an absolute replica count in Ceiling mode. A report without
	// one omits the field, so a report written for Reported mode cannot tell a
	// Ceiling consumer that the cluster serves zero replicas.
	Servable *int32 `json:"servable,omitempty"`
	// Weight is the cluster's capacity in replicas for Reported mode, already
	// normalized by the producer for its hardware and workload.
	Weight     *float64  `json:"weight,omitempty"`
	ObservedAt time.Time `json:"observedAt"`
}

type capacitySample struct {
	servable float64
	expires  time.Time
}

// capacityState is the poller's running view of one cluster.
type capacityState struct {
	// generation identifies the polling pass allowed to update this state. A
	// completion from an older pass cannot overwrite a newer observation.
	generation uint64
	// window holds accepted readings, oldest first. The applied value is
	// derived from them by estimate.
	window []capacitySample
	// reported is the accepted value, or nil without a fresh quorum. Nil means
	// the cluster keeps its replica count in either mode.
	reported *float64
	// reason describes a failed observation or insufficient evidence. A failed
	// poll can leave a fresh cached value in Reported mode.
	reason string
	// nextPollTime is the next absolute deadline for this exact target and
	// policy. Advancing from the prior deadline avoids cadence drift.
	nextPollTime time.Time
	inFlight     bool
}

// capacityKey identifies the source of one capacity history. Owner, URL, and
// effective policy are part of the identity so replacements or policy changes
// start from an empty window instead of inheriting unrelated observations.
type capacityKey struct {
	ownerUID     types.UID
	mapKey       types.NamespacedName
	cluster      string
	url          string
	policyDigest string
}

type capacityWork struct {
	target     Target
	key        capacityKey
	generation uint64
	future     *ObserverFuture
	recorded   atomic.Bool
}

// CapacityReconcileRequest is the complete capacity-polling input for one
// InferenceService.
type CapacityReconcileRequest struct {
	OwnerUID types.UID
	Map      types.NamespacedName
	Policy   ResolvedCapacityPolicy
	Targets  []Target
}

// observe adds a reading and returns the resulting estimate. Fewer than quorum
// readings cannot supply an accepted value in either mode.
func (st *capacityState) observe(v float64, expires time.Time, config CapacityConfig) (float64, bool) {
	st.window = append(st.window, capacitySample{servable: v, expires: expires})
	if samples := config.samples(); len(st.window) > samples {
		st.window = st.window[len(st.window)-samples:]
	}
	return st.estimate(config)
}

// estimate derives the applied value from the window once it holds quorum
// readings.
//
// As a ceiling it is the quorum-th smallest reading, so a lower value needs
// quorum readings to corroborate it. In Reported mode the value is the cluster's
// capacity, and a high reading can overload the cluster as surely as a low one
// starves it. So the value is the lower median: a single outlier cannot move it
// in a window of three or more readings, and of two readings the smaller wins.
func (st *capacityState) estimate(config CapacityConfig) (float64, bool) {
	quorum := config.quorum()
	if len(st.window) < quorum {
		return 0, false
	}
	sorted := make([]float64, len(st.window))
	for i := range st.window {
		sorted[i] = st.window[i].servable
	}
	slices.Sort(sorted)
	if config.Mode == v1beta1.RoutingCapacityModeReported {
		return sorted[(len(sorted)-1)/2], true
	}
	return sorted[quorum-1], true
}

// expire removes evidence that has reached its producer timestamp's maximum
// age and recomputes the estimate from the remaining fresh samples. Samples can
// arrive with out-of-order timestamps, so every entry is checked.
func (st *capacityState) expire(config CapacityConfig, now time.Time) {
	kept := st.window[:0]
	for _, sample := range st.window {
		if sample.expires.After(now) {
			kept = append(kept, sample)
		}
	}
	if len(kept) == len(st.window) {
		return
	}
	st.window = kept
	if v, ok := st.estimate(config); ok {
		st.reported = &v
		// Expiry is not a successful observation; preserve any failed-poll reason.
	} else {
		st.reported = nil
		if len(st.window) == 0 {
			st.reason = capacityStaleReason(config.MaxAge)
		} else {
			st.reason = fmt.Sprintf(
				"capacity report awaiting quorum after stale samples expired (%d/%d accepted samples)",
				len(st.window), config.quorum(),
			)
		}
	}
}

// CapacityPoller collects capacity using the prober's HTTP client and worker
// limits. Ceiling mode falls back to replica counts on failures. Reported mode
// keeps fresh accepted samples through a failed poll; without them the cluster
// falls back to its replica count.
type CapacityPoller struct {
	Client *http.Client
	Clock  clock.Clock
	// Executor owns the fixed worker and queue bounds shared by active routing
	// observations.
	Executor *ObserverExecutor
	// MaxResponseBytes is the configured upper bound for one response body.
	MaxResponseBytes int64

	mu        sync.RWMutex
	nextToken uint64
	state     map[capacityKey]*capacityState
}

// NewCapacityPoller builds a poller sharing the given HTTP client and bounded
// observer executor.
func NewCapacityPoller(
	executor *ObserverExecutor,
	client *http.Client,
	clk clock.Clock,
	maxResponseBytes int64,
) (*CapacityPoller, error) {
	if err := checkObserverInputs("capacity", executor, client, clk, maxResponseBytes); err != nil {
		return nil, err
	}
	return &CapacityPoller{
		Client:           client,
		Clock:            clk,
		Executor:         executor,
		MaxResponseBytes: maxResponseBytes,
		state:            map[capacityKey]*capacityState{},
	}, nil
}

// Reported returns the accepted count or weight, or nil without fresh evidence.
// The caller applies the selected mode's behavior for a missing report.
func (c *CapacityPoller) Reported(tgt Target, policy ResolvedCapacityPolicy) *float64 {
	reported, _ := c.observation(tgt, policy)
	return reported
}

// FellOpen returns the observation failure reason. Reported mode may retain a
// fresh cached value alongside that reason. Empty means usable or disabled.
func (c *CapacityPoller) FellOpen(tgt Target, policy ResolvedCapacityPolicy) string {
	_, reason := c.observation(tgt, policy)
	return reason
}

// observation returns one freshness-consistent view for projection. It expires
// samples under the same lock used to copy the result, so a caller cannot pair
// a pre-expiry count with a post-expiry fallback reason.
func (c *CapacityPoller) observation(
	tgt Target,
	policy ResolvedCapacityPolicy,
) (*float64, string) {
	if c == nil || !policy.IsEnabled() {
		return nil, ""
	}
	k, err := c.keyFor(tgt, policy)
	if err != nil {
		return nil, fmt.Sprintf("capacity target is invalid: %v", err)
	}
	if c.Clock == nil {
		return nil, "capacity clock is not configured"
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.state[k]
	if !ok {
		return nil, "no capacity report yet"
	}
	st.expire(policy.Capacity, c.Clock.Now())
	if st.reported == nil {
		return nil, st.reason
	}
	v := *st.reported
	return &v, st.reason
}

// Forget removes every capacity history for one TrafficMap. An observation
// already in flight retains its old generation and cannot recreate the state.
func (c *CapacityPoller) Forget(mapKey types.NamespacedName) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.state {
		if key.mapKey == mapKey {
			delete(c.state, key)
		}
	}
}

// Reconcile synchronously runs every due capacity poll for one
// InferenceService and returns the delay until its earliest absolute deadline.
// A zero delay with an enabled non-empty request means the deadline is already
// due.
func (c *CapacityPoller) Reconcile(ctx context.Context, req CapacityReconcileRequest) (time.Duration, error) {
	if c == nil {
		return 0, errors.New("capacity poller is nil")
	}
	if ctx == nil {
		return 0, errors.New("capacity reconcile context is nil")
	}
	if c.Clock == nil {
		return 0, errors.New("capacity clock is nil")
	}
	if req.OwnerUID == "" {
		return 0, errors.New("capacity reconcile owner UID is empty")
	}
	if req.Map.Namespace == "" || req.Map.Name == "" {
		return 0, errors.New("capacity reconcile TrafficMap namespace and name are required")
	}
	if !req.Policy.IsEnabled() {
		c.Forget(req.Map)
		return 0, nil
	}
	if !req.Policy.Capacity.IsEnabled() {
		return 0, errors.New("enabled capacity policy has no request path")
	}
	if err := req.Policy.Capacity.Validate(); err != nil {
		return 0, err
	}
	wantDigest, err := capacityPolicyDigest(req.Policy.Capacity)
	if err != nil {
		return 0, err
	}
	if req.Policy.PolicyDigest != wantDigest {
		return 0, fmt.Errorf("capacity policy digest %q does not match effective policy", req.Policy.PolicyDigest)
	}

	targets, err := normalizeCapacityTargets(req)
	if err != nil {
		return 0, err
	}
	work := c.prepare(req, targets)
	accepted := make([]*capacityWork, 0, len(work))
	var reconcileErrs []error
	for i := range work {
		item := &work[i]
		if c.Executor == nil {
			reason := "capacity observer executor is not configured"
			item.recorded.Store(c.recordCurrent(
				item.key, item.generation, req.Policy.Capacity, nil, time.Time{}, reason,
			))
			continue
		}
		future, submitErr := c.Executor.Submit(ctx, req.Policy.Capacity.Timeout, func(jobCtx context.Context) error {
			reported, observedAt, reason := c.pollOneDetailed(jobCtx, item.target, req.Policy.Capacity)
			item.recorded.Store(c.recordCurrent(
				item.key, item.generation, req.Policy.Capacity, reported, observedAt, reason,
			))
			return nil
		})
		if submitErr != nil {
			if ctx.Err() != nil {
				c.abandon(item.key, item.generation)
				for j := i + 1; j < len(work); j++ {
					c.abandon(work[j].key, work[j].generation)
				}
				reconcileErrs = append(reconcileErrs,
					fmt.Errorf("submit capacity poll for cluster %q: %w", item.target.Cluster, submitErr))
				break
			}
			reason := fmt.Sprintf("capacity observation could not be submitted: %v", submitErr)
			item.recorded.Store(c.recordCurrent(
				item.key, item.generation, req.Policy.Capacity, nil, time.Time{}, reason,
			))
			continue
		}
		item.future = future
		accepted = append(accepted, item)
	}

	for _, item := range accepted {
		if waitErr := item.future.Wait(ctx); waitErr != nil && !item.recorded.Load() {
			c.abandon(item.key, item.generation)
			reconcileErrs = append(reconcileErrs,
				fmt.Errorf("wait for capacity poll of cluster %q: %w", item.target.Cluster, waitErr))
		}
	}
	if len(reconcileErrs) != 0 {
		return 0, errors.Join(reconcileErrs...)
	}
	return c.nextDelay(req.OwnerUID, req.Map, targets, req.Policy, c.Clock.Now()), nil
}

func normalizeCapacityTargets(req CapacityReconcileRequest) ([]Target, error) {
	targets := make([]Target, len(req.Targets))
	seen := make(map[string]struct{}, len(req.Targets))
	for i, target := range req.Targets {
		if target.OwnerUID == "" {
			return nil, fmt.Errorf("capacity target cluster %q has an empty owner UID", target.Cluster)
		}
		if target.OwnerUID != req.OwnerUID {
			return nil, fmt.Errorf("capacity target cluster %q has owner UID %q, want %q",
				target.Cluster, target.OwnerUID, req.OwnerUID)
		}
		if target.Map.Namespace == "" || target.Map.Name == "" {
			return nil, fmt.Errorf("capacity target cluster %q has an empty TrafficMap identity", target.Cluster)
		}
		if target.Map != req.Map {
			return nil, fmt.Errorf("capacity target cluster %q belongs to TrafficMap %q, want %q",
				target.Cluster, target.Map.String(), req.Map.String())
		}
		if target.Cluster == "" {
			return nil, errors.New("capacity target cluster is empty")
		}
		if _, found := seen[target.Cluster]; found {
			return nil, fmt.Errorf("capacity target cluster %q is duplicated", target.Cluster)
		}
		seen[target.Cluster] = struct{}{}
		endpoint, err := CanonicalEndpoint(target.URL)
		if err != nil {
			return nil, fmt.Errorf("capacity target cluster %q: %w", target.Cluster, err)
		}
		target.OwnerUID = req.OwnerUID
		target.Map = req.Map
		target.URL = endpoint
		targets[i] = target
	}
	slices.SortFunc(targets, func(a, b Target) int {
		if a.Cluster < b.Cluster {
			return -1
		}
		if a.Cluster > b.Cluster {
			return 1
		}
		return 0
	})
	return targets, nil
}

// pollOneDetailed fetches and validates one cluster's capacity report. It
// returns the accepted count with the time it was observed, or nil plus the
// reason the plan stands instead.
func (c *CapacityPoller) pollOneDetailed(
	ctx context.Context,
	tgt Target,
	config CapacityConfig,
) (*float64, time.Time, string) {
	config = cloneCapacityConfig(config)
	capacityURL, err := capacityURL(tgt, config)
	if err != nil {
		return nil, time.Time{}, fmt.Sprintf("capacity request URL is invalid: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, config.Method, capacityURL, nil)
	if err != nil {
		return nil, time.Time{}, fmt.Sprintf("capacity request could not be built: %v", err)
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, time.Time{}, fmt.Sprintf("capacity endpoint unreachable: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, time.Time{}, fmt.Sprintf("capacity endpoint returned HTTP %d", resp.StatusCode)
	}
	body, err := readCapacityBody(resp.Body, c.MaxResponseBytes)
	if err != nil {
		return nil, time.Time{}, fmt.Sprintf("reading capacity response: %v", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, time.Time{}, fmt.Sprintf("capacity request did not complete: %v", err)
	}
	now := c.Clock.Now()
	report, reason := config.decode(body, now)
	if reason != "" {
		return nil, time.Time{}, reason
	}
	if err := ctx.Err(); err != nil {
		return nil, time.Time{}, fmt.Sprintf("capacity request did not complete: %v", err)
	}
	var v float64
	if config.Mode == v1beta1.RoutingCapacityModeReported {
		if report.Weight == nil {
			return nil, time.Time{}, "capacity report has no weight"
		}
		v = *report.Weight
	} else {
		if report.Servable == nil {
			return nil, time.Time{}, "capacity report has no servable count"
		}
		v = float64(*report.Servable)
	}
	if math.IsNaN(v) || math.IsInf(v, 0) || v > math.MaxInt32 {
		return nil, time.Time{}, "capacity report must be finite and no greater than int32 maximum"
	}
	if v < 0 {
		// A negative value is malformed, not an explicit zero-capacity report.
		return nil, time.Time{}, fmt.Sprintf("capacity report is negative (%v)", v)
	}
	if report.ObservedAt.After(now) {
		return nil, time.Time{}, "capacity report timestamp is in the future"
	}
	if age := now.Sub(report.ObservedAt); age >= config.MaxAge {
		return nil, time.Time{}, capacityStaleReason(config.MaxAge)
	}
	return &v, report.ObservedAt, ""
}

func capacityStaleReason(maxAge time.Duration) string {
	return fmt.Sprintf("capacity report is stale (at least configured maximum age %s)", maxAge)
}

func readCapacityBody(body io.Reader, maxResponseBytes int64) ([]byte, error) {
	if maxResponseBytes <= 0 {
		return nil, fmt.Errorf("maximum response size must be positive")
	}
	readLimit := maxResponseBytes
	if readLimit < math.MaxInt64 {
		readLimit++
	}
	b, err := io.ReadAll(io.LimitReader(body, readLimit))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > maxResponseBytes {
		return nil, fmt.Errorf("response exceeds configured limit of %d bytes", maxResponseBytes)
	}
	return b, nil
}

func capacityURL(tgt Target, config CapacityConfig) (string, error) {
	endpoint, err := CanonicalEndpoint(tgt.URL)
	if err != nil {
		return "", err
	}
	capacityURL, err := url.JoinPath(endpoint, config.Path)
	if err != nil {
		return "", fmt.Errorf("join endpoint and capacity path: %w", err)
	}
	return capacityURL, nil
}

func (c *CapacityPoller) keyFor(tgt Target, policy ResolvedCapacityPolicy) (capacityKey, error) {
	if !policy.IsEnabled() || policy.PolicyDigest == "" {
		return capacityKey{}, errors.New("capacity policy is disabled or has no digest")
	}
	wantDigest, err := capacityPolicyDigest(policy.Capacity)
	if err != nil {
		return capacityKey{}, err
	}
	if policy.PolicyDigest != wantDigest {
		return capacityKey{}, fmt.Errorf("capacity policy digest %q does not match effective policy", policy.PolicyDigest)
	}
	endpoint, err := CanonicalEndpoint(tgt.URL)
	if err != nil {
		return capacityKey{}, err
	}
	return capacityKey{
		ownerUID:     tgt.OwnerUID,
		mapKey:       tgt.Map,
		cluster:      tgt.Cluster,
		url:          endpoint,
		policyDigest: policy.PolicyDigest,
	}, nil
}

// decode turns a response body into a report in servable units by handing it to
// the configured format, or explains why it cannot.
func (c CapacityConfig) decode(body []byte, now time.Time) (CapacityReport, string) {
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
		report.ObservedAt = now
	}
	return report, ""
}

// prepare selects the due targets for one TrafficMap. Pruning is scoped to that
// map so independent InferenceServices never discard one another's windows.
func (c *CapacityPoller) prepare(req CapacityReconcileRequest, targets []Target) []capacityWork {
	now := c.Clock.Now()
	live := make(map[capacityKey]struct{}, len(targets))
	work := make([]capacityWork, 0, len(targets))

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, target := range targets {
		key, err := c.keyFor(target, req.Policy)
		if err != nil {
			continue
		}
		live[key] = struct{}{}
		state, found := c.state[key]
		if !found {
			state = &capacityState{
				reason:       "no capacity report yet",
				nextPollTime: now,
			}
			c.state[key] = state
		}
		state.expire(req.Policy.Capacity, now)
		if state.inFlight || state.nextPollTime.After(now) {
			continue
		}
		c.nextToken++
		if c.nextToken == 0 {
			c.nextToken++
		}
		state.generation = c.nextToken
		state.inFlight = true
		state.nextPollTime = advanceCapacityDeadline(state.nextPollTime, now, req.Policy.Capacity.Period)
		work = append(work, capacityWork{
			target:     target,
			key:        key,
			generation: state.generation,
		})
	}
	for key := range c.state {
		if key.mapKey != req.Map {
			continue
		}
		if _, found := live[key]; !found {
			delete(c.state, key)
		}
	}
	return work
}

func advanceCapacityDeadline(deadline, now time.Time, period time.Duration) time.Time {
	if deadline.IsZero() {
		return now.Add(period)
	}
	if deadline.After(now) {
		return deadline
	}
	return deadline.Add((now.Sub(deadline)/period + 1) * period)
}

func (c *CapacityPoller) nextDelay(
	ownerUID types.UID,
	mapKey types.NamespacedName,
	targets []Target,
	policy ResolvedCapacityPolicy,
	now time.Time,
) time.Duration {
	var (
		found bool
		next  time.Time
	)
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, target := range targets {
		key, err := c.keyFor(target, policy)
		if err != nil || key.ownerUID != ownerUID || key.mapKey != mapKey {
			continue
		}
		state, ok := c.state[key]
		if !ok {
			continue
		}
		deadline := state.nextPollTime
		for _, sample := range state.window {
			if !sample.expires.After(now) {
				return 0
			}
			if deadline.IsZero() || sample.expires.Before(deadline) {
				deadline = sample.expires
			}
		}
		if deadline.IsZero() || (found && !deadline.Before(next)) {
			continue
		}
		found = true
		next = deadline
	}
	if !found || !next.After(now) {
		return 0
	}
	return next.Sub(now)
}

// record stores a cluster's outcome without a polling generation. It supports
// synchronous callers and tests; executor jobs use recordCurrent so a late
// predecessor cannot overwrite current state.
func (c *CapacityPoller) record(
	tgt Target,
	policy ResolvedCapacityPolicy,
	reported *float64,
	reason string,
) bool {
	if c == nil || c.Clock == nil {
		return false
	}
	return c.recordAt(tgt, policy, reported, c.Clock.Now(), reason)
}

func (c *CapacityPoller) recordAt(
	tgt Target,
	policy ResolvedCapacityPolicy,
	reported *float64,
	observedAt time.Time,
	reason string,
) bool {
	if c == nil || c.Clock == nil {
		return false
	}
	k, err := c.keyFor(tgt, policy)
	if err != nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.state[k]
	if !ok {
		st = &capacityState{}
		c.state[k] = st
	}
	return c.recordLocked(st, policy.Capacity, reported, observedAt, reason, c.Clock.Now())
}

func (c *CapacityPoller) recordCurrent(
	k capacityKey,
	generation uint64,
	config CapacityConfig,
	reported *float64,
	observedAt time.Time,
	reason string,
) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.state[k]
	if !ok || !st.inFlight || st.generation != generation {
		return false
	}
	st.inFlight = false
	c.recordLocked(st, config, reported, observedAt, reason, c.Clock.Now())
	return true
}

// recordLocked folds one poll's outcome into the cluster's state under the
// poller's lock and reports whether the applied value or the reason moved.
func (c *CapacityPoller) recordLocked(
	st *capacityState,
	config CapacityConfig,
	reported *float64,
	observedAt time.Time,
	reason string,
	now time.Time,
) bool {
	previousReported := st.reported
	previousReason := st.reason
	reported, observedAt, reason = st.admitReport(config, reported, observedAt, reason, now)
	var applied *float64
	switch {
	case reported != nil:
		applied, reason = st.acceptReport(*reported, observedAt.Add(config.MaxAge), config, reason)
	case config.Mode == v1beta1.RoutingCapacityModeReported:
		applied, reason = st.keepFreshEstimate(config, reason, now)
	default:
		applied, reason = st.releaseCeiling(reason)
	}
	changed := !sameReport(previousReported, applied) || previousReason != reason
	st.reported = applied
	st.reason = reason
	return changed
}

// admitReport is the report as the record admits it: one without a timestamp
// was observed now; one already past the maximum age is dropped as stale,
// with that as the reason; a fresh one first drops the samples past their
// age.
func (st *capacityState) admitReport(config CapacityConfig, reported *float64, observedAt time.Time, reason string, now time.Time) (*float64, time.Time, string) {
	if reported == nil {
		return nil, observedAt, reason
	}
	if observedAt.IsZero() {
		observedAt = now
	}
	if !observedAt.Add(config.MaxAge).After(now) {
		return nil, observedAt, capacityStaleReason(config.MaxAge)
	}
	st.expire(config, now)
	return reported, observedAt, reason
}

// acceptReport joins a fresh report to the window. At quorum the estimate is
// applied and any reason the poll gave is cleared; below it the poll's
// reason stands, else the state says the window awaits quorum.
func (st *capacityState) acceptReport(v float64, expires time.Time, config CapacityConfig, reason string) (*float64, string) {
	if applied, ok := st.observe(v, expires, config); ok {
		return &applied, ""
	}
	if reason == "" {
		reason = fmt.Sprintf("capacity report awaiting quorum (%d/%d accepted samples)", len(st.window), config.quorum())
	}
	return nil, reason
}

// keepFreshEstimate is a failed poll in Reported mode. A failed poll cannot
// refresh a report: the samples inside their age bound still stand and their
// estimate stays applied; once they expire the cluster keeps its replica
// count.
func (st *capacityState) keepFreshEstimate(config CapacityConfig, reason string, now time.Time) (*float64, string) {
	st.expire(config, now)
	var applied *float64
	if v, ok := st.estimate(config); ok {
		applied = &v
	}
	return applied, reasonOrUnavailable(reason)
}

// releaseCeiling is a failed poll in Ceiling mode: the ceiling falls open and
// the window goes with it, since a reading kept would constrain a cluster the
// poller can no longer observe, the opposite of failing open.
func (st *capacityState) releaseCeiling(reason string) (*float64, string) {
	st.window = nil
	return nil, reasonOrUnavailable(reason)
}

// reasonOrUnavailable is the poll's reason where it gave one, else the plain
// statement that no report is available.
func reasonOrUnavailable(reason string) string {
	if reason == "" {
		return "capacity report unavailable"
	}
	return reason
}

func (c *CapacityPoller) abandon(key capacityKey, generation uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, found := c.state[key]
	if !found || !state.inFlight || state.generation != generation {
		return
	}
	state.inFlight = false
	state.nextPollTime = c.Clock.Now()
}

// sameReport compares two optional values, treating nil as its own value so a
// cluster that stops reporting is a change rather than a no-op.
func sameReport(a, b *float64) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}
