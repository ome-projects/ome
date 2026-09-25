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

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
)

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

type capacitySample struct {
	servable int32
	expires  time.Time
}

// capacityState is the poller's running view of one home.
type capacityState struct {
	// generation identifies the polling pass allowed to update this state. A
	// completion from an older pass cannot overwrite a newer observation.
	generation uint64
	// window holds the most recent accepted readings, oldest first. The applied
	// ceiling is their Quorum-th smallest, so no single reading -- however low
	// -- can set it alone.
	window []capacitySample
	// reported is the applied ceiling, or nil when the home has not answered
	// usably. Nil means the control-plane plan stands.
	reported *int32
	// reason explains why reported is nil, for the fail-open condition. It is
	// empty only while an applied ceiling exists.
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

// observe folds one accepted reading into the window and returns the resulting
// ceiling -- the quorum-th smallest retained reading -- and whether one applies
// at all.
//
// Until the window holds quorum readings, none does: the plan stands. A ceiling
// derived from fewer readings than the quorum would be exactly the thing the
// quorum exists to prevent, since the first reading after startup or recovery
// could come from the one broken reporter and would set the ceiling with
// nothing to corroborate it.
func (st *capacityState) observe(v int32, expires time.Time, samples, quorum int) (int32, bool) {
	st.window = append(st.window, capacitySample{servable: v, expires: expires})
	if len(st.window) > samples {
		st.window = st.window[len(st.window)-samples:]
	}
	return st.ceiling(quorum)
}

func (st *capacityState) ceiling(quorum int) (int32, bool) {
	if len(st.window) < quorum {
		return 0, false
	}
	sorted := make([]int32, len(st.window))
	for i := range st.window {
		sorted[i] = st.window[i].servable
	}
	slices.Sort(sorted)
	return sorted[quorum-1], true
}

// expire removes evidence that has reached its producer timestamp's maximum
// age and recomputes the ceiling from the remaining fresh samples. Samples can
// arrive with out-of-order timestamps, so every entry is checked.
func (st *capacityState) expire(config CapacityConfig, now time.Time) bool {
	kept := st.window[:0]
	for _, sample := range st.window {
		if sample.expires.After(now) {
			kept = append(kept, sample)
		}
	}
	if len(kept) == len(st.window) {
		return false
	}
	st.window = kept
	if v, ok := st.ceiling(config.quorum()); ok {
		st.reported = &v
		st.reason = ""
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
	return true
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
	Log    logr.Logger
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
	log logr.Logger,
) (*CapacityPoller, error) {
	if executor == nil {
		return nil, errors.New("capacity observer executor is nil")
	}
	if client == nil {
		return nil, errors.New("capacity HTTP client is nil")
	}
	if clk == nil {
		return nil, errors.New("capacity clock is nil")
	}
	if maxResponseBytes <= 0 {
		return nil, fmt.Errorf("capacity maximum response bytes must be positive, got %d", maxResponseBytes)
	}
	return &CapacityPoller{
		Log:              log,
		Client:           client,
		Clock:            clk,
		Executor:         executor,
		MaxResponseBytes: maxResponseBytes,
		state:            map[capacityKey]*capacityState{},
	}, nil
}

// Reported returns the home's last accepted servable count, or nil when the
// plan should stand — polling off, no answer yet, or the last answer was
// unusable. Nil is the fail-open value.
func (c *CapacityPoller) Reported(tgt Target, policy ResolvedCapacityPolicy) *int32 {
	reported, _ := c.observation(tgt, policy)
	return reported
}

// FellOpen reports why a home's report was not applied, or empty when it was
// applied or polling is off.
//
// Silent fallback is as bad as fail-closed for an operator: the weight looks
// the same whether the home agreed with the plan or the poll failed, and those
// have opposite remediations.
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
) (*int32, string) {
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
	return &v, ""
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

// pollOne fetches and validates one home's capacity report. It returns the
// accepted count, or nil plus the reason the plan stands instead.
func (c *CapacityPoller) pollOne(
	ctx context.Context,
	tgt Target,
	config CapacityConfig,
) (*int32, string) {
	reported, _, reason := c.pollOneDetailed(ctx, tgt, config)
	return reported, reason
}

func (c *CapacityPoller) pollOneDetailed(
	ctx context.Context,
	tgt Target,
	config CapacityConfig,
) (*int32, time.Time, string) {
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
	if report.Servable < 0 {
		// Malformed, not an instruction to serve nothing. A parse or arithmetic
		// bug on the home must not be able to black-hole it.
		return nil, time.Time{}, fmt.Sprintf("capacity report is negative (%d)", report.Servable)
	}
	if report.ObservedAt.After(now) {
		return nil, time.Time{}, "capacity report timestamp is in the future"
	}
	if age := now.Sub(report.ObservedAt); age >= config.MaxAge {
		return nil, time.Time{}, capacityStaleReason(config.MaxAge)
	}
	v := report.Servable
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

// record stores a home's outcome without a polling generation. It supports
// synchronous callers and tests; executor jobs use recordCurrent so a late
// predecessor cannot overwrite current state.
func (c *CapacityPoller) record(
	tgt Target,
	policy ResolvedCapacityPolicy,
	reported *int32,
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
	reported *int32,
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
	reported *int32,
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

func (c *CapacityPoller) recordLocked(
	st *capacityState,
	config CapacityConfig,
	reported *int32,
	observedAt time.Time,
	reason string,
	now time.Time,
) bool {
	previousReported := st.reported
	previousReason := st.reason
	var applied *int32
	if reported != nil {
		if observedAt.IsZero() {
			observedAt = now
		}
		expires := observedAt.Add(config.MaxAge)
		if !expires.After(now) {
			reported = nil
			reason = capacityStaleReason(config.MaxAge)
		} else {
			st.expire(config, now)
		}
	}
	if reported != nil {
		if v, ok := st.observe(*reported, observedAt.Add(config.MaxAge), config.samples(), config.quorum()); ok {
			applied = &v
			reason = ""
		} else if reason == "" {
			reason = fmt.Sprintf("capacity report awaiting quorum (%d/%d accepted samples)",
				len(st.window), config.quorum())
		}
	} else {
		// A failed poll falls open and releases the ceiling, so the window must
		// not survive it: retaining readings would keep constraining a home we
		// can no longer observe, which is the opposite of failing open.
		st.window = nil
		if reason == "" {
			reason = "capacity report unavailable"
		}
	}
	changed := !sameReport(previousReported, applied) || previousReason != reason
	st.reported = applied
	st.reason = reason
	return changed
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
