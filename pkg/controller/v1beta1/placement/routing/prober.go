package routing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Target identifies one observed serving cluster and its externally addressable
// endpoint. OwnerUID prevents a recreated InferenceService from inheriting
// observations belonging to the previous object at the same name.
type Target struct {
	OwnerUID     types.UID
	Map          types.NamespacedName
	Cluster      string
	URL          string
	PolicyDigest string
}

// ProbeReconcileRequest is the complete probe input for one InferenceService.
// Persisted is optional and is considered only when initializing state.
type ProbeReconcileRequest struct {
	OwnerUID  types.UID
	Map       types.NamespacedName
	Policy    ResolvedProbePolicy
	Targets   []Target
	Persisted *v1beta1.TrafficMap
}

type probeIdentity struct {
	OwnerUID     types.UID
	Map          types.NamespacedName
	Cluster      string
	Endpoint     string
	PolicyDigest string
}

// targetProbeState is the prober's running state for one exact target identity.
type targetProbeState struct {
	identity probeIdentity

	gated               bool
	consecutiveFailures int
	consecutivePasses   int
	hasConclusiveResult bool
	lastResult          v1beta1.ProbeResult
	lastMessage         string
	lastProbeTime       time.Time
	observed            bool

	failureThreshold int
	successThreshold int
	nextProbeTime    time.Time
	inFlight         bool
	generation       uint64
}

// Prober runs due endpoint probes through the shared ObserverExecutor. The
// routing reconciler owns scheduling: each call submits all due targets, waits
// for their bounded work, records the results, and receives the delay until the
// next absolute deadline.
type Prober struct {
	Executor         *ObserverExecutor
	Client           *http.Client
	Clock            clock.Clock
	MaxResponseBytes int64

	mu        sync.RWMutex
	state     map[probeIdentity]*targetProbeState
	nextToken uint64
}

// checkObserverInputs refuses a missing executor, HTTP client or clock and a
// response bound that is not positive; kind names the observer in the message.
func checkObserverInputs(kind string, executor *ObserverExecutor, client *http.Client, clk clock.Clock, maxResponseBytes int64) error {
	if executor == nil {
		return fmt.Errorf("%s observer executor is nil", kind)
	}
	if client == nil {
		return fmt.Errorf("%s HTTP client is nil", kind)
	}
	if clk == nil {
		return fmt.Errorf("%s clock is nil", kind)
	}
	if maxResponseBytes <= 0 {
		return fmt.Errorf("%s maximum response bytes must be positive, got %d", kind, maxResponseBytes)
	}
	return nil
}

// NewProber builds a reconcile-driven prober with explicit process-wide
// execution and response-size bounds.
func NewProber(
	executor *ObserverExecutor,
	client *http.Client,
	clk clock.Clock,
	maxResponseBytes int64,
) (*Prober, error) {
	if err := checkObserverInputs("probe", executor, client, clk, maxResponseBytes); err != nil {
		return nil, err
	}
	return &Prober{
		Executor:         executor,
		Client:           client,
		Clock:            clk,
		MaxResponseBytes: maxResponseBytes,
		state:            map[probeIdentity]*targetProbeState{},
	}, nil
}

// NewObserverClient builds the HTTP client shared by routing observers.
// Redirects are returned to the caller for policy classification instead of
// following an untrusted endpoint to a different authority.
func NewObserverClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// Reconcile synchronously runs every due probe for one InferenceService and
// returns the delay until its earliest absolute probe deadline. A zero delay
// with an enabled non-empty request means the deadline is already due.
func (p *Prober) Reconcile(ctx context.Context, req ProbeReconcileRequest) (time.Duration, error) {
	if err := p.checkReconcileInputs(ctx, req); err != nil {
		return 0, err
	}
	if !req.Policy.IsEnabled() {
		p.Forget(req.Map)
		return 0, nil
	}
	if err := checkProbePolicy(req.Policy); err != nil {
		return 0, err
	}
	targets, err := normalizeProbeTargets(req)
	if err != nil {
		return 0, err
	}
	accepted, submitErr := p.submitProbes(ctx, req, p.prepare(req, targets))
	var reconcileErrs []error
	if submitErr != nil {
		reconcileErrs = append(reconcileErrs, submitErr)
	}
	reconcileErrs = append(reconcileErrs, p.awaitProbes(ctx, accepted)...)
	if len(reconcileErrs) != 0 {
		return 0, errors.Join(reconcileErrs...)
	}
	return p.nextDelay(req.OwnerUID, req.Map, targets, p.Clock.Now()), nil
}

// checkReconcileInputs refuses a nil prober or context and a request that
// does not name its owner and TrafficMap.
func (p *Prober) checkReconcileInputs(ctx context.Context, req ProbeReconcileRequest) error {
	if p == nil {
		return errors.New("prober is nil")
	}
	if ctx == nil {
		return errors.New("probe reconcile context is nil")
	}
	if req.OwnerUID == "" {
		return errors.New("probe reconcile owner UID is empty")
	}
	if req.Map.Namespace == "" || req.Map.Name == "" {
		return errors.New("probe reconcile TrafficMap namespace and name are required")
	}
	return nil
}

// checkProbePolicy refuses an enabled policy whose probe configuration is
// invalid or whose recorded digest is not the digest of that configuration.
func checkProbePolicy(policy ResolvedProbePolicy) error {
	if err := policy.Probe.Validate(); err != nil {
		return err
	}
	wantDigest, err := probePolicyDigest(policy.Probe)
	if err != nil {
		return err
	}
	if policy.PolicyDigest != wantDigest {
		return fmt.Errorf("probe policy digest %q does not match effective policy", policy.PolicyDigest)
	}
	return nil
}

// submitProbes hands every prepared probe to the executor, in order, and
// returns the accepted ones. The first refusal ends the pass: that probe and
// every later one are released so the next pass may probe them again.
func (p *Prober) submitProbes(ctx context.Context, req ProbeReconcileRequest, jobs []scheduledProbe) ([]*scheduledProbe, error) {
	accepted := make([]*scheduledProbe, 0, len(jobs))
	for i := range jobs {
		job := &jobs[i]
		future, submitErr := p.Executor.Submit(ctx, req.Policy.Probe.Timeout, func(jobCtx context.Context) error {
			outcome, message := p.probeOne(jobCtx, job.target, req.Policy.Probe)
			job.recorded.Store(p.record(job.identity, job.generation, outcome, message, p.Clock.Now()))
			return nil
		})
		if submitErr != nil {
			for j := i; j < len(jobs); j++ {
				p.abandon(jobs[j].identity, jobs[j].generation)
			}
			return accepted, fmt.Errorf("submit probe for cluster %q: %w", job.target.Cluster, submitErr)
		}
		job.future = future
		accepted = append(accepted, job)
	}
	return accepted, nil
}

// awaitProbes waits for every accepted probe. A probe whose wait ends before
// its result was recorded is released so the next pass may probe it again,
// and the wait's error is reported.
func (p *Prober) awaitProbes(ctx context.Context, accepted []*scheduledProbe) []error {
	var waitErrs []error
	for _, job := range accepted {
		if waitErr := job.future.Wait(ctx); waitErr != nil && !job.recorded.Load() {
			p.abandon(job.identity, job.generation)
			waitErrs = append(waitErrs, fmt.Errorf("wait for probe of cluster %q: %w", job.target.Cluster, waitErr))
		}
	}
	return waitErrs
}

type scheduledProbe struct {
	target     Target
	identity   probeIdentity
	generation uint64
	future     *ObserverFuture
	recorded   atomic.Bool
}

func (p *Prober) prepare(req ProbeReconcileRequest, targets []Target) []scheduledProbe {
	now := p.Clock.Now()
	live := make(map[probeIdentity]struct{}, len(targets))
	jobs := make([]scheduledProbe, 0, len(targets))

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, target := range targets {
		identity := identityForTarget(target)
		live[identity] = struct{}{}
		state, found := p.state[identity]
		if !found {
			state = &targetProbeState{
				identity:         identity,
				lastResult:       v1beta1.ProbeResultUnknown,
				failureThreshold: req.Policy.Probe.FailureThreshold,
				successThreshold: req.Policy.Probe.SuccessThreshold,
				nextProbeTime:    now,
			}
			p.hydrate(state, req.Persisted, req.Policy.Probe.Period, now)
			p.state[identity] = state
		}
		if state.inFlight || state.nextProbeTime.After(now) {
			continue
		}
		p.nextToken++
		if p.nextToken == 0 {
			p.nextToken++
		}
		state.generation = p.nextToken
		state.inFlight = true
		state.nextProbeTime = advanceDeadline(state.nextProbeTime, now, req.Policy.Probe.Period)
		jobs = append(jobs, scheduledProbe{
			target:     target,
			identity:   identity,
			generation: state.generation,
		})
	}
	for identity := range p.state {
		if identity.Map != req.Map {
			continue
		}
		if _, found := live[identity]; !found {
			delete(p.state, identity)
		}
	}
	return jobs
}

// normalizeProbeTargets checks the request's targets by the rules both
// observers share and by the probe's own: every target carries the digest of
// the policy the request resolved.
func normalizeProbeTargets(req ProbeReconcileRequest) ([]Target, error) {
	return normalizeTargets("probe", req.OwnerUID, req.Map, req.Targets, func(target Target) error {
		if target.PolicyDigest == "" {
			return fmt.Errorf("probe target cluster %q has an empty policy digest", target.Cluster)
		}
		if target.PolicyDigest != req.Policy.PolicyDigest {
			return fmt.Errorf("probe target cluster %q has policy digest %q, want %q",
				target.Cluster, target.PolicyDigest, req.Policy.PolicyDigest)
		}
		return nil
	})
}

func (p *Prober) nextDelay(ownerUID types.UID, mapKey types.NamespacedName, targets []Target, now time.Time) time.Duration {
	var (
		found bool
		next  time.Time
	)
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, target := range targets {
		identity := identityForTarget(target)
		if identity.OwnerUID != ownerUID || identity.Map != mapKey {
			continue
		}
		state, ok := p.state[identity]
		if !ok || (found && !state.nextProbeTime.Before(next)) {
			continue
		}
		found = true
		next = state.nextProbeTime
	}
	if !found || !next.After(now) {
		return 0
	}
	return next.Sub(now)
}

// Reachable reports the probe gate for the exact target identity. Nil means no
// conclusive evidence exists for this identity.
func (p *Prober) Reachable(target Target) *bool {
	state := p.lookup(target)
	if state == nil || !state.hasConclusiveResult {
		return nil
	}
	reachable := !state.gated
	return &reachable
}

// Provenance renders the latest observation for the exact target identity.
func (p *Prober) Provenance(target Target) *v1beta1.TrafficMapProbe {
	state := p.lookup(target)
	if state == nil || !state.observed {
		return nil
	}
	probe := &v1beta1.TrafficMapProbe{
		PolicyDigest:        state.identity.PolicyDigest,
		Result:              state.lastResult,
		Gated:               state.gated,
		ConsecutiveFailures: int32(state.consecutiveFailures),
		Message:             state.lastMessage,
	}
	if !state.lastProbeTime.IsZero() {
		timestamp := metav1.NewTime(state.lastProbeTime)
		probe.LastProbeTime = &timestamp
	}
	return probe
}

func (p *Prober) lookup(target Target) *targetProbeState {
	if p == nil || target.PolicyDigest == "" {
		return nil
	}
	endpoint, err := CanonicalEndpoint(target.URL)
	if err != nil {
		return nil
	}
	identity := identityForTarget(target)
	identity.Endpoint = endpoint
	p.mu.RLock()
	defer p.mu.RUnlock()
	state, found := p.state[identity]
	if !found {
		return nil
	}
	copy := *state
	return &copy
}

func identityForTarget(target Target) probeIdentity {
	return probeIdentity{
		OwnerUID:     target.OwnerUID,
		Map:          target.Map,
		Cluster:      target.Cluster,
		Endpoint:     target.URL,
		PolicyDigest: target.PolicyDigest,
	}
}

// Forget removes all probe state for a TrafficMap. An in-flight result carries
// its old identity and generation and cannot recreate forgotten state.
func (p *Prober) Forget(mapKey types.NamespacedName) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for identity := range p.state {
		if identity.Map == mapKey {
			delete(p.state, identity)
		}
	}
}

// probeOne issues one request. Its context deadline is supplied by the
// ObserverExecutor after the job leaves the queue.
func (p *Prober) probeOne(ctx context.Context, target Target, config ProbeConfig) (ProbeOutcome, string) {
	probeURL, err := url.JoinPath(target.URL, config.Path)
	if err != nil {
		return ProbeOutcomeInconclusive, fmt.Sprintf("probe could not run: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, config.Method, probeURL, nil)
	if err != nil {
		return ProbeOutcomeInconclusive, fmt.Sprintf("probe could not run: %v", err)
	}
	response, err := p.Client.Do(req)
	if err != nil {
		return ProbeOutcomeFail, fmt.Sprintf("transport error: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if _, err := io.Copy(io.Discard, io.LimitReader(response.Body, p.MaxResponseBytes)); err != nil {
		return ProbeOutcomeFail, fmt.Sprintf("response body error: %v", err)
	}
	if err := ctx.Err(); err != nil {
		return ProbeOutcomeFail, fmt.Sprintf("transport error: %v", err)
	}

	switch config.ClassifyStatus(response.StatusCode) {
	case ProbeOutcomePass:
		return ProbeOutcomePass, fmt.Sprintf("HTTP %d", response.StatusCode)
	case ProbeOutcomeFail:
		return ProbeOutcomeFail, fmt.Sprintf("HTTP %d", response.StatusCode)
	default:
		return ProbeOutcomeInconclusive, fmt.Sprintf("HTTP %d (not in accept or gate list)", response.StatusCode)
	}
}

func (p *Prober) record(
	identity probeIdentity,
	generation uint64,
	outcome ProbeOutcome,
	message string,
	observedAt time.Time,
) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	state, found := p.state[identity]
	if !found || !state.inFlight || state.generation != generation {
		return false
	}
	state.inFlight = false
	state.observed = true
	state.lastMessage = message
	state.lastProbeTime = observedAt

	switch outcome {
	case ProbeOutcomeInconclusive:
		state.lastResult = v1beta1.ProbeResultUnknown
	case ProbeOutcomeFail:
		state.consecutivePasses = 0
		if state.consecutiveFailures < math.MaxInt32 {
			state.consecutiveFailures++
		}
		state.lastResult = v1beta1.ProbeResultFailing
		state.hasConclusiveResult = true
		if state.consecutiveFailures >= state.failureThreshold {
			state.gated = true
		}
	case ProbeOutcomePass:
		state.consecutiveFailures = 0
		state.lastResult = v1beta1.ProbeResultPassing
		state.hasConclusiveResult = true
		if !state.gated {
			state.consecutivePasses = 0
			break
		}
		if state.consecutivePasses < math.MaxInt32 {
			state.consecutivePasses++
		}
		if state.consecutivePasses >= state.successThreshold {
			state.gated = false
			state.consecutivePasses = 0
		}
	}
	return true
}

func (p *Prober) abandon(identity probeIdentity, generation uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state, found := p.state[identity]
	if !found || !state.inFlight || state.generation != generation {
		return
	}
	state.inFlight = false
	state.nextProbeTime = p.Clock.Now()
}

func (p *Prober) hydrate(state *targetProbeState, persisted *v1beta1.TrafficMap, period time.Duration, now time.Time) {
	probe := persistedProbe(persisted, state.identity)
	if probe == nil {
		return
	}
	state.gated = probe.Gated
	state.consecutiveFailures = max(0, int(probe.ConsecutiveFailures))
	state.consecutivePasses = 0
	state.lastResult = probe.Result
	if state.lastResult == "" {
		state.lastResult = v1beta1.ProbeResultUnknown
	}
	state.hasConclusiveResult = state.gated || state.consecutiveFailures > 0 ||
		state.lastResult == v1beta1.ProbeResultPassing || state.lastResult == v1beta1.ProbeResultFailing
	state.lastMessage = probe.Message
	state.observed = true
	if probe.LastProbeTime != nil {
		state.lastProbeTime = probe.LastProbeTime.Time
		if state.lastProbeTime.After(now) {
			state.nextProbeTime = now
			return
		}
		state.nextProbeTime = state.lastProbeTime.Add(period)
		return
	}
	state.nextProbeTime = now
}

func persistedProbe(persisted *v1beta1.TrafficMap, identity probeIdentity) *v1beta1.TrafficMapProbe {
	if persisted == nil ||
		persisted.Namespace != identity.Map.Namespace || persisted.Name != identity.Map.Name {
		return nil
	}
	ownerUID, owned := trafficMapOwnerUID(persisted)
	if !owned || ownerUID != identity.OwnerUID {
		return nil
	}
	for i := range persisted.Spec.Entries {
		entry := &persisted.Spec.Entries[i]
		if entry.Cluster != identity.Cluster || entry.Endpoint == nil || entry.Probe == nil ||
			entry.Probe.PolicyDigest != identity.PolicyDigest {
			continue
		}
		endpoint, err := CanonicalEndpoint(entry.Endpoint.String())
		if err == nil && endpoint == identity.Endpoint {
			return entry.Probe.DeepCopy()
		}
	}
	return nil
}

// CanonicalEndpoint returns a stable identity for an absolute endpoint URL.
func CanonicalEndpoint(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse endpoint URL: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" || parsed.Opaque != "" {
		return "", fmt.Errorf("endpoint URL %q must be absolute", raw)
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("endpoint URL %q must use HTTP or HTTPS", raw)
	}
	parsed.Host = strings.ToLower(parsed.Host)
	parsed.Fragment = ""
	parsed.RawFragment = ""
	escapedPath := parsed.EscapedPath()
	if escapedPath == "" {
		return parsed.String(), nil
	}
	escapedPath = path.Clean(escapedPath)
	if escapedPath == "." || escapedPath == "/" {
		parsed.Path = ""
		parsed.RawPath = ""
		return parsed.String(), nil
	}
	decodedPath, err := url.PathUnescape(escapedPath)
	if err != nil {
		return "", fmt.Errorf("parse endpoint URL path: %w", err)
	}
	parsed.Path = decodedPath
	parsed.RawPath = escapedPath
	return parsed.String(), nil
}

func trafficMapOwnerUID(trafficMap *v1beta1.TrafficMap) (types.UID, bool) {
	if trafficMap == nil || trafficMap.Spec.Service == "" ||
		trafficMap.Spec.Service != trafficMap.Name {
		return "", false
	}
	owner := metav1.GetControllerOf(trafficMap)
	if owner == nil || owner.UID == "" ||
		owner.APIVersion != v1beta1.SchemeGroupVersion.String() ||
		owner.Kind != "InferenceService" ||
		owner.Name != trafficMap.Name || owner.Name != trafficMap.Spec.Service {
		return "", false
	}
	return owner.UID, true
}
