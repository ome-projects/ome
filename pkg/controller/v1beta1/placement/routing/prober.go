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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Target identifies one observed serving home and its externally addressable
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

// homeProbeState is the prober's running verdict for one exact target identity.
type homeProbeState struct {
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
	Log              logr.Logger

	mu        sync.RWMutex
	state     map[probeIdentity]*homeProbeState
	nextToken uint64
}

// NewProber builds a reconcile-driven prober with explicit process-wide
// execution and response-size bounds.
func NewProber(
	executor *ObserverExecutor,
	client *http.Client,
	clk clock.Clock,
	maxResponseBytes int64,
	log logr.Logger,
) (*Prober, error) {
	if executor == nil {
		return nil, errors.New("probe observer executor is nil")
	}
	if client == nil {
		return nil, errors.New("probe HTTP client is nil")
	}
	if clk == nil {
		return nil, errors.New("probe clock is nil")
	}
	if maxResponseBytes <= 0 {
		return nil, fmt.Errorf("probe maximum response bytes must be positive, got %d", maxResponseBytes)
	}
	return &Prober{
		Executor:         executor,
		Client:           client,
		Clock:            clk,
		MaxResponseBytes: maxResponseBytes,
		Log:              log,
		state:            map[probeIdentity]*homeProbeState{},
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
	if p == nil {
		return 0, errors.New("prober is nil")
	}
	if ctx == nil {
		return 0, errors.New("probe reconcile context is nil")
	}
	if req.OwnerUID == "" {
		return 0, errors.New("probe reconcile owner UID is empty")
	}
	if req.Map.Namespace == "" || req.Map.Name == "" {
		return 0, errors.New("probe reconcile TrafficMap namespace and name are required")
	}
	if !req.Policy.IsEnabled() {
		p.Forget(req.Map)
		return 0, nil
	}
	if err := req.Policy.Probe.Validate(); err != nil {
		return 0, err
	}
	wantDigest, err := probePolicyDigest(req.Policy.Probe)
	if err != nil {
		return 0, err
	}
	if req.Policy.PolicyDigest != wantDigest {
		return 0, fmt.Errorf("probe policy digest %q does not match effective policy", req.Policy.PolicyDigest)
	}

	targets, err := normalizeProbeTargets(req)
	if err != nil {
		return 0, err
	}
	jobs := p.prepare(req, targets)
	var reconcileErrs []error
	accepted := make([]*scheduledProbe, 0, len(jobs))
	for i := range jobs {
		job := &jobs[i]
		future, submitErr := p.Executor.Submit(ctx, req.Policy.Probe.Timeout, func(jobCtx context.Context) error {
			verdict, message := p.probeOne(jobCtx, job.target, req.Policy.Probe)
			job.recorded.Store(p.record(job.identity, job.generation, verdict, message, p.Clock.Now()))
			return nil
		})
		if submitErr != nil {
			p.abandon(job.identity, job.generation)
			reconcileErrs = append(reconcileErrs,
				fmt.Errorf("submit probe for cluster %q: %w", job.target.Cluster, submitErr))
			for j := i + 1; j < len(jobs); j++ {
				p.abandon(jobs[j].identity, jobs[j].generation)
			}
			break
		}
		job.future = future
		accepted = append(accepted, job)
	}

	for _, job := range accepted {
		if waitErr := job.future.Wait(ctx); waitErr != nil && !job.recorded.Load() {
			p.abandon(job.identity, job.generation)
			reconcileErrs = append(reconcileErrs,
				fmt.Errorf("wait for probe of cluster %q: %w", job.target.Cluster, waitErr))
		}
	}
	if len(reconcileErrs) != 0 {
		return 0, errors.Join(reconcileErrs...)
	}
	return p.nextDelay(req.OwnerUID, req.Map, targets, p.Clock.Now()), nil
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
			state = &homeProbeState{
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
		state.nextProbeTime = advanceProbeDeadline(state.nextProbeTime, now, req.Policy.Probe.Period)
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

func normalizeProbeTargets(req ProbeReconcileRequest) ([]Target, error) {
	targets := make([]Target, len(req.Targets))
	seen := make(map[string]struct{}, len(req.Targets))
	for i, target := range req.Targets {
		if target.OwnerUID == "" {
			return nil, fmt.Errorf("probe target cluster %q has an empty owner UID", target.Cluster)
		}
		if target.OwnerUID != req.OwnerUID {
			return nil, fmt.Errorf("probe target cluster %q has owner UID %q, want %q",
				target.Cluster, target.OwnerUID, req.OwnerUID)
		}
		if target.Map.Namespace == "" || target.Map.Name == "" {
			return nil, fmt.Errorf("probe target cluster %q has an empty TrafficMap identity", target.Cluster)
		}
		if target.Map != req.Map {
			return nil, fmt.Errorf("probe target cluster %q belongs to TrafficMap %q, want %q",
				target.Cluster, target.Map.String(), req.Map.String())
		}
		if target.PolicyDigest == "" {
			return nil, fmt.Errorf("probe target cluster %q has an empty policy digest", target.Cluster)
		}
		if target.PolicyDigest != req.Policy.PolicyDigest {
			return nil, fmt.Errorf("probe target cluster %q has policy digest %q, want %q",
				target.Cluster, target.PolicyDigest, req.Policy.PolicyDigest)
		}
		if target.Cluster == "" {
			return nil, errors.New("probe target cluster is empty")
		}
		if _, found := seen[target.Cluster]; found {
			return nil, fmt.Errorf("probe target cluster %q is duplicated", target.Cluster)
		}
		seen[target.Cluster] = struct{}{}
		endpoint, err := CanonicalEndpoint(target.URL)
		if err != nil {
			return nil, fmt.Errorf("probe target cluster %q: %w", target.Cluster, err)
		}
		target.OwnerUID = req.OwnerUID
		target.Map = req.Map
		target.URL = endpoint
		target.PolicyDigest = req.Policy.PolicyDigest
		targets[i] = target
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].Cluster < targets[j].Cluster })
	return targets, nil
}

func advanceProbeDeadline(deadline, now time.Time, period time.Duration) time.Time {
	if deadline.IsZero() {
		return now.Add(period)
	}
	if deadline.After(now) {
		return deadline
	}
	return deadline.Add((now.Sub(deadline)/period + 1) * period)
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

func (p *Prober) lookup(target Target) *homeProbeState {
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
func (p *Prober) probeOne(ctx context.Context, target Target, config ProbeConfig) (Verdict, string) {
	probeURL, err := url.JoinPath(target.URL, config.Path)
	if err != nil {
		return VerdictInconclusive, fmt.Sprintf("probe could not run: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, config.Method, probeURL, nil)
	if err != nil {
		return VerdictInconclusive, fmt.Sprintf("probe could not run: %v", err)
	}
	response, err := p.Client.Do(req)
	if err != nil {
		return VerdictFail, fmt.Sprintf("transport error: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if _, err := io.Copy(io.Discard, io.LimitReader(response.Body, p.MaxResponseBytes)); err != nil {
		return VerdictFail, fmt.Sprintf("response body error: %v", err)
	}
	if err := ctx.Err(); err != nil {
		return VerdictFail, fmt.Sprintf("transport error: %v", err)
	}

	switch verdict := config.ClassifyStatus(response.StatusCode); verdict {
	case VerdictPass:
		return VerdictPass, fmt.Sprintf("HTTP %d", response.StatusCode)
	case VerdictFail:
		return VerdictFail, fmt.Sprintf("HTTP %d", response.StatusCode)
	default:
		return VerdictInconclusive, fmt.Sprintf("HTTP %d (not in accept or gate list)", response.StatusCode)
	}
}

func (p *Prober) record(
	identity probeIdentity,
	generation uint64,
	verdict Verdict,
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

	switch verdict {
	case VerdictInconclusive:
		state.lastResult = v1beta1.ProbeResultUnknown
	case VerdictFail:
		state.consecutivePasses = 0
		if state.consecutiveFailures < math.MaxInt32 {
			state.consecutiveFailures++
		}
		state.lastResult = v1beta1.ProbeResultFailing
		state.hasConclusiveResult = true
		if state.consecutiveFailures >= state.failureThreshold {
			state.gated = true
		}
	case VerdictPass:
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

func (p *Prober) hydrate(state *homeProbeState, persisted *v1beta1.TrafficMap, period time.Duration, now time.Time) {
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

// TargetsFromTrafficMaps builds the observer target list from addressable
// TrafficMap entries.
func TargetsFromTrafficMaps(maps []v1beta1.TrafficMap) []Target {
	var targets []Target
	for i := range maps {
		trafficMap := &maps[i]
		ownerUID, owned := trafficMapOwnerUID(trafficMap)
		if !owned {
			continue
		}
		for j := range trafficMap.Spec.Entries {
			entry := &trafficMap.Spec.Entries[j]
			if entry.Endpoint == nil || entry.Endpoint.Host == "" {
				continue
			}
			endpoint := entry.Endpoint.String()
			if canonical, err := CanonicalEndpoint(endpoint); err == nil {
				endpoint = canonical
			}
			var policyDigest string
			if entry.Probe != nil {
				policyDigest = entry.Probe.PolicyDigest
			}
			targets = append(targets, Target{
				OwnerUID:     ownerUID,
				Map:          types.NamespacedName{Name: trafficMap.Name, Namespace: trafficMap.Namespace},
				Cluster:      entry.Cluster,
				URL:          endpoint,
				PolicyDigest: policyDigest,
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
