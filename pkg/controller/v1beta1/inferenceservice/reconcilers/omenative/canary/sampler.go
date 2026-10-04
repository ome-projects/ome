package canary

import (
	"context"
	"fmt"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
)

// SampleKey identifies one analysis sampling target: a specific step of a
// specific canary revision of one Component, under a specific effective analysis
// request (PlanHash). PlanHash fingerprints the checks plus the resolved source
// (address, headers, auth), so editing any of them changes the key: the edited
// plan starts a fresh query instead of consuming a cached or in-flight result
// produced under the old configuration. Old keys (a finished step/revision, or
// a superseded plan) stop being read and age out of the cache.
type SampleKey struct {
	Namespace string
	ISVCName  string
	Component string
	Revision  string
	Step      int32
	PlanHash  string
}

// SampleRequest is a fully-snapshotted query the background eval runs WITHOUT
// touching the live InferenceService (which the reconcile owns — a goroutine
// reading it would race). The reconcile resolves the source address, bearer
// token, and template context up front; the eval only performs the HTTP query.
type SampleRequest struct {
	Key             SampleKey
	ServerAddress   string
	BearerToken     string
	Headers         map[string]string
	TemplateContext analysis.TemplateContext
	Analysis        *v1beta1.RolloutAnalysis // the step's checks (metrics) — read-only in eval
	QueryTimeout    time.Duration
}

// evalFunc runs the (slow) query for one request. Injected so tests exercise the
// cache/dedup/eviction without real HTTP; production is prometheusEval.
type evalFunc func(ctx context.Context, req SampleRequest) analysis.Result

// Sampler decouples Prometheus querying from the reconcile loop. Get is a
// non-blocking cache read that, on a miss, kicks a bounded background query and
// returns immediately; when the query lands it caches the result and emits a
// controller event so the ISVC re-reconciles and consumes it. A slow or hung
// Prometheus therefore never blocks a reconcile worker — only a background
// goroutine, capped by the semaphore.
type Sampler struct {
	eval   evalFunc
	events chan<- event.GenericEvent
	sem    chan struct{} // caps concurrent background queries
	ttl    time.Duration
	now    func() time.Time

	mu       sync.Mutex
	cache    map[SampleKey]sampleEntry
	inflight map[SampleKey]struct{}
}

type sampleEntry struct {
	result analysis.Result
	at     time.Time // whole seconds: see Get
}

// NewPrometheusSampler builds the production Sampler: the Prometheus-backed eval
// wired to the controller's event channel. The controller constructs one at
// startup and threads it through Dispatch.
func NewPrometheusSampler(events chan<- event.GenericEvent, maxConcurrency int, ttl time.Duration) *Sampler {
	return NewSampler(prometheusEval, events, maxConcurrency, ttl)
}

// NewSampler builds a Sampler. maxConcurrency caps in-flight background queries
// (clamped to >=1); ttl bounds how long an idle cached result is kept; events
// receives a GenericEvent carrying the ISVC whenever a query completes, so the
// controller re-reconciles. A nil events channel disables wake-up (the reconcile
// requeue is then the only path); useful in tests.
func NewSampler(eval evalFunc, events chan<- event.GenericEvent, maxConcurrency int, ttl time.Duration) *Sampler {
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}
	return &Sampler{
		eval:     eval,
		events:   events,
		sem:      make(chan struct{}, maxConcurrency),
		ttl:      ttl,
		now:      time.Now,
		cache:    make(map[SampleKey]sampleEntry),
		inflight: make(map[SampleKey]struct{}),
	}
}

// Get returns the cached result for req.Key when one was produced strictly after
// `since` — i.e. a sample the caller has not already consumed (the caller passes
// its last-consumed time and stores the returned producedAt). producedAt is at
// whole seconds, the precision of the metav1.Time status field the caller keeps
// it in; a finer stamp would read as newer than its own stored copy and be
// served again. On a miss it ensures a background query is in flight (deduped
// per key) and returns ok=false. It never blocks on the query.
func (s *Sampler) Get(req SampleRequest, since time.Time) (result analysis.Result, producedAt time.Time, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictLocked()
	if e, present := s.cache[req.Key]; present && e.at.After(since) {
		return e.result, e.at, true
	}
	if _, busy := s.inflight[req.Key]; !busy {
		s.inflight[req.Key] = struct{}{}
		go s.run(req)
	}
	return analysis.Result{}, time.Time{}, false
}

// run executes one background query, caches the result, clears in-flight, and
// wakes the controller. It is bounded by the semaphore and uses a standalone
// context (the reconcile's ctx is gone by now) with the request's query timeout.
func (s *Sampler) run(req SampleRequest) {
	// A non-blocking acquire that fails means every slot is busy and this query is
	// about to queue — surface that as queue depth + a starvation count so operators
	// can see MaxConcurrency throttling the effective sample interval. The semantics
	// are unchanged: the subsequent blocking send is the same wait as before.
	select {
	case s.sem <- struct{}{}:
	default:
		canarySamplerStarvedTotal.Inc()
		canarySamplerQueueDepth.Inc()
		s.sem <- struct{}{}
		canarySamplerQueueDepth.Dec()
	}
	canarySamplerInflight.Inc()
	defer func() { canarySamplerInflight.Dec(); <-s.sem }()

	ctx := context.Background()
	if req.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.QueryTimeout)
		defer cancel()
	}
	res := s.eval(ctx, req)

	s.mu.Lock()
	s.cache[req.Key] = sampleEntry{result: res, at: s.now().Truncate(time.Second)}
	delete(s.inflight, req.Key)
	s.mu.Unlock()

	s.signal(req.Key)
}

// signal wakes the controller to reconcile the ISVC whose sample just landed.
// The send is non-blocking: if the buffer is full the event is dropped and the
// reconcile's own requeue picks the result up — a full channel must never block a
// query goroutine.
func (s *Sampler) signal(key SampleKey) {
	if s.events == nil {
		return
	}
	ev := event.GenericEvent{Object: &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.ISVCName},
	}}
	select {
	case s.events <- ev:
	default:
	}
}

// evictLocked drops cache entries older than ttl (lazy GC on access), so keys for
// finished steps/revisions don't accumulate. The caller holds mu.
func (s *Sampler) evictLocked() {
	if s.ttl <= 0 {
		return
	}
	cutoff := s.now().Add(-s.ttl)
	for k, e := range s.cache {
		if e.at.Before(cutoff) {
			delete(s.cache, k)
		}
	}
}

// prometheusEval is the production eval: build a Querier for the request's
// resolved source and run its metrics. A client-build failure becomes an
// inconclusive result so the reconcile holds rather than reading it as a breach.
func prometheusEval(ctx context.Context, req SampleRequest) analysis.Result {
	q, err := analysis.NewQuerier(req.ServerAddress, req.BearerToken, req.Headers)
	if err != nil {
		return inconclusiveResult("prometheus", fmt.Sprintf("build client for %q: %v", req.ServerAddress, err))
	}
	return analysis.Evaluate(ctx, q, req.Analysis, req.TemplateContext)
}
