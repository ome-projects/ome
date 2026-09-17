package waitengine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	clocktesting "k8s.io/utils/clock/testing"
)

type boolSource struct{ gets, watches int }

func (s *boolSource) Get(context.Context) (Snapshot[bool], error) {
	s.gets++
	return Snapshot[bool]{Value: true, UID: types.UID("uid-a"), ResourceVersion: "opaque:rv"}, nil
}

type response struct {
	snapshot Snapshot[bool]
	err      error
}
type scriptedSource struct {
	mu           sync.Mutex
	responses    []response
	watcher      watch.Interface
	watchErr     error
	requests     chan string
	blockGet     bool
	watchFactory func() watch.Interface
}

func (s *scriptedSource) Get(ctx context.Context) (Snapshot[bool], error) {
	s.requests <- "get"
	if s.blockGet {
		<-ctx.Done()
		return Snapshot[bool]{}, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.responses[0]
	if len(s.responses) > 1 {
		s.responses = s.responses[1:]
	}
	return r.snapshot, r.err
}
func (s *scriptedSource) Watch(_ context.Context, rv string) (watch.Interface, error) {
	s.requests <- "watch:" + rv
	if s.watchFactory != nil {
		return s.watchFactory(), s.watchErr
	}
	return s.watcher, s.watchErr
}
func (*scriptedSource) Decode(obj runtime.Object) (Snapshot[bool], error) {
	m, ok := obj.(*metav1.PartialObjectMetadata)
	if !ok {
		return Snapshot[bool]{}, errors.New("private decode credential")
	}
	return Snapshot[bool]{Value: m.Name == "ready", UID: m.UID, ResourceVersion: m.ResourceVersion, Deleting: m.DeletionTimestamp != nil}, nil
}
func falseSnapshot() Snapshot[bool] {
	return Snapshot[bool]{UID: "uid-a", ResourceVersion: "opaque:rv"}
}
func object(uid, rv, name string) *metav1.PartialObjectMetadata {
	return &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), ResourceVersion: rv, Name: name}}
}
func boolPredicate(v bool) (Decision, error) {
	if v {
		return Decision{Matched: true, Reason: ReasonMatched}, nil
	}
	return Decision{Reason: ReasonNotMatched}, nil
}

type completion struct {
	result Result[bool]
	err    error
}

func startEngine(t *testing.T, s *scriptedSource, ctx context.Context, o Options) <-chan completion {
	t.Helper()
	done := make(chan completion, 1)
	go func() { r, err := Run(ctx, s, boolPredicate, o); done <- completion{r, err} }()
	return done
}
func request(t *testing.T, s *scriptedSource, want string) {
	t.Helper()
	select {
	case got := <-s.requests:
		require.Equal(t, want, got)
	case <-time.After(time.Second):
		t.Fatal("request not received")
	}
}
func finish(t *testing.T, ch <-chan completion) completion {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(time.Second):
		t.Fatal("engine did not finish")
		return completion{}
	}
}
func sourceFor(r ...response) *scriptedSource {
	return &scriptedSource{responses: r, requests: make(chan string, 20), watcher: watch.NewRaceFreeFake()}
}

func TestWatchMatchAndOpaqueResourceVersion(t *testing.T) {
	s := sourceFor(response{snapshot: falseSnapshot()})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startEngine(t, s, ctx, Options{Timeout: time.Minute})
	request(t, s, "get")
	request(t, s, "watch:opaque:rv")
	w := s.watcher.(*watch.RaceFreeFakeWatcher)
	w.Action(watch.Bookmark, &metav1.PartialObjectMetadata{})
	w.Modify(object("uid-a", "not-a-number", "ready"))
	c := finish(t, done)
	require.NoError(t, c.err)
	require.Equal(t, OutcomeMatched, c.result.Outcome)
	require.Equal(t, 2, c.result.Counts.Events)
	require.Equal(t, 2, c.result.Counts.Observations)
	require.True(t, w.IsStopped())
}
func TestDeletionAndReplacementNeverMatch(t *testing.T) {
	for _, tc := range []struct {
		name, uid string
		deleted   bool
		want      Outcome
	}{{"replacement", "uid-b", false, OutcomeReplaced}, {"deleted", "uid-a", true, OutcomeDeleted}} {
		t.Run(tc.name, func(t *testing.T) {
			s := sourceFor(response{snapshot: falseSnapshot()})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := startEngine(t, s, ctx, Options{Timeout: time.Minute})
			request(t, s, "get")
			request(t, s, "watch:opaque:rv")
			w := s.watcher.(*watch.RaceFreeFakeWatcher)
			if tc.deleted {
				w.Delete(object(tc.uid, "rv2", "ready"))
			} else {
				w.Modify(object(tc.uid, "rv2", "ready"))
			}
			c := finish(t, done)
			require.NoError(t, c.err)
			require.Equal(t, tc.want, c.result.Outcome)
			require.True(t, w.IsStopped())
		})
	}
}
func TestTimeoutAndParentCancelCloseWatch(t *testing.T) {
	for _, parent := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "parent"}[parent], func(t *testing.T) {
			clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
			s := sourceFor(response{snapshot: falseSnapshot()})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := startEngine(t, s, ctx, Options{Timeout: time.Minute, Clock: clk})
			request(t, s, "get")
			request(t, s, "watch:opaque:rv")
			if parent {
				cancel()
			} else {
				clk.Step(time.Minute)
			}
			c := finish(t, done)
			if parent {
				require.Error(t, c.err)
				require.Equal(t, ReasonCanceled, c.err.(*Error).Reason)
			} else {
				require.NoError(t, c.err)
				require.Equal(t, OutcomeTimedOut, c.result.Outcome)
			}
			require.True(t, s.watcher.(*watch.RaceFreeFakeWatcher).IsStopped())
		})
	}
}
func TestTimeoutCancelsCooperativeInitialGet(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	s := sourceFor()
	s.blockGet = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startEngine(t, s, ctx, Options{Timeout: time.Minute, Clock: clk})
	request(t, s, "get")
	clk.Step(time.Minute)
	c := finish(t, done)
	require.NoError(t, c.err)
	require.Equal(t, OutcomeTimedOut, c.result.Outcome)
}
func TestForbiddenWatchFallsBackToBoundedPolling(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	snap := falseSnapshot()
	ready := snap
	ready.Value = true
	s := sourceFor(response{snapshot: snap}, response{snapshot: ready})
	s.watchErr = apierrors.NewForbidden(schema.GroupResource{Resource: "inferenceservices"}, "service", errors.New("secret"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startEngine(t, s, ctx, Options{Timeout: time.Minute, Clock: clk})
	request(t, s, "get")
	request(t, s, "watch:opaque:rv")
	require.Eventually(t, func() bool { return clk.HasWaiters() }, time.Second, time.Millisecond)
	// The command deadline and polling timer are both installed before advancing.
	require.Eventually(t, func() bool { return clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(5 * time.Second)
	request(t, s, "get")
	c := finish(t, done)
	require.NoError(t, c.err)
	require.Equal(t, OutcomeMatched, c.result.Outcome)
	require.True(t, c.result.Fallback)
	require.Equal(t, MethodPoll, c.result.Method)
	require.Equal(t, 1, c.result.Counts.Polls)
}
func TestPollOnlySkipsWatchAndKeepsFallbackFalse(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	snap := falseSnapshot()
	ready := snap
	ready.Value = true
	s := sourceFor(response{snapshot: snap}, response{snapshot: ready})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startEngine(t, s, ctx, Options{Timeout: time.Minute, Clock: clk, PollOnly: true})
	request(t, s, "get")
	require.Eventually(t, func() bool { return clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(5 * time.Second)
	request(t, s, "get")
	c := finish(t, done)
	require.NoError(t, c.err)
	require.Equal(t, OutcomeMatched, c.result.Outcome)
	require.Equal(t, MethodPoll, c.result.Method)
	require.False(t, c.result.Fallback)
	require.Equal(t, 0, c.result.Counts.Watches)
	require.Equal(t, 1, c.result.Counts.Polls)
	require.Empty(t, s.requests)
}

func TestPollOnlyRetriesSourceLocalDeadline(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	ready := falseSnapshot()
	ready.Value = true
	s := sourceFor(response{err: context.DeadlineExceeded}, response{snapshot: ready})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startEngine(t, s, ctx, Options{Timeout: time.Minute, Clock: clk, PollOnly: true})
	request(t, s, "get")
	require.Eventually(t, func() bool { return clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(5 * time.Second)
	request(t, s, "get")
	result := finish(t, done)
	require.NoError(t, result.err)
	require.Equal(t, OutcomeMatched, result.result.Outcome)
	require.Equal(t, MethodPoll, result.result.Method)
	require.Equal(t, 2, result.result.Counts.Gets)
	require.Equal(t, 1, result.result.Counts.Polls)
	require.Zero(t, result.result.Counts.Watches)
	require.False(t, result.result.Fallback)
}

func TestPollOnlyParentCancellationStillStopsAfterSourceDeadline(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	s := sourceFor(response{err: context.DeadlineExceeded})
	ctx, cancel := context.WithCancel(context.Background())
	done := startEngine(t, s, ctx, Options{Timeout: time.Minute, Clock: clk, PollOnly: true})
	request(t, s, "get")
	require.Eventually(t, func() bool { return clk.Waiters() == 2 }, time.Second, time.Millisecond)
	cancel()
	result := finish(t, done)
	require.Error(t, result.err)
	require.Equal(t, ReasonCanceled, result.err.(*Error).Reason)
	require.Equal(t, 1, result.result.Counts.Gets)
}

func TestPollOnlySourceDeadlineStopsAtOverallTimeout(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	s := sourceFor(response{err: context.DeadlineExceeded})
	done := startEngine(t, s, context.Background(), Options{Timeout: time.Minute, Clock: clk, PollOnly: true})
	request(t, s, "get")
	require.Eventually(t, func() bool { return clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(time.Minute)
	result := finish(t, done)
	require.NoError(t, result.err)
	require.Equal(t, OutcomeTimedOut, result.result.Outcome)
	// The poll and overall timers may fire together at this fake-clock step.
	require.GreaterOrEqual(t, result.result.Counts.Gets, 1)
	require.LessOrEqual(t, result.result.Counts.Gets, 2)
}

func TestWatchModeSourceDeadlineStillFailsClosed(t *testing.T) {
	s := sourceFor(response{err: context.DeadlineExceeded})
	result, err := Run(context.Background(), s, boolPredicate, Options{Timeout: time.Minute})
	require.Error(t, err)
	require.Equal(t, ReasonAcquisitionFailed, err.(*Error).Reason)
	require.Empty(t, result.Outcome)
	require.Equal(t, 1, result.Counts.Gets)
}

func TestInitialAndLaterNotFound(t *testing.T) {
	nf := apierrors.NewNotFound(schema.GroupResource{Resource: "inferenceservices"}, "private")
	s := sourceFor(response{err: nf})
	r, err := Run(context.Background(), s, boolPredicate, Options{Timeout: time.Minute})
	require.NoError(t, err)
	require.Equal(t, OutcomeNotFound, r.Outcome)
	s = sourceFor(response{snapshot: falseSnapshot()}, response{err: nf})
	s.watchErr = apierrors.NewResourceExpired("private")
	r, err = Run(context.Background(), s, boolPredicate, Options{Timeout: time.Minute})
	require.NoError(t, err)
	require.Equal(t, OutcomeDeleted, r.Outcome)
}

func TestExpiredWatchCanMatchRefreshedNamedGet(t *testing.T) {
	snap := falseSnapshot()
	matched := snap
	matched.Value = true
	matched.ResourceVersion = "opaque-refresh"
	s := sourceFor(response{snapshot: snap}, response{snapshot: matched})
	s.watchErr = apierrors.NewResourceExpired("PRIVATE")
	r, err := Run(context.Background(), s, boolPredicate, Options{Timeout: time.Minute})
	require.NoError(t, err)
	require.Equal(t, OutcomeMatched, r.Outcome)
	require.Equal(t, "RefreshGET", string(r.Method))
	require.Equal(t, 2, r.Counts.Gets)
	require.Equal(t, 1, r.Counts.Watches)
}
func TestMalformedIdentityPredicateAndAcquisitionFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name      string
		r         response
		predicate Predicate[bool]
		want      Reason
	}{
		{"identity", response{snapshot: Snapshot[bool]{Value: true}}, boolPredicate, ReasonInvalidIdentity},
		{"predicate", response{snapshot: falseSnapshot()}, func(bool) (Decision, error) { return Decision{}, &Error{Reason: ReasonInspectionLimit} }, ReasonInspectionLimit},
		{"acquisition", response{err: errors.New("Bearer private")}, boolPredicate, ReasonAcquisitionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := Run(context.Background(), sourceFor(tc.r), tc.predicate, Options{Timeout: time.Minute})
			require.Error(t, err)
			require.Equal(t, tc.want, err.(*Error).Reason)
			require.Empty(t, r.Outcome)
			require.NotContains(t, err.Error(), "private")
		})
	}
}
func TestEventBudgetStopsWatch(t *testing.T) {
	s := sourceFor(response{snapshot: falseSnapshot()})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startEngine(t, s, ctx, Options{Timeout: time.Minute, EventBudget: 2})
	request(t, s, "get")
	request(t, s, "watch:opaque:rv")
	w := s.watcher.(*watch.RaceFreeFakeWatcher)
	w.Action(watch.Bookmark, &metav1.PartialObjectMetadata{})
	w.Action(watch.Bookmark, &metav1.PartialObjectMetadata{})
	w.Action(watch.Bookmark, &metav1.PartialObjectMetadata{})
	c := finish(t, done)
	require.Equal(t, ReasonEventLimit, c.err.(*Error).Reason)
	require.True(t, w.IsStopped())
}

func TestFirstAndLastAdmittedEventCanMatch(t *testing.T) {
	for _, budget := range []int{1, 2} {
		t.Run(map[int]string{1: "first", 2: "last"}[budget], func(t *testing.T) {
			s := sourceFor(response{snapshot: falseSnapshot()})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := startEngine(t, s, ctx, Options{Timeout: time.Minute, EventBudget: budget})
			request(t, s, "get")
			request(t, s, "watch:opaque:rv")
			w := s.watcher.(*watch.RaceFreeFakeWatcher)
			if budget == 2 {
				w.Action(watch.Bookmark, &metav1.PartialObjectMetadata{})
			}
			w.Modify(object("uid-a", "rv2", "ready"))
			c := finish(t, done)
			require.NoError(t, c.err)
			require.Equal(t, OutcomeMatched, c.result.Outcome)
			require.Equal(t, budget, c.result.Counts.Events)
		})
	}
}

func TestShortPollingIntervalDoesNotExpandAcquisitionBudget(t *testing.T) {
	s := &boolSource{}
	_, err := Run(context.Background(), s, boolPredicate, Options{Timeout: time.Minute, PollInterval: time.Second})
	require.Error(t, err)
	require.Zero(t, s.gets)
}

func TestWatchServerAndStreamErrorCanPoll(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "server", true: "stream"}[stream], func(t *testing.T) {
			clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
			snap := falseSnapshot()
			matched := snap
			matched.Value = true
			s := sourceFor(response{snapshot: snap}, response{snapshot: matched})
			if !stream {
				s.watchErr = apierrors.NewInternalError(errors.New("PRIVATE"))
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := startEngine(t, s, ctx, Options{Timeout: time.Minute, Clock: clk})
			request(t, s, "get")
			request(t, s, "watch:opaque:rv")
			if stream {
				s.watcher.(*watch.RaceFreeFakeWatcher).Error(&metav1.Status{Reason: metav1.StatusReasonInternalError, Code: 500, Message: "PRIVATE"})
			}
			require.Eventually(t, func() bool { return clk.Waiters() == 2 }, time.Second, time.Millisecond)
			clk.Step(5 * time.Second)
			request(t, s, "get")
			c := finish(t, done)
			require.NoError(t, c.err)
			require.Equal(t, OutcomeMatched, c.result.Outcome)
			require.True(t, c.result.Fallback)
		})
	}
}
func TestTwoEarlyEOFsThrottleIntoPolling(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	snap := falseSnapshot()
	matched := snap
	matched.Value = true
	s := sourceFor(response{snapshot: snap}, response{snapshot: snap}, response{snapshot: matched})
	s.watchFactory = func() watch.Interface { return watch.NewEmptyWatch() }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startEngine(t, s, ctx, Options{Timeout: time.Minute, Clock: clk})
	request(t, s, "get")
	request(t, s, "watch:opaque:rv")
	request(t, s, "get")
	request(t, s, "watch:opaque:rv")
	require.Eventually(t, func() bool { return clk.Waiters() == 2 }, time.Second, time.Millisecond)
	require.Empty(t, s.requests)
	clk.Step(5 * time.Second)
	request(t, s, "get")
	c := finish(t, done)
	require.NoError(t, c.err)
	require.Equal(t, OutcomeMatched, c.result.Outcome)
	require.Equal(t, 2, c.result.Counts.Watches)
	require.Equal(t, 3, c.result.Counts.Gets)
}
func TestWatchFailureAndMalformedEventsAreGeneral(t *testing.T) {
	for _, tc := range []struct {
		name  string
		event watch.Event
		want  Reason
	}{
		{"decode", watch.Event{Type: watch.Modified, Object: &metav1.Status{}}, ReasonInvalidIdentity},
		{"unknown", watch.Event{Type: "PRIVATE", Object: object("uid-a", "rv", "ready")}, ReasonAcquisitionFailed},
		{"unauthorized", watch.Event{Type: watch.Error, Object: &metav1.Status{Code: 401, Reason: metav1.StatusReasonUnauthorized}}, ReasonAcquisitionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := sourceFor(response{snapshot: falseSnapshot()})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := startEngine(t, s, ctx, Options{Timeout: time.Minute})
			request(t, s, "get")
			request(t, s, "watch:opaque:rv")
			s.watcher.(*watch.RaceFreeFakeWatcher).Action(tc.event.Type, tc.event.Object)
			c := finish(t, done)
			require.Equal(t, tc.want, c.err.(*Error).Reason)
		})
	}
	s := sourceFor(response{snapshot: falseSnapshot()})
	s.watchErr = apierrors.NewUnauthorized("PRIVATE")
	_, err := Run(context.Background(), s, boolPredicate, Options{Timeout: time.Minute})
	require.Equal(t, ReasonAcquisitionFailed, err.(*Error).Reason)
	s = sourceFor(response{snapshot: falseSnapshot()})
	s.watcher = nil
	_, err = Run(context.Background(), s, boolPredicate, Options{Timeout: time.Minute})
	require.Error(t, err)
}
func TestInvalidOptionsDeletingAndGeneralPredicateError(t *testing.T) {
	for _, o := range []Options{{}, {Timeout: -time.Second}, {Timeout: 25 * time.Hour}, {Timeout: time.Minute, PollInterval: -time.Second}, {Timeout: time.Minute, PollInterval: 6 * time.Second}, {Timeout: time.Minute, EventBudget: -1}, {Timeout: time.Minute, EventBudget: 4097}} {
		_, err := Run(context.Background(), sourceFor(response{snapshot: falseSnapshot()}), boolPredicate, o)
		require.Equal(t, ReasonInvalidOptions, err.(*Error).Reason)
	}
	_, err := Run[bool](context.Background(), nil, boolPredicate, Options{Timeout: time.Minute})
	require.Error(t, err)
	_, err = Run(context.Background(), sourceFor(response{snapshot: falseSnapshot()}), nil, Options{Timeout: time.Minute})
	require.Error(t, err)
	snap := falseSnapshot()
	snap.Deleting = true
	snap.Value = true
	r, err := Run(context.Background(), sourceFor(response{snapshot: snap}), boolPredicate, Options{Timeout: time.Minute})
	require.NoError(t, err)
	require.Equal(t, OutcomeDeleted, r.Outcome)
	_, err = Run(context.Background(), sourceFor(response{snapshot: falseSnapshot()}), func(bool) (Decision, error) { return Decision{}, errors.New("PRIVATE") }, Options{Timeout: time.Minute})
	require.Equal(t, ReasonInvalidCondition, err.(*Error).Reason)
}
func TestNamedGetBudgetReturnsGeneralNotFabricatedTimeout(t *testing.T) {
	s := sourceFor(response{snapshot: falseSnapshot()})
	state := runState[bool]{ctx: context.Background(), parent: context.Background(), source: s, predicate: boolPredicate, getBudget: 1, result: Result[bool]{}}
	stop, err := state.get()
	require.False(t, stop)
	require.NoError(t, err)
	stop, err = state.get()
	require.True(t, stop)
	require.Equal(t, ReasonBudgetExceeded, err.(*Error).Reason)
	require.Empty(t, state.result.Outcome)
	require.Equal(t, 1, state.result.Counts.Gets)
}
func (s *boolSource) Watch(context.Context, string) (watch.Interface, error) {
	s.watches++
	return watch.NewFake(), nil
}
func (*boolSource) Decode(runtime.Object) (Snapshot[bool], error) { panic("unexpected decode") }

func TestInitialMatchDoesNotOpenWatch(t *testing.T) {
	s := &boolSource{}
	r, err := Run(context.Background(), s, func(v bool) (Decision, error) { return Decision{Matched: v, Reason: ReasonMatched}, nil }, Options{Timeout: time.Minute})
	require.NoError(t, err)
	require.Equal(t, OutcomeMatched, r.Outcome)
	require.Equal(t, 1, s.gets)
	require.Zero(t, s.watches)
	require.Equal(t, 1, r.Counts.Observations)
}
