// Package waitengine observes a single bound object without exposing it as output.
package waitengine

import (
	"context"
	"errors"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/utils/clock"
)

type Snapshot[T any] struct {
	Value           T
	UID             types.UID
	ResourceVersion string
	Deleting        bool
}
type Source[T any] interface {
	Get(context.Context) (Snapshot[T], error)
	Watch(context.Context, string) (watch.Interface, error)
	Decode(runtime.Object) (Snapshot[T], error)
}
type Reason string

const (
	ReasonMatched                 Reason = "ConditionMatched"
	ReasonNotRecorded             Reason = "ConditionNotRecorded"
	ReasonNotMatched              Reason = "ConditionNotMatched"
	ReasonInvalidCondition        Reason = "InvalidCondition"
	ReasonInvalidIdentity         Reason = "InvalidIdentity"
	ReasonInspectionLimit         Reason = "ConditionInspectionLimit"
	ReasonAcquisitionFailed       Reason = "AcquisitionFailed"
	ReasonBudgetExceeded          Reason = "AcquisitionBudgetExceeded"
	ReasonEventLimit              Reason = "EventBudgetExceeded"
	ReasonCanceled                Reason = "Canceled"
	ReasonInvalidOptions          Reason = "InvalidOptions"
	ReasonRolloutMatched          Reason = "RolloutMatched"
	ReasonRolloutNotMatched       Reason = "RolloutNotMatched"
	ReasonRolloutNotRecorded      Reason = "RolloutNotRecorded"
	ReasonInvalidRollout          Reason = "InvalidRollout"
	ReasonRolloutInspectionLimit  Reason = "RolloutInspectionLimit"
	ReasonMigrationMatched        Reason = "MigrationMatched"
	ReasonMigrationNotRecorded    Reason = "MigrationNotRecorded"
	ReasonMigrationInProgress     Reason = "MigrationInProgress"
	ReasonInvalidMigration        Reason = "InvalidMigration"
	ReasonReplicaReadyMatched     Reason = "ReplicaReadyMatched"
	ReasonReplicaReadyNotMatched  Reason = "ReplicaReadyNotMatched"
	ReasonReplicaReadyNotRecorded Reason = "ReplicaReadyNotRecorded"
	ReasonInvalidReplicaReady     Reason = "InvalidReplicaReadyEvidence"
)

type Error struct{ Reason Reason }

func (e *Error) Error() string { return string(e.Reason) }

type Decision struct {
	Matched bool
	Reason  Reason
}
type Predicate[T any] func(T) (Decision, error)
type Outcome string

const (
	OutcomeMatched  Outcome = "Matched"
	OutcomeTimedOut Outcome = "TimedOut"
	OutcomeNotFound Outcome = "NotFound"
	OutcomeDeleted  Outcome = "Deleted"
	OutcomeReplaced Outcome = "Replaced"
)

type Method string

const (
	MethodInitialGET Method = "InitialGET"
	MethodRefreshGET Method = "RefreshGET"
	MethodWatch      Method = "Watch"
	MethodPoll       Method = "Poll"
)

type Counts struct{ Gets, Watches, Polls, Events, Observations int }
type Options struct {
	Timeout, PollInterval time.Duration
	EventBudget           int
	PollOnly              bool
	Clock                 clock.Clock
}
type Result[T any] struct {
	Outcome        Outcome
	Reason         Reason
	Last           T
	HasObservation bool
	Counts         Counts
	Elapsed        time.Duration
	Method         Method
	Fallback       bool
}

func Run[T any](ctx context.Context, source Source[T], predicate Predicate[T], options Options) (Result[T], error) {
	if options.Timeout <= 0 || options.Timeout > 24*time.Hour || source == nil || predicate == nil {
		return Result[T]{}, &Error{Reason: ReasonInvalidOptions}
	}
	if options.PollInterval == 0 {
		options.PollInterval = 5 * time.Second
	}
	if options.PollInterval != 5*time.Second || options.EventBudget < 0 || options.EventBudget > 4096 {
		return Result[T]{}, &Error{Reason: ReasonInvalidOptions}
	}
	if options.EventBudget == 0 {
		options.EventBudget = 4096
	}
	if options.Clock == nil {
		options.Clock = clock.RealClock{}
	}
	start := options.Clock.Now()
	child, cancel := context.WithCancelCause(ctx)
	timer := options.Clock.NewTimer(options.Timeout)
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-timer.C():
			cancel(errTimeout)
		case <-child.Done():
		}
	}()
	defer func() { cancel(nil); timer.Stop(); <-done }()
	s := runState[T]{ctx: child, parent: ctx, source: source, predicate: predicate, options: options,
		getBudget: int((options.Timeout+options.PollInterval-1)/options.PollInterval) + 2,
		deadline:  start.Add(options.Timeout),
		result:    Result[T]{Method: MethodInitialGET}}
	err := s.run()
	s.result.Elapsed = options.Clock.Since(start)
	if ctx.Err() != nil {
		s.result.Outcome = ""
		err = &Error{Reason: ReasonCanceled}
	}
	return s.result, err
}

var errTimeout = errors.New("wait command timeout")

const (
	maxWatchOpenings   = 2
	maxWatchRetryDelay = 30 * time.Second
)

type watchAction uint8

const (
	watchFail watchAction = iota
	watchResume
	watchRefresh
	watchPoll
)

type runState[T any] struct {
	ctx, parent context.Context
	source      Source[T]
	predicate   Predicate[T]
	options     Options
	getBudget   int
	deadline    time.Time
	uid         types.UID
	rv          string
	result      Result[T]
}

func (s *runState[T]) canceled() (bool, error) {
	if s.parent.Err() != nil {
		return true, &Error{Reason: ReasonCanceled}
	}
	if !s.deadline.IsZero() && !s.options.Clock.Now().Before(s.deadline) {
		s.result.Outcome = OutcomeTimedOut
		return true, nil
	}
	if s.ctx.Err() == nil {
		return false, nil
	}
	if errors.Is(context.Cause(s.ctx), errTimeout) {
		s.result.Outcome = OutcomeTimedOut
		return true, nil
	}
	return true, &Error{Reason: ReasonCanceled}
}

func (s *runState[T]) observe(snapshot Snapshot[T], deleted bool) (bool, error) {
	if snapshot.UID == "" || snapshot.ResourceVersion == "" {
		return true, &Error{Reason: ReasonInvalidIdentity}
	}
	if s.uid == "" {
		s.uid = snapshot.UID
	}
	if snapshot.UID != s.uid {
		s.result.Outcome = OutcomeReplaced
		return true, nil
	}
	if deleted || snapshot.Deleting {
		s.result.Outcome = OutcomeDeleted
		return true, nil
	}
	decision, err := s.predicate(snapshot.Value)
	if err != nil {
		var safe *Error
		if errors.As(err, &safe) {
			return true, safe
		}
		return true, &Error{Reason: ReasonInvalidCondition}
	}
	s.rv = snapshot.ResourceVersion
	s.result.Last, s.result.HasObservation = snapshot.Value, true
	s.result.Counts.Observations++
	s.result.Reason = decision.Reason
	if decision.Matched {
		s.result.Outcome = OutcomeMatched
		return true, nil
	}
	return false, nil
}

func (s *runState[T]) get() (bool, error) {
	if stop, err := s.canceled(); stop {
		return true, err
	}
	if s.result.Counts.Gets >= s.getBudget {
		return true, &Error{Reason: ReasonBudgetExceeded}
	}
	s.result.Counts.Gets++
	snapshot, err := s.source.Get(s.ctx)
	if stop, cancelErr := s.canceled(); stop {
		return true, cancelErr
	}
	if err != nil {
		// Poll-only sources bound each named GET independently. A source-local
		// deadline does not exhaust the overall wait; the next poll may succeed.
		// canceled above still makes the overall deadline or parent cancel
		// terminal, and every other acquisition error remains fail-closed.
		if s.options.PollOnly && errors.Is(err, context.DeadlineExceeded) {
			return false, nil
		}
		if apierrors.IsNotFound(err) {
			if s.uid == "" {
				s.result.Outcome = OutcomeNotFound
			} else {
				s.result.Outcome = OutcomeDeleted
			}
			return true, nil
		}
		return true, &Error{Reason: ReasonAcquisitionFailed}
	}
	return s.observe(snapshot, false)
}

func (s *runState[T]) run() error {
	if stop, err := s.get(); stop {
		return err
	}
	if s.options.PollOnly {
		return s.poll()
	}
	for opening := 0; opening < maxWatchOpenings; opening++ {
		if stop, err := s.canceled(); stop {
			return err
		}
		s.result.Counts.Watches++
		w, err := s.source.Watch(s.ctx, s.rv)
		if stop, cancelErr := s.canceled(); stop {
			if w != nil {
				w.Stop()
			}
			return cancelErr
		}
		var action watchAction
		var delay time.Duration
		var actionErr error
		if err != nil {
			if w != nil {
				w.Stop()
			}
			action, delay = classifyWatchFailure(err)
		} else {
			if w == nil {
				return &Error{Reason: ReasonAcquisitionFailed}
			}
			action, delay, actionErr = s.consume(w)
		}
		switch action {
		case watchFail:
			if actionErr != nil {
				return actionErr
			}
			if s.result.Outcome != "" {
				return nil
			}
			return &Error{Reason: ReasonAcquisitionFailed}
		case watchPoll:
			return s.poll()
		case watchRefresh:
			s.result.Method = MethodRefreshGET
			if stop, getErr := s.get(); stop {
				return getErr
			}
			if opening+1 >= maxWatchOpenings {
				return s.poll()
			}
		case watchResume:
			if opening+1 >= maxWatchOpenings {
				return s.poll()
			}
			if stop, retryErr := s.waitWatchRetry(delay); stop {
				return retryErr
			}
		}
	}
	return s.poll()
}

func classifyWatchFailure(err error) (watchAction, time.Duration) {
	if apierrors.IsResourceExpired(err) || apierrors.IsGone(err) {
		return watchRefresh, 0
	}
	transient := apierrors.IsTooManyRequests(err) || apierrors.IsServerTimeout(err) ||
		apierrors.IsTimeout(err) || apierrors.IsInternalError(err) || apierrors.IsServiceUnavailable(err)
	if transient {
		if seconds, suggested := apierrors.SuggestsClientDelay(err); suggested && seconds > 0 {
			maxSeconds := int(maxWatchRetryDelay / time.Second)
			if seconds > maxSeconds {
				seconds = maxSeconds
			}
			return watchResume, time.Duration(seconds) * time.Second
		}
		return watchPoll, 0
	}
	if apierrors.IsForbidden(err) || apierrors.IsMethodNotSupported(err) || apierrors.IsNotFound(err) {
		return watchPoll, 0
	}
	return watchFail, 0
}

func (s *runState[T]) waitWatchRetry(delay time.Duration) (bool, error) {
	if delay <= 0 {
		return s.canceled()
	}
	timer := s.options.Clock.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
	case <-timer.C():
	}
	return s.canceled()
}

func (s *runState[T]) consume(w watch.Interface) (watchAction, time.Duration, error) {
	defer w.Stop()
	for {
		select {
		case <-s.ctx.Done():
			_, err := s.canceled()
			return watchFail, 0, err
		case event, ok := <-w.ResultChan():
			if !ok {
				return watchResume, 0, nil
			}
			if done, cancelErr := s.canceled(); done {
				return watchFail, 0, cancelErr
			}
			if s.result.Counts.Events >= s.options.EventBudget {
				return watchFail, 0, &Error{Reason: ReasonEventLimit}
			}
			s.result.Counts.Events++
			switch event.Type {
			case watch.Bookmark:
				accessor, accessorErr := meta.Accessor(event.Object)
				if accessorErr != nil || accessor.GetResourceVersion() == "" {
					return watchFail, 0, &Error{Reason: ReasonInvalidIdentity}
				}
				s.rv = accessor.GetResourceVersion()
				continue
			case watch.Error:
				eventErr := apierrors.FromObject(event.Object)
				action, delay := classifyWatchFailure(eventErr)
				if action == watchFail {
					return watchFail, 0, &Error{Reason: ReasonAcquisitionFailed}
				}
				return action, delay, nil
			case watch.Added, watch.Modified, watch.Deleted:
				snapshot, decodeErr := s.source.Decode(event.Object)
				if decodeErr != nil {
					return watchFail, 0, &Error{Reason: ReasonInvalidIdentity}
				}
				s.result.Method = MethodWatch
				done, observeErr := s.observe(snapshot, event.Type == watch.Deleted)
				if done {
					return watchFail, 0, observeErr
				}
			default:
				return watchFail, 0, &Error{Reason: ReasonAcquisitionFailed}
			}
		}
	}
}

func (s *runState[T]) poll() error {
	if stop, err := s.canceled(); stop {
		return err
	}
	for {
		timer := s.options.Clock.NewTimer(s.options.PollInterval)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			_, err := s.canceled()
			return err
		case <-timer.C():
		}
		timer.Stop()
		if stop, err := s.canceled(); stop {
			return err
		}
		s.result.Fallback = !s.options.PollOnly
		s.result.Method = MethodPoll
		s.result.Counts.Polls++
		if stop, err := s.get(); stop {
			return err
		}
	}
}
