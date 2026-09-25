package routing

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrObserverExecutorNotRunning means the executor has not entered Start.
	ErrObserverExecutorNotRunning = errors.New("observer executor is not running")
	// ErrObserverExecutorStopped means the executor is shutting down or has
	// already stopped.
	ErrObserverExecutorStopped = errors.New("observer executor is stopped")
	// ErrObserverExecutorAlreadyStarted means Start was called more than once.
	ErrObserverExecutorAlreadyStarted = errors.New("observer executor was already started")
)

// ObserverJob is one bounded probe or capacity observation. Implementations
// must stop when ctx is cancelled so executor shutdown cannot strand a worker.
type ObserverJob func(ctx context.Context) error

type observerTask struct {
	ctx     context.Context
	timeout time.Duration
	run     ObserverJob

	done chan struct{}
	err  error
}

func (t *observerTask) complete(err error) {
	t.err = err
	close(t.done)
}

// ObserverFuture tracks one accepted observation. It is safe for multiple
// callers to wait for the same job.
type ObserverFuture struct {
	task *observerTask
}

// Wait blocks until the observation finishes or either the submit context or
// wait context is cancelled. Cancelling a queued observation prevents it from
// being invoked when a worker reaches it.
func (f *ObserverFuture) Wait(ctx context.Context) error {
	if f == nil || f.task == nil {
		return errors.New("observer future is nil")
	}
	if ctx == nil {
		return errors.New("observer future wait context is nil")
	}

	select {
	case <-f.task.done:
		return f.task.err
	default:
	}

	select {
	case <-f.task.done:
		return f.task.err
	case <-f.task.ctx.Done():
		return f.task.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

type observerExecutorState uint8

const (
	observerExecutorCreated observerExecutorState = iota
	observerExecutorRunning
	observerExecutorStopping
	observerExecutorStopped
)

// ObserverExecutor is the shared worker pool for active routing observations.
// One process-wide instance lets probes and capacity polls share the same
// configured concurrency and queue bounds.
//
// Each worker executes jobs directly. The executor therefore creates exactly
// workerCount goroutines instead of one goroutine per target waiting behind a
// semaphore. A job's timeout starts in the worker, after queueing has ended.
type ObserverExecutor struct {
	workerCount int
	queue       chan *observerTask

	mu         sync.Mutex
	state      observerExecutorState
	runCtx     context.Context
	submitters sync.WaitGroup
}

// NewObserverExecutor constructs an executor with explicit process-wide
// bounds. queueCapacity may be zero for an unbuffered handoff.
func NewObserverExecutor(workerCount, queueCapacity int) (*ObserverExecutor, error) {
	if workerCount <= 0 {
		return nil, fmt.Errorf("observer executor worker count must be positive, got %d", workerCount)
	}
	if queueCapacity < 0 {
		return nil, fmt.Errorf("observer executor queue capacity must not be negative, got %d", queueCapacity)
	}
	return &ObserverExecutor{
		workerCount: workerCount,
		queue:       make(chan *observerTask, queueCapacity),
		state:       observerExecutorCreated,
	}, nil
}

// Start runs the fixed workers until ctx is cancelled. Shutdown rejects new
// submissions, cancels in-flight jobs, and resolves every accepted queued job
// before returning.
func (e *ObserverExecutor) Start(ctx context.Context) error {
	if ctx == nil {
		return errors.New("observer executor start context is nil")
	}

	e.mu.Lock()
	if e.state != observerExecutorCreated {
		e.mu.Unlock()
		return ErrObserverExecutorAlreadyStarted
	}
	e.state = observerExecutorRunning
	e.runCtx = ctx
	e.mu.Unlock()

	var workers sync.WaitGroup
	workers.Add(e.workerCount)
	for range e.workerCount {
		go func() {
			defer workers.Done()
			e.worker(ctx)
		}()
	}

	<-ctx.Done()

	// Changing the state under the same lock Submit uses prevents any new
	// submitter from joining after shutdown starts. Existing submitters observe
	// ctx cancellation and leave before the queue is closed.
	e.mu.Lock()
	e.state = observerExecutorStopping
	e.mu.Unlock()
	e.submitters.Wait()
	close(e.queue)
	workers.Wait()

	e.mu.Lock()
	e.state = observerExecutorStopped
	e.mu.Unlock()
	return nil
}

// NeedLeaderElection is false because the executor has no external effects of
// its own. Leader-elected observers submit the work, while starting this pool
// process-wide avoids startup ordering races with those observers.
func (e *ObserverExecutor) NeedLeaderElection() bool { return false }

// Submit queues one observation. It blocks only while the configured queue is
// full and stops waiting when ctx is cancelled or executor shutdown begins.
// The timeout is created by the worker, so time spent queued does not consume
// the request's execution budget.
func (e *ObserverExecutor) Submit(ctx context.Context, timeout time.Duration, run ObserverJob) (*ObserverFuture, error) {
	if ctx == nil {
		return nil, errors.New("observer job context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("observer job timeout must be positive, got %s", timeout)
	}
	if run == nil {
		return nil, errors.New("observer job is nil")
	}

	e.mu.Lock()
	switch e.state {
	case observerExecutorCreated:
		e.mu.Unlock()
		return nil, ErrObserverExecutorNotRunning
	case observerExecutorStopping, observerExecutorStopped:
		e.mu.Unlock()
		return nil, ErrObserverExecutorStopped
	}
	runCtx := e.runCtx
	e.submitters.Add(1)
	e.mu.Unlock()
	defer e.submitters.Done()

	task := &observerTask{
		ctx:     ctx,
		timeout: timeout,
		run:     run,
		done:    make(chan struct{}),
	}
	select {
	case e.queue <- task:
		return &ObserverFuture{task: task}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-runCtx.Done():
		return nil, ErrObserverExecutorStopped
	}
}

func (e *ObserverExecutor) worker(runCtx context.Context) {
	for task := range e.queue {
		e.execute(runCtx, task)
	}
}

func (e *ObserverExecutor) execute(runCtx context.Context, task *observerTask) {
	if err := task.ctx.Err(); err != nil {
		task.complete(err)
		return
	}
	if err := runCtx.Err(); err != nil {
		task.complete(err)
		return
	}

	jobCtx, cancel := context.WithTimeout(context.WithoutCancel(task.ctx), task.timeout)
	stopSubmitCancellation := context.AfterFunc(task.ctx, cancel)
	stopRunCancellation := context.AfterFunc(runCtx, cancel)
	defer func() {
		stopSubmitCancellation()
		stopRunCancellation()
		cancel()
	}()
	if err := jobCtx.Err(); err != nil {
		task.complete(err)
		return
	}

	err := task.run(jobCtx)
	if err == nil {
		err = jobCtx.Err()
	}
	task.complete(err)
}
