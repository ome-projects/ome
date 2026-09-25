package routing

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const observerTestTimeout = 5 * time.Second

func startTestObserverExecutor(t *testing.T, workers, queueCapacity int) (*ObserverExecutor, context.CancelFunc, <-chan error) {
	t.Helper()
	executor, err := NewObserverExecutor(workers, queueCapacity)
	if err != nil {
		t.Fatalf("NewObserverExecutor() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- executor.Start(ctx)
	}()

	deadline := time.NewTimer(observerTestTimeout)
	defer deadline.Stop()
	for {
		executor.mu.Lock()
		running := executor.state == observerExecutorRunning
		executor.mu.Unlock()
		if running {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("ObserverExecutor.Start() returned before startup: %v", err)
		case <-deadline.C:
			t.Fatal("ObserverExecutor.Start() did not enter running state")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	t.Cleanup(func() {
		cancel()
		executor.mu.Lock()
		stopped := executor.state == observerExecutorStopped
		executor.mu.Unlock()
		if stopped {
			return
		}
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("ObserverExecutor.Start() error = %v", err)
			}
		case <-time.After(observerTestTimeout):
			t.Error("ObserverExecutor.Start() did not shut down")
		}
	})
	return executor, cancel, done
}

func waitFuture(t *testing.T, future *ObserverFuture) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), observerTestTimeout)
	defer cancel()
	return future.Wait(ctx)
}

func TestNewObserverExecutorRequiresExplicitBounds(t *testing.T) {
	for _, tc := range []struct {
		name          string
		workers       int
		queueCapacity int
	}{
		{name: "zero workers", workers: 0, queueCapacity: 1},
		{name: "negative workers", workers: -1, queueCapacity: 1},
		{name: "negative queue capacity", workers: 1, queueCapacity: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewObserverExecutor(tc.workers, tc.queueCapacity); err == nil {
				t.Fatal("NewObserverExecutor() error = nil, want invalid-bound error")
			}
		})
	}

	if _, err := NewObserverExecutor(1, 0); err != nil {
		t.Fatalf("NewObserverExecutor() with an unbuffered queue error = %v", err)
	}
}

func TestObserverExecutorUsesFixedWorkerBound(t *testing.T) {
	const (
		workerCount = 2
		jobCount    = 12
	)
	executor, _, _ := startTestObserverExecutor(t, workerCount, jobCount)

	release := make(chan struct{})
	started := make(chan struct{}, jobCount)
	var inFlight atomic.Int32
	var maximum atomic.Int32
	futures := make([]*ObserverFuture, 0, jobCount)
	for range jobCount {
		future, err := executor.Submit(context.Background(), observerTestTimeout, func(ctx context.Context) error {
			current := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				old := maximum.Load()
				if current <= old || maximum.CompareAndSwap(old, current) {
					break
				}
			}
			started <- struct{}{}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
		futures = append(futures, future)
	}

	for range workerCount {
		select {
		case <-started:
		case <-time.After(observerTestTimeout):
			t.Fatal("configured workers did not start")
		}
	}
	select {
	case <-started:
		t.Fatal("more jobs started than the configured worker bound")
	default:
	}
	close(release)
	for _, future := range futures {
		if err := waitFuture(t, future); err != nil {
			t.Fatalf("ObserverFuture.Wait() error = %v", err)
		}
	}
	if got := maximum.Load(); got != workerCount {
		t.Fatalf("maximum concurrent jobs = %d, want %d", got, workerCount)
	}
}

func TestObserverExecutorSkipsCancelledQueuedJob(t *testing.T) {
	executor, _, _ := startTestObserverExecutor(t, 1, 1)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	first, err := executor.Submit(context.Background(), observerTestTimeout, func(ctx context.Context) error {
		close(firstStarted)
		select {
		case <-releaseFirst:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil {
		t.Fatalf("Submit(first) error = %v", err)
	}
	<-firstStarted

	queuedCtx, cancelQueued := context.WithCancel(context.Background())
	var invoked atomic.Bool
	queued, err := executor.Submit(queuedCtx, observerTestTimeout, func(context.Context) error {
		invoked.Store(true)
		return nil
	})
	if err != nil {
		t.Fatalf("Submit(queued) error = %v", err)
	}
	cancelQueued()
	if err := waitFuture(t, queued); !errors.Is(err, context.Canceled) {
		t.Fatalf("queued ObserverFuture.Wait() error = %v, want context.Canceled", err)
	}

	close(releaseFirst)
	if err := waitFuture(t, first); err != nil {
		t.Fatalf("first ObserverFuture.Wait() error = %v", err)
	}
	select {
	case <-queued.task.done:
	case <-time.After(observerTestTimeout):
		t.Fatal("cancelled queued job was not resolved")
	}
	if invoked.Load() {
		t.Fatal("cancelled queued job was invoked")
	}
}

func TestObserverExecutorSubmitStopsWaitingWhenQueueIsFull(t *testing.T) {
	executor, _, _ := startTestObserverExecutor(t, 1, 0)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	first, err := executor.Submit(context.Background(), observerTestTimeout, func(ctx context.Context) error {
		close(firstStarted)
		select {
		case <-releaseFirst:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil {
		t.Fatalf("Submit(first) error = %v", err)
	}
	<-firstStarted

	queuedCtx, cancelQueued := context.WithCancel(context.Background())
	submitDone := make(chan error, 1)
	go func() {
		_, submitErr := executor.Submit(queuedCtx, observerTestTimeout, func(context.Context) error {
			t.Error("job from cancelled Submit was invoked")
			return nil
		})
		submitDone <- submitErr
	}()
	cancelQueued()
	select {
	case err := <-submitDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Submit() error = %v, want context.Canceled", err)
		}
	case <-time.After(observerTestTimeout):
		t.Fatal("Submit() stayed blocked after its context was cancelled")
	}

	close(releaseFirst)
	if err := waitFuture(t, first); err != nil {
		t.Fatalf("first ObserverFuture.Wait() error = %v", err)
	}
}

func TestObserverExecutorTimeoutStartsInWorker(t *testing.T) {
	const jobTimeout = 100 * time.Millisecond
	executor, _, _ := startTestObserverExecutor(t, 1, 1)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	first, err := executor.Submit(context.Background(), observerTestTimeout, func(ctx context.Context) error {
		close(firstStarted)
		select {
		case <-releaseFirst:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil {
		t.Fatalf("Submit(first) error = %v", err)
	}
	<-firstStarted

	secondStarted := make(chan error, 1)
	second, err := executor.Submit(context.Background(), jobTimeout, func(ctx context.Context) error {
		secondStarted <- ctx.Err()
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("Submit(second) error = %v", err)
	}
	select {
	case err := <-secondStarted:
		t.Fatalf("queued job started before a worker was available, initial context error = %v", err)
	case <-time.After(2 * jobTimeout):
	}
	select {
	case <-second.task.done:
		t.Fatalf("queued job timed out before acquiring a worker: %v", second.task.err)
	default:
	}

	close(releaseFirst)
	if err := waitFuture(t, first); err != nil {
		t.Fatalf("first ObserverFuture.Wait() error = %v", err)
	}
	select {
	case err := <-secondStarted:
		if err != nil {
			t.Fatalf("job context was already expired at worker acquisition: %v", err)
		}
	case <-time.After(observerTestTimeout):
		t.Fatal("queued job did not start after worker release")
	}
	if err := waitFuture(t, second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second ObserverFuture.Wait() error = %v, want context.DeadlineExceeded", err)
	}
}

func TestObserverExecutorPreservesSubmitContextValues(t *testing.T) {
	type contextKey struct{}
	executor, _, _ := startTestObserverExecutor(t, 1, 1)
	submitCtx := context.WithValue(context.Background(), contextKey{}, "request-value")
	observed := make(chan any, 1)
	future, err := executor.Submit(submitCtx, observerTestTimeout, func(ctx context.Context) error {
		observed <- ctx.Value(contextKey{})
		return nil
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := waitFuture(t, future); err != nil {
		t.Fatalf("ObserverFuture.Wait() error = %v", err)
	}
	if got := <-observed; got != "request-value" {
		t.Fatalf("job context value = %v, want request-value", got)
	}
}

func TestObserverExecutorShutdownCancelsAndDrainsAcceptedJobs(t *testing.T) {
	executor, cancelExecutor, executorDone := startTestObserverExecutor(t, 1, 2)

	runningStarted := make(chan struct{})
	running, err := executor.Submit(context.Background(), observerTestTimeout, func(ctx context.Context) error {
		close(runningStarted)
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("Submit(running) error = %v", err)
	}
	<-runningStarted

	var queuedInvocations atomic.Int32
	queued := make([]*ObserverFuture, 0, 2)
	for range 2 {
		future, submitErr := executor.Submit(context.Background(), observerTestTimeout, func(context.Context) error {
			queuedInvocations.Add(1)
			return nil
		})
		if submitErr != nil {
			t.Fatalf("Submit(queued) error = %v", submitErr)
		}
		queued = append(queued, future)
	}

	cancelExecutor()
	select {
	case err := <-executorDone:
		if err != nil {
			t.Fatalf("ObserverExecutor.Start() error = %v", err)
		}
	case <-time.After(observerTestTimeout):
		t.Fatal("ObserverExecutor.Start() did not complete clean shutdown")
	}
	if err := waitFuture(t, running); !errors.Is(err, context.Canceled) {
		t.Fatalf("running ObserverFuture.Wait() error = %v, want context.Canceled", err)
	}
	for _, future := range queued {
		if err := waitFuture(t, future); !errors.Is(err, context.Canceled) {
			t.Fatalf("queued ObserverFuture.Wait() error = %v, want context.Canceled", err)
		}
	}
	if got := queuedInvocations.Load(); got != 0 {
		t.Fatalf("queued jobs invoked during shutdown = %d, want 0", got)
	}
	if _, err := executor.Submit(context.Background(), observerTestTimeout, func(context.Context) error { return nil }); !errors.Is(err, ErrObserverExecutorStopped) {
		t.Fatalf("Submit() after shutdown error = %v, want ErrObserverExecutorStopped", err)
	}
}

func TestObserverExecutorRejectsInvalidLifecycleAndJobs(t *testing.T) {
	executor, err := NewObserverExecutor(1, 1)
	if err != nil {
		t.Fatalf("NewObserverExecutor() error = %v", err)
	}
	if _, err := executor.Submit(context.Background(), time.Second, func(context.Context) error { return nil }); !errors.Is(err, ErrObserverExecutorNotRunning) {
		t.Fatalf("Submit() before Start error = %v, want ErrObserverExecutorNotRunning", err)
	}

	started, cancel, done := startTestObserverExecutor(t, 1, 1)
	if err := started.Start(context.Background()); !errors.Is(err, ErrObserverExecutorAlreadyStarted) {
		t.Fatalf("second Start() error = %v, want ErrObserverExecutorAlreadyStarted", err)
	}
	if _, err := started.Submit(context.Background(), 0, func(context.Context) error { return nil }); err == nil {
		t.Fatal("Submit() with zero timeout error = nil")
	}
	if _, err := started.Submit(context.Background(), time.Second, nil); err == nil {
		t.Fatal("Submit() with nil job error = nil")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(observerTestTimeout):
		t.Fatal("ObserverExecutor.Start() did not stop")
	}
}

func TestObserverFutureSupportsConcurrentWaiters(t *testing.T) {
	executor, _, _ := startTestObserverExecutor(t, 1, 1)
	release := make(chan struct{})
	future, err := executor.Submit(context.Background(), observerTestTimeout, func(context.Context) error {
		<-release
		return nil
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	const waiterCount = 4
	results := make(chan error, waiterCount)
	var waiters sync.WaitGroup
	waiters.Add(waiterCount)
	for range waiterCount {
		go func() {
			defer waiters.Done()
			results <- future.Wait(context.Background())
		}()
	}
	close(release)
	waiters.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("ObserverFuture.Wait() error = %v", err)
		}
	}
}
