package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"k8s.io/cli-runtime/pkg/genericiooptions"
)

func TestRunIsTestableWithoutProcessExit(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	streams := genericiooptions.IOStreams{
		In:     strings.NewReader(""),
		Out:    &stdout,
		ErrOut: &stderr,
	}
	if code := run([]string{"--help"}, streams); code != 0 {
		t.Fatalf("run(--help) = %d, want 0; stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "kubectl ome <command>") {
		t.Fatalf("stdout = %q, want root help", stdout.String())
	}
}

func TestRunReturnsErrorCodeWithoutExiting(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	streams := genericiooptions.IOStreams{
		In:     strings.NewReader(""),
		Out:    &bytes.Buffer{},
		ErrOut: &stderr,
	}
	if code := run([]string{"not-a-command"}, streams); code != 1 {
		t.Fatalf("run(bad command) = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "error: unknown command") {
		t.Fatalf("stderr = %q, want unknown-command diagnostic", stderr.String())
	}
}

func TestRunContextPropagatesCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	streams := genericiooptions.IOStreams{Out: &stdout, ErrOut: &stderr}
	args := []string{"--kubeconfig=" + t.TempDir() + "/missing", "cluster", "status"}
	if code := runContext(ctx, args, streams); code != 1 {
		t.Fatalf("runContext() = %d, want 1", code)
	}
	if stdout.Len() != 0 || stderr.String() != "error: context canceled\n" {
		t.Fatalf("canceled command stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestRunWithSignalContextStopsBeforeReturning(t *testing.T) {
	t.Parallel()

	for _, canceled := range []bool{false, true} {
		name := "success"
		if canceled {
			name = "cancellation"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var stdout, stderr bytes.Buffer
			streams := genericiooptions.IOStreams{Out: &stdout, ErrOut: &stderr}
			args := []string{"--help"}
			wantCode, wantStderr := 0, ""
			if canceled {
				args = []string{"--kubeconfig=" + t.TempDir() + "/missing", "cluster", "status"}
				wantCode, wantStderr = 1, "error: context canceled\n"
			}
			notifyCalls, stopCalls := 0, 0
			notify := func(parent context.Context, signals ...os.Signal) (context.Context, context.CancelFunc) {
				notifyCalls++
				if parent != context.Background() || len(signals) != 2 || signals[0] != os.Interrupt || signals[1] != syscall.SIGTERM {
					t.Fatalf("unexpected signal registration: parent=%v signals=%v", parent, signals)
				}
				ctx, cancel := context.WithCancel(parent)
				t.Cleanup(cancel)
				if canceled {
					cancel()
				}
				return ctx, func() {
					stopCalls++
					cancel()
				}
			}
			if code := runWithSignalContext(args, streams, notify, runContext); code != wantCode {
				t.Fatalf("runWithSignalContext() = %d, want %d", code, wantCode)
			}
			if notifyCalls != 1 || stopCalls != 1 {
				t.Fatalf("notify calls = %d, stop calls = %d; want one each before return", notifyCalls, stopCalls)
			}
			if stderr.String() != wantStderr {
				t.Fatalf("stderr = %q, want %q", stderr.String(), wantStderr)
			}
		})
	}
}

func TestRunWithSignalContextStopsBeforeCanceledRunnerReturns(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, stopped := make(chan struct{}), make(chan struct{})
	release, exited := make(chan struct{}), make(chan struct{})
	var stopCalls atomic.Int32
	notify := func(context.Context, ...os.Signal) (context.Context, context.CancelFunc) {
		return ctx, func() {
			if stopCalls.Add(1) == 1 {
				close(stopped)
			}
			cancel()
		}
	}
	runner := func(got context.Context, _ []string, _ genericiooptions.IOStreams) int {
		if got != ctx {
			t.Error("runner did not receive signal context")
		}
		close(started)
		<-release
		return 17
	}
	var code int
	go func() {
		code = runWithSignalContext(nil, genericiooptions.IOStreams{}, notify, runner)
		close(exited)
	}()
	t.Cleanup(func() {
		close(release)
		select {
		case <-exited:
			if code != 17 || stopCalls.Load() != 1 {
				t.Errorf("code=%d stop calls=%d, want 17 and 1", code, stopCalls.Load())
			}
		case <-time.After(5 * time.Second):
			t.Error("signal helper did not finish after runner was released")
		}
	})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not start")
	}
	select {
	case <-stopped:
		t.Fatal("signal notification stopped before cancellation or runner completion")
	default:
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("signal notification was not stopped while canceled runner remained blocked")
	}
	select {
	case <-exited:
		t.Fatal("signal helper returned before runner was released")
	default:
	}
}
