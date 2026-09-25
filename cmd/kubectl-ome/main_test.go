package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

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
