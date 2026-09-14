package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/alfred-simulator/protocol"
	"sigs.k8s.io/ome/alfred-simulator/worker"
)

const testBackend = "test-default-worker"

type rejectingReader struct{}

func (rejectingReader) Read([]byte) (int, error) {
	return 0, errors.New("stdin must not be read")
}

func TestRunPrintProfileDoesNotReadRequest(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"--backend", testBackend,
		"--scheduler-config", defaultConfigPath(),
		"--print-profile",
	}, rejectingReader{}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run returned %d, stderr: %s", code, stderr.String())
	}

	var got profileOutput
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode profile output: %v\noutput: %s", err, stdout.String())
	}
	if got.Identity.Backend != testBackend {
		t.Fatalf("backend = %q, want %q", got.Identity.Backend, testBackend)
	}
	if got.Identity.SchedulerName != corev1.DefaultSchedulerName {
		t.Fatalf("schedulerName = %q, want %q", got.Identity.SchedulerName, corev1.DefaultSchedulerName)
	}
	if got.Identity.SchedulerVersion != worker.SchedulerVersion {
		t.Fatalf("schedulerVersion = %q, want %q", got.Identity.SchedulerVersion, worker.SchedulerVersion)
	}
	if !strings.HasPrefix(got.Identity.ConfigurationID, "sha256:") {
		t.Fatalf("configurationID = %q, want calculated sha256 identity", got.Identity.ConfigurationID)
	}
	if got.GangScheduling {
		t.Fatal("default scheduler unexpectedly advertised gang scheduling")
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr is not empty: %s", stderr.String())
	}
}

func TestRunEvaluatesOneCompleteRequest(t *testing.T) {
	configYAML, err := io.ReadAll(mustOpen(t, defaultConfigPath()))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := worker.LoadProfile(testBackend, configYAML)
	if err != nil {
		t.Fatalf("load profile: %v", err)
	}
	request := completeRequest(t, profile.Identity)
	input, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"--backend", testBackend,
		"--scheduler-config", defaultConfigPath(),
		"--timeout", "5s",
	}, bytes.NewReader(input), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run returned %d, stderr: %s", code, stderr.String())
	}

	var got protocol.Result
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode result: %v\noutput: %s", err, stdout.String())
	}
	if got.Decision != protocol.DecisionFeasible {
		t.Fatalf("decision = %q, want %q: %+v", got.Decision, protocol.DecisionFeasible, got)
	}
	if len(got.Placements) != 1 || got.Placements[0].NodeName != "destination" {
		t.Fatalf("placements = %+v, want replacement on destination", got.Placements)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr is not empty: %s", stderr.String())
	}
}

func TestRunRejectsInvalidOptions(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "missing backend",
			args: []string{"--scheduler-config", defaultConfigPath(), "--print-profile"},
			want: "backend",
		},
		{
			name: "missing scheduler config",
			args: []string{"--backend", testBackend, "--print-profile"},
			want: "scheduler-config",
		},
		{
			name: "zero timeout",
			args: []string{"--backend", testBackend, "--scheduler-config", defaultConfigPath(), "--timeout", "0s"},
			want: "timeout",
		},
		{
			name: "timeout above maximum",
			args: []string{"--backend", testBackend, "--scheduler-config", defaultConfigPath(), "--timeout", "61s"},
			want: "timeout",
		},
		{
			name: "no kubeconfig option",
			args: []string{"--backend", testBackend, "--scheduler-config", defaultConfigPath(), "--kubeconfig", "cluster.conf"},
			want: "kubeconfig",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(test.args, strings.NewReader("{}"), &stdout, &stderr); code == 0 {
				t.Fatalf("run succeeded, stdout: %s", stdout.String())
			}
			if !strings.Contains(stderr.String(), test.want) {
				t.Fatalf("stderr %q does not contain %q", stderr.String(), test.want)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout is not empty: %s", stdout.String())
			}
		})
	}
}

func TestParseOptionsDefaultsTimeout(t *testing.T) {
	got, err := parseOptions([]string{
		"--backend", testBackend,
		"--scheduler-config", defaultConfigPath(),
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if got.timeout != 10*time.Second {
		t.Fatalf("timeout = %s, want 10s", got.timeout)
	}
}

func TestRunRejectsInvalidConfiguration(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "invalid.yaml")
	if err := os.WriteFile(configPath, []byte("not: a scheduler configuration\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"--backend", testBackend,
		"--scheduler-config", configPath,
		"--print-profile",
	}, rejectingReader{}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("run accepted invalid configuration, stdout: %s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "load scheduler profile") {
		t.Fatalf("stderr %q does not contain configuration diagnostic", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout is not empty: %s", stdout.String())
	}
}

func TestRunRejectsMalformedAndOversizedInput(t *testing.T) {
	args := []string{"--backend", testBackend, "--scheduler-config", defaultConfigPath()}
	tests := []struct {
		name  string
		input io.Reader
		want  string
	}{
		{name: "malformed", input: strings.NewReader("{"), want: "decode request"},
		{name: "oversized", input: io.LimitReader(zeroReader{}, maxInputBytes+1), want: "decode request"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(args, test.input, &stdout, &stderr); code == 0 {
				t.Fatalf("run accepted %s input", test.name)
			}
			if !strings.Contains(stderr.String(), test.want) {
				t.Fatalf("stderr %q does not contain %q", stderr.String(), test.want)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout is not empty: %s", stdout.String())
			}
		})
	}
}

func TestRunContextPropagatesCancellation(t *testing.T) {
	configYAML, err := io.ReadAll(mustOpen(t, defaultConfigPath()))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := worker.LoadProfile(testBackend, configYAML)
	if err != nil {
		t.Fatalf("load profile: %v", err)
	}
	input, err := json.Marshal(completeRequest(t, profile.Identity))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var stdout, stderr bytes.Buffer
	code := runContext(ctx, []string{
		"--backend", testBackend,
		"--scheduler-config", defaultConfigPath(),
		"--timeout", "5s",
	}, bytes.NewReader(input), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run returned %d after cancellation, stderr: %s", code, stderr.String())
	}
	var got protocol.Result
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode result: %v\noutput: %s", err, stdout.String())
	}
	if got.Decision != protocol.DecisionUnsupported {
		t.Fatalf("decision = %q, want %q", got.Decision, protocol.DecisionUnsupported)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr is not empty: %s", stderr.String())
	}
}

func TestRunWithOwnedInputCancellationUnblocksRead(t *testing.T) {
	stdin, inputWriter := io.Pipe()
	t.Cleanup(func() { _ = inputWriter.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- runWithOwnedInput(ctx, []string{
			"--backend", testBackend,
			"--scheduler-config", defaultConfigPath(),
		}, stdin, &stdout, &stderr)
	}()
	cancel()

	select {
	case code := <-done:
		if code == 0 {
			t.Fatalf("run succeeded after interrupted input, stdout: %s", stdout.String())
		}
		if !strings.Contains(stderr.String(), "decode request") {
			t.Fatalf("stderr %q does not contain input diagnostic", stderr.String())
		}
		if stdout.Len() != 0 {
			t.Fatalf("stdout is not empty: %s", stdout.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not unblock request input")
	}
}

func TestRunDoesNotEncodeUnexpectedEvaluationError(t *testing.T) {
	configYAML, err := io.ReadAll(mustOpen(t, defaultConfigPath()))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := worker.LoadProfile(testBackend, configYAML)
	if err != nil {
		t.Fatalf("load profile: %v", err)
	}
	request := completeRequest(t, profile.Identity)
	request.Profile.Backend = "different-worker"
	input, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"--backend", testBackend,
		"--scheduler-config", defaultConfigPath(),
	}, bytes.NewReader(input), &stdout, &stderr)
	if code == 0 {
		t.Fatalf("run accepted profile mismatch, stdout: %s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "evaluate request") {
		t.Fatalf("stderr %q does not contain evaluation diagnostic", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout is not empty: %s", stdout.String())
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func defaultConfigPath() string {
	return filepath.Join("..", "..", "examples", "default-scheduler.yaml")
}

func mustOpen(t *testing.T, path string) io.ReadCloser {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func completeRequest(t *testing.T, identity protocol.ProfileIdentity) protocol.Request {
	t.Helper()
	resources := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("4"),
		corev1.ResourceMemory: resource.MustParse("8Gi"),
		corev1.ResourcePods:   resource.MustParse("32"),
	}
	node := func(name string) corev1.Node {
		return corev1.Node{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Node"},
			ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid")},
			Status:     corev1.NodeStatus{Capacity: resources, Allocatable: resources},
		}
	}
	pod := func(name, nodeName string) corev1.Pod {
		return corev1.Pod{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "simulation-test",
				Name:      name,
				UID:       types.UID(name + "-uid"),
			},
			Spec: corev1.PodSpec{
				SchedulerName: corev1.DefaultSchedulerName,
				NodeName:      nodeName,
				Containers: []corev1.Container{{
					Name:  "worker",
					Image: "example.invalid/worker:test",
					Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("1"),
					}},
				}},
			},
		}
	}
	source := pod("source", "source-node")
	replacement := pod("replacement", "")
	namespace := corev1.Namespace{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{Name: "simulation-test", UID: "namespace-uid"},
	}

	request := protocol.Request{
		SchemaVersion:   protocol.SchemaVersion,
		RequestID:       "cli-test-request",
		Profile:         identity,
		ReplacementPods: []corev1.Pod{replacement},
		SourcePods:      []corev1.Pod{source},
		SnapshotID:      "cli-test-snapshot",
		SnapshotTime:    metav1.Now(),
		ExcludedNodes:   []string{"source-node"},
	}
	for _, object := range []any{node("source-node"), node("destination"), namespace, source} {
		raw, err := json.Marshal(object)
		if err != nil {
			t.Fatal(err)
		}
		request.ClusterObjects = append(request.ClusterObjects, runtime.RawExtension{Raw: raw})
	}
	return request
}
