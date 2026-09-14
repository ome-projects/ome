package process

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
)

const (
	testManifestLimit   = 1 << 20
	testConfigLimit     = 1 << 20
	testRequestLimit    = 16 << 20
	testResultLimit     = 16 << 20
	testProfileLimit    = 64 << 10
	testStderrLimit     = 64 << 10
	ordinaryTestTimeout = 10 * time.Second
)

type helperConfig struct {
	SchedulerName    string `json:"schedulerName"`
	SchedulerVersion string `json:"schedulerVersion"`
	ConfigurationID  string `json:"configurationID"`
	GangScheduling   bool   `json:"gangScheduling"`
	Mode             string `json:"mode,omitempty"`
	DelayMillis      int    `json:"delayMillis,omitempty"`
	ExpectedTimeout  string `json:"expectedTimeout,omitempty"`
}

var helperBinary string

func TestMain(m *testing.M) {
	tempDir, err := os.MkdirTemp("", "alfred-process-worker-")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "create helper directory: %v\n", err)
		os.Exit(1)
	}
	helperBinary = filepath.Join(tempDir, "worker.test")
	sourcePath := filepath.Join(tempDir, "main.go")
	if err := os.WriteFile(sourcePath, []byte(helperSource), 0o600); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "write helper worker: %v\n", err)
		_ = os.RemoveAll(tempDir)
		os.Exit(1)
	}
	command := exec.Command("go", "build", "-o", helperBinary, sourcePath)
	command.Stdout = os.Stderr
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "build helper worker: %v\n", err)
		_ = os.RemoveAll(tempDir)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(tempDir)
	os.Exit(code)
}

const helperSource = `package main

import (
	"encoding/json"
	"flag"
	"io"
	"os"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/alfred/scheduling"
)

type config struct {
	SchedulerName string
	SchedulerVersion string
	ConfigurationID string
	GangScheduling bool
	Mode string
	DelayMillis int
	ExpectedTimeout string
}

func main() {
	flags := flag.NewFlagSet("worker", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var backend, configPath, timeout string
	var printProfile bool
	flags.StringVar(&backend, "backend", "", "")
	flags.StringVar(&configPath, "scheduler-config", "", "")
	flags.StringVar(&timeout, "timeout", "", "")
	flags.BoolVar(&printProfile, "print-profile", false, "")
	if err := flags.Parse(os.Args[1:]); err != nil || flags.NArg() != 0 || backend == "" || configPath == "" || timeout == "" {
		os.Exit(2)
	}
	contents, err := os.ReadFile(configPath)
	if err != nil { os.Exit(3) }
	var cfg config
	if err := json.Unmarshal(contents, &cfg); err != nil { os.Exit(4) }
	identity := scheduling.ProfileIdentity{SchedulerName: cfg.SchedulerName, Backend: backend, SchedulerVersion: cfg.SchedulerVersion, ConfigurationID: cfg.ConfigurationID}
	if printProfile {
		switch cfg.Mode {
		case "profile-null":
			_, _ = io.WriteString(os.Stdout, "null\n"); return
		case "profile-unknown":
			write(map[string]any{"identity": identity, "gangScheduling": cfg.GangScheduling, "extra": true}); return
		case "profile-trailing":
			write(map[string]any{"identity": identity, "gangScheduling": cfg.GangScheduling}); _, _ = io.WriteString(os.Stdout, "{}\n"); return
		case "profile-oversized":
			_, _ = io.WriteString(os.Stdout, strings.Repeat("x", (64 << 10) + 1)); return
		case "profile-mismatch":
			identity.ConfigurationID += "-different"
		case "gang-mismatch":
			cfg.GangScheduling = !cfg.GangScheduling
		}
		write(map[string]any{"identity": identity, "gangScheduling": cfg.GangScheduling}); return
	}
	if cfg.ExpectedTimeout != "" && cfg.ExpectedTimeout != timeout { os.Exit(5) }
	if cfg.Mode == "timeout-unsupported" {
		delay, err := time.ParseDuration(timeout); if err != nil { os.Exit(10) }; time.Sleep(delay)
	} else if cfg.DelayMillis > 0 {
		time.Sleep(time.Duration(cfg.DelayMillis) * time.Millisecond)
	}
	var request scheduling.Request
	if err := json.NewDecoder(os.Stdin).Decode(&request); err != nil { os.Exit(6) }
	result := scheduling.Result{SchemaVersion: request.SchemaVersion, RequestID: request.RequestID, SnapshotID: request.SnapshotID, SnapshotTime: request.SnapshotTime, Profile: request.Profile, Decision: scheduling.DecisionInfeasible, Reason: scheduling.SimulationReasonNoFeasiblePlacement}
	switch cfg.Mode {
	case "timeout-unsupported":
		result.Decision = scheduling.DecisionUnsupported; result.Reason = scheduling.SimulationReasonUnsupported
	case "result-null":
		_, _ = io.WriteString(os.Stdout, "null\n"); return
	case "result-unknown":
		var object map[string]any; encoded, _ := json.Marshal(result); _ = json.Unmarshal(encoded, &object); object["extra"] = true; write(object); return
	case "result-trailing":
		write(result); _, _ = io.WriteString(os.Stdout, "{}\n"); return
	case "result-oversized":
		_, _ = io.WriteString(os.Stdout, strings.Repeat("x", (16 << 20) + 1)); return
	case "stderr-oversized":
		_, _ = io.WriteString(os.Stderr, strings.Repeat("secret", (64 << 10) / 6 + 2)); write(result); return
	case "environment-check":
		if len(os.Environ()) != 0 { _, _ = io.WriteString(os.Stderr, "environment leaked"); os.Exit(7) }
	case "nonzero-valid":
		write(result); os.Exit(8)
	case "result-profile-mismatch":
		result.Profile.ConfigurationID += "-drifted"
	case "negative-bad-reason":
		result.Reason = scheduling.SimulationReasonUnsupported
	}
	write(result)
}

func write(value any) {
	if err := json.NewEncoder(os.Stdout).Encode(value); err != nil { os.Exit(9) }
}
`

func mustJSON(value any) string {
	contents, _ := json.Marshal(value)
	return string(contents)
}

func TestNewRegistrySelectsExactIdentityAndIsImmutable(t *testing.T) {
	first := newBackend(t, "worker-a", helperConfig{SchedulerName: "scheduler-a", SchedulerVersion: "v1", ConfigurationID: "config-a"})
	second := newBackend(t, "worker-b", helperConfig{SchedulerName: "scheduler-b", SchedulerVersion: "v2", ConfigurationID: "config-b"})
	entries := []Backend{first, second}
	registry, err := NewRegistry(context.Background(), entries, ordinaryTestTimeout)
	if err != nil {
		t.Fatal(err)
	}
	entries[1] = first

	request := validRequest(second.Identity, false)
	if _, err := registry.Evaluate(context.Background(), request); err != nil {
		t.Fatalf("Evaluate() = %v, want exact second backend", err)
	}
	request.Profile.ConfigurationID = "config-unknown"
	if _, err := registry.Evaluate(context.Background(), request); err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("Evaluate() error = %v, want exact lookup rejection", err)
	}
}

func TestNewRegistryRejectsInvalidTimeoutEntriesAndProbeClaims(t *testing.T) {
	valid := newBackend(t, "worker", helperConfig{SchedulerName: "scheduler", SchedulerVersion: "v1", ConfigurationID: "config"})
	tests := []struct {
		name    string
		entries []Backend
		timeout time.Duration
		wantErr string
	}{
		{name: "zero timeout", entries: []Backend{valid}, wantErr: "timeout"},
		{name: "timeout too large", entries: []Backend{valid}, timeout: time.Minute + time.Nanosecond, wantErr: "timeout"},
		{name: "duplicate", entries: []Backend{valid, valid}, timeout: ordinaryTestTimeout, wantErr: "duplicate"},
		{name: "too many workers", entries: repeatBackend(valid, 33), timeout: ordinaryTestTimeout, wantErr: "32"},
		{name: "profile mismatch", entries: []Backend{newBackend(t, "profile-mismatch", helperConfig{SchedulerName: "scheduler", SchedulerVersion: "v1", ConfigurationID: "config", Mode: "profile-mismatch"})}, timeout: ordinaryTestTimeout, wantErr: "identity"},
		{name: "gang mismatch", entries: []Backend{newBackend(t, "gang-mismatch", helperConfig{SchedulerName: "scheduler", SchedulerVersion: "v1", ConfigurationID: "config", GangScheduling: true, Mode: "gang-mismatch"})}, timeout: ordinaryTestTimeout, wantErr: "gang"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewRegistry(context.Background(), tc.entries, tc.timeout)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("NewRegistry() error = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestLoadStrictManifest(t *testing.T) {
	backend := newBackend(t, "worker", helperConfig{SchedulerName: "scheduler", SchedulerVersion: "v1", ConfigurationID: "config"})
	validManifest := mustJSON(struct {
		Workers []Backend `json:"workers"`
	}{[]Backend{backend}})
	tests := []struct {
		name     string
		contents string
		wantErr  string
	}{
		{name: "valid", contents: validManifest},
		{name: "unknown field", contents: strings.TrimSuffix(validManifest, "}") + `,"extra":true}`, wantErr: "unknown"},
		{name: "trailing object", contents: validManifest + `{}`, wantErr: "single"},
		{name: "null", contents: "null", wantErr: "null"},
		{name: "oversized", contents: strings.Repeat(" ", testManifestLimit+1), wantErr: "large"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "workers.json")
			if err := os.WriteFile(path, []byte(tc.contents), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(context.Background(), path, ordinaryTestTimeout)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Load() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), tc.wantErr) {
				t.Fatalf("Load() error = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestNewRegistryRejectsInvalidFiles(t *testing.T) {
	valid := newBackend(t, "worker", helperConfig{SchedulerName: "scheduler", SchedulerVersion: "v1", ConfigurationID: "config"})
	missing := valid
	missing.SchedulerConfigPath = filepath.Join(t.TempDir(), "missing")
	oversized := valid
	oversized.SchedulerConfigPath = filepath.Join(t.TempDir(), "large-config")
	if err := os.WriteFile(oversized.SchedulerConfigPath, make([]byte, testConfigLimit+1), 0o600); err != nil {
		t.Fatal(err)
	}
	relative := valid
	relative.BinaryPath = "relative-worker"
	nonExecutable := valid
	nonExecutable.BinaryPath = filepath.Join(t.TempDir(), "worker")
	if err := os.WriteFile(nonExecutable.BinaryPath, []byte("worker"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		backend Backend
		wantErr string
	}{
		{name: "missing config", backend: missing, wantErr: "configuration"},
		{name: "oversized config", backend: oversized, wantErr: "large"},
		{name: "relative binary", backend: relative, wantErr: "absolute"},
		{name: "non executable binary", backend: nonExecutable, wantErr: "executable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewRegistry(context.Background(), []Backend{tc.backend}, ordinaryTestTimeout)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), tc.wantErr) {
				t.Fatalf("NewRegistry() error = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestProbeStrictOutputAndLimits(t *testing.T) {
	for _, tc := range []struct {
		mode    string
		wantErr string
	}{
		{mode: "profile-null", wantErr: "null"},
		{mode: "profile-unknown", wantErr: "unknown"},
		{mode: "profile-trailing", wantErr: "single"},
		{mode: "profile-oversized", wantErr: "large"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			backend := newBackend(t, tc.mode, helperConfig{SchedulerName: "scheduler", SchedulerVersion: "v1", ConfigurationID: "config", Mode: tc.mode})
			_, err := NewRegistry(context.Background(), []Backend{backend}, ordinaryTestTimeout)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), tc.wantErr) {
				t.Fatalf("NewRegistry() error = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestEvaluateRejectsGangMismatchAndConfigDrift(t *testing.T) {
	backend := newBackend(t, "worker", helperConfig{SchedulerName: "scheduler", SchedulerVersion: "v1", ConfigurationID: "config"})
	registry, err := NewRegistry(context.Background(), []Backend{backend}, ordinaryTestTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Evaluate(context.Background(), validRequest(backend.Identity, true)); err == nil || !strings.Contains(strings.ToLower(err.Error()), "gang") {
		t.Fatalf("Evaluate() error = %v, want gang rejection", err)
	}
	if err := os.WriteFile(backend.SchedulerConfigPath, make([]byte, testConfigLimit+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Evaluate(context.Background(), validRequest(backend.Identity, false)); err == nil || !strings.Contains(strings.ToLower(err.Error()), "large") {
		t.Fatalf("Evaluate() error = %v, want config size rejection", err)
	}
}

func TestEvaluateStrictResultAndWorkerFailures(t *testing.T) {
	for _, tc := range []struct {
		mode       string
		wantErr    string
		secretText string
	}{
		{mode: "result-null", wantErr: "null"},
		{mode: "result-unknown", wantErr: "unknown"},
		{mode: "result-trailing", wantErr: "single"},
		{mode: "result-oversized", wantErr: "large"},
		{mode: "stderr-oversized", wantErr: "stderr", secretText: "secretsecret"},
		{mode: "nonzero-valid", wantErr: "worker"},
		{mode: "result-profile-mismatch", wantErr: "profile identity"},
		{mode: "negative-bad-reason", wantErr: "invalid reason"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			backend := newBackend(t, tc.mode, helperConfig{SchedulerName: "scheduler", SchedulerVersion: "v1", ConfigurationID: "config", Mode: tc.mode})
			registry, err := NewRegistry(context.Background(), []Backend{backend}, ordinaryTestTimeout)
			if err != nil {
				t.Fatal(err)
			}
			_, err = registry.Evaluate(context.Background(), validRequest(backend.Identity, false))
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), tc.wantErr) {
				t.Fatalf("Evaluate() error = %v, want mention of %q", err, tc.wantErr)
			}
			if tc.secretText != "" && strings.Contains(err.Error(), tc.secretText) {
				t.Fatalf("Evaluate() leaked raw stderr: %v", err)
			}
		})
	}
}

func TestEvaluateRejectsOversizedRequest(t *testing.T) {
	backend := newBackend(t, "worker", helperConfig{SchedulerName: "scheduler", SchedulerVersion: "v1", ConfigurationID: "config"})
	registry, err := NewRegistry(context.Background(), []Backend{backend}, ordinaryTestTimeout)
	if err != nil {
		t.Fatal(err)
	}
	request := validRequest(backend.Identity, false)
	request.ReplacementPods[0].Annotations = map[string]string{"large": strings.Repeat("x", testRequestLimit)}
	if _, err := registry.Evaluate(context.Background(), request); err == nil || !strings.Contains(strings.ToLower(err.Error()), "large") {
		t.Fatalf("Evaluate() error = %v, want request size rejection", err)
	}
}

func TestEvaluateUsesEmptyEnvironmentAndFixedTimeout(t *testing.T) {
	backend := newBackend(t, "environment-check", helperConfig{SchedulerName: "scheduler", SchedulerVersion: "v1", ConfigurationID: "config", Mode: "environment-check", ExpectedTimeout: "8s"})
	registry, err := NewRegistry(context.Background(), []Backend{backend}, ordinaryTestTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("ALFRED_TEST_SECRET", "must-not-reach-worker")
	if _, err := registry.Evaluate(context.Background(), validRequest(backend.Identity, false)); err != nil {
		t.Fatalf("Evaluate() = %v, want empty worker environment and fixed timeout", err)
	}
}

func TestEvaluateAllowsWorkerTimeoutResponseBeforeHardDeadline(t *testing.T) {
	backend := newBackend(t, "timeout-unsupported", helperConfig{
		SchedulerName: "scheduler", SchedulerVersion: "v1", ConfigurationID: "config",
		Mode: "timeout-unsupported", ExpectedTimeout: "400ms",
	})
	registry, err := NewRegistry(context.Background(), []Backend{backend}, ordinaryTestTimeout)
	if err != nil {
		t.Fatal(err)
	}
	registry.timeout = 500 * time.Millisecond
	result, err := registry.Evaluate(context.Background(), validRequest(backend.Identity, false))
	if err != nil {
		t.Fatalf("Evaluate() = %v, want valid timeout response", err)
	}
	if result.Decision != scheduling.DecisionUnsupported {
		t.Fatalf("Evaluate() decision = %q, want %q", result.Decision, scheduling.DecisionUnsupported)
	}
}

func TestEvaluateAllowsWorkerTimeoutResponseBeforeEarlierCallerDeadline(t *testing.T) {
	backend := newBackend(t, "caller-timeout-unsupported", helperConfig{
		SchedulerName: "scheduler", SchedulerVersion: "v1", ConfigurationID: "config",
		Mode: "timeout-unsupported",
	})
	registry, err := NewRegistry(context.Background(), []Backend{backend}, ordinaryTestTimeout)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := registry.Evaluate(ctx, validRequest(backend.Identity, false))
	if err != nil {
		t.Fatalf("Evaluate() = %v, want valid response before earlier caller deadline", err)
	}
	if result.Decision != scheduling.DecisionUnsupported {
		t.Fatalf("Evaluate() decision = %q, want %q", result.Decision, scheduling.DecisionUnsupported)
	}
}

func TestEvaluateHonorsCancellationTimeoutAndSemaphore(t *testing.T) {
	t.Run("registry timeout", func(t *testing.T) {
		backend := newBackend(t, "slow", helperConfig{SchedulerName: "scheduler", SchedulerVersion: "v1", ConfigurationID: "config", DelayMillis: 1000})
		registry, err := NewRegistry(context.Background(), []Backend{backend}, ordinaryTestTimeout)
		if err != nil {
			t.Fatal(err)
		}
		registry.timeout = 50 * time.Millisecond
		started := time.Now()
		if _, err := registry.Evaluate(context.Background(), validRequest(backend.Identity, false)); err == nil || !strings.Contains(strings.ToLower(err.Error()), "deadline") {
			t.Fatalf("Evaluate() error = %v, want deadline", err)
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("Evaluate() took %s after deadline", elapsed)
		}
	})

	t.Run("caller cancellation", func(t *testing.T) {
		backend := newBackend(t, "slow", helperConfig{SchedulerName: "scheduler", SchedulerVersion: "v1", ConfigurationID: "config", DelayMillis: 1000})
		registry, err := NewRegistry(context.Background(), []Backend{backend}, ordinaryTestTimeout)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := registry.Evaluate(ctx, validRequest(backend.Identity, false)); err == nil || !strings.Contains(strings.ToLower(err.Error()), "cancel") {
			t.Fatalf("Evaluate() error = %v, want cancellation", err)
		}
	})

	t.Run("one process at a time", func(t *testing.T) {
		backend := newBackend(t, "serialized", helperConfig{SchedulerName: "scheduler", SchedulerVersion: "v1", ConfigurationID: "config", DelayMillis: 150})
		registry, err := NewRegistry(context.Background(), []Backend{backend}, ordinaryTestTimeout)
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := registry.Evaluate(context.Background(), validRequest(backend.Identity, false))
				errs <- err
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		if elapsed := time.Since(started); elapsed < 250*time.Millisecond {
			t.Fatalf("two evaluations completed concurrently in %s", elapsed)
		}
	})
}

func newBackend(t *testing.T, backend string, config helperConfig) Backend {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "scheduler-config.json")
	contents, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	return Backend{
		BinaryPath:          helperBinary,
		SchedulerConfigPath: configPath,
		Identity: scheduling.ProfileIdentity{
			SchedulerName:    config.SchedulerName,
			Backend:          backend,
			SchedulerVersion: config.SchedulerVersion,
			ConfigurationID:  config.ConfigurationID,
		},
		GangScheduling: config.GangScheduling,
	}
}

func repeatBackend(backend Backend, count int) []Backend {
	result := make([]Backend, count)
	for i := range result {
		result[i] = backend
		result[i].Identity.Backend = fmt.Sprintf("worker-%d", i)
	}
	return result
}

func validRequest(profile scheduling.ProfileIdentity, requireGang bool) scheduling.Request {
	return scheduling.Request{
		SchemaVersion: scheduling.SimulationSchemaV1,
		RequestID:     "request-current",
		Profile:       profile,
		ReplacementPods: []corev1.Pod{{
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "replacement", UID: types.UID("replacement-uid")},
			Spec:       corev1.PodSpec{SchedulerName: profile.SchedulerName},
		}},
		SourcePods: []corev1.Pod{{
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "source", UID: types.UID("source-uid")},
			Spec:       corev1.PodSpec{NodeName: "source-node"},
		}},
		SnapshotID:    "snapshot-current",
		SnapshotTime:  metav1.NewTime(time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)),
		RequireGang:   requireGang,
		ExcludedNodes: []string{"source-node"},
	}
}
