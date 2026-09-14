// Package process runs trusted scheduler simulator binaries behind a strict,
// bounded subprocess protocol. The configured executable is part of the
// security boundary; this package does not provide credential-safe OS
// isolation.
package process

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/ome/pkg/alfred/scheduling"
)

const (
	maxTimeout       = time.Minute
	maxWorkers       = 32
	maxManifestBytes = 1 << 20
	maxConfigBytes   = 1 << 20
	maxRequestBytes  = 16 << 20
	maxResultBytes   = 16 << 20
	maxProfileBytes  = 64 << 10
	maxStderrBytes   = 64 << 10
	workerWaitDelay  = 2 * time.Second
)

// Backend binds an exact scheduler profile identity to one trusted worker.
// Both paths must come from immutable, trusted deployment configuration, not
// from Alfred's mutable policy ConfigMap.
type Backend struct {
	BinaryPath          string                     `json:"binaryPath"`
	SchedulerConfigPath string                     `json:"schedulerConfigPath"`
	Identity            scheduling.ProfileIdentity `json:"identity"`
	GangScheduling      bool                       `json:"gangScheduling"`
}

type manifest struct {
	Workers []Backend `json:"workers"`
}

// Registry is an immutable set of probed workers. The semaphore bounds the
// registry to one running worker process at a time.
type Registry struct {
	workers map[scheduling.ProfileIdentity]Backend
	timeout time.Duration
	worker  chan struct{}
}

type profileOutput struct {
	Identity       scheduling.ProfileIdentity `json:"identity"`
	GangScheduling bool                       `json:"gangScheduling"`
}

// Load strictly reads a trusted startup manifest and probes every worker.
func Load(ctx context.Context, path string, timeout time.Duration) (*Registry, error) {
	contents, err := readRegularFile(path, maxManifestBytes, "worker manifest", false)
	if err != nil {
		return nil, err
	}
	decoded, err := decodeStrictObject[manifest](contents, "worker manifest")
	if err != nil {
		return nil, err
	}
	return NewRegistry(ctx, decoded.Workers, timeout)
}

// NewRegistry validates and probes a bounded, immutable worker set.
func NewRegistry(ctx context.Context, entries []Backend, timeout time.Duration) (*Registry, error) {
	if timeout <= 0 || timeout > maxTimeout {
		return nil, fmt.Errorf("worker timeout must be positive and no greater than %s", maxTimeout)
	}
	if len(entries) > maxWorkers {
		return nil, fmt.Errorf("worker registry has %d entries; maximum is %d", len(entries), maxWorkers)
	}
	workers := make(map[scheduling.ProfileIdentity]Backend, len(entries))
	for i := range entries {
		entry := entries[i]
		if err := validateIdentity(entry.Identity); err != nil {
			return nil, fmt.Errorf("worker %d identity: %w", i, err)
		}
		if _, exists := workers[entry.Identity]; exists {
			return nil, fmt.Errorf("duplicate worker profile identity at index %d", i)
		}
		if _, err := readRegularFile(entry.BinaryPath, 0, "worker binary", true); err != nil {
			return nil, fmt.Errorf("worker %d: %w", i, err)
		}
		if _, err := readRegularFile(entry.SchedulerConfigPath, maxConfigBytes, "scheduler configuration", false); err != nil {
			return nil, fmt.Errorf("worker %d: %w", i, err)
		}
		workers[entry.Identity] = entry
	}

	registry := &Registry{workers: workers, timeout: timeout, worker: make(chan struct{}, 1)}
	for i := range entries {
		if err := registry.probe(ctx, entries[i]); err != nil {
			return nil, fmt.Errorf("probe worker %d: %w", i, err)
		}
	}
	return registry, nil
}

// Evaluate selects only an exact identity match and validates the complete
// response, including negative decisions, before returning it.
func (r *Registry) Evaluate(ctx context.Context, request scheduling.Request) (scheduling.Result, error) {
	entry, ok := r.workers[request.Profile]
	if !ok {
		return scheduling.Result{}, fmt.Errorf("scheduler profile identity is not registered")
	}
	if request.RequireGang && !entry.GangScheduling {
		return scheduling.Result{}, fmt.Errorf("scheduler profile does not support gang scheduling")
	}
	if _, err := readRegularFile(entry.SchedulerConfigPath, maxConfigBytes, "scheduler configuration", false); err != nil {
		return scheduling.Result{}, err
	}

	requestBytes, err := encodeBounded(request, maxRequestBytes, "simulation request")
	if err != nil {
		return scheduling.Result{}, err
	}
	if err := r.acquire(ctx); err != nil {
		return scheduling.Result{}, err
	}
	defer r.release()
	processTimeout, err := effectiveProcessTimeout(ctx, r.timeout)
	if err != nil {
		return scheduling.Result{}, err
	}
	// The worker's evaluation budget is shorter than the parent hard deadline
	// so it can serialize a negative timeout response and exit cleanly.
	evaluationTimeout := processTimeout - processTimeout/5

	args := []string{
		"--backend", entry.Identity.Backend,
		"--scheduler-config", entry.SchedulerConfigPath,
		"--timeout", evaluationTimeout.String(),
	}
	stdout, err := run(ctx, entry.BinaryPath, args, requestBytes, processTimeout, maxResultBytes)
	if err != nil {
		return scheduling.Result{}, err
	}
	result, err := decodeStrictObject[scheduling.Result](stdout, "simulation result")
	if err != nil {
		return scheduling.Result{}, err
	}
	if err := scheduling.ValidateResponse(request, result); err != nil {
		return scheduling.Result{}, fmt.Errorf("validate simulation result: %w", err)
	}
	return result, nil
}

func effectiveProcessTimeout(ctx context.Context, configured time.Duration) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("scheduler worker: %w", err)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return configured, nil
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0, fmt.Errorf("scheduler worker: %w", context.DeadlineExceeded)
	}
	if remaining < configured {
		return remaining, nil
	}
	return configured, nil
}

func (r *Registry) probe(ctx context.Context, entry Backend) error {
	if err := r.acquire(ctx); err != nil {
		return err
	}
	defer r.release()
	args := []string{
		"--backend", entry.Identity.Backend,
		"--scheduler-config", entry.SchedulerConfigPath,
		"--timeout", r.timeout.String(),
		"--print-profile",
	}
	stdout, err := run(ctx, entry.BinaryPath, args, nil, r.timeout, maxProfileBytes)
	if err != nil {
		return err
	}
	profile, err := decodeStrictObject[profileOutput](stdout, "worker profile")
	if err != nil {
		return err
	}
	if profile.Identity != entry.Identity {
		return fmt.Errorf("worker profile identity does not match registry entry")
	}
	if profile.GangScheduling != entry.GangScheduling {
		return fmt.Errorf("worker gang scheduling claim does not match registry entry")
	}
	return nil
}

func (r *Registry) acquire(ctx context.Context) error {
	select {
	case r.worker <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for scheduler worker: %w", ctx.Err())
	}
}

func (r *Registry) release() {
	<-r.worker
}

func run(parent context.Context, binary string, args []string, stdin []byte, timeout time.Duration, stdoutLimit int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	stdout := newBoundedWriter(stdoutLimit, cancel)
	stderr := newBoundedWriter(maxStderrBytes, cancel)
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = []string{}
	cmd.Stdin = bytes.NewReader(stdin)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.WaitDelay = workerWaitDelay
	err := cmd.Run()
	if stdout.overflowed() {
		return nil, fmt.Errorf("worker stdout is too large")
	}
	if stderr.overflowed() {
		return nil, fmt.Errorf("worker stderr is too large")
	}
	if parent.Err() != nil {
		return nil, fmt.Errorf("scheduler worker: %w", parent.Err())
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("scheduler worker: %w", ctx.Err())
	}
	if err != nil {
		return nil, fmt.Errorf("scheduler worker exited unsuccessfully")
	}
	return stdout.bytes(), nil
}

type boundedWriter struct {
	mu       sync.Mutex
	buffer   bytes.Buffer
	limit    int64
	overflow bool
	cancel   context.CancelFunc
}

func newBoundedWriter(limit int64, cancel context.CancelFunc) *boundedWriter {
	return &boundedWriter{limit: limit, cancel: cancel}
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	remaining := w.limit - int64(w.buffer.Len())
	if remaining > 0 {
		keep := int64(len(p))
		if keep > remaining {
			keep = remaining
		}
		_, _ = w.buffer.Write(p[:int(keep)])
	}
	if int64(len(p)) > remaining {
		w.overflow = true
		if w.cancel != nil {
			w.cancel()
		}
	}
	return len(p), nil
}

func (w *boundedWriter) overflowed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.overflow
}

func (w *boundedWriter) bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return bytes.Clone(w.buffer.Bytes())
}

func encodeBounded(value any, limit int64, label string) ([]byte, error) {
	writer := newBoundedWriter(limit, nil)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		return nil, fmt.Errorf("encode %s: %w", label, err)
	}
	if writer.overflowed() {
		return nil, fmt.Errorf("%s is too large", label)
	}
	return writer.bytes(), nil
}

func decodeStrictObject[T any](contents []byte, label string) (T, error) {
	var value T
	if bytes.Equal(bytes.TrimSpace(contents), []byte("null")) {
		return value, fmt.Errorf("%s must not be null", label)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("decode %s: %w", label, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err != nil {
			return value, fmt.Errorf("decode %s trailing data: %w", label, err)
		}
		return value, fmt.Errorf("%s must contain a single JSON object", label)
	}
	return value, nil
}

func readRegularFile(path string, limit int64, label string, executable bool) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("%s path must be absolute", label)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect %s: %w", label, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s path must name a regular file", label)
	}
	if executable && info.Mode().Perm()&0o111 == 0 {
		return nil, fmt.Errorf("worker binary is not executable")
	}
	if limit > 0 && info.Size() > limit {
		return nil, fmt.Errorf("%s is too large", label)
	}
	if limit == 0 {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", label, err)
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", label, err)
	}
	if int64(len(contents)) > limit {
		return nil, fmt.Errorf("%s is too large", label)
	}
	return contents, nil
}

func validateIdentity(identity scheduling.ProfileIdentity) error {
	for name, value := range map[string]string{
		"scheduler name":    identity.SchedulerName,
		"backend":           identity.Backend,
		"scheduler version": identity.SchedulerVersion,
		"configuration ID":  identity.ConfigurationID,
	} {
		if value == "" || strings.TrimSpace(value) != value {
			return fmt.Errorf("%s must be non-empty and unpadded", name)
		}
	}
	return nil
}
