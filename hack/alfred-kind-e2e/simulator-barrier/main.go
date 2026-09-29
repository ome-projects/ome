// Command alfred-simulator-barrier is a test-image-only, fail-closed wrapper
// around the real Alfred simulator. It withholds one narrowly identified
// acceptance-test response until a localhost client releases it.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/ome/pkg/alfred/scheduling"
)

const (
	defaultChildPath      = "/alfred-simulator"
	defaultConfigPath     = "/etc/alfred-simulation/barrier.json"
	defaultListenAddress  = "127.0.0.1:18081"
	defaultControlBaseURL = "http://127.0.0.1:18081"
	defaultHoldTimeout    = 10 * time.Second
	maximumHoldTimeout    = 10 * time.Second
	maximumChildTimeout   = time.Minute
	defaultControlWait    = 180 * time.Second
	defaultControlRequest = 2 * time.Second
	defaultControlRetry   = 100 * time.Millisecond
	defaultRequestLimit   = int64(16 << 20)
	defaultResultLimit    = int64(16 << 20)
	defaultProfileLimit   = int64(64 << 10)
	defaultStderrLimit    = int64(64 << 10)
	defaultConfigLimit    = int64(64 << 10)
	defaultReleaseLimit   = int64(4 << 10)
	defaultHeldLimit      = int64(48 << 20)
	defaultReceiptLimit   = int64(64 << 10)
	childWaitDelay        = 2 * time.Second
	serverShutdownTimeout = time.Second
)

type barrierConfig struct {
	Profile   scheduling.ProfileIdentity `json:"profile"`
	Namespace string                     `json:"namespace"`
	Workload  string                     `json:"workload"`
}

type heldDocument struct {
	RequestBytes  []byte `json:"requestBytes"`
	ResultBytes   []byte `json:"resultBytes"`
	RequestSHA256 string `json:"requestSHA256"`
	ResultSHA256  string `json:"resultSHA256"`
	HeldAt        string `json:"heldAt"`
	Deadline      string `json:"deadline"`
	Nonce         string `json:"nonce"`
}

type releaseRequest struct {
	Nonce string `json:"nonce"`
}

type releaseReceipt struct {
	ReleasedAt    string `json:"releasedAt"`
	RequestSHA256 string `json:"requestSHA256"`
	ResultSHA256  string `json:"resultSHA256"`
}

type options struct {
	childPath             string
	configPath            string
	listen                func(string, string) (net.Listener, error)
	now                   func() time.Time
	holdTimeout           time.Duration
	requestLimit          int64
	resultLimit           int64
	profileLimit          int64
	stderrLimit           int64
	configLimit           int64
	releaseBodyLimit      int64
	controlBaseURL        string
	controlWaitTimeout    time.Duration
	controlRequestTimeout time.Duration
	controlRetryDelay     time.Duration
	heldResponseLimit     int64
	receiptResponseLimit  int64
}

func productionOptions() options {
	return options{
		childPath: defaultChildPath, configPath: defaultConfigPath,
		listen: net.Listen, now: time.Now, holdTimeout: defaultHoldTimeout,
		requestLimit: defaultRequestLimit, resultLimit: defaultResultLimit,
		profileLimit: defaultProfileLimit, stderrLimit: defaultStderrLimit,
		configLimit: defaultConfigLimit, releaseBodyLimit: defaultReleaseLimit,
		controlBaseURL: defaultControlBaseURL, controlWaitTimeout: defaultControlWait,
		controlRequestTimeout: defaultControlRequest, controlRetryDelay: defaultControlRetry,
		heldResponseLimit: defaultHeldLimit, receiptResponseLimit: defaultReceiptLimit,
	}
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr, productionOptions()); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(parent context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, opts options) error {
	_ = stderr // Child stderr is intentionally captured but never disclosed.
	control, ok, err := controlInvocation(args)
	if err != nil {
		return err
	}
	if ok {
		if err := validateControlOptions(opts); err != nil {
			return err
		}
		if control == "--wait-held" {
			return waitHeld(parent, stdout, opts)
		}
		return releaseHeld(parent, stdin, stdout, opts)
	}
	if err := validateOptions(opts); err != nil {
		return err
	}
	childTimeout, printProfile, err := invocation(args)
	if err != nil {
		return err
	}
	if printProfile {
		resultBytes, err := runChild(parent, opts.childPath, args, nil, childTimeout, opts.profileLimit, opts.stderrLimit)
		if err != nil {
			return err
		}
		return writeExact(stdout, resultBytes)
	}

	requestBytes, err := readBounded(stdin, opts.requestLimit, "simulation request")
	if err != nil {
		return err
	}
	var request scheduling.Request
	if err := decodeStrictObject(requestBytes, "simulation request", &request); err != nil {
		return err
	}
	config, err := loadConfig(opts.configPath, opts.configLimit)
	if err != nil {
		return err
	}
	resultBytes, err := runChild(parent, opts.childPath, args, requestBytes, childTimeout, opts.resultLimit, opts.stderrLimit)
	if err != nil {
		return err
	}
	var result scheduling.Result
	if err := decodeStrictObject(resultBytes, "simulation result", &result); err != nil {
		return err
	}
	if err := scheduling.ValidateResponse(request, result); err != nil {
		return fmt.Errorf("validate simulation result: %w", err)
	}
	if !matchesBarrier(request, result, config) {
		return writeExact(stdout, resultBytes)
	}
	return hold(parent, requestBytes, resultBytes, stdout, opts)
}

func controlInvocation(args []string) (string, bool, error) {
	mode := ""
	for _, arg := range args {
		if arg != "--wait-held" && arg != "--release-held" {
			continue
		}
		if mode != "" || len(args) != 1 {
			return "", false, fmt.Errorf("control mode must be exactly --wait-held or --release-held")
		}
		mode = arg
	}
	return mode, mode != "", nil
}

func validateControlOptions(opts options) error {
	if opts.controlBaseURL == "" {
		return fmt.Errorf("control base URL is not configured")
	}
	if opts.controlWaitTimeout <= 0 || opts.controlWaitTimeout > defaultControlWait {
		return fmt.Errorf("control wait timeout must be positive and no greater than %s", defaultControlWait)
	}
	if opts.controlRequestTimeout <= 0 || opts.controlRetryDelay <= 0 {
		return fmt.Errorf("control request timeout and retry delay must be positive")
	}
	if opts.heldResponseLimit <= 0 || opts.receiptResponseLimit <= 0 || opts.releaseBodyLimit <= 0 {
		return fmt.Errorf("control byte limits must be positive")
	}
	return nil
}

func waitHeld(parent context.Context, stdout io.Writer, opts options) error {
	ctx, cancel := context.WithTimeout(parent, opts.controlWaitTimeout)
	defer cancel()
	client, closeClient := controlClient(opts.controlRequestTimeout)
	defer closeClient()
	for {
		requestCtx, requestCancel := context.WithTimeout(ctx, opts.controlRequestTimeout)
		request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, opts.controlBaseURL+"/held", nil)
		if err != nil {
			requestCancel()
			return fmt.Errorf("create held request: %w", err)
		}
		response, requestErr := client.Do(request)
		if requestErr == nil {
			raw, readErr := readBounded(response.Body, opts.heldResponseLimit, "held response")
			closeErr := response.Body.Close()
			requestCancel()
			if readErr != nil {
				return readErr
			}
			if closeErr != nil {
				return fmt.Errorf("close held response: %w", closeErr)
			}
			if response.StatusCode != http.StatusOK {
				return fmt.Errorf("held endpoint returned HTTP %d", response.StatusCode)
			}
			return writeExact(stdout, raw)
		}
		requestCancel()
		if ctx.Err() != nil {
			return fmt.Errorf("wait for held response: %w", ctx.Err())
		}
		timer := time.NewTimer(opts.controlRetryDelay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return fmt.Errorf("wait for held response: %w", ctx.Err())
		}
	}
}

func releaseHeld(parent context.Context, stdin io.Reader, stdout io.Writer, opts options) error {
	raw, err := readBounded(stdin, opts.releaseBodyLimit, "release input")
	if err != nil {
		return err
	}
	var release releaseRequest
	if err := decodeStrictObject(raw, "release input", &release); err != nil {
		return err
	}
	if release.Nonce == "" {
		return fmt.Errorf("release nonce must not be empty")
	}
	ctx, cancel := context.WithTimeout(parent, opts.controlRequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, opts.controlBaseURL+"/release", bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("create release request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	client, closeClient := controlClient(opts.controlRequestTimeout)
	defer closeClient()
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("post release request: %w", err)
	}
	responseBytes, readErr := readBounded(response.Body, opts.receiptResponseLimit, "release receipt")
	closeErr := response.Body.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return fmt.Errorf("close release receipt: %w", closeErr)
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("release endpoint returned HTTP %d", response.StatusCode)
	}
	return writeExact(stdout, responseBytes)
}

func controlClient(timeout time.Duration) (*http.Client, func()) {
	transport := &http.Transport{
		Proxy: nil, DisableKeepAlives: true,
		DialContext:           (&net.Dialer{Timeout: timeout}).DialContext,
		ResponseHeaderTimeout: timeout,
	}
	client := &http.Client{
		Transport: transport, Timeout: timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return client, transport.CloseIdleConnections
}

func validateOptions(opts options) error {
	if !filepath.IsAbs(opts.childPath) || !filepath.IsAbs(opts.configPath) {
		return fmt.Errorf("child and barrier configuration paths must be absolute")
	}
	if opts.listen == nil || opts.now == nil {
		return fmt.Errorf("barrier dependencies are not configured")
	}
	if opts.holdTimeout <= 0 || opts.holdTimeout > maximumHoldTimeout {
		return fmt.Errorf("hold timeout must be positive and no greater than %s", maximumHoldTimeout)
	}
	for label, limit := range map[string]int64{
		"request": opts.requestLimit, "result": opts.resultLimit,
		"profile": opts.profileLimit, "stderr": opts.stderrLimit,
		"configuration": opts.configLimit, "release body": opts.releaseBodyLimit,
	} {
		if limit <= 0 {
			return fmt.Errorf("%s limit must be positive", label)
		}
	}
	return nil
}

func invocation(args []string) (time.Duration, bool, error) {
	var timeoutText string
	printProfile := false
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--print-profile":
			printProfile = true
		case args[i] == "--timeout":
			if timeoutText != "" || i+1 >= len(args) {
				return 0, false, fmt.Errorf("exactly one --timeout value is required")
			}
			i++
			timeoutText = args[i]
		case strings.HasPrefix(args[i], "--timeout="):
			if timeoutText != "" {
				return 0, false, fmt.Errorf("exactly one --timeout value is required")
			}
			timeoutText = strings.TrimPrefix(args[i], "--timeout=")
		}
	}
	if timeoutText == "" {
		return 0, false, fmt.Errorf("exactly one --timeout value is required")
	}
	timeout, err := time.ParseDuration(timeoutText)
	if err != nil || timeout <= 0 || timeout > maximumChildTimeout {
		return 0, false, fmt.Errorf("worker timeout must be positive and no greater than %s", maximumChildTimeout)
	}
	return timeout, printProfile, nil
}

func loadConfig(path string, limit int64) (barrierConfig, error) {
	var config barrierConfig
	file, err := os.Open(path)
	if err != nil {
		return config, fmt.Errorf("open barrier configuration: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return config, fmt.Errorf("barrier configuration must be a regular file")
	}
	raw, err := readBounded(file, limit, "barrier configuration")
	if err != nil {
		return config, err
	}
	if err := decodeStrictObject(raw, "barrier configuration", &config); err != nil {
		return config, err
	}
	if config.Namespace != "alfred-e2e" {
		return config, fmt.Errorf("barrier source namespace must be alfred-e2e")
	}
	if config.Workload != "single" && config.Workload != "gang" {
		return config, fmt.Errorf("barrier workload must be single or gang")
	}
	for label, value := range map[string]string{
		"schedulerName":    config.Profile.SchedulerName,
		"backend":          config.Profile.Backend,
		"schedulerVersion": config.Profile.SchedulerVersion,
		"configurationID":  config.Profile.ConfigurationID,
	} {
		if value == "" || strings.TrimSpace(value) != value {
			return config, fmt.Errorf("barrier profile %s must be non-empty and unpadded", label)
		}
	}
	return config, nil
}

func matchesBarrier(request scheduling.Request, result scheduling.Result, config barrierConfig) bool {
	if result.Decision != scheduling.DecisionFeasible || request.RequestID != "preflight" ||
		request.MigrationFromNode == "" || len(request.ExcludedNodes) != 1 ||
		request.ExcludedNodes[0] != request.MigrationFromNode || request.Profile != config.Profile {
		return false
	}
	wantCount, wantGang := 1, false
	if config.Workload == "gang" {
		wantCount, wantGang = 2, true
	} else if config.Workload != "single" {
		return false
	}
	if len(request.SourcePods) != wantCount || len(request.ReplacementPods) != wantCount || request.RequireGang != wantGang {
		return false
	}
	for i := range request.SourcePods {
		pod := &request.SourcePods[i]
		if pod.Namespace != config.Namespace || pod.Labels["ome.io/inferenceservice"] != config.Workload {
			return false
		}
	}
	return true
}

func hold(parent context.Context, requestBytes, resultBytes []byte, stdout io.Writer, opts options) error {
	listener, err := opts.listen("tcp", defaultListenAddress)
	if err != nil {
		return fmt.Errorf("listen for simulator release: %w", err)
	}

	nonce, err := randomNonce()
	if err != nil {
		_ = listener.Close()
		return err
	}
	heldAt := opts.now().UTC()
	deadline := heldAt.Add(opts.holdTimeout)
	evidence := heldDocument{
		RequestBytes: bytes.Clone(requestBytes), ResultBytes: bytes.Clone(resultBytes),
		RequestSHA256: digest(requestBytes), ResultSHA256: digest(resultBytes),
		HeldAt: heldAt.Format(time.RFC3339Nano), Deadline: deadline.Format(time.RFC3339Nano),
		Nonce: nonce,
	}
	released := make(chan releaseReceipt, 1)
	failed := make(chan error, 1)
	server := &http.Server{
		Handler:           newBarrierHandler(evidence, nonce, deadline, released, failed, opts.releaseBodyLimit, opts.now),
		ReadHeaderTimeout: 500 * time.Millisecond, ReadTimeout: time.Second,
		WriteTimeout: time.Second, IdleTimeout: 500 * time.Millisecond, MaxHeaderBytes: 8 << 10,
	}
	serveDone := make(chan error, 1)
	go func() {
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveDone <- err
	}()

	timer := time.NewTimer(opts.holdTimeout)
	defer timer.Stop()
	var outcome error
	authorized := false
	select {
	case <-released:
		authorized = true
	case err := <-failed:
		outcome = fmt.Errorf("release request rejected: %w", err)
	case err := <-serveDone:
		if err == nil {
			outcome = fmt.Errorf("release server stopped before authorization")
		} else {
			outcome = fmt.Errorf("serve release endpoint: %w", err)
		}
		serveDone = nil
	case <-timer.C:
		outcome = fmt.Errorf("release hold timed out")
	case <-parent.Done():
		outcome = fmt.Errorf("release hold: %w", parent.Err())
	}
	if err := stopServer(server, serveDone); err != nil && outcome == nil {
		outcome = err
	}
	// A replay may race the successful handler before Shutdown has drained it.
	// Preserve fail-closed semantics even when the release signal won the first
	// select: any request error observed during shutdown cancels authorization.
	if authorized && outcome == nil {
		select {
		case err := <-failed:
			outcome = fmt.Errorf("release request rejected: %w", err)
		default:
		}
	}
	if outcome != nil || !authorized {
		if outcome == nil {
			outcome = fmt.Errorf("release was not authorized")
		}
		return outcome
	}
	return writeExact(stdout, resultBytes)
}

func stopServer(server *http.Server, serveDone <-chan error) error {
	ctx, cancel := context.WithTimeout(context.Background(), serverShutdownTimeout)
	defer cancel()
	shutdownErr := server.Shutdown(ctx)
	if serveDone == nil {
		return shutdownErr
	}
	select {
	case serveErr := <-serveDone:
		if shutdownErr != nil {
			return fmt.Errorf("shutdown release server: %w", shutdownErr)
		}
		if serveErr != nil {
			return fmt.Errorf("serve release endpoint: %w", serveErr)
		}
		return nil
	case <-ctx.Done():
		_ = server.Close()
		return fmt.Errorf("shutdown release server: %w", ctx.Err())
	}
}

type barrierHandler struct {
	evidence  heldDocument
	nonce     string
	deadline  time.Time
	released  chan<- releaseReceipt
	failed    chan<- error
	bodyLimit int64
	now       func() time.Time
	mu        sync.Mutex
	used      bool
	failOnce  sync.Once
}

func newBarrierHandler(evidence heldDocument, nonce string, deadline time.Time, released chan<- releaseReceipt, failed chan<- error, bodyLimit int64, now func() time.Time) http.Handler {
	return &barrierHandler{evidence: evidence, nonce: nonce, deadline: deadline, released: released, failed: failed, bodyLimit: bodyLimit, now: now}
}

func (h *barrierHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		h.reject(w, http.StatusNotFound, errors.New("query strings are not accepted"))
		return
	}
	switch r.URL.Path {
	case "/held":
		if r.Method != http.MethodGet || r.ContentLength > 0 {
			h.reject(w, http.StatusMethodNotAllowed, errors.New("invalid held request"))
			return
		}
		if err := writeJSON(w, http.StatusOK, h.evidence); err != nil {
			h.fail(err)
		}
	case "/release":
		if r.Method != http.MethodPost {
			h.reject(w, http.StatusMethodNotAllowed, errors.New("invalid release method"))
			return
		}
		h.release(w, r)
	default:
		h.reject(w, http.StatusNotFound, errors.New("unknown endpoint"))
	}
}

func (h *barrierHandler) release(w http.ResponseWriter, r *http.Request) {
	raw, err := readBounded(r.Body, h.bodyLimit, "release request")
	if err != nil {
		h.reject(w, http.StatusRequestEntityTooLarge, err)
		return
	}
	var request releaseRequest
	if err := decodeStrictObject(raw, "release request", &request); err != nil {
		h.reject(w, http.StatusBadRequest, err)
		return
	}
	releasedAt := h.now().UTC()
	h.mu.Lock()
	if h.used || request.Nonce == "" || request.Nonce != h.nonce || !releasedAt.Before(h.deadline) {
		h.mu.Unlock()
		h.reject(w, http.StatusForbidden, errors.New("release is not authorized"))
		return
	}
	h.used = true
	h.mu.Unlock()
	receipt := releaseReceipt{
		ReleasedAt:    releasedAt.Format(time.RFC3339Nano),
		RequestSHA256: h.evidence.RequestSHA256, ResultSHA256: h.evidence.ResultSHA256,
	}
	if err := writeJSON(w, http.StatusOK, receipt); err != nil {
		h.fail(fmt.Errorf("write release receipt: %w", err))
		return
	}
	select {
	case h.released <- receipt:
	default:
		h.fail(errors.New("release receipt could not be delivered"))
	}
}

func (h *barrierHandler) reject(w http.ResponseWriter, status int, err error) {
	h.fail(err)
	http.Error(w, http.StatusText(status), status)
}

func (h *barrierHandler) fail(err error) {
	h.failOnce.Do(func() {
		select {
		case h.failed <- err:
		default:
		}
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	n, err := w.Write(raw)
	if err != nil {
		return err
	}
	if n != len(raw) {
		return io.ErrShortWrite
	}
	return nil
}

func randomNonce() (string, error) {
	raw := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, raw); err != nil {
		return "", fmt.Errorf("generate release nonce: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func runChild(parent context.Context, binary string, args []string, stdin []byte, timeout time.Duration, stdoutLimit, stderrLimit int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	stdout := newBoundedWriter(stdoutLimit, cancel)
	stderr := newBoundedWriter(stderrLimit, cancel)
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = []string{}
	cmd.Stdin = bytes.NewReader(stdin)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.WaitDelay = childWaitDelay
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

func readBounded(reader io.Reader, limit int64, label string) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", label, err)
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("%s is too large", label)
	}
	return raw, nil
}

func decodeStrictObject(raw []byte, label string, value any) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("%s must not be null", label)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("decode %s: %w", label, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err != nil {
			return fmt.Errorf("decode %s trailing data: %w", label, err)
		}
		return fmt.Errorf("%s must contain one JSON object", label)
	}
	return nil
}

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func writeExact(writer io.Writer, raw []byte) error {
	n, err := writer.Write(raw)
	if err != nil {
		return fmt.Errorf("write worker stdout: %w", err)
	}
	if n != len(raw) {
		return fmt.Errorf("write worker stdout: %w", io.ErrShortWrite)
	}
	return nil
}
