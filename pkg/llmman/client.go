// Package llmman is a client for a running `llmman serve` daemon
// (https://github.com/llmmanorg/llmman), used to acquire models published as
// CNCF ModelPack (https://github.com/modelpack/model-spec) OCI artifacts.
//
// The daemon owns the registry work (ModelPack media types, registry auth,
// resumable blob download and a content-addressed local store), so it is not
// reimplemented here. Two pieces are needed, because the daemon deliberately
// exposes no local path: `POST /api/pull` streams the download, and
// `llmman resolve --no-pull` reports where the bytes landed. That means a pull
// needs both the daemon reachable and the binary on PATH; each missing piece
// has its own error.
//
// Only the standard library is used, so this adds no dependency to OME.
package llmman

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	// DefaultHost and DefaultPort match `llmman serve`'s own defaults.
	DefaultHost = "127.0.0.1"
	DefaultPort = "17434"

	// HostEnv is the same variable llmman's own clients read.
	HostEnv = "LLMMAN_HOST"
	// BinEnv overrides the llmman binary location.
	BinEnv = "OME_LLMMAN_BIN"

	defaultBinary = "llmman"
)

// ProgressFunc receives coarse pull progress. completed and total are zero
// when the daemon does not report byte counts for a step.
type ProgressFunc func(status string, completed, total int64)

// Endpoint returns the http origin of the llmman daemon.
//
// LLMMAN_HOST is parsed as [scheme://]host[:port]; a wildcard bind host
// (0.0.0.0 or ::) is rewritten to loopback, since a client cannot connect to
// "every interface". This mirrors llmman's own client-side resolution.
func Endpoint() string {
	raw := strings.Trim(strings.TrimSpace(os.Getenv(HostEnv)), `"'`)
	if raw == "" {
		return "http://" + net.JoinHostPort(DefaultHost, DefaultPort)
	}
	if idx := strings.Index(raw, "://"); idx >= 0 {
		raw = raw[idx+3:]
	}
	if idx := strings.Index(raw, "/"); idx >= 0 {
		raw = raw[:idx]
	}

	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		host, port = raw, DefaultPort
	}
	if host == "" {
		host = DefaultHost
	}
	if port == "" {
		port = DefaultPort
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		if ip.To4() != nil {
			host = "127.0.0.1"
		} else {
			host = "::1"
		}
	}
	return "http://" + net.JoinHostPort(host, port)
}

// Binary returns the llmman executable name, overridable via OME_LLMMAN_BIN.
func Binary() string {
	if v := strings.TrimSpace(os.Getenv(BinEnv)); v != "" {
		return v
	}
	return defaultBinary
}

// CheckDaemon confirms an llmman daemon is listening and answering.
//
// /api/version is llmman's own identity endpoint; a response without a version
// field means something else is bound to the port, which is worth
// distinguishing from nothing listening at all.
func CheckDaemon(ctx context.Context, client *http.Client, endpoint string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/api/version", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("no llmman daemon reachable at %s: %w. Start one with `llmman serve`, or point %s at an existing daemon", endpoint, err, HostEnv)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("llmman daemon at %s answered /api/version with HTTP %d", endpoint, resp.StatusCode)
	}
	var payload struct {
		Version string `json:"version"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil || payload.Version == "" {
		return fmt.Errorf("the server at %s is not an llmman daemon (no version in /api/version)", endpoint)
	}
	return nil
}

// pullLine is one newline-delimited JSON object from /api/pull, mirroring
// Ollama's ProgressResponse.
type pullLine struct {
	Status    string `json:"status"`
	Error     string `json:"error"`
	Total     int64  `json:"total"`
	Completed int64  `json:"completed"`
}

// Pull streams POST /api/pull until the daemon reports success.
//
// An error can arrive in-band at HTTP 200, and a stream that ends without
// success is also a failure. Neither is treated as a completed pull.
func Pull(ctx context.Context, client *http.Client, endpoint, reference string, progress ProgressFunc) error {
	body, err := json.Marshal(map[string]string{"model": reference})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/api/pull", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("llmman pull of %q failed: %w", reference, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("llmman pull of %q failed: HTTP %d", reference, resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	succeeded := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var l pullLine
		if err := json.Unmarshal([]byte(line), &l); err != nil {
			// Tolerate a non-JSON diagnostic rather than aborting a pull that
			// may still be progressing.
			continue
		}
		if l.Error != "" {
			return fmt.Errorf("llmman pull of %q failed: %s", reference, l.Error)
		}
		if l.Status == "success" {
			succeeded = true
			continue
		}
		if progress != nil && l.Status != "" {
			progress(l.Status, l.Completed, l.Total)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading llmman pull stream for %q: %w", reference, err)
	}
	if !succeeded {
		return fmt.Errorf("llmman pull of %q ended without reporting success", reference)
	}
	return nil
}

type resolveOutput struct {
	Path string `json:"path"`
}

// Resolve reports where the daemon's pull left the model on disk.
//
// --no-pull guarantees this only reports on bytes /api/pull already fetched,
// so the daemon stays the only thing that touches the network.
func Resolve(ctx context.Context, reference string) (string, error) {
	binary := Binary()
	if _, err := exec.LookPath(binary); err != nil {
		if _, statErr := os.Stat(binary); statErr != nil {
			return "", fmt.Errorf("%q not found: install llmman (https://github.com/llmmanorg/llmman) and put it on PATH, or set %s to its location", binary, BinEnv)
		}
	}

	cmd := exec.CommandContext(ctx, binary, "resolve", "--no-pull", reference)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("`%s resolve --no-pull %s` failed: %w: %s", binary, reference, err, strings.TrimSpace(stderr.String()))
	}
	return parseResolveOutput(stdout.String(), reference)
}

// parseResolveOutput takes the last non-empty stdout line, so a diagnostic
// that leaks onto stdout does not break resolution, and ignores unknown JSON
// keys so the contract can grow.
func parseResolveOutput(stdout, reference string) (string, error) {
	var last string
	for _, line := range strings.Split(stdout, "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			last = trimmed
		}
	}
	if last == "" {
		return "", fmt.Errorf("llmman resolve %q printed nothing on stdout", reference)
	}

	var out resolveOutput
	if err := json.Unmarshal([]byte(last), &out); err != nil {
		return "", fmt.Errorf("llmman resolve %q: could not parse output as JSON: %s", reference, last)
	}
	if strings.TrimSpace(out.Path) == "" {
		return "", fmt.Errorf("llmman resolve %q returned an empty path", reference)
	}
	if _, err := os.Stat(out.Path); err != nil {
		return "", fmt.Errorf("llmman resolve %q reported path %q which does not exist: %w", reference, out.Path, err)
	}
	return out.Path, nil
}

// PullAndResolve performs the full acquisition: probe, pull, report the path.
func PullAndResolve(ctx context.Context, reference string, progress ProgressFunc) (string, error) {
	endpoint := Endpoint()
	if err := CheckDaemon(ctx, ProbeClient(), endpoint); err != nil {
		return "", err
	}
	if err := Pull(ctx, DefaultClient(), endpoint, reference, progress); err != nil {
		return "", err
	}
	return Resolve(ctx, reference)
}

// DefaultClient has no overall timeout: a multi-gigabyte pull is expected to
// be long-running, and cancellation is the caller's context.
func DefaultClient() *http.Client {
	return &http.Client{Transport: http.DefaultTransport}
}

// ProbeClient is used only for the short /api/version reachability check.
func ProbeClient() *http.Client {
	return &http.Client{Timeout: 5 * time.Second}
}
