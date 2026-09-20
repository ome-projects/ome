package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	hfRevisionMaxAttempts      = 3
	hfRevisionMaxResponseBytes = 1 << 20
	hfRevisionMaxRetryDelay    = 2 * time.Second
	hfMetadataMaxRedirects     = 3
)

// resolveHfRevision pins a model revision before downloading. Token is the
// caller's effective token; credential fallback remains the caller's concern.
func resolveHfRevision(ctx context.Context, modelID, revision, token, endpoint string) (string, error) {
	modelID = strings.TrimSpace(modelID)
	if !isValidHfModelID(modelID) {
		return "", fmt.Errorf("invalid Hugging Face model ID")
	}
	revision = strings.TrimSpace(revision)
	if isValidHfCommitSHA(revision) {
		return strings.ToLower(revision), nil
	}
	if revision == "" {
		revision = "main"
	}
	metadataURL, err := hfRevisionMetadataURL(modelID, revision, endpoint)
	if err != nil {
		return "", err
	}
	client := NewHTTPClientWithTimeout(DefaultRequestTimeout)
	client.CheckRedirect = checkHfMetadataRedirect
	var lastErr error
	for attempt := 1; attempt <= hfRevisionMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, metadataURL, nil)
		if err != nil {
			return "", fmt.Errorf("cannot build Hugging Face revision request")
		}
		request.Header.Set("Accept", "application/json")
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		sha, retry, retryAfter, err := fetchHfRevisionMetadata(client, request)
		if err == nil {
			return sha, nil
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		lastErr = err
		if !retry || attempt == hfRevisionMaxAttempts {
			break
		}
		timer := time.NewTimer(hfRevisionRetryDelay(attempt, retryAfter))
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
	}
	return "", fmt.Errorf("resolve Hugging Face revision: %w", lastErr)
}

// HF aliases and renamed repositories may redirect within the configured hub.
// Bound that chain and never forward credentials to a different origin.
func checkHfMetadataRedirect(request *http.Request, via []*http.Request) error {
	if len(via) == 0 || len(via) > hfMetadataMaxRedirects || request.URL.User != nil ||
		request.URL.Scheme != via[0].URL.Scheme || !strings.EqualFold(request.URL.Host, via[0].URL.Host) {
		return http.ErrUseLastResponse
	}
	return nil
}

func hfRevisionMetadataURL(modelID, revision, endpoint string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}
	base, err := url.Parse(endpoint)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" ||
		base.User != nil || base.RawQuery != "" || base.ForceQuery || base.Fragment != "" || base.Opaque != "" {
		return "", fmt.Errorf("invalid Hugging Face endpoint")
	}
	return strings.TrimRight(base.String(), "/") + "/api/models/" + modelID + "/revision/" + url.PathEscape(revision), nil
}

// fetchHfRevisionMetadata closes every response before the caller retries.
// Error response bodies are never read into errors, since they can echo tokens.
func fetchHfRevisionMetadata(client *http.Client, request *http.Request) (sha string, retry bool, retryAfter string, err error) {
	response, err := client.Do(request)
	if err != nil {
		return "", true, "", fmt.Errorf("Hugging Face revision request failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		retry = response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 && response.StatusCode < 600
		return "", retry, response.Header.Get("Retry-After"), fmt.Errorf("Hugging Face revision metadata returned HTTP %d", response.StatusCode)
	}
	if response.ContentLength > hfRevisionMaxResponseBytes {
		return "", false, "", fmt.Errorf("Hugging Face revision metadata exceeds %d bytes", hfRevisionMaxResponseBytes)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, hfRevisionMaxResponseBytes+1))
	if err != nil {
		return "", true, "", fmt.Errorf("cannot read Hugging Face revision metadata: %w", err)
	}
	if len(body) > hfRevisionMaxResponseBytes {
		return "", false, "", fmt.Errorf("Hugging Face revision metadata exceeds %d bytes", hfRevisionMaxResponseBytes)
	}
	var metadata struct {
		SHA string `json:"sha"`
	}
	if err := json.Unmarshal(body, &metadata); err != nil {
		return "", false, "", fmt.Errorf("invalid Hugging Face revision metadata JSON")
	}
	if !isValidHfCommitSHA(metadata.SHA) {
		return "", false, "", fmt.Errorf("Hugging Face revision metadata has no valid 40-character commit SHA")
	}
	return strings.ToLower(metadata.SHA), false, "", nil
}

func hfRevisionRetryDelay(attempt int, retryAfter string) time.Duration {
	if seconds, err := strconv.ParseUint(strings.TrimSpace(retryAfter), 10, 64); err == nil {
		if seconds >= uint64(hfRevisionMaxRetryDelay/time.Second) {
			return hfRevisionMaxRetryDelay
		}
		return time.Duration(seconds) * time.Second
	}
	if date, err := http.ParseTime(retryAfter); err == nil {
		return min(max(time.Until(date), 0), hfRevisionMaxRetryDelay)
	}
	// The package-level rand/v2 functions are concurrency-safe; the existing
	// HF client's shared rand.Rand must not be used by parallel resolutions.
	delay := 250 * time.Millisecond * time.Duration(1<<(attempt-1))
	return min(delay+time.Duration(rand.Int64N(int64(delay/2)+1)), hfRevisionMaxRetryDelay)
}
