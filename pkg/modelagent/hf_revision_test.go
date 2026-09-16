package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testHfRevisionSHA = "abcdef0123456789abcdef0123456789abcdef01"

func TestResolveHfRevisionMetadataRequest(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		modelID  string
		revision string
		token    string
		basePath string
		wantPath string
	}{
		{name: "default main", modelID: "org/model", wantPath: "/api/models/org/model/revision/main"},
		{name: "explicit branch", modelID: "org/model", revision: "release-v2", token: "effective-token", wantPath: "/api/models/org/model/revision/release-v2"},
		{name: "branch with slash", modelID: "org/model", revision: "feature/new-weights", token: "effective-token", wantPath: "/api/models/org/model/revision/feature%2Fnew-weights"},
		{name: "pull request ref", modelID: "org/model", revision: "refs/pr/12", wantPath: "/api/models/org/model/revision/refs%2Fpr%2F12"},
		{name: "escaped delimiters", modelID: "org/model", revision: "ref%/#?", wantPath: "/api/models/org/model/revision/ref%25%2F%23%3F"},
		{name: "custom endpoint prefix", modelID: " org/model ", revision: "main", basePath: "/hub/", wantPath: "/hub/api/models/org/model/revision/main"},
		{name: "unscoped model", modelID: "gpt2", wantPath: "/api/models/gpt2/revision/main"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				assert.Equal(t, http.MethodGet, r.Method)
				assert.Equal(t, tc.wantPath, r.URL.EscapedPath())
				assert.Empty(t, r.URL.RawQuery)
				wantAuth := ""
				if tc.token != "" {
					wantAuth = "Bearer " + tc.token
				}
				assert.Equal(t, wantAuth, r.Header.Get("Authorization"))
				_ = json.NewEncoder(w).Encode(map[string]string{"sha": strings.ToUpper(testHfRevisionSHA)})
			}))
			defer server.Close()

			sha, err := resolveHfRevision(context.Background(), tc.modelID, tc.revision, tc.token, server.URL+tc.basePath)

			require.NoError(t, err)
			assert.Equal(t, testHfRevisionSHA, sha)
			assert.Equal(t, int32(1), requests.Load())
		})
	}
}

func TestResolveHfRevisionPinnedSHAIsLocal(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	for _, endpoint := range []string{server.URL, "", ":not-an-endpoint"} {
		sha, err := resolveHfRevision(context.Background(), "org/model", strings.ToUpper(testHfRevisionSHA), "token", endpoint)
		require.NoError(t, err)
		assert.Equal(t, testHfRevisionSHA, sha)
	}
	assert.Zero(t, requests.Load())
}

func TestResolveHfRevisionRejectsInvalidModelIDsLocally(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1) }))
	defer server.Close()
	for _, modelID := range []string{"", " ", "../model", "org/../model", "org/model/extra", "org/model?token=secret", "org/model#ref", "org\\model", "org//model", "org/model.git", strings.Repeat("a", 97)} {
		for _, revision := range []string{"main", testHfRevisionSHA} {
			sha, err := resolveHfRevision(context.Background(), modelID, revision, "token", server.URL)
			require.Error(t, err, "model %q", modelID)
			assert.Empty(t, sha)
		}
	}
	assert.Zero(t, requests.Load())
}

func TestResolveHfRevisionRetryCounts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		statuses []int
		wantErr  bool
	}{
		{name: "429 then success", statuses: []int{429, 200}},
		{name: "503 then success", statuses: []int{503, 200}},
		{name: "third attempt succeeds", statuses: []int{500, 502, 200}},
		{name: "429 stops at three", statuses: []int{429, 429, 429}, wantErr: true},
		{name: "503 stops at three", statuses: []int{503, 503, 503}, wantErr: true},
		{name: "unauthorized", statuses: []int{401}, wantErr: true},
		{name: "forbidden", statuses: []int{403}, wantErr: true},
		{name: "not found", statuses: []int{404}, wantErr: true},
		{name: "bad request", statuses: []int{400}, wantErr: true},
		{name: "request timeout status is permanent", statuses: []int{408}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				i := int(requests.Add(1)) - 1
				if i >= len(tc.statuses) {
					t.Errorf("unexpected attempt %d", i+1)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(tc.statuses[i])
				if tc.statuses[i] == http.StatusOK {
					_ = json.NewEncoder(w).Encode(map[string]string{"sha": testHfRevisionSHA})
				} else {
					_, _ = fmt.Fprint(w, "do not expose effective-token from this body")
				}
			}))
			defer server.Close()

			sha, err := resolveHfRevision(context.Background(), "org/model", "main", "effective-token", server.URL)

			if tc.wantErr {
				require.Error(t, err)
				assert.NotContains(t, err.Error(), "effective-token")
				assert.Contains(t, err.Error(), fmt.Sprint(tc.statuses[len(tc.statuses)-1]))
				assert.Empty(t, sha)
			} else {
				require.NoError(t, err)
				assert.Equal(t, testHfRevisionSHA, sha)
			}
			assert.Equal(t, int32(len(tc.statuses)), requests.Load())
		})
	}
}

func TestResolveHfRevisionRetriesNetworkFailures(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"connection closed", "body interrupted"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				conn, rw, err := w.(http.Hijacker).Hijack()
				if !assert.NoError(t, err) {
					return
				}
				if mode == "body interrupted" {
					_, _ = rw.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 1000\r\nConnection: close\r\n\r\n{\"sha\":")
					_ = rw.Flush()
				}
				_ = conn.Close()
			}))
			defer server.Close()

			sha, err := resolveHfRevision(context.Background(), "org/model", "main", "", server.URL)

			require.Error(t, err)
			assert.Empty(t, sha)
			assert.Equal(t, int32(3), requests.Load())
		})
	}
}

func TestResolveHfRevisionRejectsInvalidMetadataWithoutRetry(t *testing.T) {
	t.Parallel()
	for _, body := range []string{
		"not JSON", `{}`, `null`, `{"sha":null}`, `{"sha":23}`, `{"sha":"short"}`,
		`{"sha":"` + strings.Repeat("z", 40) + `"}`,
		`{"sha":" ` + testHfRevisionSHA + `"}`,
		`{"sha":"` + testHfRevisionSHA + `"} {}`,
		strings.Repeat(" ", hfRevisionMaxResponseBytes+1),
	} {
		t.Run(fmt.Sprintf("body length %d prefix %.20s", len(body), body), func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				_, _ = fmt.Fprint(w, body)
			}))
			defer server.Close()

			sha, err := resolveHfRevision(context.Background(), "org/model", "main", "", server.URL)

			require.Error(t, err)
			assert.Empty(t, sha)
			assert.Equal(t, int32(1), requests.Load())
		})
	}
}

func TestResolveHfRevisionCancellation(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"before request", "response headers", "response body", "retry delay"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				switch mode {
				case "response headers":
					<-r.Context().Done()
				case "response body":
					w.WriteHeader(http.StatusOK)
					_, _ = fmt.Fprint(w, `{"sha":`)
					w.(http.Flusher).Flush()
					<-r.Context().Done()
				case "retry delay":
					w.Header().Set("Retry-After", "86400")
					w.WriteHeader(http.StatusTooManyRequests)
				default:
					t.Error("cancelled request reached server")
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			if mode == "before request" {
				cancel()
			}
			started := time.Now()

			sha, err := resolveHfRevision(ctx, "org/model", "main", "", server.URL)

			require.ErrorIs(t, err, ctx.Err())
			assert.Empty(t, sha)
			assert.Less(t, time.Since(started), time.Second)
			wantRequests := int32(1)
			if mode == "before request" {
				wantRequests = 0
			}
			assert.Equal(t, wantRequests, requests.Load())
		})
	}
}

func TestHfRevisionMetadataURLDefaultAndInvalidEndpoints(t *testing.T) {
	t.Parallel()
	metadataURL, err := hfRevisionMetadataURL("org/model", "refs/pr/1", "")
	require.NoError(t, err)
	assert.Equal(t, DefaultEndpoint+"/api/models/org/model/revision/refs%2Fpr%2F1", metadataURL)
	for _, endpoint := range []string{
		"file:///tmp/model", "not-a-url", "/relative", "https://", "https://user:secret@example.com",
		"https://example.com?token=secret", "https://example.com?", "https://example.com#secret", ":bad%url",
	} {
		_, err := resolveHfRevision(context.Background(), "org/model", "main", "effective-token", endpoint)
		require.Error(t, err, "endpoint %q", endpoint)
		assert.NotContains(t, err.Error(), "secret")
		assert.NotContains(t, err.Error(), "effective-token")
	}
}

func TestHfRevisionRetryDelayIsBounded(t *testing.T) {
	t.Parallel()
	assert.Zero(t, hfRevisionRetryDelay(1, "0"))
	assert.Equal(t, time.Second, hfRevisionRetryDelay(1, "1"))
	assert.Equal(t, hfRevisionMaxRetryDelay, hfRevisionRetryDelay(1, "86400"))
	assert.Equal(t, hfRevisionMaxRetryDelay, hfRevisionRetryDelay(1, "18446744073709551615"))
	assert.Equal(t, hfRevisionMaxRetryDelay, hfRevisionRetryDelay(1, time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)))
	assert.Zero(t, hfRevisionRetryDelay(1, time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)))
	for attempt := 1; attempt < hfRevisionMaxAttempts; attempt++ {
		for _, header := range []string{"", "-1", "invalid", strings.Repeat("9", 100)} {
			delay := hfRevisionRetryDelay(attempt, header)
			assert.Greater(t, delay, time.Duration(0))
			assert.LessOrEqual(t, delay, hfRevisionMaxRetryDelay)
		}
	}
}

func TestResolveHfRevisionRejectsCrossOriginRedirects(t *testing.T) {
	t.Parallel()
	var redirectedRequests atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectedRequests.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]string{"sha": testHfRevisionSHA})
	}))
	defer destination.Close()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	_, err := resolveHfRevision(context.Background(), "org/model", "main", "token", server.URL)

	require.ErrorContains(t, err, "307")
	assert.Equal(t, int32(1), requests.Load())
	assert.Zero(t, redirectedRequests.Load())
}
