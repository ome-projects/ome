package waitsource

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	clocktesting "k8s.io/utils/clock/testing"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
)

func wireService() *ome.InferenceService {
	return &ome.InferenceService{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"}, ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "work", UID: types.UID("private-uid"), ResourceVersion: "opaque:rv"}}
}
func TestRealWireNamedGetAndExactWatch(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, "GET", r.Method)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("watch") == "true" {
			require.Equal(t, "/apis/ome.io/v1beta1/namespaces/work/inferenceservices", r.URL.Path)
			require.Equal(t, "metadata.name=service", r.URL.Query().Get("fieldSelector"))
			require.Equal(t, "opaque:rv", r.URL.Query().Get("resourceVersion"))
			require.Equal(t, "10s", r.URL.Query().Get("timeout"))
			require.Len(t, r.URL.Query(), 4)
			require.NoError(t, json.NewEncoder(w).Encode(struct {
				Type   string                `json:"type"`
				Object *ome.InferenceService `json:"object"`
			}{"MODIFIED", wireService()}))
			return
		}
		require.Equal(t, "/apis/ome.io/v1beta1/namespaces/work/inferenceservices/service", r.URL.Path)
		require.Equal(t, "timeout=10s", r.URL.RawQuery)
		require.NoError(t, json.NewEncoder(w).Encode(wireService()))
	}))
	defer server.Close()
	s, err := NewInferenceService(&rest.Config{Host: server.URL}, "work", "service")
	require.NoError(t, err)
	snap, err := s.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, types.UID("private-uid"), snap.UID)
	require.Equal(t, "opaque:rv", snap.ResourceVersion)
	w, err := s.Watch(context.Background(), snap.ResourceVersion)
	require.NoError(t, err)
	defer w.Stop()
	select {
	case e := <-w.ResultChan():
		require.Equal(t, watch.Modified, e.Type)
		decoded, decodeErr := s.Decode(e.Object)
		require.NoError(t, decodeErr)
		require.Equal(t, "service", decoded.Value.Name)
	case <-time.After(time.Second):
		t.Fatal("no watch event")
	}
	require.Equal(t, int32(2), requests.Load())
}
func TestNamedGet429IsNotRetried(t *testing.T) {
	var n atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"TooManyRequests","code":429,"message":"Bearer PRIVATE"}`)
	}))
	defer server.Close()
	s, err := NewInferenceService(&rest.Config{Host: server.URL}, "work", "service")
	require.NoError(t, err)
	_, err = s.Get(context.Background())
	require.True(t, apierrors.IsTooManyRequests(err))
	require.Equal(t, int32(1), n.Load())
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type trackedBody struct {
	io.Reader
	closed atomic.Int32
	read   atomic.Int64
}

func (b *trackedBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.read.Add(int64(n))
	return n, err
}
func (b *trackedBody) Close() error { b.closed.Add(1); return nil }
func TestUnknownLengthBodyBoundAndCallerWrapperPreserved(t *testing.T) {
	body := &trackedBody{Reader: strings.NewReader(strings.Repeat(" ", 2*1024*1024+1000))}
	var wrapped atomic.Int32
	cfg := &rest.Config{Host: "https://localhost", Timeout: 3 * time.Second, WrapTransport: func(http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			wrapped.Add(1)
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: body, ContentLength: -1, Request: r}, nil
		})
	}}
	old := cfg.WrapTransport
	s, err := NewInferenceService(cfg, "work", "service")
	require.NoError(t, err)
	_, err = s.Get(context.Background())
	require.Error(t, err)
	require.LessOrEqual(t, body.read.Load(), int64(2*1024*1024+1))
	require.Equal(t, int32(1), body.closed.Load())
	require.Equal(t, int32(1), wrapped.Load())
	require.Equal(t, 3*time.Second, cfg.Timeout)
	require.Nil(t, cfg.GroupVersion)
	require.Empty(t, cfg.APIPath)
	require.NotNil(t, old)
	require.Equal(t, 3*time.Second, s.config.Timeout)
}
func TestRequestTimeoutCapAndInvalidConstruction(t *testing.T) {
	for _, tc := range []struct{ input, want time.Duration }{{0, 10 * time.Second}, {time.Minute, 10 * time.Second}, {time.Second, time.Second}} {
		s, err := NewInferenceService(&rest.Config{Host: "http://localhost", Timeout: tc.input}, "work", "service")
		require.NoError(t, err)
		require.Equal(t, tc.want, s.config.Timeout)
	}
	for _, tc := range []struct {
		cfg      *rest.Config
		ns, name string
	}{{nil, "work", "service"}, {&rest.Config{Host: "http://localhost"}, "bad/ns", "service"}, {&rest.Config{Host: "http://localhost"}, "work", "../secret"}} {
		_, err := NewInferenceService(tc.cfg, tc.ns, tc.name)
		require.Error(t, err)
	}
}
func TestDecodeRejectsUnrelatedAndMalformedIdentity(t *testing.T) {
	s, err := NewInferenceService(&rest.Config{Host: "http://localhost"}, "work", "service")
	require.NoError(t, err)
	for _, alter := range []func(*ome.InferenceService){func(v *ome.InferenceService) { v.Name = "other" }, func(v *ome.InferenceService) { v.Namespace = "other" }, func(v *ome.InferenceService) { v.UID = "" }, func(v *ome.InferenceService) { v.ResourceVersion = "" }, func(v *ome.InferenceService) { v.Kind = "Other" }} {
		v := wireService()
		alter(v)
		_, err = s.Decode(v)
		require.Error(t, err)
	}
	_, err = s.Decode(&metav1.Status{})
	require.Error(t, err)
}

func TestRealWireOversizedWatchStreamFallsBackWithoutPartialMatch(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	requests := make(chan string, 4)
	var gets atomic.Int32
	var watchClosed atomic.Int32
	var watchBytes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("watch") == "true" {
			require.Equal(t, "metadata.name=service", r.URL.Query().Get("fieldSelector"))
			requests <- "watch"
			// The Ready-shaped object is embedded in an over-budget incomplete frame,
			// so no prefix can become a satisfying watch observation.
			_, _ = io.WriteString(w, `{"type":"MODIFIED","object":{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"service","namespace":"work","uid":"uid","resourceVersion":"rv"},"status":{"conditions":[{"type":"Ready","status":"True","message":"`)
			w.(http.Flusher).Flush()
			_, _ = io.WriteString(w, strings.Repeat("PRIVATE-WATCH-MESSAGE", 120000))
			w.(http.Flusher).Flush()
			return
		}
		requests <- "get"
		n := gets.Add(1)
		status := "False"
		if n > 1 {
			status = "True"
		}
		_, _ = io.WriteString(w, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"service","namespace":"work","uid":"uid","resourceVersion":"rv"},"status":{"conditions":[{"type":"Ready","status":"`+status+`"}]}}`)
	}))
	defer server.Close()
	cfg := &rest.Config{Host: server.URL, WrapTransport: func(base http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			response, err := base.RoundTrip(r)
			if err == nil && r.URL.Query().Get("watch") == "true" {
				response.Body = &watchBodyTracker{body: response.Body, closed: &watchClosed, bytes: &watchBytes}
			}
			return response, err
		})
	}}
	source, err := NewInferenceService(cfg, "work", "service")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type completion struct {
		r   waitengine.Result[*ome.InferenceService]
		err error
	}
	done := make(chan completion, 1)
	go func() {
		r, e := waitengine.Run(ctx, source, func(v *ome.InferenceService) (waitengine.Decision, error) {
			return waitengine.Decision{Matched: v.Status.Conditions[0].Status == "True"}, nil
		}, waitengine.Options{Timeout: time.Minute, Clock: clk})
		done <- completion{r, e}
	}()
	for _, want := range []string{"get", "watch"} {
		select {
		case got := <-requests:
			require.Equal(t, want, got)
		case <-time.After(time.Second):
			t.Fatal("wire request missing")
		}
	}
	require.Eventually(t, func() bool { return clk.Waiters() == 2 }, time.Second, time.Millisecond)
	require.Equal(t, int32(1), watchClosed.Load())
	require.LessOrEqual(t, watchBytes.Load(), int64(2*1024*1024+1))
	require.Equal(t, int32(1), gets.Load())
	clk.Step(5 * time.Second)
	select {
	case c := <-done:
		require.NoError(t, c.err)
		require.Equal(t, waitengine.OutcomeMatched, c.r.Outcome)
		require.True(t, c.r.Fallback)
		require.Equal(t, waitengine.MethodPoll, c.r.Method)
		require.Equal(t, 2, c.r.Counts.Gets)
		require.Equal(t, 2, c.r.Counts.Observations)
	case <-time.After(time.Second):
		t.Fatal("poll fallback did not finish")
	}
}

type watchBodyTracker struct {
	body   io.ReadCloser
	closed *atomic.Int32
	bytes  *atomic.Int64
}

func (b *watchBodyTracker) Read(p []byte) (int, error) {
	n, err := b.body.Read(p)
	b.bytes.Add(int64(n))
	return n, err
}
func (b *watchBodyTracker) Close() error { b.closed.Add(1); return b.body.Close() }

func TestRealWireParentCancellationClosesActiveWatch(t *testing.T) {
	watchStarted := make(chan struct{})
	watchEnded := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("watch") == "true" {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			close(watchStarted)
			<-r.Context().Done()
			close(watchEnded)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(wireService()))
	}))
	defer server.Close()
	source, err := NewInferenceService(&rest.Config{Host: server.URL}, "work", "service")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, e := waitengine.Run(ctx, source, func(*ome.InferenceService) (waitengine.Decision, error) { return waitengine.Decision{}, nil }, waitengine.Options{Timeout: time.Minute})
		done <- e
	}()
	select {
	case <-watchStarted:
	case <-time.After(time.Second):
		t.Fatal("watch not opened")
	}
	cancel()
	select {
	case err := <-done:
		require.Equal(t, waitengine.ReasonCanceled, err.(*waitengine.Error).Reason)
	case <-time.After(time.Second):
		t.Fatal("engine did not cancel")
	}
	select {
	case <-watchEnded:
	case <-time.After(time.Second):
		t.Fatal("watch response body not closed")
	}
}
