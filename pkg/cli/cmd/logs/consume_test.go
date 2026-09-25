package logs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

func TestConsumeLogsNonFollowOpensOneResponseAtATime(t *testing.T) {
	tracker := newStreamTracker(3)
	tracker.releaseAll()
	targets := []logTarget{
		{podName: "chat-decoder-0", prefix: "[decoder/chat-decoder-0] "},
		{podName: "chat-engine-0", prefix: "[engine/chat-engine-0] "},
		{podName: "chat-router-0", prefix: "[router/chat-router-0] "},
	}

	var out bytes.Buffer
	require.NoError(t, consumeLogs(context.Background(), nil, targets, false, 5, tracker.open, &out))
	assert.Equal(t, int32(1), tracker.maximum.Load())
	assert.Zero(t, tracker.active.Load())
	assert.Equal(t, int32(3), tracker.closed.Load())
	assert.Equal(t,
		"[decoder/chat-decoder-0] chat-decoder-0\n"+
			"[engine/chat-engine-0] chat-engine-0\n"+
			"[router/chat-router-0] chat-router-0\n",
		out.String())
}

func TestConsumeLogsFollowNeverExceedsCap(t *testing.T) {
	tracker := newStreamTracker(5)
	targets := []logTarget{
		{podName: "chat-0"},
		{podName: "chat-1"},
		{podName: "chat-2"},
		{podName: "chat-3"},
		{podName: "chat-4"},
	}
	result := make(chan error, 1)
	go func() {
		result <- consumeLogs(context.Background(), nil, targets, true, 5, tracker.open, io.Discard)
	}()
	tracker.waitUntilAllOpened(t)
	assert.Equal(t, int32(5), tracker.maximum.Load())
	assert.Equal(t, int32(5), tracker.active.Load())
	tracker.releaseAll()
	require.NoError(t, receiveConsumeError(t, result))
	assert.Zero(t, tracker.active.Load())
	assert.Equal(t, int32(5), tracker.closed.Load())
}

func TestConsumeLogsFollowRejectsOverCapBeforeOpening(t *testing.T) {
	var opened atomic.Int32
	open := func(context.Context, coreclient.PodInterface, logTarget) (io.ReadCloser, error) {
		opened.Add(1)
		return io.NopCloser(strings.NewReader("unexpected\n")), nil
	}
	targets := make([]logTarget, 6)
	err := consumeLogs(context.Background(), nil, targets, true, 5, open, io.Discard)
	require.EqualError(t, err, "you are attempting to follow 6 log streams, but maximum allowed concurrency is 5, use --max-log-requests to increase the limit")
	assert.Zero(t, opened.Load())
}

func TestConsumeLogsStartupFailureCancelsAndClosesEarlierStreams(t *testing.T) {
	want := errors.New("open failed")
	first := newCountingReadCloser(strings.NewReader("unused\n"))
	var calls atomic.Int32
	var firstContext context.Context
	open := func(ctx context.Context, _ coreclient.PodInterface, target logTarget) (io.ReadCloser, error) {
		switch calls.Add(1) {
		case 1:
			firstContext = ctx
			return first, nil
		case 2:
			return nil, want
		default:
			t.Fatalf("opened a later target after %s", target.podName)
			return nil, errors.New("unreachable")
		}
	}
	err := consumeLogs(context.Background(), nil, []logTarget{
		{podName: "chat-decoder-0"},
		{podName: "chat-engine-0"},
		{podName: "chat-router-0"},
	}, true, 5, open, io.Discard)
	require.ErrorIs(t, err, want)
	assert.Contains(t, err.Error(), "streaming logs for pod chat-engine-0")
	require.NotNil(t, firstContext)
	require.ErrorIs(t, context.Cause(firstContext), want)
	assert.Equal(t, int32(1), first.closes.Load())
	assert.Equal(t, int32(2), calls.Load())
}

func TestDefaultOpenLogStreamPreservesClientGoTransportBehavior(t *testing.T) {
	var sourceWrapped, destinationWrapped, destinationQuery string
	var transportCalls atomic.Int32
	warnings := &countingWarningHandler{}
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		destinationWrapped = r.Header.Get("X-Fixture-Transport")
		destinationQuery = r.URL.RawQuery
		w.Header().Set("Warning", `299 fixture "stream warning"`)
		_, _ = io.WriteString(w, "wire log\n")
	}))
	defer destination.Close()

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceWrapped = r.Header.Get("X-Fixture-Transport")
		location := destination.URL + r.URL.Path
		if r.URL.RawQuery != "" {
			location += "?" + r.URL.RawQuery
		}
		http.Redirect(w, r, location, http.StatusTemporaryRedirect)
	}))
	defer source.Close()

	sourceURL := strings.Replace(source.URL, "127.0.0.1", "localhost", 1)
	client, err := kubernetes.NewForConfig(&rest.Config{
		Host:                      sourceURL,
		WarningHandlerWithContext: warnings,
		WrapTransport: func(delegate http.RoundTripper) http.RoundTripper {
			return roundTripperFunc(func(request *http.Request) (*http.Response, error) {
				transportCalls.Add(1)
				request = request.Clone(request.Context())
				request.Header.Set("X-Fixture-Transport", "present")
				return delegate.RoundTrip(request)
			})
		},
	})
	require.NoError(t, err)
	tail, limit := int64(7), int64(2048)
	reader, err := defaultOpenLogStream(context.Background(), client.CoreV1().Pods("team-a"), logTarget{
		podName: "chat-engine-0",
		options: corev1.PodLogOptions{
			TailLines:  &tail,
			LimitBytes: &limit,
		},
	})
	require.NoError(t, err)
	raw, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, "wire log\n", string(raw))
	assert.Equal(t, "present", sourceWrapped)
	assert.Equal(t, "present", destinationWrapped)
	assert.Equal(t, int32(2), transportCalls.Load())
	assert.Contains(t, destinationQuery, "tailLines=7")
	assert.Contains(t, destinationQuery, "limitBytes=2048")
	assert.NotContains(t, destinationQuery, "follow=")
	assert.Equal(t, int32(1), warnings.count.Load())
}

type streamTracker struct {
	total   int
	release chan struct{}
	opened  chan struct{}
	once    sync.Once
	active  atomic.Int32
	maximum atomic.Int32
	closed  atomic.Int32
}

func newStreamTracker(total int) *streamTracker {
	return &streamTracker{
		total:   total,
		release: make(chan struct{}),
		opened:  make(chan struct{}, total),
	}
}

func (t *streamTracker) open(_ context.Context, _ coreclient.PodInterface, target logTarget) (io.ReadCloser, error) {
	active := t.active.Add(1)
	for {
		maximum := t.maximum.Load()
		if active <= maximum || t.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	t.opened <- struct{}{}
	return &trackedReadCloser{tracker: t, name: target.podName, closed: make(chan struct{})}, nil
}

func (t *streamTracker) waitUntilAllOpened(testingT *testing.T) {
	testingT.Helper()
	for range t.total {
		select {
		case <-t.opened:
		case <-time.After(time.Second):
			testingT.Fatal("not all log responses opened")
		}
	}
}

func (t *streamTracker) releaseAll() { t.once.Do(func() { close(t.release) }) }

type trackedReadCloser struct {
	tracker *streamTracker
	name    string
	closed  chan struct{}
	once    sync.Once
	sent    bool
}

func (r *trackedReadCloser) Read(p []byte) (int, error) {
	select {
	case <-r.tracker.release:
	case <-r.closed:
		return 0, context.Canceled
	}
	if r.sent {
		return 0, io.EOF
	}
	r.sent = true
	return copy(p, r.name+"\n"), nil
}

func (r *trackedReadCloser) Close() error {
	r.once.Do(func() {
		close(r.closed)
		r.tracker.active.Add(-1)
		r.tracker.closed.Add(1)
	})
	return nil
}

type countingReadCloser struct {
	io.Reader
	closes atomic.Int32
}

func newCountingReadCloser(reader io.Reader) *countingReadCloser {
	return &countingReadCloser{Reader: reader}
}

func (r *countingReadCloser) Close() error {
	r.closes.Add(1)
	return nil
}

func receiveConsumeError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("log consumption did not finish")
		return nil
	}
}

type countingWarningHandler struct{ count atomic.Int32 }

func (h *countingWarningHandler) HandleWarningHeaderWithContext(context.Context, int, string, string) {
	h.count.Add(1)
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
