package transport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

func TestJSONPatchNeverReplaysRetryAfter(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(429)
		_, _ = io.WriteString(w, "busy")
	}))
	defer server.Close()
	client, err := New(&rest.Config{Host: server.URL})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = client.JSONPatch(ctx, Resource{Namespace: "prod", Resource: "inferenceservices", Name: "chat"}, []byte(`[]`), JSONPatchOptions{})
	require.Error(t, err)
	require.EqualValues(t, 1, calls.Load(), "operator must explicitly retry a guarded mutation")
}

func TestBoundedPatchResponseCapsReadsAndPreservesTransportWrapper(t *testing.T) {
	for _, size := range []int{64, 65, 4096} {
		t.Run(strings.Repeat("x", size%10), func(t *testing.T) {
			var observed atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, strings.Repeat("x", size))
			}))
			defer server.Close()
			config := &rest.Config{Host: server.URL, WrapTransport: func(inner http.RoundTripper) http.RoundTripper {
				return roundTripFunc(func(r *http.Request) (*http.Response, error) {
					observed.Add(1)
					return inner.RoundTrip(r)
				})
			}}
			client, err := NewBounded(config, 64)
			require.NoError(t, err)
			body, err := client.JSONPatch(context.Background(), Resource{Namespace: "prod", Resource: "inferenceservices", Name: "chat"}, []byte(`[]`), JSONPatchOptions{})
			if size <= 64 {
				require.NoError(t, err)
				require.Len(t, body, size)
			} else {
				require.Error(t, err)
				require.Empty(t, body)
			}
			require.EqualValues(t, 1, observed.Load())
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// An infinite unknown-length body proves the limit applies before Raw's full
// read, rather than checking the length after already buffering a response.
func TestBoundedPatchStopsUnknownLengthResponseAndClosesBody(t *testing.T) {
	for _, contentLength := range []int64{-1, 4096} {
		body := &countingBody{}
		config := &rest.Config{Host: "https://example.invalid", WrapTransport: func(http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: body, ContentLength: contentLength}, nil
			})
		}}
		client, err := NewBounded(config, 64)
		require.NoError(t, err)
		data, err := client.JSONPatch(context.Background(), Resource{Namespace: "prod", Resource: "inferenceservices", Name: "chat"}, []byte(`[]`), JSONPatchOptions{})
		require.ErrorIs(t, err, ErrResponseTooLarge)
		require.Empty(t, data)
		require.Equal(t, 65, body.read)
		require.True(t, body.closed)
		n, err := (&boundedBody{ReadCloser: body, exceeded: true}).Read(make([]byte, 512))
		require.Zero(t, n)
		require.ErrorIs(t, err, ErrResponseTooLarge)
		require.Equal(t, 65, body.read)
	}
	for _, limit := range []int64{-1, 0, 64*1024*1024 + 1} {
		_, err := NewBounded(&rest.Config{Host: "https://example.invalid"}, limit)
		require.Error(t, err)
	}
	_, err := NewBounded(nil, 64)
	require.Error(t, err)
}

type countingBody struct {
	read   int
	closed bool
}

func (b *countingBody) Read(buffer []byte) (int, error) {
	for i := range buffer {
		buffer[i] = 'x'
	}
	b.read += len(buffer)
	return len(buffer), nil
}

func (b *countingBody) Close() error { b.closed = true; return nil }
