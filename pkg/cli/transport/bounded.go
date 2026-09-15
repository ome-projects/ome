package transport

import (
	"errors"
	"io"
	"net/http"

	"k8s.io/client-go/rest"
)

// ErrResponseTooLarge identifies a bounded response read, not request failure.
// A mutation may already have been accepted; callers must not replay it.
var ErrResponseTooLarge = errors.New("API response exceeds safety bounds")

// NewBounded constructs a non-streaming client that reads at most limit+1
// response bytes, including error responses. Existing wrappers are preserved.
// Do not use this client for streaming watches.
func NewBounded(config *rest.Config, limit int64) (*Client, error) {
	if config == nil || limit < 1 || limit > 64*1024*1024 {
		return nil, errors.New("transport: bounded response configuration is invalid")
	}
	cfg := rest.CopyConfig(config)
	cfg.Wrap(func(inner http.RoundTripper) http.RoundTripper {
		return &boundedTransport{inner: inner, limit: limit}
	})
	return New(cfg)
}

type boundedTransport struct {
	inner http.RoundTripper
	limit int64
}

func (t *boundedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.inner.RoundTrip(request)
	if err != nil || response == nil || response.Body == nil {
		return response, err
	}
	response.Body = &boundedBody{ReadCloser: response.Body, remaining: t.limit}
	return response, nil
}

type boundedBody struct {
	io.ReadCloser
	remaining int64
	exceeded  bool
}

func (b *boundedBody) Read(buffer []byte) (int, error) {
	if b.exceeded {
		return 0, ErrResponseTooLarge
	}
	if int64(len(buffer)) > b.remaining+1 {
		buffer = buffer[:b.remaining+1]
	}
	n, err := b.ReadCloser.Read(buffer)
	b.remaining -= int64(n)
	if b.remaining < 0 {
		b.exceeded = true
		return n, ErrResponseTooLarge
	}
	return n, err
}
