package waitsource

import (
	"errors"
	"io"
	"net/http"
	"sync"
)

const responseByteLimit int64 = 2 * 1024 * 1024

var errBodyLimit = errors.New("WaitResponseBudgetExceeded")
var errBodyRead = errors.New("WaitResponseUnreadable")

type boundedTransport struct{ base http.RoundTripper }

func (t boundedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(r)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, err
	}
	if response == nil || response.Body == nil {
		return nil, errSource
	}
	if response.ContentLength > responseByteLimit {
		_ = response.Body.Close()
		return nil, errBodyLimit
	}
	copy := new(http.Response)
	*copy = *response
	copy.Body = &boundedBody{body: response.Body, remaining: responseByteLimit + 1}
	return copy, nil
}

// boundedBody reads at most limit+1 bytes even for unknown-length/chunked
// bodies. An over-budget read is discarded, never decoded as a partial frame.
type boundedBody struct {
	body      io.ReadCloser
	remaining int64
	once      sync.Once
	closeErr  error
	failed    bool
}

func (b *boundedBody) Read(p []byte) (int, error) {
	if b.failed {
		return 0, errBodyLimit
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.body.Read(p)
	b.remaining -= int64(n)
	if b.remaining == 0 {
		b.failed = true
		_ = b.Close()
		return 0, errBodyLimit
	}
	if err != nil {
		_ = b.Close()
		if err != io.EOF {
			return 0, errBodyRead
		}
	}
	return n, err
}
func (b *boundedBody) Close() error {
	b.once.Do(func() { b.closeErr = b.body.Close() })
	return b.closeErr
}
