package factory

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// ActionRuntimeResolver optionally bounds background discovery for actions.
type ActionRuntimeResolver interface {
	RuntimeClientForAction(context.Context) (ctrlclient.Client, error)
}

func (f *defaultFactory) RuntimeClientForAction(ctx context.Context) (ctrlclient.Client, error) {
	if f == nil || ctx == nil || f.flags == nil && f.rest == nil {
		return nil, errors.New("action runtime configuration is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	config, err := f.RESTConfig()
	if err != nil {
		return nil, err
	}
	config = rest.CopyConfig(config)
	if config.Timeout <= 0 || config.Timeout > 10*time.Second {
		config.Timeout = 10 * time.Second
	}
	config.Wrap(func(inner http.RoundTripper) http.RoundTripper {
		return &actionContextTransport{inner: inner, ctx: ctx}
	})
	// This client is deliberately not cached: background discovery must be
	// canceled with this action, without poisoning future read commands.
	return newRuntimeClient(config)
}

type actionContextTransport struct {
	inner http.RoundTripper
	ctx   context.Context
}

func (t *actionContextTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancel(request.Context())
	stop := context.AfterFunc(t.ctx, cancel)
	if t.ctx.Err() != nil {
		cancel()
	}
	var once sync.Once
	cleanup := func() { once.Do(func() { stop(); cancel() }) }
	response, err := t.inner.RoundTrip(request.WithContext(ctx))
	if err != nil || response == nil || response.Body == nil {
		cleanup()
		return response, err
	}
	response.Body = &actionResponseBody{ReadCloser: response.Body, cleanup: cleanup}
	return response, nil
}

type actionResponseBody struct {
	io.ReadCloser
	cleanup func()
}

func (b *actionResponseBody) Close() error {
	err := b.ReadCloser.Close()
	b.cleanup()
	return err
}
