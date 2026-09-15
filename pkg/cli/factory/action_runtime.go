package factory

import (
	"context"
	"io"
	"net/http"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// ActionRuntimeResolver optionally bounds background discovery for actions.
type ActionRuntimeResolver interface {
	RuntimeClientForAction(context.Context) (ctrlclient.Client, error)
}

func (f *defaultFactory) RuntimeClientForAction(ctx context.Context) (ctrlclient.Client, error) {
	config, httpClient, err := f.actionConfigAndClient(ctx)
	if err != nil {
		return nil, err
	}
	// This client is deliberately not cached: background discovery must be
	// canceled with this action, without poisoning future read commands.
	// controller-runtime supplies Options.HTTPClient to both its dynamic
	// discovery mapper and object clients (NewDynamicRESTMapper uses the public
	// discovery ConfigAndClient constructor).
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	return ctrlclient.New(config, ctrlclient.Options{Scheme: scheme, HTTPClient: httpClient})
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
