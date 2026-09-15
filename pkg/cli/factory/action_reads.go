package factory

import (
	"context"
	"errors"
	"net/http"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

// ActionReadClientsResolver optionally provides action-owned, uncached GET/LIST
// clients. Injected Static/custom factories keep their explicit clients.
type ActionReadClientsResolver interface {
	OMEClientForAction(context.Context) (versioned.Interface, error)
	KubeClientForAction(context.Context) (kubernetes.Interface, error)
}

func (f *defaultFactory) actionConfigAndClient(ctx context.Context) (*rest.Config, *http.Client, error) {
	if f == nil || ctx == nil || f.flags == nil && f.rest == nil {
		return nil, nil, errors.New("action read configuration is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	config, err := f.RESTConfig()
	if err != nil {
		return nil, nil, err
	}
	if config == nil {
		return nil, nil, errors.New("action read configuration is unavailable")
	}
	// rest.CopyConfig assigns ExecProvider.Config through its shared pointer.
	// Take ownership before that operation, including its error paths.
	local := *config
	if local.ExecProvider != nil {
		local.ExecProvider = local.ExecProvider.DeepCopy()
	}
	owned := rest.CopyConfig(&local)
	if owned.Timeout <= 0 || owned.Timeout > 10*time.Second {
		owned.Timeout = 10 * time.Second
	}
	owned.WarningHandler = rest.NoWarnings{}
	owned.WarningHandlerWithContext = rest.NoWarnings{}
	owned.Wrap(func(inner http.RoundTripper) http.RoundTripper {
		return &actionContextTransport{inner: inner, ctx: ctx}
	})
	client, err := rest.HTTPClientFor(owned)
	if err != nil {
		return nil, nil, err
	}
	// HTTPClientFor may return http.DefaultClient. Never mutate or copy a used
	// client: construct a fresh one with the selected transport and timeout.
	fresh := &http.Client{Transport: client.Transport, Timeout: client.Timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return owned, fresh, nil
}

func (f *defaultFactory) OMEClientForAction(ctx context.Context) (versioned.Interface, error) {
	config, client, err := f.actionConfigAndClient(ctx)
	if err != nil {
		return nil, err
	}
	return versioned.NewForConfigAndClient(config, client)
}

func (f *defaultFactory) KubeClientForAction(ctx context.Context) (kubernetes.Interface, error) {
	config, client, err := f.actionConfigAndClient(ctx)
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfigAndClient(config, client)
}
