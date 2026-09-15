package doctorcollection

import (
	"errors"
	"net/http"

	"k8s.io/client-go/discovery"
	appsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
	"k8s.io/client-go/rest"
	omev1 "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

// Clients can only be populated by NewClients. Factory-owned REST clients
// cannot safely be copied, and arbitrary rest.Interface implementations do
// not expose a redirect policy. The zero value fails closed before any read.
type Clients struct {
	discovery, apps, ome rest.Interface
	httpClient           *http.Client
}

// NewClients constructs fresh fixed-purpose REST clients without API requests.
// It preserves the effective configuration's transport/TLS/auth, wrappers and
// timeout, but rejects every HTTP redirect before a destination request.
// Custom RoundTripper implementations may still fan out internally.
func NewClients(config *rest.Config, selectedISVC bool) (Clients, error) {
	failure := errors.New("doctor API client construction failed")
	if config == nil {
		return Clients{}, failure
	}
	local := *config
	if local.ExecProvider != nil {
		// CopyConfig replaces ExecProvider.Config through its provider pointer.
		// Detach the provider before that first copy to preserve caller ownership.
		local.ExecProvider = local.ExecProvider.DeepCopy()
	}
	copied := rest.CopyConfig(&local)
	inherited, err := rest.HTTPClientFor(copied)
	if err != nil {
		return Clients{}, failure
	}
	// HTTPClientFor may return http.DefaultClient. Copy only the HTTP client,
	// never a RESTClient (whose content provider contains an atomic.Bool).
	isolated := *inherited
	isolated.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	discoveryClient, err := discovery.NewDiscoveryClientForConfigAndClient(copied, &isolated)
	if err != nil {
		return Clients{}, failure
	}
	appsClient, err := appsv1.NewForConfigAndClient(copied, &isolated)
	if err != nil {
		return Clients{}, failure
	}
	clients := Clients{discovery: discoveryClient.RESTClient(), apps: appsClient.RESTClient(), httpClient: &isolated}
	if selectedISVC {
		omeClient, err := omev1.NewForConfigAndClient(copied, &isolated)
		if err != nil {
			return Clients{}, failure
		}
		clients.ome = omeClient.RESTClient()
	}
	return clients, nil
}

func (c Clients) valid(selectedISVC bool) bool {
	if c.httpClient == nil || c.httpClient.CheckRedirect == nil {
		return false
	}
	owned := func(client rest.Interface) bool {
		concrete, ok := client.(*rest.RESTClient)
		return ok && concrete != nil && concrete.Client == c.httpClient
	}
	return owned(c.discovery) && owned(c.apps) && (!selectedISVC || owned(c.ome))
}
