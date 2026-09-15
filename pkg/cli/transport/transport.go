// Package transport provides the CLI's narrow REST seam for OME API requests
// whose wire contracts are not represented by the generated clients.
package transport

import (
	"context"
	"errors"
	"net/http"
	"time"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

var (
	transportScheme         = runtime.NewScheme()
	transportCodecs         = serializer.NewCodecFactory(transportScheme)
	transportParameterCodec = runtime.NewParameterCodec(transportScheme)
)

// ErrResponseIdentity means a response cannot prove mutation acceptance.
// The request may already have applied; callers must not replay it.
var ErrResponseIdentity = errors.New("API response identity is invalid or ambiguous; outcome unknown")

func init() {
	metav1.AddToGroupVersion(transportScheme, schema.GroupVersion{Version: "v1"})
	utilruntime.Must(v1beta1.AddToScheme(transportScheme))
	utilruntime.Must(autoscalingv1.AddToScheme(transportScheme))
}

// Resource identifies one OME API object.
type Resource struct {
	Namespace string
	Resource  string
	Name      string
}

// Collection identifies one OME API resource collection.
type Collection struct {
	Namespace string
	Resource  string
}

// JSONPatchOptions controls optional JSON Patch request behavior.
type JSONPatchOptions struct {
	DryRun bool
}

// Client sends OME-specific REST requests.
type Client struct {
	rest rest.Interface
}

// New constructs a Client without modifying config or following HTTP redirects.
// A redirect can replay a mutation or remove its dry-run query.
func New(config *rest.Config) (*Client, error) {
	if config == nil {
		return nil, errors.New("transport: REST config is nil")
	}

	cfg := copyTransportConfig(config)
	groupVersion := v1beta1.SchemeGroupVersion
	cfg.GroupVersion = &groupVersion
	cfg.APIPath = "/apis"
	cfg.ContentType = runtime.ContentTypeJSON
	cfg.AcceptContentTypes = runtime.ContentTypeJSON
	cfg.NegotiatedSerializer = rest.CodecFactoryForGeneratedClient(transportScheme, transportCodecs).WithoutConversion()
	if cfg.UserAgent == "" {
		cfg.UserAgent = rest.DefaultKubernetesUserAgent()
	}

	inherited, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return nil, err
	}
	// HTTPClientFor may return the shared default. Copy only http.Client,
	// never a RESTClient (which contains atomic state).
	isolated := *inherited
	isolated.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	restClient, err := rest.RESTClientForConfigAndClient(cfg, &isolated)
	if err != nil {
		return nil, err
	}
	return &Client{rest: restClient}, nil
}

func copyTransportConfig(config *rest.Config) *rest.Config {
	local := *config
	if local.ExecProvider != nil {
		// CopyConfig replaces Config through the shared ExecProvider pointer.
		local.ExecProvider = local.ExecProvider.DeepCopy()
	}
	return rest.CopyConfig(&local)
}

// JSONPatch applies patch exactly as supplied and returns an unmodified JSON
// object response with unambiguous identity fields. It never replays a request.
func (c *Client) JSONPatch(ctx context.Context, resource Resource, patch []byte, options JSONPatchOptions) ([]byte, error) {
	return c.jsonPatch(ctx, resource, "", patch, options)
}

func (c *Client) jsonPatch(ctx context.Context, resource Resource, subresource string, patch []byte, options JSONPatchOptions) ([]byte, error) {
	patchOptions := metav1.PatchOptions{}
	if options.DryRun {
		patchOptions.DryRun = []string{metav1.DryRunAll}
	}

	request := c.rest.Patch(types.JSONPatchType).
		Namespace(resource.Namespace).
		Resource(resource.Resource).
		Name(resource.Name)
	if subresource != "" {
		request = request.SubResource(subresource)
	}
	result := request.
		VersionedParams(&patchOptions, transportParameterCodec).
		Body(patch).
		WarningHandlerWithContext(rest.NoWarnings{}).
		MaxRetries(0).
		Do(ctx)
	if err := result.Error(); err != nil {
		return nil, err
	}
	raw, err := result.Raw()
	if err != nil {
		return nil, err
	}
	if !unambiguousResponseIdentity(raw) {
		return nil, ErrResponseIdentity
	}
	return raw, nil
}

// Watch opens a streaming watch for an OME API resource collection.
func (c *Client) Watch(ctx context.Context, collection Collection, options metav1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if options.TimeoutSeconds != nil {
		timeout = time.Duration(*options.TimeoutSeconds) * time.Second
	}
	options.Watch = true
	return c.rest.Get().
		NamespaceIfScoped(collection.Namespace, collection.Namespace != "").
		Resource(collection.Resource).
		VersionedParams(&options, transportParameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// GetInferenceReplicaScale returns an InferenceReplica's scale subresource.
func (c *Client) GetInferenceReplicaScale(ctx context.Context, namespace, name string, options metav1.GetOptions) (*autoscalingv1.Scale, error) {
	result := c.rest.Get().
		Namespace(namespace).
		Resource("inferencereplicas").
		Name(name).
		SubResource("scale").
		VersionedParams(&options, transportParameterCodec).
		WarningHandlerWithContext(rest.NoWarnings{}).
		MaxRetries(0).
		Do(ctx)
	if err := result.Error(); err != nil {
		return nil, err
	}
	raw, err := result.Raw()
	if err != nil {
		return nil, err
	}
	return decodeScaleResponse(raw)
}

// UpdateInferenceReplicaScale updates an InferenceReplica's scale subresource.
func (c *Client) UpdateInferenceReplicaScale(ctx context.Context, namespace, name string, scale *autoscalingv1.Scale, options metav1.UpdateOptions) (*autoscalingv1.Scale, error) {
	result := &autoscalingv1.Scale{}
	err := c.rest.Put().
		Namespace(namespace).
		Resource("inferencereplicas").
		Name(name).
		SubResource("scale").
		VersionedParams(&options, transportParameterCodec).
		Body(scale).
		Do(ctx).
		Into(result)
	return result, err
}
