// Package waitsource provides a narrow GET/WATCH-only wait acquisition source.
package waitsource

import (
	"context"
	"errors"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
)

var errSource = errors.New("InvalidWaitSource")

type InferenceService struct {
	client          rest.Interface
	config          *rest.Config
	namespace, name string
}

// NewInferenceService copies the resolved config, preserving authentication,
// TLS and caller wrappers while bounding each response and request. Cancellation
// is cooperative: external credential plugins/custom transports may ignore it.
func NewInferenceService(config *rest.Config, namespace, name string) (*InferenceService, error) {
	if config == nil || len(utilvalidation.IsDNS1123Label(namespace)) > 0 || len(utilvalidation.IsDNS1123Subdomain(name)) > 0 {
		return nil, errSource
	}
	// CopyConfig normalizes ExecProvider.Config in place. Detach the caller's
	// provider before that first copy, including on construction failure.
	owned := *config
	if config.ExecProvider != nil {
		owned.ExecProvider = config.ExecProvider.DeepCopy()
	}
	cp := rest.CopyConfig(&owned)
	scheme := runtime.NewScheme()
	if err := ome.AddToScheme(scheme); err != nil {
		return nil, errSource
	}
	gv := ome.SchemeGroupVersion
	cp.GroupVersion = &gv
	cp.APIPath = "/apis"
	cp.NegotiatedSerializer = serializer.WithoutConversionCodecFactory{CodecFactory: serializer.NewCodecFactory(scheme)}
	cp.ContentType = "application/json"
	cp.AcceptContentTypes = "application/json"
	cp.WarningHandler = rest.NoWarnings{}
	cp.WarningHandlerWithContext = rest.NoWarnings{}
	if cp.Timeout <= 0 || cp.Timeout > 10*time.Second {
		cp.Timeout = 10 * time.Second
	}
	cp.Wrap(func(base http.RoundTripper) http.RoundTripper { return boundedTransport{base: base} })
	selected, err := rest.HTTPClientFor(cp)
	if err != nil {
		return nil, errSource
	}
	// A named read must not turn into an unselected read via any redirect.
	// Use a fresh client rather than changing a shared or default client.
	httpClient := &http.Client{Transport: selected.Transport, Timeout: cp.Timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	client, err := rest.RESTClientForConfigAndClient(cp, httpClient)
	if err != nil {
		return nil, errSource
	}
	return &InferenceService{client: client, config: cp, namespace: namespace, name: name}, nil
}

func (s *InferenceService) Get(ctx context.Context) (waitengine.Snapshot[*ome.InferenceService], error) {
	value := &ome.InferenceService{}
	err := s.client.Get().Namespace(s.namespace).Resource("inferenceservices").Name(s.name).MaxRetries(0).Do(ctx).Into(value)
	if err != nil {
		return waitengine.Snapshot[*ome.InferenceService]{}, err
	}
	return s.Decode(value)
}

func (s *InferenceService) Watch(ctx context.Context, resourceVersion string) (watch.Interface, error) {
	if resourceVersion == "" {
		return nil, errSource
	}
	return s.client.Get().Namespace(s.namespace).Resource("inferenceservices").
		Param("watch", "true").Param("fieldSelector", "metadata.name="+s.name).
		Param("resourceVersion", resourceVersion).MaxRetries(0).Watch(ctx)
}

func (s *InferenceService) Decode(obj runtime.Object) (waitengine.Snapshot[*ome.InferenceService], error) {
	v, ok := obj.(*ome.InferenceService)
	if !ok || v == nil || v.Name != s.name || v.Namespace != s.namespace || v.UID == "" || v.ResourceVersion == "" ||
		(v.Kind != "" && v.Kind != "InferenceService") || (v.APIVersion != "" && v.APIVersion != "ome.io/v1beta1") {
		return waitengine.Snapshot[*ome.InferenceService]{}, errSource
	}
	copy := v.DeepCopy()
	return waitengine.Snapshot[*ome.InferenceService]{Value: copy, UID: copy.UID, ResourceVersion: copy.ResourceVersion, Deleting: copy.DeletionTimestamp != nil}, nil
}
