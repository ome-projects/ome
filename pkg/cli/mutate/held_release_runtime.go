package mutate

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	appsclient "k8s.io/client-go/kubernetes/typed/apps/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/paging"
)

// HeldRuntimeBinding retains only private admitted originals for finite
// revalidation. Generated/custom clients have already decoded their payload;
// this guard prevents resolver copies/fingerprints, not initial decoding.
type HeldRuntimeBinding struct {
	mu      sync.Mutex
	objects map[string]runtime.Object
	reads   int
}

func (*HeldRuntimeBinding) String() string   { return "<HeldRuntimeBinding redacted>" }
func (*HeldRuntimeBinding) GoString() string { return "<HeldRuntimeBinding redacted>" }

func (b *HeldRuntimeBinding) admit(value runtime.Object) error {
	if value == nil || !boundedPrivatePayload(value) {
		return ErrBounds
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reads++
	if b.reads > 96 {
		return ErrBounds
	}
	accessor, err := meta.Accessor(value)
	if err != nil {
		return ErrRuntime
	}
	key := reflect.TypeOf(value).String() + "/" + accessor.GetNamespace() + "/" + accessor.GetName()
	if old, exists := b.objects[key]; exists {
		if !reflect.DeepEqual(old, value) {
			return ErrStale
		}
		return nil
	}
	if len(b.objects) >= 64 {
		return ErrBounds
	}
	// Inspect the aggregate admitted originals before creating another copy.
	if !boundedPrivatePayload(struct {
		Objects map[string]runtime.Object
		Next    runtime.Object
	}{b.objects, value}) {
		return ErrBounds
	}
	b.objects[key] = value.DeepCopyObject()
	return nil
}

// Matches compares private typed snapshots; no secret-derived fingerprint is
// exposed as a mutation identity or report field.
func (b *HeldRuntimeBinding) Matches(other *HeldRuntimeBinding) bool {
	if b == nil || other == nil {
		return false
	}
	if b == other {
		return true
	}
	return reflect.DeepEqual(b.snapshot(), other.snapshot())
}

func (b *HeldRuntimeBinding) snapshot() map[string]runtime.Object {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := make(map[string]runtime.Object, len(b.objects))
	for key, value := range b.objects {
		result[key] = value
	}
	return result
}

type heldRuntimeClient struct {
	ctrlclient.Client
	binding *HeldRuntimeBinding
	timeout time.Duration
}

func (c *heldRuntimeClient) Get(ctx context.Context, key ctrlclient.ObjectKey, value ctrlclient.Object, opts ...ctrlclient.GetOption) error {
	call, cancel := context.WithTimeout(ctx, heldRequestTimeout(c.timeout))
	defer cancel()
	err := c.Client.Get(call, key, value, opts...)
	if call.Err() != nil {
		return call.Err()
	}
	if err != nil {
		// The existing resolver uses NotFound for established namespace-to-
		// cluster fallback. Retain only that safe classification, never the
		// upstream object name, details or arbitrary API message.
		if apierrors.IsNotFound(err) {
			return &apierrors.StatusError{ErrStatus: metav1.Status{Status: metav1.StatusFailure, Reason: metav1.StatusReasonNotFound, Code: 404, Message: "required runtime source not found"}}
		}
		return SafeAPIError(err)
	}
	if value == nil || value.GetName() != key.Name || value.GetNamespace() != key.Namespace {
		return ErrRuntime
	}
	if err = validateHeldRuntimeGVK(value); err != nil {
		return err
	}
	if err = validateHeldRuntimeIdentity(value); err != nil {
		return err
	}
	return c.binding.admit(value)
}

func (c *heldRuntimeClient) List(ctx context.Context, value ctrlclient.ObjectList, opts ...ctrlclient.ListOption) error {
	call, cancel := context.WithTimeout(ctx, heldRequestTimeout(c.timeout))
	defer cancel()
	err := c.Client.List(call, value, opts...)
	if call.Err() != nil {
		return call.Err()
	}
	if err != nil {
		return SafeAPIError(err)
	}
	if value == nil || !boundedPrivatePayload(value) {
		return ErrBounds
	}
	if err = validateHeldRuntimeGVK(value); err != nil {
		return err
	}
	items, err := meta.ExtractList(value)
	if err != nil || len(items) > 16 {
		return ErrBounds
	}
	requestedNS := (&ctrlclient.ListOptions{}).ApplyOptions(opts).Namespace
	for _, item := range items {
		if err = validateHeldRuntimeGVK(item); err != nil {
			return err
		}
		if err = validateHeldRuntimeIdentity(item); err != nil {
			return err
		}
		metadata, _ := meta.Accessor(item)
		if metadata.GetNamespace() != requestedNS {
			return ErrRuntime
		}
		if err = c.binding.admit(item); err != nil {
			return err
		}
	}
	return nil
}

func validateHeldRuntimeIdentity(value runtime.Object) error {
	metadata, err := meta.Accessor(value)
	if err != nil || !SafeScalar(metadata.GetName()) || len(validation.IsDNS1123Subdomain(metadata.GetName())) != 0 || !SafeScalar(string(metadata.GetUID())) || !SafeScalar(metadata.GetResourceVersion()) || metadata.GetDeletionTimestamp() != nil {
		return ErrRuntime
	}
	clusterScoped := false
	switch value.(type) {
	case *v1beta1.ClusterServingRuntime, *v1beta1.ClusterBaseModel:
		clusterScoped = true
	}
	if clusterScoped {
		if metadata.GetNamespace() != "" {
			return ErrRuntime
		}
	} else if !SafeScalar(metadata.GetNamespace()) || len(validation.IsDNS1123Label(metadata.GetNamespace())) != 0 {
		return ErrRuntime
	}
	// ControllerRevision metadata generation is ordinarily unset. Its writer
	// ordinal and source consistency are checked by the existing pin resolver.
	if _, revision := value.(*appsv1.ControllerRevision); !revision && metadata.GetGeneration() <= 0 {
		return ErrRuntime
	}
	return nil
}

func validateHeldRuntimeGVK(value runtime.Object) error {
	var expected string
	groupVersion := "ome.io/v1beta1"
	switch value.(type) {
	case *v1beta1.ServingRuntime:
		expected = "ServingRuntime"
	case *v1beta1.ServingRuntimeList:
		expected = "ServingRuntimeList"
	case *v1beta1.ClusterServingRuntime:
		expected = "ClusterServingRuntime"
	case *v1beta1.ClusterServingRuntimeList:
		expected = "ClusterServingRuntimeList"
	case *v1beta1.BaseModel:
		expected = "BaseModel"
	case *v1beta1.BaseModelList:
		expected = "BaseModelList"
	case *v1beta1.ClusterBaseModel:
		expected = "ClusterBaseModel"
	case *v1beta1.ClusterBaseModelList:
		expected = "ClusterBaseModelList"
	case *appsv1.ControllerRevision:
		expected = "ControllerRevision"
		groupVersion = "apps/v1"
	case *appsv1.ControllerRevisionList:
		expected = "ControllerRevisionList"
		groupVersion = "apps/v1"
	default:
		return ErrRuntime
	}
	gvk := value.GetObjectKind().GroupVersionKind()
	if gvk.Kind != "" && gvk.Kind != expected || gvk.GroupVersion().String() != "" && gvk.GroupVersion().String() != groupVersion {
		return ErrRuntime
	}
	return nil
}

type heldAppsClient struct {
	appsclient.AppsV1Interface
	binding *HeldRuntimeBinding
	timeout time.Duration
}

func (c heldAppsClient) ControllerRevisions(ns string) appsclient.ControllerRevisionInterface {
	return heldRevisionClient{ControllerRevisionInterface: c.AppsV1Interface.ControllerRevisions(ns), binding: c.binding, timeout: c.timeout}
}

type heldRevisionClient struct {
	appsclient.ControllerRevisionInterface
	binding *HeldRuntimeBinding
	timeout time.Duration
}

func (c heldRevisionClient) Get(ctx context.Context, name string, opts metav1.GetOptions) (*appsv1.ControllerRevision, error) {
	call, cancel := context.WithTimeout(ctx, heldRequestTimeout(c.timeout))
	defer cancel()
	value, err := c.ControllerRevisionInterface.Get(call, name, opts)
	if call.Err() != nil {
		return nil, call.Err()
	}
	if err != nil {
		return nil, SafeAPIError(err)
	}
	if value == nil || value.Name != name {
		return nil, ErrRuntime
	}
	if err = validateHeldRuntimeGVK(value); err != nil {
		return nil, err
	}
	if err = validateHeldRuntimeIdentity(value); err != nil {
		return nil, err
	}
	if err = c.binding.admit(value); err != nil {
		return nil, err
	}
	return value, nil
}
func (c heldRevisionClient) List(ctx context.Context, opts metav1.ListOptions) (*appsv1.ControllerRevisionList, error) {
	call, cancel := context.WithTimeout(ctx, heldRequestTimeout(c.timeout))
	defer cancel()
	value, err := c.ControllerRevisionInterface.List(call, opts)
	if call.Err() != nil {
		return nil, call.Err()
	}
	if err != nil {
		return nil, SafeAPIError(err)
	}
	if value == nil || len(value.Items) > 16 || !boundedPrivatePayload(value) {
		return nil, ErrBounds
	}
	if err = validateHeldRuntimeGVK(value); err != nil {
		return nil, err
	}
	for i := range value.Items {
		if err = validateHeldRuntimeGVK(&value.Items[i]); err != nil {
			return nil, err
		}
		if err = validateHeldRuntimeIdentity(&value.Items[i]); err != nil {
			return nil, err
		}
		if err = c.binding.admit(&value.Items[i]); err != nil {
			return nil, err
		}
	}
	return value, nil
}

// NewHeldReleaseRuntimeResolver places whole typed admission before existing
// effective resolver copies, raw revision parsing and runtime fingerprinting.
func NewHeldReleaseRuntimeResolver(apps appsclient.AppsV1Interface, client ctrlclient.Client, omeNamespace string, requestTimeout ...time.Duration) (*effective.RuntimePinResolver, *HeldRuntimeBinding, error) {
	if apps == nil || client == nil {
		return nil, nil, errors.New("held-release runtime clients are unavailable")
	}
	binding := &HeldRuntimeBinding{objects: map[string]runtime.Object{}}
	timeout := heldRequestTimeout(requestTimeout...)
	limits := paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: timeout}
	live, err := effective.NewBoundedRuntimeResolver(&heldRuntimeClient{Client: client, binding: binding, timeout: timeout}, limits)
	if err != nil {
		return nil, nil, ErrRuntime
	}
	resolver, err := effective.NewRuntimePinResolver(heldAppsClient{AppsV1Interface: apps, binding: binding, timeout: timeout}, live, omeNamespace, limits)
	if err != nil {
		return nil, nil, ErrRuntime
	}
	return resolver, binding, nil
}

func heldRequestTimeout(values ...time.Duration) time.Duration {
	timeout := 10 * time.Second
	for _, value := range values {
		if value > 0 && value < timeout {
			timeout = value
		}
	}
	return timeout
}
