package effective

import (
	"context"
	"errors"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	appstyped "k8s.io/client-go/kubernetes/typed/apps/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/actionbounds"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimerevision"
)

// RuntimeSyncResolver uses uncached action reads; each Resolve starts new snapshots.
type RuntimeSyncResolver struct {
	revisions appstyped.ControllerRevisionsGetter
	client    ctrlclient.Client
	namespace string
	limits    paging.Limits
}

func NewRuntimeSyncResolver(revisions appstyped.ControllerRevisionsGetter, client ctrlclient.Client, namespace string, limits paging.Limits) (*RuntimeSyncResolver, error) {
	if revisions == nil || client == nil || namespace == "" || limits.PageSize <= 0 || limits.PageSize > 16 || limits.MaxItems <= 0 || limits.MaxItems > 32 || limits.MaxPages <= 0 || limits.MaxPages > 2 || limits.RequestTimeout <= 0 || limits.RequestTimeout > 10*time.Second {
		return nil, ErrRuntimeSyncEvidence
	}
	return &RuntimeSyncResolver{revisions, client, namespace, limits}, nil
}
func (r *RuntimeSyncResolver) Resolve(ctx context.Context, v *v1beta1.InferenceService) (RuntimeSyncEvidence, error) {
	if r == nil || ctx == nil || v == nil || !actionbounds.PrivatePayload(v) {
		return RuntimeSyncEvidence{}, ErrRuntimeSyncEvidence
	}
	// The real revision writer uses the source name as a Kubernetes label value.
	// A DNS-valid but label-invalid name cannot produce auditable writer history.
	if v.Spec.Runtime == nil || v.Spec.Runtime.Name == "" || len(validation.IsValidLabelValue(v.Spec.Runtime.Name)) != 0 {
		return RuntimeSyncEvidence{}, ErrRuntimeSyncEvidence
	}
	if err := ctx.Err(); err != nil {
		return RuntimeSyncEvidence{}, err
	}
	reads := map[string]string{}
	client := &syncReadClient{Client: r.client, reads: reads, timeout: r.limits.RequestTimeout}
	getNamespace := func(ns string) revisionNamespace {
		return &syncRevisionClient{revisionNamespace: r.revisions.ControllerRevisions(ns), namespace: ns, reads: reads, timeout: r.limits.RequestTimeout}
	}
	pin, err := newRuntimePinResolver(getNamespace, NewRuntimeResolver(client), r.namespace, r.limits)
	if err != nil {
		return RuntimeSyncEvidence{}, ErrRuntimeSyncEvidence
	}
	state, err := pin.Resolve(ctx, v, RuntimeResolveOptions{IncludeHistory: true})
	if err != nil {
		return RuntimeSyncEvidence{}, syncContextError(err)
	}
	if state.live == nil || state.live.Runtime.spec == nil {
		return RuntimeSyncEvidence{}, ErrRuntimeSyncEvidence
	}
	_, short, err := runtimerevision.Hash(state.live.Runtime.spec)
	if err != nil {
		return RuntimeSyncEvidence{}, ErrRuntimeSyncEvidence
	}
	name := runtimerevision.Name(runtimerevision.SourceKind(state.DeclaredSourceKind), state.DeclaredSourceNamespace, state.RuntimeName, short)
	object, err := getNamespace(r.namespace).Get(ctx, name, metav1.GetOptions{})
	absent := apierrors.IsNotFound(err)
	if err != nil && !absent {
		return RuntimeSyncEvidence{}, syncContextError(err)
	}
	target := RuntimeRevisionObservation{}
	if !absent {
		target = inspectRuntimeRevision(object, r.namespace, name, state.RuntimeName, runtimerevision.SourceKind(state.DeclaredSourceKind), state.DeclaredSourceNamespace)
	}
	if err := ctx.Err(); err != nil {
		return RuntimeSyncEvidence{}, err
	}
	return prepareRuntimeSyncEvidence(v, state, target, absent, reads)
}
func syncContextError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return ErrRuntimeSyncEvidence
}
func recordSyncRead(reads map[string]string, key string, value any) error {
	digest := syncDigest(value)
	if digest == "" {
		return ErrRuntimeSyncEvidence
	}
	if old, ok := reads[key]; ok && old != digest {
		return ErrRuntimeSyncEvidence
	}
	reads[key] = digest
	return nil
}

type syncReadClient struct {
	ctrlclient.Client
	reads   map[string]string
	timeout time.Duration
}

func (c *syncReadClient) Get(ctx context.Context, key ctrlclient.ObjectKey, object ctrlclient.Object, opts ...ctrlclient.GetOption) error {
	request, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	// Preserve typed NotFound internally: selector/model/inheritance fallback needs it.
	if err := c.Client.Get(request, key, object, opts...); err != nil {
		return err
	}
	if err := request.Err(); err != nil {
		return err
	}
	if !actionbounds.PrivatePayload(object) || object.GetName() != key.Name || object.GetNamespace() != key.Namespace || object.GetUID() == "" || object.GetResourceVersion() == "" || object.GetGeneration() <= 0 || object.GetDeletionTimestamp() != nil {
		return ErrRuntimeSyncEvidence
	}
	kind := ""
	switch object.(type) {
	case *v1beta1.ServingRuntime:
		kind = "ServingRuntime"
	case *v1beta1.ClusterServingRuntime:
		kind = "ClusterServingRuntime"
	case *v1beta1.BaseModel:
		kind = "BaseModel"
	case *v1beta1.ClusterBaseModel:
		kind = "ClusterBaseModel"
	default:
		return ErrRuntimeSyncEvidence
	}
	gvk := object.GetObjectKind().GroupVersionKind()
	if gvk.Kind != "" && gvk.Kind != kind || gvk.Group != "" && gvk.Group != "ome.io" || gvk.Version != "" && gvk.Version != "v1beta1" {
		return ErrRuntimeSyncEvidence
	}
	switch typed := object.(type) {
	case *v1beta1.ServingRuntime:
		if typed.Spec.IsDisabled() {
			return ErrRuntimeSyncEvidence
		}
	case *v1beta1.ClusterServingRuntime:
		if typed.Spec.IsDisabled() {
			return ErrRuntimeSyncEvidence
		}
	}
	return recordSyncRead(c.reads, kind+"/"+key.Namespace+"/"+key.Name, object)
}
func (c *syncReadClient) List(context.Context, ctrlclient.ObjectList, ...ctrlclient.ListOption) error {
	return ErrRuntimeSyncEvidence
} // named intent never needs automatic-selection LIST.

type syncRevisionClient struct {
	revisionNamespace
	namespace string
	reads     map[string]string
	timeout   time.Duration
}

func (c *syncRevisionClient) Get(ctx context.Context, name string, opts metav1.GetOptions) (*appsv1.ControllerRevision, error) {
	request, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	object, err := c.revisionNamespace.Get(request, name, opts)
	if apierrors.IsNotFound(err) {
		if e := recordSyncRead(c.reads, "revision/"+c.namespace+"/"+name, "NotFound"); e != nil {
			return nil, e
		}
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	if err = request.Err(); err != nil {
		return nil, err
	}
	if !validSyncRevision(object, c.namespace, name) {
		return nil, ErrRuntimeSyncEvidence
	}
	if err = recordSyncRead(c.reads, "revision/"+c.namespace+"/"+name, object); err != nil {
		return nil, err
	}
	return object, nil
}
func (c *syncRevisionClient) List(ctx context.Context, opts metav1.ListOptions) (*appsv1.ControllerRevisionList, error) {
	request, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	list, err := c.revisionNamespace.List(request, opts)
	if err != nil {
		return nil, err
	}
	if err = request.Err(); err != nil {
		return nil, err
	}
	if list == nil || len(list.Items) > 16 {
		return nil, ErrRuntimeSyncEvidence
	}
	// Cap each complete object before aggregate retention, defensive copy or DecodeSpec.
	for i := range list.Items {
		object := &list.Items[i]
		if !validSyncRevision(object, c.namespace, object.Name) {
			return nil, ErrRuntimeSyncEvidence
		}
		if err = recordSyncRead(c.reads, "revision/"+c.namespace+"/"+object.Name, object); err != nil {
			return nil, err
		}
	}
	if err = recordSyncRead(c.reads, "history/"+opts.Continue, struct {
		Items    []appsv1.ControllerRevision
		Continue string
	}{list.Items, list.Continue}); err != nil {
		return nil, err
	}
	return list, nil
}
func validSyncRevision(object *appsv1.ControllerRevision, namespace, name string) bool {
	return object != nil && actionbounds.PrivatePayload(object) && actionbounds.JSONPayload(object.Data.Raw) && object.Data.Object == nil && object.Name == name && object.Namespace == namespace && object.UID != "" && object.ResourceVersion != "" && object.DeletionTimestamp == nil && (object.Kind == "" || object.Kind == "ControllerRevision") && (object.APIVersion == "" || object.APIVersion == "apps/v1") && len(object.Labels) <= 256 && len(object.Annotations) <= 256 && object.Labels[constants.RuntimeRevisionOfLabelKey] != ""
}
