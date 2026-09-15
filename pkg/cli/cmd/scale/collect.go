package scale

import (
	"context"
	"errors"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/mutate"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/paging"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/transport"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

type collected struct {
	parent       *v1beta1.InferenceService
	state        *effective.RuntimeState
	source       effective.ManualScaleSource
	scale        mutate.ScaleEvidence
	transport    *transport.Client
	reader       ctrlclient.Reader
	context      string
	omeNamespace string
	ome          omeclient.OmeV1beta1Interface
}

func collect(ctx context.Context, f factory.Factory, omeOptions *namespace.Options, name string, component v1beta1.ComponentType, clock reportv1alpha1.Clock) (*collected, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ns, _, err := f.Namespace()
	if err != nil {
		return nil, mutate.SafeAPIError(err)
	}
	resolved, err := omeOptions.Resolve(ns)
	if err != nil || ns == "" || len(validation.IsDNS1123Label(ns)) != 0 {
		return nil, errors.New("scale namespace is unavailable or invalid")
	}
	resolver, ok := f.(factory.ContextResolver)
	if !ok {
		return nil, errors.New("scale context is unavailable")
	}
	contextName, err := resolver.ContextName()
	if err != nil || !mutate.SafeScalar(contextName) {
		return nil, errors.New("scale context is unavailable or unsafe")
	}
	var ome versioned.Interface
	if owned, ok := f.(factory.ActionReadClientsResolver); ok {
		ome, err = owned.OMEClientForAction(ctx)
	} else {
		ome, err = f.OMEClient()
	}
	if err != nil || ome == nil {
		return nil, mutate.SafeAPIError(errOrMissing(err))
	}
	requestContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	parent, err := ome.OmeV1beta1().InferenceServices(ns).Get(requestContext, name, metav1.GetOptions{})
	cancel()
	if err != nil {
		return nil, mutate.SafeAPIError(err)
	}
	if parent == nil || parent.Name != name || parent.Namespace != ns {
		return nil, mutate.ErrUnsafeTarget
	}
	if err := mutate.ValidateTarget(parent); err != nil {
		return nil, err
	}
	var kube kubernetes.Interface
	if owned, ok := f.(factory.ActionReadClientsResolver); ok {
		kube, err = owned.KubeClientForAction(ctx)
	} else {
		kube, err = f.KubeClient()
	}
	if err != nil || kube == nil {
		return nil, mutate.SafeAPIError(errOrMissing(err))
	}
	var reader ctrlclient.Client
	if owned, ok := f.(factory.ActionRuntimeResolver); ok {
		reader, err = owned.RuntimeClientForAction(ctx)
	} else {
		reader, err = f.RuntimeClient()
	}
	if err != nil || reader == nil {
		return nil, mutate.SafeAPIError(errOrMissing(err))
	}
	limits := paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: 10 * time.Second}
	live, err := effective.NewBoundedRuntimeResolver(reader, limits)
	if err != nil {
		return nil, effective.ErrManualScaleSource
	}
	pins, err := effective.NewRuntimePinResolver(kube.AppsV1(), live, resolved.OMENamespace, limits)
	if err != nil {
		return nil, effective.ErrManualScaleSource
	}
	state, err := pins.Resolve(ctx, parent, effective.RuntimeResolveOptions{})
	if err != nil {
		return nil, mutate.SafeAPIError(err)
	}
	if _, err := mutate.RequireNativeRuntime(parent, state); err != nil {
		return nil, err
	}
	source, err := effective.ResolveManualScaleSource(parent, state, component)
	if err != nil {
		return nil, err
	}
	ref := parent.Status.Components[component].ScaleTargetRef
	requestContext, cancel = context.WithTimeout(ctx, 10*time.Second)
	replica, err := ome.OmeV1beta1().InferenceReplicas(ns).Get(requestContext, ref.Name, metav1.GetOptions{})
	cancel()
	if err != nil {
		return nil, mutate.SafeAPIError(err)
	}
	if replica == nil || replica.Name != ref.Name || replica.Namespace != ns {
		return nil, mutate.ErrScaleEvidence
	}
	config, err := f.RESTConfig()
	if err != nil {
		return nil, mutate.SafeAPIError(err)
	}
	config, err = actionConfig(config)
	if err != nil {
		return nil, err
	}
	client, err := transport.NewBounded(config, 1<<20)
	if err != nil {
		return nil, mutate.SafeAPIError(err)
	}
	requestContext, cancel = context.WithTimeout(ctx, 10*time.Second)
	scale, err := client.GetInferenceReplicaScale(requestContext, ns, replica.Name, metav1.GetOptions{})
	cancel()
	if err != nil {
		return nil, mutate.SafeAPIError(err)
	}
	evidence, err := mutate.InspectScaleEvidence(parent, replica, scale, source, clock)
	if err != nil {
		return nil, err
	}
	evidence, err = mutate.CollectScalePinnedTargets(ctx, ome.OmeV1beta1(), evidence, clock)
	if err != nil {
		return nil, err
	}
	return &collected{parent: parent, state: state, source: source, scale: evidence, transport: client, reader: reader, context: contextName, omeNamespace: resolved.OMENamespace, ome: ome.OmeV1beta1()}, nil
}

func errOrMissing(err error) error {
	if err != nil {
		return err
	}
	return errors.New("required action client is unavailable")
}
