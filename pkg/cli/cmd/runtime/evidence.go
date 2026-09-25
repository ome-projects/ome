package runtime

import (
	"context"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/apierror"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/paging"
)

var (
	errNilInferenceService               = errors.New("InferenceService GET returned no object")
	errInferenceServiceNameMismatch      = errors.New("InferenceService GET returned a different name")
	errInferenceServiceNamespaceMismatch = errors.New("InferenceService GET returned a different namespace")
	errInferenceServiceUIDEmpty          = errors.New("InferenceService GET returned an empty UID")
	errInferenceServiceVersionEmpty      = errors.New("InferenceService GET returned an empty resourceVersion")
)

type runtimeEvidenceOptions struct {
	IncludeHistory bool
}

type runtimeEvidence struct {
	inferenceService *v1beta1.InferenceService
	state            *effective.RuntimeState
}

func collectRuntimeEvidence(
	ctx context.Context,
	f factory.Factory,
	namespaceOptions *namespace.Options,
	name string,
	limits paging.Limits,
	options runtimeEvidenceOptions,
) (*runtimeEvidence, error) {
	if err := limits.Validate(); err != nil {
		return nil, fmt.Errorf(
			"construct runtime evidence resolver: %w",
			errors.New("revision paging limits are invalid"),
		)
	}
	workloadNamespace, _, err := f.Namespace()
	if err != nil {
		return nil, fmt.Errorf("resolve workload namespace: %w", err)
	}
	resolved, err := namespaceOptions.Resolve(workloadNamespace)
	if err != nil {
		return nil, err
	}

	omeClient, err := f.OMEClient()
	if err != nil {
		return nil, fmt.Errorf("construct OME client: %w", err)
	}
	requestContext, cancel := context.WithTimeout(ctx, limits.RequestTimeout)
	isvc, err := omeClient.OmeV1beta1().InferenceServices(resolved.WorkloadNamespace).
		Get(requestContext, name, metav1.GetOptions{})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("get InferenceService: %w", apierror.Friendly(err))
	}
	if err := bindInferenceService(isvc, resolved.WorkloadNamespace, name); err != nil {
		return nil, err
	}

	kubeClient, err := f.KubeClient()
	if err != nil {
		return nil, fmt.Errorf("construct Kubernetes client: %w", err)
	}
	runtimeClient, err := f.RuntimeClient()
	if err != nil {
		return nil, fmt.Errorf("construct runtime client: %w", err)
	}
	liveResolver := effective.NewRuntimeResolver(runtimeClient)
	pinResolver, err := effective.NewRuntimePinResolver(
		kubeClient.AppsV1(), liveResolver, resolved.OMENamespace, limits,
	)
	if err != nil {
		return nil, fmt.Errorf("construct runtime evidence resolver: %w", err)
	}
	state, err := pinResolver.Resolve(ctx, isvc, effective.RuntimeResolveOptions{
		IncludeHistory: options.IncludeHistory,
	})
	if err != nil {
		return nil, fmt.Errorf("collect runtime evidence: %w", err)
	}
	return &runtimeEvidence{inferenceService: isvc, state: state}, nil
}

func bindInferenceService(isvc *v1beta1.InferenceService, wantNamespace, name string) error {
	switch {
	case isvc == nil:
		return errNilInferenceService
	case isvc.Name != name:
		return errInferenceServiceNameMismatch
	case isvc.Namespace != wantNamespace:
		return errInferenceServiceNamespaceMismatch
	case isvc.UID == "":
		return errInferenceServiceUIDEmpty
	case isvc.ResourceVersion == "":
		return errInferenceServiceVersionEmpty
	default:
		return nil
	}
}
