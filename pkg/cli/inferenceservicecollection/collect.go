// Package inferenceservicecollection reads the bounded InferenceService
// snapshot used to attribute services to serving runtimes.
package inferenceservicecollection

import (
	"context"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

// Completeness describes the bounded observations in an InferenceService
// collection.
type Completeness struct {
	ObservedPages int
	ObservedItems int
	Truncated     bool
}

// Result contains a defensive snapshot and its collection completeness.
type Result struct {
	InferenceServices []omev1beta1.InferenceService
	Completeness      Completeness
}

// Collect reads InferenceServices across all namespaces through bounded
// Kubernetes pagination.
func Collect(
	ctx context.Context,
	client omeclient.OmeV1beta1Interface,
	limits paging.Limits,
) (Result, error) {
	return collect(ctx, client, metav1.NamespaceAll, limits)
}

// CollectInNamespace reads InferenceServices only in namespace through
// bounded Kubernetes pagination.
func CollectInNamespace(
	ctx context.Context,
	client omeclient.OmeV1beta1Interface,
	namespace string,
	limits paging.Limits,
) (Result, error) {
	return collect(ctx, client, namespace, limits)
}

func collect(
	ctx context.Context,
	client omeclient.OmeV1beta1Interface,
	namespace string,
	limits paging.Limits,
) (Result, error) {
	result := Result{InferenceServices: []omev1beta1.InferenceService{}}
	observedPages := 0
	services, err := paging.ListBounded(
		ctx,
		metav1.ListOptions{},
		limits,
		func(
			requestCtx context.Context,
			options metav1.ListOptions,
		) (paging.Page[omev1beta1.InferenceService], error) {
			list, listErr := client.InferenceServices(namespace).List(requestCtx, options)
			if requestErr := requestCtx.Err(); requestErr != nil {
				return paging.Page[omev1beta1.InferenceService]{}, requestErr
			}
			if listErr != nil {
				return paging.Page[omev1beta1.InferenceService]{}, listErr
			}
			if list == nil {
				return paging.Page[omev1beta1.InferenceService]{}, errors.New("empty response")
			}
			observedPages++
			return paging.Page[omev1beta1.InferenceService]{
				Items: list.Items, Continue: list.Continue,
			}, nil
		},
	)
	result.InferenceServices = copyInferenceServices(services.Items)
	result.Completeness = Completeness{
		ObservedPages: observedPages,
		ObservedItems: len(services.Items),
		Truncated:     services.Truncated,
	}
	if err != nil {
		return result, fmt.Errorf("list InferenceServices: %w", err)
	}
	return result, nil
}

func copyInferenceServices(items []omev1beta1.InferenceService) []omev1beta1.InferenceService {
	result := make([]omev1beta1.InferenceService, len(items))
	for i := range items {
		result[i] = *items[i].DeepCopy()
	}
	return result
}
