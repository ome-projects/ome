// Package managercache holds the shape of the controller manager's informer
// cache: which pods it holds and what it drops from every object before it
// stores it. cmd/manager and the integration suites build their managers from
// this one shape, so a controller that reads what the cache does not hold
// fails in the suites as it fails in production.
package managercache

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// Options is the manager's cache configuration. Pods are the one cluster-scale
// type, and an unscoped informer holds every pod in the cluster before any
// reconcile runs, so the pod cache holds only pods carrying the
// ome.io/inferenceservice label key; every pod OME creates carries it, so the
// scope hides nothing the controllers read. A cached pod keeps only the parts
// the controllers read (query.PodCacheTransform); a reader of another part
// reads the pod live. managedFields are dropped from every cached object; OME
// never reads them. Field indexes are not part of the shape: each controller
// registers its own when it is set up.
func Options() (cache.Options, error) {
	omePods, err := podSelector()
	if err != nil {
		return cache.Options{}, err
	}
	return cache.Options{
		DefaultTransform: cache.TransformStripManagedFields(),
		ByObject: map[client.Object]cache.ByObject{
			&corev1.Pod{}: {Label: omePods, Transform: query.PodCacheTransform()},
		},
	}, nil
}

// podSelector selects the pods the cache holds: those carrying the
// ome.io/inferenceservice label key.
func podSelector() (labels.Selector, error) {
	omePodReq, err := labels.NewRequirement(constants.InferenceServicePodLabelKey, selection.Exists, nil)
	if err != nil {
		return nil, err
	}
	return labels.NewSelector().Add(*omePodReq), nil
}
