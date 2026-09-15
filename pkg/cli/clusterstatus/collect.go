// Package clusterstatus collects and projects observational WorkloadCluster
// evidence exclusively from the current Kubernetes context.
package clusterstatus

import (
	"context"
	"errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

// Snapshot owns its objects and retains the bounded read's source windows.
type Snapshot struct {
	Items       []ome.WorkloadCluster
	Pages       int
	Returned    int
	Truncated   bool
	Unavailable r.UnavailableReason
	Limits      paging.Limits
}

// Collect issues only a named cluster-scoped GET or bounded LIST. Optional
// source errors are classified without retaining arbitrary server error text.
// Cancellation aborts rather than masquerading as an ordinary empty report.
func Collect(ctx context.Context, client omeclient.OmeV1beta1Interface, name string, limits paging.Limits) (Snapshot, error) {
	s := Snapshot{Items: []ome.WorkloadCluster{}, Limits: limits}
	if err := ctx.Err(); err != nil {
		return s, err
	}
	if client == nil {
		return s, errors.New("OME client is required")
	}
	if name != "" && len(validation.IsDNS1123Subdomain(name)) != 0 {
		return s, errors.New("invalid WorkloadCluster name")
	}
	if limits.PageSize <= 0 || limits.MaxItems <= 0 || limits.MaxPages <= 0 || limits.RequestTimeout <= 0 {
		return s, errors.New("invalid collection limits")
	}
	if name != "" {
		requestCtx, cancel := context.WithTimeout(ctx, limits.RequestTimeout)
		w, err := client.WorkloadClusters().Get(requestCtx, name, metav1.GetOptions{})
		requestErr := requestCtx.Err()
		cancel()
		s.Pages = 1
		if ctx.Err() != nil {
			return s, ctx.Err()
		}
		if requestErr != nil {
			s.Unavailable = r.UnavailableUnreadable
			return s, nil
		}
		if err != nil {
			s.Unavailable = classify(err)
			return s, nil
		}
		if w == nil {
			s.Unavailable = r.UnavailableMalformedPayload
			return s, nil
		}
		s.Returned = 1
		if w.Name != name || w.Namespace != "" {
			s.Unavailable = r.UnavailableMalformedPayload
			return s, nil
		}
		s.Items = append(s.Items, *w.DeepCopy())
		return s, nil
	}
	listed, err := paging.ListBounded(ctx, metav1.ListOptions{}, limits, func(requestCtx context.Context, options metav1.ListOptions) (paging.Page[ome.WorkloadCluster], error) {
		list, listErr := client.WorkloadClusters().List(requestCtx, options)
		if requestCtx.Err() != nil {
			return paging.Page[ome.WorkloadCluster]{}, requestCtx.Err()
		}
		if listErr != nil {
			return paging.Page[ome.WorkloadCluster]{}, listErr
		}
		if list == nil {
			return paging.Page[ome.WorkloadCluster]{}, errMalformedResponse
		}
		s.Returned += len(list.Items)
		return paging.Page[ome.WorkloadCluster]{Items: list.Items, Continue: list.Continue}, nil
	})
	if ctx.Err() != nil {
		return s, ctx.Err()
	}
	s.Pages, s.Truncated = listed.Pages, listed.Truncated
	if err != nil {
		s.Unavailable = classify(err)
		s.Truncated = len(listed.Items) > 0
	}
	for i := range listed.Items {
		s.Items = append(s.Items, *listed.Items[i].DeepCopy())
	}
	return s, nil
}

var errMalformedResponse = errors.New("malformed WorkloadCluster response")

func classify(err error) r.UnavailableReason {
	switch {
	case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
		return r.UnavailableForbidden
	case apierrors.IsNotFound(err):
		return r.UnavailableNotFound
	case errors.Is(err, errMalformedResponse):
		return r.UnavailableMalformedPayload
	default:
		return r.UnavailableUnreadable
	}
}
