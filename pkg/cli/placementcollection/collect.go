// Package placementcollection acquires bounded current-context observations.
// It never resolves workload-cluster credentials or contacts serving endpoints.
package placementcollection

import (
	"context"
	"errors"
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	validation "k8s.io/apimachinery/pkg/util/validation"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned/scheme"
	client "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

type View string

const (
	Status   View = "status"
	Explain  View = "explain"
	Endpoint View = "endpoint"
)

type Result struct {
	InferenceService      *ome.InferenceService
	WorkloadClusters      []ome.WorkloadCluster
	Fleet                 v.PlacementAcquisition
	TrafficMap            *ome.TrafficMap
	TrafficMapAcquisition v.PlacementAcquisition
}

type limits struct{ collection, request time.Duration }

func Collect(ctx context.Context, c client.OmeV1beta1Interface, namespace, name string, view View) (Result, error) {
	return collect(ctx, c, namespace, name, view, limits{30 * time.Second, 10 * time.Second})
}

func collect(ctx context.Context, c client.OmeV1beta1Interface, namespace, name string, view View, budget limits) (Result, error) {
	result := Result{WorkloadClusters: []ome.WorkloadCluster{}}
	if c == nil || len(validation.IsDNS1123Label(namespace)) != 0 || len(validation.IsDNS1123Subdomain(name)) != 0 || (view != Status && view != Explain && view != Endpoint) {
		return result, errors.New("invalid placement collection request")
	}
	if c.RESTClient() == nil {
		return result, errors.New("placement OME REST client unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, budget.collection)
	defer cancel()
	requestCtx, requestCancel := context.WithTimeout(ctx, budget.request)
	isvc := &ome.InferenceService{}
	err := c.RESTClient().Get().Namespace(namespace).Resource("inferenceservices").Name(name).MaxRetries(0).Do(requestCtx).Into(isvc)
	if requestCtx.Err() != nil {
		err = requestCtx.Err()
	}
	requestCancel()
	if err != nil {
		return result, errors.New("InferenceService read unavailable: " + string(unavailable(err, true)))
	}
	if isvc == nil || isvc.Name != name || isvc.Namespace != namespace || isvc.UID == "" || isvc.Generation < 0 {
		return result, errors.New("InferenceService identity or generation invalid")
	}
	result.InferenceService = isvc.DeepCopy()
	if view == Status {
		return result, nil
	}
	if view == Endpoint {
		requestCtx, requestCancel = context.WithTimeout(ctx, budget.request)
		tm := &ome.TrafficMap{}
		readErr := c.RESTClient().Get().Namespace(namespace).Resource("trafficmaps").Name(name).MaxRetries(0).Do(requestCtx).Into(tm)
		if requestCtx.Err() != nil {
			readErr = requestCtx.Err()
		}
		requestCancel()
		if readErr != nil {
			result.TrafficMapAcquisition = v.PlacementAcquisition{State: "Unavailable", Reason: unavailable(readErr, true)}
		} else {
			result.TrafficMap = tm.DeepCopy()
			result.TrafficMapAcquisition = v.PlacementAcquisition{State: "Observed", Returned: 1, Admitted: 1, Pages: 1, Complete: true}
		}
		return result, nil
	}
	result.Fleet.State = "Observed"
	continuation := ""
	for page := 0; page < 2; page++ {
		requestCtx, requestCancel = context.WithTimeout(ctx, budget.request)
		list := &ome.WorkloadClusterList{}
		options := metav1.ListOptions{Limit: 32, Continue: continuation}
		readErr := c.RESTClient().Get().Resource("workloadclusters").VersionedParams(&options, scheme.ParameterCodec).MaxRetries(0).Do(requestCtx).Into(list)
		if requestCtx.Err() != nil {
			readErr = requestCtx.Err()
		}
		requestCancel()
		if readErr != nil || list == nil {
			result.Fleet.State = "Unavailable"
			result.Fleet.Reason = "MalformedPayload"
			if readErr != nil {
				result.Fleet.Reason = unavailable(readErr, false)
			}
			if result.Fleet.Pages > 0 {
				result.Fleet.State = "Partial"
			}
			return result, nil
		}
		result.Fleet.Pages++
		result.Fleet.Returned += len(list.Items)
		keep := len(list.Items)
		if keep > 32 {
			keep = 32
		}
		for i := 0; i < keep; i++ {
			result.WorkloadClusters = append(result.WorkloadClusters, *list.Items[i].DeepCopy())
		}
		result.Fleet.Admitted = len(result.WorkloadClusters)
		if len(list.Items) > 32 {
			result.Fleet.State = "Partial"
			result.Fleet.Reason = "ItemBudgetExceeded"
			result.Fleet.Truncated = true
			return result, nil
		}
		if list.Continue == "" {
			result.Fleet.Complete = true
			return result, nil
		}
		if len(list.Continue) > 4096 || list.Continue == continuation {
			result.Fleet.State = "Partial"
			result.Fleet.Reason = "MalformedContinuation"
			return result, nil
		}
		continuation = list.Continue
	}
	result.Fleet.State = "Partial"
	result.Fleet.Reason = "PageBudgetExceeded"
	result.Fleet.Truncated = true
	return result, nil
}

func unavailable(err error, named bool) v.PlacementValue {
	switch {
	case errors.Is(err, context.Canceled):
		return "Cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "Timeout"
	case kerrors.IsForbidden(err):
		return "Forbidden"
	case kerrors.IsNotFound(err):
		if named {
			return "NotFound"
		}
		var status kerrors.APIStatus
		if errors.As(err, &status) && (status.Status().Details == nil || status.Status().Details.Name == "") {
			return "UnsupportedAPI"
		}
		return "NotFound"
	case kerrors.IsMethodNotSupported(err):
		return "UnsupportedAPI"
	default:
		return "Unreadable"
	}
}
