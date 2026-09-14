// Package migrationcollection reads the exact parent and bounded
// InferenceReplica snapshot used by migration status.
package migrationcollection

import (
	"context"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/apierror"
	"sigs.k8s.io/ome/pkg/cli/paging"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

var (
	ErrClientRequired                    = errors.New("OME client is required")
	ErrInferenceServiceNameInvalid       = errors.New("inference service name is invalid")
	ErrInferenceServiceNamespaceInvalid  = errors.New("inference service namespace is invalid")
	ErrInferenceServiceRequired          = errors.New("inference service response is required")
	ErrInferenceServiceNameMismatch      = errors.New("returned inference service name does not match request")
	ErrInferenceServiceNamespaceMismatch = errors.New("returned inference service namespace does not match request")
	ErrInferenceServiceUIDMissing        = errors.New("returned inference service has no UID")
	ErrInferenceServiceUIDInvalid        = errors.New("returned inference service UID is invalid")
)

// Completeness describes the bounded InferenceReplica observation.
type Completeness struct {
	ObservedPages int
	ObservedItems int
	Truncated     bool
}

// Result is an owned snapshot of the exact parent and related replicas.
type Result struct {
	InferenceService  *omev1beta1.InferenceService
	InferenceReplicas []omev1beta1.InferenceReplica
	Completeness      Completeness
}

// Collect performs one exact namespaced parent GET followed by bounded
// InferenceReplica pagination. Parent names that fit a Kubernetes label value
// use the relationship label selector; longer names use a bounded namespace
// scan and retain only exact typed parent and controller-owner matches.
func Collect(
	ctx context.Context,
	client omeclient.OmeV1beta1Interface,
	namespace string,
	name string,
	limits paging.Limits,
) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if client == nil {
		return Result{}, ErrClientRequired
	}
	if len(utilvalidation.IsDNS1123Label(namespace)) > 0 {
		return Result{}, ErrInferenceServiceNamespaceInvalid
	}
	if len(utilvalidation.IsDNS1123Subdomain(name)) > 0 {
		return Result{}, ErrInferenceServiceNameInvalid
	}
	if err := validateLimits(limits); err != nil {
		return Result{}, err
	}

	getCtx, cancel := context.WithTimeout(ctx, limits.RequestTimeout)
	isvc, err := client.InferenceServices(namespace).Get(getCtx, name, metav1.GetOptions{})
	requestErr := getCtx.Err()
	cancel()
	if requestErr != nil {
		return Result{}, requestErr
	}
	if err != nil {
		return Result{}, fmt.Errorf("get InferenceService: %w", apierror.Friendly(err))
	}
	if isvc == nil {
		return Result{}, ErrInferenceServiceRequired
	}
	if isvc.Name != name {
		return Result{}, ErrInferenceServiceNameMismatch
	}
	if isvc.Namespace != namespace {
		return Result{}, ErrInferenceServiceNamespaceMismatch
	}
	if isvc.UID == "" {
		return Result{}, ErrInferenceServiceUIDMissing
	}
	if !safeUID(string(isvc.UID)) {
		return Result{}, ErrInferenceServiceUIDInvalid
	}

	listOptions := metav1.ListOptions{}
	useRelationshipSelector := len(utilvalidation.IsValidLabelValue(name)) == 0
	if useRelationshipSelector {
		listOptions.LabelSelector = labels.Set{constants.InferenceServicePodLabelKey: name}.AsSelector().String()
	}
	listed, err := paging.ListBounded(
		ctx,
		listOptions,
		limits,
		func(requestCtx context.Context, options metav1.ListOptions) (paging.Page[omev1beta1.InferenceReplica], error) {
			list, listErr := client.InferenceReplicas(namespace).List(requestCtx, options)
			if requestErr := requestCtx.Err(); requestErr != nil {
				return paging.Page[omev1beta1.InferenceReplica]{}, requestErr
			}
			if listErr != nil {
				return paging.Page[omev1beta1.InferenceReplica]{}, apierror.Friendly(listErr)
			}
			if list == nil {
				return paging.Page[omev1beta1.InferenceReplica]{}, errors.New("empty InferenceReplica list response")
			}
			return paging.Page[omev1beta1.InferenceReplica]{Items: list.Items, Continue: list.Continue}, nil
		},
	)
	if err != nil {
		return Result{}, fmt.Errorf("list related InferenceReplicas: %w", err)
	}
	replicas := listed.Items
	if !useRelationshipSelector {
		replicas = exactlyRelatedInferenceReplicas(listed.Items, isvc)
	}

	result := Result{
		InferenceService:  isvc.DeepCopy(),
		InferenceReplicas: copyInferenceReplicas(replicas),
		Completeness: Completeness{
			ObservedPages: listed.Pages,
			ObservedItems: len(listed.Items),
			Truncated:     listed.Truncated,
		},
	}
	return result, nil
}

func exactlyRelatedInferenceReplicas(
	items []omev1beta1.InferenceReplica,
	parent *omev1beta1.InferenceService,
) []omev1beta1.InferenceReplica {
	result := make([]omev1beta1.InferenceReplica, 0, len(items))
	for i := range items {
		item := &items[i]
		if item.Namespace != parent.Namespace || item.Spec.ParentRef.Name != parent.Name ||
			!hasExactControllerOwner(item.OwnerReferences, parent) {
			continue
		}
		result = append(result, *item)
	}
	return result
}

func hasExactControllerOwner(references []metav1.OwnerReference, parent *omev1beta1.InferenceService) bool {
	controllers := 0
	matched := false
	for _, reference := range references {
		if reference.Controller == nil || !*reference.Controller {
			continue
		}
		controllers++
		matched = reference.APIVersion == omev1beta1.SchemeGroupVersion.String() &&
			reference.Kind == "InferenceService" && reference.Name == parent.Name && reference.UID == parent.UID
	}
	return controllers == 1 && matched
}

func validateLimits(limits paging.Limits) error {
	switch {
	case limits.PageSize <= 0:
		return errors.New("page size must be positive")
	case limits.MaxItems <= 0:
		return errors.New("item limit must be positive")
	case limits.MaxPages <= 0:
		return errors.New("page limit must be positive")
	case limits.RequestTimeout <= 0:
		return errors.New("request timeout must be positive")
	default:
		return nil
	}
}

func safeUID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for i := range len(value) {
		char := value[i]
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-' || char == '_' || char == '.') {
			return false
		}
	}
	return true
}

func copyInferenceReplicas(items []omev1beta1.InferenceReplica) []omev1beta1.InferenceReplica {
	result := make([]omev1beta1.InferenceReplica, len(items))
	for i := range items {
		result[i] = *items[i].DeepCopy()
	}
	return result
}
