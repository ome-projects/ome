// Package migrationhistorycollection reads the bounded inputs used by
// migration history without copying arbitrary ConfigMap metadata or data.
package migrationhistorycollection

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/apierror"
	"sigs.k8s.io/ome/pkg/cli/paging"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

const (
	// AuditConfigMapSuffix is the controller's deterministic audit suffix.
	AuditConfigMapSuffix = "-ome-migration-audit"
	// AuditHistoryKey is the sole ConfigMap data key this collector retains.
	AuditHistoryKey = "history.json"
)

var (
	ErrOMEClientRequired               = errors.New("OME client is required")
	ErrKubeClientRequired              = errors.New("Kubernetes client is required")
	ErrNamespaceInvalid                = errors.New("namespace is invalid")
	ErrInferenceServiceNameInvalid     = errors.New("inference service name is invalid")
	ErrLimitsInvalid                   = errors.New("collection limits are invalid")
	ErrInferenceServiceRequired        = errors.New("inference service response is required")
	ErrInferenceServiceIdentityInvalid = errors.New("inference service identity is invalid")
)

// Availability is a closed, cause-free read result.
type Availability string

const (
	AvailabilityAvailable  Availability = "Available"
	AvailabilityAbsent     Availability = "Absent"
	AvailabilityForbidden  Availability = "Forbidden"
	AvailabilityUnreadable Availability = "Unreadable"
	AvailabilityInvalid    Availability = "Invalid"
)

// String is deliberately fixed so errors cannot leak through diagnostics.
func (a Availability) String() string {
	switch a {
	case AvailabilityAvailable, AvailabilityAbsent, AvailabilityForbidden,
		AvailabilityUnreadable, AvailabilityInvalid:
		return string(a)
	default:
		return "Invalid"
	}
}

// ReplicaObservation describes the bounded authoritative collection.
type ReplicaObservation struct {
	Availability  Availability
	ObservedPages int
	ObservedItems int
	Truncated     bool
}

// String returns only allowlisted availability and bounded counts.
func (o ReplicaObservation) String() string {
	return fmt.Sprintf("availability=%s pages=%d items=%d truncated=%t",
		o.Availability.String(), o.ObservedPages, o.ObservedItems, o.Truncated)
}

// AuditObservation retains only the exact ledger value and safe identity.
type AuditObservation struct {
	Namespace    string
	Name         string
	Availability Availability
	HistoryJSON  string
}

// Result is an owned snapshot. It deliberately excludes audit annotations,
// labels, owner lists, UID, resourceVersion, and unrelated ConfigMap keys.
type Result struct {
	InferenceService  *omev1beta1.InferenceService
	InferenceReplicas []omev1beta1.InferenceReplica
	Replicas          ReplicaObservation
	Audit             AuditObservation
}

// Collect gets the exact parent, a bounded related IR collection, and one
// optional audit ConfigMap from the workload namespace. Only the parent is a
// required read; unavailable secondary sources remain typed partial evidence.
func Collect(
	ctx context.Context,
	ome omeclient.OmeV1beta1Interface,
	kube kubernetes.Interface,
	namespace string,
	name string,
	limits paging.Limits,
) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if nilInterface(ome) {
		return Result{}, ErrOMEClientRequired
	}
	if nilInterface(kube) {
		return Result{}, ErrKubeClientRequired
	}
	if len(utilvalidation.IsDNS1123Label(namespace)) > 0 {
		return Result{}, ErrNamespaceInvalid
	}
	if len(utilvalidation.IsDNS1123Subdomain(name)) > 0 {
		return Result{}, ErrInferenceServiceNameInvalid
	}
	if !validLimits(limits) {
		return Result{}, ErrLimitsInvalid
	}

	requestCtx, cancel := context.WithTimeout(ctx, limits.RequestTimeout)
	parent, err := ome.InferenceServices(namespace).Get(requestCtx, name, metav1.GetOptions{})
	requestErr := requestCtx.Err()
	cancel()
	if requestErr != nil {
		return Result{}, requestErr
	}
	if err != nil {
		return Result{}, fmt.Errorf("get InferenceService: %w", apierror.Friendly(err))
	}
	if parent == nil {
		return Result{}, ErrInferenceServiceRequired
	}
	if !validParent(parent, namespace, name) {
		return Result{}, ErrInferenceServiceIdentityInvalid
	}

	result := Result{InferenceService: parent.DeepCopy()}
	result.Replicas = collectReplicas(ctx, ome, parent, limits, &result.InferenceReplicas)
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	result.Audit = collectAudit(ctx, kube, parent, limits.RequestTimeout)
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	return result, nil
}

func collectReplicas(
	ctx context.Context,
	ome omeclient.OmeV1beta1Interface,
	parent *omev1beta1.InferenceService,
	limits paging.Limits,
	destination *[]omev1beta1.InferenceReplica,
) ReplicaObservation {
	base := metav1.ListOptions{}
	useSelector := len(utilvalidation.IsValidLabelValue(parent.Name)) == 0
	if useSelector {
		base.LabelSelector = labels.Set{constants.InferenceServicePodLabelKey: parent.Name}.AsSelector().String()
	}
	listed, err := paging.ListBounded(
		ctx, base, limits,
		func(requestCtx context.Context, options metav1.ListOptions) (paging.Page[omev1beta1.InferenceReplica], error) {
			list, listErr := ome.InferenceReplicas(parent.Namespace).List(requestCtx, options)
			if requestErr := requestCtx.Err(); requestErr != nil {
				return paging.Page[omev1beta1.InferenceReplica]{}, requestErr
			}
			if listErr != nil {
				return paging.Page[omev1beta1.InferenceReplica]{}, listErr
			}
			if list == nil {
				return paging.Page[omev1beta1.InferenceReplica]{}, errors.New("empty InferenceReplica list response")
			}
			return paging.Page[omev1beta1.InferenceReplica]{Items: list.Items, Continue: list.Continue}, nil
		},
	)
	observation := ReplicaObservation{ObservedPages: listed.Pages, ObservedItems: len(listed.Items), Truncated: listed.Truncated}
	if err != nil {
		switch {
		case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
			observation.Availability = AvailabilityForbidden
		default:
			observation.Availability = AvailabilityUnreadable
		}
		return observation
	}
	observation.Availability = AvailabilityAvailable
	items := listed.Items
	if !useSelector {
		items = exactRelated(items, parent)
	}
	*destination = deepCopyReplicas(items)
	return observation
}

func collectAudit(
	ctx context.Context,
	kube kubernetes.Interface,
	parent *omev1beta1.InferenceService,
	timeout time.Duration,
) AuditObservation {
	name := parent.Name + AuditConfigMapSuffix
	result := AuditObservation{Namespace: parent.Namespace, Name: name}
	if len(utilvalidation.IsDNS1123Subdomain(name)) > 0 {
		result.Availability = AvailabilityInvalid
		return result
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	configMap, err := kube.CoreV1().ConfigMaps(parent.Namespace).Get(requestCtx, name, metav1.GetOptions{})
	requestErr := requestCtx.Err()
	cancel()
	if requestErr != nil {
		result.Availability = AvailabilityUnreadable
		return result
	}
	if err != nil {
		switch {
		case apierrors.IsNotFound(err):
			result.Availability = AvailabilityAbsent
		case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
			result.Availability = AvailabilityForbidden
		default:
			result.Availability = AvailabilityUnreadable
		}
		return result
	}
	if configMap == nil || configMap.Name != name || configMap.Namespace != parent.Namespace ||
		!hasExactControllerOwner(configMap.OwnerReferences, parent) {
		result.Availability = AvailabilityInvalid
		return result
	}
	result.Availability = AvailabilityAvailable
	result.HistoryJSON = configMap.Data[AuditHistoryKey]
	return result
}

func validLimits(limits paging.Limits) bool {
	return limits.PageSize > 0 && limits.MaxItems > 0 && limits.MaxPages > 0 && limits.RequestTimeout > 0
}

func validParent(parent *omev1beta1.InferenceService, namespace, name string) bool {
	return parent.Name == name && parent.Namespace == namespace && safeUID(string(parent.UID))
}

func exactRelated(items []omev1beta1.InferenceReplica, parent *omev1beta1.InferenceService) []omev1beta1.InferenceReplica {
	result := make([]omev1beta1.InferenceReplica, 0, len(items))
	for i := range items {
		item := &items[i]
		if item.Namespace == parent.Namespace && item.Spec.ParentRef.Name == parent.Name &&
			hasExactControllerOwner(item.OwnerReferences, parent) {
			result = append(result, *item)
		}
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

func deepCopyReplicas(items []omev1beta1.InferenceReplica) []omev1beta1.InferenceReplica {
	result := make([]omev1beta1.InferenceReplica, len(items))
	for i := range items {
		result[i] = *items[i].DeepCopy()
	}
	return result
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

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	return reflected.Kind() == reflect.Pointer && reflected.IsNil()
}
