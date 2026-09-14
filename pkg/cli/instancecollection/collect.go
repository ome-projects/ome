// Package instancecollection performs bounded, identity-checked reads of the
// InferenceReplicas related to one exact InferenceService.
package instancecollection

import (
	"context"
	"errors"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/constants"
)

var (
	ErrInferenceServiceRequired          = errors.New("instance collection requires an InferenceService")
	ErrInferenceServiceNameRequired      = errors.New("instance collection requires an InferenceService name")
	ErrInferenceServiceNamespaceRequired = errors.New("instance collection requires an InferenceService namespace")
	ErrInferenceServiceIdentityInvalid   = errors.New("instance collection requires a safe InferenceService identity")
	ErrListerRequired                    = errors.New("instance collection requires an InferenceReplica lister")
	ErrMaxStatusRowsInvalid              = errors.New("instance collection requires a positive status-row limit")
)

const (
	invalidIdentityName = "INVALID"
	maxUIDLength        = 128
)

type RejectionReason string

const (
	RejectionMetadata        RejectionReason = "Metadata"
	RejectionLabel           RejectionReason = "Label"
	RejectionParentReference RejectionReason = "ParentReference"
	RejectionOwnerReference  RejectionReason = "OwnerReference"
	RejectionComponent       RejectionReason = "Component"
)

type Rejection struct {
	Name   string
	Reason RejectionReason
}

// Limits bounds both Kubernetes list acquisition and the total number of
// nested InstanceStatuses copied into the trusted projection boundary.
type Limits struct {
	Paging        paging.Limits
	MaxStatusRows int
}

// StatusRowsTruncation identifies a related InferenceReplica whose nested
// status exceeded the collection work budget. No status row from that source
// is copied or scanned.
type StatusRowsTruncation struct {
	Name      string
	Component omev1beta1.ComponentType
}

type Result struct {
	Items               []omev1beta1.InferenceReplica
	Rejected            []Rejection
	StatusRowsTruncated []StatusRowsTruncation
	Pages               int
	Truncated           bool
}

type Lister interface {
	List(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error)
}

// CollectRelated lists with the canonical parent label and then validates the
// immutable parent reference and controller owner identity before returning an
// object. A matching label alone is never treated as ownership evidence.
func CollectRelated(
	ctx context.Context,
	lister Lister,
	isvc *omev1beta1.InferenceService,
	limits Limits,
) (Result, error) {
	if isvc == nil {
		return Result{}, ErrInferenceServiceRequired
	}
	if isvc.Name == "" {
		return Result{}, ErrInferenceServiceNameRequired
	}
	if isvc.Namespace == "" {
		return Result{}, ErrInferenceServiceNamespaceRequired
	}
	if len(validation.IsDNS1123Subdomain(isvc.Name)) != 0 ||
		len(validation.IsDNS1123Label(isvc.Namespace)) != 0 || !validUID(isvc.UID) {
		return Result{}, ErrInferenceServiceIdentityInvalid
	}
	if lister == nil {
		return Result{}, ErrListerRequired
	}
	if limits.MaxStatusRows <= 0 {
		return Result{}, ErrMaxStatusRowsInvalid
	}
	selector := labels.Set{constants.InferenceServiceLabel: isvc.Name}.AsSelector().String()
	listed, err := paging.ListBounded(
		ctx,
		metav1.ListOptions{LabelSelector: selector},
		limits.Paging,
		func(requestCtx context.Context, options metav1.ListOptions) (paging.Page[omev1beta1.InferenceReplica], error) {
			page, listErr := lister.List(requestCtx, options)
			if requestErr := requestCtx.Err(); requestErr != nil {
				return paging.Page[omev1beta1.InferenceReplica]{}, requestErr
			}
			if listErr != nil {
				return paging.Page[omev1beta1.InferenceReplica]{}, listErr
			}
			if page == nil {
				return paging.Page[omev1beta1.InferenceReplica]{}, errors.New("InferenceReplica list returned nil")
			}
			return paging.Page[omev1beta1.InferenceReplica]{Items: page.Items, Continue: page.Continue}, nil
		},
	)

	result := Result{
		Items:               make([]omev1beta1.InferenceReplica, 0, len(listed.Items)),
		Rejected:            make([]Rejection, 0),
		StatusRowsTruncated: make([]StatusRowsTruncation, 0),
		Pages:               listed.Pages, Truncated: listed.Truncated,
	}
	accepted := make([]*omev1beta1.InferenceReplica, 0, len(listed.Items))
	for i := range listed.Items {
		item := listed.Items[i]
		if reason := rejectionReason(&item, isvc); reason != "" {
			result.Rejected = append(result.Rejected, Rejection{Name: safeName(item.Name), Reason: reason})
			continue
		}
		accepted = append(accepted, &listed.Items[i])
	}
	sort.Slice(accepted, func(i, j int) bool {
		left, right := accepted[i], accepted[j]
		if componentRank(left.Spec.Component) != componentRank(right.Spec.Component) {
			return componentRank(left.Spec.Component) < componentRank(right.Spec.Component)
		}
		if left.Spec.Component != right.Spec.Component {
			return left.Spec.Component < right.Spec.Component
		}
		if left.Name != right.Name {
			return left.Name < right.Name
		}
		return left.UID < right.UID
	})
	remainingRows := limits.MaxStatusRows
	for _, item := range accepted {
		copyRows := len(item.Status.InstanceStatuses) <= remainingRows
		if !copyRows {
			result.StatusRowsTruncated = append(result.StatusRowsTruncated, StatusRowsTruncation{
				Name: item.Name, Component: item.Spec.Component,
			})
		}
		result.Items = append(result.Items, boundedReplicaCopy(item, isvc, copyRows))
		if copyRows {
			remainingRows -= len(item.Status.InstanceStatuses)
		}
	}
	return result, err
}

// boundedReplicaCopy copies only fields consumed by instance projection. It
// deliberately excludes opaque spec/status payloads and deep-copies at most
// the caller's already-admitted nested status rows.
func boundedReplicaCopy(
	ir *omev1beta1.InferenceReplica,
	isvc *omev1beta1.InferenceService,
	copyRows bool,
) omev1beta1.InferenceReplica {
	controller := true
	result := omev1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: ir.Name, Namespace: ir.Namespace, UID: ir.UID, Generation: ir.Generation,
			Labels: map[string]string{
				constants.InferenceServiceLabel: ir.Labels[constants.InferenceServiceLabel],
				constants.OMEComponentLabel:     ir.Labels[constants.OMEComponentLabel],
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: omev1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
				Name: isvc.Name, UID: isvc.UID, Controller: &controller,
			}},
		},
		Spec: omev1beta1.InferenceReplicaSpec{
			ParentRef: omev1beta1.ParentReference{Name: ir.Spec.ParentRef.Name},
			Component: ir.Spec.Component,
		},
		Status: omev1beta1.InferenceReplicaStatus{
			ObservedGeneration: ir.Status.ObservedGeneration,
			Replicas:           ir.Status.Replicas, ReadyReplicas: ir.Status.ReadyReplicas,
			ServingReplicas: ir.Status.ServingReplicas, AvailableReplicas: ir.Status.AvailableReplicas,
			UpdatedReplicas: ir.Status.UpdatedReplicas, UpdatedReadyReplicas: ir.Status.UpdatedReadyReplicas,
			CurrentRevision: ir.Status.CurrentRevision, UpdateRevision: ir.Status.UpdateRevision,
		},
	}
	if parentGeneration, present := ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]; present {
		result.Annotations = map[string]string{
			constants.InferenceReplicaParentGenerationAnnotationKey: parentGeneration,
		}
	}
	if !copyRows {
		return result
	}
	result.Status.InstanceStatuses = make([]omev1beta1.OMENativeInstanceStatus, len(ir.Status.InstanceStatuses))
	for i := range ir.Status.InstanceStatuses {
		source := &ir.Status.InstanceStatuses[i]
		row := omev1beta1.OMENativeInstanceStatus{
			Index: source.Index, Incarnation: source.Incarnation, Phase: source.Phase,
			RunningRevision: source.RunningRevision, TargetRevision: source.TargetRevision,
			PodCount: source.PodCount, ServingPodCount: source.ServingPodCount,
			AvailablePodCount: source.AvailablePodCount,
			Admitted:          source.Admitted,
		}
		if source.Operation != nil {
			row.Operation = &omev1beta1.InstanceOperation{}
		}
		if source.LastFailure != nil {
			row.LastFailure = &omev1beta1.InstanceTermination{}
		}
		result.Status.InstanceStatuses[i] = row
	}
	return result
}

func rejectionReason(ir *omev1beta1.InferenceReplica, isvc *omev1beta1.InferenceService) RejectionReason {
	if len(validation.IsDNS1123Subdomain(ir.Name)) != 0 || ir.Namespace != isvc.Namespace || !validUID(ir.UID) {
		return RejectionMetadata
	}
	if ir.Labels[constants.InferenceServiceLabel] != isvc.Name {
		return RejectionLabel
	}
	if ir.Spec.ParentRef.Name != isvc.Name {
		return RejectionParentReference
	}
	if !validOwner(ir.OwnerReferences, isvc) {
		return RejectionOwnerReference
	}
	if !validComponent(ir.Spec.Component) ||
		ir.Labels[constants.OMEComponentLabel] != string(ir.Spec.Component) {
		return RejectionComponent
	}
	return ""
}

func validOwner(owners []metav1.OwnerReference, isvc *omev1beta1.InferenceService) bool {
	var controller *metav1.OwnerReference
	for i := range owners {
		if owners[i].Controller == nil || !*owners[i].Controller {
			continue
		}
		if controller != nil {
			return false
		}
		controller = &owners[i]
	}
	if controller == nil || controller.APIVersion != omev1beta1.SchemeGroupVersion.String() ||
		controller.Kind != "InferenceService" || controller.Name != isvc.Name {
		return false
	}
	return controller.UID == isvc.UID
}

func safeName(name string) string {
	if len(validation.IsDNS1123Subdomain(name)) != 0 {
		return invalidIdentityName
	}
	return name
}

func validUID(uid types.UID) bool {
	if len(uid) == 0 || len(uid) > maxUIDLength {
		return false
	}
	for _, value := range []byte(uid) {
		if (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
			(value >= '0' && value <= '9') || value == '-' || value == '_' || value == '.' {
			continue
		}
		return false
	}
	return true
}

func validComponent(component omev1beta1.ComponentType) bool {
	switch component {
	case omev1beta1.EngineComponent, omev1beta1.DecoderComponent, omev1beta1.RouterComponent:
		return true
	default:
		return false
	}
}

func componentRank(component omev1beta1.ComponentType) int {
	switch component {
	case omev1beta1.EngineComponent:
		return 0
	case omev1beta1.DecoderComponent:
		return 1
	case omev1beta1.RouterComponent:
		return 2
	default:
		return 3
	}
}
