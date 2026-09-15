// Package instancecollection performs bounded, identity-checked reads of the
// InferenceReplicas related to one exact InferenceService.
package instancecollection

import (
	"context"
	"errors"
	"sort"
	"strings"

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
	ErrMaxRetryBlocksInvalid             = errors.New("instance collection requires a non-negative retry-block limit")
	ErrDetailLimitsInvalid               = errors.New("instance collection detail limits are invalid")
)

const (
	invalidIdentityName    = "INVALID"
	maxUIDLength           = 128
	maxRetryRevisionBytes  = 1024
	maxRetryReasonBytes    = 4096
	maxInstanceDetailBytes = 1024
	maxInstanceBaseBytes   = 1024
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
	// MaxRetryBlocks bounds retry-block copies across every accepted source.
	// Zero disables collection for callers that do not consume these records.
	MaxRetryBlocks int
	Details        DetailLimits
}

// DetailLimits enables bounded copying of the nested fields needed by the
// single-instance status command. A zero value disables detail copying so the
// existing instance-list boundary remains unchanged.
type DetailLimits struct {
	MaxConditions        int
	MaxScannedConditions int
	MaxNodeHints         int
	MaxScannedNodeHints  int
	MaxMigrations        int
	MaxScannedMigrations int
	SelectedComponent    omev1beta1.ComponentType
	SelectedIndex        int32
}

type DetailKind string

const (
	DetailConditions DetailKind = "Conditions"
	DetailNodeHints  DetailKind = "NodeHints"
	DetailMigrations DetailKind = "Migrations"
)

type DetailTruncation struct {
	Name      string
	Component omev1beta1.ComponentType
	Index     int32
	Kind      DetailKind
}

// DetailMalformed identifies rejected detail records independently of an
// output or scan cutoff. It carries no raw condition values.
type DetailMalformed struct {
	Name      string
	Component omev1beta1.ComponentType
	Index     int32
	Kind      DetailKind
}

// StatusRowsTruncation identifies a related InferenceReplica whose nested
// status exceeded the collection work budget. No status row from that source
// is copied or scanned.
type StatusRowsTruncation struct {
	Name      string
	Component omev1beta1.ComponentType
}

// RetryBlocksTruncation identifies a related InferenceReplica whose nested
// retry-block list exceeded the collection work budget. No block from that
// source is copied or scanned.
type RetryBlocksTruncation struct {
	Name      string
	Component omev1beta1.ComponentType
}

type Result struct {
	Items                []omev1beta1.InferenceReplica
	Rejected             []Rejection
	StatusRowsTruncated  []StatusRowsTruncation
	RetryBlocksTruncated []RetryBlocksTruncation
	DetailsTruncated     []DetailTruncation
	DetailsMalformed     []DetailMalformed
	Pages                int
	Truncated            bool
}

type Lister interface {
	List(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error)
}

// CollectRelated uses the canonical parent label when the parent name is a
// valid label value, then validates the immutable parent reference and exact
// controller owner identity before returning an object. Longer valid parent
// names use the same bounded namespace scan without a relationship selector.
// A matching label alone is never treated as ownership evidence.
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
	if limits.MaxRetryBlocks < 0 {
		return Result{}, ErrMaxRetryBlocksInvalid
	}
	if !validDetailLimits(limits.Details) {
		return Result{}, ErrDetailLimitsInvalid
	}
	requireRelationshipLabel := len(validation.IsValidLabelValue(isvc.Name)) == 0
	listOptions := metav1.ListOptions{}
	if requireRelationshipLabel {
		listOptions.LabelSelector = labels.Set{constants.InferenceServiceLabel: isvc.Name}.AsSelector().String()
	}
	listed, err := paging.ListBounded(
		ctx,
		listOptions,
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
		Items:                make([]omev1beta1.InferenceReplica, 0, len(listed.Items)),
		Rejected:             make([]Rejection, 0),
		StatusRowsTruncated:  make([]StatusRowsTruncation, 0),
		RetryBlocksTruncated: make([]RetryBlocksTruncation, 0),
		DetailsTruncated:     make([]DetailTruncation, 0),
		DetailsMalformed:     make([]DetailMalformed, 0),
		Pages:                listed.Pages, Truncated: listed.Truncated,
	}
	accepted := make([]*omev1beta1.InferenceReplica, 0, len(listed.Items))
	for i := range listed.Items {
		item := listed.Items[i]
		if !requireRelationshipLabel && !claimsTargetParent(&item, isvc) {
			continue
		}
		if reason := rejectionReason(&item, isvc, requireRelationshipLabel); reason != "" {
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
	remainingRetryBlocks := limits.MaxRetryBlocks
	for _, item := range accepted {
		copyRows := len(item.Status.InstanceStatuses) <= remainingRows
		if !copyRows {
			result.StatusRowsTruncated = append(result.StatusRowsTruncated, StatusRowsTruncation{
				Name: item.Name, Component: item.Spec.Component,
			})
		}
		copyRetryBlocks := limits.MaxRetryBlocks > 0 && len(item.Status.RetryBlocks) <= remainingRetryBlocks
		if limits.MaxRetryBlocks > 0 && !copyRetryBlocks {
			result.RetryBlocksTruncated = append(result.RetryBlocksTruncated, RetryBlocksTruncation{
				Name: item.Name, Component: item.Spec.Component,
			})
		}
		copied, truncations, malformed := boundedReplicaCopy(item, isvc, copyRows, copyRetryBlocks, limits.Details)
		result.Items = append(result.Items, copied)
		result.DetailsTruncated = append(result.DetailsTruncated, truncations...)
		result.DetailsMalformed = append(result.DetailsMalformed, malformed...)
		if copyRows {
			remainingRows -= len(item.Status.InstanceStatuses)
		}
		if copyRetryBlocks {
			remainingRetryBlocks -= len(item.Status.RetryBlocks)
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
	copyRetryBlocks bool,
	detailLimits DetailLimits,
) (omev1beta1.InferenceReplica, []DetailTruncation, []DetailMalformed) {
	controller := true
	result := omev1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: ir.Name, Namespace: ir.Namespace, UID: ir.UID, Generation: ir.Generation,
			DeletionTimestamp: copyTime(ir.DeletionTimestamp),
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
			CurrentRevision: boundedClone(ir.Status.CurrentRevision, maxInstanceBaseBytes),
			UpdateRevision:  boundedClone(ir.Status.UpdateRevision, maxInstanceBaseBytes),
		},
	}
	if parentGeneration, present := ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]; present {
		result.Annotations = map[string]string{
			constants.InferenceReplicaParentGenerationAnnotationKey: boundedClone(parentGeneration, maxInstanceBaseBytes),
		}
	}
	if !copyRows {
		return copyBoundedRetryBlocks(result, ir, copyRetryBlocks), nil, nil
	}
	truncations := make([]DetailTruncation, 0)
	malformed := make([]DetailMalformed, 0)
	result.Status.InstanceStatuses = make([]omev1beta1.OMENativeInstanceStatus, len(ir.Status.InstanceStatuses))
	for i := range ir.Status.InstanceStatuses {
		source := &ir.Status.InstanceStatuses[i]
		row := omev1beta1.OMENativeInstanceStatus{
			Index: source.Index, Incarnation: source.Incarnation, Phase: source.Phase,
			RunningRevision: boundedClone(source.RunningRevision, maxInstanceBaseBytes),
			TargetRevision:  boundedClone(source.TargetRevision, maxInstanceBaseBytes),
			PodCount:        source.PodCount, ServingPodCount: source.ServingPodCount,
			AvailablePodCount: source.AvailablePodCount,
			Admitted:          source.Admitted,
		}
		if detailLimits.selects(ir.Spec.Component, source.Index) {
			row.ReadySince = copyTime(source.ReadySince)
			row.ActiveOrdinal = source.ActiveOrdinal
			var conditionsMalformed bool
			row.Conditions, truncations, conditionsMalformed = copyConditions(
				source.Conditions, detailLimits, ir, source.Index, truncations,
			)
			if conditionsMalformed {
				malformed = append(malformed, DetailMalformed{Name: ir.Name, Component: ir.Spec.Component, Index: source.Index, Kind: DetailConditions})
			}
			if source.Operation != nil {
				operation := *source.Operation
				operation.ID = boundedClone(source.Operation.ID, maxInstanceDetailBytes)
				operation.Step = boundedClone(source.Operation.Step, maxInstanceDetailBytes)
				operation.TargetRevision = boundedClone(source.Operation.TargetRevision, maxInstanceDetailBytes)
				operation.Reason = boundedClone(source.Operation.Reason, maxInstanceDetailBytes)
				operation.FromNode = boundedClone(source.Operation.FromNode, maxInstanceDetailBytes)
				operation.RequestUUID = boundedClone(source.Operation.RequestUUID, maxInstanceDetailBytes)
				operation.SurgeIndex = copyInt32(source.Operation.SurgeIndex)
				operation.HintTargetNodes = nil
				if len(source.Operation.HintTargetNodes) > detailLimits.MaxScannedNodeHints {
					truncations = appendDetailTruncation(truncations, ir, source.Index, DetailNodeHints)
				} else {
					operation.HintTargetNodes = make([]string, len(source.Operation.HintTargetNodes))
					for i := range source.Operation.HintTargetNodes {
						operation.HintTargetNodes[i] = boundedClone(source.Operation.HintTargetNodes[i], maxInstanceDetailBytes)
					}
					sort.Strings(operation.HintTargetNodes)
					if len(operation.HintTargetNodes) > detailLimits.MaxNodeHints {
						operation.HintTargetNodes = operation.HintTargetNodes[:detailLimits.MaxNodeHints]
						truncations = appendDetailTruncation(truncations, ir, source.Index, DetailNodeHints)
					}
				}
				row.Operation = &operation
			}
			if source.LastFailure != nil {
				failure := *source.LastFailure
				failure.PodName = boundedClone(source.LastFailure.PodName, maxInstanceDetailBytes)
				failure.ContainerName = boundedClone(source.LastFailure.ContainerName, maxInstanceDetailBytes)
				failure.Reason = boundedClone(source.LastFailure.Reason, maxInstanceDetailBytes)
				failure.ExitCode = copyInt32(source.LastFailure.ExitCode)
				failure.Message = ""
				row.LastFailure = &failure
			}
		} else if !detailLimits.enabled() {
			if source.Operation != nil {
				row.Operation = &omev1beta1.InstanceOperation{}
			}
			if source.LastFailure != nil {
				row.LastFailure = &omev1beta1.InstanceTermination{}
			}
		}
		result.Status.InstanceStatuses[i] = row
	}
	if detailLimits.MaxMigrations > 0 && detailLimits.SelectedComponent == ir.Spec.Component {
		result.Status.Migrations, truncations = copyMigrations(ir.Status.Migrations, detailLimits, ir, truncations)
	}
	return copyBoundedRetryBlocks(result, ir, copyRetryBlocks), truncations, malformed
}

func copyMigrations(
	input []omev1beta1.MigrationStatus,
	limits DetailLimits,
	ir *omev1beta1.InferenceReplica,
	truncations []DetailTruncation,
) ([]omev1beta1.MigrationStatus, []DetailTruncation) {
	if len(input) > limits.MaxScannedMigrations {
		return nil, appendDetailTruncation(truncations, ir, limits.SelectedIndex, DetailMigrations)
	}
	identities := make(map[string]int, len(input))
	for i := range input {
		identities[input[i].RequestUUID]++
	}
	selected := make([]omev1beta1.MigrationStatus, 0, min(len(input), limits.MaxMigrations))
	for i := range input {
		source := &input[i]
		if source.SourceInstance != limits.SelectedIndex && (source.SurgeInstance == nil || *source.SurgeInstance != limits.SelectedIndex) {
			continue
		}
		if identities[source.RequestUUID] != 1 {
			truncations = appendDetailTruncation(truncations, ir, limits.SelectedIndex, DetailMigrations)
			continue
		}
		copy := *source
		copy.RequestUUID = boundedClone(source.RequestUUID, maxInstanceDetailBytes)
		copy.SurgeInstance = copyInt32(source.SurgeInstance)
		copy.AllocatedAt = copyTime(source.AllocatedAt)
		copy.FromNode = boundedClone(source.FromNode, maxInstanceDetailBytes)
		copy.Reason = boundedClone(source.Reason, maxInstanceDetailBytes)
		copy.Message = boundedClone(source.Message, maxInstanceDetailBytes)
		copy.StartedAt = *copyTime(&source.StartedAt)
		copy.Deadline = *copyTime(&source.Deadline)
		copy.CompletedAt = copyTime(source.CompletedAt)
		copy.Succeeded = copyBool(source.Succeeded)
		copy.HintTargetNodes = nil
		if len(source.HintTargetNodes) > limits.MaxScannedNodeHints {
			truncations = appendDetailTruncation(truncations, ir, limits.SelectedIndex, DetailMigrations)
		} else {
			copy.HintTargetNodes = make([]string, len(source.HintTargetNodes))
			for j := range source.HintTargetNodes {
				copy.HintTargetNodes[j] = boundedClone(source.HintTargetNodes[j], maxInstanceDetailBytes)
			}
			sort.Strings(copy.HintTargetNodes)
			if len(copy.HintTargetNodes) > limits.MaxNodeHints {
				copy.HintTargetNodes = copy.HintTargetNodes[:limits.MaxNodeHints]
				truncations = appendDetailTruncation(truncations, ir, limits.SelectedIndex, DetailMigrations)
			}
		}
		selected = append(selected, copy)
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].RequestUUID < selected[j].RequestUUID })
	if len(selected) > limits.MaxMigrations {
		selected = selected[:limits.MaxMigrations]
		truncations = appendDetailTruncation(truncations, ir, limits.SelectedIndex, DetailMigrations)
	}
	return selected, truncations
}

func validDetailLimits(limits DetailLimits) bool {
	if !limits.enabled() {
		return limits == (DetailLimits{})
	}
	return limits.MaxConditions > 0 && limits.MaxScannedConditions >= limits.MaxConditions &&
		limits.MaxNodeHints > 0 && limits.MaxScannedNodeHints >= limits.MaxNodeHints &&
		((limits.MaxMigrations == 0 && limits.MaxScannedMigrations == 0) ||
			(limits.MaxMigrations > 0 && limits.MaxScannedMigrations >= limits.MaxMigrations)) &&
		validDetailComponent(limits.SelectedComponent) && limits.SelectedIndex >= 0
}

func (limits DetailLimits) enabled() bool {
	return limits.MaxConditions != 0 || limits.MaxScannedConditions != 0 ||
		limits.MaxNodeHints != 0 || limits.MaxScannedNodeHints != 0 ||
		limits.MaxMigrations != 0 || limits.MaxScannedMigrations != 0 ||
		limits.SelectedComponent != "" || limits.SelectedIndex != 0
}

func (limits DetailLimits) selects(component omev1beta1.ComponentType, index int32) bool {
	return limits.enabled() && limits.SelectedComponent == component && limits.SelectedIndex == index
}

func validDetailComponent(component omev1beta1.ComponentType) bool {
	switch component {
	case omev1beta1.EngineComponent, omev1beta1.DecoderComponent, omev1beta1.RouterComponent:
		return true
	default:
		return false
	}
}

func copyConditions(
	input []metav1.Condition,
	limits DetailLimits,
	ir *omev1beta1.InferenceReplica,
	index int32,
	truncations []DetailTruncation,
) ([]metav1.Condition, []DetailTruncation, bool) {
	if len(input) > limits.MaxScannedConditions {
		return nil, appendDetailTruncation(truncations, ir, index, DetailConditions), false
	}
	conditions := make([]metav1.Condition, len(input))
	for i := range input {
		conditions[i] = metav1.Condition{
			Type: boundedClone(input[i].Type, maxInstanceDetailBytes), Status: input[i].Status,
			ObservedGeneration: input[i].ObservedGeneration, LastTransitionTime: input[i].LastTransitionTime,
			Reason: boundedClone(input[i].Reason, maxInstanceDetailBytes),
		}
	}
	sort.Slice(conditions, func(i, j int) bool { return conditionLess(conditions[i], conditions[j]) })
	// Inspect complete groups within the admitted scan budget before capping.
	// A condition type has one authoritative value; differing allowlisted
	// records are ambiguous, even if their status happens to agree.
	valid := conditions[:0]
	malformed := false
	for start := 0; start < len(conditions); {
		end := start + 1
		for end < len(conditions) && conditions[end].Type == conditions[start].Type {
			end++
		}
		condition := conditions[start]
		ambiguous := conditionLess(condition, conditions[end-1])
		if ambiguous || (condition.Status != metav1.ConditionTrue && condition.Status != metav1.ConditionFalse && condition.Status != metav1.ConditionUnknown) ||
			condition.ObservedGeneration < 0 || condition.ObservedGeneration > ir.Generation {
			malformed = true
		} else {
			valid = append(valid, condition)
		}
		start = end
	}
	conditions = valid
	if len(conditions) > limits.MaxConditions {
		conditions = conditions[:limits.MaxConditions]
		truncations = appendDetailTruncation(truncations, ir, index, DetailConditions)
	}
	return conditions, truncations, malformed
}

func conditionLess(left, right metav1.Condition) bool {
	if left.Type != right.Type {
		return left.Type < right.Type
	}
	if left.Status != right.Status {
		return left.Status < right.Status
	}
	if left.Reason != right.Reason {
		return left.Reason < right.Reason
	}
	if left.ObservedGeneration != right.ObservedGeneration {
		return left.ObservedGeneration < right.ObservedGeneration
	}
	return left.LastTransitionTime.Before(&right.LastTransitionTime)
}

func appendDetailTruncation(
	truncations []DetailTruncation,
	ir *omev1beta1.InferenceReplica,
	index int32,
	kind DetailKind,
) []DetailTruncation {
	return append(truncations, DetailTruncation{
		Name: ir.Name, Component: ir.Spec.Component, Index: index, Kind: kind,
	})
}

func copyInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func copyBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func copyBoundedRetryBlocks(
	result omev1beta1.InferenceReplica,
	ir *omev1beta1.InferenceReplica,
	copyRetryBlocks bool,
) omev1beta1.InferenceReplica {
	if !copyRetryBlocks {
		return result
	}
	result.Status.RetryBlocks = make([]omev1beta1.RetryBlock, len(ir.Status.RetryBlocks))
	for i := range ir.Status.RetryBlocks {
		source := &ir.Status.RetryBlocks[i]
		result.Status.RetryBlocks[i] = omev1beta1.RetryBlock{
			TargetRevision: boundedClone(source.TargetRevision, maxRetryRevisionBytes),
			State:          source.State, AttemptsStarted: source.AttemptsStarted,
			NextRetryAt: copyTime(source.NextRetryAt), FirstFailureAt: copyTime(source.FirstFailureAt),
			LastFailureAt: copyTime(source.LastFailureAt),
			Reason:        boundedClone(source.Reason, maxRetryReasonBytes),
		}
	}
	return result
}

func copyTime(value *metav1.Time) *metav1.Time {
	if value == nil {
		return nil
	}
	return value.DeepCopy()
}

func boundedClone(value string, maxBytes int) string {
	if len(value) > maxBytes {
		value = value[:maxBytes]
	}
	// A single-byte replacement keeps the sanitized copy within the byte
	// budget even when the cut lands inside a multi-byte rune or the source
	// contains many invalid byte sequences.
	return strings.Clone(strings.ToValidUTF8(value, "?"))
}

func claimsTargetParent(
	ir *omev1beta1.InferenceReplica,
	isvc *omev1beta1.InferenceService,
) bool {
	if ir.Spec.ParentRef.Name == isvc.Name ||
		ir.Labels[constants.InferenceServiceLabel] == isvc.Name {
		return true
	}
	for i := range ir.OwnerReferences {
		owner := &ir.OwnerReferences[i]
		if owner.UID == isvc.UID ||
			(owner.Kind == "InferenceService" && owner.Name == isvc.Name) {
			return true
		}
	}
	return false
}

func rejectionReason(
	ir *omev1beta1.InferenceReplica,
	isvc *omev1beta1.InferenceService,
	requireRelationshipLabel bool,
) RejectionReason {
	if len(validation.IsDNS1123Subdomain(ir.Name)) != 0 || ir.Namespace != isvc.Namespace || !validUID(ir.UID) {
		return RejectionMetadata
	}
	if requireRelationshipLabel && ir.Labels[constants.InferenceServiceLabel] != isvc.Name {
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
