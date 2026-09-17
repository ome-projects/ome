package mutate

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

var revisionHash = regexp.MustCompile(`^[0-9a-f]{8}$`)
var requestUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// CollectReplicaEvidence reads only current-context related IRs. Every source
// and every bounded relevant record is checked before any positive conclusion.
func CollectReplicaEvidence(ctx context.Context, client omeclient.OmeV1beta1Interface, v *v1beta1.InferenceService, components []string, clock reportv1alpha1.Clock) (ReplicaEvidence, error) {
	return collectReplicaEvidence(ctx, client, v, components, clock, nil, nil)
}

// collected is private action evidence, copied only after whole-source bounds
// and identity validation. An error never grants authority to its prefix.
func collectReplicaEvidence(ctx context.Context, client omeclient.OmeV1beta1Interface, v *v1beta1.InferenceService, components []string, clock reportv1alpha1.Clock, collected, rawCollected *[]v1beta1.InferenceReplica) (ReplicaEvidence, error) {
	if err := ValidateTarget(v); err != nil {
		return ReplicaEvidence{}, err
	}
	if ctx == nil || client == nil || len(components) == 0 || len(components) > 3 {
		return ReplicaEvidence{}, ErrRuntime
	}
	if clock == nil {
		clock = reportv1alpha1.SystemClock{}
	}
	base := metav1.ListOptions{}
	if len(v.Name) <= 63 {
		base.LabelSelector = labels.Set{constants.InferenceServiceLabel: v.Name}.AsSelector().String()
	}
	list, err := paging.ListBounded(ctx, base, paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: 10 * time.Second}, func(requestCtx context.Context, opts metav1.ListOptions) (paging.Page[v1beta1.InferenceReplica], error) {
		value, e := client.InferenceReplicas(v.Namespace).List(requestCtx, opts)
		if e != nil {
			return paging.Page[v1beta1.InferenceReplica]{}, SafeAPIError(e)
		}
		if value == nil {
			return paging.Page[v1beta1.InferenceReplica]{}, ErrStale
		}
		if e = requestCtx.Err(); e != nil {
			return paging.Page[v1beta1.InferenceReplica]{}, e
		}
		if len(value.Items) > 16 {
			return paging.Page[v1beta1.InferenceReplica]{}, ErrBounds
		}
		for i := range value.Items {
			if !boundedPrivatePayload(&value.Items[i]) || !replicaPayloadBounded(&value.Items[i]) {
				return paging.Page[v1beta1.InferenceReplica]{}, ErrBounds
			}
		}
		return paging.Page[v1beta1.InferenceReplica]{Items: value.Items, Continue: value.Continue}, nil
	})
	if err != nil {
		return ReplicaEvidence{}, err
	}
	if list.Truncated {
		return ReplicaEvidence{}, ErrBounds
	}
	result := ReplicaEvidence{complete: true, uid: string(v.UID), resourceVersion: v.ResourceVersion, sources: map[v1beta1.ComponentType]string{}}
	seen := map[v1beta1.ComponentType]bool{}
	for i := range list.Items {
		ir := &list.Items[i]
		if len(ir.OwnerReferences) > 16 {
			return ReplicaEvidence{}, ErrBounds
		}
		claims := ir.Spec.ParentRef.Name == v.Name || ir.Labels[constants.InferenceServiceLabel] == v.Name
		for _, ref := range ir.OwnerReferences {
			if ref.UID == v.UID && ref.Controller != nil && *ref.Controller {
				claims = true
			}
		}
		if !claims && base.LabelSelector == "" {
			continue
		}
		evidence, e := inspectReplica(ir, v, components, clock.Now())
		if e != nil {
			return ReplicaEvidence{}, e
		}
		if seen[ir.Spec.Component] {
			return ReplicaEvidence{}, ErrStale
		}
		seen[ir.Spec.Component] = true
		if collected != nil {
			*collected = append(*collected, *evidence.logicalReplica)
		}
		if rawCollected != nil {
			*rawCollected = append(*rawCollected, *ir.DeepCopy())
		}
		if slices.Contains(components, string(ir.Spec.Component)) {
			revision := ir.Status.UpdateRevision
			if revision == "" {
				revision = ir.Status.CurrentRevision
			}
			result.sources[ir.Spec.Component] = strings.TrimPrefix(revision, ir.Name+"-")
		}
		result.active = result.active || evidence.active
		result.transient = result.transient || evidence.transient
		result.operations += evidence.operations
		result.migrations += evidence.migrations
	}
	if err = ctx.Err(); err != nil {
		return ReplicaEvidence{}, err
	}
	return result, nil
}

func inspectReplica(ir *v1beta1.InferenceReplica, v *v1beta1.InferenceService, components []string, now time.Time) (ReplicaEvidence, error) {
	if ir == nil || !SafeScalar(ir.Name) || !SafeScalar(string(ir.UID)) || !SafeScalar(ir.ResourceVersion) || ir.Namespace != v.Namespace || ir.Spec.ParentRef.Name != v.Name || ir.Generation <= 0 || ir.DeletionTimestamp != nil {
		return ReplicaEvidence{}, ErrStale
	}
	if ir.Kind != "" && ir.Kind != "InferenceReplica" || ir.APIVersion != "" && ir.APIVersion != "ome.io/v1beta1" {
		return ReplicaEvidence{}, ErrStale
	}
	if len(ir.OwnerReferences) > 16 || len(ir.Annotations) > 256 || len(ir.Status.Migrations) > 256 || len(ir.Status.Conditions) > 64 {
		return ReplicaEvidence{}, ErrBounds
	}
	logical, err := normalizedActionReplica(ir)
	if err != nil {
		return ReplicaEvidence{}, err
	}
	ir = logical
	if len(ir.Status.InstanceStatuses) > 2048 {
		return ReplicaEvidence{}, ErrBounds
	}
	controllers := 0
	for _, ref := range ir.OwnerReferences {
		if ref.Controller != nil && *ref.Controller {
			controllers++
			if ref.APIVersion != "ome.io/v1beta1" || ref.Kind != "InferenceService" || ref.Name != v.Name || ref.UID != v.UID {
				return ReplicaEvidence{}, ErrStale
			}
		}
	}
	if controllers != 1 || ir.Status.ObservedGeneration != ir.Generation || ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] != strconv.FormatInt(v.Generation, 10) {
		return ReplicaEvidence{}, ErrStale
	}
	if ir.Spec.Component != v1beta1.EngineComponent && ir.Spec.Component != v1beta1.DecoderComponent && ir.Spec.Component != v1beta1.RouterComponent {
		return ReplicaEvidence{}, ErrStale
	}
	if ir.Name != v.Name+"-"+string(ir.Spec.Component) {
		return ReplicaEvidence{}, ErrStale
	}
	if !slices.Contains(components, string(ir.Spec.Component)) {
		return ReplicaEvidence{complete: true, logicalReplica: ir}, nil
	}
	validRevision := func(value string) bool {
		return value == "" || strings.HasPrefix(value, ir.Name+"-") && len(value) == len(ir.Name)+9 && revisionHash.MatchString(strings.TrimPrefix(value, ir.Name+"-"))
	}
	if !validRevision(ir.Status.CurrentRevision) || !validRevision(ir.Status.UpdateRevision) || ir.Status.Replicas < 0 || ir.Status.UpdatedReplicas < 0 || ir.Status.UpdatedReplicas > ir.Status.Replicas {
		return ReplicaEvidence{}, ErrStale
	}
	for _, count := range []int32{ir.Status.ReadyReplicas, ir.Status.ServingReplicas, ir.Status.AvailableReplicas, ir.Status.UpdatedReadyReplicas} {
		if count < 0 || count > ir.Status.Replicas {
			return ReplicaEvidence{}, ErrStale
		}
	}
	if ir.Status.UpdatedReadyReplicas > ir.Status.UpdatedReplicas {
		return ReplicaEvidence{}, ErrStale
	}
	result := ReplicaEvidence{complete: true, active: ir.Status.UpdateRevision != "" && ir.Status.CurrentRevision != ir.Status.UpdateRevision, logicalReplica: ir}
	indices := map[int32]bool{}
	type migrationOperation struct {
		index, sibling int32
		request        string
		phase          v1beta1.OMENativeInstancePhase
	}
	migrationOps := []migrationOperation{}
	for _, row := range ir.Status.InstanceStatuses {
		if row.Index < 0 || indices[row.Index] || row.Incarnation < 0 || !validRevision(row.RunningRevision) || !validRevision(row.TargetRevision) || len(row.Conditions) > 64 {
			return ReplicaEvidence{}, ErrStale
		}
		indices[row.Index] = true
		switch row.Phase {
		case v1beta1.OMENativeInstancePending, v1beta1.OMENativeInstanceCreating, v1beta1.OMENativeInstanceReady, v1beta1.OMENativeInstanceUpdating, v1beta1.OMENativeInstanceRestarting, v1beta1.OMENativeInstanceMigrating, v1beta1.OMENativeInstanceFailed, v1beta1.OMENativeInstanceDeleting:
		default:
			return ReplicaEvidence{}, ErrStale
		}
		switch row.Phase {
		case v1beta1.OMENativeInstancePending, v1beta1.OMENativeInstanceCreating, v1beta1.OMENativeInstanceUpdating, v1beta1.OMENativeInstanceRestarting, v1beta1.OMENativeInstanceMigrating, v1beta1.OMENativeInstanceDeleting:
			result.transient = true
		}
		op := row.Operation
		if op == nil {
			continue
		}
		if !SafeScalar(op.ID) || !SafeScalar(op.Step) || op.StartedAt.IsZero() || op.LastProgressAt.Before(&op.StartedAt) || op.LastProgressAt.Time.After(now) || !validRevision(op.TargetRevision) || op.RetryCount < 0 || len(op.Reason) > 4096 || len(op.FromNode) > 256 || len(op.HintTargetNodes) > 64 {
			return ReplicaEvidence{}, ErrStale
		}
		if !validOperationPhase(op.Type, row.Phase) || !op.Deadline.IsZero() && op.Deadline.Before(&op.StartedAt) {
			return ReplicaEvidence{}, ErrStale
		}
		switch op.Type {
		case v1beta1.InstanceOperationCreate, v1beta1.InstanceOperationUpdate, v1beta1.InstanceOperationRestart, v1beta1.InstanceOperationMigrate:
			if op.Type == v1beta1.InstanceOperationMigrate {
				if !requestUUID.MatchString(op.RequestUUID) || op.SurgeIndex == nil || *op.SurgeIndex < 0 || *op.SurgeIndex == row.Index {
					return ReplicaEvidence{}, ErrStale
				}
				migrationOps = append(migrationOps, migrationOperation{index: row.Index, sibling: *op.SurgeIndex, request: op.RequestUUID, phase: row.Phase})
			}
			// Deadline failure preserves diagnostics on Operation. Failed
			// Update can continue surge cleanup; failed Create/Restart do not
			// establish a current operation merely by retaining that pointer.
			if row.Phase != v1beta1.OMENativeInstanceFailed || op.Type == v1beta1.InstanceOperationUpdate {
				result.active = true
				result.operations++
			}
		case v1beta1.InstanceOperationDelete:
		default:
			return ReplicaEvidence{}, ErrStale
		}
	}
	ids := map[string]bool{}
	migrationRecords := map[string]v1beta1.MigrationStatus{}
	for _, record := range ir.Status.Migrations {
		if !requestUUID.MatchString(record.RequestUUID) || ids[record.RequestUUID] || record.SourceInstance < 0 || record.StartedAt.IsZero() || record.StartedAt.Time.After(now) || len(record.Reason) > 4096 || len(record.Message) > 4096 || len(record.HintTargetNodes) > 64 {
			return ReplicaEvidence{}, ErrStale
		}
		ids[record.RequestUUID] = true
		migrationRecords[record.RequestUUID] = record
		if !validMigrationIdentity(record, now) {
			return ReplicaEvidence{}, ErrStale
		}
		if record.Trigger != v1beta1.MigrationTriggerManual && record.Trigger != v1beta1.MigrationTriggerAuto || record.Trigger == v1beta1.MigrationTriggerAuto && record.Phase != v1beta1.MigrationPhaseRelocated || record.Trigger == v1beta1.MigrationTriggerManual && record.Phase == v1beta1.MigrationPhaseRelocated {
			return ReplicaEvidence{}, ErrStale
		}
		if record.CompletedAt != nil && (record.CompletedAt.Before(&record.StartedAt) || record.CompletedAt.Time.After(now)) {
			return ReplicaEvidence{}, ErrStale
		}
		switch record.Phase {
		case v1beta1.MigrationPhaseCompleted, v1beta1.MigrationPhaseFailed, v1beta1.MigrationPhaseRelocated:
			continue
		case v1beta1.MigrationPhaseAccepted, v1beta1.MigrationPhaseSurgePending, v1beta1.MigrationPhaseSurgeReady, v1beta1.MigrationPhaseDraining:
		default:
			return ReplicaEvidence{}, ErrStale
		}
		if record.Trigger != v1beta1.MigrationTriggerManual || !SafeScalar(record.FromNode) || record.CompletedAt != nil || record.Deadline.Before(&record.StartedAt) && !record.Deadline.IsZero() {
			return ReplicaEvidence{}, ErrStale
		}
		result.active = true
		result.migrations++
	}
	for _, op := range migrationOps {
		record, exists := migrationRecords[op.request]
		if !exists || record.Trigger != v1beta1.MigrationTriggerManual || record.Phase.Terminal() || record.SurgeInstance == nil {
			return ReplicaEvidence{}, ErrStale
		}
		source := record.SourceInstance == op.index && *record.SurgeInstance == op.sibling && op.phase == v1beta1.OMENativeInstanceMigrating
		surge := *record.SurgeInstance == op.index && record.SourceInstance == op.sibling && op.phase == v1beta1.OMENativeInstanceCreating
		if !source && !surge {
			return ReplicaEvidence{}, ErrStale
		}
	}
	return result, nil
}

// normalizedActionReplica validates and decodes only a private bounded copy.
// The raw fetched object remains available for exact CAS revalidation.
func normalizedActionReplica(ir *v1beta1.InferenceReplica) (*v1beta1.InferenceReplica, error) {
	if ir == nil {
		return nil, ErrStale
	}
	if !boundedPrivatePayload(ir) || !replicaPayloadBounded(ir) {
		return nil, ErrBounds
	}
	logical := ir.DeepCopy()
	_, err := irstatus.NewDecoder(2048).Decode(logical)
	if err != nil {
		if reason, ok := irstatus.ErrorReasonOf(err); ok && reason == irstatus.ErrorReasonCardinalityLimit {
			return nil, ErrBounds
		}
		return nil, ErrStale
	}
	if !replicaPayloadBounded(logical) || !boundedPrivatePayload(logical) {
		return nil, ErrBounds
	}
	return logical, nil
}

func validOperationPhase(kind v1beta1.InstanceOperationType, phase v1beta1.OMENativeInstancePhase) bool {
	switch kind {
	case v1beta1.InstanceOperationCreate:
		return phase == v1beta1.OMENativeInstanceCreating || phase == v1beta1.OMENativeInstanceFailed
	case v1beta1.InstanceOperationRestart:
		return phase == v1beta1.OMENativeInstanceRestarting || phase == v1beta1.OMENativeInstanceFailed
	case v1beta1.InstanceOperationUpdate:
		return phase == v1beta1.OMENativeInstanceUpdating || phase == v1beta1.OMENativeInstanceCreating || phase == v1beta1.OMENativeInstanceFailed
	case v1beta1.InstanceOperationMigrate:
		return phase == v1beta1.OMENativeInstanceMigrating || phase == v1beta1.OMENativeInstanceCreating
	case v1beta1.InstanceOperationDelete:
		return phase == v1beta1.OMENativeInstanceDeleting || phase == v1beta1.OMENativeInstanceFailed
	default:
		return false
	}
}

func validMigrationIdentity(row v1beta1.MigrationStatus, now time.Time) bool {
	if row.Trigger == v1beta1.MigrationTriggerManual && row.Attempt != 0 || row.Trigger == v1beta1.MigrationTriggerAuto && row.Attempt <= 0 {
		return false
	}
	if row.Deadline.IsZero() || row.Deadline.Before(&row.StartedAt) {
		return false
	}
	if row.AllocatedAt != nil && (row.AllocatedAt.IsZero() || row.AllocatedAt.Before(&row.StartedAt) || row.AllocatedAt.Time.After(now)) {
		return false
	}
	hasSurge := row.SurgeInstance != nil
	hasAllocation := row.AllocatedAt != nil
	legacyQueued := row.Phase == v1beta1.MigrationPhaseAccepted && hasSurge && *row.SurgeInstance == -1 && !hasAllocation
	if hasSurge && !legacyQueued && (*row.SurgeInstance < 0 || *row.SurgeInstance == row.SourceInstance) {
		return false
	}
	if hasSurge != hasAllocation && !legacyQueued {
		return false
	}
	switch row.Phase {
	case v1beta1.MigrationPhaseAccepted:
		if (hasSurge || hasAllocation) && !legacyQueued {
			return false
		}
	case v1beta1.MigrationPhaseSurgePending, v1beta1.MigrationPhaseSurgeReady, v1beta1.MigrationPhaseDraining, v1beta1.MigrationPhaseCompleted:
		if !hasSurge || !hasAllocation {
			return false
		}
	case v1beta1.MigrationPhaseRelocated:
		if hasSurge || hasAllocation {
			return false
		}
	}
	if row.Phase.Terminal() != (row.CompletedAt != nil) {
		return false
	}
	if row.CompletedAt != nil && (row.CompletedAt.IsZero() || row.CompletedAt.Before(&row.StartedAt) || row.CompletedAt.Time.After(now) || row.AllocatedAt != nil && row.CompletedAt.Before(row.AllocatedAt)) {
		return false
	}
	return row.Succeeded == nil || row.Trigger == v1beta1.MigrationTriggerAuto && row.Phase == v1beta1.MigrationPhaseRelocated && *row.Succeeded
}

// Bound the complete private fields consumed by applicability checks, before
// walking them for semantics. Never copy their content to an output contract.
func replicaPayloadBounded(ir *v1beta1.InferenceReplica) bool {
	metadataBytes := 0
	for k, v := range ir.Annotations {
		metadataBytes += len(k) + len(v)
		if metadataBytes > 65536 {
			return false
		}
	}
	for k, v := range ir.Labels {
		metadataBytes += len(k) + len(v)
		if metadataBytes > 65536 {
			return false
		}
	}
	if len(ir.Labels) > 256 || len(ir.Status.RetryBlocks) > 64 || len(ir.Status.Traffic) > 8 {
		return false
	}
	payloadBytes := 0
	add := func(values ...string) bool {
		for _, value := range values {
			payloadBytes += len(value)
			if len(value) > 4096 || payloadBytes > 1024*1024 {
				return false
			}
		}
		return true
	}
	conditions := func(values []metav1.Condition) bool {
		if len(values) > 64 {
			return false
		}
		for _, value := range values {
			if !add(value.Type, string(value.Status), value.Reason, value.Message) {
				return false
			}
		}
		return true
	}
	if !conditions(ir.Status.Conditions) {
		return false
	}
	for _, row := range ir.Status.InstanceStatuses {
		if !add(string(row.Phase), row.RunningRevision, row.TargetRevision) || !conditions(row.Conditions) {
			return false
		}
		if row.Operation != nil {
			op := row.Operation
			if !add(op.ID, string(op.Type), op.Step, op.TargetRevision, op.Reason, op.FromNode, op.RequestUUID) || len(op.HintTargetNodes) > 64 {
				return false
			}
			for _, node := range op.HintTargetNodes {
				if len(node) > 256 || !add(node) {
					return false
				}
			}
		}
	}
	for _, row := range ir.Status.Migrations {
		if !add(row.RequestUUID, string(row.Trigger), row.FromNode, string(row.Phase), row.Reason, row.Message) || len(row.HintTargetNodes) > 64 {
			return false
		}
		for _, node := range row.HintTargetNodes {
			if len(node) > 256 || !add(node) {
				return false
			}
		}
	}
	for _, row := range ir.Status.RetryBlocks {
		if !add(row.TargetRevision, string(row.State), row.Reason) {
			return false
		}
	}
	return true
}

// SafeAPIError never copies arbitrary API/status payloads into action output.
func SafeAPIError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return errors.New("required Kubernetes API request failed; check access and connectivity")
}
