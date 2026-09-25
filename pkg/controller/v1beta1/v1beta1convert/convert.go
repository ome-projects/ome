// Package v1beta1convert contains adapter helpers that bridge between
// workload-owned types and the v1beta1.OMENative* source-of-truth types
// that controllers (ISVC OMENative dispatch, InferenceReplica) read and
// write. These converters are the only place in the repo where the
// workload package's type set crosses the boundary into v1beta1.OMENative*
// — workload code itself never imports pkg/apis/ome/v1beta1.
//
// The converters are field-for-field mirrors. They round-trip cleanly
// for every value of every enum; round-trip tests live in convert_test.go.
//
// The package sits at the workload boundary by design: every caller is
// an adapter (an ISVC-side or IR-side reconciler) that needs to map
// CRD-shape values into workload-shape values (or vice versa) at a
// single seam. Placing the converters under pkg/controller/v1beta1/
// (a sibling of workload/, not inside the workload tree and not under
// any specific reconciler tree) keeps the dependency direction clean —
// owner-CRD adapters depend on the converters, the converters depend on
// workload + v1beta1, and the workload package itself stays free of
// v1beta1 imports.
package v1beta1convert

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// ComponentTypeToWorkload converts a v1beta1.ComponentType to a
// workload.ComponentType. Unknown values map to the empty string so
// adapters tolerate API-version skew without panicking.
func ComponentTypeToWorkload(v v1beta1.ComponentType) workloadtypes.ComponentType {
	switch v {
	case v1beta1.RouterComponent:
		return workloadtypes.ComponentRouter
	case v1beta1.EngineComponent:
		return workloadtypes.ComponentEngine
	case v1beta1.DecoderComponent:
		return workloadtypes.ComponentDecoder
	default:
		return workloadtypes.ComponentType("")
	}
}

// ComponentTypeFromWorkload converts a workload.ComponentType to a
// v1beta1.ComponentType. Unknown values map to the empty string so
// adapters tolerate API-version skew without panicking.
func ComponentTypeFromWorkload(w workloadtypes.ComponentType) v1beta1.ComponentType {
	switch w {
	case workloadtypes.ComponentRouter:
		return v1beta1.RouterComponent
	case workloadtypes.ComponentEngine:
		return v1beta1.EngineComponent
	case workloadtypes.ComponentDecoder:
		return v1beta1.DecoderComponent
	default:
		return v1beta1.ComponentType("")
	}
}

// InstancePhaseToWorkload converts a v1beta1.OMENativeInstancePhase to
// a workload.InstancePhase. Unknown values map to the empty phase so
// adapters tolerate API-version skew without panicking.
func InstancePhaseToWorkload(v v1beta1.OMENativeInstancePhase) workloadtypes.InstancePhase {
	switch v {
	case "":
		return workloadtypes.InstancePhaseEmpty
	case v1beta1.OMENativeInstancePending:
		return workloadtypes.InstancePhasePending
	case v1beta1.OMENativeInstanceCreating:
		return workloadtypes.InstancePhaseCreating
	case v1beta1.OMENativeInstanceReady:
		return workloadtypes.InstancePhaseReady
	case v1beta1.OMENativeInstanceUpdating:
		return workloadtypes.InstancePhaseUpdating
	case v1beta1.OMENativeInstanceRestarting:
		return workloadtypes.InstancePhaseRestarting
	case v1beta1.OMENativeInstanceMigrating:
		return workloadtypes.InstancePhaseMigrating
	case v1beta1.OMENativeInstanceFailed:
		return workloadtypes.InstancePhaseFailed
	case v1beta1.OMENativeInstanceDeleting:
		return workloadtypes.InstancePhaseDeleting
	default:
		return workloadtypes.InstancePhaseEmpty
	}
}

// InstancePhaseFromWorkload converts a workload.InstancePhase to a
// v1beta1.OMENativeInstancePhase. Unknown values map to the empty
// phase so adapters tolerate API-version skew without panicking.
func InstancePhaseFromWorkload(w workloadtypes.InstancePhase) v1beta1.OMENativeInstancePhase {
	switch w {
	case workloadtypes.InstancePhaseEmpty:
		return v1beta1.OMENativeInstancePhase("")
	case workloadtypes.InstancePhasePending:
		return v1beta1.OMENativeInstancePending
	case workloadtypes.InstancePhaseCreating:
		return v1beta1.OMENativeInstanceCreating
	case workloadtypes.InstancePhaseReady:
		return v1beta1.OMENativeInstanceReady
	case workloadtypes.InstancePhaseUpdating:
		return v1beta1.OMENativeInstanceUpdating
	case workloadtypes.InstancePhaseRestarting:
		return v1beta1.OMENativeInstanceRestarting
	case workloadtypes.InstancePhaseMigrating:
		return v1beta1.OMENativeInstanceMigrating
	case workloadtypes.InstancePhaseFailed:
		return v1beta1.OMENativeInstanceFailed
	case workloadtypes.InstancePhaseDeleting:
		return v1beta1.OMENativeInstanceDeleting
	default:
		return v1beta1.OMENativeInstancePhase("")
	}
}

// UpdateStrategyTypeToWorkload converts a v1beta1.UpdateStrategyType
// to a workload.UpdateStrategyType. Unknown values map to the empty string
// so adapters tolerate API-version skew without panicking.
//
// The workload state machine compares against workload.UpdateStrategyType
// constants exclusively — this converter is the only seam where the
// CRD-shape strategy enters workload code, so any future drift between
// the v1beta1 enum and the workload enum becomes a compile-time error.
func UpdateStrategyTypeToWorkload(v v1beta1.UpdateStrategyType) workloadtypes.UpdateStrategyType {
	switch v {
	case v1beta1.UpdateStrategySurgeThenDrain:
		return workloadtypes.UpdateStrategySurgeThenDrain
	case v1beta1.UpdateStrategyRecreatePod:
		return workloadtypes.UpdateStrategyRecreatePod
	case v1beta1.UpdateStrategyInPlaceIfPossible:
		return workloadtypes.UpdateStrategyInPlaceIfPossible
	case v1beta1.UpdateStrategyInPlaceOnly:
		return workloadtypes.UpdateStrategyInPlaceOnly
	default:
		return workloadtypes.UpdateStrategyType("")
	}
}

// UpdateStrategyTypeFromWorkload converts a workload.UpdateStrategyType
// to a v1beta1.UpdateStrategyType. Unknown values map to the
// empty string so adapters tolerate API-version skew without panicking.
func UpdateStrategyTypeFromWorkload(w workloadtypes.UpdateStrategyType) v1beta1.UpdateStrategyType {
	switch w {
	case workloadtypes.UpdateStrategySurgeThenDrain:
		return v1beta1.UpdateStrategySurgeThenDrain
	case workloadtypes.UpdateStrategyRecreatePod:
		return v1beta1.UpdateStrategyRecreatePod
	case workloadtypes.UpdateStrategyInPlaceIfPossible:
		return v1beta1.UpdateStrategyInPlaceIfPossible
	case workloadtypes.UpdateStrategyInPlaceOnly:
		return v1beta1.UpdateStrategyInPlaceOnly
	default:
		return v1beta1.UpdateStrategyType("")
	}
}

// InstanceOperationTypeToWorkload converts a v1beta1.InstanceOperationType
// to a workload.InstanceOperationType. Unknown values map to the empty
// string so adapters tolerate API-version skew without panicking.
func InstanceOperationTypeToWorkload(v v1beta1.InstanceOperationType) workloadtypes.InstanceOperationType {
	switch v {
	case v1beta1.InstanceOperationCreate:
		return workloadtypes.InstanceOperationCreate
	case v1beta1.InstanceOperationUpdate:
		return workloadtypes.InstanceOperationUpdate
	case v1beta1.InstanceOperationRestart:
		return workloadtypes.InstanceOperationRestart
	case v1beta1.InstanceOperationMigrate:
		return workloadtypes.InstanceOperationMigrate
	case v1beta1.InstanceOperationDelete:
		return workloadtypes.InstanceOperationDelete
	default:
		return workloadtypes.InstanceOperationType("")
	}
}

// InstanceOperationTypeFromWorkload converts a workload.InstanceOperationType
// to a v1beta1.InstanceOperationType. Unknown values map to the empty
// string so adapters tolerate API-version skew without panicking.
func InstanceOperationTypeFromWorkload(w workloadtypes.InstanceOperationType) v1beta1.InstanceOperationType {
	switch w {
	case workloadtypes.InstanceOperationCreate:
		return v1beta1.InstanceOperationCreate
	case workloadtypes.InstanceOperationUpdate:
		return v1beta1.InstanceOperationUpdate
	case workloadtypes.InstanceOperationRestart:
		return v1beta1.InstanceOperationRestart
	case workloadtypes.InstanceOperationMigrate:
		return v1beta1.InstanceOperationMigrate
	case workloadtypes.InstanceOperationDelete:
		return v1beta1.InstanceOperationDelete
	default:
		return v1beta1.InstanceOperationType("")
	}
}

// InstanceOperationToWorkload converts a *v1beta1.InstanceOperation to
// a *workload.InstanceOperation. Returns nil if v is nil. SurgeIndex is
// copied by value (the returned struct allocates a fresh *int32 so
// callers can mutate it independently). HintTargetNodes is copied
// element-by-element into a freshly allocated slice for the same reason.
func InstanceOperationToWorkload(v *v1beta1.InstanceOperation) *workloadtypes.InstanceOperation {
	if v == nil {
		return nil
	}
	out := &workloadtypes.InstanceOperation{
		ID:             v.ID,
		Type:           InstanceOperationTypeToWorkload(v.Type),
		Step:           v.Step,
		StartedAt:      v.StartedAt,
		LastProgressAt: v.LastProgressAt,
		Deadline:       v.Deadline,
		RetryCount:     v.RetryCount,
		TargetRevision: v.TargetRevision,
		Reason:         v.Reason,
		Waiting:        v.Waiting,
		Strategy:       UpdateStrategyTypeToWorkload(v1beta1.UpdateStrategyType(v.Strategy)),
		FromNode:       v.FromNode,
		RequestUUID:    v.RequestUUID,
	}
	if v.CapacityRefusedAt != nil {
		at := *v.CapacityRefusedAt
		out.CapacityRefusedAt = &at
	}
	if v.SurgeIndex != nil {
		s := *v.SurgeIndex
		out.SurgeIndex = &s
	}
	if v.HintTargetNodes != nil {
		out.HintTargetNodes = append([]string(nil), v.HintTargetNodes...)
	}
	return out
}

// InstanceOperationFromWorkload converts a *workload.InstanceOperation
// to a *v1beta1.InstanceOperation. Returns nil if w is nil. Pointer and
// slice fields are deep-copied for the same isolation reasons as
// InstanceOperationToWorkload.
func InstanceOperationFromWorkload(w *workloadtypes.InstanceOperation) *v1beta1.InstanceOperation {
	if w == nil {
		return nil
	}
	out := &v1beta1.InstanceOperation{
		ID:             w.ID,
		Type:           InstanceOperationTypeFromWorkload(w.Type),
		Step:           w.Step,
		StartedAt:      w.StartedAt,
		LastProgressAt: w.LastProgressAt,
		Deadline:       w.Deadline,
		RetryCount:     w.RetryCount,
		TargetRevision: w.TargetRevision,
		Reason:         w.Reason,
		Waiting:        w.Waiting,
		Strategy:       string(UpdateStrategyTypeFromWorkload(w.Strategy)),
		FromNode:       w.FromNode,
		RequestUUID:    w.RequestUUID,
	}
	if w.CapacityRefusedAt != nil {
		at := *w.CapacityRefusedAt
		out.CapacityRefusedAt = &at
	}
	if w.SurgeIndex != nil {
		s := *w.SurgeIndex
		out.SurgeIndex = &s
	}
	if w.HintTargetNodes != nil {
		out.HintTargetNodes = append([]string(nil), w.HintTargetNodes...)
	}
	return out
}

// InstanceTerminationToWorkload converts a *v1beta1.InstanceTermination
// to a *workload.InstanceTermination. Returns nil if v is nil. ExitCode is
// copied by value (the returned struct allocates a fresh *int32) so callers
// can mutate it independently.
func InstanceTerminationToWorkload(v *v1beta1.InstanceTermination) *workloadtypes.InstanceTermination {
	if v == nil {
		return nil
	}
	out := &workloadtypes.InstanceTermination{
		PodName:       v.PodName,
		ContainerName: v.ContainerName,
		Reason:        v.Reason,
		Message:       v.Message,
		Time:          v.Time,
	}
	if v.ExitCode != nil {
		e := *v.ExitCode
		out.ExitCode = &e
	}
	return out
}

// InstanceTerminationFromWorkload converts a *workload.InstanceTermination
// to a *v1beta1.InstanceTermination. Returns nil if w is nil. ExitCode is
// deep-copied for the same isolation reason as InstanceTerminationToWorkload.
func InstanceTerminationFromWorkload(w *workloadtypes.InstanceTermination) *v1beta1.InstanceTermination {
	if w == nil {
		return nil
	}
	out := &v1beta1.InstanceTermination{
		PodName:       w.PodName,
		ContainerName: w.ContainerName,
		Reason:        w.Reason,
		Message:       w.Message,
		Time:          w.Time,
	}
	if w.ExitCode != nil {
		e := *w.ExitCode
		out.ExitCode = &e
	}
	return out
}

// InstanceStatusToWorkload converts a v1beta1.OMENativeInstanceStatus
// to a workload.InstanceStatus. NodesOccupied and Conditions slices
// are deep-copied so the returned struct can be mutated independently
// of the source. Operation is allocated via InstanceOperationToWorkload;
// LastFailure via InstanceTerminationToWorkload.
func InstanceStatusToWorkload(v v1beta1.OMENativeInstanceStatus) workloadtypes.InstanceStatus {
	out := workloadtypes.InstanceStatus{
		Index:             v.Index,
		Incarnation:       v.Incarnation,
		Phase:             InstancePhaseToWorkload(v.Phase),
		RunningRevision:   v.RunningRevision,
		TargetRevision:    v.TargetRevision,
		PodCount:          v.PodCount,
		ReadyPodCount:     v.ReadyPodCount,
		ServingPodCount:   v.ServingPodCount,
		AvailablePodCount: v.AvailablePodCount,
		ScheduledPodCount: v.ScheduledPodCount,
		Admitted:          v.Admitted,
		ActiveOrdinal:     v.ActiveOrdinal,
		ReadySince:        v.ReadySince.DeepCopy(),
		Operation:         InstanceOperationToWorkload(v.Operation),
		LastFailure:       InstanceTerminationToWorkload(v.LastFailure),
	}
	if v.NodesOccupied != nil {
		out.NodesOccupied = append([]string(nil), v.NodesOccupied...)
	}
	if v.Conditions != nil {
		out.Conditions = append(out.Conditions, v.Conditions...)
	}
	if v.Announced != nil {
		out.Announced = append([]string(nil), v.Announced...)
	}
	return out
}

// InstanceStatusFromWorkload converts a workload.InstanceStatus to a
// v1beta1.OMENativeInstanceStatus. Slices and Operation pointer are
// deep-copied for the same isolation reasons as
// InstanceStatusToWorkload.
func InstanceStatusFromWorkload(w workloadtypes.InstanceStatus) v1beta1.OMENativeInstanceStatus {
	out := v1beta1.OMENativeInstanceStatus{
		Index:             w.Index,
		Incarnation:       w.Incarnation,
		Phase:             InstancePhaseFromWorkload(w.Phase),
		RunningRevision:   w.RunningRevision,
		TargetRevision:    w.TargetRevision,
		PodCount:          w.PodCount,
		ReadyPodCount:     w.ReadyPodCount,
		ServingPodCount:   w.ServingPodCount,
		AvailablePodCount: w.AvailablePodCount,
		ScheduledPodCount: w.ScheduledPodCount,
		Admitted:          w.Admitted,
		ActiveOrdinal:     w.ActiveOrdinal,
		Operation:         InstanceOperationFromWorkload(w.Operation),
		ReadySince:        w.ReadySince.DeepCopy(),
		LastFailure:       InstanceTerminationFromWorkload(w.LastFailure),
	}
	if w.NodesOccupied != nil {
		out.NodesOccupied = append([]string(nil), w.NodesOccupied...)
	}
	if w.Conditions != nil {
		out.Conditions = append(out.Conditions, w.Conditions...)
	}
	if w.Announced != nil {
		out.Announced = append([]string(nil), w.Announced...)
	}
	return out
}

// InstanceStatusSliceToWorkload converts a slice of
// v1beta1.OMENativeInstanceStatus to a slice of workload.InstanceStatus.
// Returns nil when in is nil so the round-trip preserves nilness.
func InstanceStatusSliceToWorkload(in []v1beta1.OMENativeInstanceStatus) []workloadtypes.InstanceStatus {
	if in == nil {
		return nil
	}
	out := make([]workloadtypes.InstanceStatus, len(in))
	for i := range in {
		out[i] = InstanceStatusToWorkload(in[i])
	}
	return out
}

// InstanceStatusSliceFromWorkload converts a slice of
// workload.InstanceStatus to a slice of v1beta1.OMENativeInstanceStatus.
// Returns nil when in is nil so the round-trip preserves nilness.
func InstanceStatusSliceFromWorkload(in []workloadtypes.InstanceStatus) []v1beta1.OMENativeInstanceStatus {
	if in == nil {
		return nil
	}
	out := make([]v1beta1.OMENativeInstanceStatus, len(in))
	for i := range in {
		out[i] = InstanceStatusFromWorkload(in[i])
	}
	return out
}

// InstanceRestartPolicyToWorkload converts a v1beta1.InstanceRestartPolicy
// to a workload.RestartPolicy. Unknown values map to the empty policy
// so adapters tolerate API-version skew without panicking.
func InstanceRestartPolicyToWorkload(v v1beta1.InstanceRestartPolicy) workloadtypes.RestartPolicy {
	switch v {
	case v1beta1.InstanceRestartPolicyNone:
		return workloadtypes.RestartPolicyNone
	case v1beta1.InstanceRestartPolicyRecreateInstance:
		return workloadtypes.RestartPolicyRecreateInstance
	default:
		return workloadtypes.RestartPolicy("")
	}
}

// InstanceReadyPolicyToWorkload converts a v1beta1.InstanceReadyPolicy
// to a workload.InstanceReadyPolicy. Unknown values map to the empty
// policy so adapters tolerate API-version skew without panicking.
func InstanceReadyPolicyToWorkload(v v1beta1.InstanceReadyPolicy) workloadtypes.InstanceReadyPolicy {
	switch v {
	case v1beta1.InstanceReadyPolicyAllPodReady:
		return workloadtypes.InstanceReadyPolicyAllPodReady
	case v1beta1.InstanceReadyPolicyNone:
		return workloadtypes.InstanceReadyPolicyNone
	default:
		return workloadtypes.InstanceReadyPolicy("")
	}
}

// MigrationModeToWorkload converts a v1beta1.MigrationPolicyMode to a
// workload.MigrationMode. Unknown values map to the empty mode so
// adapters tolerate API-version skew without panicking.
func MigrationModeToWorkload(v v1beta1.MigrationPolicyMode) workloadtypes.MigrationMode {
	switch v {
	case v1beta1.MigrationPolicyModeAuto:
		return workloadtypes.MigrationModeAuto
	case v1beta1.MigrationPolicyModeSurge:
		return workloadtypes.MigrationModeSurge
	case v1beta1.MigrationPolicyModeNever:
		return workloadtypes.MigrationModeNever
	default:
		return workloadtypes.MigrationMode("")
	}
}

// UpdateStrategyToWorkload converts a v1beta1.UpdateStrategy to a
// workload.UpdateStrategy. The nested strategy pointers are deep-copied
// so the returned struct can be mutated independently of the source.
func UpdateStrategyToWorkload(v v1beta1.UpdateStrategy) workloadtypes.UpdateStrategy {
	out := workloadtypes.UpdateStrategy{
		Type: UpdateStrategyTypeToWorkload(v.Type),
	}
	if v.InPlaceUpdateStrategy != nil {
		out.InPlaceUpdateStrategy = &workloadtypes.InPlaceUpdateStrategy{}
		if v.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle != nil {
			m := *v.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle
			out.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle = &m
		}
	}
	if v.RollingUpdate != nil {
		out.RollingUpdate = &workloadtypes.RollingUpdate{}
		if v.RollingUpdate.Partition != nil {
			p := *v.RollingUpdate.Partition
			out.RollingUpdate.Partition = &p
		}
		if v.RollingUpdate.MaxUnavailable != nil {
			m := *v.RollingUpdate.MaxUnavailable
			out.RollingUpdate.MaxUnavailable = &m
		}
		if v.RollingUpdate.MaxSurge != nil {
			s := *v.RollingUpdate.MaxSurge
			out.RollingUpdate.MaxSurge = &s
		}
	}
	return out
}

// LifecycleSpecToWorkload converts a v1beta1.LifecycleSpec to a
// workload.Lifecycle. Nested pointers are deep-copied so the returned
// struct can be mutated independently of the source. Use this at the
// adapter boundary so the workload package itself stays free of
// v1beta1 imports.
func LifecycleSpecToWorkload(v v1beta1.LifecycleSpec) workloadtypes.Lifecycle {
	out := workloadtypes.Lifecycle{}
	if v.RestartPolicy != nil {
		p := InstanceRestartPolicyToWorkload(*v.RestartPolicy)
		out.RestartPolicy = &p
	}
	if v.UpdateStrategy != nil {
		us := UpdateStrategyToWorkload(*v.UpdateStrategy)
		out.UpdateStrategy = &us
	}
	if v.ReadyPolicy != nil {
		p := InstanceReadyPolicyToWorkload(*v.ReadyPolicy)
		out.ReadyPolicy = &p
	}
	if v.InstanceReadyTimeout != nil {
		d := *v.InstanceReadyTimeout
		out.InstanceReadyTimeout = &d
	}
	if v.MigrationPolicy != nil {
		out.MigrationPolicy = &workloadtypes.MigrationPolicy{
			Mode: MigrationModeToWorkload(v.MigrationPolicy.Mode),
		}
	}
	return out
}
