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
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
)

// ComponentTypeToWorkload converts a v1beta1.ComponentType to a
// workload.ComponentType. Unknown values map to the empty string so
// adapters tolerate API-version skew without panicking.
func ComponentTypeToWorkload(v v1beta1.ComponentType) workload.ComponentType {
	switch v {
	case v1beta1.RouterComponent:
		return workload.ComponentRouter
	case v1beta1.EngineComponent:
		return workload.ComponentEngine
	case v1beta1.DecoderComponent:
		return workload.ComponentDecoder
	default:
		return workload.ComponentType("")
	}
}

// ComponentTypeFromWorkload converts a workload.ComponentType to a
// v1beta1.ComponentType. Unknown values map to the empty string so
// adapters tolerate API-version skew without panicking.
func ComponentTypeFromWorkload(w workload.ComponentType) v1beta1.ComponentType {
	switch w {
	case workload.ComponentRouter:
		return v1beta1.RouterComponent
	case workload.ComponentEngine:
		return v1beta1.EngineComponent
	case workload.ComponentDecoder:
		return v1beta1.DecoderComponent
	default:
		return v1beta1.ComponentType("")
	}
}

// InstancePhaseToWorkload converts a v1beta1.OMENativeInstancePhase to
// a workload.InstancePhase. Unknown values map to the empty phase so
// adapters tolerate API-version skew without panicking.
func InstancePhaseToWorkload(v v1beta1.OMENativeInstancePhase) workload.InstancePhase {
	switch v {
	case "":
		return workload.InstancePhaseEmpty
	case v1beta1.OMENativeInstancePending:
		return workload.InstancePhasePending
	case v1beta1.OMENativeInstanceCreating:
		return workload.InstancePhaseCreating
	case v1beta1.OMENativeInstanceReady:
		return workload.InstancePhaseReady
	case v1beta1.OMENativeInstanceUpdating:
		return workload.InstancePhaseUpdating
	case v1beta1.OMENativeInstanceRestarting:
		return workload.InstancePhaseRestarting
	case v1beta1.OMENativeInstanceMigrating:
		return workload.InstancePhaseMigrating
	case v1beta1.OMENativeInstanceFailed:
		return workload.InstancePhaseFailed
	case v1beta1.OMENativeInstanceDeleting:
		return workload.InstancePhaseDeleting
	default:
		return workload.InstancePhaseEmpty
	}
}

// InstancePhaseFromWorkload converts a workload.InstancePhase to a
// v1beta1.OMENativeInstancePhase. Unknown values map to the empty
// phase so adapters tolerate API-version skew without panicking.
func InstancePhaseFromWorkload(w workload.InstancePhase) v1beta1.OMENativeInstancePhase {
	switch w {
	case workload.InstancePhaseEmpty:
		return v1beta1.OMENativeInstancePhase("")
	case workload.InstancePhasePending:
		return v1beta1.OMENativeInstancePending
	case workload.InstancePhaseCreating:
		return v1beta1.OMENativeInstanceCreating
	case workload.InstancePhaseReady:
		return v1beta1.OMENativeInstanceReady
	case workload.InstancePhaseUpdating:
		return v1beta1.OMENativeInstanceUpdating
	case workload.InstancePhaseRestarting:
		return v1beta1.OMENativeInstanceRestarting
	case workload.InstancePhaseMigrating:
		return v1beta1.OMENativeInstanceMigrating
	case workload.InstancePhaseFailed:
		return v1beta1.OMENativeInstanceFailed
	case workload.InstancePhaseDeleting:
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
func UpdateStrategyTypeToWorkload(v v1beta1.UpdateStrategyType) workload.UpdateStrategyType {
	switch v {
	case v1beta1.UpdateStrategySurgeThenDrain:
		return workload.UpdateStrategySurgeThenDrain
	case v1beta1.UpdateStrategyRecreatePod:
		return workload.UpdateStrategyRecreatePod
	case v1beta1.UpdateStrategyInPlaceIfPossible:
		return workload.UpdateStrategyInPlaceIfPossible
	case v1beta1.UpdateStrategyInPlaceOnly:
		return workload.UpdateStrategyInPlaceOnly
	default:
		return workload.UpdateStrategyType("")
	}
}

// UpdateStrategyTypeFromWorkload converts a workload.UpdateStrategyType
// to a v1beta1.UpdateStrategyType. Unknown values map to the
// empty string so adapters tolerate API-version skew without panicking.
func UpdateStrategyTypeFromWorkload(w workload.UpdateStrategyType) v1beta1.UpdateStrategyType {
	switch w {
	case workload.UpdateStrategySurgeThenDrain:
		return v1beta1.UpdateStrategySurgeThenDrain
	case workload.UpdateStrategyRecreatePod:
		return v1beta1.UpdateStrategyRecreatePod
	case workload.UpdateStrategyInPlaceIfPossible:
		return v1beta1.UpdateStrategyInPlaceIfPossible
	case workload.UpdateStrategyInPlaceOnly:
		return v1beta1.UpdateStrategyInPlaceOnly
	default:
		return v1beta1.UpdateStrategyType("")
	}
}

// InstanceOperationTypeToWorkload converts a v1beta1.InstanceOperationType
// to a workload.InstanceOperationType. Unknown values map to the empty
// string so adapters tolerate API-version skew without panicking.
func InstanceOperationTypeToWorkload(v v1beta1.InstanceOperationType) workload.InstanceOperationType {
	switch v {
	case v1beta1.InstanceOperationCreate:
		return workload.InstanceOperationCreate
	case v1beta1.InstanceOperationUpdate:
		return workload.InstanceOperationUpdate
	case v1beta1.InstanceOperationRestart:
		return workload.InstanceOperationRestart
	case v1beta1.InstanceOperationMigrate:
		return workload.InstanceOperationMigrate
	case v1beta1.InstanceOperationDelete:
		return workload.InstanceOperationDelete
	default:
		return workload.InstanceOperationType("")
	}
}

// InstanceOperationTypeFromWorkload converts a workload.InstanceOperationType
// to a v1beta1.InstanceOperationType. Unknown values map to the empty
// string so adapters tolerate API-version skew without panicking.
func InstanceOperationTypeFromWorkload(w workload.InstanceOperationType) v1beta1.InstanceOperationType {
	switch w {
	case workload.InstanceOperationCreate:
		return v1beta1.InstanceOperationCreate
	case workload.InstanceOperationUpdate:
		return v1beta1.InstanceOperationUpdate
	case workload.InstanceOperationRestart:
		return v1beta1.InstanceOperationRestart
	case workload.InstanceOperationMigrate:
		return v1beta1.InstanceOperationMigrate
	case workload.InstanceOperationDelete:
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
func InstanceOperationToWorkload(v *v1beta1.InstanceOperation) *workload.InstanceOperation {
	if v == nil {
		return nil
	}
	out := &workload.InstanceOperation{
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
		FromNode:       v.FromNode,
		RequestUUID:    v.RequestUUID,
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
func InstanceOperationFromWorkload(w *workload.InstanceOperation) *v1beta1.InstanceOperation {
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
		FromNode:       w.FromNode,
		RequestUUID:    w.RequestUUID,
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
func InstanceTerminationToWorkload(v *v1beta1.InstanceTermination) *workload.InstanceTermination {
	if v == nil {
		return nil
	}
	out := &workload.InstanceTermination{
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
func InstanceTerminationFromWorkload(w *workload.InstanceTermination) *v1beta1.InstanceTermination {
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
func InstanceStatusToWorkload(v v1beta1.OMENativeInstanceStatus) workload.InstanceStatus {
	out := workload.InstanceStatus{
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
	return out
}

// InstanceStatusFromWorkload converts a workload.InstanceStatus to a
// v1beta1.OMENativeInstanceStatus. Slices and Operation pointer are
// deep-copied for the same isolation reasons as
// InstanceStatusToWorkload.
func InstanceStatusFromWorkload(w workload.InstanceStatus) v1beta1.OMENativeInstanceStatus {
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
	return out
}

// InstanceStatusSliceToWorkload converts a slice of
// v1beta1.OMENativeInstanceStatus to a slice of workload.InstanceStatus.
// Returns nil when in is nil so the round-trip preserves nilness.
func InstanceStatusSliceToWorkload(in []v1beta1.OMENativeInstanceStatus) []workload.InstanceStatus {
	if in == nil {
		return nil
	}
	out := make([]workload.InstanceStatus, len(in))
	for i := range in {
		out[i] = InstanceStatusToWorkload(in[i])
	}
	return out
}

// InstanceStatusSliceFromWorkload converts a slice of
// workload.InstanceStatus to a slice of v1beta1.OMENativeInstanceStatus.
// Returns nil when in is nil so the round-trip preserves nilness.
func InstanceStatusSliceFromWorkload(in []workload.InstanceStatus) []v1beta1.OMENativeInstanceStatus {
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
func InstanceRestartPolicyToWorkload(v v1beta1.InstanceRestartPolicy) workload.RestartPolicy {
	switch v {
	case v1beta1.InstanceRestartPolicyNone:
		return workload.RestartPolicyNone
	case v1beta1.InstanceRestartPolicyRecreateInstance:
		return workload.RestartPolicyRecreateInstance
	default:
		return workload.RestartPolicy("")
	}
}

// InstanceReadyPolicyToWorkload converts a v1beta1.InstanceReadyPolicy
// to a workload.InstanceReadyPolicy. Unknown values map to the empty
// policy so adapters tolerate API-version skew without panicking.
func InstanceReadyPolicyToWorkload(v v1beta1.InstanceReadyPolicy) workload.InstanceReadyPolicy {
	switch v {
	case v1beta1.InstanceReadyPolicyAllPodReady:
		return workload.InstanceReadyPolicyAllPodReady
	case v1beta1.InstanceReadyPolicyNone:
		return workload.InstanceReadyPolicyNone
	default:
		return workload.InstanceReadyPolicy("")
	}
}

// MigrationModeToWorkload converts a v1beta1.MigrationPolicyMode to a
// workload.MigrationMode. Unknown values map to the empty mode so
// adapters tolerate API-version skew without panicking.
func MigrationModeToWorkload(v v1beta1.MigrationPolicyMode) workload.MigrationMode {
	switch v {
	case v1beta1.MigrationPolicyModeAuto:
		return workload.MigrationModeAuto
	case v1beta1.MigrationPolicyModeSurge:
		return workload.MigrationModeSurge
	case v1beta1.MigrationPolicyModeNever:
		return workload.MigrationModeNever
	default:
		return workload.MigrationMode("")
	}
}

// UpdateStrategyToWorkload converts a v1beta1.UpdateStrategy to a
// workload.UpdateStrategy. The nested InPlaceUpdateStrategy and
// RollingUpdate pointers are deep-copied so the returned struct can
// be mutated independently of the source.
func UpdateStrategyToWorkload(v v1beta1.UpdateStrategy) workload.UpdateStrategy {
	out := workload.UpdateStrategy{
		Type: UpdateStrategyTypeToWorkload(v.Type),
	}
	if v.InPlaceUpdateStrategy != nil {
		out.InPlaceUpdateStrategy = &workload.InPlaceUpdateStrategy{}
		if v.InPlaceUpdateStrategy.GracePeriodSeconds != nil {
			g := *v.InPlaceUpdateStrategy.GracePeriodSeconds
			out.InPlaceUpdateStrategy.GracePeriodSeconds = &g
		}
		if v.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle != nil {
			m := *v.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle
			out.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle = &m
		}
	}
	if v.RollingUpdate != nil {
		out.RollingUpdate = &workload.RollingUpdate{}
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
func LifecycleSpecToWorkload(v v1beta1.LifecycleSpec) workload.Lifecycle {
	out := workload.Lifecycle{}
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
		out.MigrationPolicy = &workload.MigrationPolicy{
			Mode: MigrationModeToWorkload(v.MigrationPolicy.Mode),
		}
	}
	return out
}
