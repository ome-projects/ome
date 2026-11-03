// Package v1beta1convert copies values between the v1beta1 API types the
// controllers read and write and the types the workload engine computes
// with. It is the one seam where the two type sets meet: the engine never
// imports pkg/apis/ome/v1beta1, and the adapters (the InferenceService
// OMENative dispatch and the InferenceReplica controller) call these
// functions at the boundary in both directions.
//
// Each enum is one table that pairs every API value with the workload
// value of the same meaning, and both directions read from that table. A
// value outside its table reads as the empty value, so a controller fed a
// newer API version carries on without a panic. The tests round-trip every
// pair and hold every constant declared on either side in its table.
package v1beta1convert

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// enumTable pairs the values of one API enum with the workload values of
// the same meaning and answers in both directions. A value outside the
// table reads as the empty value either way.
type enumTable[API ~string, Workload ~string] struct {
	workloadOf map[API]Workload
	apiOf      map[Workload]API
}

// newEnumTable builds a table from its API-to-workload pairs and derives
// the reverse direction once.
func newEnumTable[API ~string, Workload ~string](workloadOf map[API]Workload) enumTable[API, Workload] {
	apiOf := make(map[Workload]API, len(workloadOf))
	for api, workload := range workloadOf {
		apiOf[workload] = api
	}
	return enumTable[API, Workload]{workloadOf: workloadOf, apiOf: apiOf}
}

func (t enumTable[API, Workload]) toWorkload(v API) Workload { return t.workloadOf[v] }

func (t enumTable[API, Workload]) fromWorkload(w Workload) API { return t.apiOf[w] }

var componentTypes = newEnumTable(map[v1beta1.ComponentType]workloadtypes.ComponentType{
	v1beta1.RouterComponent:  workloadtypes.ComponentRouter,
	v1beta1.EngineComponent:  workloadtypes.ComponentEngine,
	v1beta1.DecoderComponent: workloadtypes.ComponentDecoder,
	v1beta1.EncoderComponent: workloadtypes.ComponentEncoder,
})

// instancePhases lists the empty phase too: a row allocated before its
// first observation carries it on both sides.
var instancePhases = newEnumTable(map[v1beta1.OMENativeInstancePhase]workloadtypes.InstancePhase{
	"":                                  workloadtypes.InstancePhaseEmpty,
	v1beta1.OMENativeInstancePending:    workloadtypes.InstancePhasePending,
	v1beta1.OMENativeInstanceCreating:   workloadtypes.InstancePhaseCreating,
	v1beta1.OMENativeInstanceReady:      workloadtypes.InstancePhaseReady,
	v1beta1.OMENativeInstanceUpdating:   workloadtypes.InstancePhaseUpdating,
	v1beta1.OMENativeInstanceRestarting: workloadtypes.InstancePhaseRestarting,
	v1beta1.OMENativeInstanceMigrating:  workloadtypes.InstancePhaseMigrating,
	v1beta1.OMENativeInstanceFailed:     workloadtypes.InstancePhaseFailed,
	v1beta1.OMENativeInstanceDeleting:   workloadtypes.InstancePhaseDeleting,
})

var updateStrategyTypes = newEnumTable(map[v1beta1.UpdateStrategyType]workloadtypes.UpdateStrategyType{
	v1beta1.UpdateStrategySurgeThenDrain:    workloadtypes.UpdateStrategySurgeThenDrain,
	v1beta1.UpdateStrategyRecreatePod:       workloadtypes.UpdateStrategyRecreatePod,
	v1beta1.UpdateStrategyInPlaceIfPossible: workloadtypes.UpdateStrategyInPlaceIfPossible,
	v1beta1.UpdateStrategyInPlaceOnly:       workloadtypes.UpdateStrategyInPlaceOnly,
})

var instanceOperationTypes = newEnumTable(map[v1beta1.InstanceOperationType]workloadtypes.InstanceOperationType{
	v1beta1.InstanceOperationCreate:  workloadtypes.InstanceOperationCreate,
	v1beta1.InstanceOperationUpdate:  workloadtypes.InstanceOperationUpdate,
	v1beta1.InstanceOperationRestart: workloadtypes.InstanceOperationRestart,
	v1beta1.InstanceOperationMigrate: workloadtypes.InstanceOperationMigrate,
	v1beta1.InstanceOperationDelete:  workloadtypes.InstanceOperationDelete,
})

var instanceRestartPolicies = newEnumTable(map[v1beta1.InstanceRestartPolicy]workloadtypes.RestartPolicy{
	v1beta1.InstanceRestartPolicyNone:             workloadtypes.RestartPolicyNone,
	v1beta1.InstanceRestartPolicyRecreateInstance: workloadtypes.RestartPolicyRecreateInstance,
})

var instanceReadyPolicies = newEnumTable(map[v1beta1.InstanceReadyPolicy]workloadtypes.InstanceReadyPolicy{
	v1beta1.InstanceReadyPolicyAllPodReady: workloadtypes.InstanceReadyPolicyAllPodReady,
	v1beta1.InstanceReadyPolicyNone:        workloadtypes.InstanceReadyPolicyNone,
})

var migrationModes = newEnumTable(map[v1beta1.MigrationPolicyMode]workloadtypes.MigrationMode{
	v1beta1.MigrationPolicyModeAuto:  workloadtypes.MigrationModeAuto,
	v1beta1.MigrationPolicyModeSurge: workloadtypes.MigrationModeSurge,
	v1beta1.MigrationPolicyModeNever: workloadtypes.MigrationModeNever,
})

// ComponentTypeToWorkload reads the workload component type of an API one.
func ComponentTypeToWorkload(v v1beta1.ComponentType) workloadtypes.ComponentType {
	return componentTypes.toWorkload(v)
}

// ComponentTypeFromWorkload reads the API component type of a workload one.
func ComponentTypeFromWorkload(w workloadtypes.ComponentType) v1beta1.ComponentType {
	return componentTypes.fromWorkload(w)
}

// InstancePhaseToWorkload reads the workload phase of an API one.
func InstancePhaseToWorkload(v v1beta1.OMENativeInstancePhase) workloadtypes.InstancePhase {
	return instancePhases.toWorkload(v)
}

// InstancePhaseFromWorkload reads the API phase of a workload one.
func InstancePhaseFromWorkload(w workloadtypes.InstancePhase) v1beta1.OMENativeInstancePhase {
	return instancePhases.fromWorkload(w)
}

// UpdateStrategyTypeToWorkload reads the workload strategy of an API one.
// The engine compares strategies against its own constants only, so this
// is the one place an API strategy enters it.
func UpdateStrategyTypeToWorkload(v v1beta1.UpdateStrategyType) workloadtypes.UpdateStrategyType {
	return updateStrategyTypes.toWorkload(v)
}

// UpdateStrategyTypeFromWorkload reads the API strategy of a workload one.
func UpdateStrategyTypeFromWorkload(w workloadtypes.UpdateStrategyType) v1beta1.UpdateStrategyType {
	return updateStrategyTypes.fromWorkload(w)
}

// InstanceOperationTypeToWorkload reads the workload operation type of an
// API one.
func InstanceOperationTypeToWorkload(v v1beta1.InstanceOperationType) workloadtypes.InstanceOperationType {
	return instanceOperationTypes.toWorkload(v)
}

// InstanceOperationTypeFromWorkload reads the API operation type of a
// workload one.
func InstanceOperationTypeFromWorkload(w workloadtypes.InstanceOperationType) v1beta1.InstanceOperationType {
	return instanceOperationTypes.fromWorkload(w)
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
	if v.PlacementWaitSince != nil {
		at := *v.PlacementWaitSince
		out.PlacementWaitSince = &at
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
	if w.PlacementWaitSince != nil {
		at := *w.PlacementWaitSince
		out.PlacementWaitSince = &at
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
//
// The phase is read as the engine holds it. The status publication writes
// Restarting over a settled Ready row whose pod set serves nothing; no
// operation owns such a row, and the engine's state of it is Ready. It
// writes Ready over a Failed row that still runs a revision and records an
// attempt at another while the pods it runs serve, whether the attempt is
// spent or the row still keeps it at its surge step; no promote leaves a
// Ready row off its running revision and no writer leaves Ready beside an
// operation, so both shapes are the publication's and the engine's state
// of them is Failed (workloadtypes.PublishedReadyOffRunningRevision,
// workloadtypes.PublishedReadyKeepingSurgeOffRunningRevision).
func InstanceStatusToWorkload(v v1beta1.OMENativeInstanceStatus) workloadtypes.InstanceStatus {
	phase := InstancePhaseToWorkload(v.Phase)
	if phase == workloadtypes.InstancePhaseRestarting && v.Operation == nil {
		phase = workloadtypes.InstancePhaseReady
	}
	operation := InstanceOperationToWorkload(v.Operation)
	published := &workloadtypes.InstanceStatus{
		Phase: phase, RunningRevision: v.RunningRevision, TargetRevision: v.TargetRevision, Operation: operation}
	if workloadtypes.PublishedReadyOffRunningRevision(published) || workloadtypes.PublishedReadyKeepingSurgeOffRunningRevision(published) {
		phase = workloadtypes.InstancePhaseFailed
	}
	out := workloadtypes.InstanceStatus{
		Index:             v.Index,
		Incarnation:       v.Incarnation,
		Phase:             phase,
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
		Operation:         operation,
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

// InstanceRestartPolicyToWorkload reads the workload restart policy of an
// API one.
func InstanceRestartPolicyToWorkload(v v1beta1.InstanceRestartPolicy) workloadtypes.RestartPolicy {
	return instanceRestartPolicies.toWorkload(v)
}

// InstanceReadyPolicyToWorkload reads the workload ready policy of an API
// one.
func InstanceReadyPolicyToWorkload(v v1beta1.InstanceReadyPolicy) workloadtypes.InstanceReadyPolicy {
	return instanceReadyPolicies.toWorkload(v)
}

// MigrationModeToWorkload reads the workload migration mode of an API one.
func MigrationModeToWorkload(v v1beta1.MigrationPolicyMode) workloadtypes.MigrationMode {
	return migrationModes.toWorkload(v)
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
