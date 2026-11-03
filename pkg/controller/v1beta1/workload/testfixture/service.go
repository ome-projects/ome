package testfixture

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// MinimalInferenceService is an InferenceService with the least an engine
// pass needs: a name, a namespace, a UID derived from the name, and an
// engine with the given minimum replica count.
func MinimalInferenceService(name, namespace string, replicas int) *v1beta1.InferenceService {
	minReplicas := replicas
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       k8stypes.UID(name + "-uid"),
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &minReplicas,
				},
			},
		},
	}
}

// InferenceReplicaName is the name of the InferenceReplica that carries a
// component's Instance rows: the service name, a dash, the component.
func InferenceReplicaName(isvc *v1beta1.InferenceService, component types.ComponentType) string {
	return isvc.Name + "-" + string(component)
}

// InferenceReplica is the replica object of (service, component) carrying
// the given Instance rows. The replica is where the rows live; a fixture
// seeds it into the fake client beside the service.
func InferenceReplica(isvc *v1beta1.InferenceService, component types.ComponentType, instances ...v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: isvc.Namespace,
			Name:      InferenceReplicaName(isvc, component),
		},
		Status: v1beta1.InferenceReplicaStatus{InstanceStatuses: instances},
	}
}

// ServiceWithReadyInstance is a service of one engine Instance plus the
// replica on which Instance 0 is Ready at the given recreation count: the
// resting state an update starts from. Both go into the fake client.
func ServiceWithReadyInstance(name, namespace string, recreation int64) (*v1beta1.InferenceService, *v1beta1.InferenceReplica) {
	isvc := MinimalInferenceService(name, namespace, 1)
	ir := InferenceReplica(isvc, types.ComponentEngine,
		v1beta1.OMENativeInstanceStatus{Index: 0, Incarnation: recreation, Phase: v1beta1.OMENativeInstanceReady},
	)
	return isvc, ir
}

// StoredInstanceStatuses re-reads the replica and returns the Instance rows
// it stores, the copy an assertion checks; nil when the replica does not
// exist.
func StoredInstanceStatuses(c client.Client, isvc *v1beta1.InferenceService, component types.ComponentType) []v1beta1.OMENativeInstanceStatus {
	ir := &v1beta1.InferenceReplica{}
	key := k8stypes.NamespacedName{Namespace: isvc.Namespace, Name: InferenceReplicaName(isvc, component)}
	if err := c.Get(context.Background(), key, ir); err != nil {
		return nil
	}
	return ir.Status.InstanceStatuses
}

// InstanceStatuses reads the replica's stored rows as the engine rows a
// pass observes.
func InstanceStatuses(c client.Client, isvc *v1beta1.InferenceService, component types.ComponentType) []types.InstanceStatus {
	var out []types.InstanceStatus
	for _, s := range StoredInstanceStatuses(c, isvc, component) {
		out = append(out, rowFromAPI(s))
	}
	return out
}

// MutateInstanceOnReplica is the row writer of a test's ReconcileInput: it
// reads the replica, applies the engine's change to the one row, and writes
// the replica's status back through the fake client, creating the replica
// on the first write when no fixture seeded one. The fake client has no
// write conflicts, so there is no retry.
func MutateInstanceOnReplica(c client.Client, isvc *v1beta1.InferenceService, component types.ComponentType) func(ctx context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
	return func(ctx context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
		ir := &v1beta1.InferenceReplica{}
		key := k8stypes.NamespacedName{Namespace: isvc.Namespace, Name: InferenceReplicaName(isvc, component)}
		create := false
		if err := c.Get(ctx, key, ir); err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("get IR: %w", err)
			}
			ir = &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}}
			create = true
		}
		pos := -1
		for i, s := range ir.Status.InstanceStatuses {
			if s.Index == idx {
				pos = i
				break
			}
		}
		row := types.InstanceStatus{Index: idx}
		if pos != -1 {
			row = rowFromAPI(ir.Status.InstanceStatuses[pos])
		}
		if !mutate(&row) {
			return nil
		}
		updated := v1beta1.OMENativeInstanceStatus{
			Index:           row.Index,
			Incarnation:     row.Incarnation,
			Phase:           v1beta1.OMENativeInstancePhase(row.Phase),
			RunningRevision: row.RunningRevision,
			TargetRevision:  row.TargetRevision,
			ActiveOrdinal:   row.ActiveOrdinal,
			Operation:       OperationToAPI(row.Operation),
			LastFailure:     TerminationToAPI(row.LastFailure),
		}
		if pos == -1 {
			ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, updated)
		} else {
			ir.Status.InstanceStatuses[pos] = updated
		}
		if create {
			// Create seeds the object; the status subresource is written separately.
			bare := &v1beta1.InferenceReplica{ObjectMeta: ir.ObjectMeta}
			if err := c.Create(ctx, bare); err != nil {
				return fmt.Errorf("create IR: %w", err)
			}
			bare.Status = ir.Status
			ir = bare
		}
		return c.Status().Update(ctx, ir)
	}
}

// RemoveInstanceOnReplica is the row remover of a test's ReconcileInput: it
// drops the row from the replica's status and forgets the Instance's
// create expectation. It reports whether a row was there to remove.
func RemoveInstanceOnReplica(c client.Client, isvc *v1beta1.InferenceService, component types.ComponentType) func(ctx context.Context, idx int32) (bool, error) {
	return func(ctx context.Context, idx int32) (bool, error) {
		ir := &v1beta1.InferenceReplica{}
		key := k8stypes.NamespacedName{Namespace: isvc.Namespace, Name: InferenceReplicaName(isvc, component)}
		if err := c.Get(ctx, key, ir); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, fmt.Errorf("get IR: %w", err)
		}
		pos := -1
		for i, s := range ir.Status.InstanceStatuses {
			if s.Index == idx {
				pos = i
				break
			}
		}
		if pos == -1 {
			return false, nil
		}
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses[:pos], ir.Status.InstanceStatuses[pos+1:]...)
		if err := c.Status().Update(ctx, ir); err != nil {
			return false, fmt.Errorf("update IR: %w", err)
		}
		types.DefaultExpectations.Forget(isvc.Namespace, isvc.Name, component, idx)
		return true, nil
	}
}

// rowFromAPI is the engine row of a stored Instance status, field by
// field: every field a pass reads has to be listed, or a fixture silently
// loses the one under test.
func rowFromAPI(s v1beta1.OMENativeInstanceStatus) types.InstanceStatus {
	return types.InstanceStatus{
		Index:           s.Index,
		Incarnation:     s.Incarnation,
		Phase:           types.InstancePhase(s.Phase),
		RunningRevision: s.RunningRevision,
		TargetRevision:  s.TargetRevision,
		ActiveOrdinal:   s.ActiveOrdinal,
		Operation:       OperationFromAPI(s.Operation),
		LastFailure:     TerminationFromAPI(s.LastFailure),
	}
}

// TerminationFromAPI is the engine's copy of a stored LastFailure, so a
// test sees the failure the pass recorded and not only the phase.
func TerminationFromAPI(t *v1beta1.InstanceTermination) *types.InstanceTermination {
	if t == nil {
		return nil
	}
	out := &types.InstanceTermination{
		PodName:       t.PodName,
		ContainerName: t.ContainerName,
		Reason:        t.Reason,
		Message:       t.Message,
		Time:          t.Time,
	}
	if t.ExitCode != nil {
		e := *t.ExitCode
		out.ExitCode = &e
	}
	return out
}

// TerminationToAPI is the stored form of the engine's LastFailure.
func TerminationToAPI(t *types.InstanceTermination) *v1beta1.InstanceTermination {
	if t == nil {
		return nil
	}
	out := &v1beta1.InstanceTermination{
		PodName:       t.PodName,
		ContainerName: t.ContainerName,
		Reason:        t.Reason,
		Message:       t.Message,
		Time:          t.Time,
	}
	if t.ExitCode != nil {
		e := *t.ExitCode
		out.ExitCode = &e
	}
	return out
}

// OperationFromAPI is the engine's copy of a stored operation. SurgeIndex
// travels too: a gang update at its surge step resumes the surge it holds
// only when the index survives the round trip.
func OperationFromAPI(op *v1beta1.InstanceOperation) *types.InstanceOperation {
	if op == nil {
		return nil
	}
	return &types.InstanceOperation{
		ID:                 op.ID,
		Type:               types.InstanceOperationType(op.Type),
		Step:               op.Step,
		StartedAt:          op.StartedAt,
		LastProgressAt:     op.LastProgressAt,
		Deadline:           op.Deadline,
		TargetRevision:     op.TargetRevision,
		Strategy:           types.UpdateStrategyType(op.Strategy),
		Reason:             op.Reason,
		Waiting:            op.Waiting,
		CapacityRefusedAt:  op.CapacityRefusedAt,
		PlacementWaitSince: op.PlacementWaitSince,
		RequestUUID:        op.RequestUUID,
		RetryCount:         op.RetryCount,
		SurgeIndex:         op.SurgeIndex,
	}
}

// OperationToAPI is the stored form of the engine's operation.
func OperationToAPI(op *types.InstanceOperation) *v1beta1.InstanceOperation {
	if op == nil {
		return nil
	}
	return &v1beta1.InstanceOperation{
		ID:                 op.ID,
		Type:               v1beta1.InstanceOperationType(op.Type),
		Step:               op.Step,
		StartedAt:          op.StartedAt,
		LastProgressAt:     op.LastProgressAt,
		Deadline:           op.Deadline,
		TargetRevision:     op.TargetRevision,
		Strategy:           string(op.Strategy),
		Reason:             op.Reason,
		Waiting:            op.Waiting,
		CapacityRefusedAt:  op.CapacityRefusedAt,
		PlacementWaitSince: op.PlacementWaitSince,
		RequestUUID:        op.RequestUUID,
		RetryCount:         op.RetryCount,
		SurgeIndex:         op.SurgeIndex,
	}
}
