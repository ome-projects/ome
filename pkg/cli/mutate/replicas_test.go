package mutate

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ktesting "k8s.io/client-go/testing"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

func storeColumnarReplica(t *testing.T, ir *v1beta1.InferenceReplica) {
	t.Helper()
	columns, err := irstatus.EncodeColumns(ir.Status.InstanceStatuses, 2048)
	require.NoError(t, err)
	encoding := v1beta1.InstanceStatusEncodingColumnarV2
	ir.Status.InstanceStatuses = nil
	ir.Status.InstanceStatusEncoding = &encoding
	ir.Status.InstanceStatusColumns = columns
}

func TestColumnarReplicaEvidenceRecognizesActiveRestart(t *testing.T) {
	v := safeTarget()
	ir := replicaFor(v)
	ir.Status.UpdateRevision = ir.Status.CurrentRevision
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceRestarting, Operation: &v1beta1.InstanceOperation{ID: "restart-0-123", Type: v1beta1.InstanceOperationRestart, Step: "Drain", StartedAt: metav1.NewTime(testNow.Add(-2e9)), LastProgressAt: metav1.NewTime(testNow.Add(-1e9))}}}
	storeColumnarReplica(t, ir)
	stored := ir.DeepCopy()

	evidence, err := CollectReplicaEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), v, []string{"engine"}, testClock)
	require.NoError(t, err)
	require.True(t, evidence.complete)
	require.True(t, evidence.active)
	require.Equal(t, 1, evidence.operations)
	require.Equal(t, stored, ir)
}

func TestColumnarReplicaCollectionRetainsUnselectedRows(t *testing.T) {
	parent := safeTarget()
	engine := replicaFor(parent)
	decoder := replicaFor(parent)
	decoder.Name = "chat-decoder"
	decoder.Spec.Component = v1beta1.DecoderComponent
	decoder.Status.CurrentRevision = "chat-decoder-aaaaaaaa"
	decoder.Status.UpdateRevision = "chat-decoder-aaaaaaaa"
	decoder.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 7, Phase: v1beta1.OMENativeInstanceReady}}
	storeColumnarReplica(t, decoder)
	stored := decoder.DeepCopy()
	var collected []v1beta1.InferenceReplica

	_, err := collectReplicaEvidence(context.Background(), omefake.NewSimpleClientset(engine, decoder).OmeV1beta1(), parent, []string{"engine"}, testClock, &collected)
	require.NoError(t, err)
	require.Len(t, collected, 2)
	for i := range collected {
		if collected[i].Spec.Component == v1beta1.DecoderComponent {
			require.Nil(t, collected[i].Status.InstanceStatusEncoding)
			require.Nil(t, collected[i].Status.InstanceStatusColumns)
			require.Equal(t, []v1beta1.OMENativeInstanceStatus{{Index: 7, Phase: v1beta1.OMENativeInstanceReady}}, collected[i].Status.InstanceStatuses)
		}
	}
	require.Equal(t, stored, decoder)
}

func TestColumnarPauseRecognizesActiveOperation(t *testing.T) {
	parent, state := nativeTarget(t)
	ir := replicaFor(parent)
	ir.Status.UpdateRevision = ir.Status.CurrentRevision
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceRestarting, Operation: &v1beta1.InstanceOperation{ID: "restart-0-123", Type: v1beta1.InstanceOperationRestart, Step: "Drain", StartedAt: metav1.NewTime(testNow.Add(-2e9)), LastProgressAt: metav1.NewTime(testNow.Add(-1e9))}}}
	storeColumnarReplica(t, ir)

	evidence, err := CollectReplicaEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), parent, []string{"engine"}, testClock)
	require.NoError(t, err)
	plan, err := PrepareRollout(parent, state, evidence, "pause", false, true, testClock)
	require.NoError(t, err)
	require.JSONEq(t, `[{"op":"test","path":"/metadata/uid","value":"uid-chat"},{"op":"test","path":"/metadata/resourceVersion","value":"42"},{"op":"add","path":"/metadata/annotations","value":{}},{"op":"add","path":"/metadata/annotations/ome.io~1rollout-paused","value":"true"}]`, string(plan.Patch()))
}

func TestColumnarReplicaEvidenceRefusesInvalidRepresentation(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*v1beta1.InferenceReplica)
		want error
	}{
		{name: "unknown encoding", edit: func(ir *v1beta1.InferenceReplica) {
			unknown := v1beta1.InstanceStatusEncoding("PRIVATE_V3")
			ir.Status.InstanceStatusEncoding = &unknown
		}, want: ErrStale},
		{name: "missing columns", edit: func(ir *v1beta1.InferenceReplica) { ir.Status.InstanceStatusColumns = nil }, want: ErrStale},
		{name: "mixed rows", edit: func(ir *v1beta1.InferenceReplica) {
			ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}}
		}, want: ErrStale},
		{name: "incomplete coverage", edit: func(ir *v1beta1.InferenceReplica) { ir.Status.InstanceStatusColumns.Members = "0-1" }, want: ErrStale},
		{name: "over row cap", edit: func(ir *v1beta1.InferenceReplica) {
			ir.Status.InstanceStatusColumns.Members = "0-2048"
			ir.Status.InstanceStatusColumns.Phases[0].Indexes = "0-2048"
		}, want: ErrBounds},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := safeTarget()
			ir := replicaFor(parent)
			ir.Status.UpdateRevision = ir.Status.CurrentRevision
			ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}}
			storeColumnarReplica(t, ir)
			tc.edit(ir)
			before := ir.DeepCopy()
			evidence, err := CollectReplicaEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), parent, []string{"engine"}, testClock)
			require.ErrorIs(t, err, tc.want)
			require.False(t, evidence.complete)
			require.NotContains(t, err.Error(), "PRIVATE_V3")
			require.Equal(t, before, ir)
		})
	}
}

func replicaFor(v *v1beta1.InferenceService) *v1beta1.InferenceReplica {
	controller := true
	return &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: "chat-engine", Namespace: v.Namespace, UID: types.UID("uid-ir"), ResourceVersion: "81", Generation: 2, Labels: map[string]string{constants.InferenceServiceLabel: v.Name}, Annotations: map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: "7"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: v.Name, UID: v.UID, Controller: &controller}}}, Spec: v1beta1.InferenceReplicaSpec{ParentRef: v1beta1.ParentReference{Name: v.Name}, Component: v1beta1.EngineComponent}, Status: v1beta1.InferenceReplicaStatus{ObservedGeneration: 2, Replicas: 1, CurrentRevision: "chat-engine-aaaaaaaa", UpdateRevision: "chat-engine-bbbbbbbb"}}
}

func validMigration(phase v1beta1.MigrationPhase) v1beta1.MigrationStatus {
	row := v1beta1.MigrationStatus{RequestUUID: "12345678-1234-4123-8123-123456789abc", Trigger: v1beta1.MigrationTriggerManual, SourceInstance: 0, FromNode: "node-a", Phase: phase, StartedAt: metav1.NewTime(testNow.Add(-2e9)), Deadline: metav1.NewTime(testNow.Add(1e9))}
	if phase == v1beta1.MigrationPhaseSurgePending || phase == v1beta1.MigrationPhaseSurgeReady || phase == v1beta1.MigrationPhaseDraining || phase == v1beta1.MigrationPhaseCompleted {
		index := int32(1)
		stamp := metav1.NewTime(testNow.Add(-1e9))
		row.SurgeInstance = &index
		row.AllocatedAt = &stamp
	}
	if phase.Terminal() {
		stamp := metav1.NewTime(testNow)
		row.CompletedAt = &stamp
	}
	if phase == v1beta1.MigrationPhaseRelocated {
		row.Trigger = v1beta1.MigrationTriggerAuto
		row.Attempt = 1
	}
	return row
}

// Transient publication observations must not decide whether an exact-current
// lifecycle operation can be paused, even when they regress or disappear.
func TestPauseDoesNotDependOnTransientPodObservations(t *testing.T) {
	nodes := make([]string, 65)
	for i := range nodes {
		nodes[i] = "node-" + strings.Repeat("a", i+1)
	}
	for _, tc := range []struct {
		name             string
		ready, scheduled int32
		nodes            []string
	}{
		{name: "absent"},
		{name: "published", ready: 2, scheduled: 2, nodes: []string{"node-a", "node-b"}},
		{name: "regressed", scheduled: 1},
		{name: "large bounded publication", ready: 65, scheduled: 65, nodes: nodes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, state := nativeTarget(t)
			ir := replicaFor(v)
			ir.Status.CurrentRevision = ir.Status.UpdateRevision
			ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceRestarting, ReadyPodCount: tc.ready, ScheduledPodCount: tc.scheduled, NodesOccupied: tc.nodes, Operation: &v1beta1.InstanceOperation{ID: "restart-0-123", Type: v1beta1.InstanceOperationRestart, Step: "Drain", StartedAt: metav1.NewTime(testNow.Add(-2e9)), LastProgressAt: metav1.NewTime(testNow.Add(-1e9))}}}
			work, err := CollectReplicaEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), v, []string{"engine"}, testClock)
			require.NoError(t, err)
			require.True(t, work.complete)
			require.True(t, work.active)
			require.Equal(t, 1, work.operations)
			require.Zero(t, work.migrations)
			plan, err := PrepareRollout(v, state, work, "pause", false, true, testClock)
			require.NoError(t, err)
			require.JSONEq(t, `[{"op":"test","path":"/metadata/uid","value":"uid-chat"},{"op":"test","path":"/metadata/resourceVersion","value":"42"},{"op":"add","path":"/metadata/annotations","value":{}},{"op":"add","path":"/metadata/annotations/ome.io~1rollout-paused","value":"true"}]`, string(plan.Patch()))
		})
	}
}

// Removing complete-object bounds would accept these payloads: the lifecycle
// evidence is valid, and the single transient list element has no semantic role.
func TestPauseStillBoundsCompleteTransientPodPayload(t *testing.T) {
	for _, payloadBytes := range []int{1_048_577, 2_097_152} {
		v := safeTarget()
		ir := replicaFor(v)
		ir.Status.CurrentRevision = ir.Status.UpdateRevision
		ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceRestarting, NodesOccupied: []string{strings.Repeat("n", payloadBytes)}, Operation: &v1beta1.InstanceOperation{ID: "restart-0-123", Type: v1beta1.InstanceOperationRestart, Step: "Drain", StartedAt: metav1.NewTime(testNow.Add(-2e9)), LastProgressAt: metav1.NewTime(testNow.Add(-1e9))}}}
		_, err := CollectReplicaEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), v, []string{"engine"}, testClock)
		require.ErrorIs(t, err, ErrBounds)
	}
}

// Dropping source binding, complete-list or all-record validation would admit
// unsafe lifecycle work. Literal operations exercise the real typed collector.
func TestCollectReplicaEvidenceRecognizesLifecycleAndRejectsUnsafeSources(t *testing.T) {
	v := safeTarget()
	for _, kind := range []v1beta1.InstanceOperationType{v1beta1.InstanceOperationCreate, v1beta1.InstanceOperationUpdate, v1beta1.InstanceOperationRestart, v1beta1.InstanceOperationMigrate, v1beta1.InstanceOperationDelete} {
		t.Run(string(kind), func(t *testing.T) {
			ir := replicaFor(v)
			ir.Status.UpdateRevision = ir.Status.CurrentRevision
			ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating, Operation: &v1beta1.InstanceOperation{ID: "op-1", Type: kind, Step: "WaitReady", StartedAt: metav1.NewTime(testNow.Add(-2e9)), LastProgressAt: metav1.NewTime(testNow.Add(-1e9)), TargetRevision: "chat-engine-aaaaaaaa"}}}
			ir.Status.InstanceStatuses[0].Phase = map[v1beta1.InstanceOperationType]v1beta1.OMENativeInstancePhase{v1beta1.InstanceOperationCreate: v1beta1.OMENativeInstanceCreating, v1beta1.InstanceOperationUpdate: v1beta1.OMENativeInstanceUpdating, v1beta1.InstanceOperationRestart: v1beta1.OMENativeInstanceRestarting, v1beta1.InstanceOperationMigrate: v1beta1.OMENativeInstanceMigrating, v1beta1.InstanceOperationDelete: v1beta1.OMENativeInstanceDeleting}[kind]
			if kind == v1beta1.InstanceOperationMigrate {
				index := int32(1)
				op := ir.Status.InstanceStatuses[0].Operation
				op.RequestUUID = "12345678-1234-4123-8123-123456789abc"
				op.SurgeIndex = &index
				op.Step = "CreateSurge"
				op.TargetRevision = ""
				ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceMigrating
				ir.Status.Migrations = []v1beta1.MigrationStatus{validMigration(v1beta1.MigrationPhaseSurgePending)}
			}
			evidence, err := CollectReplicaEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), v, []string{"engine"}, testClock)
			require.NoError(t, err)
			require.True(t, evidence.complete)
			require.Equal(t, kind != v1beta1.InstanceOperationDelete, evidence.active)
		})
	}
	cases := []struct {
		name   string
		change func(*v1beta1.InferenceReplica)
	}{
		{"wrong namespace", func(ir *v1beta1.InferenceReplica) { ir.Namespace = "other" }},
		{"wrong canonical name", func(ir *v1beta1.InferenceReplica) {
			ir.Name = "other-engine"
			ir.Status.CurrentRevision = "other-engine-aaaaaaaa"
			ir.Status.UpdateRevision = "other-engine-bbbbbbbb"
		}},
		{"oversized private metadata", func(ir *v1beta1.InferenceReplica) { ir.Annotations["private"] = strings.Repeat("s", 65537) }},
		{"wrong owner uid", func(ir *v1beta1.InferenceReplica) { ir.OwnerReferences[0].UID = "old-owner" }},
		{"missing source version", func(ir *v1beta1.InferenceReplica) { ir.ResourceVersion = "" }},
		{"stale source", func(ir *v1beta1.InferenceReplica) { ir.Status.ObservedGeneration = 1 }},
		{"ahead source", func(ir *v1beta1.InferenceReplica) { ir.Status.ObservedGeneration = 3 }},
		{"missing source generation", func(ir *v1beta1.InferenceReplica) { ir.Status.ObservedGeneration = 0 }},
		{"missing parent projection", func(ir *v1beta1.InferenceReplica) { ir.Annotations = nil }},
		{"ahead parent projection", func(ir *v1beta1.InferenceReplica) {
			ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "8"
		}},
		{"stale parent projection", func(ir *v1beta1.InferenceReplica) {
			ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "6"
		}},
		{"hashless revision", func(ir *v1beta1.InferenceReplica) { ir.Status.UpdateRevision = "bad-secret" }},
		{"late malformed operation", func(ir *v1beta1.InferenceReplica) {
			ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}, {Index: 1, Phase: v1beta1.OMENativeInstanceUpdating, Operation: &v1beta1.InstanceOperation{}}}
		}},
		{"oversized operation rows", func(ir *v1beta1.InferenceReplica) {
			ir.Status.InstanceStatuses = make([]v1beta1.OMENativeInstanceStatus, 2049)
		}},
		{"unknown migration", func(ir *v1beta1.InferenceReplica) {
			ir.Status.Migrations = []v1beta1.MigrationStatus{{Phase: "secret"}}
		}},
		{"unknown terminal trigger", func(ir *v1beta1.InferenceReplica) {
			ir.Status.Migrations = []v1beta1.MigrationStatus{{RequestUUID: "12345678-1234-4123-8123-123456789abc", Trigger: "PRIVATE", Phase: v1beta1.MigrationPhaseCompleted, StartedAt: metav1.NewTime(testNow.Add(-1e9))}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ir := replicaFor(v)
			tc.change(ir)
			_, err := inspectReplica(ir, v, []string{"engine"}, testNow)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "secret")
		})
	}
}

func TestCollectReplicaEvidenceManualMigrationIsWorkNotTerminalHistory(t *testing.T) {
	v := safeTarget()
	ir := replicaFor(v)
	ir.Status.UpdateRevision = ir.Status.CurrentRevision
	for _, phase := range []v1beta1.MigrationPhase{v1beta1.MigrationPhaseAccepted, v1beta1.MigrationPhaseSurgePending, v1beta1.MigrationPhaseSurgeReady, v1beta1.MigrationPhaseDraining, v1beta1.MigrationPhaseCompleted, v1beta1.MigrationPhaseFailed, v1beta1.MigrationPhaseRelocated} {
		ir.Status.Migrations = []v1beta1.MigrationStatus{validMigration(phase)}
		evidence, err := CollectReplicaEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), v, []string{"engine"}, testClock)
		require.NoError(t, err)
		require.Equal(t, !phase.Terminal(), evidence.active)
	}
}

func TestReplicaCountersAndMigrationAllocationAreCompleteEvidence(t *testing.T) {
	v := safeTarget()
	for _, edit := range []func(*v1beta1.InferenceReplica){
		func(ir *v1beta1.InferenceReplica) { ir.Status.UpdatedReadyReplicas = 2 },
		func(ir *v1beta1.InferenceReplica) { ir.Status.ReadyReplicas = -1 },
		func(ir *v1beta1.InferenceReplica) { ir.Status.ServingReplicas = 2 },
		func(ir *v1beta1.InferenceReplica) { ir.Status.AvailableReplicas = 2 },
		func(ir *v1beta1.InferenceReplica) {
			ir.Status.Migrations = []v1beta1.MigrationStatus{validMigration(v1beta1.MigrationPhaseSurgeReady)}
			index := int32(-1)
			ir.Status.Migrations[0].SurgeInstance = &index
		},
		func(ir *v1beta1.InferenceReplica) {
			ir.Status.Migrations = []v1beta1.MigrationStatus{validMigration(v1beta1.MigrationPhaseSurgeReady)}
			stamp := metav1.NewTime(testNow.Add(1e9))
			ir.Status.Migrations[0].AllocatedAt = &stamp
		},
		func(ir *v1beta1.InferenceReplica) {
			ir.Status.Migrations = []v1beta1.MigrationStatus{validMigration(v1beta1.MigrationPhaseAccepted)}
			ir.Status.Migrations[0].Attempt = -1
		},
	} {
		ir := replicaFor(v)
		edit(ir)
		_, err := inspectReplica(ir, v, []string{"engine"}, testNow)
		require.ErrorIs(t, err, ErrStale)
	}
}

func TestReplicaOperationTypeMustMatchCurrentPhase(t *testing.T) {
	v := safeTarget()
	for _, kind := range []v1beta1.InstanceOperationType{v1beta1.InstanceOperationCreate, v1beta1.InstanceOperationRestart, v1beta1.InstanceOperationUpdate, v1beta1.InstanceOperationDelete} {
		ir := replicaFor(v)
		ir.Status.UpdateRevision = ir.Status.CurrentRevision
		ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady, Operation: &v1beta1.InstanceOperation{ID: "op-1", Type: kind, Step: "WaitReady", StartedAt: metav1.NewTime(testNow.Add(-2e9)), LastProgressAt: metav1.NewTime(testNow.Add(-1e9))}}}
		_, err := inspectReplica(ir, v, []string{"engine"}, testNow)
		require.ErrorIs(t, err, ErrStale, kind)
	}
}

func TestMigrationOperationIdentityAndActualSourceSurgeRoles(t *testing.T) {
	v := safeTarget()
	for _, surgeRole := range []bool{false, true} {
		ir := replicaFor(v)
		ir.Status.CurrentRevision = ir.Status.UpdateRevision
		index, sibling, phase := int32(0), int32(1), v1beta1.OMENativeInstanceMigrating
		if surgeRole {
			index, sibling, phase = 1, 0, v1beta1.OMENativeInstanceCreating
		}
		ir.Status.Migrations = []v1beta1.MigrationStatus{validMigration(v1beta1.MigrationPhaseSurgePending)}
		ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: index, Phase: phase, Operation: &v1beta1.InstanceOperation{ID: "migrate-op", Type: v1beta1.InstanceOperationMigrate, Step: "CreateSurge", RequestUUID: ir.Status.Migrations[0].RequestUUID, SurgeIndex: &sibling, StartedAt: metav1.NewTime(testNow.Add(-2e9)), LastProgressAt: metav1.NewTime(testNow.Add(-1e9))}}}
		evidence, err := inspectReplica(ir, v, []string{"engine"}, testNow)
		require.NoError(t, err)
		require.True(t, evidence.active)
		require.Equal(t, 1, evidence.operations)
		for _, edit := range []func(*v1beta1.InferenceReplica){
			func(copy *v1beta1.InferenceReplica) {
				copy.Status.InstanceStatuses[0].Operation.RequestUUID = "not-a-uuid"
			},
			func(copy *v1beta1.InferenceReplica) { copy.Status.InstanceStatuses[0].Operation.SurgeIndex = nil },
			func(copy *v1beta1.InferenceReplica) {
				i := int32(-1)
				copy.Status.InstanceStatuses[0].Operation.SurgeIndex = &i
			},
			func(copy *v1beta1.InferenceReplica) {
				i := index
				copy.Status.InstanceStatuses[0].Operation.SurgeIndex = &i
			},
			func(copy *v1beta1.InferenceReplica) { copy.Status.Migrations = nil },
			func(copy *v1beta1.InferenceReplica) {
				copy.Status.Migrations[0] = validMigration(v1beta1.MigrationPhaseCompleted)
			},
			func(copy *v1beta1.InferenceReplica) {
				copy.Status.Migrations[0].RequestUUID = "12345678-1234-4123-8123-123456789abd"
			},
			func(copy *v1beta1.InferenceReplica) { copy.Status.Migrations[0].SourceInstance = 4 },
			func(copy *v1beta1.InferenceReplica) {
				copy.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceReady
			},
			func(copy *v1beta1.InferenceReplica) {
				copy.Status.InstanceStatuses[0].Operation.Deadline = metav1.NewTime(testNow.Add(-3e9))
			},
		} {
			copy := ir.DeepCopy()
			edit(copy)
			_, err = inspectReplica(copy, v, []string{"engine"}, testNow)
			require.ErrorIs(t, err, ErrStale)
		}
	}
}

func TestMigrationRecordConservativeIdentityBoundaries(t *testing.T) {
	legacy := validMigration(v1beta1.MigrationPhaseAccepted)
	index := int32(-1)
	legacy.SurgeInstance = &index
	require.True(t, validMigrationIdentity(legacy, testNow), "legacy preallocation is explicitly recognized by the controller")
	auto := validMigration(v1beta1.MigrationPhaseRelocated)
	yes := true
	auto.Succeeded = &yes
	require.True(t, validMigrationIdentity(auto, testNow))
	for _, edit := range []func(*v1beta1.MigrationStatus){
		func(r *v1beta1.MigrationStatus) { r.Deadline = metav1.Time{} },
		func(r *v1beta1.MigrationStatus) { r.Deadline = metav1.NewTime(testNow.Add(-3e9)) },
		func(r *v1beta1.MigrationStatus) { r.SurgeInstance = nil },
		func(r *v1beta1.MigrationStatus) { r.AllocatedAt = nil },
		func(r *v1beta1.MigrationStatus) { r.Phase = v1beta1.MigrationPhaseAccepted },
		func(r *v1beta1.MigrationStatus) { r.Phase = v1beta1.MigrationPhaseRelocated },
		func(r *v1beta1.MigrationStatus) { stamp := metav1.Time{}; r.AllocatedAt = &stamp },
		func(r *v1beta1.MigrationStatus) { stamp := metav1.NewTime(testNow.Add(-3e9)); r.AllocatedAt = &stamp },
		func(r *v1beta1.MigrationStatus) { r.CompletedAt = &r.StartedAt },
		func(r *v1beta1.MigrationStatus) { r.Succeeded = &yes },
	} {
		r := validMigration(v1beta1.MigrationPhaseSurgeReady)
		edit(&r)
		require.False(t, validMigrationIdentity(r, testNow))
	}
	for _, stamp := range []metav1.Time{{}, metav1.NewTime(testNow.Add(-3e9)), metav1.NewTime(testNow.Add(1e9)), metav1.NewTime(testNow.Add(-1500e6))} {
		r := validMigration(v1beta1.MigrationPhaseCompleted)
		r.CompletedAt = &stamp
		require.False(t, validMigrationIdentity(r, testNow))
	}
	for _, kind := range []v1beta1.InstanceOperationType{v1beta1.InstanceOperationCreate, v1beta1.InstanceOperationRestart, v1beta1.InstanceOperationUpdate, v1beta1.InstanceOperationDelete} {
		ir := replicaFor(safeTarget())
		ir.Status.UpdateRevision = ir.Status.CurrentRevision
		ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceFailed, Operation: &v1beta1.InstanceOperation{ID: "failed-op", Type: kind, Step: "WaitReady", StartedAt: metav1.NewTime(testNow.Add(-2e9)), LastProgressAt: metav1.NewTime(testNow.Add(-1e9))}}}
		evidence, err := inspectReplica(ir, safeTarget(), []string{"engine"}, testNow)
		require.NoError(t, err)
		require.Equal(t, kind == v1beta1.InstanceOperationUpdate, evidence.active)
	}
}

func TestCollectorRequiresCompleteBoundedReadAndPrivateErrors(t *testing.T) {
	v := safeTarget()
	for _, cause := range []error{errors.New("SECRET_API_STATUS"), context.Canceled, context.DeadlineExceeded} {
		client := omefake.NewSimpleClientset()
		client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, cause })
		_, err := CollectReplicaEvidence(context.Background(), client.OmeV1beta1(), v, []string{"engine"}, testClock)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "SECRET_API_STATUS")
		if cause == context.Canceled || cause == context.DeadlineExceeded {
			require.ErrorIs(t, err, cause)
		}
	}
	require.NoError(t, SafeAPIError(nil))
	_, err := CollectReplicaEvidence(context.Background(), nil, v, []string{"engine"}, testClock)
	require.ErrorIs(t, err, ErrRuntime)
	_, err = CollectReplicaEvidence(context.Background(), omefake.NewSimpleClientset().OmeV1beta1(), nil, []string{"engine"}, testClock)
	require.ErrorIs(t, err, ErrUnsafeTarget)
	client := omefake.NewSimpleClientset()
	page := 0
	client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
		page++
		return true, &v1beta1.InferenceReplicaList{ListMeta: metav1.ListMeta{Continue: strings.Repeat("m", page)}, Items: make([]v1beta1.InferenceReplica, 16)}, nil
	})
	_, err = CollectReplicaEvidence(context.Background(), client.OmeV1beta1(), v, []string{"engine"}, nil)
	require.ErrorIs(t, err, ErrBounds)
	first, duplicate := replicaFor(v), replicaFor(v)
	duplicate.Name = "duplicate"
	client = omefake.NewSimpleClientset()
	client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &v1beta1.InferenceReplicaList{Items: []v1beta1.InferenceReplica{*first, *duplicate}}, nil
	})
	_, err = CollectReplicaEvidence(context.Background(), client.OmeV1beta1(), v, []string{"engine"}, testClock)
	require.ErrorIs(t, err, ErrStale)
}

func TestReplicaCompletePrivateBoundsAndMissingLifecycleRecords(t *testing.T) {
	v := safeTarget()
	for _, row := range []struct {
		name string
		edit func(*v1beta1.InferenceReplica)
	}{
		{"label bytes", func(ir *v1beta1.InferenceReplica) { ir.Labels["private"] = strings.Repeat("p", 65537) }},
		{"operation node", func(ir *v1beta1.InferenceReplica) {
			ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Operation: &v1beta1.InstanceOperation{HintTargetNodes: []string{strings.Repeat("n", 257)}}}}
		}},
		{"operation payload", func(ir *v1beta1.InferenceReplica) {
			ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Operation: &v1beta1.InstanceOperation{Reason: strings.Repeat("p", 4097)}}}
		}},
		{"migration node", func(ir *v1beta1.InferenceReplica) {
			ir.Status.Migrations = []v1beta1.MigrationStatus{{HintTargetNodes: []string{strings.Repeat("n", 257)}}}
		}},
		{"migration payload", func(ir *v1beta1.InferenceReplica) {
			ir.Status.Migrations = []v1beta1.MigrationStatus{{Message: strings.Repeat("p", 4097)}}
		}},
		{"condition payload", func(ir *v1beta1.InferenceReplica) {
			ir.Status.Conditions = []metav1.Condition{{Message: strings.Repeat("p", 4097)}}
		}},
		{"retry payload", func(ir *v1beta1.InferenceReplica) {
			ir.Status.RetryBlocks = []v1beta1.RetryBlock{{Reason: strings.Repeat("p", 4097)}}
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			ir := replicaFor(v)
			row.edit(ir)
			_, err := inspectReplica(ir, v, []string{"engine"}, testNow)
			require.ErrorIs(t, err, ErrBounds)
		})
	}
	ir := replicaFor(v)
	ir.Status.CurrentRevision = ""
	ir.Status.UpdateRevision = ""
	evidence, err := inspectReplica(ir, v, []string{"decoder"}, testNow)
	require.NoError(t, err)
	require.False(t, evidence.active)
	_, err = inspectReplica(nil, v, []string{"engine"}, testNow)
	require.ErrorIs(t, err, ErrStale)
	ir = replicaFor(v)
	ir.OwnerReferences = nil
	_, err = inspectReplica(ir, v, []string{"engine"}, testNow)
	require.ErrorIs(t, err, ErrStale)
	ir = replicaFor(v)
	ir.Spec.Component = "PRIVATE"
	_, err = inspectReplica(ir, v, []string{"engine"}, testNow)
	require.ErrorIs(t, err, ErrStale)
	ir = replicaFor(v)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Phase: "PRIVATE"}}
	_, err = inspectReplica(ir, v, []string{"engine"}, testNow)
	require.ErrorIs(t, err, ErrStale)
	ir = replicaFor(v)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}, {Index: 0, Phase: v1beta1.OMENativeInstanceReady}}
	_, err = inspectReplica(ir, v, []string{"engine"}, testNow)
	require.ErrorIs(t, err, ErrStale)
}
