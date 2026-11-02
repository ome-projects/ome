package v1beta1convert

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// --- enum round-trips (v1beta1 → workload → v1beta1) ---------------------

func TestComponentTypeRoundtrip(t *testing.T) {
	cases := []v1beta1.ComponentType{
		v1beta1.RouterComponent,
		v1beta1.EngineComponent,
		v1beta1.DecoderComponent,
		v1beta1.EncoderComponent,
	}
	for _, in := range cases {
		t.Run(string(in), func(t *testing.T) {
			got := ComponentTypeFromWorkload(ComponentTypeToWorkload(in))
			if got != in {
				t.Fatalf("round-trip mismatch: in=%q got=%q", in, got)
			}
		})
	}
}

func TestInstancePhaseRoundtrip(t *testing.T) {
	cases := []v1beta1.OMENativeInstancePhase{
		"", // empty / unset
		v1beta1.OMENativeInstancePending,
		v1beta1.OMENativeInstanceCreating,
		v1beta1.OMENativeInstanceReady,
		v1beta1.OMENativeInstanceUpdating,
		v1beta1.OMENativeInstanceRestarting,
		v1beta1.OMENativeInstanceMigrating,
		v1beta1.OMENativeInstanceFailed,
		v1beta1.OMENativeInstanceDeleting,
	}
	for _, in := range cases {
		t.Run(string(in), func(t *testing.T) {
			got := InstancePhaseFromWorkload(InstancePhaseToWorkload(in))
			if got != in {
				t.Fatalf("round-trip mismatch: in=%q got=%q", in, got)
			}
		})
	}
}

func TestInstanceOperationTypeRoundtrip(t *testing.T) {
	cases := []v1beta1.InstanceOperationType{
		v1beta1.InstanceOperationCreate,
		v1beta1.InstanceOperationUpdate,
		v1beta1.InstanceOperationRestart,
		v1beta1.InstanceOperationMigrate,
		v1beta1.InstanceOperationDelete,
	}
	for _, in := range cases {
		t.Run(string(in), func(t *testing.T) {
			got := InstanceOperationTypeFromWorkload(InstanceOperationTypeToWorkload(in))
			if got != in {
				t.Fatalf("round-trip mismatch: in=%q got=%q", in, got)
			}
		})
	}
}

func TestUpdateStrategyTypeRoundtrip(t *testing.T) {
	cases := []v1beta1.UpdateStrategyType{
		v1beta1.UpdateStrategySurgeThenDrain,
		v1beta1.UpdateStrategyRecreatePod,
		v1beta1.UpdateStrategyInPlaceIfPossible,
		v1beta1.UpdateStrategyInPlaceOnly,
	}
	for _, in := range cases {
		t.Run(string(in), func(t *testing.T) {
			got := UpdateStrategyTypeFromWorkload(UpdateStrategyTypeToWorkload(in))
			if got != in {
				t.Fatalf("round-trip mismatch: in=%q got=%q", in, got)
			}
		})
	}
}

// --- enum reverse round-trips (workload → v1beta1 → workload) ------------

func TestComponentTypeRoundtripReverse(t *testing.T) {
	cases := []workloadtypes.ComponentType{
		workloadtypes.ComponentRouter,
		workloadtypes.ComponentEngine,
		workloadtypes.ComponentDecoder,
		workloadtypes.ComponentEncoder,
	}
	for _, in := range cases {
		t.Run(string(in), func(t *testing.T) {
			got := ComponentTypeToWorkload(ComponentTypeFromWorkload(in))
			if got != in {
				t.Fatalf("reverse round-trip mismatch: in=%q got=%q", in, got)
			}
		})
	}
}

func TestInstancePhaseRoundtripReverse(t *testing.T) {
	cases := []workloadtypes.InstancePhase{
		workloadtypes.InstancePhaseEmpty,
		workloadtypes.InstancePhasePending,
		workloadtypes.InstancePhaseCreating,
		workloadtypes.InstancePhaseReady,
		workloadtypes.InstancePhaseUpdating,
		workloadtypes.InstancePhaseRestarting,
		workloadtypes.InstancePhaseMigrating,
		workloadtypes.InstancePhaseFailed,
		workloadtypes.InstancePhaseDeleting,
	}
	for _, in := range cases {
		t.Run(string(in), func(t *testing.T) {
			got := InstancePhaseToWorkload(InstancePhaseFromWorkload(in))
			if got != in {
				t.Fatalf("reverse round-trip mismatch: in=%q got=%q", in, got)
			}
		})
	}
}

func TestInstanceOperationTypeRoundtripReverse(t *testing.T) {
	cases := []workloadtypes.InstanceOperationType{
		workloadtypes.InstanceOperationCreate,
		workloadtypes.InstanceOperationUpdate,
		workloadtypes.InstanceOperationRestart,
		workloadtypes.InstanceOperationMigrate,
		workloadtypes.InstanceOperationDelete,
	}
	for _, in := range cases {
		t.Run(string(in), func(t *testing.T) {
			got := InstanceOperationTypeToWorkload(InstanceOperationTypeFromWorkload(in))
			if got != in {
				t.Fatalf("reverse round-trip mismatch: in=%q got=%q", in, got)
			}
		})
	}
}

func TestUpdateStrategyTypeRoundtripReverse(t *testing.T) {
	cases := []workloadtypes.UpdateStrategyType{
		workloadtypes.UpdateStrategySurgeThenDrain,
		workloadtypes.UpdateStrategyRecreatePod,
		workloadtypes.UpdateStrategyInPlaceIfPossible,
		workloadtypes.UpdateStrategyInPlaceOnly,
	}
	for _, in := range cases {
		t.Run(string(in), func(t *testing.T) {
			got := UpdateStrategyTypeToWorkload(UpdateStrategyTypeFromWorkload(in))
			if got != in {
				t.Fatalf("reverse round-trip mismatch: in=%q got=%q", in, got)
			}
		})
	}
}

// --- unknown-value tolerance --------------------------------------------

func TestComponentType_UnknownValueReturnsEmpty(t *testing.T) {
	if got := ComponentTypeToWorkload(v1beta1.ComponentType("future-component")); got != "" {
		t.Fatalf("unknown v1beta1.ComponentType should map to empty; got=%q", got)
	}
	if got := ComponentTypeFromWorkload(workloadtypes.ComponentType("future-component")); got != "" {
		t.Fatalf("unknown workload.ComponentType should map to empty; got=%q", got)
	}
}

func TestInstancePhase_UnknownValueReturnsEmpty(t *testing.T) {
	if got := InstancePhaseToWorkload(v1beta1.OMENativeInstancePhase("Suspended")); got != workloadtypes.InstancePhaseEmpty {
		t.Fatalf("unknown v1beta1 phase should map to empty; got=%q", got)
	}
	if got := InstancePhaseFromWorkload(workloadtypes.InstancePhase("Suspended")); got != "" {
		t.Fatalf("unknown workload phase should map to empty; got=%q", got)
	}
}

func TestInstanceOperationType_UnknownValueReturnsEmpty(t *testing.T) {
	if got := InstanceOperationTypeToWorkload(v1beta1.InstanceOperationType("Rotate")); got != "" {
		t.Fatalf("unknown v1beta1 op type should map to empty; got=%q", got)
	}
	if got := InstanceOperationTypeFromWorkload(workloadtypes.InstanceOperationType("Rotate")); got != "" {
		t.Fatalf("unknown workload op type should map to empty; got=%q", got)
	}
}

func TestUpdateStrategyType_UnknownValueReturnsEmpty(t *testing.T) {
	if got := UpdateStrategyTypeToWorkload(v1beta1.UpdateStrategyType("BlueGreen")); got != "" {
		t.Fatalf("unknown v1beta1 strategy should map to empty; got=%q", got)
	}
	if got := UpdateStrategyTypeFromWorkload(workloadtypes.UpdateStrategyType("BlueGreen")); got != "" {
		t.Fatalf("unknown workload strategy should map to empty; got=%q", got)
	}
}

// --- InstanceOperation struct round-trips -------------------------------

func TestInstanceOperationRoundtrip_AllFieldsPopulated(t *testing.T) {
	started := metav1.NewTime(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	progress := metav1.NewTime(time.Date(2026, 1, 1, 12, 5, 0, 0, time.UTC))
	deadline := metav1.NewTime(time.Date(2026, 1, 1, 12, 30, 0, 0, time.UTC))
	placementWait := metav1.NewTime(time.Date(2026, 1, 1, 12, 10, 0, 0, time.UTC))
	surge := int32(7)
	in := &v1beta1.InstanceOperation{
		ID:                 "op-abc-123",
		Type:               v1beta1.InstanceOperationMigrate,
		Step:               "Drain",
		StartedAt:          started,
		LastProgressAt:     progress,
		Deadline:           deadline,
		RetryCount:         3,
		TargetRevision:     "rev-7",
		Reason:             "node-fragmentation",
		PlacementWaitSince: &placementWait,
		SurgeIndex:         &surge,
		FromNode:           "node-a",
		HintTargetNodes:    []string{"node-x", "node-y"},
		RequestUUID:        "uuid-deadbeef",
	}
	got := InstanceOperationFromWorkload(InstanceOperationToWorkload(in))
	if diff := cmp.Diff(in, got); diff != "" {
		t.Fatalf("round-trip mismatch (-want +got):\n%s", diff)
	}
	// Verify deep copy: mutating the returned slice / pointer must not
	// affect the input.
	if got.SurgeIndex == in.SurgeIndex {
		t.Fatalf("SurgeIndex pointer was not deep-copied")
	}
	if got.PlacementWaitSince == in.PlacementWaitSince {
		t.Fatalf("PlacementWaitSince pointer was not deep-copied")
	}
	if len(got.HintTargetNodes) > 0 && len(in.HintTargetNodes) > 0 {
		got.HintTargetNodes[0] = "mutated"
		if in.HintTargetNodes[0] == "mutated" {
			t.Fatalf("HintTargetNodes slice was not deep-copied")
		}
	}
}

func TestInstanceOperationRoundtrip_NilInput(t *testing.T) {
	if got := InstanceOperationToWorkload(nil); got != nil {
		t.Fatalf("expected nil for nil v1beta1 input; got=%+v", got)
	}
	if got := InstanceOperationFromWorkload(nil); got != nil {
		t.Fatalf("expected nil for nil workload input; got=%+v", got)
	}
}

func TestInstanceOperationRoundtrip_OptionalFieldsZero(t *testing.T) {
	// All optional pointer / slice fields nil; verifies the converter
	// preserves zero/unset state.
	in := &v1beta1.InstanceOperation{
		ID:             "op-1",
		Type:           v1beta1.InstanceOperationCreate,
		Step:           "WaitReady",
		StartedAt:      metav1.NewTime(time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)),
		LastProgressAt: metav1.NewTime(time.Date(2026, 2, 1, 0, 1, 0, 0, time.UTC)),
		Deadline:       metav1.NewTime(time.Date(2026, 2, 1, 0, 5, 0, 0, time.UTC)),
	}
	got := InstanceOperationFromWorkload(InstanceOperationToWorkload(in))
	if diff := cmp.Diff(in, got); diff != "" {
		t.Fatalf("round-trip mismatch (-want +got):\n%s", diff)
	}
	if got.SurgeIndex != nil {
		t.Fatalf("SurgeIndex should remain nil; got=%v", got.SurgeIndex)
	}
	if got.HintTargetNodes != nil {
		t.Fatalf("HintTargetNodes should remain nil; got=%v", got.HintTargetNodes)
	}
}

// --- InstanceTermination struct round-trips -----------------------------

func TestInstanceTerminationRoundtrip_AllFieldsPopulated(t *testing.T) {
	exit := int32(137)
	in := &v1beta1.InstanceTermination{
		PodName:       "engine-0-leader-0",
		ContainerName: "main",
		Reason:        "OOMKilled",
		ExitCode:      &exit,
		Message:       "out of memory",
		Time:          metav1.NewTime(time.Date(2026, 4, 1, 8, 0, 0, 0, time.UTC)),
	}
	got := InstanceTerminationFromWorkload(InstanceTerminationToWorkload(in))
	if diff := cmp.Diff(in, got); diff != "" {
		t.Fatalf("round-trip mismatch (-want +got):\n%s", diff)
	}
	// ExitCode must be deep-copied, not aliased.
	if got.ExitCode == in.ExitCode {
		t.Fatalf("ExitCode pointer was not deep-copied")
	}
}

func TestInstanceTerminationRoundtrip_NilInput(t *testing.T) {
	if got := InstanceTerminationToWorkload(nil); got != nil {
		t.Fatalf("expected nil for nil v1beta1 input; got=%+v", got)
	}
	if got := InstanceTerminationFromWorkload(nil); got != nil {
		t.Fatalf("expected nil for nil workload input; got=%+v", got)
	}
}

func TestInstanceTerminationRoundtrip_NilExitCode(t *testing.T) {
	in := &v1beta1.InstanceTermination{
		PodName: "engine-0-leader-0",
		Reason:  "ImagePullBackOff",
		// ExitCode nil: a stuck waiting-state record.
	}
	got := InstanceTerminationFromWorkload(InstanceTerminationToWorkload(in))
	if diff := cmp.Diff(in, got); diff != "" {
		t.Fatalf("round-trip mismatch (-want +got):\n%s", diff)
	}
	if got.ExitCode != nil {
		t.Fatalf("ExitCode should remain nil; got=%v", got.ExitCode)
	}
}

// --- InstanceStatus struct round-trips ----------------------------------

func TestInstanceStatusRoundtrip_AllFieldsPopulated(t *testing.T) {
	started := metav1.NewTime(time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC))
	progress := metav1.NewTime(time.Date(2026, 3, 1, 9, 1, 0, 0, time.UTC))
	deadline := metav1.NewTime(time.Date(2026, 3, 1, 9, 10, 0, 0, time.UTC))
	condTime := metav1.NewTime(time.Date(2026, 3, 1, 9, 0, 30, 0, time.UTC))
	in := v1beta1.OMENativeInstanceStatus{
		Index:             5,
		Incarnation:       42,
		Phase:             v1beta1.OMENativeInstanceUpdating,
		RunningRevision:   "rev-3",
		TargetRevision:    "rev-4",
		PodCount:          2,
		ReadyPodCount:     1,
		ServingPodCount:   1,
		AvailablePodCount: 1,
		ScheduledPodCount: 2,
		Admitted:          true,
		NodesOccupied:     []string{"node-a", "node-b"},
		Conditions: []metav1.Condition{
			{
				Type:               "AllPodsReady",
				Status:             "False",
				LastTransitionTime: condTime,
				Reason:             "OnePodPending",
				Message:            "pod-1 is Pending",
				ObservedGeneration: 7,
			},
		},
		Operation: &v1beta1.InstanceOperation{
			ID:             "op-99",
			Type:           v1beta1.InstanceOperationUpdate,
			Step:           "Recreate",
			StartedAt:      started,
			LastProgressAt: progress,
			Deadline:       deadline,
			RetryCount:     1,
			TargetRevision: "rev-4",
			Reason:         "template-change",
		},
		ActiveOrdinal: 1,
		LastFailure: &v1beta1.InstanceTermination{
			PodName:       "engine-5-leader-0",
			ContainerName: "main",
			Reason:        "OOMKilled",
			ExitCode:      func() *int32 { e := int32(137); return &e }(),
			Message:       "out of memory",
			Time:          condTime,
		},
	}
	got := InstanceStatusFromWorkload(InstanceStatusToWorkload(in))
	if diff := cmp.Diff(in, got); diff != "" {
		t.Fatalf("round-trip mismatch (-want +got):\n%s", diff)
	}
}

func TestInstanceStatusRoundtrip_EmptyOperation(t *testing.T) {
	in := v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Phase:           v1beta1.OMENativeInstanceReady,
		RunningRevision: "rev-1",
		// No Operation, no NodesOccupied, no Conditions.
	}
	got := InstanceStatusFromWorkload(InstanceStatusToWorkload(in))
	if diff := cmp.Diff(in, got); diff != "" {
		t.Fatalf("round-trip mismatch (-want +got):\n%s", diff)
	}
	if got.Operation != nil {
		t.Fatalf("Operation should remain nil; got=%+v", got.Operation)
	}
}

// The status publication writes Restarting over a settled Ready row whose
// pod set serves nothing. The engine reads that row as Ready: no operation
// owns it, and its revision and ReadySince anchor are untouched. A
// Restarting row that carries its Restart operation is a repair in flight
// and reads as it is.
func TestInstanceStatusToWorkload_PublishedRestartingReadsReady(t *testing.T) {
	readySince := metav1.NewTime(time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC))
	published := v1beta1.OMENativeInstanceStatus{
		Index: 2, Incarnation: 3, Phase: v1beta1.OMENativeInstanceRestarting,
		RunningRevision: "rev-1", PodCount: 1, ReadySince: &readySince,
	}
	got := InstanceStatusToWorkload(published)
	if got.Phase != workloadtypes.InstancePhaseReady || got.Operation != nil {
		t.Fatalf("published Restarting read as phase %s operation %+v, want Ready with no operation", got.Phase, got.Operation)
	}
	if got.RunningRevision != "rev-1" || got.ReadySince == nil || !got.ReadySince.Equal(&readySince) || got.Incarnation != 3 {
		t.Fatalf("row = %+v, want revision, ReadySince and incarnation kept", got)
	}

	repairing := published
	repairing.Operation = &v1beta1.InstanceOperation{ID: "restart-2", Type: v1beta1.InstanceOperationRestart, Step: "Drain"}
	if got := InstanceStatusToWorkload(repairing); got.Phase != workloadtypes.InstancePhaseRestarting || got.Operation == nil {
		t.Fatalf("repair in flight read as phase %s operation %+v, want Restarting with its operation", got.Phase, got.Operation)
	}
}

// The status publication writes Ready over a settled Failed row that still
// runs a revision and records a spent attempt at another while the pods it
// runs serve. The engine reads that row as Failed: no promote leaves a Ready
// row off its running revision, so the shape is the publication's. The
// record, the revisions and the anchor come through untouched. A Ready row
// with no target revision, one whose target is the revision it runs, one
// that never ran a revision, and one carrying an operation read as they are.
func TestInstanceStatusToWorkload_PublishedReadyOffRunningRevisionReadsFailed(t *testing.T) {
	readySince := metav1.NewTime(time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC))
	failedAt := metav1.NewTime(readySince.Add(10 * time.Minute))
	published := v1beta1.OMENativeInstanceStatus{
		Index: 2, Incarnation: 3, Phase: v1beta1.OMENativeInstanceReady,
		RunningRevision: "rev-1", TargetRevision: "rev-2", PodCount: 2, ReadySince: &readySince,
		LastFailure: &v1beta1.InstanceTermination{PodName: "p-2-1", Reason: "CrashLoopBackOff", Time: failedAt},
	}
	got := InstanceStatusToWorkload(published)
	if got.Phase != workloadtypes.InstancePhaseFailed || got.Operation != nil {
		t.Fatalf("published Ready off its running revision read as phase %s operation %+v, want Failed with no operation", got.Phase, got.Operation)
	}
	if got.RunningRevision != "rev-1" || got.TargetRevision != "rev-2" || got.Incarnation != 3 ||
		got.ReadySince == nil || !got.ReadySince.Equal(&readySince) ||
		got.LastFailure == nil || got.LastFailure.PodName != "p-2-1" || !got.LastFailure.Time.Equal(&failedAt) {
		t.Fatalf("row = %+v, want revisions, anchor, incarnation and record kept", got)
	}
	back := InstanceStatusFromWorkload(got)
	if back.Phase != v1beta1.OMENativeInstanceFailed {
		t.Fatalf("the engine's shape encodes as %s, want Failed: only the publication writes Ready over it", back.Phase)
	}

	asIs := map[string]v1beta1.OMENativeInstanceStatus{
		"no target revision":        {Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-1"},
		"target is the running one": {Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-1", TargetRevision: "rev-1"},
		"never ran a revision":      {Phase: v1beta1.OMENativeInstanceReady, TargetRevision: "rev-2"},
		"a recreate attempt owns the row": {Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-1", TargetRevision: "rev-2",
			Operation: &v1beta1.InstanceOperation{ID: "update-2", Type: v1beta1.InstanceOperationUpdate, Step: "Drain"}},
	}
	for name, row := range asIs {
		if got := InstanceStatusToWorkload(row); got.Phase != workloadtypes.InstancePhaseReady {
			t.Errorf("%s: read as %s, want Ready", name, got.Phase)
		}
	}
}

// The status publication writes Ready over a Failed row that still keeps
// the surge attempt it failed at while the pods on the revision it runs
// serve. The engine reads that row as Failed: no writer leaves Ready beside
// an operation, so the shape is the publication's. The attempt, the record,
// the revisions and the anchor come through untouched, single-pod and gang
// alike. A source past its drain hand-over, the replacement's own row, a
// roll still Updating and a Ready row with no target revision read as they
// are.
func TestInstanceStatusToWorkload_PublishedReadyKeepingSurgeReadsFailed(t *testing.T) {
	readySince := metav1.NewTime(time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC))
	failedAt := metav1.NewTime(readySince.Add(10 * time.Minute))
	slot := int32(2)
	surge := func(step string, surgeIdx *int32) *v1beta1.InstanceOperation {
		return &v1beta1.InstanceOperation{ID: "gangsurge-0", Type: v1beta1.InstanceOperationUpdate, Step: step, TargetRevision: "rev-2", SurgeIndex: surgeIdx}
	}
	for name, op := range map[string]*v1beta1.InstanceOperation{"gang": surge("Surge", &slot), "single-pod": surge("Surge", nil)} {
		published := v1beta1.OMENativeInstanceStatus{
			Index: 0, Incarnation: 3, Phase: v1beta1.OMENativeInstanceReady,
			RunningRevision: "rev-1", TargetRevision: "rev-2", PodCount: 2, ReadySince: &readySince, Operation: op,
			LastFailure: &v1beta1.InstanceTermination{PodName: "p-2-leader-0", Reason: "CrashLoopBackOff", Time: failedAt},
		}
		got := InstanceStatusToWorkload(published)
		if got.Phase != workloadtypes.InstancePhaseFailed {
			t.Fatalf("%s: published Ready keeping its surge read as phase %s, want Failed", name, got.Phase)
		}
		if got.Operation == nil || got.Operation.Step != workloadtypes.UpdateStepSurge || got.Operation.TargetRevision != "rev-2" ||
			(op.SurgeIndex != nil) != (got.Operation.SurgeIndex != nil) {
			t.Fatalf("%s: operation = %+v, want the surge attempt kept", name, got.Operation)
		}
		if got.RunningRevision != "rev-1" || got.TargetRevision != "rev-2" || got.Incarnation != 3 ||
			got.ReadySince == nil || !got.ReadySince.Equal(&readySince) ||
			got.LastFailure == nil || got.LastFailure.PodName != "p-2-leader-0" || !got.LastFailure.Time.Equal(&failedAt) {
			t.Fatalf("%s: row = %+v, want revisions, anchor, incarnation and record kept", name, got)
		}
		if back := InstanceStatusFromWorkload(got); back.Phase != v1beta1.OMENativeInstanceFailed {
			t.Fatalf("%s: the engine's shape encodes as %s, want Failed: only the publication writes Ready over it", name, back.Phase)
		}
	}

	asIs := map[string]struct {
		row  v1beta1.OMENativeInstanceStatus
		want workloadtypes.InstancePhase
	}{
		"past the drain hand-over": {v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-1", TargetRevision: "rev-2",
			Operation: surge("SurgeDrain", &slot)}, workloadtypes.InstancePhaseReady},
		"the replacement's own row": {v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceFailed, TargetRevision: "rev-2",
			Operation: surge("GangSurgeTarget", nil)}, workloadtypes.InstancePhaseFailed},
		"a roll still Updating": {v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: "rev-1", TargetRevision: "rev-2",
			Operation: surge("Surge", &slot)}, workloadtypes.InstancePhaseUpdating},
		"no target revision": {v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-1",
			Operation: surge("Surge", &slot)}, workloadtypes.InstancePhaseReady},
	}
	for name, tc := range asIs {
		if got := InstanceStatusToWorkload(tc.row); got.Phase != tc.want {
			t.Errorf("%s: read as %s, want %s", name, got.Phase, tc.want)
		}
	}
}

func TestInstanceStatusRoundtrip_NodesOccupiedDeepCopy(t *testing.T) {
	in := v1beta1.OMENativeInstanceStatus{
		Index:         3,
		Phase:         v1beta1.OMENativeInstanceReady,
		NodesOccupied: []string{"n1", "n2"},
	}
	mid := InstanceStatusToWorkload(in)
	mid.NodesOccupied[0] = "mutated"
	if in.NodesOccupied[0] == "mutated" {
		t.Fatalf("NodesOccupied was not deep-copied on To conversion")
	}
}

// --- UpdateStrategy.RollingUpdate conversion ----------------------------

// TestUpdateStrategyToWorkload_RollingUpdate_IntOrString: both
// MaxUnavailable and MaxSurge flow through the converter as
// *intstr.IntOrString, preserving integer and percent shapes verbatim.
// The deep-copy guarantee holds — mutating either pointer on the output
// must not bleed into the input.
func TestUpdateStrategyToWorkload_RollingUpdate_IntOrString(t *testing.T) {
	partition := int32(3)
	mu := intstr.FromString("25%")
	ms := intstr.FromInt(2)
	in := v1beta1.UpdateStrategy{
		Type: v1beta1.UpdateStrategySurgeThenDrain,
		RollingUpdate: &v1beta1.RollingUpdate{
			Partition:      &partition,
			MaxUnavailable: &mu,
			MaxSurge:       &ms,
		},
	}
	out := UpdateStrategyToWorkload(in)
	if out.RollingUpdate == nil {
		t.Fatalf("RollingUpdate must be carried through, got nil")
	}
	if out.RollingUpdate.Partition == nil || *out.RollingUpdate.Partition != partition {
		t.Fatalf("Partition mismatch: got %v want %d", out.RollingUpdate.Partition, partition)
	}
	if out.RollingUpdate.MaxUnavailable == nil ||
		out.RollingUpdate.MaxUnavailable.Type != intstr.String ||
		out.RollingUpdate.MaxUnavailable.StrVal != "25%" {
		t.Fatalf("MaxUnavailable percent mismatch: got %+v", out.RollingUpdate.MaxUnavailable)
	}
	if out.RollingUpdate.MaxSurge == nil ||
		out.RollingUpdate.MaxSurge.Type != intstr.Int ||
		out.RollingUpdate.MaxSurge.IntVal != 2 {
		t.Fatalf("MaxSurge integer mismatch: got %+v", out.RollingUpdate.MaxSurge)
	}
	// Deep-copy guarantee: the converter must allocate fresh pointers
	// so adapters can mutate output independently of source.
	if out.RollingUpdate.MaxUnavailable == in.RollingUpdate.MaxUnavailable {
		t.Fatalf("MaxUnavailable pointer aliased — converter must deep-copy")
	}
	if out.RollingUpdate.MaxSurge == in.RollingUpdate.MaxSurge {
		t.Fatalf("MaxSurge pointer aliased — converter must deep-copy")
	}
}

// TestUpdateStrategyToWorkload_RollingUpdate_NilFields verifies the
// converter omits MaxUnavailable / MaxSurge when the source has them
// unset. The workload layer relies on nil = "use default" to fall back
// to the reconciler's documented defaults.
func TestUpdateStrategyToWorkload_RollingUpdate_NilFields(t *testing.T) {
	partition := int32(0)
	in := v1beta1.UpdateStrategy{
		RollingUpdate: &v1beta1.RollingUpdate{
			Partition: &partition,
		},
	}
	out := UpdateStrategyToWorkload(in)
	if out.RollingUpdate == nil {
		t.Fatalf("RollingUpdate must be carried through, got nil")
	}
	if out.RollingUpdate.MaxUnavailable != nil {
		t.Fatalf("MaxUnavailable should remain nil; got %+v", out.RollingUpdate.MaxUnavailable)
	}
	if out.RollingUpdate.MaxSurge != nil {
		t.Fatalf("MaxSurge should remain nil; got %+v", out.RollingUpdate.MaxSurge)
	}
}

// TestUpdateStrategyToWorkload_RollingUpdate_AbsentDoesNotPopulate
// confirms a nil source RollingUpdate stays nil on the output — the
// converter never invents a default RollingUpdate struct.
func TestUpdateStrategyToWorkload_RollingUpdate_AbsentDoesNotPopulate(t *testing.T) {
	in := v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyRecreatePod}
	out := UpdateStrategyToWorkload(in)
	if out.RollingUpdate != nil {
		t.Fatalf("expected RollingUpdate to remain nil; got %+v", out.RollingUpdate)
	}
}

func TestInstanceStatusSliceRoundtrip(t *testing.T) {
	in := []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-1"},
		{Index: 1, Phase: v1beta1.OMENativeInstanceCreating, TargetRevision: "rev-1", PodCount: 1},
		{Index: 2, Phase: v1beta1.OMENativeInstanceFailed, RunningRevision: "rev-0"},
	}
	got := InstanceStatusSliceFromWorkload(InstanceStatusSliceToWorkload(in))
	if diff := cmp.Diff(in, got); diff != "" {
		t.Fatalf("slice round-trip mismatch (-want +got):\n%s", diff)
	}
}

func TestInstanceStatusSliceRoundtrip_NilPreserved(t *testing.T) {
	if got := InstanceStatusSliceToWorkload(nil); got != nil {
		t.Fatalf("nil v1beta1 slice should map to nil workload slice; got=%v", got)
	}
	if got := InstanceStatusSliceFromWorkload(nil); got != nil {
		t.Fatalf("nil workload slice should map to nil v1beta1 slice; got=%v", got)
	}
}

func TestInstanceStatusSliceRoundtrip_EmptyPreserved(t *testing.T) {
	in := []v1beta1.OMENativeInstanceStatus{}
	got := InstanceStatusSliceFromWorkload(InstanceStatusSliceToWorkload(in))
	if got == nil {
		t.Fatalf("empty (non-nil) v1beta1 slice should round-trip to empty (non-nil); got nil")
	}
	if len(got) != 0 {
		t.Fatalf("expected len 0; got=%d", len(got))
	}
}

// The pinned strategy has to survive both directions: the update pass reads
// it back from status to decide the mode of an attempt already under way, so
// a field that drops in conversion silently un-pins every operation.
func TestInstanceOperationStrategyRoundtrip(t *testing.T) {
	cases := []v1beta1.UpdateStrategyType{
		v1beta1.UpdateStrategySurgeThenDrain,
		v1beta1.UpdateStrategyRecreatePod,
		v1beta1.UpdateStrategyInPlaceIfPossible,
		v1beta1.UpdateStrategyInPlaceOnly,
		"",
	}
	for _, in := range cases {
		t.Run(string(in), func(t *testing.T) {
			op := &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Strategy: string(in)}
			got := InstanceOperationFromWorkload(InstanceOperationToWorkload(op))
			if got.Strategy != op.Strategy {
				t.Fatalf("round-trip mismatch: in=%q got=%q", op.Strategy, got.Strategy)
			}
		})
	}
}

// --- enum tables ----------------------------------------------------------

// enumTableUnderTest lowers one table to strings so every table can go
// through the same checks: its pairs, the two lookups, and the names of the
// types its constants are declared on in the API and workload packages.
type enumTableUnderTest struct {
	name         string
	apiType      string
	workloadType string
	pairs        map[string]string
	toWorkload   func(string) string
	fromWorkload func(string) string
}

func lowerEnumTable[API ~string, Workload ~string](name, apiType, workloadType string, table enumTable[API, Workload]) enumTableUnderTest {
	pairs := make(map[string]string, len(table.workloadOf))
	for api, workload := range table.workloadOf {
		pairs[string(api)] = string(workload)
	}
	return enumTableUnderTest{
		name:         name,
		apiType:      apiType,
		workloadType: workloadType,
		pairs:        pairs,
		toWorkload:   func(v string) string { return string(table.toWorkload(API(v))) },
		fromWorkload: func(w string) string { return string(table.fromWorkload(Workload(w))) },
	}
}

func enumTablesUnderTest() []enumTableUnderTest {
	return []enumTableUnderTest{
		lowerEnumTable("component types", "ComponentType", "ComponentType", componentTypes),
		lowerEnumTable("instance phases", "OMENativeInstancePhase", "InstancePhase", instancePhases),
		lowerEnumTable("update strategy types", "UpdateStrategyType", "UpdateStrategyType", updateStrategyTypes),
		lowerEnumTable("instance operation types", "InstanceOperationType", "InstanceOperationType", instanceOperationTypes),
		lowerEnumTable("instance restart policies", "InstanceRestartPolicy", "RestartPolicy", instanceRestartPolicies),
		lowerEnumTable("instance ready policies", "InstanceReadyPolicy", "InstanceReadyPolicy", instanceReadyPolicies),
		lowerEnumTable("migration modes", "MigrationPolicyMode", "MigrationMode", migrationModes),
	}
}

func TestEnumTablesRoundTripEveryPair(t *testing.T) {
	for _, table := range enumTablesUnderTest() {
		t.Run(table.name, func(t *testing.T) {
			if len(table.pairs) == 0 {
				t.Fatal("the table is empty")
			}
			for api, workload := range table.pairs {
				if got := table.toWorkload(api); got != workload {
					t.Errorf("toWorkload(%q) = %q, want %q", api, got, workload)
				}
				if got := table.fromWorkload(workload); got != api {
					t.Errorf("fromWorkload(%q) = %q, want %q", workload, got, api)
				}
			}
		})
	}
}

func TestEnumTablesReadAnUnknownValueAsEmpty(t *testing.T) {
	for _, table := range enumTablesUnderTest() {
		t.Run(table.name, func(t *testing.T) {
			if got := table.toWorkload("NotDeclared"); got != "" {
				t.Errorf("toWorkload of an undeclared value = %q, want empty", got)
			}
			if got := table.fromWorkload("NotDeclared"); got != "" {
				t.Errorf("fromWorkload of an undeclared value = %q, want empty", got)
			}
		})
	}
}

// Every constant the API package and the workload package declare for an
// enum must be in that enum's table, or a value the operator can write
// would cross this seam as the empty value.
func TestEnumTablesListEveryDeclaredConstant(t *testing.T) {
	api := declaredStringConstants(t, filepath.Join("..", "..", "..", "apis", "ome", "v1beta1"))
	workload := declaredStringConstants(t, filepath.Join("..", "workload", "types"))
	for _, table := range enumTablesUnderTest() {
		t.Run(table.name, func(t *testing.T) {
			declaredAPI := api.ofType(t, table.apiType)
			for value, constant := range declaredAPI {
				if _, ok := table.pairs[value]; !ok {
					t.Errorf("API constant %s (%q) is not in the table", constant, value)
				}
			}
			workloadValues := make(map[string]bool, len(table.pairs))
			for _, w := range table.pairs {
				workloadValues[w] = true
			}
			for value, constant := range workload.ofType(t, table.workloadType) {
				if !workloadValues[value] {
					t.Errorf("workload constant %s (%q) is not in the table", constant, value)
				}
			}
		})
	}
}

// declaredConstants holds, per type name, the string constants a package
// declares (value to constant name) and the names of any constant of that
// type whose value the reader could not take as a plain string literal.
type declaredConstants struct {
	byType map[string]map[string]string
	unread map[string][]string
}

func (d declaredConstants) ofType(t *testing.T, typeName string) map[string]string {
	t.Helper()
	if names := d.unread[typeName]; len(names) > 0 {
		t.Fatalf("constants of type %s are not plain string literals: %s", typeName, strings.Join(names, ", "))
	}
	values := d.byType[typeName]
	if len(values) == 0 {
		t.Fatalf("no string constant of type %s is declared", typeName)
	}
	return values
}

// declaredStringConstants parses the non-test, non-generated Go files of
// one package directory and collects its typed string constants.
func declaredStringConstants(t *testing.T, dir string) declaredConstants {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	out := declaredConstants{byType: map[string]map[string]string{}, unread: map[string][]string{}}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || strings.HasPrefix(name, "zz_generated") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				out.collect(spec.(*ast.ValueSpec))
			}
		}
	}
	return out
}

func (d declaredConstants) collect(spec *ast.ValueSpec) {
	typeName, ok := spec.Type.(*ast.Ident)
	if !ok {
		return
	}
	for i, ident := range spec.Names {
		value, ok := stringLiteral(spec.Values, i)
		if !ok {
			d.unread[typeName.Name] = append(d.unread[typeName.Name], ident.Name)
			continue
		}
		if d.byType[typeName.Name] == nil {
			d.byType[typeName.Name] = map[string]string{}
		}
		d.byType[typeName.Name][value] = ident.Name
	}
}

func stringLiteral(values []ast.Expr, i int) (string, bool) {
	if i >= len(values) {
		return "", false
	}
	lit, ok := values[i].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	value, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	return value, true
}
