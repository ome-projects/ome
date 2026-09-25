package v1beta1convert

import (
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
	surge := int32(7)
	in := &v1beta1.InstanceOperation{
		ID:              "op-abc-123",
		Type:            v1beta1.InstanceOperationMigrate,
		Step:            "Drain",
		StartedAt:       started,
		LastProgressAt:  progress,
		Deadline:        deadline,
		RetryCount:      3,
		TargetRevision:  "rev-7",
		Reason:          "node-fragmentation",
		SurgeIndex:      &surge,
		FromNode:        "node-a",
		HintTargetNodes: []string{"node-x", "node-y"},
		RequestUUID:     "uuid-deadbeef",
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
