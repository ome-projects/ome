package coordination

import (
	"fmt"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The group surge budget on an engine of gangs. A gang cannot swap one pod,
// so its surge builds the whole replacement gang as a row of its own at the
// first free index, beside its source: the source row carries the attempt at
// the Surge step and names the replacement's index, the replacement row
// carries the attempt at the GangSurgeTarget step until the old gang leaves
// and the replacement is promoted in its place. While a pair stands the
// replica publishes one row more than its spec asks for. The budget is a
// share of the count the spec asks for, so the extra row must not widen it:
// 4 gangs under the default quarter surge one at a time.

// gangRollFixture is one engine of gangs mid-roll under SurgeThenDrain, as
// its replica publishes it: desired gangs of one leader and one worker,
// promoted of them already replaced (the replacement row Ready on the new
// version at the index it was built at, the old row gone), open of them
// being replaced now (the pair described above, the replacement's pods not
// yet Ready), and failed rows whose pods died with no attempt open. Rows not
// listed are Ready on the old version. promotedUnready marks the promoted
// gangs' pods as restarting, so the member reads Waiting.
type gangRollFixture struct {
	desired         int32
	promoted        int32
	open            int32
	failed          int32
	promotedUnready bool
}

// gangPods is the pods of one gang here: one leader and one worker.
const gangPods int32 = 2

// gangRollRows builds the rows in index order. Replacement indices are
// taken from the desired count upward, as the engine takes the first free
// index, so the rows of a roll with promoted gangs start at promoted.
func gangRollRows(f gangRollFixture) []v1beta1.OMENativeInstanceStatus {
	target, old := batchRevision(v1beta1.EngineComponent, "target"), batchRevision(v1beta1.EngineComponent, "old")
	var rows []v1beta1.OMENativeInstanceStatus
	oldRow := func(index int32) v1beta1.OMENativeInstanceStatus {
		return v1beta1.OMENativeInstanceStatus{Index: index, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: old,
			PodCount: gangPods, ReadyPodCount: gangPods, ServingPodCount: gangPods, AvailablePodCount: gangPods}
	}
	next := f.promoted
	for i := int32(0); i < f.open; i++ {
		source := oldRow(next)
		source.Phase = v1beta1.OMENativeInstanceUpdating
		source.TargetRevision = target
		source.Operation = &v1beta1.InstanceOperation{ID: fmt.Sprintf("gangsurge-%d", next), Type: v1beta1.InstanceOperationUpdate,
			Step: workloadtypes.UpdateStepSurge, TargetRevision: target, SurgeIndex: ptr.To(f.desired + f.promoted + i)}
		rows = append(rows, source)
		next++
	}
	for i := int32(0); i < f.failed; i++ {
		rows = append(rows, v1beta1.OMENativeInstanceStatus{Index: next, Phase: v1beta1.OMENativeInstanceFailed, RunningRevision: old})
		next++
	}
	for ; next < f.desired; next++ {
		rows = append(rows, oldRow(next))
	}
	for i := int32(0); i < f.promoted; i++ {
		ready := gangPods
		if f.promotedUnready {
			ready = gangPods - 1
		}
		rows = append(rows, v1beta1.OMENativeInstanceStatus{Index: f.desired + i, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: target,
			PodCount: gangPods, ReadyPodCount: ready, ServingPodCount: ready, AvailablePodCount: ready})
	}
	for i := int32(0); i < f.open; i++ {
		k := f.desired + f.promoted + i
		rows = append(rows, v1beta1.OMENativeInstanceStatus{Index: k, Phase: v1beta1.OMENativeInstanceCreating, TargetRevision: target, PodCount: gangPods,
			Operation: &v1beta1.InstanceOperation{ID: fmt.Sprintf("gangsurgetarget-%d", k), Type: v1beta1.InstanceOperationUpdate,
				Step: workloadtypes.UpdateStepGangSurgeTarget, TargetRevision: target}})
	}
	return rows
}

// gangRollReplica is the engine's InferenceReplica for the fixture: the
// desired count, a gang of one leader and one worker, SurgeThenDrain with
// its own maxSurge of 2 (the group's quarter is the stricter cap), and the
// counters the replica controller derives from the rows.
func gangRollReplica(f gangRollFixture) *v1beta1.InferenceReplica {
	eng := v1beta1.EngineComponent
	target, old := batchRevision(eng, "target"), batchRevision(eng, "old")
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: batchNamespace, Name: batchServiceName + "-" + string(eng)},
		Spec: v1beta1.InferenceReplicaSpec{
			Component: eng,
			Replicas:  ptr.To(f.desired),
			Runners:   []v1beta1.Runner{{Name: v1beta1.RunnerNameLeader, Size: 1}, {Name: v1beta1.RunnerNameWorker, Size: 1}},
			Lifecycle: &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{
				Type:          v1beta1.UpdateStrategySurgeThenDrain,
				RollingUpdate: &v1beta1.RollingUpdate{MaxSurge: ptr.To(intstr.FromInt32(2))},
			}},
		},
		Status: v1beta1.InferenceReplicaStatus{CurrentRevision: old, UpdateRevision: target, InstanceStatuses: gangRollRows(f)},
	}
	ir.Status.Replicas = int32(len(ir.Status.InstanceStatuses))
	promotedReady := f.promoted
	if f.promotedUnready {
		promotedReady = 0
	}
	ir.Status.UpdatedReplicas = f.promoted
	ir.Status.UpdatedReadyReplicas = promotedReady
	ir.Status.ServingReplicas = f.desired - f.promoted - f.failed + promotedReady
	ir.Status.ReadyReplicas = ir.Status.ServingReplicas
	return ir
}

// gangRollPhase is the phase the coordination pass computes for the fixture
// under the named policy: the old gangs' pods beside the promoted and the
// replacement gangs' pods, a failed gang with none.
func gangRollPhase(policy v1beta1.CoordinationPolicy, f gangRollFixture) v1beta1.CoordinationPhase {
	eng := v1beta1.EngineComponent
	ir := gangRollReplica(f)
	pods := map[string]int32{
		batchRevision(eng, "old"):    (f.desired - f.promoted - f.failed) * gangPods,
		batchRevision(eng, "target"): (f.promoted + f.open) * gangPods,
	}
	obs := GroupObservation{
		Group:      ResolvedGroup{Name: "0", Policy: policy, Components: []v1beta1.ComponentType{eng}},
		Components: map[v1beta1.ComponentType]ComponentObservation{eng: buildComponentObservation(&ir.Status, eng, pods, 0)},
	}
	return ComputeTransition(obs).Phase
}

// gangRollService is the service the fixture rolls in: a group of the engine
// alone that names no budget, blueGreen or rollingUpdate, with a run open.
func gangRollService(policy v1beta1.CoordinationPolicy) *v1beta1.InferenceService {
	return batchService(batchGroupFixture{components: []v1beta1.ComponentType{v1beta1.EngineComponent}, blueGreen: policy == v1beta1.CoordinationPolicyBlueGreen}, nil)
}

func gangRollClient(t *testing.T, f gangRollFixture) client.Client {
	t.Helper()
	return fakeclient.New(t, fakeclient.WithObjects(gangRollReplica(f)))
}

// expectSurgeHeld asks the gate stack about one more engine surge with
// inFlight surges already taken this pass and requires the Budget hold with
// the given numbers.
func expectSurgeHeld(t *testing.T, isvc *v1beta1.InferenceService, c client.Client, inFlight int32, want string) {
	t.Helper()
	allowed, gate, reason := batchGate(t, isvc, c, v1beta1.EngineComponent, workloadtypes.UpdateStrategySurgeThenDrain, inFlight)
	if allowed || gate != v1beta1.RolloutHoldGateBudget || !strings.Contains(reason, want) {
		t.Errorf("surge with %d open this pass: allowed=%v gate=%q reason=%q, want the Budget hold %q", inFlight, allowed, gate, reason, want)
	}
}

// expectSurgeAdmitted is the admitted counterpart.
func expectSurgeAdmitted(t *testing.T, isvc *v1beta1.InferenceService, c client.Client, inFlight int32) {
	t.Helper()
	if allowed, gate, reason := batchGate(t, isvc, c, v1beta1.EngineComponent, workloadtypes.UpdateStrategySurgeThenDrain, inFlight); !allowed {
		t.Errorf("surge with %d open this pass: held under %q with %q, want it admitted", inFlight, gate, reason)
	}
}

// TestEvaluateUpdateGate_AGangSurgeRowDoesNotWidenTheGroupSurgeBudget is the
// operator's story: 4 engine gangs of one leader and one worker under
// SurgeThenDrain, in a blueGreen group of the engine alone that names no
// budget, so the group's surge budget is a quarter of 4 rounded up, one gang.
// Version 2 is pushed. With no pair open the first surge is admitted and a
// second in the same pass is held. With one pair open the replica publishes
// 5 rows, and the next surge must still be held: the budget is a quarter of
// the 4 gangs the spec asks for, not of the 5 rows. With two pairs open (6
// rows) the next surge is held by the same budget of 1. Once the first old
// gang has left, the rows read 1, 2, 3 and 4 and the second pair opens at 5;
// the same hold applies. On 8 gangs the budget is 2 and the third surge is held
// while two pairs stand.
func TestEvaluateUpdateGate_AGangSurgeRowDoesNotWidenTheGroupSurgeBudget(t *testing.T) {
	isvc := gangRollService(v1beta1.CoordinationPolicyBlueGreen)
	t.Run("no-pair-open-4-rows", func(t *testing.T) {
		c := gangRollClient(t, gangRollFixture{desired: 4})
		expectSurgeAdmitted(t, isvc, c, 0)
		expectSurgeHeld(t, isvc, c, 1, "surge budget 1 exhausted (current 0, in-flight 1, would become 2)")
	})
	t.Run("one-pair-open-5-rows", func(t *testing.T) {
		c := gangRollClient(t, gangRollFixture{desired: 4, open: 1})
		expectSurgeHeld(t, isvc, c, 0, "surge budget 1 exhausted (current 1, in-flight 0, would become 2)")
	})
	t.Run("two-pairs-open-6-rows", func(t *testing.T) {
		c := gangRollClient(t, gangRollFixture{desired: 4, open: 2})
		expectSurgeHeld(t, isvc, c, 0, "surge budget 1 exhausted (current 2, in-flight 0, would become 3)")
	})
	t.Run("one-promoted-one-pair-open-5-rows", func(t *testing.T) {
		c := gangRollClient(t, gangRollFixture{desired: 4, promoted: 1, open: 1})
		expectSurgeHeld(t, isvc, c, 0, "surge budget 1 exhausted (current 1, in-flight 0, would become 2)")
	})
	t.Run("eight-gangs-two-pairs-open-10-rows", func(t *testing.T) {
		c := gangRollClient(t, gangRollFixture{desired: 8, open: 2})
		expectSurgeHeld(t, isvc, c, 0, "surge budget 2 exhausted (current 2, in-flight 0, would become 3)")
	})
}

// TestCheckSurge_PercentBudgetIsSizedByTheDesiredCountNotTheRows asks the
// surge budget gate alone, off the same replica: the quarter is scaled
// against the 4 gangs the spec asks for, so one open pair (5 rows) spends it,
// and a replica that records no desired count is sized by its rows.
func TestCheckSurge_PercentBudgetIsSizedByTheDesiredCountNotTheRows(t *testing.T) {
	isvc := gangRollService(v1beta1.CoordinationPolicyBlueGreen)
	ask := func(t *testing.T, ir *v1beta1.InferenceReplica, inFlight int32) (bool, string) {
		t.Helper()
		c := fakeclient.New(t, fakeclient.WithObjects(ir))
		return ResolveGateContext(t.Context(), c, isvc, v1beta1.EngineComponent).CheckSurge(inFlight)
	}
	t.Run("one-pair-open-spends-the-quarter-of-four", func(t *testing.T) {
		allowed, reason := ask(t, gangRollReplica(gangRollFixture{desired: 4, open: 1}), 0)
		if allowed || !strings.Contains(reason, "surge budget 1 exhausted (current 1, in-flight 0, would become 2)") {
			t.Fatalf("allowed=%v reason=%q, want the budget of one spent by the open pair", allowed, reason)
		}
	})
	t.Run("no-desired-count-recorded-sizes-by-the-rows", func(t *testing.T) {
		ir := gangRollReplica(gangRollFixture{desired: 4, open: 1})
		ir.Spec.Replicas = nil
		if allowed, reason := ask(t, ir, 0); !allowed {
			t.Fatalf("held with %q, want the quarter of the 5 rows, 2, to admit the second surge", reason)
		}
	})
}

// TestEvaluateUpdateGate_SurgeBudgetHoldsWhateverPhaseTheGroupReads builds
// the pictures of the gang roll in which the group reads each rolling phase
// and Failed, checks the phase off the same replica the gate reads, and
// requires the budget hold on the next surge in every one: a pair open with
// nothing promoted (Surging), a promoted gang restarting beside an open pair
// (Waiting), a promoted gang beside an open pair under blueGreen (Shifting)
// and under rollingUpdate (ScalingDown), and a gang whose pods died beside an
// open pair (Failed).
func TestEvaluateUpdateGate_SurgeBudgetHoldsWhateverPhaseTheGroupReads(t *testing.T) {
	blueGreen, rolling := v1beta1.CoordinationPolicyBlueGreen, v1beta1.CoordinationPolicyRollingUpdate
	cases := []struct {
		name    string
		policy  v1beta1.CoordinationPolicy
		fixture gangRollFixture
		phase   v1beta1.CoordinationPhase
	}{
		{"Surging", blueGreen, gangRollFixture{desired: 4, open: 1}, v1beta1.CoordinationPhaseSurging},
		{"Waiting", blueGreen, gangRollFixture{desired: 4, promoted: 1, open: 1, promotedUnready: true}, v1beta1.CoordinationPhaseWaiting},
		{"Shifting", blueGreen, gangRollFixture{desired: 4, promoted: 1, open: 1}, v1beta1.CoordinationPhaseShifting},
		{"ScalingDown", rolling, gangRollFixture{desired: 4, promoted: 1, open: 1}, v1beta1.CoordinationPhaseScalingDown},
		{"Failed", blueGreen, gangRollFixture{desired: 4, open: 1, failed: 1}, v1beta1.CoordinationPhaseFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := gangRollPhase(tc.policy, tc.fixture); got != tc.phase {
				t.Fatalf("group phase %q, want %q", got, tc.phase)
			}
			expectSurgeHeld(t, gangRollService(tc.policy), gangRollClient(t, tc.fixture), 0, "surge budget 1 exhausted (current 1, in-flight 0, would become 2)")
		})
	}
}
