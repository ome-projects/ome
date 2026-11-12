package coordination

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The batch step: the members of a prefill/decode group roll in batches of
// each member's own step, and nobody opens batch b+1 until everybody's
// batch b serves. The drain-first tables use 6 prefill pods rolled 3 at a
// time beside 2 decode pods rolled 1 at a time, both under RecreatePod; the
// surge tables use 12 prefill beside 8 decode, both SurgeThenDrain under
// the default budget, so the steps are 3 and 2.

const batchServiceName, batchNamespace = "svc", "ns"

// batchRevision names a Component's revision the way the replica records
// it; "target" is the revision forwardTarget builds for the gate.
func batchRevision(comp v1beta1.ComponentType, which string) string {
	return batchServiceName + "-" + string(comp) + "-" + which
}

// batchMemberFixture is one member of the group as its InferenceReplica
// publishes it: the desired count, its own maxUnavailable, its strategy and
// shape, and its Instances by what the roll has done to them. Instances not
// listed are Ready on the old revision.
type batchMemberFixture struct {
	desired     int32
	step        string // the member's own maxUnavailable
	surge       string // the member's own maxSurge
	strategy    v1beta1.UpdateStrategyType
	gang        bool
	promoted    int32 // Ready on the target revision
	inFlight    int32 // a recreate toward the target is open
	inPlace     int32 // an in-place patch toward the target is open
	surging     int32 // a surge toward the target is open, the source still serving
	handingOver int32 // a surge whose replacement serves is draining its source
	failed      int32 // the attempt toward the target ended Failed
	// Two attempts the escalation stopped with their pods kept, each at the
	// step it records for that and with ImagePullBackOff as the last
	// failure: an in-place attempt whose old pod still serves, Ready over
	// it, and a recreate attempt whose new pod set serves nothing, Failed.
	// Either counts on the retry record when the escalation stops it.
	parked  int32
	stopped int32
	// failedSurge is a surge whose replacement failed while the old pod
	// serves, as the replica publishes it: Ready, no operation, the target
	// kept; failedGangSurge is the gang form, the attempt kept at Surge
	// while the replacement gang is retired.
	failedSurge     int32
	failedGangSurge int32
	creating        int32 // created on the target by a scale-up, not serving yet
	retiring        int32 // Ready on the old revision beyond the desired count: rows a scale-down has not yet retired
	partition       int32 // the member's own rollingUpdate.partition: that many Instances stay on the old revision
	held            int32 // the member's retry record for the target is Held after that many failed attempts
	// backoff is the member's retry record for the target in Backoff after
	// that many failed attempts the schedule counted: the next start waits
	// on it.
	backoff int32
	// unplaceable is a recreate whose pod the scheduler cannot place: the
	// attempt open at its drain step with the Unschedulable waiting token.
	unplaceable int32
	// graceEnded is an attempt the placement grace ended with nothing
	// counted on the record: Failed with no operation and the target kept
	// under a drain-first strategy, Ready over the old pod with the target
	// kept under SurgeThenDrain, the last failure Unschedulable either way.
	graceEnded int32
}

// heldRecordReason is the last failure the fixtures' Held retry records name.
const heldRecordReason = "the image cannot be pulled"

// stoppedAttemptReason is the last failure a stopped attempt's row records:
// the kubelet gave up pulling the new image.
const stoppedAttemptReason = "ImagePullBackOff"

// movable is the number of Instances the roll may move: the desired count
// less the Instances the partition holds on their revision.
func (m batchMemberFixture) movable() int32 {
	if m.partition >= m.desired {
		return 0
	}
	return m.desired - m.partition
}

func batchReplica(comp v1beta1.ComponentType, m batchMemberFixture) *v1beta1.InferenceReplica {
	target, old := batchRevision(comp, "target"), batchRevision(comp, "old")
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: batchNamespace, Name: batchServiceName + "-" + string(comp)},
		Spec: v1beta1.InferenceReplicaSpec{
			Component: comp,
			Replicas:  ptr.To(m.desired),
			Lifecycle: &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{Type: m.strategy}},
		},
		Status: v1beta1.InferenceReplicaStatus{CurrentRevision: old, UpdateRevision: target, Replicas: m.desired},
	}
	if m.step != "" || m.surge != "" || m.partition > 0 {
		ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate = &v1beta1.RollingUpdate{}
		if m.step != "" {
			ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable = ptr.To(intstr.Parse(m.step))
		}
		if m.surge != "" {
			ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge = ptr.To(intstr.Parse(m.surge))
		}
		if m.partition > 0 {
			ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.Partition = ptr.To(m.partition)
		}
	}
	if m.gang {
		ir.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameLeader, Size: 1}, {Name: v1beta1.RunnerNameWorker, Size: 2}}
	}
	attempt := func(step string) *v1beta1.InstanceOperation {
		return &v1beta1.InstanceOperation{ID: "attempt", Type: v1beta1.InstanceOperationUpdate, Step: step, TargetRevision: target}
	}
	var rows []v1beta1.OMENativeInstanceStatus
	add := func(n int32, row v1beta1.OMENativeInstanceStatus) {
		for i := int32(0); i < n; i++ {
			row.Index = int32(len(rows))
			if row.Operation != nil {
				op := *row.Operation
				row.Operation = &op
			}
			rows = append(rows, row)
		}
	}
	add(m.promoted, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceReady, RunningRevision: target, PodCount: 1, ServingPodCount: 1})
	add(m.inFlight, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: old, TargetRevision: target, Operation: attempt(workloadtypes.UpdateStepDrain)})
	add(m.inPlace, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: old, TargetRevision: target, PodCount: 1, Operation: attempt(workloadtypes.UpdateStepInPlace)})
	add(m.surging, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: old, TargetRevision: target, PodCount: 2, ServingPodCount: 1, Operation: attempt(workloadtypes.UpdateStepSurge)})
	add(m.handingOver, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: old, TargetRevision: target, PodCount: 2, ServingPodCount: 1, Operation: attempt(workloadtypes.UpdateStepSurgeDrain)})
	endedStep := workloadtypes.UpdateStepDrain
	if m.strategy == v1beta1.UpdateStrategySurgeThenDrain {
		endedStep = workloadtypes.UpdateStepSurge
	}
	add(m.failed, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceFailed, RunningRevision: old, TargetRevision: target, Operation: attempt(endedStep)})
	imagePull := &v1beta1.InstanceTermination{Reason: stoppedAttemptReason}
	add(m.parked, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceReady, RunningRevision: old, TargetRevision: target, PodCount: 1, ServingPodCount: 1, Operation: attempt(workloadtypes.UpdateStepParked), LastFailure: imagePull})
	add(m.stopped, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceFailed, RunningRevision: old, TargetRevision: target, PodCount: 1, Operation: attempt(workloadtypes.UpdateStepParked), LastFailure: imagePull})
	unplaced := attempt(workloadtypes.UpdateStepDrain)
	unplaced.Waiting = workloadtypes.WaitingReasonUnschedulable
	add(m.unplaceable, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: old, TargetRevision: target, PodCount: 1, Operation: unplaced})
	unschedulable := &v1beta1.InstanceTermination{Reason: workloadtypes.WaitingReasonUnschedulable}
	graceEndedServing := int32(0)
	if m.strategy == v1beta1.UpdateStrategySurgeThenDrain {
		graceEndedServing = m.graceEnded
		add(m.graceEnded, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceReady, RunningRevision: old, TargetRevision: target, PodCount: 2, ServingPodCount: 1, LastFailure: unschedulable})
	} else {
		add(m.graceEnded, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceFailed, RunningRevision: old, TargetRevision: target, PodCount: 1, LastFailure: unschedulable})
	}
	add(m.failedSurge, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceReady, RunningRevision: old, TargetRevision: target, PodCount: 2, ServingPodCount: 1})
	add(m.failedGangSurge, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceReady, RunningRevision: old, TargetRevision: target, PodCount: 2, ServingPodCount: 1, Operation: attempt(workloadtypes.UpdateStepSurge)})
	// A scale-up's new rows take the indices after the old ones, as the
	// engine creates them; a scale-down's not-yet-retired rows sit above the
	// desired count.
	untouched := m.desired - m.creating - int32(len(rows))
	add(untouched, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceReady, RunningRevision: old, PodCount: 1, ServingPodCount: 1})
	add(m.creating, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceCreating, TargetRevision: target, Operation: &v1beta1.InstanceOperation{ID: "create", Type: v1beta1.InstanceOperationCreate, TargetRevision: target}})
	add(m.retiring, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceReady, RunningRevision: old, PodCount: 1, ServingPodCount: 1})
	ir.Status.InstanceStatuses = rows
	if m.held > 0 {
		ir.Status.RetryBlocks = []v1beta1.RetryBlock{{TargetRevision: target, State: v1beta1.RetryBlockHeld, AttemptsStarted: m.held, Reason: heldRecordReason}}
	} else if m.backoff > 0 {
		ir.Status.RetryBlocks = []v1beta1.RetryBlock{{TargetRevision: target, State: v1beta1.RetryBlockBackoff, AttemptsStarted: m.backoff, Reason: heldRecordReason}}
	}
	ir.Status.UpdatedReplicas = m.promoted
	ir.Status.UpdatedReadyReplicas = m.promoted
	ir.Status.ServingReplicas = m.promoted + m.surging + m.handingOver + m.parked + m.failedSurge + m.failedGangSurge + graceEndedServing + untouched + m.retiring
	ir.Status.ReadyReplicas = ir.Status.ServingReplicas
	return ir
}

// batchGroupFixture is the group the members roll in: one rollingUpdate
// group over the listed Components (engine and decoder when empty) with the
// given group budgets (an empty one is left to the default), under
// maintainRatio at tolerance when set. The active run records every member
// moving from the old revision to the target.
type batchGroupFixture struct {
	maxUnavailable string
	maxSurge       string
	tolerance      *int32
	components     []v1beta1.ComponentType
	// blueGreen declares the group with the blueGreen progression instead
	// of rollingUpdate; the gates read the same replicas either way.
	blueGreen bool
	// recordedBeside are further groups declared beside the group above
	// when the run records the plan.
	recordedBeside []v1beta1.RolloutGroup
	// edit is the operator's change to the rollout block after the run
	// recorded the plan; nil leaves the spec as recorded.
	edit func(spec *v1beta1.RolloutSpec)
}

func batchService(g batchGroupFixture, members map[v1beta1.ComponentType]batchMemberFixture) *v1beta1.InferenceService {
	components := g.components
	if len(components) == 0 {
		components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	}
	group := v1beta1.RolloutGroup{
		Components:    components,
		RollingUpdate: &v1beta1.GroupRollingUpdate{},
	}
	policy := v1beta1.CoordinationPolicyRollingUpdate
	if g.blueGreen {
		group.RollingUpdate, group.BlueGreen = nil, &v1beta1.GroupBlueGreen{}
		policy = v1beta1.CoordinationPolicyBlueGreen
	}
	if g.maxUnavailable != "" {
		group.RollingUpdate.MaxUnavailable = ptr.To(intstr.Parse(g.maxUnavailable))
	}
	if g.maxSurge != "" {
		group.RollingUpdate.MaxSurge = ptr.To(intstr.Parse(g.maxSurge))
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: batchServiceName, Namespace: batchNamespace},
		Spec:       v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{group}}},
	}
	if g.tolerance != nil {
		isvc.Spec.Rollout.Groups[0].MaintainRatio = &v1beta1.MaintainRatio{Tolerance: g.tolerance}
		original := map[v1beta1.ComponentType]int32{}
		for comp, m := range members {
			original[comp] = m.desired
		}
		isvc.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{Groups: []v1beta1.RolloutCoordinationGroupStatus{{
			Name:          "0",
			Components:    components,
			Policy:        policy,
			ObservedRatio: &v1beta1.RolloutCoordinationRatio{Original: original},
		}}}
	}
	isvc.Spec.Rollout.Groups = append(isvc.Spec.Rollout.Groups, g.recordedBeside...)
	isvc = pinActiveRun(isvc)
	for _, comp := range components {
		isvc.Status.Rollout.ActiveRun.TargetRevisions = append(isvc.Status.Rollout.ActiveRun.TargetRevisions,
			v1beta1.RolloutRunTarget{Component: comp, Revision: "target", StableRevision: "old"})
	}
	if g.edit != nil {
		g.edit(isvc.Spec.Rollout)
	}
	return isvc
}

func batchClient(t *testing.T, members map[v1beta1.ComponentType]batchMemberFixture) client.Client {
	t.Helper()
	var objs []client.Object
	for comp, m := range members {
		objs = append(objs, batchReplica(comp, m))
	}
	return fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(objs...).Build()
}

// batchGate asks the production decision site about one more start of comp
// on the given mechanism, with inFlight starts already taken this wake-up:
// surges for SurgeThenDrain, pods out of rotation otherwise.
func batchGate(t *testing.T, isvc *v1beta1.InferenceService, c client.Client, comp v1beta1.ComponentType, mechanism workloadtypes.UpdateStrategyType, inFlight int32) (bool, v1beta1.RolloutHoldGate, string) {
	t.Helper()
	return batchGateWith(t, isvc, c, comp, mechanism, inFlight, GroupDefaults{})
}

// batchGateWith is batchGate under the operator's group defaults, the
// prefill/decode rules among them.
func batchGateWith(t *testing.T, isvc *v1beta1.InferenceService, c client.Client, comp v1beta1.ComponentType, mechanism workloadtypes.UpdateStrategyType, inFlight int32, defaults GroupDefaults) (bool, v1beta1.RolloutHoldGate, string) {
	t.Helper()
	var inFlightSurge, inFlightUnavail int32
	if mechanism == workloadtypes.UpdateStrategySurgeThenDrain {
		inFlightSurge = inFlight
	} else {
		inFlightUnavail = inFlight
	}
	return EvaluateUpdateGate(context.Background(), c, isvc, comp, "", forwardTarget(isvc, comp), nil, defaults, mechanism, inFlightSurge, inFlightUnavail)
}

// keepRatio turns the ratio rule on, with the operator's default tolerance.
func keepRatio(tolerance int32) GroupDefaults {
	return GroupDefaults{RatioTolerancePercent: ptr.To(tolerance), PrefillDecode: PrefillDecodeRules{KeepRatio: true}}
}

// keepRatioDecoderFirst turns the ratio rule on and names the decoder as
// the side that moves first inside every batch.
func keepRatioDecoderFirst(tolerance int32) GroupDefaults {
	d := keepRatio(tolerance)
	d.PrefillDecode.FirstSide = v1beta1.DecoderComponent
	return d
}

// rollPicture is how many Instances of each side have moved, or are
// moving, to version 2 after one round of a roll.
type rollPicture struct{ engine, decoder int32 }

func (p rollPicture) String() string { return fmt.Sprintf("%d:%d", p.engine, p.decoder) }

// pictures renders a list of rollPictures for a failure message.
func pictures(ps []rollPicture) string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, p.String())
	}
	return strings.Join(out, " ")
}

// offTheLine is how far a picture sits from the two sides' shared line,
// in decode Instances: the gap between the engine's share and the
// decoder's share of their desired counts, scaled to the decoder's side.
func offTheLine(p rollPicture, engines, decoders int32) float64 {
	gap := float64(p.engine)/float64(engines) - float64(p.decoder)/float64(decoders)
	if gap < 0 {
		gap = -gap
	}
	return gap * float64(decoders)
}

// driveBatchRoll walks a prefill/decode roll through the production gate
// the way the dispatcher does, one round at a time: each side, decoder
// first, opens starts until the gate holds it, every admitted start lands
// on its row as an open attempt before the next question, and when neither
// side can start anything more the open attempts serve and the picture is
// recorded. An Instance the partition holds is never asked about, as the
// dispatcher never selects one. It returns the pictures and whether a
// round moved nothing.
func driveBatchRoll(t *testing.T, group batchGroupFixture, engine, decoder batchMemberFixture, mechanism workloadtypes.UpdateStrategyType, defaults GroupDefaults) (rounds []rollPicture, stalled bool) {
	t.Helper()
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	members := map[v1beta1.ComponentType]*batchMemberFixture{eng: &engine, dec: &decoder}
	for round := 0; round < 64; round++ {
		if engine.promoted == engine.movable() && decoder.promoted == decoder.movable() {
			return rounds, false
		}
		moved := false
		for _, comp := range []v1beta1.ComponentType{dec, eng} {
			m := members[comp]
			for m.promoted+m.inFlight+m.surging < m.movable() {
				fixtures := map[v1beta1.ComponentType]batchMemberFixture{eng: engine, dec: decoder}
				isvc := batchService(group, fixtures)
				allowed, _, _ := batchGateWith(t, isvc, batchClient(t, fixtures), comp, mechanism, 0, defaults)
				if !allowed {
					break
				}
				if mechanism == workloadtypes.UpdateStrategySurgeThenDrain {
					m.surging++
				} else {
					m.inFlight++
				}
				moved = true
			}
		}
		if !moved {
			return rounds, true
		}
		for _, m := range members {
			m.promoted += m.inFlight + m.surging
			m.inFlight, m.surging = 0, 0
		}
		rounds = append(rounds, rollPicture{engine: engine.promoted, decoder: decoder.promoted})
	}
	t.Fatalf("the roll did not finish in 64 rounds: %s", pictures(rounds))
	return nil, true
}

// twelveEight is the 12 prefill beside 8 decode shape, both sides at a
// 10 percent budget of their own, under the given strategy.
func twelveEight(strategy v1beta1.UpdateStrategyType) (engine, decoder batchMemberFixture) {
	engine = batchMemberFixture{desired: 12, strategy: strategy}
	decoder = batchMemberFixture{desired: 8, strategy: strategy}
	if strategy == v1beta1.UpdateStrategySurgeThenDrain {
		engine.surge, decoder.surge = "10%", "10%"
	} else {
		engine.step, decoder.step = "10%", "10%"
	}
	return engine, decoder
}

// TestEvaluateUpdateGate_KeepRatioSizesEachBatchToTheTolerance drives 12
// prefill beside 8 decode, both at a 10 percent budget, through the gate
// under the ratio rule at a 10 percent tolerance. The decoder steps one
// Instance per batch; the prefill side's batches are 2, 2, 1, 2, 1, 2, 1, 1,
// so the picture reads 2:1, 4:2, 5:3, 7:4, 8:5, 10:6, 11:7, 12:8 and never
// sits more than its tolerance, 0.8 decode Instances, off the 3:2 line
// (0.67 at the widest picture). The tolerance comes
// from the group's maintainRatio in one case and from the operator's
// configured default, the group naming none, in the other; the rule holds
// on the drain-first and the surge path alike.
func TestEvaluateUpdateGate_KeepRatioSizesEachBatchToTheTolerance(t *testing.T) {
	want := []rollPicture{{2, 1}, {4, 2}, {5, 3}, {7, 4}, {8, 5}, {10, 6}, {11, 7}, {12, 8}}
	cases := []struct {
		name      string
		strategy  v1beta1.UpdateStrategyType
		group     batchGroupFixture
		mechanism workloadtypes.UpdateStrategyType
		defaults  GroupDefaults
	}{
		{
			name: "RecreatePod, the group names a tolerance of 10", strategy: v1beta1.UpdateStrategyRecreatePod,
			group: batchGroupFixture{maxUnavailable: "10%", tolerance: ptr.To(int32(10))}, mechanism: workloadtypes.UpdateStrategyRecreatePod,
			defaults: GroupDefaults{PrefillDecode: PrefillDecodeRules{KeepRatio: true}},
		},
		{
			name: "RecreatePod, the group names no ratio and the operator's default is 10", strategy: v1beta1.UpdateStrategyRecreatePod,
			group: batchGroupFixture{maxUnavailable: "10%"}, mechanism: workloadtypes.UpdateStrategyRecreatePod,
			defaults: keepRatio(10),
		},
		{
			name: "SurgeThenDrain at a surge budget of 10 percent", strategy: v1beta1.UpdateStrategySurgeThenDrain,
			group: batchGroupFixture{maxUnavailable: "0", maxSurge: "10%"}, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			defaults: keepRatio(10),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine, decoder := twelveEight(tc.strategy)
			got, stalled := driveBatchRoll(t, tc.group, engine, decoder, tc.mechanism, tc.defaults)
			if stalled {
				t.Fatalf("the roll stalled after %s", pictures(got))
			}
			for _, p := range got {
				if off := offTheLine(p, 12, 8); off > 0.8001 {
					t.Errorf("picture %s sits %.2f decode Instances off the 3:2 line; a tolerance of 10 allows 0.8", p, off)
				}
			}
			if pictures(got) != pictures(want) {
				t.Errorf("pictures after each round:\n got %s\nwant %s", pictures(got), pictures(want))
			}
		})
	}
}

// batchPacings are the two pacings the batch step applies under alike.
var batchPacings = []struct {
	name      string
	tolerance *int32
}{
	{name: "PerComponent"},
	{name: "maintainRatio-50", tolerance: ptr.To(int32(50))},
}

type batchCase struct {
	name       string
	group      string // the group's maxUnavailable
	groupSurge string // the group's maxSurge
	engine     batchMemberFixture
	decoder    batchMemberFixture
	comp       v1beta1.ComponentType
	mechanism  workloadtypes.UpdateStrategyType // RecreatePod when empty
	inFlight   int32
	wantHold   bool
	wantReason string
}

// runBatchCases asks the gate about every case under every pacing and
// holds the answer to the case's expectation.
func runBatchCases(t *testing.T, cases []batchCase) {
	t.Helper()
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	for _, pacing := range batchPacings {
		for _, tc := range cases {
			t.Run(pacing.name+"/"+tc.name, func(t *testing.T) {
				mechanism := tc.mechanism
				if mechanism == "" {
					mechanism = workloadtypes.UpdateStrategyRecreatePod
				}
				members := map[v1beta1.ComponentType]batchMemberFixture{eng: tc.engine, dec: tc.decoder}
				isvc := batchService(batchGroupFixture{maxUnavailable: tc.group, maxSurge: tc.groupSurge, tolerance: pacing.tolerance}, members)
				allowed, gate, reason := batchGate(t, isvc, batchClient(t, members), tc.comp, mechanism, tc.inFlight)
				if tc.wantHold == allowed {
					t.Fatalf("allowed=%v, want hold=%v (gate=%q reason=%q)", allowed, tc.wantHold, gate, reason)
				}
				if !tc.wantHold {
					return
				}
				if gate != v1beta1.RolloutHoldGateRatio {
					t.Errorf("gate=%q, want %q (reason=%q)", gate, v1beta1.RolloutHoldGateRatio, reason)
				}
				if reason != tc.wantReason {
					t.Errorf("reason=%q, want %q", reason, tc.wantReason)
				}
			})
		}
	}
}

// TestEvaluateUpdateGate_BatchStepPacesDrainFirstMembers holds the batch step
// to the worked examples: 6 prefill at 3 beside 2 decode at 1, 8 at 2 beside
// 4 at 1, and 7 at 3 with a last batch of one. A row reads "with these
// Instances promoted, moving or ended, may comp open one more drain?", and
// a hold is reported under the Ratio gate with the batch reason.
func TestEvaluateUpdateGate_BatchStepPacesDrainFirstMembers(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate := v1beta1.UpdateStrategyRecreatePod
	six := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.step, m.strategy = 6, "3", recreate
		return m
	}
	two := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.step, m.strategy = 2, "1", recreate
		return m
	}
	eight := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.step, m.strategy = 8, "2", recreate
		return m
	}
	four := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.step, m.strategy = 4, "1", recreate
		return m
	}
	seven := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.step, m.strategy = 7, "3", recreate
		return m
	}
	cases := []batchCase{
		{
			name: "6:2 at 3/1 decode fast: the decoder's second drain waits for the first prefill batch", group: "3",
			engine: six(batchMemberFixture{inFlight: 3}), decoder: two(batchMemberFixture{promoted: 1}), comp: dec,
			wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (0 of 3 serving)",
		},
		{
			name: "6:2 at 3/1 decode fast: one new prefill serving is not the batch", group: "3",
			engine: six(batchMemberFixture{promoted: 1, inFlight: 2}), decoder: two(batchMemberFixture{promoted: 1}), comp: dec,
			wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (1 of 3 serving)",
		},
		{
			name: "6:2 at 3/1 prefill fast: the engine's fourth drain waits for the first decode", group: "3",
			engine: six(batchMemberFixture{promoted: 3}), decoder: two(batchMemberFixture{inFlight: 1}), comp: eng,
			wantHold: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving)",
		},
		{
			name: "6:2 at 3/1 at the step boundary: the engine's fourth drain is admitted", group: "3",
			engine: six(batchMemberFixture{promoted: 3}), decoder: two(batchMemberFixture{promoted: 1}), comp: eng,
		},
		{
			name: "6:2 at 3/1 at the step boundary: the decoder's second drain is admitted", group: "3",
			engine: six(batchMemberFixture{promoted: 3}), decoder: two(batchMemberFixture{promoted: 1}), comp: dec,
		},
		{
			name: "6:2 at 3/1 first batch: the third prefill drain of one wake-up is admitted", group: "3",
			engine: six(batchMemberFixture{}), decoder: two(batchMemberFixture{}), comp: eng, inFlight: 2,
		},
		{
			name: "6:2 at 3/1 first batch: the decoder's first drain is admitted beside three prefill drains", group: "3",
			engine: six(batchMemberFixture{inFlight: 3}), decoder: two(batchMemberFixture{}), comp: dec,
		},
		{
			name: "6:2 at 3/1 first batch: a fourth prefill drain opens batch 2 before batch 1 serves", group: "3",
			engine: six(batchMemberFixture{inFlight: 3}), decoder: two(batchMemberFixture{inFlight: 1}), comp: eng,
			wantHold: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving)",
		},
		{
			name: "8:4 at 2/1 decode fast: the decoder's second drain waits for two prefill", group: "2",
			engine: eight(batchMemberFixture{inFlight: 2}), decoder: four(batchMemberFixture{promoted: 1}), comp: dec,
			wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (0 of 2 serving)",
		},
		{
			name: "8:4 at 2/1 prefill fast: the engine's third drain waits for the first decode", group: "2",
			engine: eight(batchMemberFixture{promoted: 2}), decoder: four(batchMemberFixture{inFlight: 1}), comp: eng,
			wantHold: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving)",
		},
		{
			name: "8:4 at 2/1 at the step boundary: the engine's third drain is admitted", group: "2",
			engine: eight(batchMemberFixture{promoted: 2}), decoder: four(batchMemberFixture{promoted: 1}), comp: eng,
		},
		{
			name: "8:4 at 2/1 at the step boundary: the decoder's second drain is admitted", group: "2",
			engine: eight(batchMemberFixture{promoted: 2}), decoder: four(batchMemberFixture{promoted: 1}), comp: dec,
		},
		{
			name: "8:4 at 2/1 third decode batch waits for the second prefill batch", group: "2",
			engine: eight(batchMemberFixture{promoted: 3, inFlight: 1}), decoder: four(batchMemberFixture{promoted: 2}), comp: dec,
			wantHold: true, wantReason: "decoder batch 3 waits for engine batch 2 to serve (3 of 4 serving)",
		},
		{
			name: "7 prefill at 3: the last batch of one is admitted once the decoder is done", group: "3",
			engine: seven(batchMemberFixture{promoted: 6}), decoder: two(batchMemberFixture{promoted: 2}), comp: eng,
		},
		{
			name: "7 prefill at 3: the last batch of one waits for the decoder's second batch", group: "3",
			engine: seven(batchMemberFixture{promoted: 6}), decoder: two(batchMemberFixture{promoted: 1, inFlight: 1}), comp: eng,
			wantHold: true, wantReason: "engine batch 3 waits for decoder batch 2 to serve (1 of 2 serving)",
		},
		{
			name: "7 prefill at 3: the decoder's second drain waits for the first batch of three", group: "3",
			engine: seven(batchMemberFixture{promoted: 2, inFlight: 1}), decoder: two(batchMemberFixture{promoted: 1}), comp: dec,
			wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (2 of 3 serving)",
		},
		{
			name: "a prefill whose attempt ended Failed and waits on the schedule counts as settled: the decoder goes on", group: "3",
			engine: six(batchMemberFixture{promoted: 2, failed: 1, backoff: 1}), decoder: two(batchMemberFixture{promoted: 1}), comp: dec,
		},
		{
			name: "a prefill batch half served and half failed still waits for its last Instance", group: "3",
			engine: six(batchMemberFixture{promoted: 1, failed: 1, inFlight: 1, backoff: 1}), decoder: two(batchMemberFixture{promoted: 1}), comp: dec,
			wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (1 of 3 serving, 1 failed)",
		},
		{
			name: "a parked prefill attempt whose record stands counts as settled: the decoder goes on", group: "3",
			engine: six(batchMemberFixture{promoted: 2, parked: 1, backoff: 1}), decoder: two(batchMemberFixture{promoted: 1}), comp: dec,
		},
		{
			name: "a finished decoder no longer paces the engine", group: "3",
			engine: six(batchMemberFixture{promoted: 4, inFlight: 1}), decoder: two(batchMemberFixture{promoted: 2}), comp: eng,
		},
		{
			name: "a finished engine no longer paces the decoder", group: "3",
			engine: six(batchMemberFixture{promoted: 6}), decoder: two(batchMemberFixture{promoted: 1}), comp: dec,
		},
		{
			name: "a scale-up from 2 to 6 prefill mid-roll raises the first prefill batch to three", group: "3",
			engine: six(batchMemberFixture{promoted: 2, creating: 4}), decoder: two(batchMemberFixture{promoted: 1}), comp: dec,
			wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (2 of 3 serving)",
		},
		{
			name: "before the scale-up two prefill were the whole first batch", group: "3",
			engine: batchMemberFixture{desired: 2, step: "3", strategy: recreate, promoted: 2}, decoder: two(batchMemberFixture{promoted: 1}), comp: dec,
		},
		{
			name: "a scaled-up prefill that serves completes the first batch", group: "3",
			engine: six(batchMemberFixture{promoted: 3, creating: 3}), decoder: two(batchMemberFixture{promoted: 1}), comp: dec,
		},
	}
	for _, pacing := range batchPacings {
		for _, tc := range cases {
			t.Run(pacing.name+"/"+tc.name, func(t *testing.T) {
				members := map[v1beta1.ComponentType]batchMemberFixture{eng: tc.engine, dec: tc.decoder}
				isvc := batchService(batchGroupFixture{maxUnavailable: tc.group, tolerance: pacing.tolerance}, members)
				allowed, gate, reason := batchGate(t, isvc, batchClient(t, members), tc.comp, workloadtypes.UpdateStrategyRecreatePod, tc.inFlight)
				if tc.wantHold == allowed {
					t.Fatalf("allowed=%v, want hold=%v (gate=%q reason=%q)", allowed, tc.wantHold, gate, reason)
				}
				if !tc.wantHold {
					return
				}
				if gate != v1beta1.RolloutHoldGateRatio {
					t.Errorf("gate=%q, want %q (reason=%q)", gate, v1beta1.RolloutHoldGateRatio, reason)
				}
				if reason != tc.wantReason {
					t.Errorf("reason=%q, want %q", reason, tc.wantReason)
				}
			})
		}
	}
}

// batchGroupPhase is the phase the coordination pass computes for the
// members as their replicas publish them, under RollingUpdate: every pod of
// a member is live, the old ones beside the promoted ones.
func batchGroupPhase(members map[v1beta1.ComponentType]batchMemberFixture, components ...v1beta1.ComponentType) v1beta1.CoordinationPhase {
	return batchGroupPhaseUnder(v1beta1.CoordinationPolicyRollingUpdate, members, components...)
}

// batchGroupPhaseUnder is batchGroupPhase under the named policy. An
// Instance a scale-up is still creating has no pod yet; one a scale-down has
// not yet retired keeps its old pod.
func batchGroupPhaseUnder(policy v1beta1.CoordinationPolicy, members map[v1beta1.ComponentType]batchMemberFixture, components ...v1beta1.ComponentType) v1beta1.CoordinationPhase {
	obs := GroupObservation{
		Group:      ResolvedGroup{Name: "0", Policy: policy, Components: components},
		Components: map[v1beta1.ComponentType]ComponentObservation{},
	}
	for _, comp := range components {
		m := members[comp]
		ir := batchReplica(comp, m)
		old := m.desired - m.promoted - m.creating + m.retiring
		pods := map[string]int32{batchRevision(comp, "old"): old, batchRevision(comp, "target"): m.promoted}
		obs.Components[comp] = buildComponentObservation(&ir.Status, comp, pods, 0)
	}
	return ComputeTransition(obs).Phase
}

// TestEvaluateUpdateGate_BatchStepReadsTheStepFromTheResizedCount is a
// resize between two batches of the reference roll: 12 prefill rolled 2 at
// a time beside 8 decode rolled 1 at a time under RecreatePod, the prefill
// side having served two batches (4 Instances) and the decode side one. The
// prefill side's next start waits for the decode side's second batch. Each
// side's step is read from the count its spec asks for now, and its batches
// are counted again from that step:
//
//   - scaled up to 24 prefill (12 Instances still being created), the step is
//     3 and the 4 started read as one full batch, so the batch step admits the
//     prefill side's next start; the group's unavailability budget then holds
//     it, because the 12 Instances the scale-up has not created yet count as
//     unavailable against the new count, until they serve;
//   - scaled down to 8 prefill (4 old Instances not yet retired), the step is
//     1, the 4 started read as four batches, and the prefill side waits for
//     the decode side's fourth batch while the decode side's own next start
//     is admitted.
//
// Under RollingUpdate the group reads ScalingDown throughout; under BlueGreen
// the same picture reads Shifting.
func TestEvaluateUpdateGate_BatchStepReadsTheStepFromTheResizedCount(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate := v1beta1.UpdateStrategyRecreatePod
	decoder := batchMemberFixture{desired: 8, step: "10%", strategy: recreate, promoted: 1}
	cases := []struct {
		name       string
		engine     batchMemberFixture
		wantGate   v1beta1.RolloutHoldGate // the gate that holds the prefill side's next start
		wantReason string
	}{
		{name: "before-the-resize", engine: batchMemberFixture{desired: 12, step: "10%", strategy: recreate, promoted: 4},
			wantGate: v1beta1.RolloutHoldGateRatio, wantReason: "engine batch 3 waits for decoder batch 2 to serve (1 of 2 serving)"},
		{name: "scaled-up-to-24", engine: batchMemberFixture{desired: 24, step: "10%", strategy: recreate, promoted: 4, creating: 12},
			wantGate: v1beta1.RolloutHoldGateBudget, wantReason: "unavailable budget 6 exhausted (current 12, in-flight 0, would become 13); counted as unavailable: Instances 12 to 23 still being created"},
		{name: "scaled-down-to-8", engine: batchMemberFixture{desired: 8, step: "10%", strategy: recreate, promoted: 4, retiring: 4},
			wantGate: v1beta1.RolloutHoldGateRatio, wantReason: "engine batch 5 waits for decoder batch 4 to serve (1 of 4 serving)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			members := map[v1beta1.ComponentType]batchMemberFixture{eng: tc.engine, dec: decoder}
			if got := batchGroupPhaseUnder(v1beta1.CoordinationPolicyRollingUpdate, members, eng, dec); got != v1beta1.CoordinationPhaseScalingDown {
				t.Fatalf("RollingUpdate group phase %q, want %q", got, v1beta1.CoordinationPhaseScalingDown)
			}
			if got := batchGroupPhaseUnder(v1beta1.CoordinationPolicyBlueGreen, members, eng, dec); got != v1beta1.CoordinationPhaseShifting {
				t.Fatalf("BlueGreen group phase %q, want %q", got, v1beta1.CoordinationPhaseShifting)
			}
			isvc := batchService(batchGroupFixture{tolerance: ptr.To(int32(20))}, members)
			c := batchClient(t, members)
			allowed, gate, reason := batchGate(t, isvc, c, eng, workloadtypes.UpdateStrategyRecreatePod, 0)
			if allowed || gate != tc.wantGate || reason != tc.wantReason {
				t.Fatalf("engine: allowed=%v gate=%q reason=%q, want a %s hold %q", allowed, gate, reason, tc.wantGate, tc.wantReason)
			}
			if allowed, gate, reason := batchGate(t, isvc, c, dec, workloadtypes.UpdateStrategyRecreatePod, 0); !allowed {
				t.Errorf("decoder: held under %q with %q, want its next start admitted", gate, reason)
			}
		})
	}
}

// TestEvaluateUpdateGate_BatchStepHoldsBetweenBatchesWhileTheGroupReadsScalingDown
// is the reference shape between two batches: 12 prefill rolled 2 at a
// time beside 8 decode rolled 1 at a time, both RecreatePod. The prefill
// side has served two batches and the decode side one; every Instance on
// the new version is Ready and the old ones still serve, so the group reads
// ScalingDown, and the prefill side's next start waits for the decode
// side's second batch while the decode side's own next start is admitted.
func TestEvaluateUpdateGate_BatchStepHoldsBetweenBatchesWhileTheGroupReadsScalingDown(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate := v1beta1.UpdateStrategyRecreatePod
	members := map[v1beta1.ComponentType]batchMemberFixture{
		eng: {desired: 12, step: "10%", strategy: recreate, promoted: 4},
		dec: {desired: 8, step: "10%", strategy: recreate, promoted: 1},
	}
	if got := batchGroupPhase(members, eng, dec); got != v1beta1.CoordinationPhaseScalingDown {
		t.Fatalf("group phase %q, want %q", got, v1beta1.CoordinationPhaseScalingDown)
	}
	isvc := batchService(batchGroupFixture{tolerance: ptr.To(int32(20))}, members)
	c := batchClient(t, members)
	allowed, gate, reason := batchGate(t, isvc, c, eng, workloadtypes.UpdateStrategyRecreatePod, 0)
	if allowed || gate != v1beta1.RolloutHoldGateRatio {
		t.Fatalf("engine: allowed=%v gate=%q reason=%q, want a Ratio hold", allowed, gate, reason)
	}
	if want := "engine batch 3 waits for decoder batch 2 to serve (1 of 2 serving)"; reason != want {
		t.Errorf("engine reason=%q, want %q", reason, want)
	}
	if allowed, gate, reason := batchGate(t, isvc, c, dec, workloadtypes.UpdateStrategyRecreatePod, 0); !allowed {
		t.Errorf("decoder: held under %q with %q, want its second batch admitted", gate, reason)
	}
}

// TestEvaluateUpdateGate_BatchStepWaivesTheRatioBand pins that the batch step
// is the lockstep of every group it governs under maintainRatio: the serving
// band and the fraction lockstep are not consulted for its members. At
// tolerance 20 the band refuses the decoder's first drain for good (one of
// two decode out beside six prefill reads six to one), whether the prefill
// drain first or surge, and the fraction lockstep holds an engine two of six
// ahead of a decoder at zero; at tolerance 10 it holds a decoder one of two
// rolled beside an engine two of six rolled, the step boundary of a 6:2
// surge roll. The batch step admits every one, since none opens a batch
// past the peer's.
func TestEvaluateUpdateGate_BatchStepWaivesTheRatioBand(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	cases := []struct {
		name      string
		tolerance int32
		engine    batchMemberFixture
		decoder   batchMemberFixture
		comp      v1beta1.ComponentType
		mechanism workloadtypes.UpdateStrategyType
		inFlight  int32
	}{
		{
			name: "the decoder's first drain is admitted beside six serving prefill", tolerance: 20,
			engine: batchMemberFixture{desired: 6, step: "3", strategy: recreate}, decoder: batchMemberFixture{desired: 2, step: "1", strategy: recreate}, comp: dec, mechanism: workloadtypes.UpdateStrategyRecreatePod,
		},
		{
			name: "the decoder's first drain is admitted beside three prefill drains", tolerance: 20,
			engine: batchMemberFixture{desired: 6, step: "3", strategy: recreate, inFlight: 3}, decoder: batchMemberFixture{desired: 2, step: "1", strategy: recreate}, comp: dec, mechanism: workloadtypes.UpdateStrategyRecreatePod,
		},
		{
			name: "the engine's third drain is admitted with two of six promoted and the decoder at zero", tolerance: 20,
			engine: batchMemberFixture{desired: 6, step: "3", strategy: recreate, promoted: 2}, decoder: batchMemberFixture{desired: 2, step: "1", strategy: recreate, inFlight: 1}, comp: eng, mechanism: workloadtypes.UpdateStrategyRecreatePod,
		},
		{
			name: "the mixed pair: the decoder's first drain is admitted beside six surging prefill", tolerance: 20,
			engine: batchMemberFixture{desired: 6, strategy: surge, surging: 2}, decoder: batchMemberFixture{desired: 2, step: "1", strategy: recreate}, comp: dec, mechanism: workloadtypes.UpdateStrategyRecreatePod,
		},
		{
			name: "the surging pair at the step boundary: the decoder's second surge is admitted where the fraction lockstep would hold it", tolerance: 10,
			engine: batchMemberFixture{desired: 6, strategy: surge, promoted: 2}, decoder: batchMemberFixture{desired: 2, strategy: surge, promoted: 1}, comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			members := map[v1beta1.ComponentType]batchMemberFixture{eng: tc.engine, dec: tc.decoder}
			isvc := batchService(batchGroupFixture{maxUnavailable: "3", tolerance: ptr.To(tc.tolerance)}, members)
			allowed, gate, reason := batchGate(t, isvc, batchClient(t, members), tc.comp, tc.mechanism, tc.inFlight)
			if !allowed {
				t.Fatalf("held by gate=%q reason=%q; the batch step governs this group and the band must not run", gate, reason)
			}
		})
	}
}

// TestEvaluateUpdateGate_BatchStepGovernsOnlyDrainFirstGroups pins the
// boundary of the rule. The group is governed when both the engine and the
// decoder step: drain-first (RecreatePod, or an in-place strategy on a
// multi-pod Instance, whose update rebuilds the gang and reaches the gate as
// a RecreatePod start) or SurgeThenDrain, the unset default included, whose
// step is its surge budget. A single-pod in-place pairing member leaves the
// other gates in charge, which admit a start the batch step would hold.
func TestEvaluateUpdateGate_BatchStepGovernsOnlyDrainFirstGroups(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	cases := []struct {
		name      string
		engine    batchMemberFixture
		decoder   batchMemberFixture
		comp      v1beta1.ComponentType
		mechanism workloadtypes.UpdateStrategyType
		wantHold  bool
	}{
		{
			name:    "both RecreatePod: the engine's fourth drain waits for the first decode",
			engine:  batchMemberFixture{desired: 6, step: "3", strategy: v1beta1.UpdateStrategyRecreatePod, promoted: 3},
			decoder: batchMemberFixture{desired: 2, step: "1", strategy: v1beta1.UpdateStrategyRecreatePod, inFlight: 1},
			comp:    eng, mechanism: workloadtypes.UpdateStrategyRecreatePod, wantHold: true,
		},
		{
			name:    "a SurgeThenDrain decoder steps on its surges: the engine's fourth drain waits for the first decode surge to hand over",
			engine:  batchMemberFixture{desired: 6, step: "3", strategy: v1beta1.UpdateStrategyRecreatePod, promoted: 3},
			decoder: batchMemberFixture{desired: 2, step: "1", strategy: v1beta1.UpdateStrategySurgeThenDrain, surging: 1},
			comp:    eng, mechanism: workloadtypes.UpdateStrategyRecreatePod, wantHold: true,
		},
		{
			name:    "a decoder with no strategy set surges by default and steps likewise: the engine's fourth drain waits",
			engine:  batchMemberFixture{desired: 6, step: "3", strategy: v1beta1.UpdateStrategyRecreatePod, promoted: 3},
			decoder: batchMemberFixture{desired: 2, step: "1", surging: 1},
			comp:    eng, mechanism: workloadtypes.UpdateStrategyRecreatePod, wantHold: true,
		},
		{
			name:    "an in-place engine gang: the decoder's second drain waits for the first gang batch",
			engine:  batchMemberFixture{desired: 6, step: "3", strategy: v1beta1.UpdateStrategyInPlaceIfPossible, gang: true, inFlight: 3},
			decoder: batchMemberFixture{desired: 2, step: "1", strategy: v1beta1.UpdateStrategyRecreatePod, promoted: 1},
			comp:    dec, mechanism: workloadtypes.UpdateStrategyRecreatePod, wantHold: true,
		},
		{
			name:    "an in-place engine gang starts as a RecreatePod and its fourth start waits for the first decode",
			engine:  batchMemberFixture{desired: 6, step: "3", strategy: v1beta1.UpdateStrategyInPlaceOnly, gang: true, promoted: 3},
			decoder: batchMemberFixture{desired: 2, step: "1", strategy: v1beta1.UpdateStrategyRecreatePod, inFlight: 1},
			comp:    eng, mechanism: workloadtypes.UpdateStrategyRecreatePod, wantHold: true,
		},
		{
			name:    "a single-pod in-place engine keeps its pods: the decoder's second drain is admitted by the other gates",
			engine:  batchMemberFixture{desired: 6, step: "3", strategy: v1beta1.UpdateStrategyInPlaceIfPossible, inPlace: 3},
			decoder: batchMemberFixture{desired: 2, step: "1", strategy: v1beta1.UpdateStrategyRecreatePod, promoted: 1},
			comp:    dec, mechanism: workloadtypes.UpdateStrategyRecreatePod,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			members := map[v1beta1.ComponentType]batchMemberFixture{eng: tc.engine, dec: tc.decoder}
			isvc := batchService(batchGroupFixture{maxUnavailable: "3"}, members)
			allowed, gate, reason := batchGate(t, isvc, batchClient(t, members), tc.comp, tc.mechanism, 0)
			if tc.wantHold == allowed {
				t.Fatalf("allowed=%v, want hold=%v (gate=%q reason=%q)", allowed, tc.wantHold, gate, reason)
			}
			if tc.wantHold && (gate != v1beta1.RolloutHoldGateRatio || !strings.Contains(reason, "batch")) {
				t.Errorf("the hold must be the Ratio gate's batch reason, got gate=%q reason=%q", gate, reason)
			}
		})
	}
}

// TestEvaluateUpdateGate_BatchStepIgnoresASingleMemberGroup pins that a
// member alone in its group has nobody to keep step with: its fourth drain
// of six at a step of three is admitted while the group budget of six has
// room, where a peer would have made it batch 2.
func TestEvaluateUpdateGate_BatchStepIgnoresASingleMemberGroup(t *testing.T) {
	eng := v1beta1.EngineComponent
	members := map[v1beta1.ComponentType]batchMemberFixture{
		eng: {desired: 6, step: "3", strategy: v1beta1.UpdateStrategyRecreatePod, inFlight: 3},
	}
	isvc := batchService(batchGroupFixture{maxUnavailable: "6", components: []v1beta1.ComponentType{eng}}, members)
	allowed, gate, reason := batchGate(t, isvc, batchClient(t, members), eng, workloadtypes.UpdateStrategyRecreatePod, 0)
	if !allowed {
		t.Fatalf("a single-member group is not paced in batches; held by gate=%q reason=%q", gate, reason)
	}
}

// TestEvaluateUpdateGate_BatchStepFailsClosedOnAReadError pins that a
// replica the batch step cannot read denies the start with a retryable
// Ratio hold, like the other gates: admitting a drain on unreadable peer
// state is how a batch runs ahead unseen.
func TestEvaluateUpdateGate_BatchStepFailsClosedOnAReadError(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	members := map[v1beta1.ComponentType]batchMemberFixture{
		eng: {desired: 6, step: "3", strategy: v1beta1.UpdateStrategyRecreatePod, promoted: 3},
		dec: {desired: 2, step: "1", strategy: v1beta1.UpdateStrategyRecreatePod, promoted: 1},
	}
	isvc := batchService(batchGroupFixture{maxUnavailable: "3"}, members)
	var objs []client.Object
	for comp, m := range members {
		objs = append(objs, batchReplica(comp, m))
	}
	failing := fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(objs...).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, isIR := obj.(*v1beta1.InferenceReplica); isIR && key.Name == batchServiceName+"-"+string(dec) {
				return errors.New("apiserver unavailable")
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}).Build()
	allowed, gate, reason := batchGate(t, isvc, failing, eng, workloadtypes.UpdateStrategyRecreatePod, 0)
	if allowed || gate != v1beta1.RolloutHoldGateRatio || !strings.Contains(reason, "failing closed") {
		t.Fatalf("an unreadable peer must fail the batch step closed under the Ratio gate: allowed=%v gate=%q reason=%q", allowed, gate, reason)
	}
}

// routerFixture is two routers at a step of one under the given strategy,
// listed in the group beside the engine and the decoder.
func routerFixture(strategy v1beta1.UpdateStrategyType, m batchMemberFixture) batchMemberFixture {
	m.desired, m.step, m.strategy = 2, "1", strategy
	return m
}

// TestEvaluateUpdateGate_BatchStepGovernsAroundRouters pins that the step
// governs on the engine and the decoder alone: a router in the group does
// not switch it off. A router that rolls drain-first joins the batches with
// its own step, like any member; one that surges, patches its pod in place,
// or has no replica of its own is left out, and the batches neither count
// nor wait on it while the engine and the decoder keep their lockstep.
func TestEvaluateUpdateGate_BatchStepGovernsAroundRouters(t *testing.T) {
	eng, dec, rtr := v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	six := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.step, m.strategy = 6, "3", recreate
		return m
	}
	two := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.step, m.strategy = 2, "1", recreate
		return m
	}
	cases := []struct {
		name       string
		engine     batchMemberFixture
		decoder    batchMemberFixture
		router     *batchMemberFixture // nil lists the router in the group with no replica of its own
		comp       v1beta1.ComponentType
		wantHold   bool
		wantReason string
	}{
		{
			name:   "two surging routers: the decoder's second drain waits for the first prefill batch",
			engine: six(batchMemberFixture{inFlight: 3}), decoder: two(batchMemberFixture{promoted: 1}), router: ptr.To(routerFixture(surge, batchMemberFixture{surging: 1})),
			comp: dec, wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (0 of 3 serving)",
		},
		{
			name:   "two surging routers: the engine's fourth drain waits for the first decode",
			engine: six(batchMemberFixture{promoted: 3}), decoder: two(batchMemberFixture{inFlight: 1}), router: ptr.To(routerFixture(surge, batchMemberFixture{promoted: 1})),
			comp: eng, wantHold: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving)",
		},
		{
			name:   "two surging routers: the batches neither count nor wait on them",
			engine: six(batchMemberFixture{promoted: 3}), decoder: two(batchMemberFixture{promoted: 1}), router: ptr.To(routerFixture(surge, batchMemberFixture{surging: 1})),
			comp: dec,
		},
		{
			name:   "a router with no replica of its own: the decoder's second drain still waits",
			engine: six(batchMemberFixture{inFlight: 3}), decoder: two(batchMemberFixture{promoted: 1}),
			comp: dec, wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (0 of 3 serving)",
		},
		{
			name:   "a single-pod in-place router keeps its pods and is left out: the decoder's second drain still waits",
			engine: six(batchMemberFixture{inFlight: 3}), decoder: two(batchMemberFixture{promoted: 1}), router: ptr.To(routerFixture(v1beta1.UpdateStrategyInPlaceIfPossible, batchMemberFixture{inPlace: 1})),
			comp: dec, wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (0 of 3 serving)",
		},
		{
			name:   "routers under RecreatePod join the batches: router batch 2 waits for engine batch 1",
			engine: six(batchMemberFixture{inFlight: 3}), decoder: two(batchMemberFixture{promoted: 1}), router: ptr.To(routerFixture(recreate, batchMemberFixture{promoted: 1})),
			comp: rtr, wantHold: true, wantReason: "router batch 2 waits for engine batch 1 to serve (0 of 3 serving)",
		},
		{
			name:   "routers under RecreatePod join the batches: the decoder's second drain waits for router batch 1",
			engine: six(batchMemberFixture{promoted: 3}), decoder: two(batchMemberFixture{promoted: 1}), router: ptr.To(routerFixture(recreate, batchMemberFixture{inFlight: 1})),
			comp: dec, wantHold: true, wantReason: "decoder batch 2 waits for router batch 1 to serve (0 of 1 serving)",
		},
		{
			name:   "routers under RecreatePod join the batches: the decoder's second drain is admitted once every batch 1 serves",
			engine: six(batchMemberFixture{promoted: 3}), decoder: two(batchMemberFixture{promoted: 1}), router: ptr.To(routerFixture(recreate, batchMemberFixture{promoted: 1})),
			comp: dec,
		},
		{
			name:   "an in-place router gang rebuilds and joins the batches: the decoder's second drain waits for router batch 1",
			engine: six(batchMemberFixture{promoted: 3}), decoder: two(batchMemberFixture{promoted: 1}), router: ptr.To(routerFixture(v1beta1.UpdateStrategyInPlaceOnly, batchMemberFixture{gang: true, inFlight: 1})),
			comp: dec, wantHold: true, wantReason: "decoder batch 2 waits for router batch 1 to serve (0 of 1 serving)",
		},
	}
	for _, pacing := range batchPacings {
		for _, tc := range cases {
			t.Run(pacing.name+"/"+tc.name, func(t *testing.T) {
				members := map[v1beta1.ComponentType]batchMemberFixture{eng: tc.engine, dec: tc.decoder}
				if tc.router != nil {
					members[rtr] = *tc.router
				}
				group := batchGroupFixture{maxUnavailable: "3", tolerance: pacing.tolerance, components: []v1beta1.ComponentType{eng, dec, rtr}}
				isvc := batchService(group, members)
				allowed, gate, reason := batchGate(t, isvc, batchClient(t, members), tc.comp, workloadtypes.UpdateStrategyRecreatePod, 0)
				if tc.wantHold == allowed {
					t.Fatalf("allowed=%v, want hold=%v (gate=%q reason=%q)", allowed, tc.wantHold, gate, reason)
				}
				if !tc.wantHold {
					return
				}
				if gate != v1beta1.RolloutHoldGateRatio {
					t.Errorf("gate=%q, want %q (reason=%q)", gate, v1beta1.RolloutHoldGateRatio, reason)
				}
				if reason != tc.wantReason {
					t.Errorf("reason=%q, want %q", reason, tc.wantReason)
				}
			})
		}
	}
}

// TestEvaluateUpdateGate_BatchStepLeavesASurgingMemberToItsOwnGates pins
// the router half of the rule. A surging router beside a drain-first engine
// and decoder steps unlike either of them and is never batch-held: its
// start is answered by the ratio and surge gates as before, so its first
// surge is admitted while the first prefill batch starts and a second surge
// waits on the surge budget, not on an engine batch.
func TestEvaluateUpdateGate_BatchStepLeavesASurgingMemberToItsOwnGates(t *testing.T) {
	eng, dec, rtr := v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	cases := []struct {
		name       string
		engine     batchMemberFixture
		decoder    batchMemberFixture
		router     batchMemberFixture
		comp       v1beta1.ComponentType
		mechanism  workloadtypes.UpdateStrategyType
		wantGate   v1beta1.RolloutHoldGate // empty when the start is admitted
		wantReason string                  // a substring of the hold
	}{
		{
			name:   "a surging router's first surge is admitted while the first prefill batch starts",
			engine: batchMemberFixture{desired: 6, step: "3", strategy: recreate, inFlight: 3}, decoder: batchMemberFixture{desired: 2, step: "1", strategy: recreate, promoted: 1},
			router: routerFixture(surge, batchMemberFixture{}), comp: rtr, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:   "a surging router's second start waits on no engine batch",
			engine: batchMemberFixture{desired: 6, step: "3", strategy: recreate, inFlight: 3}, decoder: batchMemberFixture{desired: 2, step: "1", strategy: recreate, promoted: 1},
			router: routerFixture(surge, batchMemberFixture{promoted: 1}), comp: rtr, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:   "a surging router's second surge beside one in flight waits on the surge budget",
			engine: batchMemberFixture{desired: 6, step: "3", strategy: recreate, inFlight: 3}, decoder: batchMemberFixture{desired: 2, step: "1", strategy: recreate, promoted: 1},
			router: routerFixture(surge, batchMemberFixture{surging: 1}), comp: rtr, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantGate: v1beta1.RolloutHoldGateBudget, wantReason: "surge budget 1 exhausted",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			members := map[v1beta1.ComponentType]batchMemberFixture{eng: tc.engine, dec: tc.decoder, rtr: tc.router}
			group := batchGroupFixture{maxUnavailable: "3", components: []v1beta1.ComponentType{eng, dec, rtr}}
			isvc := batchService(group, members)
			allowed, gate, reason := batchGate(t, isvc, batchClient(t, members), tc.comp, tc.mechanism, 0)
			if strings.Contains(reason, "batch") {
				t.Fatalf("the batch step must not answer this start: gate=%q reason=%q", gate, reason)
			}
			if tc.wantGate == "" {
				if !allowed {
					t.Fatalf("held by gate=%q reason=%q, want the start admitted", gate, reason)
				}
				return
			}
			if allowed || gate != tc.wantGate || !strings.Contains(reason, tc.wantReason) {
				t.Fatalf("allowed=%v gate=%q reason=%q, want a %s hold reading %q", allowed, gate, reason, tc.wantGate, tc.wantReason)
			}
		})
	}
}

// The batch step counts drains off the rows: when a scale-down that lands
// mid-roll retires the Instances being rebuilt (4:2 to 3:1, one gang a step
// under the 25% default), the members start over and the last serving decode
// gang is admitted at once. The last case is the shape a different rank would give.
func TestEvaluateUpdateGate_BatchStepRestartsWhenAScaleDownRetiresTheOpenDrain(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate := v1beta1.UpdateStrategyRecreatePod
	cases := []batchCase{
		{
			name: "the scale has landed and the wave has not run: the open drains still count, so the decoder's next drain waits for the prefill batch", group: "25%",
			engine:  batchMemberFixture{desired: 3, step: "2", strategy: recreate, inFlight: 1, retiring: 1},
			decoder: batchMemberFixture{desired: 1, step: "1", strategy: recreate, inFlight: 1, retiring: 1}, comp: dec,
			wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (0 of 1 serving)",
		},
		{
			name: "the scale has landed and the wave has not run: the prefill side's next drain waits for the decode batch", group: "25%",
			engine:  batchMemberFixture{desired: 3, step: "2", strategy: recreate, inFlight: 1, retiring: 1},
			decoder: batchMemberFixture{desired: 1, step: "1", strategy: recreate, inFlight: 1, retiring: 1}, comp: eng,
			wantHold: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving)",
		},
		{
			name: "the wave retired both rebuilding Instances: no drain counts, so the decoder's last serving Instance is admitted", group: "25%",
			engine:  batchMemberFixture{desired: 3, step: "2", strategy: recreate},
			decoder: batchMemberFixture{desired: 1, step: "1", strategy: recreate}, comp: dec,
		},
		{
			name: "the wave retired both rebuilding Instances: the prefill side's next drain is admitted as well", group: "25%",
			engine:  batchMemberFixture{desired: 3, step: "2", strategy: recreate},
			decoder: batchMemberFixture{desired: 1, step: "1", strategy: recreate}, comp: eng,
		},
		{
			name: "had the serving decode Instance been retired instead, the kept rebuild would still count and the decode side would wait for the prefill batch", group: "25%",
			engine:  batchMemberFixture{desired: 3, step: "2", strategy: recreate},
			decoder: batchMemberFixture{desired: 1, step: "1", strategy: recreate, inFlight: 1}, comp: dec,
			wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (0 of 1 serving)",
		},
	}
	for _, pacing := range batchPacings {
		for _, tc := range cases {
			t.Run(pacing.name+"/"+tc.name, func(t *testing.T) {
				members := map[v1beta1.ComponentType]batchMemberFixture{eng: tc.engine, dec: tc.decoder}
				isvc := batchService(batchGroupFixture{maxUnavailable: tc.group, tolerance: pacing.tolerance}, members)
				allowed, gate, reason := batchGate(t, isvc, batchClient(t, members), tc.comp, workloadtypes.UpdateStrategyRecreatePod, 0)
				if tc.wantHold == allowed {
					t.Fatalf("allowed=%v, want hold=%v (gate=%q reason=%q)", allowed, tc.wantHold, gate, reason)
				}
				if !tc.wantHold {
					return
				}
				if gate != v1beta1.RolloutHoldGateRatio {
					t.Errorf("gate=%q, want %q (reason=%q)", gate, v1beta1.RolloutHoldGateRatio, reason)
				}
				if reason != tc.wantReason {
					t.Errorf("reason=%q, want %q", reason, tc.wantReason)
				}
			})
		}
	}
}

// TestEvaluateUpdateGate_BatchStepPacesSurgingMembers holds the batch step
// to the surging shape: 12 prefill beside 8 decode, both SurgeThenDrain in a
// group that names no budget, so each side's step is a quarter of its count
// rounded up, 3 and 2. A row reads "with these Instances promoted, surging
// or ended, may comp open one more surge?"; a hold is reported under the
// Ratio gate with the batch reason. A surge counts from the moment its
// attempt opens; a hand-over in progress is that same start, not a new one,
// and promoted means the hand-over is complete.
func TestEvaluateUpdateGate_BatchStepPacesSurgingMembers(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge := v1beta1.UpdateStrategySurgeThenDrain
	twelve := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.strategy = 12, surge
		return m
	}
	eight := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.strategy = 8, surge
		return m
	}
	cases := []batchCase{
		{
			name:   "batch 0 opens on both sides: the engine's third surge of one wake-up is admitted",
			engine: twelve(batchMemberFixture{}), decoder: eight(batchMemberFixture{}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain, inFlight: 2,
		},
		{
			name:   "batch 0 opens on both sides: the decoder's second surge is admitted beside three prefill surges",
			engine: twelve(batchMemberFixture{surging: 3}), decoder: eight(batchMemberFixture{}), comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain, inFlight: 1,
		},
		{
			name:   "the engine's fourth surge opens batch 2 before batch 1 serves",
			engine: twelve(batchMemberFixture{surging: 3}), decoder: eight(batchMemberFixture{surging: 2}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantHold: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 2 serving)",
		},
		{
			name:   "prefill fast: the engine's fourth surge waits for the two new decode",
			engine: twelve(batchMemberFixture{promoted: 3}), decoder: eight(batchMemberFixture{surging: 2}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantHold: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 2 serving)",
		},
		{
			name:   "prefill fast: one new decode serving is not the batch",
			engine: twelve(batchMemberFixture{promoted: 3}), decoder: eight(batchMemberFixture{promoted: 1, surging: 1}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantHold: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (1 of 2 serving)",
		},
		{
			name:   "at the step boundary: the engine's fourth surge is admitted",
			engine: twelve(batchMemberFixture{promoted: 3}), decoder: eight(batchMemberFixture{promoted: 2}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:   "decode fast: the decoder's third surge waits for the three new prefill",
			engine: twelve(batchMemberFixture{surging: 3}), decoder: eight(batchMemberFixture{promoted: 2}), comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (0 of 3 serving)",
		},
		{
			name:   "decode fast: two new prefill serving is not the batch",
			engine: twelve(batchMemberFixture{promoted: 2, surging: 1}), decoder: eight(batchMemberFixture{promoted: 2}), comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (2 of 3 serving)",
		},
		{
			name:   "at the step boundary: the decoder's third surge is admitted",
			engine: twelve(batchMemberFixture{promoted: 3}), decoder: eight(batchMemberFixture{promoted: 2}), comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:   "a prefill handing over is a start already counted, not a new one: the fourth surge still waits",
			engine: twelve(batchMemberFixture{handingOver: 3}), decoder: eight(batchMemberFixture{surging: 2}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantHold: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 2 serving)",
		},
		{
			name:   "a member's next batch waits for its own hand-overs too",
			engine: twelve(batchMemberFixture{promoted: 3}), decoder: eight(batchMemberFixture{handingOver: 2}), comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantHold: true, wantReason: "decoder batch 2 waits for decoder batch 1 to serve (0 of 2 serving)",
		},
		{
			name:   "a surge whose replacement failed and waits on the schedule counts as settled: the decoder goes on",
			engine: twelve(batchMemberFixture{promoted: 2, failed: 1, backoff: 1}), decoder: eight(batchMemberFixture{promoted: 2}), comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:   "a prefill batch one served, one failed and one open still waits for its open Instance",
			engine: twelve(batchMemberFixture{promoted: 1, failed: 1, surging: 1, backoff: 1}), decoder: eight(batchMemberFixture{promoted: 2}), comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (1 of 3 serving, 1 failed)",
		},
		{
			name:   "a parked attempt whose record stands counts as settled: the decoder goes on",
			engine: twelve(batchMemberFixture{promoted: 2, parked: 1, backoff: 1}), decoder: eight(batchMemberFixture{promoted: 2}), comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:   "the last prefill batch waits for the third decode batch",
			engine: twelve(batchMemberFixture{promoted: 9}), decoder: eight(batchMemberFixture{promoted: 4, surging: 2}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantHold: true, wantReason: "engine batch 4 waits for decoder batch 3 to serve (4 of 6 serving)",
		},
		{
			name:   "a finished decoder no longer paces the engine",
			engine: twelve(batchMemberFixture{promoted: 9}), decoder: eight(batchMemberFixture{promoted: 8}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:   "a finished engine no longer paces the decoder",
			engine: twelve(batchMemberFixture{promoted: 12}), decoder: eight(batchMemberFixture{promoted: 6}), comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:   "a member's own maxSurge below the group budget is its step: two prefill make a batch",
			engine: twelve(batchMemberFixture{surge: "2", promoted: 2}), decoder: eight(batchMemberFixture{surging: 2}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantHold: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 2 serving)",
		},
		{
			name: "a group maxSurge of 50 percent widens the steps to 6 and 4: the fourth prefill surge is within batch 1", groupSurge: "50%",
			engine: twelve(batchMemberFixture{surging: 3}), decoder: eight(batchMemberFixture{surging: 2}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name: "a group maxSurge of 50 percent widens the steps to 6 and 4: the seventh prefill surge waits for four new decode", groupSurge: "50%",
			engine: twelve(batchMemberFixture{promoted: 6}), decoder: eight(batchMemberFixture{surging: 4}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantHold: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 4 serving)",
		},
	}
	runBatchCases(t, cases)
}

// TestEvaluateUpdateGate_BatchStepPacesAMixedPair holds the batch step to a
// group whose prefill surges and whose decode rolls drain-first: 6 prefill
// under SurgeThenDrain, their step the default quarter of six rounded up to
// two, beside 2 decode under RecreatePod one at a time. Each side counts
// its own kind of start, and neither opens its next batch before the
// other's previous batch serves.
func TestEvaluateUpdateGate_BatchStepPacesAMixedPair(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge, recreate := v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyRecreatePod
	six := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.strategy = 6, surge
		return m
	}
	two := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.step, m.strategy = 2, "1", recreate
		return m
	}
	cases := []batchCase{
		{
			name: "batch 0: the engine's second surge is admitted beside the decoder's first drain", group: "1",
			engine: six(batchMemberFixture{surging: 1}), decoder: two(batchMemberFixture{inFlight: 1}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name: "batch 0: the decoder's first drain is admitted beside two prefill surges", group: "1",
			engine: six(batchMemberFixture{surging: 2}), decoder: two(batchMemberFixture{}), comp: dec,
		},
		{
			name: "the engine's third surge waits for the first new decode", group: "1",
			engine: six(batchMemberFixture{promoted: 2}), decoder: two(batchMemberFixture{inFlight: 1}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantHold: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving)",
		},
		{
			name: "the decoder's second drain waits for the first two prefill to hand over", group: "1",
			engine: six(batchMemberFixture{surging: 2}), decoder: two(batchMemberFixture{promoted: 1}), comp: dec,
			wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (0 of 2 serving)",
		},
		{
			name: "the decoder's second drain waits while the prefill hand-over is in progress", group: "1",
			engine: six(batchMemberFixture{promoted: 1, handingOver: 1}), decoder: two(batchMemberFixture{promoted: 1}), comp: dec,
			wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (1 of 2 serving)",
		},
		{
			name: "at the step boundary: the engine's third surge is admitted", group: "1",
			engine: six(batchMemberFixture{promoted: 2}), decoder: two(batchMemberFixture{promoted: 1}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name: "at the step boundary: the decoder's second drain is admitted", group: "1",
			engine: six(batchMemberFixture{promoted: 2}), decoder: two(batchMemberFixture{promoted: 1}), comp: dec,
		},
		{
			name: "a finished decoder no longer paces the engine", group: "1",
			engine: six(batchMemberFixture{promoted: 4}), decoder: two(batchMemberFixture{promoted: 2}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name: "the roll turned around, 6 decode surging beside 2 prefill drained one at a time: the decoder's third surge waits for the first prefill", group: "1",
			engine:  batchMemberFixture{desired: 2, step: "1", strategy: recreate, inFlight: 1},
			decoder: batchMemberFixture{desired: 6, strategy: surge, promoted: 2}, comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (0 of 1 serving)",
		},
	}
	runBatchCases(t, cases)
}

// TestEvaluateUpdateGate_BatchStepBatchesRoutersByStepKind pins how a
// router joins a group with a surging pairing member: it is batched when it
// steps the way some pairing member does, surges beside a surging pair or
// either kind beside a mixed pair, and is left to its own gates when it
// drains first beside a pair that only surges.
func TestEvaluateUpdateGate_BatchStepBatchesRoutersByStepKind(t *testing.T) {
	eng, dec, rtr := v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent
	surge, recreate := v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyRecreatePod
	twelve := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.strategy = 12, surge
		return m
	}
	eight := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.strategy = 8, surge
		return m
	}
	six := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.strategy = 6, surge
		return m
	}
	two := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.step, m.strategy = 2, "1", recreate
		return m
	}
	cases := []struct {
		name       string
		group      string
		engine     batchMemberFixture
		decoder    batchMemberFixture
		router     batchMemberFixture
		comp       v1beta1.ComponentType
		mechanism  workloadtypes.UpdateStrategyType
		wantHold   bool
		wantReason string // the exact hold, or a substring the admitted start's reason must not carry
	}{
		{
			name:   "a surging router joins a surging pair: router batch 2 waits for engine batch 1",
			engine: twelve(batchMemberFixture{surging: 3}), decoder: eight(batchMemberFixture{promoted: 2}), router: routerFixture(surge, batchMemberFixture{promoted: 1}),
			comp: rtr, mechanism: workloadtypes.UpdateStrategySurgeThenDrain, wantHold: true, wantReason: "router batch 2 waits for engine batch 1 to serve (0 of 3 serving)",
		},
		{
			name:   "a surging router joins a surging pair: the engine's fourth surge waits for router batch 1",
			engine: twelve(batchMemberFixture{promoted: 3}), decoder: eight(batchMemberFixture{promoted: 2}), router: routerFixture(surge, batchMemberFixture{surging: 1}),
			comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain, wantHold: true, wantReason: "engine batch 2 waits for router batch 1 to serve (0 of 1 serving)",
		},
		{
			name:   "a drain-first router beside a surging pair is left out: the engine's fourth surge is admitted",
			engine: twelve(batchMemberFixture{promoted: 3}), decoder: eight(batchMemberFixture{promoted: 2}), router: routerFixture(recreate, batchMemberFixture{inFlight: 1}),
			comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:   "a drain-first router beside a surging pair rolls on its own gates: its first drain is admitted while the pair's surges are still coming up",
			engine: twelve(batchMemberFixture{surging: 3}), decoder: eight(batchMemberFixture{surging: 2}), router: routerFixture(recreate, batchMemberFixture{}),
			comp: rtr, mechanism: workloadtypes.UpdateStrategyRecreatePod,
		},
		{
			name: "a surging router joins a mixed pair: the engine's third surge waits for router batch 1", group: "1",
			engine: six(batchMemberFixture{promoted: 2}), decoder: two(batchMemberFixture{promoted: 1}), router: routerFixture(surge, batchMemberFixture{surging: 1}),
			comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain, wantHold: true, wantReason: "engine batch 2 waits for router batch 1 to serve (0 of 1 serving)",
		},
		{
			name: "a drain-first router joins a mixed pair: the decoder's second drain waits for router batch 1", group: "1",
			engine: six(batchMemberFixture{promoted: 2}), decoder: two(batchMemberFixture{promoted: 1}), router: routerFixture(recreate, batchMemberFixture{inFlight: 1}),
			comp: dec, mechanism: workloadtypes.UpdateStrategyRecreatePod, wantHold: true, wantReason: "decoder batch 2 waits for router batch 1 to serve (0 of 1 serving)",
		},
		{
			name: "a surging decoder beside drain-first routers: the engine's fourth drain waits for the decode surge to hand over", group: "3",
			engine:  batchMemberFixture{desired: 6, step: "3", strategy: recreate, promoted: 3},
			decoder: batchMemberFixture{desired: 2, step: "1", strategy: surge, surging: 1},
			router:  routerFixture(recreate, batchMemberFixture{}),
			comp:    eng, mechanism: workloadtypes.UpdateStrategyRecreatePod, wantHold: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving)",
		},
		{
			name: "a surging engine beside drain-first routers: the decoder's second drain waits for the prefill surges to hand over", group: "3",
			engine:  batchMemberFixture{desired: 6, step: "3", strategy: surge, surging: 2},
			decoder: batchMemberFixture{desired: 2, step: "1", strategy: recreate, promoted: 1},
			router:  routerFixture(recreate, batchMemberFixture{inFlight: 1}),
			comp:    dec, mechanism: workloadtypes.UpdateStrategyRecreatePod, wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (0 of 2 serving)",
		},
	}
	for _, pacing := range batchPacings {
		for _, tc := range cases {
			t.Run(pacing.name+"/"+tc.name, func(t *testing.T) {
				members := map[v1beta1.ComponentType]batchMemberFixture{eng: tc.engine, dec: tc.decoder, rtr: tc.router}
				group := batchGroupFixture{maxUnavailable: tc.group, tolerance: pacing.tolerance, components: []v1beta1.ComponentType{eng, dec, rtr}}
				isvc := batchService(group, members)
				allowed, gate, reason := batchGate(t, isvc, batchClient(t, members), tc.comp, tc.mechanism, 0)
				if tc.wantHold == allowed {
					t.Fatalf("allowed=%v, want hold=%v (gate=%q reason=%q)", allowed, tc.wantHold, gate, reason)
				}
				if !tc.wantHold {
					if strings.Contains(reason, "batch") {
						t.Fatalf("the batch step must not answer this start: gate=%q reason=%q", gate, reason)
					}
					return
				}
				if gate != v1beta1.RolloutHoldGateRatio {
					t.Errorf("gate=%q, want %q (reason=%q)", gate, v1beta1.RolloutHoldGateRatio, reason)
				}
				if reason != tc.wantReason {
					t.Errorf("reason=%q, want %q", reason, tc.wantReason)
				}
			})
		}
	}
}

// TestEvaluateUpdateGate_BatchStepNeverHoldsAHandOver pins that the batch
// step answers starts alone. While the engine's fourth surge is held for
// the first decode batch, the hand-over of the three surges already
// started, which consults only the drain gate, goes through.
func TestEvaluateUpdateGate_BatchStepNeverHoldsAHandOver(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge := v1beta1.UpdateStrategySurgeThenDrain
	members := map[v1beta1.ComponentType]batchMemberFixture{
		eng: {desired: 12, strategy: surge, handingOver: 3},
		dec: {desired: 8, strategy: surge, surging: 2},
	}
	isvc := batchService(batchGroupFixture{}, members)
	c := batchClient(t, members)
	if allowed, gate, reason := batchGate(t, isvc, c, eng, workloadtypes.UpdateStrategySurgeThenDrain, 0); allowed || gate != v1beta1.RolloutHoldGateRatio {
		t.Fatalf("the engine's fourth surge must wait for the first decode batch: allowed=%v gate=%q reason=%q", allowed, gate, reason)
	}
	sourcePods := []string{batchServiceName + "-engine-0-0", batchServiceName + "-engine-1-0", batchServiceName + "-engine-2-0"}
	allowed, gate, reason := EvaluatePairingDrain(context.Background(), c, isvc, eng, forwardTarget(isvc, eng), GroupDefaults{}, sourcePods)
	if !allowed || strings.Contains(reason, "batch") {
		t.Fatalf("a hand-over is never a start and is never batch-held: allowed=%v gate=%q reason=%q", allowed, gate, reason)
	}
}

// TestEvaluateUpdateGate_BatchStepReadsAFailedSurgeAsEnded holds the batch
// step to the row the replica publishes for a surge whose replacement
// failed while the old pod serves: Ready, no operation, the pushed revision
// kept as the row's target, which the engine reads as Failed. That surge
// ended, so the retry ladder's next attempt on the Instance is admitted as
// the re-opening of its own batch, the other side's next batch waits only
// while that attempt is open, and a hold that waits on the batch names the
// failed count. With 3 prefill beside 3 decode, both SurgeThenDrain at a
// maxSurge of 1, both first surges fail and the sides retry one attempt at
// a time; with 12 prefill beside 8 decode one of the three prefill surges
// fails and neither side stops. The gang form of the row, the attempt kept
// at its Surge step while the replacement gang is retired, is never
// batch-held either; its surge slot is the budget gate's to free. Every
// failure here is one the side's retry schedule counted, so its record
// stands in Backoff; an end the record does not count is started again
// at once and read as still to start (batch_step_starts_again_test.go).
func TestEvaluateUpdateGate_BatchStepReadsAFailedSurgeAsEnded(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge := v1beta1.UpdateStrategySurgeThenDrain
	three := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.surge, m.strategy = 3, "1", surge
		return m
	}
	twelve := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.strategy = 12, surge
		return m
	}
	eight := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.strategy = 8, surge
		return m
	}
	cases := []batchCase{
		{
			name:   "3:3 both first surges failed: the engine's retry re-opens its batch",
			engine: three(batchMemberFixture{failedSurge: 1, backoff: 1}), decoder: three(batchMemberFixture{failedSurge: 1, backoff: 1}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:   "3:3 both first surges failed: the decoder's retry re-opens its batch",
			engine: three(batchMemberFixture{failedSurge: 1, backoff: 1}), decoder: three(batchMemberFixture{failedSurge: 1, backoff: 1}), comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:   "3:3 one-sided: the decoder's first surge served, the engine's failed: the engine's retry is admitted",
			engine: three(batchMemberFixture{failedSurge: 1, backoff: 1}), decoder: three(batchMemberFixture{promoted: 1}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:   "3:3 one-sided: the decoder's second surge is admitted beside the engine's failed first",
			engine: three(batchMemberFixture{failedSurge: 1, backoff: 1}), decoder: three(batchMemberFixture{promoted: 1}), comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:   "3:3 the engine's retry is open: the decoder's retry waits for it, one attempt at a time",
			engine: three(batchMemberFixture{surging: 1}), decoder: three(batchMemberFixture{failedSurge: 1, backoff: 1}), comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (0 of 1 serving)",
		},
		{
			name:   "3:3 the engine's retry is open: the engine's own next batch waits for it",
			engine: three(batchMemberFixture{surging: 1}), decoder: three(batchMemberFixture{failedSurge: 1, backoff: 1}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantHold: true, wantReason: "engine batch 2 waits for engine batch 1 to serve (0 of 1 serving)",
		},
		{
			name:   "12:8 one of the three prefill surges failed: the prefill side's next start is admitted",
			engine: twelve(batchMemberFixture{promoted: 2, failedSurge: 1, backoff: 1}), decoder: eight(batchMemberFixture{promoted: 2}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:   "12:8 one of the three prefill surges failed: the decode side's second batch is admitted",
			engine: twelve(batchMemberFixture{promoted: 2, failedSurge: 1, backoff: 1}), decoder: eight(batchMemberFixture{promoted: 2}), comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:   "12:8 a prefill batch one served, one failed and one open names its failed count",
			engine: twelve(batchMemberFixture{promoted: 1, failedSurge: 1, surging: 1, backoff: 1}), decoder: eight(batchMemberFixture{promoted: 2}), comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (1 of 3 serving, 1 failed)",
		},
		{
			name:   "12:8 the prefill ladder spent: the decode side's third batch waits for prefill batch 2 and names the failed Instance",
			engine: twelve(batchMemberFixture{promoted: 2, failedSurge: 1, backoff: 1}), decoder: eight(batchMemberFixture{promoted: 4}), comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantHold: true, wantReason: "decoder batch 3 waits for engine batch 2 to serve (2 of 6 serving, 1 failed)",
		},
	}
	runBatchCases(t, cases)

	gang := []struct {
		name    string
		engine  batchMemberFixture
		decoder batchMemberFixture
		comp    v1beta1.ComponentType
	}{
		{
			name:   "3:3 both first gang surges failed: the engine's retry is not batch-held",
			engine: three(batchMemberFixture{failedGangSurge: 1, backoff: 1}), decoder: three(batchMemberFixture{failedGangSurge: 1, backoff: 1}), comp: eng,
		},
		{
			name:   "3:3 both first gang surges failed: the decoder's retry is not batch-held",
			engine: three(batchMemberFixture{failedGangSurge: 1, backoff: 1}), decoder: three(batchMemberFixture{failedGangSurge: 1, backoff: 1}), comp: dec,
		},
		{
			name:   "12:8 one of the three prefill gang surges failed: the decode side's second batch is not batch-held",
			engine: twelve(batchMemberFixture{promoted: 2, failedGangSurge: 1, backoff: 1}), decoder: eight(batchMemberFixture{promoted: 2}), comp: dec,
		},
	}
	for _, tc := range gang {
		t.Run(tc.name, func(t *testing.T) {
			members := map[v1beta1.ComponentType]batchMemberFixture{eng: tc.engine, dec: tc.decoder}
			isvc := batchService(batchGroupFixture{}, members)
			allowed, gate, reason := batchGate(t, isvc, batchClient(t, members), tc.comp, workloadtypes.UpdateStrategySurgeThenDrain, 0)
			if strings.Contains(reason, "batch") {
				t.Fatalf("a failed gang surge ended and must not be batch-waited on: allowed=%v gate=%q reason=%q", allowed, gate, reason)
			}
			t.Logf("allowed=%v gate=%q reason=%q", allowed, gate, reason)
		})
	}
}

// TestEvaluateUpdateGate_FirstSideMovesFirstInsideEveryBatch pins the
// first-side rule on 12 prefill beside 8 decode at 10 percent. With the
// decoder named, inside each batch the engine's starts wait until the
// decoder's new Instance of that batch serves, under the Ratio gate with a
// reason naming the decoder; the decoder is never held by it, and the
// usual wait on the previous batch still holds the decoder's next drain
// until the new prefill serve. With the engine named the rule reads the
// other way round: the decoder's first drain waits for the first prefill
// batch to serve, and the engine's starts are free. The setting is a
// switch of its own: with the ratio rule off it layers on the step's own
// batches, on a drain-first pair and on a surging pair alike.
func TestEvaluateUpdateGate_FirstSideMovesFirstInsideEveryBatch(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate := v1beta1.UpdateStrategyRecreatePod
	shape := func(engine, decoder batchMemberFixture) (batchMemberFixture, batchMemberFixture) {
		e, d := twelveEight(recreate)
		e.promoted, e.inFlight, d.promoted, d.inFlight = engine.promoted, engine.inFlight, decoder.promoted, decoder.inFlight
		return e, d
	}
	decoderFirst := keepRatioDecoderFirst(10)
	engineFirst := keepRatio(10)
	engineFirst.PrefillDecode.FirstSide = eng
	decoderFirstOnly := GroupDefaults{PrefillDecode: PrefillDecodeRules{FirstSide: dec}}
	cases := []struct {
		name       string
		engine     batchMemberFixture
		decoder    batchMemberFixture
		comp       v1beta1.ComponentType
		defaults   GroupDefaults
		wantHold   bool
		wantReason string
	}{
		{
			name: "decoder named: the engine's first start waits for the first new decoder", comp: eng, defaults: decoderFirst,
			wantHold: true, wantReason: "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving)",
		},
		{
			name: "decoder named: a decoder still starting does not release the engine", decoder: batchMemberFixture{inFlight: 1}, comp: eng, defaults: decoderFirst,
			wantHold: true, wantReason: "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving)",
		},
		{
			name: "decoder named: the decoder's first drain is admitted at once", comp: dec, defaults: decoderFirst,
		},
		{
			name: "decoder named: the first new decoder serves, so the engine's first start is admitted", decoder: batchMemberFixture{promoted: 1}, comp: eng, defaults: decoderFirst,
		},
		{
			name: "decoder named: the first new decoder serves, so the engine's second start of the batch is admitted", engine: batchMemberFixture{inFlight: 1}, decoder: batchMemberFixture{promoted: 1}, comp: eng, defaults: decoderFirst,
		},
		{
			name: "decoder named: the first batch of two prefill is full, so the third start waits for the batch to serve", engine: batchMemberFixture{inFlight: 2}, decoder: batchMemberFixture{promoted: 1}, comp: eng, defaults: decoderFirst,
			wantHold: true, wantReason: "engine batch 2 waits for engine batch 1 to serve (0 of 2 serving)",
		},
		{
			name: "decoder named: the decoder's second drain waits for the first prefill batch to serve", engine: batchMemberFixture{inFlight: 2}, decoder: batchMemberFixture{promoted: 1}, comp: dec, defaults: decoderFirst,
			wantHold: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (0 of 2 serving)",
		},
		{
			name: "decoder named: batch three is a single prefill and waits for the third new decoder", engine: batchMemberFixture{promoted: 4}, decoder: batchMemberFixture{promoted: 2, inFlight: 1}, comp: eng, defaults: decoderFirst,
			wantHold: true, wantReason: "engine batch 3 waits for decoder batch 3 to serve first (2 of 3 serving)",
		},
		{
			name: "decoder named: batch three is a single prefill, so a second start waits for it to serve", engine: batchMemberFixture{promoted: 4, inFlight: 1}, decoder: batchMemberFixture{promoted: 3}, comp: eng, defaults: decoderFirst,
			wantHold: true, wantReason: "engine batch 4 waits for engine batch 3 to serve (4 of 5 serving)",
		},
		{
			name: "engine named: the decoder's first drain waits for the first prefill batch", comp: dec, defaults: engineFirst,
			wantHold: true, wantReason: "decoder batch 1 waits for engine batch 1 to serve first (0 of 2 serving)",
		},
		{
			name: "engine named: one of two new prefill serving does not release the decoder", engine: batchMemberFixture{promoted: 1, inFlight: 1}, comp: dec, defaults: engineFirst,
			wantHold: true, wantReason: "decoder batch 1 waits for engine batch 1 to serve first (1 of 2 serving)",
		},
		{
			name: "engine named: the engine's first start is admitted at once", comp: eng, defaults: engineFirst,
		},
		{
			name: "engine named: the first prefill batch serves, so the decoder's first drain is admitted", engine: batchMemberFixture{promoted: 2}, comp: dec, defaults: engineFirst,
		},
		{
			name: "the switch alone holds the engine on a drain-first group", comp: eng, defaults: decoderFirstOnly,
			wantHold: true, wantReason: "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving)",
		},
		{
			name: "the switch alone leaves the decoder free on a drain-first group", comp: dec, defaults: decoderFirstOnly,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine, decoder := shape(tc.engine, tc.decoder)
			members := map[v1beta1.ComponentType]batchMemberFixture{eng: engine, dec: decoder}
			isvc := batchService(batchGroupFixture{maxUnavailable: "10%"}, members)
			allowed, gate, reason := batchGateWith(t, isvc, batchClient(t, members), tc.comp, workloadtypes.UpdateStrategyRecreatePod, 0, tc.defaults)
			if tc.wantHold == allowed {
				t.Fatalf("allowed=%v, want hold=%v (gate=%q reason=%q)", allowed, tc.wantHold, gate, reason)
			}
			if !tc.wantHold {
				return
			}
			if gate != v1beta1.RolloutHoldGateRatio {
				t.Errorf("gate=%q, want %q (reason=%q)", gate, v1beta1.RolloutHoldGateRatio, reason)
			}
			if reason != tc.wantReason {
				t.Errorf("reason=%q, want %q", reason, tc.wantReason)
			}
		})
	}
	t.Run("the switch alone layers on the batches of a surging pair", func(t *testing.T) {
		engine, decoder := twelveEight(v1beta1.UpdateStrategySurgeThenDrain)
		members := map[v1beta1.ComponentType]batchMemberFixture{eng: engine, dec: decoder}
		isvc := batchService(batchGroupFixture{maxUnavailable: "0", maxSurge: "10%"}, members)
		allowed, gate, reason := batchGateWith(t, isvc, batchClient(t, members), eng, workloadtypes.UpdateStrategySurgeThenDrain, 0, decoderFirstOnly)
		if allowed || gate != v1beta1.RolloutHoldGateRatio || reason != "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving)" {
			t.Fatalf("the step paces a surging pair on its own, so the named side leads there too: allowed=%v gate=%q reason=%q", allowed, gate, reason)
		}
	})
}

// TestEvaluateUpdateGate_KeepRatioWithTheDecoderFirstWalksTheBatches drives
// 12 prefill beside 8 decode through the gate with the ratio rule on and
// the decoder named as the first side. Each batch now takes two rounds:
// the decoder moves and serves, then the prefill side takes its batch, so
// the picture reads 0:1, 2:1, 2:2, 4:2, 4:3, 5:3 and so on to 12:8. Between
// batches the picture sits within the tolerance of 10, 0.8 decode
// Instances, of the 3:2 line; inside a batch the named side leads by its
// own step, one decode Instance (0:1 at the very start), on the drain-first
// and the surge path alike.
func TestEvaluateUpdateGate_KeepRatioWithTheDecoderFirstWalksTheBatches(t *testing.T) {
	want := []rollPicture{{0, 1}, {2, 1}, {2, 2}, {4, 2}, {4, 3}, {5, 3}, {5, 4}, {7, 4}, {7, 5}, {8, 5}, {8, 6}, {10, 6}, {10, 7}, {11, 7}, {11, 8}, {12, 8}}
	for _, tc := range []struct {
		name      string
		strategy  v1beta1.UpdateStrategyType
		group     batchGroupFixture
		mechanism workloadtypes.UpdateStrategyType
	}{
		{name: "RecreatePod", strategy: v1beta1.UpdateStrategyRecreatePod, group: batchGroupFixture{maxUnavailable: "10%"}, mechanism: workloadtypes.UpdateStrategyRecreatePod},
		{name: "SurgeThenDrain", strategy: v1beta1.UpdateStrategySurgeThenDrain, group: batchGroupFixture{maxUnavailable: "0", maxSurge: "10%"}, mechanism: workloadtypes.UpdateStrategySurgeThenDrain},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, decoder := twelveEight(tc.strategy)
			got, stalled := driveBatchRoll(t, tc.group, engine, decoder, tc.mechanism, keepRatioDecoderFirst(10))
			if stalled {
				t.Fatalf("the roll stalled after %s", pictures(got))
			}
			for _, p := range got {
				if off := offTheLine(p, 12, 8); off > 1.0001 {
					t.Errorf("picture %s sits %.2f decode Instances off the 3:2 line; the named side may lead by its step of one", p, off)
				}
			}
			if pictures(got) != pictures(want) {
				t.Errorf("pictures after each round:\n got %s\nwant %s", pictures(got), pictures(want))
			}
		})
	}
}

// TestEvaluateUpdateGate_KeepRatioGovernsEveryUpdatePath pins where the
// ratio rule reaches: with it on, the batch step paces a group whose
// engine patches single pods in place beside a decoder that drains first,
// which the other gates answer without it, and it keeps pacing the surging
// and the mixed pairs the step paces on its own, each surging member by
// its own maxSurge. Without the rule the in-place member is left to the
// other gates, so absent config changes nothing.
func TestEvaluateUpdateGate_KeepRatioGovernsEveryUpdatePath(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate, surge, inPlace := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyInPlaceIfPossible
	cases := []struct {
		name       string
		engine     batchMemberFixture
		decoder    batchMemberFixture
		group      batchGroupFixture
		comp       v1beta1.ComponentType
		mechanism  workloadtypes.UpdateStrategyType
		defaults   GroupDefaults
		wantBatch  bool
		wantReason string
	}{
		{
			name:   "a surging engine beside a drain-first decoder: the engine's third surge waits for the first new decoder",
			engine: batchMemberFixture{desired: 12, surge: "10%", strategy: surge, surging: 2}, decoder: batchMemberFixture{desired: 8, step: "10%", strategy: recreate, inFlight: 1},
			group: batchGroupFixture{maxUnavailable: "10%", maxSurge: "10%"}, comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain, defaults: keepRatio(10),
			wantBatch: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving)",
		},
		{
			name:   "a surging engine beside a drain-first decoder: the decoder's second drain waits for the first prefill batch",
			engine: batchMemberFixture{desired: 12, surge: "10%", strategy: surge, surging: 2}, decoder: batchMemberFixture{desired: 8, step: "10%", strategy: recreate, promoted: 1},
			group: batchGroupFixture{maxUnavailable: "10%", maxSurge: "10%"}, comp: dec, mechanism: workloadtypes.UpdateStrategyRecreatePod, defaults: keepRatio(10),
			wantBatch: true, wantReason: "decoder batch 2 waits for engine batch 1 to serve (0 of 2 serving)",
		},
		{
			name:   "an engine patching single pods in place beside a drain-first decoder: its third patch waits for the first new decoder",
			engine: batchMemberFixture{desired: 12, step: "10%", strategy: inPlace, inPlace: 2}, decoder: batchMemberFixture{desired: 8, step: "10%", strategy: recreate, inFlight: 1},
			group: batchGroupFixture{maxUnavailable: "10%"}, comp: eng, mechanism: workloadtypes.UpdateStrategyInPlaceIfPossible, defaults: keepRatio(10),
			wantBatch: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving)",
		},
		{
			name:   "both surging under the rule: the engine's third surge waits for the first new decoder",
			engine: batchMemberFixture{desired: 12, surge: "10%", strategy: surge, surging: 2}, decoder: batchMemberFixture{desired: 8, surge: "10%", strategy: surge, surging: 1},
			group: batchGroupFixture{maxUnavailable: "0", maxSurge: "10%"}, comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain, defaults: keepRatio(10),
			wantBatch: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving)",
		},
		{
			name:   "both surging without the rule: the step paces a surging pair on its own, so the third surge waits for the first new decoder",
			engine: batchMemberFixture{desired: 12, surge: "10%", strategy: surge, surging: 2}, decoder: batchMemberFixture{desired: 8, surge: "10%", strategy: surge, surging: 1},
			group: batchGroupFixture{maxUnavailable: "0", maxSurge: "10%"}, comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
			wantBatch: true, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving)",
		},
		{
			name:   "an in-place engine without the rule is left to the other gates",
			engine: batchMemberFixture{desired: 12, step: "10%", strategy: inPlace, inPlace: 2}, decoder: batchMemberFixture{desired: 8, step: "10%", strategy: recreate, inFlight: 1},
			group: batchGroupFixture{maxUnavailable: "10%"}, comp: eng, mechanism: workloadtypes.UpdateStrategyInPlaceIfPossible,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			members := map[v1beta1.ComponentType]batchMemberFixture{eng: tc.engine, dec: tc.decoder}
			isvc := batchService(tc.group, members)
			allowed, gate, reason := batchGateWith(t, isvc, batchClient(t, members), tc.comp, tc.mechanism, 0, tc.defaults)
			if !tc.wantBatch {
				if strings.Contains(reason, "batch") {
					t.Fatalf("the batch step must not answer this start: allowed=%v gate=%q reason=%q", allowed, gate, reason)
				}
				return
			}
			if allowed || gate != v1beta1.RolloutHoldGateRatio || reason != tc.wantReason {
				t.Fatalf("allowed=%v gate=%q reason=%q, want a Ratio hold reading %q", allowed, gate, reason, tc.wantReason)
			}
		})
	}
}

// TestEvaluateUpdateGate_KeepRatioOptsOutAtToleranceOneHundred pins the
// opt-out: a group that sets maintainRatio with a tolerance of 100 keeps
// every batch at the sides' own budgets under the rule, so 12 prefill
// beside 8 decode at 10 percent walks 2:1, 4:2, 6:3, 8:4, 10:5, 12:6,
// 12:7, 12:8 as it does without the rule.
func TestEvaluateUpdateGate_KeepRatioOptsOutAtToleranceOneHundred(t *testing.T) {
	want := []rollPicture{{2, 1}, {4, 2}, {6, 3}, {8, 4}, {10, 5}, {12, 6}, {12, 7}, {12, 8}}
	engine, decoder := twelveEight(v1beta1.UpdateStrategyRecreatePod)
	group := batchGroupFixture{maxUnavailable: "10%", tolerance: ptr.To(int32(100))}
	got, stalled := driveBatchRoll(t, group, engine, decoder, workloadtypes.UpdateStrategyRecreatePod, keepRatio(10))
	if stalled {
		t.Fatalf("the roll stalled after %s", pictures(got))
	}
	if pictures(got) != pictures(want) {
		t.Errorf("pictures after each round:\n got %s\nwant %s", pictures(got), pictures(want))
	}
	t.Run("and without the rule the walk is the same", func(t *testing.T) {
		engine, decoder := twelveEight(v1beta1.UpdateStrategyRecreatePod)
		got, _ := driveBatchRoll(t, batchGroupFixture{maxUnavailable: "10%"}, engine, decoder, workloadtypes.UpdateStrategyRecreatePod, GroupDefaults{})
		if pictures(got) != pictures(want) {
			t.Errorf("pictures after each round:\n got %s\nwant %s", pictures(got), pictures(want))
		}
	})
}

// TestEvaluateUpdateGate_KeepRatioNeverStalls drives 12 prefill beside 8
// decode at 10 percent to completion under the ratio rule at a tolerance
// of 5 and of 0, on the drain-first and the surge path, with the group
// naming the tolerance itself. The band and the lockstep, which the rule
// replaces for such a group, would hold the drain-first roll after its
// first engine at these tolerances; the rule shrinks the batches instead,
// moves at least one Instance per batch, and finishes with both sides on
// version 2, every picture at most 0.67 decode Instances off the 3:2 line.
func TestEvaluateUpdateGate_KeepRatioNeverStalls(t *testing.T) {
	for _, tolerance := range []int32{5, 0} {
		for _, tc := range []struct {
			name      string
			strategy  v1beta1.UpdateStrategyType
			group     batchGroupFixture
			mechanism workloadtypes.UpdateStrategyType
		}{
			{name: "RecreatePod", strategy: v1beta1.UpdateStrategyRecreatePod, group: batchGroupFixture{maxUnavailable: "10%", tolerance: ptr.To(tolerance)}, mechanism: workloadtypes.UpdateStrategyRecreatePod},
			{name: "SurgeThenDrain", strategy: v1beta1.UpdateStrategySurgeThenDrain, group: batchGroupFixture{maxUnavailable: "0", maxSurge: "10%", tolerance: ptr.To(tolerance)}, mechanism: workloadtypes.UpdateStrategySurgeThenDrain},
		} {
			t.Run(fmt.Sprintf("tolerance %d/%s", tolerance, tc.name), func(t *testing.T) {
				engine, decoder := twelveEight(tc.strategy)
				defaults := GroupDefaults{PrefillDecode: PrefillDecodeRules{KeepRatio: true}}
				got, stalled := driveBatchRoll(t, tc.group, engine, decoder, tc.mechanism, defaults)
				if stalled {
					t.Fatalf("the roll stalled after %s", pictures(got))
				}
				if last := got[len(got)-1]; last != (rollPicture{12, 8}) {
					t.Fatalf("the roll ended at %s, want 12:8 (%s)", last, pictures(got))
				}
				for _, p := range got {
					if off := offTheLine(p, 12, 8); off > 0.8001 {
						t.Errorf("picture %s sits %.2f decode Instances off the 3:2 line; a tolerance of 10 allows 0.8", p, off)
					}
				}
				t.Logf("tolerance %d, %s: %s", tolerance, tc.name, pictures(got))
			})
		}
	}
}

// partitionedEightEight is the 8 prefill beside 8 decode shape with a
// partition of 2 on the prefill side, both sides moving one Instance at a
// time under the given strategy: the prefill side rolls 6 Instances and
// keeps 2 on the old version, the decode side rolls all 8.
func partitionedEightEight(strategy v1beta1.UpdateStrategyType) (engine, decoder batchMemberFixture) {
	engine = batchMemberFixture{desired: 8, strategy: strategy, partition: 2}
	decoder = batchMemberFixture{desired: 8, strategy: strategy}
	if strategy == v1beta1.UpdateStrategySurgeThenDrain {
		engine.surge, decoder.surge = "1", "1"
	} else {
		engine.step, decoder.step = "1", "1"
	}
	return engine, decoder
}

// TestEvaluateUpdateGate_BatchStepPlansOverTheInstancesAPartitionLetsMove
// is the 8 prefill beside 8 decode picture in one rollingUpdate group, both
// sides moving one Instance at a time, with a partition of 2 on the prefill
// side. The batch step plans each side's batches over the Instances the
// roll may move, the side's count less the ones its partition holds: once
// the 6 movable prefill Instances serve the prefill side is done, and the
// decode side's seventh and eighth starts are admitted instead of waiting
// for a seventh prefill batch that can never serve. The whole roll walks
// 1:1 to 6:8 and ends. A hold on a partitioned side names its movable
// count, a side the partition holds whole is done at once, and with the
// decoder named first the prefill side's last batch does not wait on a
// decode Instance the partition holds. The partition lowered to zero
// afterwards starts the two held Instances as a new batch: one at a time
// under a configured tolerance, and outside the batch pacing when none is
// configured, since the decoder is wholly on version 2 by then and the old
// prefill have no partner of their version left. The drain-first path and
// the surge path read alike.
func TestEvaluateUpdateGate_BatchStepPlansOverTheInstancesAPartitionLetsMove(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	for _, mechanism := range []workloadtypes.UpdateStrategyType{workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain} {
		t.Run(string(mechanism), func(t *testing.T) {
			engine, decoder := partitionedEightEight(v1beta1.UpdateStrategyType(mechanism))
			ask := func(t *testing.T, e, d batchMemberFixture, comp v1beta1.ComponentType, defaults GroupDefaults) (bool, string) {
				t.Helper()
				fixtures := map[v1beta1.ComponentType]batchMemberFixture{eng: e, dec: d}
				allowed, _, reason := batchGateWith(t, batchService(batchGroupFixture{}, fixtures), batchClient(t, fixtures), comp, mechanism, 0, defaults)
				return allowed, reason
			}
			t.Run("the decoder's eighth start is admitted once the six movable prefill Instances serve", func(t *testing.T) {
				e, d := engine, decoder
				e.promoted, d.promoted = 6, 7
				if allowed, reason := ask(t, e, d, dec, GroupDefaults{}); !allowed {
					t.Fatalf("the decoder's eighth start must be admitted; got the hold %q", reason)
				}
			})
			t.Run("the roll walks 1:1 to 6:8 and ends", func(t *testing.T) {
				want := []rollPicture{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}, {6, 7}, {6, 8}}
				got, stalled := driveBatchRoll(t, batchGroupFixture{}, engine, decoder, mechanism, GroupDefaults{})
				if stalled {
					t.Fatalf("the roll stalled after %s", pictures(got))
				}
				if pictures(got) != pictures(want) {
					t.Errorf("pictures after each round:\n got %s\nwant %s", pictures(got), pictures(want))
				}
			})
			t.Run("a hold on a partitioned side names its movable count", func(t *testing.T) {
				e, d := engine, decoder
				e.promoted, d.promoted = 4, 5
				if mechanism == workloadtypes.UpdateStrategySurgeThenDrain {
					e.surging = 1
				} else {
					e.inFlight = 1
				}
				want := "decoder batch 6 waits for engine batch 5 to serve (4 of 5 serving, 6 of 8 movable)"
				if allowed, reason := ask(t, e, d, dec, GroupDefaults{}); allowed || reason != want {
					t.Fatalf("allowed=%v reason=%q, want the hold %q", allowed, reason, want)
				}
			})
			t.Run("a side the partition holds whole is done at once", func(t *testing.T) {
				e, d := engine, decoder
				e.partition, d.promoted = 8, 3
				if allowed, reason := ask(t, e, d, dec, GroupDefaults{}); !allowed {
					t.Fatalf("the decoder rolls alone beside a prefill side held whole; got the hold %q", reason)
				}
			})
			t.Run("with the decoder named first, the prefill side's last batch does not wait on a decode Instance the partition holds", func(t *testing.T) {
				e, d := decoder, engine
				e.promoted, d.promoted = 6, 6
				if allowed, reason := ask(t, e, d, eng, keepRatioDecoderFirst(100)); !allowed {
					t.Fatalf("the engine's seventh start must be admitted; got the hold %q", reason)
				}
			})
			t.Run("the partition lowered to zero starts the held Instances as a new batch, one at a time under a configured tolerance", func(t *testing.T) {
				e, d := engine, decoder
				e.partition, e.promoted, d.promoted = 0, 6, 8
				if allowed, reason := ask(t, e, d, eng, keepRatio(100)); !allowed {
					t.Fatalf("the first held Instance's start must be admitted; got the hold %q", reason)
				}
				if mechanism == workloadtypes.UpdateStrategySurgeThenDrain {
					e.surging = 1
				} else {
					e.inFlight = 1
				}
				want := "engine batch 8 waits for engine batch 7 to serve (6 of 7 serving)"
				if allowed, reason := ask(t, e, d, eng, keepRatio(100)); allowed || reason != want {
					t.Fatalf("allowed=%v reason=%q, want the hold %q", allowed, reason, want)
				}
				e.surging, e.inFlight = 0, 0
				got, stalled := driveBatchRoll(t, batchGroupFixture{}, e, d, mechanism, keepRatio(100))
				if want := []rollPicture{{7, 8}, {8, 8}}; stalled || pictures(got) != pictures(want) {
					t.Errorf("pictures after each round:\n got %s (stalled %v)\nwant %s", pictures(got), stalled, pictures(want))
				}
			})
			t.Run("with no tolerance configured the held Instances roll outside the batch pacing, the decoder being wholly on version 2", func(t *testing.T) {
				e, d := engine, decoder
				e.partition, e.promoted, d.promoted = 0, 6, 8
				if mechanism == workloadtypes.UpdateStrategySurgeThenDrain {
					e.surging = 1
				} else {
					e.inFlight = 1
				}
				if allowed, reason := ask(t, e, d, eng, GroupDefaults{}); !allowed {
					t.Fatalf("the second held Instance's start is the budgets' to pace, not the batch step's; got the hold %q", reason)
				}
			})
		})
	}
}

// TestEvaluateUpdateGate_KeepRatioPlansOverTheMovableInstances drives 12
// prefill beside 8 decode, both at a 10 percent budget, under the ratio
// rule at a tolerance of 10 with a partition of 2 on the prefill side. The
// plan is made from the movable counts, 10 prefill beside 8 decode, so by
// pairedBatchPlan's own arithmetic the decode side steps one Instance per
// batch and the prefill side 2, 1, 1, 2, 1, 1, 1, 1: the picture reads
// 2:1, 3:2, 4:3, 6:4, 7:5, 8:6, 9:7, 10:8 and the roll ends with the two
// held prefill Instances on the old version. The partition lowered to zero
// afterwards plans over 12:8 again, so the held Instances start as the
// plan's last two batches of one, 11:8 then 12:8.
func TestEvaluateUpdateGate_KeepRatioPlansOverTheMovableInstances(t *testing.T) {
	prefillMarks, decodeMarks := pairedBatchPlan(10, 8, 2, 1, 10)
	want := make([]rollPicture, 0, len(prefillMarks))
	for i := range prefillMarks {
		want = append(want, rollPicture{engine: prefillMarks[i], decoder: decodeMarks[i]})
	}
	if stated := []rollPicture{{2, 1}, {3, 2}, {4, 3}, {6, 4}, {7, 5}, {8, 6}, {9, 7}, {10, 8}}; pictures(want) != pictures(stated) {
		t.Fatalf("the plan over 10:8 at steps 2 and 1 and a tolerance of 10 reads %s; this test states %s", pictures(want), pictures(stated))
	}
	for _, tc := range []struct {
		name      string
		strategy  v1beta1.UpdateStrategyType
		group     batchGroupFixture
		mechanism workloadtypes.UpdateStrategyType
	}{
		{name: "RecreatePod", strategy: v1beta1.UpdateStrategyRecreatePod, group: batchGroupFixture{maxUnavailable: "10%"}, mechanism: workloadtypes.UpdateStrategyRecreatePod},
		{name: "SurgeThenDrain", strategy: v1beta1.UpdateStrategySurgeThenDrain, group: batchGroupFixture{maxUnavailable: "0", maxSurge: "10%"}, mechanism: workloadtypes.UpdateStrategySurgeThenDrain},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, decoder := twelveEight(tc.strategy)
			engine.partition = 2
			got, stalled := driveBatchRoll(t, tc.group, engine, decoder, tc.mechanism, keepRatio(10))
			if stalled {
				t.Fatalf("the roll stalled after %s", pictures(got))
			}
			if pictures(got) != pictures(want) {
				t.Errorf("pictures after each round:\n got %s\nwant %s", pictures(got), pictures(want))
			}
			engine.partition, engine.promoted, decoder.promoted = 0, 10, 8
			got, stalled = driveBatchRoll(t, tc.group, engine, decoder, tc.mechanism, keepRatio(10))
			if want := []rollPicture{{11, 8}, {12, 8}}; stalled || pictures(got) != pictures(want) {
				t.Errorf("after the partition is lowered, pictures after each round:\n got %s (stalled %v)\nwant %s", pictures(got), stalled, pictures(want))
			}
		})
	}
}

// TestEvaluateUpdateGate_TheOtherSideReadsHeldWhenTheNamedSideIsHeld is
// the push onto an image no pod can pull on 3 prefill beside 3 decode, both
// SurgeThenDrain at a maxSurge of 1, with the decoder named to move first.
// The decoder's first surge fails three times and its retry record reads
// Held. The engine's next start belongs to batch 2 and would wait for the
// decoder's batch 2 to serve first, which a Held decoder never opens: the
// gate reads Held, names the decoder and its record, and says what resumes
// both sides. A release of the record lets the decoder start its ended
// attempt again at once, so the engine waits on that attempt with the
// Instance named, and the decoder's batch serving admits the engine. Every
// other wait is as before: the named side's own answer, a Held side that
// is not the named one (where a spent retry schedule leaves the other side
// is left open), and a group with no first side. The engine's own
// failed surge is one its schedule counted.
func TestEvaluateUpdateGate_TheOtherSideReadsHeldWhenTheNamedSideIsHeld(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge := v1beta1.UpdateStrategySurgeThenDrain
	three := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.surge, m.strategy = 3, "1", surge
		return m
	}
	one := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.surge, m.strategy = 1, "1", surge
		return m
	}
	decoderFirst := GroupDefaults{PrefillDecode: PrefillDecodeRules{FirstSide: dec}}
	engineFirst := GroupDefaults{PrefillDecode: PrefillDecodeRules{FirstSide: eng}}
	heldDecoder := "engine batch 2 waits for decoder batch 2 to serve first (0 of 2 serving, 1 failed), and the decoder is held: " +
		"update to svc-decoder-target held after 3 failed attempt(s): the image cannot be pulled; " +
		"a corrected revision, or a release of the held revision on the decoder, resumes both sides"
	heldEngine := "decoder batch 2 waits for engine batch 2 to serve first (0 of 2 serving, 1 failed), and the engine is held: " +
		"update to svc-engine-target held after 3 failed attempt(s): the image cannot be pulled; " +
		"a corrected revision, or a release of the held revision on the engine, resumes both sides"
	cases := []struct {
		name       string
		engine     batchMemberFixture
		decoder    batchMemberFixture
		comp       v1beta1.ComponentType
		defaults   GroupDefaults
		wantGate   v1beta1.RolloutHoldGate
		wantReason string
	}{
		{
			name:   "decoder named and held: the engine's next start reads Held and names the decoder",
			engine: three(batchMemberFixture{failedSurge: 1, backoff: 1}), decoder: three(batchMemberFixture{failedSurge: 1, held: 3}), comp: eng, defaults: decoderFirst,
			wantGate: v1beta1.RolloutHoldGateHeld, wantReason: heldDecoder,
		},
		{
			name:   "decoder named and held: the engine's first start reads Held too, the ended decoder attempt settles nothing while the record is held",
			engine: three(batchMemberFixture{}), decoder: three(batchMemberFixture{failedSurge: 1, held: 3}), comp: eng, defaults: decoderFirst,
			wantGate: v1beta1.RolloutHoldGateHeld, wantReason: "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving, 1 failed), and the decoder is held: " +
				"update to svc-decoder-target held after 3 failed attempt(s): the image cannot be pulled; " +
				"a corrected revision, or a release of the held revision on the decoder, resumes both sides",
		},
		{
			name:   "decoder named and held, both sides scaled to one: the engine's retry of its own batch reads Held with the decoder's record",
			engine: one(batchMemberFixture{failedSurge: 1, backoff: 1}), decoder: one(batchMemberFixture{failedSurge: 1, held: 3}), comp: eng, defaults: decoderFirst,
			wantGate: v1beta1.RolloutHoldGateHeld, wantReason: "engine batch 2 waits for decoder batch 2 to serve first (0 of 1 serving, 1 failed), and the decoder is held: " +
				"update to svc-decoder-target held after 3 failed attempt(s): the image cannot be pulled; " +
				"a corrected revision, or a release of the held revision on the decoder, resumes both sides",
		},
		{
			name:   "decoder named and held: the decoder's own answer is the gate's, not the retry schedule's",
			engine: three(batchMemberFixture{failedSurge: 1, backoff: 1}), decoder: three(batchMemberFixture{failedSurge: 1, held: 3}), comp: dec, defaults: decoderFirst,
		},
		{
			name:   "engine named and held: the decoder's next start reads Held and names the engine",
			engine: three(batchMemberFixture{failedSurge: 1, held: 3}), decoder: three(batchMemberFixture{failedSurge: 1, backoff: 1}), comp: dec, defaults: engineFirst,
			wantGate: v1beta1.RolloutHoldGateHeld, wantReason: heldEngine,
		},
		{
			name:   "the record released: the decoder starts its ended attempt again at once and the engine's next start waits on it",
			engine: three(batchMemberFixture{failedSurge: 1, backoff: 1}), decoder: three(batchMemberFixture{failedSurge: 1}), comp: eng, defaults: decoderFirst,
			wantGate: v1beta1.RolloutHoldGateRatio, wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving; decoder Instance 0 starts again)",
		},
		{
			name:   "the decoder's retry served and its second surge is open: the engine still waits for it",
			engine: three(batchMemberFixture{failedSurge: 1, backoff: 1}), decoder: three(batchMemberFixture{promoted: 1, surging: 1}), comp: eng, defaults: decoderFirst,
			wantGate: v1beta1.RolloutHoldGateRatio, wantReason: "engine batch 2 waits for decoder batch 2 to serve first (1 of 2 serving)",
		},
		{
			name:   "the decoder's second batch serves: the engine's retry is admitted",
			engine: three(batchMemberFixture{failedSurge: 1, backoff: 1}), decoder: three(batchMemberFixture{promoted: 2}), comp: eng, defaults: decoderFirst,
		},
		{
			name:   "decoder named, the engine held: the decoder waits on the engine's batch as before",
			engine: three(batchMemberFixture{failedSurge: 1, held: 3}), decoder: three(batchMemberFixture{promoted: 2}), comp: dec, defaults: decoderFirst,
			wantGate: v1beta1.RolloutHoldGateRatio, wantReason: "decoder batch 3 waits for engine batch 2 to serve (0 of 2 serving, 1 failed)",
		},
		{
			name:   "no first side, the decoder held: the engine waits on the decoder's batch as before",
			engine: three(batchMemberFixture{promoted: 2}), decoder: three(batchMemberFixture{failedSurge: 1, held: 3}), comp: eng,
			wantGate: v1beta1.RolloutHoldGateRatio, wantReason: "engine batch 3 waits for decoder batch 2 to serve (0 of 2 serving, 1 failed)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			members := map[v1beta1.ComponentType]batchMemberFixture{eng: tc.engine, dec: tc.decoder}
			isvc := batchService(batchGroupFixture{maxUnavailable: "0", maxSurge: "1"}, members)
			allowed, gate, reason := batchGateWith(t, isvc, batchClient(t, members), tc.comp, workloadtypes.UpdateStrategySurgeThenDrain, 0, tc.defaults)
			if wantHold := tc.wantGate != ""; wantHold == allowed {
				t.Fatalf("allowed=%v, want hold=%v (gate=%q reason=%q)", allowed, wantHold, gate, reason)
			}
			if gate != tc.wantGate {
				t.Errorf("gate=%q, want %q (reason=%q)", gate, tc.wantGate, reason)
			}
			if reason != tc.wantReason {
				t.Errorf("reason=%q, want %q", reason, tc.wantReason)
			}
		})
	}
}
