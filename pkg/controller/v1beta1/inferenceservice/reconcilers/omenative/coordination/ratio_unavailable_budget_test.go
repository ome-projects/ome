package coordination

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The group unavailable budget on an engine of gangs whose roll changed
// strategy while a gang surge pair stands. The operator pushed version 2
// under SurgeThenDrain and OME built one gang's replacement as a row of its
// own beside its source, the pair ratio_surge_budget_test.go describes. The
// operator then changed the update strategy to RecreatePod, so every later
// start takes a gang out of rotation and asks the unavailable budget. That
// budget is a share of the Instance count the spec asks for, so the extra
// row must not widen it: 4 gangs at 25 percent lose one gang at a time, and
// 3 of the 4 keep serving, whether the replica publishes 4 rows or 5.

// drainFirstGangRoll is gangRollReplica after the strategy change: the rows
// as the surge wrote them, the spec now asking for RecreatePod. With
// replacementServing the open pairs' replacement gangs serve beside their
// sources, the moment before the old gangs leave, and the replica counts
// them as serving.
func drainFirstGangRoll(f gangRollFixture, replacementServing bool) *v1beta1.InferenceReplica {
	ir := gangRollReplica(f)
	ir.Spec.Lifecycle.UpdateStrategy = &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyRecreatePod}
	if !replacementServing {
		return ir
	}
	for i := range ir.Status.InstanceStatuses {
		row := &ir.Status.InstanceStatuses[i]
		if row.Operation == nil || row.Operation.Step != workloadtypes.UpdateStepGangSurgeTarget {
			continue
		}
		row.ReadyPodCount, row.ServingPodCount, row.AvailablePodCount = gangPods, gangPods, gangPods
		ir.Status.ServingReplicas++
		ir.Status.ReadyReplicas++
	}
	return ir
}

func drainFirstGangClient(t *testing.T, f gangRollFixture, replacementServing bool) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(drainFirstGangRoll(f, replacementServing)).Build()
}

// namedQuarterService is the service the gangs roll in when the group names
// its budget: a rollingUpdate group of the engine alone with maxUnavailable
// 25 percent and a run open. gangRollService is the group that names none
// and is read with the default quarter.
func namedQuarterService() *v1beta1.InferenceService {
	return batchService(batchGroupFixture{maxUnavailable: "25%", components: []v1beta1.ComponentType{v1beta1.EngineComponent}}, nil)
}

// expectDrainHeld asks the gate stack about taking one more engine gang out
// of rotation with inFlight gangs already taken this pass and requires the
// Budget hold with the given numbers.
func expectDrainHeld(t *testing.T, isvc *v1beta1.InferenceService, c client.Client, inFlight int32, want string) {
	t.Helper()
	allowed, gate, reason := batchGate(t, isvc, c, v1beta1.EngineComponent, workloadtypes.UpdateStrategyRecreatePod, inFlight)
	if allowed || gate != v1beta1.RolloutHoldGateBudget || !strings.Contains(reason, want) {
		t.Errorf("drain with %d out this pass: allowed=%v gate=%q reason=%q, want the Budget hold %q", inFlight, allowed, gate, reason, want)
	}
}

// expectDrainAdmitted is the admitted counterpart.
func expectDrainAdmitted(t *testing.T, isvc *v1beta1.InferenceService, c client.Client, inFlight int32) {
	t.Helper()
	if allowed, gate, reason := batchGate(t, isvc, c, v1beta1.EngineComponent, workloadtypes.UpdateStrategyRecreatePod, inFlight); !allowed {
		t.Errorf("drain with %d out this pass: held under %q with %q, want it admitted", inFlight, gate, reason)
	}
}

// TestEvaluateUpdateGate_AGangSurgeRowDoesNotWidenTheGroupUnavailableBudget
// is the operator's story: 4 engine gangs of one leader and one worker in a
// blueGreen group of the engine alone that names no budget, so the group's
// unavailable budget is the default quarter of 4 rounded up, one gang.
// Version 2 was pushed under SurgeThenDrain, one gang's replacement stands
// beside its source, 5 rows for 4 gangs, and the operator changed the
// strategy to RecreatePod. With no pair open the first drain is admitted and
// a second in the same pass is held. With the pair open the same holds, and
// the hold names the budget of one, not a quarter of the 5 rows. Once the
// replacement gang serves beside its source the replica counts 5 serving,
// and a second drain in the same pass must still be held: otherwise two
// gangs leave rotation and 2 of the 4 serve once the pair's old gang goes.
// On 8 gangs with two pairs open the budget is 2.
func TestEvaluateUpdateGate_AGangSurgeRowDoesNotWidenTheGroupUnavailableBudget(t *testing.T) {
	isvc := gangRollService(v1beta1.CoordinationPolicyBlueGreen)
	const heldAtOne = "unavailable budget 1 exhausted (current 0, in-flight 1, would become 2)"
	t.Run("no-pair-open-4-rows", func(t *testing.T) {
		c := drainFirstGangClient(t, gangRollFixture{desired: 4}, false)
		expectDrainAdmitted(t, isvc, c, 0)
		expectDrainHeld(t, isvc, c, 1, heldAtOne)
	})
	t.Run("one-pair-open-5-rows", func(t *testing.T) {
		c := drainFirstGangClient(t, gangRollFixture{desired: 4, open: 1}, false)
		expectDrainAdmitted(t, isvc, c, 0)
		expectDrainHeld(t, isvc, c, 1, heldAtOne)
	})
	t.Run("one-pair-open-replacement-serving-5-rows", func(t *testing.T) {
		c := drainFirstGangClient(t, gangRollFixture{desired: 4, open: 1}, true)
		expectDrainAdmitted(t, isvc, c, 0)
		expectDrainHeld(t, isvc, c, 1, heldAtOne)
	})
	t.Run("one-promoted-one-pair-open-5-rows", func(t *testing.T) {
		c := drainFirstGangClient(t, gangRollFixture{desired: 4, promoted: 1, open: 1}, false)
		expectDrainAdmitted(t, isvc, c, 0)
		expectDrainHeld(t, isvc, c, 1, heldAtOne)
	})
	t.Run("eight-gangs-two-pairs-open-10-rows", func(t *testing.T) {
		c := drainFirstGangClient(t, gangRollFixture{desired: 8, open: 2}, false)
		expectDrainAdmitted(t, isvc, c, 1)
		expectDrainHeld(t, isvc, c, 2, "unavailable budget 2 exhausted (current 0, in-flight 2, would become 3)")
	})
}

// TestCheckUnavailability_PercentBudgetIsSizedByTheDesiredCountNotTheRows
// asks the unavailable budget gate alone, off the same replica: the quarter
// is scaled against the 4 gangs the spec asks for, so with one pair open and
// its replacement serving (5 rows, 5 serving) the second drain of a pass is
// held, under the default quarter and under a named one alike, and a replica
// that records no desired count is sized by its rows.
func TestCheckUnavailability_PercentBudgetIsSizedByTheDesiredCountNotTheRows(t *testing.T) {
	ask := func(t *testing.T, isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, inFlight int32) (bool, string) {
		t.Helper()
		c := fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(ir).Build()
		return ResolveGateContext(t.Context(), c, isvc, v1beta1.EngineComponent).CheckUnavailability(inFlight)
	}
	const secondHeld = "unavailable budget 1 exhausted (current 0, in-flight 1, would become 2)"
	t.Run("one-pair-open-holds-the-second-drain-of-a-pass", func(t *testing.T) {
		allowed, reason := ask(t, gangRollService(v1beta1.CoordinationPolicyBlueGreen), drainFirstGangRoll(gangRollFixture{desired: 4, open: 1}, true), 1)
		if allowed || !strings.Contains(reason, secondHeld) {
			t.Fatalf("allowed=%v reason=%q, want the budget of one spent by the first drain", allowed, reason)
		}
	})
	t.Run("a-named-quarter-reads-the-same", func(t *testing.T) {
		allowed, reason := ask(t, namedQuarterService(), drainFirstGangRoll(gangRollFixture{desired: 4, open: 1}, true), 1)
		if allowed || !strings.Contains(reason, secondHeld) {
			t.Fatalf("allowed=%v reason=%q, want the named quarter of 4 to hold the second drain", allowed, reason)
		}
	})
	t.Run("no-desired-count-recorded-sizes-by-the-rows", func(t *testing.T) {
		ir := drainFirstGangRoll(gangRollFixture{desired: 4, open: 1}, true)
		ir.Spec.Replicas = nil
		if allowed, reason := ask(t, gangRollService(v1beta1.CoordinationPolicyBlueGreen), ir, 1); !allowed {
			t.Fatalf("held with %q, want the quarter of the 5 rows, 2, to admit the second drain", reason)
		}
	})
}

// TestEvaluateUpdateGate_UnavailableBudgetHoldsWhateverPhaseTheGroupReads
// builds the pictures of the gang roll in which the group reads each rolling
// phase and Failed, the pictures the surge budget is proved on, checks the
// phase off the same replica the gate reads, and requires the budget of one
// on the next drain in every one. Where the picture has a gang down already,
// a promoted gang restarting (Waiting) or a gang whose pods died (Failed),
// the first drain of the pass is held; elsewhere the second is.
func TestEvaluateUpdateGate_UnavailableBudgetHoldsWhateverPhaseTheGroupReads(t *testing.T) {
	blueGreen, rolling := v1beta1.CoordinationPolicyBlueGreen, v1beta1.CoordinationPolicyRollingUpdate
	const secondHeld = "unavailable budget 1 exhausted (current 0, in-flight 1, would become 2)"
	const firstHeld = "unavailable budget 1 exhausted (current 1, in-flight 0, would become 2)"
	cases := []struct {
		name     string
		policy   v1beta1.CoordinationPolicy
		fixture  gangRollFixture
		phase    v1beta1.CoordinationPhase
		inFlight int32
		want     string
	}{
		{"Surging", blueGreen, gangRollFixture{desired: 4, open: 1}, v1beta1.CoordinationPhaseSurging, 1, secondHeld},
		{"Waiting", blueGreen, gangRollFixture{desired: 4, promoted: 1, open: 1, promotedUnready: true}, v1beta1.CoordinationPhaseWaiting, 0, firstHeld},
		{"Shifting", blueGreen, gangRollFixture{desired: 4, promoted: 1, open: 1}, v1beta1.CoordinationPhaseShifting, 1, secondHeld},
		{"ScalingDown", rolling, gangRollFixture{desired: 4, promoted: 1, open: 1}, v1beta1.CoordinationPhaseScalingDown, 1, secondHeld},
		{"Failed", blueGreen, gangRollFixture{desired: 4, open: 1, failed: 1}, v1beta1.CoordinationPhaseFailed, 0, firstHeld},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := gangRollPhase(tc.policy, tc.fixture); got != tc.phase {
				t.Fatalf("group phase %q, want %q", got, tc.phase)
			}
			expectDrainHeld(t, gangRollService(tc.policy), drainFirstGangClient(t, tc.fixture, false), tc.inFlight, tc.want)
		})
	}
}

// TestCheckUnavailability_AReplacementGangServingBesideItsSourceCountsOnce
// keeps a second gang in rotation while one is already down. 4 gangs with
// one allowed down: gang 1 is down and gang 0's replacement serves beside
// its source, so the replica counts 4 serving rows for the 4 gangs the spec
// asks for, though only 3 Instances serve. The budget counts the
// replacement once with its source, reads one Instance out, and holds the
// next drain naming the gang down. A promoted gang whose pods are
// restarting beside the serving pair reads the same.
func TestCheckUnavailability_AReplacementGangServingBesideItsSourceCountsOnce(t *testing.T) {
	const firstHeld = "unavailable budget 1 exhausted (current 1, in-flight 0, would become 2)"
	cases := []struct {
		name    string
		fixture gangRollFixture
		phase   v1beta1.CoordinationPhase
		want    string
	}{
		{"one-gang-down-beside-the-serving-pair", gangRollFixture{desired: 4, open: 1, failed: 1}, v1beta1.CoordinationPhaseFailed, firstHeld + "; counted as unavailable: Instance 1 Failed"},
		{"a-promoted-gang-restarting-beside-the-serving-pair", gangRollFixture{desired: 4, promoted: 1, open: 1, promotedUnready: true}, v1beta1.CoordinationPhaseWaiting, firstHeld},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := gangRollPhase(v1beta1.CoordinationPolicyBlueGreen, tc.fixture); got != tc.phase {
				t.Fatalf("group phase %q, want %q", got, tc.phase)
			}
			expectDrainHeld(t, gangRollService(v1beta1.CoordinationPolicyBlueGreen), drainFirstGangClient(t, tc.fixture, true), 0, tc.want)
		})
	}
}

// crashLoopDecoderPods are the decoder's four single pods as the engine
// labels them under the service's replica prefix, every one Ready and
// serving; the kubelet restarted the runner of Instance 0, whose new run
// is Ready again for readyAgainFor.
func crashLoopDecoderPods(now time.Time, readyAgainFor time.Duration) []client.Object {
	objs := make([]client.Object, 0, 4)
	for idx := int32(0); idx < 4; idx++ {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: batchNamespace,
			Name:      fmt.Sprintf("%s-decoder-%d-default-0", batchServiceName, idx),
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: batchServiceName,
				constants.OMEComponentLabel:           string(v1beta1.DecoderComponent),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelInstanceIdx:                strconv.Itoa(int(idx)),
				query.LabelRunner:                     "default",
				query.LabelPodOrdinal:                 "0",
			},
			CreationTimestamp: metav1.NewTime(now.Add(-time.Hour)),
		}}
		readyAt := now.Add(-time.Hour)
		runner := corev1.ContainerStatus{Name: constants.MainContainerName, Ready: true,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(readyAt)}}}
		if idx == 0 {
			readyAt = now.Add(-readyAgainFor)
			runner.RestartCount = 1
			runner.State.Running.StartedAt = metav1.NewTime(readyAt.Add(-2 * time.Second))
			runner.LastTerminationState = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 1, Reason: "Error",
				StartedAt:  metav1.NewTime(now.Add(-time.Hour)),
				FinishedAt: metav1.NewTime(readyAt.Add(-3 * time.Second)),
			}}
		}
		pod.Status.Phase = corev1.PodRunning
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{runner}
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(readyAt)},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(readyAt)},
			{Type: query.ServingConditionType, Status: corev1.ConditionTrue},
		}
		objs = append(objs, pod)
	}
	return objs
}

// TestEvaluateUpdateGate_UnavailableBudgetCountsAnInstanceUpOnlyBetweenItsCrashes
// is the operator's story: 4 prefill and 4 decode Instances in one
// rollingUpdate group with the group's maxUnavailable 1 and each
// Component's own 2, under RecreatePod. One decode runner dies every 15 s
// and the kubelet restarts it, and the replica counts the Instance as
// serving whenever its pod is Ready at the status write. A push asks to
// take a serving decode Instance out of rotation while the crash-looping
// one is Ready again for 5 s of the 30 s window: that Instance is the one
// the group's budget allows down, so the start is held under Budget and
// the hold names it. A gate that read the published count of 4 serving
// would admit the start and leave 2 of the 4 decode Instances out while
// the group promised at most one. Once the pod
// has held Ready for the window the start is admitted. A row that records
// no entry into Ready reads the pod's own restart record the same way, and
// the window is the operator's grace for a pod the kubelet keeps restarting
// when minReadySeconds is unset.
func TestEvaluateUpdateGate_UnavailableBudgetCountsAnInstanceUpOnlyBetweenItsCrashes(t *testing.T) {
	members := map[v1beta1.ComponentType]batchMemberFixture{
		v1beta1.EngineComponent:  {desired: 4, step: "2", strategy: v1beta1.UpdateStrategyRecreatePod},
		v1beta1.DecoderComponent: {desired: 4, step: "2", strategy: v1beta1.UpdateStrategyRecreatePod},
	}
	if got := batchGroupPhase(members, v1beta1.EngineComponent, v1beta1.DecoderComponent); got != v1beta1.CoordinationPhaseSurging {
		t.Fatalf("group phase %q, want %q", got, v1beta1.CoordinationPhaseSurging)
	}
	isvc := batchService(batchGroupFixture{maxUnavailable: "1"}, members)
	const held = "unavailable budget 1 exhausted (current 1, in-flight 0, would become 2); Instance 0 not serving (pod svc-decoder-0-default-0 restarted"
	cases := []struct {
		name            string
		readyAgainFor   time.Duration
		noReadyAnchor   bool
		minReadySeconds int32
		defaults        GroupDefaults
		want            string
	}{
		{name: "Ready again for 5 s of a 30 s window", readyAgainFor: 5 * time.Second, minReadySeconds: 30,
			want: held + " since Ready, Ready again for less than the 30s window"},
		{name: "Ready again for the whole window", readyAgainFor: 40 * time.Second, minReadySeconds: 30},
		{name: "no record of when the Instance entered Ready", readyAgainFor: 5 * time.Second, noReadyAnchor: true, minReadySeconds: 30,
			want: held + ", with no record of when the Instance entered Ready, Ready again for less than the 30s window"},
		{name: "the window is the operator's grace", readyAgainFor: 5 * time.Second, defaults: GroupDefaults{ProvingGrace: time.Minute},
			want: held + " since Ready, Ready again for less than the 60s window"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			decoder := batchReplica(v1beta1.DecoderComponent, members[v1beta1.DecoderComponent])
			decoder.Spec.MinReadySeconds = tc.minReadySeconds
			for i := range decoder.Status.InstanceStatuses {
				if !tc.noReadyAnchor {
					decoder.Status.InstanceStatuses[i].ReadySince = &metav1.Time{Time: now.Add(-10 * time.Minute)}
				}
			}
			objs := append([]client.Object{batchReplica(v1beta1.EngineComponent, members[v1beta1.EngineComponent]), decoder},
				crashLoopDecoderPods(now, tc.readyAgainFor)...)
			c := fakeclient.New(t, fakeclient.WithObjects(objs...))
			allowed, gate, reason := batchGateWith(t, isvc, c, v1beta1.DecoderComponent, workloadtypes.UpdateStrategyRecreatePod, 0, tc.defaults)
			if tc.want == "" {
				if !allowed {
					t.Fatalf("held under %q with %q, want the start admitted once the pod has held Ready for the window", gate, reason)
				}
				return
			}
			if allowed || gate != v1beta1.RolloutHoldGateBudget || !strings.HasPrefix(reason, tc.want) {
				t.Fatalf("allowed=%v gate=%q reason=%q, want the Budget hold %q...", allowed, gate, reason, tc.want)
			}
		})
	}
}

// TestEvaluateUpdateGate_UnavailableBudgetReadsARolledInstancesFailureAgainstTheTargetsBirth
// is the operator's story: two engine gangs and a decoder roll in one
// rollingUpdate group with the group's maxUnavailable 1. One gang's leader
// on the old revision points at an image the cluster cannot pull; the
// repair that rebuilds it on that revision records the failure on its row. The push then rolls that gang
// first and its fresh set on the fixed revision comes up 21 s after the
// recorded failure, inside the operator's 10 min grace. The group's budget
// reads that failure against the target's birth, as the roll does: a death
// older than the target belongs to the set on the revision before, so
// the rolled gang serves and the healthy gang's start is admitted. A death
// recorded after the target was born is a rebuild still proving itself,
// and the start is held under Budget naming it.
func TestEvaluateUpdateGate_UnavailableBudgetReadsARolledInstancesFailureAgainstTheTargetsBirth(t *testing.T) {
	members := map[v1beta1.ComponentType]batchMemberFixture{
		v1beta1.EngineComponent:  {desired: 4, step: "2", strategy: v1beta1.UpdateStrategyRecreatePod, promoted: 1},
		v1beta1.DecoderComponent: {desired: 4, step: "2", strategy: v1beta1.UpdateStrategyRecreatePod, promoted: 1},
	}
	isvc := batchService(batchGroupFixture{maxUnavailable: "1"}, members)
	const held = "unavailable budget 1 exhausted (current 1, in-flight 0, would become 2); Instance 0 not serving (pod svc-decoder-0-default-0 rebuilt after the failure recorded at"
	cases := []struct {
		name       string
		targetBorn time.Duration // relative to the recorded failure
		want       string
	}{
		{name: "the failure older than the target", targetBorn: 10 * time.Second},
		{name: "the failure after the target was born", targetBorn: -time.Minute, want: held},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			readyAgain := now.Add(-5 * time.Second)
			created := readyAgain.Add(-6 * time.Second)
			failedAt := created.Add(-21 * time.Second)
			decoder := batchReplica(v1beta1.DecoderComponent, members[v1beta1.DecoderComponent])
			decoder.Spec.MinReadySeconds = 30
			for i := range decoder.Status.InstanceStatuses {
				decoder.Status.InstanceStatuses[i].ReadySince = &metav1.Time{Time: now.Add(-10 * time.Minute)}
			}
			rolled := &decoder.Status.InstanceStatuses[0]
			rolled.ReadySince = &metav1.Time{Time: readyAgain}
			rolled.LastFailure = &v1beta1.InstanceTermination{PodName: "svc-decoder-0-default-0", ContainerName: constants.MainContainerName, Reason: "Error", Time: metav1.NewTime(failedAt)}
			objs := append([]client.Object{batchReplica(v1beta1.EngineComponent, members[v1beta1.EngineComponent]), decoder},
				crashLoopDecoderPods(now, 5*time.Second)...)
			fresh := objs[2].(*corev1.Pod)
			fresh.CreationTimestamp = metav1.NewTime(created)
			fresh.Status.ContainerStatuses[0].RestartCount = 0
			fresh.Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{}
			fresh.Status.ContainerStatuses[0].State.Running.StartedAt = metav1.NewTime(created)
			c := fakeclient.New(t, fakeclient.WithObjects(objs...))
			target := forwardTarget(isvc, v1beta1.DecoderComponent)
			target.CreationTimestamp = metav1.NewTime(failedAt.Add(tc.targetBorn))
			allowed, gate, reason := EvaluateUpdateGate(context.Background(), c, isvc, v1beta1.DecoderComponent, "", target, nil, GroupDefaults{}, workloadtypes.UpdateStrategyRecreatePod, 0, 0)
			if tc.want == "" {
				if !allowed {
					t.Fatalf("held under %q with %q, want the start admitted: the failure belongs to the set on the revision before", gate, reason)
				}
				return
			}
			if allowed || gate != v1beta1.RolloutHoldGateBudget || !strings.HasPrefix(reason, tc.want) {
				t.Fatalf("allowed=%v gate=%q reason=%q, want the Budget hold %q...", allowed, gate, reason, tc.want)
			}
		})
	}
}

// TestEvaluateUpdateGate_UnavailableBudgetReadsTheHandedPods proves the gate
// reads the Component's own pods from the list the caller hands it, once per
// consult, and lists them through the reader only when no list is handed:
// a reader that holds no pods beside a handed list carrying the
// crash-looping decode Instance holds the start and names the Instance; the
// same pods in the reader and no list handed hold it too; a handed list that
// fails holds the start closed.
func TestEvaluateUpdateGate_UnavailableBudgetReadsTheHandedPods(t *testing.T) {
	members := map[v1beta1.ComponentType]batchMemberFixture{
		v1beta1.EngineComponent:  {desired: 4, step: "2", strategy: v1beta1.UpdateStrategyRecreatePod},
		v1beta1.DecoderComponent: {desired: 4, step: "2", strategy: v1beta1.UpdateStrategyRecreatePod},
	}
	isvc := batchService(batchGroupFixture{maxUnavailable: "1"}, members)
	now := time.Now()
	decoder := batchReplica(v1beta1.DecoderComponent, members[v1beta1.DecoderComponent])
	decoder.Spec.MinReadySeconds = 30
	for i := range decoder.Status.InstanceStatuses {
		decoder.Status.InstanceStatuses[i].ReadySince = &metav1.Time{Time: now.Add(-10 * time.Minute)}
	}
	replicas := []client.Object{batchReplica(v1beta1.EngineComponent, members[v1beta1.EngineComponent]), decoder}
	crashLooping := crashLoopDecoderPods(now, 5*time.Second)
	pods := make([]*corev1.Pod, 0, len(crashLooping))
	for _, obj := range crashLooping {
		pods = append(pods, obj.(*corev1.Pod))
	}
	const held = "unavailable budget 1 exhausted (current 1, in-flight 0, would become 2); Instance 0 not serving (pod svc-decoder-0-default-0 restarted since Ready"
	cases := []struct {
		name      string
		inReader  []client.Object
		handed    func() ([]*corev1.Pod, error)
		want      string
		wantReads int
	}{
		{name: "the handed list carries the crash-looping Instance, the reader holds no pods", inReader: replicas,
			handed: func() ([]*corev1.Pod, error) { return pods, nil }, want: held, wantReads: 1},
		{name: "no list handed, the reader holds the pods", inReader: append(append([]client.Object{}, replicas...), crashLooping...), want: held},
		{name: "the handed list fails", inReader: replicas,
			handed: func() ([]*corev1.Pod, error) { return nil, errors.New("pod list refused") },
			want:   "cannot list decoder pods, failing closed: pod list refused", wantReads: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := fakeclient.New(t, fakeclient.WithObjects(tc.inReader...))
			reads := 0
			var source PodSource
			if tc.handed != nil {
				source = func() ([]*corev1.Pod, error) { reads++; return tc.handed() }
			}
			allowed, gate, reason := EvaluateUpdateGateWithPods(context.Background(), c, isvc, v1beta1.DecoderComponent, "", forwardTarget(isvc, v1beta1.DecoderComponent), nil, GroupDefaults{}, workloadtypes.UpdateStrategyRecreatePod, 0, 0, source)
			if allowed || gate != v1beta1.RolloutHoldGateBudget || !strings.HasPrefix(reason, tc.want) {
				t.Fatalf("allowed=%v gate=%q reason=%q, want the Budget hold %q...", allowed, gate, reason, tc.want)
			}
			if reads != tc.wantReads {
				t.Fatalf("the handed list was read %d time(s) in one consult, want %d", reads, tc.wantReads)
			}
		})
	}
}
