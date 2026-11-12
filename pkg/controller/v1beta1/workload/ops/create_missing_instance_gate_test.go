package ops

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// allowCreateForMissingInstance decides whether the Create pass may build the
// pods an Instance is missing. A fresh start at a revision with a retry
// record asks the record: a held revision, a backoff still running or an
// attempt already authorized refuses it, and the refused start is reported
// settled, with the backoff's remaining wait. A rebuild asks the record only
// about a held revision, and a refused rebuild keeps the pass's cadence.
// Under RecreateInstanceOnPodRestart a Ready Instance, a Ready one set back
// to Pending, and a leaderAndWorker short of a member are the restart pass's while a
// live pod of theirs stands. One row per rule: the Instance as the pass
// reads it and the answer.

const createGateRevision = "engine-rev2"

var createGateNow = time.Unix(1_800_000_000, 0).UTC()

// createGateWorld is what the gate reads: the pass input, the Component
// plan, the Instance plan, its pods and the revision it would build at.
type createGateWorld struct {
	input    workload.ReconcileInput
	plan     workload.ComponentPlan
	inst     workload.InstancePlan
	existing []*corev1.Pod
	rev      string
}

// createGateAnswer is the gate's answer: whether the pass may build, whether
// a refused start counts as settled, and how long a backoff still runs.
type createGateAnswer struct {
	allowed      bool
	treatedReady bool
	retryAfter   time.Duration
}

type createGateRow struct {
	name  string
	world createGateWorld
	want  createGateAnswer
}

var (
	gateAllowed  = createGateAnswer{allowed: true}
	gateSettled  = createGateAnswer{treatedReady: true}
	gateDeferred = createGateAnswer{}
)

// gateWorld is Instance 0 of an engine under the policy given, its set made
// of the runner sizes, read beside the statusRows and retry records of its
// replica; rev is the revision the pass would build at.
func gateWorld(policy workload.RestartPolicy, runners []int32, statusRows []workload.InstanceStatus, pods []*corev1.Pod, rev string, blocks ...workload.RetryBlock) createGateWorld {
	inst := workload.InstancePlan{Index: 0, Incarnation: 1}
	for i, size := range runners {
		inst.Runners = append(inst.Runners, workload.RunnerPlan{Name: fmt.Sprintf("runner-%d", i), Size: size})
	}
	plan := workload.ComponentPlan{Component: workload.ComponentEngine, RestartPolicy: policy, Instances: []workload.InstancePlan{inst}}
	input := workload.ReconcileInput{
		Clock: clocktesting.NewFakeClock(createGateNow),
		ObservedState: workload.WorkloadObservedState{
			InstanceStatuses: statusRows,
			RetryBlocks:      blocks,
			UpdateRevision:   createGateRevision,
		},
	}
	return createGateWorld{input: input, plan: plan, inst: inst, existing: pods, rev: rev}
}

// onePod is a one-runner set; leaderAndWorker is a gang of a leader and one
// worker.
var (
	onePod          = []int32{1}
	leaderAndWorker = []int32{1, 1}
)

func gateLivePod(name string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
}

func gateFinishedPod(name string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}, Status: corev1.PodStatus{Phase: corev1.PodFailed}}
}

func heldRecord(rev string) workload.RetryBlock {
	return workload.RetryBlock{TargetRevision: rev, State: workload.RetryBlockHeld, AttemptsStarted: 3, Reason: "Error"}
}

func backoffRecord(rev string, at time.Time) workload.RetryBlock {
	return workload.RetryBlock{TargetRevision: rev, State: workload.RetryBlockBackoff, AttemptsStarted: 1, NextRetryAt: &metav1.Time{Time: at}, Reason: "Error"}
}

func authorizedRecord(rev string) workload.RetryBlock {
	return workload.RetryBlock{TargetRevision: rev, State: workload.RetryBlockRetryInProgress, AttemptsStarted: 2, Reason: "Error"}
}

// The statusRows of Instance 0 the gate reads, and the peers at Instance 1.
var (
	// endedAttemptRow is a Failed row whose operation claims no verb: an ended
	// attempt, the shape a fresh start opens over.
	endedAttemptRow = workload.InstanceStatus{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseFailed}
	// formingRow is a row whose Create attempt has committed its pod set.
	formingRow = workload.InstanceStatus{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseCreating, PodCount: 2,
		Operation: &workload.InstanceOperation{Type: workload.InstanceOperationCreate, Step: status.CreateStepCreatePods, TargetRevision: createGateRevision}}
	// servingRow is a serving Instance.
	servingRow = workload.InstanceStatus{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: createGateRevision}
	// pendingAgainRow is a Ready Instance written Pending for losing every pod.
	pendingAgainRow = workload.InstanceStatus{Index: 0, Incarnation: 1, Phase: workload.InstancePhasePending, RunningRevision: createGateRevision}
	// stoppedAttemptRow is a roll attempt stopped after its deadline, its pods kept.
	stoppedAttemptRow = workload.InstanceStatus{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseFailed,
		Operation: &workload.InstanceOperation{Type: workload.InstanceOperationUpdate, Step: workload.UpdateStepParked, TargetRevision: createGateRevision, Waiting: string(workload.RolloutHoldGateRetryBlock)}}
	// updatingPeer is another Instance mid-roll toward the revision.
	updatingPeer = workload.InstanceStatus{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseUpdating,
		Operation: &workload.InstanceOperation{Type: workload.InstanceOperationUpdate, Step: workload.UpdateStepDrain, TargetRevision: createGateRevision}}
	// formingPeer is another Instance whose Create attempt is open.
	formingPeer = workload.InstanceStatus{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseCreating,
		Operation: &workload.InstanceOperation{Type: workload.InstanceOperationCreate, Step: status.CreateStepCreatePods, TargetRevision: createGateRevision}}
)

func statusRows(statuses ...workload.InstanceStatus) []workload.InstanceStatus { return statuses }

var createGateRows = []createGateRow{
	{
		name:  "a start without a revision asks no retry record",
		world: gateWorld(workload.RestartPolicyNone, onePod, nil, nil, "", heldRecord(createGateRevision)),
		want:  gateAllowed,
	},
	{
		name:  "a fresh start at a revision with no retry record is allowed",
		world: gateWorld(workload.RestartPolicyNone, onePod, nil, nil, createGateRevision),
		want:  gateAllowed,
	},
	{
		name:  "a fresh start with no row at a held revision is refused and reported settled",
		world: gateWorld(workload.RestartPolicyNone, onePod, nil, nil, createGateRevision, heldRecord(createGateRevision)),
		want:  gateSettled,
	},
	{
		name:  "a fresh start over an ended attempt waits out the backoff, reported settled with the wait",
		world: gateWorld(workload.RestartPolicyNone, onePod, statusRows(endedAttemptRow), nil, createGateRevision, backoffRecord(createGateRevision, createGateNow.Add(90*time.Second))),
		want:  createGateAnswer{treatedReady: true, retryAfter: 90 * time.Second},
	},
	{
		name:  "a fresh start opens once the backoff is due",
		world: gateWorld(workload.RestartPolicyNone, onePod, statusRows(endedAttemptRow), nil, createGateRevision, backoffRecord(createGateRevision, createGateNow.Add(-time.Second))),
		want:  gateAllowed,
	},
	{
		name:  "a fresh start waits while a roll attempt at the revision holds the one authorization",
		world: gateWorld(workload.RestartPolicyNone, onePod, statusRows(endedAttemptRow, updatingPeer), nil, createGateRevision, authorizedRecord(createGateRevision)),
		want:  gateSettled,
	},
	{
		name:  "a fresh start waits while another Instance's Create attempt is open",
		world: gateWorld(workload.RestartPolicyNone, onePod, statusRows(endedAttemptRow, formingPeer), nil, createGateRevision, authorizedRecord(createGateRevision)),
		want:  gateSettled,
	},
	{
		name:  "an authorization with no attempt in flight is due",
		world: gateWorld(workload.RestartPolicyNone, onePod, statusRows(endedAttemptRow), nil, createGateRevision, authorizedRecord(createGateRevision)),
		want:  gateAllowed,
	},
	{
		name:  "a rebuild at a held revision is refused and keeps the pass's cadence",
		world: gateWorld(workload.RestartPolicyNone, onePod, statusRows(formingRow), nil, createGateRevision, heldRecord(createGateRevision)),
		want:  gateDeferred,
	},
	{
		name:  "a rebuild asks nothing of a backoff",
		world: gateWorld(workload.RestartPolicyNone, onePod, statusRows(formingRow), nil, createGateRevision, backoffRecord(createGateRevision, createGateNow.Add(90*time.Second))),
		want:  gateAllowed,
	},
	{
		name:  "a stopped roll attempt whose pod still stands is a rebuild",
		world: gateWorld(workload.RestartPolicyNone, onePod, statusRows(stoppedAttemptRow), []*corev1.Pod{gateLivePod("engine-0")}, createGateRevision, heldRecord(createGateRevision)),
		want:  gateDeferred,
	},
	{
		name:  "a stopped roll attempt left only finished pods is a fresh start",
		world: gateWorld(workload.RestartPolicyNone, onePod, statusRows(stoppedAttemptRow), []*corev1.Pod{gateFinishedPod("engine-0")}, createGateRevision, heldRecord(createGateRevision)),
		want:  gateSettled,
	},
	{
		name:  "a Ready Instance beside a live pod is the restart pass's under RecreateInstance",
		world: gateWorld(workload.RestartPolicyRecreateInstance, leaderAndWorker, statusRows(servingRow), []*corev1.Pod{gateLivePod("engine-0")}, createGateRevision),
		want:  gateDeferred,
	},
	{
		name:  "a Ready Instance set back to Pending beside a live pod is the restart pass's",
		world: gateWorld(workload.RestartPolicyRecreateInstance, leaderAndWorker, statusRows(pendingAgainRow), []*corev1.Pod{gateLivePod("engine-0")}, createGateRevision),
		want:  gateDeferred,
	},
	{
		name:  "a formingRow leaderAndWorker short of a member is the restart pass's",
		world: gateWorld(workload.RestartPolicyRecreateInstance, leaderAndWorker, statusRows(formingRow), []*corev1.Pod{gateLivePod("engine-0")}, createGateRevision),
		want:  gateDeferred,
	},
	{
		name:  "a set left only in finished pods is rebuilt by this pass",
		world: gateWorld(workload.RestartPolicyRecreateInstance, leaderAndWorker, statusRows(servingRow), []*corev1.Pod{gateFinishedPod("engine-0"), gateFinishedPod("engine-0-worker")}, createGateRevision),
		want:  gateAllowed,
	},
	{
		name:  "under policy None a Ready Instance beside a live pod is filled by this pass",
		world: gateWorld(workload.RestartPolicyNone, leaderAndWorker, statusRows(servingRow), []*corev1.Pod{gateLivePod("engine-0")}, createGateRevision),
		want:  gateAllowed,
	},
	{
		name:  "a single-pod Instance formingRow beside its own pod is no lost leaderAndWorker",
		world: gateWorld(workload.RestartPolicyRecreateInstance, onePod, statusRows(formingRow), []*corev1.Pod{gateLivePod("engine-0")}, createGateRevision),
		want:  gateAllowed,
	},
}

func TestAllowCreateForMissingInstance(t *testing.T) {
	for _, row := range createGateRows {
		t.Run(row.name, func(t *testing.T) {
			w := row.world
			allowed, treatedReady, retryAfter := allowCreateForMissingInstance(w.input, w.plan, w.inst, w.existing, w.rev)
			got := createGateAnswer{allowed: allowed, treatedReady: treatedReady, retryAfter: retryAfter}
			if diff := cmp.Diff(row.want, got, cmp.AllowUnexported(createGateAnswer{})); diff != "" {
				t.Errorf("answer (-want +got):\n%s", diff)
			}
		})
	}
}
