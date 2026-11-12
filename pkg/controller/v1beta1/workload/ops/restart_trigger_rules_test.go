package ops_test

import (
	"regexp"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// triggerWorld is what one trigger reading sees: the pass input, the
// Component plan, the Instance and its pods.
type triggerWorld struct {
	input workload.ReconcileInput
	plan  workload.ComponentPlan
	inst  workload.InstancePlan
	pods  []*corev1.Pod
}

// triggerAround names a reason by the fragments of its text around the
// words the test does not spell; between two fragments stands exactly one
// word. The token {summary} stands for the row's recorded failure as
// ops.RepairFailureSummary reads it.
type triggerAround []string

func (a triggerAround) matches(reason, summary string) bool {
	quoted := make([]string, len(a))
	for i, part := range a {
		quoted[i] = regexp.QuoteMeta(strings.ReplaceAll(part, "{summary}", summary))
	}
	return regexp.MustCompile("^" + strings.Join(quoted, `\S+`) + "$").MatchString(reason)
}

// triggerRow is one rule of DetectRestartTriggerWithPods: the world and
// the answer it must give. reason is exact unless around is set.
type triggerRow struct {
	name   string
	world  func(t *testing.T) triggerWorld
	needs  bool
	reason string
	around triggerAround
}

// rowWorld is a world built from one status row with no cluster behind it,
// the plan of a single-pod engine under the policy given.
func rowWorld(row workload.InstanceStatus, policy workload.RestartPolicy, pods []*corev1.Pod, blocks ...workload.RetryBlock) triggerWorld {
	plan := buildPlanSinglePodEngine(1)
	plan.RestartPolicy = policy
	return triggerWorld{input: gangLossInput(row, blocks...), plan: plan, inst: plan.Instances[0], pods: pods}
}

// readyWorld is a Ready single-pod engine Instance read through the fake
// cluster, under RecreateInstance, with the pods given.
func readyWorld(t *testing.T, pods ...*corev1.Pod) triggerWorld {
	t.Helper()
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	c := newFakeClient(t, isvc, ir)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	return triggerWorld{input: buildTestInput(isvc, c, workload.ComponentEngine), plan: plan, inst: plan.Instances[0], pods: pods}
}

// refusedRepairWorld is a repair stopped at Failed whose rebuild the
// apiserver refused, its failure recorded failedAgo before now, with a
// schedule of three attempts a minute apart.
func refusedRepairWorld(t *testing.T, failedAgo time.Duration) triggerWorld {
	t.Helper()
	resetExpectations(t)
	now := time.Now().Truncate(time.Second)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 2)
	row := spentRepairRowOn(now, 0, workload.RejectionReasonInvalidPodSpec)
	row.LastFailure.Time = metav1.NewTime(now.Add(-failedAgo))
	ir.Status.InstanceStatuses[0] = row
	c := newFakeClient(t, isvc, ir)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.Clock = clocktesting.NewFakeClock(now)
	input.UpdateRetryPolicy = retryLadder(3, time.Minute)
	input.PassWake = &workload.PassWake{}
	return triggerWorld{input: input, plan: plan, inst: plan.Instances[0]}
}

// brokenSetRepairWorld is a repair stopped at Failed whose rebuilt pod is
// still waiting in CrashLoopBackOff, its failure recorded failedAgo before
// now, with the same schedule.
func brokenSetRepairWorld(t *testing.T, failedAgo time.Duration) triggerWorld {
	t.Helper()
	input, plan, pod, _, _ := spentRepairFixture(t, time.Now().Truncate(time.Second), 0, failedAgo, retryLadder(3, time.Minute))
	return triggerWorld{input: input, plan: plan, inst: plan.Instances[0], pods: []*corev1.Pod{pod}}
}

var triggerRows = []triggerRow{
	{name: "no row for the Instance answers nothing", world: func(t *testing.T) triggerWorld {
		plan := buildPlanSinglePodEngine(1)
		plan.RestartPolicy = workload.RestartPolicyRecreateInstance
		return triggerWorld{plan: plan, inst: plan.Instances[0]}
	}},
	{name: "a migrating row answers nothing", world: func(t *testing.T) triggerWorld {
		return rowWorld(workload.InstanceStatus{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseMigrating}, workload.RestartPolicyRecreateInstance, nil)
	}},
	{name: "a Ready row an open migration owns is not repaired for its missing pods", world: func(t *testing.T) triggerWorld {
		return rowWorld(workload.InstanceStatus{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseReady,
			Operation: &workload.InstanceOperation{Type: workload.InstanceOperationMigrate, Step: "Drain"}}, workload.RestartPolicyRecreateInstance, nil)
	}},
	{name: "an open restart is driven on whatever the policy says", world: func(t *testing.T) triggerWorld {
		return rowWorld(workload.InstanceStatus{Index: 0, Incarnation: 2, Phase: workload.InstancePhaseRestarting,
			Operation: &workload.InstanceOperation{Type: workload.InstanceOperationRestart, Step: "Drain"}}, workload.RestartPolicyNone, nil)
	}, needs: true},
	{name: "a repair whose rebuild was refused retries once its schedule is due", world: func(t *testing.T) triggerWorld {
		return refusedRepairWorld(t, 2*time.Minute)
	}, needs: true, around: triggerAround{"repair ", " (attempt 1) after {summary}"}},
	{name: "a repair whose rebuild was refused waits for its schedule, pods or none", world: func(t *testing.T) triggerWorld {
		return refusedRepairWorld(t, 10*time.Second)
	}},
	{name: "a repair stopped at Failed with no live pod left closes", world: func(t *testing.T) triggerWorld {
		resetExpectations(t)
		now := time.Now().Truncate(time.Second)
		isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 2)
		ir.Status.InstanceStatuses[0] = spentRepairRow(now, 0)
		c := newFakeClient(t, isvc, ir)
		plan := buildPlanSinglePodEngineForRestart(c, isvc)
		return triggerWorld{input: buildTestInput(isvc, c, workload.ComponentEngine), plan: plan, inst: plan.Instances[0]}
	}, needs: true, around: triggerAround{"repair ", " after {summary} closed: no live pod left to resume or rebuild"}},
	{name: "a repair stopped at Failed whose set is still broken retries once its schedule is due", world: func(t *testing.T) triggerWorld {
		return brokenSetRepairWorld(t, 2*time.Minute)
	}, needs: true, around: triggerAround{"repair ", " (attempt 1) after {summary}"}},
	{name: "a repair stopped at Failed waits for its schedule while its set is broken", world: func(t *testing.T) triggerWorld {
		return brokenSetRepairWorld(t, 10*time.Second)
	}},
	{name: "a crash-loop stop whose set serves again is reopened", world: func(t *testing.T) triggerWorld {
		h := newHeldCrashLoop(t, v1beta1.OMENativeInstanceFailed, true, 30*time.Second)
		return triggerWorld{input: h.input(true), plan: h.plan, inst: h.plan.Instances[0], pods: []*corev1.Pod{h.pod}}
	}, needs: true, around: triggerAround{"crash-loop ", " on revision " + crashLoopRevision + " serves again"}},
	{name: "a pod waiting in a terminal reason past the grace is repaired under policy None", world: func(t *testing.T) triggerWorld {
		input, plan, pod := crashLoopInput(t, time.Minute)
		return triggerWorld{input: input, plan: plan, inst: plan.Instances[0], pods: []*corev1.Pod{pod}}
	}, needs: true, around: triggerAround{"pod llama-70b-engine-0-default-0 ", " in CrashLoopBackOff past the ", "-pod grace"}},
	{name: "a crash of the promoted set inside its window is remembered under policy None", world: func(t *testing.T) triggerWorld {
		h := newPushedUnreadyAfterCrash(t, workload.RestartPolicyNone)
		return triggerWorld{input: h.input(), plan: h.plan, inst: h.plan.Instances[0], pods: []*corev1.Pod{h.pod}}
	}, needs: true, reason: "pod llama-70b-engine-0-default-0 crashed after promotion: Error"},
	{name: "a policy other than RecreateInstance repairs no pod loss", world: func(t *testing.T) triggerWorld {
		w := readyWorld(t)
		w.plan.RestartPolicy = workload.RestartPolicyNone
		return w
	}},
	{name: "a forming gang that lost a member is rebuilt", world: func(t *testing.T) triggerWorld {
		status := workload.InstanceStatus{Index: 0, Incarnation: 74, Phase: workload.InstancePhaseCreating, PodCount: 2,
			RunningRevision: gangLossRevision, TargetRevision: gangLossRevision,
			Operation: &workload.InstanceOperation{Type: workload.InstanceOperationCreate, Step: "CreatePods"}}
		plan := workload.ComponentPlan{Component: workload.ComponentEngine, RestartPolicy: workload.RestartPolicyRecreateInstance}
		inst := workload.InstancePlan{Index: 0, Incarnation: 74, Runners: []workload.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}
		return triggerWorld{input: gangLossInput(status), plan: plan, inst: inst, pods: gangLossPods(1)}
	}, needs: true, reason: "gang member lost: 1 of 2 pods present"},
	{name: "a row below Ready that never ran a revision is left to Create", world: func(t *testing.T) triggerWorld {
		return rowWorld(workload.InstanceStatus{Index: 0, Incarnation: 1, Phase: workload.InstancePhasePending}, workload.RestartPolicyRecreateInstance, nil)
	}},
	{name: "a Ready row set back to Pending for losing its pods is rebuilt by the count", world: func(t *testing.T) triggerWorld {
		return rowWorld(workload.InstanceStatus{Index: 0, Incarnation: 1, Phase: workload.InstancePhasePending, RunningRevision: gangLossRevision}, workload.RestartPolicyRecreateInstance, nil)
	}, needs: true, reason: "pod count 0 below desired 1"},
	{name: "a row set back to Pending whose revision is held is not rebuilt", world: func(t *testing.T) triggerWorld {
		return rowWorld(workload.InstanceStatus{Index: 0, Incarnation: 1, Phase: workload.InstancePhasePending, RunningRevision: gangLossRevision}, workload.RestartPolicyRecreateInstance, nil,
			workload.RetryBlock{TargetRevision: gangLossRevision, State: workload.RetryBlockHeld, AttemptsStarted: 3, Reason: "Error"})
	}},
	{name: "a runner restarted after Ready is rebuilt with the termination named", world: func(t *testing.T) triggerWorld {
		resetExpectations(t)
		isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
		readySince := metav1.NewTime(time.Now().Add(-time.Hour))
		ir.Status.InstanceStatuses[0].ReadySince = &readySince
		pod := podForInstance(isvc, 0, true, true)
		pod.Status.Phase = corev1.PodRunning
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{runnerStatus(constants.MainContainerName, readySince.Add(10*time.Minute),
			&corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137, FinishedAt: metav1.NewTime(readySince.Add(9 * time.Minute))})}
		c := newFakeClient(t, isvc, ir, pod)
		plan := buildPlanSinglePodEngineForRestart(c, isvc)
		return triggerWorld{input: buildTestInput(isvc, c, workload.ComponentEngine), plan: plan, inst: plan.Instances[0], pods: []*corev1.Pod{pod}}
	}, needs: true, reason: "pod llama-70b-engine-0-default-0 container ome-container restarted after Ready: OOMKilled (exit 137)"},
	{name: "a failed pod with a termination record is rebuilt with the record named", world: func(t *testing.T) triggerWorld {
		w := readyWorld(t)
		isvc := w.input.OwnerObject.(*v1beta1.InferenceService)
		w.pods = []*corev1.Pod{failedPodOOM(isvc, 1)}
		return w
	}, needs: true, reason: "pod llama-70b-engine-0-default-0 container main failed (OOMKilled, exit 137)"},
	{name: "a failed pod without a container record is rebuilt with the pod's failure named", world: func(t *testing.T) triggerWorld {
		w := readyWorld(t)
		pod := podForInstance(w.input.OwnerObject.(*v1beta1.InferenceService), 0, false, false)
		pod.Status.Phase = corev1.PodFailed
		w.pods = []*corev1.Pod{pod}
		return w
	}, needs: true, reason: "pod llama-70b-engine-0-default-0 failed (PodFailed)"},
	{name: "a Ready Instance short of its set is rebuilt by the count, a finished pod not counted", world: func(t *testing.T) triggerWorld {
		w := readyWorld(t)
		pod := podForInstance(w.input.OwnerObject.(*v1beta1.InferenceService), 0, false, false)
		pod.Status.Phase = corev1.PodSucceeded
		w.pods = []*corev1.Pod{pod}
		return w
	}, needs: true, reason: "pod count 0 below desired 1"},
	{name: "a Ready Instance with its set present answers nothing", world: func(t *testing.T) triggerWorld {
		w := readyWorld(t)
		w.pods = []*corev1.Pod{podForInstance(w.input.OwnerObject.(*v1beta1.InferenceService), 0, true, true)}
		return w
	}},
}

// TestDetectRestartTriggerWithPodsRules holds every answer of the restart
// trigger: the rows it leaves alone, the open repair it drives, the
// stopped repairs it closes or retries, the crash loops it names, and the
// pod losses the RecreateInstance policy repairs.
func TestDetectRestartTriggerWithPodsRules(t *testing.T) {
	for _, row := range triggerRows {
		t.Run(row.name, func(t *testing.T) {
			w := row.world(t)
			needs, reason := ops.DetectRestartTriggerWithPods(w.input, w.plan, w.inst, w.pods)
			if needs != row.needs {
				t.Fatalf("needs = %t (reason %q), want %t", needs, reason, row.needs)
			}
			summary := ops.RepairFailureSummary(w.input.ObservedState.Instance(w.inst.Index))
			switch {
			case row.around != nil && !row.around.matches(reason, summary):
				t.Fatalf("reason = %q, want the fragments %q around single words, summary %q", reason, row.around, summary)
			case row.around == nil && reason != row.reason:
				t.Fatalf("reason = %q, want %q", reason, row.reason)
			}
		})
	}
}
