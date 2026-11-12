package ops_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// olderRevisionName is a revision the row ran before the fixture revision
// became the roll target: a stored revision of its own, so a rebuild on it
// renders.
const olderRevisionName = "llama-70b-engine-older"

// heldRepairOpenedAt is when the fixture's repair started; a record's
// holding failure is dated against it.
var heldRepairOpenedAt = time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)

// heldRepairFixture is a repair at the pass that reaches its rebuild: the
// row Restarting at set number 2 with the failure that opened the repair
// on it, the old set gone, pods the case supplies, and the retry records
// the pass observes. The InstanceFailed warnings the pass emits are
// collected.
func heldRepairFixture(t *testing.T, gang bool, running string, blocks []workload.RetryBlock, pods ...client.Object) (client.Client, *workload.ReconcileInput, workload.ComponentPlan, *[]string) {
	t.Helper()
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	runner := "default"
	if gang {
		runner = "leader"
	}
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     2,
		Phase:           v1beta1.OMENativeInstanceRestarting,
		RunningRevision: running,
		Operation:       &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "runner restarted after Ready", StartedAt: metav1.NewTime(heldRepairOpenedAt)},
		LastFailure:     &v1beta1.InstanceTermination{PodName: query.PodName(isvc.Name, workload.ComponentEngine, 0, runner, 0), ContainerName: "main", Reason: "Error", ExitCode: ptr.To(int32(1))},
	}
	objs := []client.Object{isvc, ir, fixtureRevision(t, isvc), storedRevision(t, isvc, olderRevisionName, fixtureTemplate(), nil)}
	objs = append(objs, pods...)
	c := newFakeClient(t, objs...)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.ObservedState.RetryBlocks = blocks
	var warnings []string
	input.WarnInstanceFailed = func(idx int32, pod, reason string) {
		warnings = append(warnings, fmt.Sprintf("instance=%d pod=%s %s", idx, pod, reason))
	}
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	if gang {
		plan = buildPlanGangEngine(workload.RestartPolicyRecreateInstance)
	}
	return c, &input, plan, &warnings
}

// heldRecord is a record held by a failure a minute after the repair
// opened.
func heldRecord(rev string) workload.RetryBlock {
	return workload.RetryBlock{TargetRevision: rev, State: workload.RetryBlockHeld, AttemptsStarted: 1, Reason: "Error", LastFailureAt: ptr.To(metav1.NewTime(heldRepairOpenedAt.Add(time.Minute)))}
}

// heldRecordBeforeTheRepair is a record held by a failure a minute before
// the repair opened.
func heldRecordBeforeTheRepair(rev string) workload.RetryBlock {
	return workload.RetryBlock{TargetRevision: rev, State: workload.RetryBlockHeld, AttemptsStarted: 1, Reason: "Error", LastFailureAt: ptr.To(metav1.NewTime(heldRepairOpenedAt.Add(-time.Minute)))}
}

func fixtureRevisionName() string { return "llama-70b-engine-" + testRevisionHash }

func podsIn(t *testing.T, c client.Client) []corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	return pods.Items
}

func rowOf(c client.Client, isvcName string) v1beta1.OMENativeInstanceStatus {
	isvc := minimalISVC(isvcName, "prod", 1)
	return instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
}

func runRepairPass(t *testing.T, c client.Client, input workload.ReconcileInput, plan workload.ComponentPlan, withTarget bool) {
	t.Helper()
	target := fixtureRevision(t, minimalISVC("llama-70b", "prod", 1))
	if !withTarget {
		target = nil
	}
	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], target, "trigger"); err != nil {
		t.Fatalf("Restart: %v", err)
	}
}

// A repair reaches its first create and finds the retry record of the
// revision it would rebuild held by a failure that landed after the
// repair opened: another Instance's crash spent the record's last attempt
// after the trigger read a record that admitted the rebuild. The repair
// builds nothing and writes the row Failed with no operation, keeping the
// failure it last recorded, and one InstanceFailed warning names the held
// revision. A member of the rebuilt set in a terminal phase is removed
// before the record is read, the restart trigger leaves the stopped row alone, and a roll
// target held as well is not followed, so the row's own revision is the
// one named.
func TestRestart_HeldRevisionBeforeTheFirstCreate_BuildsNoSet(t *testing.T) {
	t.Run("single-pod Instance, no pod left, its revision held", func(t *testing.T) {
		c, input, plan, warnings := heldRepairFixture(t, false, fixtureRevisionName(), []workload.RetryBlock{heldRecord(fixtureRevisionName())})
		runRepairPass(t, c, *input, plan, false)
		assertStoppedOnTheHold(t, c, *warnings, "llama-70b-engine-0-default-0", "Error")
	})
	t.Run("leader+worker gang, no pod left, its revision held", func(t *testing.T) {
		c, input, plan, warnings := heldRepairFixture(t, true, fixtureRevisionName(), []workload.RetryBlock{heldRecord(fixtureRevisionName())})
		runRepairPass(t, c, *input, plan, false)
		assertStoppedOnTheHold(t, c, *warnings, "llama-70b-engine-0-leader-0", "Error")
	})
	t.Run("after the stop the restart trigger leaves the row alone: the Create pass owns it", func(t *testing.T) {
		c, input, plan, warnings := heldRepairFixture(t, false, fixtureRevisionName(), []workload.RetryBlock{heldRecord(fixtureRevisionName())})
		runRepairPass(t, c, *input, plan, false)
		assertStoppedOnTheHold(t, c, *warnings, "llama-70b-engine-0-default-0", "Error")
		next := refreshInput(c, *input)
		if needs, reason := ops.DetectRestartTriggerWithPods(next, plan, plan.Instances[0], nil); needs {
			t.Fatalf("the restart trigger selected the stopped row again (%q); a Failed row with no operation is the Create pass's", reason)
		}
	})
	t.Run("a rebuilt member in a terminal phase is removed first, and the next pass stops on the hold", func(t *testing.T) {
		leader := gangPod(minimalISVC("llama-70b", "prod", 1), 0, "leader", 0, 2, false, false)
		leader.UID = "rebuilt-leader"
		leader.Status.Phase = corev1.PodFailed
		c, input, plan, warnings := heldRepairFixture(t, true, fixtureRevisionName(), []workload.RetryBlock{heldRecord(fixtureRevisionName())}, leader)
		runRepairPass(t, c, *input, plan, false)
		if got := len(podsIn(t, c)); got != 0 {
			t.Fatalf("the first pass left %d pod(s), want the member in a terminal phase removed and nothing created", got)
		}
		if row := rowOf(c, "llama-70b"); row.Phase != v1beta1.OMENativeInstanceRestarting {
			t.Fatalf("the first pass wrote the row %q, want the repair still open while the name is freed", row.Phase)
		}
		*input = refreshInput(c, *input)
		runRepairPass(t, c, *input, plan, false)
		// The removed member never ran, so its pod-level record does not
		// replace the exit the repair answers, and the stop names that exit.
		assertStoppedOnTheHold(t, c, *warnings, "llama-70b-engine-0-leader-0", "Error")
	})
	t.Run("the row's revision held and the roll target held too: the rebuild cannot follow the target and stops", func(t *testing.T) {
		c, input, plan, warnings := heldRepairFixture(t, false, olderRevisionName, []workload.RetryBlock{heldRecord(olderRevisionName), heldRecord(fixtureRevisionName())})
		runRepairPass(t, c, *input, plan, true)
		assertStoppedOnTheHold(t, c, *warnings, "llama-70b-engine-0-default-0", "Error")
		if !strings.Contains((*warnings)[0], olderRevisionName) {
			t.Errorf("the warning names %q, want the row's own revision %s", (*warnings)[0], olderRevisionName)
		}
	})
}

// The same pass builds its set when the record admits it: a record in
// backoff stops nothing; a record held before the repair opened was read
// by the trigger, which admitted the rebuild; a rebuilt set with a live
// member is finished; a rebuild that follows an open roll target asks
// about the target, not the row's revision; and a roll target held on its
// own is not followed, so the set is built on the row's revision.
func TestRestart_HeldRevisionBeforeTheFirstCreate_StillBuilds(t *testing.T) {
	t.Run("a record in backoff stops nothing: the set is built at the row's revision", func(t *testing.T) {
		backoff := workload.RetryBlock{TargetRevision: fixtureRevisionName(), State: workload.RetryBlockBackoff, AttemptsStarted: 1, Reason: "Error", LastFailureAt: ptr.To(metav1.NewTime(heldRepairOpenedAt.Add(time.Minute)))}
		c, input, plan, warnings := heldRepairFixture(t, false, fixtureRevisionName(), []workload.RetryBlock{backoff})
		runRepairPass(t, c, *input, plan, false)
		assertBuilt(t, c, *warnings, 1, testRevisionHash)
	})
	t.Run("a record held before the repair opened was read by the trigger: the set is built", func(t *testing.T) {
		c, input, plan, warnings := heldRepairFixture(t, true, fixtureRevisionName(), []workload.RetryBlock{heldRecordBeforeTheRepair(fixtureRevisionName())})
		runRepairPass(t, c, *input, plan, false)
		assertBuilt(t, c, *warnings, 2, testRevisionHash)
	})
	t.Run("a rebuilt set with a live member is finished: the missing member is created under the hold", func(t *testing.T) {
		leader := gangPod(minimalISVC("llama-70b", "prod", 1), 0, "leader", 0, 2, false, false)
		leader.UID = "rebuilt-leader"
		leader.Status.Phase = corev1.PodRunning
		c, input, plan, warnings := heldRepairFixture(t, true, fixtureRevisionName(), []workload.RetryBlock{heldRecord(fixtureRevisionName())}, leader)
		runRepairPass(t, c, *input, plan, false)
		assertBuilt(t, c, *warnings, 2, testRevisionHash)
	})
	t.Run("the row's revision held while the rebuild follows an open roll target: built at the target", func(t *testing.T) {
		c, input, plan, warnings := heldRepairFixture(t, false, olderRevisionName, []workload.RetryBlock{heldRecord(olderRevisionName)})
		runRepairPass(t, c, *input, plan, true)
		assertBuilt(t, c, *warnings, 1, testRevisionHash)
	})
	t.Run("only the roll target held: the rebuild keeps the row's revision and builds on it", func(t *testing.T) {
		c, input, plan, warnings := heldRepairFixture(t, false, olderRevisionName, []workload.RetryBlock{heldRecord(fixtureRevisionName())})
		runRepairPass(t, c, *input, plan, true)
		assertBuilt(t, c, *warnings, 1, query.RevisionHashFromControllerRevisionName(olderRevisionName))
	})
}

// refreshInput re-reads the rows the previous pass wrote, as the next
// reconcile would.
func refreshInput(c client.Client, input workload.ReconcileInput) workload.ReconcileInput {
	fresh := buildTestInput(minimalISVC("llama-70b", "prod", 1), c, workload.ComponentEngine)
	fresh.ObservedState.RetryBlocks = input.ObservedState.RetryBlocks
	fresh.WarnInstanceFailed = input.WarnInstanceFailed
	return fresh
}

// assertStoppedOnTheHold holds the pass to the stop: no pod, the row
// Failed with no operation and the failure the repair last recorded kept,
// one warning naming that failure and the held revision.
func assertStoppedOnTheHold(t *testing.T, c client.Client, warnings []string, failedPod, reason string) {
	t.Helper()
	if pods := podsIn(t, c); len(pods) != 0 {
		names := make([]string, 0, len(pods))
		for _, p := range pods {
			names = append(names, p.Name)
		}
		t.Fatalf("a held revision got %d pod(s) %v, want none", len(pods), names)
	}
	row := rowOf(c, "llama-70b")
	if row.Phase != v1beta1.OMENativeInstanceFailed || row.Operation != nil {
		t.Fatalf("row = phase %q operation %+v, want Failed with no operation", row.Phase, row.Operation)
	}
	if row.LastFailure == nil || row.LastFailure.PodName != failedPod || row.LastFailure.Reason != reason {
		t.Errorf("row.LastFailure = %+v, want the failure the repair recorded kept (pod %s, reason %s)", row.LastFailure, failedPod, reason)
	}
	if row.Incarnation != 2 {
		t.Errorf("the row's set number = %d, want the attempt's 2 kept", row.Incarnation)
	}
	if len(warnings) != 1 {
		t.Fatalf("InstanceFailed warnings = %v, want exactly one", warnings)
	}
	w := warnings[0]
	if !strings.HasPrefix(w, "instance=0 pod="+failedPod+" "+reason+": revision ") || !strings.Contains(w, " held after 1 failed attempts, so the repair builds no set on it") {
		t.Errorf("the warning reads %q, want the failed pod, its reason, the held revision and its spent attempts", w)
	}
}

// assertBuilt holds the pass to a rebuild: want pods at set number 2
// carrying the revision hash, the row still Restarting, no warning.
func assertBuilt(t *testing.T, c client.Client, warnings []string, want int, hash string) {
	t.Helper()
	pods := podsIn(t, c)
	if len(pods) != want {
		t.Fatalf("pods = %d, want %d", len(pods), want)
	}
	for _, p := range pods {
		if p.Labels[query.LabelInstanceIncarnation] != "2" {
			t.Errorf("pod %s carries set number %q, want the attempt's 2", p.Name, p.Labels[query.LabelInstanceIncarnation])
		}
		if p.Labels[query.LabelRevisionHash] != hash {
			t.Errorf("pod %s carries revision %q, want %q", p.Name, p.Labels[query.LabelRevisionHash], hash)
		}
	}
	if row := rowOf(c, "llama-70b"); row.Phase != v1beta1.OMENativeInstanceRestarting || row.Operation == nil {
		t.Errorf("row = phase %q operation %+v, want the repair still open", row.Phase, row.Operation)
	}
	if len(warnings) != 0 {
		t.Errorf("a rebuild the record admits warned %v, want nothing", warnings)
	}
}
