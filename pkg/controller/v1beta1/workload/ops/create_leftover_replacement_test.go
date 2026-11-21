package ops

import (
	"context"
	"reflect"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	fixture "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/testfixture"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// A Create attempt's finish is decided on the pods at the names the set
// asks for. A pod of the Instance at another name is what a surge left
// behind: it neither holds the finish back nor gets the serving gate.

// leftoverRevision is a third revision: the corrected push after the one
// that failed.
const leftoverRevision = "llama-70b-engine-8c9d0e1f"

// rebuiltSourceRow is Instance 1 as the Create pass meets it after its
// surge toward revision B failed twice with the source pod gone: the row
// still records the target B, and a Create attempt rebuilds the set on
// revision A, the revision the Instance served.
func rebuiltSourceRow() workload.InstanceStatus {
	row := creatingRow(1, finishRevisionA)
	row.RunningRevision = finishRevisionA
	row.TargetRevision = finishRevisionB
	row.LastFailure = &workload.InstanceTermination{Reason: "ImagePullBackOff", PodName: podSetName(1, workload.RunnerDefault, 1),
		Time: metav1.NewTime(createFinishNow.Add(-time.Minute))}
	return row
}

// rebuiltPod is the pod the attempt built at ordinal 0: Running, its
// containers Ready, no serving gate written yet, so PodReady is False.
func rebuiltPod() *corev1.Pod {
	pod := podOf(1, fixture.RevisionHashA, gateLost, fixture.GateAbsent)
	pod.CreationTimestamp = metav1.NewTime(createFinishNow.Add(-2 * time.Minute))
	return pod
}

// leftoverReplacement is the pod the last surge attempt left at ordinal 1
// on revision B: Pending, its image cannot be pulled.
func leftoverReplacement() *corev1.Pod {
	pod := podAt(1, workload.RunnerDefault, 1, fixture.ContainerWaiting("ImagePullBackOff"), fixture.GateAbsent)
	pod.Labels[query.LabelRevisionHash] = fixture.RevisionHashB
	pod.Status.Phase = corev1.PodPending
	return pod
}

// leftoverOutcome is what one Create pass did with the picture.
type leftoverOutcome struct {
	rows   map[int32]workload.InstanceStatus
	gates  []string
	events []string
	// replacementRemoved reports whether the pass deleted the pod the surge
	// left behind; replacementServing whether it wrote its serving gate.
	replacementRemoved bool
	replacementServing bool
}

// runLeftoverPass runs one Create pass over Instance 0, Ready on revision
// A, and Instance 1 as the rows say, with the pods bucketed under Instance
// 1 and held by a fake apiserver for the gate writes; held adds the retry
// record that holds revision B after its failed attempts.
func runLeftoverPass(t *testing.T, rows []workload.InstanceStatus, instanceOnePods []*corev1.Pod, target string, held bool) leftoverOutcome {
	t.Helper()
	ctx := context.Background()
	single := []workload.RunnerPlan{{Name: workload.RunnerDefault, Size: 1}}
	plan := workload.ComponentPlan{Component: workload.ComponentEngine, Replicas: 2, InstanceReadyTimeout: 30 * time.Minute,
		UpdateStrategy: workload.UpdateStrategy{Type: workload.UpdateStrategySurgeThenDrain},
		Instances:      []workload.InstancePlan{{Index: 0, Incarnation: 1, Runners: single}, {Index: 1, Incarnation: 1, Runners: single}}}
	pods := map[int32][]*corev1.Pod{0: {podOf(0, fixture.RevisionHashA)}, 1: instanceOnePods}
	writer := &finishWriter{rows: rowsByIndex(rows), appliesUnchanged: true}
	input := finishPassInput(rows, writer)
	if held {
		input.ObservedState.RetryBlocks = []workload.RetryBlock{{TargetRevision: finishRevisionB, State: workload.RetryBlockHeld, AttemptsStarted: 2}}
	}
	var objects []client.Object
	for _, list := range pods {
		for _, pod := range list {
			objects = append(objects, pod.DeepCopy())
		}
	}
	apiserver := fakeclient.New(t, fakeclient.WithObjects(objects...), fakeclient.WithStatusSubresource(&corev1.Pod{}))
	recorder := record.NewFakeRecorder(16)
	deps := workload.Deps{Client: apiserver, Recorder: recorder, Expectations: workload.NewExpectations()}
	revision := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: target, Namespace: createNamesNamespace}}
	if _, err := createFilteredBatched(ctx, deps, input, plan, revision, keepEveryIndex, pods); err != nil {
		t.Fatalf("the Create pass failed: %v", err)
	}
	out := leftoverOutcome{rows: writer.rows, gates: gatesWrittenAmongTheRemaining(t, ctx, apiserver, pods), events: eventReasons(recorder)}
	replacement := &corev1.Pod{}
	switch err := apiserver.Get(ctx, client.ObjectKey{Namespace: createNamesNamespace, Name: podSetName(1, workload.RunnerDefault, 1)}, replacement); {
	case apierrors.IsNotFound(err):
		out.replacementRemoved = true
	case err != nil:
		t.Fatalf("read the replacement: %v", err)
	default:
		out.replacementServing = podreadiness.IsServing(replacement)
	}
	return out
}

// TestCreate_ARebuiltSetBesideAFailedSurgeReplacement:
// two Instances of one side under SurgeThenDrain; the pushed revision B
// cannot be pulled. Instance 1's surge attempt is open when its node stops
// reporting, and the force-delete policy removes its revision A pod while
// the revision B replacement waits in ImagePullBackOff. The attempt fails
// twice, revision B is held, and the Create pass rebuilds Instance 1 on
// revision A at its own ordinal, beside the replacement the last attempt
// left at the other ordinal. The rebuilt pod runs with its containers
// Ready. The pass that sees that writes its serving gate, the pass that
// sees PodReady promotes the row Ready on revision A, and the leftover gets
// no gate, whether the roll is still held on revision B or a corrected
// revision C has been pushed. An Instance whose own set is healthy does not
// wait on a pod the set never asked for.
func TestCreate_ARebuiltSetBesideAFailedSurgeReplacement(t *testing.T) {
	rebuilt := podSetName(1, workload.RunnerDefault, 0)
	for _, tc := range []struct {
		name   string
		target string
		held   bool
	}{
		{name: "while the roll is held on the revision that failed", target: finishRevisionB, held: true},
		{name: "after the corrected push", target: leftoverRevision, held: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := []workload.InstanceStatus{readyRow(0, finishRevisionA), rebuiltSourceRow()}
			first := runLeftoverPass(t, rows, []*corev1.Pod{rebuiltPod(), leftoverReplacement()}, tc.target, tc.held)
			if !reflect.DeepEqual(first.gates, []string{rebuilt}) {
				t.Fatalf("the pass that saw the rebuilt pod's containers Ready wrote the serving gate on %v, want %v; the row reads %s with its attempt %v",
					first.gates, []string{rebuilt}, first.rows[1].Phase, first.rows[1].Operation != nil)
			}
			if row := first.rows[1]; row.Phase != workload.InstancePhaseCreating || row.Operation == nil {
				t.Fatalf("the gate pass moved the row before PodReady folded the gate in: %+v", row)
			}
			if !first.replacementRemoved {
				t.Fatalf("the replacement the failed attempt left at the other ordinal still stands after the gate pass")
			}

			// The kubelet folds the gate into PodReady; the next pass promotes.
			served := rebuiltPod()
			fixture.SetCondition(served, podreadiness.ConditionType, corev1.ConditionTrue, createFinishNow)
			fixture.SetCondition(served, corev1.PodReady, corev1.ConditionTrue, createFinishNow)
			second := runLeftoverPass(t, []workload.InstanceStatus{first.rows[0], first.rows[1]}, []*corev1.Pod{served}, tc.target, tc.held)
			row := second.rows[1]
			if row.Phase != workload.InstancePhaseReady || row.RunningRevision != finishRevisionA || row.Operation != nil {
				t.Fatalf("the pass that saw the rebuilt pod PodReady left the row %s on %q with its attempt %v, want Ready on %q with no attempt",
					row.Phase, row.RunningRevision, row.Operation != nil, finishRevisionA)
			}
			if !reflect.DeepEqual(second.events, []string{"InstanceReady"}) {
				t.Fatalf("the promote recorded %v, want [InstanceReady]", second.events)
			}
			if first.replacementServing {
				t.Fatalf("the replacement the failed attempt left behind was put into service")
			}
		})
	}
}

// gatesWrittenAmongTheRemaining names the pods whose serving gate the pass
// wrote True, leaving out a pod the pass deleted.
func gatesWrittenAmongTheRemaining(t *testing.T, ctx context.Context, c client.Client, pods map[int32][]*corev1.Pod) []string {
	t.Helper()
	remaining := map[int32][]*corev1.Pod{}
	for index, list := range pods {
		for _, pod := range list {
			if err := c.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{}); apierrors.IsNotFound(err) {
				continue
			}
			remaining[index] = append(remaining[index], pod)
		}
	}
	return gatesWritten(t, ctx, c, remaining)
}
