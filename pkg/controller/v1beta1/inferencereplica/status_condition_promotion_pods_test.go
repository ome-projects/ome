package inferencereplica

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// promotionPicture is one end of a pass as the promotion finds it: the
// stored status the pass's copy is decoded from, the Component's pods the
// pass hands the promotion, and what the promotion must leave behind.
type promotionPicture struct {
	name   string
	status func(*v1beta1.InferenceReplicaStatus)
	pods   func(ir *v1beta1.InferenceReplica, pods []*corev1.Pod) []*corev1.Pod
	// writes is how many status writes the promotion makes, current the
	// revision it leaves stored, reopened whether the CurrentRevisionWithdrawn
	// condition stands afterwards: the current revision cleared, the rollout
	// open again.
	writes   int
	current  string
	reopened bool
}

// labelRevision relabels pod with the revision named.
func labelRevision(pod *corev1.Pod, revision string) {
	pod.Labels[query.LabelRevisionHash] = query.RevisionFromName(revision).Hash()
}

// promotionExtraPod is one more Running pod of the Component under index,
// at ordinal, labelled with revision, with an address of its own.
func promotionExtraPod(ir *v1beta1.InferenceReplica, index, ordinal int32, revision string) *corev1.Pod {
	pod := podForIR(ir, index, string(v1beta1.RunnerNameDefault), ordinal, true, true)
	labelRevision(pod, revision)
	pod.Spec.NodeName = "node-0"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = fmt.Sprintf("10.9.%d.%d", index, ordinal)
	return pod
}

// currentCleared is the status after the promotion cleared the current
// revision: no current revision, the update revision still named and the
// CurrentRevisionWithdrawn condition standing, recorded as a pass records it.
func currentCleared(st *v1beta1.InferenceReplicaStatus) {
	st.CurrentRevision = ""
	recordCurrentRevisionWithdrawal(st, "", promotionTarget, publicationClock)
}

// rowFailed puts the row at index in Failed with no attempt open on it: a
// spent attempt, which leaves the Instance's pods to decide its revision.
func rowFailed(index int) func(*v1beta1.InferenceReplicaStatus) {
	return func(st *v1beta1.InferenceReplicaStatus) {
		st.InstanceStatuses[index].Phase = v1beta1.OMENativeInstanceFailed
	}
}

// rowRepairing puts the row at index under a repair in flight: Restarting
// with its Restart operation at the drain step, on the revision it runs. A
// row that is not Ready keeps the rows from deciding the roll on their own.
func rowRepairing(index int) func(*v1beta1.InferenceReplicaStatus) {
	return func(st *v1beta1.InferenceReplicaStatus) {
		row := &st.InstanceStatuses[index]
		row.Phase = v1beta1.OMENativeInstanceRestarting
		row.Operation = &v1beta1.InstanceOperation{
			ID: "restart-1", Type: v1beta1.InstanceOperationRestart, Step: workloadtypes.RestartStepDrain,
			StartedAt: metav1.NewTime(publicationClock), LastProgressAt: metav1.NewTime(publicationClock),
		}
	}
}

// promotionTarget and promotionOther are the spec target of the pictures
// and the other revision a pod may carry.
const promotionTarget, promotionOther = irstatustest.Revision, irstatustest.OtherRevision

// promotionPictures are the pictures of TestPromotionDecidesTheCurrentRevisionOverThePodsItIsHanded.
func promotionPictures() []promotionPicture {
	same := func(*v1beta1.InferenceReplicaStatus) {}
	asTheyAre := func(_ *v1beta1.InferenceReplica, pods []*corev1.Pod) []*corev1.Pod { return pods }
	podOnOther := func(index int) func(*v1beta1.InferenceReplica, []*corev1.Pod) []*corev1.Pod {
		return func(_ *v1beta1.InferenceReplica, pods []*corev1.Pod) []*corev1.Pod {
			labelRevision(pods[index], promotionOther)
			return pods
		}
	}
	failedThenCleared := func(st *v1beta1.InferenceReplicaStatus) {
		rowFailed(3)(st)
		currentCleared(st)
	}
	return []promotionPicture{
		{"every Instance Ready on the target with every pod labelled with it: nothing is written",
			same, asTheyAre, 0, promotionTarget, false},
		{"a pod labelled with another revision under a Ready Instance changes nothing: complete rows decide alone",
			same, podOnOther(2), 0, promotionTarget, false},
		{"an Instance under repair whose live pod carries another revision clears the current revision",
			rowRepairing(3), podOnOther(3), 1, "", true},
		{"an Instance under repair whose live pod carries the target keeps the current revision",
			rowRepairing(3), asTheyAre, 0, promotionTarget, false},
		{"a pod of another revision that has finished decides nothing",
			rowRepairing(3),
			func(_ *v1beta1.InferenceReplica, pods []*corev1.Pod) []*corev1.Pod {
				labelRevision(pods[3], promotionOther)
				pods[3].Status.Phase = corev1.PodSucceeded
				return pods
			}, 0, promotionTarget, false},
		{"a pod without a revision label leaves the row's recorded revision to decide",
			rowRepairing(3),
			func(_ *v1beta1.InferenceReplica, pods []*corev1.Pod) []*corev1.Pod {
				delete(pods[3].Labels, query.LabelRevisionHash)
				return pods
			}, 0, promotionTarget, false},
		{"one pod of an Instance on another revision is enough: the first of two",
			rowRepairing(3),
			func(ir *v1beta1.InferenceReplica, pods []*corev1.Pod) []*corev1.Pod {
				labelRevision(pods[3], promotionOther)
				return append(pods, promotionExtraPod(ir, 3, 1, promotionTarget))
			}, 1, "", true},
		{"one pod of an Instance on another revision is enough: the second of two",
			rowRepairing(3),
			func(ir *v1beta1.InferenceReplica, pods []*corev1.Pod) []*corev1.Pod {
				return append(pods, promotionExtraPod(ir, 3, 1, promotionOther))
			}, 1, "", true},
		{"a live pod of another revision under an index with no row clears the current revision",
			rowRepairing(3),
			func(ir *v1beta1.InferenceReplica, pods []*corev1.Pod) []*corev1.Pod {
				return append(pods, promotionExtraPod(ir, 9, 0, promotionOther))
			}, 1, "", true},
		{"a cleared current revision comes back once every Instance is Ready on it, whatever its pods carry",
			currentCleared, podOnOther(2), 1, promotionTarget, false},
		{"the restore waits while a Failed Instance's live pod still carries another revision",
			failedThenCleared, podOnOther(3), 0, "", true},
		{"the restore lands once that Instance's pod carries the cleared revision",
			failedThenCleared, asTheyAre, 1, promotionTarget, false},
	}
}

// checkPromotionPicture stores the picture's replica and builds its pods,
// runs the pass's promotion over the pods bucketed by Instance and holds
// what it leaves behind: the write count, the stored revision pair, the
// CurrentRevisionWithdrawn condition and the mirror onto the pass's copy.
func checkPromotionPicture(t *testing.T, pic promotionPicture) {
	t.Helper()
	shape := publicationShape{name: "readySince/8", instances: 8}
	ir := publicationReplica(shape)
	pic.status(&ir.Status)
	pods := pic.pods(ir, publicationPods(ir, shape.instances))
	f := buildPublicationFixture(t, pic.name, ir, pods)
	before := f.writes.Count()
	ended, _, err := f.pass.promoteCurrentRevision(context.Background(), nil, query.BucketPodsByInstanceIdx(pods))
	if ended || err != nil {
		t.Fatalf("promoteCurrentRevision: ended=%t err=%v", ended, err)
	}
	if writes := f.writes.Count() - before; writes != pic.writes {
		t.Errorf("the promotion wrote %d times, want %d", writes, pic.writes)
	}
	stored := f.storedStatus(t)
	if stored.CurrentRevision != pic.current || stored.UpdateRevision != promotionTarget {
		t.Errorf("stored revision pair current=%q update=%q, want current=%q update=%q", stored.CurrentRevision, stored.UpdateRevision, pic.current, promotionTarget)
	}
	if reopened := apimeta.IsStatusConditionTrue(stored.Conditions, InferenceReplicaConditionCurrentRevisionWithdrawn); reopened != pic.reopened {
		t.Errorf("the CurrentRevisionWithdrawn condition stands: %t, want %t", reopened, pic.reopened)
	}
	if f.pass.ir.Status.CurrentRevision != pic.current {
		t.Errorf("the pass's copy names current=%q after the promotion, want %q: a committed write is mirrored onto the copy", f.pass.ir.Status.CurrentRevision, pic.current)
	}
}

// TestPromotionDecidesTheCurrentRevisionOverThePodsItIsHanded holds what the
// end of a pass writes as the current revision, from the rows and the pods
// the pass hands it. Eight single-pod Instances, the stored current and
// update revision both the spec target. Rows that read complete decide on
// their own: every Instance Ready on the target promotes or keeps the
// target whatever label a pod carries. Where the rows do not decide, the
// pods do: a live pod carrying another revision under an Instance whose
// repair is in flight, or under an index with no row, clears the current
// revision; a finished pod and a pod without a revision label decide
// nothing; one pod of an Instance on another revision is enough; a
// cleared current revision waits for a Failed Instance's pod to carry it
// again and comes back once it does.
func TestPromotionDecidesTheCurrentRevisionOverThePodsItIsHanded(t *testing.T) {
	for _, pic := range promotionPictures() {
		t.Run(pic.name, func(t *testing.T) { checkPromotionPicture(t, pic) })
	}
}
