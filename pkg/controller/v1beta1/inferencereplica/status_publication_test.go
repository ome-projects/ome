package inferencereplica

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
)

// probeShape is a replica of four single-pod Instances, small enough to
// read every count by hand.
var probeShape = statusPublicationShape{name: "probe", instances: 4, pods: 1}

// setServing turns the serving condition of pod on or off.
func setServing(pod *corev1.Pod, serving bool) {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type != query.ServingConditionType {
			continue
		}
		pod.Status.Conditions[i].Status = corev1.ConditionFalse
		if serving {
			pod.Status.Conditions[i].Status = corev1.ConditionTrue
		}
	}
}

// stored reads the replica from the store and decodes it, as a live read
// does.
func (f *statusPublicationFixture) stored(tb testing.TB) *v1beta1.InferenceReplica {
	tb.Helper()
	live := &v1beta1.InferenceReplica{}
	if _, err := irstatus.GetDecoded(context.Background(), irstatus.NewReader(f.apiserver, f.reconciler.InstanceStatusDecoder), f.key, live); err != nil {
		tb.Fatalf("read the stored replica: %v", err)
	}
	return live
}

// storeStatus writes a change to the stored replica's status through the
// controller's own writer, as a pass this pass's copy has not seen did.
func (f *statusPublicationFixture) storeStatus(tb testing.TB, change func(*v1beta1.InferenceReplicaStatus)) {
	tb.Helper()
	live := &v1beta1.InferenceReplica{}
	source, err := irstatus.GetDecoded(context.Background(), irstatus.NewReader(f.apiserver, f.reconciler.InstanceStatusDecoder), f.key, live)
	if err != nil {
		tb.Fatalf("read the stored replica: %v", err)
	}
	change(&live.Status)
	if err := updateInferenceReplicaStatus(context.Background(), f.reconciler.statusWriter(), live, source); err != nil {
		tb.Fatalf("write the stored replica: %v", err)
	}
}

// expectCounts fails unless the last status write made the live reads and
// the writes given.
func expectCounts(tb testing.TB, step string, gotReads, wantReads, gotWrites, wantWrites int) {
	tb.Helper()
	if gotReads != wantReads || gotWrites != wantWrites {
		tb.Errorf("%s: live reads=%d writes=%d, want %d and %d", step, gotReads, gotWrites, wantReads, wantWrites)
	}
}

func TestAggregateAndWriteStatus_ReplicaAtRestIsDecidedOnTheCopy(t *testing.T) {
	f := newStatusPublicationFixture(t, probeShape)
	reads, writes := f.runCounting(t)
	expectCounts(t, "at rest", reads, 0, writes, 0)
	if f.passCopy.Status.ReadyReplicas != 4 || f.passCopy.Status.ServingReplicas != 4 {
		t.Errorf("the copy's counters after the publication: ready=%d serving=%d, want 4 and 4", f.passCopy.Status.ReadyReplicas, f.passCopy.Status.ServingReplicas)
	}
	for _, row := range f.passCopy.Status.InstanceStatuses {
		if row.ReadyPodCount != 1 || row.ServingPodCount != 1 {
			t.Errorf("Instance %d on the copy: readyPods=%d servingPods=%d, want 1 and 1: the publication is mirrored onto the copy for the rest of the pass", row.Index, row.ReadyPodCount, row.ServingPodCount)
		}
	}
}

func TestAggregateAndWriteStatus_TheCopysPodCountersAreComparedAsStored(t *testing.T) {
	f := newStatusPublicationFixture(t, probeShape)
	reads, writes := f.runCounting(t)
	expectCounts(t, "first publication", reads, 0, writes, 0)
	if f.passCopy.Status.InstanceStatuses[0].ReadyPodCount != 1 {
		t.Fatalf("the first publication left no pod counters on the copy")
	}
	reads, writes = f.runCounting(t)
	expectCounts(t, "second publication over the same copy", reads, 0, writes, 0)
}

func TestAggregateAndWriteStatus_ACopyThatCallsForAWriteIsWrittenFromTheLiveObject(t *testing.T) {
	f := newStatusPublicationFixture(t, probeShape)
	// The live object carries a record the copy has not seen: a retry
	// record for another revision, which the publication keeps as it is.
	f.storeStatus(t, func(status *v1beta1.InferenceReplicaStatus) {
		status.RetryBlocks = append(status.RetryBlocks, v1beta1.RetryBlock{TargetRevision: "another-revision", State: v1beta1.RetryBlockHeld, AttemptsStarted: 2, Reason: "kept"})
	})
	setServing(f.pods[2], false)

	reads, writes := f.runCounting(t)
	expectCounts(t, "a pod out of serving", reads, 1, writes, 1)
	stored := f.stored(t)
	if stored.Status.ServingReplicas != 3 {
		t.Errorf("stored servingReplicas=%d, want 3", stored.Status.ServingReplicas)
	}
	if len(stored.Status.RetryBlocks) != 1 || stored.Status.RetryBlocks[0].TargetRevision != "another-revision" {
		t.Errorf("stored retry records=%+v, want the live object's record kept: the write is decided on the live object, not on the copy", stored.Status.RetryBlocks)
	}
	if f.passCopy.Status.ServingReplicas != 3 {
		t.Errorf("the copy's servingReplicas=%d after the write, want 3 mirrored", f.passCopy.Status.ServingReplicas)
	}
}

func TestAggregateAndWriteStatus_ACopyBehindItsOwnWriteWaitsForThePassThatShowsIt(t *testing.T) {
	f := newStatusPublicationFixture(t, probeShape)
	// The pass before this one saw a pod out of serving and wrote
	// servingReplicas 3. The cache has not shown that write yet.
	before := f.passCopy.DeepCopy()
	setServing(f.pods[2], false)
	reads, writes := f.runCounting(t)
	expectCounts(t, "the previous pass", reads, 1, writes, 1)
	setServing(f.pods[2], true)
	f.passCopy = before

	// This pass's copy predates that write and its pods call for the
	// counters the copy already carries, so nothing is read or written: the
	// live object stays one write behind until the write's own event brings
	// the next pass.
	reads, writes = f.runCounting(t)
	expectCounts(t, "the pass over the copy behind the write", reads, 0, writes, 0)
	if stored := f.stored(t); stored.Status.ServingReplicas != 3 {
		t.Fatalf("stored servingReplicas=%d after the pass over the stale copy, want 3 still", stored.Status.ServingReplicas)
	}

	// The next pass decodes a copy that shows the write; its pods call for
	// servingReplicas 4, so it reads the live object and writes it.
	f.decodePassCopy(t)
	reads, writes = f.runCounting(t)
	expectCounts(t, "the pass that shows the write", reads, 1, writes, 1)
	if stored := f.stored(t); stored.Status.ServingReplicas != 4 {
		t.Errorf("stored servingReplicas=%d after the repairing pass, want 4", stored.Status.ServingReplicas)
	}
}

func TestDecidePublication_APublicationThatCannotBeBuiltCountsAsAFailedWrite(t *testing.T) {
	f := newStatusPublicationFixture(t, probeShape)
	errorsBefore := irStatusUpdateMetric(t, obsmetrics.ResultError)
	readsBefore, writesBefore := f.replicaReads.Count(), f.writes.Count()
	// A publication is built over the pods the cache listed; a reading of
	// another provenance cannot be published and fails the write.
	publication := statusPublication{
		plan:             f.plan,
		target:           f.target,
		desiredByIdx:     workloadstatus.DesiredPodCountByInstance(f.plan),
		pods:             workload.NewAPIReaderSelectorPodObservation(f.pods, query.BucketPodsByInstanceIdx(f.pods)),
		availableByPod:   map[string]struct{}{},
		templateHash:     templateHashOf(f.passCopy),
		instancesAtStart: f.instancesAtStart,
	}
	if _, _, err := f.reconciler.decidePublication(context.Background(), f.passCopy, publication); err == nil {
		t.Fatalf("decidePublication over a reading that cannot be published returned no error")
	}
	if got := irStatusUpdateMetric(t, obsmetrics.ResultError); got != errorsBefore+1 {
		t.Errorf("status-update errors counted: %v, want %v: a publication that cannot be built is a failed status write", got, errorsBefore+1)
	}
	expectCounts(t, "the failed publication", f.replicaReads.Count()-readsBefore, 0, f.writes.Count()-writesBefore, 0)
}

func TestAggregateAndWriteStatus_AReplacedOwnerIsNotWritten(t *testing.T) {
	f := newStatusPublicationFixture(t, probeShape)
	setServing(f.pods[2], false)
	// The replica was deleted and created again under the same name.
	f.storeStatus(t, func(*v1beta1.InferenceReplicaStatus) {})
	replaced := f.stored(t)
	if err := f.apiserver.Delete(context.Background(), replaced); err != nil {
		t.Fatalf("delete the replica: %v", err)
	}
	replaced.ResourceVersion = ""
	replaced.UID = types.UID("another-uid")
	if err := f.apiserver.Create(context.Background(), replaced); err != nil {
		t.Fatalf("create the replica again: %v", err)
	}

	reads, writes := f.runCounting(t)
	expectCounts(t, "the owner replaced", reads, 1, writes, 0)
}
