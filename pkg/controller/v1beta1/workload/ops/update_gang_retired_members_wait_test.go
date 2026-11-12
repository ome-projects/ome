package ops

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestGangSurgeRetirement_RowsNameTheWaitWhileRetiredMembersTerminate:
// a gang surge whose replacement failed is retired. The delete pass asks
// for the members' deletion under the grace, and the members stay
// Terminating for a while. From that pass until the last member is gone
// the source's kept surge and the replacement's cleanup step both carry
// the RetiredReplacementTerminating waiting token, so a reader of either
// row sees what the pair waits for. A pass that finds the same members
// still terminating writes nothing more, and the write that ends the pair
// takes the token away with the operation.
func TestGangSurgeRetirement_RowsNameTheWaitWhileRetiredMembersTerminate(t *testing.T) {
	input, store, c, surgeIndex, _ := gangCleanupRejectionFixture(t)
	ctx := context.Background()
	const keep = "test.ome.io/keep-terminating"

	// The failed shape: the escalation failed the source and kept its surge.
	source := store.statuses[0]
	source.Phase = workload.InstancePhaseFailed
	source.LastFailure = &workload.InstanceTermination{Reason: "ContainersNotReady", Message: "the replacement gang never became ready"}
	store.statuses[0] = source

	// The members stay Terminating until the test lets them go.
	members, err := query.LiveListPodsForInstance(ctx, c, input.Key.Namespace, input.Key.OwnerName, workload.ComponentEngine, surgeIndex)
	if err != nil || len(members) != 2 {
		t.Fatalf("replacement members: %d, err=%v; want 2", len(members), err)
	}
	for _, pod := range members {
		pod.Finalizers = append(pod.Finalizers, keep)
		if err := c.Update(ctx, pod); err != nil {
			t.Fatalf("keep member %s: %v", pod.Name, err)
		}
	}

	blocks := map[string]workload.RetryBlock{}
	pass := gangCleanupRejectionPass(t, &input, store, workload.Deps{Client: c}, blocks, surgeIndex)

	// The delete pass: both members are asked to go, both rows name the wait.
	if done, err := pass(); err != nil || done {
		t.Fatalf("delete pass: done=%v err=%v, want done=false err=nil", done, err)
	}
	for _, pod := range members {
		fresh := &corev1.Pod{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(pod), fresh); err != nil {
			t.Fatalf("re-read member %s: %v", pod.Name, err)
		}
		if fresh.DeletionTimestamp == nil {
			t.Fatalf("member %s is not terminating after the delete pass", pod.Name)
		}
	}
	for _, idx := range []int32{0, surgeIndex} {
		row := store.statuses[idx]
		if row.Operation == nil || row.Operation.Waiting != workload.WaitingReasonRetiredReplacementTerminating {
			t.Fatalf("row %d after the delete pass: operation %+v, want the %s waiting token", idx, row.Operation, workload.WaitingReasonRetiredReplacementTerminating)
		}
	}
	if store.statuses[0].Phase != workload.InstancePhaseFailed {
		t.Fatalf("source after the delete pass: phase %s, want Failed with its surge kept", store.statuses[0].Phase)
	}
	if store.statuses[0].LastFailure == nil || store.statuses[0].LastFailure.Reason != "ContainersNotReady" {
		t.Fatalf("source failure record after the delete pass: %+v, want the failure that retired the replacement kept", store.statuses[0].LastFailure)
	}

	// The same members still terminating: nothing is written again.
	writes := store.writes
	if done, err := pass(); err != nil || done {
		t.Fatalf("pass over terminating members: done=%v err=%v, want done=false err=nil", done, err)
	}
	if store.writes != writes {
		t.Fatalf("a pass over the same terminating members wrote the rows again (%d writes, was %d)", store.writes, writes)
	}

	// The members leave; the pair ends in one write and the token with it.
	for _, pod := range members {
		fresh := &corev1.Pod{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(pod), fresh); err != nil {
			t.Fatalf("re-read member %s: %v", pod.Name, err)
		}
		fresh.Finalizers = nil
		if err := c.Update(ctx, fresh); err != nil {
			t.Fatalf("let member %s go: %v", pod.Name, err)
		}
	}
	if left, err := query.LiveListPodsForInstance(ctx, c, input.Key.Namespace, input.Key.OwnerName, workload.ComponentEngine, surgeIndex); err != nil || len(left) != 0 {
		t.Fatalf("members after their finalizers are gone: %d, err=%v; want none", len(left), err)
	}
	if done, err := pass(); err != nil || done {
		t.Fatalf("end pass: done=%v err=%v, want done=false err=nil (the retirement always requeues)", done, err)
	}
	if kept, found := store.statuses[surgeIndex]; found {
		t.Fatalf("the replacement's row was kept after its members were gone: %+v", kept)
	}
	ended := store.statuses[0]
	if ended.Phase != workload.InstancePhaseReady || ended.Operation != nil {
		t.Fatalf("source after the pair ended: %+v, want Ready on its running revision with no operation", ended)
	}
}
