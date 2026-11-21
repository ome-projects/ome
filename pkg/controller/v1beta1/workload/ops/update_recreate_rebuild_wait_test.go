package ops

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestRecreateUpdate_TheRebuildWaitsForTheCountOfTheOldPodsEnd: a recreate
// roll deletes an Instance's old pod and creates a new pod under the same
// name. The pod watch counts each create and delete that OME sent when it
// sees the pod change. The roll waits for the count of every create and
// delete of the Instance before it creates the new pod. A pass that finds
// the old pod gone while its delete is uncounted creates nothing. The first
// pass after the count creates the pod.
func TestRecreateUpdate_TheRebuildWaitsForTheCountOfTheOldPodsEnd(t *testing.T) {
	f := recreateDrainInFlightFixture(t)
	exp, _ := outstandingDelete(f)
	pods := func() int {
		t.Helper()
		list := &corev1.PodList{}
		if err := f.c.List(context.Background(), list, client.InNamespace(f.isvc.Namespace)); err != nil {
			t.Fatalf("list pods: %v", err)
		}
		return len(list.Items)
	}

	done, err := runWithExpectations(f, exp, nil)
	if err != nil {
		t.Fatalf("UpdateWithPods with the delete uncounted: %v", err)
	}
	if done {
		t.Errorf("done: got true want false (the rebuilt pod does not exist yet)")
	}
	if got := pods(); got != 0 {
		t.Fatalf("pods while the old pod's delete is uncounted: got %d want 0", got)
	}

	exp.ObservedDelete(f.isvc.Namespace, f.isvc.Name, workload.ComponentEngine, 0)
	if _, err := runWithExpectations(f, exp, nil); err != nil {
		t.Fatalf("UpdateWithPods after the count: %v", err)
	}
	if got := pods(); got != 1 {
		t.Fatalf("pods after the count: got %d want 1", got)
	}
}
