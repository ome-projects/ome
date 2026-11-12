package gang

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The PodGroup API can be removed from a cluster while the manager runs.
// The manager discovered the API at its start and the cache keeps
// answering, so only a read of the apiserver shows the API gone. These
// tests hold what the pass and the surge callback do when that read is the
// one before a write: nothing is written, the Component says gang
// scheduling is unavailable, and every gang keeps the key its pods carry.

// noPodGroupAPI is the apiserver's answer to a PodGroup list or write once
// the API is removed.
func noPodGroupAPI() error {
	return &apimeta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "scheduling.x-k8s.io", Kind: "PodGroup"}}
}

// newTwoReadersWithoutThePodGroupAPI is a Component whose PodGroup API was
// removed after its pods were created: the cache has shown the groups
// deleted and holds none, and the apiserver refuses every PodGroup list
// and write.
func newTwoReadersWithoutThePodGroupAPI(t *testing.T, pods []client.Object) *twoReaders {
	t.Helper()
	return newTwoReadersWith(t, pods, pods,
		[]fakeclient.Option{fakeclient.FailingWrites(fakeclient.EveryCall, noPodGroupAPI()).OnKind(&schedulingv1alpha1.PodGroup{})},
		[]fakeclient.Option{fakeclient.FailingVerb(fakeclient.List, fakeclient.EveryCall, noPodGroupAPI()).OnKind(&schedulingv1alpha1.PodGroupList{})})
}

// TestEnsurePodGroups_TheAPIRemovedIsFoundOnTheLiveReadAndTheGangKeepsItsPodsKey:
// the PodGroup API is removed under a gang whose pods are running. The
// cache shows the gang's name free, so the pass asks the apiserver before
// it writes, and the apiserver answers that there is no PodGroup API. The
// pass creates no group, writes GangSchedulingUnavailable=True, and the
// gang keeps the key its pods carry.
func TestEnsurePodGroups_TheAPIRemovedIsFoundOnTheLiveReadAndTheGangKeepsItsPodsKey(t *testing.T) {
	r := newTwoReadersWithoutThePodGroupAPI(t, []client.Object{leaderPod(), workerPod(cachedPodsKey)})
	input, plan, conditions := r.planOnKeyRecordingConditions(true, 1, cachedPodsKey)
	topology, err := EnsurePodGroupsWithState(context.Background(), r.deps, input, plan, r.inventory(t, true))
	if err != nil {
		t.Fatalf("EnsurePodGroupsWithState with the PodGroup API removed: %v", err)
	}
	if key := topology[0]; key != cachedPodsKey {
		t.Errorf("topology key %q, want the pods' %q", key, cachedPodsKey)
	}
	assertCondition(t, conditions, metav1.ConditionTrue, string(workloadtypes.ReasonPodGroupCRDNotInstalled))
	r.requireReads(t, 1, 1, 0)
	r.requireGroupReads(t, 1, 1)
}

// TestEnsurePodGroups_ATransientErrorOnTheLiveReadEndsThePassAndLeavesTheCondition:
// the apiserver answers the PodGroup list with a transient failure, not
// with a missing API. The pass ends with that error and writes nothing,
// and the condition keeps saying gang scheduling is available: only a
// missing API takes the pass onto the path without groups.
func TestEnsurePodGroups_ATransientErrorOnTheLiveReadEndsThePassAndLeavesTheCondition(t *testing.T) {
	pods := []client.Object{leaderPod(), workerPod(cachedPodsKey)}
	r := newTwoReadersWith(t, pods, pods, nil,
		[]fakeclient.Option{fakeclient.FailingVerb(fakeclient.List, fakeclient.EveryCall,
			apierrors.NewServiceUnavailable("the apiserver is busy")).OnKind(&schedulingv1alpha1.PodGroupList{})})
	input, plan, conditions := r.planOnKeyRecordingConditions(true, 1, cachedPodsKey)
	_, err := EnsurePodGroupsWithState(context.Background(), r.deps, input, plan, r.inventory(t, true))
	if err == nil || !apierrors.IsServiceUnavailable(err) {
		t.Fatalf("EnsurePodGroupsWithState under a transient list failure: error %v, want the failure", err)
	}
	assertCondition(t, conditions, metav1.ConditionFalse, string(workloadtypes.ReasonGangSchedulingAvailable))
	r.requireReads(t, 1, 0, 0)
	r.requireGroupReads(t, 1, 1)
}

// TestEnsureSurgePodGroup_TheAPIRemovedIsFoundOnTheLiveReadAndTheSurgeKeepsItsPodsKey:
// a surge Instance's group is written just before its pods, after the
// apiserver's read of the cache's inventory. With the PodGroup API removed
// that read answers no API: the callback writes no group, writes
// GangSchedulingUnavailable=True, and the surge keeps the key its pods
// carry.
func TestEnsureSurgePodGroup_TheAPIRemovedIsFoundOnTheLiveReadAndTheSurgeKeepsItsPodsKey(t *testing.T) {
	r := newTwoReadersWithoutThePodGroupAPI(t, []client.Object{leaderPod(), workerPod(cachedPodsKey)})
	input, plan, conditions := r.planOnKeyRecordingConditions(true, 1, cachedPodsKey)
	key, err := EnsureSurgePodGroupWithState(r.deps, r.inventory(t, true))(context.Background(), input, plan, plan.Instances[0])
	if err != nil {
		t.Fatalf("EnsureSurgePodGroupWithState with the PodGroup API removed: %v", err)
	}
	if key != cachedPodsKey {
		t.Errorf("surge topology key %q, want the pods' %q", key, cachedPodsKey)
	}
	assertCondition(t, conditions, metav1.ConditionTrue, string(workloadtypes.ReasonPodGroupCRDNotInstalled))
	r.requireReads(t, 0, 1, 0)
	r.requireGroupReads(t, 1, 1)
}
