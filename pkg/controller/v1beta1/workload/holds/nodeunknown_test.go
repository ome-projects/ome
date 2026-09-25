package holds

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// quietPod is a pod whose kubelet has stopped reporting: it still holds
// its stable name, and nothing but proven node death frees it.
func quietPod(name, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec:       corev1.PodSpec{NodeName: node},
		Status:     corev1.PodStatus{Phase: corev1.PodUnknown},
	}
}

// singlePodPlan is the plan entry for a one-Runner Instance.
func singlePodPlan(idx int32) *types.InstancePlan {
	return &types.InstancePlan{Index: idx, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}}
}

// applyPlanned runs the hold pass with the row's planned Instance
// attached, which is what says which pod names the rebuild needs.
func applyPlanned(t *testing.T, store *rowStore, pods map[int32][]*corev1.Pod) {
	t.Helper()
	applyPlannedUnder(t, store, pods, types.Deps{}, nil)
}

// applyPlannedUnder is applyPlanned with a force-delete policy in force
// and a reader the node-death reading can consult.
func applyPlannedUnder(t *testing.T, store *rowStore, pods map[int32][]*corev1.Pod, deps types.Deps, policy *types.ForceDeletePolicy) {
	t.Helper()
	in := store.input()
	in.ForceDelete = policy
	rows := rowsFor(in, 1)
	for i := range rows {
		rows[i].Instance = singlePodPlan(rows[i].Status.Index)
	}
	if _, err := Run(context.Background(), PassInput{
		Deps:  deps,
		Input: in,
		Plan:  types.ComponentPlan{Component: types.ComponentEngine},
		Rows:  rows,
		Pods: func(context.Context) (map[int32][]*corev1.Pod, error) {
			return pods, nil
		},
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestNodeUnknown_HoldsATargetNameASilentNodeKeeps: a pod of the rebuild
// whose phase went Unknown keeps its stable name — a returning kubelet
// may still be running it — so the row reports the wait instead of
// spending its deadline in silence, and LastFailure names the pod and
// its node. The token is released as soon as no target name is held in
// that phase, whether the sweep freed it or the kubelet came back.
func TestNodeUnknown_HoldsATargetNameASilentNodeKeeps(t *testing.T) {
	const target = "svc-a-engine-0-default-0"

	t.Run("records the wait on the held name", func(t *testing.T) {
		store, _ := newStore(creatingRow(""))
		applyPlanned(t, store, map[int32][]*corev1.Pod{0: {quietPod(target, "node-a")}})
		if got := store.waiting(0); got != types.WaitingReasonNodeUnknown {
			t.Errorf("waiting = %q, want %q", got, types.WaitingReasonNodeUnknown)
		}
		lf := store.lastFailure(0)
		if lf == nil || lf.PodName != target || !strings.Contains(lf.Message, "node-a") {
			t.Errorf("lastFailure = %+v, want the pod and its node named", lf)
		}
	})

	t.Run("a name the sweep frees this pass is not a wait", func(t *testing.T) {
		// No Node object behind the pod: the sweep's node-death reading is
		// NodeGone, so the create pass force-deletes the pod this pass and
		// the hold has nothing to report. Recording it anyway would park
		// the deadline for one pass and restart it on the next.
		scheme := runtime.NewScheme()
		if err := corev1.AddToScheme(scheme); err != nil {
			t.Fatalf("scheme: %v", err)
		}
		policy := &types.ForceDeletePolicy{OverdueSlack: time.Minute, NodeUnreachableThreshold: 5 * time.Minute}
		store, _ := newStore(creatingRow(""))
		applyPlannedUnder(t, store, map[int32][]*corev1.Pod{0: {quietPod(target, "node-gone")}},
			types.Deps{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}, policy)
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want none: the sweep frees the name on this pass", got)
		}
		if store.writes != 0 {
			t.Errorf("writes = %d, want 0", store.writes)
		}
	})

	t.Run("a name the plan does not ask for holds nothing", func(t *testing.T) {
		store, _ := newStore(creatingRow(""))
		applyPlanned(t, store, map[int32][]*corev1.Pod{0: {quietPod("svc-a-engine-0-default-1", "node-a")}})
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want none: the rebuild needs no such name", got)
		}
	})

	t.Run("released once the name is free", func(t *testing.T) {
		store, _ := newStore(creatingRow(types.WaitingReasonNodeUnknown))
		applyPlanned(t, store, nil)
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want the token released", got)
		}
	})

	t.Run("a teardown row is left to DeleteBatch", func(t *testing.T) {
		row := creatingRow("")
		row.Phase = types.InstancePhaseDeleting
		row.Operation = operation("delete-0", types.InstanceOperationDelete, "Drain", "")
		store, _ := newStore(row)
		applyPlanned(t, store, map[int32][]*corev1.Pod{0: {quietPod(target, "node-a")}})
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want none on a row DeleteBatch owns", got)
		}
	})
}

// TestNodeUnknown_HoldsTheSurgeOrdinalOfASinglePodRow: the replacement of
// a single-pod surge is built at the ordinal opposite the active one, on
// the same row, so while the surge is in flight that name is one the
// rebuild needs. A silent node holding it is reported as the row's wait
// the way a held create target is; the same name on a row that is not
// surging is nobody's target, and a gang source, whose replacement is
// built under its own index, reads nothing at the other ordinal.
func TestNodeUnknown_HoldsTheSurgeOrdinalOfASinglePodRow(t *testing.T) {
	const surgeName = "svc-a-engine-0-default-1"

	t.Run("a surging row asks for the surge ordinal", func(t *testing.T) {
		store, _ := newStore(surgingRow(""))
		applyPlanned(t, store, map[int32][]*corev1.Pod{0: {quietPod(surgeName, "node-a")}})
		if got := store.waiting(0); got != types.WaitingReasonNodeUnknown {
			t.Errorf("waiting = %q, want %q: the surge ordinal is a target name", got, types.WaitingReasonNodeUnknown)
		}
		if lf := store.lastFailure(0); lf == nil || lf.PodName != surgeName {
			t.Errorf("lastFailure = %+v, want the replacement named", lf)
		}
	})

	t.Run("the surge ordinal is asked for through the drain step", func(t *testing.T) {
		row := surgingRow("")
		row.Operation.Step = types.UpdateStepSurgeDrain
		store, _ := newStore(row)
		applyPlanned(t, store, map[int32][]*corev1.Pod{0: {quietPod(surgeName, "node-a")}})
		if got := store.waiting(0); got != types.WaitingReasonNodeUnknown {
			t.Errorf("waiting = %q, want %q", got, types.WaitingReasonNodeUnknown)
		}
	})

	t.Run("a row that is not surging asks for the active ordinal only", func(t *testing.T) {
		store, _ := newStore(creatingRow(""))
		applyPlanned(t, store, map[int32][]*corev1.Pod{0: {quietPod(surgeName, "node-a")}})
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want none: no surge is building at that ordinal", got)
		}
	})

	t.Run("a gang source reads nothing at the other ordinal", func(t *testing.T) {
		row := surgingRow("")
		surgeIdx := int32(2)
		row.Operation.SurgeIndex = &surgeIdx
		store, _ := newStore(row)
		applyPlanned(t, store, map[int32][]*corev1.Pod{0: {quietPod(surgeName, "node-a")}})
		if got := store.waiting(0); got == types.WaitingReasonNodeUnknown {
			t.Errorf("waiting = %q, want not NodeUnknown: a gang replacement is built under its own index", got)
		}
	})
}
