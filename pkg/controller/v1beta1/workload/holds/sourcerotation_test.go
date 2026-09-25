package holds

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const (
	surgeTarget = "svc-a-engine-targethash"
	sourceHash  = "sourcehash"
)

// surgingRow is a single-pod Instance mid-surge: the source is still in
// rotation until the drain step, so anything out of it before then was
// taken out by something else.
func surgingRow(waiting string) types.InstanceStatus {
	row := types.InstanceStatus{
		Index:     0,
		Phase:     types.InstancePhaseUpdating,
		PodCount:  1,
		Operation: operation("update-0", types.InstanceOperationUpdate, types.UpdateStepSurge, waiting),
	}
	row.Operation.TargetRevision = surgeTarget
	return row
}

// sourcePod is a pod of the revision the surge is replacing. serving
// drives the lifecycle gate the controller writes; routable decides
// whether the pod is addressed by a per-revision routed Service at all.
func sourcePod(name string, serving, routable bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: map[string]string{}},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			},
		},
	}
	if routable {
		pod.Labels[query.LabelRevisionHash] = sourceHash
	}
	status := corev1.ConditionFalse
	if serving {
		status = corev1.ConditionTrue
	}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type: podreadiness.ConditionType, Status: status,
	})
	return pod
}

// applySurge runs the hold pass with a reader behind it, so the routed
// view of a source can be read from EndpointSlices.
func applySurge(t *testing.T, store *rowStore, pods []*corev1.Pod, slices ...*discoveryv1.EndpointSlice) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	builder := fake.NewClientBuilder().WithScheme(scheme)
	for _, slice := range slices {
		builder = builder.WithObjects(slice)
	}
	c := builder.Build()
	in := store.input()
	if _, err := Run(context.Background(), PassInput{
		Deps:  types.Deps{Client: c},
		Input: in,
		Plan:  types.ComponentPlan{Component: types.ComponentEngine},
		Rows:  rowsFor(in, 1),
		Pods: func(context.Context) (map[int32][]*corev1.Pod, error) {
			return map[int32][]*corev1.Pod{0: pods}, nil
		},
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestSourceRotation_ReportsASourceTakenOutFromUnderTheSurge: a surge
// keeps its source in rotation until the drain step, so a source out of
// rotation while the step is still Surge is the Instance serving
// nothing. Both views the rollout owns can say so on their own — the
// lifecycle serving gate and the pod's endpoint in the per-revision
// routed Service — and either one saying "out" is out.
func TestSourceRotation_ReportsASourceTakenOutFromUnderTheSurge(t *testing.T) {
	t.Run("the serving gate going false is a report", func(t *testing.T) {
		store, _ := newStore(surgingRow(""))
		applySurge(t, store, []*corev1.Pod{sourcePod("svc-a-engine-0-default-0", false, false)})
		if got := store.waiting(0); got != types.WaitingReasonSourceUnrouted {
			t.Errorf("waiting = %q, want %q", got, types.WaitingReasonSourceUnrouted)
		}
		if lf := store.lastFailure(0); lf == nil || lf.PodName != "svc-a-engine-0-default-0" {
			t.Errorf("lastFailure = %+v, want the source named", lf)
		}
	})

	t.Run("a serving source with no routed Service is in rotation", func(t *testing.T) {
		store, _ := newStore(surgingRow(types.WaitingReasonSourceUnrouted))
		applySurge(t, store, []*corev1.Pod{sourcePod("svc-a-engine-0-default-0", true, false)})
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want released: the gate is the only view there is", got)
		}
	})

	t.Run("a serving pod set does not retire the report", func(t *testing.T) {
		// The arms that judge on pod health release on a serving pod
		// set; this one asks its own question, because pod conditions
		// can read healthy while nothing is in rotation.
		store, _ := newStore(surgingRow(""))
		pod := sourcePod("svc-a-engine-0-default-0", true, true)
		service := query.PerRevisionServiceName("svc-a", types.ComponentEngine, sourceHash)
		applySurge(t, store, []*corev1.Pod{pod}, sliceWithout(service, "svc-a-engine-0-default-1"))
		if got := store.waiting(0); got != types.WaitingReasonSourceUnrouted {
			t.Errorf("waiting = %q, want the report kept: the source has no routable endpoint", got)
		}
	})

	t.Run("released past the surge step", func(t *testing.T) {
		row := surgingRow(types.WaitingReasonSourceUnrouted)
		row.Operation.Step = types.UpdateStepSurgeDrain
		store, _ := newStore(row)
		applySurge(t, store, []*corev1.Pod{sourcePod("svc-a-engine-0-default-0", false, false)})
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want released: the surge takes its own source out at Drain", got)
		}
	})

	t.Run("only the update pass's row may report one", func(t *testing.T) {
		row := surgingRow("")
		row.Phase = types.InstancePhaseMigrating
		row.Operation.Type = types.InstanceOperationMigrate
		store, _ := newStore(row)
		applySurge(t, store, []*corev1.Pod{sourcePod("svc-a-engine-0-default-0", false, false)})
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want none: only a surge takes a source out", got)
		}
	})

	t.Run("an incumbent fact keeps the row", func(t *testing.T) {
		store, _ := newStore(surgingRow(types.WaitingReasonUnschedulable))
		pods := []*corev1.Pod{
			sourcePod("svc-a-engine-0-default-0", false, false),
			unschedulablePod("svc-a-engine-0-default-1", schedulerMessage, metav1.Now()),
		}
		applySurge(t, store, pods)
		if got := store.waiting(0); got != types.WaitingReasonUnschedulable {
			t.Errorf("waiting = %q, want the incumbent token kept", got)
		}
	})
}

// sliceWithout is an EndpointSlice for service whose only ready endpoint
// is some other pod, so the source reads as out of rotation.
func sliceWithout(service, otherPod string) *discoveryv1.EndpointSlice {
	ready := true
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      service + "-abc",
			Namespace: "ns",
			Labels:    map[string]string{discoveryv1.LabelServiceName: service},
		},
		Endpoints: []discoveryv1.Endpoint{{
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: "ns", Name: otherPod},
		}},
	}
}
