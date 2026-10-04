package holds

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// admissionMessage is the apiserver's wording when it fails closed on a
// webhook it could not call.
const admissionMessage = `Internal error occurred: failed calling webhook "pod-mutator.example.com": failed to call webhook: Post "https://ome-webhook.example.svc:443/mutate-pods?timeout=10s": connection refused`

// servingPod is a live pod in rotation: ContainersReady with the serving
// gate True.
func servingPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: podreadiness.ConditionType, Status: corev1.ConditionTrue},
		}},
	}
}

// TestAdmission_RecordsTheRefusalFromTheWriteSite: the apiserver being
// unable to consult admission is an answer to a write the create or
// patch site made, so that site records the wait, with the apiserver's
// own words on LastFailure, and announces it exactly once per episode.
// The pass keeps the row's report until the site releases it, or until
// the row is plainly in rotation.
func TestAdmission_RecordsTheRefusalFromTheWriteSite(t *testing.T) {
	ctx, plan := context.Background(), types.ComponentPlan{}

	t.Run("records the token with the apiserver's words", func(t *testing.T) {
		store, in := newStore(creatingRow(""))
		entered, err := RecordAdmissionRefusal(ctx, in, plan, 0, "svc-a-engine-0-default-0", admissionMessage)
		if err != nil || !entered {
			t.Fatalf("RecordAdmissionRefusal: entered=%v err=%v want true, nil", entered, err)
		}
		if got := store.waiting(0); got != types.RejectionReasonAdmissionUnavailable {
			t.Errorf("waiting = %q, want %q", got, types.RejectionReasonAdmissionUnavailable)
		}
		lf := store.lastFailure(0)
		if lf == nil || lf.Reason != types.RejectionReasonAdmissionUnavailable || lf.Message != admissionMessage || lf.PodName != "svc-a-engine-0-default-0" {
			t.Errorf("lastFailure = %+v, want the apiserver's words on the refused pod under the token", lf)
		}
		if lf != nil && lf.Time.IsZero() {
			t.Error("lastFailure.Time must mark when the wait began")
		}
	})

	t.Run("a repeat refusal is write-free and not an entry", func(t *testing.T) {
		store, in := newStore(creatingRow(""))
		if _, err := RecordAdmissionRefusal(ctx, in, plan, 0, "svc-a-engine-0-default-0", admissionMessage); err != nil {
			t.Fatal(err)
		}
		store.writes = 0
		entered, err := RecordAdmissionRefusal(ctx, store.input(), plan, 0, "svc-a-engine-0-default-0", "Internal error occurred: failed calling webhook \"pod-mutator.example.com\": context deadline exceeded")
		if err != nil || entered {
			t.Fatalf("repeat: entered=%v err=%v want false, nil", entered, err)
		}
		if store.writes != 0 {
			t.Errorf("writes on a repeat refusal = %d, want 0: the words move every pass, the wait does not", store.writes)
		}
	})

	t.Run("released by the site once a write goes through", func(t *testing.T) {
		store, in := newStore(creatingRow(types.RejectionReasonAdmissionUnavailable))
		if err := ReleaseAdmissionRefusal(ctx, in, 0); err != nil {
			t.Fatal(err)
		}
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want the token released", got)
		}
	})

	t.Run("the release leaves another authority's token alone", func(t *testing.T) {
		store, in := newStore(creatingRow(types.WaitingReasonUnschedulable))
		if err := ReleaseAdmissionRefusal(ctx, in, 0); err != nil {
			t.Fatal(err)
		}
		if got := store.waiting(0); got != types.WaitingReasonUnschedulable {
			t.Errorf("waiting = %q, want the scheduler's token kept", got)
		}
	})

	t.Run("another authority keeps the row and nothing is announced", func(t *testing.T) {
		store, in := newStore(creatingRow(types.WaitingReasonUnschedulable))
		entered, err := RecordAdmissionRefusal(ctx, in, plan, 0, "svc-a-engine-0-default-0", admissionMessage)
		if err != nil || entered {
			t.Fatalf("behind the scheduler: entered=%v err=%v want false, nil", entered, err)
		}
		if got := store.waiting(0); got != types.WaitingReasonUnschedulable {
			t.Errorf("waiting = %q, want the incumbent token kept", got)
		}
		if store.lastFailure(0) != nil {
			t.Error("lastFailure must not be overwritten on a row another authority reports")
		}
	})

	t.Run("the pass keeps the recorded report and the park reads it", func(t *testing.T) {
		store, in := newStore(creatingRow(""))
		if _, err := RecordAdmissionRefusal(ctx, in, plan, 0, "svc-a-engine-0-default-0", admissionMessage); err != nil {
			t.Fatal(err)
		}
		store.writes = 0
		apply(t, store.input(), plan, nil)
		apply(t, store.input(), plan, nil)
		if got := store.waiting(0); got != types.RejectionReasonAdmissionUnavailable {
			t.Errorf("waiting after the hold pass = %q, want the recorded wait kept", got)
		}
		if store.writes != 0 {
			t.Errorf("writes by the hold pass on an unchanged wait = %d, want 0", store.writes)
		}
		if !types.OperationExternallyHeld(store.rows[0].Operation) {
			t.Error("the deadline park must read the wait off the token")
		}
	})

	t.Run("a row in rotation keeps the wait", func(t *testing.T) {
		// A surge or in-place roll writes to a row whose pods are serving,
		// so rotation says nothing about whether admission took the
		// write; only the write site learns that. Retiring the token
		// here would have the site re-enter the wait on every pass.
		store, in := newStore(creatingRow(types.RejectionReasonAdmissionUnavailable))
		store.writes = 0
		apply(t, in, plan, map[int32][]*corev1.Pod{0: {servingPod("svc-a-engine-0-default-0")}})
		if got := store.waiting(0); got != types.RejectionReasonAdmissionUnavailable {
			t.Errorf("waiting = %q, want the recorded wait kept on a serving row", got)
		}
		if store.writes != 0 {
			t.Errorf("writes by the hold pass on a serving held row = %d, want 0", store.writes)
		}
	})
}
