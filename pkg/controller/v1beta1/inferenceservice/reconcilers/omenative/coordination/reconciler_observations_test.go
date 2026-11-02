package coordination

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// mixedPairingService is an engine and a decoder under one RollingUpdate
// group, mid-roll: the engine's Instance already serves version 2 while the
// decoder's still serves version 1, each side's replica mid-update.
func mixedPairingService() (*v1beta1.InferenceService, []runtime.Object) {
	isvc := testOMENativeISVC()
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		},
	}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{{
			Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			RollingUpdate: &v1beta1.GroupRollingUpdate{},
		}},
	}
	replica := func(component v1beta1.ComponentType) *v1beta1.InferenceReplica {
		name := isvc.Name + "-" + string(component)
		return &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: isvc.Namespace},
			Spec:       v1beta1.InferenceReplicaSpec{Replicas: int32Ptr(1)},
			Status: v1beta1.InferenceReplicaStatus{
				Replicas: 1, ServingReplicas: 1,
				CurrentRevision: name + "-v1", UpdateRevision: name + "-v2",
			},
		}
	}
	objs := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "v2", 0),
		buildPod(isvc, v1beta1.DecoderComponent, "v1", 0),
		replica(v1beta1.EngineComponent), replica(v1beta1.DecoderComponent),
	}
	return isvc, objs
}

// countEvents is how many of the recorder's events carry the reason.
func countEvents(events []string, reason string) int {
	n := 0
	for _, e := range events {
		if strings.Contains(e, " "+reason+" ") {
			n++
		}
	}
	return n
}

// An engine and a decoder roll under a RollingUpdate group; the engine's
// Instance serves version 2 while the decoder's still serves version 1.
// The pass that first sees the two revisions side by side records one
// MixedPairingObserved event. The passes after it see the same picture
// and record nothing more: a roll that keeps the picture for ten
// minutes writes one event, not one per pass, so the events that follow
// (the run's close among them) keep their share of the service's event
// budget.
func TestReconcile_MixedPairingRecordedOncePerRoll(t *testing.T) {
	isvc, objs := mixedPairingService()
	c := testClient(objs...)
	rec := record.NewFakeRecorder(64)
	for pass := 1; pass <= 3; pass++ {
		if _, err := Reconcile(context.Background(), ReconcileInputs{
			ISVC: isvc, Client: c, Reader: c, Recorder: rec, Now: time.Now(),
			ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent, v1beta1.DecoderComponent),
			ComponentRunnerPorts:     testComponentRunnerPorts(),
		}); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
	}
	events := drainEvents(rec)
	if got := countEvents(events, EventReasonMixedPairingObserved); got != 1 {
		t.Fatalf("three passes over one mixed picture recorded %s %d time(s), want 1: %v", EventReasonMixedPairingObserved, got, events)
	}
	group := isvc.Status.RolloutCoordination.Groups[0]
	if !group.MixedPairing {
		t.Fatalf("the status must carry the picture while it holds; got %+v", group)
	}

	// The decoder reaches version 2: the picture is uniform again and the
	// status says so. A later push that splits the sides again is a new
	// observation and its event is recorded again.
	pass := func(label string) {
		t.Helper()
		if _, err := Reconcile(context.Background(), ReconcileInputs{
			ISVC: isvc, Client: c, Reader: c, Recorder: rec, Now: time.Now(),
			ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent, v1beta1.DecoderComponent),
			ComponentRunnerPorts:     testComponentRunnerPorts(),
		}); err != nil {
			t.Fatalf("%s: %v", label, err)
		}
	}
	relabel := func(component v1beta1.ComponentType, revision string) {
		t.Helper()
		pod := &corev1.Pod{}
		key := client.ObjectKey{Namespace: isvc.Namespace, Name: podName(isvc.Name, component, 0)}
		if err := c.Get(context.Background(), key, pod); err != nil {
			t.Fatalf("get %s: %v", key.Name, err)
		}
		pod.Labels[query.LabelRevisionHash] = revision
		if err := c.Update(context.Background(), pod); err != nil {
			t.Fatalf("update %s: %v", key.Name, err)
		}
	}
	relabel(v1beta1.DecoderComponent, "v2")
	pass("uniform picture")
	if group := isvc.Status.RolloutCoordination.Groups[0]; group.MixedPairing {
		t.Fatalf("both sides on version 2 must clear the picture; got %+v", group)
	}
	relabel(v1beta1.EngineComponent, "v3")
	pass("second roll")
	pass("second roll, next pass")
	if got := countEvents(drainEvents(rec), EventReasonMixedPairingObserved); got != 1 {
		t.Fatalf("the second roll's split must be recorded once more, got %d", got)
	}
}

// recordObservations records each observation's event on the pass it begins
// and counts it on every pass it holds; a pass that finds it already on the
// previous status, or finds it gone, records no event.
func TestRecordObservations_RecordsTheEventOnTheFirstPassOnly(t *testing.T) {
	isvc := testOMENativeISVC()
	g := ResolvedGroup{Name: "0", Policy: v1beta1.CoordinationPolicyRollingUpdate}
	type status = v1beta1.RolloutCoordinationGroupStatus
	for _, tc := range []struct {
		name           string
		previous, next status
		want           []string
	}{
		{"both begin", status{}, status{MixedPairing: true, SurgeHeldByRatio: true},
			[]string{EventReasonMixedPairingObserved, EventReasonRatioSkewRejected}},
		{"both hold", status{MixedPairing: true, SurgeHeldByRatio: true}, status{MixedPairing: true, SurgeHeldByRatio: true}, nil},
		{"both end", status{MixedPairing: true, SurgeHeldByRatio: true}, status{}, nil},
		{"the refusal begins under a held picture", status{MixedPairing: true}, status{MixedPairing: true, SurgeHeldByRatio: true},
			[]string{EventReasonRatioSkewRejected}},
		{"the refusal returns after a pass without it", status{MixedPairing: true}, status{MixedPairing: true, SurgeHeldByRatio: true},
			[]string{EventReasonRatioSkewRejected}},
		{"nothing observed", status{}, status{}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := record.NewFakeRecorder(8)
			recordObservations(rec, isvc, g, tc.previous, tc.next, nil)
			var got []string
			for _, e := range drainEvents(rec) {
				got = append(got, strings.Fields(e)[1])
			}
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("events %v, want %v", got, tc.want)
			}
		})
	}
	// A nil recorder records nothing, as with every emitter here.
	recordObservations(nil, isvc, g, status{}, status{MixedPairing: true, SurgeHeldByRatio: true}, nil)
}

// RatioBalanced pacing holds a member's next start while the member runs
// ahead of its peer by more than the tolerance: 12 engines with 4 already on
// version 2 beside 8 decoders with none, at tolerance 20. The event names the
// move that was held by the member's strategy: a recreate or an in-place
// restart under a drain-first strategy, a surge start under SurgeThenDrain.
func TestReconcile_RatioHoldEventNamesTheHeldMove(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	for _, tc := range []struct {
		name     string
		strategy v1beta1.UpdateStrategyType
		want     string
	}{
		{"recreate", v1beta1.UpdateStrategyRecreatePod, "held the next recreate of engine in group 0"},
		{"in-place restart", v1beta1.UpdateStrategyInPlaceIfPossible, "held the next in-place restart of engine in group 0"},
		{"surge start", v1beta1.UpdateStrategySurgeThenDrain, "held the next surge start of engine in group 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			members := map[v1beta1.ComponentType]batchMemberFixture{
				eng: {desired: 12, strategy: tc.strategy, promoted: 4},
				dec: {desired: 8, strategy: tc.strategy},
			}
			isvc := batchService(batchGroupFixture{tolerance: int32Ptr(20)}, members)
			c := testClient(batchReplica(eng, members[eng]), batchReplica(dec, members[dec]))
			rec := record.NewFakeRecorder(16)
			if _, err := Reconcile(context.Background(), ReconcileInputs{
				ISVC: isvc, Client: c, Reader: c, Recorder: rec, Now: time.Now(),
				ComponentDeploymentModes: testOMENativeModes(eng, dec),
				ComponentRunnerPorts:     testComponentRunnerPorts(),
			}); err != nil {
				t.Fatalf("pass: %v", err)
			}
			var got []string
			for _, e := range drainEvents(rec) {
				if strings.Contains(e, " "+EventReasonRatioSkewRejected+" ") {
					got = append(got, e)
				}
			}
			if len(got) != 1 || !strings.Contains(got[0], tc.want) {
				t.Fatalf("%s events %q, want one naming %q", EventReasonRatioSkewRejected, got, tc.want)
			}
		})
	}
}
