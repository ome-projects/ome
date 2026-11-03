package canary

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// A prefill/decode unit canaried on both members: the engine is the primary
// and the decoder a secondary. The dispatch reads a crash loop and a restart
// on every member whose pods are canary pods, so a decoder canary pod that
// keeps dying stops the unit Failed, and a decoder pod that died once holds a timed
// step until it has served a full soak, exactly as the engine's pods do.

// pdWire is an [engine, decoder] canary of two Instances a side, one of each
// on the canary revision, armed through the dispatcher under a step list: the
// readings the step machine gets come from both members' pods and rows.
type pdWire struct {
	ctx    context.Context
	client client.Client
	rec    *record.FakeRecorder
	deps   DispatchDeps
	isvc   *v1beta1.InferenceService
	ns     string
	name   string
}

// armPDWire arms the unit at its first step with the split serving on both
// members. The run opened before the canary pods served, as a push does,
// and every row has been Ready since before the story starts.
func armPDWire(t *testing.T, steps []v1beta1.RolloutGroupStep, t0 time.Time) *pdWire {
	t.Helper()
	ns, name := "default", "pd-wire"
	n2 := 2
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Canary:     &v1beta1.GroupCanary{Steps: steps},
	}}}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.OpenedAt = metav1.NewTime(t0.Add(-5 * time.Minute))
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "engnew", StableRevision: "engold"},
		{Component: v1beta1.DecoderComponent, Revision: "decnew", StableRevision: "decold"},
	}
	readyAt := t0.Add(-2 * time.Minute)
	member := func(comp v1beta1.ComponentType, stable, target string) *v1beta1.InferenceReplica {
		r := ir(ns, name, comp, target)
		r.Generation = 2
		r.Status.CurrentRevision = r.Name + "-" + stable
		r.Status.ObservedGeneration = r.Generation
		r.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
			instanceRow(0, v1beta1.OMENativeInstanceReady, r.Name+"-"+stable, &readyAt, 1),
			instanceRow(1, v1beta1.OMENativeInstanceReady, r.Name+"-"+target, &readyAt, 1),
		}
		return r
	}
	c := fakeclient.New(t,
		fakeclient.WithStatusSubresource(&v1beta1.InferenceReplica{}),
		fakeclient.WithObjects(
			isvc,
			member(v1beta1.EngineComponent, "engold", "engnew"),
			member(v1beta1.DecoderComponent, "decold", "decnew"),
			canaryPod(ns, name, "engine", "engold", name+"-engine-0"),
			canaryPod(ns, name, "engine", "engnew", name+"-engine-1"),
			canaryPod(ns, name, "decoder", "decold", name+"-decoder-0"),
			canaryPod(ns, name, "decoder", "decnew", name+"-decoder-1"),
			canaryControllerRevision(ns, name, "engine", "engold", 1),
			canaryControllerRevision(ns, name, "engine", "engnew", 2),
			canaryControllerRevision(ns, name, "decoder", "decold", 1),
			canaryControllerRevision(ns, name, "decoder", "decnew", 2),
		),
	)
	rec := record.NewFakeRecorder(16)
	w := &pdWire{ctx: context.Background(), client: c, rec: rec, isvc: isvc, ns: ns, name: name,
		deps: DispatchDeps{Client: c, Reader: c, Recorder: rec, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{}), Now: t0}}
	w.dispatchAt(t, t0)
	if got := w.phase(); got != v1beta1.RolloutPhasePaused {
		t.Fatalf("the split must serve on both members before the story starts, got %q", got)
	}
	return w
}

func (w *pdWire) dispatchAt(t *testing.T, now time.Time) {
	t.Helper()
	w.deps.Now = now
	syncInstanceRows(t, w.client, w.isvc, v1beta1.EngineComponent, v1beta1.DecoderComponent)
	if _, err := Dispatch(w.ctx, w.deps); err != nil {
		t.Fatalf("Dispatch at %v: %v", now, err)
	}
}

func (w *pdWire) phase() v1beta1.RolloutPhase {
	return w.isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase
}

func (w *pdWire) status() *v1beta1.CanaryStatus {
	return rollout.CanaryStatusFor(&w.isvc.Status, v1beta1.EngineComponent)
}

// setRunner rewrites one pod's runner status and readiness.
func (w *pdWire) setRunner(t *testing.T, pod string, status corev1.ContainerStatus, ready bool) {
	t.Helper()
	live := &corev1.Pod{}
	if err := w.client.Get(w.ctx, types.NamespacedName{Namespace: w.ns, Name: pod}, live); err != nil {
		t.Fatal(err)
	}
	live.Status.ContainerStatuses = []corev1.ContainerStatus{status}
	if ready {
		readyAgain(live)
	} else {
		notReady(live)
	}
	if err := w.client.Status().Update(w.ctx, live); err != nil {
		t.Fatal(err)
	}
}

// The decoder's canary pod keeps dying after it served: the dispatch reads
// the crash loop on the secondary, the unit stops Failed at its step with the
// decoder pod named, and both members' splits return to their stable
// revisions, as they do for an engine pod.
func TestDispatch_PDDecoderCanaryPodCrashLoopFailsTheUnit(t *testing.T) {
	t0 := time.Unix(100000, 0)
	w := armPDWire(t, twoStep(), t0)

	// Ten seconds into the hold the decoder's canary runner starts a second
	// run, dies again and the kubelet backs off before the next start.
	w.setRunner(t, w.name+"-decoder-1", runnerInCrashLoopBackOff(t0.Add(10*time.Second), t0.Add(20*time.Second)), false)
	w.dispatchAt(t, t0.Add(30*time.Second))

	cs := w.status()
	if w.phase() != v1beta1.RolloutPhaseFailed || cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureCapacityTimeout || cs.CurrentStep != 0 {
		t.Fatalf("a decoder canary pod in a crash loop must stop the unit Failed at its step, got phase %q status %+v", w.phase(), cs)
	}
	select {
	case ev := <-w.rec.Events:
		if !strings.Contains(ev, EventReasonCanaryPodCrashed) || !strings.Contains(ev, w.name+"-decoder-1") {
			t.Fatalf("the Failed stop must name the decoder pod that died, got %q", ev)
		}
	default:
		t.Fatal("the Failed stop must be announced with an event")
	}
	expectTargets(t, w.isvc, v1beta1.EngineComponent, map[string]trafficWant{"engold": {percent: 100}})
	expectTargets(t, w.isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decold": {percent: 100}})
}

// The decoder's canary pod dies once inside a timed soak and the kubelet
// brings it back: the soak measures from the newest restart of any member's
// canary pod, so the step holds past the soak since the split first served
// and moves once the restarted decoder has served a full soak.
func TestDispatch_PDDecoderRestartInSoakHoldsTheStepForAFullSoak(t *testing.T) {
	t0 := time.Unix(100000, 0)
	w := armPDWire(t, twoStepTimed(20*time.Second), t0)

	// Ten seconds into the soak the decoder's canary runner dies; the
	// kubelet has it running again two seconds later and it is serving.
	w.setRunner(t, w.name+"-decoder-1", runnerDied(t0.Add(10*time.Second)), true)
	w.dispatchAt(t, t0.Add(25*time.Second))
	cs := w.status()
	if cs.CurrentStep != 0 || w.phase() != v1beta1.RolloutPhasePaused {
		t.Fatalf("a decoder restart inside the soak must hold the step until a full soak since it, got step %d phase %q", cs.CurrentStep, w.phase())
	}

	w.dispatchAt(t, t0.Add(33*time.Second))
	cs = w.status()
	if cs.CurrentStep != 1 || w.phase() != v1beta1.RolloutPhasePending {
		t.Fatalf("a full soak since the decoder's restart must advance, got step %d phase %q", cs.CurrentStep, w.phase())
	}
}
