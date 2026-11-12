package canary

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// A manual prefill-decode canary with two engine and two decoder Instances
// holds at its first split: one Instance of each on the canary revision.
// The serving canary engine pod is deleted by hand. The step keeps its
// phase and its recorded share while the split rests on the stable
// Instances, nothing moves to a revision with no serving pod, the stable
// Instances stay held until the engine is rebuilt on the canary revision,
// and the steps then land the canary revision on every Instance of both
// members. The two orderings an operator can produce are both covered: the
// deletion while the hold stands, and the deletion in the same second as
// the promote, where the pass that applies the promote still counts the
// pod and the pass after it finds the pod on its way out.

// pdPodLoss is the PD canary at its first split with the pod identities
// the passes read through the client: the canary pod the walk loses, on
// the engine unless a walk names the decoder.
type pdPodLoss struct {
	*pdCanary
	lostPod       types.NamespacedName
	lostComponent string
	lostHash      string
}

func newPDPodLoss(t *testing.T, name string) *pdPodLoss {
	return newPDPodLossOf(t, name, "engine", "engnew")
}

// newPDDecoderPodLoss is the same split with the decoder's canary pod as the
// one the walk loses.
func newPDDecoderPodLoss(t *testing.T, name string) *pdPodLoss {
	return newPDPodLossOf(t, name, "decoder", "decnew")
}

func newPDPodLossOf(t *testing.T, name, component, hash string) *pdPodLoss {
	t.Helper()
	f := newPDCanary(t, name, true)
	f.deps.Now = time.Unix(1000, 0)
	f.deps.DefaultReadyTimeout = cellReadyTimeout
	f.dispatch("serve")
	if f.phase() != v1beta1.RolloutPhasePaused {
		t.Fatalf("the split must serve under the manual hold, got %q", f.phase())
	}
	p := &pdPodLoss{pdCanary: f, lostPod: types.NamespacedName{Namespace: f.ns, Name: name + "-" + component + "-1"}, lostComponent: component, lostHash: hash}
	p.expectSplit()
	return p
}

func (p *pdPodLoss) tick(pass string) {
	p.t.Helper()
	p.deps.Now = p.deps.Now.Add(time.Second)
	p.dispatch(pass)
}

func (p *pdPodLoss) status() *v1beta1.CanaryStatus {
	return rollout.CanaryStatusFor(&p.isvc.Status, v1beta1.EngineComponent)
}

func (p *pdPodLoss) partition(comp v1beta1.ComponentType) int32 {
	p.t.Helper()
	var ext *v1beta1.ComponentExtensionSpec
	switch comp {
	case v1beta1.EngineComponent:
		ext = &p.isvc.Spec.Engine.ComponentExtensionSpec
	case v1beta1.DecoderComponent:
		ext = &p.isvc.Spec.Decoder.ComponentExtensionSpec
	}
	got := StepPartition(p.isvc, rollout.Policies{}, comp, DesiredReplicas(nil, ext))
	if got == nil {
		p.t.Fatalf("%s projects no canary partition", comp)
	}
	return *got
}

// startDeleting marks the lost canary pod under deletion: the deletion
// reached the controller while the kubelet still reports the pod Running
// and Ready, the shape a hand deletion has inside its grace period.
func (p *pdPodLoss) startDeleting() {
	p.t.Helper()
	ctx := context.Background()
	pod := &corev1.Pod{}
	if err := p.c.Get(ctx, p.lostPod, pod); err != nil {
		p.t.Fatal(err)
	}
	pod.Finalizers = []string{"test/grace"}
	if err := p.c.Update(ctx, pod); err != nil {
		p.t.Fatal(err)
	}
	if err := p.c.Delete(ctx, pod); err != nil {
		p.t.Fatal(err)
	}
	if err := p.c.Get(ctx, p.lostPod, pod); err != nil || pod.DeletionTimestamp == nil {
		p.t.Fatalf("the pod must stay readable under deletion, got err=%v pod=%+v", err, pod.ObjectMeta)
	}
}

// finishDeleting lets the pod leave.
func (p *pdPodLoss) finishDeleting() {
	p.t.Helper()
	ctx := context.Background()
	pod := &corev1.Pod{}
	if err := p.c.Get(ctx, p.lostPod, pod); err != nil {
		p.t.Fatal(err)
	}
	pod.Finalizers = nil
	if err := p.c.Update(ctx, pod); err != nil {
		p.t.Fatal(err)
	}
	if err := p.c.Get(ctx, p.lostPod, pod); !apierrors.IsNotFound(err) {
		p.t.Fatalf("the pod must be gone, got err=%v", err)
	}
}

// rebuild is the workload's repair: the lost Instance is rebuilt on its
// canary revision and serves.
func (p *pdPodLoss) rebuild() {
	p.t.Helper()
	if err := p.c.Create(context.Background(), canaryPod(p.ns, p.name, p.lostComponent, p.lostHash, p.lostPod.Name)); err != nil {
		p.t.Fatal(err)
	}
}

// readyCanaryEnginePods counts the engine pods that serve the canary
// revision, as the capacity gate counts them.
func (p *pdPodLoss) readyCanaryEnginePods() int {
	p.t.Helper()
	pods := &corev1.PodList{}
	if err := p.c.List(context.Background(), pods); err != nil {
		p.t.Fatal(err)
	}
	n := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Labels[constants.OMEComponentLabel] == "engine" && pod.Labels["ome.io/revision-hash"] == "engnew" && pod.DeletionTimestamp == nil {
			n++
		}
	}
	return n
}

func (p *pdPodLoss) expectSplit() {
	p.t.Helper()
	p.expectTraffic(v1beta1.EngineComponent, map[string]trafficWant{
		"engnew": {percent: 50, protocol: pdCanaryProtocol, latest: true},
		"engold": {percent: 50, protocol: pdStableProtocol},
	})
	p.expectTraffic(v1beta1.DecoderComponent, map[string]trafficWant{
		"decnew": {percent: 50, protocol: pdCanaryProtocol, latest: true},
		"decold": {percent: 50, protocol: pdStableProtocol},
	})
}

// expectRestingOnStable asserts the split of both members rests on the
// stable revision while the canary path is broken.
func (p *pdPodLoss) expectRestingOnStable() {
	p.t.Helper()
	p.expectTraffic(v1beta1.EngineComponent, map[string]trafficWant{
		"engold": {percent: 100, protocol: pdStableProtocol},
	})
	p.expectTraffic(v1beta1.DecoderComponent, map[string]trafficWant{
		"decold": {percent: 100, protocol: pdStableProtocol},
	})
}

func (p *pdPodLoss) expectCanaryOnly() {
	p.t.Helper()
	p.expectTraffic(v1beta1.EngineComponent, map[string]trafficWant{
		"engnew": {percent: 100, protocol: pdCanaryProtocol, latest: true},
	})
	p.expectTraffic(v1beta1.DecoderComponent, map[string]trafficWant{
		"decnew": {percent: 100, protocol: pdCanaryProtocol, latest: true},
	})
}

// expectHeld asserts a pass that holds the canary on the engine's lost
// capacity: the step and its recorded share stand, the wait is recorded,
// the stable Instances stay held and the split rests on them.
func (p *pdPodLoss) expectHeld(step int32, phase v1beta1.RolloutPhase, when string) {
	p.t.Helper()
	cs := p.status()
	if cs.CurrentStep != step || cs.ObservedTrafficWeight != 50 || cs.CapacityWaitSince == nil || cs.Failed != nil {
		p.t.Fatalf("%s: the hold must keep step %d and its share and record the wait, got %+v", when, step, cs)
	}
	if p.phase() != phase {
		p.t.Fatalf("%s: phase %q, want %q", when, p.phase(), phase)
	}
	if e, d := p.partition(v1beta1.EngineComponent), p.partition(v1beta1.DecoderComponent); e != 1 || d != 1 {
		p.t.Fatalf("%s: the stable Instances must stay held, got partitions engine=%d decoder=%d", when, e, d)
	}
	p.expectRestingOnStable()
}

// expectDone asserts the canary finished with every Instance of both
// members on its canary revision.
func (p *pdPodLoss) expectDone() {
	p.t.Helper()
	cs := p.status()
	if cs.CurrentStep != 2 || p.phase() != v1beta1.RolloutPhaseStable {
		p.t.Fatalf("the steps must complete on the canary revision, got %+v phase=%q", cs, p.phase())
	}
	if n := p.readyCanaryEnginePods(); n != 2 {
		p.t.Fatalf("every engine Instance must serve the canary revision, got %d of 2", n)
	}
	if e, d := p.partition(v1beta1.EngineComponent), p.partition(v1beta1.DecoderComponent); e != 0 || d != 0 {
		p.t.Fatalf("a finished canary holds nothing, got partitions engine=%d decoder=%d", e, d)
	}
	p.expectCanaryOnly()
}

// finish drives the final step once the engine serves its canary pod
// again: 100% moves on the capacity the pass counted, the release waits
// for the next pass's count, and the released Instances roll.
func (p *pdPodLoss) finish() {
	p.t.Helper()
	p.tick("shift")
	cs := p.status()
	if cs.CurrentStep != 1 || p.phase() != v1beta1.RolloutPhasePromoting || cs.CapacityWaitSince != nil {
		p.t.Fatalf("the pass that moves traffic to 100%% keeps the final step and clears the wait, got %+v phase=%q", cs, p.phase())
	}
	if e := p.partition(v1beta1.EngineComponent); e != 1 {
		p.t.Fatalf("the stable Instance is released only on a later count, got partition %d", e)
	}
	p.expectCanaryOnly()
	p.tick("release")
	if cs.CurrentStep != 2 || p.phase() != v1beta1.RolloutPhasePromoting {
		p.t.Fatalf("the release writes the sentinel on its own count, got %+v phase=%q", cs, p.phase())
	}
	if e, d := p.partition(v1beta1.EngineComponent), p.partition(v1beta1.DecoderComponent); e != 0 || d != 0 {
		p.t.Fatalf("the sentinel releases the held Instances, got partitions engine=%d decoder=%d", e, d)
	}
	p.movePod("engine", "engold", "engnew", "0")
	p.movePod("decoder", "decold", "decnew", "0")
	p.tick("cut over")
	p.expectDone()
}

// The pod is deleted while the manual hold stands. The step keeps its
// split, the rebuilt engine serves it again, and the promote that follows
// lands the canary revision on every Instance.
func TestDispatch_PDManualHoldKeepsItsEngineThroughACanaryPodDeletion(t *testing.T) {
	p := newPDPodLoss(t, "pdhold")

	p.startDeleting()
	p.tick("deletion seen")
	p.expectHeld(0, v1beta1.RolloutPhasePaused, "pod under deletion")

	p.finishDeleting()
	p.tick("pod gone")
	p.expectHeld(0, v1beta1.RolloutPhasePaused, "pod gone")

	p.rebuild()
	p.tick("rebuilt")
	cs := p.status()
	if cs.CurrentStep != 0 || p.phase() != v1beta1.RolloutPhasePaused || cs.CapacityWaitSince != nil {
		t.Fatalf("the rebuilt pod serves the split again under the hold, got %+v phase=%q", cs, p.phase())
	}
	p.expectSplit()

	annotate(p.isvc, constants.RolloutPromoteAnnotation, "engnew")
	p.tick("promote")
	delete(p.isvc.Annotations, constants.RolloutPromoteAnnotation)
	if cs.CurrentStep != 1 || p.phase() != v1beta1.RolloutPhasePending {
		t.Fatalf("the promote stages the final step, got %+v phase=%q", cs, p.phase())
	}
	p.finish()
}

// The decoder's canary pod is deleted while the manual hold stands. The
// dispatch reads the decoder's capacity off its own replica, so the step
// holds on the secondary exactly as it holds on the primary: the step and
// its recorded share stand, the wait is clocked, the stable Instances of
// both members stay held, both members' splits rest on the stable revisions
// while the decoder's canary revision has no serving pod, and the rebuilt
// decoder serves the split again.
func TestDispatch_PDManualHoldKeepsItsDecoderThroughACanaryPodDeletion(t *testing.T) {
	p := newPDDecoderPodLoss(t, "pddec")

	p.startDeleting()
	p.tick("deletion seen")
	p.expectHeld(0, v1beta1.RolloutPhasePaused, "decoder pod under deletion")

	p.finishDeleting()
	p.tick("pod gone")
	p.expectHeld(0, v1beta1.RolloutPhasePaused, "decoder pod gone")

	p.rebuild()
	p.tick("rebuilt")
	cs := p.status()
	if cs.CurrentStep != 0 || p.phase() != v1beta1.RolloutPhasePaused || cs.CapacityWaitSince != nil {
		t.Fatalf("the rebuilt decoder serves the split again under the hold, got %+v phase=%q", cs, p.phase())
	}
	p.expectSplit()

	annotate(p.isvc, constants.RolloutPromoteAnnotation, "engnew")
	p.tick("promote")
	delete(p.isvc.Annotations, constants.RolloutPromoteAnnotation)
	if cs.CurrentStep != 1 || p.phase() != v1beta1.RolloutPhasePending {
		t.Fatalf("the promote stages the final step, got %+v phase=%q", cs, p.phase())
	}
	p.finish()
}

// The promote and the deletion land in the same second. The pass that
// applies the promote still counts the pod; the pass after it finds the pod
// on its way out and holds the final step with the stable Instances still
// held. No traffic and no release lands on a revision with no serving pod,
// and the rebuilt engine carries the canary to completion.
func TestDispatch_PDManualCanaryKeepsItsEngineWhenThePodIsDeletedAtPromote(t *testing.T) {
	p := newPDPodLoss(t, "pdrace")

	annotate(p.isvc, constants.RolloutPromoteAnnotation, "engnew")
	p.tick("promote")
	delete(p.isvc.Annotations, constants.RolloutPromoteAnnotation)
	cs := p.status()
	if cs.CurrentStep != 1 || p.phase() != v1beta1.RolloutPhasePending {
		t.Fatalf("the promote stages the final step, got %+v phase=%q", cs, p.phase())
	}

	p.startDeleting()
	p.tick("deletion seen")
	p.expectHeld(1, v1beta1.RolloutPhasePending, "pod under deletion")

	p.finishDeleting()
	p.tick("pod gone")
	p.expectHeld(1, v1beta1.RolloutPhasePending, "pod gone")

	// Inside the ready timeout the hold stands; nothing stops.
	p.deps.Now = p.deps.Now.Add(cellReadyTimeout / 2)
	p.tick("still gone")
	p.expectHeld(1, v1beta1.RolloutPhasePending, "half the ready timeout in")

	p.rebuild()
	p.finish()
}

// The deletion reaches the controller one pass after the promote: the pass
// that stages the final step and the pass that moves traffic to 100% both
// still count the pod, and the pass after them finds it on its way out.
// The release waits on a count of its own, so the stable Instances stay
// held through the drain until the rebuilt engine serves; the drain's 100%
// write stands meanwhile, and the pass that counts the rebuilt engine is
// the release.
func TestDispatch_PDManualCanaryKeepsItsEngineWhenThePodIsDeletedAfterTheShift(t *testing.T) {
	p := newPDPodLoss(t, "pdlate")

	annotate(p.isvc, constants.RolloutPromoteAnnotation, "engnew")
	p.tick("promote")
	delete(p.isvc.Annotations, constants.RolloutPromoteAnnotation)
	p.tick("shift")
	cs := p.status()
	if cs.CurrentStep != 1 || p.phase() != v1beta1.RolloutPhasePromoting {
		t.Fatalf("the pass that moves traffic to 100%% keeps the final step, got %+v phase=%q", cs, p.phase())
	}
	p.expectCanaryOnly()

	p.startDeleting()
	p.tick("deletion seen")
	held := func(when string) {
		t.Helper()
		if cs.CurrentStep != 1 || p.phase() != v1beta1.RolloutPhasePromoting || cs.CapacityWaitSince == nil || cs.Failed != nil {
			t.Fatalf("%s: the drain must hold the final step on the lost capacity, got %+v phase=%q", when, cs, p.phase())
		}
		if e, d := p.partition(v1beta1.EngineComponent), p.partition(v1beta1.DecoderComponent); e != 1 || d != 1 {
			t.Fatalf("%s: the stable Instances must stay held, got partitions engine=%d decoder=%d", when, e, d)
		}
	}
	held("pod under deletion")

	p.finishDeleting()
	p.tick("pod gone")
	held("pod gone")

	// The 100% write landed before the loss, so the pass that counts the
	// rebuilt engine is the release: it clears the wait, writes the sentinel
	// and lets the held Instances roll.
	p.rebuild()
	p.tick("rebuilt")
	if cs.CurrentStep != 2 || p.phase() != v1beta1.RolloutPhasePromoting || cs.CapacityWaitSince != nil {
		t.Fatalf("the pass that counts the rebuilt engine releases the held Instances, got %+v phase=%q", cs, p.phase())
	}
	if e, d := p.partition(v1beta1.EngineComponent), p.partition(v1beta1.DecoderComponent); e != 0 || d != 0 {
		t.Fatalf("the sentinel releases the held Instances, got partitions engine=%d decoder=%d", e, d)
	}
	p.movePod("engine", "engold", "engnew", "0")
	p.movePod("decoder", "decold", "decnew", "0")
	p.tick("cut over")
	p.expectDone()
}
