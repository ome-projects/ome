package canary

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
)

// Every member of a canary group publishes the ladder's weights on its own
// revisions: the primary's percents at each step, one 100% target on its
// promoted revision at completion and on its stable revision after a
// rollback, and one 100% target on its current revision when the bump left
// it untouched. Routing pairs engine and decoder targets by equal pairing
// protocol values, so a member with no targets has nothing to pair.

const (
	pdStableProtocol = "pair-v1"
	pdCanaryProtocol = "pair-v2"
)

// trafficWant is one expected target: its share, its pairing protocol and
// whether it is flagged as the incoming revision.
type trafficWant struct {
	percent  int32
	protocol string
	latest   bool
}

// expectTargets checks one Component's targets, keyed by revision hash.
func expectTargets(t *testing.T, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, want map[string]trafficWant) {
	t.Helper()
	got := isvc.Status.Components[comp].Traffic
	if len(got) != len(want) {
		t.Fatalf("%s traffic = %+v, want targets on %v", comp, got, want)
	}
	for _, target := range got {
		hash := query.RevisionFromName(target.RevisionName).Hash()
		w, ok := want[hash]
		if !ok {
			t.Fatalf("%s publishes a target on %q, want only %v: %+v", comp, hash, want, got)
		}
		if target.Percent != w.percent || target.PairingProtocol != w.protocol || target.LatestRevision != w.latest {
			t.Fatalf("%s target %s = %d%% protocol %q latest %v, want %d%% protocol %q latest %v",
				comp, hash, target.Percent, target.PairingProtocol, target.LatestRevision, w.percent, w.protocol, w.latest)
		}
	}
}

// pairedRevision is a ControllerRevision minted under a pairing protocol.
func pairedRevision(ns, isvc, comp, hash string, rev int64, protocol string) *appsv1.ControllerRevision {
	cr := canaryControllerRevision(ns, isvc, comp, hash, rev)
	cr.Annotations = map[string]string{query.LabelPairingProtocol: protocol}
	return cr
}

// pdCanary is an [engine, decoder] canary group, engine primary, two
// replicas per Component, on the first step of the manual two-step ladder:
// one ready pod per revision per Component, the run pinned toward engnew/decnew
// over the stable engold/decold, every revision minted under a pairing protocol.
// An unbumped decoder keeps decold as both its target and its stable revision.
type pdCanary struct {
	t    *testing.T
	ns   string
	name string
	isvc *v1beta1.InferenceService
	c    client.Client
	deps DispatchDeps
}

func newPDCanary(t *testing.T, name string, decoderBumped bool) *pdCanary {
	t.Helper()
	ns := "default"
	n2 := 2
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Canary:     &v1beta1.GroupCanary{Steps: twoStep()},
	}}}
	decoderTarget := "decnew"
	if !decoderBumped {
		decoderTarget = "decold"
	}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "engnew", StableRevision: "engold"},
		{Component: v1beta1.DecoderComponent, Revision: decoderTarget, StableRevision: "decold"},
	}
	engineIR := ir(ns, name, v1beta1.EngineComponent, "engnew")
	engineIR.Status.CurrentRevision = name + "-engine-engold"
	decoderIR := ir(ns, name, v1beta1.DecoderComponent, decoderTarget)
	decoderIR.Status.CurrentRevision = name + "-decoder-decold"
	objs := []runtime.Object{isvc, engineIR, decoderIR,
		canaryPod(ns, name, "engine", "engold", name+"-engine-0"),
		canaryPod(ns, name, "engine", "engnew", name+"-engine-1"),
		canaryPod(ns, name, "decoder", "decold", name+"-decoder-0"),
		canaryPod(ns, name, "decoder", decoderTarget, name+"-decoder-1"),
		pairedRevision(ns, name, "engine", "engold", 1, pdStableProtocol),
		pairedRevision(ns, name, "engine", "engnew", 2, pdCanaryProtocol),
		pairedRevision(ns, name, "decoder", "decold", 1, pdStableProtocol),
	}
	if decoderBumped {
		objs = append(objs, pairedRevision(ns, name, "decoder", "decnew", 2, pdCanaryProtocol))
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
	return &pdCanary{t: t, ns: ns, name: name, isvc: isvc, c: c, deps: DispatchDeps{
		Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc),
	}}
}

func (f *pdCanary) dispatch(pass string) {
	f.t.Helper()
	if _, err := Dispatch(context.Background(), f.deps); err != nil {
		f.t.Fatalf("Dispatch (%s): %v", pass, err)
	}
}

// movePod re-creates one instance's pod on another revision.
func (f *pdCanary) movePod(comp, from, to string, index string) {
	f.t.Helper()
	ctx := context.Background()
	name := f.name + "-" + comp + "-" + index
	if err := f.c.Delete(ctx, canaryPod(f.ns, f.name, comp, from, name)); err != nil {
		f.t.Fatal(err)
	}
	if err := f.c.Create(ctx, canaryPod(f.ns, f.name, comp, to, name)); err != nil {
		f.t.Fatal(err)
	}
}

// retarget republishes one Component's IR target revision.
func (f *pdCanary) retarget(comp v1beta1.ComponentType, hash string) {
	f.t.Helper()
	key := types.NamespacedName{Namespace: f.ns, Name: f.name + "-" + string(comp)}
	updateIR(f.t, f.c, key, func(ir *v1beta1.InferenceReplica) {
		ir.Status.UpdateRevision = f.name + "-" + string(comp) + "-" + hash
	})
}

func (f *pdCanary) phase() v1beta1.RolloutPhase {
	return f.isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase
}

func (f *pdCanary) expectTraffic(comp v1beta1.ComponentType, want map[string]trafficWant) {
	f.t.Helper()
	expectTargets(f.t, f.isvc, comp, want)
}

func TestDispatch_SecondaryPublishesTheStepWeights(t *testing.T) {
	f := newPDCanary(t, "pdstep", true)
	f.dispatch("serve")
	if f.phase() != v1beta1.RolloutPhasePaused {
		t.Fatalf("the split must serve at the manual step, got %q", f.phase())
	}
	f.expectTraffic(v1beta1.EngineComponent, map[string]trafficWant{
		"engnew": {percent: 50, protocol: pdCanaryProtocol, latest: true},
		"engold": {percent: 50, protocol: pdStableProtocol},
	})
	f.expectTraffic(v1beta1.DecoderComponent, map[string]trafficWant{
		"decnew": {percent: 50, protocol: pdCanaryProtocol, latest: true},
		"decold": {percent: 50, protocol: pdStableProtocol},
	})
}

func TestDispatch_SecondaryConvergesWithThePromotion(t *testing.T) {
	f := newPDCanary(t, "pddone", true)
	f.dispatch("serve")
	annotate(f.isvc, constants.RolloutPromoteAnnotation, "engnew")
	f.dispatch("advance")
	if f.phase() != v1beta1.RolloutPhasePending {
		t.Fatalf("the final step stages its capacity first, got %q", f.phase())
	}
	// The final step's capacity: every instance of every member on its target.
	f.movePod("engine", "engold", "engnew", "0")
	f.movePod("decoder", "decold", "decnew", "0")
	f.dispatch("cut over")
	cs := rollout.CanaryStatusFor(&f.isvc.Status, v1beta1.EngineComponent)
	if cs == nil || cs.CurrentStep != 2 || f.phase() != v1beta1.RolloutPhaseStable {
		t.Fatalf("the ladder must complete at 100%%, got %+v phase=%q", cs, f.phase())
	}
	done := func() {
		t.Helper()
		f.expectTraffic(v1beta1.EngineComponent, map[string]trafficWant{
			"engnew": {percent: 100, protocol: pdCanaryProtocol, latest: true},
		})
		f.expectTraffic(v1beta1.DecoderComponent, map[string]trafficWant{
			"decnew": {percent: 100, protocol: pdCanaryProtocol, latest: true},
		})
	}
	done()
	// The done sentinel keeps every member's converged target.
	f.dispatch("done")
	done()
}

func TestDispatch_SecondaryFollowsTheRollback(t *testing.T) {
	f := newPDCanary(t, "pdback", true)
	f.dispatch("serve")
	annotateStored(t, f.c, f.isvc, constants.RolloutRollbackAnnotation, "true")
	f.dispatch("roll back")
	if f.phase() != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the rejected pods still exist, got %q", f.phase())
	}
	stable := func() {
		t.Helper()
		f.expectTraffic(v1beta1.EngineComponent, map[string]trafficWant{
			"engold": {percent: 100, protocol: pdStableProtocol},
		})
		f.expectTraffic(v1beta1.DecoderComponent, map[string]trafficWant{
			"decold": {percent: 100, protocol: pdStableProtocol},
		})
	}
	stable()
	// The rejected pods drain and each IR republishes its stable revision.
	f.movePod("engine", "engnew", "engold", "1")
	f.movePod("decoder", "decnew", "decold", "1")
	f.retarget(v1beta1.EngineComponent, "engold")
	f.retarget(v1beta1.DecoderComponent, "decold")
	f.dispatch("drained")
	if f.phase() != v1beta1.RolloutPhaseRolledBack {
		t.Fatalf("every member is back on stable, got %q", f.phase())
	}
	stable()
}

func TestDispatch_UnbumpedSecondaryKeepsOneTarget(t *testing.T) {
	f := newPDCanary(t, "pdpart", false)
	f.dispatch("serve")
	if f.phase() != v1beta1.RolloutPhasePaused {
		t.Fatalf("the split must serve at the manual step, got %q", f.phase())
	}
	f.expectTraffic(v1beta1.EngineComponent, map[string]trafficWant{
		"engnew": {percent: 50, protocol: pdCanaryProtocol, latest: true},
		"engold": {percent: 50, protocol: pdStableProtocol},
	})
	f.expectTraffic(v1beta1.DecoderComponent, map[string]trafficWant{
		"decold": {percent: 100, protocol: pdStableProtocol, latest: true},
	})
}

// pdInputs drives the executor alone over a two-member unit: the engine
// primary's inputs and the decoder's revision pair.
func pdInputs(pods map[string]int32, decoder MemberRevisions) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	in := baseInputs(isvc, pods)
	in.CanaryPairingProtocol, in.StablePairingProtocol = pdCanaryProtocol, pdStableProtocol
	in.Secondaries = map[v1beta1.ComponentType]MemberRevisions{v1beta1.DecoderComponent: decoder}
	return isvc, in
}

func bumpedDecoder() MemberRevisions {
	return MemberRevisions{
		CanaryRevisionHash: "decnew", StableRevisionHash: "decold",
		CanaryPairingProtocol: pdCanaryProtocol, StablePairingProtocol: pdStableProtocol,
	}
}

func TestReconcile_SecondaryTrafficFollowsTheLadder(t *testing.T) {
	isvc, in := pdInputs(map[string]int32{"new": 2, "old": 2}, bumpedDecoder())
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{
		"new": {percent: 50, protocol: pdCanaryProtocol, latest: true},
		"old": {percent: 50, protocol: pdStableProtocol},
	})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{
		"decnew": {percent: 50, protocol: pdCanaryProtocol, latest: true},
		"decold": {percent: 50, protocol: pdStableProtocol},
	})

	// The promote advances to the final step, which stages before it shifts.
	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	mustReconcile(t, isvc, in)
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("the final step waits for its capacity, got %q", phaseOf(isvc))
	}
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{
		"decnew": {percent: 50, protocol: pdCanaryProtocol, latest: true},
		"decold": {percent: 50, protocol: pdStableProtocol},
	})

	in.PerRevisionPods = map[string]int32{"new": 4}
	if res := mustReconcile(t, isvc, in); !res.Complete {
		t.Fatalf("the ungated final step completes at 100%%, got %+v", res)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{
		"new": {percent: 100, protocol: pdCanaryProtocol, latest: true},
	})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{
		"decnew": {percent: 100, protocol: pdCanaryProtocol, latest: true},
	})
}

func TestReconcile_SecondaryTrafficReturnsToStableOnRollback(t *testing.T) {
	isvc, in := pdInputs(map[string]int32{"new": 2, "old": 2}, bumpedDecoder())
	mustReconcile(t, isvc, in)
	annotate(isvc, constants.RolloutRollbackAnnotation, "true")
	if res := mustReconcile(t, isvc, in); !res.RolledBack {
		t.Fatalf("the request rolls the unit back, got %+v", res)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{
		"old": {percent: 100, protocol: pdStableProtocol},
	})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{
		"decold": {percent: 100, protocol: pdStableProtocol},
	})
}

// A member with no stable revision has nothing to shift traffic from and is
// sent no revert: it serves its target alone through the ladder and through
// a rollback.
func TestReconcile_SecondaryWithoutStableServesItsTarget(t *testing.T) {
	isvc, in := pdInputs(map[string]int32{"new": 2, "old": 2},
		MemberRevisions{CanaryRevisionHash: "decnew", CanaryPairingProtocol: pdCanaryProtocol})
	mustReconcile(t, isvc, in)
	only := map[string]trafficWant{"decnew": {percent: 100, protocol: pdCanaryProtocol, latest: true}}
	expectTargets(t, isvc, v1beta1.DecoderComponent, only)
	annotate(isvc, constants.RolloutRollbackAnnotation, "true")
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.DecoderComponent, only)
}

func TestReconcile_UnbumpedSecondaryKeepsItsRevision(t *testing.T) {
	isvc, in := pdInputs(map[string]int32{"new": 2, "old": 2}, MemberRevisions{
		CanaryRevisionHash: "decold", StableRevisionHash: "decold",
		CanaryPairingProtocol: pdStableProtocol, StablePairingProtocol: pdStableProtocol,
	})
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{
		"new": {percent: 50, protocol: pdCanaryProtocol, latest: true},
		"old": {percent: 50, protocol: pdStableProtocol},
	})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{
		"decold": {percent: 100, protocol: pdStableProtocol, latest: true},
	})
}

func TestMemberWeights(t *testing.T) {
	bumped := bumpedDecoder()
	if w := memberWeights(bumped, 30); len(w) != 2 ||
		w[0].RevisionHash != "decnew" || w[0].Percent != 30 || !w[0].LatestRevision || w[0].PairingProtocol != pdCanaryProtocol ||
		w[1].RevisionHash != "decold" || w[1].Percent != 70 || w[1].LatestRevision || w[1].PairingProtocol != pdStableProtocol {
		t.Fatalf("a bumped member splits like the primary, got %+v", w)
	}
	if w := memberWeights(MemberRevisions{CanaryRevisionHash: "d", StableRevisionHash: "d", CanaryPairingProtocol: "p"}, 30); len(w) != 1 ||
		w[0].RevisionHash != "d" || w[0].Percent != 100 || !w[0].LatestRevision || w[0].PairingProtocol != "p" {
		t.Fatalf("an unbumped member serves its revision alone, got %+v", w)
	}
	if w := memberWeights(MemberRevisions{CanaryRevisionHash: "decnew"}, 30); len(w) != 1 || w[0].RevisionHash != "decnew" || w[0].Percent != 100 {
		t.Fatalf("a member with no stable revision serves its target alone, got %+v", w)
	}
	if w := memberWeights(MemberRevisions{StableRevisionHash: "decold"}, 30); w != nil {
		t.Fatalf("a member with no target has no weights, got %+v", w)
	}
	if w := memberStableWeights(bumped); len(w) != 1 || w[0].RevisionHash != "decold" || w[0].Percent != 100 || w[0].LatestRevision || w[0].PairingProtocol != pdStableProtocol {
		t.Fatalf("a rolled-back member returns to its stable revision, got %+v", w)
	}
	if w := memberStableWeights(MemberRevisions{CanaryRevisionHash: "decnew"}); w != nil {
		t.Fatalf("a member with no stable revision has nothing to return to, got %+v", w)
	}
}
