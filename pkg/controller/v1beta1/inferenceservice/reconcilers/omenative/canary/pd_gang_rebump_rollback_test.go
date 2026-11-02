package canary

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// pdGangUnit is a prefill/decode unit behind a router: three engine gangs
// and three decoder gangs of a leader and a worker each, two router pods.
// The router is the unit's primary and is never pushed, so the unit's
// record keeps the router's one revision as both its canary and its stable
// hash; the engine and the decoder carry the pushed revisions.
type pdGangUnit struct {
	t    *testing.T
	ctx  context.Context
	ns   string
	isvc *v1beta1.InferenceService
	c    client.Client
	deps DispatchDeps
}

const (
	pdGangRouterRev = "rtr"
	pdGangStartRev  = "v1"
)

func newPDGangUnit(t *testing.T, name string) *pdGangUnit {
	t.Helper()
	ns := "default"
	n2, n3 := 2, 3
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n3}}
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n3}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent},
		Canary:     &v1beta1.GroupCanary{Steps: twoStep()},
	}}}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = pdGangTargets("v2")

	routerIR := ir(ns, name, v1beta1.RouterComponent, pdGangRouterRev)
	routerIR.Status.CurrentRevision = name + "-router-" + pdGangRouterRev
	gangIR := func(comp v1beta1.ComponentType) *v1beta1.InferenceReplica {
		r := ir(ns, name, comp, "v2")
		r.Status.CurrentRevision = name + "-" + string(comp) + "-" + pdGangStartRev
		r.Spec.Runners = []v1beta1.Runner{
			{Name: v1beta1.RunnerNameLeader, Size: 1},
			{Name: v1beta1.RunnerNameWorker, Size: 1},
		}
		return r
	}
	objects := []client.Object{isvc, routerIR, gangIR(v1beta1.EngineComponent), gangIR(v1beta1.DecoderComponent),
		canaryControllerRevision(ns, name, "router", pdGangRouterRev, 1),
	}
	for _, comp := range []string{"engine", "decoder"} {
		objects = append(objects,
			canaryControllerRevision(ns, name, comp, pdGangStartRev, 1),
			canaryControllerRevision(ns, name, comp, "v2", 2),
		)
	}
	u := &pdGangUnit{t: t, ctx: context.Background(), ns: ns, isvc: isvc}
	for i := 0; i < 2; i++ {
		objects = append(objects, u.routerPod(i))
	}
	// The first step stages two of three gangs on the pushed revision; the
	// third gang still runs the starting revision.
	for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		objects = append(objects, u.gang(comp, pdGangStartRev, 0, true)...)
		objects = append(objects, u.gang(comp, "v2", 1, true)...)
		objects = append(objects, u.gang(comp, "v2", 3, true)...)
	}
	u.c = fakeclient.New(t, fakeclient.WithObjects(objects...))
	u.deps = DispatchDeps{Client: u.c, Reader: u.c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	return u
}

// pdGangTargets is the run's pinned pair per member: the router stays on
// its one revision, the engine and the decoder are pushed from the starting
// revision to rev.
func pdGangTargets(rev string) []v1beta1.RolloutRunTarget {
	return []v1beta1.RolloutRunTarget{
		{Component: v1beta1.RouterComponent, Revision: pdGangRouterRev, StableRevision: pdGangRouterRev},
		{Component: v1beta1.EngineComponent, Revision: rev, StableRevision: pdGangStartRev},
		{Component: v1beta1.DecoderComponent, Revision: rev, StableRevision: pdGangStartRev},
	}
}

func (u *pdGangUnit) routerPod(index int) *corev1.Pod {
	name := fmt.Sprintf("%s-router-%d", u.isvc.Name, index)
	pod := canaryPod(u.ns, u.isvc.Name, "router", pdGangRouterRev, name)
	pod.Labels[query.LabelInstanceIdx] = strconv.Itoa(index)
	return pod
}

// gang is one Instance's leader and worker on a revision, Ready or not.
func (u *pdGangUnit) gang(comp v1beta1.ComponentType, rev string, index int, ready bool) []client.Object {
	var pods []client.Object
	for _, runner := range []v1beta1.RunnerName{v1beta1.RunnerNameLeader, v1beta1.RunnerNameWorker} {
		name := fmt.Sprintf("%s-%s-%d-%s-0", u.isvc.Name, comp, index, runner)
		pod := canaryRunnerPod(u.ns, u.isvc.Name, string(comp), rev, name, runner, "0")
		pod.Labels[query.LabelInstanceIdx] = strconv.Itoa(index)
		if !ready {
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
		}
		pods = append(pods, pod)
	}
	return pods
}

func (u *pdGangUnit) createGang(comp v1beta1.ComponentType, rev string, index int, ready bool) {
	u.t.Helper()
	for _, obj := range u.gang(comp, rev, index, ready) {
		if err := u.c.Create(u.ctx, obj); err != nil {
			u.t.Fatal(err)
		}
	}
}

func (u *pdGangUnit) deleteGang(comp v1beta1.ComponentType, rev string, index int) {
	u.t.Helper()
	for _, obj := range u.gang(comp, rev, index, true) {
		if err := u.c.Delete(u.ctx, obj); err != nil {
			u.t.Fatal(err)
		}
	}
}

func (u *pdGangUnit) dispatch(pass string) Outcome {
	u.t.Helper()
	syncInstanceRows(u.t, u.c, u.isvc, v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent)
	out, err := Dispatch(u.ctx, u.deps)
	if err != nil {
		u.t.Fatalf("Dispatch (%s): %v", pass, err)
	}
	return out
}

func (u *pdGangUnit) status() *v1beta1.CanaryStatus {
	return rollout.CanaryStatusFor(&u.isvc.Status, v1beta1.RouterComponent)
}

func (u *pdGangUnit) phase() v1beta1.RolloutPhase {
	return u.isvc.Status.Components[v1beta1.RouterComponent].RolloutPhase
}

// rollbackTarget is the revert signal one member's replica carries.
func (u *pdGangUnit) rollbackTarget(comp v1beta1.ComponentType) string {
	u.t.Helper()
	got := &v1beta1.InferenceReplica{}
	if err := u.c.Get(u.ctx, types.NamespacedName{Namespace: u.ns, Name: u.isvc.Name + "-" + string(comp)}, got); err != nil {
		u.t.Fatal(err)
	}
	if got.Spec.Pacing == nil || got.Spec.Pacing.RollbackToRevision == nil {
		return ""
	}
	return *got.Spec.Pacing.RollbackToRevision
}

// push re-targets the unit the way the run layer and the controller do: the
// active run is superseded by a fresh one toward rev that carries the
// starting revision as the stable of every pushed member, each pushed
// member's replica reports the new target, and the bind resets the unit
// before the executor's pass.
func (u *pdGangUnit) push(rev string, crRevision int64) {
	u.t.Helper()
	reopenRun(u.isvc, pdGangTargets(rev)...)
	u.deps.Group = rollout.CanaryGroup(u.isvc, rollout.Policies{})
	for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		key := types.NamespacedName{Namespace: u.ns, Name: u.isvc.Name + "-" + string(comp)}
		updateIR(u.t, u.c, key, func(ir *v1beta1.InferenceReplica) {
			ir.Status.UpdateRevision = u.isvc.Name + "-" + string(comp) + "-" + rev
		})
		if err := u.c.Create(u.ctx, canaryControllerRevision(u.ns, u.isvc.Name, string(comp), rev, crRevision)); err != nil {
			u.t.Fatal(err)
		}
	}
	if err := BindRun(u.ctx, u.c, u.isvc, u.deps.Group, false); err != nil {
		u.t.Fatal(err)
	}
}

// A prefill/decode unit of gangs is pushed to a second version and holds at
// its first step; the operator pushes a third and then a fourth version
// while nothing newer serves, then asks for a rollback while the fourth
// version is still forming. The rollback points every pushed member at the
// starting version, not at the second or the third one, drains every gang
// off every pushed version, records the rolled-back revision and settles
// RolledBack.
func TestDispatch_PDGangUnitPushedAgainAtTheFirstStepThenRolledBack(t *testing.T) {
	u := newPDGangUnit(t, "pd-gangs")

	u.dispatch("serve")
	if u.phase() != v1beta1.RolloutPhasePaused || u.status().CurrentStep != 0 {
		t.Fatalf("two gangs per member on the pushed revision serve the first step under the manual pause, got phase=%q status=%+v", u.phase(), u.status())
	}

	// The third version lands while the first step holds. The second
	// version's gangs still stand, and a replacement forms toward the third
	// version at the index the roll freed.
	u.push("v3", 3)
	for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		u.createGang(comp, "v3", 2, false)
	}
	u.dispatch("retargeted")
	if cs := u.status(); cs.CurrentStep != 0 || cs.RolledBackRevisionHash != "" || u.phase() != v1beta1.RolloutPhasePending {
		t.Fatalf("the third version re-arms the unit at step 0 with no capacity yet, got phase=%q status=%+v", u.phase(), cs)
	}
	for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		if got := u.rollbackTarget(comp); got != "" {
			t.Fatalf("%s carries a revert signal before any rollback: %q", comp, got)
		}
	}

	// The fourth version lands while the third is still forming: the
	// workload withdraws the third version's replacement and a new one forms
	// toward the fourth version at the same index.
	u.push("v4", 4)
	for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		u.deleteGang(comp, "v3", 2)
		u.createGang(comp, "v4", 2, false)
	}
	u.dispatch("retargeted again")
	if cs := u.status(); cs.CurrentStep != 0 || cs.RolledBackRevisionHash != "" || u.phase() != v1beta1.RolloutPhasePending {
		t.Fatalf("the fourth version re-arms the unit at step 0 with no capacity yet, got phase=%q status=%+v", u.phase(), cs)
	}

	annotateStored(t, u.c, u.isvc, constants.RolloutRollbackAnnotation, "true")
	u.dispatch("rollback")
	if cs := u.status(); cs.RolledBackRevisionHash == "" || u.phase() != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the rollback records the rejection and drains the pushed versions, got phase=%q status=%+v", u.phase(), cs)
	}
	for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		want := u.isvc.Name + "-" + string(comp) + "-" + pdGangStartRev
		if got := u.rollbackTarget(comp); got != want {
			t.Fatalf("%s must be pointed at the starting version %q after three pushes, got %q", comp, want, got)
		}
	}
	if got := u.rollbackTarget(v1beta1.RouterComponent); got != u.isvc.Name+"-router-"+pdGangRouterRev {
		t.Fatalf("the router, never pushed, is pointed at its own revision, got %q", got)
	}

	// The workload withdraws the forming fourth-version gangs and rolls the
	// second-version gangs back; while any gang is off the starting version
	// the revert is still in flight.
	for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		u.deleteGang(comp, "v4", 2)
		u.deleteGang(comp, "v2", 3)
		u.createGang(comp, pdGangStartRev, 2, true)
	}
	u.dispatch("partly reverted")
	if u.phase() != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("a gang still on the second version keeps the revert in flight, got %q", u.phase())
	}
	for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		u.deleteGang(comp, "v2", 1)
		u.createGang(comp, pdGangStartRev, 1, true)
	}
	u.dispatch("reverted")
	if u.phase() != v1beta1.RolloutPhaseRolledBack {
		t.Fatalf("every gang back on the starting version settles the unit RolledBack, got %q status=%+v", u.phase(), u.status())
	}
	if cs := u.status(); cs.RolledBackRevisionHash == "" {
		t.Fatalf("the rolled-back revision must stay recorded, got %+v", cs)
	}
	for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		got := query.RevisionFromName(u.isvc.Status.Components[comp].LatestRolledoutRevision).Hash()
		if got != pdGangStartRev {
			t.Fatalf("%s must record the starting version as rolled out after the revert, got %q", comp, got)
		}
	}
	// The hold stands on the next pass: nothing re-arms toward a rejected
	// version and the request stays spent.
	u.dispatch("held")
	if cs := u.status(); u.phase() != v1beta1.RolloutPhaseRolledBack || cs.RolledBackRevisionHash == "" {
		t.Fatalf("the settled hold must stand, got phase=%q status=%+v", u.phase(), cs)
	}
}
