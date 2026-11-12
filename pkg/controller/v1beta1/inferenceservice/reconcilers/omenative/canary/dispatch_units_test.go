package canary

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// unitGroup is one canary group over one unit with the manual two-step plan.
func unitGroup(members ...v1beta1.ComponentType) v1beta1.RolloutGroup {
	return v1beta1.RolloutGroup{Components: members, Canary: &v1beta1.GroupCanary{Steps: twoStep()}}
}

// twoUnitService is an InferenceService with a router of two Instances and an
// engine of four, each its own canary unit. groups is the plan order.
func twoUnitService(ns, name string, groups ...v1beta1.RolloutGroup) *v1beta1.InferenceService {
	n2, n4 := 2, 4
	concurrent := v1beta1.RolloutGroupOrderingConcurrent
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: v1beta1.InferenceServiceSpec{
			Router:  &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}},
			Engine:  &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}},
			Rollout: &v1beta1.RolloutSpec{GroupOrdering: &concurrent, Groups: groups},
		},
	}
}

// unitRun is the controller's view of one pass over a two-unit service on the
// fake client: each replica publishes the rows for its pods, every canary
// group of the recorded plan dispatches in plan order over the one in-memory
// object, then the annotations the passes took are removed and the object
// re-read, as the status flush does.
type unitRun struct {
	t    *testing.T
	ctx  context.Context
	c    client.Client
	isvc *v1beta1.InferenceService
}

func (r *unitRun) pass(name string) {
	r.t.Helper()
	syncInstanceRows(r.t, r.c, r.isvc, v1beta1.RouterComponent, v1beta1.EngineComponent)
	var consume []string
	for _, g := range rollout.CanaryGroups(r.isvc, rollout.Policies{}) {
		out, err := Dispatch(r.ctx, DispatchDeps{Client: r.c, Reader: r.c, ISVC: r.isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: g})
		if err != nil {
			r.t.Fatalf("Dispatch %s (%s): %v", rollout.PrimaryOf(g), name, err)
		}
		consume = append(consume, out.Consume...)
	}
	flushed(r.t, r.c, r.isvc, &Result{Consume: consume})
}

// openRun records the live spec as a fresh run toward the given targets.
func (r *unitRun) openRun(targets ...v1beta1.RolloutRunTarget) {
	r.t.Helper()
	pinActiveRun(r.isvc)
	r.isvc.Status.Rollout.ActiveRun.TargetRevisions = targets
	for _, g := range rollout.CanaryGroups(r.isvc, rollout.Policies{}) {
		if err := BindRun(r.ctx, r.c, r.isvc, g, RunOpen{}); err != nil {
			r.t.Fatal(err)
		}
	}
}

func (r *unitRun) phase(c v1beta1.ComponentType) v1beta1.RolloutPhase {
	return r.isvc.Status.Components[c].RolloutPhase
}

func (r *unitRun) status(c v1beta1.ComponentType) *v1beta1.CanaryStatus {
	return rollout.CanaryStatusFor(&r.isvc.Status, c)
}

func (r *unitRun) requestStored() bool {
	r.t.Helper()
	_, ok := storedAnnotations(r.t, r.c, r.isvc)[constants.RolloutRollbackAnnotation]
	return ok
}

func (r *unitRun) irKey(c v1beta1.ComponentType) types.NamespacedName {
	return types.NamespacedName{Namespace: r.isvc.Namespace, Name: r.isvc.Name + "-" + string(c)}
}

func (r *unitRun) setUpdateRevision(c v1beta1.ComponentType, hash string) {
	r.t.Helper()
	updateIR(r.t, r.c, r.irKey(c), func(ir *v1beta1.InferenceReplica) {
		ir.Status.UpdateRevision = r.isvc.Name + "-" + string(c) + "-" + hash
	})
}

func (r *unitRun) addPods(c v1beta1.ComponentType, hash string, names ...string) {
	r.t.Helper()
	for _, name := range names {
		if err := r.c.Create(r.ctx, canaryPod(r.isvc.Namespace, r.isvc.Name, string(c), hash, name)); err != nil {
			r.t.Fatal(err)
		}
	}
}

func (r *unitRun) removePods(c v1beta1.ComponentType, hash string, names ...string) {
	r.t.Helper()
	for _, name := range names {
		if err := r.c.Delete(r.ctx, canaryPod(r.isvc.Namespace, r.isvc.Name, string(c), hash, name)); err != nil {
			r.t.Fatal(err)
		}
	}
}

// newTwoUnitRun seeds the fake client with the service, one InferenceReplica
// per unit resting on its stable revision, that revision's pods and
// ControllerRevision, and the router's canary revision as its recorded target.
func newTwoUnitRun(t *testing.T, isvc *v1beta1.InferenceService) *unitRun {
	t.Helper()
	ns := isvc.Namespace
	routerIR := ir(ns, isvc.Name, v1beta1.RouterComponent, "rnew")
	routerIR.Status.CurrentRevision = isvc.Name + "-router-rold"
	engineIR := ir(ns, isvc.Name, v1beta1.EngineComponent, "oldrev")
	engineIR.Status.CurrentRevision = isvc.Name + "-engine-oldrev"
	c := fakeclient.New(t, fakeclient.WithObjects(
		isvc, routerIR, engineIR,
		canaryPod(ns, isvc.Name, "router", "rold", "rold-0"),
		canaryPod(ns, isvc.Name, "router", "rnew", "rnew-1"),
		canaryPod(ns, isvc.Name, "engine", "oldrev", "oldrev-0"),
		canaryPod(ns, isvc.Name, "engine", "oldrev", "oldrev-1"),
		canaryPod(ns, isvc.Name, "engine", "oldrev", "oldrev-2"),
		canaryPod(ns, isvc.Name, "engine", "oldrev", "oldrev-3"),
		canaryControllerRevision(ns, isvc.Name, "router", "rold", 1),
		canaryControllerRevision(ns, isvc.Name, "router", "rnew", 2),
		canaryControllerRevision(ns, isvc.Name, "engine", "oldrev", 1),
	))
	return &unitRun{t: t, ctx: context.Background(), c: c, isvc: isvc}
}

// A rollback request is the rolled-back unit's own for as long as its hold
// lasts. A run that another unit opens later, over a plan that did not carry
// that unit while the request was applied, starts the new unit and walks its
// steps without ever rolling it back.
func TestDispatch_RequestHeldByOneUnitDoesNotRollBackAnother(t *testing.T) {
	isvc := twoUnitService("default", "two-units", unitGroup(v1beta1.RouterComponent))
	r := newTwoUnitRun(t, isvc)
	router, engine := v1beta1.RouterComponent, v1beta1.EngineComponent

	r.openRun(v1beta1.RolloutRunTarget{Component: router, Revision: "rnew", StableRevision: "rold"})
	r.pass("start")
	if cs := r.status(router); cs == nil || cs.CanaryRevisionHash != "rnew" || r.phase(router) != v1beta1.RolloutPhasePaused {
		t.Fatalf("the router canary must serve its first step, got %+v phase=%q", cs, r.phase(router))
	}

	annotateStored(t, r.c, isvc, constants.RolloutRollbackAnnotation, "true")
	r.pass("rollback")
	if cs := r.status(router); cs.RolledBackRevisionHash != "rnew" || r.phase(router) != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the request must reject the router canary, got %+v phase=%q", cs, r.phase(router))
	}
	r.removePods(router, "rnew", "rnew-1")
	r.setUpdateRevision(router, "rold")
	r.pass("drain")
	if r.phase(router) != v1beta1.RolloutPhaseRolledBack {
		t.Fatalf("the hold must complete once the rejected pod is gone, got %q", r.phase(router))
	}
	if !r.requestStored() {
		t.Fatal("the request must stay on the object through the hold")
	}

	// The run closes on the hold; the hold and its request outlive it.
	isvc.Status.Rollout.ActiveRun = nil
	r.pass("closed")
	if cs := r.status(router); cs.RolledBackRevisionHash != "rnew" || r.phase(router) != v1beta1.RolloutPhaseRolledBack {
		t.Fatalf("the hold must survive the run's close, got %+v phase=%q", cs, r.phase(router))
	}
	if !r.requestStored() {
		t.Fatal("the request must stay on the object after the run closes")
	}

	// The engine gets a plan of its own, first in plan order, and a new
	// target: the run opened for it keeps the held router at its stable
	// revision, and the engine's first pass under it comes before the
	// router's.
	isvc.Spec.Rollout.Groups = append([]v1beta1.RolloutGroup{unitGroup(engine)}, isvc.Spec.Rollout.Groups...)
	r.setUpdateRevision(engine, "newrev")
	if err := r.c.Create(r.ctx, canaryControllerRevision(isvc.Namespace, isvc.Name, "engine", "newrev", 2)); err != nil {
		t.Fatal(err)
	}
	r.openRun(
		v1beta1.RolloutRunTarget{Component: engine, Revision: "newrev", StableRevision: "oldrev"},
		v1beta1.RolloutRunTarget{Component: router, Revision: "rold", StableRevision: "rold"},
	)
	r.pass("engine opens")
	if cs := r.status(engine); cs == nil || cs.RolledBackRevisionHash != "" || cs.CanaryRevisionHash != "newrev" || cs.CurrentStep != 0 || r.phase(engine) != v1beta1.RolloutPhasePending {
		t.Fatalf("the router's request rolled the engine back on its first pass: %+v phase=%q", cs, r.phase(engine))
	}

	// The engine walks its steps to completion; no pass rolls it back.
	r.addPods(engine, "newrev", "newrev-2", "newrev-3")
	r.pass("engine serves")
	if cs := r.status(engine); cs.RolledBackRevisionHash != "" || r.phase(engine) != v1beta1.RolloutPhasePaused {
		t.Fatalf("the engine must serve its first step, got %+v phase=%q", cs, r.phase(engine))
	}
	annotateStored(t, r.c, isvc, constants.RolloutPromoteAnnotation, "newrev")
	r.pass("engine promoted")
	r.removePods(engine, "oldrev", "oldrev-0", "oldrev-1", "oldrev-2", "oldrev-3")
	r.addPods(engine, "newrev", "newrev-0", "newrev-1")
	updateIR(t, r.c, r.irKey(engine), func(ir *v1beta1.InferenceReplica) { ir.Status.CurrentRevision = isvc.Name + "-engine-newrev" })
	r.pass("engine cuts over")
	r.pass("engine done")
	if cs := r.status(engine); cs.RolledBackRevisionHash != "" || cs.CurrentStep != 2 || r.phase(engine) != v1beta1.RolloutPhaseStable {
		t.Fatalf("the engine must complete its steps, got %+v phase=%q", cs, r.phase(engine))
	}
}

// Within one pass every unit of the plan dispatches over one object. A unit
// with no canary must leave a request another unit's canary can apply, in
// whichever order the plan lists them, and must leave the request the unit
// that applied it holds: the request stays through the hold, and a run opened
// later for the idle unit starts it without rolling it back.
func TestDispatch_IdleUnitLeavesTheRequestForTheLiveUnit(t *testing.T) {
	router, engine := v1beta1.RouterComponent, v1beta1.EngineComponent
	for name, groups := range map[string][]v1beta1.RolloutGroup{
		"live unit first": {unitGroup(router), unitGroup(engine)},
		"idle unit first": {unitGroup(engine), unitGroup(router)},
	} {
		t.Run(name, func(t *testing.T) {
			isvc := twoUnitService("default", "two-units", groups...)
			r := newTwoUnitRun(t, isvc)
			r.openRun(
				v1beta1.RolloutRunTarget{Component: router, Revision: "rnew", StableRevision: "rold"},
				v1beta1.RolloutRunTarget{Component: engine, Revision: "oldrev", StableRevision: "oldrev"},
			)
			r.pass("start")
			if cs := r.status(router); cs == nil || cs.CanaryRevisionHash != "rnew" || r.phase(router) != v1beta1.RolloutPhasePaused {
				t.Fatalf("the router canary must serve its first step, got %+v phase=%q", cs, r.phase(router))
			}
			if cs := r.status(engine); cs != nil {
				t.Fatalf("the engine sits the run out and must carry no record, got %+v", cs)
			}

			annotateStored(t, r.c, isvc, constants.RolloutRollbackAnnotation, "true")
			r.pass("rollback")
			if cs := r.status(router); cs == nil || cs.RolledBackRevisionHash != "rnew" || r.phase(router) != v1beta1.RolloutPhaseRollingBack {
				t.Fatalf("the idle engine took the request before the router could apply it: %+v phase=%q", cs, r.phase(router))
			}
			if !r.requestStored() {
				t.Fatal("the request must stay on the object through the router's hold")
			}
			r.removePods(router, "rnew", "rnew-1")
			r.setUpdateRevision(router, "rold")
			r.pass("drain")
			r.pass("held")
			if r.phase(router) != v1beta1.RolloutPhaseRolledBack {
				t.Fatalf("the hold must complete once the rejected pod is gone, got %q", r.phase(router))
			}
			if !r.requestStored() {
				t.Fatal("the idle engine must leave the request the router holds")
			}
			if cs := r.status(engine); cs != nil {
				t.Fatalf("the engine must still carry no record, got %+v", cs)
			}

			// A new target for the engine: the run opened for it starts the
			// engine, and the router's request does not roll it back.
			r.setUpdateRevision(engine, "newrev")
			if err := r.c.Create(r.ctx, canaryControllerRevision(isvc.Namespace, isvc.Name, "engine", "newrev", 2)); err != nil {
				t.Fatal(err)
			}
			r.openRun(
				v1beta1.RolloutRunTarget{Component: router, Revision: "rold", StableRevision: "rold"},
				v1beta1.RolloutRunTarget{Component: engine, Revision: "newrev", StableRevision: "oldrev"},
			)
			r.pass("engine opens")
			if cs := r.status(engine); cs == nil || cs.RolledBackRevisionHash != "" || cs.CanaryRevisionHash != "newrev" || r.phase(engine) != v1beta1.RolloutPhasePending {
				t.Fatalf("the router's request rolled the engine back on its first pass: %+v phase=%q", cs, r.phase(engine))
			}
		})
	}
}

// A push to one unit closes the run and a fresh run opens over the
// whole service. The other unit is recorded at the target it already holds,
// so the bind keeps its record, its step and its split; only the pushed unit
// starts over.
func TestBindRunKeepsAUnitRecordedAtTheTargetItHolds(t *testing.T) {
	router, engine := v1beta1.RouterComponent, v1beta1.EngineComponent
	isvc := twoUnitService("default", "two-units", unitGroup(router), unitGroup(engine))
	r := newTwoUnitRun(t, isvc)
	r.setUpdateRevision(engine, "newrev")
	if err := r.c.Create(r.ctx, canaryControllerRevision(isvc.Namespace, isvc.Name, "engine", "newrev", 2)); err != nil {
		t.Fatal(err)
	}
	r.openRun(
		v1beta1.RolloutRunTarget{Component: router, Revision: "rnew", StableRevision: "rold"},
		v1beta1.RolloutRunTarget{Component: engine, Revision: "newrev", StableRevision: "oldrev"},
	)
	r.pass("start")
	r.addPods(engine, "newrev", "newrev-2", "newrev-3")
	r.pass("the engine's first step serves")
	if r.phase(engine) != v1beta1.RolloutPhasePaused {
		t.Fatalf("the engine must serve its first step, got phase %q", r.phase(engine))
	}
	annotateStored(t, r.c, isvc, constants.RolloutPromoteAnnotation, "newrev")
	r.pass("the engine is promoted to its final step")
	before := r.status(engine).DeepCopy()
	if before.CurrentStep != 1 || before.PromotedThrough != "newrev" {
		t.Fatalf("the engine must stand at its final step with the promote recorded, got %+v", before)
	}

	// The router is pushed again: the run layer closes the run and opens
	// a fresh one with the engine's targets unchanged.
	r.setUpdateRevision(router, "rnewer")
	if err := r.c.Create(r.ctx, canaryControllerRevision(isvc.Namespace, isvc.Name, "router", "rnewer", 3)); err != nil {
		t.Fatal(err)
	}
	r.openRun(
		v1beta1.RolloutRunTarget{Component: router, Revision: "rnewer", StableRevision: "rold"},
		v1beta1.RolloutRunTarget{Component: engine, Revision: "newrev", StableRevision: "oldrev"},
	)
	after := r.status(engine)
	if after.CurrentStep != before.CurrentStep || after.TargetID != before.TargetID || after.PromotedThrough != before.PromotedThrough || !after.StepEnteredTime.Equal(before.StepEnteredTime) {
		t.Fatalf("the router's push must leave the engine's record where it stands: before %+v, after %+v", before, after)
	}
	if r.phase(engine) != v1beta1.RolloutPhasePending {
		t.Fatalf("the engine keeps its final step's phase, got %q", r.phase(engine))
	}
	if cs := r.status(router); cs.CanaryRevisionHash != "rnewer" || cs.CurrentStep != 0 {
		t.Fatalf("the router unit must start over toward rnewer: %+v", cs)
	}
}
