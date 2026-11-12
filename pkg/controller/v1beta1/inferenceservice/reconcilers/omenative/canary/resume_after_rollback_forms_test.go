package canary

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// A resume after a completed rollback walks a referenced canary through the
// same transitions as its inline form, from the RolledBack hold to Stable:
// the hold clears, a fresh run opens over the rejected revision, the canary
// runs to its end and the run closes.

const canaryInstances = 4

// canaryTransition is one distinct state the unit passed through.
type canaryTransition struct {
	Run   bool
	State rollout.CanaryState
	Phase v1beta1.RolloutPhase
	Step  int32
}

// canaryWorld is one engine canary and the cluster around it: between
// controller passes the IR controller republishes the target once the
// rollback override is gone, and the rollout engine rolls the instances the
// projected partition releases, under a recorded run only, as the plan gate
// allows.
type canaryWorld struct {
	t         *testing.T
	ctx       context.Context
	c         client.Client
	isvc      *v1beta1.InferenceService
	policies  rollout.Policies
	now       time.Time
	instances []string
	promoted  bool
}

func (w *canaryWorld) revisionName(hash string) string {
	return w.isvc.Name + "-engine-" + hash
}

func newCanaryWorld(t *testing.T, f executorForm) *canaryWorld {
	t.Helper()
	n := canaryInstances
	isvc := f.isvc
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n}}
	isvc.Annotations = map[string]string{constants.RolloutResumeAnnotation: "new"}
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, &v1beta1.CanaryStatus{
		TargetID: "t0", CanaryRevisionHash: "new", StableRevisionHash: "old", RolledBackRevisionHash: "new",
	})
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseRolledBack)
	isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{
		Outcome: v1beta1.RolloutRunRolledBack,
		TargetRevisions: []v1beta1.RolloutRunTarget{
			{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
		},
	}}
	w := &canaryWorld{t: t, ctx: context.Background(), isvc: isvc, now: time.Unix(1000, 0)}

	// The revert completed: every instance is back on stable and the IR is
	// still overridden onto the stable revision, while its update revision
	// keeps reporting the spec's revision, as the replica controller does.
	stable := w.revisionName("old")
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: isvc.Namespace, Name: isvc.Name + "-engine", Generation: 1}}
	ir.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{RollbackToRevision: &stable}
	ir.Status.CurrentRevision, ir.Status.UpdateRevision = stable, w.revisionName("new")
	ir.Status.Replicas, ir.Status.UpdatedReplicas = canaryInstances, 0
	ir.Status.ObservedGeneration = 1

	objects := []client.Object{isvc, ir,
		canaryControllerRevision(isvc.Namespace, isvc.Name, "engine", "old", 1),
		canaryControllerRevision(isvc.Namespace, isvc.Name, "engine", "new", 2)}
	for i := 0; i < canaryInstances; i++ {
		w.instances = append(w.instances, "old")
		objects = append(objects, canaryPod(isvc.Namespace, isvc.Name, "engine", "old", fmt.Sprintf("old-%d", i)))
	}
	if f.policy != nil {
		objects = append(objects, f.policy)
	}
	w.c = fakeclient.New(t, fakeclient.WithStatusSubresource(&v1beta1.InferenceReplica{}), fakeclient.WithObjects(objects...))
	return w
}

// pass runs one controller pass the way the controller sequences it, then
// lets the cluster react, and reports the unit's state after the pass.
func (w *canaryWorld) pass() canaryTransition {
	t := w.t
	run, err := rolloutrun.Reconcile(w.ctx, rolloutrun.Inputs{Client: w.c, Reader: w.c, ISVC: w.isvc, Now: w.now, FeatureEnabled: true})
	if err != nil {
		t.Fatalf("run layer: %v", err)
	}
	w.policies = run.Policies
	groups := rollout.CanaryGroups(w.isvc, w.policies)
	if run.Opened {
		for _, g := range groups {
			if err := BindRun(w.ctx, w.c, w.isvc, g, RunOpen{Adopted: run.Adopted, Closed: run.Closed}); err != nil {
				t.Fatalf("bind: %v", err)
			}
		}
	}
	for _, g := range groups {
		syncInstanceRows(t, w.c, w.isvc, v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent)
		out, err := Dispatch(w.ctx, DispatchDeps{Client: w.c, Reader: w.c, ISVC: w.isvc, Now: w.now, ComponentRunnerPorts: canaryRunnerPorts(), Group: g})
		if err != nil {
			t.Fatalf("dispatch: %v", err)
		}
		if len(out.Consume) > 0 {
			if err := ConsumeAnnotations(w.ctx, w.c, w.isvc, out.Consume); err != nil {
				t.Fatal(err)
			}
			for _, key := range out.Consume {
				delete(w.isvc.Annotations, key)
			}
		}
	}
	cs := rollout.CanaryStatusFor(&w.isvc.Status, v1beta1.EngineComponent)
	tr := canaryTransition{Run: v1beta1.RolloutRunActive(w.isvc), State: rollout.StateOf(cs, phaseOf(w.isvc), len(twoStep())), Phase: phaseOf(w.isvc)}
	if cs != nil {
		tr.Step = cs.CurrentStep
	}
	// The operator promotes the manual step once it serves its split.
	if tr.State == rollout.CanaryStateServing && !w.promoted {
		annotate(w.isvc, constants.RolloutPromoteAnnotation, cs.CanaryRevisionHash)
		w.promoted = true
	}
	w.react()
	w.now = w.now.Add(10 * time.Second)
	return tr
}

// react is the cluster's response to the pass: the IR controller's counters
// and the pods the rollout engine moves. The pods move toward the rollback
// target while one is set; the update revision keeps reporting the spec's
// revision either way, as the replica controller publishes it.
func (w *canaryWorld) react() {
	t := w.t
	ir := &v1beta1.InferenceReplica{}
	if err := w.c.Get(w.ctx, client.ObjectKey{Namespace: w.isvc.Namespace, Name: w.isvc.Name + "-engine"}, ir); err != nil {
		t.Fatal(err)
	}
	target := "new"
	if ir.Spec.Pacing != nil && ir.Spec.Pacing.RollbackToRevision != nil {
		target = "old"
	}
	if v1beta1.RolloutRunActive(w.isvc) {
		held := int32(canaryInstances)
		if p, ok := EffectivePartition(w.isvc, w.policies, v1beta1.EngineComponent, canaryInstances); ok {
			held = *p
		}
		for i := int(held); i < canaryInstances; i++ {
			if w.instances[i] == target {
				continue
			}
			gone := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: w.isvc.Namespace, Name: fmt.Sprintf("%s-%d", w.instances[i], i)}}
			if err := w.c.Delete(w.ctx, gone); err != nil {
				t.Fatal(err)
			}
			if err := w.c.Create(w.ctx, canaryPod(w.isvc.Namespace, w.isvc.Name, "engine", target, fmt.Sprintf("%s-%d", target, i))); err != nil {
				t.Fatal(err)
			}
			w.instances[i] = target
		}
	}
	updated := int32(0)
	for _, rev := range w.instances {
		if rev == "new" {
			updated++
		}
	}
	ir.Status.UpdateRevision = w.revisionName("new")
	ir.Status.Replicas, ir.Status.UpdatedReplicas = canaryInstances, updated
	if updated == canaryInstances {
		ir.Status.CurrentRevision = w.revisionName("new")
	}
	ir.Status.ObservedGeneration = ir.Generation
	if err := w.c.Status().Update(w.ctx, ir); err != nil {
		t.Fatal(err)
	}
}

// walk runs passes until the unit is Stable with its run closed, or the
// budget is spent, and returns the distinct transitions in order.
func (w *canaryWorld) walk(passes int) []canaryTransition {
	var seen []canaryTransition
	for i := 0; i < passes; i++ {
		tr := w.pass()
		if len(seen) == 0 || seen[len(seen)-1] != tr {
			seen = append(seen, tr)
		}
		if tr.Phase == v1beta1.RolloutPhaseStable && !tr.Run {
			break
		}
	}
	return seen
}

func TestDispatch_ReferencedCanaryResumeAfterRollbackWalksTheInlineSteps(t *testing.T) {
	walks := map[string][]canaryTransition{}
	for _, f := range executorForms() {
		walks[f.name] = newCanaryWorld(t, f).walk(20)
	}
	if !reflect.DeepEqual(walks["inline"], walks["referenced"]) {
		t.Fatalf("the referenced canary did not walk the inline steps\ninline:     %+v\nreferenced: %+v", walks["inline"], walks["referenced"])
	}
	inline := walks["inline"]
	last := inline[len(inline)-1]
	if last.Phase != v1beta1.RolloutPhaseStable || last.Run || last.State != rollout.CanaryStateDone {
		t.Fatalf("the canary must end Stable with its run closed, got %+v", inline)
	}
	if inline[0].State == rollout.CanaryStateRolledBack {
		t.Fatalf("the first pass must clear the hold, got %+v", inline)
	}
}
