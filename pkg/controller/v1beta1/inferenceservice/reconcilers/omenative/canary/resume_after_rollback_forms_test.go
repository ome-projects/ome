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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun"
	"sigs.k8s.io/ome/pkg/rollout"
)

// A resume after a completed rollback walks a referenced canary through the
// same transitions as its inline twin, from the RolledBack hold to Stable:
// the hold clears, a fresh run opens over the rejected revision, the ladder
// runs to its end and the run closes.

const ladderInstances = 4

// ladderTransition is one distinct state the unit passed through.
type ladderTransition struct {
	Run   bool
	State rollout.CanaryState
	Phase v1beta1.RolloutPhase
	Step  int32
}

// ladderWorld is one engine canary and the cluster around it: between
// controller passes the IR controller republishes the target once the
// rollback override is gone, and the rollout engine rolls the instances the
// projected partition releases, under a pinned run only, as the plan gate
// allows.
type ladderWorld struct {
	t         *testing.T
	ctx       context.Context
	c         client.Client
	isvc      *v1beta1.InferenceService
	policies  rollout.Policies
	now       time.Time
	instances []string
	promoted  bool
}

func (w *ladderWorld) revisionName(hash string) string {
	return w.isvc.Name + "-engine-" + hash
}

func newLadderWorld(t *testing.T, f executorForm) *ladderWorld {
	t.Helper()
	n := ladderInstances
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
	w := &ladderWorld{t: t, ctx: context.Background(), isvc: isvc, now: time.Unix(1000, 0)}

	// The revert completed: every instance is back on stable and the IR is
	// still overridden onto the stable revision.
	stable := w.revisionName("old")
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: isvc.Namespace, Name: isvc.Name + "-engine", Generation: 1}}
	ir.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{RollbackToRevision: &stable}
	ir.Status.CurrentRevision, ir.Status.UpdateRevision = stable, stable
	ir.Status.Replicas, ir.Status.UpdatedReplicas = ladderInstances, ladderInstances
	ir.Status.ObservedGeneration = 1

	builder := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithRuntimeObjects(isvc, ir,
			canaryControllerRevision(isvc.Namespace, isvc.Name, "engine", "old", 1),
			canaryControllerRevision(isvc.Namespace, isvc.Name, "engine", "new", 2))
	for i := 0; i < ladderInstances; i++ {
		w.instances = append(w.instances, "old")
		builder.WithRuntimeObjects(canaryPod(isvc.Namespace, isvc.Name, "engine", "old", fmt.Sprintf("old-%d", i)))
	}
	if f.policy != nil {
		builder.WithRuntimeObjects(f.policy)
	}
	w.c = builder.Build()
	return w
}

// pass runs one controller pass the way the controller sequences it, then
// lets the cluster react, and reports the unit's state after the pass.
func (w *ladderWorld) pass() ladderTransition {
	t := w.t
	run, err := rolloutrun.Reconcile(w.ctx, rolloutrun.Inputs{Client: w.c, Reader: w.c, ISVC: w.isvc, Now: w.now, FeatureEnabled: true})
	if err != nil {
		t.Fatalf("run layer: %v", err)
	}
	w.policies = run.Policies
	groups := rollout.CanaryGroups(w.isvc, w.policies)
	if run.Opened {
		for _, g := range groups {
			if err := BindRun(w.ctx, w.c, w.isvc, g, run.Adopted); err != nil {
				t.Fatalf("bind: %v", err)
			}
		}
	}
	for _, g := range groups {
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
	tr := ladderTransition{Run: v1beta1.RolloutRunActive(w.isvc), State: rollout.StateOf(cs, phaseOf(w.isvc), len(twoStep())), Phase: phaseOf(w.isvc)}
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

// react is the cluster's response to the pass: the IR controller's target and
// counters, and the pods the rollout engine moves.
func (w *ladderWorld) react() {
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
		held := int32(ladderInstances)
		if p, ok := EffectivePartition(w.isvc, w.policies, v1beta1.EngineComponent, ladderInstances); ok {
			held = *p
		}
		for i := int(held); i < ladderInstances; i++ {
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
		if rev == target {
			updated++
		}
	}
	ir.Status.UpdateRevision = w.revisionName(target)
	ir.Status.Replicas, ir.Status.UpdatedReplicas = ladderInstances, updated
	if updated == ladderInstances {
		ir.Status.CurrentRevision = w.revisionName(target)
	}
	ir.Status.ObservedGeneration = ir.Generation
	if err := w.c.Status().Update(w.ctx, ir); err != nil {
		t.Fatal(err)
	}
}

// walk runs passes until the unit is Stable with its run closed, or the
// budget is spent, and returns the distinct transitions in order.
func (w *ladderWorld) walk(passes int) []ladderTransition {
	var seen []ladderTransition
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

func TestDispatch_ReferencedCanaryResumeAfterRollbackWalksTheInlineLadder(t *testing.T) {
	walks := map[string][]ladderTransition{}
	for _, f := range executorForms() {
		walks[f.name] = newLadderWorld(t, f).walk(20)
	}
	if !reflect.DeepEqual(walks["inline"], walks["referenced"]) {
		t.Fatalf("the referenced canary did not walk the inline ladder\ninline:     %+v\nreferenced: %+v", walks["inline"], walks["referenced"])
	}
	inline := walks["inline"]
	last := inline[len(inline)-1]
	if last.Phase != v1beta1.RolloutPhaseStable || last.Run || last.State != rollout.CanaryStateDone {
		t.Fatalf("the ladder must end Stable with its run closed, got %+v", inline)
	}
	if inline[0].State == rollout.CanaryStateRolledBack {
		t.Fatalf("the first pass must clear the hold, got %+v", inline)
	}
}
