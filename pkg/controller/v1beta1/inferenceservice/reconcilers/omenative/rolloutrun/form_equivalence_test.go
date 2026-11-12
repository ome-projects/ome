package rolloutrun

import (
	"context"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// The run layer decides the same thing for a canary declared inline and for
// the same canary supplied through a policy reference, in every phase of a
// run's life and under every operator verb. Provenance (source, policy
// identity, digest) is the one thing that legitimately differs, so it is
// left out of the comparison.

// runForm is one declaration of the same canary plus the objects a
// pass must find in the cluster for it.
type runForm struct {
	name    string
	isvc    *v1beta1.InferenceService
	objects []runtime.Object
}

func runForms() []runForm {
	inline := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary:     canaryBody(50, 100),
	})
	policy := &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "canary-policy", Namespace: "ns", Generation: 2},
		Spec:       v1beta1.RolloutPolicySpec{Canary: canaryBody(50, 100)},
	}
	referenced := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		PolicyRef:  &v1beta1.RolloutPolicyRef{Name: policy.Name, Progression: v1beta1.RolloutProgressionCanary},
	})
	return []runForm{{name: "inline", isvc: inline}, {name: "referenced", isvc: referenced, objects: []runtime.Object{policy}}}
}

// holdingIR is the IR a completed rollback leaves: the rejected revision is
// still its spec target while every instance is back on stable.
func holdingIR() *v1beta1.InferenceReplica {
	ir := irFixture(oldRev, newRev)
	ir.Status.Replicas = 2
	ir.Status.UpdatedReplicas = 0
	return ir
}

func rolledBackRecord(t *testing.T, isvc *v1beta1.InferenceService, rejection string, phase v1beta1.RolloutPhase) {
	t.Helper()
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			RolloutPhase: phase,
			Canary: &v1beta1.CanaryStatus{
				CanaryRevisionHash:     hashOf(t, newRev),
				StableRevisionHash:     hashOf(t, oldRev),
				RolledBackRevisionHash: rejection,
			},
		},
	}
	isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{
		Outcome: v1beta1.RolloutRunRolledBack,
		TargetRevisions: []v1beta1.RolloutRunTarget{
			{Component: v1beta1.EngineComponent, Revision: hashOf(t, newRev), StableRevision: hashOf(t, oldRev)},
		},
	}}
}

// runPhases shape a form into one phase of a run's life. Each returns the
// inputs for the pass under test, after any pass needed to reach the phase.
var runPhases = []struct {
	name string
	// runAfter is whether the pass under test leaves a run recorded.
	runAfter bool
	build    func(t *testing.T, f runForm) Inputs
}{
	{"no run, target diverged", true, func(t *testing.T, f runForm) Inputs {
		return testInputs(t, f.isvc, append([]runtime.Object{holdingIR()}, f.objects...)...)
	}},
	{"open run, nothing moved", true, func(t *testing.T, f runForm) Inputs {
		in := testInputs(t, f.isvc, append([]runtime.Object{irFixture(oldRev, newRev)}, f.objects...)...)
		if _, err := Reconcile(context.Background(), in); err != nil {
			t.Fatal(err)
		}
		return in
	}},
	{"open run, target moved", true, func(t *testing.T, f runForm) Inputs {
		ir := irFixture(oldRev, newRev)
		in := testInputs(t, f.isvc, append([]runtime.Object{ir}, f.objects...)...)
		if _, err := Reconcile(context.Background(), in); err != nil {
			t.Fatal(err)
		}
		live := &v1beta1.InferenceReplica{}
		if err := in.Client.Get(context.Background(), client.ObjectKeyFromObject(ir), live); err != nil {
			t.Fatal(err)
		}
		live.Status.UpdateRevision = "llm-a-engine-cccccccc"
		if err := in.Client.Update(context.Background(), live); err != nil {
			t.Fatal(err)
		}
		return in
	}},
	{"closed after rollback, hold standing", false, func(t *testing.T, f runForm) Inputs {
		rolledBackRecord(t, f.isvc, hashOf(t, newRev), v1beta1.RolloutPhaseRolledBack)
		return testInputs(t, f.isvc, append([]runtime.Object{holdingIR()}, f.objects...)...)
	}},
	// The executor applied a resume: the hold is cleared, nothing else moved.
	{"closed after rollback, hold cleared", true, func(t *testing.T, f runForm) Inputs {
		rolledBackRecord(t, f.isvc, "", v1beta1.RolloutPhasePending)
		return testInputs(t, f.isvc, append([]runtime.Object{holdingIR()}, f.objects...)...)
	}},
}

var runVerbs = []struct {
	name  string
	apply func(isvc *v1beta1.InferenceService)
}{
	{"read", func(*v1beta1.InferenceService) {}},
	{"promote", func(isvc *v1beta1.InferenceService) {
		annotateRun(isvc, constants.RolloutPromoteAnnotation, "bbbbbbbb")
	}},
	{"pause", func(isvc *v1beta1.InferenceService) { annotateRun(isvc, constants.PausedRolloutAnnotation, "true") }},
	{"rollback", func(isvc *v1beta1.InferenceService) { annotateRun(isvc, constants.RolloutRollbackAnnotation, "true") }},
	{"resume", func(isvc *v1beta1.InferenceService) { annotateRun(isvc, constants.RolloutResumeAnnotation, "bbbbbbbb") }},
}

func annotateRun(isvc *v1beta1.InferenceService, key, value string) {
	if isvc.Annotations == nil {
		isvc.Annotations = map[string]string{}
	}
	isvc.Annotations[key] = value
}

// runDecision is what one run-layer pass decided and left behind, without
// the provenance fields.
type runDecision struct {
	Outcome        Outcome
	RunActive      bool
	RecordedCanary *v1beta1.GroupCanary
	RecordedGroups []v1beta1.ComponentType
	Targets        []v1beta1.RolloutRunTarget
	LastOutcome    v1beta1.RolloutRunOutcome
	PlanReady      string
	Drift          string
}

func decideRun(t *testing.T, in Inputs) (runDecision, string) {
	t.Helper()
	out, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	isvc := in.ISVC
	// The observed policies are the pass's reading, not a decision; the two
	// forms observe different sets by construction. The run a retarget
	// closed is a reading too: its group bodies are compared, its provenance
	// and its id differ by form.
	out.Policies = rollout.Policies{}
	out.Closed = closedGroupBodies(out.Closed)
	d := runDecision{Outcome: out, RunActive: v1beta1.RolloutRunActive(isvc)}
	if isvc.Status.Rollout != nil {
		if active := isvc.Status.Rollout.ActiveRun; active != nil {
			d.Targets = active.TargetRevisions
			for i := range active.Plan.Groups {
				g := &active.Plan.Groups[i].Group
				d.RecordedGroups = append(d.RecordedGroups, g.Components...)
				if g.Canary != nil {
					d.RecordedCanary = g.Canary
				}
				if g.PolicyRef != nil {
					t.Fatalf("a recorded group must never carry a reference: %+v", g)
				}
			}
		}
		if last := isvc.Status.Rollout.LastRun; last != nil {
			d.LastOutcome = last.Outcome
		}
	}
	d.PlanReady = conditionLabel(isvc.Status.GetCondition(apis.ConditionType(v1beta1.RolloutPlanReadyCondition)))
	d.Drift = conditionLabel(isvc.Status.GetCondition(apis.ConditionType(v1beta1.RolloutPlanDriftCondition)))
	raw, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return d, string(raw)
}

// closedGroupBodies keeps only the group bodies of a closed run.
func closedGroupBodies(closed *v1beta1.RolloutRun) *v1beta1.RolloutRun {
	if closed == nil {
		return nil
	}
	bodies := &v1beta1.RolloutRun{}
	for i := range closed.Plan.Groups {
		bodies.Plan.Groups = append(bodies.Plan.Groups, v1beta1.RolloutRunGroup{Group: closed.Plan.Groups[i].Group})
	}
	return bodies
}

func conditionLabel(c *apis.Condition) string {
	if c == nil {
		return ""
	}
	return string(c.Status) + "/" + c.Reason
}

func TestReconcile_ReferencedCanaryRunDecidesLikeInline(t *testing.T) {
	for _, phase := range runPhases {
		for _, verb := range runVerbs {
			t.Run(phase.name+"/"+verb.name, func(t *testing.T) {
				decisions := map[string]runDecision{}
				readings := map[string]string{}
				for _, f := range runForms() {
					in := phase.build(t, f)
					verb.apply(f.isvc)
					decisions[f.name], readings[f.name] = decideRun(t, in)
				}
				if readings["inline"] != readings["referenced"] {
					t.Fatalf("the referenced canary's run decided differently from its inline form\ninline:\n%s\nreferenced:\n%s", readings["inline"], readings["referenced"])
				}
				// The comparison is not vacuous: the inline form's plan is ready,
				// the phase leaves the run it should, and an open run records the canary.
				inline := decisions["inline"]
				if inline.PlanReady == "" || inline.PlanReady == string(corev1.ConditionFalse)+"/"+v1beta1.RolloutPlanReasonPolicyNotFound {
					t.Fatalf("the inline form's plan must resolve, got %q", inline.PlanReady)
				}
				if inline.RunActive != phase.runAfter {
					t.Fatalf("the inline form must leave run recorded=%v, got %+v", phase.runAfter, inline)
				}
				if inline.RunActive && inline.RecordedCanary == nil {
					t.Fatalf("an open run must record the inline canary, got %+v", inline)
				}
			})
		}
	}
}
