package canary

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

// The executor decides the same thing for a canary declared inline and for
// the same canary supplied through a policy reference, in every phase of a
// run's life and under every operator verb: it reads a resolved body, so
// nothing it decides can depend on the form the operator declared.

// executorForm is one declaration of the same canary ladder.
type executorForm struct {
	name   string
	isvc   *v1beta1.InferenceService
	policy *v1beta1.RolloutPolicy
}

func executorForms() []executorForm {
	inline := canaryISVC(twoStep(), nil)
	inline.Namespace = "ns"
	policy := &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "canary-policy", Namespace: "ns"},
		Spec:       v1beta1.RolloutPolicySpec{Canary: &v1beta1.GroupCanary{Steps: twoStep()}},
	}
	referenced := inline.DeepCopy()
	referenced.Spec.Rollout.Groups[0].Canary = nil
	referenced.Spec.Rollout.Groups[0].PolicyRef = &v1beta1.RolloutPolicyRef{Name: policy.Name, Progression: v1beta1.RolloutProgressionCanary}
	return []executorForm{{name: "inline", isvc: inline}, {name: "referenced", isvc: referenced, policy: policy}}
}

// pinComposedRun pins the form's resolved group as the active run, the way
// the run layer does: a referenced body is composed onto the group first.
func pinComposedRun(t *testing.T, f executorForm, targets ...v1beta1.RolloutRunTarget) *v1beta1.RolloutGroup {
	t.Helper()
	var spec *v1beta1.RolloutPolicySpec
	source := v1beta1.RolloutPlanSourceInline
	if f.policy != nil {
		spec = &f.policy.Spec
		source = v1beta1.RolloutPlanSourcePolicy
	}
	composed, err := rolloutpolicy.ComposeGroup(&f.isvc.Spec.Rollout.Groups[0], spec)
	if err != nil {
		t.Fatal(err)
	}
	f.isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
		RunID:           "run-1",
		OpenedAt:        metav1.Time{Time: time.Unix(900, 0)},
		TargetRevisions: targets,
		Plan:            v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{Source: source, Group: composed}}},
	}}
	return &f.isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group
}

func servingFirstSplit() *v1beta1.CanaryStatus {
	return &v1beta1.CanaryStatus{TargetID: "t1", CanaryRevisionHash: "new", StableRevisionHash: "old", ObservedTrafficWeight: 50}
}

// executorPhases shape a form into one phase of a run's life and set the
// inputs the controller would derive there.
var executorPhases = []struct {
	name  string
	apply func(t *testing.T, f executorForm, in *ReconcileInputs)
}{
	// A ladder serving its first split with no run pinned: the state a lost
	// run leaves behind.
	{"no run", func(t *testing.T, f executorForm, in *ReconcileInputs) {
		rollout.SetCanaryStatusFor(&f.isvc.Status, v1beta1.EngineComponent, servingFirstSplit())
		setPhase(f.isvc, v1beta1.EngineComponent, v1beta1.RolloutPhasePaused)
		in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
		in.RunActive = false
	}},
	{"open run", func(t *testing.T, f executorForm, in *ReconcileInputs) {
		rollout.SetCanaryStatusFor(&f.isvc.Status, v1beta1.EngineComponent, servingFirstSplit())
		setPhase(f.isvc, v1beta1.EngineComponent, v1beta1.RolloutPhasePaused)
		g := pinComposedRun(t, f, v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"})
		in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
		in.RunActive = true
		in.TargetID = activeCanaryTargetID(f.isvc, g)
	}},
	// The rollback completed and closed its run; the IR reports the stable
	// revision as the target until the hold is cleared.
	{"closed after rollback", func(t *testing.T, f executorForm, in *ReconcileInputs) {
		cs := servingFirstSplit()
		cs.ObservedTrafficWeight = 0
		cs.RolledBackRevisionHash = "new"
		rollout.SetCanaryStatusFor(&f.isvc.Status, v1beta1.EngineComponent, cs)
		setPhase(f.isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseRolledBack)
		f.isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{
			Outcome: v1beta1.RolloutRunRolledBack,
			TargetRevisions: []v1beta1.RolloutRunTarget{
				{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
			},
		}}
		in.PerRevisionPods = map[string]int32{"old": 4}
		in.RunActive = false
		in.CanaryRevisionHash = "old"
	}},
}

var executorVerbs = []struct {
	name  string
	apply func(isvc *v1beta1.InferenceService)
}{
	{"read", func(*v1beta1.InferenceService) {}},
	{"promote", func(isvc *v1beta1.InferenceService) { annotate(isvc, constants.RolloutPromoteAnnotation, "new") }},
	{"pause", func(isvc *v1beta1.InferenceService) { annotate(isvc, constants.PausedRolloutAnnotation, "true") }},
	{"rollback", func(isvc *v1beta1.InferenceService) { annotate(isvc, constants.RolloutRollbackAnnotation, "true") }},
	{"resume", func(isvc *v1beta1.InferenceService) { annotate(isvc, constants.RolloutResumeAnnotation, "new") }},
}

// executorDecision is what one pass decided and left behind.
type executorDecision struct {
	Result      Result
	State       rollout.CanaryState
	Component   v1beta1.ComponentStatusSpec
	Annotations map[string]string
}

func decide(t *testing.T, isvc *v1beta1.InferenceService, in ReconcileInputs) (executorDecision, string) {
	t.Helper()
	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	d := executorDecision{
		Result:      *res,
		State:       rollout.StateOf(rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent), phaseOf(isvc), len(twoStep())),
		Component:   isvc.Status.Components[v1beta1.EngineComponent],
		Annotations: isvc.Annotations,
	}
	raw, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return d, string(raw)
}

func TestReconcile_ReferencedCanaryDecidesLikeInline(t *testing.T) {
	for _, phase := range executorPhases {
		for _, verb := range executorVerbs {
			t.Run(phase.name+"/"+verb.name, func(t *testing.T) {
				decisions := map[string]executorDecision{}
				readings := map[string]string{}
				for _, f := range executorForms() {
					in := baseInputs(f.isvc, nil)
					phase.apply(t, f, &in)
					verb.apply(f.isvc)
					// The group is resolved through the effective view, the
					// same view the dispatcher selects groups from.
					in.Group = nil
					in.Policies = rollout.PoliciesOf(f.isvc.Namespace, f.policy)
					decisions[f.name], readings[f.name] = decide(t, f.isvc, in)
				}
				if readings["inline"] != readings["referenced"] {
					t.Fatalf("the referenced canary decided differently from its inline twin\ninline:\n%s\nreferenced:\n%s", readings["inline"], readings["referenced"])
				}
				// The comparison is not vacuous: the inline twin runs its ladder.
				if !decisions["inline"].Result.Active {
					t.Fatalf("the inline twin must drive its ladder, got %+v", decisions["inline"].Result)
				}
			})
		}
	}
}

// A resume after a completed rollback clears the hold and consumes the verb
// on both forms; the inline form's behaviour is the reference.
func TestReconcile_ReferencedCanaryResumeAfterRollbackClearsTheHold(t *testing.T) {
	closed := executorPhases[len(executorPhases)-1]
	for _, f := range executorForms() {
		t.Run(f.name, func(t *testing.T) {
			in := baseInputs(f.isvc, nil)
			closed.apply(t, f, &in)
			annotate(f.isvc, constants.RolloutResumeAnnotation, "new")
			in.Group = nil
			in.Policies = rollout.PoliciesOf(f.isvc.Namespace, f.policy)
			d, _ := decide(t, f.isvc, in)
			if phaseOf(f.isvc) != v1beta1.RolloutPhasePending || d.Component.Canary == nil || d.Component.Canary.RolledBackRevisionHash != "" {
				t.Fatalf("resume must clear the RolledBack hold, got phase=%q canary=%+v", phaseOf(f.isvc), d.Component.Canary)
			}
			if len(d.Result.Consume) != 1 || d.Result.Consume[0] != constants.RolloutResumeAnnotation {
				t.Fatalf("resume must be consumed, got %+v", d.Result.Consume)
			}
		})
	}
}

// projection is what the controller projects onto the InferenceReplica for
// a Component of the group.
type projection struct {
	Effective *int32
	Active    bool
	Step      *int32
	PlanGate  *int32
}

func TestProjection_ReferencedCanaryProjectsLikeInline(t *testing.T) {
	n := 4
	ext := &v1beta1.ComponentExtensionSpec{MinReplicas: &n, MaxReplicas: 4}
	for _, phase := range executorPhases {
		t.Run(phase.name, func(t *testing.T) {
			readings := map[string]string{}
			var inline projection
			for _, f := range executorForms() {
				in := baseInputs(f.isvc, nil)
				phase.apply(t, f, &in)
				policies := rollout.PoliciesOf(f.isvc.Namespace, f.policy)
				p := projection{
					Step:     StepPartition(f.isvc, policies, v1beta1.EngineComponent, ext),
					PlanGate: PlanGateHoldPartition(f.isvc, policies, v1beta1.EngineComponent, ext),
				}
				p.Effective, p.Active = EffectivePartition(f.isvc, policies, v1beta1.EngineComponent, int32(n))
				raw, err := json.Marshal(p)
				if err != nil {
					t.Fatal(err)
				}
				readings[f.name] = string(raw)
				if f.name == "inline" {
					inline = p
				}
			}
			if readings["inline"] != readings["referenced"] {
				t.Fatalf("the referenced canary projects differently from its inline twin\ninline:     %s\nreferenced: %s", readings["inline"], readings["referenced"])
			}
			if !inline.Active || inline.Step == nil {
				t.Fatalf("the inline twin must project a step partition, got %s", readings["inline"])
			}
		})
	}
}
