package rollout

import (
	"encoding/json"
	"sort"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

// A canary declared inline and the same canary supplied through a policy
// reference are one plan to every reader of the effective view, whether no
// run has opened, a run is open, or the last run closed.

// viewForm is one declaration of the same canary body: the inline group, or
// a group whose only progression is a reference to policy.
type viewForm struct {
	name   string
	isvc   *v1beta1.InferenceService
	policy *v1beta1.RolloutPolicy
}

func viewSteps() []v1beta1.RolloutGroupStep {
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
}

func viewForms() []viewForm {
	inline := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "ns"},
		Spec: v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			Canary:     &v1beta1.GroupCanary{Steps: viewSteps()},
		}}}},
	}
	policy := &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "canary-policy", Namespace: "ns", Generation: 3},
		Spec:       v1beta1.RolloutPolicySpec{Canary: &v1beta1.GroupCanary{Steps: viewSteps()}},
	}
	referenced := inline.DeepCopy()
	referenced.Spec.Rollout.Groups[0].Canary = nil
	referenced.Spec.Rollout.Groups[0].PolicyRef = &v1beta1.RolloutPolicyRef{Name: policy.Name, Progression: v1beta1.RolloutProgressionCanary}
	return []viewForm{{name: "inline", isvc: inline}, {name: "referenced", isvc: referenced, policy: policy}}
}

// composedGroup is the group a run pins for the form: the inline body, or
// the referenced policy's body composed onto the group.
func composedGroup(t *testing.T, f viewForm) v1beta1.RolloutGroup {
	t.Helper()
	var spec *v1beta1.RolloutPolicySpec
	if f.policy != nil {
		spec = &f.policy.Spec
	}
	composed, err := rolloutpolicy.ComposeGroup(&f.isvc.Spec.Rollout.Groups[0], spec)
	if err != nil {
		t.Fatal(err)
	}
	return composed
}

func servingStatus() *v1beta1.CanaryStatus {
	return &v1beta1.CanaryStatus{TargetID: "t1", CanaryRevisionHash: "new", StableRevisionHash: "old", ObservedTrafficWeight: 50}
}

// viewPhases shape the ISVC into each phase of a run's life.
var viewPhases = []struct {
	name  string
	apply func(t *testing.T, f viewForm)
}{
	{"no run", func(t *testing.T, f viewForm) {
		SetCanaryStatusFor(&f.isvc.Status, v1beta1.EngineComponent, servingStatus())
	}},
	{"open run", func(t *testing.T, f viewForm) {
		SetCanaryStatusFor(&f.isvc.Status, v1beta1.EngineComponent, servingStatus())
		source := v1beta1.RolloutPlanSourceInline
		if f.policy != nil {
			source = v1beta1.RolloutPlanSourcePolicy
		}
		f.isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
			RunID: "run-1",
			TargetRevisions: []v1beta1.RolloutRunTarget{
				{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
			},
			Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{Source: source, Group: composedGroup(t, f)}}},
		}}
	}},
	{"closed after rollback", func(t *testing.T, f viewForm) {
		cs := servingStatus()
		cs.RolledBackRevisionHash = "new"
		SetCanaryStatusFor(&f.isvc.Status, v1beta1.EngineComponent, cs)
		f.isvc.Status.Components[v1beta1.EngineComponent] = v1beta1.ComponentStatusSpec{
			RolloutPhase: v1beta1.RolloutPhaseRolledBack, Canary: cs,
		}
		f.isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{
			Outcome: v1beta1.RolloutRunRolledBack,
			TargetRevisions: []v1beta1.RolloutRunTarget{
				{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
			},
		}}
	}},
}

// viewReading is everything the executors read off the effective view.
type viewReading struct {
	Groups    int
	GroupFor  *v1beta1.RolloutGroup
	First     *v1beta1.RolloutGroup
	Status    *v1beta1.CanaryStatus
	Effective *v1beta1.RolloutSpec
	Owned     []string
}

func readView(t *testing.T, isvc *v1beta1.InferenceService, policies Policies) string {
	t.Helper()
	var owned []string
	for c := range CanaryOwnedComponents(isvc) {
		owned = append(owned, string(c))
	}
	sort.Strings(owned)
	raw, err := json.MarshalIndent(viewReading{
		Groups:    len(CanaryGroups(isvc, policies)),
		GroupFor:  CanaryGroupFor(isvc, policies, v1beta1.EngineComponent),
		First:     CanaryGroup(isvc, policies),
		Status:    GroupCanaryStatusFor(isvc, policies, v1beta1.EngineComponent),
		Effective: Effective(isvc, policies),
		Owned:     owned,
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestEffectiveView_ReferencedCanaryReadsLikeInline(t *testing.T) {
	for _, phase := range viewPhases {
		t.Run(phase.name, func(t *testing.T) {
			readings := map[string]string{}
			for _, f := range viewForms() {
				phase.apply(t, f)
				readings[f.name] = readView(t, f.isvc, PoliciesOf(f.isvc.Namespace, f.policy))
			}
			if readings["inline"] != readings["referenced"] {
				t.Fatalf("the referenced canary reads differently from its inline twin\ninline:\n%s\nreferenced:\n%s", readings["inline"], readings["referenced"])
			}
			// The comparison is not vacuous: the inline form carries the ladder.
			if g := CanaryGroupFor(viewForms()[0].isvc, Policies{}, v1beta1.EngineComponent); g == nil || g.Canary == nil {
				t.Fatal("the inline twin must read as a canary group")
			}
		})
	}
}
