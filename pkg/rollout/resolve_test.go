package rollout

import (
	"errors"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func canaryPolicy(name string, generation int64) *v1beta1.RolloutPolicy {
	return &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Generation: generation},
		Spec:       v1beta1.RolloutPolicySpec{Canary: &v1beta1.GroupCanary{Steps: viewSteps()}},
	}
}

func refGroup(name string, kind v1beta1.RolloutProgressionKind) v1beta1.RolloutGroup {
	return v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		PolicyRef:  &v1beta1.RolloutPolicyRef{Name: name, Progression: kind},
	}
}

func TestResolveGroup(t *testing.T) {
	policy := canaryPolicy("canary-policy", 7)
	observed := PoliciesOf("ns", policy)
	invalid := PoliciesOf("ns", policy)
	invalid.Invalid = map[string]error{policy.Name: errors.New("steps must end at 100% traffic")}

	cases := []struct {
		name     string
		group    v1beta1.RolloutGroup
		policies Policies
		reason   string
		source   v1beta1.RolloutPlanSource
		gen      int64
		canary   bool
	}{
		{name: "inline canary", group: v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: &v1beta1.GroupCanary{Steps: viewSteps()}},
			policies: Policies{}, source: v1beta1.RolloutPlanSourceInline, canary: true},
		{name: "inline outranks a reference", group: func() v1beta1.RolloutGroup {
			g := refGroup(policy.Name, v1beta1.RolloutProgressionCanary)
			g.BlueGreen = &v1beta1.GroupBlueGreen{}
			return g
		}(), policies: observed, source: v1beta1.RolloutPlanSourceInline},
		{name: "no progression defaults to blueGreen", group: v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}},
			policies: Policies{}, source: v1beta1.RolloutPlanSourceInline},
		{name: "referenced canary", group: refGroup(policy.Name, v1beta1.RolloutProgressionCanary),
			policies: observed, source: v1beta1.RolloutPlanSourcePolicy, gen: 7, canary: true},
		{name: "policy surface not installed", group: refGroup(policy.Name, v1beta1.RolloutProgressionCanary),
			policies: Policies{Namespace: "ns"}, reason: v1beta1.RolloutPlanReasonPlanInvalid},
		{name: "policy missing", group: refGroup("absent", v1beta1.RolloutProgressionCanary),
			policies: observed, reason: v1beta1.RolloutPlanReasonPolicyNotFound},
		{name: "policy body invalid", group: refGroup(policy.Name, v1beta1.RolloutProgressionCanary),
			policies: invalid, reason: v1beta1.RolloutPlanReasonPolicyNotReady},
		{name: "declared kind mismatch", group: refGroup(policy.Name, v1beta1.RolloutProgressionBlueGreen),
			policies: observed, reason: v1beta1.RolloutPlanReasonProgressionMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := ResolveGroup(0, &tc.group, tc.policies)
			if tc.reason != "" {
				var unresolved *Unresolved
				if !errors.As(err, &unresolved) || unresolved.Reason != tc.reason {
					t.Fatalf("want an unresolved reference parked under %s, got %v", tc.reason, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if res.Source != tc.source || res.PolicyGeneration != tc.gen || (res.PolicyRef != nil) != (tc.source == v1beta1.RolloutPlanSourcePolicy) {
				t.Fatalf("provenance = %+v", res)
			}
			if res.Group.PolicyRef != nil {
				t.Fatalf("a resolved group never carries a reference: %+v", res.Group)
			}
			if (res.Group.Canary != nil) != tc.canary {
				t.Fatalf("canary body present = %v, want %v", res.Group.Canary != nil, tc.canary)
			}
			if !tc.canary && res.Group.BlueGreen == nil {
				t.Fatalf("a non-canary resolution pins an explicit blueGreen, got %+v", res.Group)
			}
			if tc.canary && !reflect.DeepEqual(res.Group.Canary.Steps, viewSteps()) {
				t.Fatalf("the resolved body is the declared ladder, got %+v", res.Group.Canary)
			}
		})
	}
}

// A reference that does not resolve leaves its group as declared in the
// live view: no body to step, so it is no canary group, while the spec's
// other groups and every inline group read untouched.
func TestEffective_UnresolvedReferenceKeepsTheDeclaredGroup(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "ns"},
		Spec: v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
			refGroup("absent", v1beta1.RolloutProgressionCanary),
			{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		}}},
	}
	for _, policies := range []Policies{{}, PoliciesOf("ns"), PoliciesOf("ns", canaryPolicy("other", 1))} {
		view := Effective(isvc, policies)
		if view != isvc.Spec.Rollout {
			t.Fatalf("a spec in which nothing resolves is read as it is, got a copy %+v", view)
		}
		if len(CanaryGroups(isvc, policies)) != 0 || CanaryGroupFor(isvc, policies, v1beta1.EngineComponent) != nil {
			t.Fatal("an unresolved reference is no executable canary group")
		}
		if g := view.Groups[0]; g.PolicyRef == nil || g.Canary != nil || g.DeclaredProgression() != v1beta1.RolloutProgressionCanary {
			t.Fatalf("the declared group must survive unresolved, got %+v", g)
		}
	}
	// Once the policy is observed, the reference resolves and the spec is
	// left untouched behind the view.
	resolved := Effective(isvc, PoliciesOf("ns", canaryPolicy("absent", 1)))
	if resolved == isvc.Spec.Rollout || resolved.Groups[0].Canary == nil || resolved.Groups[0].PolicyRef != nil {
		t.Fatalf("the resolved view carries the body and no reference, got %+v", resolved.Groups[0])
	}
	if isvc.Spec.Rollout.Groups[0].Canary != nil || resolved.Groups[1].BlueGreen == nil {
		t.Fatal("resolution must not write into the spec or drop the inline groups")
	}
}
