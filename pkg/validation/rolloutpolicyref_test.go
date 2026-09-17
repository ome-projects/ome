package validation

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// declaredRef builds a policyRef declaring the given progression kind.
func declaredRef(kind v1beta1.RolloutProgressionKind) *v1beta1.RolloutPolicyRef {
	return &v1beta1.RolloutPolicyRef{Name: "canary-std-v1", Progression: kind}
}

// refGroup builds a ref-only rollout group (no inline arm).
func refGroup(kind v1beta1.RolloutProgressionKind, components ...v1beta1.ComponentType) v1beta1.RolloutGroup {
	return v1beta1.RolloutGroup{Components: components, PolicyRef: declaredRef(kind)}
}

// omeNativeSpecWithGroups attaches the groups to an engine+decoder OMENative
// spec — the shape most kind-resolution rules read.
func omeNativeSpecWithGroups(groups ...v1beta1.RolloutGroup) *v1beta1.InferenceServiceSpec {
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{Groups: groups}
	return spec
}

func wantReason(t *testing.T, err error, reason, scenario string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), reason) {
		t.Errorf("%s: got %v, want reason %s", scenario, err, reason)
	}
}

func TestRolloutGroupKind(t *testing.T) {
	canaryBody := &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}}
	cases := []struct {
		name  string
		group v1beta1.RolloutGroup
		want  v1beta1.RolloutProgressionKind
	}{
		{"inline canary", v1beta1.RolloutGroup{Canary: canaryBody}, v1beta1.RolloutProgressionCanary},
		{"inline blueGreen", v1beta1.RolloutGroup{BlueGreen: &v1beta1.GroupBlueGreen{}}, v1beta1.RolloutProgressionBlueGreen},
		{"inline rollingUpdate", v1beta1.RolloutGroup{RollingUpdate: &v1beta1.GroupRollingUpdate{}}, v1beta1.RolloutProgressionRollingUpdate},
		{"ref-only declared canary", v1beta1.RolloutGroup{PolicyRef: declaredRef(v1beta1.RolloutProgressionCanary)}, v1beta1.RolloutProgressionCanary},
		{"ref-only declared blueGreen", v1beta1.RolloutGroup{PolicyRef: declaredRef(v1beta1.RolloutProgressionBlueGreen)}, v1beta1.RolloutProgressionBlueGreen},
		{"ref-only declared rollingUpdate", v1beta1.RolloutGroup{PolicyRef: declaredRef(v1beta1.RolloutProgressionRollingUpdate)}, v1beta1.RolloutProgressionRollingUpdate},
		{"inline wins over ref", v1beta1.RolloutGroup{BlueGreen: &v1beta1.GroupBlueGreen{}, PolicyRef: declaredRef(v1beta1.RolloutProgressionCanary)}, v1beta1.RolloutProgressionBlueGreen},
		{"inline canary wins over rollingUpdate ref", v1beta1.RolloutGroup{Canary: canaryBody, PolicyRef: declaredRef(v1beta1.RolloutProgressionRollingUpdate)}, v1beta1.RolloutProgressionCanary},
		{"neither defaults to blueGreen", v1beta1.RolloutGroup{}, v1beta1.RolloutProgressionBlueGreen},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := rolloutGroupKind(&tc.group); got != tc.want {
				t.Errorf("rolloutGroupKind = %q, want %q", got, tc.want)
			}
		})
	}
}

// A declared-canary ref counts toward the one-canary-max exactly like an
// inline canary block.
func TestValidateCanary_DeclaredCanaryRefCountsTowardOneCanaryMax(t *testing.T) {
	spec := omeNativeSpecWithGroups(
		v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			}},
		},
		refGroup(v1beta1.RolloutProgressionCanary, v1beta1.DecoderComponent),
	)
	wantReason(t, ValidateCanary(spec), ReasonMultipleCanaryGroups, "inline canary + declared-canary ref")
}

// A declared-canary ref group faces the entrypoint rule without the policy
// body being dereferenced.
func TestValidateCanary_DeclaredCanaryRefRequiresEntrypoint(t *testing.T) {
	spec := omeNativeSpecWithGroups(refGroup(v1beta1.RolloutProgressionCanary, v1beta1.DecoderComponent))
	err := ValidateCanary(spec)
	wantReason(t, err, ReasonCanaryInvalid, "declared-canary ref without the entrypoint")
	if err != nil && !strings.Contains(err.Error(), "entrypoint") {
		t.Errorf("error should name the entrypoint rule: %v", err)
	}
}

// A well-shaped declared-canary ref admits: the shape rules pass and there
// is no inline body to plan-validate.
func TestValidateCanary_DeclaredCanaryRefValidShapeAccepted(t *testing.T) {
	spec := omeNativeSpecWithGroups(refGroup(v1beta1.RolloutProgressionCanary, v1beta1.EngineComponent, v1beta1.DecoderComponent))
	if err := ValidateCanary(spec); err != nil {
		t.Errorf("valid declared-canary ref rejected: %v", err)
	}
}

func TestValidateCanary_DeclaredCanaryRefMaintainRatioRejected(t *testing.T) {
	g := refGroup(v1beta1.RolloutProgressionCanary, v1beta1.EngineComponent)
	g.MaintainRatio = &v1beta1.MaintainRatio{}
	spec := omeNativeSpecWithGroups(g)
	wantReason(t, ValidateCanary(spec), ReasonCanaryInvalid, "maintainRatio on a declared-canary ref")
}

// The OMENative gate applies to declared-canary ref members, with the canary
// reason — and ValidateCoordination skips the group entirely, so the canary
// rule is the only one that fires.
func TestValidateCanary_DeclaredCanaryRefRequiresOMENative(t *testing.T) {
	mode := constants.RawDeployment
	spec := &v1beta1.InferenceServiceSpec{
		DeploymentMode: &mode,
		Engine:         &v1beta1.EngineSpec{},
		Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
			refGroup(v1beta1.RolloutProgressionCanary, v1beta1.EngineComponent),
		}},
	}
	wantReason(t, ValidateCanary(spec), ReasonCanaryRequiresOMENative, "non-OMENative declared-canary ref member")
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("ValidateCoordination must skip canary-kind groups, got: %v", err)
	}
}

// A group carrying both an inline arm and a ref validates as the INLINE
// kind: an inline blueGreen beside a declared-canary ref is not a canary
// group, so neither the one-run-per-unit nor the unit-entrypoint rule sees it.
func TestValidateCanary_InlineArmOutranksDeclaredCanaryRef(t *testing.T) {
	spec := omeNativeSpecWithGroups(
		v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			}},
		},
		v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.DecoderComponent},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
			PolicyRef:  declaredRef(v1beta1.RolloutProgressionCanary),
		},
	)
	if err := ValidateCanary(spec); err != nil {
		t.Errorf("inline blueGreen + shadowed canary ref must not count as a canary group: %v", err)
	}
}

func TestValidateCoordination_SoakOnDeclaredCanaryRefRejected(t *testing.T) {
	g := refGroup(v1beta1.RolloutProgressionCanary, v1beta1.EngineComponent)
	g.Soak = &metav1.Duration{Duration: time.Minute}
	spec := omeNativeSpecWithGroups(g)
	wantReason(t, ValidateCoordination(spec), ReasonSoakNotHonored, "soak on a declared-canary ref")
}

// Declared kinds shape the Sequential collapse exactly like inline arms: a
// declared-rollingUpdate ref breaks it, declared-blueGreen refs keep it.
func TestRolloutCollapse_DeclaredKinds(t *testing.T) {
	rollingUpdateMix := []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		refGroup(v1beta1.RolloutProgressionRollingUpdate, v1beta1.EngineComponent),
	}
	if rolloutCollapsesToSequential(rollingUpdateMix) {
		t.Error("a declared-rollingUpdate ref must not collapse to Sequential")
	}
	wantReason(t, ValidateRolloutOrderingEnforced(omeNativeSpecWithGroups(rollingUpdateMix...)),
		ReasonGroupOrderingNotHonored, "multi-group list with a declared-rollingUpdate ref")

	blueGreenRun := []v1beta1.RolloutGroup{
		refGroup(v1beta1.RolloutProgressionBlueGreen, v1beta1.DecoderComponent),
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
	}
	if !rolloutCollapsesToSequential(blueGreenRun) {
		t.Error("a run of single-Component blueGreen groups must collapse, declared refs included")
	}
	spec := omeNativeSpecWithGroups(blueGreenRun...)
	if err := ValidateRolloutOrderingEnforced(spec); err != nil {
		t.Errorf("declared-blueGreen ref run rejected: %v", err)
	}
	spec.Rollout.Groups[0].Soak = &metav1.Duration{Duration: time.Minute}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("soak on a collapsing run with a declared-blueGreen ref rejected: %v", err)
	}
}

// A ref-only group with no inline body faces no plan/budget checks here, and
// a declared-rollingUpdate ref group obeys the lockstep contract.
func TestValidateCoordinationUpdate_DeclaredRollingUpdateRefLockstep(t *testing.T) {
	group := refGroup(v1beta1.RolloutProgressionRollingUpdate, v1beta1.EngineComponent, v1beta1.DecoderComponent)

	oldSpec := omeNativePDSpecWithImages("v1", "v1")
	newSpec := omeNativePDSpecWithImages("v2", "v1")
	newSpec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{group}}
	wantLockstepViolation(t, ValidateCoordinationUpdate(oldSpec, newSpec), "one-sided bump under a declared-rollingUpdate ref")

	bothBumped := omeNativePDSpecWithImages("v2", "v2")
	bothBumped.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{group}}
	if err := ValidateCoordinationUpdate(oldSpec, bothBumped); err != nil {
		t.Errorf("lockstep-satisfying bump rejected: %v", err)
	}
	if err := ValidateCoordination(bothBumped); err != nil {
		t.Errorf("ref-only rollingUpdate group must face no inline budget checks: %v", err)
	}
}

// A pairing-protocol change is coordinated by a declared blueGreen/canary
// ref group holding both components, and NOT by a declared-rollingUpdate one.
func TestValidatePairingProtocolUpdate_DeclaredKinds(t *testing.T) {
	specWithProtocol := func(protocol string, groups ...v1beta1.RolloutGroup) *v1beta1.InferenceServiceSpec {
		spec := omeNativeSpecWithGroups(groups...)
		spec.Rollout.PairingProtocol = &protocol
		return spec
	}
	pair := []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}

	oldSpec := specWithProtocol("kvproto-v1")
	newSpec := specWithProtocol("kvproto-v2", refGroup(v1beta1.RolloutProgressionBlueGreen, pair...))
	if err := ValidatePairingProtocolUpdate(oldSpec, newSpec); err != nil {
		t.Errorf("declared-blueGreen ref pair group must coordinate the change: %v", err)
	}

	newSpec = specWithProtocol("kvproto-v2", refGroup(v1beta1.RolloutProgressionRollingUpdate, pair...))
	wantReason(t, ValidatePairingProtocolUpdate(oldSpec, newSpec),
		ReasonPairingProtocolChangeUncoordinated, "declared-rollingUpdate ref pair group")
}

func TestValidateRolloutPolicyRefs(t *testing.T) {
	specWithRef := func(ref *v1beta1.RolloutPolicyRef) *v1beta1.InferenceServiceSpec {
		return omeNativeSpecWithGroups(v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			PolicyRef:  ref,
		})
	}
	valid := declaredRef(v1beta1.RolloutProgressionCanary)

	t.Run("nil spec and no refs are OK", func(t *testing.T) {
		if err := ValidateRolloutPolicyRefs(nil, false); err != nil {
			t.Errorf("nil spec: %v", err)
		}
		if err := ValidateRolloutPolicyRefs(omeNativeSpecWithGroups(v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
		}), false); err != nil {
			t.Errorf("ref-free spec with the feature disabled: %v", err)
		}
	})

	t.Run("feature disabled rejects any ref", func(t *testing.T) {
		wantReason(t, ValidateRolloutPolicyRefs(specWithRef(valid), false),
			ReasonRolloutPolicyRefUnsupported, "ref with the feature disabled")
	})

	t.Run("valid kinds admitted", func(t *testing.T) {
		for _, kind := range []string{"", RolloutPolicyKind} {
			ref := declaredRef(v1beta1.RolloutProgressionRollingUpdate)
			ref.Kind = kind
			if err := ValidateRolloutPolicyRefs(specWithRef(ref), true); err != nil {
				t.Errorf("kind %q: %v", kind, err)
			}
		}
	})

	t.Run("reserved kind rejected", func(t *testing.T) {
		ref := declaredRef(v1beta1.RolloutProgressionCanary)
		ref.Kind = "ClusterRolloutPolicy"
		wantReason(t, ValidateRolloutPolicyRefs(specWithRef(ref), true),
			ReasonRolloutPolicyRefInvalid, "reserved cluster kind")
	})

	t.Run("empty name rejected", func(t *testing.T) {
		ref := declaredRef(v1beta1.RolloutProgressionCanary)
		ref.Name = ""
		wantReason(t, ValidateRolloutPolicyRefs(specWithRef(ref), true),
			ReasonRolloutPolicyRefInvalid, "empty name")
	})

	t.Run("progression outside the enum rejected", func(t *testing.T) {
		for _, progression := range []v1beta1.RolloutProgressionKind{"", "Canary", "recreate"} {
			ref := declaredRef(progression)
			wantReason(t, ValidateRolloutPolicyRefs(specWithRef(ref), true),
				ReasonRolloutPolicyRefInvalid, "progression "+string(progression))
		}
	})
}

func TestValidateInlineRolloutPlanSize(t *testing.T) {
	canaryGroup := v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("10%"), Traffic: 10},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}

	t.Run("zero cap means uncapped", func(t *testing.T) {
		if err := ValidateInlineRolloutPlanSize(omeNativeSpecWithGroups(canaryGroup), 0); err != nil {
			t.Errorf("uncapped: %v", err)
		}
	})

	t.Run("oversized inline body rejected with the arm named", func(t *testing.T) {
		err := ValidateInlineRolloutPlanSize(omeNativeSpecWithGroups(canaryGroup), 16)
		wantReason(t, err, ReasonRolloutPlanTooLarge, "canary body over the cap")
		if err != nil && !strings.Contains(err.Error(), "spec.rollout.groups[0].canary") {
			t.Errorf("error should name the group and arm: %v", err)
		}
	})

	t.Run("body under the cap admitted", func(t *testing.T) {
		if err := ValidateInlineRolloutPlanSize(omeNativeSpecWithGroups(canaryGroup), 4096); err != nil {
			t.Errorf("under the cap: %v", err)
		}
	})

	t.Run("ref-only group has no inline body to cap", func(t *testing.T) {
		spec := omeNativeSpecWithGroups(refGroup(v1beta1.RolloutProgressionCanary, v1beta1.EngineComponent))
		if err := ValidateInlineRolloutPlanSize(spec, 1); err != nil {
			t.Errorf("ref-only group must not be capped here: %v", err)
		}
	})
}
