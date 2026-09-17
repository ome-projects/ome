package validation

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestValidateCoordination_NilSpecIsOK(t *testing.T) {
	if err := ValidateCoordination(nil); err != nil {
		t.Errorf("nil spec: unexpected error %v", err)
	}
}

func TestValidateCoordination_MissingCoordinationBlockIsOK(t *testing.T) {
	spec := &v1beta1.InferenceServiceSpec{}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("missing coordination: unexpected error %v", err)
	}
}

func TestValidateCoordination_EmptyGroupsIsOK(t *testing.T) {
	spec := &v1beta1.InferenceServiceSpec{
		Rollout: &v1beta1.RolloutSpec{Groups: nil},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("empty groups: unexpected error %v", err)
	}
}

func TestValidateCoordination_DuplicateComponentRejected(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonDuplicateComponentInCoordinationGroups) {
		t.Errorf("duplicate component: got %v want error with %s", err, ReasonDuplicateComponentInCoordinationGroups)
	}
}

func TestValidateCoordination_InvalidComponentName(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{"frontend"}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonInvalidComponentInCoordinationGroup) {
		t.Errorf("invalid component: got %v want %s", err, ReasonInvalidComponentInCoordinationGroup)
	}
}

func TestValidateCoordination_OrphanGroup(t *testing.T) {
	// Spec declares only engine; group references decoder + router.
	spec := omeNativeEngineOnlySpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.RouterComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonOrphanCoordinationGroup) {
		t.Errorf("orphan group: got %v want %s", err, ReasonOrphanCoordinationGroup)
	}
}

func TestValidateCoordination_GroupMembership(t *testing.T) {
	// Every component named by a group must be declared on the ISVC. The
	// error names the group index and each missing member (quoted), so a
	// partially-declared group cannot slip through on the strength of the
	// members that do exist.
	cases := []struct {
		name        string
		spec        *v1beta1.InferenceServiceSpec
		components  []v1beta1.ComponentType
		wantErr     bool
		wantSubstrs []string
		notSubstrs  []string
	}{
		{
			name:        "fully undeclared group",
			spec:        omeNativeEngineOnlySpec(),
			components:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.RouterComponent},
			wantErr:     true,
			wantSubstrs: []string{"groups[0]", `"decoder"`, `"router"`, ReasonOrphanCoordinationGroup},
		},
		{
			name:        "partially undeclared group",
			spec:        omeNativeEngineOnlySpec(),
			components:  []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			wantErr:     true,
			wantSubstrs: []string{"groups[0]", `"decoder"`, ReasonOrphanCoordinationGroup},
			notSubstrs:  []string{`"engine"`},
		},
		{
			name:       "fully declared group",
			spec:       omeNativePDSpec(),
			components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			wantErr:    false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.spec.Rollout = &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{
					{Components: tc.components, BlueGreen: &v1beta1.GroupBlueGreen{}},
				},
			}
			err := ValidateCoordination(tc.spec)
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected rejection, got nil")
			}
			for _, s := range tc.wantSubstrs {
				if !strings.Contains(err.Error(), s) {
					t.Errorf("error %q missing %q", err.Error(), s)
				}
			}
			for _, s := range tc.notSubstrs {
				if strings.Contains(err.Error(), s) {
					t.Errorf("error %q should not name declared member %q", err.Error(), s)
				}
			}
		})
	}
}

func TestValidateCoordination_MembershipErrorNamesGroupIndex(t *testing.T) {
	// With multiple groups the error must point at the offending group's
	// index, not the first group.
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
			{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.RouterComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), "groups[1]") || !strings.Contains(err.Error(), `"router"`) {
		t.Errorf("got %v want error naming groups[1] and \"router\"", err)
	}
}

func TestValidateCoordination_UndeclaredMemberFailsModeCheckToo(t *testing.T) {
	// The OMENative-mode check does not skip undeclared members: an
	// undeclared component resolves to an empty mode and fails the check.
	// (Through the public entry point the orphan check fires first; this
	// pins the mode check's own behavior for defense in depth.)
	spec := omeNativeEngineOnlySpec()
	groups := []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
	}
	err := validateComponentsAreOMENative(spec, groups)
	if err == nil || !strings.Contains(err.Error(), ReasonCoordinationRequiresOMENative) {
		t.Errorf("undeclared member: got %v want %s", err, ReasonCoordinationRequiresOMENative)
	}
}

func TestValidateCoordination_ToleranceRange(t *testing.T) {
	// Value-level mirror of the CRD Minimum=0/Maximum=100 bounds. Omitted
	// (nil) is valid — the operator-configured default applies at
	// resolution — and explicit 0 stays a valid, distinct request.
	cases := []struct {
		name      string
		tolerance *int32
		wantErr   bool
	}{
		{name: "omitted", tolerance: nil, wantErr: false},
		{name: "explicit zero", tolerance: ptr.To(int32(0)), wantErr: false},
		{name: "normal", tolerance: ptr.To(int32(25)), wantErr: false},
		{name: "upper bound", tolerance: ptr.To(int32(100)), wantErr: false},
		{name: "negative", tolerance: ptr.To(int32(-1)), wantErr: true},
		{name: "over 100", tolerance: ptr.To(int32(101)), wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := omeNativePDSpec()
			spec.Rollout = &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					BlueGreen:     &v1beta1.GroupBlueGreen{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: tc.tolerance},
				}},
			}
			err := ValidateCoordination(spec)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), ReasonRatioToleranceOutOfRange) {
					t.Errorf("got %v want error with %s", err, ReasonRatioToleranceOutOfRange)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestCoordinationRatioToleranceWarning_OmittedToleranceEmpty(t *testing.T) {
	// maintainRatio with no tolerance defers to the operator-configured
	// default; the webhook cannot know that value, so it must not warn.
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{{
			Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			BlueGreen:     &v1beta1.GroupBlueGreen{},
			MaintainRatio: &v1beta1.MaintainRatio{},
		}},
	}
	if w := CoordinationRatioToleranceWarning(spec); w != "" {
		t.Errorf("omitted tolerance: got %q want empty", w)
	}
}

func TestValidateCoordination_ComponentMustBeOMENative(t *testing.T) {
	spec := omeNativePDSpec()
	// Strip the OMENative annotation off engine.
	spec.Engine.Annotations = map[string]string{}
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonCoordinationRequiresOMENative) {
		t.Errorf("non-OMENative component: got %v want %s", err, ReasonCoordinationRequiresOMENative)
	}
}

func TestValidateCoordination_InvalidOrderComponent(t *testing.T) {
	// Order references router, which is not one of the group's components → the
	// per-group order-subset rule rejects it (still enforced in v2).
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
				Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.RouterComponent},
			},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonInvalidCoordinationOrder) {
		t.Errorf("foreign order entry: got %v want %s", err, ReasonInvalidCoordinationOrder)
	}
}

func TestValidateCoordination_HappyPath_BlueGreen(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("happy atomic: unexpected error %v", err)
	}
}

// Sequential is spelled in v2 as one single-Component group per Component, in
// list order (decoder first, then engine) — the controller collapses this back
// to Sequential. The validator must accept it.
func TestValidateCoordination_HappyPath_Sequential(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("happy sequential: unexpected error %v", err)
	}
}

// Positive case: a multi-Component group guarding the cross-Component ratio
// (v1 BlueGreen + RatioBalanced pacing → v2 BlueGreen + MaintainRatio) is
// accepted. The validator must not over-reject.
func TestValidateCoordination_RatioBalancedWithBlueGreenAccepted(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				BlueGreen:     &v1beta1.GroupBlueGreen{},
				MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(5))},
			},
		},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("BlueGreen+MaintainRatio: unexpected error %v", err)
	}
}

// maxSurge=0 AND maxUnavailable=0 (both explicit zero) is unstartable —
// no surge, no drain — and must be rejected.
func TestValidateCoordination_ZeroBudgetPacingRejected(t *testing.T) {
	zero := intstr.FromInt(0)
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				RollingUpdate: &v1beta1.GroupRollingUpdate{
					MaxSurge:       &zero,
					MaxUnavailable: &zero,
				},
			},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonZeroBudgetPacingUnstartable) {
		t.Errorf("zero-budget pacing: got %v want error with %s", err, ReasonZeroBudgetPacingUnstartable)
	}
}

// TestValidateCoordination_SoakOnNonCollapsingRejected: a multi-Component
// blueGreen group with a soak does not collapse to the Sequential state machine,
// so the engine would drop the soak. Admission rejects it.
func TestValidateCoordination_SoakOnNonCollapsingRejected(t *testing.T) {
	mode := constants.OMENative
	soak := &metav1.Duration{Duration: 10 * time.Minute}
	spec := &v1beta1.InferenceServiceSpec{
		DeploymentMode: &mode,
		Engine:         &v1beta1.EngineSpec{},
		Decoder:        &v1beta1.DecoderSpec{},
		Router:         &v1beta1.RouterSpec{},
		Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Soak: soak},
			{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}},
		}},
	}
	if err := ValidateCoordination(spec); err == nil || !strings.Contains(err.Error(), ReasonSoakNotHonored) {
		t.Fatalf("soak on a non-collapsing group must be rejected, got: %v", err)
	}
}

// TestValidateCoordination_SoakOnSequenceAccepted: a run of single-Component
// blueGreen groups collapses to Sequential, which honors soak, so it passes.
func TestValidateCoordination_SoakOnSequenceAccepted(t *testing.T) {
	mode := constants.OMENative
	soak := &metav1.Duration{Duration: 10 * time.Minute}
	spec := &v1beta1.InferenceServiceSpec{
		DeploymentMode: &mode,
		Engine:         &v1beta1.EngineSpec{},
		Decoder:        &v1beta1.DecoderSpec{},
		Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, Soak: soak},
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}},
		}},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Fatalf("soak on a collapsing single-Component sequence must pass: %v", err)
	}
}

// String "0%" values are equivalent to literal 0 and must produce the
// same zero-budget rejection.
func TestValidateCoordination_ZeroBudgetPacingRejectedStringForm(t *testing.T) {
	zeroPct := intstr.FromString("0%")
	zeroInt := intstr.FromInt(0)
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				RollingUpdate: &v1beta1.GroupRollingUpdate{
					MaxSurge:       &zeroPct,
					MaxUnavailable: &zeroInt,
				},
			},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonZeroBudgetPacingUnstartable) {
		t.Errorf("zero-budget (str/int mix): got %v want error with %s", err, ReasonZeroBudgetPacingUnstartable)
	}
}

// Semantic rejection matrix for group rollingUpdate budgets: negative
// integers, non-percent strings, malformed percents, and percents outside
// 0%-100% must all be rejected, and the error must carry the exact
// spec.rollout.groups[i].rollingUpdate.* field path. Without this check the
// runtime parser resolves every one of these to a zero budget.
func TestValidateCoordination_GroupBudgetSemanticRejections(t *testing.T) {
	cases := []struct {
		name       string
		surge      *intstr.IntOrString
		unavail    *intstr.IntOrString
		wantReason string
		wantPath   string
	}{
		{"negative-int-surge", intstrPtr(intstr.FromInt(-1)), nil,
			ReasonInvalidRollingUpdateInteger, "spec.rollout.groups[0].rollingUpdate.maxSurge"},
		{"negative-int-unavailable", nil, intstrPtr(intstr.FromInt(-2)),
			ReasonInvalidRollingUpdateInteger, "spec.rollout.groups[0].rollingUpdate.maxUnavailable"},
		{"non-percent-string-surge", intstrPtr(intstr.FromString("abc")), nil,
			ReasonInvalidRollingUpdatePercent, "spec.rollout.groups[0].rollingUpdate.maxSurge"},
		{"plain-number-string-unavailable", nil, intstrPtr(intstr.FromString("25")),
			ReasonInvalidRollingUpdatePercent, "spec.rollout.groups[0].rollingUpdate.maxUnavailable"},
		{"malformed-percent-surge", intstrPtr(intstr.FromString("foo%")), nil,
			ReasonInvalidRollingUpdatePercent, "spec.rollout.groups[0].rollingUpdate.maxSurge"},
		{"negative-percent-unavailable", nil, intstrPtr(intstr.FromString("-25%")),
			ReasonInvalidRollingUpdatePercent, "spec.rollout.groups[0].rollingUpdate.maxUnavailable"},
		{"over-hundred-percent-surge", intstrPtr(intstr.FromString("150%")), nil,
			ReasonInvalidRollingUpdatePercent, "spec.rollout.groups[0].rollingUpdate.maxSurge"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := omeNativePDSpec()
			spec.Rollout = &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{
					{
						Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
						RollingUpdate: &v1beta1.GroupRollingUpdate{
							MaxSurge:       tc.surge,
							MaxUnavailable: tc.unavail,
						},
					},
				},
			}
			err := ValidateCoordination(spec)
			if err == nil {
				t.Fatalf("expected rejection, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantReason) {
				t.Errorf("got %v want reason %s", err, tc.wantReason)
			}
			if !strings.Contains(err.Error(), tc.wantPath) {
				t.Errorf("got %v want exact path %s", err, tc.wantPath)
			}
		})
	}
}

// A budget pair where BOTH values resolve to zero at runtime ("abc" parses to
// zero, as does an explicit 0) must be rejected — otherwise the rollout has
// neither surge nor drain headroom and stalls silently. The malformed half is
// reported first with its exact path.
func TestValidateCoordination_MalformedPairResolvingToZeroRejected(t *testing.T) {
	abc := intstr.FromString("abc")
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				RollingUpdate: &v1beta1.GroupRollingUpdate{
					MaxSurge:       &abc,
					MaxUnavailable: &abc,
				},
			},
		},
	}
	err := ValidateCoordination(spec)
	if err == nil {
		t.Fatal("malformed budget pair resolving to zero must be rejected, got nil")
	}
	if !strings.Contains(err.Error(), "spec.rollout.groups[0].rollingUpdate.maxSurge") {
		t.Errorf("got %v want exact maxSurge path", err)
	}
}

// The zero-budget pair rule uses runtime-RESOLVED values, not just literal
// zeros: a malformed string and a negative integer both resolve to zero, so
// the pair deadlocks even though neither is written as 0. Exercised directly
// to pin the rule independent of the per-field checks that run before it.
func TestValidatePacingNotZeroBudget_ResolvedZeroPairRejected(t *testing.T) {
	abc := intstr.FromString("abc")
	neg := intstr.FromInt(-1)
	groups := []v1beta1.RolloutGroup{
		{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			RollingUpdate: &v1beta1.GroupRollingUpdate{
				MaxSurge:       &abc,
				MaxUnavailable: &neg,
			},
		},
	}
	err := validatePacingNotZeroBudget(groups)
	if err == nil || !strings.Contains(err.Error(), ReasonZeroBudgetPacingUnstartable) {
		t.Errorf("resolved-zero pair: got %v want error with %s", err, ReasonZeroBudgetPacingUnstartable)
	}
	if !strings.Contains(err.Error(), "spec.rollout.groups[0].rollingUpdate") {
		t.Errorf("got %v want exact rollingUpdate path", err)
	}
}

// The field path in a budget error carries the index of the offending group,
// not always [0].
func TestValidateCoordination_GroupBudgetPathCarriesGroupIndex(t *testing.T) {
	bad := intstr.FromString("150%")
	mode := constants.OMENative
	spec := &v1beta1.InferenceServiceSpec{
		DeploymentMode: &mode,
		Engine:         &v1beta1.EngineSpec{},
		Decoder:        &v1beta1.DecoderSpec{},
		Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
			{
				Components:    []v1beta1.ComponentType{v1beta1.DecoderComponent},
				RollingUpdate: &v1beta1.GroupRollingUpdate{MaxSurge: &bad},
			},
		}},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), "spec.rollout.groups[1].rollingUpdate.maxSurge") {
		t.Errorf("got %v want path spec.rollout.groups[1].rollingUpdate.maxSurge", err)
	}
}

// Valid budget shapes: positive integers, in-range percents, boundary
// percents, zero on one knob only, and nil (defaulted) knobs are all accepted.
func TestValidateCoordination_GroupBudgetValidShapesAccepted(t *testing.T) {
	cases := []struct {
		name    string
		surge   *intstr.IntOrString
		unavail *intstr.IntOrString
	}{
		{"positive-ints", intstrPtr(intstr.FromInt(2)), intstrPtr(intstr.FromInt(1))},
		{"mid-percents", intstrPtr(intstr.FromString("25%")), intstrPtr(intstr.FromString("50%"))},
		{"boundary-percents", intstrPtr(intstr.FromString("100%")), intstrPtr(intstr.FromString("100%"))},
		{"zero-surge-percent-unavailable", intstrPtr(intstr.FromInt(0)), intstrPtr(intstr.FromString("25%"))},
		{"zero-percent-surge-int-unavailable", intstrPtr(intstr.FromString("0%")), intstrPtr(intstr.FromInt(1))},
		{"nil-surge-zero-unavailable", nil, intstrPtr(intstr.FromInt(0))},
		{"zero-surge-nil-unavailable", intstrPtr(intstr.FromInt(0)), nil},
		{"both-nil", nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := omeNativePDSpec()
			spec.Rollout = &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{
					{
						Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
						RollingUpdate: &v1beta1.GroupRollingUpdate{
							MaxSurge:       tc.surge,
							MaxUnavailable: tc.unavail,
						},
					},
				},
			}
			if err := ValidateCoordination(spec); err != nil {
				t.Errorf("unexpected error %v", err)
			}
		})
	}
}

// Positive case: pacing with non-zero MaxSurge is accepted even when
// MaxUnavailable=0 — the rollout still has surge headroom.
func TestValidateCoordination_NonZeroSurgePacingAccepted(t *testing.T) {
	surge := intstr.FromInt(2)
	zero := intstr.FromInt(0)
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				RollingUpdate: &v1beta1.GroupRollingUpdate{
					MaxSurge:       &surge,
					MaxUnavailable: &zero,
				},
			},
		},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("non-zero surge + zero unavailable: unexpected error %v", err)
	}
}

// Positive case: pacing with non-zero MaxUnavailable is accepted even
// when MaxSurge=0 — the rollout still has drain headroom.
func TestValidateCoordination_NonZeroUnavailablePacingAccepted(t *testing.T) {
	zero := intstr.FromInt(0)
	pct := intstr.FromString("25%")
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				RollingUpdate: &v1beta1.GroupRollingUpdate{
					MaxSurge:       &zero,
					MaxUnavailable: &pct,
				},
			},
		},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("zero surge + non-zero unavailable: unexpected error %v", err)
	}
}

// Positive case: nil pacing values (no explicit zero) are accepted —
// the defaulter fills 25% for both MaxSurge and MaxUnavailable. The
// zero-budget check only fires when both are explicitly set to 0.
func TestValidateCoordination_NilPacingAccepted(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				RollingUpdate: &v1beta1.GroupRollingUpdate{
					// MaxSurge / MaxUnavailable nil — defaulter fills 25%.
				},
			},
		},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("nil pacing values: unexpected error %v", err)
	}
}

// Positive case: omitted pacing knobs entirely is accepted — the
// defaulter fills a complete budget.
func TestValidateCoordination_OmittedPacingAccepted(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
				// No pacing — BlueGreen carries none.
			},
		},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("omitted pacing: unexpected error %v", err)
	}
}

// The zero-budget check only fires for rollingUpdate groups. A BlueGreen group
// guarding the ratio (v1 RatioBalanced) has no maxSurge/maxUnavailable budget,
// so the same zero shape is not even expressible — and MaintainRatio is accepted.
func TestValidateCoordination_RatioBalancedWithZeroBudgetNotRejected(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				BlueGreen:     &v1beta1.GroupBlueGreen{},
				MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(5))},
			},
		},
	}
	if err := ValidateCoordination(spec); err != nil {
		t.Errorf("BlueGreen + MaintainRatio: unexpected error %v (zero-budget check is rollingUpdate-only)", err)
	}
}

func TestValidateCoordinationUpdate_RollingUpdateBothBumped(t *testing.T) {
	oldSpec := omeNativePDSpecWithImages("v1", "v1")
	newSpec := omeNativePDSpecWithImages("v2", "v2")
	newSpec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
		},
	}
	if err := ValidateCoordinationUpdate(oldSpec, newSpec); err != nil {
		t.Errorf("pairedmixable both-bumped: unexpected error %v", err)
	}
}

func TestValidateCoordinationUpdate_RollingUpdateOnlyOneBumpedRejected(t *testing.T) {
	oldSpec := omeNativePDSpecWithImages("v1", "v1")
	newSpec := omeNativePDSpecWithImages("v2", "v1")
	newSpec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
		},
	}
	err := ValidateCoordinationUpdate(oldSpec, newSpec)
	if err == nil || !strings.Contains(err.Error(), ReasonRollingUpdateLockstepViolation) {
		t.Errorf("rollingupdate lockstep: got %v want %s", err, ReasonRollingUpdateLockstepViolation)
	}
}

func TestValidateCoordinationUpdate_RollingUpdateNoBumpsNoOp(t *testing.T) {
	oldSpec := omeNativePDSpecWithImages("v1", "v1")
	newSpec := omeNativePDSpecWithImages("v1", "v1")
	newSpec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
		},
	}
	if err := ValidateCoordinationUpdate(oldSpec, newSpec); err != nil {
		t.Errorf("no bumps: unexpected error %v", err)
	}
}

func TestCoordinationRatioToleranceWarning_AboveThreshold(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				BlueGreen:     &v1beta1.GroupBlueGreen{},
				MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(80))},
			},
		},
	}
	w := CoordinationRatioToleranceWarning(spec)
	if !strings.Contains(w, ReasonRatioToleranceTooHigh) {
		t.Errorf("tolerance warning: got %q want contains %s", w, ReasonRatioToleranceTooHigh)
	}
}

func TestCoordinationRatioToleranceWarning_BelowThresholdEmpty(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				BlueGreen:     &v1beta1.GroupBlueGreen{},
				MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(5))},
			},
		},
	}
	if w := CoordinationRatioToleranceWarning(spec); w != "" {
		t.Errorf("safe tolerance: got %q want empty", w)
	}
}

// --- Rollout-ordering enforceability ---

func TestValidateRolloutOrderingEnforced_NilSpecAndNoGroupsOK(t *testing.T) {
	if err := ValidateRolloutOrderingEnforced(nil); err != nil {
		t.Errorf("nil spec: unexpected error %v", err)
	}
	if err := ValidateRolloutOrderingEnforced(&v1beta1.InferenceServiceSpec{}); err != nil {
		t.Errorf("no rollout: unexpected error %v", err)
	}
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{}}
	if err := ValidateRolloutOrderingEnforced(spec); err != nil {
		t.Errorf("empty groups: unexpected error %v", err)
	}
}

// A pure run of single-Component blueGreen groups is the one multi-group shape
// whose list order the engine enforces — it must stay accepted, in both the
// explicit-blueGreen and the omitted-progression spelling.
func TestValidateRolloutOrderingEnforced_SingleComponentBlueGreenRunAccepted(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}},
	}}
	if err := ValidateRolloutOrderingEnforced(spec); err != nil {
		t.Errorf("two-group run: unexpected error %v", err)
	}
	spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}},
		{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}},
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}},
	}}
	if err := ValidateRolloutOrderingEnforced(spec); err != nil {
		t.Errorf("three-group run: unexpected error %v", err)
	}
}

// A single group makes no cross-group ordering promise, so every progression
// shape stays accepted as long as it carries no Order.
func TestValidateRolloutOrderingEnforced_SingleGroupShapesAccepted(t *testing.T) {
	cases := map[string]v1beta1.RolloutGroup{
		"multi-Component blueGreen": {
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
		},
		"multi-Component rollingUpdate": {
			Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			RollingUpdate: &v1beta1.GroupRollingUpdate{},
		},
		"single-Component rollingUpdate": {
			Components:    []v1beta1.ComponentType{v1beta1.EngineComponent},
			RollingUpdate: &v1beta1.GroupRollingUpdate{},
		},
		"canary": {
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			Canary:     &v1beta1.GroupCanary{},
		},
	}
	for name, g := range cases {
		spec := omeNativePDSpec()
		spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{g}}
		if err := ValidateRolloutOrderingEnforced(spec); err != nil {
			t.Errorf("%s (single group): unexpected error %v", name, err)
		}
	}
}

// Mixed-progression lists promise an order that runs concurrently: blueGreen
// followed by rollingUpdate, and rollingUpdate followed by canary.
func TestValidateRolloutOrderingEnforced_MixedProgressionListRejected(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
	}}
	err := ValidateRolloutOrderingEnforced(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonGroupOrderingNotHonored) {
		t.Errorf("blueGreen then rollingUpdate: got %v want %s", err, ReasonGroupOrderingNotHonored)
	}

	spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: &v1beta1.GroupCanary{}},
	}}
	err = ValidateRolloutOrderingEnforced(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonGroupOrderingNotHonored) {
		t.Errorf("rollingUpdate then canary: got %v want %s", err, ReasonGroupOrderingNotHonored)
	}
}

func TestValidateRolloutOrderingEnforced_MultiComponentGroupInListRejected(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Router = &v1beta1.RouterSpec{}
	spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
	}}
	err := ValidateRolloutOrderingEnforced(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonGroupOrderingNotHonored) {
		t.Errorf("multi-Component group in ordered list: got %v want %s", err, ReasonGroupOrderingNotHonored)
	}
	if err != nil && !strings.Contains(err.Error(), "groups[0]") {
		t.Errorf("error should name the offending group: got %q", err.Error())
	}
}

// A canary group can never be part of the collapsed run (the canary engine
// drives it independently), so mixing one into a multi-group list breaks the
// promised order.
func TestValidateRolloutOrderingEnforced_CanaryInListRejected(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: &v1beta1.GroupCanary{}},
		{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
	}}
	err := ValidateRolloutOrderingEnforced(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonGroupOrderingNotHonored) {
		t.Errorf("canary in ordered list: got %v want %s", err, ReasonGroupOrderingNotHonored)
	}
}

// Order is rejected on every shape it could be written on — no progression
// applies it.
func TestValidateRolloutOrderingEnforced_OrderRejected(t *testing.T) {
	cases := map[string]v1beta1.RolloutGroup{
		"multi-Component blueGreen": {
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
			Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		},
		"multi-Component rollingUpdate": {
			Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			RollingUpdate: &v1beta1.GroupRollingUpdate{},
			Order:         []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		},
		"canary": {
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Canary:     &v1beta1.GroupCanary{},
			Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		},
		"single-Component (trivial order)": {
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
			Order:      []v1beta1.ComponentType{v1beta1.EngineComponent},
		},
	}
	for name, g := range cases {
		spec := omeNativePDSpec()
		spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{g}}
		err := ValidateRolloutOrderingEnforced(spec)
		if err == nil || !strings.Contains(err.Error(), ReasonOrderNotHonored) {
			t.Errorf("%s with order: got %v want %s", name, err, ReasonOrderNotHonored)
		}
	}
}

// Ratchet: an update that does not touch spec.rollout is admitted even when
// the stored rollout carries an unenforced shape, so existing objects keep
// reconciling.
func TestValidateRolloutOrderingEnforcedUpdate_UntouchedRolloutAdmitted(t *testing.T) {
	unenforced := &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
	}}
	oldSpec := omeNativePDSpecWithImages("v1", "v1")
	oldSpec.Rollout = unenforced
	newSpec := omeNativePDSpecWithImages("v2", "v2") // image bump, rollout untouched
	newSpec.Rollout = unenforced.DeepCopy()
	if err := ValidateRolloutOrderingEnforcedUpdate(oldSpec, newSpec); err != nil {
		t.Errorf("untouched rollout: unexpected error %v", err)
	}
}

// Ratchet: any update that changes spec.rollout must land on an enforced
// shape — including an edit that swaps one unenforced shape for another.
func TestValidateRolloutOrderingEnforcedUpdate_ChangedRolloutRejected(t *testing.T) {
	oldSpec := omeNativePDSpec()
	newSpec := omeNativePDSpec()
	newSpec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
	}}
	err := ValidateRolloutOrderingEnforcedUpdate(oldSpec, newSpec)
	if err == nil || !strings.Contains(err.Error(), ReasonGroupOrderingNotHonored) {
		t.Errorf("added unenforced rollout: got %v want %s", err, ReasonGroupOrderingNotHonored)
	}

	oldSpec = omeNativePDSpec()
	oldSpec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}, Order: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent}},
	}}
	newSpec = omeNativePDSpec()
	newSpec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}, Order: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}},
	}}
	err = ValidateRolloutOrderingEnforcedUpdate(oldSpec, newSpec)
	if err == nil || !strings.Contains(err.Error(), ReasonOrderNotHonored) {
		t.Errorf("edited order: got %v want %s", err, ReasonOrderNotHonored)
	}
}

// Ratchet: an update that changes the rollout INTO an enforced shape passes —
// the ratchet only blocks writes that keep or introduce unenforced ordering.
func TestValidateRolloutOrderingEnforcedUpdate_ChangedToEnforcedAccepted(t *testing.T) {
	oldSpec := omeNativePDSpec()
	oldSpec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
	}}
	newSpec := omeNativePDSpec()
	newSpec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
	}}
	if err := ValidateRolloutOrderingEnforcedUpdate(oldSpec, newSpec); err != nil {
		t.Errorf("corrected rollout: unexpected error %v", err)
	}
}

// Test fixtures.

func omeNativePDSpec() *v1beta1.InferenceServiceSpec {
	annot := map[string]string{constants.DeploymentMode: string(constants.OMENative)}
	return &v1beta1.InferenceServiceSpec{
		Engine: &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: annot},
		},
		Decoder: &v1beta1.DecoderSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: annot},
		},
	}
}

func omeNativePDSpecWithImages(engineImage, decoderImage string) *v1beta1.InferenceServiceSpec {
	spec := omeNativePDSpec()
	spec.Engine.Runner = &v1beta1.RunnerSpec{Container: corev1.Container{Image: engineImage}}
	spec.Decoder.Runner = &v1beta1.RunnerSpec{Container: corev1.Container{Image: decoderImage}}
	return spec
}

func omeNativeEngineOnlySpec() *v1beta1.InferenceServiceSpec {
	annot := map[string]string{constants.DeploymentMode: string(constants.OMENative)}
	return &v1beta1.InferenceServiceSpec{
		Engine: &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: annot},
		},
	}
}

// ---------------------------------------------------------------------------
// groupOrdering: Concurrent — the opt-out from the cross-group ordering promise
// ---------------------------------------------------------------------------

// The shape this whole field exists for: a router canary and an engine canary
// on one InferenceService, rolling as unrelated rollouts.
func TestValidateRolloutOrderingEnforced_ConcurrentAdmitsIndependentCanaryGroups(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Router = &v1beta1.RouterSpec{}
	spec.Rollout = &v1beta1.RolloutSpec{
		GroupOrdering: ptr.To(v1beta1.RolloutGroupOrderingConcurrent),
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, Canary: &v1beta1.GroupCanary{}},
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: &v1beta1.GroupCanary{}},
		},
	}
	if err := ValidateRolloutOrderingEnforced(spec); err != nil {
		t.Errorf("Concurrent must admit two independent canary groups: %v", err)
	}
}

// Every shape Sequential rejects is admitted under Concurrent, because none of
// them was ever going to be sequenced.
func TestValidateRolloutOrderingEnforced_ConcurrentAdmitsEveryUnsequenceableShape(t *testing.T) {
	cases := map[string][]v1beta1.RolloutGroup{
		"blueGreen then rollingUpdate": {
			{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
		},
		"rollingUpdate then canary": {
			{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: &v1beta1.GroupCanary{}},
		},
		"multi-Component group in list": {
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
			{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		},
	}
	for name, groups := range cases {
		spec := omeNativePDSpec()
		spec.Router = &v1beta1.RouterSpec{}
		spec.Rollout = &v1beta1.RolloutSpec{GroupOrdering: ptr.To(v1beta1.RolloutGroupOrderingConcurrent), Groups: groups}
		if err := ValidateRolloutOrderingEnforced(spec); err != nil {
			t.Errorf("%s under Concurrent: unexpected error %v", name, err)
		}
	}
}

// Concurrent relaxes ONLY the cross-group ordering rule. Order is dead
// configuration under either value and stays rejected, as do the cross-group
// structural rules that are not about sequencing.
func TestValidateRolloutOrderingEnforced_ConcurrentStillRejectsOrder(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Router = &v1beta1.RouterSpec{}
	spec.Rollout = &v1beta1.RolloutSpec{
		GroupOrdering: ptr.To(v1beta1.RolloutGroupOrderingConcurrent),
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Order:      []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary:     &v1beta1.GroupCanary{},
			},
			{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, Canary: &v1beta1.GroupCanary{}},
		},
	}
	err := ValidateRolloutOrderingEnforced(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonOrderNotHonored) {
		t.Errorf("Concurrent must not excuse order: got %v want %s", err, ReasonOrderNotHonored)
	}
}

// Absent and explicit-Sequential must behave identically, so an object stored
// before the field existed keeps its original meaning.
func TestValidateRolloutOrderingEnforced_UnsetMatchesSequential(t *testing.T) {
	groups := []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, Canary: &v1beta1.GroupCanary{}},
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: &v1beta1.GroupCanary{}},
	}
	for name, ordering := range map[string]*v1beta1.RolloutGroupOrdering{
		"unset":               nil,
		"explicit Sequential": ptr.To(v1beta1.RolloutGroupOrderingSequential),
	} {
		spec := omeNativePDSpec()
		spec.Router = &v1beta1.RouterSpec{}
		spec.Rollout = &v1beta1.RolloutSpec{GroupOrdering: ordering, Groups: groups}
		err := ValidateRolloutOrderingEnforced(spec)
		if err == nil || !strings.Contains(err.Error(), ReasonGroupOrderingNotHonored) {
			t.Errorf("%s: got %v want %s", name, err, ReasonGroupOrderingNotHonored)
		}
	}
}

// The rejection must point at the escape hatch, or an operator has no way to
// discover it from the error alone.
func TestValidateRolloutOrderingEnforced_ErrorNamesTheEscapeHatch(t *testing.T) {
	spec := omeNativePDSpec()
	spec.Router = &v1beta1.RouterSpec{}
	spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, Canary: &v1beta1.GroupCanary{}},
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: &v1beta1.GroupCanary{}},
	}}
	err := ValidateRolloutOrderingEnforced(spec)
	if err == nil || !strings.Contains(err.Error(), "groupOrdering: Concurrent") {
		t.Errorf("error must name groupOrdering: Concurrent, got %v", err)
	}
}

// The update ratchet must carry the relaxation too: flipping an admitted
// Sequential list to Concurrent is itself a rollout edit.
func TestValidateRolloutOrderingEnforcedUpdate_SequentialToConcurrent(t *testing.T) {
	oldSpec := omeNativePDSpec()
	oldSpec.Router = &v1beta1.RouterSpec{}
	oldSpec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
	}}
	newSpec := omeNativePDSpec()
	newSpec.Router = &v1beta1.RouterSpec{}
	newSpec.Rollout = &v1beta1.RolloutSpec{
		GroupOrdering: ptr.To(v1beta1.RolloutGroupOrderingConcurrent),
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, Canary: &v1beta1.GroupCanary{}},
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: &v1beta1.GroupCanary{}},
		},
	}
	if err := ValidateRolloutOrderingEnforcedUpdate(oldSpec, newSpec); err != nil {
		t.Errorf("Sequential blueGreen -> Concurrent canary must be admitted: %v", err)
	}
	// Going back is fine when the groups go back too: a run of single-Component
	// blueGreen groups is the one shape Sequential really enforces.
	if err := ValidateRolloutOrderingEnforcedUpdate(newSpec, oldSpec); err != nil {
		t.Errorf("Concurrent canary -> Sequential blueGreen must be admitted: %v", err)
	}
	// Clearing the declaration while KEEPING the canary groups re-imposes a
	// promise they cannot keep, so it is rejected.
	regressed := newSpec.DeepCopy()
	regressed.Rollout.GroupOrdering = nil
	err := ValidateRolloutOrderingEnforcedUpdate(newSpec, regressed)
	if err == nil || !strings.Contains(err.Error(), ReasonGroupOrderingNotHonored) {
		t.Errorf("clearing Concurrent on a canary list must be rejected: got %v", err)
	}
}
