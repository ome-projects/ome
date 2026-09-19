package coordination

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestResolveGroups_NilSpecReturnsNil(t *testing.T) {
	if got := ResolveGroups(nil, GroupDefaults{}); got != nil {
		t.Errorf("nil spec: got %+v want nil", got)
	}
}

func TestResolveGroups_EmptyGroupsReturnsNil(t *testing.T) {
	if got := ResolveGroups(&v1beta1.RolloutSpec{}, GroupDefaults{}); got != nil {
		t.Errorf("empty Groups: got %+v want nil", got)
	}
}

func TestResolveGroups_AppliesPacingDefaults(t *testing.T) {
	spec := &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
			},
		},
	}
	groups := ResolveGroups(spec, GroupDefaults{})
	if len(groups) != 1 {
		t.Fatalf("groups: got %d want 1", len(groups))
	}
	g := groups[0]
	if g.Name != "0" {
		t.Errorf("Name: got %q want 0", g.Name)
	}
	if g.Pacing.Type != v1beta1.CoordinationPacingPerComponent {
		t.Errorf("Pacing.Type default: got %q want PerComponent", g.Pacing.Type)
	}
	if g.Pacing.MaxSurge == nil || g.Pacing.MaxSurge.StrVal != "25%" {
		t.Errorf("Pacing.MaxSurge default: got %+v want 25%%", g.Pacing.MaxSurge)
	}
	if g.Pacing.MaxUnavailable == nil || g.Pacing.MaxUnavailable.StrVal != "25%" {
		t.Errorf("Pacing.MaxUnavailable default: got %+v want 25%%", g.Pacing.MaxUnavailable)
	}
}

func TestResolveGroups_NoProgressionDefaultsBlueGreen(t *testing.T) {
	// A group that names no progression (no canary/blueGreen/rollingUpdate) is the
	// ergonomic "just roll this" spelling; the resolve layer treats it as
	// blueGreen. This is what makes `components: [engine]` with no progression a
	// real rollout rather than a no-op.
	spec := &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}},
		},
	}
	groups := ResolveGroups(spec, GroupDefaults{})
	if len(groups) != 1 {
		t.Fatalf("groups: got %d want 1", len(groups))
	}
	if groups[0].Policy != v1beta1.CoordinationPolicyBlueGreen {
		t.Errorf("no-progression group Policy: got %q want BlueGreen", groups[0].Policy)
	}
}

func TestResolveGroups_NoProgressionPairCollapsesSequential(t *testing.T) {
	// The intuitive "roll decoder, then engine" spelling: two single-Component
	// groups, neither naming a progression. Each defaults to blueGreen, so the run
	// collapses into one Sequential group whose Order is the list order. Without
	// the blueGreen default these would not collapse and would (wrongly) roll
	// concurrently — so this locks the contract that the no-ceremony sequential
	// spelling actually sequences.
	spec := &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}},
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}},
		},
	}
	groups := ResolveGroups(spec, GroupDefaults{})
	if len(groups) != 1 {
		t.Fatalf("expected one collapsed group, got %d: %+v", len(groups), groups)
	}
	g := groups[0]
	if g.Policy != v1beta1.CoordinationPolicySequential {
		t.Fatalf("Policy: got %q want Sequential", g.Policy)
	}
	if len(g.Order) != 2 || g.Order[0] != v1beta1.DecoderComponent || g.Order[1] != v1beta1.EngineComponent {
		t.Errorf("Order: got %v want [decoder engine]", g.Order)
	}
}

func TestResolveGroups_RatioBalancedTolerancePropagates(t *testing.T) {
	// v2: cross-Component ratio guarding is expressed via MaintainRatio on a
	// multi-Component group; it maps to RatioBalanced pacing for both
	// rollingUpdate (this test) and blueGreen (see
	// TestResolveGroups_MaintainRatioOnBlueGreen). An explicit tolerance is
	// copied verbatim; the omitted-tolerance fills are covered by
	// TestResolveGroups_ToleranceDefaultResolution.
	spec := &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				RollingUpdate: &v1beta1.GroupRollingUpdate{},
				MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(5))},
			},
		},
	}
	g := ResolveGroups(spec, GroupDefaults{})[0]
	if g.Pacing.RatioTolerancePercent == nil {
		t.Fatalf("RatioTolerancePercent: got nil want 5")
	}
	if *g.Pacing.RatioTolerancePercent != 5 {
		t.Errorf("RatioTolerancePercent: got %d want 5", *g.Pacing.RatioTolerancePercent)
	}
}

func TestResolveGroups_MaintainRatioOnBlueGreen(t *testing.T) {
	// maintainRatio guards the cross-Component replica ratio on ANY
	// multi-Component group regardless of progression — blueGreen included (the
	// canonical PD rollout pairs blueGreen with maintainRatio). resolveGroup
	// must map it to RatioBalanced pacing, not silently drop it: the engine's
	// CheckRatio gate keys on Pacing.Type==RatioBalanced, so dropping it here
	// leaves the ratio guard the operator declared completely inert.
	spec := &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				BlueGreen:     &v1beta1.GroupBlueGreen{},
				MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(7))},
			},
		},
	}
	g := ResolveGroups(spec, GroupDefaults{})[0]
	if g.Pacing.Type != v1beta1.CoordinationPacingRatioBalanced {
		t.Fatalf("Pacing.Type: got %q want RatioBalanced", g.Pacing.Type)
	}
	if g.Pacing.RatioTolerancePercent == nil || *g.Pacing.RatioTolerancePercent != 7 {
		t.Errorf("RatioTolerancePercent: got %v want 7", g.Pacing.RatioTolerancePercent)
	}
}

func TestResolveGroups_MaintainRatioIgnoredOnSingleComponent(t *testing.T) {
	// maintainRatio is meaningful only on multi-Component groups — there is no
	// cross-Component ratio for one Component — so the field doc says it is
	// ignored on single-Component groups and the resolved pacing stays
	// PerComponent.
	spec := &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components:    []v1beta1.ComponentType{v1beta1.EngineComponent},
				BlueGreen:     &v1beta1.GroupBlueGreen{},
				MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(7))},
			},
		},
	}
	g := ResolveGroups(spec, GroupDefaults{})[0]
	if g.Pacing.Type != v1beta1.CoordinationPacingPerComponent {
		t.Errorf("single-Component maintainRatio: got %q want PerComponent (ignored)", g.Pacing.Type)
	}
}

func TestResolveGroups_ToleranceDefaultResolution(t *testing.T) {
	// The effective ratio tolerance follows a strict precedence: an explicit
	// per-group tolerance (including 0) wins; an omitted tolerance takes the
	// operator-configured default; with neither, it stays nil — the ratio
	// guard then enforces no drift bound rather than a baked-in number.
	mkSpec := func(tolerance *int32) *v1beta1.RolloutSpec {
		return &v1beta1.RolloutSpec{
			Groups: []v1beta1.RolloutGroup{{
				Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				BlueGreen:     &v1beta1.GroupBlueGreen{},
				MaintainRatio: &v1beta1.MaintainRatio{Tolerance: tolerance},
			}},
		}
	}
	cases := []struct {
		name      string
		tolerance *int32
		defaults  GroupDefaults
		want      *int32
	}{
		{name: "omitted with configured default", tolerance: nil,
			defaults: GroupDefaults{RatioTolerancePercent: ptr.To(int32(9))}, want: ptr.To(int32(9))},
		{name: "omitted without configured default", tolerance: nil,
			defaults: GroupDefaults{}, want: nil},
		{name: "explicit zero beats configured default", tolerance: ptr.To(int32(0)),
			defaults: GroupDefaults{RatioTolerancePercent: ptr.To(int32(9))}, want: ptr.To(int32(0))},
		{name: "explicit value beats configured default", tolerance: ptr.To(int32(7)),
			defaults: GroupDefaults{RatioTolerancePercent: ptr.To(int32(9))}, want: ptr.To(int32(7))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := ResolveGroups(mkSpec(tc.tolerance), tc.defaults)[0]
			if g.Pacing.Type != v1beta1.CoordinationPacingRatioBalanced {
				t.Fatalf("Pacing.Type: got %q want RatioBalanced", g.Pacing.Type)
			}
			got := g.Pacing.RatioTolerancePercent
			switch {
			case tc.want == nil && got != nil:
				t.Errorf("RatioTolerancePercent: got %d want nil", *got)
			case tc.want != nil && got == nil:
				t.Errorf("RatioTolerancePercent: got nil want %d", *tc.want)
			case tc.want != nil && got != nil && *got != *tc.want:
				t.Errorf("RatioTolerancePercent: got %d want %d", *got, *tc.want)
			}
		})
	}
}

func TestResolveGroups_DedupComponentsWithinGroup(t *testing.T) {
	spec := &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{
					v1beta1.EngineComponent,
					v1beta1.EngineComponent,
					v1beta1.DecoderComponent,
				},
				BlueGreen: &v1beta1.GroupBlueGreen{},
			},
		},
	}
	g := ResolveGroups(spec, GroupDefaults{})[0]
	if len(g.Components) != 2 {
		t.Errorf("dedup: got %d components want 2", len(g.Components))
	}
	if g.Components[0] != v1beta1.EngineComponent || g.Components[1] != v1beta1.DecoderComponent {
		t.Errorf("order preserved: got %v", g.Components)
	}
}

func TestResolveGroups_PreservesUserSuppliedPacing(t *testing.T) {
	// The surge budget rides on a rollingUpdate group's MaxSurge. (There is
	// no Independent policy: a Component left out of every group rolls
	// independently with no resolved pacing to assert against, so the
	// surge-budget-flows-through check lives on rollingUpdate.)
	maxSurge := intstr.FromInt(5)
	spec := &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				RollingUpdate: &v1beta1.GroupRollingUpdate{
					MaxSurge: &maxSurge,
				},
			},
		},
	}
	g := ResolveGroups(spec, GroupDefaults{})[0]
	if g.Pacing.MaxSurge.IntValue() != 5 {
		t.Errorf("MaxSurge preserved: got %+v want 5", g.Pacing.MaxSurge)
	}
}

func TestMembershipFor_FindsComponentsAcrossGroups(t *testing.T) {
	// v2: each group declares its own progression. The router's own
	// single-Component blueGreen group is the v2 spelling of "router rolls
	// on its own" (Independent is gone). The 2-Component engine+decoder
	// group keeps the run from collapsing into a Sequential.
	groups := ResolveGroups(&v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
			{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		},
	}, GroupDefaults{})
	if g, ok := MembershipFor(groups, v1beta1.EngineComponent); !ok || g.Policy != v1beta1.CoordinationPolicyBlueGreen {
		t.Errorf("engine: got (%+v, %v) want BlueGreen", g, ok)
	}
	if g, ok := MembershipFor(groups, v1beta1.RouterComponent); !ok || g.Policy != v1beta1.CoordinationPolicyBlueGreen {
		t.Errorf("router: got (%+v, %v) want BlueGreen", g, ok)
	}
}

func TestMembershipFor_UnknownComponentReturnsFalse(t *testing.T) {
	groups := ResolveGroups(&v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		},
	}, GroupDefaults{})
	if _, ok := MembershipFor(groups, v1beta1.DecoderComponent); ok {
		t.Errorf("decoder should not be a member")
	}
}

func TestValidateGroupShape_BlueGreen(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
	}
	if err := ValidateGroupShape(g); err != nil {
		t.Errorf("BlueGreen two-Component: unexpected error: %v", err)
	}
}

func TestValidateGroupShape_RejectsEmptyComponents(t *testing.T) {
	g := ResolvedGroup{
		Name:   "0",
		Policy: v1beta1.CoordinationPolicyBlueGreen,
	}
	if err := ValidateGroupShape(g); err == nil {
		t.Errorf("empty Components: want error")
	}
}

func TestValidateGroupShape_RollingUpdateSingleComponentAllowed(t *testing.T) {
	// v2: rollingUpdate is a per-group progression that is meaningful for a
	// single Component (gradual pod replacement, paced by the per-pod
	// updateStrategy beneath it). The engine drives a one-Component rollingUpdate
	// group cleanly (the surge/ratio gates short-circuit single-Component
	// groups), so the shape is valid — and admission already accepts it, so the
	// runtime net must agree rather than wedge every reconcile.
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicyRollingUpdate,
	}
	if err := ValidateGroupShape(g); err != nil {
		t.Errorf("RollingUpdate single-Component: unexpected error: %v", err)
	}
}

func TestCollapseSequential_LastGroupSoakIgnored(t *testing.T) {
	// Soak is "the wait AFTER this group completes, before the next begins" and
	// is ignored on the LAST group (nothing follows it). collapseSequential folds
	// per-group soaks into one scalar via max(); it must exclude the last group so
	// its soak can't inflate the inter-Component wait.
	spec := &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}, Soak: &metav1.Duration{Duration: 30 * time.Second}},
			{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}, Soak: &metav1.Duration{Duration: 10 * time.Minute}},
		},
	}
	g := ResolveGroups(spec, GroupDefaults{})
	if len(g) != 1 || g[0].Policy != v1beta1.CoordinationPolicySequential {
		t.Fatalf("expected one collapsed Sequential group, got %+v", g)
	}
	if g[0].Soak != 30*time.Second {
		t.Errorf("Soak: got %v want 30s (the last group's 10m must be ignored)", g[0].Soak)
	}
}

func TestValidateGroupShape_SequentialRequiresOrder(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
	}
	if err := ValidateGroupShape(g); err == nil {
		t.Errorf("Sequential without Order: want error")
	}
}

func TestValidateGroupShape_SequentialOrderMustCoverComponents(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent, v1beta1.RouterComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
	}
	if err := ValidateGroupShape(g); err == nil {
		t.Errorf("Sequential partial Order: want error")
	}
}

func TestValidateGroupShape_SequentialOrderMustNotIntroduceForeignComponents(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.RouterComponent},
	}
	if err := ValidateGroupShape(g); err == nil {
		t.Errorf("Sequential Order with foreign Component: want error")
	}
}

func TestValidateGroupShape_RejectsUnknownPolicy(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicy("Bogus"),
	}
	if err := ValidateGroupShape(g); err == nil {
		t.Errorf("unknown policy: want error")
	}
}

func TestValidateGroupShape_SequentialSingleComponent(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
		Order:      []v1beta1.ComponentType{v1beta1.EngineComponent},
	}
	if err := ValidateGroupShape(g); err == nil {
		t.Errorf("Sequential single-Component: want error")
	}
}
