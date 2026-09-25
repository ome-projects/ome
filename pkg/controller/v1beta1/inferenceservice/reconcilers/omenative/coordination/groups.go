package coordination

import (
	"fmt"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils"
)

// ResolvedGroup carries the spec-derived view of one coordination
// group, after defaults are applied and the group is indexed.
type ResolvedGroup struct {
	// Name is the stable identifier for the group, derived from the
	// group's position in spec.rollout.groups[].
	Name string

	// Index is the group's position in
	// spec.rollout.groups[].
	Index int

	// Components is the deduplicated, order-preserving Component set
	// for the group.
	Components []v1beta1.ComponentType

	// Policy is the coordination policy after spec resolution.
	Policy v1beta1.CoordinationPolicy

	// Order pins the in-group sequence. Required for Sequential;
	// optional for BlueGreen; nil for Independent / RollingUpdate.
	Order []v1beta1.ComponentType

	// Pacing is the effective pacing block after defaults are applied.
	// Never nil.
	Pacing v1beta1.CoordinationPacing

	// Soak is the duration the Sequential state machine waits after a
	// Component completes its rollout before starting the next
	// Component in Order. Zero means "no wait." Only honored for
	// Policy=Sequential; ignored for every other policy.
	Soak time.Duration
}

// GroupDefaults carries the operator-configured fill-in values applied while
// resolving spec.rollout groups. The zero value means "nothing configured":
// each knob degrades to its documented unconfigured behavior instead of a
// baked-in literal.
type GroupDefaults struct {
	// RatioTolerancePercent fills MaintainRatio.Tolerance when a group sets
	// maintainRatio but omits the tolerance (the coordination block of the
	// operator's ConfigMap supplies the value). Nil means unconfigured: an
	// omitted tolerance resolves to nil and the ratio guard enforces no
	// drift bound.
	RatioTolerancePercent *int32
}

// ResolveGroups turns spec.rollout into a slice of ResolvedGroups in declaration
// order, for the COORDINATION-style progressions only (blueGreen / rollingUpdate).
// Canary groups are driven by the canary engine, not coordination, so they are
// skipped here. Returns nil when no rollout (or no coordination-style group) is
// declared. defaults carries the operator-configured fill-ins; callers without
// access to operator configuration pass the zero value and get the documented
// unconfigured behavior.
func ResolveGroups(spec *v1beta1.RolloutSpec, defaults GroupDefaults) []ResolvedGroup {
	if spec == nil || len(spec.Groups) == 0 {
		return nil
	}
	out := make([]ResolvedGroup, 0, len(spec.Groups))
	for i := range spec.Groups {
		g := &spec.Groups[i]
		// Keyed on the DECLARED progression, not an inline body: a group whose
		// canary comes from a policyRef has no inline arm to test, and reading
		// it as blueGreen would hand a gated Component to the coordination
		// engine and surge it with nothing analysing the result.
		if g.DeclaredProgression() == v1beta1.RolloutProgressionCanary {
			continue // canary groups are the canary engine's responsibility
		}
		out = append(out, resolveGroup(i, g, defaults))
	}
	// v2 ordered groups roll one at a time, in list order. When the rollout is a
	// run of single-Component blueGreen groups, that is exactly v1 Sequential —
	// collapse them into one Sequential ResolvedGroup so the proven Sequential
	// state machine (per-Component blue-green surge, soak, hold-the-rest) drives
	// them. (Mixed-progression / multi-Component ordered groups don't collapse and
	// run independently for now — general cross-group ordering is a follow-on.)
	if seq, ok := collapseSequential(out, defaults); ok {
		return []ResolvedGroup{seq}
	}
	return out
}

// collapseSequential turns a list of 2+ single-Component blueGreen ResolvedGroups
// (the v2 spelling of v1 Sequential) into one Sequential ResolvedGroup whose Order
// is the Components in list order. Returns (zero,false) when the list isn't a pure
// single-Component-blueGreen sequence — those groups keep their own resolution.
func collapseSequential(groups []ResolvedGroup, defaults GroupDefaults) (ResolvedGroup, bool) {
	if len(groups) < 2 {
		return ResolvedGroup{}, false
	}
	order := make([]v1beta1.ComponentType, 0, len(groups))
	var soak time.Duration
	for i := range groups {
		g := &groups[i]
		if g.Policy != v1beta1.CoordinationPolicyBlueGreen || len(g.Components) != 1 {
			return ResolvedGroup{}, false
		}
		order = append(order, g.Components[0])
		// Soak is the wait AFTER a group before the next, so the LAST group's soak
		// is ignored (nothing follows it). The engine carries one Soak between
		// every Component, so the rest collapse to max().
		if i < len(groups)-1 && g.Soak > soak {
			soak = g.Soak
		}
	}
	return ResolvedGroup{
		Name:       strconv.Itoa(0),
		Index:      0,
		Components: append([]v1beta1.ComponentType(nil), order...),
		Policy:     v1beta1.CoordinationPolicySequential,
		Order:      order,
		Pacing:     pacingWithDefaults(nil, defaults),
		Soak:       soak,
	}, true
}

// resolveGroup maps one v2 RolloutGroup (blueGreen | rollingUpdate) onto the
// engine's internal ResolvedGroup. A v2 group rolls its Components together, so
// the resolved policy is BlueGreen or RollingUpdate — Sequential-across-Components
// is now expressed as separate ordered groups, not a policy. MaintainRatio maps to
// RatioBalanced pacing.
func resolveGroup(idx int, g *v1beta1.RolloutGroup, defaults GroupDefaults) ResolvedGroup {
	components := dedupComponents(g.Components)
	out := ResolvedGroup{
		Name:       strconv.Itoa(idx),
		Index:      idx,
		Components: components,
		Order:      append([]v1beta1.ComponentType(nil), g.Order...),
	}
	if g.Soak != nil {
		out.Soak = g.Soak.Duration
	}
	// The DECLARED progression, so a ref-only group resolves to the policy it
	// names rather than to the no-progression default. Canary groups were
	// already skipped by the caller; rollingUpdate and blueGreen are the two
	// coordination-style kinds left. "No progression" and explicit blueGreen
	// share the blueGreen path, which is what lets collapseSequential fold a
	// run of single-Component groups into the Sequential state machine.
	if g.DeclaredProgression() == v1beta1.RolloutProgressionRollingUpdate {
		out.Policy = v1beta1.CoordinationPolicyRollingUpdate
	} else {
		out.Policy = v1beta1.CoordinationPolicyBlueGreen
	}
	out.Pacing = groupPacing(g, len(components), defaults)
	return out
}

// groupPacing builds the internal pacing for a v2 group: the surge/unavailable
// budget from a rollingUpdate group, plus RatioBalanced when MaintainRatio is set
// on a multi-Component group. MaintainRatio maps regardless of progression
// (blueGreen included) because the engine's ratio guard keys on
// Pacing.Type==RatioBalanced, not the policy — mapping it only for rollingUpdate
// would leave a blueGreen group's guard inert. Ignored on single-Component groups.
// An explicit Tolerance (including 0) is copied verbatim; an omitted Tolerance is
// left nil for pacingWithDefaults to fill from the operator-configured default.
func groupPacing(g *v1beta1.RolloutGroup, componentCount int, defaults GroupDefaults) v1beta1.CoordinationPacing {
	in := &v1beta1.CoordinationPacing{}
	if g.RollingUpdate != nil {
		in.MaxSurge = g.RollingUpdate.MaxSurge
		in.MaxUnavailable = g.RollingUpdate.MaxUnavailable
	}
	if g.MaintainRatio != nil && componentCount >= 2 {
		in.Type = v1beta1.CoordinationPacingRatioBalanced
		if g.MaintainRatio.Tolerance != nil {
			t := *g.MaintainRatio.Tolerance
			in.RatioTolerancePercent = &t
		}
	}
	return pacingWithDefaults(in, defaults)
}

// pacingWithDefaults applies the pacing default fills (25% surge, 25%
// unavailable, PerComponent type, and — for RatioBalanced — the
// operator-configured ratio tolerance). The ratio tolerance has no in-code
// fallback: when the group omits it and the operator configured no default,
// RatioTolerancePercent stays nil and the ratio guard enforces no drift bound.
func pacingWithDefaults(in *v1beta1.CoordinationPacing, defaults GroupDefaults) v1beta1.CoordinationPacing {
	out := v1beta1.CoordinationPacing{}
	if in != nil {
		out = *in.DeepCopy()
	}
	if out.Type == "" {
		out.Type = v1beta1.CoordinationPacingPerComponent
	}
	if out.MaxSurge == nil {
		out.MaxSurge = utils.PtrIntOrStringFromString("25%")
	}
	if out.MaxUnavailable == nil {
		// 25% matches K8s Deployment convention. A default of 0
		// deadlocks single-replica rollouts because the gate would
		// refuse to take the only pod offline; 25% gives at-least-1
		// budget at any positive replica count via the floor-rounded
		// MaxUnavailableBudget math.
		out.MaxUnavailable = utils.PtrIntOrStringFromString("25%")
	}
	if out.Type == v1beta1.CoordinationPacingRatioBalanced &&
		out.RatioTolerancePercent == nil && defaults.RatioTolerancePercent != nil {
		t := *defaults.RatioTolerancePercent
		out.RatioTolerancePercent = &t
	}
	return out
}

// dedupComponents preserves declaration order and removes duplicate
// Components within a single group. Cross-group uniqueness is the
// admission webhook's job; this is the within-group safety belt for
// resolved code paths.
func dedupComponents(in []v1beta1.ComponentType) []v1beta1.ComponentType {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[v1beta1.ComponentType]struct{}, len(in))
	out := make([]v1beta1.ComponentType, 0, len(in))
	for _, c := range in {
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	return out
}

// MembershipFor returns the ResolvedGroup that owns the given Component,
// or (zero, false) when the Component rolls independently.
func MembershipFor(groups []ResolvedGroup, c v1beta1.ComponentType) (ResolvedGroup, bool) {
	for _, g := range groups {
		for _, gc := range g.Components {
			if gc == c {
				return g, true
			}
		}
	}
	return ResolvedGroup{}, false
}

// ValidateGroupShape returns a non-nil error when g is internally
// inconsistent (Sequential without Order, Sequential single-Component,
// etc.). This is the runtime safety net for the admission webhook —
// the controller refuses to act on a group that the webhook would
// have rejected, in case the object was created via a path that
// bypassed validation.
func ValidateGroupShape(g ResolvedGroup) error {
	switch g.Policy {
	case v1beta1.CoordinationPolicyBlueGreen,
		v1beta1.CoordinationPolicyIndependent,
		v1beta1.CoordinationPolicyRollingUpdate,
		v1beta1.CoordinationPolicySequential:
	default:
		return fmt.Errorf("group %s: unknown policy %q", g.Name, g.Policy)
	}

	if len(g.Components) == 0 {
		return fmt.Errorf("group %s: empty components", g.Name)
	}

	// Single-Component rollingUpdate is valid (admission accepts it; the engine
	// drives one Component cleanly), so there is no min-Component check here.

	if g.Policy == v1beta1.CoordinationPolicySequential {
		if len(g.Components) < 2 {
			return fmt.Errorf("group %s: Sequential requires at least 2 components", g.Name)
		}
		if len(g.Order) == 0 {
			return fmt.Errorf("group %s: Sequential requires order", g.Name)
		}
		if len(g.Order) != len(g.Components) {
			return fmt.Errorf("group %s: Sequential order must cover every component", g.Name)
		}
		want := make(map[v1beta1.ComponentType]struct{}, len(g.Components))
		for _, c := range g.Components {
			want[c] = struct{}{}
		}
		for _, c := range g.Order {
			if _, ok := want[c]; !ok {
				return fmt.Errorf("group %s: order entry %q is not in components", g.Name, c)
			}
			delete(want, c)
		}
		// RatioBalanced is meaningless for Sequential — only one
		// Component is in flight at a time, so there's no cross-Component
		// ratio to balance. Reject so operators see the conflict at
		// admission instead of getting silently downgraded pacing.
		if g.Pacing.Type == v1beta1.CoordinationPacingRatioBalanced {
			return fmt.Errorf("group %s: Sequential cannot use RatioBalanced pacing — only one Component rolls at a time", g.Name)
		}
	}

	// maxSurge=0 AND maxUnavailable=0 deadlocks any rollout (no surge
	// headroom, no drain headroom). Applies to every policy that uses
	// pacing for per-Component bumps (BlueGreen, Independent,
	// RollingUpdate); Sequential's per-Component pacing inherits the
	// same check.
	if g.Pacing.Type == v1beta1.CoordinationPacingPerComponent {
		if pacingZeroSurge(g.Pacing) && pacingZeroUnavailable(g.Pacing) {
			return fmt.Errorf("group %s: maxSurge=0 AND maxUnavailable=0 deadlocks rollouts — set at least one non-zero", g.Name)
		}
	}
	return nil
}

// pacingZeroSurge reports whether the resolved pacing's MaxSurge
// computes to 0 for any non-zero replica count (i.e., MaxSurge is
// literally 0 or 0%, not just a percent that rounds to 0 at low
// replica counts).
func pacingZeroSurge(p v1beta1.CoordinationPacing) bool {
	if p.MaxSurge == nil {
		return false
	}
	if p.MaxSurge.Type == intstr.Int {
		return p.MaxSurge.IntValue() == 0
	}
	return p.MaxSurge.StrVal == "0%" || p.MaxSurge.StrVal == "0"
}

// pacingZeroUnavailable mirrors pacingZeroSurge for MaxUnavailable.
// Nil is treated as zero defensively; in practice the resolved group
// has defaults applied via pacingWithDefaults before this is called.
func pacingZeroUnavailable(p v1beta1.CoordinationPacing) bool {
	if p.MaxUnavailable == nil {
		return true
	}
	if p.MaxUnavailable.Type == intstr.Int {
		return p.MaxUnavailable.IntValue() == 0
	}
	return p.MaxUnavailable.StrVal == "0%" || p.MaxUnavailable.StrVal == "0"
}
