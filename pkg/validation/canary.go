package validation

import (
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// ReasonCanaryInvalid is the admission rejection reason for a malformed canary
// progression on a spec.rollout.groups[] entry. Operators and tests reference it.
const ReasonCanaryInvalid = "CanaryInvalid"

// ReasonCanaryRequiresOMENative is the rejection reason for a canary group whose
// Component is not OMENative. Canary relies on per-revision pod selection,
// partition staging, and the InferenceReplica object — all OMENative/IR-managed
// constructs — so other deployment modes would silently wedge at Pending.
const ReasonCanaryRequiresOMENative = "CanaryRequiresOMENative"

// ReasonMultipleCanaryGroups is the rejection reason for a spec.rollout whose
// canary groups overlap on a unit. Independent units may each carry a run, but
// two runs driving one unit would contend for its revision, step counter and
// traffic entry, and a duplicate traffic entry fails apiserver validation for
// the entire status write.
const ReasonMultipleCanaryGroups = "MultipleCanaryGroups"

// ReasonAnalysisInvalid is the rejection reason for a malformed metric-gated step
// (a step's analysis). It guards the spec-internal rules a pure validator can
// check: every analysis step has a complete config — at least one metric, a
// numeric threshold, a valid operator, a positive interval, and a failure limit
// >= 1. The metrics source (GroupCanary.Prometheus) is environmental and is
// resolved controller-side, not checked here.
const ReasonAnalysisInvalid = "AnalysisInvalid"

// ValidateCanary checks every spec.rollout.groups[] entry whose progression
// kind is canary: an inline canary block, or a policyRef declaring canary
// (inline wins when both are set). Nil-safe; returns nil when there is none.
// The CRD's CEL rule keeps the progression one-of; these are the value rules
// CEL cannot express, run in this order: one canary run per unit across the
// groups, then per group the plan body when it is inline (a referenced body
// is checked at policy admission and again at run open), the members, the
// units, and no maintainRatio.
func ValidateCanary(spec *v1beta1.InferenceServiceSpec) error {
	groups := spec.GetRolloutGroups()
	if err := validateOneCanaryRunPerUnit(groups); err != nil {
		return err
	}
	modeFor := omenativeDeploymentModeMap(spec)
	declared := declaredComponents(spec)
	for gi := range groups {
		g := &groups[gi]
		if rolloutGroupKind(g) != v1beta1.RolloutProgressionCanary {
			continue
		}
		if g.Canary != nil {
			if err := ValidateCanaryPlan(fmt.Sprintf("spec.rollout.groups[%d].canary", gi), g.Canary); err != nil {
				return err
			}
		}
		if err := validateCanaryMembers(gi, g, declared, modeFor); err != nil {
			return err
		}
		if err := validateCanaryUnits(gi, g, declared); err != nil {
			return err
		}
		// The canary engine does not enforce maintainRatio, so a group that
		// sets it is refused rather than run without the guard it asked for.
		if g.MaintainRatio != nil {
			return fmt.Errorf("spec.rollout.groups[%d]: maintainRatio is not honored on a canary group (the canary engine does not enforce it) — remove it, or use a blueGreen/rollingUpdate group for ratio-guarded rollout (%s)",
				gi, ReasonCanaryInvalid)
		}
	}
	return nil
}

// validateOneCanaryRunPerUnit refuses a unit that two canary groups drive.
// Groups on different units may each carry a run; one unit carries at most
// one (ReasonMultipleCanaryGroups says why).
func validateOneCanaryRunPerUnit(groups []v1beta1.RolloutGroup) error {
	ownerOf := map[v1beta1.ComponentType]int{}
	for gi := range groups {
		if rolloutGroupKind(&groups[gi]) != v1beta1.RolloutProgressionCanary {
			continue
		}
		for _, u := range rollout.CanaryUnitsOf(&groups[gi]) {
			if prev, taken := ownerOf[u]; taken {
				return fmt.Errorf("spec.rollout.groups: canary unit %q is driven by both groups[%d] and groups[%d]; a unit may have at most one canary run (%s)",
					u, prev, gi, ReasonMultipleCanaryGroups)
			}
			ownerOf[u] = gi
		}
	}
	return nil
}

// validateCanaryMembers checks every member of canary group gi: a known
// Component, declared on the InferenceService, and OMENative. The primary
// Component is picked among the members, and only OMENative has the
// per-revision pods and the InferenceReplica the steps drive.
func validateCanaryMembers(gi int, g *v1beta1.RolloutGroup, declared map[v1beta1.ComponentType]struct{}, modeFor map[v1beta1.ComponentType]string) error {
	for _, comp := range g.Components {
		if !isRole(comp) {
			return fmt.Errorf("spec.rollout.groups[%d]: canary component %q is not one of %s (%s)",
				gi, comp, roleNames("/"), ReasonCanaryInvalid)
		}
		if _, ok := declared[comp]; !ok {
			return fmt.Errorf("spec.rollout.groups[%d]: canary component %q is not declared on the InferenceService (%s)",
				gi, comp, ReasonCanaryInvalid)
		}
		if m, ok := modeFor[comp]; ok && m != string(constants.OMENative) {
			return fmt.Errorf("spec.rollout.groups[%d]: component %q has deploymentMode=%q; canary requires OMENative (%s)",
				gi, comp, m, ReasonCanaryRequiresOMENative)
		}
	}
	return nil
}

// validateCanaryUnits checks that canary group gi names the entrypoint
// Component of every unit it touches (the router for the router unit, the
// engine for the engine unit), since the step machine and the traffic weight
// are driven through it. The roles that pair (engine, decoder, encoder) are
// one unit, so when the InferenceService declares two or more of them a group
// names all or none: a new-protocol prefill with no pairable peer fails
// requests.
func validateCanaryUnits(gi int, g *v1beta1.RolloutGroup, declared map[v1beta1.ComponentType]struct{}) error {
	has := map[v1beta1.ComponentType]bool{}
	for _, comp := range g.Components {
		has[comp] = true
	}
	for _, u := range rollout.CanaryUnitsOf(g) {
		if !has[u] {
			return fmt.Errorf("spec.rollout.groups[%d]: a canary group covering the %q unit must include its entrypoint Component %q — that is what the step machine and traffic weight are driven through (%s)",
				gi, u, u, ReasonCanaryInvalid)
		}
	}
	var pairing []string
	named := 0
	for _, r := range v1beta1.Roles {
		if _, ok := declared[r.Type]; !ok || !r.Pairs {
			continue
		}
		pairing = append(pairing, string(r.Type))
		if has[r.Type] {
			named++
		}
	}
	if len(pairing) >= 2 && named > 0 && named < len(pairing) {
		declares, either, all := "both "+pairing[0]+" and "+pairing[1], "either", "both"
		if len(pairing) > 2 {
			declares = strings.Join(pairing[:len(pairing)-1], ", ") + " and " + pairing[len(pairing)-1]
			either, all = "any of them", "all of them"
		}
		return fmt.Errorf("spec.rollout.groups[%d]: this InferenceService declares %s, so a canary group naming %s must name %s — they roll as one unit (%s)",
			gi, declares, either, all, ReasonCanaryInvalid)
	}
	return nil
}

// ValidateCanaryPlan enforces the plan-body rules of one canary progression —
// the rules that hold independent of the consumer's component shape, so the
// same function validates an inline group's canary, a RolloutPolicy body, and
// the composed effective plan at run open. where names the body's field path
// in errors (e.g. "spec.rollout.groups[2].canary" or, for a policy,
// "spec.canary").
func ValidateCanaryPlan(where string, c *v1beta1.GroupCanary) error {
	if len(c.Steps) == 0 {
		return fmt.Errorf("%s.steps must not be empty (%s)", where, ReasonCanaryInvalid)
	}
	if err := validateCanaryAnalysis(where, c); err != nil {
		return err
	}
	if c.ReadyTimeout != nil && c.ReadyTimeout.Duration <= 0 {
		return fmt.Errorf("%s.readyTimeout must be > 0 when set (%s)", where, ReasonCanaryInvalid)
	}
	last := c.Steps[len(c.Steps)-1]
	if last.Traffic != 100 {
		return fmt.Errorf("%s final step traffic must be 100, got %d (%s)", where, last.Traffic, ReasonCanaryInvalid)
	}
	var prevWeight int32
	for i, s := range c.Steps {
		// Strict capacity parsing: the runtime resolver maps anything it cannot
		// parse to zero new capacity, so admission must reject every such form
		// instead of letting a step silently stage nothing.
		val, isPercent, perr := parseCanaryCapacity(s.Capacity)
		if perr != nil {
			return fmt.Errorf("%s.steps[%d].capacity: %v (%s)", where, i, perr, ReasonCanaryInvalid)
		}
		if s.Traffic < 0 || s.Traffic > 100 {
			return fmt.Errorf("%s.steps[%d].traffic: traffic %d must be in [0,100] (%s)", where, i, s.Traffic, ReasonCanaryInvalid)
		}
		if s.Traffic < prevWeight {
			return fmt.Errorf("%s.steps[%d].traffic: traffic %d must be >= the previous step's %d (canary weights are non-decreasing) (%s)",
				where, i, s.Traffic, prevWeight, ReasonCanaryInvalid)
		}
		prevWeight = s.Traffic
		if s.Traffic > 0 && val == 0 {
			return fmt.Errorf("%s.steps[%d].capacity: capacity %q resolves to zero new capacity but traffic is %d — a step cannot send traffic to zero capacity (%s)",
				where, i, s.Capacity.String(), s.Traffic, ReasonCanaryInvalid)
		}
		// Only the percentage form of the final capacity is checkable at
		// admission (absolute counts need the desired replica count); the done
		// sentinel also forces partition 0.
		if i == len(c.Steps)-1 && isPercent && val != 100 {
			return fmt.Errorf("%s.steps[%d].capacity: final step capacity must be 100%% (rollout completes at full capacity), got %q (%s)",
				where, i, s.Capacity.String(), ReasonCanaryInvalid)
		}
	}
	return nil
}

// validateCanaryAnalysis enforces the spec-internal rules for each metric-gated
// step: a positive interval, failureLimit >= 1, and at least one
// well-formed metric. The metrics source (GroupCanary.Prometheus) is not checked
// here — an unreachable source is surfaced controller-side as an inconclusive
// sample, not an admission error.
func validateCanaryAnalysis(where string, c *v1beta1.GroupCanary) error {
	for si := range c.Steps {
		a := c.Steps[si].Analysis
		if a == nil {
			continue
		}
		if a.Interval.Duration <= 0 {
			return fmt.Errorf("%s.steps[%d].analysis: interval must be > 0 (%s)",
				where, si, ReasonAnalysisInvalid)
		}
		if a.FailureLimit < 1 {
			return fmt.Errorf("%s.steps[%d].analysis: failureLimit must be >= 1 (%s)",
				where, si, ReasonAnalysisInvalid)
		}
		if len(a.Metrics) == 0 {
			return fmt.Errorf("%s.steps[%d].analysis: at least one metric is required (%s)",
				where, si, ReasonAnalysisInvalid)
		}
		if err := validateAnalysisMetrics(fmt.Sprintf("%s.steps[%d].analysis", where, si), a.Metrics); err != nil {
			return err
		}
	}
	return nil
}

// validateAnalysisMetrics checks each metric is well-formed: unique non-empty
// name, non-empty query, numeric threshold, valid operator. where names the
// enclosing analysis block's field path.
func validateAnalysisMetrics(where string, metrics []v1beta1.AnalysisMetric) error {
	seen := make(map[string]struct{}, len(metrics))
	for mi := range metrics {
		m := &metrics[mi]
		if m.Name == "" {
			return fmt.Errorf("%s.metrics[%d].name must not be empty (%s)", where, mi, ReasonAnalysisInvalid)
		}
		if _, dup := seen[m.Name]; dup {
			return fmt.Errorf("%s.metrics[%d]: duplicate metric name %q (%s)", where, mi, m.Name, ReasonAnalysisInvalid)
		}
		seen[m.Name] = struct{}{}
		if strings.TrimSpace(m.Query) == "" {
			return fmt.Errorf("%s.metrics[%d] (%q): query must not be empty (%s)", where, mi, m.Name, ReasonAnalysisInvalid)
		}
		if _, err := strconv.ParseFloat(m.Threshold, 64); err != nil {
			return fmt.Errorf("%s.metrics[%d] (%q): threshold %q must be numeric (%s)", where, mi, m.Name, m.Threshold, ReasonAnalysisInvalid)
		}
		switch m.Operator {
		case v1beta1.ComparisonLT, v1beta1.ComparisonLTE, v1beta1.ComparisonGT, v1beta1.ComparisonGTE:
		default:
			return fmt.Errorf("%s.metrics[%d] (%q): operator %q must be one of LT/LTE/GT/GTE (%s)", where, mi, m.Name, m.Operator, ReasonAnalysisInvalid)
		}
	}
	return nil
}

// parseCanaryCapacity strictly resolves a step's Capacity to its numeric value.
// A string must be "<N>%" with integer N in [0,100]; an integer must be >= 0.
// Everything else is rejected: the runtime int/percent resolver maps a value it
// cannot parse to zero, so an admission-time reject here is the only thing
// standing between a typo and a step that silently stages no new capacity.
// Absolute counts must use the integer form — a quoted count like "3" is not a
// percentage and would resolve to zero at runtime.
func parseCanaryCapacity(c intstr.IntOrString) (value int, isPercent bool, err error) {
	if c.Type == intstr.Int {
		n := c.IntValue()
		if n < 0 {
			return 0, false, fmt.Errorf("capacity %d must not be negative", n)
		}
		return n, false, nil
	}
	raw := c.StrVal
	if !strings.HasSuffix(raw, "%") {
		return 0, false, fmt.Errorf("capacity %q is not a percentage — use \"<N>%%\" for a fraction of desired replicas, or an unquoted integer for an absolute count", raw)
	}
	n, atoiErr := strconv.Atoi(strings.TrimSuffix(raw, "%"))
	if atoiErr != nil {
		return 0, true, fmt.Errorf("capacity %q is not a valid percentage — use \"<N>%%\" with integer N in [0,100]", raw)
	}
	if n < 0 {
		return 0, true, fmt.Errorf("capacity %q must not be negative", raw)
	}
	if n > 100 {
		return 0, true, fmt.Errorf("capacity %q must not exceed 100%% (capacity is a fraction of desired replicas)", raw)
	}
	return n, true, nil
}
