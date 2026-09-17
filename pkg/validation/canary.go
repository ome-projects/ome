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
// KIND is canary — an inline canary block or a policyRef declaring canary
// (inline wins when both are set). Returns nil when none (nil-safe). The CRD
// CEL rule enforces the one-of progression (canary may span multiple
// Components, primary-driven); these are the value-level rules CEL can't
// express. The shape rules below apply to every canary-kind group; the plan
// body is checked only when inline — a referenced body is validated at
// policy admission and again at run open. Each rule guards a concrete
// failure:
//   - Steps non-empty — an empty plan has nothing to execute.
//   - Every Component in the group is OMENative — other modes wedge at Pending.
//   - Final step Traffic == 100 — otherwise the rollout never fully cuts over.
//   - Final step Capacity == 100% — otherwise it completes on a partial fleet.
//   - Every step Capacity parses strictly (integer >= 0, or "<N>%" with N in
//     [0,100]) — the runtime resolver maps unparsable values to zero, so a
//     malformed capacity would silently stage no new pods.
//   - Traffic in [0,100], non-decreasing, and Traffic>0 requires Capacity>0.
//   - Each analysis step's config is complete and well-formed (delegated to
//     validateCanaryAnalysis).
func ValidateCanary(spec *v1beta1.InferenceServiceSpec) error {
	groups := spec.GetRolloutGroups()
	modeFor := omenativeDeploymentModeMap(spec)
	declared := declaredComponents(spec)
	// One canary run per unit. Two canary groups may coexist — a router run and
	// an engine run advance independently on disjoint Components — but two runs
	// driving the same unit would each believe they own its revision and step
	// counter, and would fight over status.components[c].traffic. A duplicate
	// entry there fails apiserver validation for the whole status write, which
	// freezes every subsequent reconcile for the InferenceService.
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
		for _, comp := range g.Components {
			// A canary group's Components must be valid and declared on the ISVC:
			// primaryComponent picks router>engine>decoder among them, so an invalid
			// or undeclared member becomes a phantom primary that wedges the roll.
			if _, ok := validComponents[comp]; !ok {
				return fmt.Errorf("spec.rollout.groups[%d]: canary component %q is not one of router/engine/decoder (%s)",
					gi, comp, ReasonCanaryInvalid)
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
		// A canary group must contain the entrypoint of every unit it touches:
		// the router for the router unit, the engine for the engine unit. The
		// unit entrypoint is what primaryComponent drives the step machine and
		// traffic weight through, so a group that stages a secondary without its
		// entrypoint has no Component carrying the roll.
		//
		// An engine unit moves real traffic without owning the external
		// entrypoint: the router discovers engine endpoints by label selector
		// with no revision term, so a step that puts a fraction of engine
		// replicas on the new revision moves roughly that fraction of requests.
		// The split is capacity-driven and mediated by the router's balancing
		// rather than exact, which is why capacity — not traffic — is the
		// contract for a non-entrypoint unit.
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
		// The engine unit is engine+decoder whenever the ISVC declares both.
		// Splitting the pair across a canary boundary can leave a new-protocol
		// prefill with no pairable decode, which fails requests outright.
		_, hasEngineSpec := declared[v1beta1.EngineComponent]
		_, hasDecoderSpec := declared[v1beta1.DecoderComponent]
		if hasEngineSpec && hasDecoderSpec &&
			has[v1beta1.EngineComponent] != has[v1beta1.DecoderComponent] {
			return fmt.Errorf("spec.rollout.groups[%d]: this InferenceService declares both engine and decoder, so a canary group naming either must name both — they roll as one unit (%s)",
				gi, ReasonCanaryInvalid)
		}
		// MaintainRatio is silently ignored by the canary engine (it gates only
		// coordination-style pacing). Reject rather than no-op so an operator is not
		// misled into thinking the cross-Component ratio is guarded during the canary.
		if g.MaintainRatio != nil {
			return fmt.Errorf("spec.rollout.groups[%d]: maintainRatio is not honored on a canary group (the canary engine does not enforce it) — remove it, or use a blueGreen/rollingUpdate group for ratio-guarded rollout (%s)",
				gi, ReasonCanaryInvalid)
		}
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
