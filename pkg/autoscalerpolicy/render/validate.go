package render

import (
	"fmt"
	"strings"

	"github.com/prometheus/prometheus/promql/parser"
	autoscalingv2 "k8s.io/api/autoscaling/v2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Validation reasons not covered by the policy Ready-condition reasons in
// pkg/apis. Reused by the admission webhook and CI tooling.
const (
	ReasonEnforcementReserved = "EnforcementRequiredReserved"
	ReasonClassTemplate       = "ClassTemplateMismatch"
	ReasonMetricTypeForced    = "DesiredCountRequiresAverageValue"
	ReasonExplicitNullValues  = "IgnoreNullValuesMustBeExplicit"
	ReasonProviderRefRequired = "ProviderRefRequired"
	ReasonValueSourceInvalid  = "ValueSourceInvalid"
)

// Issue is one validation finding. Reason values come from the
// AutoscalerPolicyReason* constants in pkg/apis plus the local constants
// above; Detail is operator-facing.
type Issue struct {
	Reason string
	Detail string
}

func (i Issue) String() string { return fmt.Sprintf("%s: %s", i.Reason, i.Detail) }

// sampleContext is the synthetic context used for admission-time sample
// renders: it exercises every variable so a template that renders here
// renders for any real consumer.
var sampleContext = Context{
	Namespace:   "sample-namespace",
	ISVCName:    "sample-isvc",
	Component:   "engine",
	MinReplicas: 1,
	MaxReplicas: 4,
	TargetName:  "sample-isvc-engine",
}

// ValidateSpec runs every check that needs no cluster state: the reserved
// enforcement shape, the template the class requires, each trigger's rules,
// the fallback's one value source, then a sample render whose rendered
// prometheus queries must parse as PromQL. The parse catches malformed
// queries; findPrecedenceTrap also flags `sum(x) > bool 0 * N`, which parses
// but multiplies inside the comparison instead of scaling its result.
// The returned issues are admission-blocking; an empty slice means valid.
//
// Rendered output also passes the per-ISVC autoscaler validation an inline
// block faces at reconcile time; ValidateSpec is the policy-shaped front
// door, not the only door.
func ValidateSpec(spec *v1beta1.AutoscalerPolicySpec) []Issue {
	var issues []Issue
	// Refusing Required outright means an older controller never meets a
	// Required policy it would read as Default.
	if spec.Enforcement == v1beta1.PolicyEnforcementRequired {
		issues = append(issues, Issue{ReasonEnforcementReserved,
			"enforcement Required is a reserved shape and not implemented; use Default"})
	}
	issues = append(issues, classTemplateIssues(spec)...)
	if spec.Keda == nil {
		return issues
	}
	for i := range spec.Keda.Triggers {
		issues = append(issues, validateTrigger(i, &spec.Keda.Triggers[i])...)
	}
	issues = append(issues, fallbackIssues(spec.Keda.Fallback)...)
	// The sample render gives every trigger a stand-in provider binding, so
	// it tests the templates and the typed derivation, not provider
	// resolution. A policy that fails this render fails for every consumer.
	if len(issues) == 0 {
		issues = append(issues, sampleRender(spec)...)
	}
	return issues
}

// classTemplateIssues requires the template the class names and no other:
// KEDA needs a keda template and forbids an hpa one, HPA forbids a keda
// one, and every other class is inline-only.
func classTemplateIssues(spec *v1beta1.AutoscalerPolicySpec) []Issue {
	var issues []Issue
	switch spec.Class {
	case v1beta1.AutoscalerKEDA:
		if spec.Keda == nil {
			issues = append(issues, Issue{ReasonClassTemplate, "class KEDA requires a keda template"})
		}
		if spec.HPA != nil {
			issues = append(issues, Issue{ReasonClassTemplate, "class KEDA forbids an hpa template"})
		}
	case v1beta1.AutoscalerHPA:
		if spec.Keda != nil {
			issues = append(issues, Issue{ReasonClassTemplate, "class HPA forbids a keda template"})
		}
	default:
		issues = append(issues, Issue{ReasonClassTemplate,
			fmt.Sprintf("class %q is not allowed in policies (External and None are inline-only)", spec.Class)})
	}
	return issues
}

// fallbackIssues requires exactly one source for the fallback replica count.
func fallbackIssues(fallback *v1beta1.FallbackTemplate) []Issue {
	if fallback == nil {
		return nil
	}
	set := 0
	if fallback.Replicas.Value != nil {
		set++
	}
	if fallback.Replicas.FromComponent != nil {
		set++
	}
	if set != 1 {
		return []Issue{{ReasonValueSourceInvalid, "fallback.replicas: exactly one of value or fromComponent must be set"}}
	}
	return nil
}

func validateTrigger(index int, trigger *v1beta1.KedaTriggerTemplate) []Issue {
	var issues []Issue
	for key := range trigger.Metadata {
		if IsForbiddenMetadataKey(key) {
			issues = append(issues, Issue{v1beta1.AutoscalerPolicyReasonForbiddenMetadataKey,
				fmt.Sprintf("trigger %d: metadata key %q is provider-owned; bind endpoints via providerRef", index, key)})
		}
	}
	if IsEndpointTriggerType(trigger.Type) {
		if trigger.ProviderRef == nil || trigger.ProviderRef.Name == "" {
			issues = append(issues, Issue{ReasonProviderRefRequired,
				fmt.Sprintf("trigger %d (%s): providerRef is required for endpoint trigger types", index, trigger.Type)})
		}
		if _, ok := trigger.Metadata["ignoreNullValues"]; !ok {
			issues = append(issues, Issue{ReasonExplicitNullValues,
				fmt.Sprintf("trigger %d (%s): ignoreNullValues must be explicit — the KEDA default (true) silently treats no-series as a healthy zero", index, trigger.Type)})
		}
	}
	if trigger.QueryReturnsDesiredReplicas && trigger.MetricType != autoscalingv2.AverageValueMetricType {
		issues = append(issues, Issue{ReasonMetricTypeForced,
			fmt.Sprintf("trigger %d: queryReturnsDesiredReplicas requires metricType AverageValue — the HPA's Value math is ceil((metric/threshold) x readyPods), which breaks desired-count queries", index)})
	}
	return issues
}

// sampleRender parses + renders every metadata template against the
// synthetic context and PromQL-parses each rendered prometheus query.
func sampleRender(spec *v1beta1.AutoscalerPolicySpec) []Issue {
	var issues []Issue
	if _, err := compileSpec(spec); err != nil {
		return append(issues, Issue{templateIssueReason(err), err.Error()})
	}
	policy := &v1beta1.AutoscalerPolicy{Spec: *spec.DeepCopy()}
	providers := Providers{}
	for i := range spec.Keda.Triggers {
		if ref := spec.Keda.Triggers[i].ProviderRef; ref != nil && ref.Name != "" {
			providers[ref.Name] = ProviderBinding{ServerAddress: "http://sample-provider.invalid"}
		}
	}
	result, err := RenderWithCache(NewCache(), policy, providers, sampleContext)
	if err != nil {
		return append(issues, Issue{v1beta1.AutoscalerPolicyReasonParseError,
			fmt.Sprintf("sample render: %v", err)})
	}

	if keda := result.Autoscaler.Keda; keda != nil {
		for i := range keda.Triggers {
			trigger := &keda.Triggers[i]
			if !IsEndpointTriggerType(trigger.Type) {
				continue
			}
			query, ok := trigger.Metadata["query"]
			if !ok || query == "" {
				continue
			}
			expr, err := parser.ParseExpr(query)
			if err != nil {
				issues = append(issues, Issue{v1beta1.AutoscalerPolicyReasonPromQLInvalid,
					fmt.Sprintf("trigger %d: rendered query does not parse as PromQL: %v", i, err)})
				continue
			}
			if trap := findPrecedenceTrap(expr); trap != "" {
				issues = append(issues, Issue{v1beta1.AutoscalerPolicyReasonPromQLInvalid,
					fmt.Sprintf("trigger %d: %s", i, trap)})
			}
		}
	}
	return issues
}

// templateIssueReason distinguishes parse failures from allowlist rejections
// for condition reporting; both are admission-blocking.
func templateIssueReason(err error) string {
	msg := err.Error()
	if strings.Contains(msg, "forbidden template construct") || strings.Contains(msg, "unknown template variable") {
		return v1beta1.AutoscalerPolicyReasonForbiddenNode
	}
	return v1beta1.AutoscalerPolicyReasonParseError
}

// findPrecedenceTrap walks a parsed query for one shape: a bool-modified
// comparison whose right-hand side is arithmetic over number literals, such
// as `sum(x) > bool 0 * 8`. `*` binds tighter than `>`, so that parses as
// `sum(x) > bool (0*8)` and returns 0 or 1 instead of 0 or 8; the author
// meant `(sum(x) > bool 0) * 8`. Returns an operator-facing message, or "".
func findPrecedenceTrap(expr parser.Expr) string {
	var found string
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if found != "" {
			return nil
		}
		binary, ok := node.(*parser.BinaryExpr)
		if !ok || !binary.ReturnBool || !binary.Op.IsComparisonOperator() {
			return nil
		}
		rhs, ok := binary.RHS.(*parser.BinaryExpr)
		if !ok || rhs.Op.IsComparisonOperator() {
			return nil
		}
		if isNumberLiteral(rhs.LHS) && isNumberLiteral(rhs.RHS) {
			found = fmt.Sprintf("comparison %q folds the arithmetic into its threshold (%q) — parenthesize the comparison, e.g. `(sum(...) %s bool N) * M`",
				binary.String(), rhs.String(), binary.Op)
		}
		return nil
	})
	return found
}

func isNumberLiteral(expr parser.Expr) bool {
	switch typed := expr.(type) {
	case *parser.NumberLiteral:
		return true
	case *parser.StepInvariantExpr:
		return isNumberLiteral(typed.Expr)
	case *parser.ParenExpr:
		return isNumberLiteral(typed.Expr)
	default:
		return false
	}
}
