// Package affinity compiles cluster selection independently of member health.
package affinity

import (
	"cmp"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Match records why a cluster matched and its effective replica weight.
type Match struct {
	TermIndexes []int32
	Weight      int64
	// MaxWeight is the sum of the matching terms' maximum ratios, with one for
	// a term that sets none. It is zero unless some term sets a replica ratio.
	MaxWeight int64
}

// Selector is an immutable compiled affinity. Its zero value matches nothing.
type Selector struct {
	unconstrained bool
	weighted      bool
	ratios        bool
	terms         []term
}

type term struct {
	labels    labels.Selector
	fields    []v1beta1.ClusterSelectorRequirement
	weight    int64
	maxWeight int64
}

// Compile validates and snapshots an affinity whose matches carry the engine's
// weight. Nil means no restriction; an explicitly empty list is invalid.
// allowWeights is true only for static Split.
func Compile(terms []v1beta1.ClusterAffinityTerm, allowWeights bool) (*Selector, error) {
	return CompileFor(terms, allowWeights, v1beta1.EngineComponent)
}

// CompileFor is Compile with matches that carry the named component's weight.
// Every component matches the same clusters.
func CompileFor(terms []v1beta1.ClusterAffinityTerm, allowWeights bool, component v1beta1.ComponentType) (*Selector, error) {
	if terms == nil {
		return &Selector{unconstrained: true}, nil
	}
	if len(terms) == 0 || len(terms) > v1beta1.MaxClusterAffinityTerms {
		return nil, fmt.Errorf("clusterAffinity must contain between 1 and %d terms", v1beta1.MaxClusterAffinityTerms)
	}
	selector := &Selector{}
	for i, input := range terms {
		compiled, err := compileTerm(input, allowWeights)
		if err != nil {
			return nil, fmt.Errorf("clusterAffinity[%d]: %w", i, err)
		}
		if ratio := termRatio(input.Weights, component); ratio != nil {
			selector.weighted = true
			compiled.weight, compiled.maxWeight = int64(ratio.MinOrOne()), int64(ratio.MaxOrOne())
		}
		selector.ratios = selector.ratios || usesRatios(input.Weights)
		selector.terms = append(selector.terms, compiled)
	}
	return selector, nil
}

// termRatio is the replica ratio a term's weights block gives a component, or
// nil when it gives none. The engine takes engine, then default; the decoder
// and the router take their own key, then default, then the engine's.
func termRatio(w *v1beta1.ClusterAffinityWeights, component v1beta1.ComponentType) *v1beta1.ReplicaRatio {
	if w == nil {
		return nil
	}
	engine := cmp.Or(w.Engine, w.Default)
	switch component {
	case v1beta1.EngineComponent:
		return engine
	case v1beta1.DecoderComponent:
		return cmp.Or(w.Decoder, w.Default, engine)
	case v1beta1.RouterComponent:
		return cmp.Or(w.Router, w.Default, engine)
	}
	return nil
}

// usesRatios reports whether a weights block sets any key.
func usesRatios(w *v1beta1.ClusterAffinityWeights) bool {
	return w != nil && slices.ContainsFunc(weightKeys(w), func(key weightKey) bool { return key.value != nil })
}

// UsesRatios reports whether any term sets a weight key.
func UsesRatios(terms []v1beta1.ClusterAffinityTerm) bool {
	return slices.ContainsFunc(terms, func(t v1beta1.ClusterAffinityTerm) bool { return usesRatios(t.Weights) })
}

// DividesRouters reports whether static Split divides routers across the
// matched clusters: when no term sets weights, or when a term sets a default
// or router ratio. Ratios that name only the engine or decoder keep the full
// router policy on each cluster.
func DividesRouters(terms []v1beta1.ClusterAffinityTerm) bool {
	if !UsesRatios(terms) {
		return true
	}
	return slices.ContainsFunc(terms, func(t v1beta1.ClusterAffinityTerm) bool {
		return t.Weights != nil && (t.Weights.Default != nil || t.Weights.Router != nil)
	})
}

func compileTerm(input v1beta1.ClusterAffinityTerm, allowWeights bool) (term, error) {
	if len(input.MatchExpressions)+len(input.MatchFields) == 0 {
		return term{}, fmt.Errorf("term must contain a label or field requirement")
	}
	if len(input.MatchExpressions) > v1beta1.MaxClusterSelectorRequirements || len(input.MatchFields) > v1beta1.MaxClusterSelectorRequirements {
		return term{}, fmt.Errorf("matchExpressions and matchFields each allow at most %d requirements", v1beta1.MaxClusterSelectorRequirements)
	}
	if err := validateWeights(input.Weights, allowWeights); err != nil {
		return term{}, err
	}
	labelSelector, err := labelRequirements(input.MatchExpressions)
	if err != nil {
		return term{}, err
	}
	fields, err := nameRequirements(input.MatchFields)
	if err != nil {
		return term{}, err
	}
	return term{labels: labelSelector, fields: fields, weight: 1, maxWeight: 1}, nil
}

// labelRequirements compiles matchExpressions into one label selector.
func labelRequirements(reqs []v1beta1.ClusterSelectorRequirement) (labels.Selector, error) {
	selector := labels.Everything()
	for i, req := range reqs {
		parsed, err := labelRequirement(req)
		if err != nil {
			return nil, fmt.Errorf("matchExpressions[%d]: %w", i, err)
		}
		selector = selector.Add(*parsed)
	}
	return selector, nil
}

// labelRequirement compiles one matchExpressions entry under the Kubernetes
// label selector rules.
func labelRequirement(req v1beta1.ClusterSelectorRequirement) (*labels.Requirement, error) {
	if len(req.Values) > v1beta1.MaxClusterSelectorValues {
		return nil, fmt.Errorf("at most %d values are allowed", v1beta1.MaxClusterSelectorValues)
	}
	op, ok := labelOperators[req.Operator]
	if !ok {
		return nil, fmt.Errorf("unsupported operator %q", req.Operator)
	}
	return labels.NewRequirement(req.Key, op, slices.Clone(req.Values))
}

// nameRequirements compiles matchFields into requirements that own their
// value lists, so a later edit of the source cannot change the selector.
func nameRequirements(reqs []v1beta1.ClusterSelectorRequirement) ([]v1beta1.ClusterSelectorRequirement, error) {
	var out []v1beta1.ClusterSelectorRequirement
	for i, req := range reqs {
		if err := validateNameRequirement(req); err != nil {
			return nil, fmt.Errorf("matchFields[%d]: %w", i, err)
		}
		req.Values = slices.Clone(req.Values)
		out = append(out, req)
	}
	return out, nil
}

// validateNameRequirement checks one matchFields entry: metadata.name with In
// or NotIn, one name up to the maximum, each a valid WorkloadCluster name.
func validateNameRequirement(req v1beta1.ClusterSelectorRequirement) error {
	if req.Key != metav1.ObjectNameField || (req.Operator != corev1.NodeSelectorOpIn && req.Operator != corev1.NodeSelectorOpNotIn) {
		return fmt.Errorf("only metadata.name with In or NotIn is supported")
	}
	if len(req.Values) == 0 || len(req.Values) > v1beta1.MaxClusterSelectorValues {
		return fmt.Errorf("between 1 and %d names are required", v1beta1.MaxClusterSelectorValues)
	}
	for _, name := range req.Values {
		if problems := validation.IsDNS1123Subdomain(name); len(problems) > 0 {
			return fmt.Errorf("invalid WorkloadCluster name %q: %v", name, problems)
		}
	}
	return nil
}

// validateWeights checks a term's weights block against the same rules the
// schema enforces, for objects admitted while a rule was absent.
func validateWeights(w *v1beta1.ClusterAffinityWeights, allowWeights bool) error {
	if w == nil {
		return nil
	}
	if !allowWeights {
		return fmt.Errorf("weights is permitted only in Split mode")
	}
	if !usesRatios(w) {
		return fmt.Errorf("weights must set at least one component")
	}
	for _, field := range weightKeys(w) {
		if field.value == nil {
			continue
		}
		if field.value.Min != nil && *field.value.Min < 1 {
			return fmt.Errorf("weights.%s.min must be positive", field.name)
		}
		if field.value.Max != nil && *field.value.Max < 1 {
			return fmt.Errorf("weights.%s.max must be positive", field.name)
		}
	}
	return nil
}

type weightKey struct {
	name  string
	value *v1beta1.ReplicaRatio
}

// weightKeys lists every key a weights block can set.
func weightKeys(w *v1beta1.ClusterAffinityWeights) []weightKey {
	return []weightKey{
		{"default", w.Default}, {"engine", w.Engine}, {"decoder", w.Decoder}, {"router", w.Router},
	}
}

var labelOperators = map[corev1.NodeSelectorOperator]selection.Operator{
	corev1.NodeSelectorOpIn:           selection.In,
	corev1.NodeSelectorOpNotIn:        selection.NotIn,
	corev1.NodeSelectorOpExists:       selection.Exists,
	corev1.NodeSelectorOpDoesNotExist: selection.DoesNotExist,
	corev1.NodeSelectorOpGt:           selection.GreaterThan,
	corev1.NodeSelectorOpLt:           selection.LessThan,
}

// Match considers labels and object identity, including disconnected clusters.
// Member preflight decides whether a matched cluster is currently eligible.
func (s *Selector) Match(cluster *v1beta1.WorkloadCluster) (Match, bool) {
	if s == nil || cluster == nil {
		return Match{}, false
	}
	if s.unconstrained {
		return Match{Weight: 1}, true
	}
	var matched Match
	for i, term := range s.terms {
		if !term.matches(cluster) {
			continue
		}
		matched.TermIndexes = append(matched.TermIndexes, int32(i))
		if s.weighted {
			matched.Weight += term.weight
			matched.MaxWeight += term.maxWeight
		} else {
			matched.Weight = 1
			matched.MaxWeight = 1
		}
	}
	if !s.ratios {
		matched.MaxWeight = 0
	}
	return matched, len(matched.TermIndexes) > 0
}

func (t term) matches(cluster *v1beta1.WorkloadCluster) bool {
	if !t.labels.Matches(labels.Set(cluster.Labels)) {
		return false
	}
	for _, field := range t.fields {
		contains := slices.Contains(field.Values, cluster.Name)
		if (field.Operator == corev1.NodeSelectorOpIn) != contains {
			return false
		}
	}
	return true
}
