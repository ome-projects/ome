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
	"k8s.io/utils/ptr"

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
		if weight := termWeight(input, component); weight != nil {
			selector.weighted = true
			compiled.weight = int64(*weight)
			compiled.maxWeight = compiled.weight
			if ratio := termRatio(input, component); ratio != nil {
				compiled.maxWeight = int64(ratio.MaxOrOne())
			}
		}
		selector.ratios = selector.ratios || usesRatios(input.Weights)
		selector.terms = append(selector.terms, compiled)
	}
	return selector, nil
}

// termWeight returns the weight a term gives a component, or nil when it gives
// none. The engine takes engine, then default; the decoder takes decoder, then
// default, then the engine's; the router takes router, then default, then the
// engine's.
func termWeight(input v1beta1.ClusterAffinityTerm, component v1beta1.ComponentType) *int32 {
	weights := input.Weights
	if weights == nil {
		return nil
	}
	if usesRatios(weights) {
		if ratio := termRatio(input, component); ratio != nil {
			return ptr.To(ratio.MinOrOne())
		}
		return nil
	}
	engine := cmp.Or(weights.Engine, weights.Default)
	switch component {
	case v1beta1.EngineComponent:
		return engine
	case v1beta1.DecoderComponent:
		return cmp.Or(weights.Decoder, weights.Default, engine)
	case v1beta1.RouterComponent:
		return cmp.Or(weights.Router, weights.Default, engine)
	}
	return nil
}

// termRatio returns the ratio a term gives a component under the same
// fallback order as termWeight, or nil when the term gives none.
func termRatio(input v1beta1.ClusterAffinityTerm, component v1beta1.ComponentType) *v1beta1.ReplicaRatio {
	weights := input.Weights
	if weights == nil {
		return nil
	}
	engine := cmp.Or(weights.EngineReplicas, weights.DefaultReplicas)
	switch component {
	case v1beta1.EngineComponent:
		return engine
	case v1beta1.DecoderComponent:
		return cmp.Or(weights.DecoderReplicas, weights.DefaultReplicas, engine)
	case v1beta1.RouterComponent:
		return cmp.Or(weights.RouterReplicas, weights.DefaultReplicas, engine)
	}
	return nil
}

func usesRatios(w *v1beta1.ClusterAffinityWeights) bool {
	return w != nil && (w.DefaultReplicas != nil || w.EngineReplicas != nil || w.DecoderReplicas != nil || w.RouterReplicas != nil)
}

// UsesRatios reports whether any term sets a replica ratio key.
func UsesRatios(terms []v1beta1.ClusterAffinityTerm) bool {
	return slices.ContainsFunc(terms, func(t v1beta1.ClusterAffinityTerm) bool { return usesRatios(t.Weights) })
}

// SetsWeights reports whether any term holds a weights block, which can give
// the decoder and router weights other than the engine's.
func SetsWeights(terms []v1beta1.ClusterAffinityTerm) bool {
	return slices.ContainsFunc(terms, func(t v1beta1.ClusterAffinityTerm) bool { return t.Weights != nil })
}

// DividesRouters reports whether static Split divides routers across the
// matched clusters: when no term sets weights, as a default weight of one on
// every term would, or when a term sets a default or router weight. Weights
// that name only the engine or decoder keep the full router policy on each.
func DividesRouters(terms []v1beta1.ClusterAffinityTerm) bool {
	if UsesRatios(terms) {
		return slices.ContainsFunc(terms, func(t v1beta1.ClusterAffinityTerm) bool {
			return t.Weights != nil && (t.Weights.DefaultReplicas != nil || t.Weights.RouterReplicas != nil)
		})
	}
	return !SetsWeights(terms) || slices.ContainsFunc(terms, func(t v1beta1.ClusterAffinityTerm) bool {
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
	out := term{labels: labels.Everything(), weight: 1, maxWeight: 1}
	if err := validateWeights(input, allowWeights); err != nil {
		return term{}, err
	}
	for i, req := range input.MatchExpressions {
		if len(req.Values) > v1beta1.MaxClusterSelectorValues {
			return term{}, fmt.Errorf("matchExpressions[%d]: at most %d values are allowed", i, v1beta1.MaxClusterSelectorValues)
		}
		op, ok := labelOperators[req.Operator]
		if !ok {
			return term{}, fmt.Errorf("matchExpressions[%d]: unsupported operator %q", i, req.Operator)
		}
		parsed, err := labels.NewRequirement(req.Key, op, slices.Clone(req.Values))
		if err != nil {
			return term{}, fmt.Errorf("matchExpressions[%d]: %w", i, err)
		}
		out.labels = out.labels.Add(*parsed)
	}
	for i, req := range input.MatchFields {
		if req.Key != metav1.ObjectNameField || (req.Operator != corev1.NodeSelectorOpIn && req.Operator != corev1.NodeSelectorOpNotIn) {
			return term{}, fmt.Errorf("matchFields[%d]: only metadata.name with In or NotIn is supported", i)
		}
		if len(req.Values) == 0 || len(req.Values) > v1beta1.MaxClusterSelectorValues {
			return term{}, fmt.Errorf("matchFields[%d]: between 1 and %d names are required", i, v1beta1.MaxClusterSelectorValues)
		}
		for _, name := range req.Values {
			if problems := validation.IsDNS1123Subdomain(name); len(problems) > 0 {
				return term{}, fmt.Errorf("matchFields[%d]: invalid WorkloadCluster name %q: %v", i, name, problems)
			}
		}
		req.Values = slices.Clone(req.Values)
		out.fields = append(out.fields, req)
	}
	return out, nil
}

// validateWeights checks a term's weights block against the same rules the
// schema enforces, for objects admitted while a rule was absent.
func validateWeights(input v1beta1.ClusterAffinityTerm, allowWeights bool) error {
	w := input.Weights
	if w == nil {
		return nil
	}
	if !allowWeights {
		return fmt.Errorf("weights is permitted only in Split mode")
	}
	if usesRatios(w) {
		return validateRatios(w)
	}
	set := 0
	for _, field := range []struct {
		name  string
		value *int32
	}{{"default", w.Default}, {"engine", w.Engine}, {"decoder", w.Decoder}, {"router", w.Router}} {
		if field.value == nil {
			continue
		}
		set++
		if *field.value < 1 {
			return fmt.Errorf("weights.%s must be positive", field.name)
		}
	}
	if set == 0 {
		return fmt.Errorf("weights must set at least one component")
	}
	return nil
}

func validateRatios(w *v1beta1.ClusterAffinityWeights) error {
	if w.Default != nil || w.Engine != nil || w.Decoder != nil || w.Router != nil {
		return fmt.Errorf("weights must use either integer weights or replica ratios, not both")
	}
	for _, field := range []struct {
		name  string
		value *v1beta1.ReplicaRatio
	}{{"defaultReplicas", w.DefaultReplicas}, {"engineReplicas", w.EngineReplicas}, {"decoderReplicas", w.DecoderReplicas}, {"routerReplicas", w.RouterReplicas}} {
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
