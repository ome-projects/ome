package affinity

import (
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func requirement(key string, op corev1.NodeSelectorOperator, values ...string) v1beta1.ClusterSelectorRequirement {
	return v1beta1.ClusterSelectorRequirement{Key: key, Operator: op, Values: values}
}

func expression(req v1beta1.ClusterSelectorRequirement) v1beta1.ClusterAffinityTerm {
	return v1beta1.ClusterAffinityTerm{MatchExpressions: []v1beta1.ClusterSelectorRequirement{req}}
}

func nameTerm(names ...string) v1beta1.ClusterAffinityTerm {
	return v1beta1.ClusterAffinityTerm{MatchFields: []v1beta1.ClusterSelectorRequirement{
		requirement(metav1.ObjectNameField, corev1.NodeSelectorOpIn, names...),
	}}
}

func TestLabelSelectorConformsToKubernetes(t *testing.T) {
	for _, req := range []v1beta1.ClusterSelectorRequirement{
		requirement("size", corev1.NodeSelectorOpIn, "8", "16"),
		requirement("size", corev1.NodeSelectorOpNotIn, "8", "16"),
		requirement("size", corev1.NodeSelectorOpExists),
		requirement("size", corev1.NodeSelectorOpDoesNotExist),
		requirement("size", corev1.NodeSelectorOpGt, "8"),
		requirement("size", corev1.NodeSelectorOpLt, "8"),
	} {
		t.Run(string(req.Operator), func(t *testing.T) {
			selector, err := Compile([]v1beta1.ClusterAffinityTerm{expression(req)}, false)
			require.NoError(t, err)
			native, err := nodeaffinity.NewNodeSelector(&corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchExpressions: []corev1.NodeSelectorRequirement{{Key: req.Key, Operator: req.Operator, Values: req.Values}},
			}}})
			require.NoError(t, err)
			for _, labels := range []map[string]string{
				nil, {"size": ""}, {"size": "4"}, {"size": "8"}, {"size": "16"}, {"size": "32"}, {"size": "08"}, {"size": "invalid"},
			} {
				meta := metav1.ObjectMeta{Name: "cluster-a", Labels: labels}
				_, matched := selector.Match(&v1beta1.WorkloadCluster{ObjectMeta: meta})
				require.Equal(t, native.Match(&corev1.Node{ObjectMeta: meta}), matched, "labels: %v", labels)
			}
		})
	}
}

func TestSelectorUsesRealNamesAndActualLabels(t *testing.T) {
	term := nameTerm("cluster-a", "cluster-b")
	term.MatchExpressions = []v1beta1.ClusterSelectorRequirement{
		requirement(metav1.ObjectNameField, corev1.NodeSelectorOpIn, "forged"),
		requirement("region", corev1.NodeSelectorOpIn, "west"),
	}
	selector, err := Compile([]v1beta1.ClusterAffinityTerm{term, nameTerm("cluster-c")}, false)
	require.NoError(t, err)
	for _, tt := range []struct {
		name   string
		labels map[string]string
		terms  []int32
	}{
		{name: "cluster-a", labels: map[string]string{metav1.ObjectNameField: "forged", "region": "west"}, terms: []int32{0}},
		{name: "cluster-b", labels: map[string]string{metav1.ObjectNameField: "forged", "region": "east"}},
		{name: "cluster-c", terms: []int32{1}},
		{name: "forged", labels: map[string]string{metav1.ObjectNameField: "cluster-a", "region": "west"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cluster := &v1beta1.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: tt.name, Labels: tt.labels}}
			before := cluster.DeepCopy()
			matched, ok := selector.Match(cluster)
			require.Equal(t, len(tt.terms) > 0, ok)
			require.Equal(t, tt.terms, matched.TermIndexes)
			require.Equal(t, before, cluster)
		})
	}
	term = nameTerm("cluster-a", "cluster-b")
	term.MatchFields[0].Operator = corev1.NodeSelectorOpNotIn
	selector, err = Compile([]v1beta1.ClusterAffinityTerm{term}, false)
	require.NoError(t, err)
	for _, name := range []string{"cluster-a", "cluster-b", "cluster-c"} {
		_, ok := selector.Match(&v1beta1.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: name}})
		require.Equal(t, name == "cluster-c", ok)
	}
}

func TestSelectorWeightOverlap(t *testing.T) {
	cluster := &v1beta1.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", Labels: map[string]string{"region": "west"}}}
	region := expression(requirement("region", corev1.NodeSelectorOpIn, "west"))
	for _, tt := range []struct {
		name    string
		weights []*int32
		want    int64
		wantMax int64
	}{
		{name: "unweighted deduplicates", weights: []*int32{nil, nil}, want: 1},
		{name: "weighted adds", weights: []*int32{ptr.To(int32(3)), ptr.To(int32(2))}, want: 5, wantMax: 2},
		{name: "omitted contributes one", weights: []*int32{ptr.To(int32(3)), nil}, want: 4, wantMax: 2},
		{name: "explicit one enables addition", weights: []*int32{ptr.To(int32(1)), nil}, want: 2, wantMax: 2},
		{name: "weight above one hundred", weights: []*int32{ptr.To(int32(400)), ptr.To(int32(168))}, want: 568, wantMax: 2},
		{name: "effective sum exceeds int32", weights: []*int32{ptr.To(int32(math.MaxInt32)), ptr.To(int32(math.MaxInt32))}, want: 2 * math.MaxInt32, wantMax: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			terms := []v1beta1.ClusterAffinityTerm{region, nameTerm("cluster-a")}
			for i := range terms {
				if tt.weights[i] != nil {
					terms[i].Weights = &v1beta1.ClusterAffinityWeights{Default: &v1beta1.ReplicaRatio{Min: tt.weights[i]}}
				}
			}
			selector, err := Compile(terms, true)
			require.NoError(t, err)
			matched, ok := selector.Match(cluster)
			require.True(t, ok)
			require.Equal(t, Match{TermIndexes: []int32{0, 1}, Weight: tt.want, MaxWeight: tt.wantMax}, matched)
			slices.Reverse(terms)
			reordered, err := Compile(terms, true)
			require.NoError(t, err)
			again, ok := reordered.Match(cluster)
			require.True(t, ok)
			require.Equal(t, matched.Weight, again.Weight)
		})
	}
	region.Weights = &v1beta1.ClusterAffinityWeights{Default: &v1beta1.ReplicaRatio{Min: ptr.To(int32(3))}}
	selector, err := Compile([]v1beta1.ClusterAffinityTerm{region, region}, true)
	require.NoError(t, err)
	matched, ok := selector.Match(cluster)
	require.True(t, ok)
	require.Equal(t, int64(6), matched.Weight, "duplicate weighted terms remain additive")
}

func TestComponentWeights(t *testing.T) {
	cluster := &v1beta1.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", Labels: map[string]string{"region": "west"}}}
	type weights struct{ engine, decoder, router int64 }
	for _, tt := range []struct {
		name   string
		blocks [2]*v1beta1.ClusterAffinityWeights
		want   weights
	}{
		{name: "nothing set weighs every component one", want: weights{1, 1, 1}},
		{name: "the others follow an engine weight", blocks: [2]*v1beta1.ClusterAffinityWeights{{Engine: &v1beta1.ReplicaRatio{Min: ptr.To[int32](3)}}}, want: weights{4, 4, 4}},
		{name: "default weighs every component", blocks: [2]*v1beta1.ClusterAffinityWeights{{Default: &v1beta1.ReplicaRatio{Min: ptr.To[int32](2)}}, {Default: &v1beta1.ReplicaRatio{Min: ptr.To[int32](5)}}}, want: weights{7, 7, 7}},
		{name: "the decoder and router take default before the engine's", blocks: [2]*v1beta1.ClusterAffinityWeights{{Default: &v1beta1.ReplicaRatio{Min: ptr.To[int32](2)}, Engine: &v1beta1.ReplicaRatio{Min: ptr.To[int32](3)}}}, want: weights{4, 3, 3}},
		{name: "a decoder without default takes the engine's", blocks: [2]*v1beta1.ClusterAffinityWeights{{Engine: &v1beta1.ReplicaRatio{Min: ptr.To[int32](3)}}, {Decoder: &v1beta1.ReplicaRatio{Min: ptr.To[int32](2)}}}, want: weights{4, 5, 4}},
		{name: "a router weight beside an engine weight", blocks: [2]*v1beta1.ClusterAffinityWeights{{Engine: &v1beta1.ReplicaRatio{Min: ptr.To[int32](2)}, Router: &v1beta1.ReplicaRatio{Min: ptr.To[int32](5)}}}, want: weights{3, 3, 6}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			terms := []v1beta1.ClusterAffinityTerm{expression(requirement("region", corev1.NodeSelectorOpIn, "west")), nameTerm("cluster-a")}
			for i := range terms {
				terms[i].Weights = tt.blocks[i]
			}
			weightOf := func(component v1beta1.ComponentType) int64 {
				selector, err := CompileFor(terms, true, component)
				require.NoError(t, err)
				matched, ok := selector.Match(cluster)
				require.True(t, ok)
				return matched.Weight
			}
			got := weights{weightOf(v1beta1.EngineComponent), weightOf(v1beta1.DecoderComponent), weightOf(v1beta1.RouterComponent)}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDividesRouters(t *testing.T) {
	for _, tt := range []struct {
		name  string
		block *v1beta1.ClusterAffinityWeights
		want  bool
	}{
		{name: "no weights divide the routers, as a default weight of one on every term does", want: true},
		{name: "an engine weight keeps the full router policy", block: &v1beta1.ClusterAffinityWeights{Engine: &v1beta1.ReplicaRatio{Min: ptr.To[int32](3)}, Decoder: &v1beta1.ReplicaRatio{Min: ptr.To[int32](2)}}},
		{name: "a default weight divides the routers", block: &v1beta1.ClusterAffinityWeights{Default: &v1beta1.ReplicaRatio{Min: ptr.To[int32](3)}, Decoder: &v1beta1.ReplicaRatio{Min: ptr.To[int32](2)}}, want: true},
		{name: "a router weight divides the routers", block: &v1beta1.ClusterAffinityWeights{Engine: &v1beta1.ReplicaRatio{Min: ptr.To[int32](3)}, Router: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)}}, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			weighted := nameTerm("cluster-a")
			weighted.Weights = tt.block
			require.Equal(t, tt.want, DividesRouters([]v1beta1.ClusterAffinityTerm{weighted, nameTerm("cluster-b")}))
		})
	}
	require.True(t, DividesRouters(nil), "an omitted affinity sets no weights, so it divides the routers")
}

func TestSelectorOmissionAndSnapshot(t *testing.T) {
	cluster := &v1beta1.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", Labels: map[string]string{"region": "west"}}}
	selector, err := Compile(nil, false)
	require.NoError(t, err)
	matched, ok := selector.Match(cluster)
	require.True(t, ok, "omitted affinity matches even a disconnected cluster")
	require.Equal(t, Match{Weight: 1}, matched)
	_, ok = new(Selector).Match(cluster)
	require.False(t, ok, "uncompiled zero value must not widen selection")
	_, ok = (*Selector)(nil).Match(cluster)
	require.False(t, ok)
	_, ok = selector.Match(nil)
	require.False(t, ok)

	term := nameTerm("cluster-a")
	term.Weights = &v1beta1.ClusterAffinityWeights{Default: &v1beta1.ReplicaRatio{Min: ptr.To(int32(3))}}
	term.MatchExpressions = []v1beta1.ClusterSelectorRequirement{requirement("region", corev1.NodeSelectorOpIn, "west")}
	selector, err = Compile([]v1beta1.ClusterAffinityTerm{term}, true)
	require.NoError(t, err)
	term.MatchFields[0].Values[0] = "cluster-b"
	term.MatchExpressions[0].Values[0] = "east"
	*term.Weights.Default.Min = 9
	matched, ok = selector.Match(cluster)
	require.True(t, ok)
	require.Equal(t, int64(3), matched.Weight, "a compiled plan must not alias mutable source fields")
}

func TestSelectorRejectsMalformedIntent(t *testing.T) {
	for _, tt := range []struct {
		name  string
		terms []v1beta1.ClusterAffinityTerm
	}{
		{name: "empty list", terms: []v1beta1.ClusterAffinityTerm{}},
		{name: "empty term", terms: []v1beta1.ClusterAffinityTerm{{}}},
		{name: "weights only", terms: []v1beta1.ClusterAffinityTerm{{Weights: &v1beta1.ClusterAffinityWeights{Default: &v1beta1.ReplicaRatio{Min: ptr.To(int32(1))}}}}},
		{name: "bad key", terms: []v1beta1.ClusterAffinityTerm{expression(requirement("bad key", corev1.NodeSelectorOpExists))}},
		{name: "bad value", terms: []v1beta1.ClusterAffinityTerm{expression(requirement("region", corev1.NodeSelectorOpIn, "bad value"))}},
		{name: "empty In", terms: []v1beta1.ClusterAffinityTerm{expression(requirement("region", corev1.NodeSelectorOpIn))}},
		{name: "empty NotIn", terms: []v1beta1.ClusterAffinityTerm{expression(requirement("region", corev1.NodeSelectorOpNotIn))}},
		{name: "Exists with value", terms: []v1beta1.ClusterAffinityTerm{expression(requirement("region", corev1.NodeSelectorOpExists, "west"))}},
		{name: "DoesNotExist with value", terms: []v1beta1.ClusterAffinityTerm{expression(requirement("region", corev1.NodeSelectorOpDoesNotExist, "west"))}},
		{name: "Gt missing integer", terms: []v1beta1.ClusterAffinityTerm{expression(requirement("size", corev1.NodeSelectorOpGt))}},
		{name: "Lt multiple integers", terms: []v1beta1.ClusterAffinityTerm{expression(requirement("size", corev1.NodeSelectorOpLt, "1", "2"))}},
		{name: "Gt noninteger", terms: []v1beta1.ClusterAffinityTerm{expression(requirement("size", corev1.NodeSelectorOpGt, "x"))}},
		{name: "unknown operator", terms: []v1beta1.ClusterAffinityTerm{expression(requirement("region", "Unknown"))}},
		{name: "empty names", terms: []v1beta1.ClusterAffinityTerm{nameTerm()}},
		{name: "invalid name", terms: []v1beta1.ClusterAffinityTerm{nameTerm("Invalid")}},
		{name: "too many terms", terms: make([]v1beta1.ClusterAffinityTerm, v1beta1.MaxClusterAffinityTerms+1)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			selector, err := Compile(tt.terms, true)
			require.Error(t, err)
			require.Nil(t, selector)
		})
	}
	for _, req := range []v1beta1.ClusterSelectorRequirement{
		requirement("metadata.namespace", corev1.NodeSelectorOpIn, "namespace-a"),
		requirement(metav1.ObjectNameField, corev1.NodeSelectorOpExists),
		requirement(metav1.ObjectNameField, corev1.NodeSelectorOpGt, "1"),
	} {
		_, err := Compile([]v1beta1.ClusterAffinityTerm{{MatchFields: []v1beta1.ClusterSelectorRequirement{req}}}, false)
		require.ErrorContains(t, err, "only metadata.name with In or NotIn")
	}
	for _, tt := range []struct {
		name    string
		weights v1beta1.ClusterAffinityWeights
		split   bool
		wantErr string
	}{
		{name: "outside Split", weights: v1beta1.ClusterAffinityWeights{Decoder: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)}}, wantErr: "weights is permitted only in Split"},
		{name: "an empty block", split: true, wantErr: "weights must set at least one component"},
		{name: "a zero weight", weights: v1beta1.ClusterAffinityWeights{Router: &v1beta1.ReplicaRatio{Min: ptr.To[int32](0)}}, split: true, wantErr: "weights.router.min must be positive"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			term := nameTerm("cluster-a")
			term.Weights = &tt.weights
			_, err := Compile([]v1beta1.ClusterAffinityTerm{term}, tt.split)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestSelectorBounds(t *testing.T) {
	for _, fields := range []bool{false, true} {
		for _, count := range []int{v1beta1.MaxClusterSelectorRequirements, v1beta1.MaxClusterSelectorRequirements + 1} {
			reqs := make([]v1beta1.ClusterSelectorRequirement, count)
			for i := range reqs {
				reqs[i] = requirement(metav1.ObjectNameField, corev1.NodeSelectorOpIn, "cluster-a")
			}
			term := v1beta1.ClusterAffinityTerm{MatchExpressions: reqs}
			if fields {
				term = v1beta1.ClusterAffinityTerm{MatchFields: reqs}
			}
			_, err := Compile([]v1beta1.ClusterAffinityTerm{term}, false)
			require.Equal(t, count > v1beta1.MaxClusterSelectorRequirements, err != nil)
		}
		for _, count := range []int{v1beta1.MaxClusterSelectorValues, v1beta1.MaxClusterSelectorValues + 1} {
			names := make([]string, count)
			for i := range names {
				names[i] = "cluster-a"
			}
			term := nameTerm(names...)
			if !fields {
				term = expression(term.MatchFields[0])
			}
			_, err := Compile([]v1beta1.ClusterAffinityTerm{term}, false)
			require.Equal(t, count > v1beta1.MaxClusterSelectorValues, err != nil)
		}
	}
}
