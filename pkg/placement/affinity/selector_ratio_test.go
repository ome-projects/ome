package affinity

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func ratio(minimum int32, maximum ...int32) *v1beta1.ReplicaRatio {
	r := &v1beta1.ReplicaRatio{Min: ptr.To(minimum)}
	if len(maximum) > 0 {
		r.Max = ptr.To(maximum[0])
	}
	return r
}

func ratioTerm(w v1beta1.ClusterAffinityWeights) v1beta1.ClusterAffinityTerm {
	t := nameTerm("cluster-a")
	t.Weights = &w
	return t
}

func matchFor(t *testing.T, terms []v1beta1.ClusterAffinityTerm, component v1beta1.ComponentType) Match {
	t.Helper()
	selector, err := CompileFor(terms, true, component)
	require.NoError(t, err)
	m, ok := selector.Match(&v1beta1.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a"}})
	require.True(t, ok)
	return m
}

func TestCompileForRatioLookup(t *testing.T) {
	full := []v1beta1.ClusterAffinityTerm{ratioTerm(v1beta1.ClusterAffinityWeights{
		EngineReplicas: ratio(2, 3), DecoderReplicas: ratio(1, 2), DefaultReplicas: ratio(4, 5),
	})}
	for component, want := range map[v1beta1.ComponentType][2]int64{
		v1beta1.EngineComponent: {2, 3}, v1beta1.DecoderComponent: {1, 2}, v1beta1.RouterComponent: {4, 5},
	} {
		m := matchFor(t, full, component)
		require.Equal(t, want, [2]int64{m.Weight, m.MaxWeight}, component)
	}

	onlyDefault := []v1beta1.ClusterAffinityTerm{ratioTerm(v1beta1.ClusterAffinityWeights{DefaultReplicas: ratio(4)})}
	onlyEngine := []v1beta1.ClusterAffinityTerm{ratioTerm(v1beta1.ClusterAffinityWeights{EngineReplicas: ratio(2)})}
	for _, component := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent} {
		m := matchFor(t, onlyDefault, component)
		require.Equal(t, [2]int64{4, 1}, [2]int64{m.Weight, m.MaxWeight}, component)
		m = matchFor(t, onlyEngine, component)
		require.Equal(t, [2]int64{2, 1}, [2]int64{m.Weight, m.MaxWeight}, component)
	}
}

func TestMatchSumsRatiosOverTerms(t *testing.T) {
	terms := []v1beta1.ClusterAffinityTerm{
		ratioTerm(v1beta1.ClusterAffinityWeights{EngineReplicas: ratio(2, 3)}),
		nameTerm("cluster-a"),
	}
	m := matchFor(t, terms, v1beta1.EngineComponent)
	require.Equal(t, int64(3), m.Weight)
	require.Equal(t, int64(4), m.MaxWeight)
}

func TestMaxWeightIsZeroWithoutRatios(t *testing.T) {
	selector, err := Compile(nil, false)
	require.NoError(t, err)
	m, ok := selector.Match(&v1beta1.WorkloadCluster{})
	require.True(t, ok)
	require.Equal(t, Match{Weight: 1}, m)
	m = matchFor(t, []v1beta1.ClusterAffinityTerm{nameTerm("cluster-a")}, v1beta1.EngineComponent)
	require.Equal(t, Match{TermIndexes: []int32{0}, Weight: 1}, m)
}

func TestRatioSelectorWithoutComponentRatioMatchesOne(t *testing.T) {
	terms := []v1beta1.ClusterAffinityTerm{ratioTerm(v1beta1.ClusterAffinityWeights{DecoderReplicas: ratio(3, 4)})}
	m := matchFor(t, terms, v1beta1.EngineComponent)
	require.Equal(t, Match{TermIndexes: []int32{0}, Weight: 1, MaxWeight: 1}, m)
}

func TestUsesRatios(t *testing.T) {
	require.False(t, UsesRatios(nil))
	require.False(t, UsesRatios([]v1beta1.ClusterAffinityTerm{nameTerm("cluster-a")}))
	require.False(t, UsesRatios([]v1beta1.ClusterAffinityTerm{ratioTerm(v1beta1.ClusterAffinityWeights{Default: ptr.To[int32](1)})}))
	for _, w := range []v1beta1.ClusterAffinityWeights{
		{DefaultReplicas: ratio(1)}, {EngineReplicas: ratio(1)}, {DecoderReplicas: ratio(1)}, {RouterReplicas: ratio(1)},
	} {
		require.True(t, UsesRatios([]v1beta1.ClusterAffinityTerm{nameTerm("cluster-b"), ratioTerm(w)}))
	}
}

func TestDividesRoutersRatioForm(t *testing.T) {
	for _, w := range []v1beta1.ClusterAffinityWeights{{EngineReplicas: ratio(1)}, {DecoderReplicas: ratio(1)}} {
		require.False(t, DividesRouters([]v1beta1.ClusterAffinityTerm{ratioTerm(w)}))
	}
	for _, w := range []v1beta1.ClusterAffinityWeights{{DefaultReplicas: ratio(1)}, {RouterReplicas: ratio(1)}} {
		require.True(t, DividesRouters([]v1beta1.ClusterAffinityTerm{ratioTerm(w)}))
	}
	require.True(t, DividesRouters([]v1beta1.ClusterAffinityTerm{nameTerm("cluster-a")}))
}

func TestCompileRejectsMixedWeightsOnOneTerm(t *testing.T) {
	_, err := Compile([]v1beta1.ClusterAffinityTerm{ratioTerm(v1beta1.ClusterAffinityWeights{
		Default: ptr.To[int32](1), EngineReplicas: ratio(1),
	})}, true)
	require.ErrorContains(t, err, "either")

	_, err = Compile([]v1beta1.ClusterAffinityTerm{ratioTerm(v1beta1.ClusterAffinityWeights{EngineReplicas: ratio(0)})}, true)
	require.Error(t, err)

	_, err = Compile([]v1beta1.ClusterAffinityTerm{ratioTerm(v1beta1.ClusterAffinityWeights{EngineReplicas: ratio(1, 0)})}, true)
	require.Error(t, err)

	_, err = Compile([]v1beta1.ClusterAffinityTerm{ratioTerm(v1beta1.ClusterAffinityWeights{EngineReplicas: ratio(1)})}, false)
	require.ErrorContains(t, err, "weights is permitted only in Split mode")
}

func TestMaxRatioBelowMinIsNotClamped(t *testing.T) {
	terms := []v1beta1.ClusterAffinityTerm{ratioTerm(v1beta1.ClusterAffinityWeights{EngineReplicas: ratio(3, 1)})}
	m := matchFor(t, terms, v1beta1.EngineComponent)
	require.Equal(t, int64(3), m.Weight)
	require.Equal(t, int64(1), m.MaxWeight)
}

func TestOmittedRatioBoundsCountAsOne(t *testing.T) {
	for name, tt := range map[string]struct {
		ratio *v1beta1.ReplicaRatio
		want  [2]int64
	}{
		"min omitted":  {&v1beta1.ReplicaRatio{Max: ptr.To[int32](3)}, [2]int64{1, 3}},
		"max omitted":  {&v1beta1.ReplicaRatio{Min: ptr.To[int32](2)}, [2]int64{2, 1}},
		"both omitted": {&v1beta1.ReplicaRatio{}, [2]int64{1, 1}},
	} {
		terms := []v1beta1.ClusterAffinityTerm{ratioTerm(v1beta1.ClusterAffinityWeights{EngineReplicas: tt.ratio})}
		m := matchFor(t, terms, v1beta1.EngineComponent)
		require.Equal(t, tt.want, [2]int64{m.Weight, m.MaxWeight}, name)
	}
}

func TestEmptyRatioWeighsAsATermWithoutRatio(t *testing.T) {
	empty := []v1beta1.ClusterAffinityTerm{ratioTerm(v1beta1.ClusterAffinityWeights{EngineReplicas: &v1beta1.ReplicaRatio{}})}
	require.True(t, UsesRatios(empty))
	plain := []v1beta1.ClusterAffinityTerm{ratioTerm(v1beta1.ClusterAffinityWeights{DecoderReplicas: ratio(3, 4)})}
	require.Equal(t, matchFor(t, plain, v1beta1.EngineComponent), matchFor(t, empty, v1beta1.EngineComponent))
}
