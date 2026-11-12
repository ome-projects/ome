package placement

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

var threeClusters = []string{"cluster-a", "cluster-b", "cluster-c"}

func registeredClusters(names ...string) []v1beta1.WorkloadCluster {
	var out []v1beta1.WorkloadCluster
	for _, name := range names {
		out = append(out, v1beta1.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: name}})
	}
	return out
}

// projectionSource is a Split source with an engine, a decoder and a router
// whose minimums are 6, 5 and 3, matched to clusters by terms.
func projectionSource(terms []v1beta1.ClusterAffinityTerm) *v1beta1.InferenceService {
	source := srcISVCSplit("", 0)
	source.Spec.Placement.Split = nil
	source.Spec.Placement.ClusterAffinity = terms
	runner := func() *v1beta1.RunnerSpec {
		return &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Image: "img"}}
	}
	source.Spec.Engine.MinReplicas = ptr.To(6)
	source.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(5)}, Runner: runner()}
	source.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(3)}, Runner: runner()}
	return source
}

func TestProjectRatioFormSummaryExample(t *testing.T) {
	source := srcISVCSplit("", 0)
	source.Spec.Placement.Split = nil
	source.Spec.Engine.MinReplicas, source.Spec.Engine.MaxReplicas = ptr.To(12), 40
	source.Spec.Placement.ClusterAffinity = weightedTermsOn(map[string]v1beta1.ClusterAffinityWeights{"cluster-a": {Engine: ratio(2, 3)}, "cluster-b": {Engine: ratio(1, 1)}})
	for name, want := range map[string][2]int{"cluster-a": {8, 30}, "cluster-b": {4, 10}} {
		member, err := ProjectSplitReplicas(source, registeredClusters("cluster-a", "cluster-b"), name)
		if err != nil {
			t.Fatal(err)
		}
		if got := [2]int{*member.Spec.Engine.MinReplicas, member.Spec.Engine.MaxReplicas}; got != want {
			t.Errorf("%s engine min and max = %v, want %v", name, got, want)
		}
	}
}

// equalRatios gives every component a {min: 1, max: 1} ratio through the
// plain keys.
func equalRatios() v1beta1.ClusterAffinityWeights {
	return v1beta1.ClusterAffinityWeights{Default: ratio(1, 1), Decoder: ratio(1, 1), Router: ratio(1, 1)}
}

// TestUnweightedSplitBlockMigratesToRatios converts unweighted sources with a
// split block to equal ratios and checks that every cluster's projected member
// is the same. Each component's maxReplicas is its per-cluster cap times the 3
// matched clusters; a component without a cap of its own takes the primary's
// cap scaled by its ratio of minimums.
func TestUnweightedSplitBlockMigratesToRatios(t *testing.T) {
	allCaps := &v1beta1.SplitCeilings{Engine: ptr.To[int32](6), Decoder: ptr.To[int32](4), Router: ptr.To[int32](6)}
	for _, tt := range []struct {
		name string
		caps *v1beta1.SplitCeilings
		// maxReplicas of the engine, decoder and router in the ratio form.
		maxReplicas [3]int
	}{
		{name: "a cap for every component", caps: allCaps, maxReplicas: [3]int{18, 12, 18}},
		// Decoder and router caps scale from the engine's 6: ceil(6*5/6)=5 and ceil(6*3/6)=3.
		{name: "engine cap only", caps: &v1beta1.SplitCeilings{Engine: ptr.To[int32](6)}, maxReplicas: [3]int{18, 15, 9}},
		// Without caps the ratio form leaves maxReplicas unset, so each member
		// stays at its share.
		{name: "no caps"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			unweighted := map[string]v1beta1.ClusterAffinityWeights{}
			ratios := map[string]v1beta1.ClusterAffinityWeights{}
			for _, name := range threeClusters {
				unweighted[name], ratios[name] = v1beta1.ClusterAffinityWeights{}, equalRatios()
			}
			before := projectionSource(weightedTermsOn(unweighted))
			for i := range before.Spec.Placement.ClusterAffinity {
				before.Spec.Placement.ClusterAffinity[i].Weights = nil
			}
			if tt.caps != nil {
				before.Spec.Placement.Split = &v1beta1.SplitSpec{MaxPerCluster: tt.caps}
			}
			after := projectionSource(weightedTermsOn(ratios))
			after.Spec.Engine.MaxReplicas = tt.maxReplicas[0]
			after.Spec.Decoder.MaxReplicas = tt.maxReplicas[1]
			after.Spec.Router.MaxReplicas = tt.maxReplicas[2]
			want, got := projectedMembers(t, before, threeClusters...), projectedMembers(t, after, threeClusters...)
			for _, name := range threeClusters {
				if !reflect.DeepEqual(want[name], got[name]) {
					t.Errorf("%s member differs after migration (-split block +ratios):\n%s", name, cmp.Diff(want[name], got[name]))
				}
			}
		})
	}
}

// projectedMembers projects every named cluster's member with the placement
// block cleared, so members compare by everything else.
func projectedMembers(t *testing.T, source *v1beta1.InferenceService, names ...string) map[string]*v1beta1.InferenceService {
	t.Helper()
	out := map[string]*v1beta1.InferenceService{}
	for _, name := range names {
		member, err := ProjectSplitReplicas(source, registeredClusters(names...), name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if member != nil {
			member.Spec.Placement = nil
		}
		out[name] = member
	}
	return out
}
