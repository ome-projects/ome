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

// projectedMembers projects every named cluster's member. A projection copies
// the source's placement block, which differs between the two weight forms, so
// it is cleared to compare the members by everything else.
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

func TestProjectRatioFormSummaryExample(t *testing.T) {
	source := srcISVCSplit("", 0)
	source.Spec.Placement.Split = nil
	source.Spec.Engine.MinReplicas, source.Spec.Engine.MaxReplicas = ptr.To(12), 40
	source.Spec.Placement.ClusterAffinity = engineRatios(map[string]*v1beta1.ReplicaRatio{"cluster-a": ratio(2, 3), "cluster-b": ratio(1, 1)})
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

// migratedWeights is one cluster's weights; in the ratio form each integer
// weight becomes the min ratio and every max ratio is 1.
type migratedWeights struct{ def, decoder, router int32 }

func (w migratedWeights) integer() v1beta1.ClusterAffinityWeights {
	return v1beta1.ClusterAffinityWeights{Default: ptr.To(w.def), Decoder: ptr.To(w.decoder), Router: ptr.To(w.router)}
}

func (w migratedWeights) ratios() v1beta1.ClusterAffinityWeights {
	return v1beta1.ClusterAffinityWeights{DefaultReplicas: ratio(w.def, 1), DecoderReplicas: ratio(w.decoder, 1), RouterReplicas: ratio(w.router, 1)}
}

// TestRatioFormMigrationEquivalence converts integer-form sources by the
// migration table and checks that every cluster's projected member is the same
// in both forms. Each component's maxReplicas is its old per-cluster cap times
// the 3 matched clusters; a component without a cap of its own takes the
// primary's cap scaled by its ratio of minimums.
func TestRatioFormMigrationEquivalence(t *testing.T) {
	distinct := map[string]migratedWeights{"cluster-a": {3, 2, 1}, "cluster-b": {2, 2, 1}, "cluster-c": {1, 1, 1}}
	every := func(w migratedWeights) map[string]migratedWeights {
		return map[string]migratedWeights{"cluster-a": w, "cluster-b": w, "cluster-c": w}
	}
	allCaps := &v1beta1.SplitCeilings{Engine: ptr.To[int32](6), Decoder: ptr.To[int32](4), Router: ptr.To[int32](6)}
	for _, tt := range []struct {
		name    string
		weights map[string]migratedWeights
		caps    *v1beta1.SplitCeilings
		// maxReplicas of the engine, decoder and router in the ratio form.
		maxReplicas [3]int
	}{
		{name: "distinct weights per cluster", weights: distinct, caps: allCaps, maxReplicas: [3]int{18, 12, 18}},
		{name: "weights 3, 2, 1 on every cluster", weights: every(migratedWeights{3, 2, 1}), caps: allCaps, maxReplicas: [3]int{18, 12, 18}},
		{name: "weights 2, 2, 1 on every cluster", weights: every(migratedWeights{2, 2, 1}), caps: allCaps, maxReplicas: [3]int{18, 12, 18}},
		{name: "weights 1, 1, 1 on every cluster", weights: every(migratedWeights{1, 1, 1}), caps: allCaps, maxReplicas: [3]int{18, 12, 18}},
		// Decoder and router caps scale from the engine's 6: ceil(6*5/6)=5 and ceil(6*3/6)=3.
		{name: "engine cap only", weights: distinct, caps: &v1beta1.SplitCeilings{Engine: ptr.To[int32](6)}, maxReplicas: [3]int{18, 15, 9}},
		// Without caps the ratio form leaves maxReplicas unset, so each member stays
		// at its share. maxReplicas equal to minReplicas would divide it by the
		// equal max ratios and let cluster-c hold 2 engines instead of 1.
		{name: "no caps", weights: distinct},
	} {
		t.Run(tt.name, func(t *testing.T) {
			integerTerms, ratioTerms := map[string]v1beta1.ClusterAffinityWeights{}, map[string]v1beta1.ClusterAffinityWeights{}
			for name, w := range tt.weights {
				integerTerms[name], ratioTerms[name] = w.integer(), w.ratios()
			}
			integer := projectionSource(weightedTermsOn(integerTerms))
			if tt.caps != nil {
				integer.Spec.Placement.Split = &v1beta1.SplitSpec{MaxPerCluster: tt.caps}
			}
			converted := projectionSource(weightedTermsOn(ratioTerms))
			converted.Spec.Engine.MaxReplicas = tt.maxReplicas[0]
			converted.Spec.Decoder.MaxReplicas = tt.maxReplicas[1]
			converted.Spec.Router.MaxReplicas = tt.maxReplicas[2]
			before, after := projectedMembers(t, integer, threeClusters...), projectedMembers(t, converted, threeClusters...)
			for _, name := range threeClusters {
				if !reflect.DeepEqual(before[name], after[name]) {
					t.Errorf("%s member differs after migration (-integer +ratio):\n%s", name, cmp.Diff(before[name], after[name]))
				}
			}
		})
	}
}
