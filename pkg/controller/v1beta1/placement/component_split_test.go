package placement

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// componentSplitSource declares static Split over member-a and member-b, one
// term each, with the engine weight in weight and the decoder's in weights.
func componentSplitSource(engine, decoder int, engineWeights, decoderWeights map[string]int32) *v1beta1.InferenceService {
	source := ratioSplitSource(engine, ptr.To(decoder))
	source.Spec.Placement.ClusterAffinity = weightedAffinity(engineWeights)
	for i, name := range []string{"member-a", "member-b"} {
		source.Spec.Placement.ClusterAffinity[i].Weights.Decoder = ptr.To(decoderWeights[name])
	}
	return source
}

func twoMembers() []v1beta1.WorkloadCluster {
	return []v1beta1.WorkloadCluster{wc("member-a", true, map[string]string{"accelerator": "gpu-a"}), wc("member-b", true, map[string]string{"accelerator": "gpu-a"})}
}

type componentFloors struct{ Engine, Decoder int32 }

// settledFloors plans the source over both members, settles every member at its
// desired share, and returns each member's floors.
func settledFloors(t *testing.T, source *v1beta1.InferenceService) (map[string]componentFloors, error) {
	t.Helper()
	proposal, err := desiredSplitPlan(source, twoMembers(), nil)
	if err != nil {
		return nil, err
	}
	candidates := settledCandidates(proposal.Assignments)
	out := map[string]componentFloors{}
	for _, candidate := range candidates {
		floors, err := splitMemberFloors(source, candidates, candidate, nil)
		if err != nil {
			t.Fatalf("%s: %v", candidate.Cluster, err)
		}
		var h componentFloors
		for _, floor := range floors {
			switch floor.Component {
			case v1beta1.EngineComponent:
				h.Engine = floor.Replicas
			case v1beta1.DecoderComponent:
				h.Decoder = floor.Replicas
			}
		}
		out[candidate.Cluster] = h
	}
	return out, nil
}

func TestSplitByComponentWeights(t *testing.T) {
	twoToOne := map[string]int32{"member-a": 2, "member-b": 1}
	even := map[string]int32{"member-a": 1, "member-b": 1}
	for _, tt := range []struct {
		name    string
		source  func() *v1beta1.InferenceService
		want    map[string]componentFloors
		wantErr string
	}{
		{name: "the same weights split as the engine weights alone", source: func() *v1beta1.InferenceService { return componentSplitSource(4, 12, twoToOne, twoToOne) },
			want: map[string]componentFloors{"member-a": {3, 9}, "member-b": {1, 3}}},
		{name: "proportional weights split as the engine weights alone", source: func() *v1beta1.InferenceService {
			return componentSplitSource(4, 12, twoToOne, map[string]int32{"member-a": 4, "member-b": 2})
		}, want: map[string]componentFloors{"member-a": {3, 9}, "member-b": {1, 3}}},
		{name: "different decoder weights split the decoders by their own weights", source: func() *v1beta1.InferenceService { return componentSplitSource(4, 12, twoToOne, even) },
			want: map[string]componentFloors{"member-a": {3, 6}, "member-b": {1, 6}}},
		{name: "a decoder weight on a cluster without an engine goes unused", source: func() *v1beta1.InferenceService { return componentSplitSource(1, 3, twoToOne, even) },
			want: map[string]componentFloors{"member-a": {1, 3}, "member-b": {0, 0}}},
		{name: "a decoder share above its scaled ceiling holds the plan", source: func() *v1beta1.InferenceService {
			source := componentSplitSource(4, 12, even, map[string]int32{"member-a": 3, "member-b": 1})
			source.Spec.Placement.Split = &v1beta1.SplitSpec{MaxPerCluster: engineCap(2)}
			return source
		}, wantErr: `decoder share 9 on cluster "member-a" exceeds its ceiling of 6 replicas`},
		{name: "fewer decoders than engine members holds the plan", source: func() *v1beta1.InferenceService {
			return componentSplitSource(2, 1, even, map[string]int32{"member-a": 3, "member-b": 1})
		}, wantErr: "decoder minimum 1 cannot give each of the 2 homes"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := settledFloors(t, tt.source())
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("floors (-want +got):\n%s", diff)
			}
		})
	}
}

// Decoder weights equal to the engine's must reproduce the plan the engine
// weights make on their own: the same shares and the same member floors.
func TestEqualDecoderWeightsMatchTheEngineWeightsAlone(t *testing.T) {
	for _, tt := range []struct {
		name             string
		engine, decoder  int
		weights          map[string]int32
		maxPerCluster    int32
		splitReplicasSet int32
	}{
		{name: "even weights", engine: 60, decoder: 40, weights: map[string]int32{"member-a": 1, "member-b": 1}},
		{name: "weighted with a remainder", engine: 60, decoder: 40, weights: map[string]int32{"member-a": 2, "member-b": 1}},
		{name: "prefill-heavy", engine: 5, decoder: 2, weights: map[string]int32{"member-a": 3, "member-b": 2}},
		{name: "split replicas size the fleet", engine: 60, decoder: 40, weights: map[string]int32{"member-a": 2, "member-b": 1}, splitReplicasSet: 30},
		{name: "a ceiling", engine: 12, decoder: 36, weights: map[string]int32{"member-a": 1, "member-b": 1}, maxPerCluster: 8},
	} {
		t.Run(tt.name, func(t *testing.T) {
			shared := ratioSplitSource(tt.engine, ptr.To(tt.decoder))
			shared.Spec.Placement.ClusterAffinity = weightedAffinity(tt.weights)
			perComponent := componentSplitSource(tt.engine, tt.decoder, tt.weights, tt.weights)
			for _, source := range []*v1beta1.InferenceService{shared, perComponent} {
				if tt.splitReplicasSet > 0 || tt.maxPerCluster > 0 {
					source.Spec.Placement.Split = &v1beta1.SplitSpec{MaxPerCluster: engineCap(tt.maxPerCluster)}
					if tt.splitReplicasSet > 0 {
						source.Spec.Placement.Split.Replicas = ptr.To(tt.splitReplicasSet)
					}
				}
			}
			want, wantErr := settledFloors(t, shared)
			got, gotErr := settledFloors(t, perComponent)
			if (wantErr == nil) != (gotErr == nil) {
				t.Fatalf("errors differ: engine weights %v, decoder weights %v", wantErr, gotErr)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("floors with decoder weights differ (-engine weights +decoder weights):\n%s", diff)
			}
		})
	}
}

func TestComponentMatchesArePersisted(t *testing.T) {
	twoToOne := map[string]int32{"member-a": 2, "member-b": 1}
	perComponent, err := desiredSplitPlan(componentSplitSource(4, 12, twoToOne, map[string]int32{"member-a": 1, "member-b": 3}), twoMembers(), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []v1beta1.ComponentAffinityMatch{{Component: v1beta1.DecoderComponent, Weight: ptr.To[int64](3), MinWeight: ptr.To[int64](3)}}
	if diff := cmp.Diff(want, perComponent.Assignments["member-b"].ComponentMatches); diff != "" {
		t.Errorf("member-b component matches (-want +got):\n%s", diff)
	}

	proposal, err := desiredSplitPlan(ratioSplitSource(4, ptr.To(12)), twoMembers(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, assignment := range proposal.Assignments {
		if assignment.ComponentMatches != nil {
			t.Errorf("%s: a plan without weights records component matches %v", name, assignment.ComponentMatches)
		}
	}
}

func TestDecoderWeightsChangeTheAcceptedPlan(t *testing.T) {
	twoToOne := map[string]int32{"member-a": 2, "member-b": 1}
	digest := func(decoder map[string]int32) string {
		proposal, err := desiredSplitPlan(componentSplitSource(4, 12, twoToOne, decoder), twoMembers(), nil)
		if err != nil {
			t.Fatal(err)
		}
		return proposal.InputDigest
	}
	even := digest(map[string]int32{"member-a": 1, "member-b": 1})
	if even != digest(map[string]int32{"member-a": 1, "member-b": 1}) {
		t.Error("the same decoder weights produced different plans")
	}
	if even == digest(map[string]int32{"member-a": 2, "member-b": 1}) {
		t.Error("new decoder weights kept the accepted plan")
	}
}

func TestServingFloorsFollowTheServiceRatio(t *testing.T) {
	twoToOne := map[string]int32{"member-a": 2, "member-b": 1}
	status := func(ready int32) *v1beta1.InferenceReplicaStatus {
		out := &v1beta1.InferenceReplicaStatus{ReadyReplicas: ready}
		for i := range ready {
			out.InstanceStatuses = append(out.InstanceStatuses, v1beta1.OMENativeInstanceStatus{Index: i, Phase: v1beta1.OMENativeInstanceReady, Admitted: true})
		}
		return out
	}
	ready := func(engine, decoder int32) map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus {
		return map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus{v1beta1.EngineComponent: status(engine), v1beta1.DecoderComponent: status(decoder)}
	}
	for _, tt := range []struct {
		name   string
		source *v1beta1.InferenceService
		want   map[string]int32
	}{
		// 3 prefill and 6 decode serve twice what 1 prefill and 6 decode do in a
		// 1:3 service.
		{name: "own decoder weights weigh each member against the service ratio",
			source: componentSplitSource(4, 12, twoToOne, map[string]int32{"member-a": 1, "member-b": 1}), want: map[string]int32{"member-a": 2, "member-b": 1}},
		{name: "matching weights keep each member's own floors",
			source: componentSplitSource(4, 12, twoToOne, twoToOne), want: map[string]int32{"member-a": 3, "member-b": 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			proposal, err := desiredSplitPlan(tt.source, twoMembers(), nil)
			if err != nil {
				t.Fatal(err)
			}
			candidates := settledCandidates(proposal.Assignments)
			tt.source.Status.Placement = &v1beta1.PlacementStatus{Candidates: candidates}
			components := []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
			got := map[string]int32{}
			for _, candidate := range candidates {
				floors, err := splitMemberFloors(tt.source, candidates, candidate, nil)
				if err != nil {
					t.Fatal(err)
				}
				memberFloors := floorMap(floors)
				got[candidate.Cluster] = placementReadyReplicas(components, ready(memberFloors[v1beta1.EngineComponent], memberFloors[v1beta1.DecoderComponent]), servingFloors(tt.source, memberFloors))
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("ready units (-want +got):\n%s", diff)
			}
		})
	}
}

func TestProportionalWeights(t *testing.T) {
	for _, tt := range []struct {
		name string
		a, b map[string]int64
		want bool
	}{
		{name: "equal", a: map[string]int64{"member-a": 2, "member-b": 1}, b: map[string]int64{"member-a": 2, "member-b": 1}, want: true},
		{name: "scaled", a: map[string]int64{"member-a": 2, "member-b": 1}, b: map[string]int64{"member-a": 6, "member-b": 3}, want: true},
		{name: "different", a: map[string]int64{"member-a": 2, "member-b": 1}, b: map[string]int64{"member-a": 1, "member-b": 1}},
		{name: "a member only one map weighs", a: map[string]int64{"member-a": 1}, b: map[string]int64{"member-a": 1, "member-b": 1}},
		{name: "large weights do not overflow", a: map[string]int64{"member-a": 1 << 62, "member-b": 1 << 61}, b: map[string]int64{"member-a": 2, "member-b": 1}, want: true},
		{name: "both empty", want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := proportionalWeights(tt.a, tt.b); got != tt.want {
				t.Errorf("proportionalWeights = %t, want %t", got, tt.want)
			}
		})
	}
}

// copyMinimums holds a member copy's engine and decoder minReplicas.
type copyMinimums struct{ Engine, Decoder int }

// threeMemberMinimums plans a source over member-a, member-b and member-c, one
// term each with no engine weight and the given decoder weight (zero leaves it
// unset), settles every member at its desired share, and returns the
// minReplicas each member copy gets.
func threeMemberMinimums(t *testing.T, engine, decoder int, decoderWeights [3]int32) (map[string]copyMinimums, error) {
	t.Helper()
	names := []string{"member-a", "member-b", "member-c"}
	source := ratioSplitSource(engine, ptr.To(decoder))
	source.Spec.Placement.ClusterAffinity = nil
	var clusters []v1beta1.WorkloadCluster
	for i, name := range names {
		term := v1beta1.ClusterAffinityTerm{MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{name}}}}
		if decoderWeights[i] > 0 {
			term.Weights = &v1beta1.ClusterAffinityWeights{Decoder: ptr.To(decoderWeights[i])}
		}
		source.Spec.Placement.ClusterAffinity = append(source.Spec.Placement.ClusterAffinity, term)
		clusters = append(clusters, wc(name, true, nil))
	}
	proposal, err := desiredSplitPlan(source, clusters, nil)
	if err != nil {
		return nil, err
	}
	var candidates []v1beta1.CandidatePlacement
	for _, name := range names {
		a := proposal.Assignments[name]
		a.CurrentReplicas = a.DesiredReplicas
		candidates = append(candidates, v1beta1.CandidatePlacement{Cluster: name, Allocation: &a})
	}
	out := map[string]copyMinimums{}
	for _, candidate := range candidates {
		bounds, err := splitPlannedBounds(source, candidates, candidate, nil)
		if err != nil {
			t.Fatalf("%s: %v", candidate.Cluster, err)
		}
		member := source.DeepCopy()
		setPlannedReplicas(member, bounds)
		out[candidate.Cluster] = copyMinimums{Engine: *member.Spec.Engine.MinReplicas, Decoder: *member.Spec.Decoder.MinReplicas}
	}
	return out, nil
}

// A one-point weight edit can switch the decoder from following the engine
// shares to its own weights. Over three equal clusters the engine lands 2,1,1;
// decoder weights 99,99,99 follow it to 20,10,10, while 99,99,100 split by
// themselves to 13,13,14, which moves seven decoders.
func TestDecoderWeightEditJumpsFromEngineSharesToOwnWeights(t *testing.T) {
	follow, err := threeMemberMinimums(t, 4, 40, [3]int32{99, 99, 99})
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(map[string]copyMinimums{"member-a": {2, 20}, "member-b": {1, 10}, "member-c": {1, 10}}, follow); diff != "" {
		t.Errorf("minimums with weights 99,99,99 (-want +got):\n%s", diff)
	}
	own, err := threeMemberMinimums(t, 4, 40, [3]int32{99, 99, 100})
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(map[string]copyMinimums{"member-a": {2, 13}, "member-b": {1, 13}, "member-c": {1, 14}}, own); diff != "" {
		t.Errorf("minimums with weights 99,99,100 (-want +got):\n%s", diff)
	}
	moved := 0
	for name, before := range follow {
		moved += max(before.Decoder-own[name].Decoder, 0)
	}
	if moved != 7 {
		t.Errorf("decoders moved = %d, want 7", moved)
	}
}

// With fewer decoders than clusters that hold an engine, the plan holds: 4
// prefill and 2 decode over three equal clusters put engines on all three.
// One more decoder gives each cluster one and resolves it.
func TestPrefillHeavyServiceHoldsOverThreeClusters(t *testing.T) {
	if _, err := threeMemberMinimums(t, 4, 2, [3]int32{}); err == nil || !strings.Contains(err.Error(), "decoder minimum 2 cannot give each of the 3") {
		t.Fatalf("error = %v, want the plan held for 2 decoders over 3 clusters", err)
	}
	got, err := threeMemberMinimums(t, 4, 3, [3]int32{})
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(map[string]copyMinimums{"member-a": {2, 1}, "member-b": {1, 1}, "member-c": {1, 1}}, got); diff != "" {
		t.Errorf("minimums with 3 decoders (-want +got):\n%s", diff)
	}
}
