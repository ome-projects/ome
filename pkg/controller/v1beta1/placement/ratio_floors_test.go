package placement

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// ratioSplitSource declares a static Split source whose engine and decoder
// minimums set the per-member ratio.
func ratioSplitSource(engine int, decoder *int) *v1beta1.InferenceService {
	source := srcISVCSplit("accelerator=gpu-a", 0)
	source.Spec.Placement.Split = nil
	source.Spec.Engine.MinReplicas = ptr.To(engine)
	source.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: decoder}, Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Image: "img"}}}
	return source
}

func weightedAffinity(weights map[string]int32) []v1beta1.ClusterAffinityTerm {
	var out []v1beta1.ClusterAffinityTerm
	for _, name := range []string{"member-a", "member-b"} {
		out = append(out, v1beta1.ClusterAffinityTerm{Weights: &v1beta1.ClusterAffinityWeights{Engine: ptr.To(weights[name])}, MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{name}}}})
	}
	return out
}

// settledCandidates turns a proposal into the accepted plan's candidates with
// every member at its desired share.
func settledCandidates(assignments map[string]v1beta1.CandidateAllocationStatus) []v1beta1.CandidatePlacement {
	var out []v1beta1.CandidatePlacement
	for _, name := range []string{"member-a", "member-b"} {
		a := assignments[name]
		a.CurrentReplicas = a.DesiredReplicas
		out = append(out, v1beta1.CandidatePlacement{Cluster: name, Allocation: &a})
	}
	return out
}

func TestSplitMemberFloorsFollowTheRatio(t *testing.T) {
	type member struct{ Engine, Decoder int32 }
	for _, tt := range []struct {
		name    string
		source  func() *v1beta1.InferenceService
		want    map[string]member
		wantErr string
	}{
		{name: "equal weights split both components", source: func() *v1beta1.InferenceService { return ratioSplitSource(60, ptr.To(40)) },
			want: map[string]member{"member-a": {30, 20}, "member-b": {30, 20}}},
		{name: "weighted shares keep the decoder sum exact", source: func() *v1beta1.InferenceService {
			source := ratioSplitSource(60, ptr.To(40))
			source.Spec.Placement.ClusterAffinity = weightedAffinity(map[string]int32{"member-a": 2, "member-b": 1})
			return source
		}, want: map[string]member{"member-a": {40, 27}, "member-b": {20, 13}}},
		{name: "zero decoder minimum places no decoder", source: func() *v1beta1.InferenceService { return ratioSplitSource(60, ptr.To(0)) },
			want: map[string]member{"member-a": {30, 0}, "member-b": {30, 0}}},
		{name: "undeclared decoder minimum pairs one decoder per engine", source: func() *v1beta1.InferenceService { return ratioSplitSource(60, nil) },
			want: map[string]member{"member-a": {30, 30}, "member-b": {30, 30}}},
		{name: "split replicas size the fleet in the declared ratio", source: func() *v1beta1.InferenceService {
			source := ratioSplitSource(60, ptr.To(40))
			source.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](30)}
			return source
		}, want: map[string]member{"member-a": {15, 10}, "member-b": {15, 10}}},
		{name: "split replicas without an engine minimum keep the declared decoder minimum", source: func() *v1beta1.InferenceService {
			source := ratioSplitSource(60, ptr.To(40))
			source.Spec.Engine.MinReplicas = nil
			source.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](30)}
			return source
		}, want: map[string]member{"member-a": {15, 20}, "member-b": {15, 20}}},
		{name: "decoder minimum below the positive share count holds the plan", source: func() *v1beta1.InferenceService { return ratioSplitSource(60, ptr.To(1)) },
			wantErr: "decoder minimum 1 cannot give each of the 2 homes with a positive engine share at least one replica"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := tt.source()
			clusters := []v1beta1.WorkloadCluster{wc("member-a", true, map[string]string{"accelerator": "gpu-a"}), wc("member-b", true, map[string]string{"accelerator": "gpu-a"})}
			proposal, err := desiredSplitPlan(source, clusters, nil)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			candidates := settledCandidates(proposal.Assignments)
			got := map[string]member{}
			for _, candidate := range candidates {
				floors, err := splitMemberFloors(source, candidates, candidate, nil)
				if err != nil {
					t.Fatalf("%s: %v", candidate.Cluster, err)
				}
				var h member
				for _, floor := range floors {
					switch floor.Component {
					case v1beta1.EngineComponent:
						h.Engine = floor.Replicas
					case v1beta1.DecoderComponent:
						h.Decoder = floor.Replicas
					default:
						t.Fatalf("%s: unexpected %s floor", candidate.Cluster, floor.Component)
					}
				}
				got[candidate.Cluster] = h
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestSplitMemberFloorsFollowCurrentShares(t *testing.T) {
	for _, tt := range []struct {
		name    string
		source  func() *v1beta1.InferenceService
		current map[string]int32
		want    map[string]int32
		// desired and the weights describe the plan; unset, every member is
		// still moving and the decoder follows the engines. held is the
		// decoder minimum each member copy holds; a member it omits has none.
		desired               map[string]int32
		weight, decoderWeight map[string]int64
		held                  map[string]int32
	}{
		// A growing member and an outgoing member: the decoder follows each member's
		// current engine target, not the plan it is moving toward.
		{name: "move within the fleet floor", source: func() *v1beta1.InferenceService { return ratioSplitSource(60, ptr.To(40)) },
			current: map[string]int32{"member-a": 45, "member-b": 15}, want: map[string]int32{"member-a": 30, "member-b": 10}},
		{name: "surge above the fleet floor keeps the ratio", source: func() *v1beta1.InferenceService { return ratioSplitSource(60, ptr.To(40)) },
			current: map[string]int32{"member-a": 60, "member-b": 30}, want: map[string]int32{"member-a": 40, "member-b": 20}},
		{name: "paired decoders follow every engine", source: func() *v1beta1.InferenceService {
			source := ratioSplitSource(2, nil)
			source.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](2)}
			return source
		}, current: map[string]int32{"member-a": 1, "member-b": 1, "member-c": 1}, want: map[string]int32{"member-a": 1, "member-b": 1, "member-c": 1}},
		// member-b's engine target falls by one while member-a and member-c keep
		// theirs, each holding 1 decoder. Until it lands, the engines in flight
		// carry one decoder more than the minimum: 4 rather than 3, split 2/1/1
		// by the engine targets. member-a already runs its final engine target
		// and has its final decoder, so it does not start a second one.
		{name: "members at their target start no decoder while another shrinks", source: func() *v1beta1.InferenceService { return ratioSplitSource(5, ptr.To(3)) },
			current: map[string]int32{"member-a": 2, "member-b": 2, "member-c": 2}, desired: map[string]int32{"member-a": 2, "member-b": 1, "member-c": 2},
			weight: map[string]int64{"member-a": 2, "member-b": 1, "member-c": 2}, held: map[string]int32{"member-a": 1, "member-b": 1, "member-c": 1},
			want: map[string]int32{"member-a": 1, "member-b": 1, "member-c": 1}},
		// The same with equal decoder weights: 7 decoders in flight split 3/2/2,
		// and each member holds 2 of the minimum of 6.
		{name: "members at their target start no weighted decoder while another shrinks", source: func() *v1beta1.InferenceService { return ratioSplitSource(11, ptr.To(6)) },
			current: map[string]int32{"member-a": 4, "member-b": 4, "member-c": 4}, desired: map[string]int32{"member-a": 4, "member-b": 3, "member-c": 4},
			weight: map[string]int64{"member-a": 4, "member-b": 3, "member-c": 4}, decoderWeight: map[string]int64{"member-a": 1, "member-b": 1, "member-c": 1},
			held: map[string]int32{"member-a": 2, "member-b": 2, "member-c": 2}, want: map[string]int32{"member-a": 2, "member-b": 2, "member-c": 2}},
		// member-c joins while member-b shrinks. Until member-c has engines, the
		// 8 decoders stay on member-a and member-b, 4 each. member-a is at its
		// engine target and will end with 2 decoders, but it keeps 4 until the
		// move gets there: no extra engines are in flight yet.
		{name: "member at its target keeps its decoders while another joins", source: func() *v1beta1.InferenceService { return ratioSplitSource(12, ptr.To(8)) },
			current: map[string]int32{"member-a": 6, "member-b": 6, "member-c": 0}, desired: map[string]int32{"member-a": 6, "member-b": 3, "member-c": 3},
			weight: map[string]int64{"member-a": 2, "member-b": 1, "member-c": 1}, decoderWeight: map[string]int64{"member-a": 1, "member-b": 1, "member-c": 2},
			held: map[string]int32{"member-a": 4, "member-b": 4}, want: map[string]int32{"member-a": 4, "member-b": 4, "member-c": 0}},
		// member-c has reached its 3 engines while member-b still runs 6, so 15
		// engines carry 10 decoders, split 3/2/5. member-c, whose copy holds no
		// decoder yet, takes its final 4 and does not start a fifth. member-a
		// keeps 3 of the 4 it holds.
		{name: "joiner at its target starts no decoder while another shrinks", source: func() *v1beta1.InferenceService { return ratioSplitSource(12, ptr.To(8)) },
			current: map[string]int32{"member-a": 6, "member-b": 6, "member-c": 3}, desired: map[string]int32{"member-a": 6, "member-b": 3, "member-c": 3},
			weight: map[string]int64{"member-a": 2, "member-b": 1, "member-c": 1}, decoderWeight: map[string]int64{"member-a": 1, "member-b": 1, "member-c": 2},
			held: map[string]int32{"member-a": 4, "member-b": 4}, want: map[string]int32{"member-a": 3, "member-b": 2, "member-c": 4}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := tt.source()
			var candidates []v1beta1.CandidatePlacement
			for _, name := range []string{"member-a", "member-b", "member-c"} {
				if current, ok := tt.current[name]; ok {
					allocation := &v1beta1.CandidateAllocationStatus{ClusterUID: types.UID(name + "-uid"), CurrentReplicas: current, DesiredReplicas: tt.desired[name]}
					if weight, ok := tt.weight[name]; ok {
						allocation.Weight = ptr.To(weight)
					}
					if weight, ok := tt.decoderWeight[name]; ok {
						allocation.ComponentMatches = []v1beta1.ComponentAffinityMatch{{Component: v1beta1.DecoderComponent, Weight: ptr.To(weight)}}
					}
					candidates = append(candidates, v1beta1.CandidatePlacement{Cluster: name, Allocation: allocation})
				}
			}
			for _, candidate := range candidates {
				var held map[v1beta1.ComponentType]int32
				if tt.held != nil {
					held = map[v1beta1.ComponentType]int32{}
					if decoders, ok := tt.held[candidate.Cluster]; ok {
						held[v1beta1.DecoderComponent] = decoders
					}
				}
				floors, err := splitMemberFloors(source, candidates, candidate, held)
				if err != nil {
					t.Fatal(err)
				}
				for _, floor := range floors {
					if floor.Component == v1beta1.DecoderComponent {
						if diff := cmp.Diff(tt.want[candidate.Cluster], floor.Replicas); diff != "" {
							t.Fatalf("%s decoder floor (-want +got):\n%s", candidate.Cluster, diff)
						}
					}
				}
			}
		})
	}
}

func TestSplitPlannedBoundsScaleTheCeiling(t *testing.T) {
	for _, tt := range []struct {
		name    string
		ceiling int32
		want    map[v1beta1.ComponentType]plannedBound
	}{
		{name: "pinned without a ceiling", want: map[v1beta1.ComponentType]plannedBound{v1beta1.EngineComponent: {Floor: 30}, v1beta1.DecoderComponent: {Floor: 20}}},
		{name: "ceiling follows the ratio", ceiling: 50, want: map[v1beta1.ComponentType]plannedBound{v1beta1.EngineComponent: {Floor: 30, Ceiling: 50}, v1beta1.DecoderComponent: {Floor: 20, Ceiling: 34}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := ratioSplitSource(60, ptr.To(40))
			source.Spec.Placement.Split = &v1beta1.SplitSpec{MaxPerCluster: engineCap(tt.ceiling)}
			clusters := []v1beta1.WorkloadCluster{wc("member-a", true, map[string]string{"accelerator": "gpu-a"}), wc("member-b", true, map[string]string{"accelerator": "gpu-a"})}
			proposal, err := desiredSplitPlan(source, clusters, nil)
			if err != nil {
				t.Fatal(err)
			}
			candidates := settledCandidates(proposal.Assignments)
			got, err := splitPlannedBounds(source, candidates, candidates[0], nil)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
			member := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MaxReplicas: 9}},
				Decoder: &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MaxReplicas: 9}},
				Router:  &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1), MaxReplicas: 7}},
			}}
			router := member.Spec.Router.DeepCopy()
			setPlannedReplicas(member, got)
			engineMax, decoderMax := 30, 20
			if tt.ceiling > 0 {
				engineMax, decoderMax = 50, 34
			}
			if diff := cmp.Diff(v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(30), MaxReplicas: engineMax}, member.Spec.Engine.ComponentExtensionSpec); diff != "" {
				t.Errorf("engine bounds (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(20), MaxReplicas: decoderMax}, member.Spec.Decoder.ComponentExtensionSpec); diff != "" {
				t.Errorf("decoder bounds (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(router, member.Spec.Router); diff != "" {
				t.Errorf("router policy changed (-want +got):\n%s", diff)
			}
		})
	}
}

func TestScaleCeilRoundsUp(t *testing.T) {
	for _, tt := range []struct {
		name                          string
		count, numerator, denominator int64
		want                          int64
	}{
		{name: "exact multiple", count: 60, numerator: 40, denominator: 60, want: 40},
		{name: "remainder rounds up", count: 50, numerator: 40, denominator: 60, want: 34},
		{name: "zero count", count: 0, numerator: 7, denominator: 3, want: 0},
		{name: "unit denominator", count: 9, numerator: 4, denominator: 1, want: 36},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := scaleCeil(tt.count, tt.numerator, tt.denominator); got != tt.want {
				t.Fatalf("scaleCeil(%d, %d, %d) = %d, want %d", tt.count, tt.numerator, tt.denominator, got, tt.want)
			}
		})
	}
	for count := int64(0); count <= 40; count++ {
		for numerator := int64(0); numerator <= 40; numerator++ {
			for denominator := int64(1); denominator <= 40; denominator++ {
				got, product := scaleCeil(count, numerator, denominator), count*numerator
				if got*denominator < product || (got-1)*denominator >= product {
					t.Fatalf("scaleCeil(%d, %d, %d) = %d is not the smallest count whose %d-fold reaches %d", count, numerator, denominator, got, denominator, product)
				}
			}
		}
	}
}

func TestPrimaryUnitsFollowRatioFloors(t *testing.T) {
	admitted := func(n int) []bool {
		out := make([]bool, n)
		for i := range out {
			out[i] = true
		}
		return out
	}
	status := func(component v1beta1.ComponentType, admittedCount int, ready int32) *v1beta1.InferenceReplicaStatus {
		s := irWithInstances(component, admitted(admittedCount)...).Status
		s.ReadyReplicas = ready
		return &s
	}
	pd := []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	for _, tt := range []struct {
		name                  string
		comps                 []v1beta1.ComponentType
		floors                map[v1beta1.ComponentType]int32
		engineAdmitted        int
		engineReady           int32
		decoderAdmitted       int
		decoderReady          int32
		wantAdmitted, wantRdy int32
	}{
		{name: "half the decoders admit half the units", comps: pd, floors: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 30, v1beta1.DecoderComponent: 20},
			engineAdmitted: 30, engineReady: 30, decoderAdmitted: 10, decoderReady: 10, wantAdmitted: 15, wantRdy: 15},
		{name: "complete ratio floors are whole units", comps: pd, floors: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 30, v1beta1.DecoderComponent: 20},
			engineAdmitted: 30, engineReady: 30, decoderAdmitted: 20, decoderReady: 20, wantAdmitted: 30, wantRdy: 30},
		{name: "surplus decoders do not add units", comps: pd, floors: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 30, v1beta1.DecoderComponent: 20},
			engineAdmitted: 20, engineReady: 20, decoderAdmitted: 40, decoderReady: 40, wantAdmitted: 20, wantRdy: 20},
		{name: "equal floors are the minimum count", comps: pd, floors: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 4, v1beta1.DecoderComponent: 4},
			engineAdmitted: 3, engineReady: 3, decoderAdmitted: 2, decoderReady: 1, wantAdmitted: 2, wantRdy: 1},
		{name: "zero decoder floor is skipped", comps: pd, floors: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 30, v1beta1.DecoderComponent: 0},
			engineAdmitted: 12, engineReady: 11, decoderAdmitted: 0, decoderReady: 0, wantAdmitted: 12, wantRdy: 11},
		{name: "engine only is the engine count", comps: []v1beta1.ComponentType{v1beta1.EngineComponent}, floors: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 30},
			engineAdmitted: 7, engineReady: 5, wantAdmitted: 7, wantRdy: 5},
		{name: "unknown floors are the minimum count", comps: pd, floors: nil,
			engineAdmitted: 3, engineReady: 3, decoderAdmitted: 2, decoderReady: 1, wantAdmitted: 2, wantRdy: 1},
		{name: "a decoder below its share leaves one ready unit", comps: pd, floors: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 1, v1beta1.DecoderComponent: 3},
			engineAdmitted: 1, engineReady: 1, decoderAdmitted: 3, decoderReady: 2, wantAdmitted: 1, wantRdy: 1},
		{name: "a component with no ready instance leaves no ready unit", comps: pd, floors: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 1, v1beta1.DecoderComponent: 3},
			engineAdmitted: 1, engineReady: 1, decoderAdmitted: 3, decoderReady: 0, wantAdmitted: 1, wantRdy: 0},
		{name: "a member short of a full admitted unit counts nothing ready", comps: pd, floors: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 1, v1beta1.DecoderComponent: 3},
			engineAdmitted: 1, engineReady: 1, decoderAdmitted: 2, decoderReady: 2, wantAdmitted: 0, wantRdy: 0},
		{name: "readiness above one unit still rounds down", comps: pd, floors: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 3, v1beta1.DecoderComponent: 9},
			engineAdmitted: 3, engineReady: 3, decoderAdmitted: 9, decoderReady: 8, wantAdmitted: 3, wantRdy: 2},
		{name: "a member without an engine instance is not lifted", comps: pd, floors: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 0, v1beta1.DecoderComponent: 3},
			engineAdmitted: 0, engineReady: 0, decoderAdmitted: 3, decoderReady: 3, wantAdmitted: 0, wantRdy: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			statuses := map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus{v1beta1.EngineComponent: status(v1beta1.EngineComponent, tt.engineAdmitted, tt.engineReady)}
			if len(tt.comps) > 1 {
				statuses[v1beta1.DecoderComponent] = status(v1beta1.DecoderComponent, tt.decoderAdmitted, tt.decoderReady)
			}
			if diff := cmp.Diff(tt.wantAdmitted, placementAdmittedReplicas(tt.comps, statuses, tt.floors)); diff != "" {
				t.Errorf("admitted units (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantRdy, placementReadyReplicas(tt.comps, statuses, tt.floors)); diff != "" {
				t.Errorf("ready units (-want +got):\n%s", diff)
			}
		})
	}
}
