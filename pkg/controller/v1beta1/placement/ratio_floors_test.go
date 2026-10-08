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
// minimums set the per-home ratio.
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
		out = append(out, v1beta1.ClusterAffinityTerm{Weight: ptr.To(weights[name]), MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{name}}}})
	}
	return out
}

// settledCandidates turns a proposal into the accepted plan's candidates with
// every home at its desired share.
func settledCandidates(assignments map[string]v1beta1.CandidateAllocationStatus) []v1beta1.CandidatePlacement {
	var out []v1beta1.CandidatePlacement
	for _, name := range []string{"member-a", "member-b"} {
		a := assignments[name]
		a.CurrentReplicas = a.DesiredReplicas
		out = append(out, v1beta1.CandidatePlacement{Cluster: name, Allocation: &a})
	}
	return out
}

func TestSplitHomeFloorsFollowTheRatio(t *testing.T) {
	type home struct{ Engine, Decoder int32 }
	for _, tt := range []struct {
		name    string
		source  func() *v1beta1.InferenceService
		want    map[string]home
		wantErr string
	}{
		{name: "equal weights split both components", source: func() *v1beta1.InferenceService { return ratioSplitSource(60, ptr.To(40)) },
			want: map[string]home{"member-a": {30, 20}, "member-b": {30, 20}}},
		{name: "weighted shares keep the decoder sum exact", source: func() *v1beta1.InferenceService {
			source := ratioSplitSource(60, ptr.To(40))
			source.Spec.Placement.ClusterAffinity = weightedAffinity(map[string]int32{"member-a": 2, "member-b": 1})
			return source
		}, want: map[string]home{"member-a": {40, 27}, "member-b": {20, 13}}},
		{name: "zero decoder minimum places no decoder", source: func() *v1beta1.InferenceService { return ratioSplitSource(60, ptr.To(0)) },
			want: map[string]home{"member-a": {30, 0}, "member-b": {30, 0}}},
		{name: "undeclared decoder minimum pairs one decoder per engine", source: func() *v1beta1.InferenceService { return ratioSplitSource(60, nil) },
			want: map[string]home{"member-a": {30, 30}, "member-b": {30, 30}}},
		{name: "split replicas size the fleet in the declared ratio", source: func() *v1beta1.InferenceService {
			source := ratioSplitSource(60, ptr.To(40))
			source.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](30)}
			return source
		}, want: map[string]home{"member-a": {15, 10}, "member-b": {15, 10}}},
		{name: "split replicas without an engine minimum keep the declared decoder minimum", source: func() *v1beta1.InferenceService {
			source := ratioSplitSource(60, ptr.To(40))
			source.Spec.Engine.MinReplicas = nil
			source.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](30)}
			return source
		}, want: map[string]home{"member-a": {15, 20}, "member-b": {15, 20}}},
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
			got := map[string]home{}
			for _, candidate := range candidates {
				floors, err := splitHomeFloors(source, candidates, candidate)
				if err != nil {
					t.Fatalf("%s: %v", candidate.Cluster, err)
				}
				var h home
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

func TestSplitHomeFloorsFollowCurrentShares(t *testing.T) {
	for _, tt := range []struct {
		name    string
		source  func() *v1beta1.InferenceService
		current map[string]int32
		want    map[string]int32
	}{
		// A growing home and an outgoing home: the decoder follows each home's
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
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := tt.source()
			var candidates []v1beta1.CandidatePlacement
			for _, name := range []string{"member-a", "member-b", "member-c"} {
				if current, ok := tt.current[name]; ok {
					candidates = append(candidates, v1beta1.CandidatePlacement{Cluster: name, Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: types.UID(name + "-uid"), CurrentReplicas: current}})
				}
			}
			for _, candidate := range candidates {
				floors, err := splitHomeFloors(source, candidates, candidate)
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
			source.Spec.Placement.Split = &v1beta1.SplitSpec{MaxReplicasPerCluster: tt.ceiling}
			clusters := []v1beta1.WorkloadCluster{wc("member-a", true, map[string]string{"accelerator": "gpu-a"}), wc("member-b", true, map[string]string{"accelerator": "gpu-a"})}
			proposal, err := desiredSplitPlan(source, clusters, nil)
			if err != nil {
				t.Fatal(err)
			}
			candidates := settledCandidates(proposal.Assignments)
			got, err := splitPlannedBounds(source, candidates, candidates[0])
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
