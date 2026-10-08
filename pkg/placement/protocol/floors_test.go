package protocol

import (
	"encoding/json"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func acceptedFloors() []v1beta1.PlacementComponentFloor {
	return []v1beta1.PlacementComponentFloor{
		{Component: v1beta1.EngineComponent, Replicas: 4},
		{Component: v1beta1.DecoderComponent, Replicas: 4},
		{Component: v1beta1.RouterComponent, Replicas: 1},
	}
}

func TestReplicaFloorValidation(t *testing.T) {
	for _, tt := range []struct {
		name    string
		floors  []v1beta1.PlacementComponentFloor
		want    int32
		wantErr bool
	}{
		{name: "engine decoder and independent router", floors: acceptedFloors(), want: 4},
		{name: "engine", floors: acceptedFloors()[:1], want: 4},
		{name: "decoder", floors: acceptedFloors()[1:2], want: 4},
		{name: "maximum count", floors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: math.MaxInt32}}, want: math.MaxInt32},
		{name: "missing", wantErr: true},
		{name: "only router", floors: acceptedFloors()[2:], wantErr: true},
		{name: "engine leads a smaller decoder floor", floors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 4}, {Component: v1beta1.DecoderComponent, Replicas: 2}}, want: 4},
		{name: "engine leads a larger decoder floor", floors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.DecoderComponent, Replicas: 5}, {Component: v1beta1.EngineComponent, Replicas: 2}}, want: 2},
		{name: "duplicate", floors: append(acceptedFloors(), acceptedFloors()[0]), wantErr: true},
		{name: "unknown component", floors: []v1beta1.PlacementComponentFloor{{Component: "unknown", Replicas: 4}}, wantErr: true},
		{name: "zero engine", floors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent}}},
		{name: "zero decoder", floors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.DecoderComponent}}},
		{name: "zero whole units with shared router", floors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent}, {Component: v1beta1.DecoderComponent}, {Component: v1beta1.RouterComponent, Replicas: 2}}},
		{name: "zero router with positive units", floors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 3}, {Component: v1beta1.RouterComponent}}, want: 3},
		{name: "zero engine with a positive decoder", floors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent}, {Component: v1beta1.DecoderComponent, Replicas: 1}}},
		{name: "positive engine with a zero decoder", floors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 1}, {Component: v1beta1.DecoderComponent}}, want: 1},
		{name: "negative", floors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: -1}}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ValidateReplicaFloors(tt.floors)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("error (-want +got): %s: %v", diff, err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestResolveAndCheckReplicaFloors(t *testing.T) {
	for _, tt := range []struct {
		name       string
		change     func(*v1beta1.InferenceServiceSpec, *[]v1beta1.PlacementComponentFloor)
		resolveErr bool
		checkErr   bool
		want       []v1beta1.PlacementComponentFloor
	}{
		{name: "exact policy"},
		{name: "zero whole-unit floors", want: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent}, {Component: v1beta1.DecoderComponent}, {Component: v1beta1.RouterComponent, Replicas: 1}}, change: func(spec *v1beta1.InferenceServiceSpec, floors *[]v1beta1.PlacementComponentFloor) {
			spec.Engine.MinReplicas, spec.Decoder.MinReplicas = ptr.To(0), ptr.To(0)
			(*floors)[0].Replicas, (*floors)[1].Replicas = 0, 0
		}},
		{name: "zero shared router floor", want: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 4}, {Component: v1beta1.DecoderComponent, Replicas: 4}, {Component: v1beta1.RouterComponent}}, change: func(spec *v1beta1.InferenceServiceSpec, floors *[]v1beta1.PlacementComponentFloor) {
			spec.Router.MinReplicas = ptr.To(0)
			(*floors)[2].Replicas = 0
		}},
		{name: "unordered authority", change: func(_ *v1beta1.InferenceServiceSpec, floors *[]v1beta1.PlacementComponentFloor) {
			slices.Reverse(*floors)
		}},
		{name: "maximum does not change the floor", change: func(spec *v1beta1.InferenceServiceSpec, _ *[]v1beta1.PlacementComponentFloor) {
			spec.Engine.MaxReplicas = 10
		}},
		{name: "missing engine floor", resolveErr: true, checkErr: true, change: func(spec *v1beta1.InferenceServiceSpec, _ *[]v1beta1.PlacementComponentFloor) {
			spec.Engine.MinReplicas = nil
		}},
		{name: "zero engine floor differs from its accepted count", checkErr: true, change: func(spec *v1beta1.InferenceServiceSpec, _ *[]v1beta1.PlacementComponentFloor) {
			spec.Engine.MinReplicas = ptr.To(0)
		}},
		{name: "ratio floors", want: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 30}, {Component: v1beta1.DecoderComponent, Replicas: 20}, {Component: v1beta1.RouterComponent, Replicas: 1}}, change: func(spec *v1beta1.InferenceServiceSpec, floors *[]v1beta1.PlacementComponentFloor) {
			spec.Engine.MinReplicas, spec.Decoder.MinReplicas = ptr.To(30), ptr.To(20)
			(*floors)[0].Replicas, (*floors)[1].Replicas = 30, 20
		}},
		{name: "ratio floors with a zero decoder", want: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 30}, {Component: v1beta1.DecoderComponent}, {Component: v1beta1.RouterComponent, Replicas: 1}}, change: func(spec *v1beta1.InferenceServiceSpec, floors *[]v1beta1.PlacementComponentFloor) {
			spec.Engine.MinReplicas, spec.Decoder.MinReplicas = ptr.To(30), ptr.To(0)
			(*floors)[0].Replicas, (*floors)[1].Replicas = 30, 0
		}},
		{name: "ratio floors without the declared decoder", checkErr: true, change: func(spec *v1beta1.InferenceServiceSpec, floors *[]v1beta1.PlacementComponentFloor) {
			spec.Engine.MinReplicas, spec.Decoder = ptr.To(30), nil
			(*floors)[0].Replicas, (*floors)[1].Replicas = 30, 20
		}},
		{name: "ratio floors with a changed decoder count", checkErr: true, change: func(spec *v1beta1.InferenceServiceSpec, floors *[]v1beta1.PlacementComponentFloor) {
			spec.Engine.MinReplicas, spec.Decoder.MinReplicas = ptr.To(30), ptr.To(21)
			(*floors)[0].Replicas, (*floors)[1].Replicas = 30, 20
		}},
		{name: "negative floor", resolveErr: true, checkErr: true, change: func(spec *v1beta1.InferenceServiceSpec, _ *[]v1beta1.PlacementComponentFloor) {
			spec.Engine.MinReplicas = ptr.To(-1)
		}},
		{name: "count overflow", resolveErr: true, checkErr: true, change: func(spec *v1beta1.InferenceServiceSpec, _ *[]v1beta1.PlacementComponentFloor) {
			spec.Engine.MinReplicas = ptr.To(int(int64(math.MaxInt32) + 1))
		}},
		{name: "decoder floor differs from its accepted count", checkErr: true, change: func(spec *v1beta1.InferenceServiceSpec, _ *[]v1beta1.PlacementComponentFloor) {
			spec.Decoder.MinReplicas = ptr.To(5)
		}},
		{name: "changed whole-unit floor", checkErr: true, change: func(spec *v1beta1.InferenceServiceSpec, _ *[]v1beta1.PlacementComponentFloor) {
			spec.Engine.MinReplicas, spec.Decoder.MinReplicas = ptr.To(5), ptr.To(5)
		}},
		{name: "changed router floor", checkErr: true, change: func(spec *v1beta1.InferenceServiceSpec, _ *[]v1beta1.PlacementComponentFloor) {
			spec.Router.MinReplicas = ptr.To(2)
		}},
		{name: "missing actual component", checkErr: true, change: func(spec *v1beta1.InferenceServiceSpec, _ *[]v1beta1.PlacementComponentFloor) { spec.Router = nil }},
		{name: "missing contract component", checkErr: true, change: func(_ *v1beta1.InferenceServiceSpec, floors *[]v1beta1.PlacementComponentFloor) {
			*floors = (*floors)[:2]
		}},
		{name: "different inventory of same length", checkErr: true, change: func(spec *v1beta1.InferenceServiceSpec, floors *[]v1beta1.PlacementComponentFloor) {
			spec.Router = nil
			*floors = append((*floors)[:1], (*floors)[2])
		}},
		{name: "empty resolved inventory", resolveErr: true, checkErr: true, change: func(spec *v1beta1.InferenceServiceSpec, _ *[]v1beta1.PlacementComponentFloor) {
			*spec = v1beta1.InferenceServiceSpec{}
		}},
		{name: "invalid contract", checkErr: true, change: func(_ *v1beta1.InferenceServiceSpec, floors *[]v1beta1.PlacementComponentFloor) { *floors = nil }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			spec := v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(4)}},
				Decoder: &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(4)}},
				Router:  &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1)}},
			}
			floors := acceptedFloors()
			if tt.change != nil {
				tt.change(&spec, &floors)
			}
			before := spec.DeepCopy()
			got, err := ResolveReplicaFloors(spec.Engine, spec.Decoder, spec.Router)
			if diff := cmp.Diff(tt.resolveErr, err != nil); diff != "" {
				t.Fatalf("resolution: %s: %v", diff, err)
			}
			if tt.resolveErr && got != nil {
				t.Fatal("unresolved policy returned authority")
			}
			if !tt.resolveErr && !tt.checkErr {
				want := tt.want
				if want == nil {
					want = acceptedFloors()
				}
				if diff := cmp.Diff(want, got); diff != "" {
					t.Fatal(diff)
				}
			}
			err = CheckReplicaFloors(floors, spec.Engine, spec.Decoder, spec.Router)
			if diff := cmp.Diff(tt.checkErr, err != nil); diff != "" {
				t.Fatalf("verification: %s: %v", diff, err)
			}
			if diff := cmp.Diff(before, &spec); diff != "" {
				t.Fatalf("mutated resolved policy: %s", diff)
			}
		})
	}
}

func TestReplicaFloorTransport(t *testing.T) {
	for _, tt := range []struct {
		name                            string
		version                         int
		flat, local, demand, omitFloors bool
		zeroFloors, unpaused            bool
		wantErr                         bool
	}{
		{name: "floor envelope", version: 3},
		{name: "floor and demand contracts", version: 3, demand: true},
		{name: "unpaused zero floors", version: 3, zeroFloors: true, unpaused: true},
		{name: "paused zero floors", version: 3, zeroFloors: true, wantErr: true},
		{name: "flat cannot carry floors", flat: true, wantErr: true},
		{name: "execution envelope cannot carry floors", version: 1, wantErr: true},
		{name: "demand envelope cannot carry floors", version: 2, demand: true, wantErr: true},
		{name: "floor envelope requires floors", version: 3, omitFloors: true, wantErr: true},
		{name: "unknown envelope", version: 4, wantErr: true},
		{name: "local ignores floor envelope", version: 3, local: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy := executionPolicy()
			if !tt.omitFloors {
				policy.ReplicaFloors = acceptedFloors()
			}
			if tt.zeroFloors {
				policy.ReplicaFloors[0].Replicas, policy.ReplicaFloors[1].Replicas = 0, 0
			}
			if tt.unpaused {
				policy.PauseSurge = false
			}
			if tt.demand {
				policy.Demand = demandPolicy().Demand
			}
			var body any = executionEnvelope{Version: tt.version, Policy: policy}
			if tt.flat {
				body = policy
			}
			raw, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.PlacementOriginUID: "source-a", constants.PlacementExecution: string(raw)}}, Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}}}
			if tt.local {
				delete(service.Annotations, constants.PlacementOriginUID)
			}
			got, err := FromDerived(service)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("decode: %s: %v", diff, err)
			}
			want := policy
			if tt.wantErr || tt.local {
				want = nil
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
	policy := executionPolicy()
	policy.ReplicaFloors = acceptedFloors()
	raw, err := Encode(policy)
	if err != nil {
		t.Fatal(err)
	}
	var got executionEnvelope
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(executionEnvelope{Version: 3, Policy: policy}, got); diff != "" {
		t.Fatal(diff)
	}
}

func TestCheckComponentReplicaFloor(t *testing.T) {
	for _, tt := range []struct {
		name                    string
		component               v1beta1.ComponentType
		spec                    *v1beta1.ComponentExtensionSpec
		ratio, invalid, wantErr bool
	}{
		{name: "matching engine", component: v1beta1.EngineComponent, spec: &v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(4)}},
		{name: "independent router", component: v1beta1.RouterComponent, spec: &v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1)}},
		{name: "missing spec", component: v1beta1.EngineComponent, wantErr: true},
		{name: "missing floor", component: v1beta1.EngineComponent, spec: &v1beta1.ComponentExtensionSpec{}, wantErr: true},
		{name: "different floor", component: v1beta1.EngineComponent, spec: &v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(5)}, wantErr: true},
		{name: "unbound component", component: "unknown", spec: &v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(4)}, wantErr: true},
		{name: "invalid authority", invalid: true, wantErr: true},
		{name: "decoder under ratio floors", ratio: true, component: v1beta1.DecoderComponent, spec: &v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(2)}},
		{name: "decoder cannot borrow the engine floor", ratio: true, component: v1beta1.DecoderComponent, spec: &v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(4)}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			floors := acceptedFloors()
			if tt.ratio {
				floors[1].Replicas = 2
			}
			if tt.invalid {
				floors = nil
			}
			err := CheckComponentReplicaFloor(floors, tt.component, tt.spec)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("component floor: %s: %v", diff, err)
			}
		})
	}
}

func TestAuthorizeReplicaFloors(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(*v1beta1.PlacementExecutionPolicy)
		wantErr bool
	}{
		{name: "same authority"},
		{name: "changed floor", wantErr: true, edit: func(p *v1beta1.PlacementExecutionPolicy) { p.ReplicaFloors[2].Replicas++ }},
		{name: "removed floor", wantErr: true, edit: func(p *v1beta1.PlacementExecutionPolicy) { p.ReplicaFloors = nil }},
		{name: "new authority changes floor", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Revision++; p.ReplicaFloors[2].Replicas++ }},
		{name: "new authority removes contract", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Revision++; p.ReplicaFloors = nil }},
		{name: "paused zero floor", wantErr: true, edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Revision++; p.ReplicaFloors[2].Replicas = 0 }},
		{name: "released zero floor", edit: func(p *v1beta1.PlacementExecutionPolicy) {
			p.Revision++
			p.PauseSurge = false
			p.ReplicaFloors[2].Replicas = 0
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			current := executionPolicy()
			current.ReplicaFloors = acceptedFloors()
			next := current.DeepCopy()
			if tt.edit != nil {
				tt.edit(next)
			}
			if diff := cmp.Diff(acceptedFloors(), current.ReplicaFloors); diff != "" {
				t.Fatalf("aliased authority: %s", diff)
			}
			err := Authorize(current, next)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("authorize: %s: %v", diff, err)
			}
		})
	}
}

func TestApportionReplicaFloors(t *testing.T) {
	floors := func(engine, decoder int32) []v1beta1.PlacementComponentFloor {
		return []v1beta1.PlacementComponentFloor{{Component: v1beta1.DecoderComponent, Replicas: decoder}, {Component: v1beta1.EngineComponent, Replicas: engine}}
	}
	for _, tt := range []struct {
		name           string
		primary        v1beta1.ComponentType
		primaryMinimum int32
		targets        map[string]int32
		minimums       map[v1beta1.ComponentType]int32
		want           map[string][]v1beta1.PlacementComponentFloor
		wantErr        string
	}{
		{name: "equal shares follow the ratio", primary: v1beta1.EngineComponent, primaryMinimum: 60, targets: map[string]int32{"member-a": 30, "member-b": 30}, minimums: map[v1beta1.ComponentType]int32{v1beta1.DecoderComponent: 40},
			want: map[string][]v1beta1.PlacementComponentFloor{"member-a": floors(30, 20), "member-b": floors(30, 20)}},
		{name: "weighted shares keep exact sums", primary: v1beta1.EngineComponent, primaryMinimum: 60, targets: map[string]int32{"member-a": 40, "member-b": 20}, minimums: map[v1beta1.ComponentType]int32{v1beta1.DecoderComponent: 40},
			want: map[string][]v1beta1.PlacementComponentFloor{"member-a": floors(40, 27), "member-b": floors(20, 13)}},
		{name: "targets above the fleet minimum keep the ratio", primary: v1beta1.EngineComponent, primaryMinimum: 60, targets: map[string]int32{"member-a": 60, "member-b": 30}, minimums: map[v1beta1.ComponentType]int32{v1beta1.DecoderComponent: 40},
			want: map[string][]v1beta1.PlacementComponentFloor{"member-a": floors(60, 40), "member-b": floors(30, 20)}},
		{name: "targets below the fleet minimum keep the ratio", primary: v1beta1.EngineComponent, primaryMinimum: 60, targets: map[string]int32{"member-a": 15, "member-b": 15}, minimums: map[v1beta1.ComponentType]int32{v1beta1.DecoderComponent: 40},
			want: map[string][]v1beta1.PlacementComponentFloor{"member-a": floors(15, 10), "member-b": floors(15, 10)}},
		{name: "paired components follow every target count", primary: v1beta1.EngineComponent, primaryMinimum: 2, targets: map[string]int32{"member-a": 1, "member-b": 1, "member-c": 1}, minimums: map[v1beta1.ComponentType]int32{v1beta1.DecoderComponent: 2},
			want: map[string][]v1beta1.PlacementComponentFloor{"member-a": floors(1, 1), "member-b": floors(1, 1), "member-c": floors(1, 1)}},
		{name: "scaled total rounds up", primary: v1beta1.EngineComponent, primaryMinimum: 60, targets: map[string]int32{"member-a": 61}, minimums: map[v1beta1.ComponentType]int32{v1beta1.DecoderComponent: 40},
			want: map[string][]v1beta1.PlacementComponentFloor{"member-a": floors(61, 41)}},
		{name: "remainder ties break by home name", primary: v1beta1.EngineComponent, primaryMinimum: 3, targets: map[string]int32{"member-a": 1, "member-b": 1, "member-c": 1}, minimums: map[v1beta1.ComponentType]int32{v1beta1.DecoderComponent: 4},
			want: map[string][]v1beta1.PlacementComponentFloor{"member-a": floors(1, 2), "member-b": floors(1, 1), "member-c": floors(1, 1)}},
		{name: "zero minimum places none", primary: v1beta1.EngineComponent, primaryMinimum: 60, targets: map[string]int32{"member-a": 30, "member-b": 30}, minimums: map[v1beta1.ComponentType]int32{v1beta1.DecoderComponent: 0},
			want: map[string][]v1beta1.PlacementComponentFloor{"member-a": floors(30, 0), "member-b": floors(30, 0)}},
		{name: "zero share receives nothing", primary: v1beta1.EngineComponent, primaryMinimum: 60, targets: map[string]int32{"member-a": 60, "member-b": 0}, minimums: map[v1beta1.ComponentType]int32{v1beta1.DecoderComponent: 40},
			want: map[string][]v1beta1.PlacementComponentFloor{"member-a": floors(60, 40), "member-b": floors(0, 0)}},
		{name: "positive share never lacks a declared component", primary: v1beta1.EngineComponent, primaryMinimum: 101, targets: map[string]int32{"member-a": 100, "member-b": 1}, minimums: map[v1beta1.ComponentType]int32{v1beta1.DecoderComponent: 2},
			want: map[string][]v1beta1.PlacementComponentFloor{"member-a": floors(100, 1), "member-b": floors(1, 1)}},
		{name: "minimum below the positive share count holds", primary: v1beta1.EngineComponent, primaryMinimum: 60, targets: map[string]int32{"member-a": 30, "member-b": 30}, minimums: map[v1beta1.ComponentType]int32{v1beta1.DecoderComponent: 1},
			wantErr: "decoder minimum 1 cannot give each of the 2 homes with a positive engine share at least one replica"},
		{name: "primary only", primary: v1beta1.EngineComponent, primaryMinimum: 3, targets: map[string]int32{"member-a": 2, "member-b": 1}, minimums: nil,
			want: map[string][]v1beta1.PlacementComponentFloor{"member-a": {{Component: v1beta1.EngineComponent, Replicas: 2}}, "member-b": {{Component: v1beta1.EngineComponent, Replicas: 1}}}},
		{name: "decoder leads an undeclared engine", primary: v1beta1.DecoderComponent, primaryMinimum: 3, targets: map[string]int32{"member-a": 3}, minimums: nil,
			want: map[string][]v1beta1.PlacementComponentFloor{"member-a": {{Component: v1beta1.DecoderComponent, Replicas: 3}}}},
		{name: "no homes", primary: v1beta1.EngineComponent, primaryMinimum: 60, targets: map[string]int32{}, minimums: map[v1beta1.ComponentType]int32{v1beta1.DecoderComponent: 40}, want: map[string][]v1beta1.PlacementComponentFloor{}},
		{name: "negative target", primary: v1beta1.EngineComponent, primaryMinimum: 1, targets: map[string]int32{"member-a": -1}, wantErr: "nonnegative"},
		{name: "negative minimum", primary: v1beta1.EngineComponent, primaryMinimum: 1, targets: map[string]int32{"member-a": 1}, minimums: map[v1beta1.ComponentType]int32{v1beta1.DecoderComponent: -1}, wantErr: "nonnegative"},
		{name: "primary minimum must be positive", primary: v1beta1.EngineComponent, targets: map[string]int32{"member-a": 1}, wantErr: "positive"},
		{name: "primary cannot be apportioned", primary: v1beta1.EngineComponent, primaryMinimum: 1, targets: map[string]int32{"member-a": 1}, minimums: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 1}, wantErr: "primary"},
		{name: "router keeps its per-home policy", primary: v1beta1.EngineComponent, primaryMinimum: 1, targets: map[string]int32{"member-a": 1}, minimums: map[v1beta1.ComponentType]int32{v1beta1.RouterComponent: 1}, wantErr: "router"},
		{name: "unnamed home", primary: v1beta1.EngineComponent, primaryMinimum: 1, targets: map[string]int32{"": 1}, wantErr: "name"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ApportionReplicaFloors(tt.primary, tt.primaryMinimum, tt.targets, tt.minimums)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
			for name, floors := range got {
				primary, err := ValidateReplicaFloors(floors)
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				if diff := cmp.Diff(tt.targets[name], primary); diff != "" {
					t.Fatalf("%s primary floor (-want +got):\n%s", name, diff)
				}
			}
		})
	}
}

func TestReplicaUnitRatio(t *testing.T) {
	for _, tt := range []struct {
		name     string
		minimums map[v1beta1.ComponentType]int32
		want     map[v1beta1.ComponentType]int64
	}{
		{name: "engine only", minimums: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 60}, want: map[v1beta1.ComponentType]int64{v1beta1.EngineComponent: 1}},
		{name: "equal minimums", minimums: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 4, v1beta1.DecoderComponent: 4}, want: map[v1beta1.ComponentType]int64{v1beta1.EngineComponent: 1, v1beta1.DecoderComponent: 1}},
		{name: "reduced ratio", minimums: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 60, v1beta1.DecoderComponent: 40}, want: map[v1beta1.ComponentType]int64{v1beta1.EngineComponent: 3, v1beta1.DecoderComponent: 2}},
		{name: "zero decoder leaves the unit", minimums: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 60, v1beta1.DecoderComponent: 0}, want: map[v1beta1.ComponentType]int64{v1beta1.EngineComponent: 1, v1beta1.DecoderComponent: 0}},
		{name: "decoder only", minimums: map[v1beta1.ComponentType]int32{v1beta1.DecoderComponent: 7}, want: map[v1beta1.ComponentType]int64{v1beta1.DecoderComponent: 1}},
		{name: "all zero", minimums: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 0, v1beta1.DecoderComponent: 0}, want: map[v1beta1.ComponentType]int64{v1beta1.EngineComponent: 0, v1beta1.DecoderComponent: 0}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, ReplicaUnitRatio(tt.minimums)); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
