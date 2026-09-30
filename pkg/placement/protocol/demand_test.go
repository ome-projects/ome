package protocol

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func demandPolicy() *v1beta1.PlacementExecutionPolicy {
	p := executionPolicy()
	p.Demand = &v1beta1.PlacementDemandContract{Fingerprint: strings.Repeat("a", 64), Components: []v1beta1.PlacementComponentDemand{
		{Component: v1beta1.EngineComponent, RenderingHash: strings.Repeat("b", 64)},
	}}
	return p
}

func TestDemandTransport(t *testing.T) {
	for _, tt := range []struct {
		name                    string
		version                 int
		flat, omitDemand, local bool
		wantErr                 bool
	}{
		{name: "demand envelope", version: 2},
		{name: "execution envelope", version: 1, omitDemand: true},
		{name: "flat execution", flat: true, omitDemand: true},
		{name: "flat cannot carry demand", flat: true, wantErr: true},
		{name: "execution version cannot carry demand", version: 1, wantErr: true},
		{name: "demand version requires demand", version: 2, omitDemand: true, wantErr: true},
		{name: "unknown version", version: 4, wantErr: true},
		{name: "local ignores demand envelope", version: 2, local: true},
		{name: "local ignores unsupported version", version: 4, local: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy := demandPolicy()
			if tt.omitDemand {
				policy.Demand = nil
			}
			var body any = executionEnvelope{Version: tt.version, Policy: policy}
			if tt.flat {
				body = policy
			}
			encoded, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
				Labels:      map[string]string{constants.PlacementOrigin: "source-a"},
				Annotations: map[string]string{constants.PlacementExecution: string(encoded)},
			}, Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}}}
			if tt.local {
				service.Labels = nil
			}
			before := service.DeepCopy()
			got, err := FromDerived(service)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("error (-want +got):\n%s\n%v", diff, err)
			}
			want := policy
			if tt.wantErr || tt.local {
				want = nil
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("authority (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(before, service); diff != "" {
				t.Fatalf("service mutated:\n%s", diff)
			}
		})
	}
	policy := demandPolicy()
	raw, err := Encode(policy)
	if err != nil {
		t.Fatal(err)
	}
	var envelope executionEnvelope
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(executionEnvelope{Version: 2, Policy: policy}, envelope); diff != "" {
		t.Fatal(diff)
	}
}

func TestDemandValidation(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*v1beta1.PlacementDemandContract)
	}{
		{"missing fingerprint", func(d *v1beta1.PlacementDemandContract) { d.Fingerprint = "" }},
		{"short fingerprint", func(d *v1beta1.PlacementDemandContract) { d.Fingerprint = "ab" }},
		{"invalid fingerprint", func(d *v1beta1.PlacementDemandContract) { d.Fingerprint = strings.Repeat("z", 64) }},
		{"uppercase fingerprint", func(d *v1beta1.PlacementDemandContract) { d.Fingerprint = strings.Repeat("A", 64) }},
		{"missing components", func(d *v1beta1.PlacementDemandContract) { d.Components = nil }},
		{"duplicate component", func(d *v1beta1.PlacementDemandContract) { d.Components = append(d.Components, d.Components[0]) }},
		{"router is outside replica demand", func(d *v1beta1.PlacementDemandContract) { d.Components[0].Component = v1beta1.RouterComponent }},
		{"missing component", func(d *v1beta1.PlacementDemandContract) { d.Components[0].Component = "" }},
		{"invalid rendering", func(d *v1beta1.PlacementDemandContract) { d.Components[0].RenderingHash = "invalid" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := demandPolicy()
			tt.edit(p.Demand)
			if _, err := Encode(p); err == nil {
				t.Fatal("invalid demand encoded")
			}
		})
	}
}

func TestDemandComponentInventory(t *testing.T) {
	for _, tt := range []struct {
		name                    string
		engine, decoder, router bool
		components              []v1beta1.ComponentType
		wantErr                 bool
	}{
		{name: "engine", engine: true, components: []v1beta1.ComponentType{v1beta1.EngineComponent}},
		{name: "decoder", decoder: true, components: []v1beta1.ComponentType{v1beta1.DecoderComponent}},
		{name: "both", engine: true, decoder: true, components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}},
		{name: "router excluded", engine: true, router: true, components: []v1beta1.ComponentType{v1beta1.EngineComponent}},
		{name: "missing decoder", engine: true, decoder: true, components: []v1beta1.ComponentType{v1beta1.EngineComponent}, wantErr: true},
		{name: "undeclared engine", decoder: true, components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := demandPolicy()
			p.Demand.Components = nil
			for _, c := range tt.components {
				p.Demand.Components = append(p.Demand.Components, v1beta1.PlacementComponentDemand{Component: c, RenderingHash: strings.Repeat("c", 64)})
			}
			raw, err := Encode(p)
			if err != nil {
				t.Fatal(err)
			}
			s := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{constants.PlacementOrigin: "source-a"}, Annotations: map[string]string{constants.PlacementExecution: raw}}}
			if tt.engine {
				s.Spec.Engine = &v1beta1.EngineSpec{}
			}
			if tt.decoder {
				s.Spec.Decoder = &v1beta1.DecoderSpec{}
			}
			if tt.router {
				s.Spec.Router = &v1beta1.RouterSpec{}
			}
			_, err = FromDerived(s)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("inventory error:\n%s\n%v", diff, err)
			}
		})
	}
}

func TestDemandInventoryRequiresCompleteInput(t *testing.T) {
	for _, tt := range []struct {
		name                string
		noService, noDemand bool
	}{
		{name: "complete input"},
		{name: "missing service", noService: true},
		{name: "missing demand", noDemand: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}}}
			demand := demandPolicy().Demand
			if tt.noService {
				s = nil
			}
			if tt.noDemand {
				demand = nil
			}
			err := ValidateDemandComponents(s, demand)
			if diff := cmp.Diff(tt.noService || tt.noDemand, err != nil); diff != "" {
				t.Fatalf("error: %s: %v", diff, err)
			}
		})
	}
}

func TestAuthorizeDemand(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(*v1beta1.PlacementExecutionPolicy)
		wantErr bool
	}{
		{name: "equal deep copy"},
		{name: "changed fingerprint", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Demand.Fingerprint = strings.Repeat("c", 64) }, wantErr: true},
		{name: "changed rendering", edit: func(p *v1beta1.PlacementExecutionPolicy) {
			p.Demand.Components[0].RenderingHash = strings.Repeat("c", 64)
		}, wantErr: true},
		{name: "removed demand", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Demand = nil }, wantErr: true},
		{name: "new authority can replace demand", edit: func(p *v1beta1.PlacementExecutionPolicy) {
			p.Revision++
			p.Demand.Fingerprint = strings.Repeat("c", 64)
		}},
		{name: "new authority can remove demand", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Revision++; p.Demand = nil }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			current := demandPolicy()
			next := current.DeepCopy()
			if tt.edit != nil {
				tt.edit(next)
			}
			if diff := cmp.Diff(demandPolicy(), current); diff != "" {
				t.Fatalf("aliased contract:\n%s", diff)
			}
			err := Authorize(current, next)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("authority error:\n%s\n%v", diff, err)
			}
		})
	}
}
