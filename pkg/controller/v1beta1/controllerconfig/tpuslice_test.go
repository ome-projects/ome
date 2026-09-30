package controllerconfig

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/tpuslice"
)

// tpuSliceBlock renders a valid block, after mutate edits its decoded form.
func tpuSliceBlock(t *testing.T, mutate func(block map[string]any)) string {
	t.Helper()
	block := map[string]any{
		"chipResource": "example.com/tpu",
		"nodeLabels": map[string]any{
			"accelerator": "example.com/accelerator",
			"topology":    "example.com/topology",
			"slice":       "example.com/slice",
		},
		"provisionOnly": map[string]any{"key": "example.com/mode", "value": "on-demand"},
		"accelerators": map[string]any{
			"acc-a": map[string]any{"sliceType": "type-a", "chipsPerHost": 4, "topologies": []any{"2x2x1", "2x2x2"}},
		},
		"slice": map[string]any{
			"ownerKindLabel": "example.com/owner-kind",
			"ownerNameLabel": "example.com/owner-name",
			"annotations":    map[string]any{"example.com/managed-by": "scheduler"},
			"readyStates":    []any{"ACTIVE", "ACTIVE_DEGRADED"},
		},
	}
	if mutate != nil {
		mutate(block)
	}
	raw, err := json.Marshal(block)
	if err != nil {
		t.Fatalf("marshal block: %v", err)
	}
	return string(raw)
}

func field(block map[string]any, path ...string) map[string]any {
	for _, p := range path {
		block = block[p].(map[string]any)
	}
	return block
}

func validTPUSliceConfig() *TPUSliceProvisioningConfig {
	return &TPUSliceProvisioningConfig{
		ChipResource: "example.com/tpu",
		NodeLabels: TPUSliceNodeLabels{
			Accelerator: "example.com/accelerator",
			Topology:    "example.com/topology",
			Slice:       "example.com/slice",
		},
		ProvisionOnly: TPUSliceLabel{Key: "example.com/mode", Value: "on-demand"},
		Accelerators: map[string]TPUSliceAccelerator{
			"acc-a": {SliceType: "type-a", ChipsPerHost: 4, Topologies: []string{"2x2x1", "2x2x2"}},
		},
		Slice: TPUSliceObject{
			OwnerKindLabel: "example.com/owner-kind",
			OwnerNameLabel: "example.com/owner-name",
			Annotations:    map[string]string{"example.com/managed-by": "scheduler"},
			ReadyStates:    []string{"ACTIVE", "ACTIVE_DEGRADED"},
		},
	}
}

func TestParseTPUSliceProvisioningConfig(t *testing.T) {
	noAnnotations := validTPUSliceConfig()
	noAnnotations.Slice.Annotations = map[string]string{}

	tests := []struct {
		name string
		// raw is the block; absent from the ConfigMap when nil.
		raw  *string
		want *TPUSliceProvisioningConfig
		// wantErr lists substrings the error must contain.
		wantErr []string
	}{
		{
			name: "absent block disables provisioning",
		},
		{
			name: "blank block disables provisioning",
			raw:  ptr.To(" \n"),
		},
		{
			name: "valid block",
			raw:  ptr.To(tpuSliceBlock(t, nil)),
			want: validTPUSliceConfig(),
		},
		{
			name: "explicitly empty annotations",
			raw: ptr.To(tpuSliceBlock(t, func(b map[string]any) {
				field(b, "slice")["annotations"] = map[string]any{}
			})),
			want: noAnnotations,
		},
		{
			name:    "empty object names every required field",
			raw:     ptr.To(`{}`),
			wantErr: []string{"chipResource: required", "nodeLabels.accelerator: required", "nodeLabels.topology: required", "nodeLabels.slice: required", "provisionOnly.key: required", "provisionOnly.value: required", "accelerators: at least one", "slice.ownerKindLabel: required", "slice.ownerNameLabel: required", "slice.annotations: required", "slice.readyStates: at least one"},
		},
		{
			name:    "null",
			raw:     ptr.To(`null`),
			wantErr: []string{"expected a JSON object"},
		},
		{
			name:    "array",
			raw:     ptr.To(`[]`),
			wantErr: []string{"expected a JSON object"},
		},
		{
			name:    "truncated object",
			raw:     ptr.To(`{"chipResource":`),
			wantErr: []string{"malformed JSON object"},
		},
		{
			name:    "trailing data",
			raw:     ptr.To(tpuSliceBlock(t, nil) + ` {}`),
			wantErr: []string{"malformed JSON object", "after top-level value"},
		},
		{
			name:    "wrong type",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { field(b, "accelerators", "acc-a")["chipsPerHost"] = "4" })),
			wantErr: []string{"malformed JSON object", "chipsPerHost"},
		},
		{
			name:    "unknown field",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { b["chipResources"] = "example.com/tpu" })),
			wantErr: []string{`unknown field "chipResources"`},
		},
		{
			name:    "unknown nested field",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { field(b, "nodeLabels")["zone"] = "example.com/zone" })),
			wantErr: []string{`unknown field "nodeLabels.zone"`},
		},
		{
			name:    "field names are case sensitive",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { b["ChipResource"] = b["chipResource"]; delete(b, "chipResource") })),
			wantErr: []string{`unknown field "ChipResource"`},
		},
		{
			name:    "duplicate field",
			raw:     ptr.To(strings.Replace(tpuSliceBlock(t, nil), `{`, `{"chipResource":"example.com/tpu",`, 1)),
			wantErr: []string{`duplicate field "chipResource"`},
		},
		{
			name:    "duplicate accelerator",
			raw:     ptr.To(strings.Replace(tpuSliceBlock(t, nil), `"accelerators":{`, `"accelerators":{"acc-a":{},`, 1)),
			wantErr: []string{`duplicate field "accelerators.acc-a"`},
		},
		{
			name:    "chip resource is not a qualified name",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { b["chipResource"] = "example.com/tpu/v1" })),
			wantErr: []string{`chipResource: "example.com/tpu/v1" is not a qualified name`},
		},
		{
			name:    "node label key is not a qualified name",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { field(b, "nodeLabels")["slice"] = "example.com/slice name" })),
			wantErr: []string{`nodeLabels.slice: "example.com/slice name" is not a qualified name`},
		},
		{
			name:    "node label keys repeat",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { field(b, "nodeLabels")["topology"] = "example.com/accelerator" })),
			wantErr: []string{`nodeLabels.topology: "example.com/accelerator" is already used by nodeLabels.accelerator`},
		},
		{
			name:    "provision-only key repeats a node label key",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { field(b, "provisionOnly")["key"] = "example.com/slice" })),
			wantErr: []string{`provisionOnly.key: "example.com/slice" is already used by nodeLabels.slice`},
		},
		{
			name:    "provision-only value is not a label value",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { field(b, "provisionOnly")["value"] = "on demand" })),
			wantErr: []string{`provisionOnly.value: "on demand" is not a label value`},
		},
		{
			name:    "no accelerators",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { b["accelerators"] = map[string]any{} })),
			wantErr: []string{"accelerators: at least one accelerator is required"},
		},
		{
			name: "accelerator name is not a label value",
			raw: ptr.To(tpuSliceBlock(t, func(b map[string]any) {
				field(b, "accelerators")["acc a"] = field(b, "accelerators", "acc-a")
			})),
			wantErr: []string{`accelerators[acc a]: "acc a" is not a label value`},
		},
		{
			name:    "accelerator without slice type",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { delete(field(b, "accelerators", "acc-a"), "sliceType") })),
			wantErr: []string{"accelerators[acc-a].sliceType: required"},
		},
		{
			name:    "accelerator without chips per host",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { field(b, "accelerators", "acc-a")["chipsPerHost"] = 0 })),
			wantErr: []string{"accelerators[acc-a].chipsPerHost: must be positive, got 0"},
		},
		{
			name:    "accelerator without topologies",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { field(b, "accelerators", "acc-a")["topologies"] = []any{} })),
			wantErr: []string{"accelerators[acc-a].topologies: at least one topology is required"},
		},
		{
			name:    "non-canonical topology",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { field(b, "accelerators", "acc-a")["topologies"] = []any{"2x2x1", "02x2x1"} })),
			wantErr: []string{`accelerators[acc-a].topologies[1]: topology "02x2x1"`},
		},
		{
			name:    "topology that is not whole hosts",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { field(b, "accelerators", "acc-a")["topologies"] = []any{"1x2x1"} })),
			wantErr: []string{"accelerators[acc-a].topologies[0]: topology 1x2x1 is 2 chips, not a whole number of 4-chip hosts"},
		},
		{
			name:    "topology listed twice",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { field(b, "accelerators", "acc-a")["topologies"] = []any{"2x2x1", "2x2x1"} })),
			wantErr: []string{`accelerators[acc-a].topologies[1]: "2x2x1" is listed twice`},
		},
		{
			name: "errors are ordered by accelerator",
			raw: ptr.To(tpuSliceBlock(t, func(b map[string]any) {
				field(b, "accelerators")["acc-c"] = map[string]any{"sliceType": "type-c", "chipsPerHost": -1, "topologies": []any{"2x2x1"}}
				field(b, "accelerators")["acc-b"] = map[string]any{"sliceType": "", "chipsPerHost": 4, "topologies": []any{"2x2x1"}}
			})),
			wantErr: []string{"accelerators[acc-b].sliceType: required\naccelerators[acc-c].chipsPerHost: must be positive, got -1"},
		},
		{
			name:    "owner kind label is not a qualified name",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { field(b, "slice")["ownerKindLabel"] = "-owner-kind" })),
			wantErr: []string{`slice.ownerKindLabel: "-owner-kind" is not a qualified name`},
		},
		{
			name:    "owner labels repeat",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { field(b, "slice")["ownerNameLabel"] = "example.com/owner-kind" })),
			wantErr: []string{`slice.ownerNameLabel: "example.com/owner-kind" is already used by slice.ownerKindLabel`},
		},
		{
			name:    "annotations omitted",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { delete(field(b, "slice"), "annotations") })),
			wantErr: []string{"slice.annotations: required; use {} for none"},
		},
		{
			name:    "annotation key is not a qualified name",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { field(b, "slice", "annotations")["managed by"] = "scheduler" })),
			wantErr: []string{`slice.annotations: "managed by" is not a qualified name`},
		},
		{
			name:    "no ready states",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { field(b, "slice")["readyStates"] = []any{} })),
			wantErr: []string{"slice.readyStates: at least one state is required"},
		},
		{
			name:    "empty ready state",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { field(b, "slice")["readyStates"] = []any{"ACTIVE", ""} })),
			wantErr: []string{"slice.readyStates[1]: must not be empty"},
		},
		{
			name:    "ready state listed twice",
			raw:     ptr.To(tpuSliceBlock(t, func(b map[string]any) { field(b, "slice")["readyStates"] = []any{"ACTIVE", "ACTIVE"} })),
			wantErr: []string{`slice.readyStates[1]: "ACTIVE" is listed twice`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := map[string]string{}
			if tt.raw != nil {
				data[TPUSliceProvisioningConfigName] = *tt.raw
			}
			got, err := ParseTPUSliceProvisioningConfig(&v1.ConfigMap{Data: data})
			if len(tt.wantErr) > 0 {
				if err == nil {
					t.Fatalf("expected an error, got config %+v", got)
				}
				for _, want := range tt.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q does not contain %q", err.Error(), want)
					}
				}
				if got != nil {
					t.Errorf("config %+v returned alongside an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("config mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNewTPUSliceProvisioningConfigReadsTheConfigMap(t *testing.T) {
	configMap := func(data map[string]string) *v1.ConfigMap {
		return &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace},
			Data:       data,
		}
	}

	cfg, err := NewTPUSliceProvisioningConfig(fake.NewSimpleClientset(configMap(map[string]string{
		TPUSliceProvisioningConfigName: tpuSliceBlock(t, nil),
	})))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if diff := cmp.Diff(validTPUSliceConfig(), cfg); diff != "" {
		t.Fatalf("config mismatch (-want +got):\n%s", diff)
	}

	cfg, err = NewTPUSliceProvisioningConfig(fake.NewSimpleClientset(configMap(map[string]string{})))
	if err != nil || cfg != nil {
		t.Fatalf("absent block = (%+v, %v), want (nil, nil)", cfg, err)
	}

	if _, err := NewTPUSliceProvisioningConfig(fake.NewSimpleClientset(configMap(map[string]string{
		TPUSliceProvisioningConfigName: `{}`,
	}))); err == nil {
		t.Fatal("an invalid block must be an error")
	}

	if _, err := NewTPUSliceProvisioningConfig(fake.NewSimpleClientset()); err == nil {
		t.Fatal("missing ConfigMap must be an error")
	}
}

func TestTPUSliceAcceleratorAllows(t *testing.T) {
	acc := validTPUSliceConfig().Accelerators["acc-a"]
	for _, tc := range []struct {
		topology string
		want     bool
	}{
		{"2x2x1", true},
		{"2x2x2", true},
		{"2x2", false},
		{"2x4x1", false},
	} {
		topology, err := tpuslice.ParseTopology(tc.topology)
		if err != nil {
			t.Fatalf("parse %q: %v", tc.topology, err)
		}
		if got := acc.Allows(topology); got != tc.want {
			t.Errorf("Allows(%s) = %v, want %v", tc.topology, got, tc.want)
		}
	}
	if acc.Allows(tpuslice.Topology{}) {
		t.Error("the absent topology must not be allowed")
	}
}

func TestTPUSliceProvisioningConfigShapeKeys(t *testing.T) {
	got := validTPUSliceConfig().ShapeKeys()
	want := tpuslice.Keys{Accelerator: "example.com/accelerator", Topology: "example.com/topology"}
	if got != want {
		t.Fatalf("ShapeKeys() = %+v, want %+v", got, want)
	}
}
