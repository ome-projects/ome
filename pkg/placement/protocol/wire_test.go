package protocol

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestExecutionWireCompatibility(t *testing.T) {
	policy := executionPolicy()
	flat, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{name: "flat policy", raw: string(flat)},
		{name: "versioned policy", raw: `{"version":1,"policy":` + string(flat) + `}`},
		{name: "future version", raw: `{"version":4,"policy":` + string(flat) + `}`, wantErr: true},
		{name: "missing version", raw: `{"policy":` + string(flat) + `}`, wantErr: true},
		{name: "null version", raw: `{"version":null,"policy":` + string(flat) + `}`, wantErr: true},
		{name: "wrong version type", raw: `{"version":"1","policy":` + string(flat) + `}`, wantErr: true},
		{name: "missing policy", raw: `{"version":1}`, wantErr: true},
		{name: "null policy", raw: `{"version":1,"policy":null}`, wantErr: true},
		{name: "invalid nested policy", raw: `{"version":1,"policy":{}}`, wantErr: true},
		{name: "unknown envelope field", raw: `{"version":1,"policy":` + string(flat) + `,"extra":true}`, wantErr: true},
		{name: "unknown nested field", raw: `{"version":1,"policy":{"unexpected":true}}`, wantErr: true},
		{name: "unknown flat field", raw: string(flat[:len(flat)-1]) + `,"extra":true}`, wantErr: true},
		{name: "mixed envelope and flat authority", raw: `{"version":1,"policy":` + string(flat) + `,"planID":"other"}`, wantErr: true},
		{name: "multiple JSON values", raw: string(flat) + `{}`, wantErr: true},
		{name: "array", raw: `[]`, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
				constants.PlacementOriginUID: string(policy.SourceUID), constants.PlacementExecution: tt.raw,
			}}}
			got, err := FromDerived(service)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("decode error (-want +got):\n%s\nerror: %v", diff, err)
			}
			var want *v1beta1.PlacementExecutionPolicy
			if !tt.wantErr {
				want = policy
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("policy (-want +got):\n%s", diff)
			}
		})
	}
}

func TestEncodedPolicyCannotBeReadAsFlatAuthority(t *testing.T) {
	policy := executionPolicy()
	raw, err := Encode(policy)
	if err != nil {
		t.Fatal(err)
	}
	var encoded map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &encoded); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := json.Unmarshal(encoded["version"], &version); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(1, version); diff != "" {
		t.Fatalf("wire version (-want +got):\n%s", diff)
	}
	var legacy v1beta1.PlacementExecutionPolicy
	if err := json.Unmarshal([]byte(raw), &legacy); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(v1beta1.PlacementExecutionPolicy{}, legacy); diff != "" {
		t.Fatalf("flat reader must receive no authority (-want +got):\n%s", diff)
	}
	if err := Validate(&legacy); err == nil {
		t.Fatal("flat reader accepted an unsupported envelope")
	}
}
