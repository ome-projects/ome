package v1beta1

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestPlacementJSONPreservesExplicitRestrictions(t *testing.T) {
	for _, tt := range []struct {
		name, data, want    string
		legacy, legacySplit bool
		affinityNil         bool
	}{
		{name: "affinity omitted", data: `{"mode":"Single"}`, want: `{"mode":"Single"}`, affinityNil: true},
		{name: "empty affinity", data: `{"mode":"Single","clusterAffinity":[]}`, want: `{"mode":"Single","clusterAffinity":[]}`},
		{name: "null affinity", data: `{"mode":"Single","clusterAffinity":null}`, want: `{"mode":"Single","clusterAffinity":[]}`},
		{name: "empty requirements", data: `{"mode":"Single","requirements":""}`, want: `{"mode":"Single","requirements":""}`, legacy: true, affinityNil: true},
		{name: "null selector", data: `{"mode":"Single","clusterSelector":null}`, want: `{"mode":"Single","clusterSelector":""}`, legacy: true, affinityNil: true},
		{name: "empty capacity map", data: `{"mode":"Single","capacityFactors":{}}`, want: `{"mode":"Single","capacityFactors":{}}`, legacy: true, affinityNil: true},
		{name: "null capacity map", data: `{"mode":"Single","capacityFactors":null}`, want: `{"mode":"Single","capacityFactors":null}`, legacy: true, affinityNil: true},
		{name: "explicit packed", data: `{"mode":"Split","split":{"spread":false}}`, want: `{"mode":"Split","split":{"spread":false}}`, legacySplit: true, affinityNil: true},
		{name: "null packed", data: `{"mode":"Split","split":{"spread":null}}`, want: `{"mode":"Split","split":{"spread":false}}`, legacySplit: true, affinityNil: true},
		{name: "zero anti-sliver", data: `{"mode":"Split","split":{"minReplicasPerCluster":0}}`, want: `{"mode":"Split","split":{"minReplicasPerCluster":0}}`, legacySplit: true, affinityNil: true},
		{name: "omitted packing", data: `{"mode":"Split","split":{"replicas":3}}`, want: `{"mode":"Split","split":{"replicas":3}}`, affinityNil: true},
		{name: "explicit surge zero", data: `{"mode":"Split","maxSurge":0}`, want: `{"mode":"Split","maxSurge":0}`, affinityNil: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var placement PlacementSpec
			if err := json.Unmarshal([]byte(tt.data), &placement); err != nil {
				t.Fatal(err)
			}
			for _, copy := range []*PlacementSpec{&placement, placement.DeepCopy()} {
				got, err := json.Marshal(copy)
				if err != nil {
					t.Fatal(err)
				}
				var gotObject, wantObject map[string]any
				if err := json.Unmarshal(got, &gotObject); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(tt.want), &wantObject); err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff(wantObject, gotObject); diff != "" {
					t.Fatal(diff)
				}
				if diff := cmp.Diff(tt.legacy, copy.HasLegacyFields()); diff != "" {
					t.Fatal(diff)
				}
				if diff := cmp.Diff(tt.legacySplit, copy.Split.HasLegacyFields()); diff != "" {
					t.Fatal(diff)
				}
				if diff := cmp.Diff(tt.affinityNil, copy.ClusterAffinity == nil); diff != "" {
					t.Fatal(diff)
				}
			}
		})
	}
}
