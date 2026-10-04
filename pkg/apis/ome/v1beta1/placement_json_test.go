package v1beta1

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestPlacementJSONPreservesExplicitRestrictions(t *testing.T) {
	for _, tt := range []struct {
		name, data, want string
		affinityNil      bool
	}{
		{name: "affinity omitted", data: `{"policy":"ClusterAffinity","mode":"Single"}`, want: `{"policy":"ClusterAffinity","mode":"Single"}`, affinityNil: true},
		{name: "empty affinity", data: `{"policy":"ClusterAffinity","mode":"Single","clusterAffinity":[]}`, want: `{"policy":"ClusterAffinity","mode":"Single","clusterAffinity":[]}`},
		{name: "null affinity", data: `{"policy":"ClusterAffinity","mode":"Single","clusterAffinity":null}`, want: `{"policy":"ClusterAffinity","mode":"Single","clusterAffinity":[]}`},
		{name: "omitted packing", data: `{"policy":"ClusterAffinity","mode":"Split","split":{"replicas":3}}`, want: `{"policy":"ClusterAffinity","mode":"Split","split":{"replicas":3}}`, affinityNil: true},
		{name: "explicit surge zero", data: `{"policy":"ClusterAffinity","mode":"Split","maxSurge":0}`, want: `{"policy":"ClusterAffinity","mode":"Split","maxSurge":0}`, affinityNil: true},
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
				if diff := cmp.Diff(tt.affinityNil, copy.ClusterAffinity == nil); diff != "" {
					t.Fatal(diff)
				}
			}
		})
	}
}
