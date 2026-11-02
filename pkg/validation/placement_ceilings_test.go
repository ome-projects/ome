package validation

import (
	"encoding/json"
	"strings"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestValidatePlacementIntent_SplitMaxPerCluster(t *testing.T) {
	// service places the components with Split and the given split block;
	// mode, when given, replaces the mode and adds any clusterAffinity.
	service := func(components, split string, mode ...string) string {
		selection := `"mode":"Split"`
		if len(mode) > 0 {
			selection = mode[0]
		}
		return `{"spec":{` + components + `,"placement":{"policy":"ClusterAffinity",` + selection + `,"split":` + split + `}}}`
	}
	pdr := `"engine":{},"decoder":{},"router":{"minReplicas":2}`
	engineOnly := `"mode":"Split","clusterAffinity":[{"weights":{"engine":1},"matchFields":[{"key":"metadata.name","operator":"In","values":["cluster-a"]}]}]`
	for _, tt := range []struct {
		name, payload, wantErr string
	}{
		{name: "a cap for every component is valid", payload: service(pdr, `{"maxPerCluster":{"default":60,"router":8}}`)},
		{name: "an engine cap alone is valid", payload: service(pdr, `{"maxPerCluster":{"engine":60}}`)},
		{
			name:    "a decoder cap without a decoder is rejected",
			payload: service(`"engine":{}`, `{"maxPerCluster":{"decoder":4}}`),
			wantErr: "spec.placement.split.maxPerCluster.decoder names a component the service does not declare",
		},
		{
			name:    "a router cap without a router is rejected",
			payload: service(`"engine":{},"decoder":{}`, `{"maxPerCluster":{"router":4}}`),
			wantErr: "spec.placement.split.maxPerCluster.router names a component the service does not declare",
		},
		{
			name:    "a router cap is rejected when weights keep the full router policy",
			payload: service(pdr, `{"maxPerCluster":{"router":4}}`, engineOnly),
			wantErr: "spec.placement.split.maxPerCluster.router applies only when Split divides the routers",
		},
		{
			name:    "a router cap is rejected in SplitByCapacity, which never divides the routers",
			payload: service(pdr, `{"maxPerCluster":{"router":4}}`, `"mode":"SplitByCapacity"`),
			wantErr: "spec.placement.split.maxPerCluster.router applies only when Split divides the routers",
		},
		{name: "default is valid when the routers are not divided", payload: service(pdr, `{"maxPerCluster":{"default":4}}`, engineOnly)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := &v1beta1.InferenceService{}
			if err := json.Unmarshal([]byte(tt.payload), source); err != nil {
				t.Fatalf("%v: %s", err, tt.payload)
			}
			err := ValidatePlacementIntent(source)
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("error %v does not contain %q", err, tt.wantErr)
			}
		})
	}
}
