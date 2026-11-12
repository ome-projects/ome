package validation

import (
	"encoding/json"
	"strings"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestValidatePlacementIntent_ComponentWeights_OnlyDeclaredComponentsInSplit(t *testing.T) {
	term := func(weights string) string {
		return `[{` + weights + `"matchFields":[{"key":"metadata.name","operator":"In","values":["cluster-a"]}]}]`
	}
	service := func(components, placement string) string {
		return `{"spec":{` + components + `,"placement":{` + placement + `}}}`
	}
	pd := `"engine":{},"decoder":{}`
	for _, tt := range []struct {
		name, payload, wantErr string
	}{
		{
			name:    "Split engine and decoder weights are valid",
			payload: service(pd, `"policy":"ClusterAffinity","mode":"Split","clusterAffinity":`+term(`"weights":{"engine":{"min":2},"decoder":{"min":1}},`)),
		},
		{
			name:    "a router weight needs a router minimum",
			payload: service(pd+`,"router":{}`, `"policy":"ClusterAffinity","mode":"Split","clusterAffinity":`+term(`"weights":{"router":{"min":1}},`)),
			wantErr: "set a positive router.minReplicas",
		},
		{
			name:    "a router weight with a minimum divides the routers",
			payload: service(pd+`,"router":{"minReplicas":2}`, `"policy":"ClusterAffinity","mode":"Split","clusterAffinity":`+term(`"weights":{"router":{"min":1}},`)),
		},
		{
			name:    "a default weight needs a router minimum",
			payload: service(pd+`,"router":{}`, `"policy":"ClusterAffinity","mode":"Split","clusterAffinity":`+term(`"weights":{"default":{"min":2}},`)),
			wantErr: "set a positive router.minReplicas",
		},
		{
			name:    "a default weight on a service without a router is valid",
			payload: service(pd, `"policy":"ClusterAffinity","mode":"Split","clusterAffinity":`+term(`"weights":{"default":{"min":2}},`)),
		},
		{
			name:    "an engine weight does not need a router minimum",
			payload: service(pd+`,"router":{}`, `"policy":"ClusterAffinity","mode":"Split","clusterAffinity":`+term(`"weights":{"engine":{"min":2}},`)),
		},
		{
			name:    "a Split source without weights divides its routers, so it needs a router minimum",
			payload: service(pd+`,"router":{}`, `"policy":"ClusterAffinity","mode":"Split","clusterAffinity":`+term(``)),
			wantErr: "set a positive router.minReplicas",
		},
		{
			name:    "a Split source without affinity divides its routers, so it needs a router minimum",
			payload: service(pd+`,"router":{}`, `"policy":"ClusterAffinity","mode":"Split"`),
			wantErr: "set a positive router.minReplicas",
		},
		{
			name:    "a Split source without weights and with a router minimum is valid",
			payload: service(pd+`,"router":{"minReplicas":2}`, `"policy":"ClusterAffinity","mode":"Split","clusterAffinity":`+term(``)),
		},
		{
			name:    "All needs no router minimum",
			payload: service(pd+`,"router":{}`, `"policy":"ClusterAffinity","mode":"All","clusterAffinity":`+term(``)),
		},
		{
			name:    "Single needs no router minimum",
			payload: service(pd+`,"router":{}`, `"policy":"ClusterAffinity","mode":"Single","clusterAffinity":`+term(``)),
		},
		{
			name:    "SplitByCapacity needs no router minimum",
			payload: service(pd+`,"router":{}`, `"policy":"ClusterAffinity","mode":"SplitByCapacity","clusterAffinity":`+term(``)),
		},
		{
			name:    "a router weight without a router is rejected",
			payload: service(pd, `"policy":"ClusterAffinity","mode":"Split","clusterAffinity":`+term(`"weights":{"router":{"min":1}},`)),
			wantErr: "clusterAffinity[0].weights.router names a component the service does not declare",
		},
		{
			name:    "an engine weight on a decoder-only service is rejected",
			payload: service(`"decoder":{}`, `"policy":"ClusterAffinity","mode":"Split","clusterAffinity":`+term(`"weights":{"engine":{"min":2}},`)),
			wantErr: "clusterAffinity[0].weights.engine names a component the service does not declare",
		},
		{
			name:    "weights are accepted only in static Split",
			payload: service(pd, `"policy":"ClusterAffinity","mode":"SplitByCapacity","clusterAffinity":`+term(`"weights":{"decoder":{"min":1}},`)),
			wantErr: "clusterAffinity weights are allowed only in Split",
		},
		{
			name:    "the per-component policy is not recognized",
			payload: service(pd, `"policy":"ComponentAffinity","mode":"Split"`),
			wantErr: "spec.placement.policy must be ClusterAffinity; the ComponentAffinity policy was removed",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := &v1beta1.InferenceService{}
			if err := json.Unmarshal([]byte(tt.payload), source); err != nil {
				t.Fatal(err)
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
