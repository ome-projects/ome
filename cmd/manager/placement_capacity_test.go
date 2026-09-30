package main

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement"
)

func TestCapacityPlacementWiring(t *testing.T) {
	for _, tt := range []struct {
		name, config  string
		want          *placement.CapacityConfig
		wantNamespace string
	}{
		{name: "absent"},
		{name: "runtime resolution only", config: `{"placement":{"memberOperatorNamespace":"operator-system"}}`, wantNamespace: "operator-system"},
		{name: "explicit", config: `{"placement":{"memberOperatorNamespace":"operator-system","capacity":{"rootName":"capacity-root","maxAge":"5m","stabilityWindow":"1m","refreshInterval":"10s"}}}`, want: &placement.CapacityConfig{RootName: "capacity-root", MaxAge: 5 * time.Minute, StabilityWindow: time.Minute, RefreshInterval: 10 * time.Second}, wantNamespace: "operator-system"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := mcWiringFromJSON(t, tt.config)
			if diff := cmp.Diff(tt.wantNamespace, got.memberOperatorNamespace); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(tt.want, got.capacity); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
