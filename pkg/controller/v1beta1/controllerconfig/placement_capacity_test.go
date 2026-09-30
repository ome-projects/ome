package controllerconfig

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestPlacementCapacityConfig(t *testing.T) {
	for _, tt := range []struct {
		name          string
		block         string
		wantErr       bool
		omitNamespace bool
	}{
		{name: "absent"},
		{name: "null", block: "null"},
		{name: "complete", block: `{"rootName":"capacity-root","maxAge":"5m","stabilityWindow":"1m","refreshInterval":"10s"}`},
		{name: "partial", block: `{"rootName":"capacity-root"}`, wantErr: true},
		{name: "missing root", block: `{"maxAge":"5m","stabilityWindow":"1m","refreshInterval":"10s"}`, wantErr: true},
		{name: "missing namespace", omitNamespace: true, block: `{"rootName":"capacity-root","maxAge":"5m","stabilityWindow":"1m","refreshInterval":"10s"}`, wantErr: true},
		{name: "nested namespace does not authorize capacity", omitNamespace: true, block: `{"rootName":"capacity-root","memberOperatorNamespace":"operator-system","maxAge":"5m","stabilityWindow":"1m","refreshInterval":"10s"}`, wantErr: true},
		{name: "zero stability", block: `{"rootName":"capacity-root","maxAge":"5m","stabilityWindow":"0s","refreshInterval":"10s"}`, wantErr: true},
		{name: "negative age", block: `{"rootName":"capacity-root","maxAge":"-5m","stabilityWindow":"1m","refreshInterval":"10s"}`, wantErr: true},
		{name: "malformed refresh", block: `{"rootName":"capacity-root","maxAge":"5m","stabilityWindow":"1m","refreshInterval":"10"}`, wantErr: true},
		{name: "refresh exceeds age", block: `{"rootName":"capacity-root","maxAge":"5m","stabilityWindow":"1m","refreshInterval":"6m"}`, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := MultiClusterConfig{}
			if tt.block != "" {
				if err := json.Unmarshal([]byte(`{"placement":{"capacity":`+tt.block+`}}`), &cfg); err != nil {
					t.Fatal(err)
				}
			}
			if cfg.Placement.Capacity != nil && !tt.omitNamespace {
				cfg.Placement.MemberOperatorNamespace = "operator-system"
			}
			err := cfg.Validate()
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("validation: %s: %v", diff, err)
			}
			if tt.name == "complete" {
				got := []time.Duration{cfg.Placement.Capacity.MaxAgeDuration(), cfg.Placement.Capacity.StabilityWindowDuration(), cfg.Placement.Capacity.RefreshIntervalDuration()}
				if diff := cmp.Diff([]time.Duration{5 * time.Minute, time.Minute, 10 * time.Second}, got); diff != "" {
					t.Fatal(diff)
				}
			}
			if tt.name == "absent" || tt.name == "null" {
				if diff := cmp.Diff((*PlacementCapacityConfig)(nil), cfg.Placement.Capacity); diff != "" {
					t.Fatal(diff)
				}
			}
		})
	}
}
