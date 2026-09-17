package main

import (
	"strings"
	"testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

func TestValidateInstanceStatusWriteTarget(t *testing.T) {
	bound := uint64(5000)
	tests := []struct {
		name    string
		cfg     *controllerconfig.OMENativeStatusConfig
		wantErr string
	}{
		{name: "nil config", wantErr: "omenativeStatus configuration is missing"},
		{name: "DenseV1", cfg: &controllerconfig.OMENativeStatusConfig{InstanceStatusEncoding: irstatus.EncodingDenseV1}},
		{name: "DenseV1 with bound", cfg: &controllerconfig.OMENativeStatusConfig{InstanceStatusEncoding: irstatus.EncodingDenseV1, MaxDecodedInstances: &bound}},
		{
			name:    "ColumnarV2 is not a supported write target",
			cfg:     &controllerconfig.OMENativeStatusConfig{InstanceStatusEncoding: irstatus.EncodingColumnarV2, MaxDecodedInstances: &bound},
			wantErr: `instanceStatusEncoding "ColumnarV2" is not a write target this manager supports; it writes DenseV1 only`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateInstanceStatusWriteTarget(tt.cfg)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}
