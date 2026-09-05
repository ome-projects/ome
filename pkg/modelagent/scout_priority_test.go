package modelagent

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestEffectiveModelDownloadPriority(t *testing.T) {
	background := v1beta1.ModelDownloadPriorityBackground
	high := v1beta1.ModelDownloadPriorityHigh
	tests := []struct {
		name    string
		storage *v1beta1.StorageSpec
		status  *v1beta1.ModelStatusSpec
		want    v1beta1.ModelDownloadPriority
	}{
		{name: "default", want: v1beta1.ModelDownloadPriorityStandard},
		{name: "user background", storage: &v1beta1.StorageSpec{DownloadPriority: &background}, want: background},
		{name: "user high", storage: &v1beta1.StorageSpec{DownloadPriority: &high}, want: high},
		{
			name:    "serving demand overrides background",
			storage: &v1beta1.StorageSpec{DownloadPriority: &background},
			status: &v1beta1.ModelStatusSpec{DownloadScheduling: &v1beta1.ModelDownloadSchedulingStatus{
				ServingDemand: true,
			}},
			want: high,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, effectiveModelDownloadPriority(tt.storage, tt.status))
		})
	}
}
