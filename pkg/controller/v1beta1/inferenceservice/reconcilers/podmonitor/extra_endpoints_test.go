package podmonitor

import (
	"testing"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestParseExtraEndpoints(t *testing.T) {
	key := constants.ExtraPodMetricsEndpointsAnnotationKey
	ep := func(port, path string) monitoringv1.PodMetricsEndpoint {
		p := port
		return monitoringv1.PodMetricsEndpoint{Port: &p, Path: path, Interval: "10s"}
	}

	tests := []struct {
		name        string
		annotations map[string]string
		want        []monitoringv1.PodMetricsEndpoint
	}{
		{name: "nil annotations", annotations: nil, want: nil},
		{name: "absent key", annotations: map[string]string{"other": "x"}, want: nil},
		{name: "empty value", annotations: map[string]string{key: ""}, want: nil},
		{name: "whitespace-only value", annotations: map[string]string{key: "   "}, want: nil},
		{
			name:        "single extra",
			annotations: map[string]string{key: "http:/engine_metrics"},
			want:        []monitoringv1.PodMetricsEndpoint{ep("http", "/engine_metrics")},
		},
		{
			name:        "two extras kept in order",
			annotations: map[string]string{key: "http:/a,metrics:/b"},
			want:        []monitoringv1.PodMetricsEndpoint{ep("http", "/a"), ep("metrics", "/b")},
		},
		{
			name:        "whitespace trimmed around entries, names and paths",
			annotations: map[string]string{key: "  http : /engine_metrics , metrics:/b "},
			want:        []monitoringv1.PodMetricsEndpoint{ep("http", "/engine_metrics"), ep("metrics", "/b")},
		},
		{
			name:        "split on first colon only (colon inside path preserved)",
			annotations: map[string]string{key: "http:/a:b"},
			want:        []monitoringv1.PodMetricsEndpoint{ep("http", "/a:b")},
		},
		{name: "malformed: no colon", annotations: map[string]string{key: "nocolon"}, want: nil},
		{name: "malformed: path without leading slash", annotations: map[string]string{key: "http:noslash"}, want: nil},
		{name: "malformed: empty port", annotations: map[string]string{key: ":/x"}, want: nil},
		{
			name:        "empty entries skipped, valid kept",
			annotations: map[string]string{key: ",http:/a,,"},
			want:        []monitoringv1.PodMetricsEndpoint{ep("http", "/a")},
		},
		{
			name:        "mixed valid and malformed: only valid kept, order preserved",
			annotations: map[string]string{key: "http:/a,bad,metrics:/b"},
			want:        []monitoringv1.PodMetricsEndpoint{ep("http", "/a"), ep("metrics", "/b")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseExtraEndpoints(tt.annotations, "10s")
			require.Len(t, got, len(tt.want))
			for i := range tt.want {
				require.NotNil(t, got[i].Port, "port[%d] must be non-nil", i)
				assert.Equal(t, *tt.want[i].Port, *got[i].Port, "port[%d]", i)
				assert.Equal(t, tt.want[i].Path, got[i].Path, "path[%d]", i)
				assert.Equal(t, tt.want[i].Interval, got[i].Interval, "interval[%d]", i)
			}
		})
	}
}

// TestParseExtraEndpoints_DistinctPointers guards the loop-pointer-aliasing
// gotcha: each endpoint's Port must point at its own string, not a shared one.
func TestParseExtraEndpoints_DistinctPointers(t *testing.T) {
	got := ParseExtraEndpoints(map[string]string{
		constants.ExtraPodMetricsEndpointsAnnotationKey: "http:/a,metrics:/b",
	}, "10s")
	require.Len(t, got, 2)
	require.NotSame(t, got[0].Port, got[1].Port)
	assert.Equal(t, "http", *got[0].Port)
	assert.Equal(t, "metrics", *got[1].Port)
}
