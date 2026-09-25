package validation

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestValidateRouting(t *testing.T) {
	t.Run("nil spec and absent routing are valid", func(t *testing.T) {
		require.NoError(t, ValidateRouting(nil))
		require.NoError(t, ValidateRouting(&v1beta1.InferenceServiceSpec{}))
	})

	t.Run("empty routing and enabled overrides are valid", func(t *testing.T) {
		require.NoError(t, ValidateRouting(routingSpec(&v1beta1.RoutingSpec{})))
		for _, enabled := range []bool{false, true} {
			enabled := enabled
			require.NoError(t, ValidateRouting(routingSpec(&v1beta1.RoutingSpec{Enabled: &enabled})))
		}
	})

	t.Run("complete routing policy is valid", func(t *testing.T) {
		routing := &v1beta1.RoutingSpec{
			CapacityFactors: map[string]resource.Quantity{
				"cluster-a": resource.MustParse("500m"),
				"cluster-b": resource.MustParse("2"),
			},
			Probe:    validRoutingProbe(),
			Capacity: validRoutingCapacity(),
			Publisher: &v1beta1.RoutingPublisherSpec{Options: map[string]string{
				"routingClass": "premium",
			}},
		}
		require.NoError(t, ValidateRouting(routingSpec(routing)))
	})

	t.Run("new and deprecated capacity factors conflict", func(t *testing.T) {
		spec := routingSpec(&v1beta1.RoutingSpec{CapacityFactors: map[string]resource.Quantity{}})
		spec.Placement = &v1beta1.PlacementSpec{CapacityFactors: map[string]resource.Quantity{}}
		err := ValidateRouting(spec)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must not both be set")
	})

	t.Run("new capacity factors must be positive", func(t *testing.T) {
		for name, quantity := range map[string]string{
			"zero":     "0",
			"negative": "-500m",
		} {
			t.Run(name, func(t *testing.T) {
				err := ValidateRouting(routingSpec(&v1beta1.RoutingSpec{
					CapacityFactors: map[string]resource.Quantity{"cluster-a": resource.MustParse(quantity)},
				}))
				require.Error(t, err)
				assert.Contains(t, err.Error(), "must be positive")
			})
		}
	})

	t.Run("deprecated capacity factors retain legacy admission behavior", func(t *testing.T) {
		spec := &v1beta1.InferenceServiceSpec{Placement: &v1beta1.PlacementSpec{
			CapacityFactors: map[string]resource.Quantity{"cluster-a": resource.MustParse("0")},
		}}
		require.NoError(t, ValidateRouting(spec))
	})
}

func TestValidateRoutingProbe(t *testing.T) {
	t.Run("disabled alone is valid", func(t *testing.T) {
		require.NoError(t, ValidateRouting(routingSpec(&v1beta1.RoutingSpec{
			Probe: &v1beta1.RoutingProbeSpec{Disabled: true},
		})))
	})

	t.Run("disabled is exclusive", func(t *testing.T) {
		probe := &v1beta1.RoutingProbeSpec{Disabled: true, Path: "/health"}
		err := ValidateRouting(routingSpec(&v1beta1.RoutingSpec{Probe: probe}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "disabled")
	})

	t.Run("safe methods are valid", func(t *testing.T) {
		for _, method := range []string{"GET", "HEAD", "POST"} {
			probe := validRoutingProbe()
			probe.Method = method
			require.NoError(t, ValidateRouting(routingSpec(&v1beta1.RoutingSpec{Probe: probe})))
		}
	})

	tests := []struct {
		name        string
		mutate      func(*v1beta1.RoutingProbeSpec)
		wantContain string
	}{
		{name: "empty", mutate: func(p *v1beta1.RoutingProbeSpec) { *p = v1beta1.RoutingProbeSpec{} }, wantContain: "path is required"},
		{name: "relative path", mutate: func(p *v1beta1.RoutingProbeSpec) { p.Path = "health" }, wantContain: "must begin"},
		{name: "missing method", mutate: func(p *v1beta1.RoutingProbeSpec) { p.Method = "" }, wantContain: "method is required"},
		{name: "unsafe method", mutate: func(p *v1beta1.RoutingProbeSpec) { p.Method = "DELETE" }, wantContain: "must be GET, HEAD, or POST"},
		{name: "missing accepted statuses", mutate: func(p *v1beta1.RoutingProbeSpec) { p.AcceptStatuses = nil }, wantContain: "acceptStatuses is required"},
		{name: "missing gated statuses", mutate: func(p *v1beta1.RoutingProbeSpec) { p.GateStatuses = nil }, wantContain: "gateStatuses is required"},
		{name: "invalid accepted status", mutate: func(p *v1beta1.RoutingProbeSpec) { p.AcceptStatuses = []int32{99} }, wantContain: "invalid HTTP status"},
		{name: "invalid gated status", mutate: func(p *v1beta1.RoutingProbeSpec) { p.GateStatuses = []int32{600} }, wantContain: "invalid HTTP status"},
		{name: "authentication failure cannot gate", mutate: func(p *v1beta1.RoutingProbeSpec) { p.GateStatuses = []int32{401} }, wantContain: "must not contain 401"},
		{name: "authorization failure cannot gate", mutate: func(p *v1beta1.RoutingProbeSpec) { p.GateStatuses = []int32{403} }, wantContain: "must not contain 403"},
		{name: "overload cannot gate", mutate: func(p *v1beta1.RoutingProbeSpec) { p.GateStatuses = []int32{429} }, wantContain: "must not contain 429"},
		{name: "overlapping statuses", mutate: func(p *v1beta1.RoutingProbeSpec) { p.GateStatuses = append(p.GateStatuses, 200) }, wantContain: "both acceptStatuses and gateStatuses"},
		{name: "zero period", mutate: func(p *v1beta1.RoutingProbeSpec) { p.Period.Duration = 0 }, wantContain: "period must be positive"},
		{name: "zero timeout", mutate: func(p *v1beta1.RoutingProbeSpec) { p.Timeout.Duration = 0 }, wantContain: "timeout must be positive"},
		{name: "timeout equals period", mutate: func(p *v1beta1.RoutingProbeSpec) { p.Timeout = p.Period }, wantContain: "must be shorter"},
		{name: "zero failure threshold", mutate: func(p *v1beta1.RoutingProbeSpec) { p.FailureThreshold = 0 }, wantContain: "failureThreshold must be positive"},
		{name: "zero success threshold", mutate: func(p *v1beta1.RoutingProbeSpec) { p.SuccessThreshold = 0 }, wantContain: "successThreshold must be positive"},
		{name: "missing all-failed policy", mutate: func(p *v1beta1.RoutingProbeSpec) { p.AllFailedPolicy = "" }, wantContain: "allFailedPolicy"},
		{name: "unknown all-failed policy", mutate: func(p *v1beta1.RoutingProbeSpec) { p.AllFailedPolicy = "Unknown" }, wantContain: "allFailedPolicy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probe := validRoutingProbe()
			tt.mutate(probe)
			err := ValidateRouting(routingSpec(&v1beta1.RoutingSpec{Probe: probe}))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantContain)
		})
	}
}

func TestValidateRoutingCapacity(t *testing.T) {
	t.Run("disabled alone is valid", func(t *testing.T) {
		require.NoError(t, ValidateRouting(routingSpec(&v1beta1.RoutingSpec{
			Capacity: &v1beta1.RoutingCapacitySpec{Disabled: true},
		})))
	})

	t.Run("disabled is exclusive", func(t *testing.T) {
		capacity := &v1beta1.RoutingCapacitySpec{Disabled: true, Options: map[string]string{"unit": "replica"}}
		err := ValidateRouting(routingSpec(&v1beta1.RoutingSpec{Capacity: capacity}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "disabled")
	})

	t.Run("safe methods are valid", func(t *testing.T) {
		for _, method := range []string{"GET", "HEAD", "POST"} {
			capacity := validRoutingCapacity()
			capacity.Method = method
			require.NoError(t, ValidateRouting(routingSpec(&v1beta1.RoutingSpec{Capacity: capacity})))
		}
	})

	tests := []struct {
		name        string
		mutate      func(*v1beta1.RoutingCapacitySpec)
		wantContain string
	}{
		{name: "empty", mutate: func(c *v1beta1.RoutingCapacitySpec) { *c = v1beta1.RoutingCapacitySpec{} }, wantContain: "path is required"},
		{name: "relative path", mutate: func(c *v1beta1.RoutingCapacitySpec) { c.Path = "capacity" }, wantContain: "must begin"},
		{name: "missing method", mutate: func(c *v1beta1.RoutingCapacitySpec) { c.Method = "" }, wantContain: "method is required"},
		{name: "unsafe method", mutate: func(c *v1beta1.RoutingCapacitySpec) { c.Method = "CONNECT" }, wantContain: "must be GET, HEAD, or POST"},
		{name: "missing format", mutate: func(c *v1beta1.RoutingCapacitySpec) { c.Format = "" }, wantContain: "format is required"},
		{name: "zero period", mutate: func(c *v1beta1.RoutingCapacitySpec) { c.Period.Duration = 0 }, wantContain: "period must be positive"},
		{name: "zero timeout", mutate: func(c *v1beta1.RoutingCapacitySpec) { c.Timeout.Duration = 0 }, wantContain: "timeout must be positive"},
		{name: "timeout equals period", mutate: func(c *v1beta1.RoutingCapacitySpec) { c.Timeout = c.Period }, wantContain: "must be shorter"},
		{name: "zero samples", mutate: func(c *v1beta1.RoutingCapacitySpec) { c.Samples = 0 }, wantContain: "samples must be positive"},
		{name: "zero quorum", mutate: func(c *v1beta1.RoutingCapacitySpec) { c.Quorum = 0 }, wantContain: "quorum must be positive"},
		{name: "quorum above samples", mutate: func(c *v1beta1.RoutingCapacitySpec) { c.Quorum = c.Samples + 1 }, wantContain: "must not exceed"},
		{name: "zero max age", mutate: func(c *v1beta1.RoutingCapacitySpec) { c.MaxAge.Duration = 0 }, wantContain: "maxAge must be positive"},
		{name: "max age below period", mutate: func(c *v1beta1.RoutingCapacitySpec) { c.MaxAge.Duration = c.Period.Duration - time.Second }, wantContain: "must be at least"},
		{name: "max age shorter than quorum window", mutate: func(c *v1beta1.RoutingCapacitySpec) { c.MaxAge.Duration = c.Period.Duration }, wantContain: "must span at least"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capacity := validRoutingCapacity()
			tt.mutate(capacity)
			err := ValidateRouting(routingSpec(&v1beta1.RoutingSpec{Capacity: capacity}))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantContain)
		})
	}
}

func TestValidateRoutingPublisher(t *testing.T) {
	tests := []struct {
		name        string
		options     map[string]string
		wantContain string
	}{
		{name: "nil options", options: nil},
		{name: "valid options", options: map[string]string{"routingClass": "premium"}},
		{name: "empty key", options: map[string]string{"": "value"}, wantContain: "whitespace-only key"},
		{name: "whitespace key", options: map[string]string{" \t": "value"}, wantContain: "whitespace-only key"},
		{name: "empty value", options: map[string]string{"key": ""}, wantContain: "must not be empty"},
		{name: "whitespace value", options: map[string]string{"key": " \n"}, wantContain: "must not be empty"},
		{name: "key too long", options: map[string]string{strings.Repeat("k", v1beta1.MaxRoutingPublisherOptionKeyBytes+1): "value"}, wantContain: "byte limit"},
		{name: "value too long", options: map[string]string{"key": strings.Repeat("v", v1beta1.MaxRoutingPublisherOptionValueBytes+1)}, wantContain: "byte value limit"},
	}

	tooMany := make(map[string]string, v1beta1.MaxRoutingPublisherOptions+1)
	for i := 0; i <= v1beta1.MaxRoutingPublisherOptions; i++ {
		tooMany[fmt.Sprintf("key-%d", i)] = "value"
	}
	tests = append(tests, struct {
		name        string
		options     map[string]string
		wantContain string
	}{name: "too many options", options: tooMany, wantContain: "at most"})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRouting(routingSpec(&v1beta1.RoutingSpec{
				Publisher: &v1beta1.RoutingPublisherSpec{Options: tt.options},
			}))
			if tt.wantContain == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantContain)
		})
	}
}

func routingSpec(routing *v1beta1.RoutingSpec) *v1beta1.InferenceServiceSpec {
	return &v1beta1.InferenceServiceSpec{Routing: routing}
}

func validRoutingProbe() *v1beta1.RoutingProbeSpec {
	return &v1beta1.RoutingProbeSpec{
		Path:             "/v1/models",
		Method:           "GET",
		AcceptStatuses:   []int32{200},
		GateStatuses:     []int32{404, 500, 502, 503, 504},
		Period:           metav1.Duration{Duration: 10 * time.Second},
		Timeout:          metav1.Duration{Duration: 3 * time.Second},
		FailureThreshold: 3,
		SuccessThreshold: 2,
		AllFailedPolicy:  v1beta1.RoutingAllFailedPolicyPreserveTraffic,
	}
}

func validRoutingCapacity() *v1beta1.RoutingCapacitySpec {
	return &v1beta1.RoutingCapacitySpec{
		Path:    "/capacity",
		Method:  "GET",
		Format:  "Report",
		Period:  metav1.Duration{Duration: 30 * time.Second},
		Timeout: metav1.Duration{Duration: 3 * time.Second},
		Samples: 10,
		Quorum:  2,
		MaxAge:  metav1.Duration{Duration: time.Minute},
	}
}
