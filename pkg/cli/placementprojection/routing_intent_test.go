package placementprojection

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/report"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestProjectExplainSummarizesDeclaredRoutingWithoutRawConfiguration(t *testing.T) {
	t.Parallel()

	snapshot := fixture(t)
	enabled := true
	snapshot.InferenceService.Spec.Routing = &ome.RoutingSpec{
		Enabled: &enabled,
		CapacityFactors: map[string]resource.Quantity{
			"west": resource.MustParse("2"),
			"east": resource.MustParse("1500m"),
		},
		Probe: &ome.RoutingProbeSpec{
			Path: "/private-probe-token", Method: "HEAD",
			AcceptStatuses: []int32{200, 204}, GateStatuses: []int32{500, 503},
			Period:           metav1.Duration{Duration: 30 * time.Second},
			Timeout:          metav1.Duration{Duration: 5 * time.Second},
			FailureThreshold: 3, SuccessThreshold: 2,
			AllFailedPolicy: ome.RoutingAllFailedPolicyPreserveTraffic,
		},
		Capacity: &ome.RoutingCapacitySpec{
			Path: "/private-capacity-token", Method: "GET",
			Format: "private-format", Options: map[string]string{
				"private-option-key": "private-option-value",
			},
			Period:  metav1.Duration{Duration: time.Minute},
			Timeout: metav1.Duration{Duration: 10 * time.Second},
			Samples: 5, Quorum: 3,
			MaxAge: metav1.Duration{Duration: 3 * time.Minute},
		},
		Publisher: &ome.RoutingPublisherSpec{Options: map[string]string{
			"private-publisher-key": "private-publisher-value",
		}},
	}

	got, err := ProjectExplain(snapshot, fixtureClock)
	require.NoError(t, err)
	intent := got.Content.Routing
	assert.Equal(t, v.PlacementValue("Declared"), intent.State)
	assert.Equal(t, v.PlacementValue("OptIn"), intent.Enablement) // codespell:ignore optin
	assert.Equal(t, v.PlacementValue("Routing"), intent.CapacityFactors.Source)
	assert.Equal(t, 2, intent.CapacityFactors.Count)
	assert.Equal(t, v.PlacementValue("Configured"), intent.Probe.State)
	assert.True(t, intent.Probe.PathPresent)
	assert.Equal(t, v.PlacementValue("HEAD"), intent.Probe.Method)
	assert.Equal(t, 2, intent.Probe.AcceptStatusCount)
	assert.Equal(t, 2, intent.Probe.GateStatusCount)
	assert.Equal(t, "30s", intent.Probe.Period)
	assert.Equal(t, "5s", intent.Probe.Timeout)
	assert.Equal(t, int32(3), intent.Probe.FailureThreshold)
	assert.Equal(t, int32(2), intent.Probe.SuccessThreshold)
	assert.Equal(t, v.PlacementValue("PreserveTraffic"), intent.Probe.AllFailedPolicy)
	assert.Equal(t, v.PlacementValue("Configured"), intent.Capacity.State)
	assert.True(t, intent.Capacity.PathPresent)
	assert.Equal(t, v.PlacementValue("GET"), intent.Capacity.Method)
	assert.True(t, intent.Capacity.FormatPresent)
	assert.Equal(t, 1, intent.Capacity.OptionCount)
	assert.Equal(t, "1m0s", intent.Capacity.Period)
	assert.Equal(t, "10s", intent.Capacity.Timeout)
	assert.Equal(t, int32(5), intent.Capacity.Samples)
	assert.Equal(t, int32(3), intent.Capacity.Quorum)
	assert.Equal(t, "3m0s", intent.Capacity.MaxAge)
	assert.Equal(t, v.PlacementValue("Configured"), intent.Publisher.State)
	assert.Equal(t, 1, intent.Publisher.OptionCount)
	assert.Contains(t, got.Content.UnobservedInputs, v.PlacementValue("OperatorRoutingDefaults"))

	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		var output bytes.Buffer
		require.NoError(t, report.Write(&output, format, got))
		for _, private := range []string{
			"private-probe-token", "private-capacity-token", "private-format",
			"private-option-key", "private-option-value",
			"private-publisher-key", "private-publisher-value",
		} {
			assert.NotContains(t, output.String(), private)
		}
		if format == report.FormatTable {
			for _, line := range strings.Split(output.String(), "\n") {
				assert.LessOrEqual(t, len(line), 80, "line %q", line)
			}
		}
	}
	var wide bytes.Buffer
	require.NoError(t, got.Content.WideTable().Write(&wide))
	for _, want := range []string{
		"Routing intent", "Declared", "Routing enablement", "OptIn", // codespell:ignore optin
		"Routing probe", "Configured", "Probe method", "HEAD; path=true",
		"Probe statuses", "accept=2; gate=2", "Capacity method",
		"GET; path=true; format=true", "Publisher options", "Configured (1)",
	} {
		assert.Contains(t, wide.String(), want)
	}
	for _, line := range strings.Split(wide.String(), "\n") {
		assert.LessOrEqual(t, len(line), 80, "wide line %q", line)
	}
}

func TestProjectExplainRoutingIntentStates(t *testing.T) {
	t.Parallel()

	falseValue := false
	tests := []struct {
		name           string
		configure      func(*ome.InferenceService)
		wantState      v.PlacementValue
		wantEnablement v.PlacementValue
		wantSource     v.PlacementValue
		wantProbe      v.PlacementValue
		wantCap        v.PlacementValue
		wantPub        v.PlacementValue
	}{
		{
			name:      "absent inherits operator defaults",
			wantState: "Absent", wantEnablement: "Inherited", wantSource: "Inherited",
			wantProbe: "Inherited", wantCap: "Inherited", wantPub: "Inherited",
		},
		{
			name: "explicitly disabled features",
			configure: func(parent *ome.InferenceService) {
				parent.Spec.Routing = &ome.RoutingSpec{
					Enabled:   &falseValue,
					Probe:     &ome.RoutingProbeSpec{Disabled: true},
					Capacity:  &ome.RoutingCapacitySpec{Disabled: true},
					Publisher: &ome.RoutingPublisherSpec{},
				}
			},
			wantState: "Declared", wantEnablement: "OptOut", wantSource: "Inherited",
			wantProbe: "Disabled", wantCap: "Disabled", wantPub: "Configured",
		},
		{
			name: "legacy capacity factors",
			configure: func(parent *ome.InferenceService) {
				parent.Spec.Placement.CapacityFactors = map[string]resource.Quantity{ //nolint:staticcheck
					"west": resource.MustParse("2"),
				}
			},
			wantState: "Declared", wantEnablement: "Inherited", wantSource: "LegacyPlacement",
			wantProbe: "Inherited", wantCap: "Inherited", wantPub: "Inherited",
		},
		{
			name: "conflicting factor sources are invalid",
			configure: func(parent *ome.InferenceService) {
				parent.Spec.Placement.CapacityFactors = map[string]resource.Quantity{ //nolint:staticcheck
					"west": resource.MustParse("2"),
				}
				parent.Spec.Routing = &ome.RoutingSpec{CapacityFactors: map[string]resource.Quantity{
					"east": resource.MustParse("3"),
				}}
			},
			wantState: "Invalid", wantEnablement: "Inherited", wantSource: "Conflict",
			wantProbe: "Inherited", wantCap: "Inherited", wantPub: "Inherited",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			snapshot := fixture(t)
			if test.configure != nil {
				test.configure(snapshot.InferenceService)
			}
			got, err := ProjectExplain(snapshot, fixtureClock)
			require.NoError(t, err)
			assert.Equal(t, test.wantState, got.Content.Routing.State)
			assert.Equal(t, test.wantEnablement, got.Content.Routing.Enablement)
			assert.Equal(t, test.wantSource, got.Content.Routing.CapacityFactors.Source)
			assert.Equal(t, test.wantProbe, got.Content.Routing.Probe.State)
			assert.Equal(t, test.wantCap, got.Content.Routing.Capacity.State)
			assert.Equal(t, test.wantPub, got.Content.Routing.Publisher.State)
		})
	}
}

func TestProjectExplainRoutingNormalizesInvalidAndOversizedInputs(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		configure func(*ome.InferenceService)
		wantState v.PlacementValue
	}{
		{
			name: "hostile invalid values",
			configure: func(parent *ome.InferenceService) {
				parent.Spec.Routing = &ome.RoutingSpec{
					Probe: &ome.RoutingProbeSpec{
						Path: "/SECRET_PATH", Method: "SECRET_METHOD\n\x1b[31m",
						AllFailedPolicy: "SECRET_POLICY",
					},
					Capacity: &ome.RoutingCapacitySpec{
						Path: "/SECRET_CAPACITY", Method: "SECRET_CAPACITY_METHOD",
						Format:  "SECRET_FORMAT",
						Options: map[string]string{"SECRET_KEY": "SECRET_VALUE"},
					},
					Publisher: &ome.RoutingPublisherSpec{Options: map[string]string{
						"SECRET_PUBLISHER_KEY": "SECRET_PUBLISHER_VALUE",
					}},
				}
			},
			wantState: "Invalid",
		},
		{
			name: "factor scan budget",
			configure: func(parent *ome.InferenceService) {
				factors := make(map[string]resource.Quantity, 257)
				for i := 0; i < 257; i++ {
					factors[fmt.Sprintf("cluster-%03d", i)] = resource.MustParse("1")
				}
				parent.Spec.Routing = &ome.RoutingSpec{CapacityFactors: factors}
			},
			wantState: "BudgetExceeded",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			snapshot := fixture(t)
			test.configure(snapshot.InferenceService)
			got, err := ProjectExplain(snapshot, fixtureClock)
			require.NoError(t, err)
			assert.Equal(t, test.wantState, got.Content.Routing.State)
			for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
				var output bytes.Buffer
				require.NoError(t, report.Write(&output, format, got))
				for _, secret := range []string{
					"SECRET_PATH", "SECRET_METHOD", "SECRET_POLICY", "SECRET_CAPACITY",
					"SECRET_CAPACITY_METHOD", "SECRET_FORMAT", "SECRET_KEY", "SECRET_VALUE",
					"SECRET_PUBLISHER_KEY", "SECRET_PUBLISHER_VALUE", "\x1b",
				} {
					assert.NotContains(t, output.String(), secret)
				}
			}
		})
	}
}

func TestProjectExplainRoutingOmitsUnavailableRequiredScalars(t *testing.T) {
	t.Parallel()

	snapshot := fixture(t)
	snapshot.InferenceService.Spec.Routing = &ome.RoutingSpec{
		Probe:    &ome.RoutingProbeSpec{Path: "/health", Method: "GET"},
		Capacity: &ome.RoutingCapacitySpec{Path: "/capacity", Method: "GET", Format: "json"},
	}

	got, err := ProjectExplain(snapshot, fixtureClock)
	require.NoError(t, err)
	require.Equal(t, v.PlacementValue("Invalid"), got.Content.Routing.State)

	for _, format := range []report.Format{report.FormatJSON, report.FormatYAML} {
		var output bytes.Buffer
		require.NoError(t, report.Write(&output, format, got))
		document := map[string]any{}
		if format == report.FormatJSON {
			require.NoError(t, json.Unmarshal(output.Bytes(), &document))
		} else {
			require.NoError(t, yaml.Unmarshal(output.Bytes(), &document))
		}
		content := document["content"].(map[string]any)
		routing := content["routing"].(map[string]any)
		probe := routing["probe"].(map[string]any)
		capacity := routing["capacity"].(map[string]any)
		for _, unavailable := range []string{
			"period", "timeout", "failureThreshold", "successThreshold",
		} {
			assert.NotContains(t, probe, unavailable)
		}
		for _, unavailable := range []string{
			"period", "timeout", "samples", "quorum", "maxAge",
		} {
			assert.NotContains(t, capacity, unavailable)
		}
		assert.Equal(t, float64(0), probe["acceptStatusCount"])
		assert.Equal(t, float64(0), capacity["optionCount"])
	}

	var wide bytes.Buffer
	require.NoError(t, got.Content.WideTable().Write(&wide))
	for _, want := range []string{
		"failure=-; success=-",
		"period=-; timeout=-",
		"samples=-; quorum=-; options=0",
		"period=-; timeout=-; maxAge=-",
	} {
		assert.Contains(t, wide.String(), want)
	}
	assert.NotContains(t, wide.String(), "=0s")
}

func TestProjectExplainRoutingPreservesInvalidNonzeroScalars(t *testing.T) {
	t.Parallel()

	snapshot := fixture(t)
	snapshot.InferenceService.Spec.Routing = &ome.RoutingSpec{
		Probe: &ome.RoutingProbeSpec{
			Path: "/health", Method: "GET",
			AcceptStatuses: []int32{200}, GateStatuses: []int32{500},
			Period:           metav1.Duration{Duration: -30 * time.Second},
			Timeout:          metav1.Duration{Duration: -5 * time.Second},
			FailureThreshold: -3, SuccessThreshold: -2,
			AllFailedPolicy: ome.RoutingAllFailedPolicyPreserveTraffic,
		},
		Capacity: &ome.RoutingCapacitySpec{
			Path: "/capacity", Method: "GET", Format: "json",
			Period:  metav1.Duration{Duration: -1 * time.Minute},
			Timeout: metav1.Duration{Duration: -10 * time.Second},
			Samples: -5, Quorum: -3,
			MaxAge: metav1.Duration{Duration: -3 * time.Minute},
		},
	}

	got, err := ProjectExplain(snapshot, fixtureClock)
	require.NoError(t, err)
	require.Equal(t, v.PlacementValue("Invalid"), got.Content.Routing.State)
	assert.Equal(t, "-30s", got.Content.Routing.Probe.Period)
	assert.Equal(t, "-5s", got.Content.Routing.Probe.Timeout)
	assert.Equal(t, int32(-3), got.Content.Routing.Probe.FailureThreshold)
	assert.Equal(t, int32(-2), got.Content.Routing.Probe.SuccessThreshold)
	assert.Equal(t, "-1m0s", got.Content.Routing.Capacity.Period)
	assert.Equal(t, "-10s", got.Content.Routing.Capacity.Timeout)
	assert.Equal(t, int32(-5), got.Content.Routing.Capacity.Samples)
	assert.Equal(t, int32(-3), got.Content.Routing.Capacity.Quorum)
	assert.Equal(t, "-3m0s", got.Content.Routing.Capacity.MaxAge)
}

func TestRoutingIntentBudgetBoundaries(t *testing.T) {
	t.Parallel()

	factors := func(count int) map[string]resource.Quantity {
		out := make(map[string]resource.Quantity, count)
		for i := 0; i < count; i++ {
			out[fmt.Sprintf("cluster-%04d", i)] = resource.MustParse("1")
		}
		return out
	}
	options := func(count int) map[string]string {
		out := make(map[string]string, count)
		for i := 0; i < count; i++ {
			out[fmt.Sprintf("option-%04d", i)] = "value"
		}
		return out
	}

	tests := []struct {
		name        string
		routing     *ome.RoutingSpec
		legacyCount int
		want        bool
	}{
		{name: "legacy factors exact", legacyCount: routingFactorScanLimit},
		{name: "legacy factors over", legacyCount: routingFactorScanLimit + 1, want: true},
		{name: "routing factors exact", routing: &ome.RoutingSpec{CapacityFactors: factors(routingFactorScanLimit)}},
		{name: "routing factors over", routing: &ome.RoutingSpec{CapacityFactors: factors(routingFactorScanLimit + 1)}, want: true},
		{name: "probe statuses exact", routing: &ome.RoutingSpec{Probe: &ome.RoutingProbeSpec{
			AcceptStatuses: make([]int32, routingProbeStatusScanLimit/2),
			GateStatuses:   make([]int32, routingProbeStatusScanLimit/2),
		}}},
		{name: "probe statuses over", routing: &ome.RoutingSpec{Probe: &ome.RoutingProbeSpec{
			AcceptStatuses: make([]int32, routingProbeStatusScanLimit),
			GateStatuses:   make([]int32, 1),
		}}, want: true},
		{name: "capacity options exact", routing: &ome.RoutingSpec{Capacity: &ome.RoutingCapacitySpec{
			Options: options(routingCapacityOptionScanLimit),
		}}},
		{name: "capacity options over", routing: &ome.RoutingSpec{Capacity: &ome.RoutingCapacitySpec{
			Options: options(routingCapacityOptionScanLimit + 1),
		}}, want: true},
		{name: "publisher options exact", routing: &ome.RoutingSpec{Publisher: &ome.RoutingPublisherSpec{
			Options: options(routingPublisherOptionScanLimit),
		}}},
		{name: "publisher options over", routing: &ome.RoutingSpec{Publisher: &ome.RoutingPublisherSpec{
			Options: options(routingPublisherOptionScanLimit + 1),
		}}, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, routingIntentExceedsBudget(test.routing, test.legacyCount))
		})
	}
}
