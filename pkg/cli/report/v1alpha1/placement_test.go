package v1alpha1

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

func TestPlacementRoutingEvidenceCanonicalAndAdditiveSchema(t *testing.T) {
	t.Parallel()

	stamp := time.Date(2026, 9, 24, 18, 42, 10, 0, time.FixedZone("west", -7*60*60))
	digest := "sha256:" + strings.Repeat("a", 64)
	content := PlacementEndpointContent{
		Routing: PlacementRouting{
			OverrideActive: PlacementCondition{
				Type:   "OverrideActive",
				Status: "True",
				Reason: "OverridesApplied",
				Source: PlacementEvidence{Freshness: "Current"},
			},
			CapacityFallback: PlacementCondition{
				Type:   "CapacityFallback",
				Status: "True",
				Reason: "EndpointCapacityUnavailable",
				Source: PlacementEvidence{Freshness: "Current"},
			},
		},
		Entries: []PlacementRoute{
			{
				Cluster:      "z-route",
				DrainRefs:    []string{"maintenance-a", "drain-a"},
				DrainPreview: PlacementPreview{State: "Validated", Total: 2, Kept: 2},
				Capacity:     &PlacementCapacity{FallbackReason: "Unreachable"},
				Probe: PlacementProbe{
					PolicyDigest:      digest,
					PolicyDigestState: "Reported",
					Gated:             true,
					GatedState:        "Reported",
					LastAttemptTime:   &stamp,
				},
			},
			{Cluster: "a-route", DrainRefs: []string{}},
		},
	}

	canonical := content.Canonical()
	require.Equal(t, []string{"drain-a", "maintenance-a"}, canonical.Entries[1].DrainRefs)
	require.Equal(t, digest, canonical.Entries[1].Probe.PolicyDigest)
	require.Equal(t, PlacementValue("Reported"), canonical.Entries[1].Probe.GatedState)
	require.Equal(t, PlacementValue("Unreachable"), canonical.Entries[1].Capacity.FallbackReason)
	require.Equal(t, PlacementValue("OverridesApplied"), canonical.Routing.OverrideActive.Reason)
	require.Equal(t, PlacementValue("EndpointCapacityUnavailable"), canonical.Routing.CapacityFallback.Reason)
	require.NotNil(t, canonical.Entries[0].DrainRefs)

	canonical.Entries[1].DrainRefs[0] = "changed"
	*canonical.Entries[1].Probe.LastAttemptTime = stamp.Add(time.Hour)
	require.Equal(t, []string{"maintenance-a", "drain-a"}, content.Entries[0].DrainRefs)
	require.True(t, content.Entries[0].Probe.LastAttemptTime.Equal(stamp))

	reportValue := NewEnvelope(
		KindPlacementEndpoint,
		Metadata{Name: "demo"},
		content,
		ClockFunc(func() time.Time { return time.Time{} }),
	)
	data, err := json.Marshal(reportValue)
	require.NoError(t, err)
	require.Contains(t, string(data), `"overrideActive"`)
	require.Contains(t, string(data), `"capacityFallback"`)
	require.Contains(t, string(data), `"policyDigestState":"Reported"`)
	require.Contains(t, string(data), `"gatedState":"Reported"`)
	require.Contains(t, string(data), `"fallbackReason":"Unreachable"`)

	empty := NewEnvelope(
		KindPlacementEndpoint,
		Metadata{Name: "demo"},
		PlacementEndpointContent{},
		ClockFunc(func() time.Time { return time.Time{} }),
	)
	emptyData, err := json.Marshal(empty)
	require.NoError(t, err)
	require.Contains(t, string(emptyData), `"entries":[]`)

	emptyRoute := NewEnvelope(
		KindPlacementEndpoint,
		Metadata{Name: "demo"},
		PlacementEndpointContent{Entries: []PlacementRoute{{Cluster: "empty"}}},
		ClockFunc(func() time.Time { return time.Time{} }),
	)
	emptyRouteData, err := json.Marshal(emptyRoute)
	require.NoError(t, err)
	require.Contains(t, string(emptyRouteData), `"drainRefs":[]`)
	require.NotContains(t, string(emptyRouteData), `"drainRefs":null`)
}

func TestPlacementRoutingEvidenceCompactWideAndWidth(t *testing.T) {
	t.Parallel()

	hostileIdentity := strings.Repeat("界", 200)
	digest := "sha256:" + strings.Repeat("a", 64)
	content := PlacementEndpointContent{
		Status: PlacementStatusContent{Placement: PlacementReported{
			ReportedCluster: hostileIdentity,
			Address:         PlacementAddress{EndpointOrigin: "https://" + hostileIdentity},
		}},
		Routing: PlacementRouting{
			Source: PlacementEvidence{Freshness: "Current"},
			OverrideActive: PlacementCondition{
				Status: "True", Reason: "OverridesApplied",
				Source: PlacementEvidence{Freshness: "Current"},
			},
			CapacityFallback: PlacementCondition{
				Status: "True", Reason: "EndpointCapacityUnavailable",
				Source: PlacementEvidence{Freshness: "Current"},
			},
		},
		Entries: []PlacementRoute{{
			Cluster:      hostileIdentity,
			Address:      PlacementAddress{EndpointOrigin: "https://" + hostileIdentity},
			DrainRefs:    []string{"drain-a", hostileIdentity},
			DrainPreview: PlacementPreview{State: "Validated", Total: 2, Kept: 2},
			Capacity:     &PlacementCapacity{FallbackReason: "Unreachable"},
			Probe: PlacementProbe{
				Result:            "Failing",
				PolicyDigest:      digest,
				PolicyDigestState: "Reported",
				Gated:             true,
				GatedState:        "Reported",
			},
		}},
	}

	require.Equal(t, "True: OverridesApplied (Current)", placementConditionCell(content.Routing.OverrideActive))
	require.Equal(t, "drains=2; gate=True; fallback=Unreachable", placementRouteEvidenceCell(content.Entries[0]))
	require.Equal(t, "drain-a, "+hostileIdentity, placementDrainRefsCell(content.Entries[0]))
	require.Equal(t, digest, placementProbePolicyCell(content.Entries[0].Probe))

	compact := content.Table()
	wide := content.WideTable()
	require.Less(t, len(compact.Rows), len(wide.Rows))

	compactOutput := placementRenderTable(t, compact)
	wideOutput := placementRenderTable(t, wide)
	for _, expected := range []string{
		"Traffic override", "Capacity fallback", "Route evidence", "Probe policy",
	} {
		require.Contains(t, compactOutput, expected)
	}
	require.Contains(t, compactOutput, "sha256:"+strings.Repeat("a", 20))
	require.NotContains(t, compactOutput, digest)
	for _, expected := range []string{"Drain refs", "Probe gate", "Home cap fallback"} {
		require.Contains(t, wideOutput, expected)
	}

	for name, output := range map[string]string{"compact": compactOutput, "wide": wideOutput} {
		for _, line := range strings.Split(output, "\n") {
			require.LessOrEqual(t, printers.CellDisplayWidth(line), 80, "%s line %q", name, line)
		}
	}
}

func TestPlacementProbePolicyCellFailsClosed(t *testing.T) {
	t.Parallel()

	validDigest := "sha256:" + strings.Repeat("a", 64)
	privateDigest := "private-token-" + strings.Repeat("b", 64)
	tests := []struct {
		name  string
		probe PlacementProbe
		want  string
	}{
		{
			name:  "reported digest",
			probe: PlacementProbe{PolicyDigest: validDigest, PolicyDigestState: "Reported"},
			want:  validDigest,
		},
		{
			name:  "not recorded ignores a stale digest",
			probe: PlacementProbe{PolicyDigest: privateDigest, PolicyDigestState: "NotRecorded"},
			want:  "NotRecorded",
		},
		{
			name:  "unavailable ignores a malformed digest",
			probe: PlacementProbe{PolicyDigest: privateDigest, PolicyDigestState: "Unavailable"},
			want:  "Unavailable",
		},
		{
			name:  "empty state ignores a digest",
			probe: PlacementProbe{PolicyDigest: privateDigest},
			want:  "NotRecorded",
		},
		{
			name:  "unknown state is unavailable",
			probe: PlacementProbe{PolicyDigest: privateDigest, PolicyDigestState: "https://private.invalid/token"},
			want:  "Unavailable",
		},
		{
			name:  "reported without digest is unavailable",
			probe: PlacementProbe{PolicyDigestState: "Reported"},
			want:  "Unavailable",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, test.want, placementProbePolicyCell(test.probe))
			content := PlacementEndpointContent{Entries: []PlacementRoute{{
				Cluster:      "home-a",
				DrainPreview: PlacementPreview{State: "NotRecorded"},
				Probe:        test.probe,
			}}}
			output := placementRenderTable(t, content.Table())
			if test.probe.PolicyDigestState == "Reported" && test.probe.PolicyDigest != "" {
				require.Contains(t, output, "sha256:"+strings.Repeat("a", 20))
				return
			}
			require.Contains(t, output, test.want)
			require.NotContains(t, output, "private-token")
			require.NotContains(t, output, "private.invalid")
		})
	}
}

func TestPlacementRouteEvidenceUsesDrainPreviewStateAndTotal(t *testing.T) {
	t.Parallel()

	retained := make([]string, 16)
	for i := range retained {
		retained[i] = "drain-" + strconv.Itoa(i)
	}
	tests := []struct {
		name   string
		route  PlacementRoute
		want   string
		forbid string
	}{
		{
			name: "validated truncation reports source total",
			route: PlacementRoute{
				DrainRefs:    retained,
				DrainPreview: PlacementPreview{State: "Validated", Total: 17, Kept: 16, Truncated: true},
			},
			want: "drains=17",
		},
		{
			name: "budget failure reports state",
			route: PlacementRoute{
				DrainPreview: PlacementPreview{State: "BudgetExceeded", Total: 65, Truncated: true},
			},
			want:   "drains=BudgetExceeded",
			forbid: "drains=0",
		},
		{
			name: "malformed payload reports state",
			route: PlacementRoute{
				DrainPreview: PlacementPreview{State: "MalformedPayload", Total: 1},
			},
			want:   "drains=MalformedPayload",
			forbid: "drains=0",
		},
		{
			name: "unavailable reports state",
			route: PlacementRoute{
				DrainPreview: PlacementPreview{State: "Unavailable"},
			},
			want:   "drains=Unavailable",
			forbid: "drains=0",
		},
		{
			name: "conflicting duplicates report precise state",
			route: PlacementRoute{
				DrainPreview: PlacementPreview{State: "ConflictingDuplicates", Total: 2},
			},
			want:   "drains=ConflictingDuplicates",
			forbid: "drains=Unavailable",
		},
		{
			name:   "missing preview reports not recorded",
			route:  PlacementRoute{},
			want:   "drains=NotRecorded",
			forbid: "drains=0",
		},
		{
			name: "unknown preview state fails closed",
			route: PlacementRoute{
				DrainPreview: PlacementPreview{State: "private-token"},
			},
			want:   "drains=Unavailable",
			forbid: "private-token",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := placementRouteEvidenceCell(test.route)
			require.Contains(t, got, test.want)
			if test.forbid != "" {
				require.NotContains(t, got, test.forbid)
			}

			test.route.Cluster = "home-a"
			output := placementRenderTable(t, (PlacementEndpointContent{
				Entries: []PlacementRoute{test.route},
			}).Table())
			require.Contains(t, output, test.want)
			if test.forbid != "" {
				require.NotContains(t, output, test.forbid)
			}
			for _, line := range strings.Split(output, "\n") {
				require.LessOrEqual(t, printers.CellDisplayWidth(line), 80, "line %q", line)
			}
		})
	}
}

func placementRenderTable(t *testing.T, table report.Table) string {
	t.Helper()
	var out bytes.Buffer
	require.NoError(t, table.Write(&out))
	return out.String()
}

func TestPlacementCanonicalDetachesNestedPointersAndAtomicOrder(t *testing.T) {
	n := int32(5)
	stamp := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	content := PlacementEndpointContent{
		Status:  PlacementStatusContent{Inputs: PlacementInputs{Split: &PlacementSplit{Replicas: PlacementCount{Value: &n}}}, Placement: PlacementReported{Homes: []PlacementHome{{Cluster: "b", AdmittedReplicas: PlacementCount{Value: &n}, Provenance: PlacementProvenance{Policies: []PlacementPolicy{{Name: "z"}, {Name: "a"}}, Components: []PlacementComponent{{Component: "router"}, {Component: "engine"}}, ActiveGroups: []PlacementRolloutGroup{{Ordinal: 0, Source: "Policy"}, {Ordinal: 1, Source: "Inline"}}}}, {Cluster: "a"}}}, Issues: []PlacementIssue{{Group: "z", Code: "a"}, {Group: "a", Code: "b"}}},
		Entries: []PlacementRoute{{Cluster: "b", Capacity: &PlacementCapacity{Allocated: PlacementCount{Value: &n}, Reported: PlacementCount{Value: &n}}, Probe: PlacementProbe{LastAttemptTime: &stamp, ConsecutiveFailures: PlacementCount{Value: &n}}}, {Cluster: "a"}}, Routing: PlacementRouting{Gateway: &PlacementGateway{Name: "gateway"}}, Conditions: []PlacementCondition{{Type: "Routable"}, {Type: "Published"}},
	}
	copyValue := content.Canonical()
	if copyValue.Entries[0].Cluster != "a" || copyValue.Status.Placement.Homes[1].Provenance.Policies[0].Name != "a" || copyValue.Status.Placement.Homes[1].Provenance.ActiveGroups[0].Source != "Policy" {
		t.Fatal("canonical map/atomic order")
	}
	*copyValue.Status.Inputs.Split.Replicas.Value = 99
	*copyValue.Status.Placement.Homes[1].AdmittedReplicas.Value = 98
	*copyValue.Entries[1].Capacity.Allocated.Value = 97
	*copyValue.Entries[1].Probe.LastAttemptTime = stamp.Add(time.Hour)
	copyValue.Routing.Gateway.Name = "changed"
	copyValue.Status.Placement.Homes[1].Provenance.Policies[0].Name = "changed"
	if n != 5 || !content.Entries[0].Probe.LastAttemptTime.Equal(stamp) || content.Routing.Gateway.Name != "gateway" || content.Status.Placement.Homes[0].Provenance.Policies[0].Name != "z" {
		t.Fatalf("canonical aliases source: %+v", content)
	}
}

func TestPlacementFourViewsBoundedAndTypedEmptyArrays(t *testing.T) {
	status := PlacementStatusContent{Placement: PlacementReported{Homes: []PlacementHome{}}}
	for i := 0; i < 8; i++ {
		status.Placement.Homes = append(status.Placement.Homes, PlacementHome{Cluster: strings.Repeat("a", 200)})
	}
	status.Placement.HomePreview = PlacementPreview{State: "Validated", Total: 8, Kept: 8}
	if len(status.Table().Rows) >= len(status.WideTable().Rows) {
		t.Fatal("wide must offer additional bounded detail")
	}
	for _, table := range []report.Table{status.Table(), status.WideTable(), (PlacementExplainContent{Status: status}).Table(), (PlacementExplainContent{Status: status}).WideTable(), (PlacementEndpointContent{Status: status}).Table(), (PlacementEndpointContent{Status: status}).WideTable()} {
		var out bytes.Buffer
		if err := table.Write(&out); err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(out.String(), "\n") {
			if len(line) > 80 {
				t.Fatalf("width=%d: %s", len(line), line)
			}
		}
	}
	empty := NewEnvelope(KindPlacementEndpoint, Metadata{Name: "demo"}, PlacementEndpointContent{}, ClockFunc(func() time.Time { return time.Time{} }))
	data, err := json.Marshal(empty)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), ":null") || !strings.Contains(string(data), `"entries":[]`) {
		t.Fatalf("unnormalized arrays: %s", data)
	}
}

func TestPlacementCanonicalEqualIdentityFullFieldTies(t *testing.T) {
	a := PlacementStatusContent{Placement: PlacementReported{Homes: []PlacementHome{{Cluster: "same", Phase: "Admitted"}, {Cluster: "same", Phase: "Placed"}}}}
	b := a
	b.Placement.Homes = []PlacementHome{a.Placement.Homes[1], a.Placement.Homes[0]}
	first, _ := json.Marshal(a.Canonical())
	second, _ := json.Marshal(b.Canonical())
	if !bytes.Equal(first, second) {
		t.Fatal("equal identity full-field tie is input-order dependent")
	}
}
