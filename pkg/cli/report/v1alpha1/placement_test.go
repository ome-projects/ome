package v1alpha1

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/cli/report"
)

func TestPlacementCanonicalDetachesNestedPointersAndAtomicOrder(t *testing.T) {
	n := int32(5)
	stamp := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	content := PlacementEndpointContent{
		Status:  PlacementStatusContent{Inputs: PlacementInputs{Split: &PlacementSplit{Replicas: PlacementCount{Value: &n}}}, Placement: PlacementReported{Homes: []PlacementHome{{Cluster: "b", AdmittedReplicas: PlacementCount{Value: &n}, Provenance: PlacementProvenance{Policies: []PlacementPolicy{{Name: "z"}, {Name: "a"}}, Components: []PlacementComponent{{Component: "router"}, {Component: "engine"}}, ActiveGroups: []PlacementRolloutGroup{{Ordinal: 0, Source: "Policy"}, {Ordinal: 1, Source: "Inline"}}}}, {Cluster: "a"}}}, Issues: []PlacementIssue{{Group: "z", Code: "a"}, {Group: "a", Code: "b"}}},
		Entries: []PlacementRoute{{Cluster: "b", Capacity: &PlacementCapacity{Allocated: PlacementCount{Value: &n}, Reported: PlacementCount{Value: &n}}, Probe: PlacementProbe{LastAttemptTime: &stamp, ConsecutiveFailures: PlacementCount{Value: &n}}}, {Cluster: "a"}}, Routing: PlacementRouting{Gateway: &PlacementGateway{Name: "gateway"}}, Conditions: []PlacementCondition{{Type: "Routable"}, {Type: "Programmed"}},
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
