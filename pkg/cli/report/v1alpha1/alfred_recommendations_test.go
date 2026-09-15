package v1alpha1

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/cli/printers"
)

func TestAlfredCanonicalCopiesAndOrdersAllFields(t *testing.T) {
	stamp := time.Date(2026, 9, 15, 4, 0, 0, 0, time.FixedZone("test", -7*3600))
	base := AlfredRecommendation{Workload: "p/chat", Component: "engine", Policy: "nodehealth", Reason: "NodeUnhealthy", Outcome: "withheld", Classification: "Withheld", Executability: "Unverifiable"}
	// Unknown enum values may be used by report consumers; total lexical
	// ordering must still distinguish them instead of tying on enum rank.
	one, two := base, base
	one.DispatchReason = "UnknownZ"
	two.DispatchReason = "UnknownA"
	input := AlfredRecommendationsContent{Recommendations: []AlfredRecommendation{one, two}, Issues: []string{"Z", "A", "A"}, Timestamp: &stamp}
	before, _ := json.Marshal(input)
	got := input.Canonical()
	if !reflect.DeepEqual(got.Issues, []string{"A", "Z"}) || got.Recommendations[0].DispatchReason != "UnknownA" || got.Timestamp.Format(time.RFC3339) != "2026-09-15T11:00:00Z" {
		t.Fatalf("canonical %+v", got)
	}
	got.Issues[0] = "changed"
	got.Recommendations[0].Workload = "changed"
	*got.Timestamp = time.Time{}
	after, _ := json.Marshal(input)
	if string(before) != string(after) {
		t.Fatal("canonical mutated caller")
	}
	zero := AlfredRecommendationsContent{}.Canonical()
	if zero.Issues == nil || zero.Recommendations == nil {
		t.Fatal("nil schema slices")
	}
}

func TestAlfredCompactWidthAndWideExpansion(t *testing.T) {
	name := strings.Repeat("a", 63)
	c := AlfredRecommendationsContent{SourceNamespace: name, ConfigName: name, RecordName: name, ConfigKey: "config.yaml", RecordKey: "last-cycle.json", State: "Partial", ConfigState: "Available", RecordState: "Available", Freshness: "Stale", ConfigMode: "recommend-only", RecordMode: "execute", Rows: 1, Scanned: 800, Invalid: 2, Omitted: 799, Issues: []string{"ConfigRecordModeMismatch", "DuplicateCandidates"}, Recommendations: []AlfredRecommendation{{Workload: name + "/" + name, Component: "engine", Instance: 2147483647, Policy: "defragmentation", Reason: "Fragmentation", Outcome: "withheld", RejectReason: "TargetUnderEvacuation", DispatchReason: "SubmissionJournalUncertain"}}}
	var compact, wide bytes.Buffer
	if err := c.Table().Write(&compact); err != nil {
		t.Fatal(err)
	}
	if err := c.WideTable().Write(&wide); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(compact.String(), "\n") {
		if printers.CellDisplayWidth(line) > 80 {
			t.Errorf("compact line too wide: %s", line)
		}
	}
	for _, literal := range []string{name + "/" + name + "/engine#2147483647", "TargetUnderEvacuation", "SubmissionJournalUncertain"} {
		if !strings.Contains(wide.String(), literal) {
			t.Errorf("wide lacks %s", literal)
		}
	}
	if strings.Contains(compact.String(), name+"/"+name) {
		t.Fatal("compact name not clipped")
	}
}
