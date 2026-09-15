package placementprojection

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/cli/report"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestOversizedUnrelatedConditionFieldsPreserveKnownTypeIndependence(t *testing.T) {
	for _, mutate := range []func(*metav1.Condition){
		func(c *metav1.Condition) { c.Message = strings.Repeat("x", 4097) },
		func(c *metav1.Condition) { c.Reason = strings.Repeat("x", 1025) },
		func(c *metav1.Condition) { c.Type = strings.Repeat("x", 254) },
	} {
		s := fixture(t)
		bad := metav1.Condition{Type: "UnsupportedEvidence", Status: metav1.ConditionTrue, Reason: "Reported", LastTransitionTime: metav1.NewTime(fixtureClock.Now().Add(-time.Hour))}
		mutate(&bad)
		s.WorkloadClusters[0].Status.Conditions = append(s.WorkloadClusters[0].Status.Conditions, bad)
		r, err := ProjectExplain(s, fixtureClock)
		if err != nil || r.Content.Clusters[0].ReportedReady.Status != "True" || r.Content.Clusters[0].ReportedReady.Source.Freshness != "Current" || r.Content.Clusters[0].ConditionPreview.State != "MalformedPayload" || len(r.Content.Issues) != 1 {
			t.Fatalf("oversized unrelated record erased known evidence: %+v err=%v", r.Content, err)
		}
		bad.Type = "Ready"
		bad.Message = strings.Repeat("x", 4097)
		s.WorkloadClusters[0].Status.Conditions = append(s.WorkloadClusters[0].Status.Conditions[:1], bad)
		r, _ = ProjectExplain(s, fixtureClock)
		if r.Content.Clusters[0].ReportedReady.Source.Freshness != "Invalid" {
			t.Fatal("oversized known type falsely became healthy")
		}
	}
	s := fixture(t)
	s.TrafficMap.Status.Conditions[0].Type = strings.Repeat("x", 254)
	r, _ := ProjectEndpoint(s, fixtureClock)
	if r.Content.ConditionPreview.State != "MalformedPayload" || r.Content.Routing.Routable.Status != "Unknown" || r.Content.Routing.Routable.Reason != "NotRecorded" || len(r.Content.Issues) != 1 || len(r.Content.Entries) != 2 {
		t.Fatal("oversized unknown identity fabricated a known condition or hid inspection")
	}
}

func TestLateNestedFutureProbeIsInspectedBeforeOuterRouteCap(t *testing.T) {
	s := fixture(t)
	base := s.TrafficMap.Spec.Entries[0]
	s.TrafficMap.Spec.Entries = nil
	for i := 0; i < 65; i++ {
		e := *base.DeepCopy()
		e.Cluster = fmt.Sprintf("route-%03d", i)
		s.TrafficMap.Spec.Entries = append(s.TrafficMap.Spec.Entries, e)
	}
	s.TrafficMap.Spec.Entries[64].Probe.LastProbeTime = &metav1.Time{Time: fixtureClock.Now().Add(time.Hour)}
	r, err := ProjectEndpoint(s, fixtureClock)
	if err != nil || len(r.Content.Entries) != 64 || r.Content.Routing.EntryPreview.State != "Validated" || !r.Content.Routing.EntryPreview.Truncated || len(r.Content.Issues) != 1 || r.Content.Issues[0] != (v.PlacementIssue{Group: "RecordedProbes", Code: "FutureObservationTime", Count: 1}) {
		t.Fatalf("late probe uninspected or independent routes discarded: %+v err=%v", r.Content, err)
	}
	if r.Content.Routing.ProbePreview.State != "Unavailable" || r.Content.Routing.ProbePreview.Total != 65 || r.Content.Routing.ProbePreview.Kept != 64 || !r.Content.Routing.ProbePreview.Truncated {
		t.Fatalf("nested probe inspection state/window lost: %+v", r.Content.Routing.ProbePreview)
	}
	for _, table := range []report.Table{r.Content.Table(), r.Content.WideTable()} {
		var out bytes.Buffer
		table.Write(&out)
		if !strings.Contains(out.String(), "RecordedProbes: FutureObservationTime (1)") {
			t.Fatalf("late probe issue hidden: %s", &out)
		}
	}
	s.TrafficMap.Spec.Entries[0].Probe.LastProbeTime = &metav1.Time{Time: fixtureClock.Now().Add(time.Hour)}
	r, _ = ProjectEndpoint(s, fixtureClock)
	e := r.Content.Entries[0]
	if e.Probe.Result != "Invalid" || e.Address.State != "Present" || e.Weight != 1 || e.Healthy || e.Capacity == nil || r.Content.Issues[0].Count != 2 {
		t.Fatalf("nested probe invalidity erased independent facts/count: %+v", r.Content)
	}
}

func TestRoutableFreshnessIsSeparateInEveryOutputFormat(t *testing.T) {
	for _, tc := range []struct {
		isvc, condition   int64
		routing, routable string
	}{{7, 3, "Current", "Stale"}, {6, 4, "Stale", "Current"}} {
		s := fixture(t)
		s.TrafficMap.Spec.ObservedISVCGeneration = tc.isvc
		s.TrafficMap.Status.Conditions[0].ObservedGeneration = tc.condition
		r, err := ProjectEndpoint(s, fixtureClock)
		if err != nil || string(r.Content.Routing.Source.Freshness) != tc.routing || string(r.Content.Routing.Routable.Source.Freshness) != tc.routable {
			t.Fatalf("literal source-specific fixture invalid: %+v err=%v", r.Content.Routing, err)
		}
		for _, table := range []report.Table{r.Content.Table(), r.Content.WideTable()} {
			var out bytes.Buffer
			table.Write(&out)
			if !strings.Contains(out.String(), "Routable freshness") || !strings.Contains(out.String(), tc.routable) {
				t.Fatalf("human condition freshness hidden: %s", &out)
			}
			for _, line := range strings.Split(out.String(), "\n") {
				if len(line) > 80 {
					t.Fatal("overwidth condition freshness")
				}
			}
		}
		for _, format := range []report.Format{report.FormatJSON, report.FormatYAML} {
			var out bytes.Buffer
			if err := report.Write(&out, format, r); err != nil || !strings.Contains(out.String(), "Stale") || !strings.Contains(out.String(), "Current") {
				t.Fatalf("machine source independence lost: %s err=%v", &out, err)
			}
		}
	}
}

func TestQuantityFormattingDoesNotMutateSnapshot(t *testing.T) {
	s := fixture(t)
	if err := json.Unmarshal([]byte(strings.Replace(mapFixture, `"factor":"1"`, `"factor":"1000m"`, 1)), s.TrafficMap); err != nil {
		t.Fatal(err)
	}
	before := s.TrafficMap.DeepCopy()
	r, err := ProjectEndpoint(s, fixtureClock)
	if err != nil || !reflect.DeepEqual(before, s.TrafficMap) {
		t.Fatal("quantity formatting mutated the input snapshot")
	}
	if r.Content.Entries[0].Capacity.Factor != "1" {
		t.Fatal("valid literal milli factor lost canonical value")
	}
}
