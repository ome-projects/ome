package crdskew

import (
	"strings"
	"testing"
)

func TestReportSetReportsAChangedPicture(t *testing.T) {
	var report Report
	current := Finding{CRD: "a.example"}
	lagging := Finding{CRD: "a.example", MissingFields: []string{"status.x"}}
	laggingMore := Finding{CRD: "a.example", MissingFields: []string{"status.x", "status.y"}}

	if report.Set(current) {
		t.Fatal("a first current finding counts as a change")
	}
	if !report.Set(lagging) {
		t.Fatal("a CRD that starts lagging is not a change")
	}
	if report.Set(lagging) {
		t.Fatal("the same picture read again counts as a change")
	}
	if !report.Set(laggingMore) {
		t.Fatal("a different set of missing fields is not a change")
	}
	if !report.Set(current) {
		t.Fatal("a CRD that is current again is not a change")
	}
	if got := report.Lagging(); len(got) != 0 {
		t.Fatalf("lagging = %+v after the CRD is current", got)
	}

	var fresh Report
	if !fresh.Set(lagging) {
		t.Fatal("a first lagging finding is not a change")
	}
	if got := fresh.Lagging(); len(got) != 1 || got[0].CRD != "a.example" {
		t.Fatalf("lagging = %+v", got)
	}
	fresh.Forget("a.example")
	if got := fresh.Lagging(); len(got) != 0 {
		t.Fatalf("lagging = %+v after the CRD was forgotten", got)
	}
}

func TestReportMessageListsEveryLaggingCRDInOrder(t *testing.T) {
	var report Report
	report.Set(Finding{CRD: "b.example", MissingFields: []string{"spec.q"}})
	report.Set(Finding{CRD: "a.example", MissingFields: []string{"status.p"}})
	report.Set(Finding{CRD: "c.example"})

	message := report.Message()
	a, b := strings.Index(message, "a.example"), strings.Index(message, "b.example")
	if a < 0 || b < 0 || a > b {
		t.Fatalf("message %q does not name the lagging CRDs in order", message)
	}
	if strings.Contains(message, "c.example") {
		t.Fatalf("message %q names a current CRD", message)
	}
	for _, part := range []string{"status.p", "spec.q", "drops"} {
		if !strings.Contains(message, part) {
			t.Fatalf("message %q lacks %q", message, part)
		}
	}
}

func TestReportNilReadsAsNothingLagging(t *testing.T) {
	var report *Report
	if report.Lagging() != nil || report.Message() != "" {
		t.Fatal("a nil report reads as lagging")
	}
	report.Forget("a.example")
}

func TestFindingSentenceForACurrentCRD(t *testing.T) {
	sentence := Finding{CRD: "a.example"}.Sentence()
	if !strings.Contains(sentence, "a.example") || strings.Contains(sentence, "lacks") {
		t.Fatalf("sentence %q", sentence)
	}
}
