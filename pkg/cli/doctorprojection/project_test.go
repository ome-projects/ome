package doctorprojection

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/cli/doctorcollection"
	"sigs.k8s.io/ome/pkg/cli/report"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestProjectVersionCandidatesNeverProveOperatorCompatibility(t *testing.T) {
	for _, tc := range []struct {
		client, image string
		want          r.DoctorComparison
	}{
		{"v1.2.3", "v1.2.9", r.DoctorWithinMinor},
		{"1.2.3", "v1.1.0", r.DoctorWithinMinor},
		{"v1.2.3", "v1.3.0", r.DoctorWithinMinor},
		{"v1.2.3", "v1.4.0", r.DoctorOutsideMinor},
		{"v1.2.3", "v2.2.3", r.DoctorMajorMismatch},
		{"unknown", "v1.2.3", r.DoctorComparisonUnknown},
		{"dev-20260915", "v1.2.3", r.DoctorComparisonUnknown},
		{"5069c980", "v1.2.3", r.DoctorComparisonUnknown},
		{"v1.2.3-alpha", "v1.2.3", r.DoctorComparisonUnknown},
		{"v1.2.3", "SECRET", r.DoctorComparisonUnknown},
		{"v1.0.0", "v1.18446744073709551615.0", r.DoctorOutsideMinor},
	} {
		t.Run(tc.client+"/"+tc.image, func(t *testing.T) {
			s := doctorcollection.Snapshot{Manager: r.DoctorManagerEvidence{ImageState: r.DoctorSelectedStableTag, ImageVersionCandidate: tc.image}}
			result := Project(s, tc.client, r.ClockFunc(func() time.Time { return time.Time{} }))
			if result.Content.Version.ImageTagComparison != tc.want {
				t.Fatalf("version=%+v want %s", result.Content.Version, tc.want)
			}
			if result.Content.Version.SkewState != r.DoctorUnverifiable || result.Content.Version.OperatorVersionState != r.DoctorUnverifiable || result.Content.HasViolations() {
				t.Fatalf("invented compatibility: %+v", result.Content)
			}
		})
	}
}

func TestProjectUsesOneClockAndSafeImmutableInputs(t *testing.T) {
	now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	calls := 0
	s := doctorcollection.Snapshot{
		Selection: doctorcollection.Selection{ContextName: "token=SECRETSECRET", WorkloadNamespace: "team-a", OMENamespace: "ome"},
		APIs:      []r.DoctorAPI{{ID: "ome.io/v1beta1/inferenceservices", Availability: r.DoctorNotDiscoverable}, {ID: "ome.io/v1beta1/inferencereplicas", Availability: r.DoctorUnavailable, Reason: r.DoctorForbidden}},
		Reads:     []r.DoctorRead{{ID: r.DoctorReadISVC, Outcome: r.DoctorNotRequested, Namespace: "team-a"}},
		Sources:   []r.SourceReference{{Kind: "APIResourceList", Name: "ome.io/v1beta1", Evidence: r.EvidenceUnavailable, UID: "SECRET"}},
		Warnings:  []r.Warning{{Code: r.WarningSourceUnavailable, Message: "SECRET"}},
		Features:  []r.DoctorFeature{{ID: "Traffic", Availability: r.DoctorAbsent, Freshness: "Current"}},
	}
	before, _ := json.Marshal(s)
	result := Project(s, "SECRET", r.ClockFunc(func() time.Time { calls++; return now }))
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("projection mutated snapshot")
	}
	if calls != 1 || result.CollectedAt != now || result.Sources[0].CollectedAt != now {
		t.Fatalf("clock=%d report=%+v", calls, result.Envelope)
	}
	if result.Content.Summary.State != "Violations" || result.Content.Summary.RequiredAPIViolations != 1 {
		t.Fatalf("summary=%+v", result.Content.Summary)
	}
	for _, format := range []report.Format{report.FormatJSON, report.FormatYAML, report.FormatTable} {
		var out bytes.Buffer
		if err := report.Write(&out, format, result); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), "SECRET") || strings.Contains(out.String(), `"generationFreshness": "Current"`) || strings.Contains(out.String(), "generationFreshness: Current") {
			t.Fatalf("%s leaked %s", format, &out)
		}
	}
	var wide bytes.Buffer
	if err := result.Content.WideTable().Write(&wide); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(wide.String(), "SECRET") {
		t.Fatal("wide leaked")
	}
	s.APIs[0].Availability = r.DoctorAvailable
	found := false
	for _, api := range result.Content.APIs {
		if api.ID != "ome.io/v1beta1/inferenceservices" {
			continue
		}
		found = true
		if !api.Required || api.Availability != r.DoctorNotDiscoverable {
			t.Fatal("report aliases input or changed the required API row")
		}
		break
	}
	if !found {
		t.Fatal("report omitted inferenceservices")
	}
}
