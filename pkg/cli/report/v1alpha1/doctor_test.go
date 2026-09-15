package v1alpha1

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/yaml"
)

func TestDoctorCanonicalPrivacyAndRepresentations(t *testing.T) {
	clock := ClockFunc(func() time.Time { return time.Date(2026, 9, 15, 12, 0, 0, 0, time.FixedZone("test", 3600)) })
	content := DoctorContent{
		Context:  DoctorContext{Name: "https://SECRET.example", WorkloadNamespace: "team-a", OMENamespace: "control"},
		APIs:     []DoctorAPI{{ID: "ome.io/v1beta1/inferenceservices", Availability: DoctorAvailable}, {ID: "SECRET", GroupVersion: "SECRET", Resource: "SECRET", Availability: "SECRET"}},
		Reads:    []DoctorRead{{ID: DoctorReadManager, Namespace: "control", Outcome: DoctorAvailable, Name: "SECRET", Method: "POST"}},
		Features: []DoctorFeature{{ID: "Traffic", Availability: DoctorPresent, Freshness: "Current"}},
		Version:  DoctorVersion{ClientVersion: "SECRET", ImageVersionCandidate: "SECRET", SkewState: "SECRET", OperatorVersionState: "SECRET"},
	}
	r := DoctorReport{Envelope: NewEnvelope("SECRET", Metadata{Name: "SECRET"}, content, clock)}
	r.Sources = []SourceReference{{Kind: "Deployment", Namespace: "control", Name: "ome-controller-manager", UID: "SECRET", ResourceVersion: "SECRET", Evidence: "SECRET"}, {Kind: "SECRET", Name: "SECRET"}}
	r.Warnings = []Warning{{Code: WarningSourceUnavailable, Message: "SECRET"}, {Code: "SECRET", Message: "SECRET"}}
	before, _ := json.Marshal(r)
	c := r.Canonical()
	after, _ := json.Marshal(r)
	if !bytes.Equal(before, after) {
		t.Fatal("Canonical mutated its caller")
	}
	if c.Kind != "DoctorReport" || c.Metadata.Name != "doctor" || c.CollectedAt.Hour() != 11 {
		t.Fatalf("wrong envelope: %+v", c.Envelope)
	}
	if len(c.Content.APIs) != 1 || c.Content.APIs[0].Resource != "inferenceservices" || !c.Content.APIs[0].Required {
		t.Fatalf("catalog not enforced: %+v", c.Content.APIs)
	}
	if c.Content.Version.SkewState != DoctorUnverifiable || c.Content.Features[0].Freshness != DoctorUnverifiable {
		t.Fatal("invented compatibility or freshness")
	}
	var machine [][]byte
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		var out bytes.Buffer
		var err error
		if format == "wide" {
			err = c.Content.WideTable().Write(&out)
		} else {
			err = report.Write(&out, report.Format(format), c)
		}
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), "SECRET") || strings.Contains(out.String(), "https://") || strings.Contains(out.String(), "POST") {
			t.Fatalf("%s leaked: %s", format, &out)
		}
		if format == "table" {
			for _, line := range strings.Split(out.String(), "\n") {
				if printers.CellDisplayWidth(line) > 80 {
					t.Fatalf("wide compact line: %s", line)
				}
			}
		}
		if format == "json" || format == "yaml" {
			data := out.Bytes()
			if format == "yaml" {
				data, err = yaml.YAMLToJSON(data)
				if err != nil {
					t.Fatal(err)
				}
			}
			machine = append(machine, data)
		}
	}
	var a, b any
	if err := json.Unmarshal(machine[0], &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(machine[1], &b); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatal("JSON/YAML differ")
	}
}

func TestDoctorViolationsAreOnlyProvedRequiredAbsence(t *testing.T) {
	for _, tc := range []struct {
		id    string
		state DoctorAvailability
		want  bool
	}{
		{"ome.io/v1beta1/inferenceservices", DoctorNotDiscoverable, true},
		{"ome.io/v1beta1/inferenceservices", DoctorUnavailable, false},
		{"ome.io/v1beta1/inferencereplicas", DoctorNotDiscoverable, false},
		{"ome.io/v1beta1/inferenceservices", DoctorAvailable, false},
	} {
		c := (DoctorContent{APIs: []DoctorAPI{{ID: DoctorAPIID(tc.id), Availability: tc.state}}}).Canonical()
		if c.HasViolations() != tc.want {
			t.Fatalf("%s/%s violated=%v", tc.id, tc.state, c.HasViolations())
		}
	}
}

func TestDoctorCanonicalArraysAndOrdering(t *testing.T) {
	r := (DoctorReport{Envelope: NewEnvelope("DoctorReport", Metadata{Name: "doctor"}, DoctorContent{}, ClockFunc(func() time.Time { return time.Time{} }))}).Canonical()
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"apis", "reads", "features", "sources", "warnings"} {
		if !strings.Contains(string(data), `"`+field+`":[]`) {
			t.Fatalf("%s not []: %s", field, data)
		}
	}
	left := DoctorContent{Reads: []DoctorRead{{ID: DoctorReadISVC, Namespace: "team-b", Name: "chat", Outcome: DoctorUnavailable, Reason: DoctorForbidden}, {ID: DoctorReadManager, Namespace: "ome", Outcome: DoctorAvailable}, {ID: DoctorReadISVC, Namespace: "team-a", Name: "chat", Outcome: DoctorUnavailable, Reason: DoctorUnauthorized}}}
	right := left
	right.Reads = []DoctorRead{left.Reads[2], left.Reads[0], left.Reads[1]}
	if !reflect.DeepEqual(left.Canonical(), right.Canonical()) {
		t.Fatal("ordering depends on input")
	}
	left.Reads[0].Namespace = "mutated"
	if right.Reads[1].Namespace != "team-b" {
		t.Fatal("test fixture alias")
	}
}

func TestDoctorUnavailableCompatibilityCannotSummarizeComplete(t *testing.T) {
	apis := DoctorAPICatalog()
	for i := range apis {
		apis[i].Availability = DoctorAvailable
	}
	c := DoctorContent{APIs: apis, Reads: []DoctorRead{{ID: DoctorReadManager, Namespace: "ome", Outcome: DoctorAvailable}}, Version: DoctorVersion{ClientVersion: "v1.2.3", ImageState: DoctorSelectedStableTag, ImageVersionCandidate: "v1.2.3"}}
	if got := c.Canonical().Summary; got.State != "Incomplete" || got.UnavailableEvidence == 0 {
		t.Fatalf("unknown running compatibility summarized as complete: %+v", got)
	}
}

func TestDoctorLegalCredentialIdentitiesArePubliclyRedacted(t *testing.T) {
	// DNS syntax permits these literal credential shapes. Output must not.
	name := "sk-aaaaaaaaaaaaaaaaaaaa"
	workload := "sk-bbbbbbbbbbbbbbbbbbbb"
	ome := "sk-cccccccccccccccccccc"
	contextName := "sk-dddddddddddddddddddd"
	r := DoctorReport{Envelope: NewEnvelope("DoctorReport", Metadata{Name: "doctor", Namespace: workload}, DoctorContent{
		Context: DoctorContext{Name: contextName, WorkloadNamespace: workload, OMENamespace: ome},
		Reads:   []DoctorRead{{ID: DoctorReadISVC, Namespace: workload, Name: name, Outcome: DoctorAvailable}, {ID: DoctorReadManager, Namespace: ome, Outcome: DoctorAvailable}},
	}, ClockFunc(func() time.Time { return time.Time{} }))}
	r.Sources = []SourceReference{{Kind: "InferenceService", Name: name, Namespace: workload, Evidence: EvidenceObserved}, {Kind: "Deployment", Name: "ome-controller-manager", Namespace: ome, Evidence: EvidenceObserved}}
	before, _ := json.Marshal(r)
	canonical := r.Canonical()
	after, _ := json.Marshal(r)
	if !bytes.Equal(before, after) {
		t.Fatal("Canonical changed private input identities")
	}
	if !reflect.DeepEqual(canonical, canonical.Canonical()) {
		t.Fatal("redacted representation is not idempotent")
	}
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			var out bytes.Buffer
			if format == "wide" {
				if err := canonical.Content.WideTable().Write(&out); err != nil {
					t.Fatal(err)
				}
			} else if err := report.Write(&out, report.Format(format), canonical); err != nil {
				t.Fatal(err)
			}
			for _, credential := range []string{name, workload, ome, contextName} {
				if strings.Contains(out.String(), credential) {
					t.Fatal("legal credential-shaped identity leaked")
				}
			}
			if !strings.Contains(out.String(), "[REDACTED]") {
				t.Fatal("missing bounded redaction marker")
			}
		})
	}
	if canonical.Metadata.Namespace != "[REDACTED]" || canonical.Content.Context.WorkloadNamespace != "[REDACTED]" || canonical.Content.Context.OMENamespace != "[REDACTED]" {
		t.Fatal("context/envelope namespace boundary not redacted")
	}
	for _, read := range canonical.Content.Reads {
		if read.Namespace != "[REDACTED]" {
			t.Fatal("read namespace boundary not redacted")
		}
		if read.ID == DoctorReadISVC && read.Name != "[REDACTED]" {
			t.Fatal("read name boundary not redacted")
		}
	}
	for _, source := range canonical.Sources {
		if source.Namespace != "[REDACTED]" {
			t.Fatal("source namespace boundary not redacted")
		}
		if source.Kind == "InferenceService" && source.Name != "[REDACTED]" {
			t.Fatal("source name boundary not redacted")
		}
	}
}

func TestDoctorPublicIdentityCanonicalPreservesSafeValuesAndMarkers(t *testing.T) {
	for _, value := range []string{"team-a", "chat.example", "[REDACTED]", "[OMITTED]"} {
		row := DoctorRead{ID: DoctorReadISVC, Namespace: "team-a", Name: value, Outcome: DoctorAvailable}
		c := (DoctorContent{Context: DoctorContext{Name: "[OMITTED]", WorkloadNamespace: "team-a", OMENamespace: "ome"}, Reads: []DoctorRead{row}}).Canonical()
		if c.Reads[0].Name != value || c.Context.WorkloadNamespace != "team-a" || c.Context.OMENamespace != "ome" || c.Context.Name != "[OMITTED]" {
			t.Fatalf("changed safe public identity %q", value)
		}
		if !reflect.DeepEqual(c, c.Canonical()) {
			t.Fatal("public identity canonicalization not idempotent")
		}
	}
}
