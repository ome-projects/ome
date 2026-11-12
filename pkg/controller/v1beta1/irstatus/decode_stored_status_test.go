package irstatus

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// storedStatusesPath is the file of complete ColumnarV2 statuses as writers
// stored them, one per form of the payload and row fixture, kept verbatim:
// the decoder is checked against bytes an API server already holds, not
// against what the current encoder would write for the same rows.
const storedStatusesPath = "testdata/stored_statuses.json"

// storedStatusForms are the payload forms the file must hold for every row
// fixture. "entries" is the form whose entries carry every row's readySince;
// "columns" the form whose readySince columns carry them, the one the writer
// selects today.
var storedStatusForms = []string{"entries", "columns"}

// currentStoredStatusForm is the form the current writer selects.
const currentStoredStatusForm = "columns"

var updateStoredStatuses = flag.Bool("update-stored-statuses", false, "re-record the current writer's form in testdata/stored_statuses.json; every other form is kept verbatim")

// storedStatusRows maps the row fixture a stored status was written for to
// its rows, the ones the decode must return.
var storedStatusRows = map[string]func() []v1beta1.OMENativeInstanceStatus{
	"readySince/50": func() []v1beta1.OMENativeInstanceStatus { return readySinceRows(50) },
	"fullFeature":   fullFeatureRows,
}

// loadStoredStatuses reads the file: form/fixture name to the stored status
// JSON.
func loadStoredStatuses(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	data, err := os.ReadFile(filepath.FromSlash(storedStatusesPath))
	if err != nil {
		t.Fatalf("read %s: %v", storedStatusesPath, err)
	}
	stored := map[string]json.RawMessage{}
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatalf("parse %s: %v", storedStatusesPath, err)
	}
	return stored
}

// TestDecoderReadsEveryStoredStatusForm decodes each stored status, through
// DecodeStatus and through the Decoder a controller reads a fetched object
// with, and requires the row fixture it was written for, readySince
// included; the file must hold every form for every fixture.
func TestDecoderReadsEveryStoredStatusForm(t *testing.T) {
	t.Parallel()
	stored := loadStoredStatuses(t)
	for _, form := range storedStatusForms {
		for fixture := range storedStatusRows {
			if _, ok := stored[form+"/"+fixture]; !ok {
				t.Errorf("%s holds no %q status for %s", storedStatusesPath, form, fixture)
			}
		}
	}
	names := make([]string, 0, len(stored))
	for name := range stored {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		form, fixture, _ := strings.Cut(name, "/")
		rowsOf, ok := storedStatusRows[fixture]
		if !ok {
			t.Fatalf("%s names the unknown row fixture %q (form %s)", name, fixture, form)
		}
		raw := stored[name]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			want := rowsOf()
			ClearPodDerivedObservations(want)

			var status v1beta1.InferenceReplicaStatus
			if err := json.Unmarshal(raw, &status); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			rows, encoding, err := DecodeStatus(&status, testLimit)
			if err != nil || encoding != EncodingColumnarV2 {
				t.Fatalf("DecodeStatus() = (%q, %v), want ColumnarV2", encoding, err)
			}
			assertRowsEqual(t, want, rows)

			var ir v1beta1.InferenceReplica
			if err := json.Unmarshal(raw, &ir.Status); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if _, err := NewDecoder(testLimit).Decode(&ir); err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if ir.Status.InstanceStatusColumns != nil || ir.Status.InstanceStatusEncoding != nil {
				t.Fatal("the decoded object still carries the columns")
			}
			assertRowsEqual(t, want, ir.Status.InstanceStatuses)
		})
	}
}

// TestStoredStatusesHoldTheCurrentWriterForm requires the file's entry for
// the current form of every row fixture to be the status the writer selects
// today, so the file follows the encoder; the other forms are never
// regenerated, which is what makes them worth decoding.
func TestStoredStatusesHoldTheCurrentWriterForm(t *testing.T) {
	stored := loadStoredStatuses(t)
	stale := false
	for fixture, rowsOf := range storedStatusRows {
		selection, err := SelectCandidate(logicalStatus(rowsOf()), testLimit)
		if err != nil || selection.ColumnarV2 == nil {
			t.Fatalf("%s: SelectCandidate() = %+v, %v", fixture, selection, err)
		}
		want := mustJSON(t, selection.ColumnarV2.Status)
		name := currentStoredStatusForm + "/" + fixture
		var got []byte
		if raw, ok := stored[name]; ok {
			got = mustJSON(t, json.RawMessage(raw))
		}
		if string(got) != string(want) {
			stale = true
			t.Errorf("%s: %q is not what the writer selects today", storedStatusesPath, name)
			stored[name] = want
		}
	}
	if !stale || !*updateStoredStatuses {
		return
	}
	data, err := json.MarshalIndent(stored, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.FromSlash(storedStatusesPath), append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("re-recorded the %q entries of %s", currentStoredStatusForm, storedStatusesPath)
}
