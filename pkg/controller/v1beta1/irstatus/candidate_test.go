package irstatus

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestSelectCandidatePrefersStrictlySmallerColumns(t *testing.T) {
	t.Parallel()
	logical := logicalStatus(uniformRows(2000))
	logical.Conditions = []metav1.Condition{{Type: "Available", Status: metav1.ConditionTrue, Reason: "Ready", LastTransitionTime: fixtureTime}}
	logical.RetryBlocks = []v1beta1.RetryBlock{{TargetRevision: fixtureOtherRevision, State: v1beta1.RetryBlockHeld, AttemptsStarted: 3}}

	selection, err := SelectCandidate(logical, testLimit)
	if err != nil {
		t.Fatalf("SelectCandidate() error = %v", err)
	}
	if selection.Selected != EncodingColumnarV2 || selection.ColumnarV2 == nil || selection.IneligibleReason != "" {
		t.Fatalf("selection = %+v, want ColumnarV2", selection)
	}
	if selection.ColumnarV2.Size >= selection.DenseV1.Size {
		t.Fatalf("columnar %d B is not smaller than dense %d B", selection.ColumnarV2.Size, selection.DenseV1.Size)
	}
	if selection.SelectedCandidate() != selection.ColumnarV2 {
		t.Fatal("SelectedCandidate() did not return the columnar candidate")
	}

	dense, columnar := selection.DenseV1.Status, selection.ColumnarV2.Status
	if dense.InstanceStatusEncoding != nil || dense.InstanceStatusColumns != nil || len(dense.InstanceStatuses) != 2000 {
		t.Fatal("dense candidate does not carry exactly the dense representation")
	}
	if columnar.InstanceStatusEncoding == nil || *columnar.InstanceStatusEncoding != v1beta1.InstanceStatusEncodingColumnarV2 ||
		columnar.InstanceStatusColumns == nil || columnar.InstanceStatuses != nil {
		t.Fatal("columnar candidate does not carry exactly the columnar representation")
	}
	for _, candidate := range []*v1beta1.InferenceReplicaStatus{dense, columnar} {
		if candidate.Replicas != logical.Replicas || candidate.CurrentRevision != logical.CurrentRevision ||
			!equality.Semantic.DeepEqual(candidate.Conditions, logical.Conditions) || !equality.Semantic.DeepEqual(candidate.RetryBlocks, logical.RetryBlocks) {
			t.Fatal("candidate lost status fields outside the per-Instance representation")
		}
	}
	if got, want := len(mustJSON(t, dense)), selection.DenseV1.Size; got != want {
		t.Fatalf("dense Size = %d, serialized %d", want, got)
	}
	if got, want := len(mustJSON(t, columnar)), selection.ColumnarV2.Size; got != want {
		t.Fatalf("columnar Size = %d, serialized %d", want, got)
	}
	assertRowsEqual(t, dense.InstanceStatuses, mustDecode(t, columnar.InstanceStatusColumns, testLimit))
}

func TestSelectCandidateKeepsDenseWhenColumnsAreLarger(t *testing.T) {
	t.Parallel()
	rows := uniformRows(2)
	for i := range rows {
		rows[i].RunningRevision = fmt.Sprintf("example-engine-%08d", i)
		rows[i].Incarnation = int64(i + 1)
		rows[i].PodCount = int32(i + 1)
	}
	selection, err := SelectCandidate(logicalStatus(rows), testLimit)
	if err != nil {
		t.Fatalf("SelectCandidate() error = %v", err)
	}
	if selection.ColumnarV2 == nil {
		t.Fatalf("eligible rows produced no columnar candidate: %+v", selection)
	}
	if selection.ColumnarV2.Size <= selection.DenseV1.Size {
		t.Fatalf("fixture does not make columns larger: columnar %d B, dense %d B", selection.ColumnarV2.Size, selection.DenseV1.Size)
	}
	if selection.Selected != EncodingDenseV1 || selection.SelectedCandidate() != &selection.DenseV1 {
		t.Fatalf("selection = %q, want DenseV1 for larger columns", selection.Selected)
	}
}

func TestSelectEncodingTieKeepsDense(t *testing.T) {
	t.Parallel()
	if got := selectEncoding(100, 100); got != EncodingDenseV1 {
		t.Fatalf("tie selected %q", got)
	}
	if got := selectEncoding(100, 99); got != EncodingColumnarV2 {
		t.Fatalf("strictly smaller columns selected %q", got)
	}
	if got := selectEncoding(99, 100); got != EncodingDenseV1 {
		t.Fatalf("larger columns selected %q", got)
	}
}

// The strict-smaller rule is exercised end to end on every exact tie the
// search below constructs by varying row count, revision sharing, and
// revision length. A shared revision grows the dense candidate by one byte
// per row and the columnar candidate by one byte, so sweeping the length
// crosses the tie point; each tie must keep DenseV1.
func TestSelectCandidateExactTiesKeepDense(t *testing.T) {
	t.Parallel()
	ties := 0
	for rowCount := 1; rowCount <= 6; rowCount++ {
		for shared := 0; shared <= 1; shared++ {
			for length := 0; length <= 200; length++ {
				rows := uniformRows(rowCount)
				for i := range rows {
					revision := strings.Repeat("r", length)
					if shared == 0 {
						revision = fmt.Sprintf("%s%d", revision, i)
					}
					rows[i].RunningRevision = revision
					rows[i].Admitted = i%2 == 0
				}
				selection, err := SelectCandidate(logicalStatus(rows), testLimit)
				if err != nil {
					t.Fatalf("SelectCandidate() error = %v", err)
				}
				if selection.ColumnarV2 != nil && selection.ColumnarV2.Size == selection.DenseV1.Size {
					ties++
					if selection.Selected != EncodingDenseV1 {
						t.Fatalf("tie at %d rows, shared=%d, length=%d selected %q", rowCount, shared, length, selection.Selected)
					}
				}
			}
		}
	}
	t.Logf("exact ties exercised end to end: %d", ties)
	if ties == 0 {
		t.Fatal("the search constructed no exact tie; the strict-smaller rule was not exercised end to end")
	}
}

func TestSelectCandidateReportsIneligibleRows(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		status     *v1beta1.InferenceReplicaStatus
		limit      uint64
		wantReason ErrorReason
	}{
		{name: "zero rows", status: logicalStatus(nil), limit: testLimit, wantReason: ErrorReasonValueDomain},
		{name: "above limit", status: logicalStatus(uniformRows(3)), limit: 2, wantReason: ErrorReasonCardinalityLimit},
		{name: "negative index", status: logicalStatus([]v1beta1.OMENativeInstanceStatus{{Index: -1, Phase: v1beta1.OMENativeInstanceReady}}), limit: testLimit, wantReason: ErrorReasonValueDomain},
		{name: "unsupported phase", status: logicalStatus([]v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: "Odd"}}), limit: testLimit, wantReason: ErrorReasonValueDomain},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			selection, err := SelectCandidate(test.status, test.limit)
			if err != nil {
				t.Fatalf("SelectCandidate() error = %v", err)
			}
			if selection.ColumnarV2 != nil || selection.Selected != EncodingDenseV1 || selection.IneligibleReason != test.wantReason {
				t.Fatalf("selection = %+v, want DenseV1 with reason %q", selection, test.wantReason)
			}
			if selection.SelectedCandidate().Status.InstanceStatusEncoding != nil {
				t.Fatal("dense candidate carries a marker")
			}
			if got, want := len(selection.DenseV1.Status.InstanceStatuses), len(test.status.InstanceStatuses); got != want {
				t.Fatalf("dense candidate has %d rows, want %d", got, want)
			}
		})
	}
}

func TestSelectCandidateRejectsNonLogicalInput(t *testing.T) {
	t.Parallel()
	marker := v1beta1.InstanceStatusEncodingColumnarV2
	columns := mustEncode(t, uniformRows(2), testLimit)
	for name, status := range map[string]*v1beta1.InferenceReplicaStatus{
		"nil":                nil,
		"carries marker":     {InstanceStatusEncoding: &marker},
		"carries columns":    {InstanceStatusColumns: columns},
		"complete columnar":  columnarStatus(columns),
		"marker plus dense":  {InstanceStatusEncoding: &marker, InstanceStatuses: uniformRows(2)},
		"columns plus dense": {InstanceStatusColumns: columns, InstanceStatuses: uniformRows(2)},
	} {
		selection, err := SelectCandidate(status, testLimit)
		if selection != nil {
			t.Fatalf("%s: returned a selection", name)
		}
		assertCodecReason(t, err, ErrorReasonRepresentationUnion)
	}
}

func TestSelectCandidateClearsPodDerivedFieldsOnBothCandidatesOnly(t *testing.T) {
	t.Parallel()
	rows := uniformRows(40)
	for i := range rows {
		rows[i].ReadyPodCount = 7
		rows[i].ScheduledPodCount = 3
		rows[i].NodesOccupied = []string{"node-a", "node-b"}
	}
	logical := logicalStatus(rows)
	before := mustJSON(t, logical)

	selection, err := SelectCandidate(logical, testLimit)
	if err != nil {
		t.Fatalf("SelectCandidate() error = %v", err)
	}
	if !bytes.Equal(before, mustJSON(t, logical)) {
		t.Fatal("SelectCandidate mutated the logical input")
	}
	for _, row := range selection.DenseV1.Status.InstanceStatuses {
		if row.ReadyPodCount != 0 || row.ScheduledPodCount != 0 || row.NodesOccupied != nil {
			t.Fatalf("dense candidate kept Pod-derived fields: %+v", row)
		}
	}
	decoded := mustDecode(t, selection.ColumnarV2.Status.InstanceStatusColumns, testLimit)
	assertRowsEqual(t, selection.DenseV1.Status.InstanceStatuses, decoded)
	if strings.Contains(string(mustJSON(t, selection.ColumnarV2.Status)), "node-a") {
		t.Fatal("columnar candidate serialized a Pod-derived field")
	}
}

func TestDenseCandidateNormalizesInPlaceWithoutCopying(t *testing.T) {
	t.Parallel()
	rows := uniformRows(40)
	for i := range rows {
		rows[i].ReadyPodCount = 7
		rows[i].ScheduledPodCount = 3
		rows[i].NodesOccupied = []string{"node-a", "node-b"}
	}
	logical := logicalStatus(rows)
	logical.Conditions = []metav1.Condition{{Type: "Available", Status: metav1.ConditionTrue, Reason: "Ready", LastTransitionTime: fixtureTime}}

	candidate, err := DenseCandidate(logical)
	if err != nil {
		t.Fatalf("DenseCandidate() error = %v", err)
	}
	if candidate.Encoding != EncodingDenseV1 || candidate.Status != logical {
		t.Fatalf("candidate = %+v, want the DenseV1 candidate built on the input itself", candidate)
	}
	if logical.InstanceStatusEncoding != nil || logical.InstanceStatusColumns != nil || len(logical.InstanceStatuses) != 40 {
		t.Fatal("dense candidate does not carry exactly the dense representation")
	}
	for _, row := range logical.InstanceStatuses {
		if row.ReadyPodCount != 0 || row.ScheduledPodCount != 0 || row.NodesOccupied != nil {
			t.Fatalf("dense candidate kept Pod-derived fields: %+v", row)
		}
	}
	if got, want := len(mustJSON(t, logical)), candidate.Size; got != want {
		t.Fatalf("Size = %d, serialized %d", want, got)
	}
	if !equality.Semantic.DeepEqual(logical.Conditions, []metav1.Condition{{Type: "Available", Status: metav1.ConditionTrue, Reason: "Ready", LastTransitionTime: fixtureTime}}) {
		t.Fatal("candidate lost status fields outside the per-Instance representation")
	}

	// The selection's dense candidate is the same normalization applied to a
	// copy, so both entry points measure identical bytes for equal input.
	selection, err := SelectCandidate(logical, testLimit)
	if err != nil {
		t.Fatalf("SelectCandidate() error = %v", err)
	}
	if selection.DenseV1.Size != candidate.Size {
		t.Fatalf("dense sizes differ: DenseCandidate %d, SelectCandidate %d", candidate.Size, selection.DenseV1.Size)
	}
}

func TestDenseCandidateRejectsNonLogicalInput(t *testing.T) {
	t.Parallel()
	encoding := v1beta1.InstanceStatusEncodingColumnarV2
	columns := mustEncode(t, uniformRows(3), testLimit)
	for name, status := range map[string]*v1beta1.InferenceReplicaStatus{
		"nil":            nil,
		"marker":         {InstanceStatusEncoding: &encoding},
		"columns":        {InstanceStatusColumns: columns},
		"marker+columns": {InstanceStatusEncoding: &encoding, InstanceStatusColumns: columns},
	} {
		var before []byte
		if status != nil {
			before = mustJSON(t, status)
		}
		candidate, err := DenseCandidate(status)
		if candidate != nil {
			t.Fatalf("%s: returned a candidate", name)
		}
		assertCodecReason(t, err, ErrorReasonRepresentationUnion)
		if status != nil && !bytes.Equal(before, mustJSON(t, status)) {
			t.Fatalf("%s: a rejected status was modified", name)
		}
	}
}

func TestSelectCandidateIsDeterministic(t *testing.T) {
	t.Parallel()
	for name, rows := range map[string][]v1beta1.OMENativeInstanceStatus{
		"representative": representativeRows(700),
		"mass failure":   massFailureRows(),
		"full feature":   fullFeatureRows(),
	} {
		first, err := SelectCandidate(logicalStatus(rows), testLimit)
		if err != nil {
			t.Fatalf("%s: SelectCandidate() error = %v", name, err)
		}
		firstJSON := mustJSON(t, first.SelectedCandidate().Status)
		for i := 0; i < 8; i++ {
			again, err := SelectCandidate(logicalStatus(rows), testLimit)
			if err != nil {
				t.Fatalf("%s: SelectCandidate() error = %v", name, err)
			}
			if again.Selected != first.Selected || again.DenseV1.Size != first.DenseV1.Size || again.ColumnarV2.Size != first.ColumnarV2.Size {
				t.Fatalf("%s: selection changed between runs: %+v vs %+v", name, first, again)
			}
			if !bytes.Equal(firstJSON, mustJSON(t, again.SelectedCandidate().Status)) {
				t.Fatalf("%s: selected bytes changed between runs", name)
			}
		}
	}
}
