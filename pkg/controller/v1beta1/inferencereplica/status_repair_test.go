package inferencereplica

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

// repairFixture is a stored IR with a Ready condition and revision pointers
// that the repair must preserve, plus a raw live read of it.
func repairFixture(t *testing.T, stored *v1beta1.InferenceReplica) (client.Client, *v1beta1.InferenceReplica) {
	t.Helper()
	stored.Status.Conditions = []metav1.Condition{{Type: InferenceReplicaConditionReady, Status: metav1.ConditionTrue, Reason: ReasonAllInstancesReady, Message: "ready", LastTransitionTime: selectionFixtureTime}}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(stored).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
	live := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(stored), live); err != nil {
		t.Fatalf("live read: %v", err)
	}
	return c, live
}

func replacementOf(rows []v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplicaStatus {
	return &v1beta1.InferenceReplicaStatus{InstanceStatuses: rows}
}

func TestRepairInstanceStatusDryRunWritesNothing(t *testing.T) {
	ctx := context.Background()
	c, live := repairFixture(t, fixtureIRWithRows("model-engine", uniformFixtureRows(8)))
	before := live.DeepCopy()
	storedBytes, _ := json.Marshal(&live.Status)

	outcome, err := RepairInstanceStatus(ctx, nil, irstatus.EncodingColumnarV2, testColumnarBound, InstanceStatusRepair{
		Live: live, ResourceVersion: live.ResourceVersion, Replacement: replacementOf(uniformFixtureRows(64)),
	}, false)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if outcome.Applied || outcome.ResourceVersion != live.ResourceVersion {
		t.Fatalf("dry run must not apply: %+v", outcome)
	}
	if outcome.StoredEncoding != string(irstatus.EncodingDenseV1) || outcome.StoredRows != 8 || outcome.StoredBytes != len(storedBytes) {
		t.Fatalf("dry run must describe the stored representation: %+v", outcome)
	}
	if outcome.ReplacementRows != 64 || outcome.SelectedEncoding != irstatus.EncodingColumnarV2 || outcome.SelectedBytes <= 0 || outcome.SelectedBytes >= outcome.StoredBytes {
		t.Fatalf("dry run must report the selected candidate and its size: %+v", outcome)
	}
	after := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(live), after); err != nil {
		t.Fatalf("re-read: %v", err)
	}
	if after.ResourceVersion != before.ResourceVersion || !equality.Semantic.DeepEqual(after.Status, before.Status) {
		t.Fatal("a dry run must not change the stored object")
	}
	if !equality.Semantic.DeepEqual(live.Status, before.Status) {
		t.Fatal("a dry run must not modify the caller's live object")
	}
}

func TestRepairInstanceStatusRefusesStaleResourceVersion(t *testing.T) {
	ctx := context.Background()
	c, live := repairFixture(t, fixtureIRWithRows("model-engine", uniformFixtureRows(8)))
	before := live.DeepCopy()

	// Another writer lands after the live read.
	moved := live.DeepCopy()
	moved.Status.ObservedGeneration = 5
	if err := c.Status().Update(ctx, moved); err != nil {
		t.Fatalf("concurrent update: %v", err)
	}

	_, err := RepairInstanceStatus(ctx, c, irstatus.EncodingDenseV1, 0, InstanceStatusRepair{
		Live: live, ResourceVersion: live.ResourceVersion, Replacement: replacementOf(uniformFixtureRows(4)),
	}, true)
	if !errors.Is(err, ErrInstanceStatusRepairRefused) || !apierrors.IsConflict(err) {
		t.Fatalf("a stale resourceVersion must be refused as a conflict, got %v", err)
	}
	after := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(live), after); err != nil {
		t.Fatalf("re-read: %v", err)
	}
	if after.ResourceVersion != moved.ResourceVersion || after.Status.ObservedGeneration != 5 || len(after.Status.InstanceStatuses) != len(before.Status.InstanceStatuses) {
		t.Fatalf("a refused repair must leave the concurrent write in place: %+v", after.Status)
	}
}

func TestRepairInstanceStatusRefusesInvalidReplacements(t *testing.T) {
	ctx := context.Background()
	_, live := repairFixture(t, fixtureIRWithRows("model-engine", uniformFixtureRows(8)))
	marker := v1beta1.InstanceStatusEncodingColumnarV2
	columns, err := irstatus.EncodeColumns(uniformFixtureRows(4), testColumnarBound)
	if err != nil {
		t.Fatalf("encode columns: %v", err)
	}
	withReplicas := replacementOf(uniformFixtureRows(4))
	withReplicas.Replicas = 4

	cases := map[string]struct {
		replacement *v1beta1.InferenceReplicaStatus
		bound       uint64
		want        string
	}{
		"nil":                    {replacement: nil, want: "a replacement is required"},
		"marker without columns": {replacement: &v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &marker}, want: "not a valid per-Instance representation"},
		"columns without marker": {replacement: &v1beta1.InferenceReplicaStatus{InstanceStatusColumns: columns}, want: "not a valid per-Instance representation"},
		"valid ColumnarV2":       {replacement: &v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &marker, InstanceStatusColumns: columns}, bound: testColumnarBound, want: "DenseV1 row set only"},
		"marker next to rows":    {replacement: &v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &marker, InstanceStatusColumns: columns, InstanceStatuses: uniformFixtureRows(4)}, bound: testColumnarBound, want: "not a valid per-Instance representation"},
		"no rows":                {replacement: replacementOf(nil), want: "carries no rows"},
		"above the bound":        {replacement: replacementOf(uniformFixtureRows(9)), bound: 8, want: "above the configured maxDecodedInstances 8"},
		"unrelated status field": {replacement: withReplicas, want: "must carry only instanceStatuses"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := RepairInstanceStatus(ctx, nil, irstatus.EncodingDenseV1, tc.bound, InstanceStatusRepair{
				Live: live, ResourceVersion: live.ResourceVersion, Replacement: tc.replacement,
			}, false)
			if !errors.Is(err, ErrInstanceStatusRepairRefused) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want a refusal containing %q", err, tc.want)
			}
		})
	}

	if _, err := RepairInstanceStatus(ctx, nil, irstatus.EncodingDenseV1, 0, InstanceStatusRepair{Live: live, Replacement: replacementOf(uniformFixtureRows(4))}, false); !errors.Is(err, ErrInstanceStatusRepairRefused) {
		t.Fatalf("a missing resourceVersion must be refused, got %v", err)
	}
	if _, err := RepairInstanceStatus(ctx, nil, irstatus.EncodingDenseV1, 0, InstanceStatusRepair{ResourceVersion: "1", Replacement: replacementOf(uniformFixtureRows(4))}, false); !errors.Is(err, ErrInstanceStatusRepairRefused) {
		t.Fatalf("a missing live object must be refused, got %v", err)
	}
	if _, err := RepairInstanceStatus(ctx, nil, irstatus.Encoding(""), 0, InstanceStatusRepair{Live: live, ResourceVersion: live.ResourceVersion, Replacement: replacementOf(uniformFixtureRows(4))}, false); !errors.Is(err, ErrInstanceStatusTargetUnset) {
		t.Fatalf("an unset target must be refused, got %v", err)
	}
}

// TestRepairInstanceStatusAppliesUnderEachTarget pins the write: the rows are
// replaced, every other status field is preserved from the live object, the
// representation is the one the target selects, and the object decodes back
// to the replacement rows.
func TestRepairInstanceStatusAppliesUnderEachTarget(t *testing.T) {
	cases := map[string]struct {
		target irstatus.Encoding
		rows   []v1beta1.OMENativeInstanceStatus
		want   irstatus.Encoding
	}{
		"DenseV1 target":                     {target: irstatus.EncodingDenseV1, rows: uniformFixtureRows(64), want: irstatus.EncodingDenseV1},
		"ColumnarV2 target, columns smaller": {target: irstatus.EncodingColumnarV2, rows: uniformFixtureRows(64), want: irstatus.EncodingColumnarV2},
		"ColumnarV2 target, dense fallback":  {target: irstatus.EncodingColumnarV2, rows: decodeFixtureIR().Status.InstanceStatuses, want: irstatus.EncodingDenseV1},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			c, live := repairFixture(t, fixtureIRWithRows("model-engine", uniformFixtureRows(8)))
			before := live.DeepCopy()
			decoder := irstatus.NewDecoder(testColumnarBound)

			outcome, err := RepairInstanceStatus(ctx, c, tc.target, testColumnarBound, InstanceStatusRepair{
				Live: live, ResourceVersion: live.ResourceVersion, Replacement: replacementOf(tc.rows),
			}, true)
			if err != nil {
				t.Fatalf("apply: %v", err)
			}
			if !outcome.Applied || outcome.SelectedEncoding != tc.want || outcome.ResourceVersion == before.ResourceVersion {
				t.Fatalf("apply outcome = %+v", outcome)
			}
			stored, encoding := storedEncoding(t, c, client.ObjectKeyFromObject(live))
			if encoding != tc.want || stored.ResourceVersion != outcome.ResourceVersion {
				t.Fatalf("stored as %s (rv %s), want %s (rv %s)", encoding, stored.ResourceVersion, tc.want, outcome.ResourceVersion)
			}
			written, _ := json.Marshal(&stored.Status)
			if len(written) != outcome.SelectedBytes {
				t.Fatalf("stored status is %d bytes, outcome reported %d", len(written), outcome.SelectedBytes)
			}
			decoded := &v1beta1.InferenceReplica{}
			if _, err := irstatus.GetDecoded(ctx, irstatus.NewReader(c, decoder), client.ObjectKeyFromObject(live), decoded); err != nil {
				t.Fatalf("decoded read: %v", err)
			}
			wantRows := make([]v1beta1.OMENativeInstanceStatus, len(tc.rows))
			for i := range tc.rows {
				tc.rows[i].DeepCopyInto(&wantRows[i])
			}
			irstatus.ClearPodDerivedObservations(wantRows)
			if !equality.Semantic.DeepEqual(decoded.Status.InstanceStatuses, wantRows) {
				t.Fatalf("repaired rows differ from the replacement")
			}
			preserved, expected := decoded.Status, before.Status
			preserved.InstanceStatuses, expected.InstanceStatuses = nil, nil
			if !equality.Semantic.DeepEqual(preserved, expected) {
				t.Fatalf("repair changed status outside the per-Instance representation:\n got:  %+v\n want: %+v", preserved, expected)
			}
			if !equality.Semantic.DeepEqual(live.Status, before.Status) || live.ResourceVersion != before.ResourceVersion {
				t.Fatal("the caller's live object must not be modified")
			}
		})
	}
}

// TestRepairInstanceStatusRepairsUndecodableObject pins the purpose of the
// entry point: a stored payload the codec refuses is replaced without being
// decoded, and the object reconciles again afterwards.
func TestRepairInstanceStatusRepairsUndecodableObject(t *testing.T) {
	ctx := context.Background()
	rows := uniformFixtureRows(8)
	corrupt := columnarTwin(t, fixtureIRWithRows("model-engine", rows))
	corrupt.Status.InstanceStatusColumns.Members = "1-0"
	c, live := repairFixture(t, corrupt)
	decoder := irstatus.NewDecoder(testColumnarBound)
	if _, err := irstatus.GetDecoded(ctx, irstatus.NewReader(c, decoder), client.ObjectKeyFromObject(live), &v1beta1.InferenceReplica{}); err == nil {
		t.Fatal("fixture must be undecodable")
	}

	outcome, err := RepairInstanceStatus(ctx, c, irstatus.EncodingColumnarV2, testColumnarBound, InstanceStatusRepair{
		Live: live, ResourceVersion: live.ResourceVersion, Replacement: replacementOf(rows),
	}, true)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !strings.HasPrefix(outcome.StoredEncoding, "undecodable (") || !strings.Contains(outcome.StoredEncoding, string(irstatus.ErrorReasonRangeOrder)) || outcome.StoredRows != -1 {
		t.Fatalf("outcome must name the codec reason for the stored payload: %+v", outcome)
	}
	decoded := &v1beta1.InferenceReplica{}
	if _, err := irstatus.GetDecoded(ctx, irstatus.NewReader(c, decoder), client.ObjectKeyFromObject(live), decoded); err != nil {
		t.Fatalf("the repaired object must decode: %v", err)
	}
	if !equality.Semantic.DeepEqual(decoded.Status.InstanceStatuses, rows) || len(decoded.Status.Conditions) != 1 {
		t.Fatalf("repaired object differs from the replacement or lost unrelated status: %+v", decoded.Status)
	}
}
