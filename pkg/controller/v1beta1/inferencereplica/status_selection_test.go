package inferencereplica

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
)

const (
	selectionFixtureRevision      = "example-engine-2f32f6fe"
	selectionFixtureOtherRevision = "example-engine-9b1c0d2e"
)

var selectionFixtureTime = metav1.NewTime(time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC))

// uniformFixtureRows mirrors the codec package's steady-state fixture: every
// row Ready on one revision at incarnation 1 with single-pod counts and
// admission.
func uniformFixtureRows(n int) []v1beta1.OMENativeInstanceStatus {
	rows := make([]v1beta1.OMENativeInstanceStatus, n)
	for i := range rows {
		rows[i] = v1beta1.OMENativeInstanceStatus{
			Index:             int32(i),
			Incarnation:       1,
			Phase:             v1beta1.OMENativeInstanceReady,
			RunningRevision:   selectionFixtureRevision,
			PodCount:          1,
			ServingPodCount:   1,
			AvailablePodCount: 1,
			Admitted:          true,
		}
	}
	return rows
}

// massFailureFixtureRows mirrors the shape of the codec package's
// mass-failure fixture: 4,000 single-pod rows after a widespread failure,
// 2,812 Ready, 814 Restarting with an in-flight Restart, 374 Failed with a
// preserved Restart, every row carrying a lastFailure record of about 130
// bytes as stored, and one row on a second revision. Phases are spread over
// the index space by a fixed stride so the index sets fragment.
func massFailureFixtureRows() []v1beta1.OMENativeInstanceStatus {
	const (
		rowCount        = 4000
		readyCount      = 2812
		restartingCount = 814
		stride          = 1103
	)
	rows := make([]v1beta1.OMENativeInstanceStatus, rowCount)
	for i := range rows {
		row := v1beta1.OMENativeInstanceStatus{
			Index:           int32(i),
			Incarnation:     1,
			RunningRevision: selectionFixtureRevision,
			LastFailure: &v1beta1.InstanceTermination{
				Reason:  "DeadlineExceeded",
				Message: "DeadlineExceeded: Restart/Drain exceeded InstanceReadyTimeout",
				Time:    selectionFixtureTime,
			},
		}
		operation := &v1beta1.InstanceOperation{
			ID:             fmt.Sprintf("2f6b6c8e-0000-4000-8000-%012d", i),
			Type:           v1beta1.InstanceOperationRestart,
			StartedAt:      selectionFixtureTime,
			LastProgressAt: selectionFixtureTime,
			Deadline:       metav1.NewTime(selectionFixtureTime.Add(30 * time.Minute)),
			TargetRevision: selectionFixtureRevision,
			Reason:         "PodFailed",
		}
		switch rank := (i * stride) % rowCount; {
		case rank < readyCount:
			row.Phase = v1beta1.OMENativeInstanceReady
			row.Incarnation = 2
			row.PodCount, row.ServingPodCount, row.AvailablePodCount = 1, 1, 1
			row.Admitted = true
		case rank < readyCount+restartingCount:
			row.Phase = v1beta1.OMENativeInstanceRestarting
			operation.Step = "WaitReady"
			row.Operation = operation
		default:
			row.Phase = v1beta1.OMENativeInstanceFailed
			operation.Step = "Recreate"
			operation.RetryCount = 3
			operation.Reason = "RestartBudgetExhausted"
			row.Operation = operation
		}
		rows[i] = row
	}
	rows[rowCount-1].RunningRevision = selectionFixtureOtherRevision
	return rows
}

// fixtureIRWithRows is a baseline IR whose status carries rows.
func fixtureIRWithRows(name string, rows []v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplica {
	ir := baselineIR(name, "default", int32(len(rows)))
	ir.Status.Replicas = int32(len(rows))
	ir.Status.CurrentRevision = selectionFixtureRevision
	ir.Status.UpdateRevision = selectionFixtureRevision
	ir.Status.InstanceStatuses = rows
	return ir
}

func writeLabels(encoding, result string) map[string]string {
	return map[string]string{"encoding": encoding, "result": result}
}

func bytesGaugeLabels(ir *v1beta1.InferenceReplica, encoding string) map[string]string {
	return map[string]string{"namespace": ir.Namespace, "name": ir.Name, "component": string(ir.Spec.Component), "encoding": encoding}
}

func otherEncodingLabel(encoding string) string {
	if encoding == obsmetrics.IRStatusEncodingDenseV1 {
		return obsmetrics.IRStatusEncodingColumnarV2
	}
	return obsmetrics.IRStatusEncodingDenseV1
}

// storedEncoding reads the raw stored object and classifies its representation.
func storedEncoding(t *testing.T, c client.Client, key client.ObjectKey) (*v1beta1.InferenceReplica, irstatus.Encoding) {
	t.Helper()
	stored := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), key, stored); err != nil {
		t.Fatalf("get stored IR: %v", err)
	}
	encoding, err := irstatus.ObservedEncoding(&stored.Status)
	if err != nil {
		t.Fatalf("stored object does not carry exactly one representation: %v", err)
	}
	return stored, encoding
}

// TestStatusWriterSelectsRepresentationByTarget drives a logical mutation
// through the writer under each target for the uniform and mass-failure
// fixtures and a two-row object: a DenseV1 target always stores the dense
// list; a ColumnarV2 target stores columns only when they are strictly
// smaller and the dense list otherwise. The stored object carries exactly
// one representation, decodes back to the same rows, and the bytes gauge and
// write counters carry the encoding that was written.
func TestStatusWriterSelectsRepresentationByTarget(t *testing.T) {
	cases := []struct {
		name   string
		target irstatus.Encoding
		rows   []v1beta1.OMENativeInstanceStatus
		want   irstatus.Encoding
	}{
		{name: "DenseV1 target, uniform", target: irstatus.EncodingDenseV1, rows: uniformFixtureRows(500), want: irstatus.EncodingDenseV1},
		{name: "DenseV1 target, mass failure", target: irstatus.EncodingDenseV1, rows: massFailureFixtureRows(), want: irstatus.EncodingDenseV1},
		{name: "ColumnarV2 target, uniform", target: irstatus.EncodingColumnarV2, rows: uniformFixtureRows(500), want: irstatus.EncodingColumnarV2},
		{name: "ColumnarV2 target, mass failure", target: irstatus.EncodingColumnarV2, rows: massFailureFixtureRows(), want: irstatus.EncodingColumnarV2},
		{name: "ColumnarV2 target, two rows fall back to DenseV1", target: irstatus.EncodingColumnarV2, rows: decodeFixtureIR().Status.InstanceStatuses, want: irstatus.EncodingDenseV1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			ir := fixtureIRWithRows("model-engine", tc.rows)
			c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ir).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
			key := client.ObjectKeyFromObject(ir)
			writer := statusWriter{Client: c, decoder: irstatus.NewDecoder(testColumnarBound), target: tc.target}
			wantLabel, otherLabel := encodingLabel(tc.want), otherEncodingLabel(encodingLabel(tc.want))
			before := map[string]float64{}
			for _, label := range []string{wantLabel, otherLabel} {
				for _, result := range []string{obsmetrics.IRStatusWriteAttempt, obsmetrics.IRStatusWriteCommitted} {
					before[label+result] = irStatusMetric(t, "ome_omenative_ir_status_writes_total", writeLabels(label, result))
				}
			}

			live := &v1beta1.InferenceReplica{}
			if _, err := irstatus.GetDecoded(ctx, irstatus.NewReader(c, writer.decoder), key, live); err != nil {
				t.Fatalf("get InferenceReplica: %v", err)
			}
			live.Status.ObservedGeneration = 7
			wantRows := make([]v1beta1.OMENativeInstanceStatus, len(tc.rows))
			for i := range tc.rows {
				wantRows[i] = *tc.rows[i].DeepCopy()
			}
			irstatus.ClearPodDerivedObservations(wantRows)

			if err := updateInferenceReplicaStatus(ctx, writer, live, irstatus.EncodingDenseV1); err != nil {
				t.Fatalf("update status: %v", err)
			}

			stored, got := storedEncoding(t, c, key)
			if got != tc.want {
				t.Fatalf("stored encoding = %s, want %s", got, tc.want)
			}
			if stored.Status.ObservedGeneration != 7 {
				t.Fatalf("logical mutation was not persisted: %+v", stored.Status)
			}
			switch tc.want {
			case irstatus.EncodingColumnarV2:
				if len(stored.Status.InstanceStatuses) != 0 || stored.Status.InstanceStatusColumns == nil {
					t.Fatalf("a ColumnarV2 object must carry columns and no dense rows")
				}
			case irstatus.EncodingDenseV1:
				if stored.Status.InstanceStatusEncoding != nil || stored.Status.InstanceStatusColumns != nil {
					t.Fatalf("a DenseV1 object must carry neither marker nor columns")
				}
			}
			decoded := &v1beta1.InferenceReplica{}
			source, err := irstatus.GetDecoded(ctx, irstatus.NewReader(c, writer.decoder), key, decoded)
			if err != nil || source != tc.want {
				t.Fatalf("decoded read: source %q err %v", source, err)
			}
			if !equality.Semantic.DeepEqual(decoded.Status.InstanceStatuses, wantRows) {
				t.Fatalf("rows read back after a %s write differ from the logical rows", tc.want)
			}
			if !equality.Semantic.DeepEqual(live.Status, decoded.Status) {
				t.Fatalf("the object after the write must equal a fresh decoded read")
			}

			written, err := json.Marshal(&stored.Status)
			if err != nil {
				t.Fatalf("marshal stored status: %v", err)
			}
			if gauge := irStatusMetric(t, "ome_omenative_ir_status_bytes", bytesGaugeLabels(ir, wantLabel)); int(gauge) != len(written) {
				t.Fatalf("bytes gauge (%s) = %v, want the stored status size %d", wantLabel, gauge, len(written))
			}
			if gauge := irStatusMetric(t, "ome_omenative_ir_status_bytes", bytesGaugeLabels(ir, otherLabel)); gauge != 0 {
				t.Fatalf("bytes gauge for the unselected encoding %s must be absent, got %v", otherLabel, gauge)
			}
			for _, result := range []string{obsmetrics.IRStatusWriteAttempt, obsmetrics.IRStatusWriteCommitted} {
				if got := irStatusMetric(t, "ome_omenative_ir_status_writes_total", writeLabels(wantLabel, result)) - before[wantLabel+result]; got != 1 {
					t.Fatalf("%s/%s delta = %v, want 1", wantLabel, result, got)
				}
				if got := irStatusMetric(t, "ome_omenative_ir_status_writes_total", writeLabels(otherLabel, result)) - before[otherLabel+result]; got != 0 {
					t.Fatalf("%s/%s delta = %v, want 0", otherLabel, result, got)
				}
			}
			t.Logf("%s: stored %s, %d bytes", tc.name, got, len(written))
		})
	}
}

// TestStatusWriterExactTieSelectsDenseV1 finds an exact size tie between the
// two candidates and proves the writer stores the dense list for it.
func TestStatusWriterExactTieSelectsDenseV1(t *testing.T) {
	ctx := context.Background()
	var tieRows []v1beta1.OMENativeInstanceStatus
search:
	for rowCount := 1; rowCount <= 6; rowCount++ {
		for shared := 0; shared <= 1; shared++ {
			for length := 0; length <= 200; length++ {
				rows := uniformFixtureRows(rowCount)
				for i := range rows {
					revision := strings.Repeat("r", length)
					if shared == 0 {
						revision = fmt.Sprintf("%s%d", revision, i)
					}
					rows[i].RunningRevision = revision
					rows[i].Admitted = i%2 == 0
				}
				selection, err := irstatus.SelectCandidate(&v1beta1.InferenceReplicaStatus{InstanceStatuses: rows}, testColumnarBound)
				if err != nil {
					t.Fatalf("SelectCandidate: %v", err)
				}
				if selection.ColumnarV2 != nil && selection.ColumnarV2.Size == selection.DenseV1.Size {
					tieRows = rows
					break search
				}
			}
		}
	}
	if tieRows == nil {
		t.Fatal("no exact tie constructed")
	}

	ir := fixtureIRWithRows("model-engine", tieRows)
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ir).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
	key := client.ObjectKeyFromObject(ir)
	live := &v1beta1.InferenceReplica{}
	if _, err := irstatus.GetDecoded(ctx, c, key, live); err != nil {
		t.Fatalf("get InferenceReplica: %v", err)
	}
	live.Status.ObservedGeneration = 2
	if err := updateInferenceReplicaStatus(ctx, columnarStatusWriter(c), live, irstatus.EncodingDenseV1); err != nil {
		t.Fatalf("update status: %v", err)
	}
	if _, got := storedEncoding(t, c, key); got != irstatus.EncodingDenseV1 {
		t.Fatalf("an exact tie stored %s, want DenseV1", got)
	}
	if gauge := irStatusMetric(t, "ome_omenative_ir_status_bytes", bytesGaugeLabels(ir, obsmetrics.IRStatusEncodingColumnarV2)); gauge != 0 {
		t.Fatalf("a tie must not publish a ColumnarV2 bytes series, got %v", gauge)
	}
}

// TestStatusWriterSwitchesGaugeSeriesWithTheSelectedEncoding pins the gauge
// on a representation switch driven by a logical mutation: after columns are
// stored, a mutation that makes every row unique selects the dense list and
// the ColumnarV2 series is deleted.
func TestStatusWriterSwitchesGaugeSeriesWithTheSelectedEncoding(t *testing.T) {
	ctx := context.Background()
	ir := fixtureIRWithRows("model-engine", uniformFixtureRows(64))
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ir).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
	key := client.ObjectKeyFromObject(ir)
	writer := columnarStatusWriter(c)
	reader := irstatus.NewReader(c, writer.decoder)

	live := &v1beta1.InferenceReplica{}
	if _, err := irstatus.GetDecoded(ctx, reader, key, live); err != nil {
		t.Fatalf("get InferenceReplica: %v", err)
	}
	if err := updateInferenceReplicaStatus(ctx, writer, live, irstatus.EncodingDenseV1); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if _, got := storedEncoding(t, c, key); got != irstatus.EncodingColumnarV2 {
		t.Fatalf("64 uniform rows stored %s, want ColumnarV2", got)
	}
	if gauge := irStatusMetric(t, "ome_omenative_ir_status_bytes", bytesGaugeLabels(ir, obsmetrics.IRStatusEncodingColumnarV2)); gauge == 0 {
		t.Fatal("ColumnarV2 bytes series must exist after a ColumnarV2 write")
	}

	// A scale-down to two rows is a logical mutation for which the dense
	// list is the smaller candidate, so the same writer switches back.
	fresh := &v1beta1.InferenceReplica{}
	source, err := irstatus.GetDecoded(ctx, reader, key, fresh)
	if err != nil || source != irstatus.EncodingColumnarV2 {
		t.Fatalf("re-read: source %q err %v", source, err)
	}
	fresh.Status.InstanceStatuses = fresh.Status.InstanceStatuses[:2]
	fresh.Status.Replicas = 2
	if err := updateInferenceReplicaStatus(ctx, writer, fresh, source); err != nil {
		t.Fatalf("second write: %v", err)
	}
	stored, got := storedEncoding(t, c, key)
	if got != irstatus.EncodingDenseV1 || len(stored.Status.InstanceStatuses) != 2 {
		t.Fatalf("two rows stored as %s with %d rows, want DenseV1 with 2", got, len(stored.Status.InstanceStatuses))
	}
	written, err := json.Marshal(&stored.Status)
	if err != nil {
		t.Fatalf("marshal stored status: %v", err)
	}
	if gauge := irStatusMetric(t, "ome_omenative_ir_status_bytes", bytesGaugeLabels(ir, obsmetrics.IRStatusEncodingDenseV1)); int(gauge) != len(written) {
		t.Fatalf("DenseV1 bytes gauge = %v, want %d", gauge, len(written))
	}
	if gauge := irStatusMetric(t, "ome_omenative_ir_status_bytes", bytesGaugeLabels(ir, obsmetrics.IRStatusEncodingColumnarV2)); gauge != 0 {
		t.Fatalf("ColumnarV2 bytes series must be deleted after the switch, got %v", gauge)
	}
}

// TestStatusWriterRefusesUnsetTarget pins that a writer without a configured
// target refuses every write before any request instead of assuming DenseV1.
func TestStatusWriterRefusesUnsetTarget(t *testing.T) {
	ctx := context.Background()
	ir := decodeFixtureIR()
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ir).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
	key := client.ObjectKeyFromObject(ir)
	before := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, key, before); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	unset := statusWriter{Client: c}
	if err := updateInferenceReplicaStatus(ctx, unset, before.DeepCopy(), irstatus.EncodingDenseV1); !errors.Is(err, ErrInstanceStatusTargetUnset) {
		t.Fatalf("logical mutation without a target must be refused with ErrInstanceStatusTargetUnset, got %v", err)
	}
	if err := convertInferenceReplicaStatus(ctx, unset, before.DeepCopy(), irstatus.EncodingColumnarV2); !errors.Is(err, ErrInstanceStatusTargetUnset) {
		t.Fatalf("conversion without a target must be refused with ErrInstanceStatusTargetUnset, got %v", err)
	}
	after := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, key, after); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	if after.ResourceVersion != before.ResourceVersion {
		t.Fatal("a refused write must not reach the API server")
	}
}
