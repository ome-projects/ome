package irstatus

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// The deterministic floor: at least 50,000 generated documents holding at
// least 1,000,000 logical rows, every one of which must round-trip and
// encode identically twice. Short mode runs a proportional sample.
func TestDeterministicVolumeFloor(t *testing.T) {
	documents, minimumRows := 50_000, 1_000_000
	if testing.Short() {
		documents, minimumRows = 2_000, 40_000
	}
	source := rand.New(rand.NewPCG(7, 11))
	totalRows := 0
	ineligible := 0
	columnarSelected := 0
	for document := 0; document < documents; document++ {
		rows := generatedRows(source, document)
		totalRows += len(rows)
		columns, err := EncodeColumns(rows, testLimit)
		if err != nil {
			ineligible++
			if _, ok := ErrorReasonOf(err); !ok {
				t.Fatalf("document %d: uncatalogued error %v", document, err)
			}
			continue
		}
		decoded, err := DecodeColumns(columns, testLimit)
		if err != nil {
			t.Fatalf("document %d: decode error %v", document, err)
		}
		assertRowsEqual(t, rows, decoded)
		if !bytes.Equal(mustJSON(t, columns), mustJSON(t, mustEncode(t, decoded, testLimit))) {
			t.Fatalf("document %d: encoding is not stable", document)
		}
		if document%1000 == 0 {
			selection, err := SelectCandidate(logicalStatus(rows), testLimit)
			if err != nil {
				t.Fatalf("document %d: SelectCandidate() error %v", document, err)
			}
			if selection.Selected == EncodingColumnarV2 {
				columnarSelected++
			}
		}
	}
	t.Logf("documents %d, rows %d, ineligible documents %d, sampled columnar selections %d", documents, totalRows, ineligible, columnarSelected)
	if totalRows < minimumRows {
		t.Fatalf("generated %d rows, want at least %d", totalRows, minimumRows)
	}
	if ineligible > documents/100 {
		t.Fatalf("%d of %d documents were ineligible; the generator should produce mostly encodable rows", ineligible, documents)
	}
}

// generatedRows varies row count, sparsity, order, and every grouped and
// exceptional field so the floor covers steady, transitional, and adversarial
// shapes. Most documents are small; a few carry thousands of rows.
func generatedRows(source *rand.Rand, document int) []v1beta1.OMENativeInstanceStatus {
	count := 1 + source.IntN(30)
	switch {
	case document%997 == 0:
		count = 4000 + source.IntN(1001)
	case document%101 == 0:
		count = 200 + source.IntN(800)
	}
	phases := []v1beta1.OMENativeInstancePhase{
		v1beta1.OMENativeInstancePending, v1beta1.OMENativeInstanceCreating, v1beta1.OMENativeInstanceReady,
		v1beta1.OMENativeInstanceUpdating, v1beta1.OMENativeInstanceRestarting, v1beta1.OMENativeInstanceMigrating,
		v1beta1.OMENativeInstanceFailed, v1beta1.OMENativeInstanceDeleting,
	}
	uniform := source.IntN(4) == 0
	sparse := source.IntN(3) == 0
	revisionPool := 1 + source.IntN(4)
	rows := make([]v1beta1.OMENativeInstanceStatus, count)
	index := int32(0)
	for i := range rows {
		if sparse {
			index += int32(source.IntN(3))
		}
		row := v1beta1.OMENativeInstanceStatus{
			Index:             index,
			Phase:             v1beta1.OMENativeInstanceReady,
			RunningRevision:   fixtureRevision,
			Incarnation:       1,
			PodCount:          1,
			ServingPodCount:   1,
			AvailablePodCount: 1,
			Admitted:          true,
		}
		if !uniform {
			row.Phase = phases[source.IntN(len(phases))]
			row.RunningRevision = fmt.Sprintf("example-engine-%08x", source.IntN(revisionPool))
			if source.IntN(5) == 0 {
				row.RunningRevision = ""
			}
			if source.IntN(4) == 0 {
				row.TargetRevision = fmt.Sprintf("example-engine-%08x", revisionPool+source.IntN(2))
			}
			row.Incarnation = int64(source.IntN(6)) - 1
			row.PodCount = int32(source.IntN(9))
			row.ServingPodCount = int32(source.IntN(9))
			row.AvailablePodCount = int32(source.IntN(9))
			row.Admitted = source.IntN(3) != 0
			row.ActiveOrdinal = int32(source.IntN(2))
			if source.IntN(8) == 0 {
				stamp := fixtureTimePlus(time.Duration(source.Int64N(int64(time.Hour))))
				row.ReadySince = &stamp
			}
			if source.IntN(10) == 0 {
				row.Operation = &v1beta1.InstanceOperation{
					ID: fmt.Sprintf("op-%d-%d", document, i), Type: v1beta1.InstanceOperationUpdate, Step: "Drain",
					StartedAt: fixtureTime, LastProgressAt: fixtureTime, Deadline: fixtureTimePlus(time.Hour), TargetRevision: row.TargetRevision,
				}
			}
			if source.IntN(10) == 0 {
				exitCode := int32(source.IntN(256))
				row.LastFailure = &v1beta1.InstanceTermination{PodName: fmt.Sprintf("pod-%d", i), Reason: "Error", ExitCode: &exitCode, Message: "exit", Time: fixtureTime}
			}
			if source.IntN(12) == 0 {
				row.Conditions = []metav1.Condition{{Type: "AllPodsReady", Status: metav1.ConditionTrue, Reason: "Ready", LastTransitionTime: fixtureTime}}
			}
		}
		rows[i] = row
		index++
	}
	if source.IntN(5) == 0 {
		source.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
	}
	return rows
}
