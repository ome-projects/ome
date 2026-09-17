package irstatus

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const fuzzLimit = 4096

// FuzzDecodeColumns feeds typed wire payloads to the decoder: every outcome
// is a catalogued error or a bounded, canonical, re-encodable row set.
func FuzzDecodeColumns(f *testing.F) {
	seeds := [][]v1beta1.OMENativeInstanceStatus{
		uniformRows(1), uniformRows(64), fullFeatureRows(), wireExampleRows(), representativeRows(300),
	}
	for _, rows := range seeds {
		columns, err := EncodeColumns(rows, fuzzLimit)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(mustJSON(f, columns))
	}
	for _, raw := range []string{
		`{}`,
		`{"members":""}`,
		`{"members":"0"}`,
		`{"members":"0","phases":[]}`,
		`{"members":"0","phases":[{"value":"Ready","indexes":"0-2147483647"}]}`,
		`{"members":"0-2147483647","phases":[{"value":"Ready","indexes":"0-2147483647"}]}`,
		`{"members":"0-3","phases":[{"value":"Ready","indexes":"0-3"}],"rowOrder":[3,2,1,0]}`,
		`{"members":"0-3","phases":[{"value":"Ready","indexes":"0-3"}],"rowOrder":[0,1,2,3]}`,
		`{"members":"0-3","phases":[{"value":"Ready","indexes":"0-3"}],"entries":[{"index":2}]}`,
		`{"members":"0-3","phases":[{"value":"Ready","indexes":"0-3"}],"entries":[{"index":2,"conditions":[]}]}`,
		`{"members":"0-3","phases":[{"value":"Ready","indexes":"0-3"}],"incarnations":[{"value":-9223372036854775808,"indexes":"1"}]}`,
		`{"members":"0-3","phases":[{"value":"Ready","indexes":"0-3"}],"podCounts":[{"value":2147483647,"indexes":"0-3"}]}`,
		`{"members":"0-3","phases":[{"value":"Ready","indexes":"0-3"}],"admitted":"","activeOrdinalOne":"0-3"}`,
	} {
		f.Add([]byte(raw))
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		var columns v1beta1.InstanceStatusColumns
		if err := json.Unmarshal(raw, &columns); err != nil {
			return
		}
		rows, err := DecodeColumns(&columns, fuzzLimit)
		if err != nil {
			if rows != nil {
				t.Fatalf("failed decode returned %d rows", len(rows))
			}
			if _, ok := ErrorReasonOf(err); !ok {
				t.Fatalf("uncatalogued error: %v", err)
			}
			return
		}
		if len(rows) == 0 || len(rows) > fuzzLimit {
			t.Fatalf("accepted payload decoded to %d rows", len(rows))
		}
		encoded, err := EncodeColumns(rows, fuzzLimit)
		if err != nil {
			t.Fatalf("decoded rows are not encodable: %v", err)
		}
		decodedAgain, err := DecodeColumns(encoded, fuzzLimit)
		if err != nil {
			t.Fatalf("re-encoded payload does not decode: %v", err)
		}
		assertRowsEqual(t, rows, decodedAgain)
		encodedAgain, err := EncodeColumns(decodedAgain, fuzzLimit)
		if err != nil {
			t.Fatal(err)
		}
		assertColumnsEqual(t, encoded, encodedAgain)
	})
}

// FuzzRowsRoundTrip derives logical rows from fuzz bytes and checks that
// eligible rows round-trip losslessly and encode deterministically.
func FuzzRowsRoundTrip(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	f.Add([]byte{255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241, 240})
	f.Fuzz(func(t *testing.T, raw []byte) {
		rows := rowsFromBytes(raw)
		columns, err := EncodeColumns(rows, fuzzLimit)
		if err != nil {
			if _, ok := ErrorReasonOf(err); !ok {
				t.Fatalf("uncatalogued error: %v", err)
			}
			return
		}
		decoded, err := DecodeColumns(columns, fuzzLimit)
		if err != nil {
			t.Fatalf("encoded rows do not decode: %v", err)
		}
		ClearPodDerivedObservations(rows)
		assertRowsEqual(t, rows, decoded)
		assertColumnsEqual(t, columns, mustEncode(t, rows, fuzzLimit))
	})
}

// rowsFromBytes is a deterministic row generator over fuzz input: each row
// consumes a few bytes that select its index, phase, values, and records.
func rowsFromBytes(raw []byte) []v1beta1.OMENativeInstanceStatus {
	phases := []v1beta1.OMENativeInstancePhase{
		v1beta1.OMENativeInstancePending, v1beta1.OMENativeInstanceCreating, v1beta1.OMENativeInstanceReady,
		v1beta1.OMENativeInstanceUpdating, v1beta1.OMENativeInstanceRestarting, v1beta1.OMENativeInstanceMigrating,
		v1beta1.OMENativeInstanceFailed, v1beta1.OMENativeInstanceDeleting, "", "Unknown",
	}
	const rowBytes = 8
	rows := make([]v1beta1.OMENativeInstanceStatus, 0, len(raw)/rowBytes)
	for offset := 0; offset+rowBytes <= len(raw); offset += rowBytes {
		chunk := raw[offset : offset+rowBytes]
		index := int32(binary.LittleEndian.Uint16(chunk[0:2]))
		if chunk[2]&1 == 1 {
			index = -index
		}
		row := v1beta1.OMENativeInstanceStatus{
			Index:             index,
			Phase:             phases[int(chunk[3])%len(phases)],
			Incarnation:       int64(int8(chunk[4])),
			PodCount:          int32(int8(chunk[5])) / 16,
			ServingPodCount:   int32(chunk[5] % 3),
			AvailablePodCount: int32(chunk[5] % 2),
			Admitted:          chunk[6]&1 == 1,
			ActiveOrdinal:     int32(chunk[6]>>1) % 3,
			ReadyPodCount:     int32(chunk[7] % 5),
		}
		if chunk[6]&2 == 2 {
			row.RunningRevision = fmt.Sprintf("rev-%d", chunk[7]%4)
		}
		if chunk[6]&4 == 4 {
			row.TargetRevision = fmt.Sprintf("rev-%d", chunk[7]%3)
		}
		if chunk[6]&8 == 8 {
			row.ReadySince = &fixtureTime
		}
		if chunk[6]&16 == 16 {
			row.Operation = &v1beta1.InstanceOperation{ID: fmt.Sprintf("op-%d", chunk[7]), Type: v1beta1.InstanceOperationRestart, Step: "Drain", StartedAt: fixtureTime, LastProgressAt: fixtureTime}
		}
		if chunk[6]&32 == 32 {
			row.LastFailure = &v1beta1.InstanceTermination{PodName: fmt.Sprintf("pod-%d", chunk[7]), Reason: "Error", Time: fixtureTime}
		}
		if chunk[6]&64 == 64 {
			row.Conditions = []metav1.Condition{{Type: "AllPodsReady", Status: metav1.ConditionFalse, Reason: "Waiting", LastTransitionTime: fixtureTime}}
		}
		if chunk[6]&128 == 128 {
			row.NodesOccupied = []string{"node-a"}
		}
		rows = append(rows, row)
	}
	return rows
}
