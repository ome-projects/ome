package irstatus

import (
	"bytes"
	"encoding/json"
	"strconv"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// denseStatusSize is the length of the JSON encoding/json writes for a
// status in the DenseV1 shape, counted without serializing the rows: the
// fields beside the rows are encoded once, small at any fleet size, and
// each row is summed field by field the way the encoder writes it. At
// 4,000 rows the list is nearly every byte of the status, so a write's
// size check costs the sum, not a second copy of the status.
func denseStatusSize(status *v1beta1.InferenceReplicaStatus) (int, error) {
	return newDenseSizer().statusSize(status)
}

// denseSizer counts the JSON length of one dense status. What it cannot
// count by hand, the fields beside the rows and the records a row carries,
// goes through one encoder into one reused buffer, so a status whose rows
// all carry records allocates no more than the encoder itself does.
type denseSizer struct {
	buffer  bytes.Buffer
	encoder *json.Encoder
}

func newDenseSizer() *denseSizer {
	sizer := &denseSizer{}
	sizer.encoder = json.NewEncoder(&sizer.buffer)
	return sizer
}

func (s *denseSizer) statusSize(status *v1beta1.InferenceReplicaStatus) (int, error) {
	rest := *status
	rest.InstanceStatuses = nil
	size, err := s.encodedSize(&rest)
	if err != nil {
		return 0, err
	}
	if len(status.InstanceStatuses) == 0 {
		return size, nil
	}
	rows, err := s.rowsSize(status.InstanceStatuses)
	if err != nil {
		return 0, err
	}
	if size > len("{}") {
		size++
	}
	return size + len(`"instanceStatuses":`) + rows, nil
}

// rowsSize is the length of the rows as a JSON list: the brackets, a comma
// between neighbours, and every row.
func (s *denseSizer) rowsSize(rows []v1beta1.OMENativeInstanceStatus) (int, error) {
	size := len("[]") + len(rows) - 1
	for i := range rows {
		row, err := s.rowSize(&rows[i])
		if err != nil {
			return 0, err
		}
		size += row
	}
	return size, nil
}

// rowSize is the length of one row as the encoder writes it. Each field
// follows its JSON tag: a field tagged omitempty is absent at its zero
// value, numbers are their decimal digits, a time is its RFC 3339 text,
// and the nested records are measured by the encoder itself.
// TestDenseRowSizeCountsEveryFieldOfTheRow holds this list to the row type.
func (s *denseSizer) rowSize(row *v1beta1.OMENativeInstanceStatus) (int, error) {
	object := jsonObjectSize{sizer: s}
	object.integer("index", int64(row.Index))
	if row.Incarnation != 0 {
		object.integer("incarnation", row.Incarnation)
	}
	object.text("phase", string(row.Phase))
	if row.RunningRevision != "" {
		object.text("runningRevision", row.RunningRevision)
	}
	if row.TargetRevision != "" {
		object.text("targetRevision", row.TargetRevision)
	}
	if row.PodCount != 0 {
		object.integer("podCount", int64(row.PodCount))
	}
	if row.ReadyPodCount != 0 {
		object.integer("readyPodCount", int64(row.ReadyPodCount))
	}
	if row.ServingPodCount != 0 {
		object.integer("servingPodCount", int64(row.ServingPodCount))
	}
	if row.AvailablePodCount != 0 {
		object.integer("availablePodCount", int64(row.AvailablePodCount))
	}
	if row.ScheduledPodCount != 0 {
		object.integer("scheduledPodCount", int64(row.ScheduledPodCount))
	}
	if row.Admitted {
		object.field("admitted", len("true"))
	}
	if len(row.NodesOccupied) > 0 {
		object.encoded("nodesOccupied", row.NodesOccupied)
	}
	if len(row.Conditions) > 0 {
		object.encoded("conditions", row.Conditions)
	}
	if row.ReadySince != nil {
		object.time("readySince", row.ReadySince)
	}
	if row.Operation != nil {
		object.encoded("operation", row.Operation)
	}
	if row.ActiveOrdinal != 0 {
		object.integer("activeOrdinal", int64(row.ActiveOrdinal))
	}
	if row.LastFailure != nil {
		object.encoded("lastFailure", row.LastFailure)
	}
	if len(row.Announced) > 0 {
		object.encoded("announced", row.Announced)
	}
	return object.total(), object.err
}

// encodedSize is the length the encoder writes for value.
func (s *denseSizer) encodedSize(value any) (int, error) {
	s.buffer.Reset()
	if err := s.encoder.Encode(value); err != nil {
		return 0, err
	}
	return s.buffer.Len() - len("\n"), nil
}

// jsonObjectSize sums one JSON object as encoding/json writes a struct: a
// quoted key, a colon and the value per written field, a comma between
// written fields, and the braces. The first failure of the encoder on a
// nested record is kept and returned with the total.
type jsonObjectSize struct {
	sizer  *denseSizer
	size   int
	fields int
	err    error
}

// field adds one written field whose value takes valueSize bytes.
func (o *jsonObjectSize) field(key string, valueSize int) {
	if o.fields > 0 {
		o.size++
	}
	o.fields++
	o.size += len(`"":`) + len(key) + valueSize
}

func (o *jsonObjectSize) integer(key string, value int64) {
	var digits [20]byte
	o.field(key, len(strconv.AppendInt(digits[:0], value, 10)))
}

// text adds a string field. Printable ASCII without the five characters
// the encoder escapes (the quote, the backslash, and the three HTML
// characters) is written as is between two quotes; any other text is
// measured by the encoder.
func (o *jsonObjectSize) text(key, value string) {
	for i := 0; i < len(value); i++ {
		if !plainJSONByte(value[i]) {
			o.encoded(key, value)
			return
		}
	}
	o.field(key, len(value)+len(`""`))
}

// time adds a metav1.Time field: null for the zero time, otherwise the
// RFC 3339 text in UTC between two quotes, 22 bytes for every year from 0
// to 9999. A year outside them is measured by the encoder.
func (o *jsonObjectSize) time(key string, value *metav1.Time) {
	switch {
	case value.IsZero():
		o.field(key, len("null"))
	case value.UTC().Year() < 0 || value.UTC().Year() > 9999:
		o.encoded(key, value)
	default:
		o.field(key, len(`"2006-01-02T15:04:05Z"`))
	}
}

// encoded adds a field measured by the encoder itself.
func (o *jsonObjectSize) encoded(key string, value any) {
	size, err := o.sizer.encodedSize(value)
	if err != nil && o.err == nil {
		o.err = err
	}
	o.field(key, size)
}

func (o *jsonObjectSize) total() int { return o.size + len("{}") }

// plainJSONByte reports whether the encoder writes b unescaped.
func plainJSONByte(b byte) bool {
	switch b {
	case '"', '\\', '<', '>', '&':
		return false
	}
	return b >= ' ' && b < utf8.RuneSelf
}
