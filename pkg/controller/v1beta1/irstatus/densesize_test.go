package irstatus

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// denseSizeFixtures are the statuses whose counted size is held to the
// encoder: every row fixture the codec tests use, with and without the
// status fields beside the rows, the shapes without rows, and rows that
// carry the Pod-derived fields a write clears.
func denseSizeFixtures() map[string]*v1beta1.InferenceReplicaStatus {
	fixtures := map[string]*v1beta1.InferenceReplicaStatus{
		"empty":        {},
		"noRows":       {Replicas: 3, CurrentRevision: fixtureRevision},
		"emptyRows":    {Replicas: 3, InstanceStatuses: []v1beta1.OMENativeInstanceStatus{}},
		"rowsOnly/1":   {InstanceStatuses: uniformRows(1)},
		"rowsOnly/2":   {InstanceStatuses: uniformRows(2)},
		"podDerived/3": {InstanceStatuses: fullFeatureRows()[:3]},
	}
	rowSets := map[string][]v1beta1.OMENativeInstanceStatus{
		"uniform/3":           uniformRows(3),
		"uniform/2000":        uniformRows(2000),
		"readySince/4000":     readySinceRows(4000),
		"representative/300":  representativeRows(300),
		"representative/4000": representativeRows(4000),
		"massFailure/4000":    massFailureRows(),
		"fullFeature/8":       fullFeatureRows(),
		"wireExample/500":     wireExampleRows(),
		"adversarial/2000":    adversarialRows(2000),
		"fuzzRows/32":         rowsFromBytes(bytes.Repeat([]byte{5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 255, 254, 253, 252, 251, 250, 249, 248}, 11)),
	}
	for name, rows := range rowSets {
		fixtures[name] = logicalStatus(rows)
		beside := logicalStatus(rows)
		beside.Conditions = []metav1.Condition{{Type: "Available", Status: metav1.ConditionTrue, Reason: "Ready", LastTransitionTime: fixtureTime}}
		beside.RetryBlocks = []v1beta1.RetryBlock{{TargetRevision: fixtureOtherRevision, State: v1beta1.RetryBlockHeld, AttemptsStarted: 3}}
		fixtures[name+"/withStatusFields"] = beside
	}
	return fixtures
}

// The counted size of a dense status is the length the encoder writes, on
// every fixture, and counting leaves the status as it was.
func TestDenseStatusSizeMatchesTheEncoder(t *testing.T) {
	t.Parallel()
	for name, status := range denseSizeFixtures() {
		before := mustJSON(t, status)
		got, err := denseStatusSize(status)
		if err != nil {
			t.Fatalf("%s: denseStatusSize() error = %v", name, err)
		}
		if got != len(before) {
			t.Errorf("%s: denseStatusSize() = %d, encoder writes %d bytes", name, got, len(before))
		}
		if after := mustJSON(t, status); !bytes.Equal(before, after) {
			t.Errorf("%s: counting changed the status", name)
		}
	}
}

// Every field of the row type is counted as the encoder writes it: alone on
// an otherwise zero row, all together, and none. A field added to the type
// without a line in rowSize fails here.
func TestDenseRowSizeCountsEveryFieldOfTheRow(t *testing.T) {
	t.Parallel()
	assertRowSize(t, "zero", v1beta1.OMENativeInstanceStatus{})
	var every v1beta1.OMENativeInstanceStatus
	rowType := reflect.TypeOf(every)
	for i := 0; i < rowType.NumField(); i++ {
		var alone v1beta1.OMENativeInstanceStatus
		setNonZero(t, reflect.ValueOf(&alone).Elem().Field(i))
		setNonZero(t, reflect.ValueOf(&every).Elem().Field(i))
		assertRowSize(t, rowType.Field(i).Name, alone)
	}
	assertRowSize(t, "every", every)

	zeroTime := metav1.Time{}
	farFuture := metav1.NewTime(time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC))
	for name, readySince := range map[string]*metav1.Time{"readySinceZero": &zeroTime, "readySinceYear10000": &farFuture} {
		row := uniformRows(1)[0]
		row.ReadySince = readySince
		assertRowSize(t, name, row)
	}
}

func assertRowSize(t *testing.T, name string, row v1beta1.OMENativeInstanceStatus) {
	t.Helper()
	got, err := newDenseSizer().rowSize(&row)
	if err != nil {
		t.Fatalf("%s: rowSize() error = %v", name, err)
	}
	if want := len(mustJSON(t, row)); got != want {
		t.Errorf("%s: rowSize() = %d, encoder writes %d bytes: %s", name, got, want, mustJSON(t, row))
	}
}

// setNonZero sets v to a value of its type that the encoder writes in full:
// a negative number of many digits, text that needs escaping, a time with a
// zone and nanoseconds, one element per list, and every field of a record.
func setNonZero(t *testing.T, v reflect.Value) {
	t.Helper()
	if v.Type() == reflect.TypeOf(metav1.Time{}) {
		v.Set(reflect.ValueOf(metav1.NewTime(time.Date(2026, 11, 2, 21, 52, 23, 456, time.FixedZone("east", 5*3600)))))
		return
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(-1234567)
	case reflect.Bool:
		v.SetBool(true)
	case reflect.String:
		v.SetString("a<b>&\"c\\\n \x7fé")
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		setNonZero(t, v.Elem())
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		setNonZero(t, v.Index(0))
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				setNonZero(t, v.Field(i))
			}
		}
	default:
		t.Fatalf("no non-zero value for a field of kind %s (%s)", v.Kind(), v.Type())
	}
}

// A text field is counted as the encoder writes it for every single byte,
// for text around each byte, and for multi-byte and invalid UTF-8 text.
func TestTextSizeMatchesTheEncoderForEveryByte(t *testing.T) {
	t.Parallel()
	texts := []string{"", "plain text", "é", "日本", " ", " ", "\xff", "\xc3", "tab\there", "quote\"back\\slash"}
	for b := 0; b < 256; b++ {
		texts = append(texts, string([]byte{byte(b)}), "x"+string([]byte{byte(b)})+"y")
	}
	for _, text := range texts {
		object := jsonObjectSize{sizer: newDenseSizer()}
		object.text("v", text)
		if object.err != nil {
			t.Fatalf("%q: error = %v", text, object.err)
		}
		want := len(mustJSON(t, struct {
			V string `json:"v"`
		}{text}))
		if got := object.total(); got != want {
			t.Errorf("%q: counted %d, encoder writes %d bytes", text, got, want)
		}
	}
}

// A time field is counted as metav1.Time writes it: null for the zero time,
// 22 bytes for a UTC year of at most four digits, the encoder's own length
// beyond.
func TestTimeSizeMatchesTheEncoder(t *testing.T) {
	t.Parallel()
	times := map[string]metav1.Time{
		"zero":       {},
		"fixture":    fixtureTime,
		"nanos":      metav1.NewTime(time.Date(2026, 11, 2, 21, 52, 23, 999999999, time.UTC)),
		"zoned":      metav1.NewTime(time.Date(2026, 11, 2, 1, 2, 3, 0, time.FixedZone("west", -11*3600))),
		"year0":      metav1.NewTime(time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)),
		"year9999":   metav1.NewTime(time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)),
		"year10000":  metav1.NewTime(time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)),
		"yearMinus1": metav1.NewTime(time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC)),
		"year123456": metav1.NewTime(time.Date(123456, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
	for name, value := range times {
		object := jsonObjectSize{sizer: newDenseSizer()}
		object.time("t", &value)
		if object.err != nil {
			t.Fatalf("%s: error = %v", name, object.err)
		}
		want := len(mustJSON(t, struct {
			T *metav1.Time `json:"t"`
		}{&value}))
		if got := object.total(); got != want {
			t.Errorf("%s: counted %d, encoder writes %d bytes", name, got, want)
		}
	}
}

// FuzzDenseStatusSizeMatchesTheEncoder derives rows from fuzz bytes and
// holds the counted size of their status to the encoder.
func FuzzDenseStatusSizeMatchesTheEncoder(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	f.Add([]byte{255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241, 240})
	f.Fuzz(func(t *testing.T, raw []byte) {
		status := logicalStatus(rowsFromBytes(raw))
		got, err := denseStatusSize(status)
		if err != nil {
			t.Fatalf("denseStatusSize() error = %v", err)
		}
		data, err := json.Marshal(status)
		if err != nil {
			t.Fatal(err)
		}
		if got != len(data) {
			t.Fatalf("denseStatusSize() = %d, encoder writes %d bytes", got, len(data))
		}
	})
}
