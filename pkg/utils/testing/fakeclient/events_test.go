package fakeclient_test

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestRecordedEvents_ThreeRecorded_ReturnsThemOldestFirstAndEmptiesTheRecorder(t *testing.T) {
	rec := record.NewFakeRecorder(8)
	subject := pod("a", "node-1")
	rec.Event(subject, corev1.EventTypeNormal, "Created", "first")
	rec.Eventf(subject, corev1.EventTypeWarning, "Refused", "second %d", 2)
	rec.Event(subject, corev1.EventTypeNormal, "Deleted", "third")

	got := fakeclient.RecordedEvents(rec)
	want := []string{"Normal Created first", "Warning Refused second 2", "Normal Deleted third"}
	if len(got) != len(want) {
		t.Fatalf("events = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("event %d = %q, want %q", i, got[i], want[i])
		}
	}
	if again := fakeclient.RecordedEvents(rec); again != nil {
		t.Errorf("second read = %q, want nil: the first read emptied the recorder", again)
	}
}

func TestRecordedEvents_NothingRecorded_ReturnsNilWithoutWaiting(t *testing.T) {
	if got := fakeclient.RecordedEvents(record.NewFakeRecorder(1)); got != nil {
		t.Errorf("events = %q, want nil", got)
	}
}
