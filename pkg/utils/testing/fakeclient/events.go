package fakeclient

import "k8s.io/client-go/tools/record"

// RecordedEvents returns every event the recorder holds, oldest first, and
// leaves it empty; a recorder with nothing queued answers nil at once. Each
// string is the recorder's "<type> <reason> <message>" line.
func RecordedEvents(rec *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case e := <-rec.Events:
			events = append(events, e)
		default:
			return events
		}
	}
}
