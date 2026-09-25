package types

import (
	"testing"
)

func TestAdapterPublished(t *testing.T) {
	pods, available := AdapterPublished(&InstanceStatus{PodCount: 3, AvailablePodCount: 2})
	if pods != 3 || available != 2 {
		t.Fatalf("published counters: got (%d, %d) want (3, 2)", pods, available)
	}
	if pods, available = AdapterPublished(nil); pods != 0 || available != 0 {
		t.Fatalf("unobserved row: got (%d, %d) want (0, 0)", pods, available)
	}
}
