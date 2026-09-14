package v1beta1

import (
	"encoding/json"
	"testing"
)

func TestTrafficMapEntry_ObservedProvenanceIsOmittedWhenUnused(t *testing.T) {
	// An entry generated without the optional observed inputs must serialize
	// exactly as it did before they existed: no probe object, no capacity
	// source, no reported count. A consumer that has not opted in should not be
	// able to tell the fields were added.
	raw, err := json.Marshal(TrafficMapEntry{Cluster: "cluster-a", Weight: 70, Healthy: true})
	if err != nil {
		t.Fatalf("Marshal entry: %v", err)
	}
	if got, want := string(raw), `{"cluster":"cluster-a","weight":70,"healthy":true}`; got != want {
		t.Fatalf("entry without observed inputs:\n got %s\nwant %s", got, want)
	}
}

func TestTrafficMapCapacity_ReportedDistinguishesZeroFromAbsent(t *testing.T) {
	// Reported is a pointer because "the home reported it can serve nothing"
	// and "the home was never asked" must not collapse to the same value. The
	// first is a real signal that drives Allocated to 0; the second means the
	// control-plane plan stands. A plain int32 would render both as 0.
	zero := int32(0)
	withZero, err := json.Marshal(TrafficMapCapacity{Allocated: 7, Reported: &zero})
	if err != nil {
		t.Fatalf("Marshal reported-zero: %v", err)
	}
	if got, want := string(withZero), `{"allocated":7,"reported":0}`; got != want {
		t.Fatalf("reported zero:\n got %s\nwant %s", got, want)
	}

	absent, err := json.Marshal(TrafficMapCapacity{Allocated: 7})
	if err != nil {
		t.Fatalf("Marshal reported-absent: %v", err)
	}
	if got, want := string(absent), `{"allocated":7}`; got != want {
		t.Fatalf("reported absent:\n got %s\nwant %s", got, want)
	}
}

func TestTrafficMapProbe_RoundTrips(t *testing.T) {
	// ConsecutiveFailures is load-bearing for hysteresis, so it has to survive a
	// round trip even when the result is Failing but the gate has not tripped.
	in := TrafficMapProbe{
		Result:              ProbeResultFailing,
		ConsecutiveFailures: 2,
		Message:             "502 Bad Gateway",
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal probe: %v", err)
	}
	var out TrafficMapProbe
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("Unmarshal probe: %v", err)
	}
	if out != in {
		t.Fatalf("probe round trip:\n got %+v\nwant %+v", out, in)
	}
}
